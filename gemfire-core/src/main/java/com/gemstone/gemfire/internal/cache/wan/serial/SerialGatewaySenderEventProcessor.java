/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.gemstone.gemfire.internal.cache.wan.serial;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.LogWriterImpl.GemFireThreadGroup;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl.EventWrapper;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.size.SingleObjectSizer;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 * 
 */
public class SerialGatewaySenderEventProcessor extends
    AbstractGatewaySenderEventProcessor {

  private final Object unprocessedEventsLock = new Object();

  protected static final int RANDOM_SLEEP_TIME = 1000;
  /**
   * A <code>Map</code> of events that have not been processed by the primary
   * yet. This map is created and used by a secondary <code>GatewaySender</code> to
   * keep track of events that have been received by the secondary but not yet
   * processed by the primary. Once an event has been processed by the primary,
   * it is removed from this map. This map will only be used in the event that
   * this <code>GatewaySender</code> becomes the primary. Any events contained in this
   * map will need to be sent to other <code>GatewayReceiver</code>s. Note:
   * unprocessedEventsLock MUST be synchronized before using this map.
   */
  private Map<EventID, EventWrapper> unprocessedEvents;

  /**
   * A <code>Map</code> of tokens (i.e. longs) of entries that we have heard of
   * from the primary but not yet the secondary. This map is created and used by
   * a secondary <code>GatewaySender</code> to keep track. Note: unprocessedEventsLock
   * MUST be synchronized before using this map. This is not a cut and paste
   * error. sync unprocessedEventsLock when using unprocessedTokens.
   */
  private Map<EventID, Long> unprocessedTokens;

  private ExecutorService executor;
  
  private final Object listenerObjectLock = new Object();
  
  private boolean failoverCompleted = false;

  private final Object failoverCompletedLock = new Object();
  
  /**
   * When the Number of unchecked events exceeds this threshold and the number
   * of tokens in the map exceeds this threshold then a check will be done for
   * old tokens.
   */
  static private final int REAP_THRESHOLD = 1000;

  /*
   * How many events have happened without a reap check being done?
   */
  private int uncheckedCount = 0;
  
  
  protected SerialGatewaySenderEventProcessor(SerialGatewaySenderImpl sender, String id) {
    super(LogWriterImpl.createThreadGroup(
        "Event Processor for GatewaySender_" + id,
        sender.getLogger()), "Event Processor for GatewaySender_"
        + id, sender);
    
    this.unprocessedEvents = new LinkedHashMap<EventID, EventWrapper>();
    this.unprocessedTokens = new LinkedHashMap<EventID, Long>();

    initializeMessageQueue(id);
    setDaemon(true);
  }

  @Override
  protected void initializeMessageQueue(String id) {
    // Create the region name
    StringBuilder regionNameBuffer = new StringBuilder();
    regionNameBuffer.append(id).append(
        "_SERIAL_GATEWAY_SENDER_QUEUE");
    String regionName = regionNameBuffer.toString();

    CacheListener listener = null;
    if (!this.sender.isPrimary()) {
      listener = new SerialSecondaryGatewayListener(this);
      initializeListenerExecutor();
    }
    // Create the region queue
    this.queue = new SerialGatewaySenderQueue(sender, regionName, listener);

    if (getLogger().fineEnabled()) {
      getLogger().fine("Created queue: " + this.queue);
    }
  }

  /**
   * @return false on failure
   */
  protected boolean waitForPrimary() {

    try {
      this.sender.getSenderAdvisor().waitToBecomePrimary();
    } catch (InterruptedException e) {
      // No need to set the interrupt bit, we're exiting the thread.
      if (!stopped()) {
        getLogger()
            .severe(
                LocalizedStrings.GatewayImpl_AN_INTERRUPTEDEXCEPTION_OCCURRED_THE_THREAD_WILL_EXIT,
                e);
      }
      shutdownListenerExecutor();
      return false;
    }
    try {
      shutdownListenerExecutor();

      // Don't allow conserve-sockets = false so that ordering is preserved.
      DistributedSystem.setThreadsSocketPolicy(true);

      // Once notification has occurred, handle failover
      if (!stopped()) {
        handleFailover();
      } else {
        return false;
      }
    } catch (RegionDestroyedException e) {
      // This happens during handleFailover
      // because the region on _eventQueue can be closed.
      if (!stopped()) {
        getLogger().fine("Terminating due to " + e);
      }
      return false;
    } catch (CancelException e) {
      if (!stopped()) {
        getLogger().fine("Terminating due to " + e);
      }
      return false;
    } finally {
      // This is a good thing even on termination, because it ends waits
      // on the part of other threads.
      completeFailover();
    }
    return true;
  }

  

  @Override
  public void run() {
    try {
      setRunningStatus();
      // If this is not a primary gateway, wait for notification
      if (!sender.isPrimary()) {
        if (!waitForPrimary()) {
          return;
        }
      } else {
        // we are the primary so mark failover as being completed
        completeFailover();
      }
      // Begin to process the message queue after becoming primary
      if (getLogger().fineEnabled()) {
        getLogger().fine("Beginning to process the message queue");
      }

      if (!sender.isPrimary()) {
        getLogger()
            .warning(
                LocalizedStrings.GatewayImpl_ABOUT_TO_PROCESS_THE_MESSAGE_QUEUE_BUT_NOT_THE_PRIMARY);
      }

      // Sleep for a bit. The random is so that if several of these are
      // started at once, they will stagger a bit.
      try {
        Thread.sleep(new Random().nextInt(RANDOM_SLEEP_TIME));
      } catch (InterruptedException e) {
        // no need to set the interrupt bit or throw an exception; just exit.
        return;
      }
      processQueue();
    } catch (CancelException e) {
      if (!this.isStopped()) {
        getLogger()
            .info(
                LocalizedStrings.GatewayImpl_A_CANCELLATION_OCCURRED_STOPPING_THE_DISPATCHER);
        setIsStopped(true);
      }
    } catch (Throwable e) {
      Error err;
      if (e instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)e)) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      getLogger()
          .severe(
              LocalizedStrings.GatewayImpl_MESSAGE_DISPATCH_FAILED_DUE_TO_UNEXPECTED_EXCEPTION,
              e);
    }
  }

  /**
   * Handle failover. This method is called when a secondary
   * <code>GatewaySender</code> becomes a primary <code>GatewaySender</code>.
   * 
   * Once this secondary becomes the primary, it must:
   * <ul>
   * <li>Remove the queue's CacheListener
   * <li>Process the map of unprocessed events (those it has seen but the
   * previous primary had not yet processed before it crashed). These will
   * include both queued and unqueued events. Remove from the queue any events
   * that were already sent
   * <li>Clear the unprocessed events map
   * </ul>
   */
  protected void handleFailover() {
    /*
     * We must hold this lock while we're processing these maps to prevent us
     * from handling a secondary event while failover occurs. See enqueueEvent
     */
    synchronized (this.unprocessedEventsLock) {
      // Remove the queue's CacheListener
      this.queue.removeCacheListener();
      this.unprocessedTokens = null;

      // Process the map of unprocessed events
      getLogger()
          .info(
              LocalizedStrings.GatewayImpl_GATEWAY_FAILOVER_INITIATED_PROCESSING_0_UNPROCESSED_EVENTS,
              this.unprocessedEvents.size());
      GatewaySenderStats statistics = this.sender.getStatistics();
      statistics.setQueueSize(eventQueueSize()); // to capture an initial
      // size
      if (!this.unprocessedEvents.isEmpty()) {
        // do a reap for bug 37603
        reapOld(statistics, true); // to get rid of timed out events
        // now iterate over the region queue to figure out what unprocessed
        // events are already in the queue
        {
          Iterator it = this.queue.getRegion().values().iterator();
          while (it.hasNext() && !stopped()) {
            Object o = it.next();
            if (o != null && o instanceof GatewaySenderEventImpl) {
              GatewaySenderEventImpl ge = (GatewaySenderEventImpl)o;
              EventWrapper unprocessedEvent = this.unprocessedEvents.remove(ge.getEventId());
              if (unprocessedEvent != null) {
                unprocessedEvent.event.release();
                if (this.unprocessedEvents.isEmpty()) {
                  break;
                }
              }
            }
          }
        }
        // now for every unprocessed event add it to the end of the queue
        {
          Iterator<Map.Entry<EventID, EventWrapper>> it = this.unprocessedEvents.entrySet().iterator();
          while (it.hasNext()) {
            if (stopped()) break;
            Map.Entry<EventID, EventWrapper> me = it.next();
            EventWrapper ew = me.getValue();
            GatewaySenderEventImpl gatewayEvent = ew.event;
              // Initialize each gateway event. This initializes the key,
              // value
              // and callback arg based on the EntryEvent.
              // TODO:wan70, remove dependencies from old code
              gatewayEvent.initialize();
            // Verify that they GatewayEventCallbackArgument is initialized.
            // If not, initialize it. It won't be initialized if a client to
            // this GatewayHub VM was the creator of this event. This Gateway
            // will be the first one to process it. If will be initialized if
            // this event was sent to this Gateway from another GatewayHub
            // (either directly or indirectly).
            GatewaySenderEventCallbackArgument seca = gatewayEvent
                .getSenderCallbackArgument();
            if (seca.getOriginatingDSId() == GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID) {
              seca.setOriginatingDSId(sender.getMyDSId());
              seca.initializeReceipientDSIds(Collections.singletonList(sender
                  .getRemoteDSId()));
            }
            it.remove();
            boolean queuedEvent = false;
            try {
              queuePrimaryEvent(gatewayEvent);
              queuedEvent = true;
            } catch (IOException ex) {
              if (!stopped()) {
                getLogger()
                    .warning(
                        LocalizedStrings.GatewayImpl_EVENT_DROPPED_DURING_FAILOVER_0,
                        gatewayEvent, ex);
              }
            } catch (CacheException ex) {
              if (!stopped()) {
                getLogger()
                    .warning(
                        LocalizedStrings.GatewayImpl_EVENT_DROPPED_DURING_FAILOVER_0,
                        gatewayEvent, ex);
              }
            } finally {
              if (!queuedEvent) {
                gatewayEvent.release();
              }
            }
          }
        }
        // Clear the unprocessed events map
        statistics.clearUnprocessedMaps();
      }

      // Iterate the entire queue and mark all events as possible
      // duplicate
      if (getLogger().infoEnabled()) {
         getLogger().info(LocalizedStrings.GatewayImpl_0__MARKING__1__EVENTS_AS_POSSIBLE_DUPLICATES,
         new Object[] {getSender(), Integer.valueOf(this.queue.size())});
      }
      Iterator it = this.queue.getRegion().values().iterator();
      while (it.hasNext() && !stopped()) {
        Object o = it.next();
        if (o != null && o instanceof GatewaySenderEventImpl) {
          GatewaySenderEventImpl ge = (GatewaySenderEventImpl)o;
          ge.setPossibleDuplicate(true);
        }
      }

      releaseUnprocessedEvents();
    } // synchronized
  }
  
  private void releaseUnprocessedEvents() {
    synchronized (this.unprocessedEventsLock) {
      Map<EventID, EventWrapper> m = this.unprocessedEvents;
      if (m != null) {
        for (EventWrapper ew: m.values()) {
          GatewaySenderEventImpl gatewayEvent = ew.event;
          gatewayEvent.release();
        }
        this.unprocessedEvents = null;
      }
    }
  }
  
  @Override
  public void closeProcessor() {
    try {
      super.closeProcessor();
    } finally {
      releaseUnprocessedEvents();
    }
  }

  /**
   * Add the input object to the event queue
   */
  @Override
  public void enqueueEvent(EnumListenerEvent operation, EntryEventImpl event)
      throws IOException, CacheException {
    // There is a case where the event is serialized for processing. The
    // region is not
    // serialized along with the event since it is a transient field. I
    // created an
    // intermediate object (GatewayEventImpl) to avoid this since the region
    // name is
    // used in the sendBatch method, and it can't be null. See EntryEventImpl
    // for details.
    GatewaySenderEventImpl senderEvent = null;

    boolean isPrimary = sender.isPrimary();
    if (!isPrimary) {
      // Fix for #40615. We need to check if we've now become the primary
      // while holding the unprocessedEventsLock. This prevents us from failing
      // over while we're processing an event as a secondaryEvent.
      synchronized (unprocessedEventsLock) {
        // Test whether this gateway is the primary.
        if (sender.isPrimary()) {
          isPrimary = true;
        } else {
          // If it is not, create an uninitialized GatewayEventImpl and
          // put it into the map of unprocessed events.
          senderEvent = new GatewaySenderEventImpl(operation, event, false, 
              this.sender.isNonWanDispatcher()); // OFFHEAP ok
          handleSecondaryEvent(senderEvent);
        }
      }
    }
    if (isPrimary) {
      Region region = event.getRegion();
      boolean isPDXRegion = (region instanceof DistributedRegion
          && ((DistributedRegion)region).isPdxTypesRegion());
      if (!isPDXRegion) {
        waitForFailoverCompletion();
      }
      // If it is, create and enqueue an initialized GatewayEventImpl
      senderEvent = new GatewaySenderEventImpl(operation, event, this.sender.isNonWanDispatcher()); // OFFHEAP ok
      queuePrimaryEvent(senderEvent);
    }
  }

  private void queuePrimaryEvent(GatewaySenderEventImpl gatewayEvent)
      throws IOException, CacheException {
    // Queue the event
    GatewaySenderStats statistics = this.sender.getStatistics();
    if (getLogger().fineEnabled()) {
      getLogger().fine(
          sender.getId() + ": Queueing event ("
              + (statistics.getEventsQueued() + 1) + "): " + gatewayEvent);
    }
    long start = statistics.startTime();
    try {
      this.queue.put(gatewayEvent);
    } catch (InterruptedException e) {
      // Asif Not expected from SingleWriteSingleReadRegionQueue as it does not
      // throw
      // InterruptedException. But since both HARegionQueue and
      // SingleReadSingleWriteRegionQueue
      // extend RegionQueue , it has to handle InterruptedException
      Thread.currentThread().interrupt();
      getSender().getCancelCriterion().checkCancelInProgress(e);
    }
    statistics.endPut(start);
    if (getLogger().fineEnabled()) {
      getLogger().fine(
          sender.getId() + ": Queued event (" + (statistics.getEventsQueued())
              + "): " + gatewayEvent);
    }
    // this._logger.warning(getGateway() + ": Queued event (" +
    // (statistics.getEventsQueued()) + "): " + gatewayEvent + " queue size: "
    // + this._eventQueue.size());
    /*
     * FAILOVER TESTING CODE System.out.println(getName() + ": Queued event (" +
     * (statistics.getEventsQueued()) + "): " + gatewayEvent.getId());
     */
    int queueSize = eventQueueSize();
    statistics.incQueueSize(1);
    if (!this.eventQueueSizeWarning
        && queueSize >= AbstractGatewaySender.QUEUE_SIZE_THRESHOLD) {
      getLogger()
          .warning(
              LocalizedStrings.GatewayImpl_0_THE_EVENT_QUEUE_SIZE_HAS_REACHED_THE_THRESHOLD_1,
              new Object[] { sender.getId(),
                  Integer.valueOf(AbstractGatewaySender.QUEUE_SIZE_THRESHOLD) });
      this.eventQueueSizeWarning = true;
    }
  }

  protected void waitForFailoverCompletion() {
    synchronized (this.failoverCompletedLock) {
      if (this.failoverCompleted) {
        return;
      }
      this.getLogger()
          .info(
              LocalizedStrings.GatewayImpl_0__WAITING_FOR_FAILOVER_COMPLETION,
              this);
      try {
        while (!this.failoverCompleted) {
          this.failoverCompletedLock.wait();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        this.sender.getCache().getCancelCriterion().checkCancelInProgress(ex);
        this.getLogger()
            .info(
                LocalizedStrings.GatewayImpl_0_DID_NOT_WAIT_FOR_FAILOVER_COMPLETION_DUE_TO_INTERRUPTION,
                this);
      }
    }
  }

  protected void completeFailover() {
    synchronized (this.failoverCompletedLock) {
      this.failoverCompleted = true;
      this.failoverCompletedLock.notifyAll();
    }
  }

  /**
   * Update an unprocessed event in the unprocessed events map. This method is
   * called by a secondary <code>GatewaySender</code> to store a gateway event until
   * it is processed by a primary <code>GatewaySender</code>. The complexity of this
   * method is the fact that the event could be processed first by either the
   * primary or secondary <code>GatewaySender</code>.
   * 
   * If the primary processes the event first, the map will already contain an
   * entry for the event (through
   * {@link com.gemstone.gemfire.internal.cache.GatewayImpl.SecondaryGatewayListener#afterDestroy}
   * ). When the secondary processes the event, it will remove it from the map.
   * 
   * If the secondary processes the event first, it will add it to the map. When
   * the primary processes the event (through
   * {@link com.gemstone.gemfire.internal.cache.GatewayImpl.SecondaryGatewayListener#afterDestroy}
   * ), it will then be removed from the map.
   * 
   * @param senderEvent
   *          The event being processed
   */
  protected void handleSecondaryEvent(GatewaySenderEventImpl senderEvent) {
    basicHandleSecondaryEvent(senderEvent);
  }

  /**
   * Update an unprocessed event in the unprocessed events map. This method is
   * called by a primary <code>Gateway</code> (through
   * {@link com.gemstone.gemfire.internal.cache.GatewayImpl.SecondaryGatewayListener#afterCreate}
   * ) to notify the secondary <code>Gateway</code> that an event has been added
   * to the queue. Once an event has been added to the queue, the secondary no
   * longer needs to keep track of it in the unprocessed events map. The
   * complexity of this method is the fact that the event could be processed
   * first by either the primary or secondary <code>Gateway</code>.
   * 
   * If the primary processes the event first, the map will not contain an entry
   * for the event. It will be added to the map in this case so that when the
   * secondary processes it, it will know that the primary has already processed
   * it, and it can be safely removed.
   * 
   * If the secondary processes the event first, the map will already contain an
   * entry for the event. In this case, the event can be removed from the map.
   * 
   * @param gatewayEvent
   *          The event being processed
   */
  protected void handlePrimaryEvent(final GatewaySenderEventImpl gatewayEvent) {
    Executor my_executor = this.executor;
    synchronized (listenerObjectLock) {
      if (my_executor == null) {
        // should mean we are now primary
        return;
      }
      try {
        my_executor.execute(new Runnable() {
          public void run() {
            basicHandlePrimaryEvent(gatewayEvent);
          }
        });
      }
      catch (RejectedExecutionException ex) {
        throw ex;
      }
    }
  }

  /**
   * Called when the primary gets rid of an event from the queue This method
   * added to fix bug 37603
   */
  protected void handlePrimaryDestroy(final GatewaySenderEventImpl gatewayEvent) {
    Executor my_executor = this.executor;
    synchronized (listenerObjectLock) {
      if (my_executor == null) {
        // should mean we are now primary
        return;
      }
      try {
        my_executor.execute(new Runnable() {
          public void run() {
            basicHandlePrimaryDestroy(gatewayEvent);
          }
        });
      }
      catch (RejectedExecutionException ex) {
        throw ex;
      }
    }
  }

  /**
   * Just remove the event from the unprocessed events map if it is present.
   * This method added to fix bug 37603
   */
  protected void basicHandlePrimaryDestroy(
      final GatewaySenderEventImpl gatewayEvent) {
    if (this.sender.isPrimary()) {
      // no need to do anything if we have become the primary
      return;
    }
    GatewaySenderStats statistics = this.sender.getStatistics();
    // Get the event from the map
    synchronized (unprocessedEventsLock) {
      if (this.unprocessedEvents == null)
        return;
      // now we can safely use the unprocessedEvents field
      EventWrapper ew = this.unprocessedEvents.remove(gatewayEvent.getEventId());
      if (ew != null) {
        ew.event.release();
        statistics.incUnprocessedEventsRemovedByPrimary();
      }
    }
  }

  protected void basicHandlePrimaryEvent(
      final GatewaySenderEventImpl gatewayEvent) {
    if (this.sender.isPrimary()) {
      // no need to do anything if we have become the primary
      return;
    }
    GatewaySenderStats statistics = this.sender.getStatistics();
    // Get the event from the map
    synchronized (unprocessedEventsLock) {
      if (this.unprocessedEvents == null)
        return;
      // now we can safely use the unprocessedEvents field
      EventWrapper ew = this.unprocessedEvents.remove(gatewayEvent.getEventId());

      if (ew == null) {
        // first time for the event
        if (getLogger().finerEnabled()) {
            getLogger().finer(
                sender.getId() + ": fromPrimary " + " event "
                    + gatewayEvent.getEventId() + ":" + gatewayEvent.getKey()
                    + "->" + gatewayEvent.getValueAsString(true)
                    + " added to unprocessed token map.");
        }
        {
          Long mapValue = Long.valueOf(System.currentTimeMillis()
              + AbstractGatewaySender.TOKEN_TIMEOUT);
          Long oldv = this.unprocessedTokens.put(gatewayEvent.getEventId(), mapValue);
          if (oldv == null) {
            statistics.incUnprocessedTokensAddedByPrimary();
          } else {
            // its ok for oldv to be non-null
            // this shouldn't happen anymore @todo add an assertion here
          }
        }
      } else {
        // already added by secondary (i.e. hub)
        // the secondary
        // gateway has already seen this event, and it can be safely
        // removed (it already was above)
        if (getLogger().finerEnabled()) {
          getLogger().finer(
              sender.getId() + ": Primary create/update event "
                  + gatewayEvent.getEventId() + ":" + gatewayEvent.getKey()
                  + "->" + gatewayEvent.getValueAsString(true)
                  + " removed from unprocessed events map");
        }
        ew.event.release();
        statistics.incUnprocessedEventsRemovedByPrimary();
      }
      reapOld(statistics, false);
    }
  }

  private void basicHandleSecondaryEvent(
      final GatewaySenderEventImpl gatewayEvent) {
    boolean freeGatewayEvent = true;
    try {
    GatewaySenderStats statistics = this.sender.getStatistics();
    // Get the event from the map

    Assert.assertHoldsLock(unprocessedEventsLock, true);
    Assert.assertTrue(unprocessedEvents != null);
    // @todo add an assertion that !getPrimary()
    // now we can safely use the unprocessedEvents field
    Long v = this.unprocessedTokens.remove(gatewayEvent.getEventId());

    if (v == null) {
      // first time for the event
      if (getLogger().finerEnabled()) {
        getLogger().finer(
            sender.getId() + ": fromSecondary " + " event "
                + gatewayEvent.getEventId() + ":" + gatewayEvent.getKey()
                + "->" + gatewayEvent.getValueAsString(true)
                + " added to unprocessed events map.");
      }
      {
        EventWrapper mapValue = new EventWrapper(gatewayEvent);
        EventWrapper oldv = this.unprocessedEvents.put(gatewayEvent.getEventId(), mapValue);
        if (oldv == null) {
          freeGatewayEvent = false;
          statistics.incUnprocessedEventsAddedBySecondary();
        } else {
          // put old one back in
          this.unprocessedEvents.put(gatewayEvent.getEventId(), oldv);
          // already added by secondary (i.e. hub)
          if (getLogger().warningEnabled()) {
            getLogger()
                .warning(
                    LocalizedStrings.GatewayImpl_0_THE_UNPROCESSED_EVENTS_MAP_ALREADY_CONTAINED_AN_EVENT_FROM_THE_HUB_1_SO_IGNORING_NEW_EVENT_2,
                    new Object[] { sender.getId(), v, gatewayEvent });
          }
        }
      }
    } else {
      // token already added by primary already removed
      if (getLogger().finerEnabled()) {
        getLogger().finer(
            sender.getId() + ": Secondary created event "
                + gatewayEvent.getEventId() + ":" + gatewayEvent.getKey()
                + "->" + gatewayEvent.getValueAsString(true)
                + " removed from unprocessed token map");
      }
      statistics.incUnprocessedTokensRemovedBySecondary();
    }
    reapOld(statistics, false);
    } finally {
      if (freeGatewayEvent) {
        gatewayEvent.release();
      }
    }
  }

  /**
   * Call to check if a cleanup of tokens needs to be done
   */
  private void reapOld(final GatewaySenderStats statistics, boolean forceEventReap) {
    synchronized (this.unprocessedEventsLock) {
      if (uncheckedCount > REAP_THRESHOLD) { // only check every X events
        uncheckedCount = 0;
        long now = System.currentTimeMillis();
        if (!forceEventReap && this.unprocessedTokens.size() > REAP_THRESHOLD) {
          Iterator<Map.Entry<EventID, Long>> it = this.unprocessedTokens.entrySet().iterator();
          int count = 0;
          while (it.hasNext()) {
            Map.Entry<EventID, Long> me = it.next();
            long meValue = me.getValue().longValue();
            if (meValue <= now) {
              // @todo log fine level message here
              // it has expired so remove it
              it.remove();
              count++;
            } else {
              // all done try again
              break;
            }
          }
          if (count > 0) {
            //statistics.incUnprocessedTokensRemovedByTimeout(count);
          }
        }
        if (forceEventReap || this.unprocessedEvents.size() > REAP_THRESHOLD) {
          Iterator<Map.Entry<EventID, EventWrapper>> it = this.unprocessedEvents.entrySet().iterator();
          int count = 0;
          while (it.hasNext()) {
            Map.Entry<EventID, EventWrapper> me = it.next();
            EventWrapper ew = me.getValue();
            if (ew.timeout <= now) {
              // @todo log fine level message here
              // it has expired so remove it
              it.remove();
              ew.event.release();
              count++;
            } else {
              // all done try again
              break;
            }
          }
          if (count > 0) {
            //statistics.incUnprocessedEventsRemovedByTimeout(count);
          }
        }
      } else {
        uncheckedCount++;
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("GatewayEventProcessor[").append("gatewaySenderId=")
    .append(sender.getId())
    .append(";remoteDSId=")
    .append(getSender().getRemoteDSId())
    .append(";batchSize=")
    .append(getSender().getBatchSize());
    buffer.append("]");
    return buffer.toString();
  }

  /**
   * Initialize the Executor that handles listener events. Only used by
   * non-primary gateway senders
   */
  private void initializeListenerExecutor() {
    // Create the ThreadGroups
    final ThreadGroup loggerGroup = LogWriterImpl.createThreadGroup(
        "Gateway Listener Group", getLogger());
    final ThreadGroup gwlGroup = new GemFireThreadGroup("Gateway Listener Group") {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        if (e instanceof Error && SystemFailure.isJVMFailureError((Error)e)) {
          SystemFailure.setFailure((Error)e); // don't throw
        }
        Thread.dumpStack();
        loggerGroup.uncaughtException(t, e);
      }
    };

    // Create the Executor
    ThreadFactory tf = new ThreadFactory() {
      public Thread newThread(Runnable command) {
        Thread thread = new Thread(gwlGroup, command,
            "Queued Gateway Listener Thread");
        thread.setDaemon(true);
        return thread;
      }
    };
    LinkedBlockingQueue<Runnable> q = new LinkedBlockingQueue<Runnable>();
    this.executor = new ThreadPoolExecutor(1, 1/* max unused */, 120,
        TimeUnit.SECONDS, q, tf);
  }

  private void shutdownListenerExecutor() {
    synchronized (listenerObjectLock) {
      if (this.executor != null) {
        this.executor.shutdown();
        this.executor = null;
      }
    }
  }
  
  @Override
  public void removeCacheListener() {
    this.queue.removeCacheListener();
  }
  
  public long estimateMemoryFootprint(SingleObjectSizer sizer) {
    return super.estimateMemoryFootprint(sizer)
        + sizer.sizeof(unprocessedEvents) + sizer.sizeof(unprocessedTokens)
        + sizer.sizeof(executor);
  }

}

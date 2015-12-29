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

package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallback;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorDiscovery;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorRequest;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorResponse;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.GatewayCancelledException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl.StaticSystemCallbacks;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.size.SingleObjectSizer;

/**
 * Abstract implementation of both Serial and Parallel GatewaySener. It handles
 * common functionality like initializing proxy.
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 7.0
 */

public abstract class AbstractGatewaySender implements GatewaySender,
    DistributionAdvisee {

  protected final Cache cache;

  protected final LogWriterI18n logger;

  protected final String id;
  
  protected final long uuid;

  protected long startTime;

  protected PoolImpl proxy;

  protected final int remoteDSId;

  protected String locName;

  protected final int socketBufferSize;

  protected final int socketReadTimeout;

  protected final int queueMemory;
  
  protected final int maxMemoryPerDispatcherQueue;

  protected final int batchSize;

  protected final int batchTimeInterval;

  protected final boolean isConflation;

  protected final boolean isPersistence;

  protected final int alertThreshold;

  protected final boolean manualStart;
  
  protected final boolean isParallel;
  
  protected final boolean isForInternalUse;

  protected final boolean isDiskSynchronous;
  
  protected final String diskStoreName;

  protected List<GatewayEventFilter> eventFilters;

  /**
   * indicates that all {@link #eventFilters} are
   * {@link GatewayEventEnqueueFilter}s
   */
  protected boolean onlyGatewayEnqueueEventFilters;

  protected List<GatewayTransportFilter> transFilters;

  @SuppressWarnings("rawtypes")
  protected List<AsyncEventListener> listeners;

  protected LocatorDiscoveryCallback locatorDiscoveryCallback;
  
  public final ReentrantReadWriteLock lifeCycleLock = new ReentrantReadWriteLock();
  
  protected GatewaySenderAdvisor senderAdvisor;
  
  private final int serialNumber;
  
  private GatewaySenderStats statistics;
  
  private Stopper stopper;
  
  private final OrderPolicy policy;
  
  private final int dispatcherThreads;
  
  protected boolean isBucketSorted;
  
  protected boolean isHDFSQueue;
  
  private int parallelismForReplicatedRegion;
  
  protected AbstractGatewaySenderEventProcessor eventProcessor;

  private ServerLocation serverLocation;
  
  protected final Object queuedEventsSync = new Object();
  
  protected volatile boolean enqueuedAllTempQueueEvents = false; 
  
  protected volatile ConcurrentLinkedQueue<EntryEventImpl> tmpQueuedEvents = new ConcurrentLinkedQueue<EntryEventImpl>();
  /**
   * The number of seconds to wait before stopping the GatewaySender.
   * Default is 0 seconds.
   */
  public static int MAXIMUM_SHUTDOWN_WAIT_TIME = Integer.getInteger(
		  "GatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME", 0).intValue();

  /**
   * The number of times to peek on shutdown before giving up and shutting down.
   */
  protected static final int MAXIMUM_SHUTDOWN_PEEKS = Integer.getInteger(
      "GatewaySender.MAXIMUM_SHUTDOWN_PEEKS", 20).intValue();
  
  public static final int QUEUE_SIZE_THRESHOLD = Integer.getInteger(
      "GatewaySender.QUEUE_SIZE_THRESHOLD", 5000).intValue();

  public static int TOKEN_TIMEOUT = Integer.getInteger(
      "GatewaySender.TOKEN_TIMEOUT", 15000).intValue();

  protected int myDSId = DEFAULT_DISTRIBUTED_SYSTEM_ID; 
  
  private final int connectionReadTimeOut = GATEWAY_CONNECTION_READ_TIMEOUT;
  private final int connectionIdleTimeOut = GATEWAY_CONNECTION_IDLE_TIMEOUT;
  
  private boolean removeFromQueueOnException = GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION;
  
  /** used to reduce warning logs in case remote locator is down (#47634) */ 
  private int proxyFailureTries = 0; 

  protected AbstractGatewaySender(Cache cache, GatewaySenderAttributes attrs) {
    this.cache = cache;
    this.logger = (LogWriterI18n)cache.getLogger();
    this.id = attrs.getId();
    final StaticSystemCallbacks sysCb = GemFireCacheImpl
        .getInternalProductCallbacks();
    if (sysCb != null) {
      this.uuid = sysCb.getGatewayUUID(this, attrs);
    }
    else {
      // TODO: GemFire does not give UUIDs yet (need those for #48335)
      this.uuid = 0;
    }
    this.socketBufferSize = attrs.getSocketBufferSize();
    this.socketReadTimeout = attrs.getSocketReadTimeout();
    this.queueMemory = attrs.getMaximumQueueMemory();
    this.batchSize = attrs.getBatchSize();
    this.batchTimeInterval = attrs.getBatchTimeInterval();
    this.isConflation = attrs.isBatchConflationEnabled();
    this.isPersistence = attrs.isPersistenceEnabled();
    this.alertThreshold = attrs.getAlertThreshold();
    this.manualStart = attrs.isManualStart();
    this.isParallel = attrs.isParallel();
    this.isForInternalUse = attrs.isForInternalUse();
    this.diskStoreName = attrs.getDiskStoreName();
    this.remoteDSId = attrs.getRemoteDSId();
    this.eventFilters = attrs.getGatewayEventFilters();
    setOnlyGatewayEnqueueEventFiltersFlag();
    this.transFilters = attrs.getGatewayTransportFilters();
    this.listeners = attrs.getAsyncEventListeners();
    this.locatorDiscoveryCallback = attrs.getGatewayLocatoDiscoveryCallback();
    this.isDiskSynchronous = attrs.isDiskSynchronous();
    this.policy = attrs.getOrderPolicy();
    this.dispatcherThreads = attrs.getDispatcherThreads();
    this.parallelismForReplicatedRegion = attrs.getParallelismForReplicatedRegion();
    //divide the maximumQueueMemory of sender equally using number of dispatcher threads.
    //if dispatcherThreads is 1 then maxMemoryPerDispatcherQueue will be same as maximumQueueMemory of sender
    this.maxMemoryPerDispatcherQueue = this.queueMemory / this.dispatcherThreads;
    this.myDSId = InternalDistributedSystem.getAnyInstance().getDistributionManager().getDistributedSystemId();
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    if (!(this.cache instanceof CacheCreation)) {
      this.stopper = new Stopper(cache.getCancelCriterion());
      this.senderAdvisor = GatewaySenderAdvisor.createGatewaySenderAdvisor(this, logger);
      if (!this.isForInternalUse()) {
        this.statistics = new GatewaySenderStats(cache.getDistributedSystem(),
            id);
      }
      else {// this sender lies underneath the AsyncEventQueue. Need to have
            // AsyncEventQueueStats
        this.statistics = new AsyncEventQueueStats(
            cache.getDistributedSystem(), AsyncEventQueueImpl
                .getAsyncEventQueueIdFromSenderId(id));
      }
    }
    this.isBucketSorted = attrs.isBucketSorted();
    this.isHDFSQueue = attrs.isHDFSQueue();
  }

  private void setOnlyGatewayEnqueueEventFiltersFlag() {
    this.onlyGatewayEnqueueEventFilters = true;
    for (GatewayEventFilter filter : this.eventFilters) {
      if (!(filter instanceof GatewayEventEnqueueFilter)) {
        this.onlyGatewayEnqueueEventFilters = false;
        break;
      }
    }
  }

  public final GatewaySenderAdvisor getSenderAdvisor() {
    return senderAdvisor;
  }

  public final GatewaySenderStats getStatistics() {
    return statistics;
  }
  
  public synchronized void initProxy() {
    // return if it is being used for WBCL or proxy is already created
    if (this.remoteDSId == DEFAULT_DISTRIBUTED_SYSTEM_ID || this.proxy != null
        && !this.proxy.isDestroyed()) {
      return;
    }

    Properties props = new Properties();
    PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
    pf.setPRSingleHopEnabled(false);
    if (this.locatorDiscoveryCallback != null) {
      pf.setLocatorDiscoveryCallback(locatorDiscoveryCallback);
    }
    pf.setReadTimeout(connectionReadTimeOut);
    pf.setIdleTimeout(connectionIdleTimeOut);
    pf.setSocketBufferSize(socketBufferSize);
    pf.setServerGroup(GatewayReceiverImpl.RECEIVER_GROUP);
    RemoteLocatorRequest request = new RemoteLocatorRequest(this.remoteDSId, pf
        .getPoolAttributes().getServerGroup());
    String locators = ((GemFireCacheImpl) this.cache).getDistributedSystem()
        .getConfig().getLocators();
    if (logger.fineEnabled()) {
      logger
          .fine("Gateway Sender is attempting to configure pool with remote locator information");
    }
    StringTokenizer locatorsOnThisVM = new StringTokenizer(locators, ",");
    while (locatorsOnThisVM.hasMoreTokens()) {
      String localLocator = locatorsOnThisVM.nextToken();
      DistributionLocatorId locatorID = new DistributionLocatorId(localLocator);
      try {
        RemoteLocatorResponse response = (RemoteLocatorResponse) TcpClient
            .requestToServer(locatorID.getHost(), locatorID.getPort(), request,
                LocatorDiscovery.WAN_LOCATOR_CONNECTION_TIMEOUT);

        if (response != null) {
          if (response.getLocators() == null) {
            if (logProxyFailure()) {
              logger
                  .warning(
                      LocalizedStrings.AbstractGatewaySender_REMOTE_LOCATOR_FOR_REMOTE_SITE_0_IS_NOT_AVAILABLE_IN_LOCAL_LOCATOR_1,
                      new Object[] { remoteDSId, localLocator });
            }
            continue;
          }
          if (logger.fineEnabled()) {
            logger.fine("Received the remote site " + this.remoteDSId
                + " locator informarion " + response.getLocators());
          }
          StringBuilder strBuffer = new StringBuilder();
          Iterator<String> itr = response.getLocators().iterator();
          while (itr.hasNext()) {
            strBuffer.append(itr.next()).append(",");
          }
          strBuffer = strBuffer.deleteCharAt(strBuffer.length() - 1);
          props.setProperty(DistributionConfig.LOCATORS_NAME, strBuffer
              .toString());
          break;
        }
      } catch (IOException ioe) {
        if (logProxyFailure()) {
          // don't print stack trace for connection failures
          String ioeStr = "";
          if (!logger.fineEnabled() && ioe instanceof ConnectException) {
            ioeStr = ": " + ioe.toString();
            ioe = null;
          }
        logger
            .warning(
                LocalizedStrings.AbstractGatewaySender_SENDER_0_IS_NOT_ABLE_TO_CONNECT_TO_LOCAL_LOCATOR_1,
                new Object[] { this.id, localLocator + ioeStr  }, ioe);        
        }
        continue;
      } catch (ClassNotFoundException e) {
        if (logProxyFailure()) {
          logger
              .warning(
                  LocalizedStrings.AbstractGatewaySender_SENDER_0_IS_NOT_ABLE_TO_CONNECT_TO_LOCAL_LOCATOR_1,
                  new Object[] { this.id, localLocator }, e);
        }
        continue;
      }
    }

    if (props.isEmpty()) {
      if (logProxyFailure()) {
        logger
            .severe(
                LocalizedStrings.AbstractGatewaySender_SENDER_0_COULD_NOT_GET_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1,
                new Object[] { this.id, this.remoteDSId });
      }
      this.proxyFailureTries++;
      throw new GatewaySenderConfigurationException(
          LocalizedStrings.AbstractGatewaySender_SENDER_0_COULD_NOT_GET_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1
              .toLocalizedString(new Object[] { this.id, this.remoteDSId}));
    }
    pf.init(props, false, true, this);
    this.proxy = ((PoolImpl) pf.create(this.getId()));
    if (this.proxyFailureTries > 0) {
      logger
          .info(
              LocalizedStrings.AbstractGatewaySender_SENDER_0_GOT_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1,
              new Object[] { this.id, this.remoteDSId, this.proxyFailureTries });
      this.proxyFailureTries = 0;
    }
  }

  public boolean isPrimary() {
    return this.getSenderAdvisor().isPrimary();
  }
  
  public void setIsPrimary(boolean isPrimary){
    this.getSenderAdvisor().setIsPrimary(isPrimary);
  }
  
  public Cache getCache() {
    return this.cache;
  }

  public LogWriterI18n getLogger() {
    return this.logger;
  }

  public int getAlertThreshold() {
    return this.alertThreshold;
  }
  
  public int getBatchSize() {
    return this.batchSize;
  }
  
  public int getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public final List<GatewayEventFilter> getGatewayEventFilters() {
    // caller should not be able to modify the list
    return Collections.unmodifiableList(this.eventFilters);
  }

  public final String getId() {
    return this.id;
  }

  public final long getUUID() {
    return this.uuid;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public int getRemoteDSId() {
    return this.remoteDSId;
  }

  public final List<GatewayTransportFilter> getGatewayTransportFilters() {
    return this.transFilters;
  }

  @SuppressWarnings("rawtypes")
  public final List<AsyncEventListener> getAsyncEventListeners() {
    return this.listeners;
  }

  public final boolean hasListeners() {
    return !this.listeners.isEmpty();
  }

  public boolean isManualStart() {
    return this.manualStart;
  }

  public int getMaximumQueueMemory() {
    return this.queueMemory;
  }
  
  public int getMaximumMemeoryPerDispatcherQueue() {
    return this.maxMemoryPerDispatcherQueue;
  }

  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public int getSocketReadTimeout() {
    return this.socketReadTimeout;
  }

  public boolean isBatchConflationEnabled() {
    return this.isConflation;
  }
  
  public boolean isPersistenceEnabled() {
    return this.isPersistence;
  }

  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }

  public int getMaxParallelismForReplicatedRegion() {
    return this.parallelismForReplicatedRegion;
  }  
    
  public LocatorDiscoveryCallback getLocatorDiscoveryCallback() {
    return this.locatorDiscoveryCallback;
  }

  public DistributionAdvisor getDistributionAdvisor() {
    return this.senderAdvisor;
  }

  public DM getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  public String getFullPath() {
    return getId();
  }

  public String getName() {
    return getId();
  }

  public DistributionAdvisee getParentAdvisee() {
    return null;
  }
  
  public int getDispatcherThreads() {
    return this.dispatcherThreads;
  }
  
  public OrderPolicy getOrderPolicy() {
    return this.policy;
  }

  public Profile getProfile() {
    return this.senderAdvisor.createProfile();
  }

  public int getSerialNumber() {
    return this.serialNumber;
  }
  
  public boolean getBucketSorted() {
    return this.isBucketSorted;
  }

  public boolean getIsHDFSQueue() {
    return this.isHDFSQueue;
  }
  
  public InternalDistributedSystem getSystem() {
    return (InternalDistributedSystem)this.cache.getDistributedSystem();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GatewaySender)) {
      return false;
    }
    AbstractGatewaySender sender = (AbstractGatewaySender)obj;
    if (sender.getId().equals(this.getId())) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.getId().hashCode();
  }

  public PoolImpl getProxy() {
    return proxy;
  }

  public void removeGatewayEventFilter(GatewayEventFilter filter) {
    this.eventFilters.remove(filter);
    if (!this.onlyGatewayEnqueueEventFilters) {
      setOnlyGatewayEnqueueEventFiltersFlag();
    }
  }

  public void addGatewayEventFilter(GatewayEventFilter filter) {
    if (this.eventFilters.isEmpty()) {
      this.eventFilters = new ArrayList<GatewayEventFilter>();
    }
    if (filter == null) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderImpl_NULL_CANNNOT_BE_ADDED_TO_GATEWAY_EVENT_FILTER_LIST
              .toLocalizedString());
    }
    this.eventFilters.add(filter);
    if (this.onlyGatewayEnqueueEventFilters
        && !(filter instanceof GatewayEventEnqueueFilter)) {
      this.onlyGatewayEnqueueEventFilters = false;
    }
  }

  public boolean isParallel() {
    return this.isParallel;
  }
  
  public boolean isForInternalUse() {
    return this.isForInternalUse;
  }
  
  abstract public void start();
  abstract public void stop();

  /**
   * Destroys the GatewaySender. Before destroying the sender, caller needs to to ensure 
   * that the sender is stopped so that all the resources (threads, connection pool etc.) 
   * will be released properly. Stopping the sender is not handled in the destroy.
   * Destroy is carried out in following steps:
   * 1. Take the lifeCycleLock. 
   * 2. If the sender is attached to any application region, throw an exception.
   * 3. Close the GatewaySenderAdvisor.
   * 4. Remove the sender from the cache.
   * 5. Destroy the region underlying the GatewaySender.
   * 
   * In case of ParallelGatewaySender, the destroy operation does distributed destroy of the 
   * QPR. In case of SerialGatewaySender, the queue region is destroyed locally.
   */
  @Override
  public void destroy() {
    if (this.logger.fineEnabled()) {
      this.logger.fine("Destroying Gateway Sender : " + this);
    }
    try {
      this.lifeCycleLock.writeLock().lock();
      // first, check if this sender is attached to any region. If so, throw
      // GatewaySenderException
      Set<LocalRegion> regions = ((GemFireCacheImpl)this.cache)
          .getApplicationRegions();
      Iterator regionItr = regions.iterator();
      while (regionItr.hasNext()) {
        LocalRegion region = (LocalRegion)regionItr.next();

        if (region.getAttributes().getGatewaySenderIds().contains(this.id)) {
          throw new GatewaySenderException(
              LocalizedStrings.GatewaySender_COULD_NOT_DESTROY_SENDER_AS_IT_IS_STILL_IN_USE
                  .toLocalizedString(this));
        }
      }

      // stop the sender first
      final AbstractGatewaySenderEventProcessor ev = this.eventProcessor;
      if (ev != null && !ev.isStopped()) {
        stop();
      }

      // close the GatewaySenderAdvisor
      GatewaySenderAdvisor advisor = this.getSenderAdvisor();
      if (advisor != null) {
        if (getLogger().fineEnabled()) {
          getLogger().fine("Stopping the GatewaySender advisor");
        }
        advisor.close();
      }

      // remove the sender from the cache
      ((GemFireCacheImpl)this.cache).removeGatewaySender(this);

      // destroy the region underneath the sender's queue
      Set<RegionQueue> regionQueues = getQueues();
      if (regionQueues != null) {
        for (RegionQueue regionQueue : regionQueues) {
          try {
            if (regionQueue instanceof ConcurrentParallelGatewaySenderQueue) {
              Set<PartitionedRegion> queueRegions = ((ConcurrentParallelGatewaySenderQueue)regionQueue)
                  .getRegions();
              for (PartitionedRegion queueRegion : queueRegions) {
                queueRegion.destroyRegion();
              }
            }
            else {// For SerialGatewaySenderQueue, do local destroy
              regionQueue.getRegion().localDestroyRegion();
            }
          }
          // Can occur in case of ParallelGatewaySenderQueue, when the region is
          // being destroyed
          // by several nodes simultaneously
          catch (RegionDestroyedException e) {
            // the region might have already been destroyed by other node. Just
            // log
            // the exception.
            this
                .getLogger()
                .info(
                    LocalizedStrings.AbstractGatewaySender_REGION_0_UNDERLYING_GATEWAYSENDER_1_IS_ALREADY_DESTROYED,
                    new Object[] { e.getRegionFullPath(), this });
          }
        }
      }//END if (regionQueues != null)
    }
    finally {
      this.lifeCycleLock.writeLock().unlock();
    }
  }

  protected boolean beforeEnqueue(EntryEventImpl event) {
    final List<GatewayEventFilter> filters = this.eventFilters;
    if (filters.size() > 0) {
      for (GatewayEventFilter filter : filters) {
        if (!filter.beforeEnqueue(event)) {
          return false;
        }
      }
    }
    return true;
  }

  protected void stompProxyDead() {
    Runnable stomper = new Runnable() {
      public void run() {
        PoolImpl bpi = proxy;
        if (bpi != null) {
          try {
            bpi.destroy();
          } catch (Exception e) {/* ignore */
          }
        }
      }
    };
    ThreadGroup tg = LogWriterImpl.createThreadGroup("Proxy Stomper Group",
        getLogger());
    Thread t = new Thread(tg, stomper, "GatewaySender Proxy Stomper");
    t.setDaemon(true);
    t.start();
    try {
      t.join(GATEWAY_SENDER_TIMEOUT * 1000);
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    this.getLogger()
        .warning(
            LocalizedStrings.GatewayImpl_GATEWAY_0_IS_NOT_CLOSING_CLEANLY_FORCING_CANCELLATION,
            this);
    // OK, either we've timed out or been interrupted. Time for
    // violence.
    t.interrupt(); // give up
    proxy.emergencyClose(); // VIOLENCE!
    this.proxy = null;
  }
  
  public int getMyDSId() {
    return this.myDSId;
  }
  
  /**
   * @param removeFromQueueOnException the removeFromQueueOnException to set
   */
  public void setRemoveFromQueueOnException(boolean removeFromQueueOnException) {
    this.removeFromQueueOnException = removeFromQueueOnException;
  }

  /**
   * @return the removeFromQueueOnException
   */
  public boolean isRemoveFromQueueOnException() {
    return removeFromQueueOnException;
  }

  public CancelCriterion getStopper() {
    return this.stopper;
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  public synchronized ServerLocation getServerLocation() {
    return serverLocation;
  }

  public synchronized boolean setServerLocation(ServerLocation location) {
    this.serverLocation = location;
    return true;
  }
  
  private class Stopper extends CancelCriterion {
    final CancelCriterion stper;

    Stopper(CancelCriterion stopper) {
      this.stper = stopper;
    }

    @Override
    public String cancelInProgress() {
      // checkFailure(); // done by stopper
      return stper.cancelInProgress();
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      RuntimeException result = stper.generateCancelledException(e);
      return result;
    }
  }

  final public RegionQueue getQueue() {
    if (this.eventProcessor != null) {
      if (!(this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        return this.eventProcessor.getQueue();
      }
      else {
        throw new IllegalArgumentException(
            "getQueue() for concurrent serial gateway sender");
      }
    }
    return null;
  }

  final public Set<RegionQueue> getQueues() {
    if (this.eventProcessor != null) {
      if (!(this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        return Collections.singleton(this.eventProcessor.getQueue());
      }
      return ((ConcurrentSerialGatewaySenderEventProcessor)this.eventProcessor)
          .getQueues();
    }
    return null;
  }

  final public Set<RegionQueue> getQueuesForConcurrentSerialGatewaySender() {
    if (this.eventProcessor != null
        && (this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
      return ((ConcurrentSerialGatewaySenderEventProcessor)this.eventProcessor)
          .getQueues();
    }
    return null;
  }
  
  final protected void waitForRunningStatus() {
    synchronized (this.eventProcessor.runningStateLock) {
      while (this.eventProcessor.getException() == null
          && this.eventProcessor.isStopped()) {
        try {
          this.eventProcessor.runningStateLock.wait();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      Exception ex = this.eventProcessor.getException();
      if (ex != null) {
        throw new GatewaySenderException(
            LocalizedStrings.Sender_COULD_NOT_START_GATEWAYSENDER_0_BECAUSE_OF_EXCEPTION_1
                .toLocalizedString(new Object[] { this.getId(), ex.getMessage() }),
            ex.getCause());
      }
    }
  }
  
  final public void pause() {
    if (this.eventProcessor != null) {
      this.lifeCycleLock.writeLock().lock();
      try {
        if (this.eventProcessor.isStopped()) {
          return;
        }
        this.eventProcessor.pauseDispatching();

        InternalDistributedSystem system = (InternalDistributedSystem) this.cache
            .getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_PAUSE, this);
        
        getLogger().info(LocalizedStrings.GatewaySender_PAUSED__0, this);

        enqueTempEvents();
      } finally {
        this.lifeCycleLock.writeLock().unlock();
      }
    }
  }

  final public void resume() {
    if (this.eventProcessor != null) {
      this.lifeCycleLock.writeLock().lock();
      try {
        if (this.eventProcessor.isStopped()) {
          return;
        }
        this.eventProcessor.resumeDispatching();

        
        InternalDistributedSystem system = (InternalDistributedSystem) this.cache
            .getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_RESUME, this);
        
        getLogger().info(LocalizedStrings.GatewaySender_RESUMED__0, this);
        
        enqueTempEvents();
      } finally {
        this.lifeCycleLock.writeLock().unlock();
      }
    }
  }
  
  final public boolean isPaused() {
    if (this.eventProcessor != null) {
      return this.eventProcessor.isPaused();
    }
    return false;
  }

  final public boolean isRunning() {
    AbstractGatewaySenderEventProcessor processor = this.eventProcessor;
    if (processor != null) {
      return !processor.isStopped();
    }
    return false;
  }

  final public AbstractGatewaySenderEventProcessor getEventProcessor() {
    return this.eventProcessor;
  }

  public void distribute(EnumListenerEvent operation, EntryEventImpl event,
      List<Integer> allRemoteDSIds) {
    final GatewaySenderStats stats = getStatistics();
    stats.incEventsReceived();
    // If the event is local (see bug 35831) or an expiration ignore it.
    //removed the check of isLocal as in notifyGAtewayHub this has been taken care
    if (/*event.getOperation().isLocal() || */event.getOperation().isExpiration()
        || event.getRegion().getDataPolicy().equals(DataPolicy.NORMAL)) {
      stats.incEventsNotQueued();
      return;
    }
    
    if (event.getOperation().isSearchOrLoad() && !getIsHDFSQueue()) {
      if (this.getLogger().fineEnabled())
        getLogger().fine("Event is not queued as it is loaded from a loader: " + event);
      stats.incEventsNotQueued();
      return; 
    }

    if (getIsHDFSQueue() && event.getOperation().isEviction()) {
      if (this.getLogger().fineEnabled())
        getLogger().fine("Eviction event not queued: " + event);
      stats.incEventsNotQueued();
      return;
    }

    if (!beforeEnqueue(event)) {
      // Yogesh: this should not be a warning message in GemFireXD or GemFire 
      // In GemFireXD it should be logged only TraceDBSynchronizer is ON 
      //getLogger().warning(LocalizedStrings
      //    .GatewayEventProcessor_EVENT_0_IS_NOT_ADDED_TO_QUEUE, event);
      if (getLogger().fineEnabled()) {
        getLogger().fine("Event is filtered : " + event);
      }
      stats.incEventsFiltered();
      return;
    }
    
    EntryEventImpl clonedEvent = new EntryEventImpl(event, true);
    boolean freeClonedEvent = true;
    try {

    setModifiedEventId(clonedEvent);
    Object callbackArg = clonedEvent.getRawCallbackArgument();
    
    if (this.getLogger().fineEnabled()) {
      // We can't deserialize here for logging purposes so don't
      // call getNewValue.
      // event.getNewValue(); // to deserialize the value if necessary
      this.getLogger().fine(
          this.isPrimary() + " : About to notify " + getId()
              + " to perform operation " + operation + " for " + clonedEvent
              + "callback arg " + callbackArg);

    }

    // TODO : This is a bit weird check but occurs if this event is already
    // distributed through GatewayHub the originalCallbackArgument is wrapped
    // with GatewayEventCallbackArgument.
    // Vice versa can happen : Post 7.0 this check will be removed
    if (callbackArg instanceof GatewayEventCallbackArgument) {
      callbackArg = ((GatewayEventCallbackArgument)callbackArg)
          .getOriginalCallbackArg();
    }
    else if (callbackArg instanceof CloneableCallbackArgument) {
      callbackArg = ((CloneableCallbackArgument)callbackArg).getClone();
    }
    if (callbackArg instanceof GatewaySenderEventCallbackArgument) {
      GatewaySenderEventCallbackArgument seca = (GatewaySenderEventCallbackArgument)callbackArg;
      if (this.getLogger().fineEnabled()) {
        this.getLogger().fine(
            this + ": Event originated in " + seca.getOriginatingDSId()
                + "My DS id " + this.getMyDSId() + " Remote DS id "
                + this.getRemoteDSId() + " the recepients are "
                + seca.getRecipientDSIds());
      }
      if (seca.getOriginatingDSId() == DEFAULT_DISTRIBUTED_SYSTEM_ID) {
        if (this.getLogger().fineEnabled()) {
          this.getLogger().fine(
              this + ": Event originated in " + seca.getOriginatingDSId()
                  + "My DS id " + this.getMyDSId() + " Remote DS id "
                  + this.getRemoteDSId() + " the recepients are "
                  + seca.getRecipientDSIds());
        }

        seca.setOriginatingDSId(this.getMyDSId());
        seca.initializeReceipientDSIds(allRemoteDSIds);

        } else {
          //if the dispatcher is GatewaySenderEventCallbackDispatcher (which is the case of WBCL), skip the below check of remoteDSId.
          //Fix for #46517
        AbstractGatewaySenderEventProcessor ep = getEventProcessor();
        if (ep != null && !(ep.getDispatcher() instanceof GatewaySenderEventCallbackDispatcher)) {
          if (seca.getOriginatingDSId() == this.getRemoteDSId()) {
            if (this.logger.fineEnabled()) {
              this.logger.fine(this + ": Event originated in "
                  + seca.getOriginatingDSId() + ". MY DS id is " + getMyDSId()
                  + ". It is being dropped as remote is originator");
            }
            return;
          } else if (seca.getRecipientDSIds().contains(this.getRemoteDSId())) {
            if (this.logger.fineEnabled()) {
              this.logger
                  .fine(this
                      + ": Event originated in "
                      + seca.getOriginatingDSId()
                      + ". MY DS id is "
                      + getMyDSId()
                      + " Remote DS id is "
                      + this.getRemoteDSId()
                      + ". It is being dropped as remote ds is already a recipient."
                      + "Recipients are " + seca.getRecipientDSIds());
            }
            return;
          }
        }
        seca.getRecipientDSIds().addAll(allRemoteDSIds);
      }
    } else {
      GatewaySenderEventCallbackArgumentImpl geCallbackArg =
          new GatewaySenderEventCallbackArgumentImpl(
              callbackArg, this.getMyDSId(), allRemoteDSIds, true);
      clonedEvent.setCallbackArgument(geCallbackArg);
    }

    if (!this.lifeCycleLock.readLock().tryLock()) {
      synchronized (this.queuedEventsSync) {
        if (!this.enqueuedAllTempQueueEvents) {
          if (!this.lifeCycleLock.readLock().tryLock()) {
            clonedEvent.setEventType(operation);
            freeClonedEvent = false;
            if (logger.fineEnabled()) {
              this.logger
                  .fine("Event :" + clonedEvent + " is added to TempQueue");
            }
            this.tmpQueuedEvents.add(clonedEvent);
            stats.incTempQueueSize();
            return;
          }
        }
      }
      if(this.enqueuedAllTempQueueEvents) {
        this.lifeCycleLock.readLock().lock();
      }
    }
    try {
      // If this gateway is not running, return
      if (!isRunning()) {
        if (logger.fineEnabled()) {
          this.logger
              .fine("Returning back without putting into the gateway sender queue");
        }
        return;
      }

      try {
        AbstractGatewaySenderEventProcessor ev = this.eventProcessor;
        if (ev == null) {
          getStopper().checkCancelInProgress(null);
          this.getCache().getDistributedSystem().getCancelCriterion()
              .checkCancelInProgress(null);
          // event processor will be null if there was an authorization
          // problem
          // connecting to the other site (bug #40681)
          if (ev == null) {
            throw new GatewayCancelledException(
                "Event processor thread is gone");
          }
        }
        ev.enqueueEvent(operation, clonedEvent);
      } catch (CancelException e) {
        this.getLogger().info(LocalizedStrings.DEBUG,
            "caught cancel exception", e);
      } catch (RegionDestroyedException e) {
        this.getLogger()
        .warning(
        LocalizedStrings.GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3,
        new Object[] { this, getId(), operation, clonedEvent }, e);
      } catch (Exception e) {
        this.getLogger().severe(LocalizedStrings
            .GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3,
                new Object[] { this, getId(), operation, clonedEvent }, e);
      }
    } finally {
      this.lifeCycleLock.readLock().unlock();
    }
    } finally {
      if (freeClonedEvent) {
        clonedEvent.release(); // fix for bug 48035
      }
    }
  }

  /**
   * During sender is getting started, if there are any cache operation on queue then that event will be stored in temp queue. 
   * Once sender is started, these event from tmp queue will be added to sender queue.
   * 
   * Apart from sender's start() method, this method also gets called from ParallelGatewaySenderQueue.addPartitionedRegionForRegion(). 
   * This is done to support the postCreateRegion scenario i.e. the sender is already running and region is created later.
   * The eventProcessor can be null when the method gets invoked through this flow: 
   * ParallelGatewaySenderImpl.start() -> ParallelGatewaySenderQueue.<init> -> ParallelGatewaySenderQueue.addPartitionedRegionForRegion 
   */
  public void enqueTempEvents() {
    if (this.eventProcessor != null) {//Fix for defect #47308
      EntryEventImpl nextEvent = null;
      final GatewaySenderStats stats = getStatistics();
      try {

        // Now finish emptying the queue with synchronization to make
        // sure we don't miss any events.
        synchronized (this.queuedEventsSync) {
          while ((nextEvent = tmpQueuedEvents.poll()) != null) {
            try {
              if (!beforeEnqueue(nextEvent)) {
                // Yogesh: this should not be a warning message in GemFireXD or
                // GemFire. In GemFireXD it should be logged only TraceDBSynchronizer
                // is ON
                // getLogger().warning(LocalizedStrings
                // .GatewayEventProcessor_EVENT_0_IS_NOT_ADDED_TO_QUEUE, event);
                stats.incEventsFiltered();
                continue;
              }
              if (logger.fineEnabled()) {
                this.logger
                    .fine("Event :" + nextEvent + " is enqueued to GatewaySenderQueue from TempQueue");
              }
              stats.decTempQueueSize();
              this.eventProcessor.enqueueEvent(nextEvent.getEventType(), nextEvent);
            } finally {
              nextEvent.release();
            }
          }
          this.enqueuedAllTempQueueEvents = true;
        }
      }
      catch (CacheException e) {
        this.getLogger().info(LocalizedStrings.DEBUG,
            "caught cancel exception", e);
      }
      catch (IOException e) {
        this
            .getLogger()
            .severe(
                LocalizedStrings.GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3,
                new Object[] { this, getId(), nextEvent.getEventType(), nextEvent }, e);
      }
    }
  }
  
  /**
   * Removes the EntryEventImpl, whose tailKey matches with the provided tailKey, 
   * from tmpQueueEvents. 
   * @param tailKey
   */
  public void removeFromTempQueueEvents(Object tailKey) {
    synchronized (this.queuedEventsSync) {
      Iterator<EntryEventImpl> itr = this.tmpQueuedEvents.iterator();
      while (itr.hasNext()) {
        EntryEventImpl event = itr.next();
        if (tailKey.equals(event.getTailKey())) {
          event.release();
          itr.remove();
          return;
        }
      }
    }
  }
  
  /**
   * During sender is getting stopped, if there are any cache operation on queue then that event will be stored in temp queue. 
   * Once sender is started, these event from tmp queue will be cleared.
   */
  public void clearTempEventsAfterSenderStopped() {
    EntryEventImpl nextEvent = null;
    while ((nextEvent = tmpQueuedEvents.poll()) != null) {
      nextEvent.release();
    }
    synchronized (this.queuedEventsSync) {
      while ((nextEvent = tmpQueuedEvents.poll()) != null) {
        nextEvent.release();
      }
      this.enqueuedAllTempQueueEvents = false;
    }
  }

  /**
   * @param clonedEvent
   */
  abstract protected void setModifiedEventId(EntryEventImpl clonedEvent);
  
  public int getTmpQueuedEventSize() {
    if (tmpQueuedEvents != null) {
      return tmpQueuedEvents.size();
    }
    return 0;
  }
  
  protected boolean logProxyFailure() {
    assert Thread.holdsLock(this);
    // always log the first failure
    if (logger.fineEnabled() || this.proxyFailureTries == 0) {
      return true;
    } else {
      // subsequent failures will be logged on 30th, 300th, 3000th try
      // each try is at 100millis from higher layer so this accounts for logging
      // after 3s, 30s and then every 5mins
      if (this.proxyFailureTries >= 3000) {
        return (this.proxyFailureTries % 3000) == 0;
      } else {
        return (this.proxyFailureTries == 30 || this.proxyFailureTries == 300);
      }
    }
  }

  public void setEnqueuedAllTempQueueEvents(boolean enqueuedAllTempQueueEvents) {
    this.enqueuedAllTempQueueEvents = enqueuedAllTempQueueEvents;
  }

  public long estimateMemoryFootprint(SingleObjectSizer sizer) {
    return sizer.sizeof(this) + eventProcessor.estimateMemoryFootprint(sizer);
  }
  
  @Override
  public boolean isNonWanDispatcher() {
    return this.getRemoteDSId() == GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;
  }
  
}

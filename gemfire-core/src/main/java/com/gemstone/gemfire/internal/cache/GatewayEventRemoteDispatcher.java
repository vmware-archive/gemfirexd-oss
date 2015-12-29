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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.GatewayConfigurationException;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ServerProxy;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionDestroyedException;
import com.gemstone.gemfire.internal.cache.GatewayImpl.GatewayEventProcessor;
import com.gemstone.gemfire.internal.cache.tier.BatchException;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * Class <code>GatewayEventRemoteDispatcher</code> dispatches batches of
 * <code>GatewayEvent</code>s to remote <code>Gateway</code>s. This dispatcher
 * is used in the WAN case.
 *
 * @author Barry Oglesby
 * @since 5.1
 */
class GatewayEventRemoteDispatcher implements GatewayEventDispatcher {

  /**
   * The <code>GatewayEventProcessor</code> used by this
   * <code>CacheListener</code> to process events.
   */
  private final GatewayEventProcessor eventProcessor;

  /**
   * The <code>Connection</code> used by this <code>GatewayEventDispatcher</code>
   */
  private volatile Connection connection;

  /**
   * The <code>LogWriterImpl</code> used by this <code>GatewayEventDispatcher</code>
   */
  private final LogWriterI18n logger;

  private final Set<String> notFoundRegions = new HashSet<String>();
  
  private final Object notFoundRegionsSync = new Object();

  protected GatewayEventRemoteDispatcher(GatewayEventProcessor eventProcessor) {
    this.eventProcessor = eventProcessor;
    this.logger = eventProcessor.getLogger();
    try {
      initializeConnection();
    }
    catch(GatewayException e) {
      if(e.getCause() instanceof GemFireSecurityException || e instanceof GatewayConfigurationException) {
        throw e;
      }
    }
  }

  /**
   * Artificial delay introduced to batch sending to simulate a real WAN link.
   * This can be set using the 'gemfire.artificialGatewayDelay' System
   * property.
   *
   * @since 5.7
   */
  private static final int ARTIFICIAL_DELAY =
    Integer.getInteger("gemfire.artificialGatewayDelay", -1).intValue();

  /**
   * Sends a batch of messages to the corresponding <code>Gateway</code>.
   * @param events The <code>List</code> of events to send
   * @param removeFromQueueOnException Boolean whether to remove from the
   * queue when a <code>BatchException</code> occurs. This will be true in
   * the normal processing case; false in the failover case.
   *
   * @return whether the batch of messages was successfully sent
   */
  public boolean dispatchBatch(List events, boolean removeFromQueueOnException) {
    GatewayStats statistics = this.eventProcessor.getGateway().getStatistics();
    boolean success = false;
    try {
      // Send the batch to the corresponding Gateway
      long start = statistics.startTime();

      // Add an artificial delay for testing purposes
      if (ARTIFICIAL_DELAY > 0) {
        try {
          Thread.sleep(ARTIFICIAL_DELAY);
        }
        catch(InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }

      dispatchBatch(events);
      statistics.endBatch(start, events.size());
      success = true;
    }
    catch (GatewayException e) {
      // A warning message should already have been logged

      // Get the cause. Remove and CacheWriterExceptions between the
     // GatewayException and the remote site exception.

      Throwable t = e.getCause();

      if (this.eventProcessor.getGateway().getProxy().isDestroyed()) {
        // if our pool is shutdown then just be silent
        
      } else
     if (t instanceof IOException || t instanceof ServerConnectivityException || t instanceof ConnectionDestroyedException) {
      // If the cause is an IOException or a ServerException, sleep and retry.
        // Sleep for a bit and recheck.
        this.eventProcessor.logBatchFine("Because of IOException, failed to dispatch the following ", events);
        try {Thread.sleep(5000);} catch (InterruptedException ie) {
          // No need to set the interrupt bit, we're exiting the thread.
          if (!this.eventProcessor.getIsStopped()) {
            this.logger.severe(LocalizedStrings.GatewayEventRemoteDispatcher_AN_INTERRUPTEDEXCEPTION_OCCURRED_BUT_THE_PROCESSOR_IS_NOT_STOPPED, e);
          }
        }
      }
      else if (t instanceof BatchException) {
       // If the cause is a BatchException, remove the successful entries up to
       // the event that failed from the queue. Take the event that failed from
       // the queue. For any other causes, stop the processor.
        BatchException be = (BatchException) t;

        // Increment the batch id
        this.eventProcessor.incrementBatchId();

        // Remove the offending message from the queue if necessary
        if (removeFromQueueOnException) {
          // Log a warning if necessary. Only log RegionDestroyedExceptions once per region.
          boolean logWarning = true;
          if (be.getCause() instanceof RegionDestroyedException) {
            RegionDestroyedException rde = (RegionDestroyedException) be.getCause();
            synchronized (this.notFoundRegionsSync) {
              if (this.notFoundRegions.contains(rde.getRegionFullPath())) {
                logWarning = false;
              } else {
                this.notFoundRegions.add(rde.getRegionFullPath());
              }
            }
          }
          if (logWarning) {
            this.logger.warning(LocalizedStrings.GatewayEventRemoteDispatcher_A_BATCHEXCEPTION_OCCURRED_PROCESSING_EVENT__0, be.getIndex(), be);
          }
          try {
            // Remove the events from the queue prior to the failed one
            // For example, if the index of the failed event == 50, the
            // failed one is the 51st event being processed and there were
            // 50 successful events processed before it.
            //todo - what to do if an exception occurs during remove()?
            this.eventProcessor.eventQueueRemove(be.getIndex());

            // Take the failed event from the queue and log it
            GatewayEventImpl ge = (GatewayEventImpl) this.eventProcessor.eventQueueTake();
            statistics.setQueueSize(this.eventProcessor.eventQueueSize());
            if (logWarning) {
              this.logger.warning(LocalizedStrings.GatewayEventRemoteDispatcher_THE_EVENT_BEING_PROCESSED_WHEN_THE_BATCHEXCEPTION_OCCURRED_WAS__0, ge);
            }
          } catch (CacheException ce) {
            this.logger.warning(LocalizedStrings.GatewayEventRemoteDispatcher_THE_FOLLOWING_CACHEEXCEPTION_OCCURRED_WHILE_REMOVING__0__EVENTS, be.getIndex(), ce);
          }
          catch (InterruptedException ie) {
            // don't reenable; we'll just exit
            this.logger.warning(LocalizedStrings.
              GatewayEventRemoteDispatcher_INTERRUPTED_WHILE_REMOVING_0_EVENTS,
              be.getIndex(), ie);
          }
        }
        else {
          this.logger.severe(LocalizedStrings.GatewayEventRemoteDispatcher_A_BATCHEXCEPTION_OCCURRED_DURING_FAILOVER_PROCESSING_EVENT__0__THIS_MESSAGE_WILL_CONTINUE_TO_BE_PROCESSED, Integer.valueOf(be.getIndex()), be);
        }
      }
      else {
        this.logger.severe(LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH, e);
        this.eventProcessor.setIsStopped(true);
      }
    }
    catch (CancelException e) {
      this.logger.fine("Stopping the processor because cancellation occurred while processing a batch");
      this.eventProcessor.setIsStopped(true);
      throw e;
    }
    catch (Exception e) {
      this.logger.severe(LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH, e);
      this.eventProcessor.setIsStopped(true);
    }
    return success;
  }

  /**
   * Sends a batch of messages to the corresponding <code>Gateway</code>.
   * @param events The <code>List</code> of events to send
   *
   * @throws GatewayException
   */
  private void dispatchBatch(List events) throws GatewayException {
    if (events.isEmpty()) {
      return;
    }
    Exception ex = null;
    Connection connection = getConnection();
    int batchId = this.eventProcessor.getBatchId();
    try {
      //this._logger.warning(this._gateway + ": Dispatching batch (id=" +
      // batchId + ") of " + events.size() + " events" + ", queue size: " +
      // this._eventQueue.size());
      ServerProxy sp = new ServerProxy(this.eventProcessor.getGateway().getProxy());
      sp.dispatchBatch(connection, events, batchId,
                       this.eventProcessor.getGateway().getEarlyAck());
      //this._logger.warning(this._gateway + ": Dispatched batch (id=" +
      // batchId + ") of " + events.size() + " events" + ", queue size: " +
      // this._eventQueue.size());
      return;
    }
    catch (ServerOperationException e) {
      Throwable t = e.getCause();
      if (t instanceof BatchException) {
        // A BatchException has occurred.
        // Do not process the connection as dead since it is not dead.
        ex = (BatchException)t;
      }
      else {
        ex = e;
      }
    }
    catch (Exception e) {
      // An Exception has occurred. Get its cause.
      Throwable t = e.getCause();
      if (t instanceof IOException) {
        // An IOException has occurred.
        ex = (IOException)t;
      }
      else {
        ex = e;
      }
    }
    // keep using the connection if we had
    //a batch exception
    //TODO - is this code even needed? OpExecutorImpl
    //automatically destroys the connection if it needs to be 
    //destroyed. All we would need to do is clear the connection
    //reference.
    if(!(ex instanceof BatchException)) {
      destroyConnection();
    }
    
    // If the code gets here, an exception has occurred.
    // Wrap the exception in a GatewayException, then log and rethrow it
    GatewayException ge = new GatewayException(LocalizedStrings.GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2.toLocalizedString(new Object[] {this, Integer.valueOf(batchId), connection}), ex);
    // no need to log it here; the caller will do the logging
    throw ge;
  }

  /**
   * Acquires or adds a new <code>Connection</code> to the corresponding
   * <code>Gateway</code>
   *
   * @return the <code>Connection</code>
   *
   * @throws GatewayException
   */
  Connection getConnection() throws GatewayException {
    if (this.connection == null) {
      // Initialize the connection
      initializeConnection();

      // Reset the batch id to 0 to match the new connection
      this.eventProcessor.resetBatchId();
    }
    return this.connection;
  }
  
  void destroyConnection() {
    Connection con = this.connection;
    if (con != null) {
      if (!con.isDestroyed()) {
        con.destroy();
        this.eventProcessor.getGateway().getProxy().returnConnection(con);
      }
      // Reset the connection so the next time through a new one will be
      // obtained
      this.connection = null;
    }
  }

  /**
   * This count is reset to 0 each time a successful connection is made.
   */
  private int failedConnectCount = 0;

  /**
   * Initializes the <code>Connection</code>.
   *
   * @throws GatewayException
   */
  private void initializeConnection() throws GatewayException, GemFireSecurityException {
    // Attempt to acquire a connection
    Connection connection;
    try {
      connection = this.eventProcessor.getGateway().getProxy().acquireConnection();
    }
    catch(ServerConnectivityException e) {
      this.failedConnectCount++;
      Throwable ex = null;
      
      if(e.getCause() instanceof GemFireSecurityException) {
        ex = e.getCause();
        if (this.failedConnectCount == 1) {
          // only log this message once; another msg is logged once we connect
          this.logger.warning(LocalizedStrings.GatewayEventRemoteDispatcher_0_COULD_NOT_CONNECT_1, new Object[] {this.eventProcessor.getGateway(), ex.getMessage()});
        }
        throw (GemFireSecurityException)ex;
      }
      if(e.getCause() instanceof GatewayConfigurationException) {
        ex = e.getCause();
        if (this.failedConnectCount == 1) {
          // only log this message once; another msg is logged once we connect
          this.logger.warning(LocalizedStrings.GatewayEventRemoteDispatcher_0_COULD_NOT_CONNECT_1, new Object[] {this.eventProcessor.getGateway(), ex.getMessage()});
        }
        throw (GatewayConfigurationException)ex;
      }
      else {
        List servers = this.eventProcessor.getGateway().getProxy().getCurrentServers();
        String ioMsg = null;
        if (servers.size() == 0) {
          ioMsg = LocalizedStrings.GatewayEventRemoteDispatcher_THERE_ARE_NO_ACTIVE_SERVERS.toLocalizedString(); 
        }
        else {
          final StringBuilder buffer = new StringBuilder();
          for (Iterator i = servers.iterator(); i.hasNext();) {
            String endpointName = String.valueOf(i.next());
            if (buffer.length() > 0) {
              buffer.append(", ");
            }
            buffer.append(endpointName);
          }
          ioMsg = LocalizedStrings.GatewayEventRemoteDispatcher_NO_AVAILABLE_CONNECTION_WAS_FOUND_BUT_THE_FOLLOWING_ACTIVE_SERVERS_EXIST_0.toLocalizedString(buffer.toString());
        }
        ex = new IOException(ioMsg);
      }
      if (this.failedConnectCount == 1) {
        // only log this message once; another msg is logged once we connect
        this.logger.warning(LocalizedStrings.GatewayEventRemoteDispatcher__0___COULD_NOT_CONNECT, this.eventProcessor.getGateway());
      }
      // Wrap the IOException in a GatewayException so it can be processed the
      // same as the other exceptions that might occur in sendBatch.
      throw new GatewayException(LocalizedStrings.GatewayEventRemoteDispatcher__0___COULD_NOT_CONNECT.toLocalizedString(this.eventProcessor.getGateway()), ex);
    }
    if (this.failedConnectCount > 0) {
      Object[] logArgs = new Object[] {this.eventProcessor.getGateway(), connection, Integer.valueOf(this.failedConnectCount)};
      this.logger.info(LocalizedStrings.GatewayEventRemoteDispatcher_0_USING_1_AFTER_2_FAILED_CONNECT_ATTEMPTS, logArgs);        
      this.failedConnectCount = 0;
    } else {
      Object[] logArgs = new Object[] {this.eventProcessor.getGateway(), connection };
      this.logger.info(LocalizedStrings.GatewayEventRemoteDispatcher_0_USING_1, logArgs);
    }
    this.connection = connection;
  }
}

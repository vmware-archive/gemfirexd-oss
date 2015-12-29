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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ServerProxy;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionDestroyedException;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.UpdateAttributesProcessor;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 *
 */
public class GatewaySenderEventRemoteDispatcher implements
    GatewaySenderEventDispatcher {

  private final AbstractGatewaySenderEventProcessor processor;

  private volatile Connection connection;

  private final LogWriterI18n logger;

  private final Set<String> notFoundRegions = new HashSet<String>();
  
  private final Object notFoundRegionsSync = new Object();
  
  private final AbstractGatewaySender sender;
  
  private AckReaderThread ackReaderThread;
  
  private ReentrantReadWriteLock connectionLifeCycleLock = new ReentrantReadWriteLock();
  
  /**
   * This count is reset to 0 each time a successful connection is made.
   */
  private int failedConnectCount = 0;
  
  public GatewaySenderEventRemoteDispatcher(AbstractGatewaySenderEventProcessor eventProcessor) {
    this.processor = eventProcessor;
    this.sender = eventProcessor.getSender();
    this.logger = eventProcessor.getLogger();
//    this.ackReaderThread = new AckReaderThread(sender);
    try {
      initializeConnection();
    }
    catch (GatewaySenderException e) {
      if (e.getCause() instanceof GemFireSecurityException) {
        throw e;
      }
    }
  }
  
  protected GatewayAck readAcknowledgement(int lastBatchIdRead) {
    ServerProxy sp = new ServerProxy(this.processor.getSender().getProxy());
    GatewayAck ack = null;
    Exception ex;
    try {
      connection = getConnection();
      if (this.logger.fineEnabled()) {
        this.logger.fine(" Receiving ack on the thread " + connection);
      }
      this.connectionLifeCycleLock.readLock().lock();
      try {
        if (connection != null) {
          ack = (GatewayAck)sp.receiveAckFromReceiver(connection);
        }
      } finally {
        this.connectionLifeCycleLock.readLock().unlock();
      }

    } catch (Exception e) {
      Throwable t = e.getCause();
      if (t instanceof BatchException70) {
        // A BatchException has occurred.
        // Do not process the connection as dead since it is not dead.
        ex = (BatchException70)t;
      } else if (e instanceof GatewaySenderException) { //This Exception is thrown from getConnection
        ex = (Exception) e.getCause();
      }else {
        ex = e;
        // keep using the connection if we had a batch exception. Else, destroy
        // it
        destroyConnection();
      }
      if (this.sender.getProxy() == null || this.sender.getProxy().isDestroyed()) {
        // if our pool is shutdown then just be silent
      } else if (ex instanceof IOException
          || ex instanceof ServerConnectivityException
          || ex instanceof ConnectionDestroyedException) {
        // If the cause is an IOException or a ServerException, sleep and retry.
        // Sleep for a bit and recheck.
        if (this.processor.getLogger().fineEnabled()) {
			try {
	          this.processor.logBatchFineIOException(
	              "Because of IOException, failed to get Ack for the following ",
	              this.processor.getBatchIdToEventsMap().get(lastBatchIdRead + 1),
	              (lastBatchIdRead + 1));
			}catch(Exception ignore){
	            //#50220 that list is not synchronized
	        }
        }
      } else {
        if (!(ex instanceof CancelException)) {
          this.logger
              .severe(
                  LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH,
                  ex);
        }
        this.processor.setIsStopped(true);
      }
    }
    return ack;
  }
  
  @Override
  public boolean dispatchBatch(List events, boolean removeFromQueueOnException) {
    GatewaySenderStats statistics = this.sender.getStatistics();
    boolean success = false;
    try {
      long start = statistics.startTime();
      success =_dispatchBatch(events);
      statistics.endBatch(start, events.size());
    } catch (GatewaySenderException ge) {

      Throwable t = ge.getCause();
      if (this.sender.getProxy() == null || this.sender.getProxy().isDestroyed()) {
        // if our pool is shutdown then just be silent
      } else if (t instanceof IOException
          || t instanceof ServerConnectivityException
          || t instanceof ConnectionDestroyedException) {
        this.processor.handleException();
        // If the cause is an IOException or a ServerException, sleep and retry.
        // Sleep for a bit and recheck.
        if (this.processor.getLogger().fineEnabled()) {
          this.logger
              .fine("Because of IOException, failed to dispatch a batch with id : "
                  + this.processor.getBatchId());
        }
      }
      else {
        this.logger.severe(LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH, ge);
        this.processor.setIsStopped(true);
      }
    }
    catch (CancelException e) {
      if (this.logger.fineEnabled()) {
        this.logger
            .fine("Stopping the processor because cancellation occurred while processing a batch");
      }
      this.processor.setIsStopped(true);
      throw e;
    } catch (Exception e) {
      this.processor.setIsStopped(true);
      this.logger
          .severe(
              LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH,
              e);
    }
    return success;
  }

  private boolean _dispatchBatch(List events) {
    Exception ex = null;
    int currentBatchId = this.processor.getBatchId();
    connection = getConnection();
    int batchIdForThisConnection = this.processor.getBatchId();
    // This means we are writing to a new connection than the previous batch.
    // i.e The connection has been reset. It also resets the batchId.
    if (currentBatchId != batchIdForThisConnection
        || this.processor.isConnectionReset()) {
      return false;
    }
    try {
      ServerProxy sp = new ServerProxy(this.sender.getProxy());
      this.connectionLifeCycleLock.readLock().lock();
      try {
        if (connection != null) {
          sp.dispatchBatch_NewWAN(connection, events, currentBatchId,
              sender.isRemoveFromQueueOnException());
          if (this.logger.fineEnabled()) {
            this.logger.fine(this.processor.getSender()
                + ": Dispatched batch (id=" + currentBatchId + ") of " + events.size()
                + " events" + ", queue size: "
                + this.processor.getQueue().size() + " on connection "
                + connection);
          }
        } else {
          throw new ConnectionDestroyedException();
        }
      }
      finally{
        this.connectionLifeCycleLock.readLock().unlock();
      }
      return true;
    }
    catch (ServerOperationException e) {
      Throwable t = e.getCause();
      if (t instanceof BatchException70) {
        // A BatchException has occurred.
        // Do not process the connection as dead since it is not dead.
        ex = (BatchException70)t;
      }
      else {
        ex = e;
        // keep using the connection if we had a batch exception. Else, destroy it
        destroyConnection();
      }
      throw new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2.toLocalizedString(
              new Object[] {this, Integer.valueOf(currentBatchId), connection}), ex);
    }
    catch (Exception e) {
      // An Exception has occurred. Get its cause.
      Throwable t = e.getCause();
      if (t instanceof IOException) {
        // An IOException has occurred.
        ex = (IOException)t;
      } else {
        ex = e;
      }
      //the cause is not going to be BatchException70. So, destroy the connection
      destroyConnection();
      
      throw new GatewaySenderException(
          LocalizedStrings.GatewayEventRemoteDispatcher_0_EXCEPTION_DURING_PROCESSING_BATCH_1_ON_CONNECTION_2.toLocalizedString(
              new Object[] {this, Integer.valueOf(currentBatchId), connection}), ex);
    }
  }
  
  /**
   * Acquires or adds a new <code>Connection</code> to the corresponding
   * <code>Gateway</code>
   *
   * @return the <code>Connection</code>
   *
   * @throws GatewaySenderException
   * @throws InterruptedException 
   */
  public Connection getConnection() throws GatewaySenderException{
    // IF the connection is null 
    // OR the connection's ServerLocation doesn't match with the one stored in sender
    // THEN initialize the connection
    if(!this.sender.isParallel()) {
      if (this.connection == null
          || !this.connection.getServer().equals(this.sender.getServerLocation())) {
        if (this.logger.fineEnabled()) {
          logger
              .fine("Initializing new connection as serverlocation of old connection is : "
                  + ((this.connection == null) ? "null" : this.connection
                      .getServer())
                  + " and the serverlocation to connect is"
                  + this.sender.getServerLocation());
        }
        // Initialize the connection
        initializeConnection();
      }
    } else {
      if (this.connection == null) {
        initializeConnection();
      }
    }
    
    // Here we might wait on a connection to another server if I was secondary
    // so don't start waiting until I am primary
    Cache cache = this.sender.getCache();
    if (cache != null && !cache.isClosed()) {
      if (this.sender.isPrimary() && (this.connection != null)) {
        if (this.ackReaderThread == null || !this.ackReaderThread.isRunning()) {
          this.ackReaderThread = new AckReaderThread(this.sender);
          this.ackReaderThread.start();
          this.ackReaderThread.waitForRunningAckReaderThreadRunningState();
        }
      }
    }
    return this.connection;
  }
  
  public boolean isConnected(){
    return connection != null;
  }
  
  public void destroyConnection() {
    this.connectionLifeCycleLock.writeLock().lock();
    try {
      Connection con = this.connection;
      if (con != null) {
        if (!con.isDestroyed()) {
          con.destroy();
         this.sender.getProxy().returnConnection(con);
        }
        
        // Reset the connection so the next time through a new one will be
        // obtained
        this.connection = null;
        this.sender.setServerLocation(null);
      }
    }
    finally {
      this.connectionLifeCycleLock.writeLock().unlock();
    }
  }

  /**
   * Initializes the <code>Connection</code>.
   *
   * @throws GatewaySenderException
   */
  private void initializeConnection() throws GatewaySenderException,
      GemFireSecurityException {
    this.connectionLifeCycleLock.writeLock().lock(); 
    try {
      // Attempt to acquire a connection
      if (this.sender.getProxy() == null
          || this.sender.getProxy().isDestroyed()) {
        this.sender.initProxy();
      } else {
        this.processor.resetBatchId();
      }
      Connection con;
      try {
        if (this.sender.isParallel()) {
          con = this.sender.getProxy().acquireConnection();
          // For parallel sender, setting server location will not matter.
          // everytime it will ask for acquire connection whenever it needs it. I
          // am saving this server location for command purpose
          sender.setServerLocation(con.getServer());  
        } else {
          synchronized (((SerialGatewaySenderImpl)this.sender)
              .getLockForConcurrentDispatcher()) {
            if (this.sender.getServerLocation() != null) {
              if (this.logger.fineEnabled()) {
                logger.fine("ServerLocation is: " + sender.getServerLocation()
                    + ". Connecting to this serverLocation...");
              }
              con = this.sender.getProxy().acquireConnection(
                  this.sender.getServerLocation());
            } else {
              if (this.logger.fineEnabled()) {
                logger
                    .fine("ServerLocation is null. Creating new connection. ");
              }
              con = this.sender.getProxy().acquireConnection();
              // Acquired connection from pool!! Update the server location
              // information in the sender and
              // distribute the information to other senders ONLY IF THIS SENDER
              // IS
              // PRIMARY
              if (this.sender.isPrimary()) {
                if (sender.getServerLocation() == null) {
                  sender.setServerLocation(con.getServer());
                }
                new UpdateAttributesProcessor(this.sender).distribute(false);
              }
            }
          }
        }
      } catch (ServerConnectivityException e) {
        this.failedConnectCount++;
        Throwable ex = null;

        if (e.getCause() instanceof GemFireSecurityException) {
          ex = e.getCause();
          if (logConnectionFailure()) {
            // only log this message once; another msg is logged once we connect
            this.logger
                .severe(
                    LocalizedStrings.GatewayEventRemoteDispatcher_0_COULD_NOT_CONNECT_1,
                    new Object[] { this.processor.getSender().getId(),
                        ex.getMessage() });
          }
          throw new GatewaySenderException(ex);
        }
        List<ServerLocation> servers = this.sender.getProxy()
            .getCurrentServers();
        String ioMsg = null;
        if (servers.size() == 0) {
          ioMsg = LocalizedStrings.GatewayEventRemoteDispatcher_THERE_ARE_NO_ACTIVE_SERVERS
              .toLocalizedString();
        } else {
          final StringBuilder buffer = new StringBuilder();
          for (ServerLocation server : servers) {
            String endpointName = String.valueOf(server);
            if (buffer.length() > 0) {
              buffer.append(", ");
            }
            buffer.append(endpointName);
          }
          ioMsg = LocalizedStrings.GatewayEventRemoteDispatcher_NO_AVAILABLE_CONNECTION_WAS_FOUND_BUT_THE_FOLLOWING_ACTIVE_SERVERS_EXIST_0
              .toLocalizedString(buffer.toString());
        }
        ex = new IOException(ioMsg);
        // Set the serverLocation to null so that a new connection can be
        // obtained in next attempt
        this.sender.setServerLocation(null);
        // Removing the condition to log the msg once as we try only
        // after conncection retry interval.
        if (logConnectionFailure()) {
          // only log this message once; another msg is logged once we connect
          this.logger
              .severe(
                  LocalizedStrings.GatewayEventRemoteDispatcher__0___COULD_NOT_CONNECT,
                  this.processor.getSender().getId());

        }
        // Wrap the IOException in a GatewayException so it can be processed the
        // same as the other exceptions that might occur in sendBatch.
        throw new GatewaySenderException(
            LocalizedStrings.GatewayEventRemoteDispatcher__0___COULD_NOT_CONNECT
                .toLocalizedString(this.processor.getSender().getId()), ex);
      }
      if (this.failedConnectCount > 0) {
        Object[] logArgs = new Object[] { this.processor.getSender().getId(),
            con, Integer.valueOf(this.failedConnectCount) };
        this.logger
            .info(
                LocalizedStrings.GatewayEventRemoteDispatcher_0_USING_1_AFTER_2_FAILED_CONNECT_ATTEMPTS,
                logArgs);
        this.failedConnectCount = 0;
      } else {
        Object[] logArgs = new Object[] { this.processor.getSender().getId(),
            con };
        this.logger.info(
            LocalizedStrings.GatewayEventRemoteDispatcher_0_USING_1, logArgs);
      }
      this.connection = con;
    }
    finally {
      this.connectionLifeCycleLock.writeLock().unlock();
    }
  }
  
  protected boolean logConnectionFailure() {
    // always log the first failure
    if (logger.fineEnabled() || this.failedConnectCount == 0) {
      return true;
    }
    else {
      // subsequent failures will be logged on 30th, 300th, 3000th try
      // each try is at 100millis from higher layer so this accounts for logging
      // after 3s, 30s and then every 5mins
      if (this.failedConnectCount >= 3000) {
        return (this.failedConnectCount % 3000) == 0;
      }
      else {
        return (this.failedConnectCount == 30 || this.failedConnectCount == 300);
      }
    }
  }
  
  public static class GatewayAck {
    private int batchId;

    private int numEvents;

    private BatchException70 be;

    public GatewayAck(BatchException70 be, int bId) {
      this.be = be;
      this.batchId = bId;
    }

    public GatewayAck(int batchId, int numEvents) {
      this.batchId = batchId;
      this.numEvents = numEvents;
    }

    /**
     * @return the numEvents
     */
    public int getNumEvents() {
      return numEvents;
    }

    /**
     * @return the batchId
     */
    public int getBatchId() {
      return batchId;
    }

    public BatchException70 getBatchException() {
      return this.be;
    }
  }
    
  class AckReaderThread extends Thread {

    private Object runningStateLock = new Object();

    /**
     * boolean to make a shutdown request
     */
    private volatile boolean shutdown = false;

    private final GemFireCacheImpl cache;

    private volatile boolean ackReaderThreadRunning = false;

    public AckReaderThread(GatewaySender sender) {
      super("AckReaderThread for : " + sender.getId());
      this.setDaemon(true);
      this.cache = (GemFireCacheImpl)((AbstractGatewaySender)sender).getCache();
    }

    public void waitForRunningAckReaderThreadRunningState() {
      synchronized (runningStateLock) {
        while (!this.ackReaderThreadRunning) {
          try {
            this.runningStateLock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    private boolean checkCancelled() {
      if (shutdown) {
        return true;
      }

      if (cache.getCancelCriterion().cancelInProgress() != null) {
        return true;
      }
      return false;
    }

    @Override
    public void run() {
      InternalDistributedSystem ids = cache.getDistributedSystem();
      int lastBatchIdRead = -1;
      if (logger.fineEnabled()) {
        logger.fine("AckReaderThread started.. ");
      }

      synchronized (runningStateLock) {
        ackReaderThreadRunning = true;
        this.runningStateLock.notifyAll();
      }

      try {
        for (;;) {
          if (checkCancelled()) {
            break;
          }
          GatewayAck ack = readAcknowledgement(lastBatchIdRead);
          if (ack != null) {
            boolean gotBatchException = ack.getBatchException() != null;
            int batchId = ack.getBatchId();
            lastBatchIdRead = batchId;
            int numEvents = ack.getNumEvents();

            if (!gotBatchException) {
              logger
                  .info(
                      LocalizedStrings.GatewaySenderEventRemoteDispatcher_GATEWAY_SENDER_0_RECEIVED_ACK_FOR_BATCH_ID_1_OF_2_EVENTS,
                      new Object[] { processor.getSender(), ack.getBatchId(),
                          ack.getNumEvents() });
            } else {
              logger
                  .info(
                      LocalizedStrings.GatewaySenderEventRemoteDispatcher_GATEWAY_SENDER_0_RECEIVED_ACK_FOR_BATCH_ID_1_WITH_EXCEPTION,
                      new Object[] { processor.getSender(), ack.getBatchId() },
                      ack.getBatchException());
            }
            // If the batch is successfully processed, remove it from the
            // queue.
            if (gotBatchException) {
              // If we get PDX related exception in the batch exception then try
              // to resend all the pdx events as well in the next batch.
              final GatewaySenderStats statistics = sender.getStatistics();
              statistics.incBatchesRedistributed();
              // log batch exceptions and remove all the events if remove from
              // exception is true
              // do not remove if it is false
              if (sender.isRemoveFromQueueOnException()) {
                // log the batchExceptions
                logBatchExceptions(ack.getBatchException());
                List events = processor.getBatchIdToEventsMap().get(
                    ack.getBatchException().getBatchId());
                if (events != null) {
                  processor.handleSuccessBatchAck(batchId, events.size());
                }
              } else {
                // we assume that batch exception will not occur for PDX related
                // events
                List<GatewaySenderEventImpl> pdxEvents = processor
                    .getBatchIdToPDXEventsMap().get(
                        ack.getBatchException().getBatchId());
                if (pdxEvents != null) {
                  for (GatewaySenderEventImpl senderEvent : pdxEvents) {
                    senderEvent.isAcked = true;
                  }
                }
                // log the batchExceptions
                logBatchExceptions(ack.getBatchException());
                // remove the events that have been processed.
                BatchException70 be = ack.getBatchException();
                List<BatchException70> exceptions = be.getExceptions();

                for (int i = 0; i < exceptions.get(0).getIndex(); i++) {
                  processor.eventQueueRemove();
                }
                // reset the sender
                processor.handleException();
              }
            } // successful batch
            else { // The batch was successful.
              processor.handleSuccessBatchAck(batchId, numEvents);
            }
          } else {
            // If we have received IOException.
            if (logger.fineEnabled()) {
              logger.fine(processor.getSender()
                  + ": Received null ack from remote site.");
            }
            processor.handleException();
            try { // This wait is before trying to getting new connection to
                  // receive ack. Without this there will be continuous call to
                  // getConnection
              Thread.sleep(GatewaySender.CONNECTION_RETRY_INTERVAL);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        }
      } catch (Exception e) {
        if (!checkCancelled()) {
          logger
              .severe(
                  LocalizedStrings.GatewayEventRemoteDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH,
                  e);
        }
        sender.lifeCycleLock.writeLock().lock();
        try {
          processor.stopProcessing();
          sender.clearTempEventsAfterSenderStopped();
        } finally {
          sender.lifeCycleLock.writeLock().unlock();
        }
        // destroyConnection();
      } finally {
        if (logger.fineEnabled()) {
          logger.fine("AckReaderThread exiting. ");
        }
        ackReaderThreadRunning = false;
      }

    }

    /**
     * @param exception 
     * 
     */
    private void logBatchExceptions(BatchException70 exception) {
      for (BatchException70 be : exception.getExceptions()) {
        boolean logWarning = true;
        if (be.getCause() instanceof RegionDestroyedException) {
          RegionDestroyedException rde = (RegionDestroyedException)be
              .getCause();
          synchronized (notFoundRegionsSync) {
            if (notFoundRegions.contains(rde.getRegionFullPath())) {
              logWarning = false;
            } else {
              notFoundRegions.add(rde.getRegionFullPath());
            }
          }
        } else if (be.getCause() instanceof IllegalStateException
            && be.getCause().getMessage().contains("Unknown pdx type")) {
          List<GatewaySenderEventImpl> pdxEvents = processor
              .getBatchIdToPDXEventsMap().get(be.getBatchId());
          if (pdxEvents != null) {
            for (GatewaySenderEventImpl senderEvent : pdxEvents) {
              senderEvent.isAcked = false;
            }
          }
        }
        if (logWarning) {
          logger
              .warning(
                  LocalizedStrings.GatewayEventRemoteDispatcher_A_BATCHEXCEPTION_OCCURRED_PROCESSING_EVENT__0,
                  be.getIndex(), be);
        }

        List events = processor.getBatchIdToEventsMap().get(be.getBatchId());

        if (events != null) {
          GatewaySenderEventImpl gsEvent = (GatewaySenderEventImpl)events
              .get(be.getIndex());

          if (logWarning) {
            logger
                .warning(
                    LocalizedStrings.GatewayEventRemoteDispatcher_THE_EVENT_BEING_PROCESSED_WHEN_THE_BATCHEXCEPTION_OCCURRED_WAS__0,
                    gsEvent);

          }
        }
      }
    }

    boolean isRunning() {
      return this.ackReaderThreadRunning;
    }

    public void shutdown() {
      // we need to destroy connection irrespective of we are listening on it or
      // not. No need to take lock as the reader thread may be blocked and we might not
      // get chance to destroy unless that returns.
      if (connection != null) {
        if (!connection.isDestroyed()) {
          connection.destroy();
          sender.getProxy().returnConnection(connection);
        }
      }
      this.shutdown = true;
      this.interrupt();
      boolean interrupted = Thread.interrupted();
      try {
        this.join(15 * 1000);
      } catch (InterruptedException e) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (this.isAlive()) {
        logger
            .warning(LocalizedStrings.GatewaySender_ACKREADERTHREAD_IGNORED_CANCELLATION);
      }
    }
  }
    
  public void stopAckReaderThread() {
    if (this.ackReaderThread != null) {
      this.ackReaderThread.shutdown();
    }    
  }
}


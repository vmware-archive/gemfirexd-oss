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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.UpdateAttributesProcessor;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderConfigurationException;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAdvisor.GatewaySenderProfile;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 *
 */
public class SerialGatewaySenderImpl extends AbstractGatewaySender {

  final ThreadGroup loggerGroup = LogWriterImpl.createThreadGroup(
      "Remote Site Discovery Logger Group", this.logger);

  public SerialGatewaySenderImpl(Cache cache,
      GatewaySenderAttributes attrs) {
    super(cache, attrs);
  }
  
  final Object lockForConcurrentDispatcher = new Object();
  
  @Override
  public void start() {
    if (logger.fineEnabled()) {
      logger.fine("Starting gatewaySender : " + this);
    }
    this.lifeCycleLock.writeLock().lock();
    try {
      if (isRunning()) {
        if (logger.warningEnabled()) {
          logger.warning(
              LocalizedStrings.GatewaySender_SENDER_0_IS_ALREADY_RUNNING,
              this.getId());
        }
        return;
      }
      if (this.remoteDSId != DEFAULT_DISTRIBUTED_SYSTEM_ID) {
        String locators = ((GemFireCacheImpl)this.cache).getDistributedSystem()
            .getConfig().getLocators();
        if (locators.length() == 0) {
          throw new GatewaySenderConfigurationException(
              LocalizedStrings.AbstractGatewaySender_LOCATOR_SHOULD_BE_CONFIGURED_BEFORE_STARTING_GATEWAY_SENDER
                  .toLocalizedString());
        }
      }
      getSenderAdvisor().initDLockService();
      if (!isPrimary()) {
        if (getSenderAdvisor().volunteerForPrimary()) {
          getSenderAdvisor().makePrimary();
        } else {
          getSenderAdvisor().makeSecondary();
        }
      }
      if (getDispatcherThreads() > 1) {
        eventProcessor = new ConcurrentSerialGatewaySenderEventProcessor(
            SerialGatewaySenderImpl.this);
      } else {
        eventProcessor = new SerialGatewaySenderEventProcessor(
            SerialGatewaySenderImpl.this, getId());
      }
      eventProcessor.start();
      waitForRunningStatus();
      this.startTime = System.currentTimeMillis();
      final GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
      cache.getPdxRegistry().gatewaySenderStarted(this);
      new UpdateAttributesProcessor(this).distribute(false);
      cache.gatewaySenderStarted(this);

      InternalDistributedSystem system = (InternalDistributedSystem) this.cache
          .getDistributedSystem();
      system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_START, this);
      
      getLogger().info(LocalizedStrings.SerialGatewaySenderImpl_STARTED__0,
          this);
  
      enqueTempEvents();
    } finally {
      this.lifeCycleLock.writeLock().unlock();
    }
  }
  
  @Override
  public void stop() {
    if (this.logger.fineEnabled()) {
      this.logger.fine("Stopping Gateway Sender : " + this);
    }
    this.lifeCycleLock.writeLock().lock();
    try {
      // Stop the dispatcher
      AbstractGatewaySenderEventProcessor ev = this.eventProcessor;
      if (ev != null && !ev.isStopped()) {
        ev.stopProcessing();
      }
      this.eventProcessor = null;

      // Stop the proxy (after the dispatcher, so the socket is still
      // alive until after the dispatcher has stopped)
      stompProxyDead();

      // Close the listeners
      for (AsyncEventListener listener : this.listeners) {
        listener.close();
      }
      this.getLogger().info(LocalizedStrings.GatewayImpl_STOPPED__0, this);
      
      clearTempEventsAfterSenderStopped();
    } finally {
      this.lifeCycleLock.writeLock().unlock();
    }
    if (this.isPrimary()) {
      try {
        DistributedLockService
            .destroy(getSenderAdvisor().getDLockServiceName());
      } catch (IllegalArgumentException e) {
        // service not found... ignore
      }
    }
    if (getQueues() != null && !getQueues().isEmpty()) {
      for (RegionQueue q : getQueues()) {
        ((SerialGatewaySenderQueue)q).cleanUp();
      }
    }
    this.setIsPrimary(false);
    new UpdateAttributesProcessor(this).distribute(false);
    ((GemFireCacheImpl)getCache()).gatewaySenderStopped(this);

    Thread lockObtainingThread = getSenderAdvisor().getLockObtainingThread();
    if (lockObtainingThread != null && lockObtainingThread.isAlive()) {
      // wait a while for thread to terminate
      try {
        lockObtainingThread.join(3000);
      } catch (InterruptedException ex) {
        // we allowed our join to be canceled
        // reset interrupt bit so this thread knows it has been interrupted
        Thread.currentThread().interrupt();
      }
      if (lockObtainingThread.isAlive()) {
        getLogger()
            .info(
                LocalizedStrings.GatewayHubImpl_COULD_NOT_STOP_LOCK_OBTAINING_THREAD_DURING_GATEWAY_HUB_SHUTDOWN);
      }
    }
    
    InternalDistributedSystem system = (InternalDistributedSystem) this.cache
        .getDistributedSystem();
    system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_STOP, this);
  }
  
  /**
   * Has a reference to a GatewayEventImpl and has a timeout value.
   */
  public static class EventWrapper  {
    /**
     * Timeout events received from secondary after 5 minutes by default.
     */
    static private final int EVENT_TIMEOUT
      = Integer.getInteger("Gateway.EVENT_TIMEOUT", 5 * 60 * 1000).intValue();
    public final long timeout;
    public final GatewaySenderEventImpl event;
    public EventWrapper(GatewaySenderEventImpl e) {
      this.event = e;
      this.timeout = System.currentTimeMillis() + EVENT_TIMEOUT;
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SerialGatewaySender{");
    sb.append("id=" + getId());
    sb.append(",remoteDsId="+ getRemoteDSId());
    sb.append(",isRunning ="+ isRunning());
    sb.append(",isPrimary ="+ isPrimary());
    sb.append("}");
    return sb.toString();
  }
 
  @Override
  public void fillInProfile(Profile profile) {
    assert profile instanceof GatewaySenderProfile;
    GatewaySenderProfile pf = (GatewaySenderProfile)profile;
    pf.Id = getId();
    pf.startTime = getStartTime();
    pf.remoteDSId = getRemoteDSId();
    pf.isRunning = isRunning();
    pf.isPrimary = isPrimary();
    pf.isParallel = false;
    pf.isBatchConflationEnabled = isBatchConflationEnabled();
    pf.isPersistenceEnabled = isPersistenceEnabled();
    pf.alertThreshold = getAlertThreshold();
    pf.manualStart = isManualStart();
    for (com.gemstone.gemfire.cache.wan.GatewayEventFilter filter : getGatewayEventFilters()) {
      pf.eventFiltersClassNames.add(filter.getClass().getName());
    }
    for (GatewayTransportFilter filter : getGatewayTransportFilters()) {
      pf.transFiltersClassNames.add(filter.getClass().getName());
    }
    for (AsyncEventListener listener : getAsyncEventListeners()) {
      pf.senderEventListenerClassNames.add(listener.getClass().getName());
    }
    pf.isDiskSynchronous = isDiskSynchronous();
    pf.dispatcherThreads = getDispatcherThreads();
    pf.orderPolicy = getOrderPolicy();
    pf.serverLocation = this.getServerLocation(); 
  }

  @Override
  protected void setModifiedEventId(EntryEventImpl clonedEvent) {
    // Nothing to do in this case
  }

  public Object getLockForConcurrentDispatcher() {
    return this.lockForConcurrentDispatcher;
  }
}

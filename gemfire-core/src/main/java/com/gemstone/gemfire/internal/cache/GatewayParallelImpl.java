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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Represents a {@link Gateway} with concurrent threads and queues processing events.
 *
 * @author Barry Oglesby
 * 
 * @since 6.5.1
 */
public class GatewayParallelImpl extends AbstractGateway {

  /**
   * The List of actual <code>Gateway</code>s handled by this
   * <code>Gateway</code>
   */
  private final List<Gateway> gateways = new ArrayList<Gateway>();

  /** Synchronizes lifecycle state including start, stop, and _isRunning */
  private final Object controlLock = new Object();

  GatewayParallelImpl(GatewayHubImpl hub, String id, int concurrencyLevel) {
    super(hub, id, id + ".rollup", null);
    setOrderPolicy(OrderPolicy.KEY);
    initializeGateways(concurrencyLevel);
  }

  public void addEndpoint(String id, String host, int port)
      throws GatewayException {
    for (Gateway gateway : getGateways()) {
      gateway.addEndpoint(id, host, port);
    }
  }

  @SuppressWarnings("rawtypes")
  public List getEndpoints() {
    List endpoints = null;
    if (getGateways().isEmpty()) {
      endpoints = Collections.emptyList();
    } else {
      endpoints = getGateways().get(0).getEndpoints();
    }
    return endpoints;
  }

  public boolean hasEndpoints() {
    boolean hasEndpoints = false;
    if (!getGateways().isEmpty()) {
      hasEndpoints = getGateways().get(0).hasEndpoints();
    }
    return hasEndpoints;
  }

  public void addListener(GatewayEventListener listener)
      throws GatewayException {
    for (Gateway gateway : getGateways()) {
      gateway.addListener(listener);
    }
  }

  public List<GatewayEventListener> getListeners() {
    List<GatewayEventListener> listeners = null;
    if (getGateways().isEmpty()) {
      listeners = Collections.emptyList();
    } else {
      listeners = getGateways().get(0).getListeners();
    }
    return listeners;
  }

  public boolean hasListeners() {
    boolean hasListeners = false;
    if (!getGateways().isEmpty()) {
      hasListeners = getGateways().get(0).hasListeners();
    }
    return hasListeners;
  }

  public void setSocketBufferSize(int socketBufferSize) {
    for (Gateway gateway : getGateways()) {
      gateway.setSocketBufferSize(socketBufferSize);
    }
  }

  public int getSocketBufferSize() {
    int socketBufferSize = 0;
    if (!getGateways().isEmpty()) {
      socketBufferSize = getGateways().get(0).getSocketBufferSize();
    }
    return socketBufferSize;
  }

  public void setSocketReadTimeout(int socketReadTimeout) {
    for (Gateway gateway : getGateways()) {
      gateway.setSocketReadTimeout(socketReadTimeout);
    }
  }

  public int getSocketReadTimeout() {
    int socketReadTimeout = 0;
    if (!getGateways().isEmpty()) {
      socketReadTimeout = getGateways().get(0).getSocketReadTimeout();
    }
    return socketReadTimeout;
  }

  public void setQueueAttributes(GatewayQueueAttributes queueAttributes) {
    for (Gateway gateway : getGateways()) {
      gateway.setQueueAttributes(queueAttributes);
    }
  }

  public GatewayQueueAttributes getQueueAttributes() {
    GatewayQueueAttributes queueAttributes = null;
    if (!getGateways().isEmpty()) {
      queueAttributes = getGateways().get(0).getQueueAttributes();
    }
    return queueAttributes;
  }

  public int getConcurrencyLevel() {
    return getGateways().size();
  }
  
  public void start() throws IOException {
    synchronized (this.controlLock) {
      if (this._isRunning) {
        return;
      }
      
      // If the current stats are closed due to previously stopping this gateway,
      // recreate them.
      if (getStatistics().isClosed()) {
        setStatistics(new GatewayStats(this._cache.getDistributedSystem(),
            getGatewayHubId(), getId() + ".rollup", null));
      }

      for (Gateway gateway : getGateways()) {
        ((GatewayImpl)gateway).start(this);
      }
      
      this._isRunning = true;
    }
  }

  public void stop() {
    synchronized (this.controlLock) {
      if (!this._isRunning) {
        return;
      }

      for (Gateway gateway : getGateways()) {
        gateway.stop();
      }

      this._isRunning = false;
    }
  }

  public boolean isConnected() {
    synchronized (this.controlLock) {
      boolean isConnected = false;
      if (!getGateways().isEmpty()) {
        isConnected = getGateways().get(0).isConnected();
      }
      return isConnected;
    }
  }

  public int getQueueSize() {
    int queueSize = 0;
    for (Gateway gateway : getGateways()) {
      queueSize += gateway.getQueueSize();
    }
    return queueSize;
  }

  public void pause() {
    synchronized (this.controlLock) {
      for (Gateway gateway : getGateways()) {
        gateway.pause();
      }
    }
  }

  public void resume() {
    synchronized (this.controlLock) {
      for (Gateway gateway : getGateways()) {
        gateway.resume();
      }
    }
  }

  public boolean isPaused() {
    synchronized (this.controlLock) {
      boolean isPaused = false;
      if (!getGateways().isEmpty()) {
        isPaused = getGateways().get(0).isPaused();
      }
      return isPaused;
    }
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
      .append("Parallel Gateway to ")
      .append(this._id)
      .toString();
  }

  @Override
  protected void setPrimary(boolean primary) {
    for (Gateway gateway : getGateways()) {
      ((GatewayImpl) gateway).setPrimary(primary);
    }
  }

  @Override
  public void emergencyClose() {
    for (Gateway gateway : getGateways()) {
      ((GatewayImpl) gateway).emergencyClose();
    }
  }

  @Override
  protected void becomePrimary() {
    for (Gateway gateway : getGateways()) {
      ((GatewayImpl) gateway).becomePrimary();
    }
  }

  @Override
  protected void distribute(EnumListenerEvent operation, EntryEventImpl event) {
    if (event.isOnPdxTypeRegion()) {
      // Distribute the PDXType event to all gateways
      for (int i=0; i<getGateways().size(); i++) {
        // Distribute the event to the gateway
        distribute(operation, event, i);
      }
    } else {
      // Get the appropriate index into the gateways
      int index = Math.abs(getHashCode(event) % getGateways().size());
      
      // Distribute the event to the gateway
      distribute(operation, event, index);
    }
  }
  
  private void distribute(EnumListenerEvent operation, EntryEventImpl event, int index) {    
    // Get the appropriate gateway
    AbstractGateway gateway = (AbstractGateway) getGateways().get(index);
    
    // Modify the event id if necessary (key ordering) to prevent events from
    // being dropped on the remote site. With key ordering, events from the same
    // thread can be put into different gateway queues, resulting in ordering
    // issues on the remote site. If events arrive out of order, they are
    // dropped. Modifying the event id prevents this.
    if (getOrderPolicy() == OrderPolicy.KEY || getOrderPolicy() == OrderPolicy.PARTITION) {
      // Create copy since the event id will be changed, otherwise the same
      // event will be changed for multiple gateways. Fix for bug 44471.
      EntryEventImpl clonedEvent = new EntryEventImpl(event);
      try {
      EventID originalEventId = clonedEvent.getEventId();
      long newThreadId = ThreadIdentifier.createFakeThreadIDForParallelGateway(index, originalEventId.getThreadID());
      //long newThreadId = (originalEventId.getThreadID() * PARALLEL_THREAD_BUFFER) + index;
      EventID newEventId = new EventID(originalEventId.getMembershipID(), newThreadId, originalEventId.getSequenceID());
      if (getLogger().fineEnabled()) {
        getLogger().fine(
            this + ": Generated event id for event with key=" + event.getKey()
                + ", index=" + index + ", original event id=" + originalEventId
                + ", new event id=" + newEventId);
      }
      clonedEvent.setEventId(newEventId);
      gateway.distribute(operation, clonedEvent);
      } finally {
        clonedEvent.release();
      }
    } else {
      // Distribute the event to the gateway
      gateway.distribute(operation, event);
    }
  }
  
  private int getHashCode(EntryEventImpl event) {
    // Get the hash code for the event based on the configured order policy
    int eventHashCode = 0;
    switch (getOrderPolicy()) {
    case KEY:
      // key ordering
      eventHashCode = event.getKey().hashCode();
      if (getLogger().fineEnabled()) {
        getLogger().fine(
            this + ": Generated key hashcode for event with key="
                + event.getKey() + ": " + eventHashCode);
      }
      break;
    case THREAD:
      // member id, thread id ordering
      // requires a lot of threads to achieve parallelism
      EventID eventId = event.getEventId();
      byte[] memberId = eventId.getMembershipID();
      long threadId = eventId.getThreadID();
      int memberIdHashCode = Arrays.hashCode(memberId);
      int threadIdHashCode = (int) (threadId ^ (threadId >>> 32));
      eventHashCode = memberIdHashCode + threadIdHashCode;
      if (getLogger().fineEnabled()) {
        getLogger().fine(
            this + ": Generated thread hashcode for event with key="
                + event.getKey() + ", memberId=" + Arrays.toString(memberId) + ", threadId="
                + threadId + ": " + eventHashCode);
      }
      break;
    case PARTITION:
      eventHashCode = 
        PartitionRegionHelper.isPartitionedRegion(event.getRegion())
          ? PartitionedRegionHelper.getHashKey(event) // Get the partition for the event
          : event.getKey().hashCode(); // Fall back to key ordering if the region is not partitioned
      if (getLogger().fineEnabled()) {
        getLogger().fine(
            this + ": Generated partition hashcode for event with key="
                + event.getKey() + ": " + eventHashCode);
      }
      break;
    }
    return eventHashCode;
  }

  public List<Gateway> getGateways() {
    return this.gateways;
  }
  
  private void initializeGateways(int concurrencyLevel) {
    // Create a GatewayImpl for each of the parallel threads
    for (int i = 0; i < concurrencyLevel; i++) {
      Gateway gateway = new GatewayImpl((GatewayHubImpl) getGatewayHub(),
          getId() + "." + i, true, getStatistics());
      getGateways().add(gateway);
    }
    StringBuilder builder = new StringBuilder();
    for (Iterator<Gateway> i = getGateways().iterator(); i.hasNext();) {
      Gateway gateway = i.next();
      builder.append(gateway.getId());
      if (i.hasNext()) {
        builder.append(", ");
      }
    }
    Object[] args = new Object[] {toString(), concurrencyLevel, builder.toString()};
    getLogger().info(LocalizedStrings.GatewayParallel_0_CREATED_1_GATEWAYS_2,
        args);
  }
}

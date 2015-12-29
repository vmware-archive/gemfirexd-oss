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
package com.gemstone.gemfire.cache.asyncqueue.internal;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.size.SingleObjectSizer;

public final class AsyncEventQueueImpl implements AsyncEventQueue {

  private final AbstractGatewaySender sender;
  private final AsyncEventListener asyncEventListener;
  private final String queueId;

  public static final String ASYNC_EVENT_QUEUE_PREFIX = "AsyncEventQueue_";

  public AsyncEventQueueImpl(AbstractGatewaySender sender,
      AsyncEventListener eventListener) {
    this.sender = sender;
    this.asyncEventListener = eventListener;
    this.queueId = getAsyncEventQueueIdFromSenderId(sender.getId());
  }

  @Override
  public String getId() {
    return this.queueId;
  }

  public final AbstractGatewaySender getSender() {
    return this.sender;
  }

  @Override
  public AsyncEventListener getAsyncEventListener() {
    return asyncEventListener;
  }

  @Override
  public int getBatchSize() {
    return sender.getBatchSize();
  }

  @Override
  public String getDiskStoreName() {
    return sender.getDiskStoreName();
  }
  
  @Override
  public int getBatchTimeInterval() {
    return sender.getBatchTimeInterval();
  }
  
  @Override
  public boolean isBatchConflationEnabled() {
    return sender.isBatchConflationEnabled();
  }

  @Override
  public int getMaximumQueueMemory() {
    return sender.getMaximumQueueMemory();
  }

  @Override
  public boolean isPersistent() {
    return sender.isPersistenceEnabled();
  }

  @Override
  public boolean isDiskSynchronous() {
    return sender.isDiskSynchronous();
  }
  
  @Override
  public int getDispatcherThreads() {
    return sender.getDispatcherThreads();
  }
  
  @Override
  public OrderPolicy getOrderPolicy() {
    return sender.getOrderPolicy();
  }
  
  @Override
  public boolean isPrimary() {
    return sender.isPrimary();
  }

  @Override
  public int size() {
    AbstractGatewaySenderEventProcessor ep = ((AbstractGatewaySender) sender).getEventProcessor();
    if (ep == null) return 0;
    RegionQueue queue = ep.getQueue();
    return queue.size();
  }

  public AsyncEventQueueStats getStatistics() {
     AbstractGatewaySender abstractSender =  (AbstractGatewaySender) this.sender;
     return ((AsyncEventQueueStats) abstractSender.getStatistics());
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AsyncEventQueue)) {
      return false;
    }
    AsyncEventQueueImpl asyncEventQueue = (AsyncEventQueueImpl) obj;
    if (asyncEventQueue.getId().equals(this.getId())) {
      return true;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  public static String getSenderIdFromAsyncEventQueueId(String asyncQueueId) {
    return ASYNC_EVENT_QUEUE_PREFIX + asyncQueueId;
  }

  public static String getAsyncEventQueueIdFromSenderId(String senderId) {
    if (!senderId.startsWith(ASYNC_EVENT_QUEUE_PREFIX)) {
      return senderId;
    }
    else {
      return senderId.substring(ASYNC_EVENT_QUEUE_PREFIX.length());
    }
  }

  public static boolean isAsyncEventQueue(String senderId) {
    return senderId.startsWith(ASYNC_EVENT_QUEUE_PREFIX);
  }

  @Override
  public boolean isParallel() {
    return sender.isParallel();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start() {
    this.sender.start();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    this.sender.stop();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() {
    this.sender.destroy();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void pause() {
    this.sender.pause();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resume() {
    this.sender.resume();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isRunning() {
    return this.sender.isRunning();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isPaused() {
    return this.sender.isPaused();
  }

  public void addGatewayEventFilter(GatewayEventFilter filter) {
    this.sender.addGatewayEventFilter(filter);
  }

  public void removeGatewayEventFilter(GatewayEventFilter filter) {
    this.sender.removeGatewayEventFilter(filter);
  }

   public boolean isBucketSorted() {
    // TODO Auto-generated method stub
    return false;
  }
   
   public long estimateMemoryFootprint(SingleObjectSizer sizer) {
     return sizer.sizeof(this) + this.sender.estimateMemoryFootprint(sizer);
   }
}

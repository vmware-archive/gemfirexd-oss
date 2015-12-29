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


import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderFactoryImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.AsyncEventQueueCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.management.ManagementException;

public final class AsyncEventQueueFactoryImpl implements AsyncEventQueueFactory {

  /**
   * Used internally to pass the attributes from this factory to the real
   * GatewaySender it is creating.
   */
  private final GatewaySenderAttributes attrs;

  private final Cache cache;
  
  private final LogWriter logger;
  
  /**
   * The default batchTimeInterval for AsyncEventQueue in milliseconds.
   */
  // [soubhik] continuing the default to be 5 ms in this branch
  // to avoid #46060 though in trunk code is moved back to 1000ms.
  public static final int DEFAULT_BATCH_TIME_INTERVAL = 5;

  public AsyncEventQueueFactoryImpl(Cache cache) {
    this.cache = cache;
    this.logger = this.cache.getLogger();
    this.attrs = new GatewaySenderAttributes();
    // set a different default for batchTimeInterval for AsyncEventQueue
    this.attrs.batchTimeInterval = DEFAULT_BATCH_TIME_INTERVAL;
  }

  @Override
  public AsyncEventQueueFactory setBatchSize(int size) {
    this.attrs.batchSize = size;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setPersistent(boolean isPersistent) {
    this.attrs.isPersistenceEnabled = isPersistent;
    return this;
  }
  
  @Override
  public AsyncEventQueueFactory setDiskStoreName(String name) {
    this.attrs.diskStoreName = name;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setMaximumQueueMemory(int memory) {
    this.attrs.maximumQueueMemory = memory;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setDiskSynchronous(boolean isSynchronous) {
    this.attrs.isDiskSynchronous = isSynchronous;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setBatchTimeInterval(int batchTimeInterval) {
    this.attrs.batchTimeInterval = batchTimeInterval;
    return this;
  }
  
  @Override
  public AsyncEventQueueFactory setBatchConflationEnabled(boolean isConflation) {
    this.attrs.isBatchConflationEnabled = isConflation;
    return this;
  }
  
  @Override
  public AsyncEventQueueFactory setDispatcherThreads(int numThreads) {
    this.attrs.dispatcherThreads = numThreads;
    return this;
  }
  
  @Override
  public AsyncEventQueueFactory setOrderPolicy(OrderPolicy policy) {
    this.attrs.policy = policy;
    return this;
  }

  public AsyncEventQueue create(String asyncQueueId, AsyncEventListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.AsyncEventQueue_ASYNC_EVENT_LISTENER_CANNOT_BE_NULL
              .toLocalizedString());
    }

    AsyncEventQueue asyncEventQueue = null;
    if (this.cache instanceof GemFireCacheImpl) {
      if (logger.fineEnabled()) {
        logger.fine("Creating GatewaySender that underlies the AsyncEventQueue");
      }
      GatewaySenderFactory senderFactory = this.cache
          .createGatewaySenderFactory();
      senderFactory.setMaximumQueueMemory(attrs.getMaximumQueueMemory());
      senderFactory.setBatchSize(attrs.getBatchSize());
      senderFactory.setBatchTimeInterval(attrs.getBatchTimeInterval());
      senderFactory.setBatchConflationEnabled(attrs.isBatchConflationEnabled());
      if (attrs.isPersistenceEnabled()) {
        senderFactory.setPersistenceEnabled(true);
      }
      senderFactory.setDiskStoreName(attrs.getDiskStoreName());
      senderFactory.setDiskSynchronous(attrs.isDiskSynchronous());
      senderFactory.setBatchConflationEnabled(attrs.isBatchConflationEnabled());
      senderFactory.setParallel(attrs.isParallel());
      senderFactory.setAlertThreshold(attrs.getAlertThreshold());
      senderFactory.setManualStart(attrs.isManualStart());
      senderFactory.setDiskSynchronous(attrs.isDiskSynchronous());
      for (GatewayEventFilter filter : attrs.getGatewayEventFilters()) {
        senderFactory.addGatewayEventFilter(filter);
      }

      senderFactory.setDispatcherThreads(attrs.getDispatcherThreads());
      senderFactory.setOrderPolicy(attrs.getOrderPolicy());

      // Type cast to GatewaySenderFactory implementation impl to add the async
      // event listener and set the isForInternalUse to true. These methods are
      // not exposed on GatewaySenderFactory
      GatewaySenderFactoryImpl factoryImpl =
          (GatewaySenderFactoryImpl)senderFactory;
      factoryImpl.setForInternalUse(true);
      factoryImpl.addAsyncEventListener(listener);
      factoryImpl.setBucketSorted(attrs.isBucketSorted());
      factoryImpl.setIsHDFSQueue(attrs.isHDFSQueue());
      // add member id to differentiate between this region and the redundant bucket 
      // region created for this queue. 
      AbstractGatewaySender sender = 
        factoryImpl.create(
            AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId(asyncQueueId));
      
      asyncEventQueue = new AsyncEventQueueImpl(sender, listener);
      try{
        ((GemFireCacheImpl) cache).addAsyncEventQueue(asyncEventQueue);
      } catch (ManagementException ex) {
        // javax.management.InstanceAlreadyExistsException can be thrown if we
        // create multiple GatewayReceivers on a node
      }
    }
    else if (this.cache instanceof CacheCreation) {
      asyncEventQueue = new AsyncEventQueueCreation(asyncQueueId, attrs,
          listener);
      ((CacheCreation)cache).addAsyncEventQueue(asyncEventQueue);
    }
    return asyncEventQueue;
  }

  public void configureAsyncEventQueue(AsyncEventQueue asyncQueueCreation) {
    this.attrs.batchSize = asyncQueueCreation.getBatchSize();
    this.attrs.batchTimeInterval = asyncQueueCreation.getBatchTimeInterval();
    this.attrs.isBatchConflationEnabled = asyncQueueCreation.isBatchConflationEnabled();
    this.attrs.isPersistenceEnabled = asyncQueueCreation.isPersistent();
    this.attrs.diskStoreName = asyncQueueCreation.getDiskStoreName();
    this.attrs.isDiskSynchronous = asyncQueueCreation.isDiskSynchronous();
    this.attrs.maximumQueueMemory = asyncQueueCreation.getMaximumQueueMemory();
    this.attrs.isParallel = asyncQueueCreation.isParallel();
    this.attrs.isBucketSorted = ((AsyncEventQueueCreation)asyncQueueCreation).isBucketSorted();
    this.attrs.isHDFSQueue = ((AsyncEventQueueCreation)asyncQueueCreation).isHDFSQueue();
    this.attrs.dispatcherThreads = asyncQueueCreation.getDispatcherThreads();
    this.attrs.policy = asyncQueueCreation.getOrderPolicy();
  }

  public AsyncEventQueueFactory setParallel(boolean isParallel) {
    this.attrs.isParallel = isParallel;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setAlertThreshold(int threshold) {
    this.attrs.alertThreshold = threshold;
    return this;
  }

  public AsyncEventQueueFactory setBucketSorted(boolean isbucketSorted) {
    this.attrs.isBucketSorted = isbucketSorted;
    return this;
  }
  public AsyncEventQueueFactory setIsHDFSQueue(boolean isHDFSQueue) {
    this.attrs.isHDFSQueue = isHDFSQueue;
    return this;
  }

  @Override
  public AsyncEventQueueFactory setManualStart(boolean start) {
    this.attrs.manualStart = start;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AsyncEventQueueFactory addGatewayEventFilter(GatewayEventFilter filter) {
    this.attrs.addGatewayEventFilter(filter);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AsyncEventQueueFactory removeGatewayEventFilter(
      GatewayEventFilter filter) {
    this.attrs.eventFilters.remove(filter);
    return this;
  }
}

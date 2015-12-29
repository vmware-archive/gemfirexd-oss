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

package com.pivotal.gemfirexd.internal.engine.access.operations;

import java.io.IOException;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.callbacks.AsyncEventListener;
import com.pivotal.gemfirexd.callbacks.DBSynchronizer;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.GfxdGatewayEventListener;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.WanProcedures;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * Encapsulates create operation for async event queues. In conjunction with
 * {@link AsyncQueueDropOperation} this helps implement atomicity for AsyncQueue
 * CREATE/DROP having proper undo in case CREATE fails on one or more nodes.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class AsyncQueueCreateOperation extends MemOperation {

  public final String id;
  public final boolean manualStart;
  public final boolean enableBatchConflation;
  public final int batchSize;
  public final int batchTimeInterval;
  public final boolean enablePersistence;
  public final boolean diskSync;
  public final String diskStoreName;
  public final int maximumQueueMemory;
  public final int alertThreshold;
  public final String className;
  public final String initParams;
  public final boolean isParallel;
  private final boolean enableBULKDMLStr;

  public AsyncQueueCreateOperation(String id, boolean manualStart,
      boolean enableBatchConflation, int batchSize, int batchTimeInterval,
      boolean enablePersistence,boolean diskSync, String diskStoreName, int maximumQueueMemory,
      int alertThreshold, boolean isParallel, String className,
      String initParams) {
    super(null);
    this.id = id;
    this.manualStart = manualStart;
    this.enableBatchConflation = enableBatchConflation;
    this.batchSize = batchSize;
    this.batchTimeInterval = batchTimeInterval;
    this.enablePersistence = enablePersistence;
    this.diskSync = diskSync;
    this.diskStoreName = diskStoreName;
    this.maximumQueueMemory = maximumQueueMemory;
    this.alertThreshold = alertThreshold;
    this.className = className;
    this.initParams = initParams;
    this.isParallel = isParallel;
    this.enableBULKDMLStr = true;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    try {
      // We need to start the listener object
      GemFireCacheImpl cache = Misc.getGemFireCache();
      AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
      factory.setManualStart(manualStart);
      factory.setBatchConflationEnabled(enableBatchConflation);
      factory.setBatchSize(batchSize);
      factory.setBatchTimeInterval(batchTimeInterval);
      factory.setPersistent(enablePersistence);
      factory.setDiskSynchronous(diskSync);
      factory.setParallel(isParallel);
      if (diskStoreName != null) {
        // check if disk store exists, else throw exception
        if (GemFireCacheImpl.getInstance().findDiskStore(diskStoreName) == null) {
          throw StandardException.newException(SQLState.DISK_STORE_ABSENT,
              diskStoreName);
        }
        factory.setDiskStoreName(diskStoreName);
      }
      else {
        // set default disk store
        factory.setDiskStoreName(GfxdConstants.GFXD_DEFAULT_DISKSTORE_NAME);
      }

      factory.setMaximumQueueMemory(maximumQueueMemory);
      factory.setAlertThreshold(alertThreshold);

      AsyncEventListener asyncListener;

      Class<?> c = ClassPathLoader.getLatest().forName(className);
      asyncListener = (AsyncEventListener)c.newInstance();
      if (!manualStart) {
        asyncListener.init(initParams);
      }
      com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener<Object, Object>
          evl = new GfxdGatewayEventListener(asyncListener, initParams);
      // In case of DBSynchronizer and remote gateway we want only PK
      // based events to get into the queue via GFE region event
      if (asyncListener instanceof DBSynchronizer) {
        factory.addGatewayEventFilter(
        // WanProcedures.getBULKDMLOptimizedDBSynchronizerFilter()
            WanProcedures.getSerialDBSynchronizerFilter(enableBULKDMLStr));
      }
      else {
        // It is an AsyncEventListener type which should not get events
        // related to temp tables or sys tables
        factory.addGatewayEventFilter(WanProcedures.getAsyncEventFilter());
      }
      factory.create(id, evl);
      if(!manualStart) {
        asyncListener.start();
      }
      
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
          ex.toString());
    }
  }

  @Override
  public MemOperation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    return new AsyncQueueDropOperation(this.id);
  }
}

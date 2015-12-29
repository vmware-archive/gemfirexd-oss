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
package com.pivotal.gemfirexd.internal.iapi.sql.dictionary;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.callbacks.AsyncEventListener;
import com.pivotal.gemfirexd.internal.catalog.DependableFinder;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.GfxdGatewayEventListener;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Dependent;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Provider;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdSysAsyncEventListenerRowFactory;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

public class GfxdAsyncEventListenerDescriptor extends TupleDescriptor implements Dependent {

  private final UUID uuid;

  private final String senderId;

  private String className;

  private final String serverGroup;

  private boolean manualStart = GatewaySender.DEFAULT_MANUAL_START;

  private boolean isBatchConflationEnabled = GatewaySender.DEFAULT_BATCH_CONFLATION;

  private int batchSize = GatewaySender.DEFAULT_BATCH_SIZE;

  private int batchTimeInterval =
      AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL;

  private boolean isPersistenceEnabled = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;

  private boolean diskSynchronous = false;//GatewaySender.DEFAULT_DISK_SYNCHRONOUS;
  
  private final String diskStoreName;

  private int maximumQueueMemory = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;

  private int alertThreshold = GatewaySender.DEFAULT_ALERT_THRESHOLD;

  private final boolean isStarted;

  private boolean isValid;

  private final String initParams;
  
  private ValueRow catalogueRow;

  private int invalidateAction;

  public GfxdAsyncEventListenerDescriptor(DataDictionary dd, UUID id,
      String senderId, String className, String serverGroup,
      Boolean manualStart, Boolean isBatchConflationEnabled, Integer batchSize,
      Integer batchTimeInterval, Boolean isPersistenceEnabled, Boolean diskSync,
      String diskStoreName, Integer maximumQueueMemory, Integer alertThreshold,
      Boolean isStarted, String initParams) {

    super(dd);
    this.uuid = id;
    this.senderId = senderId;
    this.className = className;
    this.serverGroup = serverGroup;
    this.manualStart = manualStart;
    this.batchSize = batchSize;
    this.batchTimeInterval = batchTimeInterval;
    this.isBatchConflationEnabled = isBatchConflationEnabled;
    this.isPersistenceEnabled = isPersistenceEnabled;
    this.diskSynchronous = diskSync;
    this.diskStoreName = diskStoreName;
    this.maximumQueueMemory = maximumQueueMemory;
    this.alertThreshold = alertThreshold;
    this.isStarted = isStarted;
    this.isValid = true;
    this.initParams = initParams;

  }

  public GfxdAsyncEventListenerDescriptor(String id, UUID uuid) {
    this.uuid = uuid;
    this.senderId = id;
    this.className = null;
    this.serverGroup = null;
    this.manualStart = false;
    this.batchSize = -1;
    this.batchTimeInterval = -1;
    this.isBatchConflationEnabled = false;
    this.isPersistenceEnabled = false;
    this.diskSynchronous = false;
    this.diskStoreName = null;
    this.maximumQueueMemory = 0;
    this.alertThreshold = 0;
    this.isStarted = false;
    this.isValid = true;
    this.initParams = null;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public int getMaximumQueueMemory() {
    return this.maximumQueueMemory;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  public int getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  public boolean isBatchConflationEnabled() {
    return this.isBatchConflationEnabled;
  }

  public boolean isPersistenceEnabled() {
    return this.isPersistenceEnabled;
  }

  public boolean isDiskSynchronous() {
    return this.diskSynchronous;
  }
  
  public int getAlertThreshold() {
    return this.alertThreshold;
  }

  public boolean isManualStart() {
    return this.manualStart;
  }

  public final String getSenderId() {
    return this.senderId;
  }

  public final String getClassName() {
    return this.className;
  }

  public String getServerGroup() {
    return this.serverGroup;
  }

  public String getInitParams() {
    return this.initParams;
  }
  
  public UUID getUUID() {
    return this.uuid;
  }

  public boolean isStarted() {
    return this.isStarted;
  }
  @Override
  public String getDescriptorType() 
  {
          return "AsyncEventListener";
  }
  
  /** @see TupleDescriptor#getDescriptorName */
  @Override
  public String getDescriptorName() { 
    return this.senderId; 
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("AsyncEventListener{");
    sb.append("id=" + getSenderId());
    sb.append(",listener=" + getClassName());
    sb.append("}");
    return sb.toString();
  }

  public DependableFinder getDependableFinder() {
    return getDependableFinder(StoredFormatIds.ASYNC_EVENT_LISTENER_DESCRIPTOR_FINDER_V01_ID);
  }

  public String getObjectName() {
    return null;
  }

  public UUID getObjectID() {
    return this.uuid;
  }

  public String getClassType() {
    return null;
  }

  public boolean isValid() {
    return this.isValid;
  }

  public void prepareToInvalidate(Provider p, int action,
      LanguageConnectionContext lcc) throws StandardException {
    LogWriter logger = Misc.getCacheLogWriter();
    logger.info("prepareToInvalidate called on: " + this + ", isValid: " + isValid);
    TabInfoImpl ti = null;
    DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
    TransactionController tc = lcc.getTransactionExecute();
    ti = dd.getNonCoreTI(DataDictionaryImpl.ASYNCEVENTLISTENERS_CATALOG_NUM);
    GfxdSysAsyncEventListenerRowFactory rf = (GfxdSysAsyncEventListenerRowFactory)ti
        .getCatalogRowFactory();
    ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
    keyRow.setColumn(1, new SQLVarchar(this.senderId));
    this.catalogueRow = (ValueRow)ti.getRow(tc, keyRow, 0);
    System.out.println(this.catalogueRow);
    this.invalidateAction = action;
    if (action == DependencyManager.DROP_JAR) {
      int rowsDeleted = ti.deleteRow(tc, keyRow, 0);
//      if (rowsDeleted == 0) {
//        // TODO: KN should we throw exception or silently ignore it
//        // as this is a drop jar execution
//      }
    }
  }

  public void makeInvalid(int action, LanguageConnectionContext lcc)
      throws StandardException {
    LogWriter logger = Misc.getCacheLogWriter();
    logger.info("makeInvalid called on: " + this + ", isValid: " + isValid);
    assert this.invalidateAction == action;
    if (!this.isValid) {
      refresh();
    }
    this.isValid = false;
  }

  private boolean refresh() {
    try {
      assert this.catalogueRow != null;
      String classNameStr = ((SQLVarchar)this.catalogueRow
          .getColumn(GfxdSysAsyncEventListenerRowFactory.LISTENER_CLASS))
          .getString();

      GemFireCacheImpl cache = Misc.getGemFireCache();
      AsyncEventQueue asyncQueue = cache.getAsyncEventQueue(this.senderId);

      AsyncEventListener asyncListener;
      Class<?> c = null;
      c = ClassPathLoader.getLatest().forName(classNameStr);
      asyncListener = (AsyncEventListener)c.newInstance();
      GfxdGatewayEventListener gfxdWrapperListener = (GfxdGatewayEventListener)asyncQueue
          .getAsyncEventListener();
      gfxdWrapperListener.refreshActualListener(asyncListener);
      return true;

    } catch (Throwable t) {
      return false;
    }
  }
}

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
package com.pivotal.gemfirexd.internal.impl.sql.catalog;

import java.sql.Types;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.uuid.UUIDFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.CatalogRowFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdAsyncEventListenerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;

public class GfxdSysAsyncEventListenerRowFactory extends CatalogRowFactory {

  public static final String TABLENAME_STRING = "ASYNCEVENTLISTENERS";

  public static final int ASYNCEEVENTLISTENERS_COLUMN_COUNT = 14;
  
  public static final int SENDER_ID = 1;

  public static final int LISTENER_CLASS = 2;
  
  public static final int SERVER_GROUPS = 3;
  
  public static final int MANUAL_START = 4;

  public static final int BATCH_CONFLATION = 5;

  public static final int BATCH_SIZE = 6;

  public static final int BATCH_TIME_INTERVAL = 7;

  public static final int IS_PERSISTENCE = 8;

  public static final int DISK_STORE_NAME = 9;

  public static final int MAX_QUEUE_MEMORY = 10;

  public static final int ALERT_THRESHOLD = 11;
  
  public static final int IS_STARTED = 12;
  
  public static final int INIT_PARAMS = 13;
  
  public static final int DISK_SYNCHRONOUS = 14;
  
  private static final int[][] indexColumnPositions = { { SENDER_ID } };

  private static final boolean[] uniqueness = null;

  private static final String[] uuids = {
      "l073400e-00b6-fdfc-71ce-000b0a763800",
      "l073400e-00b6-fbba-75d4-000b0a763800",
      "l073400e-00b6-00b9-bbde-000b0a763800"

  };

  GfxdSysAsyncEventListenerRowFactory(UUIDFactory uuidf, ExecutionFactory ef,
      DataValueFactory dvf) {
    super(uuidf, ef, dvf);
    initInfo(ASYNCEEVENTLISTENERS_COLUMN_COUNT, TABLENAME_STRING,
        indexColumnPositions, uniqueness, uuids);
  }

  public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
      throws StandardException {
    DataTypeDescriptor dtd;
    ExecRow row;
    DataValueDescriptor col;
    UUID uuid = null;

    String senderId = null;
    String className = null;
    String serverGroup = null;
    Boolean manualStart = null;
    Integer maximumQueueMemory = null;
    Integer batchSize = null;
    Integer batchTimeInterval = null;
    Boolean isBatchConflationEnabled = null;
    Boolean isPersistenceEnabled = null;
    Boolean diskSynchronous = null;
    String diskStoreName = null;
    Integer alertThreshold = null;
    Boolean isStarted = null;
    String initParams = null;
    
    if (td != null) {
      GfxdAsyncEventListenerDescriptor dsd = (GfxdAsyncEventListenerDescriptor)td;
      uuid = dsd.getUUID();
      senderId = dsd.getSenderId();
      className = dsd.getClassName();
      serverGroup = dsd.getServerGroup();
      manualStart = dsd.isManualStart();
      maximumQueueMemory = dsd.getMaximumQueueMemory();
      batchSize = dsd.getBatchSize();
      batchTimeInterval = dsd.getBatchTimeInterval();
      isBatchConflationEnabled = dsd.isBatchConflationEnabled();
      isPersistenceEnabled = dsd.isPersistenceEnabled();
      diskSynchronous = dsd.isDiskSynchronous();
      diskStoreName = dsd.getDiskStoreName();
      alertThreshold = dsd.getAlertThreshold();
      isStarted = dsd.isStarted();
      initParams = dsd.getInitParams();
    }

    /* Build the row to insert */
    row = getExecutionFactory().getValueRow(ASYNCEEVENTLISTENERS_COLUMN_COUNT);

    row.setColumn(SENDER_ID, new SQLVarchar(senderId));
    row.setColumn(LISTENER_CLASS, new SQLVarchar(className));
    row.setColumn(SERVER_GROUPS, new SQLVarchar(serverGroup));
    row.setColumn(MANUAL_START, new SQLBoolean(manualStart));
    row.setColumn(BATCH_CONFLATION, new SQLBoolean(isBatchConflationEnabled));
    row.setColumn(BATCH_SIZE, new SQLInteger(batchSize));
    row.setColumn(BATCH_TIME_INTERVAL, new SQLInteger(batchTimeInterval));
    row.setColumn(IS_PERSISTENCE, new SQLBoolean(isPersistenceEnabled));
    row.setColumn(DISK_SYNCHRONOUS, new SQLBoolean(diskSynchronous));
    row.setColumn(DISK_STORE_NAME, new SQLVarchar(diskStoreName));
    row.setColumn(MAX_QUEUE_MEMORY, new SQLInteger(maximumQueueMemory));
    row.setColumn(ALERT_THRESHOLD, new SQLInteger(alertThreshold));
    row.setColumn(IS_STARTED, new SQLBoolean(isStarted));
    row.setColumn(INIT_PARAMS, new SQLVarchar(initParams));
    return row;
  }

  public TupleDescriptor buildDescriptor(ExecRow row,
      TupleDescriptor parentDesc, DataDictionary dd) throws StandardException

  {
    if (SanityManager.DEBUG) {
      SanityManager.ASSERT(row.nColumns() == ASYNCEEVENTLISTENERS_COLUMN_COUNT,
          "Wrong number of columns for a ASYNCEEVENTLISTENERS row");
    }

    DataValueDescriptor col;
    UUID id;

    col = row.getColumn(SENDER_ID);
    String senderId = col.getString();

    id = getUUIDFactory().recreateUUID(senderId);

    col = row.getColumn(LISTENER_CLASS);
    String className  = col.getString();

    col = row.getColumn(SERVER_GROUPS);
    String serverGroups = col.getString();

    col = row.getColumn(MANUAL_START);
    Boolean manualStart = col.getBoolean();

    col = row.getColumn(BATCH_CONFLATION);
    Boolean batchConflation = col.getBoolean();
    
    col = row.getColumn(BATCH_SIZE);
    Integer batchSize = col.getInt();
    
    col = row.getColumn(BATCH_TIME_INTERVAL);
    Integer batchTimeInterval = col.getInt();
    
    col = row.getColumn(IS_PERSISTENCE);
    Boolean isPersistent = col.getBoolean();
    
    col = row.getColumn(DISK_SYNCHRONOUS);
    Boolean diskSynchronous = col.getBoolean();
    
    col = row.getColumn(DISK_STORE_NAME);
    String diskStoreName = col.getString();
    
    col = row.getColumn(MAX_QUEUE_MEMORY);
    Integer maxQueueMemory = col.getInt();

    col = row.getColumn(ALERT_THRESHOLD);
    Integer alertThreshold = col.getInt();

    col = row.getColumn(IS_STARTED);
    Boolean isStarted = col.getBoolean();

    col = row.getColumn(INIT_PARAMS);
    String initParams = col.getString();
    
    return new GfxdAsyncEventListenerDescriptor(dd, id, senderId, className,
        serverGroups, manualStart, batchConflation, batchSize,
        batchTimeInterval, isPersistent, diskSynchronous, diskStoreName, maxQueueMemory,
        alertThreshold, isStarted, initParams);
  }

  public SystemColumn[] buildColumnList() {
    return new SystemColumn[] {
        SystemColumnImpl.getIdentifierColumn("ID", false),
        SystemColumnImpl.getColumn("LISTENER_CLASS", Types.VARCHAR, false),
        SystemColumnImpl.getColumn("SERVER_GROUPS", Types.VARCHAR, false),
        SystemColumnImpl.getColumn("MANUAL_START", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("BATCH_CONFLATION", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("BATCH_SIZE", Types.INTEGER, false),
        SystemColumnImpl.getColumn("BATCH_TIME_INTERVAL", Types.INTEGER, false),
        SystemColumnImpl.getColumn("IS_PERSISTENCE", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("DISK_STORE_NAME", Types.VARCHAR, true),
        SystemColumnImpl.getColumn("MAX_QUEUE_MEMORY", Types.INTEGER, false),
        SystemColumnImpl.getColumn("ALERT_THRESHOLD", Types.INTEGER, false),
        SystemColumnImpl.getColumn("IS_STARTED", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("INIT_PARAMS", Types.VARCHAR, true),
        SystemColumnImpl.getColumn("DISK_SYNCHRONOUS", Types.BOOLEAN, false)
  };
  }

}

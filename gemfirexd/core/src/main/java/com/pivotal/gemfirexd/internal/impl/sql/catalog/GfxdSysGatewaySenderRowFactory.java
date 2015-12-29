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
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdGatewaySenderDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;

public class GfxdSysGatewaySenderRowFactory extends CatalogRowFactory {

  public static final String TABLENAME_STRING = "GATEWAYSENDERS";

  public static final int GATEWAYSENDERS_COLUMN_COUNT = 15;
  
  public static final int SENDER_ID = 1;

  public static final int REMOTE_DS_ID = 2;
  
  public static final int SERVER_GROUPS = 3;
  
  public static final int SOCKET_BUFFER_SIZE = 4;

  public static final int MANUAL_START = 5;

  public static final int SOCKET_READ_TIMEOUT = 6;

  public static final int BATCH_CONFLATION = 7;

  public static final int BATCH_SIZE = 8;

  public static final int BATCH_TIME_INTERVAL = 9;

  public static final int IS_PERSISTENCE = 10;

  public static final int DISK_STORE_NAME = 11;

  public static final int MAX_QUEUE_MEMORY = 12;

  public static final int ALERT_THRESHOLD = 13;
  
  public static final int IS_STARTED = 14;
  
  public static final int DISK_SYNCHRONOUS = 15;
  
  private static final int[][] indexColumnPositions = { { SENDER_ID } };

  private static final boolean[] uniqueness = null;

  private static final String[] uuids = {
      "g073400e-00b6-fdfc-71ce-000b0a763800",
      "g073400e-00b6-fbba-75d4-000b0a763800",
      "g073400e-00b6-00b9-bbde-000b0a763800"
  };

  GfxdSysGatewaySenderRowFactory(UUIDFactory uuidf, ExecutionFactory ef,
      DataValueFactory dvf) {
    super(uuidf, ef, dvf);
    initInfo(GATEWAYSENDERS_COLUMN_COUNT, TABLENAME_STRING,
        indexColumnPositions, uniqueness, uuids);
  }

  public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
      throws StandardException {
    DataTypeDescriptor dtd;
    ExecRow row;
    DataValueDescriptor col;
    UUID uuid = null;
    String senderId = null;
    Integer remoteDs = null;
    String serverGroup = null;
    Integer socketBufferSize = null;
    Boolean manualStart = null;
    Integer socketReadTimeout = null;
    Integer maximumQueueMemory = null;
    Integer batchSize = null;
    Integer batchTimeInterval = null;
    Boolean isBatchConflationEnabled = null;
    Boolean isPersistenceEnabled = null;
    Boolean diskSynchronous = null;
    String diskStoreName = null;
    Integer alertThreshold = null;
    Boolean isStarted = null;
    
    if (td != null) {
      GfxdGatewaySenderDescriptor dsd = (GfxdGatewaySenderDescriptor)td;
      uuid = dsd.getUUID();
      senderId = dsd.getSenderId();
      remoteDs = dsd.getRemoteDistributedSystemId();
      serverGroup = dsd.getServerGroup();
      socketBufferSize = dsd.getSocketBufferSize();
      manualStart = dsd.isManualStart();
      socketReadTimeout = dsd.getSocketReadTimeout();
      maximumQueueMemory = dsd.getMaximumQueueMemory();
      batchSize = dsd.getBatchSize();
      batchTimeInterval = dsd.getBatchTimeInterval();
      isBatchConflationEnabled = dsd.isBatchConflationEnabled();
      isPersistenceEnabled = dsd.isPersistenceEnabled();
      diskSynchronous = dsd.isDiskSynchronous();
      diskStoreName = dsd.getDiskStoreName();
      alertThreshold = dsd.getAlertThreshold();
      isStarted = dsd.isStarted();
    }

    /* Build the row to insert */
    row = getExecutionFactory().getValueRow(GATEWAYSENDERS_COLUMN_COUNT);
    
    row.setColumn(SENDER_ID, new SQLChar(senderId));
    row.setColumn(REMOTE_DS_ID, new SQLInteger(remoteDs));
    row.setColumn(SERVER_GROUPS, new SQLChar(serverGroup));
    row.setColumn(SOCKET_BUFFER_SIZE, new SQLInteger(socketBufferSize));
    row.setColumn(MANUAL_START, new SQLBoolean(manualStart));
    row.setColumn(SOCKET_READ_TIMEOUT, new SQLInteger(socketReadTimeout));
    row.setColumn(BATCH_CONFLATION, new SQLBoolean(isBatchConflationEnabled));
    row.setColumn(BATCH_SIZE, new SQLInteger(batchSize));
    row.setColumn(BATCH_TIME_INTERVAL, new SQLInteger(batchTimeInterval));
    row.setColumn(IS_PERSISTENCE, new SQLBoolean(isPersistenceEnabled));
    row.setColumn(DISK_STORE_NAME, new SQLChar(diskStoreName));
    row.setColumn(MAX_QUEUE_MEMORY, new SQLInteger(maximumQueueMemory));
    row.setColumn(ALERT_THRESHOLD, new SQLInteger(alertThreshold));
    row.setColumn(IS_STARTED, new SQLBoolean(isStarted));
    row.setColumn(DISK_SYNCHRONOUS, new SQLBoolean(diskSynchronous));
    return row;
  }

  public TupleDescriptor buildDescriptor(ExecRow row,
      TupleDescriptor parentDesc, DataDictionary dd) throws StandardException

  {
    if (SanityManager.DEBUG) {
      SanityManager.ASSERT(row.nColumns() == GATEWAYSENDERS_COLUMN_COUNT,
          "Wrong number of columns for a GATEWAYSENDERS row");
    }

    DataValueDescriptor col;
    UUID id;

    col = row.getColumn(SENDER_ID);
    String senderId = col.getString();

    id = getUUIDFactory().recreateUUID(senderId);

    col = row.getColumn(REMOTE_DS_ID);
    Integer remoteDsId = col.getInt();

    col = row.getColumn(SERVER_GROUPS);
    String serverGroups = col.getString();

    col = row.getColumn(SOCKET_BUFFER_SIZE);
    Integer socketBufferSize = col.getInt();

    col = row.getColumn(MANUAL_START);
    Boolean manualStart = col.getBoolean();

    col = row.getColumn(SOCKET_READ_TIMEOUT);
    Integer socketReadTimeout = col.getInt();

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
    
    
    return new GfxdGatewaySenderDescriptor(dd, id, senderId, remoteDsId,
        serverGroups, socketBufferSize, manualStart, socketReadTimeout,
        batchConflation, batchSize, batchTimeInterval, isPersistent, diskSynchronous,
        diskStoreName, maxQueueMemory, alertThreshold, isStarted);
  }

  public SystemColumn[] buildColumnList() {
    return new SystemColumn[] {
        SystemColumnImpl.getIdentifierColumn("SENDER_ID", false),
        SystemColumnImpl.getColumn("REMOTE_DS_ID", Types.INTEGER, false),
        SystemColumnImpl.getColumn("SERVER_GROUPS", Types.VARCHAR, false),
        SystemColumnImpl.getColumn("SOCKET_BUFFER_SIZE", Types.INTEGER, false),
        SystemColumnImpl.getColumn("MANUAL_START", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("SOCKET_READ_TIMEOUT", Types.INTEGER, false),
        SystemColumnImpl.getColumn("BATCH_CONFLATION", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("BATCH_SIZE", Types.INTEGER, false),
        SystemColumnImpl.getColumn("BATCH_TIME_INTERVAL", Types.INTEGER, false),
        SystemColumnImpl.getColumn("IS_PERSISTENCE", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("DISK_STORE_NAME", Types.VARCHAR, true),
        SystemColumnImpl.getColumn("MAX_QUEUE_MEMORY", Types.INTEGER, false),
        SystemColumnImpl.getColumn("ALERT_THRESHOLD", Types.INTEGER, false),
        SystemColumnImpl.getColumn("IS_STARTED", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("DISK_SYNCHRONOUS", Types.BOOLEAN, false)};
  }

}

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
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdDiskStoreDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLLongint;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;

/**
 * 
 * @author Asif
 * 
 */
public class GfxdSYSDISKSTORESRowFactory extends CatalogRowFactory
{

  public static final String TABLENAME_STRING = "SYSDISKSTORES";

  public static final int SYSDISKSTORES_COLUMN_COUNT = 9;

  public static final int SYSDISKSTORES_NAME = 1;

  public static final int SYSDISKSTORES_MAXLOGSIZE = 2;

  public static final int SYSDISKSTORES_AUTOCOMPACT = 3;

  public static final int SYSDISKSTORES_ALLOWFORCECOMPACTION = 4;

  public static final int SYSDISKSTORES_COMPACTIONTHRESHOLD = 5;

  public static final int SYSDISKSTORES_TIMEINTERVAL = 6;

  public static final int SYSDISKSTORES_WRITEBUFFERSIZE = 7;

  public static final int SYSDISKSTORES_QUEUESIZE = 8;

  public static final int SYSDISKSTORES_DIR_PATH_SIZE = 9;

  private static final int[][] indexColumnPositions = { { SYSDISKSTORES_NAME } };

  // if you add a non-unique index allocate this array.
  private static final boolean[] uniqueness = null;

  private static final String[] uuids = {
      "a073400e-00b6-fdfc-71ce-000b0a763800" // catalog UUID
      , "a073400e-00b6-fbba-75d4-000b0a763800" // heap UUID
      , "a073400e-00b6-00b9-bbde-000b0a763800" // SYSDISKSTORES_INDEX1

  };

  // ///////////////////////////////////////////////////////////////////////////
  //
  // CONSTRUCTORS
  //
  // ///////////////////////////////////////////////////////////////////////////

  GfxdSYSDISKSTORESRowFactory(UUIDFactory uuidf, ExecutionFactory ef,
      DataValueFactory dvf) {
    super(uuidf, ef, dvf);
    initInfo(SYSDISKSTORES_COLUMN_COUNT, TABLENAME_STRING,
        indexColumnPositions, uniqueness, uuids);
  }

  public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
      throws StandardException
  {
    ExecRow row;
    UUID uuid = null;
    String diskStoreName = null;
    long maxLogSize = -1;
    String autoCompact = null;
    String allowForceCompaction = null;
    int compactionThreshold = -1;
    long timeInterval = -1;
    int writeBufferSize = -1;
    int queueSize = -1;
    String dirPathSize = null;

    if (td != null) {
      GfxdDiskStoreDescriptor dsd = (GfxdDiskStoreDescriptor)td;
      diskStoreName = dsd.getDiskStoreName();
      uuid = dsd.getUUID();
      maxLogSize = dsd.getMaxLogSize();
      autoCompact = dsd.getAutocompact();
      allowForceCompaction = dsd.getAllowForceCompaction();
      compactionThreshold = dsd.getCompactionThreshold();
      timeInterval = dsd.getTimeInterval();
      writeBufferSize = dsd.getWriteBufferSize();
      queueSize = dsd.getQueueSize();
      dirPathSize = dsd.getDirPathAndSize();

    }

    /* Build the row to insert */
    row = getExecutionFactory().getValueRow(SYSDISKSTORES_COLUMN_COUNT);

    /* 1st column is diskstorename */
    row.setColumn(1, new SQLChar(diskStoreName));

    /* 2nd column is logsize */
    row.setColumn(2, new SQLLongint(maxLogSize));

    /* 3rd column is autocompact */
    row.setColumn(3, new SQLVarchar(autoCompact));

    /* 4th column is allowforcecompaction */
    row.setColumn(4, new SQLVarchar(allowForceCompaction));

    /* 5th column is compactionthreshold */
    row.setColumn(5, new SQLInteger(compactionThreshold));

    /* 6th column is TimeInterval */
    row.setColumn(6, new SQLLongint(timeInterval));

    /* 7th column is writeBufferSize */
    row.setColumn(7, new SQLInteger(writeBufferSize));

    /* 8th column is QueueSize */
    row.setColumn(8, new SQLInteger(queueSize));

    /* 9th column is Dircetion paths and sizes */
    row.setColumn(9, new SQLVarchar(dirPathSize));
    return row;
  }

  public TupleDescriptor buildDescriptor(ExecRow row,
      TupleDescriptor parentDesc, DataDictionary dd) throws StandardException

  {
    if (SanityManager.DEBUG) {
      SanityManager.ASSERT(row.nColumns() == SYSDISKSTORES_COLUMN_COUNT,
          "Wrong number of columns for a SYSDISKSTORES row");
    }

    DataValueDescriptor col;
    UUID id;

    UUIDFactory uuidFactory = getUUIDFactory();

    col = row.getColumn(SYSDISKSTORES_NAME);
    String diskStoreName = col.getString();

    id = getUUIDFactory().recreateUUID(diskStoreName);

    col = row.getColumn(SYSDISKSTORES_MAXLOGSIZE);
    int maxLogSize = col.getInt();

    col = row.getColumn(SYSDISKSTORES_AUTOCOMPACT);
    String autoCompact = col.getString();

    col = row.getColumn(SYSDISKSTORES_ALLOWFORCECOMPACTION);
    String allowForceCompact = col.getString();

    col = row.getColumn(SYSDISKSTORES_COMPACTIONTHRESHOLD);
    int comapctionThreshold = col.getInt();

    col = row.getColumn(SYSDISKSTORES_TIMEINTERVAL);
    int timeInterval = col.getInt();

    col = row.getColumn(SYSDISKSTORES_WRITEBUFFERSIZE);
    int writeBufferSize = col.getInt();

    col = row.getColumn(SYSDISKSTORES_QUEUESIZE);
    int queueSize = col.getInt();

    col = row.getColumn(SYSDISKSTORES_DIR_PATH_SIZE);
    String dirPathAndSize = col.getString();
    return new GfxdDiskStoreDescriptor(dd, id, diskStoreName, maxLogSize,
        autoCompact, allowForceCompact, comapctionThreshold, timeInterval,
        writeBufferSize, queueSize, dirPathAndSize);
  }

  /**
   * Builds a list of columns suitable for creating this Catalog.
   * 
   * 
   * @return array of SystemColumn suitable for making this catalog.
   */
  public SystemColumn[] buildColumnList()
  {
    return new SystemColumn[] {
        SystemColumnImpl.getIdentifierColumn("NAME", false),
//        SystemColumnImpl.getIndicatorColumn("LOCKGRANULARITY"),
//        SystemColumnImpl.getIdentifierColumn("SERVERGROUPS", false),
        SystemColumnImpl.getColumn("MAXLOGSIZE", Types.BIGINT, false),
        SystemColumnImpl.getColumn("AUTOCOMPACT", Types.VARCHAR, false,6),
        SystemColumnImpl.getColumn("ALLOWFORCECOMPACTION", Types.VARCHAR, false,6),
        SystemColumnImpl.getColumn("COMPACTIONTHRESHOLD", Types.INTEGER, false,3),
        SystemColumnImpl.getColumn("TIMEINTERVAL", Types.BIGINT, false),
        SystemColumnImpl.getColumn("WRITEBUFFERSIZE", Types.INTEGER, false),
        SystemColumnImpl.getColumn("QUEUESIZE", Types.INTEGER, false),
        SystemColumnImpl.getColumn("DIR_PATH_SIZE", Types.VARCHAR, false)};
  }

}

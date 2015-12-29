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
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdHDFSStoreDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.iapi.types.SQLDouble;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLLongint;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;

/**
 * 
 * @author jianxiachen
 *
 */

public class GfxdSYSHDFSSTORESRowFactory extends CatalogRowFactory
{

  public static final String TABLENAME_STRING = "SYSHDFSSTORES";

  public static final int SYSHDFSSTORES_COLUMN_COUNT = 23;

  public static final int SYSHDFSSTORES_NAME = 1;

  public static final int SYSHDFSSTORES_NAMENODE = 2;

  public static final int SYSHDFSSTORES_HOMEDIR = 3;
  
  public static final int SYSHDFSSTORES_MAXQUEUEMEMORY = 4;
  
  public static final int SYSHDFSSTORES_BATCHSIZE = 5;
  
  public static final int SYSHDFSSTORES_BATCHINTERVAL = 6;
  
  public static final int SYSHDFSSTORES_ISPERSISTENT = 7;
  
  public static final int SYSHDFSSTORES_DISKSYNC = 8;
  
  public static final int SYSHDFSSTORES_DISKSTORE = 9;
  
  public static final int SYSHDFSSTORES_AUTOCOMPACT = 10;
  
  public static final int SYSHDFSSTORES_AUTOMAJORCOMPACT = 11;
  
  public static final int SYSHDFSSTORES_MAXINPUTFILESIZE = 12;
  
  public static final int SYSHDFSSTORES_MININPUTFILECOUNT = 13;
  
  public static final int SYSHDFSSTORES_MAXINPUTFILECOUNT = 14;
  
  public static final int SYSHDFSSTORES_MAXCONCURRENCY = 15;
  
  public static final int SYSHDFSSTORES_MAJORCOMPACTIONINTERVAL = 16;
  
  public static final int SYSHDFSSTORES_MAJORCOMPACTIONCONCURRENCY = 17;
  
  public static final int SYSHDFSSTORES_HDFSCLIENTCONFIGFILE = 18;
  
  public static final int SYSHDFSSTORES_BLOCKCACHESIZE = 19;
  
  public static final int SYSHDFSSTORES_MAXFILESIZEWRITEONLYTABLE = 20;
  
  public static final int SYSHDFSSTORES_FILEROLLOVERINTERVALWRITEONLYTABLE = 21;
  
  public static final int SYSHDFSSTORES_PURGEINTERVAL = 22;
  
  public static final int SYSHDFSSTORES_DISPATCHERTHREADS = 23;

  private static final int[][] indexColumnPositions = { { SYSHDFSSTORES_NAME } };

  // if you add a non-unique index allocate this array.
  private static final boolean[] uniqueness = null;

  private static final String[] uuids = {
      "t073400e-00b6-fdfc-71ce-000b0a763800" // catalog UUID
      , "t073400e-00b6-fbba-75d4-000b0a763800" // heap UUID
      , "t073400e-00b6-00b9-bbde-000b0a763800" // SYSHDFSSTORES_INDEX1

  };

  // ///////////////////////////////////////////////////////////////////////////
  //
  // CONSTRUCTORS
  //
  // ///////////////////////////////////////////////////////////////////////////

  GfxdSYSHDFSSTORESRowFactory(UUIDFactory uuidf, ExecutionFactory ef,
      DataValueFactory dvf) {
    super(uuidf, ef, dvf);
    initInfo(SYSHDFSSTORES_COLUMN_COUNT, TABLENAME_STRING,
        indexColumnPositions, uniqueness, uuids);
  }

  public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
      throws StandardException
  {
    ExecRow row;
    UUID uuid = null;
    String hdfsStoreName = null;
    String nameNode = null;
    String homeDir = null;
    float blockCacheSize = -1;
    String hdfsClientConfigFile = null;
    int maxQueueMemory = -1;
    int batchSize = -1;
    int batchInterval = -1;
    boolean isPersistent = false;
    boolean isDiskSync = false;
    String diskStore = null;
    boolean autoCompact = false;
    boolean autoMajorCompact = false;
    int maxInputFileSize = -1;
    int minInputFileCount = -1;
    int maxInputFileCount = -1;
    int maxConcurrency = -1;
    int majorCompactionInterval = -1;
    int majorCompactionConcurrency = -1;
    int maxfilesizeWriteOnly = -1;
    int timeForRolloverWriteOnly = -1;
    int purgeInterval = -1;
    int dispatcherThreads = 1;
    
    if (td != null) {
      GfxdHDFSStoreDescriptor hsd = (GfxdHDFSStoreDescriptor) td;
      hdfsStoreName = hsd.getHDFSStoreName();
      nameNode = hsd.getNameNode();
      homeDir = hsd.getHomeDir();
      hdfsClientConfigFile = hsd.getHDFSClientConfigFile();
      blockCacheSize = hsd.getBlockCacheSize();
      maxQueueMemory = hsd.getMaxQueueMemory();
      batchSize = hsd.getBatchSize();
      batchInterval = hsd.getBatchInterval();
      isPersistent = hsd.isPersistenceEnabled();
      isDiskSync = hsd.isDiskSynchronous();
      diskStore = hsd.getDiskStoreName();
      autoCompact = hsd.isAutoCompact();
      autoMajorCompact = hsd.isAutoMajorCompact();
      maxInputFileSize = hsd.getMaxInputFileSize();
      minInputFileCount = hsd.getMinInputFileCount();
      maxInputFileCount = hsd.getMaxInputFileCount();
      maxConcurrency = hsd.getMaxConcurrency();
      majorCompactionInterval = hsd.getMajorCompactionInterval();
      majorCompactionConcurrency = hsd.getMajorCompactionConcurrency();
      maxfilesizeWriteOnly = hsd.getMaxFileSizeWriteOnly();
      timeForRolloverWriteOnly = hsd.getFileRolloverInterval();
      purgeInterval = hsd.getPurgeInterval();
      uuid = hsd.getUUID();      
      dispatcherThreads = hsd.getDispatcherThreads();
    }

    /* Build the row to insert */
    row = getExecutionFactory().getValueRow(SYSHDFSSTORES_COLUMN_COUNT);
    row.setColumn(1, new SQLChar(hdfsStoreName));
    row.setColumn(2, new SQLVarchar(nameNode));
    row.setColumn(3, new SQLVarchar(homeDir));
    row.setColumn(4, new SQLInteger(maxQueueMemory));
    row.setColumn(5, new SQLInteger(batchSize));
    row.setColumn(6, new SQLInteger(batchInterval));
    row.setColumn(7, new SQLBoolean(isPersistent));
    row.setColumn(8, new SQLBoolean(isDiskSync));  
    row.setColumn(9, new SQLVarchar(diskStore));    
    row.setColumn(10, new SQLBoolean(autoCompact));
    row.setColumn(11, new SQLBoolean(autoMajorCompact));
    row.setColumn(12, new SQLInteger(maxInputFileSize));
    row.setColumn(13, new SQLInteger(minInputFileCount));
    row.setColumn(14, new SQLInteger(maxInputFileCount));
    row.setColumn(15, new SQLInteger(maxConcurrency));
    row.setColumn(16, new SQLInteger(majorCompactionInterval));
    row.setColumn(17, new SQLInteger(majorCompactionConcurrency));    
    row.setColumn(18, new SQLVarchar(hdfsClientConfigFile));
    row.setColumn(19, new SQLDouble(blockCacheSize));
    row.setColumn(20, new SQLLongint(maxfilesizeWriteOnly));
    row.setColumn(21, new SQLLongint(timeForRolloverWriteOnly));
    row.setColumn(22, new SQLInteger(purgeInterval));
    row.setColumn(23, new SQLInteger(dispatcherThreads));
    return row;
  }

  public TupleDescriptor buildDescriptor(ExecRow row,
      TupleDescriptor parentDesc, DataDictionary dd) throws StandardException

  {
    if (SanityManager.DEBUG) {
      SanityManager.ASSERT(row.nColumns() == SYSHDFSSTORES_COLUMN_COUNT,
          "Wrong number of columns for a SYSDISKSTORES row");
    }

    DataValueDescriptor col;
    UUID id;

    col = row.getColumn(SYSHDFSSTORES_NAME);
    String hdfsStoreName = col.getString();

    id = getUUIDFactory().recreateUUID(hdfsStoreName);

    col = row.getColumn(SYSHDFSSTORES_NAMENODE);
    String nameNode = col.getString();

    col = row.getColumn(SYSHDFSSTORES_HOMEDIR);
    String homeDir = col.getString();
    
    col = row.getColumn(SYSHDFSSTORES_MAXQUEUEMEMORY);
    int maxQueueMemory = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_BATCHSIZE);
    int batchSize = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_BATCHINTERVAL);
    int batchInterval = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_ISPERSISTENT);
    boolean isPersistent = col.getBoolean();
    
    col = row.getColumn(SYSHDFSSTORES_DISKSYNC);
    boolean isDiskSync = col.getBoolean();
    
    col = row.getColumn(SYSHDFSSTORES_DISKSTORE);
    String diskStore = col.getString();
    
    col = row.getColumn(SYSHDFSSTORES_AUTOCOMPACT);
    boolean autoCompact = col.getBoolean();
    
    col = row.getColumn(SYSHDFSSTORES_AUTOMAJORCOMPACT);
    boolean autoMajorCompact = col.getBoolean();
    
    col = row.getColumn(SYSHDFSSTORES_MAXINPUTFILESIZE);
    int maxInputFileSize = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_MININPUTFILECOUNT);
    int minInputFileCount = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_MAXINPUTFILECOUNT);
    int maxInputFileCount = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_MAXCONCURRENCY);
    int maxConcurrency = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_MAJORCOMPACTIONINTERVAL);
    int majorCompactionInterval = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_MAJORCOMPACTIONCONCURRENCY);
    int majorCompactionConcurrency = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_HDFSCLIENTCONFIGFILE);
    String hdfsClientConfigFile = col.getString();
    
    col = row.getColumn(SYSHDFSSTORES_BLOCKCACHESIZE);
    float blockCacheSize = col.getFloat();
    
    col = row.getColumn(SYSHDFSSTORES_MAXFILESIZEWRITEONLYTABLE);
    int maxFileSizeWriteOnly = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_FILEROLLOVERINTERVALWRITEONLYTABLE);
    int timeForRollOver = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_PURGEINTERVAL);
    int purgeInterval = col.getInt();
    
    col = row.getColumn(SYSHDFSSTORES_DISPATCHERTHREADS);
    int dispatcherThreads = col.getInt();

    return new GfxdHDFSStoreDescriptor(dd, id, hdfsStoreName, nameNode,
        homeDir, maxQueueMemory, batchSize, batchInterval, isPersistent, isDiskSync, diskStore, autoCompact, autoMajorCompact,
        maxInputFileSize, minInputFileCount, maxInputFileCount, maxConcurrency, majorCompactionInterval, majorCompactionConcurrency,
        hdfsClientConfigFile, blockCacheSize, maxFileSizeWriteOnly, timeForRollOver, purgeInterval, dispatcherThreads);
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
        SystemColumnImpl.getColumn("NAMENODE", Types.VARCHAR, false),
        SystemColumnImpl.getColumn("HOMEDIR", Types.VARCHAR, false),
        SystemColumnImpl.getColumn("MAXQUEUEMEMORY", Types.INTEGER, false),
        SystemColumnImpl.getColumn("BATCHSIZE", Types.INTEGER, false),
        SystemColumnImpl.getColumn("BATCHTIMEINTERVALMILLIS", Types.INTEGER, false),
        SystemColumnImpl.getColumn("QUEUEPERSISTENT", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("DISKSYNCHRONOUS", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("DISKSTORENAME", Types.VARCHAR, true),
        SystemColumnImpl.getColumn("MINORCOMPACT", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("MAJORCOMPACT", Types.BOOLEAN, false),
        SystemColumnImpl.getColumn("MAXINPUTFILESIZE", Types.INTEGER, false),
        SystemColumnImpl.getColumn("MININPUTFILECOUNT", Types.INTEGER, false),
        SystemColumnImpl.getColumn("MAXINPUTFILECOUNT", Types.INTEGER, false),
        SystemColumnImpl.getColumn("MINORCOMPACTIONTHREADS", Types.INTEGER, false),
        SystemColumnImpl.getColumn("MAJORCOMPACTIONINTERVALMINS", Types.INTEGER, false),
        SystemColumnImpl.getColumn("MAJORCOMPACTIONTHREADS", Types.INTEGER, false),
        SystemColumnImpl.getColumn("CLIENTCONFIGFILE", Types.VARCHAR, true),
        SystemColumnImpl.getColumn("BLOCKCACHESIZE", Types.FLOAT, false),
        SystemColumnImpl.getColumn("MAXWRITEONLYFILESIZE", Types.INTEGER, false),        
        SystemColumnImpl.getColumn("WRITEONLYFILEROLLOVERINTERVALSECS", Types.INTEGER, false),
        SystemColumnImpl.getColumn("PURGEINTERVALMINS", Types.INTEGER, false),
        SystemColumnImpl.getColumn("DISPATCHERTHREADS", Types.INTEGER, false)
    };
  }

}

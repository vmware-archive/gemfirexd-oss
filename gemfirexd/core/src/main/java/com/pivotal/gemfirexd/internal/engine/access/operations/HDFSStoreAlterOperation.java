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

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSCompactionConfigMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSEventQueueAttributesMutator;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreMutatorImpl;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;
import com.pivotal.gemfirexd.internal.iapi.types.SQLLongint;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdSYSHDFSSTORESRowFactory;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * @author Ashvin
 * @author hemantb
 */
public class HDFSStoreAlterOperation extends MemOperation {
  private final HDFSStoreMutator mutator;
  private final String storeName;
  private HDFSStore oldStoreConfig;

  public HDFSStoreAlterOperation(HDFSStoreMutator hsm, String storeName) {
    super(null);
    this.mutator = hsm;
    this.storeName = storeName;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final GemFireTransaction tc = (GemFireTransaction) xact;
    final LanguageConnectionContext lcc = tc.getLanguageConnectionContext();
    DataDictionary dd = lcc.getDataDictionary();
    
    // If mutator is null, no need to do anything. This can happen 
    // in case of rollback. See HDFSStoreAlterOperation#generateUndo
    if (this.mutator == null)
      return;
    // Check to see if this hdfsstore already exists in the cache
    // TODO: HDFS: server groups support in ALTER HDFSSTORE
    HDFSStoreImpl hsi = Misc.getGemFireCache().findHDFSStore(storeName);
    
    if (ServerGroupUtils.isDataStore() && hsi == null) {
      throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST,
          "HDFSSTORE", storeName);
    }
    
    // If the named store does not exist in DataDictionary of 
    // accessor node, no need to send the DDL to other nodes  
    if (!ServerGroupUtils.isDataStore()) {
      int mode = dd.startReading(lcc);
      try {
        ExecIndexRow keyRow = dd.getExecutionFactory()
        .getIndexableRow(1);
        keyRow.setColumn(1, new SQLVarchar(storeName));
        TabInfoImpl ti = ((DataDictionaryImpl)dd).getNonCoreTI(DataDictionaryImpl.SYSHDFSSTORES_CATALOG_NUM);
        ExecRow oldRow = ti.getRow(tc, keyRow, 0);
        if (oldRow == null) {
          throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST,
              "HDFSSTORE", storeName);
        }
      } finally {
        dd.doneReading(mode, lcc);
      }
    }
    
    dd.startWriting(lcc);
    // update HDFS Store 
    try {
      if (ServerGroupUtils.isDataStore()) {
        // update HDFSSTore only if it is a datastore 
        oldStoreConfig = hsi.alter(mutator);
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
            "AlterHDFSStoreNode: updated hdfsstore configuration for "
                + storeName);
      }  
      
      // Fetch the DD row to update it
      ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
      keyRow.setColumn(1, new SQLVarchar(storeName));
      TabInfoImpl ti = ((DataDictionaryImpl)dd).getNonCoreTI(DataDictionaryImpl.SYSHDFSSTORES_CATALOG_NUM);
      ExecRow curRow= ti.getRow((TransactionController)tc, keyRow, 0);
      
      if (curRow == null) {
        throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST,
            "HDFSSTORE", storeName);
      }
      boolean[] bArray = new boolean[1];
      
      // Mutate the DD row      
      ExecRow mutatedRow = mutateRow(curRow);
      
      // Update the DD row
      ti.updateRow(keyRow, mutatedRow, 0,
          bArray,
          null,
          tc);
      
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "AlterHDFSStoreNode: updated hdfsstore configuration for "
              + storeName + " in SYS table");
    
      
    } catch (Throwable t) {
      throw StandardException.newException(SQLState.HDFS_ERROR, t,
          t.getMessage());
    }
  }

  /*
   * Mutates the DD row with new values.  
   */
  public ExecRow mutateRow(ExecRow currRow) {
    ExecRow mutableRow = currRow.getClone();
    
    if (mutator.getFileRolloverInterval() >= 0) {
      logAttrMutation("fileRolloverInterval", mutator.getFileRolloverInterval());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_FILEROLLOVERINTERVALWRITEONLYTABLE,
          new SQLLongint((Integer)mutator.getFileRolloverInterval()));
    }
    if (mutator.getMaxFileSize() >= 0) {
      logAttrMutation("MaxFileSize", mutator.getFileRolloverInterval());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_MAXFILESIZEWRITEONLYTABLE,
          new SQLLongint((Integer)mutator.getMaxFileSize()));
    }

    HDFSEventQueueAttributesMutator qMutator = mutator.getHDFSEventQueueAttributesMutator();

    if (qMutator.getBatchSizeMB() >= 0) {
      logAttrMutation("batchSizeMB", mutator.getFileRolloverInterval());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_BATCHSIZE,
          new SQLLongint(qMutator.getBatchSizeMB()));
    }
    if (qMutator.getBatchTimeInterval() >= 0) {
      logAttrMutation("batchTimeInterval", mutator.getFileRolloverInterval());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_BATCHINTERVAL,
          new SQLLongint(qMutator.getBatchTimeInterval()));
    }
    HDFSCompactionConfigMutator compactionMutator = mutator.getCompactionConfigMutator();
    
    if (compactionMutator.getMaxInputFileCount() >= 0) {
      logAttrMutation("maxInputFileCount", compactionMutator.getMaxInputFileCount());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_MAXINPUTFILECOUNT,
          new SQLLongint(compactionMutator.getMaxInputFileCount()));
    }
    if (compactionMutator.getMaxInputFileSizeMB() >= 0) {
      logAttrMutation("MaxInputFileSizeMB", compactionMutator.getMaxInputFileSizeMB());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_MAXINPUTFILESIZE,
          new SQLLongint(compactionMutator.getMaxInputFileSizeMB()));
    }
    if (compactionMutator.getMaxThreads() >= 0) {
      logAttrMutation("MaxThreads", compactionMutator.getMaxThreads());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_MAXCONCURRENCY ,
          new SQLLongint(compactionMutator.getMaxThreads()));
    }
    if (compactionMutator.getMinInputFileCount() >= 0) {
      logAttrMutation("MinInputFileCount", compactionMutator.getMinInputFileCount());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_MININPUTFILECOUNT,
          new SQLLongint(compactionMutator.getMinInputFileCount()));

    }
    if (compactionMutator.getAutoCompaction() != null) {
      logAttrMutation("AutoCompaction", compactionMutator.getAutoCompaction());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_AUTOCOMPACT,
          new SQLBoolean(compactionMutator.getAutoCompaction()));
    }
    
    if (compactionMutator.getMajorCompactionIntervalMins() > -1) {
      logAttrMutation("MajorCompactionIntervalMins", compactionMutator.getMajorCompactionIntervalMins());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_MAJORCOMPACTIONINTERVAL,
          new SQLLongint(compactionMutator.getMajorCompactionIntervalMins()));
    }
    if (compactionMutator.getMajorCompactionMaxThreads() >= 0) {
      logAttrMutation("MajorCompactionMaxThreads", compactionMutator.getMajorCompactionMaxThreads());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_MAJORCOMPACTIONCONCURRENCY,
          new SQLLongint(compactionMutator.getMajorCompactionMaxThreads()));
    }
    if (compactionMutator.getAutoMajorCompaction() != null) {
      logAttrMutation("AutoMajorCompaction", compactionMutator.getAutoMajorCompaction());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_AUTOMAJORCOMPACT,
          new SQLBoolean(compactionMutator.getAutoMajorCompaction()));
    }
    
    if (compactionMutator.getOldFilesCleanupIntervalMins() >= 0) {
      logAttrMutation("OldFilesCleanupIntervalMins", compactionMutator.getOldFilesCleanupIntervalMins());
      mutableRow.setColumn(GfxdSYSHDFSSTORESRowFactory.SYSHDFSSTORES_PURGEINTERVAL,
          new SQLLongint(compactionMutator.getOldFilesCleanupIntervalMins()));
    }
    
    return mutableRow;
  }
  
  private void logAttrMutation(String field, Object fileRolloverInterval) {
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "HDFSStoreAlterOperation: Mutating fields for "
            + storeName + ". Updated field = " + field +  " value: " + fileRolloverInterval);   
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    if (oldStoreConfig != null){
      HDFSStoreMutator oldConfigBasedMutator = new HDFSStoreMutatorImpl(
          oldStoreConfig);
      return new HDFSStoreAlterOperation(oldConfigBasedMutator, storeName);
    } else 
      return new HDFSStoreAlterOperation(null, storeName);
    
  }
}

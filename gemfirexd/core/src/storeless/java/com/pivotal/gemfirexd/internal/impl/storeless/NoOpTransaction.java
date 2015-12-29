/*

   Derby - Class org.apache.impl.storeless.NoOpTransaction

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package com.pivotal.gemfirexd.internal.impl.storeless;

import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.cache.IsolationLevel;
import com.pivotal.gemfirexd.internal.engine.locks.GfxdLockable;
// GemStone changes BEGIN
import com.gemstone.gemfire.internal.cache.TXId;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.services.locks.CompatibilitySpace;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.AccessFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.BackingStoreHashtable;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.DatabaseInstant;
import com.pivotal.gemfirexd.internal.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.FileResource;
import com.pivotal.gemfirexd.internal.iapi.store.access.GroupFetchScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowLocationRetRowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortController;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortCostController;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortObserver;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Loggable;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;

/**
 * A TransactionController that does nothing.
 * This allows the existing transaction aware language
 * code to remain unchanged while not supporting transactions
 * for the storeless engine.
 */
class NoOpTransaction implements TransactionController {

    public AccessFactory getAccessManager() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean conglomerateExists(long conglomId) throws StandardException {
        // TODO Auto-generated method stub
        return false;
    }

    public long createConglomerate(String implementation,
            DataValueDescriptor[] template, ColumnOrdering[] columnOrder,
            int[] collation_ids,
            Properties properties, int temporaryFlag) throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }
  
    public long createAndLoadConglomerate(String implementation,
            DataValueDescriptor[] template, ColumnOrdering[] columnOrder,
            int[] collation_ids,
            Properties properties, int temporaryFlag,
            RowLocationRetRowSource rowSource, long[] rowCount)
            throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public long recreateAndLoadConglomerate(String implementation,
            boolean recreate_ifempty, DataValueDescriptor[] template,
            ColumnOrdering[] columnOrder, 
            int[] collation_ids,
            Properties properties,
            int temporaryFlag, long orig_conglomId,
            RowLocationRetRowSource rowSource, long[] rowCount)
            throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public void addColumnToConglomerate(long conglomId, int column_id,
            Storable template_column, int collation_id) throws StandardException {
        // TODO Auto-generated method stub

    }

    public void dropConglomerate(long conglomId) throws StandardException {
        // TODO Auto-generated method stub

    }

    public long findConglomid(long containerid) throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public long findContainerid(long conglomid) throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public TransactionController startNestedUserTransaction(boolean readOnly)
            throws StandardException {
        return this;
    }

    public Properties getUserCreateConglomPropList() {
        // TODO Auto-generated method stub
        return null;
    }

    public ConglomerateController openConglomerate(long conglomId,
            boolean hold, int open_mode, int lock_level, int isolation_level)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public ConglomerateController openCompiledConglomerate(boolean hold,
            int open_mode, int lock_level, int isolation_level,
            StaticCompiledOpenConglomInfo static_info,
            DynamicCompiledOpenConglomInfo dynamic_info)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public BackingStoreHashtable createBackingStoreHashtableFromScan(
            long conglomId, int open_mode, int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator, long max_rowcnt, int[] key_column_numbers,
            boolean remove_duplicates, long estimated_rowcnt,
            long max_inmemory_rowcnt, int initialCapacity, float loadFactor,
            boolean collect_runtimestats, boolean skipNullKeyColumns,
// GemStone changes BEGIN
// added the activation argument
            boolean keepAfterCommit, Activation activation)
// GemStone changes END
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public ScanController openScan(long conglomId, boolean hold, int open_mode,
            int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
// GemStone changes BEGIN
// added the activation argument
            int stopSearchOperator, Activation activation) throws StandardException {
// GemStone changes END
        // TODO Auto-generated method stub
        return null;
    }

    public ScanController openCompiledScan(boolean hold, int open_mode,
            int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator, StaticCompiledOpenConglomInfo static_info,
            DynamicCompiledOpenConglomInfo dynamic_info)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }
  
  // Gemstone changes BEGIN
  public ScanController openCompiledScan(boolean hold, int open_mode,
      int lock_level, int isolation_level, FormatableBitSet scanColumnList,
      DataValueDescriptor[] startKeyValue, int startSearchOperator,
      Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
      int stopSearchOperator, StaticCompiledOpenConglomInfo static_info,
      DynamicCompiledOpenConglomInfo dynamic_info, Activation act) throws StandardException {
    throw new AssertionError("Open compiled scan with local data set " +
    		"should never be called on NoOpTransaction");
  }
  // Gemstone changes END.

    public GroupFetchScanController openGroupFetchScan(long conglomId,
            boolean hold, int open_mode, int lock_level, int isolation_level,
            FormatableBitSet scanColumnList,
            DataValueDescriptor[] startKeyValue, int startSearchOperator,
            Qualifier[][] qualifier, DataValueDescriptor[] stopKeyValue,
            int stopSearchOperator) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public GroupFetchScanController defragmentConglomerate(long conglomId,
            boolean online, boolean hold, int open_mode, int lock_level,
            int isolation_level) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public void purgeConglomerate(long conglomId) throws StandardException {
        // TODO Auto-generated method stub

    }

    public void compressConglomerate(long conglomId) throws StandardException {
        // TODO Auto-generated method stub

    }

    public boolean fetchMaxOnBtree(long conglomId, int open_mode,
            int lock_level, int isolation_level,
            FormatableBitSet scanColumnList, DataValueDescriptor[] fetchRow)
            throws StandardException {
        // TODO Auto-generated method stub
        return false;
    }

    public StoreCostController openStoreCost(long conglomId)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public int countOpens(int which_to_count) throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public String debugOpened() throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public FileResource getFileHandler() {
        // TODO Auto-generated method stub
        return null;
    }

    public CompatibilitySpace getLockSpace() {
        // TODO Auto-generated method stub
        return null;
    }

    public StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(
            long conglomId) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo(
            long conglomId) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public long[] getCacheStats(String cacheName) {
        // TODO Auto-generated method stub
        return null;
    }

    public void resetCacheStats(String cacheName) {
        // TODO Auto-generated method stub

    }

    public void logAndDo(Loggable operation) throws StandardException {
        // TODO Auto-generated method stub

    }

    public long createSort(Properties implParameters,
            DataValueDescriptor[] template, ColumnOrdering[] columnOrdering,
            SortObserver sortObserver, boolean alreadyInOrder,
            long estimatedRows, int estimatedRowSize) throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public void dropSort(long sortid) throws StandardException {
        // TODO Auto-generated method stub

    }

    public SortController openSort(long id) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public SortCostController openSortCostController(Properties implParameters)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public RowLocationRetRowSource openSortRowSource(long id)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public ScanController openSortScan(long id, boolean hold)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean anyoneBlocked() {
        // TODO Auto-generated method stub
        return false;
    }

    public void abort() throws StandardException {
        // TODO Auto-generated method stub

    }

// Gemstone changes BEGIN
    public DatabaseInstant commit() throws StandardException {
      return null;
    /* (original derby code) public void commit() throws StandardException { */
// GemStone changes END
        // TODO Auto-generated method stub

    }

    public DatabaseInstant commitNoSync(int commitflag)
            throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public void destroy() {
        // TODO Auto-generated method stub

    }

    public ContextManager getContextManager() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getTransactionIdString() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getActiveStateTxIdString() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isIdle() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isGlobal() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isPristine() {
        // TODO Auto-generated method stub
        return false;
    }

    public int releaseSavePoint(String name, Object kindOfSavepoint)
            throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int rollbackToSavePoint(String name, boolean close_controllers,
            Object kindOfSavepoint) throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public int setSavePoint(String name, Object kindOfSavepoint)
            throws StandardException {
        // TODO Auto-generated method stub
        return 0;
    }

    public Object createXATransactionFromLocalTransaction(int format_id,
            byte[] global_id, byte[] branch_id) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public Serializable getProperty(String key) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public Serializable getPropertyDefault(String key) throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean propertyDefaultIsVisible(String key)
            throws StandardException {
        // TODO Auto-generated method stub
        return false;
    }

    public void setProperty(String key, Serializable value,
            boolean dbOnlyProperty) throws StandardException {
        // TODO Auto-generated method stub

    }

    public void setPropertyDefault(String key, Serializable value)
            throws StandardException {
        // TODO Auto-generated method stub

    }

    public Properties getProperties() throws StandardException {
        // TODO Auto-generated method stub
        return null;
    }
    public DataValueFactory getDataValueFactory() throws StandardException {
        // TODO Auto-generated method stub
        return(null);
    }

    public void setNoLockWait(boolean noWait) {
        // TODO Auto-generated method stub
    }

// GemStone changes BEGIN

  @Override
  public void setLanguageConnectionContext(LanguageConnectionContext lcc) {
  }

  @Override
  public LanguageConnectionContext getLanguageConnectionContext() {
    return null;
  }

  @Override
  public void releaseAllLocks(boolean force, boolean removeRef) {
  }

  @Override
  public boolean skipLocks() {
    return false;
  }

  @Override
  public boolean skipLocks(Object warnForOwnerIfSkipLocksForConnection,
      GfxdLockable lockable) {
    return false;
  }

  @Override
  public void setSkipLocks(boolean skip) {
  }

  @Override
  public void setSkipLocksForConnection(boolean skip) {
  }

  @Override
  public void disableLogging() {
  }

  @Override
  public void enableLogging() {
  }

  @Override
  public boolean needLogging() {
    return true;
  }

  @Override
  public DatabaseInstant commitForSetIsolationLevel() throws StandardException {
    throw new UnsupportedOperationException("Commit not supported.");
  }

  @Override
  public void beginTransaction(IsolationLevel isolationLevel) {

  }

  @Override
  public boolean isTransactional() {
    return false;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public long createSort(Properties implParameters, ExecRow template,
      ColumnOrdering[] columnOrdering, SortObserver sortObserver,
      boolean alreadyInOrder, long estimatedRows, int estimatedRowSize,
      long maxSortLimit) throws StandardException {
    return 0;
  }
// GemStone changes END
}

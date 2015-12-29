/*
 * Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.ScanResultSet
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.TemporaryRowHolder;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * Abstract <code>ResultSet</code> class for <code>NoPutResultSet</code>s which
 * contain a scan. Returns rows that may be a column sub-set of the
 * rows in the underlying object to be scanned. If accessedCols is
 * not null then a sub-set of columns will be fetched from the underlying
 * object (usually into the candidate row object), then the returned
 * rows will be a compacted form of that row, with the not-fetched columns
 * moved out. If accessedCols is null then the full row will be returned.
 * <BR>
 * Example: if accessedCols indicates that we want to retrieve columns
 * 1 and 4, then candidate row will have space for 5
 * columns (because that's the size of the rows in the underlying object),
 * but only cols "1" and "4" will have values:
 * <BR>
 * <pre>
 *     0    1    2    3    4
 *  [  - , COL1,  - ,  - , COL4 ]
 *  </pre>
 *  <BR>
 * Rows returned by this ScanResultSet will have the values:
 * <BR>
 * <pre>
 *     0     1
 *  [ COL1, COL4 ]
 * </pre>
 */
abstract class ScanResultSet extends NoPutResultSetImpl {

    /** If true, the table is marked as table locked in SYS.SYSTABLES. */
    protected /* GemStone change private */ final boolean tableLocked;
    /** If true, the isolation level is unspecified and must be refreshed on
     * each open. */
    private final boolean unspecifiedIsolationLevel;
    /** The lock mode supplied through the constructor. */
    private final int suppliedLockMode;
    /** Tells whether the isolation level needs to be updated. */
    private boolean isolationLevelNeedsUpdate;

    /** The actual lock mode used. */
    int lockMode;
    /** The scan isolation level. */
    int isolationLevel;

    /** The candidate row, matches the shape of the rows in
     * the underlying object to be scanned.
     */
  // Gemstone changes BEGIN
  // making non-final so can be replaced with a CompactExecRow if using byte arrays
    /*final*/ ExecRow candidate;
  // GemStone changes END
    
    /**
     * If not null indicates the subset of columns that
     * need to be pulled from the underlying object to be scanned.
     * Set from the PreparedStatement's saved objects, if it exists.
     */
    protected final FormatableBitSet accessedCols;

    /**
     * Construct a <code>ScanResultSet</code>.
     *
     * @param activation the activation
     * @param resultSetNumber number of the result set (unique within statement)
     * @param resultRowAllocator method which generates rows
     * @param lockMode lock mode (record or table)
     * @param tableLocked true if marked as table locked in SYS.SYSTABLES
     * @param isolationLevel language isolation level for the result set
     * @param colRefItem Identifier of saved object for accessedCols,
     * -1 if need to fetch all columns.
     * @param optimizerEstimatedRowCount estimated row count
     * @param optimizerEstimatedCost estimated cost
     */
    ScanResultSet(Activation activation, int resultSetNumber,
                  GeneratedMethod resultRowAllocator,
                  int lockMode, boolean tableLocked, int isolationLevel,
                  int colRefItem,
                  double optimizerEstimatedRowCount,
                  double optimizerEstimatedCost) throws StandardException {
// GemStone changes BEGIN
        this(activation, resultSetNumber, (ExecRow)resultRowAllocator
            .invoke(activation), lockMode, tableLocked, isolationLevel,
            colRefItem != -1 ? (FormatableBitSet)(activation.
                getSavedObject(colRefItem)) : null, optimizerEstimatedRowCount,
            optimizerEstimatedCost);
        /* (original code)
        super(activation, resultSetNumber,
              optimizerEstimatedRowCount,
              optimizerEstimatedCost);

        this.tableLocked = tableLocked;
        suppliedLockMode = lockMode;

        if (isolationLevel == ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL) {
            unspecifiedIsolationLevel = true;
            isolationLevel = getLanguageConnectionContext().getCurrentIsolationLevel();
        } else {
            unspecifiedIsolationLevel = false;
        }

        this.lockMode = getLockMode(isolationLevel);
        this.isolationLevel =
            translateLanguageIsolationLevel(isolationLevel);

        /* Only call row allocators once *
        candidate = (ExecRow) resultRowAllocator.invoke(activation);
        
        this.accessedCols = colRefItem != -1 ?
            (FormatableBitSet)(activation.getPreparedStatement().
                getSavedObject(colRefItem)) : null;      
        */
        
        // GemStone changes BEGIN
        printResultSetHierarchy();
        // GemStone changes END
    }

    /**
     * Construct a <code>ScanResultSet</code>.
     * 
     * @param activation
     *          the activation
     * @param resultSetNumber
     *          number of the result set (unique within statement)
     * @param candidate
     *          candidate row to use as template for generating the rows
     * @param lockMode
     *          lock mode (record or table)
     * @param tableLocked
     *          true if marked as table locked in SYS.SYSTABLES
     * @param isolationLevel
     *          language isolation level for the result set
     * @param accessedCols
     *          the columns being accessed
     * @param optimizerEstimatedRowCount
     *          estimated row count
     * @param optimizerEstimatedCost
     *          estimated cost
     */
    ScanResultSet(Activation activation, int resultSetNumber, ExecRow candidate,
        int lockMode, boolean tableLocked, int isolationLevel,
        FormatableBitSet accessedCols, double optimizerEstimatedRowCount,
        double optimizerEstimatedCost) throws StandardException {
      super(activation, resultSetNumber, optimizerEstimatedRowCount,
          optimizerEstimatedCost);

      this.tableLocked = tableLocked;
      this.suppliedLockMode = lockMode;

      if (isolationLevel == ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL) {
        unspecifiedIsolationLevel = true;
        isolationLevel = getLanguageConnectionContext()
            .getCurrentIsolationLevel();
      }
      else {
        unspecifiedIsolationLevel = false;
      }

      this.lockMode = getLockMode(isolationLevel);
      this.isolationLevel = translateLanguageIsolationLevel(isolationLevel);

      /* Only call row allocators once */
      this.candidate = candidate;
      this.accessedCols = accessedCols;
      
      // GemStone changes BEGIN
      printResultSetHierarchy();
      // GemStone changes END
    }

    // methods for use by IndexRowToBaseRowRS
    protected TemporaryRowHolder getFutureForUpdateRows() {
      return null;
    }

    protected TemporaryRowHolderResultSet getFutureRowResultSet() {
      return null;
    }

    protected void setFutureRowResultSet(
        TemporaryRowHolderResultSet futureRowResultSet) {
      throw new AssertionError("setFutureRowResultSet: unexpected call for "
          + getClass().getName());
    }

    protected boolean sourceDrained() {
      throw new AssertionError("sourceDrained: unexpected call for "
          + getClass().getName());
    }
// GemStone changes END
    /**
     * Initialize the isolation level and the lock mode. If the result set was
     * constructed with an explicit isolation level, or if the isolation level
     * has already been initialized, this is a no-op. All sub-classes should
     * invoke this method from their <code>openCore()</code> methods.
     */
    void initIsolationLevel() {
        if (isolationLevelNeedsUpdate) {
            int languageLevel = getLanguageConnectionContext().getCurrentIsolationLevel();
            lockMode = getLockMode(languageLevel);
            isolationLevel = translateLanguageIsolationLevel(languageLevel);
            isolationLevelNeedsUpdate = false;
        }
    }

    /**
     * Get the lock mode based on the language isolation level. Always do row
     * locking unless the isolation level is serializable or the table is
     * marked as table locked.
     *
     * @param languageLevel the (language) isolation level
     * @return lock mode
     */
    private int getLockMode(int languageLevel) {
        /* NOTE: always do row locking on READ COMMITTED/UNCOMITTED scans,
         * unless the table is marked as table locked (in sys.systables)
         * This is to improve concurrency.  Also see FromBaseTable's
         * updateTargetLockMode (KEEP THESE TWO PLACES CONSISTENT!
         * bug 4318).
         */
        /* NOTE: always do row locking on READ COMMITTED/UNCOMMITTED
         *       and repeatable read scans unless the table is marked as
         *       table locked (in sys.systables).
         *
         *       We always get instantaneous locks as we will complete
         *       the scan before returning any rows and we will fully
         *       requalify the row if we need to go to the heap on a next().
         */
        if (tableLocked ||
                (languageLevel ==
                     ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL)) {
            return suppliedLockMode;
        } else {
            return TransactionController.MODE_RECORD;
        }
    }

    /**
     * Translate isolation level from language to store.
     *
     * @param languageLevel language isolation level
     * @return store isolation level
     */
    private int translateLanguageIsolationLevel(int languageLevel) {

        switch (languageLevel) {
        case ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL:
            return TransactionController.ISOLATION_READ_UNCOMMITTED;
        case ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL:
            /*
             * Now we see if we can get instantaneous locks
             * if we are getting share locks.
             * (For example, we can get instantaneous locks
             * when doing a bulk fetch.)
             */
            if (!canGetInstantaneousLocks()) {
                return TransactionController.ISOLATION_READ_COMMITTED;
            }
            return TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK;
        case ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL:
            return TransactionController.ISOLATION_REPEATABLE_READ;
        case ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL:
            return TransactionController.ISOLATION_SERIALIZABLE;
        }

      // GemStone changes BEGIN
      // allow TRANSACTION_NONE (0)
      /*
        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT("Unknown isolation level - " +
                                      languageLevel);
        }
       */
      // GemStone changes END

        return 0;
    }

    /**
     * Can we get instantaneous locks when getting share row
     * locks at READ COMMITTED.
     */
    abstract boolean canGetInstantaneousLocks();

    /**
     * Return the isolation level of the scan in the result set.
     */
    public int getScanIsolationLevel() {
        return isolationLevel;
    }

    /**
     * Close the result set.
     *
     * @exception StandardException if an error occurs
     */
    public void close(boolean cleanupOnError) throws StandardException {
        // need to update isolation level on next open if it was unspecified
        isolationLevelNeedsUpdate = unspecifiedIsolationLevel;
        // Prepare row array for reuse (DERBY-827).
        candidate.resetRowArray();
        super.close(cleanupOnError);
    }
    
    @Override
    public void printResultSetHierarchy() {
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceNCJ) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
              "ResultSet Created: " + this.getClass().getSimpleName()
                  + " with resultSetNumber=" + resultSetNumber);
        }
      }
    }
}

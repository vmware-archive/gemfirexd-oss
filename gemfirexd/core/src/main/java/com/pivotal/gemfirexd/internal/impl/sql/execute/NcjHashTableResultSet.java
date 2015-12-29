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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.util.Iterator;
import java.util.List;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.StatementContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.NoPutResultSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.BackingStoreHashtable;
import com.pivotal.gemfirexd.internal.iapi.store.access.NcjBackingStoreHashtable;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

/**
 * @author vivekb
 *
 */
public class NcjHashTableResultSet extends HashTableResultSet {
  
  private Iterator hashIter;

  NcjHashTableResultSet(NoPutResultSet s, Activation a, GeneratedMethod str,
      Qualifier[][] nextQualifiers, GeneratedMethod p, int resultSetNumber,
      int mapRefItem, boolean reuseResult, int keyColItem,
      boolean removeDuplicates, long maxInMemoryRowCount, int initialCapacity,
      float loadFactor, boolean skipNullKeyColumns,
      double optimizerEstimatedRowCount, double optimizerEstimatedCost)
      throws StandardException {
    super(s, a, str, nextQualifiers, p, resultSetNumber, mapRefItem, reuseResult,
        keyColItem, removeDuplicates, maxInMemoryRowCount, initialCapacity,
        loadFactor, skipNullKeyColumns, optimizerEstimatedRowCount,
        optimizerEstimatedCost);
    // TODO Auto-generated constructor stub
  }
  
  @Override
  protected BackingStoreHashtable getBackingStoreHashtable()
      throws StandardException {
    // Get the current transaction controller
    TransactionController tc = activation.getTransactionController();
    return new NcjBackingStoreHashtable(tc, this, keyColumns, removeDuplicates,
        (int)optimizerEstimatedRowCount, maxInMemoryRowCount,
        (int)initialCapacity, loadFactor, skipNullKeyColumns, false, this
            .getLanguageConnectionContext().getNcjBatchSize());
  }
  
  @Override
  public boolean moreRowsExpected() {
    if (ht != null) {
      return ht.moreRowsExpected();
    }

    return false;
  }
  
  @Override
  public boolean fillUpHashTable() throws StandardException {
    if (ht != null) {
      return ht.fillUpHashTable();
    }

    return false;
  }

  @Override
  public void purgeHashTable() {
    if (ht != null) {
      ht.purgeHashTable();;
    }
  }
  
  @Override
  protected void resetProbeVariables() throws StandardException
  {
    super.resetProbeVariables();
    this.hashIter = null;
  }

  /*
   * (non-Javadoc)
   * @see com.pivotal.gemfirexd.internal.impl.sql.execute.HashTableResultSet#getNextRowCore()
   * 
   * Do not handled "rak" variables. Not sure if needed?
   */
  @Override
  public ExecRow getNextRowCore() throws StandardException {
    if (this.nextQualifiers != null) {
      return super.getNextRowCore();
    }
    checkCancellationFlag();

    ExecRow result = null;
    DataValueDescriptor[] columns = null;
    // GemStone changes BEGIN
    Object rak = null;
    // GemStone changes END
    beginTime = statisticsTimingOn ? XPLAINUtil.nanoTime() : 0;
    if (isOpen) {
      do {
        if (firstNext) {
          firstNext = false;
          this.hashIter = this.hashTableIterator();
        }
        
        SanityManager.ASSERT(this.hashIter != null,
            "NcjHashTableResultSet is not open");
        
        if (numFetchedOnNext < entryVectorSize) {
          // We are walking a list and there are more rows left.
          columns = (DataValueDescriptor[])entryVector.get(numFetchedOnNext);
        }
        else if (hashIter.hasNext()) {
          Object hashEntry = hashIter.next();
          SanityManager.ASSERT(hashEntry != null, "Null row from hash table");
          if (hashEntry instanceof List) {
            entryVector = (List)hashEntry;
            entryVectorSize = entryVector.size();
            columns = (DataValueDescriptor[])entryVector.get(0);
          }
          else {
            entryVector = null;
            entryVectorSize = 0;
            columns = (DataValueDescriptor[])hashEntry;
          }
        }
        
        if (columns != null) {
          if (SanityManager.DEBUG) {
            // Columns is really a Storable[]
            for (int i = 0; i < columns.length; i++) {
              if (!(columns[0] instanceof Storable)) {
                SanityManager.THROWASSERT("columns[" + i
                    + "] expected to be Storable, not "
                    + columns[i].getClass().getName());
              }
            }
          }

          for (int index = 0; index < columns.length; index++) {
            nextCandidate.setColumn(index + 1, columns[index]);
          }

          if (nextCandidate != null) {
            nextCandidate.clearAllRegionAndKeyInfo();
          }

          result = doProjection(nextCandidate);

          numFetchedOnNext++;
        }
        else {
          result = null;
        }
      } while (result == null && numFetchedOnNext < entryVectorSize);
    }

    setCurrentRow(result);

    if (statisticsTimingOn)
      nextTime += getElapsedNanos(beginTime);

    if (runTimeStatsOn) {
      if (!isTopResultSet) {
        /* This is simply for RunTimeStats */
        /* We first need to get the subquery tracking array via the StatementContext */
        StatementContext sc = activation.getLanguageConnectionContext()
            .getStatementContext();
        subqueryTrackingArray = sc.getSubqueryTrackingArray();
      }
      if (statisticsTimingOn)
        nextTime += getElapsedNanos(beginTime);
    }

    if (result != null) {
      if (this.localTXState != null && this.isTopResultSet && isForUpdate()) {
        updateRowLocationPostRead();
      }
    }

    return result;
  }
}

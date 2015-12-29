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
package com.pivotal.gemfirexd.internal.iapi.store.access;

import java.util.Arrays;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * @author vivekb
 * 
 */
public class NcjBackingStoreHashtable extends BackingStoreHashtable {
  final int batchSize;

  private boolean moreRowsExpected;

  public NcjBackingStoreHashtable(TransactionController tc,
      RowSource row_source, int[] key_column_numbers,
      boolean remove_duplicates, long estimated_rowcnt,
      long max_inmemory_rowcnt, int initialCapacity, float loadFactor,
      boolean skipNullKeyColumns, boolean keepAfterCommit, int batchSize)
      throws StandardException {
    super(tc, row_source, key_column_numbers, remove_duplicates,
        estimated_rowcnt, max_inmemory_rowcnt, initialCapacity, loadFactor,
        skipNullKeyColumns, keepAfterCommit, false);
    if (batchSize <= 0) {
      this.batchSize = Integer.MAX_VALUE;
    }
    else {
      this.batchSize = batchSize;
    }
    fillUpHashTable();
  }

  @Override
  public boolean fillUpHashTable() throws StandardException {
    this.moreRowsExpected = false;
    assertHashTableNotNull();
    SanityManager.ASSERT(this.row_source != null);
    boolean needsToClone = row_source.needsToClone();
    DataValueDescriptor[] row;
    int row_count = 0;
    while ((row = getNextRowFromRowSource()) != null) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager
            .DEBUG_PRINT(
                GfxdConstants.TRACE_NCJ_ITER,
                "NcjBackingStoreHashTable::fillUpHashTable() calling add_row_to_hash_table for row = "
                    + Arrays.toString(row) + ", row_count: " + row_count);
      }

      add_row_to_hash_table(row, needsToClone, null);
      row_count++;

      if (row_count >= this.batchSize) {
        this.moreRowsExpected = true;
        break;
      }
    }

    if (GemFireXDUtils.TraceNCJ) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
          "NcjBackingStoreHashTable::fillUpHashTable() calling add_row_to_hash_table. "
              + " batchSize: " + this.batchSize + ", needsToclone: "
              + needsToClone + ", row_count: " + row_count
              + ", moreRowsExpected: " + this.moreRowsExpected);
    }

    return row_count > 0;
  }

  @Override
  public boolean moreRowsExpected() {
    return this.moreRowsExpected;
  }
}

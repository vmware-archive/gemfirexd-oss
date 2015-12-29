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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.access.index;


import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CostEstimate;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostController;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostResult;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * @author yjing
 *
 */
public class Hash1IndexCostController extends MemIndexCostController {

  /** Only lookup these estimates from raw store once. */
  private long row_size;

  /** the number of base table rows */
  private long numBaseRows;

  /**
   * Factor multipled to costing for index hash joins since we never prefer hash
   * joins on indexes.
   */
  private static final double HASH_JOIN_COST_FACTOR = 100.0d;

  @Override
  protected void postInit() {
    final GemFireContainer baseContainer = this.open_conglom.getBaseContainer();
    this.row_size = baseContainer.getRowSize();
    this.numBaseRows = baseContainer.getNumRows();
    if (this.row_size < 1) {
      this.row_size = 1;
    }
    if (this.numBaseRows < 1) {
      this.numBaseRows = 1;
    }
  }
  
  @Override
  public long getEstimatedRowCount() throws StandardException {
    return this.numBaseRows;
  }

  @Override
  public double getFetchFromFullKeyCost(FormatableBitSet validColumns,
      int access_type, CostEstimate costEstimate) throws StandardException {
    double cardinality = 1.0d;
    // give preference to indexes with larger number of columns to be on par
    // with the adjustments in SortedMap2IndexCostController
    final GemFireContainer baseContainer = this.open_conglom.getBaseContainer();
    if (baseContainer.getExtraTableInfo() != null) {
      final int[] pkCols = baseContainer.getExtraTableInfo()
          .getPrimaryKeyColumns();
      int numIndexedColumns;
      if (pkCols != null && (numIndexedColumns = pkCols.length) > 1) {
        cardinality = cardinality / (1.0 + (numIndexedColumns / 100.0));
      }
    }
    double cost = cardinality * this.row_size * BASE_ROW_PER_BYTECOST;
    // never use HASH join for single row match
    if (access_type != StoreCostController.STORECOST_SCAN_SET) {
      cost += BASE_GROUPSCAN_ROW_COST;
     
    }
    else {
      cost += (this.numBaseRows * BASE_HASHSCAN_ROW_FETCH_COST * HASH_JOIN_COST_FACTOR);
      if (cost <= 0.0d) {
        cost = Double.MAX_VALUE;
      }
    }
    if(costEstimate != null) {
      costEstimate.setCost(cost, 1, 1);
    }
    return cost;
    
  }

  @Override
  public void getScanCost(int scan_type, long row_count, int group_size,
      boolean forUpdate, FormatableBitSet scanColumnList,
      DataValueDescriptor[] template, DataValueDescriptor[] startKeyValue,
      int startSearchOperator, DataValueDescriptor[] stopKeyValue,
      int stopSearchOperator, boolean reopen_scan, int access_type,
       StoreCostResult cost_result)
      throws StandardException {

    GemFireXDQueryObserver sqo = GemFireXDQueryObserverHolder.getInstance();

    // access_type is a flag to indicate single row fetch or IN clause
    // when set to STORECOST_SCAN_SET
    // scan_type is for JoinStrategy and we don't like hash joins on indexes ...
    if (access_type == StoreCostController.STORECOST_SCAN_SET
        && scan_type != StoreCostController.STORECOST_SCAN_SET) {

        /** shoubhik20090217:
         *  lets make the scan cost equivalent to MemHeapCostController#getScanCost
         *  as now Hash1Index merely wraps around GemFire region (memstore).
         *  Without this, primary index is never considered by the optimizer while 
         *  scanning heap @see ticket 40222.
         *  [sumedh] now the cost is of a single row and hence much smaller than
         *  MemHeap scan
         */
      final long estimated_row_count = 1;
      double cost = estimated_row_count * this.row_size * BASE_ROW_PER_BYTECOST;
      cost += (estimated_row_count * BASE_GROUPSCAN_ROW_COST);
      if (sqo != null) {
        cost = sqo.overrideDerbyOptimizerIndexUsageCostForHash1IndexScan(
            this.open_conglom, cost);
      }
      cost_result.setEstimatedCost(cost);
      cost_result.setEstimatedRowCount(estimated_row_count);
    }
    else {
      // cannot use this index so increase the cost so it is never picked;
      // however, making this as Double.MAX_VALUE causes other problematic
      // behaviour like skipping some combinations, so try to avoid that
      long rowCount = row_count < 1 ? this.numBaseRows : row_count;
      double cost = ((rowCount * this.row_size) + 1)
          * BASE_ROW_PER_BYTECOST;
      cost += (rowCount * BASE_GROUPSCAN_ROW_COST);
      cost *= HASH_JOIN_COST_FACTOR;
      if (cost <= 0.0d) {
        cost = Double.MAX_VALUE;
      }
      if (sqo != null) {
        cost = sqo.overrideDerbyOptimizerIndexUsageCostForHash1IndexScan(
            this.open_conglom, cost);
      }

      cost_result.setEstimatedCost(cost);
      cost_result.setEstimatedRowCount(row_count);
    }
  }
}

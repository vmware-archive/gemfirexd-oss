
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
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

/*
 This file was based on the MemStore patch written by Knut Magne, published
 under the Derby issue DERBY-2798 and released under the same license,
 ASF, as described above. The MemStore patch was in turn based on Derby source
 files.
 */
package com.pivotal.gemfirexd.internal.engine.access.index;

import com.gemstone.gemfire.internal.concurrent.ConcurrentSkipListMap;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CostEstimate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer;
import com.pivotal.gemfirexd.internal.iapi.store.access.ScanController;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostController;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostResult;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * The SortedMap2IndexCostController interface provides methods that an access client
 * (most likely the system optimizer) can use to get store's estimated cost of
 * various operations on the conglomerate the StoreCostController was opened
 * for.
 *
 * <p>It is likely that the implementation of SortedMap2IndexCostController will open the
 * conglomerate and will leave the conglomerate open until the
 * StoreCostController is closed. This represents a significant amount of work,
 * so the caller if possible should attempt to open the StoreCostController once
 * per unit of work and rather than close and reopen the controller. For
 * instance if the optimizer needs to cost 2 different scans against a single
 * conglomerate, it should use one instance of the SortedMap2IndexCostController.
 *
 * <p>The locking behavior of the implementation of a SortedMap2IndexCostController is
 * undefined, it may or may not get locks on the underlying conglomerate. It may
 * or may not hold locks until end of transaction. An optimal implementation
 * will not get any locks on the underlying conglomerate, thus allowing
 * concurrent access to the table by a executing query while another query is
 * optimizing.
 *
 * <p>The SortedMap2IndexCostController gives 2 kinds of cost information
 */

public class SortedMap2IndexCostController extends MemIndexCostController  {

  private ConcurrentSkipListMap<Object, Object> skipListMap;

  private double locatingCost;

  /** the number of base table rows */
  private long numBaseRows;
  

  /**
   * Factor multipled to costing for index hash joins since we never prefer hash
   * joins on indexes.
   */
  private static final double HASH_JOIN_COST_FACTOR = 10.0d;

  @Override
  protected void postInit() 
    throws StandardException {

     this.skipListMap = open_conglom.getGemFireContainer().getSkipListMap();
     this.num_rows = this.skipListMap.size();
     if (this.open_conglom.isUnique()) {
       this.numBaseRows = this.num_rows;
     }
     else {
       this.numBaseRows = this.open_conglom.getBaseContainer().getNumRows();
     }
     //final Map.Entry<?, ?> lastEntry = this.skipListMap.lastEntry();
     //if (lastEntry != null) {
       /* shoubhik20081223:
        * row_size cannot be just number of index columns. It is important to consider
        * variable strings & potential de-serialization/stringCompare CPU cost amoung
        * various indexes defined.
        * 
        * As indexes are sorted, Strings will be sorted as per SQLChar#compareString 
        * & will have the last key to be of valid maximum value length. 
        * Ideally for strings we should do a special handling of (lastKey-firstKey)/2 
        * to offset one of a big string.
        * For other data types probably it doesn't matters. Decimals &
        * VarBinary also gets taken care by picking up the last key.
        * 
        * Also, as RowLocation is expected to be same for all local indexes,
        * not accounting for it in row_size.
        */
       //this.row_size = this.open_conglom.getConglomerate().getKeySize(
       //    lastEntry.getKey());
     //}
     //else {
       /*soubhik20100419. lets compute the class size instead.
        * addressing 41743.
        * 
        assert this.skipListMap.size() == 0;
       /*
        * sticking to earlier implementation for client end.
        * / 
       this.row_size = this.open_conglom.getConglomerate().indexColumns;
       
       */
       //this.row_size = this.open_conglom.getConglomerate().estimatedRowsize;
    //}
    this.row_size = this.open_conglom.getBaseContainer().getRowSize();
    if (this.num_rows < 1) {
      this.num_rows = 1;
    }
    if (this.numBaseRows < 1) {
      this.numBaseRows = 1;
    }
    if (this.row_size < 1) {
      this.row_size = 1;
    }
    this.locatingCost = treeHeight(this.num_rows) * BASE_GROUPSCAN_ROW_COST;
  }

  private double treeHeight(long numRows) {
    if (numRows < 3) {
      return 1.0d;
    }
    else {
      return Math.log(numRows) / Math.log(2.0d);
    }
  }

  @Override
  public double getFetchFromFullKeyCost(FormatableBitSet validColumns,
      int access_type, CostEstimate costEstimate) throws StandardException {
    /* soubhik20081223:
     * row_size need not be added as FromBaseTable#scanCostAfterSelectivity
     * accounts for it.
     */
    final GemFireContainer indexContainer = this.open_conglom
        .getGemFireContainer();
    // give some preference to single row scans always, so reduce cardinality
    double cardinality = getCardinality() / 1.1d;
    if (cardinality < 1.0d) {
      cardinality = 1.1d;
    }
    // give preference to indexes with larger number of columns since they are
    // likely to prune more
    double pruneFactor = 1d;
    if (indexContainer.getExtraIndexInfo() != null) {
      final int[] pkCols = indexContainer.getExtraIndexInfo()
          .getPrimaryKeyColumns();
      int numIndexedColumns;
      if (pkCols != null && (numIndexedColumns = pkCols.length) > 1) {
       pruneFactor = 1 / (1.0d + (numIndexedColumns * 1d/ this.open_conglom.getBaseContainer().getNumColumns()));
       
      }
    }
   
    cardinality = cardinality * pruneFactor;
    double cost =  cardinality * this.row_size * BASE_ROW_PER_BYTECOST;
    
    // never use HASH join for single row match
    if (access_type != StoreCostController.STORECOST_SCAN_SET) {
      cost += this.locatingCost;      
    }else {
     cost += (this.locatingCost * this.numBaseRows * HASH_JOIN_COST_FACTOR);
    }
    if(costEstimate != null) {
      costEstimate.setCost(cost, cardinality, cardinality);
    }
    return cost;
  }

  public void getScanCost(int scan_type, long row_count, int group_size,
      boolean forUpdate, FormatableBitSet scanColumnList,
      DataValueDescriptor[] template, DataValueDescriptor[] startKeyValue,
      int startSearchOperator, DataValueDescriptor[] stopKeyValue,
      int stopSearchOperator, boolean reopen_scan, int access_type,
      StoreCostResult cost_result)
      throws StandardException {

    double cost = 0.0d;
    long rowCount = 0;
    boolean costEstimationDone = false;
    double pruneFactor = 1.0d;
    
    boolean fromInclusive = startSearchOperator == ScanController.GE;
    boolean toInclusive = stopSearchOperator == ScanController.GT;
    int numColsInIndex = this.open_conglom.getGemFireContainer().numColumns();
    final GemFireContainer indexContainer = this.open_conglom
        .getGemFireContainer();
    if ((startKeyValue == null) && (stopKeyValue == null)) {
      // in general we don't like hash joins on indexes ...
      if (scan_type == StoreCostController.STORECOST_SCAN_SET) {
        rowCount = row_count < 0 ? this.numBaseRows : row_count;
        cost += ((rowCount * (double)row_size) * BASE_ROW_PER_BYTECOST);
        cost += (rowCount * BASE_HASHSCAN_ROW_FETCH_COST
            * HASH_JOIN_COST_FACTOR);
        cost *= (treeHeight(rowCount) + 2.0d);
        costEstimationDone = true;
        
      }
      // for the case of table scan (but not index join), GFXD base table scan
      // is cheaper than going through index which derby uses for supposedly
      // better concurrency
      else if (access_type == StoreCostController.STORECOST_SCAN_SET) {
        // this case will normally prune to exactly one row (equality)
        cost += this.locatingCost;
        rowCount = 1;        
      }
      else {
        cost += this.locatingCost;
        // check for the case when neither start nor stop key matches
        if (access_type == 0) {
          rowCount =  this.num_rows ;/* balance against the +5 in row_count */;
          // adding some cost for iterating index + RowLocation fetch compared
          // to MemHeapScanController, so that latter will be preferred as far
          // as possible; don't make this infinite else higher layers ignore
          // many other valid combinations
          pruneFactor = 1.3d;
        }
        else {
          rowCount = this.num_rows;
          // assume the query will prune some portion of the tree since
          // start/stop key is null for prepared stmts
          pruneFactor = (access_type == StoreCostController.STORECOST_SCAN_INDEX_FULL_KEY)? 0.1d : 0.6d;
        }
      }
    }
    else if ((startKeyValue == null) && (stopKeyValue != null)) {
      
      cost += this.locatingCost;
      rowCount = skipListMap.headMap(
          OpenMemIndex.newLocalKeyObject(stopKeyValue, indexContainer),
          toInclusive).size();
     
    }
    else if ((startKeyValue != null) && (stopKeyValue == null)) {
      
      cost += this.locatingCost;
      rowCount = skipListMap.tailMap(
          OpenMemIndex.newLocalKeyObject(startKeyValue, indexContainer),
          fromInclusive).size();
    }
    else if ((startKeyValue != null) && (stopKeyValue != null)) {
      
      cost += this.locatingCost;
      rowCount = skipListMap.subMap(
          OpenMemIndex.newLocalKeyObject(startKeyValue, indexContainer),
          fromInclusive,
          OpenMemIndex.newLocalKeyObject(stopKeyValue, indexContainer),
          toInclusive).size();
    }
    /* shoubhik20081223: 
     * row_count is the base Table's total row count. Simply rowCount of the index
     * gives an idea of cardinality but not the number of rows it will prune if this
     * index is used as a filter. Ratio between row count of the underlying table & 
     * index row count gives a better idea of how many rows this index will prune 
     * and multiplying by the row_size will approximately estimate 
     * the number of comparisons involved to service any generic filter.
     * 
     * Notice that this cost gets multiplied by the selectivity of the index column
     * for unknown ranges or dynamic parameter where optimizer at this stage doesn't
     * have any idea of the value. 
     */
    if (!costEstimationDone) {
     
      double numIndexKeys = rowCount;
      double cardinality = getCardinality();
      // give preference to indexes with larger number of columns since they are
      // likely to prune more
      //Asif: The extra column's which act as qualifiers are accounted for in the costing in from base table.
      //Give preferene to that index which uses maximum number of columns in its start/stop key
      // because they will be useful in trimming the row count. rest of the cols even if part of index but not useful
      // will be treated as qualifiers
      
     /* if (indexContainer.getExtraIndexInfo() != null) {
        final int[] pkCols = indexContainer.getExtraIndexInfo()
            .getPrimaryKeyColumns();
        int numIndexedColumns;
        if (pkCols != null && (numIndexedColumns = pkCols.length) > 1) {
          cardinality = cardinality / (1.0 + (numIndexedColumns / 100.0d));
        }
      }*/
      int usefulCols = startKeyValue != null? startKeyValue.length :0;
      if(stopKeyValue != null && stopKeyValue.length > usefulCols) {
        usefulCols = stopKeyValue.length;            
      }
      
      if(usefulCols > 1 ) {
        pruneFactor = pruneFactor / (1.0d + (usefulCols *1d /  this.open_conglom.getBaseContainer().getNumColumns()));
      }
      
      rowCount *= (cardinality * pruneFactor); 
      if (rowCount < 1) {
        rowCount = 1;
      }
     
     
      if(access_type == 0 && startKeyValue == null && stopKeyValue == null) {
        rowCount +=5;
      }
      
      cost += ((rowCount * this.row_size)  * BASE_ROW_PER_BYTECOST );
      // in general we don't like hash joins on indexes ...
      if (scan_type == StoreCostController.STORECOST_SCAN_SET) {
        cost += (rowCount * BASE_HASHSCAN_ROW_FETCH_COST
            * HASH_JOIN_COST_FACTOR);
        cost *= (treeHeight(this.numBaseRows) + 2.0d);
      }
      else {
        cost += (rowCount  * StoreCostController.BASE_GROUPSCAN_ROW_COST );
      }
    }
    GemFireXDQueryObserver sqo = GemFireXDQueryObserverHolder.getInstance();
    if (sqo != null) {
      cost = sqo.overrideDerbyOptimizerIndexUsageCostForSortedIndexScan(
          this.open_conglom, cost);
    }
    cost_result.setEstimatedCost(cost);
    cost_result.setEstimatedRowCount(rowCount);
  }
  /*
  private  double estimateGroupByMatchPrunFactor(int matchedGroupingColumns) {
 // if there are GROUP BY columns then reduce the cost in proportion
    // with the number of columns that are indexed
    double pruneFactor = 1d;
    final GemFireContainer indexContainer = this.open_conglom
        .getGemFireContainer();
    if (matchedGroupingColumns > 0) {
      // reset pruneFactor first
      if (pruneFactor > 1.0) {
        pruneFactor = 1.0;
      }
      int numIndexedColumns = indexContainer.getExtraIndexInfo()
          .getPrimaryKeyColumns().length;
      // for best case of all columns being used, reduce the prune factor
      // by a third, while for other cases increase proportionately
      double groupOrderFactor = ((numIndexedColumns / (matchedGroupingColumns
          * 3.0)) - (1.0 / 3.0)) * (1.0 / 4.0) + (1.0 / 3.0);
      if (groupOrderFactor > 2.0 / 3.0) {
        groupOrderFactor = 2.0 / 3.0;
      }
       pruneFactor *= groupOrderFactor;
      
    }
    return pruneFactor;
  }*/

  @Override
  public final double getCardinality() {
    final double cardinality = this.num_rows > 0 ? (double)this.numBaseRows / (double)this.num_rows :0;
    if (cardinality >= 1.0d) {
      return cardinality;
    }
    return 1.0d;
  }
  
  
}

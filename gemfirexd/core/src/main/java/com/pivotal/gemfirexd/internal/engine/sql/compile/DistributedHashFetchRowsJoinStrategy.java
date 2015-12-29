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
package com.pivotal.gemfirexd.internal.engine.sql.compile;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CostEstimate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.ExpressionClassBuilderInterface;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.OptimizablePredicate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.OptimizablePredicateList;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.store.access.StoreCostController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.sql.compile.HashJoinStrategy;

/**
 * A distributed Hash join strategy that will fetch remote table(s) row(s) in
 * batches (PullJoinStrategy).
 * 
 * @author soubhikc
 * 
 */
public class DistributedHashFetchRowsJoinStrategy extends HashJoinStrategy
    implements DistributedJoinStrategy {
  
  public DistributedHashFetchRowsJoinStrategy() {
    
  }

  @Override
  public boolean feasible(Optimizable innerTable,
      OptimizablePredicateList predList, Optimizer optimizer)
      throws StandardException {
    return super.feasible(innerTable, predList, optimizer);
    //TODO: SB:DJ: add func(c1) = func(c2) or alike join relation's feasibility.
  }

  @Override
  public boolean bulkFetchOK() {
    return true;
  }

  @Override
  public boolean ignoreBulkFetch() {
    return false;
  }

  @Override
  public boolean multiplyBaseCostByOuterRows() {
    return true;
  }

  @Override
  public OptimizablePredicateList getBasePredicates(
      OptimizablePredicateList predList,
      OptimizablePredicateList basePredicates, Optimizable innerTable)
      throws StandardException {
    if (SanityManager.DEBUG) {
      SanityManager.ASSERT(basePredicates.size() == 0,
          "The base predicate list should be empty.");
    }

    for (int i = predList.size() - 1; i >= 0; i--) {
      OptimizablePredicate pred = predList.getOptPredicate(i);

      if (pred.getReferencedMap().contains(innerTable.getReferencedTableMap())) {
        basePredicates.addOptPredicate(pred);
        predList.removeOptPredicate(i);
      }
    }

    basePredicates.classify(innerTable, innerTable.getCurrentAccessPath()
        .getConglomerateDescriptor());

    return basePredicates;
  }

  @Override
  public double nonBasePredicateSelectivity(Optimizable innerTable,
      OptimizablePredicateList predList) throws StandardException {
    return super.nonBasePredicateSelectivity(innerTable, predList);
  }

  @Override
  public void putBasePredicates(OptimizablePredicateList predList,
      OptimizablePredicateList basePredicates) throws StandardException {
    super.putBasePredicates(predList, basePredicates);
  }

  @Override
  public void estimateCost(Optimizable innerTable,
      OptimizablePredicateList predList, ConglomerateDescriptor cd,
      CostEstimate outerCost, Optimizer optimizer, CostEstimate costEstimate)
      throws StandardException {
    super.estimateCost(innerTable, predList, cd, outerCost, optimizer,
        costEstimate);
    //TODO sb:DJ: add n/w cost based on join relation.
  }

  @Override
  public int maxCapacity(int userSpecifiedCapacity, int maxMemoryPerTable,
      double perRowUsage) {
    return Integer.MAX_VALUE;
  }

  @Override
  public String getName() {
    return "DISTRIBUTED-HASH-FETCH-ROWS";
  }

  @Override
  public int scanCostType() {
    return StoreCostController.STORECOST_REMOTE_FETCH_ROWS;
  }

  @Override
  public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
    SanityManager.THROWASSERT("This method should not be called");
    return "getRemoteFetchResultSet";
  }

  @Override
  public String joinResultSetMethodName() {
    return "getNcjPullJoinResultSet";
  }

  @Override
  public String halfOuterJoinResultSetMethodName() {
    SanityManager.THROWASSERT("This method should not be called");
    return "getDistributedHashLeftOuterJoinResultSet";
  }

  @Override
  public int getScanArgs(TransactionController tc, MethodBuilder mb,
      Optimizable innerTable, OptimizablePredicateList storeRestrictionList,
      OptimizablePredicateList nonStoreRestrictionList,
      ExpressionClassBuilderInterface acb, int bulkFetch,
      MethodBuilder resultRowAllocator, int colRefItem, int indexColItem,
      int lockMode, boolean tableLocked, int isolationLevel,
      int maxMemoryPerTable, boolean genInListVals, boolean delayScanOpening,
      boolean optimizeForOffHeap, boolean indexAccesesBaseTable)
      throws StandardException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void divideUpPredicateLists(Optimizable innerTable,
      OptimizablePredicateList originalRestrictionList,
      OptimizablePredicateList storeRestrictionList,
      OptimizablePredicateList nonStoreRestrictionList,
      OptimizablePredicateList requalificationRestrictionList, DataDictionary dd)
      throws StandardException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isHashJoin() {
    return true;
  }

  @Override
  public boolean doesMaterialization() {
    return true;
  }

}

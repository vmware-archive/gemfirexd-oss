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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

/**
 * Class representing a group of AbstractConditionQueryInfo object having
 * dependency on a given column. It gets created if there are more than one
 * conditions in where clause dependent on a column being considered and atleast
 * one of those conditions is parameterized.
 * 
 * @author Asif
 * 
 */
public class ParameterizedConditionsWrapperQueryInfo extends
    AbstractConditionQueryInfo {
  /**
   * Can be an instance of ComparisonQueryInfo , RangeQueryInfo or
   * ConstantConditionsQueryInfo
   */
  
  private AbstractConditionQueryInfo constantCondition = null;

  /**
   * List containing parameterized ComparisonQueryInfo
   */
  private final List<AbstractConditionQueryInfo> parameterizedConditions = new ArrayList<AbstractConditionQueryInfo>(
      4);

  ParameterizedConditionsWrapperQueryInfo(
      AbstractConditionQueryInfo condition1,
      AbstractConditionQueryInfo condition2) {

    if (condition1.isWhereClauseDynamic()) {
      this.parameterizedConditions.add(condition1);
      if (condition2.isWhereClauseDynamic()) {
        this.parameterizedConditions.add(condition2);
      }
      else {
        this.constantCondition = condition2;
      }
    }
    else {
      this.constantCondition = condition1;
      assert condition2.isWhereClauseDynamic();
      this.parameterizedConditions.add(condition2);
    }

  }

  AbstractConditionQueryInfo createOrAddToGroup(
      AbstractConditionQueryInfo operand, boolean ignore, Activation activation, boolean forSingleHopPreparePhase)
      throws StandardException {
    // the createConstantCondtionsWrapperIfMergeNotPossible boolean will always
    // be true
    assert ignore;
    assert activation == null;
    if (operand.isWhereClauseDynamic()) {
      this.parameterizedConditions.add(operand);
    }
    else {
      // If constantCondition is an instance of ComparisonQueryInfo or
      // RangeQueryInfo, the merge operation
      // may return an instance of ConstantConditionsQueryInfo. But if it is an
      // instance of
      // ConstantConditionsQueryInfo, it will return itself.
      if(this.constantCondition  != null) {
        this.constantCondition = this.constantCondition.createOrAddToGroup(
          operand, true, activation, false);
      }else {
        this.constantCondition = operand;
      }
    }
    return this;
  }

  /**
   * Test API only
   * 
   * @return Returns a List containing parameterized AbstractConditionQueryInfo
   *         objects
   * 
   */
  List getParameterizedOperands() {
    return Collections.unmodifiableList(this.parameterizedConditions);
  }

  /**
   * Test API only
   * 
   * @return Returns AbstractConditionQueryInfo representing the constant
   *         conditions. Could be any one of ComparisonQueryInfo, RangeQueryInfo
   *         or ConstantConditionsQueryInfo
   * 
   */
  AbstractConditionQueryInfo getConstantOperand() {
    return this.constantCondition;
  }
  
  //TODO:Asif: Currently computing the nodes of constant condition separately from the 
  // parameterized condition. The parameterized conditions are being grouped 
  // among themselves & not with the constant condition. It might be better
  // in terms of node prunning if the grouping could happen across the constant
  // and parameterized conditions, but that would mean generation of new structures
  // and code becoming messier ( we cannot modify the constant condition object).
  // At some point later , we should look into this optimization
  @Override
  public void computeNodes(Set<Object> routingKeys, Activation activation,
      boolean forSingleHopPreparePhase)
      throws StandardException {
    assert routingKeys.size() ==1;
    assert routingKeys.contains(ResolverUtils.TOK_ALL_NODES);
    //First compute the nodes for constant condition
    Set<Object> nodesCollector = new HashSet<Object>();
    nodesCollector.add(ResolverUtils.TOK_ALL_NODES);
    if (this.constantCondition != null) {
      this.constantCondition.computeNodes(nodesCollector, activation, forSingleHopPreparePhase);
      if(!nodesCollector.contains(ResolverUtils.TOK_ALL_NODES)) {
        routingKeys.remove(ResolverUtils.TOK_ALL_NODES);
        routingKeys.addAll(nodesCollector); 
        nodesCollector.clear();
        nodesCollector.add(ResolverUtils.TOK_ALL_NODES);
      }else {
        assert nodesCollector.size() == 1;
      }
    }    
    
    AbstractConditionQueryInfo tempAcqi = this.groupParameterizedConditions(
        activation, forSingleHopPreparePhase);
    
    tempAcqi.computeNodes(nodesCollector, activation, forSingleHopPreparePhase);
    if(!nodesCollector.contains(ResolverUtils.TOK_ALL_NODES)) {
      if(routingKeys.contains(ResolverUtils.TOK_ALL_NODES)) {
        routingKeys.remove(ResolverUtils.TOK_ALL_NODES);
        routingKeys.addAll(nodesCollector);
      }else {
        routingKeys.retainAll(nodesCollector);
      }
    }
  }
  
  AbstractConditionQueryInfo groupParameterizedConditions(
      Activation activation, boolean forSingleHopPreparePhase)
      throws StandardException {
    //Try to group the parameterized conditions
    Iterator<AbstractConditionQueryInfo> itr = this.parameterizedConditions
        .iterator();
    assert itr.hasNext();
    AbstractConditionQueryInfo tempAcqi = itr.next();
    while (itr.hasNext()) {
      tempAcqi = tempAcqi.createOrAddToGroup(itr.next(), true /*
       * create ConstantConditionsWrapper if merge not
       * possible*/, activation, forSingleHopPreparePhase);
    }
    return tempAcqi;
    
  }


  
  @Override
  boolean isWhereClauseDynamic() {
    return true;
  }
  
}

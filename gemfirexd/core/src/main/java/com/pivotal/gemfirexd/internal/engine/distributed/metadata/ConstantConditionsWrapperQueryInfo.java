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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

/**
 * This class groups all the conditions of where clause dependent on a given
 * column. The List can contain instances of RangeQueryInfo or
 * ComparisonQueryInfo of non parameterized types. Since this class groups the
 * conditions which cannot be merged among themselves, it represents a
 * collection of such conditions. The boolean
 * createConstantConditionsWrapperIfMergeNotPossible in createOrAddToGroup
 * function is always passed as false, as if two conditions are not mergeable we
 * still do not want to create new ConstantConditionsWrapperQueryInfo instance,
 * as this object will store non mergeable operand in its List. The presence of
 * this Object implies that there are at least two non mergeable
 * AbstractConditionQueryInfo object for a column.
 * This object will get created only for AND junction
 * 
 * @author Asif
 * 
 */
public class ConstantConditionsWrapperQueryInfo extends
    AbstractConditionQueryInfo {
  private final List<AbstractConditionQueryInfo> constantConditions;

  ConstantConditionsWrapperQueryInfo(AbstractConditionQueryInfo constCond1,
      AbstractConditionQueryInfo constCond2) {
    this.constantConditions = new ArrayList<AbstractConditionQueryInfo>(4);
    this.constantConditions.add(constCond1);
    this.constantConditions.add(constCond2);
  }
  
  

  AbstractConditionQueryInfo createOrAddToGroup(
      AbstractConditionQueryInfo operand, boolean ignore /*
                                                           * we are here it
                                                           * implies that
                                                           * ConstantConditionsWrapper
                                                           * is already created,
                                                           * so should not be
                                                           * created again
                                                           */, Activation activation, boolean forSingleHopPreparePhase
  ) throws StandardException {
    assert ignore;
    if (activation == null && operand.isWhereClauseDynamic()) {
      return new ParameterizedConditionsWrapperQueryInfo(this, operand);
    }
    else {
      Iterator<AbstractConditionQueryInfo> itr = constantConditions.iterator();
      AbstractConditionQueryInfo temp;
      AbstractConditionQueryInfo mergedOp = null;
      while (itr.hasNext()) {
        temp = itr.next();
        mergedOp = temp
            .createOrAddToGroup(operand, false /*
                                                 * do not create
                                                 * ConstantConditionsWrapper if
                                                 * merge is not possible
                                                 */, activation, false);
        if (mergedOp != null) {
          // If mergedOp is not null and same as temp, that means the operand
          // has been
          // annihilated by temp or merged in temp (in case of RangeQuerYinfo)
          // & so should not even add the current operand.
          // If it is not null and different than temp, that means reverse & we
          // should
          // remove temp and add mergedOp instead
          if (mergedOp != temp) {
            itr.remove();
            constantConditions.add(mergedOp);
          }
          break;
        }
      }
      // The operand could not be merged so add it to the existing list
      if (mergedOp == null) {
        constantConditions.add(operand);
      }
    }
    return this;
  }

 
  
  @Override
  public void computeNodes(Set<Object> routingKeys, Activation activation,
      boolean forSingleHopPreparePhase) throws StandardException {
    //The List could contain RangeQueryInfo or ComparisonQueryInfo
    //This object implies we are part of AND junction so
    // we can do valid interection here
    assert routingKeys.size() ==1 ;
    assert routingKeys.contains(ResolverUtils.TOK_ALL_NODES) ;
    Set<Object> nodesCollector = new HashSet<Object>();
    nodesCollector.add(ResolverUtils.TOK_ALL_NODES);
    Iterator<AbstractConditionQueryInfo> itr = this.constantConditions.iterator();
    while(itr.hasNext()) {
      itr.next().computeNodes(nodesCollector, activation, forSingleHopPreparePhase);
      if( !nodesCollector.contains(ResolverUtils.TOK_ALL_NODES)) {
        if(routingKeys.contains(ResolverUtils.TOK_ALL_NODES)) {
          //This is the first computtion of nodes so remove
          // default all nodes & add the specific prunned nodes
          routingKeys.remove(ResolverUtils.TOK_ALL_NODES);
          routingKeys.addAll(nodesCollector);
        }else {
          //Just keep intersection
          routingKeys.retainAll(nodesCollector);
        }
        nodesCollector.clear();
        nodesCollector.add(ResolverUtils.TOK_ALL_NODES);
      }/*else {
        //Ignore. The condition will not cause any prunning        
      }*/
      
    }
  }

  /**
   * Test API only
   * 
   * @return Returns a List containing Objects of type RangeQueryInfo or
   *         ComparisonQueryInfo
   * 
   */
  List getOperands() {
    return Collections.unmodifiableList(this.constantConditions);
  }



  @Override
  boolean isWhereClauseDynamic() {
    return false;
  }
  
}

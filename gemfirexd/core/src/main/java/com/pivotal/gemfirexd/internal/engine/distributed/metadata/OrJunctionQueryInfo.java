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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import com.gemstone.gemfire.LogWriter;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Collections;
import java.util.Set;

/**
 * This class contains a flat structue of all the ComparisonQueryInfo Objects
 * and at the max, one JunctionQueryInfo . A presence of JunctionQueryInfo as an
 * operand present in this class, necessarily means that the child
 * JunctionQueryInfo is of type opposite to this JunctionQueryInfo. A
 * JunctionQueryInfo is formed if there exist more than one conditions in the
 * where clause
 * 
 * 
 * @author Asif
 * 
 */
public class OrJunctionQueryInfo extends JunctionQueryInfo {

  //If there is any child JunctionQueryInfo , it will be present at 0 index
  private final List<AbstractConditionQueryInfo> operands = new ArrayList<AbstractConditionQueryInfo>(
      5);

  OrJunctionQueryInfo(AbstractConditionQueryInfo op1,
      AbstractConditionQueryInfo op2) {
    //int pos1 = op1.getActualColumnPostionOfOperand();
    //int pos2 = op2.getActualColumnPostionOfOperand();
    this.setIsDynamic( op1.isWhereClauseDynamic() || op2.isWhereClauseDynamic());
    this.setIsStaticallyNotGetConvertible(true);
    this.operands.add(op1);
    this.operands.add(op2);
  }

  OrJunctionQueryInfo(int junctionType, JunctionQueryInfo jqi,
      AbstractConditionQueryInfo op1) {
    super(jqi);
    if(op1 != null) {
      if(!this.isWhereClauseDynamic()) {
        this.setIsDynamic(op1.isWhereClauseDynamic());
      }    
      this.operands.add(op1);
    }else {
      this.setIsStaticallyNotGetConvertible(jqi.isStaticallyNotGetConvertible());
    }
    //  Asif : Creation of a junction using this constructor 
    //  necessarily means that there is a combination of OR and AND and so cannot be converted into
    // a Region.get
    // ( Is it always true ?)    

  }

  OrJunctionQueryInfo(JunctionQueryInfo jqi1, JunctionQueryInfo jqi2) {
    super(jqi1, jqi2);
    assert jqi1.getJunctionType() == QueryInfoConstants.AND_JUNCTION;
    assert jqi2.getJunctionType() == QueryInfoConstants.AND_JUNCTION;
    if ((this.colocationFailReason = jqi1
        .isEquiJoinColocationCriteriaFullfilled(null)) == null
        && (this.colocationFailReason = jqi2
            .isEquiJoinColocationCriteriaFullfilled(null)) == null) {
      this.colocationCriteria = jqi1.colocationCriteria;
    }
    // We could have a case where 
    //                            OR
    //                             |
    //                          --------
    //                          |       |
    //                        AND      AND
                            
     // and both the AND are get convertible. In such case the query is Get convertible.
    //Override the default Behaviour of JunctionQueryInfo  where it is set as statically 
    // not get convertible as true

    //Override the default true of JunctionQueryInfo constructor
    this.setIsStaticallyNotGetConvertible(jqi1.isStaticallyNotGetConvertible()
        || jqi2.isStaticallyNotGetConvertible());
  }

  @Override
  JunctionQueryInfo mergeNonJunctionOperand(AbstractConditionQueryInfo operand,
      int operandJunctionType) throws StandardException {
    //If an OR junction is added with a non junction operand , it would mean it is statically
    // not get convertible.
    // It is to be noted that for same column based OR conditions , the tree create a
    // ListNode ( InQueryInfo) so that means that if an OR junction is created it is
    // bound to be made of different columns & so it will be statically not get convertible.
                               
    this.setIsStaticallyNotGetConvertible(true);
    if (this.getJunctionType() == operandJunctionType) {
      this.operands.add(operand);
      //this.isStatcNotGetConvertible = operand.getActualColumnPostionOfOperand() == -1;
      if( !this.isWhereClauseDynamic()) {
        this.setIsDynamic(operand.isWhereClauseDynamic());  
      }     
      return this;
    }
    else {
      return new AndJunctionQueryInfo(operandJunctionType, this, operand);
    }

  }

  /*
   Object isConvertibleToGet(int pkColumn, int tableNumber)
   throws StandardException {
   throw new UnsupportedOperationException(
   "For JunctionQueryInfo this method should not have been invoked");
   }*/

  @Override
  Object isConvertibleToGet(int[][] pkColumns, TableQueryInfo tqi)
      throws StandardException {
    Object[] retType = null;   
    if (!this.isStaticallyNotGetConvertible()) {
      assert this.children != null;
      retType = new Object[this.children.length];
      // Each individual operand should be primary key convertible
      assert this.operands.isEmpty();
      for( int i = 0 ; i < this.children.length ;++i) {
        Object aKey = this.children[i].isConvertibleToGet(pkColumns, tqi);
        if( aKey == null) {
          retType = null;
          break;
        }else {
          retType[i] = aKey;
        }
      } 
    }  
    return retType;

  }
  
  /*
   * (non-Javadoc)
   * @see isConvertibleToGet(...)
   */
  @Override
  Object isConvertibleToGetOnLocalIndex(int[][] ikColumns, TableQueryInfo tqi)
      throws StandardException {
    Object[] retType = null;
    if (!this.isStaticallyNotGetConvertible()) {
      assert this.children != null;
      retType = new Object[this.children.length];
      // Each individual operand should be primary key convertible
      assert this.operands.isEmpty();
      for (int i = 0; i < this.children.length; ++i) {
        Object aKey = this.children[i].isConvertibleToGetOnLocalIndex(
            ikColumns, tqi);
        if (aKey == null) {
          retType = null;
          break;
        }
        else {
          retType[i] = aKey;
        }
      }
    }
    return retType;
  }

  /**
   * Test API only
   * 
   * @return Returns a List containing ComparisonQueryInfo & JunctionQueryInfo
   *         Objects in the AND or OR Junction
   * 
   */
  @Override
  List<AbstractConditionQueryInfo> getOperands() {
    return Collections.unmodifiableList(this.operands);
  }

  /**
   * Test API
   * 
   * @return int indicating the type of junction ( AND orOR)
   */
  @Override
  int getJunctionType() {
    return QueryInfoConstants.OR_JUNCTION;
  }
  
  @Override
  public void computeNodes(Set<Object> routingKeys, Activation activation,
      boolean forSingleHopPreparePhase) throws StandardException {
    LogWriter logger = Misc.getCacheLogWriter();
    // We need to use only column based operands.
    // Ideally it would make sense to call compute nodes
    // only on those columns which have some global index or are partitioning
    // key.
    // Since it is not possible to get handle of Region here , we will for time
    // being
    // iterate over the map
    assert routingKeys.size() == 1;
    assert routingKeys.contains(ResolverUtils.TOK_ALL_NODES);
    boolean isPruningPossible = true;
    if (logger.fineEnabled()) {
      logger
          .fine("OrJunctionQueryInfo::computeNodes: Before prunning node  size = "
              + routingKeys.size());
      logger
          .fine("OrJunctionQueryInfo::computeNodes: Before prunning nodes are"
              + routingKeys);
    }
    try {
      List<Object> tempCollector = new ArrayList<Object>(
          10);
      Iterator<AbstractConditionQueryInfo> itr = this.operands.iterator();
      AbstractConditionQueryInfo acqi;
      // If even one of the condition returns TOK_ALL nodes, no prunning
      // will be possible
      Set<Object> nodesCollector = new HashSet<Object>();
      nodesCollector.add(ResolverUtils.TOK_ALL_NODES);
      isPruningPossible = true;
      while (itr.hasNext()) {
        acqi = itr.next();
        assert acqi instanceof ComparisonQueryInfo;
        acqi.computeNodes(nodesCollector, activation, forSingleHopPreparePhase);
        if (nodesCollector.contains(ResolverUtils.TOK_ALL_NODES)) {
          // No prunning possible
          isPruningPossible = false;
          break;
        }
        else {
          tempCollector.addAll(nodesCollector);
          nodesCollector.clear();
          nodesCollector.add(ResolverUtils.TOK_ALL_NODES);
        }
      }
      if (isPruningPossible) {
        assert nodesCollector.size() == 1;
        assert nodesCollector.contains(ResolverUtils.TOK_ALL_NODES);
        if (this.children != null) {
          for (int i = 0; i < this.children.length; ++i) {
            this.children[i].computeNodes(nodesCollector, activation, forSingleHopPreparePhase);
            if (nodesCollector.contains(ResolverUtils.TOK_ALL_NODES)) {
              // return no prunning possible
              return;
            }
            else {
              tempCollector.addAll(nodesCollector);
            }
            nodesCollector.clear();
            nodesCollector.add(ResolverUtils.TOK_ALL_NODES);
          }
        }
        // we are here implying prunning is possible
        routingKeys.remove(ResolverUtils.TOK_ALL_NODES);
        routingKeys.addAll(tempCollector);
      }
    }
    finally {
      if (logger.fineEnabled()) {
        logger
            .fine("AndJunctionQueryInfo::computeNodes: After prunning nodes are"
                + routingKeys);
      }
    }
  }  

  @Override
  public String isEquiJoinColocationCriteriaFullfilled(TableQueryInfo ncjTqi) {
    // each of the operands should satisfy the colocation criteria for
    // the OR junction
    String retVal = null;
    for (AbstractConditionQueryInfo acqi : this.operands) {
      if ((retVal = acqi.isEquiJoinColocationCriteriaFullfilled(ncjTqi)) != null) {
        return retVal;
      }
    }
    if (this.children != null) {
      for (JunctionQueryInfo jqi : this.children) {
        if ((retVal = jqi.isEquiJoinColocationCriteriaFullfilled(ncjTqi)) != null) {
          return retVal;
        }
      }
    }
    return null;
  }
}

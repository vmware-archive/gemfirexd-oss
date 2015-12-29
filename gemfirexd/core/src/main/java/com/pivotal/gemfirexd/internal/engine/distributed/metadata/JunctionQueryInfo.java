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

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

import java.util.ArrayList;
import java.util.List;

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
public abstract class JunctionQueryInfo extends AbstractConditionQueryInfo {

  
  //The IS_DYNAMIC indicates that there is atleast one
  // ComparisonQueryInfo which is parameterized . The boolean does
  // not depend upon the state of child JunctionQueryInfo present if any.
  // So this boolean can be false, even if the child JunctionQueryInfo is
  // overall dynamic.
  //Note that IS_DYNAMIC only refers to the dynamic nature of the JunctionQueryInfo clause
  


  final static byte   IS_DYNAMIC = 0x1, IS_STATIC_NOT_CONVERTIBLE = 0x2, HAS_IN_PRED= 0x4;
  
  private  byte state = 0x0;
  
  //Though ideally we would want a junction to have exactly one child (like OR having an AND or vice versa),
  // It may be difficult to merge two junctions where each of them has valid child, in that
  //sense it would need merging of children. So instead of merging we will just add
  // the child junctions. Though at a given level of Junction, its children must be of same type
  
  JunctionQueryInfo[] children ;  
  //The colocation criteria will have not null/ meaningful value most of the time only
  // for ANDJunction. 
  //But in a special case, even an ORJunction can be executed even if it has equi
  //join condition, so the colocation matrix is moved to JunctionQueryInfo level.
  // The case is     OR
  //                 |
  //             ---------
  //             |        |
  //           AND       AND
  //Here if both the AND conditions satisfy colocation criteria, the OR will also 
  // satisfy 
  //Asif: I do not think it needs to be volatile as the access is confined to single thread at a time
  //which is guarded by a lock at higher level
  ColocationCriteria colocationCriteria ;
  String colocationFailReason;


  JunctionQueryInfo( JunctionQueryInfo jqi1,
      JunctionQueryInfo jqi2) { 
    this.children = new JunctionQueryInfo[2];
    
    this.children[0]=jqi1;
    this.children[1]=jqi2;
    assert jqi1.getJunctionType() == jqi2.getJunctionType();
    //  Asif : Creation of a junction using this constructor 
    //  necessarily means that there is a combination of OR and AND and so cannot be converted into
    // a Region.get
    // ( Is it always true ?)
    this.setIsStaticallyNotGetConvertible(true);
    
    this.setIsDynamic(jqi1.isWhereClauseDynamic() || jqi2.isWhereClauseDynamic());
     //No need to set the bit for the IN predicate
  }
  
  JunctionQueryInfo( JunctionQueryInfo jqi)  {
    this.children = new JunctionQueryInfo[1];
    this.children[0] = jqi;
    this.setIsStaticallyNotGetConvertible(true);
    this.setIsDynamic(jqi.isWhereClauseDynamic());
//  No need to set the IN predicate clause
  }
  
  JunctionQueryInfo( )  {
    //The value at index zero may still be null
    //this.children =new JunctionQueryInfo[1];
    this.children = null;
  }
  
  @Override
  final AbstractConditionQueryInfo mergeOperand(AbstractConditionQueryInfo operand,
      int operandJunctionType, boolean isTopLevel) throws StandardException {
    // If the operand is null then just return this
    // else if the operand is ComaprisonQueryInfo , then create a new junction.
    // else if the operand is JunctionQueryInfo of the type same as that passed
    // then add to the junction
    if (operand == null) {
      if(this.getJunctionType() == operandJunctionType || isTopLevel) {
        return this;
      }else {
        return (operandJunctionType == QueryInfoConstants.AND_JUNCTION) ? new AndJunctionQueryInfo(QueryInfoConstants.AND_JUNCTION,this,null)
        : new OrJunctionQueryInfo(QueryInfoConstants.OR_JUNCTION,this,null);
      }
    }
    // turn new JunctionQueryInfo(this, operand, operandJunctionType);
    return operand.mergeJunctionOperand(this, operandJunctionType);
  }
  
  @Override
  JunctionQueryInfo mergeJunctionOperand(JunctionQueryInfo junctionOperand,
      int operandJunctionType) {
    JunctionQueryInfo root = null;
    JunctionQueryInfo localChild = null;
    
    if (junctionOperand.getJunctionType() == this.getJunctionType() ) {
        if(operandJunctionType != this.getJunctionType()) {
         root = (operandJunctionType == QueryInfoConstants.AND_JUNCTION) ? new AndJunctionQueryInfo(this,junctionOperand): new OrJunctionQueryInfo(this,junctionOperand);
        }else {
          //TODO:The LHS & RHS need to be merged 
          
          throw new UnsupportedOperationException("Not supported yet");
        }
    }
    else {
      if (operandJunctionType == this.getJunctionType()) {
        root = this;
        localChild = junctionOperand;
        if(!root.isStaticallyNotGetConvertible()) {
          root.setIsStaticallyNotGetConvertible(localChild.isStaticallyNotGetConvertible());
        }
      }
      else if (operandJunctionType == junctionOperand.getJunctionType()) {
        root = junctionOperand;
        localChild = this;
        if(root.getJunctionType() == QueryInfoConstants.OR_JUNCTION) {
          if(!root.isStaticallyNotGetConvertible()) {
            root.setIsStaticallyNotGetConvertible(localChild.isStaticallyNotGetConvertible());
          } 
        }else {
          root.setIsStaticallyNotGetConvertible(true);
        }        
      }
      
      if(root.children == null) {
        root.children = new JunctionQueryInfo[]{localChild};
      }else {
        JunctionQueryInfo [] newChildren = new JunctionQueryInfo[root.children.length+1];
        int i =0;
        for(; i < root.children.length;++i ) {
          JunctionQueryInfo temp = root.children[i];
          assert temp.getJunctionType() == localChild.getJunctionType();
          newChildren[i] = root.children[i];          
        }
        newChildren[i] = localChild;
        root.children = newChildren;
      }     
      
      
      if(!root.isWhereClauseDynamic()) {
        root.setIsDynamic(localChild.isWhereClauseDynamic());
      }
    }   
    
    return root;

  }

  @Override
  final boolean isStaticallyNotGetConvertible() {
    return GemFireXDUtils.isSet(this.state , IS_STATIC_NOT_CONVERTIBLE)  ;    
  } 
 
  final void setIsStaticallyNotGetConvertible(boolean on) {
    this.state = GemFireXDUtils.set(this.state , IS_STATIC_NOT_CONVERTIBLE, on)  ;    
  } 
  
  final void setIsDynamic(boolean on) {
    this.state = GemFireXDUtils.set(this.state ,IS_DYNAMIC, on)  ;    
  }
  
  /* we will set this flag only if there is a possibility of
   * statment being primary key type. Otherwise we will 
   * not set this flag
   */
  final void setHasINPredicate(boolean on) {
    this.state = GemFireXDUtils.set(this.state ,HAS_IN_PRED, on)  ;    
  }
  
  @Override
  public final boolean isWhereClauseDynamic() {
     return GemFireXDUtils.isSet(this.state, IS_DYNAMIC);    
  }
  public final boolean hasINPredicate() {
    return GemFireXDUtils.isSet(this.state, HAS_IN_PRED);    
 }
  abstract int getJunctionType() ;
  
  /**
   * Test API only
   * 
   * @return Returns a List containing ComparisonQueryInfo & JunctionQueryInfo
   *         Objects in the AND or OR Junction
   * 
   */
  abstract List getOperands() ;

  
}

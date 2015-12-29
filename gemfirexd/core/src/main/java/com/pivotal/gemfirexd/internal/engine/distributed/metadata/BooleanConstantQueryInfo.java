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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

/**
 * The instance of this class represents a constant boolean condition
 * @since GemFireXD
 * @author Asif
 * @see BooleanConstantNode
 *
 */
public class BooleanConstantQueryInfo extends AbstractConditionQueryInfo{
  private final boolean value;
  
  public BooleanConstantQueryInfo(boolean val) {
    this.value = val;  
  }
  AbstractConditionQueryInfo addOrCreateToJunction(AbstractConditionQueryInfo operand, int operandJunctionType) {
    throw new UnsupportedOperationException();
  }

  @Override
  int getActualColumnPostionOfOperand() {
    return -1;
  }

  @Override
  Object isConvertibleToGet(int pkColumn, TableQueryInfo tqi)
      throws StandardException {
    return false;
  }

  @Override
  Object isConvertibleToGet(int[][] pkColumns, TableQueryInfo tqi) throws StandardException {    
    return null;
  }
  
  /*
   * (non-Javadoc)
   * @seeisConvertibleToGet(...)
   */
  @Override
  Object isConvertibleToGetOnLocalIndex(int ikColumn, TableQueryInfo tqi)
      throws StandardException {
    return null;
  }
  
  /*
   * (non-Javadoc)
   * @seeisConvertibleToGet(...)
   */
  @Override
  Object isConvertibleToGetOnLocalIndex(int[][] ikColumns, TableQueryInfo tqi)
      throws StandardException {
    return null;
  }
  
  @Override
  JunctionQueryInfo mergeJunctionOperand(JunctionQueryInfo junctionOperand,
      int operandJunctionType) throws StandardException {
    return junctionOperand.mergeNonJunctionOperand(this, operandJunctionType);
  }

  @Override
  JunctionQueryInfo mergeNonJunctionOperand(AbstractConditionQueryInfo operand,
      int operandJunctionType) throws StandardException {
    return operandJunctionType == QueryInfoConstants.AND_JUNCTION ?new AndJunctionQueryInfo(this, operand):new OrJunctionQueryInfo(this,operand);
  }

 
  @Override
  boolean isStaticallyNotGetConvertible() {
    return true;
  }
  @Override
  AbstractConditionQueryInfo mergeOperand(AbstractConditionQueryInfo operand,
      int operandJunctionType, boolean isTopLevel) throws StandardException {
    // If the operand is null then just return this
    // else if the operand is ComaprisonQueryInfo , then create a new junction.
    // else if the operand is JunctionQueryInfo of the type same as that passed
    // then add to the junction
    if (operand == null) {
      return this;
    }    
    return operand.mergeNonJunctionOperand(this, operandJunctionType);
  }
  @Override
  boolean isWhereClauseDynamic() {    
    return false;
  }
  

}

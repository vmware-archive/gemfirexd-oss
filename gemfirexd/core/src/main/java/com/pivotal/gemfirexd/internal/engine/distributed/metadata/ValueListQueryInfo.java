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

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.shared.common.ColumnRoutingObjectInfo;

/**
 * Class wrapping the multiple instances of type ValueQueryInfo. 
 * It is used to wrap the RHS part of the IN clause.
 * Also it is used as a super class to create a transient structure
 * for storing the multi partitioning keys which is used for node computation
 * @see InQueryInfo
 * @see AndJunctionQueryInfo
 * @author Asif
 *
 */
public class ValueListQueryInfo extends AbstractQueryInfo implements ValueQueryInfo {

  private final ValueQueryInfo [] valueArray ;
  private final boolean isListDynamic ;
 
  public ValueListQueryInfo(ValueQueryInfo[] vqiArr, boolean isListDynamic) {
    this.valueArray = vqiArr;
    this.isListDynamic = isListDynamic;
  }

  /** This class implements ValueQueryInfo interface which contains this method.
   * Both ConstantQueryInfo and ParameterQueryInfo{@link ParameterQueryInfo#evaluate(Activation)} implement this interface. The primary
   * reason for this is to distinguish those QueryInfo objects which evaluate to a yield
   * an underlying value ( of constant nature) . It helps avoid instanceof check &
   * extra type casting 
   * 
   */
  //This method is baecse ValueQueryInfo interface implemented which in turn
  // is to avoid instance of. Is there a better method?
  //TODO:Asif: Should we cache the Object[] created 
  
  public Object[] evaluate(Activation activation) throws StandardException {
    int len = valueArray.length;
    Object[] retVal = new Object[len];
    for(int i = 0 ; i < len;++i) {
      retVal[i] = this.valueArray[i].evaluate(activation);   
    }
    return retVal;
  }
  
  int getSize() {
    return this.valueArray.length;
  }
  
  @Override
  public boolean isDynamic() {
    return this.isListDynamic;
  }
  
  //TODO:Asif: Should we cache the DataValueDescriptor[] object
  @Override
  public Object[] getPrimaryKey() throws StandardException {
    int len = this.valueArray.length;
    Object[] pkArrr = new Object[len];   
    for(int i = 0 ; i < len;++i) {
      pkArrr[i] = ((QueryInfo)this.valueArray[i]).getPrimaryKey();   
    }
    return pkArrr;
  }
  
  @Override
  public Object[] getIndexKey() throws StandardException {
    int len = this.valueArray.length;
    Object[] pkArrr = new Object[len];
    for (int i = 0; i < len; ++i) {
      pkArrr[i] = ((QueryInfo)this.valueArray[i]).getIndexKey();
    }
    return pkArrr;
  }
  
  /**
   * Previously it was for Test API purpose only, but now using it the code
   * @return
   */
  ValueQueryInfo[] getOperands() {    
    return this.valueArray;
  }

  public DataValueDescriptor evaluateToGetDataValueDescriptor(Activation activation) throws StandardException
  {
    throw new UnsupportedOperationException("This method is not supported . Invoke evaluateToGetDataValueDescriptorArray(Activation) instead.");
  }

  public DataValueDescriptor[] evaluateToGetDataValueDescriptorArray(
      Activation activation) throws StandardException {
    int len = this.valueArray.length;
    DataValueDescriptor[] dvdArr = new DataValueDescriptor[len];
    for (int i = 0; i < len; ++i) {
      dvdArr[i] = this.valueArray[i]
          .evaluateToGetDataValueDescriptor(activation);
    }
    return dvdArr;
  }

  public ColumnRoutingObjectInfo[] evaluateToGetColumnInfos(
      Activation activation) throws StandardException {
    int len = this.valueArray.length;
    ColumnRoutingObjectInfo[] ret = new ColumnRoutingObjectInfo[len];
    for (int i = 0; i < len; i++) {
      ret[i] = ComparisonQueryInfo.getAppropriateRoutingObjectInfo(activation,
          this.valueArray[i], null);
    }
    return ret;
  }
  
  public int typeOfValue() {
    return ValueQueryInfo.MIX_OF_PARAMETER_CONSTANT_VALUE;
  }

  public int evaluateToGetParameterNumber(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetParameterNumber should not be called for ValueLiastQueryInfo");
  }

  public int[] evaluateToGetParameterArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetParameterArray should not be called for ValueLiastQueryInfo");
  }

  public DataValueDescriptor evaluateToGetConstant(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetConstant should not be called for ValueLiastQueryInfo");
  }

  public DataValueDescriptor[] evaluateToGetConstantArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetConstantArray should not be called for ValueLiastQueryInfo");
  }

  public Object[] evaluateToGetObjectArray(Activation activation)
      throws StandardException {
    int len = this.valueArray.length;
    Object[] objArr = new Object[len];
    for (int i = 0; i < len; ++i) {
      int type = this.valueArray[i].typeOfValue();
      switch (type) {
        case ValueQueryInfo.PARAMETER_VALUE:
          objArr[i] = Integer.valueOf(this.valueArray[i]
              .evaluateToGetParameterNumber(activation));
          break;

        case ValueQueryInfo.CONSTANT_VALUE:
        case ValueQueryInfo.PARAMETERIZED_CONSTANT_VALUE:
          objArr[i] = this.valueArray[i].evaluateToGetConstant(activation);
          break;

        default:
          throw new GemFireXDRuntimeException("ValueQueryInfo type: " + type
              + " not expected here");
      }
    }
    return objArr;
  }

  @Override
  public int getParamType(Activation activation) throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  public int getMaximumWidth() {
    return 0;
  }
}

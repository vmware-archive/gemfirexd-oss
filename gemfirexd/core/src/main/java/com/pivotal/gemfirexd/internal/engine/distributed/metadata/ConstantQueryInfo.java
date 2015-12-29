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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.*;
import com.pivotal.gemfirexd.internal.shared.common.ColumnRoutingObjectInfo;

/**
 * @author Asif
 * @see ConstantNode#computeQueryInfo()
 *
 */
public class ConstantQueryInfo extends AbstractQueryInfo implements ValueQueryInfo {

  private DataValueDescriptor value;
  private int maxWidth;

  public ConstantQueryInfo(DataValueDescriptor value) {
    this.value = value;  
  }
  
  //TODO:Asif: Is it Ok to implement it as getPrimaryKey() ?
  @Override
  public DataValueDescriptor getPrimaryKey() throws StandardException {
    return this.value;
  }
  
  @Override
  public DataValueDescriptor getIndexKey() throws StandardException {
    return this.value;
  }

  public DataValueDescriptor getValue() {
    return this.value;
  }

  public void setValue(DataValueDescriptor newValue) {
    this.value = newValue;
  }

  /** This class implements ValueQueryInfo interface which contains this method.
   * Both ConstantQueryInfo and ParameterQueryInfo{@link ParameterQueryInfo#evaluate(Activation)} implement this interface. The primary
   * reason for this is to distinguish those QueryInfo objects which evaluate to yield
   * an underlying value ( of constant nature ) . It helps avoid instance of check &
   * extra type casting 
   * 
   */
  //This method is baecse ValueQueryInfo interface implemented which in turn
  // is to avoid instance of. Is there a better method?
  public Object evaluate(Activation activation) throws StandardException {
    return this.value.getObject();
  }
  
  @Override
  public String toString() {
    try {
      // Rahul : otherwise tostring can cause null pointer.
      if (this.value == null) {
        return "null" ;
      }
      return this.value.getObject().toString();
    }catch(StandardException se) {
      return se.toString();
    }
  }

  /**
   * The caller if intends to use the DataValueDescriptor
   * should clone the DataValueDescriptor object.
   */
  public DataValueDescriptor evaluateToGetDataValueDescriptor(Activation activation) throws StandardException
  {
    return this.value;
  }

  public DataValueDescriptor[] evaluateToGetDataValueDescriptorArray(
      Activation activation) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supported . Invoke evaluateToGetDataValueDescriptor(Activation) instead.");
  }

  public int typeOfValue() {
    return ValueQueryInfo.CONSTANT_VALUE;
  }

  public int evaluateToGetParameterNumber(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetParameterNumber should not be called for ConstantQueryInfo");
  }

  public int[] evaluateToGetParameterArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetParameterArray should not be called for ConstantQueryInfo");
  }

  public DataValueDescriptor evaluateToGetConstant(Activation activation)
      throws StandardException {
    return this.value;
  }

  public DataValueDescriptor[] evaluateToGetConstantArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetConstantArray should not be called for ConstantQueryInfo");
  }

  public Object[] evaluateToGetObjectArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetObjectArray should not be called for ConstantQueryInfo");
  }

  @Override
  public int getParamType(Activation activation) throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  @Override
  public ColumnRoutingObjectInfo[] evaluateToGetColumnInfos(
      Activation activation) {
    // TODO Auto-generated method stub
    return null;
  }

  public int getMaximumWidth() {
    return this.maxWidth;
  }

  public void setMaxWidth(int maxWidth) {
    this.maxWidth = maxWidth;
  }

}

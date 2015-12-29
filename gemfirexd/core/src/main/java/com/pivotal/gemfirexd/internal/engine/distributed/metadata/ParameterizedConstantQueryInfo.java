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
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.shared.common.ColumnRoutingObjectInfo;

public final class ParameterizedConstantQueryInfo extends AbstractQueryInfo
    implements ValueQueryInfo {

  private static final long serialVersionUID = 1L;

  private final int constantIndex;
  //TODO : Asif: after stabilization see if we can do away with constant index 
  private final int indexRelativeToQuery;

  public ParameterizedConstantQueryInfo(int constantIndex, int indexRelativeToQuery) {
    this.constantIndex = constantIndex;
    this.indexRelativeToQuery = indexRelativeToQuery;
  }

  public final int getConstantIndex() {
    return this.constantIndex;
  }

  @Override
  public boolean isDynamic() {
    return true;
  }

  @Override
  public Object getPrimaryKey() {
    return new PrimaryDynamicKey(this);
  }
  
  @Override
  public Object getIndexKey() {
    return new PrimaryDynamicKey(this);
  }

  @Override
  public Object evaluate(Activation activation) throws StandardException {
    return activation.getParameterValueSet().getParameter(this.indexRelativeToQuery);
  }

  @Override
  public DataValueDescriptor evaluateToGetDataValueDescriptor(
      Activation activation) throws StandardException {
    final ParameterValueSet pvs = activation.getParameterValueSet();
    assert pvs != null;
    return pvs.getParameter(this.indexRelativeToQuery);
  }

  @Override
  public DataValueDescriptor[] evaluateToGetDataValueDescriptorArray(
      Activation activation) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supported . Invoke evaluateToGetDataValueDescriptor(Activation) instead.");
  }

  @Override
  public int typeOfValue() {
    return ValueQueryInfo.PARAMETERIZED_CONSTANT_VALUE;
  }

  @Override
  public int evaluateToGetParameterNumber(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetParameterNumber should not be called for ParameterizedConstantQueryInfo");
  }

  @Override
  public int[] evaluateToGetParameterArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetParameterArray should not be called for ParameterizedConstantQueryInfo");
  }

  @Override
  public DataValueDescriptor evaluateToGetConstant(Activation activation)
      throws StandardException {
    return evaluateToGetDataValueDescriptor(activation);
  }

  @Override
  public DataValueDescriptor[] evaluateToGetConstantArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetConstantArray is not supported . "
            + "Invoke evaluateToGetDataValueDescriptor(Activation) instead.");
  }

  @Override
  public Object[] evaluateToGetObjectArray(Activation activation)
      throws StandardException {
    throw new UnsupportedOperationException(
        "evaluateToGetObjectArray should not be called for ParameterizedConstantQueryInfo");
  }

  public int getParamType(Activation activation) throws StandardException {
    throw new UnsupportedOperationException("Override the method appropriately");
  }

  @Override
  public ColumnRoutingObjectInfo[] evaluateToGetColumnInfos(
      Activation activation) {
    return null;
  }

  public int getMaximumWidth() {
    return 0;
  }

  @Override
  public int hashCode() {
    return this.constantIndex * 2; // avoid clashes with integers
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other.getClass() == ParameterizedConstantQueryInfo.class) {
      return this.constantIndex == ((ParameterizedConstantQueryInfo)other)
          .getConstantIndex();
    }
    return false;
  }
}

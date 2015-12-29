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
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.shared.common.ColumnRoutingObjectInfo;

/**
 * The interface is implemented by ConstantQueryInfo and ParameterQueryInfo
 * This represents an instance of value which may be constants or 
 * its value can be obtained from activation
 * @author Asif
 * @see ConstantQueryInfo
 * @see ParameterQueryInfo
 *
 */
public interface ValueQueryInfo extends QueryInfo {

  public static int PARAMETER_VALUE = 0;
  
  public static int CONSTANT_VALUE = 1;
  
  public static int PARAMETERIZED_CONSTANT_VALUE = 2;
  
  public static int MIX_OF_PARAMETER_CONSTANT_VALUE = 3;
  
  public Object evaluate(Activation activation) throws StandardException;

  /**
   * The caller if intends to use the DataValueDescriptor
   * should clone the DataValueDescriptor object.
   * Has meaningful implementation in {@link ConstantQueryInfo}  and {@link ParameterQueryInfo}
   * 
   */
  public DataValueDescriptor evaluateToGetDataValueDescriptor( Activation activation) throws StandardException;
  
  public DataValueDescriptor[] evaluateToGetDataValueDescriptorArray(
      Activation activation) throws StandardException;

  public ColumnRoutingObjectInfo[] evaluateToGetColumnInfos(Activation activation) throws StandardException;
  
  public int typeOfValue();
  
  public int evaluateToGetParameterNumber( Activation activation) throws StandardException;
  
  public int[] evaluateToGetParameterArray(Activation activation)
      throws StandardException;

  public DataValueDescriptor evaluateToGetConstant(Activation activation)
      throws StandardException;

  public DataValueDescriptor[] evaluateToGetConstantArray(Activation activation)
      throws StandardException;
  
  public Object[] evaluateToGetObjectArray(Activation activation)
      throws StandardException;
  
  public int getParamType(Activation activation) throws StandardException;
  
  public int getMaximumWidth();
}

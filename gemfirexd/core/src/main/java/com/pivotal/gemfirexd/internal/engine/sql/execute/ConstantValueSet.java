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
package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.util.List;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;

/**
 * 
 * @author Shoubhik
 * 
 */
public interface ConstantValueSet extends ParameterValueSet {

  int[] getTokenKinds();

  String[] getTokenImages();

  DataTypeDescriptor[] getParamTypes();

  List<TypeCompiler> getOrigTypeCompilers();

  void setActivation(Activation ownedActivation);

  void refreshTypes(DataTypeDescriptor[] dtds);

  String getConstantImage(int position);

  int validateParameterizedData() throws StandardException;
}

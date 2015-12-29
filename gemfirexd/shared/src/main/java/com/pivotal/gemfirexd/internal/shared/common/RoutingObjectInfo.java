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
package com.pivotal.gemfirexd.internal.shared.common;

import java.io.Externalizable;

/**
 *
 * @author kneeraj
 *
 */
public interface RoutingObjectInfo extends Externalizable {

  public static int CONSTANT = 1;
  
  public static int PARAMETER = 2;
  
  public static int LIST = 3;
  
  public static int RANGE = 4;
  
  public static int INVALID = 8;
  
  public boolean isValueAConstant();
  
  public Object getConstantObjectValue();
  
  public int getParameterNumber();
  
  //public Object getRoutingObject(Object value, ClientResolver resolver);
  
  
  /**
   * This is added so that at the end of collecting all the routing object 
   * info it can be seen whether the same resolver is being used for calculating 
   * all the routing object info. 
   * @return
   */
  public Object getResolver();
}

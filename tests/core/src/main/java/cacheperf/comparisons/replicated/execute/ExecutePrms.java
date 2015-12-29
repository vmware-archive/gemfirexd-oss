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

package cacheperf.comparisons.replicated.execute;

import com.gemstone.gemfire.cache.execute.Function;
import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.HydraConfigException;

/**
 * A class used to store keys for test configuration settings.
 */
public class ExecutePrms extends BasePrms {

  /**
   * (String)
   * Class name of function to register with the function execution service.
   * Assumes that the class has a default no-argument constructor.  Required.
   */
  public static Long function;
  public static Function getFunction() {
    Long key = function;
    String classname = tasktab().stringAt(key, tab().stringAt(key));
    try {
      return (Function)AbstractDescription.getInstance(key, classname);
    } catch (ClassCastException e) {
      String s = BasePrms.nameForKey(key)
               + " does not implement Function: " + classname;
      throw new HydraConfigException(s);
    }
  }

  
  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(ExecutePrms.class);
  }
}

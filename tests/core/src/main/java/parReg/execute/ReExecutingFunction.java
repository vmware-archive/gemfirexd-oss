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
package parReg.execute;

import util.TestException;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;

public class ReExecutingFunction extends FunctionAdapter {

  @Override
  public void execute(FunctionContext context) {
    if (!(context instanceof RegionFunctionContext)) {
      throw new TestException(
          "Test issue - ReExecutionFunction should be done with onRegion");
    }

    RegionFunctionContext regionContext = (RegionFunctionContext)context;

    try {
      if (!regionContext.isPossibleDuplicate()) {
        throw new IllegalStateException(
            "Test decided - This function cannot be executed with isPossibleDuplicate as false");
      }
      else {
        hydra.Log.getLogWriter().info("Executed successfully on the node");
        regionContext.getResultSender().lastResult(
            "Executed successfully on the node");
      }
    }
    catch (IllegalStateException hasToBeReexecutedException) {
      hydra.Log.getLogWriter().info("Throwing FITE by the function");
      throw new FunctionInvocationTargetException(hasToBeReexecutedException);
    }
  }

  @Override
  public String getId() {
    return "ReExecutingFunction";
  }

  public boolean isHA() {
    return true;
  }

}

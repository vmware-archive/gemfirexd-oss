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

import hydra.Log;

import java.util.ArrayList;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

public class ExceptionHandlingFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {

    ArrayList<String> list = new ArrayList<String>();

    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      
      public boolean done() {
        DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
        if(ds!= null)
          return true;
        else
          return false;
      }

      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 15000, 1000, false);
    
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    String localVM;
    
    try {
      localVM = ds.getDistributedMember().getId();
    }
    catch (NullPointerException npe) { // This can be thrown during node down
                                        // scenarios
      throw new FunctionInvocationTargetException(npe);
    }
    
    Log.getLogWriter().info(
        "Inside function execution (some time) node " + localVM);
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
    }
    list.add(localVM);
    Log.getLogWriter().info(
        "Completed function execution (some time) node " + localVM);
    context.getResultSender().lastResult(list);
  }

  public String getId() {
    return "ExceptionHandlingFunction";
  }

  public boolean optimizeForWrite() {
    return true;
  }
  public boolean isHA() {
    return false;
  }


}

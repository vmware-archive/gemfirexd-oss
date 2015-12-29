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
package cli;

import hydra.Log;

import java.util.ArrayList;
import java.util.List;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;

/**
 * @author lynng
 *
 */
public class DeployFcn1 extends FunctionAdapter {

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.execute.FunctionAdapter#execute(com.gemstone.gemfire.cache.execute.FunctionContext)
   *  Expects 2 arguments; returns the second one
   */
  @Override
  public void execute(FunctionContext context) {
    Log.getLogWriter().info("In version1 DeployFcn1 execute with context " + context);

    // retrieve and log function arguments
    Object args = context.getArguments();
    if (args == null) {
       Log.getLogWriter().info("arguments is null");
       context.getResultSender().lastResult("Function executed successfully");
       return;
    }
    if (args.getClass().isArray()) {
      Object[] arguments = (Object[])args;
      Log.getLogWriter().info("arguments is Array with componentType " + arguments.getClass().getComponentType() + 
            " and size " + arguments.length);
      for (int i = 0; i < arguments.length; i++) {
         if (arguments[i] == null) {
           Log.getLogWriter().info("argument[" + i + "] is " + arguments[i]);
         } else {
           Log.getLogWriter().info("argument[" + i + "] is " + arguments[i] + " of class " + arguments[i].getClass().getName());
         }
      }
      // return the last argument
      context.getResultSender().lastResult(arguments[arguments.length-1]);
    } else {
      Log.getLogWriter().info("arguments is " + args + " of class " + args.getClass().getName());
      context.getResultSender().lastResult(args);
    }
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.execute.FunctionAdapter#getId()
   */
  @Override
  public String getId() {
    String id = this.getClass().getName();
    String logStr = "Returning " + id + " from version2 " + this.getClass().getName() + ".getId()";
    Log.getLogWriter().info(logStr);
    return id;
  }

}

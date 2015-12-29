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
import java.util.Set;

import util.TestHelper;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

public class HAExceptionHandlingFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
      regionContext = (RegionFunctionContext)context;
    }

    String argument = null;
    ArrayList list = new ArrayList();
    if (regionContext.getArguments() != null
        && regionContext.getArguments() instanceof String) {
      argument = (String)regionContext.getArguments();
    }

    if (argument.equalsIgnoreCase("executeForCacheClose")) {
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      String localVM = ds.getDistributedMember().getId();
      Log.getLogWriter().info(
          "Inside function execution (executeForCacheClose) node " + localVM);
      TestHelper.waitForCounter(ExecuteExceptionBB.getBB(),
          "ExecuteExceptionBB.cacheCloseCompleted",
          ExecuteExceptionBB.cacheCloseCompleted, 1, true, 60000);
      Log.getLogWriter()
          .info(
              "Completed function execution (executeForCacheClose) node "
                  + localVM);
      list.add(localVM);
      ((RegionFunctionContext)context).getResultSender().lastResult(list);
    }

    if (argument.equalsIgnoreCase("executeForVMDown")) {
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      String localVM = ds.getDistributedMember().getId();
      Log.getLogWriter().info(
          "Inside function execution (executeForVmCrash) node " + localVM);
      TestHelper.waitForCounter(ExecuteExceptionBB.getBB(),
          "ExecuteExceptionBB.vmCrashComplete",
          ExecuteExceptionBB.vmCrashComplete, 1, true, 60000);
      Log.getLogWriter().info(
          "Completed function execution (executeForVmCrash) node " + localVM);
      list.add(localVM);
      ((RegionFunctionContext)context).getResultSender().lastResult(list);
    }

    if (argument.equalsIgnoreCase("ExecuteForSomeTime")) {

      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      String localVM = ds.getDistributedMember().getId();
      Log.getLogWriter().info(
          "Inside function execution (some time) node " + localVM);

      WaitCriterion wc = new WaitCriterion() {
        String excuse;

        public boolean done() {
          return false;
        }

        public String description() {
          return excuse;
        }
      };
      DistributedTestCase.waitForCriterion(wc, 50000, 1000, false);

      list.add(localVM);
      Log.getLogWriter().info(
          "Completed function execution (some time) node " + localVM);
      ((RegionFunctionContext)context).getResultSender().lastResult(list);
    }
    if (argument.equalsIgnoreCase("waitForKeyDestroys")) {
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      String localVM = ds.getDistributedMember().getId();
      Log.getLogWriter().info(
          "Inside function execution (waitForKeyDestroys) node " + localVM);
      TestHelper.waitForCounter(ExecuteExceptionBB.getBB(),
          "ExecuteExceptionBB.keyDestroyCompleted",
          ExecuteExceptionBB.keyDestroyCompleted, 1, true, 60000);
      Set filter = (Set)regionContext.getFilter();
      PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
      for (Object obj : filter) {
        pr.destroy(obj);
      }

      Log.getLogWriter().info(
          "Completed function execution (waitForKeyDestroys) node " + localVM);
      list.add(localVM);
      ((RegionFunctionContext)context).getResultSender().lastResult(list);
    }
  }

  public String getId() {
    return "HAExceptionHandlingFunction";
  }

  public boolean optimizeForWrite() {
    return true;
  }
  public boolean isHA() {
    return false;
  }


}

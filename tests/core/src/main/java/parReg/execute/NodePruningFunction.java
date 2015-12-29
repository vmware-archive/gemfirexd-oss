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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import parReg.colocation.KeyResolver;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class NodePruningFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
      regionContext = (RegionFunctionContext)context;
    }

    if (regionContext != null) {
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      String localVM = ds.getDistributedMember().getId();
      ArrayList list = new ArrayList();

      if (regionContext.getFilter() != null) {
        Set keySet = regionContext.getFilter();
        HashSet keySetHashCodes = new HashSet();
        Iterator keySetItr = keySet.iterator();
        while (keySetItr.hasNext()) {
          KeyResolver key = (KeyResolver)keySetItr.next();
          keySetHashCodes.add(key.getRoutingHint().toString());
        }
        Log.getLogWriter().info(
            "In the function execute:node: " + localVM
                + " Partition resolvers for the filters received : "
                + keySetHashCodes.toString());
      }

      if (regionContext.getArguments() != null) {
        String nodeToGetExecutede = (String)regionContext.getArguments();
        if (!localVM.equals(nodeToGetExecutede)) {
          throw new FunctionException(
              "This function should have executed on local node "
                  + nodeToGetExecutede + " but got executed on " + localVM);
        }
      }
      list.add(localVM);
      regionContext.getResultSender().lastResult(list);
    }
  }

  public String getId() {
    return "NodePruningFunction";
  }

}

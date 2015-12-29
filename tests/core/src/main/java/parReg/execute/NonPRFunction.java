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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import util.TestException;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.execute.MultiRegionFunctionContext;

public class NonPRFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
      regionContext = (RegionFunctionContext)context;

      Region region = regionContext.getDataSet();
      // verify that the execution happens on replicated region and not one with
      // empty datapolicy
      if (region.getAttributes().getDataPolicy() != DataPolicy.REPLICATE) {
        throw new TestException(
            "For non PR region the function execution on region happened on non replicate region");
      }

      // Member id is passed is function only if the execution happened on
      // replicated region
      if (regionContext.getArguments() != null
          && regionContext.getArguments() instanceof DistributedMember) {
        DistributedMember vmId = (DistributedMember)regionContext
            .getArguments();
        DistributedSystem ds = regionContext.getDataSet().getCache()
            .getDistributedSystem();
        DistributedMember localVM = ds.getDistributedMember();

        if (!localVM.equals(vmId)) {
          throw new TestException(
              "The execution on replicated region was not supposed to happen on a remote node.");
        }
      }

      Set keySet = region.keySet();
      ArrayList list = new ArrayList();
      Iterator iterator = keySet.iterator();

      while (iterator.hasNext()) {
        list.add(region.get(iterator.next()));
      }
      context.getResultSender().lastResult(list);
    }
    else {// OnRegions case
      hydra.Log.getLogWriter().info("OnRegions execution");
      MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext)context;
      Set<Region> regionSet = mrContext.getRegions();
      ArrayList list = new ArrayList();
      for (Region aRegion : regionSet) {
        DataPolicy dataPolicy = aRegion.getAttributes().getDataPolicy();

        if (dataPolicy == DataPolicy.EMPTY || dataPolicy == DataPolicy.NORMAL) {
          throw new TestException(
              "OnRegions Function got executed on illegal datapolicy region node "
                  + dataPolicy);
        }
        else {
          hydra.Log.getLogWriter().info(
              "OnRegions function execution happened on the node with datapolicy for region "
                  + dataPolicy);
        }
        list.add(dataPolicy);
      }
      context.getResultSender().lastResult(list);

    }



  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }
  public boolean isHA() {
    return false;
  }
}

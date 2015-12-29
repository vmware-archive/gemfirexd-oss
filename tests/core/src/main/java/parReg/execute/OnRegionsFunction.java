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

import java.util.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.cache.execute.MultiRegionFunctionContext;

public class OnRegionsFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {

    hydra.Log.getLogWriter().info("Inside execute of OnRegionsFunction");
    MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext)context;
    Set<Region> regionSet = mrContext.getRegions();

    hydra.Log.getLogWriter().info(
        "Got the region set of size " + regionSet.size()
            + " and region set is " + regionSet + " and args is "
            + context.getArguments());

    if (context.getArguments() instanceof String
        && context.getArguments().equals("regionSize")) {
      for (Region aRegion : regionSet) {
        Map map = new HashMap();
        hydra.Log.getLogWriter().info(
            "For the region " + aRegion.getName() + " region size is "
                + aRegion.size());
        map.put(aRegion.getName(), aRegion.size());
        context.getResultSender().sendResult((HashMap)map);
      }

      context.getResultSender().lastResult(new HashMap());
    }

    if (context.getArguments() instanceof String
        && context.getArguments().equals("invalidateEntries")) {
      for (Region aRegion : regionSet) {
        Set keySet = aRegion.keySet();
        for (Object key : keySet) {
          aRegion.invalidate(key);
        }
      }

      context.getResultSender().lastResult(Boolean.TRUE);
    }

    hydra.Log.getLogWriter().info("Completed execute of OnRegionsFunction");
  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean isHA() {
    return true;
  }
}


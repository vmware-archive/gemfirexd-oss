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
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class SecondRandomFunction extends FunctionAdapter {
  
  public void execute(FunctionContext context) {
    Log.getLogWriter().info("Invoking execute in SecondRandomFunction");
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
       regionContext = (RegionFunctionContext)context;
       isPartitionedRegionContext = 
         PartitionRegionHelper.isPartitionedRegion(regionContext.getDataSet());
    }

    if (isPartitionedRegionContext) {
      if (regionContext.getFilter() == null) {
        Log.getLogWriter().info(" No routing objects set");
        PartitionedRegion pr = (PartitionedRegion)regionContext.getDataSet();
        int counter = 0;
        Set keySet = pr.keys();
        Iterator iterator = keySet.iterator();
        ArrayList list = new ArrayList();
        while (iterator.hasNext()) {
          counter++;
          Object key = (Object)iterator.next();
          list.add(new Boolean(pr.containsKey(key)));
          if (counter >= 10)
            break;
        }
        context.getResultSender().lastResult(list);
      }
      else {
        Log.getLogWriter().info(" With routing objects");
        Region pr = PartitionRegionHelper.getLocalDataForContext(
            regionContext);
        Set keySet = regionContext.getFilter();
        Iterator iterator = keySet.iterator();
        ArrayList list = new ArrayList();
        while (iterator.hasNext()) {
          Object key = (Object)iterator.next();
          list.add(new Boolean(pr.containsKey(key)));
        }
        context.getResultSender().lastResult(list);
      }
    }
    else if (isRegionContext) {
//      if (regionContext.getFilter() == null) {
//        Log.getLogWriter().info(" No routing objects set");
//        Region region = regionContext.getDataSet();
//        int counter = 0;
//        Set keySet = region.keySet();
//        Iterator iterator = keySet.iterator();
//        ArrayList list = new ArrayList();
//        while (iterator.hasNext()) {
//          counter++;
//          Object key = (Object)iterator.next();
//          list.add(new Boolean(region.containsKey(key)));
//          if (counter >= 10)
//            break;
//        }
//        return list;
//      }
//      else {
        Log.getLogWriter().info(" With routing objects");
        Region region = regionContext.getDataSet();
        Set keySet = null ; //regionContext.getFilter();
        Iterator iterator = keySet.iterator();
        ArrayList list = new ArrayList();
        while (iterator.hasNext()) {
          Object key = (Object)iterator.next();
          list.add(new Boolean(region.containsKey(key)));
        }
        context.getResultSender().lastResult(list);
//      }

    }
    else {
      Log.getLogWriter().info(" No routing objects set");
      ArrayList list = new ArrayList();
      DistributionConfig dc = ((InternalDistributedSystem)InternalDistributedSystem
          .getAnyInstance()).getConfig();
      list.add(new Integer(dc.getArchiveDiskSpaceLimit()));
      list.add(new Integer(dc.getAsyncDistributionTimeout()));
      list.add(new Integer(dc.getMemberTimeout()));
      context.getResultSender().lastResult(list);
    }
  }

  public String getId() {
    return "ReadCheckFunction";
  }

  public boolean hasResult() {
    return true;
  }
  public boolean isHA() {
    return true;
  }

}

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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class FirstRandomFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    if (isRegionContext) {
      RegionFunctionContext regionContext = (RegionFunctionContext)context;
      isPartitionedRegionContext = 
        PartitionRegionHelper.isPartitionedRegion(regionContext.getDataSet());
    }

    if (isPartitionedRegionContext) {
      RegionFunctionContext prContext = (RegionFunctionContext)context;
      Log
          .getLogWriter()
          .info(
              "Invoking execute in FirstRandomFunction in PartitionedRegionFunctionContext");
      ArrayList prProperties = new ArrayList();

      if (prContext.getArguments() == null) {
        PartitionedRegion pr = (PartitionedRegion)prContext.getDataSet();
        prProperties.add(new Integer(pr.getLocalMaxMemory()));
        prProperties.add(pr.getName());
        prProperties.add(new Integer(pr.getTotalNumberOfBuckets()));
        prProperties.add(new Integer(pr.getPRId()));
      }
      else {
        Region lpr = PartitionRegionHelper.getLocalDataForContext(prContext);
        prProperties.add(lpr.getFullPath());
        prProperties.add(lpr.getName());
      }
      context.getResultSender().lastResult( prProperties);
    }
    else if (isRegionContext) {
      RegionFunctionContext regionContext = (RegionFunctionContext)context;
      Log.getLogWriter().info(
          "Invoking execute in FirstRandomFunction in RegionFunctionContext");
      ArrayList regionProperties = new ArrayList();

      if (regionContext.getArguments() == null) {
        Region region = regionContext.getDataSet();
        regionProperties.add(new Integer(region.size()));
        regionProperties.add(region.getName());
        regionProperties.add(new Integer(region.hashCode()));
        regionProperties.add(new Integer(region.size()));
      }
      else {
        Region region = regionContext.getDataSet();
        regionProperties.add(region.getFullPath());
        regionProperties.add(region.getName());
      }
      context.getResultSender().lastResult(regionProperties);
    }
    else {
      Log.getLogWriter().info(
          "Invoking execute in FirstRandomFunction in FunctionContext");

      if (context.getArguments() == null) {
        DistributionConfig dc = ((InternalDistributedSystem)InternalDistributedSystem
            .getAnyInstance()).getConfig();
        ArrayList list = new ArrayList();
        list.add(dc.getCacheXmlFile());
        context.getResultSender().sendResult(list);
        list.clear();
        list.add(dc.getName());
        context.getResultSender().lastResult(list);
      }
      else {
        DistributionConfig dc = ((InternalDistributedSystem)InternalDistributedSystem
            .getAnyInstance()).getConfig();
        ArrayList list = new ArrayList();
        list.add(dc.getCacheXmlFile());
        context.getResultSender().lastResult(list);
      }
    }
  }

  public String getId() {
    return "PRPropertiesFunction";
  }

  public boolean hasResult() {
    return true;
  }
  public boolean isHA() {
    return true;
  }


}

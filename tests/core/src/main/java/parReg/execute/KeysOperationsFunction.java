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
import hydra.RemoteTestModule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.FixedPartitionResolver;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class KeysOperationsFunction extends FunctionAdapter implements
    Declarable {
  
  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
       regionContext = (RegionFunctionContext)context;
       isPartitionedRegionContext = 
         PartitionRegionHelper.isPartitionedRegion(regionContext.getDataSet());
    }

    if (isPartitionedRegionContext) {
      Region localDataSet = PartitionRegionHelper.getLocalDataForContext(regionContext);
      Set keySet = null;
      if (regionContext.getArguments() != null
          && regionContext.getArguments().toString().equalsIgnoreCase(
              "MultiNode")) {
        keySet = regionContext.getFilter();
        Iterator iterator = keySet.iterator();
        boolean sent = false;
        while (iterator.hasNext()) {
          ArrayList list = new ArrayList();
          Object key = (Object)iterator.next();
          Object value = localDataSet.get(key);
          String keyName = " vm id " + RemoteTestModule.getMyVmid() + "_"
              + localDataSet.getName() + "_" + key.toString();
          list.add(keyName);
          if (iterator.hasNext()) {
            context.getResultSender().sendResult(list);
          }
          else {
            sent = true;
            context.getResultSender().lastResult(list);
          }
        }
        if(!sent) {
          context.getResultSender().lastResult(new ArrayList());
        }
      }
      else if (regionContext.getFilter() == null) {
        if (regionContext.getArguments() != null
            && regionContext.getArguments().toString().equalsIgnoreCase(
                "GetFilter")) {

          Region region = regionContext.getDataSet();
          // keySet = regionContext.getFilter();
          keySet = ((Region)PartitionRegionHelper
              .getLocalDataForContext(regionContext)).keySet();

          Iterator iterator = keySet.iterator();

          boolean sent = false;
          while (iterator.hasNext()) {
            HashMap map = new HashMap();
            Object key = iterator.next();
            Object value = region.get(key);
            map.put(key, value);
            if (iterator.hasNext()) {
              context.getResultSender().sendResult(map);
            }
            else {
              sent = true;
              context.getResultSender().lastResult(map);
            }
          }
          if(!sent) {
            context.getResultSender().lastResult(new HashMap());
          }
        }
        else {
          //Log.getLogWriter().info(" No routing objects set");
          keySet = localDataSet.keySet();
          Iterator iterator = keySet.iterator();
          HashMap map = new HashMap();
          while (iterator.hasNext()) {
            Object key = (Object)iterator.next();
            Object value = null;
            if(key instanceof FixedPartitionResolver){
              value = new Boolean(localDataSet.containsKey(key));  
            }else{
              value = new Boolean(false);
            }
            String keyName = " vm id " + RemoteTestModule.getMyVmid() + "_"
                + localDataSet.getName() + "_" + key.toString();
            ;
            map.put(keyName, value);
          }
          context.getResultSender().lastResult(map);
        }
      }
      else {
        Log.getLogWriter().info(" With routing objects");
        keySet = regionContext.getFilter();
        Iterator iterator = keySet.iterator();
        HashMap map = new HashMap();
        while (iterator.hasNext()) {
          Object key = (Object)iterator.next();
          Object value = localDataSet.get(key);
          map.put(key, value);
        }
        context.getResultSender().lastResult(map);
      }
    }
    else if (isRegionContext) {
      Region region = regionContext.getDataSet();
      Set keySet = null;
      if (regionContext.getArguments() != null
          && regionContext.getArguments().toString()
              .equalsIgnoreCase(
              "MultiNode")) {
        Log.getLogWriter().info(
            "Replicated region Function Execution with arguments");
        keySet = regionContext.getFilter();
        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          ArrayList list = new ArrayList();
          Object key = (Object)iterator.next();
          Object value = region.get(key);
          String keyName = key.toString() + " vm id "
              + RemoteTestModule.getMyVmid();
          list.add(keyName);
          if (iterator.hasNext()) {
            context.getResultSender().sendResult(list);
          }
          else {
            context.getResultSender().lastResult(list);
          }
        }

      }
//      else if (regContext.getFilter() == null) {
//        Log.getLogWriter().info(" No routing objects set");
//        region = regContext.getDataSet();
//        keySet = region.keySet();
//        Iterator iterator = keySet.iterator();
//        HashMap map = new HashMap();
//        while (iterator.hasNext()) {
//          Object key = (Object)iterator.next();
//          Object value = new Boolean(region.containsKey(key));
//          String keyName = key.toString() + " vm id "
//              + RemoteTestModule.getMyVmid();
//          map.put(keyName, value);
//        }
//        return map;
//
//      }
      else {
        Log.getLogWriter().info(
            " Replicated region Function Execution With routing objects");
        keySet = regionContext.getFilter();
        Iterator iterator = keySet.iterator();
        while (iterator.hasNext()) {
          HashMap map = new HashMap();
          Object key = (Object)iterator.next();
          Object value = region.get(key);
          map.put(key, value);
          if (iterator.hasNext()) {
            context.getResultSender().sendResult(map);
          }
          else {
            context.getResultSender().lastResult(map);
          }
        }
      }
    }
    else { // added by kishor
      context.getResultSender().lastResult(null);
    }
  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }

  public void init(Properties props) {
  }
  
  public boolean isHA() {
    return true;
  }

}

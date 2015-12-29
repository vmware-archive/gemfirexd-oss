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
package parReg.tx;

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import hydra.*;

import util.*;

public class GetKeySet extends FunctionAdapter implements
    Declarable {
  
  public void execute(FunctionContext context) {

    DistributedMember dm = CacheHelper.getCache().getDistributedSystem().getDistributedMember();

    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    RegionFunctionContext regionContext = null;
    if (!(context instanceof RegionFunctionContext)) {
      throw new TestException("Function requires PartitionedRegionContext!");
    }

    regionContext = (RegionFunctionContext)context;
    isPartitionedRegionContext = 
      PartitionRegionHelper.isPartitionedRegion(regionContext.getDataSet());
    if (!isPartitionedRegionContext) {
      throw new TestException("Function requires PartitionedRegionContext!");
    }

    Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + dm + ", invoked from " + context.getArguments().toString() + " with filter " + regionContext.getFilter());

    //Region aRegion = (Region)PartitionRegionHelper.getLocalDataForContext(regionContext);
    Region aRegion = (Region)regionContext.getDataSet();
    Set keySet = aRegion.keySet();
    int numKeys = keySet.size();

    Set primaryKeySet = new HashSet();
    for (Iterator it = keySet.iterator(); it.hasNext();) {
      Object key = it.next();
      DistributedMember primary = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
      // if this member is the primary, add to the keySet
      if (primary.equals(dm)) {
        primaryKeySet.add(key);
      }
    }
    Log.getLogWriter().info("Region " + aRegion.getName() + " has " + numKeys + " entries and " + primaryKeySet.size() + " entries for which it is the primary");

    //Log.getLogWriter().info("Region " + aRegion.getName() + " has " + numKeys + " entries");
    context.getResultSender().lastResult(new KeySetResult(dm, primaryKeySet));
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

  public void init(Properties props) {
  }

}

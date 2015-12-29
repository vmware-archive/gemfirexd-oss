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

public class GetNewKey extends FunctionAdapter implements
    Declarable {
  
  public void execute(FunctionContext context) {

    DistributedMember localDM = CacheHelper.getCache().getDistributedSystem().getDistributedMember();

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

    Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + localDM + ", invoked from " + context.getArguments().toString() + " with filter " + regionContext.getFilter());

    Region aRegion = (Region)regionContext.getDataSet();
    Set filter = regionContext.getFilter();
    Object sampleKey = filter.iterator().next();

    DistributedMember targetDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, sampleKey);
    Log.getLogWriter().info("Looking for a new key hosted by same member as " + sampleKey + ", DistributedMember " + targetDM);
 
    // Use this to determine a new key colocated with sampleKey
    String key = null;
    do {
       // take a look at the routing code for the next potential key
       String pKey = NameFactory.getNextPositiveObjectName();
       DistributedMember potentialDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, pKey);
       if (targetDM.equals(potentialDM)) {
          // we've found it!
          key = pKey;
          Log.getLogWriter().info("Found " + key + " for DM " + targetDM);
       }
    } while (key == null);
    context.getResultSender().lastResult(key);
  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }

  public void init(Properties props) {
  }

}

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

public class VerifyCustomPartitioningFunction extends FunctionAdapter implements
    Declarable {
  
  public void execute(FunctionContext context) {

    ArrayList list = new ArrayList();

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

    Region aRegion = (Region)PartitionRegionHelper.getLocalDataForContext(regionContext);
    Set keySet = aRegion.keySet();
    int numKeys = keySet.size();
    Log.getLogWriter().info("Region " + aRegion.getName() + " has " + numKeys + " entries");
    int numVms = TestConfig.getInstance().getTotalVMs()-1;
    for (Iterator it = keySet.iterator(); it.hasNext();) {
      Object key = it.next();
      if (key instanceof ModRoutingObject) {
        key = ((ModRoutingObject)key).getKey();
      } 
      // All keys in this VM should map to the same value 
      int hash = (int)NameFactory.getCounterForName(key) % numVms;
      if (!list.contains(hash)) {
        list.add(hash); 
      }
    } 
    if (list.size() > 1) {
      String err = "All keys for " + dm + " did not hash to same hashCode.  HashCodes = " + list + "\n" + TestHelper.getStackTrace();
      Log.getLogWriter().info(err);
      throw new TestException(err);
    }
    context.getResultSender().lastResult(new ModResult(dm, numKeys, list));
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

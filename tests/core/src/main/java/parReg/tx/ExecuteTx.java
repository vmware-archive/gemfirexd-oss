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

public class ExecuteTx extends OperationsClient implements Function, Declarable {
  
  private Object[] keySet;
  private ObjectNameListener myListener;
  private int numVms;  // number of client vms (does not include locator)

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
    keySet = aRegion.keySet().toArray();
    myListener = (ObjectNameListener)aRegion.getAttributes().getCacheListener();
    numVms = TestConfig.getInstance().getTotalVMs()-1;

    initializeOperationsClient();
    doEntryOperations(aRegion);

    context.getResultSender().lastResult(true);
  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    // todo@lhughes -- return true once ResultSender/Collector implemented
    return true;
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties props) {
  }

//==========================================================
// override OperationsClient methods to work on local keySet
//==========================================================

   /** Return a random key currently in the given region.
    *  @param aRegion The region to use for getting an existing key 
    *  @returns A key from the local keySet (given the functionContext)
    */
   protected Object getExistingKey(Region aRegion) {
     if (keySet.length == 0) {
       Log.getLogWriter().info("Could not get a random key from " + aRegion.getFullPath() + " because the region has no keys");
       return null;
     }

     Object key = keySet[TestConfig.tab().getRandGen().nextInt(0, keySet.length-1)];
     Log.getLogWriter().info("getExistingKey() in " + RemoteTestModule.getMyClientName() + " returning " + key + " from keySet " + keySet);
     return key;
   }

   /** Return a new key, never before used in the test.
    *  This must match the existing keySet (in terms of hashing/colocation)
    */
   protected Object getNewKey() {
      List hashCodes = myListener.getHashCodes();
      // get the next key which maps to our VMs hashCodes
      Object key = null;
      int counter = 0;
      int hash = 0;
      do {
        key = NameFactory.getNextPositiveObjectName();
        counter = (int)NameFactory.getCounterForName(key);
        hash = counter % numVms;
      } while (!hashCodes.contains(hash));
      Log.getLogWriter().info("getNewKey() returning key " + key + " with hashCode " + hash + ", local hashCodes = " + hashCodes);
      return key;
   }

  public boolean isHA() {
    return false;
  }
}

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
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import hydra.*;

import util.*;

/**
 *  Function for use in transaction tests which store ValueHolders
 *  in the cache.
 */

public class GetValueForKey extends FunctionAdapter implements
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

    Region aRegion = (Region)regionContext.getDataSet();
    String key = (String)context.getArguments();
  
    // get value from local VM without disturbing TxState
    // this is also the only way to get the Token.INVALID (vs. null) for 
    // invalidated entries.
    Object o = diskReg.DiskRegUtil.getValueInVM( (LocalRegion)aRegion, key );

    Log.getLogWriter().info("Region " + aRegion.getName() + " has key " + key + " with value " + o + " in member " + dm);
    if (o instanceof BaseValueHolder) {
       BaseValueHolder val = (BaseValueHolder)o;
       context.getResultSender().lastResult(val);
    } else if (o instanceof Token.Invalid) {
       Token.Invalid val = (Token.Invalid)o;
       context.getResultSender().lastResult(val);
    } else if (o instanceof Token.LocalInvalid) {
       Token.LocalInvalid val = (Token.LocalInvalid)o;
       context.getResultSender().lastResult(val);
    } else {
       // cover any other objects (unexpected or null (destroyed))
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

}

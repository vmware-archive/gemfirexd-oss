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

public class ExecuteOp implements Function, Declarable {
  
  public void execute(FunctionContext context) {

    BaseValueHolder vh = null;   // return value of the method (possibly null)
    ResultSender rs = context.getResultSender();
    DistributedMember dm = CacheHelper.getCache().getDistributedSystem().getDistributedMember();

    final boolean isRegionContext = context instanceof RegionFunctionContext;
    boolean isPartitionedRegionContext = false;
    RegionFunctionContext regionContext = null;
    if (!(context instanceof RegionFunctionContext)) {
      throw new TestException("Function requires RegionFunctionContext!");
    }

    regionContext = (RegionFunctionContext)context;

    if( regionContext.getArguments() instanceof ArrayList) {
      Region aRegion = regionContext.getDataSet();

      // Arguments are an ArrayList with DistributedMember.toString(),
      // tx.Operation.opName, key, callback and newValue (if applicable for opName)
      ArrayList argumentList = (ArrayList) regionContext.getArguments();
      String forDM = (String)argumentList.get(0);
      String opName = (String)argumentList.get(1);
      Object key = argumentList.get(2);
  
      // no callback or values for gets
      Object value = null;
      Object callback = null;
      if (argumentList.size() > 3) {
        value = argumentList.get(3);
      }

      // values not required for destroy, invalidate
      if (argumentList.size() > 4) {
        callback = argumentList.get(4);
      }

      // For partitionedRegions, only execute on primary
      // todo@lhughes - figure out how to limit this for replicated regions
      // for now, only execute on PRs
      boolean isPrimary = false;
      if (PartitionRegionHelper.isPartitionedRegion(aRegion)) {
        DistributedMember primaryDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
        if (primaryDM.equals(dm)) {
          isPrimary = true;
        } 
      }

      if (isPrimary) {
        Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + dm + ", invoked from " + forDM + " with filter " + regionContext.getFilter());
        StringBuffer aStr = new StringBuffer();
        aStr.append("ExecuteOp args: ");
        aStr.append("dm = " + forDM + ", ");
        aStr.append("opName = " + opName + ", ");
        aStr.append("value = " + value + ", ");
        aStr.append("callback = " + callback);
        Log.getLogWriter().info(aStr.toString());
  
        if (opName.equalsIgnoreCase(tx.Operation.ENTRY_CREATE)) {
          aRegion.create(key, value, callback);
        } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_UPDATE)) {
          Log.getLogWriter().info("Executing region.update (" + key + ", " + value + ", " + callback + ")");
          vh = (BaseValueHolder)(aRegion.put(key, value, callback));
        } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_DESTROY)) {
          vh = (BaseValueHolder)(aRegion.destroy(key, callback));
        } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_INVAL)) {
          aRegion.invalidate(key, callback);
        } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_GET_NEW_KEY)) {
          vh = (BaseValueHolder)(aRegion.get(key));
        } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_GET_EXIST_KEY)) {
          vh = (BaseValueHolder)(aRegion.get(key));
        } else if (opName.equalsIgnoreCase(tx.Operation.ENTRY_GET_PREV_KEY)) {
          vh = (BaseValueHolder)(aRegion.get(key));
        } else { // unknown operation
          throw new TestException("Unknown operation " + opName);
        }
      } else {
          Log.getLogWriter().info("ExecuteOp not executing on member " + dm + " because it is not the primary");
      }
    } else {
      throw new TestException("ExecuteOp requires an argument list with DistributedMember, opName, key, newValue, and callback");
    }
    rs.lastResult(vh);
  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties props) {
  }

  public boolean isHA() {
    return false;
  }
}

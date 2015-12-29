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

import getInitialImage.InitImageBB;
import hydra.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import parReg.colocation.Month;
import util.TestException;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class LocalDataSetFunctionPrimaryExecuteFalse extends FunctionAdapter
    implements Declarable {

  public void execute(FunctionContext context) {
    RegionFunctionContext regionContext = (RegionFunctionContext)context;
    LocalDataSet localDataSet = (LocalDataSet)PartitionRegionHelper
        .getLocalDataForContext(regionContext);
    Region region = regionContext.getDataSet();
    int totalNumOfBuckets = ((PartitionedRegion)region)
        .getTotalNumberOfBuckets();

    Set<Integer> expectedBucketSet = new HashSet<Integer>();

    if (regionContext.getFilter() != null) {
      Set filterSet = regionContext.getFilter();
      Iterator<Integer> keySetItr = filterSet.iterator();
      while (keySetItr.hasNext()) {
        Object key = keySetItr.next();
        Month routingObject = (Month)InitImageBB.getBB().getSharedMap()
            .get(key);
        expectedBucketSet.add(routingObject.hashCode() % totalNumOfBuckets);
      }

      Set<Integer> executedBucketSet = localDataSet.getBucketSet();

      if (executedBucketSet.containsAll(expectedBucketSet)
          && executedBucketSet.size() == expectedBucketSet.size()) {
        Log.getLogWriter().info(
            "For region " + region.getName()
                + " execution happened on all the expected bucket list "
                + expectedBucketSet);
      }
      else {
        throw new TestException("For region " + region.getName()
            + " execution was supposed to happen on the buckets "
            + expectedBucketSet + " but happened on " + executedBucketSet);
      }
    }
    else {
      expectedBucketSet = localDataSet.getBucketSet();
    }

    Map colocatedRegionsMap = PartitionRegionHelper.getColocatedRegions(localDataSet);

    Iterator iterator = colocatedRegionsMap.entrySet().iterator();
    Map.Entry entry = null;
    String colocatedRegion = null;
    LocalDataSet colocatedRegionLocalDataSet = null;
    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      colocatedRegion = (String)entry.getKey();
      colocatedRegionLocalDataSet = (LocalDataSet)entry.getValue();

      Set<Integer> colocatedRegionExecutedBucketSet = colocatedRegionLocalDataSet
          .getBucketSet();

      if (colocatedRegionExecutedBucketSet.containsAll(expectedBucketSet)
          && colocatedRegionExecutedBucketSet.size() == expectedBucketSet
              .size()) {
        Log.getLogWriter().info(
            "For colocated region " + colocatedRegion
                + " execution happened on all the expected bucket list "
                + expectedBucketSet);
      }
      else {
        throw new TestException("For colocated region " + colocatedRegion
            + " execution was supposed to happen on the buckets "
            + expectedBucketSet + " but happened on "
            + colocatedRegionExecutedBucketSet);
      }
    }

    ArrayList<Object> list = new ArrayList<Object>();
    list.addAll(localDataSet.getBucketSet());

    Log.getLogWriter().info("Returning list " + list);

    context.getResultSender().lastResult(list);

  }

  public String getId() {
    return "LocalDataSetFunctionPrimaryExecuteFalse";
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

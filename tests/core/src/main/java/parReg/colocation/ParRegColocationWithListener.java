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
package parReg.colocation;

import hydra.CacheHelper;
import hydra.Log;
import hydra.RegionHelper;

import java.util.HashSet;
import java.util.Set;

import parReg.execute.UpdateBBPartitionListener;
import rebalance.RebalancePrms;
import rebalance.RebalanceUtil;
import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

public class ParRegColocationWithListener extends ParRegColocation {

  private static RebalanceOperation rebalanceOp;

  public static void HydraTask_verifyEmptyRecreatedBuckets() {
    String regionDescriptName = null;
    Set recreatedBucketSet = new HashSet();
    for (int j = 0; j < regionDescriptNames.size(); j++) {
      regionDescriptName = (String)(regionDescriptNames.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      PartitionedRegion aRegion = (PartitionedRegion)theCache
          .getRegion(regionName);
      if (aRegion.getPartitionListeners() != null
          && aRegion.getPartitionListeners().length > 1) {
        UpdateBBPartitionListener partitionListener = (UpdateBBPartitionListener)aRegion
            .getPartitionListeners()[1];
        recreatedBucketSet = partitionListener.getReCreatedBucketSet();
      }
    }

    for (int i = 0; i < regionDescriptNames.size(); i++) {
      regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      PartitionedRegion aRegion = (PartitionedRegion)theCache
          .getRegion(regionName);
      if (aRegion.getPartitionListeners() == null || aRegion.getPartitionListeners().length == 0) {
        validateEmptyBuckets(aRegion, recreatedBucketSet);
      }
    }
  }

  /**
   * Rebalance via the cache api. Verify that balance has improved.
   */
  public static void HydraTask_rebalanceTask() {
    Log.getLogWriter().info("In HydraTask_rebalanceTask");
    rebalance();
  }

  public static void validateEmptyBuckets(PartitionedRegion aRegion,
      Set recreatedBucketSet) {
    hydra.Log.getLogWriter().info(
        "Recreated bucket set is " + recreatedBucketSet);
    for (Object bucketId : recreatedBucketSet) {
      try {
        Set bucketKeys = aRegion.getBucketKeys((Integer)bucketId);
        if (bucketKeys.size() != 0) {
          throw new TestException("For the bucketId " + bucketId
              + " of region " + aRegion.getName()
              + " bucket is not empty as expected");
        }
      }
      catch (Exception e) {
        throw new TestException("Caught Exception ", e);
      }
    }
  }

  public static void rebalance() {
    ResourceManager rm = CacheHelper.getCache().getResourceManager();
    RebalanceFactory factory = rm.createRebalanceFactory();

    // /INCLUDE REGIONS
    Set<String> includedRegionSet = new HashSet<String>();
    for (int i = 1; i <= 1; i++) {
      PartitionedRegion includedRegion = (PartitionedRegion)theCache
          .getRegion(Region.SEPARATOR + "clientRegion" + i);
      if (includedRegion == null) {
        throw new TestException("Test Issue: Included region cannot be null");
      }
      includedRegionSet.add(includedRegion.getName());
    }
    hydra.Log.getLogWriter().info(
        "Included regions for rebalancing " + includedRegionSet);

    factory.includeRegions(includedRegionSet);

    RebalanceOperation rebalanceOp = factory.start();
    try {
      RebalanceResults rebalanceResults = rebalanceOp.getResults();
      Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "Rebalance"));
    }
    catch (Exception e) {
      throw new TestException("Caught the exception ", e);
    }
  }

  public static void setResourceObserver() {
    // VM should have already created PR via initialization task
    Cache myCache = CacheHelper.getCache();
    if (myCache == null) {
      throw new TestException(
          "setResourceObserver() expects hydra client to have created cache and PR via initialization tasks");
    }

    ResourceManager rm = myCache.getResourceManager();

    // Setup test hooks to listen for rebalance/recovery start/finish
    InternalResourceManager.ResourceObserver ro = RebalancePrms
        .getResourceObserver();
    if (ro != null) {
      ((InternalResourceManager)rm).setResourceObserver(ro);
      Log.getLogWriter().info("Installed ResourceObserver " + ro.toString());
    }
  }

}

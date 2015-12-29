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
/**
 * 
 */
package parReg.partitionMan;

import java.util.HashSet;
import java.util.Set;

import util.TestException;
import hydra.CacheHelper;
import hydra.Log;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionListener;
import com.gemstone.gemfire.cache.partition.PartitionManager;

/**
 * @author lynn
 *
 */
public class PRListener implements PartitionListener {

  //the region that has this listener installed
  // to do colocation with PartitionListener/PartitionManager this is installed in only one region
  private static Region colocatedWithRegion; 
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.partition.PartitionListener#afterPrimary(int)
   */
  public void afterPrimary(int bucketId) {
    Log.getLogWriter().info("In PRListener.afterPrimary for bucketId " + bucketId);
    Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
    Set<Region<?, ?>> allRegions = new HashSet(rootRegions);
    for (Region aRegion: rootRegions) {
      allRegions.addAll(aRegion.subregions(true));
    }
    for (Region aRegion: allRegions) {
      if (!aRegion.getFullPath().equals(colocatedWithRegion.getFullPath())) {
        Log.getLogWriter().info("Calling PartitionManager.createPrimaryBucket(" + aRegion.getFullPath() +
            ", " + bucketId + "<bucketId>, true, true)");
        long startTime = System.currentTimeMillis();
        PartitionManager.createPrimaryBucket(aRegion, bucketId, true, true);
        long duration = System.currentTimeMillis() - startTime;
        Log.getLogWriter().info("Done calling PartitionManager.createPrimaryBucket(" + aRegion.getFullPath() +
            ", " + bucketId + "<bucketId>, true, true), duration is " + duration + "ms");
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.partition.PartitionListener#afterRegionCreate(com.gemstone.gemfire.cache.Region)
   */
  public void afterRegionCreate(Region<?, ?> aRegion) {
    Log.getLogWriter().info("In PRListener.afterRegionCreate for " + aRegion.getFullPath());
    synchronized (PRListener.class) {
      if (colocatedWithRegion != null) {
        throw new TestException("PRListener was installed in more than one region; it was already installed in " + 
                                 colocatedWithRegion.getFullPath() + " and now is being installed in " + aRegion.getFullPath());
      }
      colocatedWithRegion = aRegion;
    }
  }

  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {}

  public void afterBucketCreated(int bucketId, Iterable<?> keys) {}

}

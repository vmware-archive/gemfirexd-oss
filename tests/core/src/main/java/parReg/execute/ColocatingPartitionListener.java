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

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionListener;
import com.gemstone.gemfire.cache.partition.PartitionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class ColocatingPartitionListener implements PartitionListener {
  
  private Set<Integer> recreatedBucketIds = new HashSet<Integer>();

  public void afterPrimary(int bucketId) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    PartitionedRegion viewPR = (PartitionedRegion)cache
        .getRegion(Region.SEPARATOR + "testRegion4");
    if(viewPR == null){
      return; //fix for 43101
      //view PR is not yet created, we can not create bucket for viewPR.
      // do not add this bucketId to recreatedBucketIds Set.
    }
    PartitionManager.createPrimaryBucket(viewPR, bucketId, true, false);
    synchronized (parReg.ParRegBB.getBB().getSharedMap()) {
      if (parReg.ParRegBB.getBB().getSharedMap().get("Bucket_" + bucketId) != null) {
        // If the bucket had already created then this is a re-creation
        recreatedBucketIds.add(bucketId);
      }
      InternalDistributedMember self = (InternalDistributedMember)cache
      .getDistributedSystem().getDistributedMember();
      parReg.ParRegBB.getBB().getSharedMap().put("Bucket_" + bucketId,
          self.getId());
    }
  }
  
  public Set<Integer> getReCreatedBucketIds() {
    return this.recreatedBucketIds;
  }

  public void afterRegionCreate(Region<?,?> region) {
    
  }

  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
  }
  
  public void afterBucketCreated(int bucketId, Iterable<?> keys) {
  }
}

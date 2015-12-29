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
package recovDelay;

import java.util.*;
import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/** Class to get the state of buckets/primaries.
 */
public class BucketState implements java.io.Serializable {
    
/** Get information about each member's buckets/primaries.
 *  @param aRegion The region to get information for. 
 *  @returns [0] A Map where the key is an Integer which is the hydra vm id of a member,
 *               and the value is an Integer which is the number of primaries in the member.
 *           [1] A Map where the key is an Integer which is the hydra vm id of a member,
 *               and the value is an Integer which is the number of buckets in the member.
 */
public static Map[] getBucketMaps(Region aRegion) {
   Map primaryMap = new TreeMap();  // key is vmId (Integer), value is number of primaries (Integer)
   Map bucketMap = new TreeMap();  // key is vmId (Integer), value is number of buckets (Integer)
   PartitionedRegion pr = (PartitionedRegion)aRegion;
   try {
      int totalBuckets = pr.getTotalNumberOfBuckets();
      for (int i = 0; i < totalBuckets; i++) {
         List aList = pr.getBucketOwnersForValidation(i);
         if (aList.size() == 0) { // there are no buckets for this bucket id
            continue;
         }
         for (int j = 0; j < aList.size(); j++) {
            Object[] tmpArr = (Object[])(aList.get(j));
            InternalDistributedMember member = (InternalDistributedMember)(tmpArr[0]);
            Boolean isPrimary = (Boolean)(tmpArr[1]);
            String host = member.getHost();
            int pid = member.getVmPid();
            Integer vmId = null;
            try {
               vmId = RemoteTestModule.Master.getVmid(host, pid);
               if (vmId == null) {
                 throw new TestException("Given host " + host + " and pid " + pid + ", hydra returned a vmID of " + vmId);
               }
            } catch (java.rmi.RemoteException e) {
               throw new TestException(TestHelper.getStackTrace(e));
            }
            Integer key = vmId;

            // add to the primary map
            Integer currPrimaryCount = (Integer)(primaryMap.get(key));
            if (isPrimary.booleanValue()) {
               if (currPrimaryCount == null) {
                  currPrimaryCount = new Integer(1);
               } else {
                  currPrimaryCount = new Integer(currPrimaryCount.intValue() + 1);
               }
            } else { 
               if (currPrimaryCount == null) {
                  currPrimaryCount = new Integer(0);
               } 
            }
            primaryMap.put(key, currPrimaryCount);

            // add to the bucket map
            Integer currBucketCount = (Integer)(bucketMap.get(key));
            if (currBucketCount == null) {
               currBucketCount = new Integer(1);
            } else {
               currBucketCount = new Integer(currBucketCount.intValue() + 1);
            }
            bucketMap.put(key, currBucketCount);
         }
      }
   } catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (com.gemstone.gemfire.internal.cache.ForceReattemptException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new Map[] {primaryMap, bucketMap};
}

/** Check that primaries have exact balance
 */
public static void checkPrimaryBalance(Region aRegion) {
   Map[] mapArr = getBucketMaps(aRegion);
   Map primaryMap = mapArr[0];
   Map bucketMap = mapArr[1];

   // number of primaries per vm
   Iterator it = primaryMap.keySet().iterator();
   int min = Integer.MAX_VALUE;
   int max = Integer.MIN_VALUE;
   int total = 0;
   while (it.hasNext()) {
      Integer vmId = (Integer)(it.next());
      int value = ((Integer)(primaryMap.get(vmId))).intValue();
      min = Math.min(min, value);
      max = Math.max(max, value);
      total += value;
   }
   int span = Math.abs(max - min);
   if (span > 1) {
      throw new TestException("Primaries are not balanced, least number of primaries in a vm: " + min +
           ", most number of primaries in a vm: " + max + "; " + primaryMap + ", buckets: " + bucketMap);
   } else {
      Log.getLogWriter().info("Primaries are balanced: " + primaryMap + ", buckets: " + bucketMap);
   }
}

}

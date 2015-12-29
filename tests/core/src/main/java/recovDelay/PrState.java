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

/** Class to hold the state of a pr in a member.
 */
public class PrState implements java.io.Serializable {
    
protected Integer vmId = null;          // the hydra vmId for this member
protected List bucketInfoList = null;   // a list of BucketInfo instances, which represent
                                        // the buckets contained in this member

/** Get information about each member and return a Map of PrState instances.
 *  @param aRegion The region to get information for. 
 *  @returns A Map where the key is an Integer which is the hydra vm id of a member,
 *           and the value is an instance of PrState.
 */
public static Map getPrMap(Region aRegion) {
   List<List<BucketInfo>> bucketList = BucketInfo.getAllBuckets(aRegion);
   return getPrMap(bucketList);
}

/** Get information about each member and return a Map of PrState instances.
 *  @param masterBucketList A list returned from BucketInfo.getAllBuckets().
 *  @returns A Map where the key is an Integer which is the hydra vm id of a member,
 *           and the value is an instance of PrState.
 */
public static Map getPrMap(List masterBucketList) {
   // keys are Integers, the hydra vmId, values are PrState instances
   Map prStateMap = new TreeMap(); 
   for (int bucketId = 0; bucketId < masterBucketList.size(); bucketId++) {
      List bucketCopiesList = (List)(masterBucketList.get(bucketId));
      if (bucketCopiesList != null) { // this bucketId has no copies
         for (int i = 0; i < bucketCopiesList.size(); i++) {
            BucketInfo info = (BucketInfo)(bucketCopiesList.get(i));
            int vmId = info.getVmId();
            PrState state = (PrState)(prStateMap.get(new Integer(vmId))); 
            if (state == null) {
               state = new PrState();
               state.setVmId(vmId);
               state.setBucketInfoList(new ArrayList());
               prStateMap.put(new Integer(vmId), state);
            }
            state.getBucketInfoList().add(info);
         }
      }
   }
   return prStateMap;
}

/** Compare two prStateMaps (maps that are the result of PrState.getPrStateMap())
 *  and return a String describing the differences. 
 *
 *  @param expectedMap The pr state map containing the expected contents.
 *  @param actualMap The pr state map to compare with.
 *  @param allowDifferentPrimaries true if the primaries can be different, false otherwise.
 *  
 *  @returns A String describing any differences, or "" if no differences.
 */
public static String comparePrState(Map expectedMap, Map actualMap, boolean allowDifferentPrimaries) {
   Log.getLogWriter().info("Comparing expected pr state map containing vmIds " + expectedMap.keySet() +
       " with actual pr state map containing vmIds " + actualMap.keySet());
   StringBuffer returnStr = new StringBuffer();
   if (!(expectedMap.keySet().equals(actualMap.keySet()))) {
      Set missingMembers = expectedMap.keySet();
      missingMembers.removeAll(actualMap.keySet());
      Set extraMembers = actualMap.keySet();
      extraMembers.removeAll(expectedMap.keySet());
      if (missingMembers.size() > 0) {
         Iterator it = missingMembers.iterator();
         while (it.hasNext()) {
            returnStr.append("Expected the PR state to contain member with vmId " + it.next() + 
                             ", but it is missing.\n");
         }
      }
      if (extraMembers.size() > 0) {
         Iterator it = extraMembers.iterator();
         while (it.hasNext()) {
            returnStr.append("The existing PR state contains member with vmId " + it.next() + 
                ", but it was unexpected.\n");
         }
      }
   } else { // the two maps contain the same keys
      Iterator it = expectedMap.keySet().iterator();
      while (it.hasNext()) {
         Object vmIdKey = it.next();
         PrState expectedPrState = (PrState)(expectedMap.get(vmIdKey));
         PrState actualPrState = (PrState)(actualMap.get(vmIdKey));
         if (!(expectedPrState.vmId.equals(actualPrState.vmId))) { // test logic problem
            throw new TestException("Test error, vmId in " + expectedPrState + " and " + 
                  actualPrState + " are located in the pr state map with key " + vmIdKey);
         }
         int vmId = expectedPrState.vmId;
         // compare the bucketInfoList (the list of buckets contained in each member)
         returnStr.append(BucketInfo.compareBucketLists(vmId,
            expectedPrState.bucketInfoList, actualPrState.bucketInfoList, allowDifferentPrimaries));
      }
   }
   return returnStr.toString();
}

/** Return true if anObj is equal to this PrState, false otherwise
 */
public boolean equals(Object anObj) {
   if (anObj == null) {
      return false;
   }
   if (!(anObj instanceof PrState)) {
      return false;
   }
   PrState arg = (PrState)anObj;
   if (vmId != arg.vmId) {
      return false;
   }
   if (bucketInfoList.size() != arg.bucketInfoList.size()) {
      return false;
   }
   for (int i = 0; i < bucketInfoList.size(); i++) {
      BucketInfo info = (BucketInfo)(bucketInfoList.get(i));
      BucketInfo argInfo = (BucketInfo)(arg.bucketInfoList.get(i));
      if (!(info.equals(argInfo))) {
         return false;
      }
   }
   return true;
}

//================================================================================
// string methods

/** Return a string representation to show a picture of the state of
 *  this pr.
 */
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("Member with vmId " + vmId + " contains " + bucketInfoList.size() + " buckets, ");
   if ((bucketInfoList == null) || (bucketInfoList.size() == 0)) {
      aStr.append("   no buckets are hosted on this member");
   } else {
      aStr.append(BucketInfo.bucketListToString(bucketInfoList));
   }
   return aStr.toString();
}

/** Return a string representation to show a picture of the state of
 *  the pr.
 *  @param prMap A map where the key is the hydra vmId (Integer)
 *         and the value is an instance of PrState.
 */
public static String prMapToString(Map prMap) {
   StringBuffer aStr = new StringBuffer();
   Iterator it = prMap.keySet().iterator();
   while (it.hasNext()) {
      PrState state = (PrState)(prMap.get(it.next()));
      aStr.append(state + "\n");
   }
   return aStr.toString();
}

/** Return a String depicting all members and all buckets of the given pr.
 * 
 * @param aRegion The region to depict. 
 * @return A String showing all members and all buckets/primaries of aRegion.
 */
public static String getPrPicture(Region aRegion) {
  return aRegion.getFullPath() + "\n" + PrState.prMapToString(PrState.getPrMap(aRegion));
}

/** Return a String depicting the given bucket ID for all members of the given pr.
 * 
 * @param aRegion The region to depict. 
 * @param bucketId The bucket to depict. 
 * @return A String showing the given bucket ID on all members.
 */
public static String getPrPicture(Region aRegion, int bucketId) {
  StringBuffer aStr = new StringBuffer();
  aStr.append(aRegion.getFullPath() + "\n");
  Map<Integer, PrState> aMap = PrState.getPrMap(aRegion);
  for (Integer vmID: aMap.keySet()) {
    PrState state = aMap.get(vmID);
    aStr.append("Member with vmId " + vmID + " contains " + state.bucketInfoList.size() + " buckets\n");
    List<BucketInfo> infoList = state.bucketInfoList;
    for (BucketInfo info: infoList) {
      if (info.bucketId == bucketId) {
        aStr.append("   " + info + "\n");
      }
    }
  }
  return aStr.toString();
}

//================================================================================
// accessors

public int getVmId() {
   return vmId;
}
public void setVmId(int vmIdArg) {
   vmId = vmIdArg;
}
public List getBucketInfoList() {
   return bucketInfoList;
}
public void setBucketInfoList(List listArg) {
   bucketInfoList = listArg;
}
}

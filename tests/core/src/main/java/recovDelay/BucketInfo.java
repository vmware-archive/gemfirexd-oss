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

import hydra.Log;
import hydra.RemoteTestModule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.BucketDump;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/** Class to hold information about a bucket.
 */
public class BucketInfo implements java.io.Serializable {

/** Serial version id to match old instances of BucketInfo, before the versions
 * field was added. versions will be null if we read an old BucketInfo object
 */
private static final long serialVersionUID = 62846770744058408L;
    
protected int bucketId = -1;          // the bucketId for the bucket whose information this instance represents
protected InternalDistributedMember member = null; // the member hosting this bucket
protected int vmId = -1;              // the hydra vm id for the member hosting this bucket
protected boolean isPrimary = false;  // whether this bucket is primary on this host
protected Map<Object, Object> entriesMap = null;      // the list of keys/values for this bucketId 
protected Map<Object, VersionTag> versions = null; //this list of keys/versions for this bucket id

/** Constructor
 *  @param bucketIdArg The bucket id for this instance.
 *  @param memberArg The InternalDistributedMembers that contain the bucketId.
 *  @param vmIdArg The hydra vm id (Integer) of this member.
 *  @param isPrimaryArg True if this bucket in memberArg is primary, false otherwise.
 *  @param entriesMapArg A list of keys/values contained in bucket in memberArg.
 */
public BucketInfo(int bucketIdArg, 
                  InternalDistributedMember memberArg, 
                  int vmIdArg, 
                  boolean isPrimaryArg, 
                  Map entriesMapArg,
                  Map<Object, VersionTag> versions) {
   bucketId = bucketIdArg;
   member = memberArg;
   vmId = vmIdArg;
   isPrimary = isPrimaryArg;
   entriesMap = entriesMapArg;
   this.versions = versions;
}

/** Collect information on all buckets, and return a List of Lists of 
 *  BucketInfo instances.
 *
 *  @param prReg A partitioned region.
 *  @return A List of Lists, where each secondary List is a List of
 *          BucketInfo instances. Each instance is a bucket copy for
 *          a particular member for the bucketID indicated by the index 
 *          into the primary List. Return List is indexed by a bucketID:
 *          List [bucketId] -->  List of size 1 through 4 (depending on the
 *                               setting of redundantCopies) containing:
 *                                  BucketInfo for a bucket copy on a particular member
 *                                  ...
 *                                  BucketInfo for a bucket copy on a particular member
 *          If there are not buckets for a particular bucketID, then 
 *          List[bucketId] is null.
 */
public static List<List<BucketInfo>> getAllBuckets(Region prReg) {
   Vector<List<BucketInfo>> bucketVec = new Vector<List<BucketInfo>>();
   try {
      PartitionedRegion pr = (PartitionedRegion)prReg;
      int totalBuckets = pr.getTotalNumberOfBuckets();
      for (int bucketId = 0; bucketId < totalBuckets; bucketId++) { // iterate each bucket id
         List aList = pr.getBucketOwnersForValidation(bucketId);
         if (aList.size() == 0) { // there are no buckets for this bucket id
            bucketVec.setSize(Math.max(bucketId+1, bucketVec.size()));
            bucketVec.setElementAt(null, bucketId);
         } else {
            List<BucketDump> listOfMaps = pr.getAllBucketEntries(bucketId);
            removePayload(listOfMaps);
            for (int j = 0; j < aList.size(); j++) { // iterate each member for this bucket id
               Object[] tmpArr = (Object[])(aList.get(j));
               InternalDistributedMember member = (InternalDistributedMember)(tmpArr[0]);
               Boolean isPrimary = (Boolean)(tmpArr[1]);
               String host = member.getHost();
               int pid = member.getVmPid();
               Integer vmId = null;
               try {
                  vmId = new Integer(RemoteTestModule.Master.getVmid(host, pid));
               } catch (java.rmi.RemoteException e) {
                  throw new TestException(TestHelper.getStackTrace(e));
               }

               // find the entries map for the member
               BucketDump entriesMap = null;
               for (int k = 0; k < listOfMaps.size(); k++) {
                  entriesMap = listOfMaps.get(k);
                  if (entriesMap.getMember().equals(member)) { // found it
                     break;
                  }
               }
               if (entriesMap == null) {
                  throw new TestException("Could not find entriesMap for bucketId " + bucketId);
               }
               //TODO - RVV - do we need to pass along the version information here?
               HashMap<Object, Object> valuesMap = new HashMap<Object, Object>(entriesMap.getValues()); // make serializable, as the one returned from the hook is not
            BucketInfo info = new BucketInfo(bucketId, member, vmId.intValue(),
                isPrimary.booleanValue(), valuesMap, entriesMap.getVersions());
               bucketVec.setSize(Math.max(bucketId+1, bucketVec.size()));
               List<BucketInfo> bucketCopyList = bucketVec.get(bucketId);
               if (bucketCopyList == null) {
                  bucketCopyList = new ArrayList<BucketInfo>();
                  bucketVec.setElementAt(bucketCopyList, bucketId);
               }
               bucketCopyList.add(info);
            }
         }
      }
   } catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (com.gemstone.gemfire.internal.cache.ForceReattemptException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   return new ArrayList<List<BucketInfo>>(bucketVec);
}

/** Return a string representation of this instance.
 */
public String toString() {
   return "BucketId " + bucketId + 
          "; isPrimary " + isPrimary + 
          "; vmId " + vmId + 
          "; member " + member + 
          "; numKeys " + entriesMap.size();
}

/** Return a String representation of the keys in the given map.
 *  @param entriesMap A Map of keys/values.
 *  @return A String contains the list of keys.
 */ 
protected String getKeysStr(Map entriesMap) {
   Iterator it = entriesMap.keySet().iterator();
   int count = 0;
   StringBuffer aStr = new StringBuffer();
   while (it.hasNext()) {
      count++;
      aStr.append(it.next() + " ");
      if (count % 10 == 0) {      
         aStr.append("\n");
      }
   }
   return aStr.toString();
}
 
/** Compare two bucket lists (lists that contain BucketInfos)
 *  and return a String describing the differences. 
 *
 *  @param vmId The vmId being compared.
 *  @param expectedList The bucket list containing the expected contents.
 *  @param actualList The bucket list to compare with.
 *  @param allowDifferentPrimaries true if the primaries can be different, false otherwise.
 *
 *  @returns A String describing any differences, or "" if no differences.
 */
public static String compareBucketLists(int vmId, List expectedList, List actualList, boolean allowDifferentPrimaries) {
   // get maps with bucketId keys, BucketInfo values for expected and actual
   Map expectedBucketMap = new HashMap();
   for (int i = 0; i < expectedList.size(); i++) {
      BucketInfo info = (BucketInfo)(expectedList.get(i));
      expectedBucketMap.put(new Integer(info.getBucketId()), info);
   }
   Map actualBucketMap = new HashMap();
   for (int i = 0; i < actualList.size(); i++) {
      BucketInfo info = (BucketInfo)(actualList.get(i));
      actualBucketMap.put(new Integer(info.getBucketId()), info);
   }
   Set expectedBucketIdSet = new TreeSet(expectedBucketMap.keySet());
   Set actualBucketIdSet = new TreeSet(actualBucketMap.keySet());
   Log.getLogWriter().info("Expected bucketIds for vmId " + vmId + ": " + expectedBucketIdSet);
   Log.getLogWriter().info("Actual bucketIds for vmId " + vmId + ": " + actualBucketIdSet);

   // check for duplicate bucketIds within each list; this is a test consistency check
   if (expectedList.size() != expectedBucketIdSet.size()) {
      throw new TestException("Test error, bucketInfoList " + expectedList + " contains duplicate bucket ids");
   }

   // check for missing or extra buckets
   Set missingBucketIds = new HashSet(expectedBucketIdSet);
   missingBucketIds.removeAll(actualBucketIdSet);
   Set extraBucketIds = new HashSet(actualBucketIdSet);
   extraBucketIds.removeAll(expectedBucketIdSet);
   StringBuffer returnStr = new StringBuffer();
   if ((missingBucketIds.size() > 0) || (extraBucketIds.size() > 0)) {
      if (missingBucketIds.size() > 0) {
         Iterator it = missingBucketIds.iterator();
         while (it.hasNext()) {
            returnStr.append("Expected member with vm id " + vmId + " to contain bucket with id " +
               it.next() + ", but it is missing.\n");
         }
      }
      if (extraBucketIds.size() > 0) {
         Iterator it = extraBucketIds.iterator();
         while (it.hasNext()) {
            returnStr.append("The member with vm id " + vmId + " contains bucket with id " + it.next() +
                ", but it is unexpected in this member.\n");
         }
      }
   } else { // we have the same bucket Ids in each list/map
      Iterator it = expectedBucketMap.keySet().iterator();
      while (it.hasNext()) {
         Object bucketIdKey = it.next();
         BucketInfo expectedInfo = (BucketInfo)(expectedBucketMap.get(bucketIdKey));
         BucketInfo actualInfo = (BucketInfo)(actualBucketMap.get(bucketIdKey));
         returnStr.append(expectedInfo.compare(actualInfo, allowDifferentPrimaries));
      }
   }
   return returnStr.toString();
}

/** Compare two bucketInfos and return a String describing the differences. 
 *
 *  @param info The BucketInfo to compare to.
 *  @param allowDifferentPrimaries true if the primaries can be different, false otherwise.
 *
 *  @returns A String describing any differences, or "" if no differences.
 */
public String compare(BucketInfo info, boolean allowDifferentPrimaries) {
   StringBuffer returnStr = new StringBuffer();
   if (bucketId != info.bucketId) {
      returnStr.append("Bucket id in <" + this + " is not equal to bucket id in " + info + ">\n");
   }
   if (!(member.toString().equals(info.member.toString()))) {
      returnStr.append("Member in <" + this + " is not equal to member in " + info + ">\n");
   }
   if (vmId != info.vmId) {
      returnStr.append("vmId in <" + this + " is not equal to vmId in " + info + ">\n");
   }
   if (!allowDifferentPrimaries) {
      if (isPrimary != info.isPrimary) {
         returnStr.append("isPrimary in <" + this + "> is not equal to isPrimary in <" + info + ">\n");
      }
   }
   if (entriesMap.size() != info.entriesMap.size()) {
      returnStr.append("entriesMap in <" + this + " is a different size than entriesMap in " + info + ">\n");
   }
   return returnStr.toString();
}

/** Return a string representation to show a picture of all the buckets
 *  in a PR.
 *  @param bucketList A List returned from BucketInfo.getAllBuckets().
 */
public static String masterBucketListToString(List bucketList) {
   StringBuffer aStr = new StringBuffer();
   for (int bucketId = 0; bucketId < bucketList.size(); bucketId++) {
      List aList = (List)(bucketList.get(bucketId));
      if ((aList == null) || (aList.size() == 0)) {
         aStr.append("No buckets exist for bucket id " + bucketId + "\n");
      } else {
         aStr.append(bucketListToString(aList));
      }
   }
   return aStr.toString();
}

/** Return a string representation to show a picture of all the buckets
 *  in a PR.
 *  @param bucketList A List of BucketInfo instances.
 */
public static String bucketListToString(List bucketList) {
   StringBuffer aStr = new StringBuffer();
   int insertPoint = aStr.length();
   int primaryCount = 0;
   for (int i = 0; i < bucketList.size(); i++) {
      BucketInfo info = (BucketInfo)(bucketList.get(i));
      aStr.append(info.toString() + "\n");
      if (info.isPrimary) {
         primaryCount++;
      }
   }
   aStr.insert(insertPoint, "Primary count: " + primaryCount + "\n");
   return aStr.toString();
}

/** Return true of the anObj is equal to this BucketInfo, false otherwise
 */
public boolean equals(Object anObj) {
   if (anObj == null) {
      return false;
   }
   if (!(anObj instanceof BucketInfo)) {
      return false;
   }
   BucketInfo arg = (BucketInfo)anObj;
   if (bucketId != arg.bucketId) {
      return false;
   }
   if (vmId != arg.vmId) {
      return false;
   }
   if (isPrimary != arg.isPrimary) {
      return false;
   }
   if (!(member.toString().equals(arg.member))) {
      return false;
   }
   return entriesMap.equals(arg.entriesMap);
}

protected static void removePayload(List<BucketDump> listOfMaps) {
   for (int i = 0; i < listOfMaps.size(); i++) {
      Map<Object, Object> aMap = listOfMaps.get(i).getValues();
      Iterator<Object> it = aMap.values().iterator();
      while (it.hasNext()) {
        Object value = it.next();
         if (value instanceof util.BaseValueHolder) {
            ((util.BaseValueHolder)value).extraObject = null;
         }
      }
   }
}

public int getBucketId() {
   return bucketId;
}
public InternalDistributedMember getMember() {
   return member;
}
public int getVmId() {
   return vmId;
}
public boolean getIsPrimary() {
   return isPrimary;
}
public Map getEntriesMap() {
   return entriesMap;
}
}

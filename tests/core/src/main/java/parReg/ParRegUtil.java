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
package parReg;

import hydra.CacheHelper;
import hydra.ClientCacheHelper;
import hydra.ClientRegionHelper;
import hydra.ConfigPrms;
import hydra.DiskStoreHelper;
import hydra.DistributedSystemHelper;
import hydra.HDFSStoreDescription;
import hydra.HDFSStoreHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.PoolDescription;
import hydra.PoolHelper;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import hdfs.HDFSUtil;
import pdx.PdxTest;
import pdx.PdxTestVersionHelper;
import rebalance.RebalanceUtil;
import util.BaseValueHolder;
import util.NameFactory;
import util.PRObserver;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.ClientMetadataService;
import com.gemstone.gemfire.cache.client.internal.ClientPartitionAdvisor;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;

/** Helper methods for partitioned region testing.
 *  Contains methods that invoke internal hooks into the partitioned
 *  region implementation.
 */
public class ParRegUtil {

// used for comparing objects in verification methods
// exact means objects must be .equal()
// equivalent, which applies only to ValueHolders, means that the myValue field
//    in a ValueHolder can be the equals after doing toString(), but not necessarily equals()
public static int EQUAL = 1;
public static int EQUIVALENT = 2;

// Used to save state for repeated calls to verify bucket copies; this is 
// useful for verifying large regions allowing the verify task to be batched
protected int verifyBucketCopiesBucketId = -1;
protected StringBuffer verifyBucketCopiesErrStr  = new StringBuffer();
protected boolean verifyBucketCopiesCompleted = false;

// blackboard key
protected static final String diskRecoveryKey = "DiskRecoveryFileNames_";


//================================================================================    
// Convenience methods for partitioned region internal hooks

/** Return a random key currently in the given region.
 *
 *  @param aRegion The region to use for getting an existing key (may
 *         or may not be a partitioned region).
 *  @param uniqueKeys True if each thread operates on a unique set of keys.
 *  @param numThreads Used if uniqueKeys is true. This is the number of 
 *         threads using uniqueKeys.
 *
 *  @returns A key in the region.
 */
public static Object getExistingKey(Region aRegion, boolean uniqueKeys, int numThreads, boolean useServerKeys) {
   Object key = null;
   try {
      PartitionedRegion parReg = (PartitionedRegion)aRegion;   
      key = getExistingKey(parReg, uniqueKeys, numThreads);
   } catch (ClassCastException e) { // not a partitioned region
      // just let key be null so we can try it the keySet() below
   }

   if (key == null) {
      Set aSet = null;
      if (useServerKeys) {
        aSet = aRegion.keySetOnServer();
      } else {
        aSet = aRegion.keySet();
      }
      Iterator it = aSet.iterator();
      return getKey(it, uniqueKeys, numThreads);
   } else {
      return key;
   }
}

/** Return a random key currently in the given region.
 *
 *  @param aRegion The PartitionedRegion to use for getting an existing key.
 *  @param uniqueKeys True if each thread operates on a unique set of keys.
 *  @param numThreads Used if uniqueKeys is true. This is the number of 
 *         threads using uniqueKeys.
 *
 *  @returns A key in the region.
 */
public static Object getExistingKey(PartitionedRegion parReg, boolean uniqueKeys, int numThreads) {
   Set keySet = null;
   try {
      keySet = parReg.getSomeKeys(TestConfig.tab().getRandGen());
   } catch (java.io.IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (java.lang.ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   Iterator iter = keySet.iterator();
   return getKey(iter, uniqueKeys, numThreads);
}

/** Given an iterator on a set of keys, return one of them 
 *  honoring uniqueKeys.
 * 
 *  @param it An iterator on a set of keys.
 *  @param uniqueKeys True if each thread operates on a unique set of keys.
 *  @param numThreads Used if uniqueKeys is true. This is the number of 
 *         threads using uniqueKeys.
 *
 *  @returns A key from the iterator.
 */
static protected Object getKey(Iterator it, boolean uniqueKeys, int numThreads) {
   int myTid = RemoteTestModule.getCurrentThread().getThreadId();
   while (it.hasNext()) {
      Object key = it.next();
      if (uniqueKeys) {
         long keyIndex = NameFactory.getCounterForName(key);
         if ((keyIndex % numThreads) == myTid) {
            return key;
         }
      } else {
         return key;
      }
   }
   return null;
}

/** Use internal hooks to get the number of entries in this VM, but not the
 *  number of entries in the entire partitioned region.
 * 
 *  aRegion must be a PartitionedRegion.
 */
public static long getLocalSize(Region aRegion) {
   return ((PartitionedRegion)aRegion).getLocalSize();
}

/** Return a random key currently in the given region.
 *
 *  @param aRegion The region to use for getting an existing key (may
 *         or may not be a partitioned region).
 *  @param uniqueKeys True if each thread operates on a unique set of keys.
 *  @param numThreads Used if uniqueKeys is true. This is the number of 
 *         threads using uniqueKeys.
 *  @param numKeysToGet Return a list of this many keys, or fewer if
 *         there aren't enough keys available.
 *  @param useServerKeys Use keysOnServer to get existing keys.
 *
 *  @returns A list of keys in the region.
 */
public static List getExistingKeys(Region aRegion, boolean uniqueKeys, int numThreads, int numKeysToGet, boolean useServerKeys) {
   Log.getLogWriter().info("Trying to get " + numKeysToGet + " existing keys...");
   Set aSet = null;
   if (useServerKeys) {
     aSet = aRegion.keySetOnServer();
   } else {
     aSet = aRegion.keySet();
   }
   Iterator it = aSet.iterator();
   return getKeys(it, uniqueKeys, numThreads, numKeysToGet);
}

/** Return a list of random keys currently in the given region.
 *
 *  @param aRegion The PartitionedRegion to use for getting an existing key.
 *  @param uniqueKeys True if each thread operates on a unique set of keys.
 *  @param numThreads Used if uniqueKeys is true. This is the number of 
 *         threads using uniqueKeys.
 *  @param numKeysToGet Return a list of this many keys, or fewer if
 *         there aren't enough keys available.
 *
 *  @returns A key in the region.
 */
public static List getExistingKeys(PartitionedRegion parReg, boolean uniqueKeys, int numThreads, int numKeysToGet) {
   Set keySet = null;
   try {
      keySet = parReg.getSomeKeys(TestConfig.tab().getRandGen());
   } catch (java.io.IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (java.lang.ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   Iterator iter = keySet.iterator();
   return getKeys(iter, uniqueKeys, numThreads, numKeysToGet);
}

/** Given an iterator on a set of keys, return a list of them 
 *  honoring uniqueKeys.
 * 
 *  @param it An iterator on a set of keys.
 *  @param uniqueKeys True if each thread operates on a unique set of keys.
 *  @param numThreads Used if uniqueKeys is true. This is the number of 
 *         threads using uniqueKeys.
 *  @param numKeysToGet Return a list of this many keys, or fewer if
 *         there aren't enough keys available.
 *
 *  @returns A key from the iterator.
 */
static protected List getKeys(Iterator it, boolean uniqueKeys, int numThreads, int numKeysToGet) {
   int myTid = RemoteTestModule.getCurrentThread().getThreadId();
   List keyList = new ArrayList();
   while (it.hasNext()) {
      Object key = it.next();
      if (uniqueKeys) {
         long keyIndex = NameFactory.getCounterForName(key);
         if ((keyIndex % numThreads) == myTid) {
            keyList.add(key);
         }
      } else {
         keyList.add(key);
      }
      if (keyList.size() >= numKeysToGet) {
         return keyList;
      }
   }
   return keyList; // return < numKeysToGet, but this was a many as we could find
}

/**
 * Get the number of regions from ClientMetadataService
 */
  public static int getRegionCountFromClientMetaData() {
    ClientMetadataService cms = ((GemFireCacheImpl)GemFireCacheImpl
        .getInstance()).getClientMetadataService();
    return cms.getClientPRMetadata_TEST_ONLY().size();
  }
  
//================================================================================    
// Verification methods for partitioned region internal hooks

/** Verify that the given key is in the local cache for aRegion with the given expected value.
 *
 * @param aRegion The region to verify.
 * @param key The key in aRegion to verify.
 * @param expectedValue The expected value of key in aRegion
 *
 * @throws TestException if aRegion's local cache does not contain the expected value for key
 */
/* lynn local caching not implmented in congo
protected static void verifyInLocalCache(Region aRegion, Object key, Object expectedValue) {
   // key should be in the local cache
   boolean localCacheContainsKey = ParRegUtil.localCacheContainsKey(aRegion, key);
   if (aRegion.getAttributes().getPartitionAttributes().isLocalCacheEnabled()) {
      if (!localCacheContainsKey) {
         throw new TestException("Expected key " + key + " to be present in the local cache, " +
                   "but localCacheContainsKey is " + localCacheContainsKey);
      }

      Object localCacheValue = ParRegUtil.localCacheGet(aRegion, key);
      if (!localCacheValue.equals(expectedValue.toString())) {
         throw new TestException("Expected local cache value for key " + key + " to be " + 
                   expectedValue + ", but it is " + localCacheValue);
      }
   } else { // no local cache
      if (localCacheContainsKey) {
         throw new TestException("No local cache, but localCacheContainsKey for key " + key + " is " +
                   localCacheContainsKey);
      }
   }
}
*/

/** Verify that the given object is an instance of ValueHolder
 *  with expectedValue as the myValue field.
 *
 * @param key The key whose value we are checking.
 * @param expectedValue The expected myValue field of a ValueHolder in aRegion, or null
 *        if the expected value should be null.
 * @param valuetoCheck This is expected to be a ValueHolder or PdxInstance
 *        representing a ValueHolder, whose myValue field compares
 *        to expectedValue, according to comparStrategy
 * @param compareStrategy Whether the compare is equals or equivalent (for ValueHolders)
 *
 * @throws TestException if the result of a get on key does not have the expected value.
 */
public static void verifyMyValue(Object key, Object expectedValue, Object valueToCheck, int compareStrategy) {
   ParRegUtilVersionHelper.verifyMyValue(key, expectedValue, valueToCheck, compareStrategy);
}

/** Verify that the two given objects are equal, giving special consideration
 *  to instances of ValueHolder.
 *
 * @param key The key whose value we are checking.
 * @param obj1 One object to compare.
 * @param obj2 The other object to compare.
 * @param compareStrategy Whether the compare is equals or equivalent (for ValueHolders)
 *
 * @throws TestException if the result of a get on key does not have the expected value.
 */
protected static void verifyValue(Object key, Object obj1, Object obj2, int compareStrategy) {
   if (obj1 instanceof BaseValueHolder) {
      verifyMyValue(key, ((BaseValueHolder)obj1).myValue, obj2, EQUAL);
   } else if (obj2 instanceof BaseValueHolder) {
      verifyMyValue(key, ((BaseValueHolder)obj2).myValue, obj1, EQUAL);
   } else if (obj1 == null) {
      if (obj2 != null) {
         throw new TestException(obj1 + " is not equal to " + TestHelper.toString(obj2));
      }
   } else if (obj2 == null) {
      if (obj1 != null) {
         throw new TestException(obj2 + " is not equal to " + TestHelper.toString(obj1));
      }
   } else if ((obj1 instanceof byte[]) && (obj2 instanceof byte[])) {
     if (!Arrays.equals((byte[])obj1, (byte[])obj2)) {
       throw new TestException(TestHelper.toString(obj1) + " is not equal to  " + TestHelper.toString(obj2));
     }
   } else if (!obj1.equals(obj2)) {
      throw new TestException(TestHelper.toString(obj1) + " is not equal to " + TestHelper.toString(obj2));
   } 
}

/** Verify containsKey for the given region and key.
 *
 * @param aRegion The region to verify.
 * @param key The key in aRegion to verify.
 * @param expected The expected value of containsKey()
 *
 * @throws TestException if containsKey() has the wrong value
 */
public static void verifyContainsKey(Region aRegion, Object key, boolean expected) {
   boolean containsKey = aRegion.containsKey(key);
   if (containsKey != expected) {
      RegionAttributes attr = aRegion.getAttributes();
      String extra = ParRegUtilVersionHelper.getVersionTagStr(aRegion, key);
      throw new TestException("Expected containsKey() for " + key + " to be " + expected + 
                " in " + aRegion.getFullPath() + ", but it is " + containsKey + extra);
   }
}

/** Verify containsValueForKey for the given region and key.
 *
 * @param aRegion The region to verify.
 * @param key The key in aRegion to verify.
 * @param expected The expected value of containsKey()
 *
 * @throws TestException if containsValueforKey() has the wrong value
 */
public static void verifyContainsValueForKey(Region aRegion, Object key, boolean expected) {
   boolean containsValueForKey = aRegion.containsValueForKey(key);
   if (containsValueForKey != expected) {
      RegionAttributes attr = aRegion.getAttributes();
      throw new TestException("Expected containsValueForKey() for " + key + " to be " + expected + 
                " in " + aRegion.getFullPath() + ", but it is " + containsValueForKey);
   }
}

/** Verify that the size of the given region is expectedSize.
 *
 * @param aRegion The region to verify.
 * @param expectedSize The expected size of aRegion.
 *
 * @throws TestException if size() has the wrong value
 */
public static void verifySize(Region aRegion, final int expectedSize) {
  int size = aRegion.size();
  if (size != expectedSize) {
    ParRegUtilVersionHelper.dumpBackingMap(aRegion);
    throw new TestException("Expected size of " + aRegion.getFullPath() + " to be " +
       expectedSize + ", but it is " + size);
  }
}

/** Verify that the size of the given region is expectedSize.
*
* @param aRegion The region to verify.
* @param expectedSize The expected size of aRegion.
*
* @throws TestException if size() has the wrong value
*/
public static void verifySize(Region aRegion, final int expectedSize, Map regionSnapshot) {
 int size = aRegion.size();
 if (size != expectedSize) {
     LogWriter log = Log.getLogWriter();
     StringBuffer sb = new StringBuffer(10000);
     sb.append("region has wrong size (").append(size)
       .append(").\n");
     StringBuffer notThere = new StringBuffer();
     notThere.append("These keys are in the region but shouldn't be:\n");
     int notThereCount = 0;
     for (Iterator it = aRegion.entrySet().iterator(); it.hasNext(); ) {
       Map.Entry entry = (Map.Entry)it.next();
       if (!regionSnapshot.containsKey(entry.getKey())) {
         notThere.append(entry.getKey()).append("\n");
         notThereCount++;
       }
     }
     if (notThereCount > 0) {
       sb.append(notThere);
     }
     StringBuffer there = new StringBuffer();
     there.append("These keys are not in the region but should be:\n");
     int thereCount = 0;
     for (Iterator it = regionSnapshot.entrySet().iterator(); it.hasNext(); ) {
       Map.Entry entry = (Map.Entry)it.next();
       if (!aRegion.containsKey(entry.getKey())) {
         there.append(entry.getKey()).append("\n");
         thereCount++;
       }
     }
     if (thereCount > 0) {
       sb.append(there);
     }
     log.info(sb.toString());
     
     throw new TestException("Expected size of " + aRegion.getFullPath() + " to be " +
        expectedSize + ", but it is " + size);
  }
}
  /**
   * Dump all data stores for the given partitioned region.
   */
  public static void dumpAllDataStores(Region aRegion) {
    try {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      pr.dumpAllBuckets(false,Log.getLogWriter().convertToLogWriterI18n());
    }
    catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    catch (ClassCastException e) {
      Log
          .getLogWriter()
          .info(
              "Not dumping data stores on "
                  + aRegion
                  + " because it is not a PartitionedRegion (probably because it's a region "
                  + " from a bridge client");
    }
  }

  /**
   * Dumps all partitioned regions of the current cache in this member. 
   */
  public static void dumpAllPartitionedRegions() {
    Log.getLogWriter().info("[dumpAllPartitionedRegions]");
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      Log.getLogWriter().info("[dumpAllPartitionedRegions] no cache found " +
          "in this member");
      return;
    }
    Set parRegs = cache.getPartitionedRegions();
    if (parRegs.isEmpty()) {
      // edge clients will be "empty"
      Log.getLogWriter().info("[dumpAllPartitionedRegions] no partitioned " +
          "regions found in this member");
    }
    Log.getLogWriter().info("[dumpAllPartitionedRegions] dumping " +
        parRegs.size() + " partitioned regions...");
    Throwable thrown = null;
    for (Iterator iter = parRegs.iterator(); iter.hasNext();) {
      try {
        PartitionedRegion pr = (PartitionedRegion) iter.next();
        Log.getLogWriter().info("[dumpAllPartitionedRegions] dumping " + pr);
        pr.dumpAllBuckets(false /* distribute? no! */, Log.getLogWriter().convertToLogWriterI18n());
      }
      catch (VirtualMachineError err) {
        throw err;
      }
      catch (Throwable e) {
        Log.getLogWriter().error("[dumpAllPartitionedRegions] failed", e);
        if (thrown == null) {
          thrown = e;
        }
      }
    }
    Log.getLogWriter().info("[dumpAllPartitionedRegions] finished dumping " +
        parRegs.size() + " partitioned regions.");
    if (thrown != null) {
      throw new TestException(TestHelper.getStackTrace(thrown));
    }
  }

/** Verify consistency for PR metadata.
 */
public static void verifyPRMetaData(Region aRegion) {
   Log.getLogWriter().info("Attempting to verify PR metadata for " + aRegion.getFullPath());
   try {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      try {
         pr.validateAllBuckets();
      } catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } catch (ClassCastException e) {
      Log.getLogWriter().info("Not verifying PR metadata on " + aRegion + 
          " because it is not a PartitionedRegion (probably because it's a region " +
          " from a bridge client");
   }
}

/** Verify there is only one primary per bucket and that the current
 *  VM's view of each bucket's primary agrees.
 *
 *  @param numExtraCopies - The number of expected extra copies
 *         of data for aRegion (in addition to the primary copy). 
 *         of data for aRegion. If this is -1, then this will expect
 *         that we have less than full redundancy at the time of this
 *         check.
 */
public static void verifyPrimaries(Region aRegion, int numExtraCopies) {
   String errStr = _verifyPrimaries(aRegion, numExtraCopies, true);
   if (errStr != null) {
      throw new TestException(errStr.toString());
   }
}

/** Verify there is only one primary per bucket and that the current
 *  VM's view of each bucket's primary agrees.
 *  Due to the asychronous nature of electing primaries, the test might
 *  need to wait a bit before all primaries are selected. If we get a
 *  primary failure, sleep, then validate primaries again until
 *     1) we pass the validation
 *     2) we have been trying to pass the validation for N millis.
 *
 *  @param numExtraCopies - The number of expected extra copies
 *         of data for aRegion (in addition to the primary copy). 
 *         If this is -1, then this will expect
 *         that we have less than full redundancy at the time of this
 *         check.
 */
public static void verifyPrimariesWithWait(Region aRegion, int numExtraCopies) {
   final int THRESHOLD_MILLIS = 180000;
   final int SLEEP_MILLIS = 5000;
   String errStr = _verifyPrimaries(aRegion, numExtraCopies, true);
   int numAttempts = 1;
   long startTime = System.currentTimeMillis();
   while (errStr != null) {
      Log.getLogWriter().info("Got a failure in verifyPrimaries, now sleeping for " +
          SLEEP_MILLIS + " millis before trying again, failure is: " + errStr);
      MasterController.sleepForMs(SLEEP_MILLIS);
      errStr = _verifyPrimaries(aRegion, numExtraCopies, false);
      numAttempts++;
      long duration = System.currentTimeMillis() - startTime;
      if (errStr == null) {
         Log.getLogWriter().info("verifyPrimaries successful after " + numAttempts + " attempts and " + duration + " millis");
         return;
      }
      if (duration > THRESHOLD_MILLIS) {
         Log.getLogWriter().info("After " + numAttempts + " attempts and " + duration + " millis, verifyPrimaries still fails: " + errStr);
         throw new TestException(errStr.toString());
      }
   }
}

/** Verify there is only one primary per bucket and that the current
 *  VM's view of each bucket's primary agrees.
 *
 *  @param numExtraCopies - The number of expected extra copies
 *         of data for aRegion (in addition to the primary copy). 
 *         If this is -1, then this will expect
 *         that we have less than full redundancy at the time of this
 *         check.
 *  @param logProgress If true, then log each bucket as it is verified, 
 *                     otherwise run silent.
 *  @returns A status string of any primary bucket failures, or null if none.
 */
public static String _verifyPrimaries(Region aRegion, int numExtraCopies, boolean logProgress) {
   Log.getLogWriter().info("Attempting to verify primaries for " + aRegion.getFullPath() + " with numExtraCopies " + numExtraCopies);
   StringBuffer errStr = new StringBuffer();
   int expectedNumBuckets = numExtraCopies + 1;
   try {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      try {
         int totalBuckets = pr.getTotalNumberOfBuckets();
         for (int i = 0; i < totalBuckets; i++) {
             if (logProgress) {
                Log.getLogWriter().info("Verifying primary for bucket id " + i + " out of " + totalBuckets + " buckets.");
             }
             List aList = pr.getBucketOwnersForValidation(i);
             if (aList.size() == 0) { // there are no buckets for this bucket id
                continue;
             }
             int primaryCount = 0;
             Object primaryMember = null;
             StringBuffer primaryStatusStr = new StringBuffer();
             for (int j = 0; j < aList.size(); j++) {
                Object[] tmpArr = (Object[])(aList.get(j));
                Object member = tmpArr[0];
                Boolean isPrimary = (Boolean)(tmpArr[1]);
                primaryStatusStr.append("<" + member + " isPrimary=" + isPrimary + "> ");
                if (isPrimary.booleanValue()) {
                   primaryCount++;
                   primaryMember = member;
                }
             }
             if (primaryCount != 1) {
                errStr.append("Expected 1 primary for bucket id " + i + ", but found " + primaryCount + ": " + primaryStatusStr + "\n");
             } else { // found 1 primary; check that this vm agrees
                com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember member = pr.getBucketPrimary(i);
                if (!(member.toString().equalsIgnoreCase(primaryMember.toString()))) {
                   errStr.append("Primary for bucket id " + i + " is " + member + ", but " + primaryMember + " claims to be the primary; " + primaryStatusStr + "\n");
                }
            }
            if (numExtraCopies != -1) {
               if (aList.size() != expectedNumBuckets) { 
                  errStr.append("For bucket id " + i + ", expected " + expectedNumBuckets + " members in primary list, but found " + aList.size() + "; " + primaryStatusStr + "\n");
               }
            } // else we don't know how many copies to expect
         }
      } catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (com.gemstone.gemfire.internal.cache.ForceReattemptException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } catch (ClassCastException e) {
      Log.getLogWriter().info("Not verifying PR primaries on " + aRegion + 
          " because it is not a PartitionedRegion (probably because it's a region " +
          " from a bridge client");
   }
   if (errStr.length() == 0) {
      return null;
   } else {
      return errStr.toString();
   }
}

/** Internal consistency check for PR; ensure that all copies of the
 *  PR's data are consistent. This returns when all buckets are verified.
 *  Compare with verifyBucketCopiesBatched which can be called repeatedly
 *  in a batched task and throws StopSchedulingTaskOnClientOrder to signal
 *  it has completed all verification. Note this is a static method.
 *
 *  @param aRegion - A partitioned region.
 *  @param numExtraCopies - The number of expected extra copies
 *         of data for aRegion (in addition to the primary copy). 
 *         If this is -1, then this will expect
 *         that we have less than full redundancy at the time of this
 *         check.
 */
public static void verifyBucketCopies(Region aRegion, int numExtraCopies) {
   Log.getLogWriter().info("Attempting to verify bucket copies for " + aRegion.getFullPath() + " with numExtraCopies " + numExtraCopies);
   try {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
   } catch (ClassCastException e) {
      Log.getLogWriter().info("Not verifying bucket copies on " + aRegion + 
          " because it is not a PartitionedRegion (probably because it's a region " +
          " from a bridge client");
      return;
   }
   ParRegUtil instance = new ParRegUtil();
   boolean done = false;
   while (!done) {
      done = instance.verify_bucket_copies(aRegion, numExtraCopies, Long.MAX_VALUE);
   } 
}

/** Internal consistency check for PR; ensure that all copies of the
 *  PR's data are consistent. This must be called repeatedly on the
 *  same ParRegUtil instance by the same thread until 
 *  StopSchedulingTaskOnClientOrder is thrown to signal completion.  
 *  This method will return after running for TestHelperPrms.minTaskGranularitySec
 *  seconds to enable batching.
 *  Compare with verifyBucketCopies, which verifies all buckets in one 
 *  call. Note this is an instance method.
 *
 *  @param aRegion - A partitioned region.
 *  @param numExtraCopies - The number of expected extra copies
 *         of data for aRegion (in addition to the primary copy). 
 *         If this is -1, then this will expect
 *         that we have less than full redundancy at the time of this
 *         check.
 */
public void verifyBucketCopiesBatched(Region aRegion, int numExtraCopies) {
   Log.getLogWriter().info("Attempting to verify bucket copies (batched) for " + aRegion.getFullPath());
   try {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
   } catch (ClassCastException e) {
      throw new StopSchedulingTaskOnClientOrder("Not verifying bucket copies on " + aRegion + 
          " because it is not a PartitionedRegion (probably because it's a region " +
          " from a bridge client");
   }
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, Long.MAX_VALUE);
   boolean done = verify_bucket_copies(aRegion, numExtraCopies, minTaskGranularitySec);
   if (done) {
      throw new StopSchedulingTaskOnClientOrder("Done verifying all buckets");
   }
}

/** Verify bucket copies (private).
 *
 *  @param aRegion - A partitioned region.
 *  @param numExtraCopies - The number of expected redundant copies
 *         of data for aRegion. If this is -1, then this will expect
 *         that we have less than full redundancy at the time of this
 *         check.
 *  @param secondsToRun - The number of seconds to verify bucket copies;
 *         This method returns either when all bucket copies are verified
 *         or when secondsToRun seconds have elapsed, whichever comes first
 *         (this allows batching). Use Long.MAX_VALUE to complete all 
 *         verification in this call.
 *
 *  @returns True if all bucket copies have been verified, false if there is
 *           still more to do.
 */
private boolean verify_bucket_copies(Region aRegion, int numExtraCopies, long secondsToRun) {
   if (verifyBucketCopiesCompleted) { 
      return true;
   }
   PartitionedRegion pr = (PartitionedRegion)aRegion;
   int totalBuckets = pr.getTotalNumberOfBuckets();
   int expectedNumCopies = numExtraCopies + 1;
   long msToRun = secondsToRun * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   while (true) {
      verifyBucketCopiesBucketId++;
      if (verifyBucketCopiesBucketId >= totalBuckets) {
         break; // we have verified all buckets
      }
      Log.getLogWriter().info("Verifying data for bucket id " + verifyBucketCopiesBucketId + " out of " + totalBuckets + " buckets");
      List listOfMaps = null;
      try {
        listOfMaps = pr.getAllBucketEntries(verifyBucketCopiesBucketId);
      }
      catch (ForceReattemptException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      // check that we have the correct number of copies of each bucket
      // listOfMaps could be size 0; this means we have no entries in this particular bucket
      int size = listOfMaps.size();
      if (size == 0) {
         continue;
      }
      if (numExtraCopies != -1) {
         if (size != expectedNumCopies) {
            verifyBucketCopiesErrStr.append("For bucketId " + verifyBucketCopiesBucketId + 
                 ", expected " + expectedNumCopies + 
                 " bucket copies, but have " + listOfMaps.size() + "\n");
         }
      } // else we don't know how many copies to expect

      // Check that all copies of the buckets have the same data
      if (listOfMaps.size() > 1) {
          Object firstMap = listOfMaps.get(0);
         for (int j = 1; j < listOfMaps.size(); j++) {
             Object aMap = listOfMaps.get(j);
             verifyBucketCopiesErrStr.append(compareBucketMaps(firstMap, aMap));
         }

      }

      if ((secondsToRun != 0) && 
          (System.currentTimeMillis() - startTime >= msToRun)) {
         return false;  // we are done with this batch
      }
   }
   verifyBucketCopiesCompleted = true;
   if (verifyBucketCopiesErrStr.length() != 0) {
     final int MAX_ERR_STR_SIZE = 5000;
     if (verifyBucketCopiesErrStr.length() > MAX_ERR_STR_SIZE) {
       String errStr = verifyBucketCopiesErrStr.toString();
       Log.getLogWriter().info("Full error string: " + errStr);
       throw new TestException(errStr.substring(0, MAX_ERR_STR_SIZE) + "\n...<more in log file>");
     }
     throw new TestException(verifyBucketCopiesErrStr.toString());
   }
   return true;
}
   
/** Compare the contents of 2 bucket maps. Return a string describing
 *  any descrepancies.
 *
 *  @param map1 A map of keys/values, ie a bucket.
 *  @param map2 A map of keys/values, ie a bucket.
 */
protected static String compareBucketMaps(Object dump1, Object dump2) {
   return ParRegUtilVersionHelper.compareBucketMaps(dump1, dump2);
}
  
/** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
 *  which is equivalent to a full GII. 
 *
 *  @param aRegion The region to register interest on.
 *  @param loopUntilSuccess If true, loop until registerInterest is successful
 *         while allowing remote PartitionOfflineExceptions.
 */
public static void registerInterest(Region aRegion, boolean loopUntilSuccess) {
   while (true) {
      try {
         Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES");
         aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
         Log.getLogWriter().info("Done calling registerInterest for all keys, " +
             "result interest policy KEYS_VALUES, " + aRegion.getFullPath() + 
             " size is " + aRegion.size());
         return;
      } catch (ServerOperationException e) {
         if (e.getCause() instanceof PartitionOfflineException) {
            Log.getLogWriter().info("Caught " + e + " caused by " + e.getCause() +
                "; retrying registerInterest, continuting test");
         } else {
            throw e;
         }
      }
      MasterController.sleepForMs(1000);
   }
}

/** Register interest with ALL_KEYS, and InterestPolicyResult = KEYS_VALUES
 *  which is equivalent to a full GII.
 *
 *  @param aRegion The region to register interest on.
 */
public static void registerInterest(Region aRegion) {
   Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES");
   aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
   Log.getLogWriter().info("Done calling registerInterest for all keys, " +
       "result interest policy KEYS_VALUES, " + aRegion.getFullPath() + 
       " size is " + aRegion.size());
}

/** Verify that each bucket copy is on a different host.
 */
public static void verifyBucketsOnUniqueHosts(Region aRegion) {
   Log.getLogWriter().info("Attempting to verify bucket copies are on unique hosts...");
   StringBuffer errStr = new StringBuffer();
   try {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      try {
         int totalBuckets = pr.getTotalNumberOfBuckets();
         // TODO: need to do something else for vsphere check?
         Object lc = null;
        if (lc == null) { // Fix for #44750
         for (int i = 0; i < totalBuckets; i++) {
            Log.getLogWriter().info("Verifying bucket copies are on unique hosts for bucket id " + i + " out of " + totalBuckets + " buckets.");
            List aList = pr.getBucketOwnersForValidation(i);
            if (aList.size() == 0) { // there are no buckets for this bucket id
//               Log.getLogWriter().info("There are no existing buckets for bucket id " + i);
               continue;
            }
            StringBuffer statusStr = new StringBuffer();
            statusStr.append("<Bucket id " + i + " has copies on the following members: ");
            boolean onSameHost = false;
            List<String> hostList = new ArrayList<String>();
            for (int j = 0; j < aList.size(); j++) {
               Object[] tmpArr = (Object[])(aList.get(j));
               InternalDistributedMember member = (InternalDistributedMember)(tmpArr[0]);
               String hostName = member.getHost();
               if (hostList.contains(hostName)) {
                  onSameHost = true;
               }
               hostList.add(hostName);
               statusStr.append(member + " ");
            }
            if (onSameHost) {
               errStr.append("Found more than 1 copy of bucket id " + i + " on the same host; " + statusStr + "\n");
            } 
         }
         } else { // We are on vSphere (ESX VMs) and are acquiring license dynamically.
           for (int i = 0; i < totalBuckets; i++) {
             Log.getLogWriter().info("Verifying bucket copies are on unique ESX hosts for bucket id " + i + " out of " + totalBuckets + " buckets.");
             List aList = pr.getBucketOwnersForValidation(i);
             if (aList.size() == 0) { // there are no buckets for this bucket id
                continue;
             }
             StringBuffer statusStr = new StringBuffer();
             statusStr.append("<Bucket id " + i + " has copies on the following members: ");
             boolean onSameHost = false;
             List<String> hostTokenList = new ArrayList<String>();
             for (int j = 0; j < aList.size(); j++) {
                Object[] tmpArr = (Object[])(aList.get(j));
                InternalDistributedMember member = (InternalDistributedMember)(tmpArr[0]);
                String hostToken = (String)(tmpArr[2]);
                if (hostTokenList.contains(hostToken)) {
                   onSameHost = true;
                }
                hostTokenList.add(hostToken);
                statusStr.append(member + "[" + hostToken + "] ");
             }
             if (onSameHost) {
                errStr.append("Found more than 1 copy of bucket id " + i + " on the same ESX host; " + statusStr + "\n");
             }
          }
         }
      } catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (com.gemstone.gemfire.internal.cache.ForceReattemptException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } catch (ClassCastException e) {
      Log.getLogWriter().info("Not verifying PR primaries on " + aRegion + 
          " because it is not a PartitionedRegion (probably because it's a region " +
          " from a bridge client");
   }
   if (errStr.length() != 0) {
      throw new TestException(errStr.toString());
   }
}

/** Do a rebalance and verify balance was improved.
 *  If evictionPercentage > 0 (the default) then we have heapLRU and this can cause
 *  simulate and rebalance results to differ if eviction kicks in between.  (See BUG 44899).
 */ 
public static void doRebalance() {
  doRebalance(CacheHelper.getCache());
}

public static void doRebalance(GemFireCache cache) {
   ResourceManager resMan = cache.getResourceManager();
   boolean heapEviction = (resMan.getEvictionHeapPercentage() > 0);
   RebalanceFactory factory = resMan.createRebalanceFactory();
   try {
      RebalanceResults simulateResults = null;
      if (!heapEviction) {  
         Log.getLogWriter().info("Calling rebalance simulate");
         RebalanceOperation simulateOp = factory.simulate();
         simulateResults = simulateOp.getResults(); 
         Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(simulateResults, "Simulate"));
      }

      Log.getLogWriter().info("Starting rebalancing");
      RebalanceOperation rebalanceOp = factory.start();
      RebalanceResults rebalanceResults = rebalanceOp.getResults();
      Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "Rebalance"));

      if (!(heapEviction || (ConfigPrms.getHadoopConfig() != null))) {
        RebalanceUtil.isBalanceImproved(rebalanceResults); // this throws an exception if not improved
        RebalanceUtil.checkSimulateAgainstRebalance(simulateResults, rebalanceResults);
      }
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Hydra task to rebalance and verify.
 *  This will only run in one vm, even if many vms execute this task.
 *  This is coordinated by a blackboard counter. Once this has executed
 *  in a single vm, it won't rebalance again (unless the counter is zeroed).
 */
public static void HydraTask_rebalance() {
   PdxTest.initClassLoader();
   long counter = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.rebalance); 
   if (counter == 1) {
      ParRegUtil.doRebalance();
   }
}

/** Waits for recovery in the current vm for each root region.
 */
public synchronized static void HydraTask_waitForRecovery() {
   HydraTask_waitForRecovery(CacheHelper.getCache());
}
public synchronized static void HydraTask_waitForRecovery(GemFireCache cache) {
   Set<Region<?,?>> regSet = cache.rootRegions();
   Integer thisVmId = RemoteTestModule.getMyVmid();
   List prNames = new ArrayList();
   Iterator it = regSet.iterator();
   int numColocatedRegions = 0;
   while (it.hasNext()) {
      Region aRegion = (Region)(it.next());
      PartitionAttributes prAttr = aRegion.getAttributes().getPartitionAttributes();
      if (prAttr != null) { // is a partitioned region
         if (prAttr.getLocalMaxMemory() != 0) { // is a data store, not an acessor
            if (prAttr.getRedundantCopies() != 0) { // this PR has redundant copies to recover
               prNames.add(aRegion.getFullPath());
            }
            if (prAttr.getColocatedWith() != null) {
               numColocatedRegions++;
            }
         }
      }
   }
   if (prNames.size() != 0) {
      if (ConfigPrms.getHadoopConfig() != null) {
         int numPRs = (prNames.size() * 2) - numColocatedRegions;
         PRObserver.waitForRebalRecov(thisVmId, 1, numPRs, null, null, false);
      } else {
         PRObserver.waitForRebalRecov(thisVmId, 1, prNames.size(), prNames, null, false);
      }
   } else {
      Log.getLogWriter().info("No recovery to wait for; there are no data store PRs in this vm");
   }
}

/** todo@lhughes -- temporary wait to recreate buckets for HDFS data
 */
public synchronized static void HydraTask_recreateBucketsFromHDFS() {
   HydraTask_recreateBucketsFromHDFS(CacheHelper.getCache());
}
public synchronized static void HydraTask_recreateBucketsFromHDFS(GemFireCache cache) {
   Set<Region<?,?>> regSet = cache.rootRegions();
   Integer thisVmId = RemoteTestModule.getMyVmid();
   Iterator it = regSet.iterator();
   while (it.hasNext()) {
      Region aRegion = (Region)(it.next());
      PartitionAttributes prAttr = aRegion.getAttributes().getPartitionAttributes();
      if (prAttr != null) { // is a partitioned region
         if (prAttr.getLocalMaxMemory() != 0) { // is a data store, not an accessor
           Set keys = aRegion.keySet();
           Log.getLogWriter().info("after aRegion.keySet() recreateBucketsFromHDFS found keySet size = " + keys.size() + " with region.size() = " + aRegion.size() + " for " + aRegion.getFullPath());
           for (Iterator kit = keys.iterator(); kit.hasNext(); ) {
              Object key = kit.next();
              aRegion.get(key);
           }
           Log.getLogWriter().info("iterating aRegion.keySet() recreateBucketsFromHDFS found keySet size = " + keys.size() + " with region.size() = " + aRegion.size() + " for " + aRegion.getFullPath());
         }
      }
   }
}

//========================================================
//  Methods to help coordinate an end task disk recovery

/** For disk tests, write the diskDir names to the blackboard for later use by recovery.
 *   Each time this is called, the diskDirs will be written to the bb shared map under
 *   a new key. 
 */
public static void writeDiskDirsToBB(Region reg) {
  String diskStoreName = reg.getAttributes().getDiskStoreName();

  // perhaps this DiskStore is meant for HDFS (vs. the Region)
  if (diskStoreName == null && (ConfigPrms.getHadoopConfig() != null)) {
     HDFSStoreDescription hsd = HDFSStoreHelper.getHDFSStoreDescription(HDFSUtil.getHDFSStoreName(reg));
     diskStoreName = hsd.getDiskStoreDescription().getName();
  }

  if (diskStoreName == null) {
    Log.getLogWriter().info("The region " + reg.getFullPath() + " does not have a diskStore; not writing disk dirs to bb");
    return;
  }
  DiskStore dStore = DiskStoreHelper.getDiskStore(diskStoreName);
  File[] diskDirs = dStore.getDiskDirs();
  List diskDirNameList = new ArrayList();
  for (File aFile: diskDirs) {
    try {
      diskDirNameList.add(aFile.getCanonicalPath());
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  long counter = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.DiskDirsCounter);
  String bbKey = diskRecoveryKey + counter;
  ParRegBB.getBB().getSharedMap().put(bbKey, diskDirNameList);
  Log.getLogWriter().info("Put into ParRegBB shared map, key " + bbKey + ", value " + diskDirNameList);
}

/** Run validation on the PR after recovery (the PR must contain data)
 */
public static void validateAfterRecovery(Region reg) {
   Log.getLogWriter().info(reg.getFullPath() + " size is " + reg.size());
   if (reg.size() == 0) {
      throw new TestException("Expected " + reg.getFullPath() + " to have a size > 0, but it has size " + reg.size());
   }
   int redundantCopies = reg.getAttributes().getPartitionAttributes().getRedundantCopies();
   ParRegUtil.verifyPRMetaData(reg);
   ParRegUtil.verifyPrimaries(reg, redundantCopies);
   ParRegUtil.verifyBucketCopies(reg, redundantCopies);
}

/** Creates a DiskStore for the given hydra region config name, setting the diskDirs 
 *   to existing disk directories if ParRegPrm-recoverFromDisk is true. Disk dirs
 *   are set to dirs previously written to the blackboard by ParRegUtil.writediskDirsToBB.
 *   A different set of diskDirs is assigned to the diskStore ech time this is called.
 *   
 *   If no recovery is specified, then creates the DiskStore without explicitly setting
 *   disk dirs.
 *   
 *   If no diskStore is specified in the hydraRRegionConfigName, then no DiskStore is created.
 *   
 * @param hydraRegionConfigName The hydra config name for the region to be
 *                 defined for recovery. 
 */
public static void createDiskStoreIfNecessary(String hydraRegionConfigName) {
  boolean recoverFromDisk = ParRegPrms.getRecoverFromDisk();
  RegionAttributes attr = RegionHelper.getRegionAttributes(hydraRegionConfigName);
  String diskStoreName = attr.getDiskStoreName();

  // perhaps this DiskStore is meant for HDFS (vs. the Region)
  if (diskStoreName == null && attr.getDataPolicy().withHDFS()) {
     String hdfsStoreName = attr.getHDFSStoreName();
     diskStoreName = HDFSStoreHelper.getHDFSStoreDescription(hdfsStoreName).getDiskStoreDescription().getName();
  }

  if (diskStoreName == null) { // no diskStore named, so no DiskStore to create
    if (hydraRegionConfigName.indexOf("accessor") >= 0) {
      Log.getLogWriter().info("No diskStore to create for " + hydraRegionConfigName);
      return;
    }

    if (recoverFromDisk) {
      throw new TestException("hydra region config name " + hydraRegionConfigName +
      " did not specify a DiskStore name, but ParRegPrms-recoverFromDisk is true");
    }
    Log.getLogWriter().info("No diskStore defined in " + hydraRegionConfigName);
    return;
  } 

  // there is a diskStoreName
  if (recoverFromDisk) {
    DiskStoreFactory dsFactory = DiskStoreHelper.getDiskStoreFactory(diskStoreName);
    long counter = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.DiskRecoveryCounter);
    String bbKey = diskRecoveryKey + counter;
    Object anObj = ParRegBB.getBB().getSharedMap().get(bbKey);
    if (anObj == null) {
      throw new TestException("Unexpected null in blackboard map for key " + bbKey);
    }
    List<String> diskNameList = (List)(anObj);
    File[] diskDirs = new File[diskNameList.size()];
    for (int i = 0; i < diskNameList.size(); i++) {
      File aFile = new File(diskNameList.get(i));
      diskDirs[i] = aFile;
    }
    dsFactory.setDiskDirs(diskDirs);
    DiskStoreHelper.createDiskStore(diskStoreName, dsFactory);
  } else {
    DiskStoreHelper.createDiskStore(diskStoreName);
  }
}

/** This differs from util.CacheUtil.createRegion(String, String, String) in
 *  that this can use disk directories previously written to the blackboard
 *  by ParRegUtil.writeDiskDirsToBB and also can call ParRegUtil.createDiskStoreIfNecessary.
 *  Some tests use end tasks to bring up a new set of vms after the regular task
 *  vms shutdown to cause a disk recovery. The end tasks have different vmIDs
 *  than the regular tasks, so end tasks must read the disk directory names
 *  created by the regular tasks from the blackboard (and contain the regular
 *  task's vmIds) and make the end tasks use them for recovery.
 *
 *  Create a region either by creating the cache with a declarative xml file
 *  or create the region programmatically. Which method chosen depends on the
 *  hydra parameter CachePrms.useDeclarativeXmlFile and whether this VM already
 *  has a connection to the distributed system.
 *
 *  @param cacheDescriptName The name of the cache description to use to create 
 *         the cache.
 *  @param regionDescriptName The name of the region description to use if the 
 *         region is created programmatically.
 *  @param xmlFile The xmlFile to use if the region is created with the xml file.
 *
 *  Note: This can only honor CachePrms.useDeclarativeXmlFile or the xmlFile
 *        parameter if we are not already connected. If we already have a connection 
 *        to the distributed system, then creating the cache will use any declarative
 *        xml file used when the connection was established, if any declarative xml file
 *        was set during the connect. See the comments in the code below.
 *
 */ 
public static Region createRegion(String cacheDescriptName, String regionDescriptName, String xmlFile) {
   Region createdRegion = null;
   RegionDescription regDescript = RegionHelper.getRegionDescription(regionDescriptName);
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   if ((ds != null) && ds.isConnected()) {
      Cache theCache = CacheHelper.getCache();
      if (theCache == null) {
         // we are already connected to the distributed system, thus the declarative
         // file (if any) was already "set" when we connected; at this point, all we
         // can do is open the cache and if there was a declarative xml file set already
         // then the region will be created when the cache is opened, otherwise we must
         // create the region programmatically
         theCache = CacheHelper.createCache(cacheDescriptName);
         createdRegion = theCache.getRegion(regDescript.getRegionName());
         if (createdRegion == null) {
            createdRegion = createRegionProgrammatically(regionDescriptName);
         }
      } else { // we are connected and already have a cache, just create the region
         createdRegion = createRegionProgrammatically(regionDescriptName);
      }
   } else { // not connected; we have the opportunity to "set" any declarative, if desired
      if (TestConfig.tab().booleanAt(util.CachePrms.useDeclarativeXmlFile)) {
         // create the region using a declarative xml file; creating the cache creates the region
         // when an xml file is passed in, assuming the xmlFile specifies a region
         Log.getLogWriter().info("Creating " + regDescript.getRegionName() + " with " + xmlFile);
         Cache theCache = CacheHelper.createCacheFromXml(xmlFile);
         if (theCache == null)
            throw new TestException("Unexpected null cache");
         createdRegion = theCache.getRegion(regDescript.getRegionName());
         if (createdRegion == null)
            throw new TestException("Unexpected null region");
         Log.getLogWriter().info("Finished creating " + TestHelper.regionToString(createdRegion, true) +
             " by using " + xmlFile);
      } else { // create the region programmatically
         Cache theCache = CacheHelper.createCache(cacheDescriptName);
         createdRegion = createRegionProgrammatically(regionDescriptName);
      }
   } 
   return createdRegion;
}

/** Return a region created programmatically, rather than with xml
 */
private static Region createRegionProgrammatically(String regDescriptName) {
   ParRegUtil.createDiskStoreIfNecessary(regDescriptName);
   RegionAttributes attr = RegionHelper.getRegionAttributes(regDescriptName);
   String regionName = RegionHelper.getRegionDescription(regDescriptName).getRegionName();
   PoolDescription poolDescript = RegionHelper.getRegionDescription(regDescriptName).getPoolDescription();
   if (poolDescript != null) {
      String poolConfigName = RegionHelper.getRegionDescription(regDescriptName).getPoolDescription().getName();
      if (poolConfigName != null) {
         PoolHelper.createPool(poolConfigName);
      }
   }
   return CacheHelper.getCache().createRegion(regionName, attr);
}

/** This differs from ParRegUtil.createRegion() in that it works on the
 *  ClientCache and ClientRegionFactory created regions.
 */ 
public static Region createClientRegion(String cacheConfig, String regionConfig, String xmlFile) {
   Region createdRegion = null;
   String regionName = ClientRegionHelper.getClientRegionDescription(regionConfig).getRegionName();
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   if ((ds != null) && ds.isConnected()) {
      GemFireCache theCache = ClientCacheHelper.getCache();
      if (theCache == null) {
         // we are already connected to the distributed system, thus the declarative
         // file (if any) was already "set" when we connected; at this point, all we
         // can do is open the cache and if there was a declarative xml file set already
         // then the region will be created when the cache is opened, otherwise we must
         // create the region programmatically
         theCache = ClientCacheHelper.createCache(cacheConfig);
         createdRegion = theCache.getRegion(regionName);
         if (createdRegion == null) {
            createdRegion = ClientRegionHelper.createRegion(regionConfig);
         }
      } else { // we are connected and already have a cache, just create the region
         createdRegion = ClientRegionHelper.createRegion(regionConfig);
      }
   } else { // not connected; we have the opportunity to "set" any declarative, if desired
      if (TestConfig.tab().booleanAt(util.CachePrms.useDeclarativeXmlFile)) {
         // create the region using a declarative xml file; creating the cache creates the region
         // when an xml file is passed in, assuming the xmlFile specifies a region
         Log.getLogWriter().info("Creating " + regionName + " with " + xmlFile);
         GemFireCache theCache = ClientCacheHelper.createCacheFromXml(xmlFile);
         if (theCache == null)
            throw new TestException("Unexpected null cache");
         createdRegion = theCache.getRegion(regionName);
         if (createdRegion == null)
            throw new TestException("Unexpected null region");
         Log.getLogWriter().info("Finished creating " + TestHelper.regionToString(createdRegion, true) +
             " by using " + xmlFile);
      } else { // create the region programmatically
         GemFireCache theCache = ClientCacheHelper.createCache(cacheConfig);
         createdRegion = ClientRegionHelper.createRegion(regionConfig);
      }
   } 
   return createdRegion;
}

/** Return the current status of the given bucketID in the current jvm. 
 * 
 * @param aRegion The region to get bucket status on.
 * @param bucketId The bucket ID of interest.
 * 
 * @return null if no buckets exist anywere for the given bucketId
 *         OR Boolean[] where
 *           [0] true if bucket exists locally, false otherwise
 *           [1] true if the local bucket is primary, false if not
 *               primary or the bucket is not hosted locally (meaning that
 *               [0] is false
 */
public static Boolean[] getBucketStatus(Region aRegion, int bucketId) {
  List aList;
  try {
    aList = ((PartitionedRegion)aRegion).getBucketOwnersForValidation(bucketId);
  } catch (ForceReattemptException e) {
    throw new TestException(TestHelper.getStackTrace(e));
  }
  if (aList.size() == 0) { // there are no buckets for this bucket id
    return null;
  }
  DistributedMember thisMember = CacheHelper.getCache().getDistributedSystem().getDistributedMember();
  for (int j = 0; j < aList.size(); j++) {
    Object[] tmpArr = (Object[])(aList.get(j));
    DistributedMember member = (DistributedMember)tmpArr[0];
    Boolean isPrimary = (Boolean)(tmpArr[1]);
    if (member.equals(thisMember)) { // bucketId is local
      return new Boolean[] {member.equals(thisMember), isPrimary};
    }
  }
  return new Boolean[] {false, false};
}
}

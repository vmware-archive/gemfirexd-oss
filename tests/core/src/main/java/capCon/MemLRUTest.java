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
package capCon;

import hydra.GsRandom;
import hydra.Log;
import hydra.TestConfig;

import java.util.Set;

import util.CacheUtil;
import util.NameBB;
import util.NameFactory;
import util.RegionDefBB;
import util.RegionDefinition;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.TxHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.offheap.OffHeapMemoryStats;

public class MemLRUTest extends CapConTest {

protected static final int KEY_LENGTH = 20;
protected static int maximumMegabytes;
public MemLRUParameters memLRUParams;
private static String spaceKey = null;              // a key with all spaces

static {
   StringBuffer sb = new StringBuffer();
   for (int i = 0; i < KEY_LENGTH; i++) sb.append(" ");
   spaceKey = sb.toString();
}

// initialization methods
// ================================================================================
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new MemLRUTest();
      testInstance.initialize();
      RegionDefinition regDef = RegionDefinition.createRegionDefinition();
      maximumMegabytes = regDef.getEvictionLimit().intValue();
      ((MemLRUTest)testInstance).memLRUParams = new MemLRUParameters(
           KEY_LENGTH, maximumMegabytes, CacheUtil.getRegion(REGION_NAME));
      CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, ((MemLRUTest)testInstance).memLRUParams);
      String aStr = testInstance.toString();
      Log.getLogWriter().info(aStr);
   }
}

// implementation of abstract methods
// ================================================================================
public Object getNewKey() {
   String key = getFixedLengthKeyForName(NameFactory.getNextPositiveObjectName());
   return key;
}

public Object getNewValue() {
   boolean fillByteArray = TestConfig.tab().booleanAt(CapConPrms.fillByteArray);
   int byteArraySize = memLRUParams.getByteArraySize();
   if (byteArraySize == MemLRUParameters.UNKNOWN) // not a fixed size test
      byteArraySize = TestConfig.tab().intAt(CapConPrms.byteArraySize);
   byte[] byteArr = new byte[byteArraySize];
   if (fillByteArray)
      TestConfig.tab().getRandGen().nextBytes(byteArr);
   return byteArr;
}

public CacheListener getEventListener() {
   return new MemLRUListener();
}

public Object getPreviousKey() {
   long RANGE = 20;
   long currentNameCounter = NameFactory.getPositiveNameCounter();
   long lowEnd = currentNameCounter - RANGE;
   if (lowEnd < 0)
      lowEnd = 0;
   long previousNameCounter = TestConfig.tab().getRandGen().nextLong(lowEnd, currentNameCounter);
   return getFixedLengthKeyForName(NameFactory.getObjectNameForCounter(previousNameCounter)); 
}

// task methods
//========================================================================
public static void HydraTask_addEntries() {
   testInstance.addEntries();
//   ((MemLRUTest)testInstance).sync();
}

public void sync() {
   CapConBB.getBB().getSharedCounters().increment(CapConBB.SyncCounter);
   TestHelper.waitForCounter(CapConBB.getBB(), "CapConBB.SyncCounter", CapConBB.SyncCounter,
              TestHelper.getNumThreads() - TestHelper.getNumVMs(),
              true, 3600000);
   try {
      Log.getLogWriter().info("Reached sync point, sleeping for 30 seconds...");
      Thread.sleep(30000);
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().info("Checking capacity after sleeping");
   MemLRUTest aTest = (MemLRUTest)testInstance;
   long totalAllowableBytes = aTest.memLRUParams.getTotalAllowableBytes();
   aTest.verifyMemCapacity(totalAllowableBytes);
   CapConBB.getBB().getSharedCounters().zero(CapConBB.SyncCounter);
   Log.getLogWriter().info("Returning from sync");
}

public static void HydraTask_monitorCapacity() {
   MemLRUTest aTest = (MemLRUTest)testInstance;
   long totalAllowableBytes = aTest.memLRUParams.getTotalAllowableBytes();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_monitorCapacity, adding for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      aTest.verifyMemCapacity(totalAllowableBytes);
      checkForEventError();
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In HydraTask_monitorCapacity, done running for " + timeToRunSec + " seconds");
}

public static void HydraTask_serialTest() {
   ((MemLRUTest)testInstance).doSerialTest(); 
}

/** Return the size of the region for a transaction boundary (commit or rollback).
 *
 *  @param currentNumEntries The current number of entries in the region
 *
 *  @return The desired number of entries in the region to do a commit or rollback.
 */
private long getRegSizeForTxBoundary() {
   long currentRegSizeInBytes = getRegionSizeInBytes();
   GsRandom rand = TestConfig.tab().getRandGen();
   long minBytes = memLRUParams.getMinEvictionBytes();
   long maxBytes = memLRUParams.getTotalAllowableBytes();
   if (currentRegSizeInBytes < minBytes) {
      // commit periodically while growing the region to its threshold
      return rand.nextLong(currentRegSizeInBytes + 1, minBytes);
   } else {
      if (rand.nextBoolean()) { // commit while within capacity
         return rand.nextLong(minBytes, maxBytes);
      } else { // commit after growing above capacity
         return rand.nextLong(maxBytes + 1, maxBytes + MemLRUParameters.MEGABYTE);
      }
   }
}

protected Object[] addEntry() {
   Object[] tmpArr = super.addEntry();
//   Object key = tmpArr[0];
//   Object value = tmpArr[1];
//   verifyProductTestView(key, value);
   return tmpArr;
}

protected void doSerialTest() {
   Region workRegion = getWorkRegion();
   long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);

   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In doSerialTest, adding for " + timeToRunSec + " seconds");
   int count = 0;
   long startTime = System.currentTimeMillis();
   boolean inTrans = false;
   long txBoundary = -1;
   boolean done = false;
   long sizeWhenTxBegan = -1;
   do {
      long currentSize = getRegionSizeInBytes();
      int beforeNumKeys = workRegion.keys().size();
      if (useTransactions && !inTrans) { // begin a transaction
         TxHelper.begin();
         inTrans = true;
         txBoundary = getRegSizeForTxBoundary();
         Log.getLogWriter().info("Targeting a transaction boundary when region size is " + txBoundary +
                                 " bytes, current size is " + currentSize + " bytes");
         sizeWhenTxBegan = currentSize;
      }
      testInstance.addEntry(); 
      int afterNumKeys = workRegion.keys().size();
      currentSize = getRegionSizeInBytes();
      boolean timeToEndTrans = (currentSize >= txBoundary); 
      done = System.currentTimeMillis() - startTime > timeToRunMS;
      if (inTrans && (timeToEndTrans || done)) {
         Log.getLogWriter().info("Before ending transaction current region size is " + currentSize + 
             " bytes, size when transaction began: " + sizeWhenTxBegan + " bytes");
         if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 75) { // most of the time do a commit
            TxHelper.commitExpectSuccess();
         } else {
            TxHelper.rollback();
            long sizeAfterRollback = getRegionSizeInBytes();
            if (sizeAfterRollback != sizeWhenTxBegan)
               throw new TestException("When transaction began size was " + sizeWhenTxBegan + " bytes, " +
                         "before rollback size was " + currentSize + " bytes, after rollback size is " +
                         sizeAfterRollback + " bytes");
         }
         inTrans = false;
         currentSize = getRegionSizeInBytes();
         Log.getLogWriter().info("After ending transaction, region size is " + currentSize + " bytes");
      }

      if (inTrans) {
         if (beforeNumKeys + 1 != afterNumKeys) {
            throw new TestException("Before adding to " + TestHelper.regionToString(workRegion, false) + ", num keys was " +
               beforeNumKeys + ", but after adding " + " num keys was " + afterNumKeys + 
               "; expected num keys to be " + (beforeNumKeys + 1) + ", " + this.toString());
         }
      } else {
         verifyMemCapacity(memLRUParams.getTotalAllowableBytes());
      }
      checkForEventError();
      done = System.currentTimeMillis() - startTime > timeToRunMS;
   } while (!done);
   Log.getLogWriter().info("In doSerialTest, done running for " + timeToRunSec + " seconds, added " +
       count + " objects to " + TestHelper.regionToString(workRegion, false) + 
       "; " + TestHelper.regionToString(workRegion, false)  + " is size " + getRegionSizeInBytes() + " bytes");
}

// end task methods
//========================================================================
public static void HydraTask_endTask() {
   Cache myCache = CacheUtil.createCache(); 
   NameBB.getBB().printSharedCounters();
   CapConBB.getBB().printSharedMap();
   CapConBB.getBB().printSharedCounters();
   RegionDefBB.printBB();
   checkForEventError();
   MemLRUTest aTest = new MemLRUTest();
   aTest.memLRUParams = (MemLRUParameters)CapConBB.getBB().getSharedMap().get(CapConBB.TEST_SETTINGS);
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();
   Region workRegion = regDef.createRootRegion(myCache, REGION_NAME, null, null, null);
   aTest.verifyMemCapacity(aTest.memLRUParams.getTotalAllowableBytes()); 
   {
     int numEntries = TestHelper.getTotalNumKeys(workRegion);
     if (numEntries != 0)
       throw new TestException("With MIRROR_NONE, expected 0 entries in region but got " + numEntries);
   }
   double numEvictions = TestHelper.getNumLRUEvictions();
   Log.getLogWriter().info("Number of lru evictions during test: " + numEvictions);
}

//private static void checkRegionEntries(Region aRegion) {
//   int endTaskSecToRun = TestConfig.tab().intAt(CapConPrms.endTaskSecToRun);
//   iterateRegionContents(aRegion, endTaskSecToRun);
//}
       
// private methods
//========================================================================

/** Get the current number of bytes in the region.
 *
 */
protected long getRegionSizeInBytes() {
   Region workRegion = getWorkRegion();
   int numKeys = workRegion.keys().size();
   CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_NUM_KEYS, numKeys);
   if (workRegion.getAttributes().getEnableOffHeapMemory()) {
     GemFireCacheImpl gfCache = (GemFireCacheImpl)(CacheUtil.getCache());
     return gfCache.getOffHeapStore().getStats().getUsedMemory();
   } else {
     return memLRUParams.getTotalBytesPerEntry() * numKeys;
   }
}

/** Verify the number of bytes used for this region. In this test keys are
 *  all strings of size KEY_LENGTH and values are all byte arrays of size
 *  byteArraySize.
 */
public void verifyMemCapacity(long byteLimit) {
  Region workRegion = getWorkRegion();
  if (workRegion.getAttributes().getEnableOffHeapMemory()) {
    Cache theCache = CacheUtil.getCache();
    if (!(theCache instanceof GemFireCacheImpl)) {
      throw new TestException("Cache cannot be cast to GemFireCacheImpl");
    }
    GemFireCacheImpl gfCache = (GemFireCacheImpl)theCache;
    OffHeapMemoryStats offHeapStats = gfCache.getOffHeapStore().getStats();
    long usedOffHeapMemory = offHeapStats.getUsedMemory();
    Log.getLogWriter().info("Off-heap memory, used bytes: " + usedOffHeapMemory + ", byteLimit: " + byteLimit);
    if (usedOffHeapMemory > byteLimit) {
      String aStr = "Expected current used off-heap memory (bytes) " + usedOffHeapMemory +
          " to be <= byteLimit " + byteLimit;
      Log.getLogWriter().info(aStr);
      throw new TestException(aStr);
    }
  } else {
    Set keys = workRegion.keys();
    int numKeys = keys.size();
    CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_NUM_KEYS, numKeys);
    long currentRegSizeInBytes = memLRUParams.getTotalBytesPerEntry() * numKeys;
    CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_REGION_SIZE_IN_BYTES, currentRegSizeInBytes);
    String logStr = "In verifyMemCapacity,\n" +
        memLRUParams.toString() + "\n" +
        "Current conditions\n" +
        "   num keys                    : " + numKeys + "\n" +
        "   checking against byte limit : " + byteLimit + "\n" +
        "   bytes in region (test view) : " + currentRegSizeInBytes;
    Log.getLogWriter().info(logStr);
    if (currentRegSizeInBytes > byteLimit) {
      String aStr = "Expected currentRegSizeInBytes " + currentRegSizeInBytes +
          " to be <= byteLimit " + byteLimit + "\n" + 
          "MAX_REGION_SIZE_IN_BYTES: " + 
          CapConBB.getBB().getSharedCounters().read(CapConBB.MAX_REGION_SIZE_IN_BYTES) + " " + 
          "MAX_NUM_KEYS: " + 
          CapConBB.getBB().getSharedCounters().read(CapConBB.MAX_NUM_KEYS) + "\n" + logStr;
      Log.getLogWriter().info(aStr);
      throw new TestException(aStr);
    }
  }
}

/** This is called when eviction occurs, from a destroy event. Verify that eviction is not occurring too early. */
protected void verifyEviction() {
   Region workRegion = getWorkRegion();
   int numKeys = workRegion.keys().size();
   CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_NUM_KEYS, numKeys);
   long currentRegSizeInBytes = getRegionSizeInBytes();
   CapConBB.getBB().getSharedCounters().setIfSmaller(CapConBB.MIN_REGION_BYTES_AT_MEMLRU_EVICTION, currentRegSizeInBytes);
   long minEvictionBytes = memLRUParams.getMinEvictionBytes();
   String logStr = "In verifyEviction,\n" +
      memLRUParams.toString() + "\n" +
      "Current conditions\n" +
      "   num keys                   : " + numKeys + "\n" +
      "   bytes in region (test view): " + currentRegSizeInBytes;
   Log.getLogWriter().info(logStr);
   if (currentRegSizeInBytes < minEvictionBytes) {
      String aStr = "Early eviction detected: Expected currentRegSizeInBytes " + currentRegSizeInBytes +
         " to be >= minEvictionBytes " + minEvictionBytes + "\n" + 
         "MAX_REGION_SIZE_IN_BYTES: " + 
         CapConBB.getBB().getSharedCounters().read(CapConBB.MAX_REGION_SIZE_IN_BYTES) + " " + 
         "MAX_NUM_KEYS: " + 
         CapConBB.getBB().getSharedCounters().read(CapConBB.MAX_NUM_KEYS) + "\n" + logStr;
      Log.getLogWriter().info(aStr);
      CapConBB.getBB().getSharedMap().put(TestHelper.EVENT_ERROR_KEY, aStr);
   }
}

private static String getFixedLengthKeyForName(String name) {
   StringBuffer sb = new StringBuffer(spaceKey);
   sb.replace(0, name.length(), name);
   if (sb.length() != KEY_LENGTH)
      throw new TestException("Expected key to be length " + KEY_LENGTH + ", but it is " + 
            TestHelper.toString(sb));
   return sb.toString();
}

///** Iterate as many elements of the region as possible in the given secToIterate seconds.
// *  Iterate in reverse order of additions, so we have a bigger chance of getting objects
// *  that have not been evicted.
// *
// *  @param aRegion Iterate the contents of this region.
// *  @param secToIterate The number of seconds to iterate from this region.
// */
//private static void iterateRegionContents(Region aRegion, long secToIterate) {
//   long millisToIterate = secToIterate * TestHelper.SEC_MILLI_FACTOR;
//   int numKeys = aRegion.keys().size();
//   Log.getLogWriter().info("Region " + TestHelper.regionToString(aRegion, false) + " has num keys " + numKeys);
//   long lastNameCounter = NameFactory.getPositiveNameCounter();
//   Object lastName = getFixedLengthKeyForName(NameFactory.getObjectNameForCounter(lastNameCounter));
////   long timeoutMS = 120000;
//   long nullValueCounter = 0;
//   long byteArrayValueCounter = 0;
//   long otherValueCounter = 0;
//   long containsKeyCounter = 0;
//   long containsValueForKeyCounter = 0;
//   StringBuffer errStr = new StringBuffer();
//   Log.getLogWriter().info("Iterating objects in " + TestHelper.regionToString(aRegion, false) + 
//       " for " + secToIterate + "seconds starting with " + lastName);
//   long count = 0;
//   long startMillis = System.currentTimeMillis();
//   for (long i = lastNameCounter; i <= 1; i--) { 
//      count++;
//      String key = getFixedLengthKeyForName(NameFactory.getObjectNameForCounter(i));
//      if (aRegion.containsKey(key))
//         containsKeyCounter++;
//      if (aRegion.containsValueForKey(key))
//         containsValueForKeyCounter++;
//      try {
//         Object value = aRegion.get(key);
//         if (value == null)
//            nullValueCounter++;
//         else if (value.getClass() == byte[].class)
//            byteArrayValueCounter++;
//         else { 
//            otherValueCounter++;
//            errStr.append("Unexpected value " + TestHelper.toString(value) + "\n");
//         }
//      } catch (CacheLoaderException e) {
//         throw new TestException(TestHelper.getStackTrace(e));
//      } catch (TimeoutException e) {
//         throw new TestException(TestHelper.getStackTrace(e));
//      }
//      if (System.currentTimeMillis() - startMillis > millisToIterate)
//         break;
//   }
//   StringBuffer logStr = new StringBuffer();
//   logStr.append("Iterated " + count + " objects starting with " + lastName);
//   logStr.append("containsKeyCounter: " + containsKeyCounter + "\n");
//   logStr.append("containsValueForKeyCounter: " + containsValueForKeyCounter + "\n");
//   logStr.append("nullValueCounter: " + nullValueCounter + "\n");
//   logStr.append("otherValueCounter: " + otherValueCounter + "\n");
//   Log.getLogWriter().info(logStr.toString());
//   if (errStr.length() > 0)
//      throw new TestException(errStr.toString());
//}

// String methods
//========================================================================
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(super.toString() + "\n");
   aStr.append(memLRUParams.toString() + "=\n");
   return aStr.toString();
}

}

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
package diskReg;

import util.*;
import hydra.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.lru.MemLRUCapacityController;
import com.gemstone.gemfire.internal.cache.LocalRegion;

public class SerialDiskRegTest extends DiskRegTest {

protected String sharedMapKey;  // key used for this VM to access the blackboard shared map
protected boolean checkLRU;     // true if this test can check LRU overflow objects
protected boolean isMemLRU;     // true if this VM uses memLRU for disk regions

// for memLRU disk regions
protected long bytesPerEntry_keyValue;   // number of bytes in the region per entry in the VM
protected long bytesPerEntry_keyOnly;    // number of bytes in the region per entry with value on disk
protected long minEvictionBytes;     // the minimum number of bytes that must be in the region before eviction occurs 
protected long maxBytes;             // the maximum number of allowable bytes in the region
protected long perEntryOverhead;     // the number of bytes of overhead for a region entry
protected long numEntriesDeviation;  // the number of bytes of this number of entries that the region's
                                     // number of bytes can deviate from the set capacity for memLRU


// initialization methods
// ================================================================================
/** initialize fields in this class and its superclass */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new SerialDiskRegTest();
      testInstance.initialize();
      Log.getLogWriter().info(testInstance.toVerboseString());
   }
}

protected void initialize() {
   super.initialize();
   // unable to verify LRU when numVMs > 1 because 1) we don't know which keys
   // are in this VM's region and 2) randomly getting objects from other caches
   // can pull in any key, even those orginially defined in another VM
   checkLRU = (numVMs == 1); 
   sharedMapKey = "OldestKey_" + ProcessMgr.getProcessId();
   DiskRegBB.getBB().getSharedMap().put(sharedMapKey, new Long(1));
   isMemLRU = false;

   EvictionAttributes evAttr = aRegion.getAttributes().getEvictionAttributes();
   if (evAttr != null) {
     isMemLRU = evAttr.getAlgorithm().isLRUMemory();
     if (isMemLRU) {
        Object sampleValue = getNewValue(getFixedLengthKeyForName("Object_1"));
        long valueSize = (new DiskRegSizer()).sizeof(sampleValue);
   
        Region aRegion = RegionHelper.getRegion(REGION_NAME);
        perEntryOverhead = ((MemLRUCapacityController)(((LocalRegion)aRegion).getEvictionController())).getPerEntryOverhead();
        bytesPerEntry_keyOnly = TestHelper.calculateStringSize(getFixedLengthKeyForName("Object_1")) + perEntryOverhead;
        bytesPerEntry_keyValue = bytesPerEntry_keyOnly + valueSize + perEntryOverhead;
        numEntriesDeviation = TestConfig.tab().intAt(DiskRegPrms.numEntriesDeviation);
        minEvictionBytes = (evictionLimit * 1024 * 1024) - (bytesPerEntry_keyValue * numEntriesDeviation);
        maxBytes = (evictionLimit * 1024 * 1024) + (bytesPerEntry_keyValue * numEntriesDeviation);
        Log.getLogWriter().info(
            "KEY_LENGTH = " + KEY_LENGTH + "\n" +
            "valueSize = " + valueSize + "\n" +
            "perEntryOverhead = " + perEntryOverhead + "\n" +
            "bytesPerEntry_keyValue = " + bytesPerEntry_keyValue + "\n" +
            "bytesPerEntry_keyOnly = " + bytesPerEntry_keyOnly + "\n" +
            "numEntriesDeviation = " + numEntriesDeviation + "\n" +
            "minEvictionBytes = " + minEvictionBytes + "\n" +
            "maxBytes = " + maxBytes);
      }
   }
}

// task methods
//========================================================================

/** Hydra task for careful validation of disk regions. Add an entry and
 *  validate it. Run for minTaskGranularitySec (specified as a hydra param)
 */
public static void HydraTask_addNew() {
   ((SerialDiskRegTest)testInstance).addNew();
}

private void addNew() {
   long exeNum = DiskRegBB.getBB().getSharedCounters().incrementAndRead(DiskRegBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_addNew, adding for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      ((SerialDiskRegTest)testInstance).doAddNew();
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In HydraTask_addNew, done running for " + timeToRunSec + " seconds");
   int numKeys = testInstance.aRegion.keys().size();
   int endTestOnNumKeysInRegion = TestConfig.tab().intAt(DiskRegPrms.endTestOnNumKeysInRegion, Integer.MAX_VALUE);
      if (numKeys >= endTestOnNumKeysInRegion) {
         // some tests keep all keys in memory; on faster machines these tests run out of
         // memory in a time based test, so make it a workload based test instead
         throw new StopSchedulingOrder("Stopping client because the region has " + numKeys + " keys, endTestOnNumKeysInRegion is " + endTestOnNumKeysInRegion);
      }
}

/** Hydra task for careful validation of disk regions. Do a get on a 
 *  previously created entry to see if we can get it from another cache.
 */
public static void HydraTask_getOld() {
   ((SerialDiskRegTest)testInstance).getOld();
}

private void getOld() {
   long exeNum = DiskRegBB.getBB().getSharedCounters().incrementAndRead(DiskRegBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_getOld, getting for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      ((SerialDiskRegTest)testInstance).doGetOld();
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In HydraTask_getOld, done running for " + timeToRunSec + " seconds");
   int numKeys = testInstance.aRegion.keys().size();
   int endTestOnNumKeysInRegion = TestConfig.tab().intAt(DiskRegPrms.endTestOnNumKeysInRegion, Integer.MAX_VALUE);
   if ((regionKind == NO_DISK) || (regionKind == DISK_FOR_PERSIST)) { //configs that keep things in memory
      if (numKeys >= endTestOnNumKeysInRegion) {
         // some tests keep all keys in memory; on faster machines these tests run out of
         // memory in a time based test, so make it a workload based test instead
         throw new StopSchedulingOrder("Stopping client because the region has " + numKeys + " keys, endTestOnNumKeysInRegion is " + endTestOnNumKeysInRegion);
      }
   }
}

// methods to do the work of the tasks
//========================================================================

/** Add a new entry to the region, and verify the add.
 */
protected void doAddNew() {
   // add the entry, getting the number of entries before and after on the disk
   int beforeNumKeys = aRegion.keys().size();
   long beforeNumInVM = DiskRegUtil.getNumEntriesInVM(aRegion);
   long beforeNumOnDisk = DiskRegUtil.getNumEntriesWrittenToDisk(aRegion);
   long beforeNumOverflowOnDisk = DiskRegUtil.getNumOverflowOnDisk(aRegion);
   
   Object[] keyAndValue;
   DataPolicy dataPolicy = aRegion.getAttributes().getDataPolicy();
   boolean putIfAbsentSupported = !(dataPolicy.equals(DataPolicy.NORMAL) || dataPolicy.equals(DataPolicy.EMPTY));
   if (putIfAbsentSupported && TestConfig.tab().getRandGen().nextBoolean()) {
     keyAndValue = putIfAbsent();
   } else {
     keyAndValue = addEntry();
   }
   Object key = keyAndValue[0];
   Object value = keyAndValue[1];
   boolean addViaGet = ((Boolean)keyAndValue[2]).booleanValue();
   verifyAfterAdd(key, value, beforeNumKeys, beforeNumInVM, beforeNumOnDisk, beforeNumOverflowOnDisk);

   if (addViaGet) { // doing a second get so as to show off bug 31102 (or lack thereof)
      Log.getLogWriter().info("Doing a second get on key " + key + "...");
      aRegion.get(key);  
   }
   verifyAfterAdd(key, value, beforeNumKeys, beforeNumInVM, beforeNumOnDisk, beforeNumOverflowOnDisk);
   checkForEventError();
}

/** Do a get of a previously added key to see if we can find it in another cache.
 */
protected void doGetOld() {
   final int RANGE = 30; // look back to the last 30 keys created by this test
   int beforeNumKeys = aRegion.keys().size();
   long beforeNumInVM = DiskRegUtil.getNumEntriesInVM(aRegion);
   long beforeNumOnDisk = DiskRegUtil.getNumEntriesWrittenToDisk(aRegion);
   long beforeNumOverflowOnDisk = DiskRegUtil.getNumOverflowOnDisk(aRegion);
   Object[] objArr = randomGet(RANGE);
   if (objArr == null) // there are no keys yet
      return;
   Object key = objArr[0];
   Object value = objArr[1];
   boolean containsKeyBeforeGet = ((Boolean)objArr[2]).booleanValue();
   if (!containsKeyBeforeGet) { // we added a new entry; verify it
      verifyAfterAdd(key, value, beforeNumKeys, beforeNumInVM, beforeNumOnDisk, beforeNumOverflowOnDisk);
   }
   checkForEventError();
}

// verification methods
//========================================================================

/** Verify the state of the region after an add.
 *
 *  @params key The key that was added. 
 *  @params value The value that was added.
 *  @params beforeNumKeys The number of keys before the add.
 *  @params beforeNumInVM The number of entries in the VM before the add.
 *  @params beforeNumOnDisk The number of entries on the disk before the add.
 *  @params beforeNumOverflowOnDisk The number of entries overflowed to disk before the add.
 *
 *  @throws TestException if any problems were detected.
 */
protected void verifyAfterAdd(Object key, 
                              Object value, 
                              long beforeNumKeys, 
                              long beforeNumInVM, 
                              long beforeNumOnDisk,
                              long beforeNumOverflowOnDisk) {
   int afterNumKeys = aRegion.size();
   long afterNumInVM = DiskRegUtil.getNumEntriesInVM(aRegion);
   long afterNumOnDisk = DiskRegUtil.getNumEntriesWrittenToDisk(aRegion);
   long afterNumOverflowOnDisk = DiskRegUtil.getNumOverflowOnDisk(aRegion);
   Object valueInVM = DiskRegUtil.getValueInVM(aRegion, key);
  Object valueOnDisk = DiskRegUtil.getValueOnDiskOrBuffer(aRegion, key);

   StringBuffer aStr = new StringBuffer();
   aStr.append("In verifyAfterAdd with key " + key + ", value " + TestHelper.toString(value) + "\n");
   aStr.append("   beforeNumKeys: " + beforeNumKeys + "\n");
   aStr.append("   beforeNumInVM: " + beforeNumInVM + "\n");
   aStr.append("   beforeNumOnDisk: " + beforeNumOnDisk + "\n");
   aStr.append("   beforeNumOverflowOnDisk: " + beforeNumOverflowOnDisk + "\n");
   aStr.append("   afterNumKeys: " + afterNumKeys + "\n");
   aStr.append("   afterNumInVM: " + afterNumInVM + "\n");
   aStr.append("   afterNumOnDisk: " + afterNumOnDisk + "\n");
   aStr.append("   afterNumOverflowOnDisk: " + afterNumOverflowOnDisk + "\n");
   aStr.append("   valueInVM: " + valueInVM + "\n");
   aStr.append("   valueOnDisk: " + valueOnDisk + "\n");
   Log.getLogWriter().info(aStr.toString());
//   printKeyStatus();

   // check expected number of keys; should increment by 1
   checkValue(beforeNumKeys+1, afterNumKeys, "after num keys");

   // check containsKey, containsValueForKey; these should always be true whether on disk or not
   boolean containsKey = aRegion.containsKey(key);
   boolean containsValueForKey = aRegion.containsValueForKey(key);
   checkValue(true, containsKey, "containsKey()");
   checkValue(true, containsValueForKey, "containsValueForKey()");

   switch (regionKind) {
      case NO_DISK: {
         // numInVM should increment
            checkValue(beforeNumKeys+1, afterNumInVM, "after numInVM"); 
         // zero should be on disk
            checkValue(0, afterNumOnDisk, "afterNumOnDisk"); 
            checkValue(0, afterNumOverflowOnDisk, "afterNumOverflowOnDisk"); 
         // value should be in VM
            checkValue(value, valueInVM, "value in VM"); 
         // value should not be on disk
            checkValue(null, valueOnDisk, "value on disk"); 
      } break;

      case DISK_FOR_OVRFLW: {
         if (isMemLRU) { 
            // The number of bytes in the VM is found by:
            //     (num entries in VM that contain key AND value) * (num bytes for a key AND value)
            //     plus
            //     (num entries in VM that contain key only) * (num bytes for a key only)
            // For an overflow configuration, the number of entries in the VM that contain 
            // the key only = number of entries overflowed to disk.
            long beforeBytesInVM = (beforeNumInVM * bytesPerEntry_keyValue) + 
                                   (beforeNumOnDisk * bytesPerEntry_keyOnly);
            long afterBytesInVM = (afterNumInVM * bytesPerEntry_keyValue) +
                                  (afterNumOverflowOnDisk * bytesPerEntry_keyOnly);
            boolean evicted = (afterNumInVM <= beforeNumInVM);
            Log.getLogWriter().info("   beforeBytesInVM: " + beforeBytesInVM);
            Log.getLogWriter().info("   afterBytesInVM: " + afterBytesInVM);
            Log.getLogWriter().info("   evicted: " + evicted);
            if (evicted) { // we evicted
               if (afterBytesInVM < minEvictionBytes) { // we evicted too early
                  throw new TestException("Early eviction: afterBytesInVM is " + afterBytesInVM +
                        ", minEvictionBytes is " + minEvictionBytes);
               }
            } 
            // whether we evicted or not, make sure num bytes didn't go too high
            // if we fill up the VM (all entries are on disk), allow the entry to 
            // be added (successfully).  All entries will be written to disk (until
            // we run out of heap.
            if (afterBytesInVM > maxBytes) { // too high
               if (afterNumInVM == 0) {      // ok if all entries are being written to disk
                  Log.getLogWriter().info("Expected afterBytesInVM(" + afterBytesInVM + ") > maxBytes (" + maxBytes + ") but afterNumInVM == 0 so all entries are being written to disk; continuing test");
               } else {
                  throw new TestException("Expected afterBytesInVM " + afterBytesInVM +
                         " to be <= maxBytes " + maxBytes);
               }
            }

            if (afterNumInVM > 0) {
               // Any eviction done was done to an older entry, not the one we just put;
               // the object we just put should be in the VM.
               checkValue(value, valueInVM, "value in VM"); 

               // value should not be on disk whether we overflowed or not
               checkValue(null, valueOnDisk, "value on disk"); 
            } else {
               // All entries are on disk, even the one we just put because the region
               // is using all its space for the keys
               checkValue(null, valueInVM, "value in VM"); 

               // value should not be on disk whether we overflowed or not
               checkValue(value, valueOnDisk, "value on disk"); 
            }
         } else { // is LRU
            boolean  overflow = (beforeNumKeys >= evictionLimit);
            if (overflow) { // this add should have caused overflow
               // numInVM should remain unchanged
                  checkValue(beforeNumInVM, afterNumInVM, "numInVM"); 
               // numOverflowOnDisk should increment
                  checkValue(beforeNumOverflowOnDisk+1, afterNumOverflowOnDisk, "numOverflowOnDisk");
               // check that the LRU entry was evicted
                  checkOverflowEviction();
            } else { // this add should NOT have caused overflow
               // numInVM should increment
                  checkValue(beforeNumKeys+1, afterNumInVM, "after numInVM"); 
               // numOnDisk should remain unchanged
                  checkValue(beforeNumOnDisk, afterNumOnDisk, "afterNumOnDisk"); 
               // numOverflowOnDisk should remain unchanged
                  checkValue(beforeNumOverflowOnDisk, afterNumOverflowOnDisk, "afterNumOnDisk"); 
            }
            // Any eviction done was done to an older entry, not the one we just put;
            // the object we just put should be in the VM.
            checkValue(value, valueInVM, "value in VM"); 

            // value should not be on disk whether we overflowed or not
            checkValue(null, valueOnDisk, "value on disk"); 
         }

      } break;

      case DISK_FOR_PERSIST: {
         // numInVM should increment
            checkValue(beforeNumInVM+1, afterNumInVM, "numInVM"); 
         // numOnDisk should increment
            checkValue(beforeNumOnDisk+1, afterNumOnDisk, "numOnDisk"); 
         // value should be in vm 
            checkValue(value, valueInVM, "value in VM"); 
         // value should be on disk 
            checkValue(value, valueOnDisk, "value on disk"); 
      } break;

      case DISK_FOR_OVRFLW_PERSIST: {
         if (isMemLRU) { 
            // The number of bytes in the VM is found by:
            //     (num entries in VM that contain key AND value) * (num bytes for a key AND value)
            //     plus
            //     (num entries in VM that contain key only) * (num bytes for a key only)
            // For an overflow with persistence configuration, the number of entries in the VM that 
            // contain the key only = 
            //    (total number of entries) - (number of entries with key AND value in the VM)
            // because all entries have a value on disk, but not all entries in the VM have a value in the VM
            long beforeNumKeysOnlyInVM = beforeNumKeys - beforeNumInVM;
            long beforeBytesInVM = (beforeNumInVM * bytesPerEntry_keyValue) + 
                                   (beforeNumKeysOnlyInVM * bytesPerEntry_keyOnly);
            long afterNumKeysOnlyInVM = afterNumKeys - afterNumInVM;
            long afterBytesInVM = (afterNumInVM * bytesPerEntry_keyValue) +
                                  (afterNumKeysOnlyInVM * bytesPerEntry_keyOnly);
            boolean evicted = (afterNumInVM <= beforeNumInVM);
            Log.getLogWriter().info("   beforeNumKeysOnlyInVM: " + beforeNumKeysOnlyInVM);
            Log.getLogWriter().info("   beforeBytesInVM: " + beforeBytesInVM);
            Log.getLogWriter().info("   afterNumKeysOnlyInVM: " + afterNumKeysOnlyInVM);
            Log.getLogWriter().info("   afterBytesInVM: " + afterBytesInVM);
            Log.getLogWriter().info("   evicted: " + evicted);
            if (evicted) { // we evicted
               if (afterBytesInVM < minEvictionBytes) { // we evicted too early
                  throw new TestException("Early eviction: afterBytesInVM is " + afterBytesInVM +
                        ", minEvictionBytes is " + minEvictionBytes);
               }
            } 
            // whether we evicted or not, make sure num bytes didn't go too high
            // if we fill up the VM (all entries are on disk), allow the entry to 
            // be added (successfully).  All entries will be written to disk (until
            // we run out of heap.
            if (afterBytesInVM > maxBytes) { // too high
               if (afterNumInVM == 0) {      // ok if all entries are being written to disk
                  Log.getLogWriter().info("Expected afterBytesInVM(" + afterBytesInVM + ") > maxBytes (" + maxBytes + ") but afterNumInVM == 0 so all entries are being written to disk; continuing test");
               } else {
                  throw new TestException("Expected afterBytesInVM " + afterBytesInVM + " to be <= maxBytes " + maxBytes);
               }
            }

            if (afterNumInVM > 0) {
               // value should be in vm whether we overflowed or not
               checkValue(value, valueInVM, "value in VM"); 

               // value should be on disk whether we overflowed or not
               checkValue(value, valueOnDisk, "value on disk"); 
             } else {
               // All entries are on disk, even the one we just put because the region
               // is using all its space for the keys
               checkValue(null, valueInVM, "value in VM"); 

               // value should not be on disk whether we overflowed or not
               checkValue(value, valueOnDisk, "value on disk"); 
             }
         } else { // is LRU
            boolean overflow = (beforeNumKeys >= evictionLimit);
            if (overflow) { // this add should have caused overflow
               // numInVM should remain unchanged
                  checkValue(beforeNumInVM, afterNumInVM, "numInVM"); 
               // numOnDisk should increment
                  checkValue(beforeNumOnDisk+1, afterNumOnDisk, "numOnDisk"); 
               // check that the LRU entry was evicted
                  checkOverflowEviction();
            } else { // this add should NOT have caused overflow
               // numInVM should increment
                  checkValue(beforeNumKeys+1, afterNumInVM, "after numInVM"); 
               // numOnDisk should increment
                  checkValue(beforeNumOnDisk+1, afterNumOnDisk, "afterNumOnDisk"); 
            }
            // value should be in vm whether we overflowed or not
            checkValue(value, valueInVM, "value in VM"); 

            // value should be on disk whether we overflowed or not
            checkValue(value, valueOnDisk, "value on disk"); 
         }

      } break;

      default: {
         throw new TestException("Unknown region kind " + regionKind);
      }
   }
}

// other methods
//========================================================================

/** Verify the long has the expected value.
 *
 *  @params expectedValue The expected value of the long.
 *  @params actualValue The actual value of the long.
 *  @params valueDescript A String used for logging an error.
 *
 *  @throws TestException if the expected is not equal to the actual.
 */
protected void checkValue(long expectedValue, long actualValue, String valueDescript) {

   if (expectedValue != actualValue) {
      printKeyStatus();
      throw new TestException("Expected " + valueDescript + " to be " + expectedValue +
                ", but it is " + actualValue);
   }
}

/** Verify the int has the expected value.
 *
 *  @params expectedValue The expected value of the int.
 *  @params actualValue The actual value of the int.
 *  @params valueDescript A String used for logging an error.
 *
 *  @throws TestException if the expected is not equal to the actual.
 */
protected void checkValue(int expectedValue, int actualValue, String valueDescript) {

   if (expectedValue != actualValue) {
      printKeyStatus();
      throw new TestException("Expected " + valueDescript + " to be " + expectedValue +
                ", but it is " + actualValue);
   }
}

/** Verify the boolean has the expected value.
 *
 *  @params expectedValue The expected value of the boolean.
 *  @params actualValue The actual value of the boolean.
 *  @params valueDescript A String used for logging an error.
 *
 *  @throws TestException if the expected is not equal to the actual.
 */
protected void checkValue(boolean expectedValue, boolean actualValue, String valueDescript) {

   if (expectedValue != actualValue) {
      throw new TestException("Expected " + valueDescript + " to be " + expectedValue +
                ", but it is " + actualValue);
   }
}

/** Verify the Object has the expected value.
 *
 *  @params expectedObj The expected value of the Object.
 *  @params actualObj The actual value of the Object.
 *  @params descript A String used for logging an error.
 *
 *  @throws TestException if the expected is not equal to the actual.
 */
protected void checkValue(Object expectedObj, Object actualObj, String descript) {
  if (expectedObj == null) {
    if (actualObj == null) {
      return;
    }
    else {
      throw new TestException("Expected " + descript + " to be "
          + TestHelper.toString(expectedObj) + ", but it is "
          + TestHelper.toString(actualObj));
    }
  }
  if (expectedObj instanceof byte[]) {
    if (actualObj instanceof byte[]) {
      if (Arrays.equals((byte[])expectedObj, (byte[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }

  if (expectedObj instanceof boolean[]) {
    if (actualObj instanceof boolean[]) {
      if (Arrays.equals((boolean[])expectedObj, (boolean[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }

  if (expectedObj instanceof long[]) {
    if (actualObj instanceof long[]) {
      if (Arrays.equals((long[])expectedObj, (long[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }

  if (expectedObj instanceof int[]) {
    if (actualObj instanceof int[]) {
      if (Arrays.equals((int[])expectedObj, (int[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }

  if (expectedObj instanceof short[]) {
    if (actualObj instanceof short[]) {
      if (Arrays.equals((short[])expectedObj, (short[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }

  if (expectedObj instanceof double[]) {
    if (actualObj instanceof double[]) {
      if (Arrays.equals((double[])expectedObj, (double[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }

  if (expectedObj instanceof float[]) {
    if (actualObj instanceof float[]) {
      if (Arrays.equals((float[])expectedObj, (float[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }

  if (expectedObj instanceof char[]) {
    if (actualObj instanceof char[]) {
      if (Arrays.equals((char[])expectedObj, (char[])actualObj)) {
        return;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }
  
  if (expectedObj instanceof StringBuffer) {
    if (actualObj instanceof StringBuffer) {
     if( ( ( (StringBuffer)expectedObj).toString() ).equals( ( (StringBuffer)actualObj ).toString() ) ) { 
        return ;
      }
      else {
        throw new TestException("Expected " + descript + " to be "
            + TestHelper.toString(expectedObj) + ", but it is "
            + TestHelper.toString(actualObj));
      }
    }
  }
  

  if (!expectedObj.equals(actualObj)) {
    throw new TestException("Expected " + descript + " to be "
        + TestHelper.toString(expectedObj) + ", but it is "
        + TestHelper.toString(actualObj));
  }
  
  
}                                                                                             
                                                                                                

  
  


/** Check that the least recently used entry was evicted as overflow. Also, 
 *  check that the next few least recently used entries were not evicted.
 */
protected void checkOverflowEviction() {
   if (!checkLRU)
      return;
   final int NEXT_LRU_TO_CHECK = 100;

   // Get the oldest key, the one we expected to have been evicted on overflow
   long expEvictedKeyCounter = ((Long)(DiskRegBB.getBB().getSharedMap().get(sharedMapKey))).longValue();
   String expEvictedKey = NameFactory.getObjectNameForCounter(expEvictedKeyCounter);
   Object valueInVM = DiskRegUtil.getValueInVM(aRegion, expEvictedKey); 
   Object valueOnDisk =  DiskRegUtil.getValueOnDiskOrBuffer(aRegion, expEvictedKey);
   checkValue(null, valueInVM, "LRU overflowed value");
   if (valueOnDisk != null) {
      printKeyStatus();
      throw new TestException("Expected LRU overflowed value on disk to be null but it is " +
                TestHelper.toString(valueOnDisk));
   }

   // check that the next least recently used entries were not evicted
   long newestKeyCounter = NameFactory.getPositiveNameCounter();
   long numToCheck = Math.min(expEvictedKeyCounter + NEXT_LRU_TO_CHECK, newestKeyCounter); 
   for (long i = expEvictedKeyCounter+1; i <= numToCheck; i++) {
      String key = NameFactory.getObjectNameForCounter(i);
      valueInVM = DiskRegUtil.getValueInVM(aRegion, key);
      checkValue(null, valueInVM, "value in VM (key: " + key + ")");
   }

   // change the least recently used key
   DiskRegBB.getBB().getSharedMap().put(sharedMapKey, new Long(expEvictedKeyCounter+1));
}

/** Prints the status of which keys are in the VM and which are on disk.
 *
 */
private void printKeyStatus() {
   long lastNameCounter = NameFactory.getPositiveNameCounter();
   BitSet onDisk = new BitSet((int)lastNameCounter+1);  // index 0 is ignored
   BitSet inVM = new BitSet((int)lastNameCounter+1);    // index 0 is ignored
   for (int index = 1; index <= lastNameCounter; index++) {
       Object key = getFixedLengthKeyForName(NameFactory.getObjectNameForCounter(index));
       Object valueInVM = DiskRegUtil.getValueInVM(aRegion, key);  
       Object valueOnDisk =  DiskRegUtil.getValueOnDiskOrBuffer(aRegion, key);
       if (valueInVM != null)
          inVM.set(index);
       if (valueOnDisk != null)
          onDisk.set(index);
   }
   Log.getLogWriter().info("Entries in VM: " + getBitSetRanges(inVM, lastNameCounter) + "\n" +
                           "Entries on disk: " + getBitSetRanges(onDisk, lastNameCounter));
       
}

private String getBitSetRanges(BitSet aBitSet, long maxBits) {
   StringBuffer aStr = new StringBuffer();
   boolean firstTime = true;
   int firstIndex = 0;
   boolean currentBoolState = false;
   for (int i = 1; i <= maxBits; i++) {
      boolean aBool = aBitSet.get(i); 
      if (firstTime) {
         firstTime = false;
         currentBoolState = aBool;
         firstIndex = i;
      } 
      if (aBool != currentBoolState) { // we changed from one state to the other
         aStr.append(firstIndex + " - " + (i-1) + " is " + currentBoolState + "\n");
         currentBoolState = aBool;
         firstIndex = i;
      } 
      if (i == maxBits) { // last index
         aStr.append(firstIndex + " - " + i + " is " + currentBoolState + "\n");
      }
   }
   return aStr.toString();
} 

      

//========================================================================
// string methods

/** Return a string describing this test instance.
 *
 */
public String toVerboseString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(super.toVerboseString());
   aStr.append("     checkLRU: " + checkLRU + "\n");
   aStr.append("     isMemLRU: " + isMemLRU + "\n");
   aStr.append("     perEntryOverhead: " + perEntryOverhead + "\n");
   aStr.append("     bytesPerEntry_keyValue: " + bytesPerEntry_keyValue + "\n");
   aStr.append("     bytesPerEntry_keyOnly: " + bytesPerEntry_keyOnly + "\n");
   aStr.append("     numEntriesDeviation: " + numEntriesDeviation + "\n");
   aStr.append("     minEvictionBytes: " + minEvictionBytes + "\n");
   aStr.append("     maxBytes: " + maxBytes + "\n");
   return aStr.toString();
}

}

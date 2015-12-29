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

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

public class MemLRUDynamicTest extends MemLRUTest {

// The bounds of the size of the region; each point within the bounds is multiplied
// by the BOUND_MULTIPLIER. For example, when the test uses the bounds to get a desired
// region size, the bounds point is multiplied by boundsMultplier to get the 
// actual maximum megabyte limit for the region
private int boundsMultiplier = -1;
private Bounds regionBounds = null;
private boolean randomCapacityChanges = false;
final long NUM_EVICTIONS_BEFORE_CHANGE = 200;

// initialization methods
// ================================================================================

/** Hydra task to initialize client threads */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new MemLRUDynamicTest();
      ((MemLRUDynamicTest)testInstance).initFields();
      testInstance.initialize();
      Log.getLogWriter().info(testInstance.toString());
   }
}

protected void initialize() {
   super.initialize();
   Region aRegion = CacheUtil.getRegion(REGION_NAME);
   int currentPoint = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.CURRENT_POINT)).intValue();
   int mm = currentPoint * boundsMultiplier;
   EvictionAttributesMutator mutator = aRegion.getAttributesMutator().getEvictionAttributesMutator();
   mutator.setMaximum(mm);
   Log.getLogWriter().info("Memory evictor is set to " + mm + " megabytes");
   memLRUParams = new MemLRUParameters(KEY_LENGTH, 1 * boundsMultiplier, aRegion);
   CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, memLRUParams);
}

/** Initialize instance fields of this test class */
protected void initFields() {
   boundsMultiplier = TestConfig.tab().intAt(CapConPrms.boundsMultiplier); 
   int targetLowPoint = TestConfig.tab().intAt(BoundsPrms.targetLowPoint); 
   int targetHighPoint = TestConfig.tab().intAt(BoundsPrms.targetHighPoint); 
   regionBounds = new Bounds(targetLowPoint, targetHighPoint, true, 
                 CapConBB.class, CapConBB.CAPCON_BB_NAME, CapConBB.CAPCON_BB_TYPE);
   CapConBB.getBB().getSharedMap().put(CapConBB.CURRENT_POINT, new Integer(1));
   randomCapacityChanges = ((Boolean)CapConBB.getBB().getSharedMap().get(CapConBB.RANDOM_CAPACITY_CHANGES)).booleanValue();
   CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE,
                                                    NUM_EVICTIONS_BEFORE_CHANGE);
}

// ================================================================================
// Hydra task methods

/** Serial execution test task */
public static void HydraTask_serialTest() {
   ((MemLRUDynamicTest)testInstance).doDynamicTest(true); 
}

/** Serial execution test task with transactions */
public static void HydraTask_serialTxTest() {
   ((MemLRUDynamicTest)testInstance).doSerialDynamicTxTest(); 
}

/** Serial execution test task with transactions */
public static void HydraTask_concTxTest() {
   ((MemLRUDynamicTest)testInstance).doConcDynamicTxTest(); 
}

/** Concurrent test task */
public static void HydraTask_concTest() {
   ((MemLRUDynamicTest)testInstance).doDynamicTest(false); 
}

/** End task */
public static void HydraTask_endTask() {
   checkForEventError();
   long numCapacityChanges = CapConBB.getBB().getSharedCounters().read(CapConBB.NUM_CAPACITY_CHANGES);
   Log.getLogWriter().warning("Number of capacity changes: " + numCapacityChanges);
}

// ================================================================================
// methods to do the work of the hydra tasks

/** Run for minTaskGranularitySec seconds; add to a region and after so
 *  many capacity controller evictions, change the capacity. If this is
 *  a serial execution test, verify the capacity after each add and
 *  capacity change.
 *
 *  @param isSerialExecution True if the test is serial and can do more
 *         validation, false otherwise.
 */
private void doDynamicTest(boolean isSerialExecution) {
   if (isSerialExecution) {
      long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
      Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   }
   memLRUParams = (MemLRUParameters)CapConBB.getBB().getSharedMap().get(CapConBB.TEST_SETTINGS);
   final long checkForCapacityChangeInterval = 10000; // millis
   Region workRegion = getWorkRegion();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In MemLRUDynamicTest.doDynamicTest, adding for " + timeToRunSec + " seconds");
   int count = 0;
   long startTime = System.currentTimeMillis();
   long lastCapacityChangeCheck = 0;  // will force a check the first time through the loop
   do {
      checkForEventError();
      boolean newCapacity = false;
      boolean newCapacityIsLower = false;
      if (System.currentTimeMillis() - lastCapacityChangeCheck >= checkForCapacityChangeInterval) {
         // see if it is time to change capacity
         lastCapacityChangeCheck = System.currentTimeMillis();
         double currentNumLRUEvictions = TestHelper.getNumLRUEvictions();
         long numLRUEvictionsToTriggerCapacityChange = CapConBB.getBB().getSharedCounters().read(CapConBB.NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE);
         Log.getLogWriter().info("Checking for capacity change, currentNumLRUEvictions = " + currentNumLRUEvictions +
                                 ", numLRUEvictionsToTriggerCapacityChange " + numLRUEvictionsToTriggerCapacityChange);
         if (currentNumLRUEvictions >= numLRUEvictionsToTriggerCapacityChange) { // change capacity
            int currentPoint = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.CURRENT_POINT)).intValue();
            int newMaximumMegabytes = -1;
            int oldCurrentPoint = currentPoint;
            if (randomCapacityChanges) {
               currentPoint = TestConfig.tab().getRandGen().nextInt(regionBounds.getTargetLowPoint(),
                                                                    regionBounds.getTargetHighPoint());
            } else {
               if (regionBounds.getDirection(currentPoint) == Bounds.UPWARD) {
                  currentPoint++;
               } else {
                  currentPoint--;
               }
            }
            newMaximumMegabytes = currentPoint * boundsMultiplier;
            Region aRegion = CacheUtil.getRegion(REGION_NAME);
            Log.getLogWriter().info("Setting new capacity (maximumMegabytes) " + newMaximumMegabytes +
                " on " + aRegion.getFullPath());
            EvictionAttributesMutator mutator = aRegion.getAttributesMutator().getEvictionAttributesMutator();
            mutator.setMaximum(newMaximumMegabytes);
            CapConBB.getBB().getSharedMap().put(CapConBB.CURRENT_POINT, new Integer(currentPoint));
            long cntr = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.NUM_CAPACITY_CHANGES);
            Log.getLogWriter().info("CapConBB.NUM_CAPACITY_CHANGES is now " + cntr);
            newCapacity = true;
            newCapacityIsLower = (currentPoint < oldCurrentPoint);
            memLRUParams = new MemLRUParameters(KEY_LENGTH, newMaximumMegabytes, aRegion);
            CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, memLRUParams);
         }
      }
   
      // add an entry to the region
      long beforeDestroyNotExp = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numAfterDestroyEvents_isNotExp);
      int beforeNumKeys = workRegion.keys().size();
      int afterNumKeys = 0;
      if (useTransactions)
         TxHelper.begin();
      testInstance.addEntry();
      if (TxHelper.exists())
         TxHelper.commitExpectSuccess();
      count++;
      afterNumKeys = workRegion.keys().size();
      CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.MAX_NUM_KEYS, afterNumKeys);
      Log.getLogWriter().info("After adding an entry: beforeNumKeys = " + beforeNumKeys +
                              ", afterNumKeys = " + afterNumKeys);

      // if we changed capacity; set the next number of evictions to trigger a capacity change;
      // we want to set this after the addEntry, so if the capacity was lower and we had lots of
      // evictions, we want to do NUM_EVICTIONS_BEFORE_CHANGE more before changing again
      if (newCapacity) {
         double currentNumLRUEvictions = TestHelper.getNumLRUEvictions();
         CapConBB.getBB().getSharedCounters().setIfLarger(CapConBB.NUM_LRU_EVICTIONS_TO_TRIGGER_CAPACITY_CHANGE, 
                  (long)currentNumLRUEvictions + NUM_EVICTIONS_BEFORE_CHANGE);
      }

      // do validation checks
      if (isSerialExecution) {
         // wait for destroy event if we evicted
         int numEvicted = (beforeNumKeys - afterNumKeys) + 1;
         if (numEvicted < 0)
            numEvicted = 0;
         Log.getLogWriter().info("Adding to the region evicted " + numEvicted + " entries");
         if (numEvicted > 0) { // we evicted; is a local destroy so see an event in only this VM 
            long expectedDestroys = beforeDestroyNotExp + numEvicted;
            TestHelper.waitForCounter(EventCountersBB.getBB(), "EventCountersBB.numAfterDestroyEvents_isNotExp",
                       EventCountersBB.numAfterDestroyEvents_isNotExp, expectedDestroys, true, 60000);
         }
         
         // check capacity after the add
         verifyMemCapacity(memLRUParams.getTotalAllowableBytes()); 
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In doDynamicTest, done running for " + timeToRunSec + " seconds, added " +
       count + " objects to " + TestHelper.regionToString(workRegion, false) + "; " + TestHelper.regionToString(workRegion, false)  + 
       " has " + workRegion.keys().size() + " entries");
}

/** Run for minTaskGranularitySec seconds; add to a region in a transaction
 *  and randomly decide when to change the limits of the capacity controller
 *  and randomly decide when to end the transaction, either by committing
 *  or rolling back. This is run in serial execution, so do validation of
 *  the region size at transaction boundaries.
 *
 */
private void doSerialDynamicTxTest() {
   long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   Region workRegion = getWorkRegion();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In MemLRUDynamicTest.doDynamicTxTest, adding for " + timeToRunSec + " seconds");
   int count = 0;
   long startTime = System.currentTimeMillis();
   memLRUParams = (MemLRUParameters)CapConBB.getBB().getSharedMap().get(CapConBB.TEST_SETTINGS);
   long totalBytesPerEntry = memLRUParams.getTotalBytesPerEntry();
   int currentNumEntries = workRegion.keys().size();
   long currentNumBytes = currentNumEntries * totalBytesPerEntry;
   int maxEntries = (int)(memLRUParams.getRegionByteLimit() / totalBytesPerEntry);
   long numBytesToChangeCapacity = TestConfig.tab().getRandGen().nextInt(
            currentNumEntries+1, maxEntries+100) * totalBytesPerEntry;
   int randy = TestConfig.tab().getRandGen().nextInt(
       currentNumEntries+1, maxEntries+100);
   long numBytesToEndTx = randy * totalBytesPerEntry;
   Log.getLogWriter().info("Setting numBytesToChangeCapacity to " + numBytesToChangeCapacity +
       " numBytesToEndTx to " + numBytesToEndTx +
       ", current num entries is " + currentNumEntries +
       ", current num bytes is " + currentNumBytes +
       ", randy="+randy+", maxEntries is "+maxEntries+
       ", totalBytesPerEntry is "+totalBytesPerEntry);
   if (numBytesToEndTx == 0) {
     throw new TestException("numBytesToEndTx is 0, currentNumEntries is " + currentNumEntries + ", maxEntries is " + maxEntries);
   }
 
   TxHelper.begin();
   int numEntriesWhenTxBegan = currentNumEntries;
   boolean done = false;
   do {
      checkForEventError();
      testInstance.addEntry();
      
      currentNumEntries = workRegion.keys().size();
      currentNumBytes = currentNumEntries * totalBytesPerEntry;
      Log.getLogWriter().info("Current number of entries is " + currentNumEntries +
          ", current number of bytes is " + currentNumBytes +
          ", numBytesToChangeCapacity is " + numBytesToChangeCapacity +
          ", numBytesToEndTx is " + numBytesToEndTx +
          ", current MemLRU maximum bytes is " + memLRUParams.getRegionByteLimit());

      if (currentNumBytes == numBytesToChangeCapacity) { // change the capacity
         int currentPoint = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.CURRENT_POINT)).intValue();
         if (randomCapacityChanges) {
            currentPoint = TestConfig.tab().getRandGen().nextInt(regionBounds.getTargetLowPoint(),
                                                                 regionBounds.getTargetHighPoint());
         } else {
            if (regionBounds.getDirection(currentPoint) == Bounds.UPWARD) {
               currentPoint++;
            } else {
               currentPoint--;
            }
         }
         CapConBB.getBB().getSharedMap().put(CapConBB.CURRENT_POINT, new Integer(currentPoint));
         int newMaximumMegabytes = currentPoint * boundsMultiplier;
         Region aRegion = CacheUtil.getRegion(REGION_NAME);
         Log.getLogWriter().info("Setting new capacity (maximumMegabytes) " + newMaximumMegabytes + 
             " on " + aRegion.getFullPath() + ", current num entries is " + currentNumEntries +
             ", current num bytes is " + currentNumBytes);
         EvictionAttributesMutator mutator = aRegion.getAttributesMutator().getEvictionAttributesMutator();
         mutator.setMaximum(newMaximumMegabytes);
         long cntr = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.NUM_CAPACITY_CHANGES);
         memLRUParams = new MemLRUParameters(KEY_LENGTH, newMaximumMegabytes, aRegion);
         CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, memLRUParams);
         maxEntries = (int)(memLRUParams.getRegionByteLimit() / totalBytesPerEntry);
         int regSizeAfterChangingCap = workRegion.keys().size();
         if (currentNumEntries != regSizeAfterChangingCap) {
            throw new TestException("Expected number of entries before changing capacity " + 
               currentNumEntries + " to be equal to num entries after changing capacity " + 
               regSizeAfterChangingCap);
         }
      }

      done = System.currentTimeMillis() - startTime > timeToRunMS;
      if (done || (currentNumBytes == numBytesToEndTx)) { // time to end the transaction
         if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 75) { // most of the time do a commit
            int beforeCommitNumEntries = currentNumEntries;
            long beforeCommitNumBytes = currentNumBytes;
            Log.getLogWriter().info("Before commit, num entries is " + beforeCommitNumEntries +
                                    ", num bytes is " + beforeCommitNumBytes);
            TxHelper.commitExpectSuccess();
            int afterCommitNumEntries = workRegion.keys().size();
            long afterCommitNumBytes = afterCommitNumEntries * totalBytesPerEntry;
            if (beforeCommitNumBytes <= memLRUParams.getRegionByteLimit()) { // should be no change in region size
               if (beforeCommitNumBytes != afterCommitNumBytes)
                  throw new TestException("Before commit num entries was " + beforeCommitNumEntries + 
                     " and numBytes was " + beforeCommitNumBytes +
                     ", after commit num entries was " + afterCommitNumEntries +
                     " and numBytes was " + afterCommitNumBytes);
            } else { // number of bytes before the commit is > current capacity setting
              testInstance.addEntry();  // add an entry to trigger eviction which should then make the region honor the memLRU setting
              afterCommitNumEntries = workRegion.size();
              afterCommitNumBytes = afterCommitNumEntries * totalBytesPerEntry;
              if (afterCommitNumBytes > memLRUParams.getRegionByteLimit()) 
                  throw new TestException("After committing, num entries is " + afterCommitNumEntries +
                     " and numBytes is " + afterCommitNumBytes +
                     ", but currentCapacity maximumMegabytes is " + memLRUParams.getRegionByteLimit());
            } 
         } else { // do a rollback
            int numEntriesBeforeRollback = currentNumEntries;
            TxHelper.rollback();
            int numEntriesAfterRollback = workRegion.keys().size();
            long afterRollbackNumBytes = numEntriesAfterRollback * totalBytesPerEntry;
            Log.getLogWriter().info("After rollback, numEntries is " + numEntriesAfterRollback +", afterRollbackNumBytes is " + afterRollbackNumBytes +
                ", maxEntries is " + maxEntries);
            if (numEntriesAfterRollback <= maxEntries) { //expect no change in region size from when tx began
              Log.getLogWriter().info("No eviction expected");
              if (numEntriesAfterRollback != numEntriesWhenTxBegan) {
                 throw new TestException("When transaction began num entries was " + numEntriesWhenTxBegan +
                    ", before rollback num entries was " + numEntriesBeforeRollback +
                    ", after rollback num entries was " + numEntriesAfterRollback);
              }
            } else { // eviction expected
              Log.getLogWriter().info("Adding an entry to trigger eviction");
              testInstance.addEntry();  // add an entry to trigger eviction which should then make the region honor the memLRU setting
              numEntriesAfterRollback = workRegion.keys().size();
              afterRollbackNumBytes = numEntriesAfterRollback * totalBytesPerEntry;
              Log.getLogWriter().info("After adding an entry to trigger eviction region size is " + numEntriesAfterRollback + ", numBytes is " + afterRollbackNumBytes);
              if (afterRollbackNumBytes > memLRUParams.getTotalAllowableBytes()) {
                throw new TestException("After rollback, num entries is " + numEntriesAfterRollback +
                   " and numBytes is " + afterRollbackNumBytes +
                   ", but currentCapacity allowable bytes is " + memLRUParams.getTotalAllowableBytes() + "\n" + memLRUParams);
              }
            }

         }

         // Now set new targets for changing capacity and ending tx.
         // Now that we have ended the tx, the size of the region cannot be more 
         // than the current capacity setting
         currentNumEntries = workRegion.keys().size();
         currentNumBytes = currentNumEntries * totalBytesPerEntry;
         maxEntries = (int)(memLRUParams.getRegionByteLimit() / totalBytesPerEntry);
         numBytesToChangeCapacity =  (TestConfig.tab().getRandGen().nextInt(
            currentNumEntries+1, maxEntries+100)) * totalBytesPerEntry;
         randy = TestConfig.tab().getRandGen().nextInt(
             currentNumEntries+1, maxEntries+100);
         numBytesToEndTx = randy * totalBytesPerEntry;
         Log.getLogWriter().info("Setting numBytesToChangeCapacity to " + numBytesToChangeCapacity +
             " numBytesToEndTx to " + numBytesToEndTx +
             ", current num entries is " + currentNumEntries +
             ", current num bytes is " + currentNumBytes +
             ", randy="+randy+", maxEntries is "+maxEntries+
             ", totalBytesPerEntry is "+totalBytesPerEntry);
         if (numBytesToEndTx == 0) {
           throw new TestException("numBytesToEndTx is 0, currentNumEntries is " + currentNumEntries + ", maxEntries is " + maxEntries);
         }

         if (!done) {
           TxHelper.begin();
           numEntriesWhenTxBegan = currentNumEntries; 
         }
      }
   } while (!done);

   Log.getLogWriter().info("In doDynamicTxTest, done running for " + timeToRunSec + " seconds, added " +
       count + " objects to " + TestHelper.regionToString(workRegion, false) + "; " + 
       TestHelper.regionToString(workRegion, false)  + 
       " has " + currentNumEntries + " entries and " + currentNumBytes + " bytes");
}

/** Run for minTaskGranularitySec seconds; add to a region in a transaction
 *  and randomly decide when to change the limits of the capacity controller
 *  and randomly decide when to end the transaction, either by committing
 *  or rolling back. This is run in concurrent execution, so do validation of
 *  the region size at transaction boundaries.
 *
 */
private void doConcDynamicTxTest() {
   long exeNum = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   Region workRegion = getWorkRegion();
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In MemLRUDynamicTest.doDynamicTxTest, adding for " + timeToRunSec + " seconds");
   int count = 0;
   long startTime = System.currentTimeMillis();
   memLRUParams = (MemLRUParameters)CapConBB.getBB().getSharedMap().get(CapConBB.TEST_SETTINGS);
   long timeToChangeCapacity = System.currentTimeMillis() + 5000;
   do {
      checkForEventError();
      TxHelper.begin();
      testInstance.addEntry();
      if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 75) { // most of the time do a commit
         TxHelper.commitExpectSuccess();
      } else { // do a rollback
         TxHelper.rollback();
      }
      
      if (System.currentTimeMillis() >= timeToChangeCapacity) { // change the capacity
         int currentPoint = ((Integer)CapConBB.getBB().getSharedMap().get(CapConBB.CURRENT_POINT)).intValue();
         if (randomCapacityChanges) {
            currentPoint = TestConfig.tab().getRandGen().nextInt(regionBounds.getTargetLowPoint(),
                                                                 regionBounds.getTargetHighPoint());
         } else {
            if (regionBounds.getDirection(currentPoint) == Bounds.UPWARD) {
               currentPoint++;
            } else {
               currentPoint--;
            }
         }
         CapConBB.getBB().getSharedMap().put(CapConBB.CURRENT_POINT, new Integer(currentPoint));
         int newMaximumMegabytes = currentPoint * boundsMultiplier;
         Region aRegion = CacheUtil.getRegion(REGION_NAME);
         Log.getLogWriter().info("Setting new capacity (maximumMegabytes) " + newMaximumMegabytes + 
             " on " + aRegion.getFullPath());
         EvictionAttributesMutator mutator = aRegion.getAttributesMutator().getEvictionAttributesMutator();
         mutator.setMaximum(newMaximumMegabytes);
         long cntr = CapConBB.getBB().getSharedCounters().incrementAndRead(CapConBB.NUM_CAPACITY_CHANGES);
         memLRUParams = new MemLRUParameters(KEY_LENGTH, newMaximumMegabytes, aRegion);
         CapConBB.getBB().getSharedMap().put(CapConBB.TEST_SETTINGS, memLRUParams);
         timeToChangeCapacity = System.currentTimeMillis() + 5000;
      }
      verifyMemCapacity(memLRUParams.getTotalAllowableBytes()); 
   } while (System.currentTimeMillis() - startTime < timeToRunMS);

   Log.getLogWriter().info("In doDynamicTxTest, done running for " + timeToRunSec + " seconds, added " +
       count + " objects to " + TestHelper.regionToString(workRegion, false));
}

// Overridden methods
//========================================================================

public CacheListener getEventListener() {
   if (TestConfig.tab().booleanAt(Prms.serialExecution)) {
      return new MemLRUListener();  // verifies evictions
   } else {
      return null;  // cannot verify evictions in concurrent test
   }
}

// implementation of abstract methods
// ================================================================================

// String methods
//========================================================================
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(super.toString() + "\n");
   aStr.append("   Region bounds:\n");
   aStr.append("      boundsMultiplier: " + boundsMultiplier + "\n");
   aStr.append("      regionBounds: " + regionBounds + "\n");
   aStr.append("      randomCapacityChanges: " + randomCapacityChanges + "\n");
   return aStr.toString();
}

}

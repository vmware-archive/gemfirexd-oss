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
package memscale;

import hydra.CacheHelper;
import hydra.DistributedSystemHelper;
import hydra.GsRandom;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedCounters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import management.test.cli.CommandTestVersionHelper;
import parReg.ParRegUtil;
import util.BaseValueHolder;
import util.NameFactory;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.internal.offheap.OffHeapMemoryStats;
import com.gemstone.gemfire.pdx.PdxInstance;

import diskRecovery.RecoveryBB;
import diskRecovery.RecoveryTestVersionHelper;

/**
 * @author lynng
 *
 */
public class OffHeapStressTest {

  private static GsRandom gsRand = TestConfig.tab().getRandGen();
  private final static int[] sizes = new int[] {1, 10000, 50, 863, 5000};
  private static HydraThreadLocal threadLocal_isLeader = new HydraThreadLocal();
  private static volatile long[] putDurationHistory = new long[100];
  private static volatile long[] failedPutDurationHistory = new long[100];
  private static volatile long[] destroyDurationHistory = new long[100];
  private static final int OP_DURATION_THRESHOLD_MS = 10000; // throw if an op takes longer than this, a very generous limit


  // blackboard keys
  private final static String allRegionsSnapshotKey = "snapshot";
  private final static String isDataStoreKey = "vmIdIsDataStore_";

  /** Create locator task
   * 
   */
  public synchronized static void HydraTask_createLocator() {
    hydra.DistributedSystemHelper.createLocator();
  }

  /** Start locator task
   * 
   */
  public synchronized static void HydraTask_startLocatorAndDS() {
    hydra.DistributedSystemHelper.startLocatorAndDS();
  }

  /**
   * Creates and initializes a server or peer.
   */
  public synchronized static void HydraTask_initializeRegions() {
    if (CacheHelper.getCache() == null) {
      CacheHelper.createCache("cache1");
    }
    if (CacheHelper.getCache().rootRegions().size() == 0) {
      CommandTestVersionHelper.createRegions();
      PRObserver.installObserverHook();
      PRObserver.initialize();
      if (thisMemberIsDataStore()) {
        MemScaleBB.getBB().getSharedMap().put(isDataStoreKey + RemoteTestModule.getMyVmid(), RemoteTestModule.getMyVmid());
      }
    }
  }

  /** Return the number of partitioned regions.
   * 
   * @return The number of partitioned regions currently defined in the cache.
   */
  private static int getPRCount() {
    Set<Region<?, ?>> allRegions = getAllRegions();
    int PRCount = 0;
    for (Region aRegion: allRegions) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        PRCount++;
      }
    }
    return PRCount;
  }

  /** Test task to repeatedly:
   *     - put until other members get OOM for off-heap memory (other members should close their cache)
   *     - do ops for a while longer
   *     - pause and validate
   *     - close all regions
   *     - validate off-heap memory
   *     - reinitialize
   */
  public static void HydraTask_offHeapCeilingTest() {
    SharedCounters sc = MemScaleBB.getBB().getSharedCounters();
    int numThreads = MemScalePrms.getNumThreads1();

    // determine if this thread is the leader; if so this thread is leader for the duration of the test
    Object value = threadLocal_isLeader.get();
    boolean isLeader = false;
    if (value == null) {
      if (!thisMemberIsAccessor() && !(thisMemberExpectsOOM())) { // accessors/oom members cannot be the leader because leaders take snapshots
        // an accessor for a replicated region is size 0
        long leaderCounter = sc.incrementAndRead(MemScaleBB.leader);
        isLeader = (leaderCounter == 1);
        threadLocal_isLeader.set(isLeader);
      }
    } else {
      isLeader = (Boolean)value;
    }
    Log.getLogWriter().info("isLeader: " + isLeader);

    if (isLeader) {
      Log.getLogWriter().info("Execution number: " + sc.incrementAndRead(MemScaleBB.currentExecutionCycle));
    }

    Log.getLogWriter().info("Putting until designated members get out of memory for off-heap");
    putUntilOthersGetOOM();
    
    Log.getLogWriter().info("About to pause with pause1 counter, then do data validation");
    pause(isLeader, "pause1", new String[] {"pause3"}, numThreads, true);
    doDataValidationOnce();
    OffHeapHelper.verifyOffHeapMemoryConsistencyOnce();

    Log.getLogWriter().info("About to pause with pause2 counter, then close all regions and do off-heap memory consistency checks");
    pause(isLeader, "pause2", new String[] {"pause4"}, numThreads, false);
    OffHeapHelper.closeAllRegions();
    OffHeapHelper.waitForOffHeapSilence(10);
    OffHeapHelper.verifyOffHeapMemoryConsistencyOnce();
    if (sc.read(MemScaleBB.currentExecutionCycle) >= MemScalePrms.getNumberExecutionCycles()) {
       sc.increment(MemScaleBB.timeToStop);
    }
    
    Log.getLogWriter().info("About to pause with pause3 counter, then reinitialize");
    pause(isLeader, "pause3", new String[] {"pause1", "receivedOOM"}, numThreads, false);
    HydraTask_initializeRegions();
    // no need to wait for redundancy recovery; all regions were closed, off-heap is empty
    if (isLeader) { // signal to OOM members it is time to reinitialize
      Log.getLogWriter().info("Leader is incrementing doneValidator to let OOM members know it is time to reinitialize");
      MemScaleBB.getBB().getSharedCounters().increment(MemScaleBB.doneValidating);
    }
    
    Log.getLogWriter().info("About to pause with pause4 counter to signal the end of the test cycle");
    pause(isLeader, "pause4", new String[] {"pause2", "doneValidating"}, MemScalePrms.getNumThreads2(), false);

    // see if it is time to stop
    if (sc.read(MemScaleBB.timeToStop) > 0) {
      throw new StopSchedulingTaskOnClientOrder("Completed " + sc.read(MemScaleBB.currentExecutionCycle)+ " test cycles");
    }
  }

  /** Return a List of vmIds for all members that are data stores.
   * 
   * @return A List of Integers which are vmIds.
   */
  private static List<Integer> getDataStoreVmIds() {
    Map sharedMap = MemScaleBB.getBB().getSharedMap().getMap();
    List<Integer> aList = new ArrayList();
    for (Object key: sharedMap.keySet()) {
      if (key instanceof String) {
        if (((String)key).startsWith(isDataStoreKey)) {
          aList.add((Integer) sharedMap.get(key));
        }
      }
    }
    return aList;
  }

  /** Test task for members that are expected to get OOM for off-heap memory and close their cache.
   * 
   *  Put until OOM for off-heap is detected.
   *  Wait for the signal to reinitialize.
   */
  public static void HydraTask_offHeapCeilingTestOOM() throws Throwable {
    SharedCounters sc = MemScaleBB.getBB().getSharedCounters();
    final boolean isLeader = false; // members that runOOM are never the leader
    Log.getLogWriter().info("isLeader: " + isLeader);

    Cache theCache = CacheHelper.getCache();
    DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
    putUntilOOMDetected(); // returns when this member closes the cache
    // verify the cache is closed and we are disconnected from the ds
    long begin = System.currentTimeMillis();
    while (!theCache.isClosed() || ds.isConnected()) {
      Thread.sleep(100);
      if (System.currentTimeMillis() > (begin + 60*1000)) {
        break;
      }
    }
    boolean cacheClosed = theCache.isClosed();
    boolean dsConnected = ds.isConnected();
    Log.getLogWriter().info("After getting off-heap OOM, cacheClosed is " + cacheClosed + ", dsConnected is " + dsConnected);
    if (!cacheClosed) {
      throw new TestException("Expected the cache to be closed, but Cache.isClosed() is " + cacheClosed);
    }
    if (dsConnected) {
      throw new TestException("Expected the distributed system to be disconnected, but isConnected() is " + dsConnected);
    }
    TestHelper.waitForCounter(MemScaleBB.getBB(), "doneValidating", MemScaleBB.doneValidating, 1, true, -1, 1000);
    DistributedSystemHelper.cleanupAfterAutoDisconnect();
    HydraTask_initializeRegions(); // for next test iteration
    pause(isLeader, "pause4", null, MemScalePrms.getNumThreads2(), false);
    logDurationHistory();

    // see if it is time to stop
    if (sc.read(MemScaleBB.timeToStop) > 0) {
      throw new StopSchedulingTaskOnClientOrder("Completed " + sc.read(MemScaleBB.currentExecutionCycle)+ " test cycles");
    }
  }

  /** Do data consistency validation once in this member.
   * 
   */
  static long endTime = 0;
  public static void doDataValidationOnce() {
    long verifyRequestedTime = System.currentTimeMillis();
    synchronized (OffHeapHelper.class) {
      if (verifyRequestedTime > endTime) { // do the verify
        verifyFromSnapshot();
        verifyPRs();
        endTime = System.currentTimeMillis();
      } else {
        Log.getLogWriter().info("This thread did not do data consistency validation because it was done by another thread in this member");
      }
    }
  }

  /** Pause all threads executing this task, return when all threads are paused.
   * 
   * @param isLeader If true, this thread is the concurrent leader which controls each step of the test (including pausing!)
   * @param pauseCounterName The name of the shared counter on the memscale.MemScaleBB blackboard to use for pausing.
   * @param counterNamesToZero The name any counters to reset to zero when the leader determines all threads have paused
   *                           (this provides a safe point for zeroing counters)
   * @param counterTarget The targe counter value wait for to pause.
   * @param writeSnapshot If true, then the leader writes a snapshot while everybody is paused.
   */
  private static void pause(boolean isLeader, String pauseCounterName, String[] counterNamesToZero, int counterTarget, boolean writeSnapshot) {
    Blackboard bb = MemScaleBB.getBB();
    SharedCounters sc = bb.getSharedCounters();
    if (isLeader) {
      // the leader waits for all OTHER threads to pause (but not itself, the leader has not yet incremented the counter)
      TestHelper.waitForCounter(bb, pauseCounterName, bb.getSharedCounter(pauseCounterName), counterTarget-1, true, -1,1000);
      Log.getLogWriter().info("Leader has determined that all threads have paused on counter " + pauseCounterName);
      // all threads have paused
      if (writeSnapshot) {
        writeSnapshot();
      }
      // zero the given counters (if any)
      if (counterNamesToZero != null) {
        for (String counterToZero: counterNamesToZero) {
          Log.getLogWriter().info("Zeroing " + counterToZero);
          sc.zero(bb.getSharedCounter(counterToZero));
        }
      }
      // now increment our counter, which allows us (the leader) and all other threads to return
      Log.getLogWriter().info("Leader is doing the last counter increment for counter " + pauseCounterName + " to allow all theads to proceed");
      sc.increment(bb.getSharedCounter(pauseCounterName));
    } else {
      // we are not the leader, increment that we are pausing, then wait for ALL threads (including the leader) to indicated they have paused
      sc.increment(bb.getSharedCounter(pauseCounterName));
      TestHelper.waitForCounter(bb, pauseCounterName, bb.getSharedCounter(pauseCounterName), counterTarget, true, -1, 1000);
    }
  }

  /** Write a snapshot of all regions
   * 
   */
  private static void writeSnapshot() {
    Set<Region<?, ?>> allRegions = getAllRegions();
    Log.getLogWriter().info("Preparing to write snapshot for " + allRegions.size() + " regions");
    Map<String, Map> allRegionsSnapshot = new HashMap<String, Map>();
    for (Region aRegion: allRegions) {
      Map regionSnapshot = new HashMap();
      for (Object key: aRegion.keySet()) {
        Object value = null;
        if (aRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
          value = aRegion.get(key);
        }
        byte[] byteArr = (byte[]) value;
        regionSnapshot.put(key, byteArr.length);
      }
      allRegionsSnapshot.put(aRegion.getFullPath(), regionSnapshot);
      Log.getLogWriter().info("Region snapshot for " + aRegion.getFullPath() + " is size " + regionSnapshot.size() + " and contains keys " + regionSnapshot.keySet());
    }
    RecoveryBB.getBB().getSharedMap().put(allRegionsSnapshotKey, allRegionsSnapshot);
    Log.getLogWriter().info("Put snapshot for " + allRegions.size() + " regions into blackboard at key " + allRegionsSnapshotKey);
  }

  /** Verify against the snapshot (all regions)
   * 
   */
  private static void verifyFromSnapshot() {
    Map<String, Map> allRegionsSnapshot = (Map)(RecoveryBB.getBB().getSharedMap().get(allRegionsSnapshotKey));
    for (String regionName: allRegionsSnapshot.keySet()) {
      Map regionSnapshot = allRegionsSnapshot.get(regionName);
      Region aRegion = CacheHelper.getCache().getRegion(regionName);
      if (aRegion == null) {
        throw new TestException("Region " + regionName + " could not be found in cache");
      }
      verifyFromSnapshot(aRegion, regionSnapshot);
    }
  }

  /** Verify that the given region is consistent with the given snapshot.
   * 
   * @param aRegion The region to verify
   * @param snapshot The expected contents of aRegion.
   */
  private static void verifyFromSnapshot(Region aRegion, Map regionSnapshot) {
    // init
    StringBuffer errStr = new StringBuffer();
    int snapshotSize = regionSnapshot.size();
    int regionSize = aRegion.size();
    long startVerifyTime = System.currentTimeMillis();
    Log.getLogWriter().info("Verifying " + aRegion.getFullPath() + "  of size " + aRegion.size() + 
        " against snapshot containing " + regionSnapshot.size() + " entries...");

    // verify
    if (aRegion.getAttributes().getDataPolicy().isEmpty()) { // region is empty
      for (Object key: regionSnapshot.keySet()) {
        Object actualValue = aRegion.get(key);
        Object expectedValue = regionSnapshot.get(key); // snapshot only stores the length of the byte[]
        errStr.append(validate(key, actualValue, expectedValue));
      }
    } else {
      if (snapshotSize != regionSize) {
        //((LocalRegion)aRegion).dumpBackingMap();
        errStr.append("Expected region " + aRegion.getFullPath() + " to be size " + snapshotSize + 
            ", but it is " + regionSize + "\n");
      }
      for (Object key: regionSnapshot.keySet()) {
        // validate using ParRegUtil calls even if the region is not PR; these calls do the desired job
        // containsKey
        try {
          ParRegUtil.verifyContainsKey(aRegion, key, true);
        } catch (TestException e) {
          errStr.append(e.getMessage() + "\n");
        }

        // containsValueForKey
        boolean containsValueForKey = aRegion.containsValueForKey(key);
        Object expectedValue = regionSnapshot.get(key);
        try {
          ParRegUtil.verifyContainsValueForKey(aRegion, key, (expectedValue != null));
        } catch (TestException e) {
          errStr.append(e.getMessage() + "\n");
        }

        // check the value
        if (containsValueForKey) {
          Object actualValue = aRegion.get(key);
          if (actualValue instanceof PdxInstance) {
            actualValue = RecoveryTestVersionHelper.toValueHolder(actualValue);
          }
          errStr.append(validate(key, actualValue, expectedValue));
        } 
        // else value in region is null; the above check for containsValueForKey
        // checked that the snapshot is also null
      }
    }

    // check for extra keys in the region that were not in the snapshot
    Set aRegionKeySet = new HashSet(aRegion.keySet()); 
    Set snapshotKeySet = regionSnapshot.keySet();
    aRegionKeySet.removeAll(snapshotKeySet);
    if (aRegionKeySet.size() != 0) {
      errStr.append("Found the following unexpected keys in " + aRegion.getFullPath() + 
          ": " + aRegionKeySet + "\n");
    }

    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
    Log.getLogWriter().info("Done verifying " + aRegion.getFullPath() + " from snapshot containing " + snapshotSize + " entries, " +
        "verification took " + (System.currentTimeMillis() - startVerifyTime) + "ms");
  }

  /** Compare a value from a region to the expecte/
   *  
   * @param key The key for the actualValue from a region.
   * @param actualValue The value from a region.
   * @param expectedValue The expected value.
   * @return A String containing information about any errors found.
   */
  private static String validate(Object key, Object actualValue, Object expectedValue) {
    boolean isEqual = true;
    if ((actualValue == null) || (expectedValue == null)) { // handle nulls
      if (actualValue != expectedValue) {
        return "For key " + key + ", expected value to be " + TestHelper.toString(expectedValue) + 
            ", but it is " + TestHelper.toString(actualValue) + "\n";
      }
    } else { // neither actualValue nor expectedValue is null
      Integer expectedLength = (Integer)expectedValue;
      byte[] actualByteArr = (byte[])actualValue;
      if (actualByteArr.length != expectedLength) {
        return "Expected " + TestHelper.toString(actualValue) + " to be a byte[] of length " + expectedLength 
            + " but it is length " + actualByteArr.length;
      }
    }
    return "";
  }
  
  /** Put into all regions until other members receive OOM for off-heap memory. Switch to
   *  doing destroys if the off-heap memory becomes too full as this member is not allowed
   *  to run out of off-heap memory.
   * 
   */
  private static void putUntilOthersGetOOM() {
    Log.getLogWriter().info("Putting until other members get OOM");
    if (thisMemberIsAccessor()) {
      accessorPutUntilOthersGetOOM();
    } else {
      OffHeapMemoryStats offHeapStats = OffHeapHelper.getOffHeapMemoryStats();
      long totalOffHeapSizeInBytes = offHeapStats.getMaxMemory();
      long thresholdInBytes = (long) (totalOffHeapSizeInBytes * 0.30);
      Set<Region<?, ?>> regionSet = getAllRegions();
      int numOOMMembers = MemScalePrms.getNumMembers();
      while (true) {
        for (Region aRegion: regionSet) {
          long currentOffHeapSizeInBytes = offHeapStats.getUsedMemory();;
          Log.getLogWriter().info("currentOffHeapSizeinBytes = " + currentOffHeapSizeInBytes + ", threshold in bytes is " + thresholdInBytes);
          if (currentOffHeapSizeInBytes < thresholdInBytes) {
            Object key = getNextKey();
            Log.getLogWriter().info("Putting " + key + " into " + aRegion.getFullPath());
            Object value = getValue();
            long startTime = System.currentTimeMillis();
            aRegion.put(key, value);
            long duration = System.currentTimeMillis() - startTime;
            handleSuccessfulPutDuration(duration);
          } else {
            try {
              try {
                Object key = aRegion.keySet().iterator().next();
                Log.getLogWriter().info("Destroying " + key + " in " + aRegion.getFullPath());
                long startTime = System.currentTimeMillis();
                aRegion.destroy(key);
                long duration = System.currentTimeMillis() - startTime;
                handleSuccessfulDestroyDuration(duration);
              } catch (NoSuchElementException e) {
                Log.getLogWriter().info("No element in " + aRegion.getFullPath() + " to destroy");
              }
            } catch (EntryNotFoundException e) { // expected with concurrent execution
              Log.getLogWriter().info("Caught expected " + e.getClass().getName());
            }
          }
          long counter = MemScaleBB.getBB().getSharedCounters().read(MemScaleBB.receivedOOM);
          if (counter == numOOMMembers) {
            Log.getLogWriter().info(counter + " members received ouf-of-off-heap exceptions");
            return;
          }
        }
      }
    }
    logDurationHistory();
  }
  
  /** Log, verify and record the duration of a successful put
   * 
   * @param duration The duration of the put in milliseconds
   */
  private static void handleSuccessfulPutDuration(long duration) {
    Log.getLogWriter().info("Put completed in " + duration + " ms");
    if (duration > OP_DURATION_THRESHOLD_MS) {
      logDurationHistory();
      throw new TestException("Bug 49735 detected: Put took " + duration + " ms, too long!");
    }
    int sec = (int) (duration / 1000);
    putDurationHistory[sec]++;
  }
  
  /** Log, verify and record the duration of a successful destroy
   * 
   * @param duration The duration of the destroy in milliseconds
   */
  private static void handleSuccessfulDestroyDuration(long duration) {
    Log.getLogWriter().info("Destroy completed in " + duration + " ms");
    if (duration > OP_DURATION_THRESHOLD_MS) {
      logDurationHistory();
      throw new TestException("Bug 49735 detected: Destroy took " + duration + " ms, too long!");
    }
    int sec = (int) (duration / 1000);
    destroyDurationHistory[sec]++;
  }
  
  /** Log, verify and record the duration of a failed put
   * 
   * @param duration The duration of the put in milliseconds
   */
  private static void handleFailedPutDuration(long duration) {
    Log.getLogWriter().info("Failed put completed in " + duration + " ms");
    if (duration > OP_DURATION_THRESHOLD_MS) {
      logDurationHistory();
      throw new TestException("Bug 49735 detected: Failed put took " + duration + " ms, too long!");
    }
    int sec = (int) (duration / 1000);
    failedPutDurationHistory[sec]++;
  }
  
  /** log the history of ops in this member
   * 
   */
  public static void logDurationHistory() {
    logHistory("successful put", putDurationHistory);
    logHistory("failed put", failedPutDurationHistory);
    logHistory("successful destroy", destroyDurationHistory);
  }
  
  private static void logHistory(String opType, long[] history) {
    StringBuilder sb = new StringBuilder();
    sb.append(opType + " duration history for this member:\n");
    int lastNonZeroIndex = 0;
    for (int i = history.length-1; i > 0; i--) {
      if (history[i] != 0) {
        lastNonZeroIndex = i;
        break;
      }
    }
    if (lastNonZeroIndex >= 0) {
      for (int i = 0; i <= lastNonZeroIndex; i++) {
        int lowerBoundMS = i * 1000;
        int upperBoundMS = lowerBoundMS + 999;
        sb.append("Num " + opType + "s between " + lowerBoundMS + " and " + upperBoundMS + " milliseconds: " + history[i] + "\n");
      }
    }
    Log.getLogWriter().info(sb.toString());
  }
  
  /** As the test runs, it collects how long operations take. This looks for operations
   *  that took too long.
   */
  public static void checkOperationTimes() {
    checkOperationTimes("successful put", putDurationHistory);
    checkOperationTimes("failed put", failedPutDurationHistory);
    checkOperationTimes("successful destroy", destroyDurationHistory);
  }
  
  /** Check if an operation took too long during this run
   * 
   * @param opName The kind of operation
   * @param history The history of timings for this operation in this member.
   */
  public static void checkOperationTimes(String opName, long[] history) {
    int lastNonZeroIndex = -1;
    for (int i = history.length-1; i > 0; i--) {
      if (history[i] != 0) {
        lastNonZeroIndex = i;
        break;
      }
    }
    int threshold = OP_DURATION_THRESHOLD_MS / 1000;
    if (lastNonZeroIndex >= threshold) {
      // if this is thrown, look in the logs for "too long" to get the exact operation and exact length of time
      throw new TestException("Bug 49735 detected: " + opName + " took longer than " + threshold + " seconds in this member");
    }
  }

  /** Accessors put until all OOM members report they have run out of memory, then they stop
   *  doing work. 
   * 
   */
  private static void accessorPutUntilOthersGetOOM() {
    Set<Region<?, ?>> regionSet = getAllRegions();
    int numOOMMembers = MemScalePrms.getNumMembers();
    while (true) {
      for (Region aRegion: regionSet) {
        long counter = MemScaleBB.getBB().getSharedCounters().read(MemScaleBB.receivedOOM);
        if (counter == numOOMMembers) {
          Log.getLogWriter().info(counter + " members received ouf-of-off-heap exceptions");
          logDurationHistory();
          return;
        }
        Object key = getNextKey();
        Log.getLogWriter().info("Putting " + key + " into " + aRegion.getFullPath());
        Object value = getValue();
        long startTime = System.currentTimeMillis();
        aRegion.put(key, value);
        long duration = System.currentTimeMillis() - startTime;
        handleSuccessfulPutDuration(duration);
      }
    }
  }

  /** Put into all regions until this member gets an OOM exception for off-heap memory
   *  and confirm that the product closed the cache after receiving the OOM exception.
   * 
   */
  private static void putUntilOOMDetected() throws Throwable {
    Log.getLogWriter().info("Putting until this member gets OOM exception");
    Set<Region<?, ?>> regionSet = getAllRegions();
    OffHeapMemoryStats stats = OffHeapHelper.getOffHeapMemoryStats();
    long previousNumCompactions = stats.getCompactions();
    while (true) {
      for (Region aRegion: regionSet) {
        long startTime = 0;
        try {
          Object key = getNextKey();
          Log.getLogWriter().info("Putting " + key);
          startTime = System.currentTimeMillis();
          aRegion.put(key, getValue());
          long duration = System.currentTimeMillis() - startTime;
          handleSuccessfulPutDuration(duration);
        } catch (Throwable t) {
          long duration = System.currentTimeMillis() - startTime;
          handleException(t); // if we return then we got an OutOfOffHeapMemoryException
          handleFailedPutDuration(duration);
          verifyCompaction(stats, previousNumCompactions); // verify that we did a compaction; this is the last chance effort before running out of off-heap
          final long begin = System.currentTimeMillis();
          final int sleepMs = 5000;
          while (true) { // verify the product closed the cache; this takes some time after throwing the OOM so be patient
            try {
              Log.getLogWriter().info("Trying to get the cache...");
              CacheFactory.getAnyInstance();
              Log.getLogWriter().info("Successfully got the cache");
            } catch (CancelException ce) {
              Log.getLogWriter().info("Caught expected " + ce.getClass().getName());
              long receivedOOM = MemScaleBB.getBB().getSharedCounters().incrementAndRead(MemScaleBB.receivedOOM);
              Log.getLogWriter().info("MemScaleBB.receivedOOM is now " + receivedOOM);
              return;
            }
            Log.getLogWriter().info("Cache not yet closed " +
                "trying again in " + sleepMs + " ms");
            MasterController.sleepForMs(sleepMs);
            if (System.currentTimeMillis() > (begin + 360*1000)) {
              throw new TestException("putUntilOOMDetected waited too long for CacheFactory.getAnyInstance() to throw CancelException");
            }
          }
        }
      }
    }
  }

  /** Verify that compaction occurred and its count is one more than the previous stat value.
   * 
   * @param stats The off-heap memory stats to use to check for compaction.
   * @param previousNumCompactions The expected number of compactions should be greater than this number.
   */
  private static void verifyCompaction(OffHeapMemoryStats stats, long previousNumCompactions) {
    long compactions = stats.getCompactions();
    long compactionTime = stats.getCompactionTime();
    Log.getLogWriter().info("Compactions (from stats): " + compactions);
    Log.getLogWriter().info("Compaction time (from stats): " + compactionTime);
    if (compactions <= previousNumCompactions) {
      throw new TestException("Expected compactions stat to be " + previousNumCompactions + ", but it is " + compactions);
    }
    if (compactionTime <= 0) {
      throw new TestException("Expected compactionTime stat to be > 0, but it is " + compactionTime);
    }
  }

  /** Given a Throwable, accept it if it involves an out of off-heap memory, otherwise throw it.
   * 
   * @param e The Throwable to check.
   * @throws Throwable 
   */
  public static void handleException(Throwable t) throws Throwable {
    Log.getLogWriter().info("In handleException");
    if (t instanceof OutOfOffHeapMemoryException) {
      Log.getLogWriter().info("Got expected " + t.getClass().getName());
    } else if (t instanceof CancelException) {
      StringBuilder causeChainStr = new StringBuilder();
      List<Throwable> causeChain = new ArrayList<Throwable>();
      List<String> causeMsg = new ArrayList<String>();
      causeChain.add(t);
      causeMsg.add(t.getMessage());
      causeChainStr.append(t.getClass().getName());
      Throwable cause = t.getCause();
      while (cause != null) {
        causeChain.add(cause);
        causeMsg.add(cause.getMessage());
        causeChainStr.append(" caused by " + cause.getClass().getName());
        cause = cause.getCause();
      }
      Throwable lastCause = causeChain.get(causeChain.size() - 1);
      String lastCauseMsg = causeMsg.get(causeMsg.size() - 1);
      if (lastCause instanceof OutOfOffHeapMemoryException) {
        // accept this
        Log.getLogWriter().info("Got expected " + OutOfOffHeapMemoryException.class.getName() + " with this exeption chain: " +
            causeChainStr);
      }
    } else {
      throw t;
    }
  }

  private static void makeRoom(int percent) {
    Log.getLogWriter().info("Destroying " + percent + " percent of entries in each region");
    Set<Region<?, ?>> regionSet = getAllRegions();
    for (Region aRegion: regionSet) {
      int numToDestroy = (int) (aRegion.size() * (percent * 0.01));
      Log.getLogWriter().info("Destroying " + numToDestroy + " in " + aRegion.getFullPath() + " of size " + aRegion.size());
      for (Object key: aRegion.keySet()) {
        aRegion.destroy(key);
      }
    }
  }

  /** Return a Set of all regions (and any subregions) existing in this member
   * 
   * @return Set of all regions (and any subregions) existing in this member
   */
  private static Set<Region<?, ?>> getAllRegions() {
    Set<Region<?, ?>> regionSet = new HashSet(CacheHelper.getCache().rootRegions());
    Set<Region<?, ?>> rootRegions = new HashSet(regionSet);
    for (Region aRegion: rootRegions) {
      regionSet.addAll(aRegion.subregions(true));
    }
    return regionSet;
  }

  /** Return a new key, never before used in this test.
   * 
   * @return The new key.
   */
  private static Object getNextKey() {
    Object key = NameFactory.getNextPositiveObjectName();
    return key;
  }

  /** Return user data (to be put into regions)
   * 
   * @return User data for a region.
   */
  private static Object getValue() {
    Object value = new byte[sizes[gsRand.nextInt(0, sizes.length-1)]];
    return value;
  }

  /** Verify partitioned regions
   * 
   *  @throws TestException if any problems were found.
   */
  private static void verifyPRs() {
    StringBuilder aStr = new StringBuilder();
    Set<Region<?, ?>> regionSet = getAllRegions();
    for (Region aRegion: regionSet) {
      if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
        try {
          ParRegUtil.verifyPRMetaData(aRegion);
        } catch (Exception e) {
          aStr.append(TestHelper.getStackTrace(e) + "\n");
        } catch (TestException e) {
          aStr.append(TestHelper.getStackTrace(e) + "\n");
        }

        // verify primaries
        try {
          ParRegUtil.verifyPrimaries(aRegion, -1);
        } catch (Exception e) {
          aStr.append(e.toString() + "\n");
        }

        // verify PR data
        try {
          ParRegUtil.verifyBucketCopies(aRegion, -1);
        } catch (Exception e) {
          aStr.append(TestHelper.getStackTrace(e) + "\n");
        } catch (TestException e) {
          aStr.append(e.toString() + "\n");
        }
      }
    }
    if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
    }
  }

  /** Return true if this member is an accessor (no regions store data) or
   *  false otherwise.
   * @return True if this member is an accessor, false otherwise.
   */
  private static boolean thisMemberIsAccessor() {
    return System.getProperty("clientName").contains("accessor");
  }

  /** Return true if this member is a data store or false otherwise.
   * @return True if this member is a data store, false otherwise.
   */
  private static boolean thisMemberIsDataStore() {
    return System.getProperty("clientName").contains("dataStore");
  }

  /** Return true if this member expects to get an OOM error.
   * @return True if this member expects OOM errors, false otherwise.
   */
  private static boolean thisMemberExpectsOOM() {
    return System.getProperty("clientName").contains("oomDataStore");
  }
}

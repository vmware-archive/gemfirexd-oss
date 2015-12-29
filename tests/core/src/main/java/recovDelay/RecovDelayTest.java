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
import hydra.*;
import util.*;
import parReg.*;
import rebalance.*;
import recovDelay.*;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

/** Test class for recovery delay in partitioned regions.
 */
public class RecovDelayTest {
    
/* The singleton instance of RecovDelayTest in this VM */
static public RecovDelayTest testInstance;
    
// key for blackboard shared map
protected static final String vmIDStr = "VmId_";
protected static final String redundantCopiesKey = "RedundantCopies";

// test startup strategies
protected static final String START_WITH_ALL = "startWithAll";
protected static final String SERIAL_STARTUP = "serialStartup";
protected static final String CONCURRENT_STARTUP = "concurrentStartup";

// instance fields
protected Region aRegion;              // the PR for this vm

// fields to record hydra params
protected String startStrategy = null;
protected String stopStrategy = null;
protected int recoveryDelay = 0;
protected int startupRecoveryDelay = 0;
protected int redundantCopies = 0;
protected int totalBucketCopies = 0;

// instance fields 
protected int numDataStoreVMs = 0;               // the number of data stores in this test
protected int totalNumVMsToStop = 0;             // the number of data stores to stop in the entire test
protected List stoppedVMs = null;                // the current List of ClientVmInfo instances of all vms 
                                                 //    stopped thus far
protected List vmsToStart = null;                // the current List of ClientVmInfo instances of all 
                                                 //    vms remaining to start
protected List liveVMs = null;                   // the current vmIds (Integer) of vms available to stop
protected ParRegTest parRegTestInstance = null;  // ParRegTest instance used for calling validation
protected List masterBucketInfoList = null;
protected Map masterPrMap = null;
protected Set masterBucketIdSet = null;
protected boolean uniqueHostTest = false;        // true if this is a unique host test, false otherwise
protected Map hostMap = null;                    // map with Integer keys (vmIds), and hostName values (Strings)
protected boolean startupRecoveryRace = false;   // if true then the test hit a scenario during startup where
                                                 // > 1 vms started concurrently such that it was a complete
                                                 // race as to which vms became members, recognized each other
                                                 // and recovered buckets; in this case we cannot guarantee
                                                 // primary balance

// delay settings
protected final int NEVER = -1;
protected final int IMMEDIATE = 0;

// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of RecovDelayTest 
 *  in this VM and the vm's data store region.
 */
public synchronized static void HydraTask_initializeDataStore() {
   if (testInstance == null) {
      testInstance = new RecovDelayTest();
      testInstance.initializeRegion("dataStoreRegion");
      testInstance.initializeInstance();
      RecovDelayBB.getBB().getSharedMap().put(vmIDStr + RemoteTestModule.getMyVmid(),
                                              RemoteTestModule.getMyHost());
   }
}
    
/** Creates and initializes the singleton instance of RecovDelayTest 
 *  in this VM and the vm's accessor region.
 */
public synchronized static void HydraTask_initializeAccessor() {
   if (testInstance == null) {
      testInstance = new RecovDelayTest();
      testInstance.initializeRegion("accessorRegion");
      testInstance.initializeInstance();
      PRObserver.initialize();
   }
}
    
/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 */
public void initializeRegion(String regDescriptName) {
   CacheHelper.createCache("cache1");
   PRObserver.installObserverHook();

   // choose redundantCopies, either 0, or a randomly choosen value between 1 and 3
   AttributesFactory factory = RegionHelper.getAttributesFactory(regDescriptName);
   RegionAttributes attr = RegionHelper.getRegionAttributes(regDescriptName);
   PartitionAttributes prAttr = attr.getPartitionAttributes();
   PartitionAttributesFactory prFactory = new PartitionAttributesFactory(prAttr);
   int redundantCopies = ((Integer)(RecovDelayBB.getBB().getSharedMap().get(redundantCopiesKey))).intValue();
   prFactory.setRedundantCopies(redundantCopies);
   factory.setPartitionAttributes(prFactory.create());
   String regionName = TestConfig.tab().stringAt(RegionPrms.regionName);
   aRegion = RegionHelper.createRegion(regionName, factory);
}
    
/**
 *  Initialize this test instance
 */
public void initializeInstance() {
   startStrategy = TestConfig.tab().stringAt(RecovDelayPrms.startStrategy);
   stopStrategy = TestConfig.tab().stringAt(RecovDelayPrms.stopStrategy);
   if (!(startStrategy.equalsIgnoreCase("group") || startStrategy.equalsIgnoreCase("single"))) { 
      throw new TestException("Unknown startStrategy " + startStrategy);
   }
   if (!(stopStrategy.equalsIgnoreCase("group") || stopStrategy.equalsIgnoreCase("single"))) { 
      throw new TestException("Unknown stopStrategy " + stopStrategy);
   }
   recoveryDelay = Integer.valueOf((String)(TestConfig.tab().vecAt(hydra.PartitionPrms.recoveryDelay)).get(1));
   startupRecoveryDelay = Integer.valueOf((String)(TestConfig.tab().vecAt(hydra.PartitionPrms.startupRecoveryDelay)).get(1));
   redundantCopies = aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
   totalBucketCopies = redundantCopies + 1;
   numDataStoreVMs = TestConfig.tab().intAt(RecovDelayPrms.numDataStoreVMs);
   totalNumVMsToStop = TestConfig.tab().intAt(RecovDelayPrms.totalNumVMsToStop);
   parRegTestInstance = new ParRegTest();
   parRegTestInstance.aRegion = aRegion;
   parRegTestInstance.highAvailability = false;
   parRegTestInstance.hasPRCacheLoader = false;
   parRegTestInstance.redundantCopies = redundantCopies;
   uniqueHostTest = DistributedSystemHelper.getGemFireDescription().getEnforceUniqueHost();
   Log.getLogWriter().info("startStrategy: " + startStrategy + "\n" +
                           "stopStrategy: " + stopStrategy + "\n" +
                           "recoveryDelay: " + recoveryDelay + "\n" +
                           "startupRecoveryDelay: " + startupRecoveryDelay + "\n" +
                           "totalNumVMsToStop: " + totalNumVMsToStop + "\n" +
                           "redundantCopies: " + redundantCopies + "\n" +
                           "totalBucketCopies: " + totalBucketCopies + "\n" +
                           "numDataStoreVMs: " + numDataStoreVMs + "\n" +
                           "uniqueHostTest: " + uniqueHostTest);
}

/** Do initialization after the load
 */
protected void initAfterLoad() {
   stoppedVMs = new ArrayList();
   liveVMs = new ArrayList(ClientVmMgr.getOtherClientVmids());
   masterBucketInfoList = BucketInfo.getAllBuckets(aRegion);
   masterPrMap = PrState.getPrMap(masterBucketInfoList);
   masterBucketIdSet = getBucketIds(masterBucketInfoList);
   Log.getLogWriter().info(PrState.prMapToString(masterPrMap));

   // save the original region snapshot 
   Map regionSnapshot = new HashMap();
   Iterator it = aRegion.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      Object value = aRegion.get(key);
      regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
   }
   ParRegBB.getBB().getSharedMap().put(ParRegBB.RegionSnapshot, regionSnapshot);
   ParRegBB.getBB().getSharedMap().put(ParRegBB.DestroyedKeys, new HashSet());

   // init the hostMap
   Map sharedMap = RecovDelayBB.getBB().getSharedMap().getMap();
   it = sharedMap.keySet().iterator();
   hostMap = new HashMap();
   while (it.hasNext()) {
      Object key = it.next();
      if ((key instanceof String) && ((String)(key)).startsWith(vmIDStr)) {
         hostMap.put(key, sharedMap.get(key));
      }         
   }
   Log.getLogWriter().info("hostMap is " + hostMap);
}

// ========================================================================
// task methods
    
/** Loads the region with keys/values.
 */
public static void HydraTask_load() {
   testInstance.load();
}

/** Checks for rough balance of buckets, keys and primaries.
 */
public static void HydraTask_checkForBalance() {
   testInstance.checkForBalance(testInstance.masterPrMap);
}

/** Task to control the delay recover test.
 *  There should only be one thread at a time calling this method.
 */
public static void HydraTask_controller() {
   testInstance.controller();
}

/** Task to control the delay recover test.
 */
public static void HydraTask_initAfterLoad() {
   testInstance.initAfterLoad();
}

/** Write the number of redundant copies for the pr to the blackboard 
 *  by using the hydra param RecovDelayPrms.redundantCopies, which can be:
 *     "zero" - Write 0 to the blackboard.
 *     "nonZero" - Randome choose 1, 2 or 3 and write to the blackboard.
 */
public static void HydraTask_initRedundantCopies() {
   String copies = TestConfig.tab().stringAt(RecovDelayPrms.redundantCopies);
   if (copies.equalsIgnoreCase("zero")) {
      RecovDelayBB.getBB().getSharedMap().put(redundantCopiesKey, new Integer(0));
   } else if (copies.equalsIgnoreCase("nonZero")) {
      int value = TestConfig.tab().getRandGen().nextInt(1, 3);
      RecovDelayBB.getBB().getSharedMap().put(redundantCopiesKey, new Integer(value));
   } else {
      try {
         int value = Integer.valueOf(copies);
         RecovDelayBB.getBB().getSharedMap().put(redundantCopiesKey, new Integer(value));
      } catch (NumberFormatException e) {
         throw new TestException("Unknown RecovDelayPrms.redundantCopies setting: " + copies);
      }
   }
}

/** If this vm has a startup delay, wait for it.
 */
public static void HydraTask_waitForStartupRecovery() {
   if ((testInstance.startupRecoveryDelay >= 0) && (testInstance.redundantCopies != 0)) {
      List startupVMs = new ArrayList(StopStartBB.getBB().getSharedMap().getMap().values());
      List vmsExpectingRecovery = StopStartVMs.getMatchVMs(startupVMs, "dataStore");
      PRObserver.waitForRebalRecov(vmsExpectingRecovery, 1, 1, null, null, false);
   }
}

// ========================================================================

/** Load the region with RecovDelayPrms.initialNumKeys.
 *  This is batched and must be called multiple times until
 *   a stop scheduling exception is thrown.
 */
protected void load() {
   final long LOG_INTERVAL_MILLIS = 10000;
   int initialNumKeys = TestConfig.tab().intAt(RecovDelayPrms.initialNumKeys);
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
   long minTaskGranularityMS = -1;
   if (minTaskGranularitySec != -1)
      minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   do {
      long loadCounter = RecovDelayBB.getBB().getSharedCounters().incrementAndRead(RecovDelayBB.loadCounter);
      if (loadCounter > initialNumKeys) {
         String aStr = "In loadRegion, loadCounter is " + loadCounter +
                       ", initialNumKeys is " + initialNumKeys + ", region size is " + aRegion.size();
         Log.getLogWriter().info(aStr);
         NameBB.getBB().printSharedCounters();
         throw new StopSchedulingTaskOnClientOrder(aStr);
      }
      Object key = NameFactory.getNextPositiveObjectName();
      Object value = new ValueHolder((String)key, null);
      aRegion.put(key, value);
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Added " + NameFactory.getPositiveNameCounter() + " out of " + initialNumKeys + 
             " entries into " + TestHelper.regionToString(aRegion, false));
         lastLogTime = System.currentTimeMillis();
      }
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

/** Main controller for recover delay tests.
 *  Stop the vms with validation, then restart them with validation.
 */  
protected void controller() {
   PRObserver.initialize();
   if (stoppedVMs.size() < totalNumVMsToStop) { // stopping
      controllerStopVMs();
      if (stoppedVMs.size() > totalNumVMsToStop) { // test consistency check
         throw new TestException("Test problem; stopped too many vms: " + stoppedVMs.size());
      }
   } else { // starting
      if (vmsToStart == null) {
         vmsToStart = new ArrayList(stoppedVMs);
      }
      controllerStartVMs();
      if (vmsToStart.size() == 0) {
         throw new StopSchedulingTaskOnClientOrder(stoppedVMs.size() + " vms have been stopped and restarted");
      }
   }
}

/** Stop vms, either one at a time or concurrently, with appropriate validation.
 *  This method gets called repeatedly to do its job, so its task can be 
 *  batched. It will stop one or more vms per call until totalNumVMsToStop has been reached.
 *
 */
protected void controllerStopVMs() {
   Log.getLogWriter().info("In controllerStopVMs, stopStrategy is " + stopStrategy + 
       ", recoveryDelay is " + recoveryDelay);
   Set currentHostSet = new HashSet();
   for (int i = 0; i < liveVMs.size(); i++) {
      currentHostSet.add(RecovDelayBB.getBB().getSharedMap().get(vmIDStr + liveVMs.get(i)));
   }

   // determine the number of vms to stop
   int numToStopThisTime = 1;
   if (stopStrategy.equalsIgnoreCase("group")) {
      // For group stopping, we stop redundantCopies vms at a time (or fewer if there
      // aren't enough vms left to stop that many), or we stop a random number if redundant
      // copies is 0
      if (redundantCopies == 0) {
          numToStopThisTime = TestConfig.tab().getRandGen().nextInt(1, liveVMs.size()-1);
      } else if (uniqueHostTest && 
                 (recoveryDelay >= 0) &&
                 (currentHostSet.size() <= redundantCopies)) {
          numToStopThisTime = Math.max(currentHostSet.size() - 1, 1);
      } else {
          numToStopThisTime = Math.min(redundantCopies, liveVMs.size()-1);
      }
   }

   // stop them all concurrently or stop them serially
   List vmsStoppedThisTime = null;
   if (TestConfig.tab().getRandGen().nextBoolean()) { // stop concurrently
      Log.getLogWriter().info("Stopping group vms concurrently");
      vmsStoppedThisTime = stopVMs(liveVMs, numToStopThisTime);
   } else {
      Log.getLogWriter().info("Stopping group vms serially");
      vmsStoppedThisTime = new ArrayList();
      for (int i = 1; i <= numToStopThisTime; i++) {
         List aList = stopVMs(liveVMs, 1);  
         vmsStoppedThisTime.addAll(aList);
      }
   }
   stoppedVMs.addAll(vmsStoppedThisTime);

   // validate whether recovery ran or not
   if (recoveryDelay == NEVER) {
      waitForNever();
      validateNoRecovery();
   } else if (recoveryDelay == IMMEDIATE) { 
      if (redundantCopies == 0) { // no recovery will happen
         waitForNever();
         validateNoRecovery();
      } else {
         validateRecoveryNow(liveVMs, numToStopThisTime, false);
      }
   } else if (recoveryDelay > 0) {
      if (redundantCopies == 0) { // no recovery will happen
         waitForNever();
         validateNoRecovery();
      } else {
         validateRecoveryWithDelay(recoveryDelay, liveVMs, numToStopThisTime, false);
      }
   } else {
      throw new TestException("unknown recoveryDelay " + recoveryDelay);
   }
   Log.getLogWriter().info("After stopping, PR picture is: " + PrState.prMapToString(PrState.getPrMap(aRegion)));
   validatePRAfterStopping(numToStopThisTime);
}

/** Start vms, either one at a time or concurrently, with appropriate validation.
 *  This method gets called repeatedly to do its job, so its task can be 
 *  batched. It will start one or more vms per call until all totalNumVMsToStop 
 *  stopped vms have been restarted.
 *
 */
protected void controllerStartVMs() {
   Log.getLogWriter().info("In controllerStartVMs, startStrategy is " + startStrategy + 
       ", startupRecoveryDelay is " + startupRecoveryDelay);
   int numStarted = 0;
   List startedVMs = null;
   if (startStrategy.equalsIgnoreCase("group")) { // Start a group of vms
      int numToStart = TestConfig.tab().getRandGen().nextInt(1, vmsToStart.size());
      startedVMs = new ArrayList();
      for (int i = 1; i <= numToStart; i++) {
         int index = TestConfig.tab().getRandGen().nextInt(0, vmsToStart.size()-1);
         startedVMs.add(vmsToStart.get(index));
         vmsToStart.remove(index);
      }
   } else { // serial; just start 1
      startedVMs = new ArrayList();
      startedVMs.add(vmsToStart.get(0));
      vmsToStart.remove(0);
   }
   StopStartVMs.startVMs(startedVMs);

   // update liveVMs
   List previousLiveVMs = new ArrayList();
   previousLiveVMs.addAll(liveVMs);
   for (int i = 0; i < startedVMs.size(); i++) {
      liveVMs.add(((ClientVmInfo)(startedVMs.get(i))).getVmid());
   }
   Log.getLogWriter().info("liveVMs: " + liveVMs);

   if (startupRecoveryDelay == NEVER) {
      waitForNever();
      validateNoRecovery();
   } else if (startupRecoveryDelay == IMMEDIATE) { 
      if (redundantCopies == 0) { // no recovery will happen
         waitForNever();
         validateNoRecovery();
      } else {
         validateRecoveryNow(startedVMs, 1, true);
      }
   } else if (startupRecoveryDelay > 0) {
      if (redundantCopies == 0) { // no recovery will happen
         waitForNever();
         validateNoRecovery();
      } else {
         validateRecoveryWithDelay(startupRecoveryDelay, startedVMs, 1, true);
      }
   } else {
      throw new TestException("unknown startupRecoveryDelay " + startupRecoveryDelay);
   }
   Log.getLogWriter().info("After starting, PR picture is: " + PrState.prMapToString(PrState.getPrMap(aRegion)));
   validatePRAfterStarting(previousLiveVMs, startedVMs);
}

//================================================================================
// validation methods

/** Do various checks on the PR after stopping:
 *     - make sure each bucket has a primary
 *     - verify consistency of PR meta data
 *     - verify data
 *     - verify bucket copy consistency 
 *
 *  @param numStoppedThisTime The number of vms stopped the last time vms were stopped.
 */
protected void validatePRAfterStopping(int numStoppedThisTime) {
   Set currentHostSet = new HashSet();
   for (int i = 0; i < liveVMs.size(); i++) {
      currentHostSet.add(RecovDelayBB.getBB().getSharedMap().get(vmIDStr + liveVMs.get(i)));
   }
   int currentNumDataStoreVMs = numDataStoreVMs - stoppedVMs.size();
   boolean expectRecovery = (recoveryDelay != NEVER) && (redundantCopies > 0) &&
           (uniqueHostTest && (currentHostSet.size() > 1));
   Log.getLogWriter().info("Validating PR after stopping,\n" +
       "current number of data store vms: " + currentNumDataStoreVMs + "\n" +
       "redundantCopies: " + redundantCopies + "\n" +
       "num stopped this time: " + numStoppedThisTime + "\n" +
       "total num stopped vms: " + stoppedVMs.size() + "\n" +
       "expect recovery: " + expectRecovery);

   Map regionSnapshot = (Map)(ParRegBB.getBB().getSharedMap().get(ParRegBB.RegionSnapshot));
   int expectedRedundantCopies = 0;
   if (expectRecovery) {
      expectedRedundantCopies = Math.min(currentNumDataStoreVMs-1, redundantCopies);
      if (uniqueHostTest) {
         expectedRedundantCopies = Math.min(currentHostSet.size()-1, redundantCopies);
      }
      if ((numStoppedThisTime > redundantCopies) ||
          (uniqueHostTest && (expectedRedundantCopies < redundantCopies))) { // possible data loss
         adjustRegionSnapshotForLostKeys(regionSnapshot);
      } // else don't change region snapshot; we should have all data previously in the snapshot
   } else { // no recovery
      if ((stoppedVMs.size() > redundantCopies) || // total number of stopped vms is more than redundantCopies
          (uniqueHostTest && currentHostSet.size() == 1)) {  // no vms to recover redundant buckets
         adjustRegionSnapshotForLostKeys(regionSnapshot);
      } // else don't change region snapshot; we should have all data previously in the snapshot
      expectedRedundantCopies = -1;
   }
   verifyBucketsInVMs(stoppedVMs, liveVMs); // the vms not yet stopped should all contain buckets
   // can't check for primary balance; we don't ever rebalance primaries when vms are stopped, 
   // even when recovery runs

   Log.getLogWriter().info("Num expected bucket copies (not including primary): " + expectedRedundantCopies);
   ParRegBB.getBB().getSharedMap().put(ParRegBB.RegionSnapshot, regionSnapshot);
   // need to set the follow for the verifyFromSnapshot to verify correctly for us
   parRegTestInstance.redundantCopies = expectedRedundantCopies; 
   parRegTestInstance.verifyFromSnapshot();
   if (uniqueHostTest) {
      ParRegUtil.verifyBucketsOnUniqueHosts(aRegion);
   }
}
   
/** Do various checks on the PR after starting:
 *     - check for balance if we recovered
 *     - make sure each bucket has a primary
 *     - verify consistency of PR meta data
 *     - verify data
 *     - verify bucket copy consistency 
 *
 * @param previousLiveVMs A List of ClientVmInfo instances of the vms
 *        that were alive prior to starting vmsStartedThisTime.
 * @param vmsStartedThisTime A List of ClientVmInfo instances of the vms
 *        started this time.
 */
protected void validatePRAfterStarting(List previousLiveVMs, List vmsStartedThisTime) {
   Set currentHostSet = new HashSet();
   for (int i = 0; i < liveVMs.size(); i++) {
      currentHostSet.add(RecovDelayBB.getBB().getSharedMap().get(vmIDStr + liveVMs.get(i)));
   }
   Set previousHostSet = new HashSet();
   for (int i = 0; i < previousLiveVMs.size(); i++) {
      Integer vmID = (Integer)(previousLiveVMs.get(i));
      previousHostSet.add(RecovDelayBB.getBB().getSharedMap().get(vmIDStr + vmID));
   }
   Set newHosts = new HashSet(currentHostSet);
   newHosts.removeAll(previousHostSet);
   int currentNumDataStoreVMs = numDataStoreVMs - vmsToStart.size();
   boolean expectRecovery = (startupRecoveryDelay != NEVER) && (redundantCopies > 0);
   boolean redundancyIncreased = ((currentNumDataStoreVMs - vmsStartedThisTime.size()) < totalBucketCopies);
   boolean capacityAdded = currentNumDataStoreVMs > totalBucketCopies;
   if (uniqueHostTest) { 
      // we only increased redundancy if we were below redundancy before AND 
      // we increased the number of hosts with vms
      redundancyIncreased = redundancyIncreased && (newHosts.size() > 0);
      if (redundancyIncreased) {
         capacityAdded = (vmsStartedThisTime.size() > 1);
      } else {
         capacityAdded = true;
      }
   }
   Log.getLogWriter().info("Validating PR after starting,\n" +
       "current number of data store vms: " + currentNumDataStoreVMs + "\n" +
       "redundantCopies: " + redundantCopies + "\n" +
       "num started this time: " + vmsStartedThisTime.size() + "\n" +
       "capacity added: " + capacityAdded + "\n" +
       "redundancy increased: " + redundancyIncreased + "\n" +
       "expect recovery: " + expectRecovery + "\n" +
       "liveVMs: " + liveVMs + "\n" +
       "current hosts with vms: " + currentHostSet + "\n" +
       "previous hosts: " + previousHostSet + "\n" +
       "new hosts: " + newHosts);

   // verify the state of new vms
   if (uniqueHostTest) {
      ParRegUtil.verifyBucketsOnUniqueHosts(aRegion);
   }
   if (capacityAdded) {
      if (redundancyIncreased) { 
         // both added capacity and increased redundancy
         // This is a concurrent start of more than 1 vm
         if (vmsStartedThisTime.size() == 1) {
            throw new TestException("Test problem; only 1 vm was started, but expected > 1");
         }
         if (expectRecovery) { 
            if (startupRecoveryDelay == IMMEDIATE) {
               // > 1 vms started up concurrently, and recovery is immediate; this is a total
               // race as to which vms recover first before others have joined or created the PR;
               // there is no guarantee of primary balance, so we can't validate balance here
               startupRecoveryRace = true;
            } else { // DELAY; we expect recovery, so this is a delay
               // all new vms know about each other when recovery runs and we should be balanced
               // however we can't tell which vms took on buckets and which didn't since we
               // are starting concurrently and we both increased redundancy and added capacity;
               // once redundancy is satisfied, vms that add capacity sit empty bdcause vms only 
               // acquire buckets to satisfy redundancy, buckets are never moved
               BucketState.checkPrimaryBalance(aRegion);
            }
         } else { // no recovery expected; each new vm should sit empty with no buckets
            if (!uniqueHostTest) {
               verifyBucketsInVMs(vmsStartedThisTime /*empty*/, null);
               if (!startupRecoveryRace) {
                  BucketState.checkPrimaryBalance(aRegion);
               }
            }
         }
      } else { 
         if (!uniqueHostTest) {
            // added capacity only; already had full redundancy, so nothing would change 
            // by adding new vms; the new vms sit empty 
            verifyBucketsInVMs(vmsStartedThisTime, null);
            if (!startupRecoveryRace) {
               BucketState.checkPrimaryBalance(aRegion);
            }
         }
      }
   } else { // this did not add capacity
      if (redundancyIncreased) { // increased redundancy only   
         if (expectRecovery) {
            if (!startupRecoveryRace) {
               BucketState.checkPrimaryBalance(aRegion);
            }
            if (!uniqueHostTest) {
               verifyBucketsInVMs(null, liveVMs);
            }
         } else {
            if (!uniqueHostTest) {
               verifyBucketsInVMs(vmsStartedThisTime, null);
               if (!startupRecoveryRace) {
                  BucketState.checkPrimaryBalance(aRegion);
               }
            }
         }
      } else { // neither added capacity or increased redundancy
         throw new TestException("Test error, capacityAdded " + capacityAdded +
                                 ", redundancyIncreased " + redundancyIncreased);  
      }
   }

   // determine number of bucket copies for pr validation
   int expectedRedundantCopies = 0;
   if (expectRecovery) {
      if (uniqueHostTest) {
         expectedRedundantCopies = Math.min(currentHostSet.size()-1, redundantCopies);
      } else {
         expectedRedundantCopies = Math.min(currentNumDataStoreVMs-1, redundantCopies);
      }
   } else { // no recovery
      expectedRedundantCopies = -1;
   }
   Log.getLogWriter().info("Num expected bucket copies (not including primary): " + expectedRedundantCopies);
   // need to set the following for the verifyFromSnapshot to verify correctly 
   parRegTestInstance.redundantCopies = expectedRedundantCopies; 
   parRegTestInstance.verifyFromSnapshot();
}
   
/** Verify that no recovery has occurred.
 *
 */
protected void validateNoRecovery() {
   Log.getLogWriter().info("In validateNoRecovery");
   long rebalRecovStartCounter = PRObserverBB.getBB().getSharedCounters().read(PRObserverBB.rebalRecovStartCounter);
   if (rebalRecovStartCounter != 0) {
      throw new TestException("Expected no recovery to occur, but rebalRecovStartCounter is " + rebalRecovStartCounter);
   }
   Log.getLogWriter().info("In validateNoRecovery, no recovery occurred");
}

/** Validate that recovery occurs now. Returns when recovery is finished.
 *
 *  @param vmsExpectingRecovery A List of either Integers (vmIds) or 
 *         ClientVmInfo instances containing vmIds of vms expected to recover.
 *  @param numRecovPerVM The number of recovery activities expected per 
 *         vm in vmsExpectingRecovery.
 *  @param isStartupRecovery true if the expected recovery is from startup,
 *         false otherwise. 
 */
protected void validateRecoveryNow(List vmsExpectingRecovery, int numRecovPerVM, boolean isStartupRecovery) {
   final int WAIT_MILLIS = 20000;
   final int SLEEP_MILLIS = 500;
   boolean started = PRObserver.waitForAnyRebalRecovToStart(WAIT_MILLIS, SLEEP_MILLIS);
   if (!started) {
      throw new TestException("Expected recovery to begin, but after waiting " +
               WAIT_MILLIS + " ms, recovery has not started");
   }
   Log.getLogWriter().info("In validateRecoveryNow, recovery has begun");
   if (isStartupRecovery) {
      PRObserver.waitForRebalRecov(vmsExpectingRecovery, 1, 1, null, null, false);
   } else {
      PRObserver.waitForRebalRecov(vmsExpectingRecovery, numRecovPerVM, 1, null, null, true);
   }
}

/** Validate that recovery occurs after the given delay. Returns when recovery is finished.
 *
 *  @param delayMillis The delay value in milliseconds.
 *  @param vmsExpectingRecovery A List of either Integers (vmIds) or 
 *         ClientVmInfo instances containing vmIds of vms expected to recover.
 *  @param numRecovPerVM The number of recovery activities expected per 
 *         vm in vmsExpectingRecovery.
 *  @param isStartupRecovery true if the expected recovery is from startup,
 *         false otherwise. 
 */
protected void validateRecoveryWithDelay(int delayMillis, List vmsExpectingRecovery, int numRecovPerVM, boolean isStartupRecovery) {
   long waitTimeMillis = delayMillis + 120000; // give a buffer on the wait time
   boolean started = PRObserver.waitForAnyRebalRecovToStart(waitTimeMillis, 1000); // poll for the recovery to run
   if (!started) { // this should never be false; either we return true or hydra times out
      throw new TestException("In validateRecoveryWithDelay, after waiting " + waitTimeMillis + 
          " ms recovery did not start,  recovery delay: " + delayMillis + " ms");
   }

   // approxTimerStartTime is when a region was created (for startup) or when
   // a member departed (for vms that leave); these events are when the timer
   // starts
   long approxTimerStartTime = PRObserverBB.getBB().getSharedCounters().read(PRObserverBB.approxTimerStartTime);
   long expectedRecovStartTime = approxTimerStartTime + delayMillis;

   // actual recovery start time
   long recovStartTime = PRObserverBB.getBB().getSharedCounters().read(PRObserverBB.rebalRecovStartTime);

   // check if the recovery started close to the expected time
   long diff = Math.abs(recovStartTime - expectedRecovStartTime); 
   long threshold = 15000;
   String aStr = "In validateRecoveryWithDelay, delay ms: " + delayMillis + 
       ", approxTimerStartTime: " + approxTimerStartTime +
       ", expected recovery start time: " + expectedRecovStartTime +
       ", actual earliest recovery start time: " + recovStartTime +
       ", difference between actual earliest recovery start time and expected recovery start time (ms): " + diff;
   Log.getLogWriter().info(aStr);
   if (diff > threshold) {
      throw new TestException("Recovery did not start within " + threshold + " ms of expected start time, " + aStr);
   }
   if (isStartupRecovery) {
      PRObserver.waitForRebalRecov(vmsExpectingRecovery, 1, 1, null, null, false);
   } else {
      PRObserver.waitForRebalRecov(vmsExpectingRecovery, numRecovPerVM, 1, null, null, true);
   }
}

/** Check for balance:
 *  1) The number of buckets per vm is roughly equal.
 *  2) The number of primaries per vm is roughly equal.
 *  3) The number of entries per bucket is roughly equal.
 *  Throw an exception if any are out of balance.
 *  This is used for an initial balance check after loading. It allows
 *  for a variance, rather than exact balance.
 *  NOTE: This checks for balance for vms that contain buckets. VMs that
 *  don't contain buckets are not considered.
 *
 *  @param prMap The return from PrState.getPrMap(Region); this
 *         the the PR picture to check.
 */
protected void checkForBalance(Map prMap) {
   Log.getLogWriter().info("Checking PR balance...");
   Iterator it = prMap.keySet().iterator();
   int numMembers = prMap.size();
   
   // retrieve information from the pr state map and save it
   Map balance_numBuckets = new TreeMap();   // key is vmId (Integer), value is number of 
                                             // buckets in this vm (Integer)
   Map balance_numPrimaries = new TreeMap(); // key is vmId (Integer), value is number of 
                                             // primaries in this vm (Integer)
   Map balance_numEntries = new TreeMap();   // key is vmId/bucketId combination (String), 
                                             // value is number of entries in the bucket
   int totalNumBuckets= 0; // the total number of buckets, including redundant buckets
   int totalNumEntries = 0; // the total number of entries, including redundant entries
   while (it.hasNext()) {
      Integer vmId = (Integer)(it.next());
      PrState state = (PrState)(prMap.get(vmId));
      List bucketList = state.getBucketInfoList(); 
      int numBuckets = bucketList.size();
      totalNumBuckets += numBuckets;

      // record number of buckets
      Integer anInteger = (Integer)(balance_numBuckets.get(vmId));
      if (anInteger == null) {
         anInteger = new Integer(numBuckets);
      } else {
         anInteger = new Integer(anInteger.intValue()+numBuckets);
      }
      balance_numBuckets.put(vmId, anInteger);

      for (int i = 0; i < bucketList.size(); i++) {
         BucketInfo info = (BucketInfo)(bucketList.get(i));
         boolean isPrimary = info.getIsPrimary();

         // record if isPrimary
         if (isPrimary) {
            anInteger = (Integer)(balance_numPrimaries.get(vmId));
            if (anInteger == null) {
               anInteger = new Integer(1);
            } else {
               anInteger = new Integer(anInteger.intValue()+1);
            }
            balance_numPrimaries.put(vmId, anInteger);
         }

         // record number of entries in the bucket
         int bucketId = info.getBucketId();
         int numEntries = info.getEntriesMap().size();
         String key = "vmId " + vmId + ", " + "bucketId" + bucketId;
         anInteger = (Integer)(balance_numEntries.get(key));
         if (anInteger == null) {
            anInteger = new Integer(numEntries);
         } else {
            throw new TestException("Test problem; key " + key + " already has an entry");
         }
         balance_numEntries.put(key, anInteger);
         totalNumEntries += numEntries;
      }
   }

   // now we have all the information; analyze it
   Log.getLogWriter().info("Primaries: " + balance_numPrimaries);
   Log.getLogWriter().info("Buckets: " + balance_numBuckets);
//   Log.getLogWriter().info("Entries: " + balance_numEntries);

   // number of primaries per vm
   StringBuffer aStr = new StringBuffer();
   it = balance_numPrimaries.keySet().iterator();
   int min = Integer.MAX_VALUE;
   int max = Integer.MIN_VALUE;
   int totalPrimaryBuckets = 0;
   while (it.hasNext()) {
      Integer vmId = (Integer)(it.next());
      int value = ((Integer)(balance_numPrimaries.get(vmId))).intValue();
      min = Math.min(min, value);
      max = Math.max(max, value);
      totalPrimaryBuckets += value;
   }
   double allowableSpan = Math.ceil(totalPrimaryBuckets * (double)0.10); 
   int span = Math.abs(max - min);
   if (span > allowableSpan) {
      aStr.append("Primaries are not balanced, least number of primaries in a vm: " + min +
           ", most number of primaries in a vm: " + max + "; " + balance_numPrimaries);
   }

   // number of buckets per vm
   it = balance_numBuckets.keySet().iterator();
   min = Integer.MAX_VALUE;
   max = Integer.MIN_VALUE;
   int totalBuckets = 0;
   while (it.hasNext()) {
      Integer vmId = (Integer)(it.next());
      int value = ((Integer)(balance_numBuckets.get(vmId))).intValue();
      min = Math.min(min, value);
      max = Math.max(max, value);
      totalBuckets += value;
   }
   allowableSpan = Math.ceil(totalBuckets * (double)0.10); 
   span = Math.abs(max - min);
   if (span > allowableSpan) {
      aStr.append("Buckets are not balanced, least number of buckets in a vm: " + min +
           ", most number of buckets in a vm: " + max + "; " + balance_numBuckets);
   }

   // number of entries per bucket
   it = balance_numEntries.keySet().iterator();
   min = Integer.MAX_VALUE;
   max = Integer.MIN_VALUE;
   int totalEntries = 0;
   while (it.hasNext()) {
      String vmIdBucketIdKey = (String)(it.next());
      int value = ((Integer)(balance_numEntries.get(vmIdBucketIdKey))).intValue();
      min = Math.min(min, value);
      max = Math.max(max, value);
      totalEntries += value;
   }
   allowableSpan = Math.ceil(totalEntries * (double)0.10); 
   span = Math.abs(max - min);
   if (span > allowableSpan) {
      aStr.append("Entries are not balanced, least number of entries in a bucket: " + min +
           ", most number of entries in a bucket: " + max + "; " + balance_numEntries);
   }

   if (aStr.length() > 0) {
      Log.getLogWriter().info(PrState.prMapToString(prMap));
      throw new TestException(aStr.toString());
   }      
   Log.getLogWriter().info("PR is balanced");
}

/** Wait a sufficient amount of time to satisfy the test that recovery never runs.
 *  This must be longer than the delays used in the test so we know it's really
 *  never. 
 */
protected void waitForNever() {
   int bufferMillis = 45000;
   int neverMillis = bufferMillis;
   if ((recoveryDelay > 0) || (startupRecoveryDelay > 0)) {
      neverMillis = Math.max(recoveryDelay, startupRecoveryDelay) + bufferMillis; 
   }
   Log.getLogWriter().info("Waiting for never (" + neverMillis + " millis)...");
   MasterController.sleepForMs(neverMillis);
   Log.getLogWriter().info("Done waiting for never (" + neverMillis + " millis)...");
}

/** Verify that the given vms contain no buckets.
 *
 *  @param emptyVMs A List of ClientVmInfo instances or vm ids (Integers).
 *                  This vms should contain no buckets. This arg can be null.
 *  @param nonEmptyVMs A List of ClientVmInfo instances or vm ids (Integers).
 *                  This vms should contain buckets. This arg can be null.
 */
protected void verifyBucketsInVMs(List emptyVMs, List nonEmptyVMs) {
   Map bucketMap = BucketState.getBucketMaps(aRegion)[1];

   // check the empty VMs
   if (emptyVMs != null) {
      for (int i = 0; i < emptyVMs.size(); i++) {
         Object vm = emptyVMs.get(i);
         Integer vmID = null;
         if (vm instanceof Integer) {
            vmID = (Integer)vm;
         } else {
            vmID = new Integer(((ClientVmInfo)vm).getVmid());
         }
         if (bucketMap.get(vmID) != null) {
            throw new TestException("Expected vm " + vm + " to contain no buckets, but it contains " + 
                      bucketMap.get(vmID));
         }
      }
   }

   // check the non-empty VMs
   if (nonEmptyVMs != null) {
      for (int i = 0; i < nonEmptyVMs.size(); i++) {
         Object vm = nonEmptyVMs.get(i);
         Integer vmID = null;
         if (vm instanceof Integer) {
            vmID = (Integer)vm;
         } else {
            vmID = new Integer(((ClientVmInfo)vm).getVmid());
         }
         if (bucketMap.get(vmID) == null) {
            throw new TestException("Expected vm " + vm + " to contain buckets, but it is empty");
         }
      }
   }
}

//================================================================================
// other methods

/** Using the list of available vms, stop numToStop vms and return a
 *  List of ClientVmInfos that were stopped.
 *  @param availableVMs A List of Integers, where the Integer is the vm id
 *         of a vm available for stopping.  This is modified upon return to 
 *         remove the vms that were stopped. 
 *  @param numToStop The desired number of vms to stop. Throws an exception of
 *         this is more than the number of vms available to stop.
 *  @return A List of ClientVmInfos that were stopped.
 */
protected List stopVMs(List availableVMs, int numToStop) {
   if (availableVMs.size() < numToStop) {
      throw new TestException("Test requested to stop " + numToStop +
                " vms, but there are only " + availableVMs.size() + " vms available");
   }
   if (numToStop <= 0) {
      throw new TestException("Cannot stop " + numToStop + " vms");
   }

   // put numToStop in vmList
   ArrayList vmList = new ArrayList();
   ArrayList stopModeList = new ArrayList();
   do {
      // add a VmId to the list of vms to stop
      int randInt = TestConfig.tab().getRandGen().nextInt(0, availableVMs.size()-1);
      Integer vmID = (Integer)(availableVMs.get(randInt));
      ClientVmInfo targetVm = new ClientVmInfo(vmID, null, null);
      vmList.add(targetVm);
      availableVMs.remove(randInt);

      // choose a stopMode
      String choice = TestConfig.tab().stringAt(StopStartPrms.stopModes);
      stopModeList.add(choice);
   } while (vmList.size() < numToStop);

   StopStartVMs.stopVMs(vmList, stopModeList);
   return vmList;
}

protected Set getBucketIds(List aBucketList) {
   Set aSet = new TreeSet();
   for (int bucketId = 0; bucketId < aBucketList.size(); bucketId++) {
      List aList = (List)(aBucketList.get(bucketId));
      if ((aList == null) || (aList.size() == 0)) {
         // no buckets exist for this bucketId
      } else {
         aSet.add(new Integer(bucketId));
      }
   }
   return aSet;
}

// return all buckets that where completely lost (primary and secondary) by stopping vms the last time
// compared to the state prior to stopping the last time
protected Set getLostBuckets() {
   List currBucketList = BucketInfo.getAllBuckets(aRegion);
   Set currBucketIds = getBucketIds(currBucketList);
   currBucketList = null; 
   Set lostBucketIds = new TreeSet(masterBucketIdSet);
   lostBucketIds.removeAll(currBucketIds);
   Log.getLogWriter().info("Lost buckets: " + lostBucketIds);
   return lostBucketIds;
}

protected void adjustRegionSnapshotForLostKeys(Map regionSnapshot) {
   // determine if we lost data
   Set lostBucketIds = getLostBuckets();
   Iterator it = lostBucketIds.iterator();
   while (it.hasNext()) {
      int bucketId = (Integer)(it.next());
      List bucketInfoList = (List)(masterBucketInfoList.get(bucketId));
      BucketInfo info = (BucketInfo)(bucketInfoList.get(0));
      Map entriesMap = info.getEntriesMap();
      Iterator it2 = entriesMap.keySet().iterator(); 
      while (it2.hasNext()) {
         Object key = it2.next();
         regionSnapshot.remove(key);
      }
   }
}

}

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

import hydra.*;
import util.*;
import java.util.*;

import newWan.WANTestPrms;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

import getInitialImage.*;
import parReg.colocation.Month;
import pdx.PdxTest;

public class KnownKeysTest extends InitImageTest {

protected static String DataStoreVmStr = "DataStoreVM_";
protected static boolean isBridgeConfiguration;
protected static boolean isGatewayConfiguration;
protected static boolean isClientCache;
protected static boolean isGatewaySenderConfiguration;

// Used for checking the size of the region in a batched task (after the load step)
// These variables are used to track the state of checking large regions
// for repeated batched calls. They must be reinitialized if a second check is
// called in this vm with a single test.
protected static Set missingKeys = new HashSet();
protected static Set missingValues = new HashSet();
protected static int verifyRegionSizeIndex = 0;
protected static boolean verifyRegionSizeCompleted = false;

// Used for checking the region contents in a batched task
// These variables are used to track the state of checking large regions
// for repeated batched calls. They must be reinitialized if a second check is
// called in this vm within a single test.
protected static int verifyRegionContentsIndex = 0;
protected static StringBuffer verifyRegionContentsErrStr = new StringBuffer();
protected static StringBuffer verifyRegionContentsSizeStr = new StringBuffer();
protected static boolean verifyRegionContentsCompleted = false;

// used for checking bucket copies in a batched test
protected static ParRegUtil parRegUtilInstance = new ParRegUtil();

// locking vars
protected boolean lockOperations;           // value of ParRegPrms.lockOperations
protected DistributedLockService distLockService; // the distributed lock service for this VM
protected static String LOCK_SERVICE_NAME = "MyLockService";
protected static String LOCK_NAME = "MyLock";

protected boolean highAvailability;         // value of ParRegPrms.highAvailability
protected static boolean diskFilesRecorded = false;

// ======================================================================== 
// initialization tasks/methods 

/** Initialize the known keys for this test
 */
public static void StartTask_initialize() {
   PRObserver.initialize();
   Vector bridgeNames = TestConfig.tab().vecAt(BridgePrms.names, null);
   boolean isBridgeConfiguration = bridgeNames != null;

   Vector gatewayNames = TestConfig.tab().vecAt(GatewayPrms.names, null);
   Vector senderNames = TestConfig.tab().vecAt(GatewaySenderPrms.names, null);
   boolean isWanConfiguration = (gatewayNames == null) ? (senderNames != null) : true;
    
   // initialize keyIntervals
   int numKeys = TestConfig.tab().intAt(InitImagePrms.numKeys);

   // lynn for now we cannot use localInvalidate or localDestroy; they are not supported for Congo
   // By design, invalidates are not propagated across WAN Sites (see LocalRegion.invokeInvalidateCallbacks()
   KeyIntervals intervals = null;
   if (isWanConfiguration || (ConfigPrms.getHadoopConfig() != null)) {
      intervals = new KeyIntervals(new int[] {KeyIntervals.NONE, 
                                             KeyIntervals.DESTROY, KeyIntervals.UPDATE_EXISTING_KEY,
                                             KeyIntervals.GET}, 
                                 numKeys);
   } else {
      intervals = new KeyIntervals(new int[] {KeyIntervals.NONE, KeyIntervals.INVALIDATE,
                                             KeyIntervals.DESTROY, KeyIntervals.UPDATE_EXISTING_KEY,
                                             KeyIntervals.GET}, 
                                 numKeys);
   }
   InitImageBB.getBB().getSharedMap().put(InitImageBB.KEY_INTERVALS, intervals);
   Log.getLogWriter().info("Created keyIntervals: " + intervals);

   // Set the counters for the next keys to use for each operation
   // lynn- add localInval and localDestroy later when supported in product
   hydra.blackboard.SharedCounters sc = InitImageBB.getBB().getSharedCounters();
   sc.setIfLarger(InitImageBB.LASTKEY_INVALIDATE, intervals.getFirstKey(KeyIntervals.INVALIDATE)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_DESTROY, intervals.getFirstKey(KeyIntervals.DESTROY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_UPDATE_EXISTING_KEY, intervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_GET, intervals.getFirstKey(KeyIntervals.GET)-1);

   // show the blackboard
   InitImageBB.getBB().printSharedMap();
   InitImageBB.getBB().printSharedCounters();
}

/** Initialize the single instance of this test class but not a region. If this VM has 
 *  already initialized its instance, then skip reinitializing.
 */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new KnownKeysTest();
      ((KnownKeysTest)testInstance).initInstance("clientRegion");
      if (isGatewayConfiguration) {
         String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
         GatewayHubHelper.createGatewayHub(gatewayHubConfig);
      }
   }
}


/**
 *  Initialize this test instance
 */
public void initInstance(String regDescriptName) {
   super.initInstance();
   isBridgeConfiguration = (TestConfig.tab().stringAt(BridgePrms.names, null) != null);
   isGatewayConfiguration = (TestConfig.tab().stringAt(GatewayPrms.names, null) != null);
   isGatewaySenderConfiguration = (TestConfig.tab().stringAt(GatewaySenderPrms.names, null) != null);
   lockOperations = TestConfig.tab().booleanAt(ParRegPrms.lockOperations, false);
   highAvailability = TestConfig.tab().booleanAt(ParRegPrms.highAvailability, false);
   
   // determine if this is an edgeClient to be configured via ClientCache
   isBridgeConfiguration = (TestConfig.tab().stringAt(BridgePrms.names, null) != null);
   if (isBridgeConfiguration && regDescriptName.equalsIgnoreCase("accessorRegion")) {
     isClientCache = (TestConfig.tab().stringAt(ClientCachePrms.names, null) != null);
   }

   if (isClientCache) {
     ClientCache myCache = ClientCacheHelper.createCache("clientCache");
     aRegion = ClientRegionHelper.createRegion(regDescriptName);

     // edgeClients always support ConcurrentMap 
     supportsConcurrentMap = true;
   } else {
     Cache myCache = CacheHelper.createCache("cache1");

     //create gateway sender before creating regions   
     PoolDescription poolDescript = RegionHelper.getRegionDescription(regDescriptName).getPoolDescription();
   
     if(isGatewaySenderConfiguration && poolDescript == null){ //gateway senders are configured and it is not an edge vm
       String senderConfig = ConfigPrms.getGatewaySenderConfig();
       GatewaySenderHelper.createGatewaySenders(senderConfig);
     }

     // used by rebalance tests only
     InternalResourceManager.ResourceObserver ro = ParRegPrms.getResourceObserver();
     if (ro != null) {
        InternalResourceManager rm = InternalResourceManager.getInternalResourceManager(myCache);
        rm.setResourceObserver(ro);
     }

     ParRegUtil.createDiskStoreIfNecessary(regDescriptName);
     RegionAttributes attr = RegionHelper.getRegionAttributes(regDescriptName);
     String regionName = RegionHelper.getRegionDescription(regDescriptName).getRegionName();
     if (poolDescript != null) {
        String poolConfigName = RegionHelper.getRegionDescription(regDescriptName).getPoolDescription().getName();
        if (poolConfigName != null) {
           PoolHelper.createPool(poolConfigName);
        }
        // edgeClients always support ConcurrentMap 
        supportsConcurrentMap = true;
     }
     String hdfsStoreConfig = attr.getHDFSStoreName();
     if (hdfsStoreConfig != null) {
        HDFSStoreHelper.createHDFSStore(hdfsStoreConfig);
     }

     aRegion = CacheHelper.getCache().createRegion(regionName, attr);
     Log.getLogWriter().info("Created region " + aRegion.getFullPath());

     isGatewayConfiguration = (TestConfig.tab().stringAt(GatewayPrms.names, null) != null);
     lockOperations = TestConfig.tab().booleanAt(ParRegPrms.lockOperations, false);
     if (lockOperations) {
        Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
        distLockService = DistributedLockService.create(LOCK_SERVICE_NAME, DistributedSystemHelper.getDistributedSystem());
        Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
     }
   }
}

/** Initialize the single instance of this test class and an
 *  accessor partitioned region.
 */
public synchronized static void HydraTask_accessorInitialize() {
   if (testInstance == null) {
      testInstance = new KnownKeysTest();
      ((KnownKeysTest)testInstance).initInstance("accessorRegion");
      if (isBridgeConfiguration) {
         ParRegUtil.registerInterest(testInstance.aRegion);
      }
   }
}

public synchronized static void HydraTask_HA_accessorInitialize() {
  if (testInstance == null) {
     testInstance = new KnownKeysTest();
     ((KnownKeysTest)testInstance).initInstance("accessorRegion");
     if (isBridgeConfiguration) {
        ParRegUtil.registerInterest(testInstance.aRegion);
     }
  }
}

/** Initialize the single instance of this test class and a
 *  dataStore partitioned region.
 */
public synchronized static void HydraTask_dataStoreInitialize() {
   if (testInstance == null) {
      testInstance = new KnownKeysTest();
      ((KnownKeysTest)testInstance).initInstance("dataStoreRegion");
      ParRegBB.getBB().getSharedMap().put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
                                          new Integer(RemoteTestModule.getMyVmid()));
      if (isBridgeConfiguration) {
         BridgeHelper.startBridgeServer("bridge");
      }
      if (isGatewayConfiguration) {
         String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
         GatewayHubHelper.createGatewayHub(gatewayHubConfig);
      }
   }
}

/** Using the Gateway endpoints stored on the hydra BB via 
 *  GatewayHubHelper.createGatewayHub, start the Gateway in the Servers.
 *  Note that all participating Gateways must write their endpoints to
 *  the blackboard prior to any gateway making this call.
 */
public synchronized static void HydraTask_startGatewayHub() {
   if (isGatewayConfiguration) {
      String gatewayConfig = ConfigPrms.getGatewayConfig();
      GatewayHubHelper.addGateways(gatewayConfig);
      GatewayHubHelper.startGatewayHub();
   }
}

/**
 * Creates GatewaySender ids based on the {@link ConfigPrms#gatewaySenderConfig} for new wan senders.
 * This has to be created before creating regions.
 */
public synchronized static void HydraTask_createGatewaySenderIds() {
  String senderConfig = ConfigPrms.getGatewaySenderConfig();
  GatewaySenderHelper.createGatewaySenderIds(senderConfig);
}

/** Hydra task to start new WAN senders which are created earlier via 
 *  GatewaySenderHelper.createGatewaySenders. It also start and creates 
 *  receivers as given in hydra.ConfigPrms#gatewayReceiverConfig    
 */
public synchronized static void HydraTask_startNewWanComponents() {
  if(TestConfig.tab().stringAt(GatewaySenderPrms.names, null) != null){
    //create and start gateway sender  
    GatewaySenderHelper.startGatewaySenders(ConfigPrms.getGatewaySenderConfig());     
    
    //create and start gateway receiver
    GatewayReceiverHelper.createAndStartGatewayReceivers(ConfigPrms.getGatewayReceiverConfig());
  }
}

/** Hydra task to write the existing diskDirs to the blackboard, if any are set.
 * 
 */
public synchronized static void HydraTask_writeDiskDirsToBB() {
  if (!diskFilesRecorded) {
    ParRegUtil.writeDiskDirsToBB(testInstance.aRegion);
    diskFilesRecorded = true;
  }
}

public synchronized static void HydraTask_HA_dataStoreInitialize() {
  if (testInstance == null) {
     PRObserver.installObserverHook();
     PRObserver.initialize(RemoteTestModule.getMyVmid());
     testInstance = new KnownKeysTest();
     ((KnownKeysTest)testInstance).initInstance("dataStoreRegion");
     ParRegBB.getBB().getSharedMap().put(DataStoreVmStr + RemoteTestModule.getMyVmid(),
                                         new Integer(RemoteTestModule.getMyVmid()));
     RegionAttributes attr = testInstance.aRegion.getAttributes();
     PartitionAttributes prAttr = attr.getPartitionAttributes();
     if (prAttr == null) { // this is not a PR datastore, but probably a replicate; done in parReg/execute tests
        ParRegBB.getBB().getSharedMap().put("recoveryDelay", new Long(-1));
        ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay", new Long(-1));
     } else {
        ParRegBB.getBB().getSharedMap().put("recoveryDelay", new Long(prAttr.getRecoveryDelay()));
        ParRegBB.getBB().getSharedMap().put("startupRecoveryDelay", new Long(prAttr.getStartupRecoveryDelay()));
     }
     if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
     }
  }
}

/** If this vm has a startup delay, wait for it.
 */
public static void HydraTask_waitForStartupRecovery() {
   List startupVMs = new ArrayList(StopStartBB.getBB().getSharedMap().getMap().values());
   List vmsExpectingRecovery = StopStartVMs.getMatchVMs(startupVMs, "dataStore");
   vmsExpectingRecovery.addAll(StopStartVMs.getMatchVMs(startupVMs, "bridge"));
   if (vmsExpectingRecovery.size() == 0) {
      throw new TestException("No startup vms to wait for");
   }
   long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
   if (startupRecoveryDelay >= 0) {
      int numPRs = (ConfigPrms.getHadoopConfig() != null) ? 2 : 1;
      PRObserver.waitForRebalRecov(vmsExpectingRecovery, 1, numPRs, null, null, false);
   }
}

/** Waits for this vm to complete redundancy recovery.
 */
public static void HydraTask_waitForMyStartupRecovery() {
  if (testInstance.aRegion.getAttributes().getPartitionAttributes().getRedundantCopies() == 0) {
    Log.getLogWriter().info("RedundantCopies is 0, so no startup recovery will occur");
    return;
  }
  int myVmID = RemoteTestModule.getMyVmid();
  long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
  if (startupRecoveryDelay >= 0) {
    int numPRs = (ConfigPrms.getHadoopConfig() != null) ? 2 : 1;
    PRObserver.waitForRebalRecov(myVmID, 1, numPRs, null, null, false);
  }
}

public static void HydraTask_validatePR() {
   int redundantCopies = testInstance.aRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
   ParRegUtil.verifyPRMetaData(testInstance.aRegion);
   ParRegUtil.verifyPrimaries(testInstance.aRegion, redundantCopies);
   ParRegUtil.verifyBucketCopies(testInstance.aRegion, redundantCopies);
}

// ======================================================================== 
// hydra tasks

/** Disconnect from the ds, in preparation of calling offline disk validation
 *  and compaction
 */
public static void HydraTask_disconnect() {
   DistributedSystemHelper.disconnect();
   testInstance = null; 
   ParRegBB.getBB().getSharedCounters().zero(ParRegBB.DiskRecoveryCounter);
   PRObserver.initialize();
   parRegUtilInstance = new ParRegUtil();
   verifyRegionContentsIndex = 0;
   verifyRegionContentsErrStr = new StringBuffer();
   verifyRegionContentsSizeStr = new StringBuffer();
   verifyRegionContentsCompleted = false;
}

/** Log the local size of the PR data store
 */
public synchronized static void HydraTask_logLocalSize() {
   Log.getLogWriter().info("Number of entries in this data store: " + 
      ParRegUtil.getLocalSize(testInstance.aRegion));
}

/** Hydra task to initialize a region and load it according to hydra param settings. 
 */
public static void HydraTask_loadRegion() {
   PdxTest.initClassLoader();
   testInstance.loadRegion();
}

/** Hydra task to execution ops, then stop scheduling.
 */
public static void HydraTask_doOps() {
   BitSet availableOps = new BitSet(operations.length);
   availableOps.flip(FIRST_OP, LAST_OP+1);
   testInstance.doOps(availableOps);
   if (availableOps.cardinality() == 0) {
      ParRegBB.getBB().getSharedCounters().increment(ParRegBB.TimeToStop);
      throw new StopSchedulingTaskOnClientOrder("Finished with ops");
   }
}

/** Hydra task to execution PR TX ops, then stop scheduling.
 */
public static void HydraTask_doPrTxOps() {
   BitSet availableOps = new BitSet(operations.length);
   // todo@lhughes -- add ops in as PR TX implementation allows
   //availableOps.flip(FIRST_OP, LAST_OP+1);
   availableOps.flip(INVALIDATE);
   availableOps.flip(DESTROY);
   availableOps.flip(UPDATE_EXISTING_KEY);
   availableOps.flip(GET);
   availableOps.flip(ADD_NEW_KEY);
   testInstance.doOps(availableOps);
   if (availableOps.cardinality() == 0) {
      ParRegBB.getBB().getSharedCounters().increment(ParRegBB.TimeToStop);
      throw new StopSchedulingTaskOnClientOrder("Finished with ops");
   }
}

/** Hydra task to wait for a period of silence (30 seconds) to allow
 *  the server queues to drain to the clients.
 */
public static void HydraTask_waitForSilence() {
   SilenceListener.waitForSilence(30 /*seconds*/, 1000);
}

/** Hydra task to verify metadata
 */
public static void HydraTask_verifyPRMetaData() {
   try {
      ParRegUtil.verifyPRMetaData(testInstance.aRegion);
   } catch (Exception e) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Hydra task to verify buckets on on unique hosts 
 */
public static void HydraTask_verifyUniqueHosts() {
   try {
      ParRegUtil.verifyBucketsOnUniqueHosts(testInstance.aRegion);
   } catch (Exception e) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException(TestHelper.getStackTrace(e));
   }
}


/** Hydra task to verify primaries
 */
public static void HydraTask_verifyPrimaries() {
   try {
      RegionAttributes attr = testInstance.aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      int redundantCopies = 0;
      if (prAttr != null) { 
         redundantCopies = prAttr.getRedundantCopies();
      }
      if (((KnownKeysTest)testInstance).highAvailability) { // with HA, wait for things to settle down
         ParRegUtil.verifyPrimariesWithWait(testInstance.aRegion, redundantCopies);
      } else {
         ParRegUtil.verifyPrimaries(testInstance.aRegion, redundantCopies);
      }
   } catch (Exception e) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Hydra task to verify bucket copies. This must be called repeatedly
 *  by the same thread until StopSchedulingTaskOnClientOrder is thrown.
 */
public static void HydraTask_verifyBucketCopiesBatched() {
   try {
      RegionAttributes attr = testInstance.aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      int redundantCopies = 0;
      if (prAttr != null) { 
         redundantCopies = prAttr.getRedundantCopies();
      }
      // the following call throws StopSchedulingTaskOnClientOrder when completed
      parRegUtilInstance.verifyBucketCopiesBatched(testInstance.aRegion, redundantCopies);
   } catch (TestException e) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Hydra task to stop/start numVmsToStop at a time.
 */
public static void HydraTask_stopStartVms() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   do { 
      ((KnownKeysTest)testInstance).stopStartVms();
      long timeToStop = ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop);
      if (timeToStop > 0) { // ops have ended
         throw new StopSchedulingTaskOnClientOrder("Ops have completed");
      }
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

/** Hydra task to verify the contents of the region after all ops.
 *  This MUST be called as a batched task, and will throw
 *  StopSchedulingTaskOnClientOrder when completed. It is necessary
 *  to reinitialize the verify state variables if this is called
 *  a second time in this VM, however an error is thrown if a second
 *  attempt it made without resetting the state variables.
 */
public static void HydraTask_verifyRegionContents() {
   InitImageBB.getBB().printSharedCounters();
   NameBB.getBB().printSharedCounters();
   testInstance.verifyRegionContents();
}

public static void HydraTask_verifyHDFSRegionContents() {
   InitImageBB.getBB().printSharedCounters();
   NameBB.getBB().printSharedCounters();
   ((KnownKeysTest)testInstance).verifyHDFSRegionContents();
}

/** Hydra task to verify the size of the region after the load.
 *  This MUST be called as a batched task, and will throw
 *  StopSchedulingTaskOnClientOrder when completed. It is necessary
 *  to reinitialize the verify state variables if this is called
 *  a second time in this VM, however an error is thrown if a second
 *  attempt it made without resetting the state variables.
 */
public static void HydraTask_verifyRegionSize() {
   ((KnownKeysTest)testInstance).verifyRegionSize();
}

/** Verify that test counters and region size are in agreement after
 *  the load. This must be called repeatedly by the same thread until 
 *  StopSchedulingTaskOnClientOrder is thrown.
 */
protected void verifyRegionSize() {
   // we already completed this check once; we can't do it again without reinitializing the 
   // verify state variables
   if (verifyRegionSizeCompleted) { 
      throw new TestException("Test configuration problem; already verified region size, cannot call this task again without resetting batch variables");
   }
   long lastLogTime = System.currentTimeMillis();
   int size = aRegion.size();
   long numOriginalKeysCreated = InitImageBB.getBB().getSharedCounters().read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED);
   int numKeysToCreate = keyIntervals.getNumKeys();
   long nameCounter = NameFactory.getPositiveNameCounter();
   Log.getLogWriter().info("In HydraTask_verifyRegionSize, region size is " + size + 
                           ", numKeysToCreate is " + numKeysToCreate +
                           ", numOriginalKeysCreated is " + numOriginalKeysCreated +
                           ", nameCounter is " + nameCounter);

   // make sure the test agrees with itself
   if ((numOriginalKeysCreated != numKeysToCreate) ||
       (numOriginalKeysCreated != nameCounter)) {
      throw new TestException("Error in test, numOriginalKeysCreated " + numOriginalKeysCreated + 
                              ", numKeysToCreate " + numKeysToCreate +
                              ", nameCounter " + nameCounter);
   }

   // make sure all keys/values are present
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   boolean first = true;
   while (verifyRegionSizeIndex < numKeysToCreate) {
      verifyRegionSizeIndex++;
      if (first) {
         Log.getLogWriter().info("In HydraTask_verifyRegionSize, starting verify with verifyRegionSizeIndex " + verifyRegionSizeIndex);
         first = false;
      }
      Object key = NameFactory.getObjectNameForCounter(verifyRegionSizeIndex);
      if (!aRegion.containsKey(key)) 
         missingKeys.add(key);
      if (!aRegion.containsValueForKey(key))
         missingValues.add(key);
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Verified " + verifyRegionSizeIndex + " keys/values out of " + numKeysToCreate);
         lastLogTime = System.currentTimeMillis();
      }
      if (System.currentTimeMillis() - startTime >= minTaskGranularityMS) {
         Log.getLogWriter().info("In HydraTask_verifyRegionSize, returning before completing verify " +
             "because of task granularity (this task must be batched to complete); last key verified is " + 
             key);
         return;  // task is batched; we are done with this batch
      }
   }
   verifyRegionSizeCompleted = true;
   if (missingKeys.size() != 0) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException("Missing " + missingKeys.size() + " keys: " + missingKeys);
   }
   if (missingValues.size() != 0) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException("Missing " + missingValues.size() + " values: " + missingValues);
   }
   if (size != numKeysToCreate) {
      throw new TestException("Unexpected region size " + size + "; expected " + numKeysToCreate);
   }
   String aStr = "In HydraTask_verifyRegionSize, verified " + verifyRegionSizeIndex + " keys and values";
   Log.getLogWriter().info(aStr);
   throw new StopSchedulingTaskOnClientOrder(aStr);
}

/** Verify the contents of the region, taking into account the keys
 *  that were destroyed, invalidated, etc (as specified in keyIntervals)
 *  Throw an error of any problems are detected.
 *  This must be called repeatedly by the same thread until 
 *  StopSchedulingTaskOnClientOrder is thrown.
 */ 
public void verifyRegionContents() {
   // we already completed this check once; we can't do it again without reinitializing the 
   // verify state variables
   if (verifyRegionContentsCompleted) {
      throw new TestException("Test configuration problem; already verified region contents, cannot call this task again without resetting batch variables");
   }

   // iterate keys
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   long size = aRegion.size();
   boolean first = true;
   int numKeysToCheck = keyIntervals.getNumKeys() + numNewKeys;
   while (verifyRegionContentsIndex < numKeysToCheck) {
      verifyRegionContentsIndex++;
      if (first) {
         Log.getLogWriter().info("In verifyRegionContents, region has " + size + 
            " keys; starting verify at verifyRegionContentsIndex " + verifyRegionContentsIndex +
            "; verifying key names with indexes through (and including) " + numKeysToCheck);
         first = false;
      }

      // check region size the first time through the loop to avoid it being called
      // multiple times when this is batched
      if (verifyRegionContentsIndex == 1) {
         if (totalNumKeys != size) {
            String tmpStr = "Expected region size to be " + totalNumKeys + ", but it is size " + size;
            Log.getLogWriter().info(tmpStr);
            verifyRegionContentsSizeStr.append(tmpStr + "\n");
         }
      }

      Object key = NameFactory.getObjectNameForCounter(verifyRegionContentsIndex);
      try {
         if (((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.NONE)) &&
              (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.NONE)))    ||
             ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.GET)) &&
             (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.GET)))) {
            // this key was untouched after its creation
            checkContainsKey(key, true, "key was untouched");
            checkContainsValueForKey(key, true, "key was untouched");
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.INVALIDATE)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.INVALIDATE))) {
            checkContainsKey(key, true, "key was invalidated");
            checkContainsValueForKey(key, false, "key was invalidated");
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE))) {
            // this key was locally invalidated
            checkContainsKey(key, true, "key was locally invalidated");
            checkContainsValueForKey(key, true, "key was locally invalidated");
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.DESTROY)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.DESTROY))) {
            // this key was destroyed
            checkContainsKey(key, false, "key was destroyed");
            checkContainsValueForKey(key, false, "key was destroyed");
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY))) {
            // this key was locally destroyed
            checkContainsKey(key, true, "key was locally destroyed");
            checkContainsValueForKey(key, true, "key was locally destroyed");
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY))) {
            // this key was updated
            checkContainsKey(key, true, "key was updated");
            checkContainsValueForKey(key, true, "key was updated");
            Object value = aRegion.get(key);
            checkUpdatedValue(key, value);
         } else if (verifyRegionContentsIndex > keyIntervals.getNumKeys()) {
            // key was newly added
            checkContainsKey(key, true, "key was new");
            checkContainsValueForKey(key, true, "key was new");
            Object value = aRegion.get(key);
            checkValue(key, value);
         }
      } catch (TestException e) {
         Log.getLogWriter().info(TestHelper.getStackTrace(e));

         Set failedOps = ParRegBB.getBB().getFailedOps(ParRegBB.FAILED_TXOPS);
         Set inDoubtOps = ParRegBB.getBB().getFailedOps(ParRegBB.INDOUBT_TXOPS);
         if (!failedOps.contains(key) && !inDoubtOps.contains(key)) {
            verifyRegionContentsErrStr.append(e.getMessage() + "\n");
         }
      }

      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Verified key " + verifyRegionContentsIndex + " out of " + totalNumKeys);
         lastLogTime = System.currentTimeMillis();
      }

      if (System.currentTimeMillis() - startTime >= minTaskGranularityMS) {
         Log.getLogWriter().info("In HydraTask_verifyRegionContents, returning before completing verify " +
             "because of task granularity (this task must be batched to complete); last key verified is " + 
             key);
         return;  // task is batched; we are done with this batch
      }
   }
   verifyRegionContentsCompleted = true;
   StringBuffer error = new StringBuffer();
   Set failedOps = ParRegBB.getBB().getFailedOps(ParRegBB.FAILED_TXOPS);
   if (failedOps.size() > 0) {
     if (verifyRegionContentsErrStr.length() > 0) {
        error.append("Test detected BUG 43428 (" + failedOps.size() + " failed transactions) with additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        throw new TestException(error.toString());
     } else {
/* behavior documented by bug 43428 will not be changed in the near future.  Don't flag this failure unless there were additional data inconsistencies reported in the test.  Log a message in case any one looks at the logs and wonders why the inconsistencies are not causing an Exception to be thrown. */
        error.append("Test detected BUG 43428 (" + failedOps.size() + " failed transactions) with no additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        //throw new TestException(error.toString());
        Log.getLogWriter().info("Inconsistencies are related to Transaction HA behavior (not currently supported): " + error.toString());
     }
   } else if (ParRegBB.getBB().getFailedOps(ParRegBB.INDOUBT_TXOPS).size() > 0) {
     failedOps = ParRegBB.getBB().getFailedOps(ParRegBB.INDOUBT_TXOPS);
     if (verifyRegionContentsErrStr.length() > 0) {
        error.append("Test reported " + failedOps.size() + " TransactionInDoubtExceptions with additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        throw new TestException(error.toString());
     } else {
/* behavior documented by bug 43428 will not be changed in the near future.  Don't flag this failure unless there were additional data inconsistencies reported in the test.  Log a message in case any one looks at the logs and wonders wh
y the inconsistencies are not causing an Exception to be thrown. */
        error.append("Test reported " + failedOps.size() + " TransactionInDoubtExceptions with no additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        //throw new TestException(error.toString());
        Log.getLogWriter().info("Inconsistencies are related to Transaction HA behavior (not currently supported): " + error.toString());
     }
   } else if (verifyRegionContentsErrStr.length() > 0) {
      // shutdownHook will cause all members to dump partitioned region info
      verifyRegionContentsSizeStr.append(verifyRegionContentsErrStr);
      throw new TestException(verifyRegionContentsSizeStr.toString());
   } 
   String aStr = "In HydraTask_verifyRegionContents, verified " + numKeysToCheck + " keys/values";
   Log.getLogWriter().info(aStr);
   throw new StopSchedulingTaskOnClientOrder(aStr);
}

/** Verify that test counters and region size are in agreement after
 *  the load. This must be called repeatedly by the same thread until 
 *  StopSchedulingTaskOnClientOrder is thrown.
 *
 *  For HDFS regions (with eviction) we cannot rely on region.size(), containsKey(), containsValueForKey()
 */
public static void HydraTask_verifyHDFSRegionSize() {
   ((KnownKeysTest)testInstance).verifyHDFSRegionSize();
}

protected void verifyHDFSRegionSize() {
   // we already completed this check once; we can't do it again without reinitializing the 
   // verify state variables
   if (verifyRegionSizeCompleted) { 
      throw new TestException("Test configuration problem; already verified region size, cannot call this task again without resetting batch variables");
   }

   // This won't work for HDFS regions with eviction, so simply return
   if (!aRegion.getAttributes().getEvictionAttributes().getAlgorithm().equals(EvictionAlgorithm.NONE)) {
      Log.getLogWriter().info("verifyHDFSRegionSize() returning (without validating) as this HDFS region is configured with eviction");
      throw new StopSchedulingTaskOnClientOrder("Cannot verify region size for HDFS regions");
   }

   long lastLogTime = System.currentTimeMillis();
   int size = aRegion.size();
   long numOriginalKeysCreated = InitImageBB.getBB().getSharedCounters().read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED);
   int numKeysToCreate = keyIntervals.getNumKeys();
   long nameCounter = NameFactory.getPositiveNameCounter();
   Log.getLogWriter().info("In HydraTask_verifyRegionSize, region size is " + size + 
                           ", numKeysToCreate is " + numKeysToCreate +
                           ", numOriginalKeysCreated is " + numOriginalKeysCreated +
                           ", nameCounter is " + nameCounter);

   // make sure the test agrees with itself
   if ((numOriginalKeysCreated != numKeysToCreate) ||
       (numOriginalKeysCreated != nameCounter)) {
      throw new TestException("Error in test, numOriginalKeysCreated " + numOriginalKeysCreated + 
                              ", numKeysToCreate " + numKeysToCreate +
                              ", nameCounter " + nameCounter);
   }

   // make sure all keys/values are present
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   boolean first = true;
   while (verifyRegionSizeIndex < numKeysToCreate) {
      verifyRegionSizeIndex++;
      if (first) {
         Log.getLogWriter().info("In HydraTask_verifyRegionSize, starting verify with verifyRegionSizeIndex " + verifyRegionSizeIndex);
         first = false;
      }
      Object key = NameFactory.getObjectNameForCounter(verifyRegionSizeIndex);
      if (!aRegion.containsKey(key)) 
         missingKeys.add(key);
      if (!aRegion.containsValueForKey(key))
         missingValues.add(key);
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Verified " + verifyRegionSizeIndex + " keys/values out of " + numKeysToCreate);
         lastLogTime = System.currentTimeMillis();
      }
      if (System.currentTimeMillis() - startTime >= minTaskGranularityMS) {
         Log.getLogWriter().info("In HydraTask_verifyRegionSize, returning before completing verify " +
             "because of task granularity (this task must be batched to complete); last key verified is " + 
             key);
         return;  // task is batched; we are done with this batch
      }
   }
   verifyRegionSizeCompleted = true;
   if (missingKeys.size() != 0) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException("Missing " + missingKeys.size() + " keys: " + missingKeys);
   }
   if (missingValues.size() != 0) {
      // shutdownHook will cause all members to dump partitioned region info
      throw new TestException("Missing " + missingValues.size() + " values: " + missingValues);
   }
   if (size != numKeysToCreate) {
      throw new TestException("Unexpected region size " + size + "; expected " + numKeysToCreate);
   }
   String aStr = "In HydraTask_verifyRegionSize, verified " + verifyRegionSizeIndex + " keys and values";
   Log.getLogWriter().info(aStr);
   throw new StopSchedulingTaskOnClientOrder(aStr);
}

/** HDFS regions are recovered lazily from disk ... only operations (like get()) which invoke the cacheLoader
 *  will retrieve data from HDFS.  So, we cannot rely on region.size(), containsKey, containsValueForKey, etc.
 */
public void verifyHDFSRegionContents() {
   // we already completed this check once; we can't do it again without reinitializing the 
   // verify state variables
   if (verifyRegionContentsCompleted) {
      throw new TestException("Test configuration problem; already verified region contents, cannot call this task again without resetting batch variables");
   }

   // iterate keys
   long lastLogTime = System.currentTimeMillis();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   long size = aRegion.size();
   boolean first = true;
   int numKeysToCheck = keyIntervals.getNumKeys() + numNewKeys;
   while (verifyRegionContentsIndex < numKeysToCheck) {
      verifyRegionContentsIndex++;
      if (first) {
         Log.getLogWriter().info("In verifyRegionContents, region has " + size + 
            " keys; starting verify at verifyRegionContentsIndex " + verifyRegionContentsIndex +
            "; verifying key names with indexes through (and including) " + numKeysToCheck);
         first = false;
      }

      // check region size the first time through the loop to avoid it being called
      // multiple times when this is batched
      if (verifyRegionContentsIndex == 1) {
         if (totalNumKeys != size) {
            String tmpStr = "Expected region size to be " + totalNumKeys + ", but it is size " + size;
            Log.getLogWriter().info(tmpStr);
            verifyRegionContentsSizeStr.append(tmpStr + "\n");
         }
      }

      Object key = NameFactory.getObjectNameForCounter(verifyRegionContentsIndex);
      try {
         if (((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.NONE)) &&
              (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.NONE)))    ||
             ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.GET)) &&
             (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.GET)))) {
            // this key was untouched after its creation
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.INVALIDATE)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.INVALIDATE))) {
            Object value = aRegion.get(key);
            if (value != null) {
              throw new TestException("Expected get(" + key + ") to return null, but it returned " + value + " key was invalidated");
            }
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.LOCAL_INVALIDATE))) {
            // this key was locally invalidated
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.DESTROY)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.DESTROY))) {
            // this key was destroyed
            boolean gotENFE = false;
            try {
               Object value = aRegion.get(key);
               if (value != null) {  // loader should return null for a destroyed entry
                  throw new TestException("Expected get(" + key + ") to return null, but it returned " + value + " : entry was destroyed");
               }
               aRegion.destroy(key); // try to destroy the destroyed entry, we should get ENFE
            } catch (EntryNotFoundException e) {
               gotENFE = true;
            }
            if (!gotENFE) {
               throw new TestException("Expected get(" + key + ") to throw EntryNotFoundException, but it did not : entry was destroyed");
            }
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.LOCAL_DESTROY))) {
            // this key was locally destroyed
            Object value = aRegion.get(key);
            checkValue(key, value);
         } else if ((verifyRegionContentsIndex >= keyIntervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)) &&
                    (verifyRegionContentsIndex <= keyIntervals.getLastKey(KeyIntervals.UPDATE_EXISTING_KEY))) {
            // this key was updated
            Object value = aRegion.get(key);
            checkUpdatedValue(key, value);
         } else if (verifyRegionContentsIndex > keyIntervals.getNumKeys()) {
            // key was newly added
            Object value = aRegion.get(key);
            checkValue(key, value);
         }
      } catch (TestException e) {
         Log.getLogWriter().info(TestHelper.getStackTrace(e));

         Set failedOps = ParRegBB.getBB().getFailedOps(ParRegBB.FAILED_TXOPS);
         Set inDoubtOps = ParRegBB.getBB().getFailedOps(ParRegBB.INDOUBT_TXOPS);
         if (!failedOps.contains(key) && !inDoubtOps.contains(key)) {
            verifyRegionContentsErrStr.append(e.getMessage() + "\n");
         }
      }

      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
         Log.getLogWriter().info("Verified key " + verifyRegionContentsIndex + " out of " + totalNumKeys);
         lastLogTime = System.currentTimeMillis();
      }

      // Note that ENDTASKS cannot be batched ... so don't batch this method for ENDTASKS
      if (System.currentTimeMillis() - startTime >= minTaskGranularityMS) {
         String taskType = RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString();
         if (!taskType.equalsIgnoreCase("ENDTASK")) {
           Log.getLogWriter().info("In HydraTask_verifyRegionContents, returning before completing verify " +
               "because of task granularity (this task must be batched to complete); last key verified is " + 
               key);
           return;  // task is batched; we are done with this batch
         }
      }
   }
   verifyRegionContentsCompleted = true;
   StringBuffer error = new StringBuffer();
   Set failedOps = ParRegBB.getBB().getFailedOps(ParRegBB.FAILED_TXOPS);
   if (failedOps.size() > 0) {
     if (verifyRegionContentsErrStr.length() > 0) {
        error.append("Test detected BUG 43428 (" + failedOps.size() + " failed transactions) with additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        throw new TestException(error.toString());
     } else {
/* behavior documented by bug 43428 will not be changed in the near future.  Don't flag this failure unless there were additional data inconsistencies reported in the test.  Log a message in case any one looks at the logs and wonders why the inconsistencies are not causing an Exception to be thrown. */
        error.append("Test detected BUG 43428 (" + failedOps.size() + " failed transactions) with no additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        //throw new TestException(error.toString());
        Log.getLogWriter().info("Inconsistencies are related to Transaction HA behavior (not currently supported): " + error.toString());
     }
   } else if (ParRegBB.getBB().getFailedOps(ParRegBB.INDOUBT_TXOPS).size() > 0) {
     failedOps = ParRegBB.getBB().getFailedOps(ParRegBB.INDOUBT_TXOPS);
     if (verifyRegionContentsErrStr.length() > 0) {
        error.append("Test reported " + failedOps.size() + " TransactionInDoubtExceptions with additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        throw new TestException(error.toString());
     } else {
/* behavior documented by bug 43428 will not be changed in the near future.  Don't flag this failure unless there were additional data inconsistencies reported in the test.  Log a message in case any one looks at the logs and wonders wh
y the inconsistencies are not causing an Exception to be thrown. */
        error.append("Test reported " + failedOps.size() + " TransactionInDoubtExceptions with no additional issues reported\n");
        error.append(verifyRegionContentsSizeStr);
        error.append(verifyRegionContentsErrStr);
        //throw new TestException(error.toString());
        Log.getLogWriter().info("Inconsistencies are related to Transaction HA behavior (not currently supported): " + error.toString());
     }
   } else if (verifyRegionContentsErrStr.length() > 0) {
      // shutdownHook will cause all members to dump partitioned region info
      verifyRegionContentsSizeStr.append(verifyRegionContentsErrStr);
      throw new TestException(verifyRegionContentsSizeStr.toString());
   } 
   String aStr = "In HydraTask_verifyRegionContents, verified " + numKeysToCheck + " keys/values";
   Log.getLogWriter().info(aStr);

   // ENDTASKs cannot be batched, so only throw the StopSchedulingException on INIT, TASK and CLOSE tasks
   String taskType = RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString();
   if (!taskType.equalsIgnoreCase("ENDTASK")) {
     throw new StopSchedulingTaskOnClientOrder(aStr);
   }
}

// ======================================================================== 
// other methods

/** Stop and start ParRegPrms.numVMsToStop. Do not return until the VMs
 *  have restarted and reinitialized
 */
protected void stopStartVms() {
   PRObserver.initialize();
   int numVMsToStop = TestConfig.tab().intAt(ParRegPrms.numVMsToStop);
   Log.getLogWriter().info("In stopStartVms, choosing " + numVMsToStop + " vm(s) to stop...");
   List vmList = getDataStoreVms();
   List targetVms = new ArrayList();
   List stopModes = new ArrayList();
   for (int i = 1; i <= numVMsToStop; i++) {
      int randInt = TestConfig.tab().getRandGen().nextInt(0, vmList.size()-1);
      targetVms.add(vmList.get(randInt));
      vmList.remove(randInt);
      stopModes.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
   }
   StopStartVMs.stopStartVMs(targetVms, stopModes);

   // wait for recovery to run
   long recoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("recoveryDelay"))).longValue();
   long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
   PRObserver.waitForRecovery(recoveryDelay, startupRecoveryDelay, 
              targetVms, vmList, numVMsToStop, 1, null, null);
      
   Log.getLogWriter().info("Done in KnownKeysTest.stopStartVms()");
}

/** Return a list of the ClientVmInfo for all data store VMs. These are obtained 
 *  from the blackboard map. 
 */
protected List getDataStoreVms() {
   List aList = new ArrayList();
   Map aMap = ParRegBB.getBB().getSharedMap().getMap();
   Iterator it = aMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      if (key instanceof String) {
         if (((String)key).startsWith(DataStoreVmStr)) {
            aList.add(new ClientVmInfo((Integer)(aMap.get(key)), null, null));
         }
      }
   }
   return aList;
}

/** Do operations on the REGION_NAME's keys using keyIntervals to specify
 *  which keys get which operations. This will return when all operations
 *  in all intervals have completed.
 *
 *  @param availableOps - Bits which are true correspond to the operations
 *                        that should be executed.
 */
public void doOps(BitSet availableOps) {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();

   // this task gets its useTransactions from the tasktab
   // defaults to false
   boolean useTransactions = InitImagePrms.useTransactions();
   boolean rolledback;

   if (useTransactions) {
      // TODO: TX: redo for new TX impl
      //TxHelper.recordClientTXOperations();
   }

   while ((availableOps.cardinality() != 0) && 
          (System.currentTimeMillis() - startTime < minTaskGranularityMS)) {
      int whichOp = getOp(availableOps, operations.length);
      boolean doneWithOps = false;

      boolean gotTheLock = false;
      if (lockOperations) {
         Log.getLogWriter().info("Trying to get distributed lock " + LOCK_NAME + "...");
         gotTheLock = distLockService.lock(LOCK_NAME, -1, -1);
         Log.getLogWriter().info("Returned from trying to get distributed lock " + LOCK_NAME +
            ", lock acquired " + gotTheLock);
         if (!gotTheLock) {
            throw new TestException("Did not get lock " + LOCK_NAME);
         }
      }

      rolledback = false;
      if (useTransactions) {
        TxHelper.begin();
      }

      try {
         switch (whichOp) {
            case ADD_NEW_KEY: 
               doneWithOps = addNewKey();
               break;
            case PUTALL_NEW_KEY: 
                doneWithOps = putAllNewKey();
                break;
            case INVALIDATE:
               doneWithOps = invalidate();
               break;
            case DESTROY:
               doneWithOps = destroy();
               break;
            case UPDATE_EXISTING_KEY:
               doneWithOps = updateExistingKey();
               break;
            case GET:
               doneWithOps = get();
               break;
            case LOCAL_INVALIDATE:
               doneWithOps = localInvalidate();
               break;
            case LOCAL_DESTROY:
               doneWithOps = localDestroy();
               break;
            default: {
               throw new TestException("Unknown operation " + whichOp);
            }
         }
      } catch (TransactionDataNodeHasDepartedException e) {
        if (!useTransactions) {
          throw new TestException("Unexpected TransactionDataNodeHasDepartedException " + TestHelper.getStackTrace(e));
        } else {
          Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
          recordFailedOps(ParRegBB.FAILED_TXOPS);
          Log.getLogWriter().info("Rolling back transaction.");
          try {
            TxHelper.rollback();
            Log.getLogWriter().info("Done Rolling back Transaction");
          } catch (TransactionException te) {
            Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching TransactionDataNodeHasDeparted during tx ops.  Expected, continuing test.");
          }
          rolledback = true;
        }
      } catch (TransactionDataRebalancedException e) {
        if (!useTransactions) {
          throw new TestException("Unexpected Exception " + e + ". " + TestHelper.getStackTrace(e));
        } else {
          Log.getLogWriter().info("Caught Exception " + e + ".  Expected with concurrent execution, continuing test.");
          recordFailedOps(ParRegBB.FAILED_TXOPS);
          Log.getLogWriter().info("Rolling back transaction.");
          try {
            TxHelper.rollback();
            Log.getLogWriter().info("Done Rolling back Transaction");
          } catch (TransactionException te) {
            Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching Exception " + e + " during tx ops.  Expected, continuing test.");
          }
          rolledback = true;
        }
      } finally {
         if (gotTheLock) {
            gotTheLock = false;
            distLockService.unlock(LOCK_NAME);
            Log.getLogWriter().info("Released distributed lock " + LOCK_NAME);
         }
      }

      if (useTransactions && !rolledback) {
        try {
          TxHelper.commit();
        } catch (TransactionDataNodeHasDepartedException e) {
          Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
          recordFailedOps(ParRegBB.FAILED_TXOPS);
        } catch (TransactionDataRebalancedException e) {
          Log.getLogWriter().info("Caught Exception " + e + ".  Expected with concurrent execution, continuing test.");
          recordFailedOps(ParRegBB.FAILED_TXOPS);
        } catch (TransactionInDoubtException e) {
          Log.getLogWriter().info("Caught TransactionInDoubtException.  Expected with concurrent execution, continuing test.");
          recordFailedOps(ParRegBB.INDOUBT_TXOPS);
        } catch (ConflictException e) {
          // we should not see any CommitConflictExceptions in KnownKeysTests (each key only updated once)
          throw new TestException("Unexpected Exception " + e + ". " + TestHelper.getStackTrace(e));
        }
      }

      if (doneWithOps) {
         Log.getLogWriter().info("Done with operation " + whichOp);
         availableOps.clear(whichOp);
      }
   }
}

/** Use TxHelper.getClientTXOperations() to get the List of TransactionalOperations which failed due to 
 *  TransactionDataNodeHasDeparted and TransactionDataRebalancedExceptions (HA tests).
 *  Add the keys for these operations to the given set (either failedTxOps or inDoubtTxOps)
 *
 */
protected void recordFailedOps(String sharedMapKey) {
   // TODO: TX: redo for new TX impl
   /*
   List opList = TxHelper.getClientTXOperations();
   Iterator it = opList.iterator();
   while (it.hasNext()) {
     TransactionalOperation op = (TransactionalOperation)it.next();
     ParRegBB.getBB().addFailedOp(sharedMapKey, op.getKey());
   }
   */
}

}

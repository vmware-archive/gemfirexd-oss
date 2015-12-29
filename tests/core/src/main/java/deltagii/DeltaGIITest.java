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

package deltagii;

import hydra.*;
import hydra.blackboard.*;
import parReg.ParRegUtil;
import pdx.PdxTest;
import util.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.GIITestHook;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.GIITestHookType;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TombstoneService;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

/**
 * @author lhughes
 * 
 * @since 7.5
 *
 */
public class DeltaGIITest {

  /** instance of this test class */
  public static DeltaGIITest testInstance = null;

  // instance fields to hold information about this test run
  private boolean isBridgeConfiguration;
  private boolean isBridgeClient;
  private RandomValues randomValues = new RandomValues(); // used to create payload
  private boolean uniqueKeys = true;

  // static fields
  static Cache theCache = null;
  static Region theRegion = null;

  // field to coordinate image provider and requester vms
  protected static HydraThreadLocal threadIsPaused = new HydraThreadLocal();

  // symbolic constants
  protected static final String VMID = "vmid_";

  //operations
  static protected final int ENTRY_ADD_OPERATION = 1;
  static protected final int ENTRY_DESTROY_OPERATION = 2;
  static protected final int ENTRY_INVALIDATE_OPERATION = 3;
  static protected final int ENTRY_LOCAL_DESTROY_OPERATION = 4;
  static protected final int ENTRY_LOCAL_INVALIDATE_OPERATION = 5;
  static protected final int ENTRY_UPDATE_OPERATION = 6;
  static protected final int ENTRY_GET_OPERATION = 7;
  static protected final int ENTRY_GET_NEW_OPERATION = 8;
  static protected final int ENTRY_PUTALL_OPERATION = 9;
  static protected final int CACHE_OPERATIONS = 10;
  static protected final int PUTS_FOR_KEY_ORDER_POLICY = 11;
  static protected final int CLEAR_REGION = 12;

  //String prefixes for event callback object
  protected static final String getCallbackPrefix = "Get originated in pid ";
  protected static final String createCallbackPrefix = "Create event originated in pid ";
  protected static final String updateCallbackPrefix = "Update event originated in pid ";
  protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
  protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";

  //=================================================
  // initialization methods
  //=================================================
  
  public synchronized static void HydraTask_createLocator() throws Throwable {
    hydra.DistributedSystemHelper.createLocator();
  }

  public synchronized static void HydraTask_startLocatorAndDS()
      throws Throwable {
    hydra.DistributedSystemHelper.startLocatorAndDS();
  }

  /** Create gateway hub
   * 
   */
  public static void HydraTask_createGatewayHub() {
    String hubConfig = ConfigPrms.getGatewayHubConfig();
    Log.getLogWriter().info("Creating gateway hub with hub config: " + hubConfig);
    GatewayHubHelper.createGatewayHub(hubConfig);
  }
  
  /** Create gateway hub
   * 
   */
  public static void HydraTask_addGatewayHub() {
    String gatewayConfig = ConfigPrms.getGatewayConfig();
    Log.getLogWriter().info("Adding gateway with gateway config: " + gatewayConfig);
    GatewayHubHelper.addGateways(gatewayConfig);
  }
  
  /** Start gateway hub
   * 
   */
  public static void HydraTask_startGatewayHub() {
    Log.getLogWriter().info("Starting gateway hub");
    GatewayHubHelper.startGatewayHub();
  }

  /** Stop the gateway hub
   * 
   */
  public static void HydraTask_stopGatewayHub() {
    GatewayHubHelper.stopGatewayHub();
    GatewayHubBlackboard.getInstance().getSharedMap().clear();
    DistributedSystemBlackboard.getInstance().getSharedMap().clear();
  }

  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() throws Throwable {
    PdxTest.initClassLoader();
    if (testInstance == null) {
      testInstance = new DeltaGIITest();
      testInstance.initializeInstance();
      testInstance.initializeRegion(ConfigPrms.getRegionConfig());
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = false;
        BridgeHelper.startBridgeServer("bridge");
      }
    }
    testInstance.initializeInstancePerThread();
  }

  /** Creates and initializes an edge client.
   */
  public synchronized static void HydraTask_initializeClient() throws Throwable {
    if (testInstance == null) {
      testInstance = new DeltaGIITest();
      testInstance.initializeInstance();
      testInstance.initializeRegion(ConfigPrms.getRegionConfig());
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        Log.getLogWriter().info("Calling registerInterest for all keys, result interest policy KEYS_VALUES for region " + theRegion.getFullPath());
        theRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
        Log.getLogWriter().info("Done calling registerInterest for all keys, " +
           "result interest policy KEYS_VALUES, " + theRegion.getFullPath() + 
           " size is " + theRegion.size());
      }
    }
    testInstance.initializeInstancePerThread();
  }

  private void initializeRegion(String regionConfig) {
    String key = VMID + RemoteTestModule.getMyVmid();
    String xmlFileName = key + ".xml";
    File xmlFile = new File(xmlFileName);
    if (!xmlFile.exists()) {   // generate the xml file (we will always initialize from xml)
      String cacheConfig = ConfigPrms.getCacheConfig();
      theCache = CacheHelper.createCache(cacheConfig);
      PoolDescription pd = RegionHelper.getRegionDescription(regionConfig).getPoolDescription();
      DiskStoreDescription dsd = RegionHelper.getRegionDescription(regionConfig).getDiskStoreDescription();
      String diskStoreName = null;
      if (dsd != null) {
          diskStoreName = dsd.getName();
      }
      Log.getLogWriter().info("About to generate xml, diskStoreName is " + diskStoreName);
      if (pd != null) {
        CacheHelper.generateCacheXmlFile(cacheConfig, null, regionConfig, null, null, pd.getName(), diskStoreName, null, xmlFileName);
      } else {
        CacheHelper.generateCacheXmlFile(cacheConfig, null, regionConfig, null, null, null, diskStoreName, null, xmlFileName);
      }
      CacheHelper.closeCache();
    } 
    // This creates both the cache and the regions (as defined by the xml file)
    theCache = CacheHelper.createCacheFromXml(xmlFileName);
    if (theCache == null) {
      throw new TestException("Unexpected null cache after creating with xmlFile " + xmlFileName);
    }
    RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
    theRegion = theCache.getRegion(rd.getRegionName());
    if (theRegion == null) {
      throw new TestException("Unexpected null region after creating with xmlFile " + xmlFileName);
    }
    Log.getLogWriter().info("Finished creating " + TestHelper.regionToString(theRegion, true) + " using " + xmlFile);
  }

  /** Initialize an instance of this test class, called once per vm.
   * 
   */
  private void initializeInstance() {
    isBridgeConfiguration = TestConfig.tab().vecAt(BridgePrms.names, null) != null;
    uniqueKeys = DeltaGIIPrms.getUseUniqueKeys();
    Log.getLogWriter().info("useUniqueKeys = " + uniqueKeys);
    if (DeltaGIIPrms.getRegisterSerializer()) {
      Log.getLogWriter().info("Registering " + VHDataSerializer.class.getName());
      DataSerializer.register(VHDataSerializer.class);
    }

    // setup for giiState callbacks (if configured)
    // Used in HA Tests to allow us to kill the requester or provider vm at a 
    // specific time during getInitialImage.
    String giiStatePrm = TestConfig.tasktab().stringAt(DeltaGIIPrms.giiState, null);
    if (giiStatePrm != null) {
      DeltaGIIBB.getBB().getSharedMap().put(DeltaGIIBB.giiStatePrm, giiStatePrm);
      setupGetInitialImageTestCallbacks(giiStatePrm);
    }
  }

  /** Initialize an instance of this test class, called for each thread
   * 
   */
  private void initializeInstancePerThread() {
    threadIsPaused.set(new Boolean(false));
    int thisThreadID = RemoteTestModule.getCurrentThread().getThreadId();
    String mapKey = DeltaGIIBB.uniqueKeyIndex + thisThreadID;
    Object mapValue = DeltaGIIBB.getBB().getSharedMap().get(mapKey);
    if (mapValue == null) {
      DeltaGIIBB.getBB().getSharedMap().put(mapKey, thisThreadID);
    } 
  }

  public void setupGetInitialImageTestCallbacks(String giiStatePrm) {
    // Local class used to provide a Runnable to the product via
    // internal apis.
    class GetInitialImageTestCallback extends GIITestHook {

    public GetInitialImageTestCallback(GIITestHookType type, String region_name) {
      super(type, region_name);
    }

      public void reset() {
        // do nothing for now
      }

      public void run() {
        Log.getLogWriter().info("Invoked the testhook run() method (See setupGetInitialImageTestCallbacks())");
        
        // only execute if the provider has signalled (via BB executeKill) that
        // we are running in a task.  Also, we only do the kill once in any test.
        long executeKill = DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.executeKill);
        if (executeKill == 1) {
          long giiCallbackInvocations = DeltaGIIBB.getBB().getSharedCounters().incrementAndRead(DeltaGIIBB.giiCallbackInvoked);
          if (giiCallbackInvocations == 1) {
            // let killing thread know we're ready
            synchronized(DeltaGIITest.killSyncObject) {
               Log.getLogWriter().info("Notifying killSyncObject of deltagii");
               DeltaGIITest.killSyncObject.notify();
            }
            MasterController.sleepForMs(30000);  // prevent initialization from completing (allow kill to occur)
          }
        }
      }
    }
    // setup callbacks 
    Log.getLogWriter().info("Invoking setGIITestHook(" + giiStatePrm + ")");
    GIITestHookType type = DeltaGIIPrms.getGIITestHookType(giiStatePrm);

    RegionDescription rd = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig());
    String regionName = rd.getRegionName();

    InitialImageOperation.setGIITestHook(new GetInitialImageTestCallback(type, regionName));
    Log.getLogWriter().info("Invoked setGIITestHook(" + giiStatePrm + " for " + regionName + ")");
  }

  //=================================================
  // hydra task methods
  //=================================================

  /** Hydra task to load the region with keys/values (as a batched task)
   * 
   */
  public static void HydraTask_load() {
    testInstance.load();
  }

  /** Add a number of keys to the region defined in this vm. The first key added
   *   is the key obtained by getting the next key from util.NameFactory. The last
   *   key added contains the key index specified by finalKeyIndex.
   *   
   *   @throws StopSchedulingTaskOnClientOrder once finalKeyIndex reached
   * 
   */
  private void load() {
    boolean allAddsCompleted = false;
    int finalKeyIndex = DeltaGIIPrms.numEntriesToLoad();
    Log.getLogWriter().info("Adding to region until biggest key has index " + finalKeyIndex);
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    long startTime = System.currentTimeMillis();
    int numCreated = 0;
    long nameCounter = 0;
    while (System.currentTimeMillis() - startTime < minTaskGranularityMS && !allAddsCompleted) {
      Object key = NameFactory.getNextPositiveObjectName();
      nameCounter = NameFactory.getCounterForName(key);
      numCreated++;
      if (nameCounter <= finalKeyIndex) {
        theRegion.put(key, getValueForKey(key));
      } else {
        allAddsCompleted = true;
      }
    }

    if (allAddsCompleted) {
      throw new StopSchedulingTaskOnClientOrder("Put " + finalKeyIndex +
                                                " entries in " + theRegion.getName());
    } else {
      Log.getLogWriter().info("Created " + numCreated +
                              " entries per region so far this task; number remaining to create " +
                              (finalKeyIndex - nameCounter));
    }
  }

  /** Reset the uniqueKeyIndex to be higher than the current blackboard counter (resulting from load)
   *
   */
  public static void HydraTask_resetUniqueKeyIndex() {
    resetUniqueKeyIndex();
  }

  /** set each newVersion uniqueKey blackboard value to a value higher
   *  than was used in the oldVersion ops to ensure unique keys and
   *  that new keys obtained by newVersion vms have never been used
   *  by oldVersion vms
   */
  public static void resetUniqueKeyIndex() {
    long max = NameBB.getBB().getSharedCounters().read(NameBB.POSITIVE_NAME_COUNTER);

    // set this threads key index to a value higher than max
    int incValue = TestHelper.getNumThreads();
    int myThreadId = RemoteTestModule.getCurrentThread().getThreadId();
    int myKeyIndexValue = myThreadId;
    while (myKeyIndexValue < max) {
      myKeyIndexValue += incValue;
    }
    Log.getLogWriter().info("Setting this threads uniqueKeyIndex to " + myKeyIndexValue);
    DeltaGIIBB.getBB().getSharedMap().put(DeltaGIIBB.uniqueKeyIndex + myThreadId, myKeyIndexValue);
  }

  /** Task to control the GII Provider VM (test controller)
   *
   */
  public static void HydraTask_giiProvider() throws Throwable {
    // if we haven't initialized yet, initialize (vm is restarting)
    if (testInstance == null) {    // restarted provider after failed gii ... do nothing
      // remaining threads are waiting for this vm to be restarted ... signal via pausing
      DeltaGIIBB.getBB().getSharedCounters().increment(DeltaGIIBB.pausing);
      MasterController.sleepForMs(30000);
      // stop scheduling (we don't really need to do anything after the provider is restarted)
      throw new StopSchedulingOrder("Num provider executions is " + DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.executionNumber));
    } else {
      testInstance.giiProvider();
    }
  }

  /** Allow the gii requester vms to do operations, pause them, then stop and restart
   *  them allowing them to recover/gii.
   */
  private void giiProvider() throws Exception {
    Log.getLogWriter().info("deltaGiiProvider  starts ...");
    logExecutionNumber();
    checkForLastIteration();

    // give other vms time to do random ops
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    Log.getLogWriter().info("Provider is sleeping for " + minTaskGranularityMS + " ms to allow other vms to do random ops...");
    MasterController.sleepForMs((int)minTaskGranularityMS);

    // signal to the other vms to pause
    Log.getLogWriter().info("Provider is pausing...");
    DeltaGIIBB.getBB().getSharedCounters().increment(DeltaGIIBB.pausing);
    int numProviderThreads = TestConfig.getInstance().getThreadGroup("providerThreads").getTotalThreads();
    int numRequesterThreads = TestConfig.getInstance().getThreadGroup("requesterThreads").getTotalThreads();
    TestHelper.waitForCounter(DeltaGIIBB.getBB(), "DeltaGIIBB.pausing",
        DeltaGIIBB.pausing, numProviderThreads + numRequesterThreads, true, -1, 2000);

    // wait for silence (to ensure all events are delivered)
    SummaryLogListener.waitForSilence(30, 1000);

    long leader = DeltaGIIBB.getBB().getSharedCounters().incrementAndRead(DeltaGIIBB.leader);
    Log.getLogWriter().info("Provider is leader: " + leader);
    if (leader == 1) { // this is the thread to write the snapshot
      // now stop requester VMs and bring them back for recovery (to cause gii)
      List<ClientVmInfo> requesterVMs = StopStartVMs.getAllVMs();
      requesterVMs = StopStartVMs.getMatchVMs(requesterVMs, "requester");

      List<String> stopModesList = new ArrayList();
      for (Object vmInfo: requesterVMs) {
        stopModesList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
      }
      Log.getLogWriter().info("Provider is stopping the requesters: " + requesterVMs);
      StopStartVMs.stopVMs(requesterVMs, stopModesList);

      DeltaGIIBB.getBB().getSharedCounters().zero(DeltaGIIBB.pausing);
      DeltaGIIBB.getBB().getSharedCounters().zero(DeltaGIIBB.writeSnapshot);
      DeltaGIIBB.getBB().getSharedCounters().zero(DeltaGIIBB.snapshotWritten);
      DeltaGIIBB.getBB().getSharedCounters().zero(DeltaGIIBB.clears);
      DeltaGIIBB.getBB().getSharedCounters().zero(DeltaGIIBB.doneVerifying);

      // Do providerOperations (if providerDoesOperations is true)
      if (DeltaGIIPrms.providerDoesOps()) {
        Log.getLogWriter().info("Provider is executing providerOperations while requester vms are offline. Ops:" + DeltaGIIPrms.getProviderOperations());
        doOperations(DeltaGIIPrms.providerOperations, 10000);
        Log.getLogWriter().info("Provider is done executing providerOperations while requester vms are offline. Ops:" + DeltaGIIPrms.getProviderOperations());
      }

      // provider writes the snapshot after it completes its optional set of operations (while requester vms are offline)
      Log.getLogWriter().info("Provider is writing the snapshot");
      writeSnapshot();

      // wait for tombstone-gc-timeout ...
      if (DeltaGIIPrms.doTombstoneGC()) {
        String regionName = theRegion.getName();
        if (theRegion.getAttributes().getDataPolicy().withPartitioning()) {
          regionName = "partition-" + regionName;
        }
        TombstoneService ts = ((GemFireCacheImpl)theCache).getTombstoneService();
        int tombstoneCnt = getStat_tombstones(regionName);
        Log.getLogWriter().info("Provider is attempting to GC tombstones. Before tombstoneCnt=" + tombstoneCnt);
        boolean result = ts.forceBatchExpirationForTests(tombstoneCnt);
        Log.getLogWriter().info("Provider is finished GCing tombstones. After tombstoneCnt=" + getStat_tombstones(regionName) + " result=" + result);
      }

      // restart all vms; init tasks for all vms compare against the blackboard, so if
      // this returns then all is well
      // targetVMs can contain requester and newMember vms (test dependent), all VMs except the provider
      List<ClientVmInfo> targetVMs = StopStartVMs.getAllVMs();
      targetVMs.removeAll(StopStartVMs.getMatchVMs(targetVMs, "provider"));

      // signal to callback hook that we want to do the kill if invoked
      // note that we only kill once per test, so we don't need to reset this
      DeltaGIIBB.getBB().getSharedCounters().increment(DeltaGIIBB.executeKill);

      // monitor (wait) on killSyncObject (when deltaGiiTestHook invokes callback to kill vm)
      startCallbackMonitor();

      Log.getLogWriter().info("Provider is starting the requesters: " + targetVMs);
      StopStartVMs.startVMs(targetVMs);

      // see if it's time to stop the test
      long counter = DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.timeToStop);
      if (counter >= 1) {
        throw new StopSchedulingOrder("It's time to stop the test, Provider execution number is " +
            DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.executionNumber));
      }

      // clear the leader counter (for next round of execution)
      DeltaGIIBB.getBB().getSharedCounters().zero(DeltaGIIBB.leader);
    } else {
        // we don't know which provider will be the target of the kill (source of the gii)
        // monitor (wait) on killSyncObject (when deltaGiiTestHook invokes callback to kill vm)
        startCallbackMonitor();
        MasterController.sleepForMs(30000);
    }
  }

  //=============================================================================
  // The the GIITestHook callback executes a 'notify' on this syncObject to
  // coordinate between non hydra threads and a hydra thread which can do a 
  // dynamic stop/start operation
  //=============================================================================
  public static Object killSyncObject = new Object();

  private static void startCallbackMonitor() {
    // in kill tests, setup a thread to monitor killSyncObject (so we can kill this vm when callback invoked)
    Thread killThread = new Thread(new Runnable() {
      public void run() {

        Log.getLogWriter().info("startCallbackMonitor spawning HydraSubthread to wait on killSyncObject");
        String giiStatePrm = (String)DeltaGIIBB.getBB().getSharedMap().get(DeltaGIIBB.giiStatePrm);
        synchronized(DeltaGIITest.killSyncObject) {
           try {
             Log.getLogWriter().info("killThread waiting on killSyncObject for giiStatePrm = " + giiStatePrm);
             DeltaGIITest.killSyncObject.wait();
           } catch (InterruptedException e) {
             Log.getLogWriter().info("killThread interrupted: VM not killed");
           }
        }

        Log.getLogWriter().info("product invoked GetInitialImageTestCallback, mean killing and restarting this VM ...");
        try {
          ClientVmMgr.stopAsync("product invoked GetInitialImageTestCallback, killing this vm",
                                ClientVmMgr.MEAN_KILL,
                                ClientVmMgr.IMMEDIATE);
        } catch (ClientVmNotFoundException e) {
          throw new TestException("Unexpected Exception " + e + " caught while attempting to recycle vm " + TestHelper.getStackTrace(e));
        }
      }
      }, "Test Callback Monitor");
    killThread = new HydraSubthread(killThread);
    killThread.start();
  }

  /** Task to respond to the provider's (controller's) commands
   *
   */
  public static void HydraTask_giiRequester() throws Throwable {
    // if we haven't initialized yet, initialize (vm is restarting)
    if (testInstance == null) {
      // monitor (wait) on killSyncObject (when deltaGiiTestHook invokes my call back to kill vm)
      startCallbackMonitor();

      // initialize
      HydraTask_initialize();

      // verify from snapshot
      HydraTask_verifyFromSnapshotAndSync();

      // verify deltagii
      HydraTask_verifyDeltaGII();
    } 
    testInstance.giiRequester();
  }

  /** Do operations and pause when directed by the image provider
   */
  private void giiRequester() throws Exception {
    long pausing = DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.pausing);
    Log.getLogWriter().info("deltaGiiRequester starts ... pausing=" + pausing);

    if (pausing > 0) { // provider has signaled to pause
      if ((Boolean) threadIsPaused.get()) { // this thread has already paused, so don't increment counter
        Log.getLogWriter().info("Requester thread has paused");
      } else {
        String regionName = theRegion.getName();
        if (theRegion.getAttributes().getDataPolicy().withPartitioning()) {
          regionName = "partition-" + regionName;
        }
        int tombstoneCnt = getStat_tombstones(regionName);
        String key = DeltaGIIBB.tombstoneCount + RemoteTestModule.getCurrentThread().getThreadId();
        Log.getLogWriter().info("Requester thread is pausing, saving tombstone count: setting '" + key +
                                "', to " + tombstoneCnt);
        threadIsPaused.set(new Boolean(true));
        DeltaGIIBB.getBB().getSharedMap().put(key, tombstoneCnt);
        DeltaGIIBB.getBB().getSharedCounters().increment(DeltaGIIBB.pausing);
      }
      MasterController.sleepForMs(5000);
    } else { // not pausing
      long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
      Log.getLogWriter().info("Requester thread is gonna do operations...");
      doOperations(minTaskGranularityMS);
    }
  }

  /** Task for newMembers (increment pausing so we won't wait for these threads).
   *
   */
  public static synchronized void HydraTask_giiNewMember() throws Exception {
    // if we haven't initialized yet, kill the vm (newMembers do gii on dynamic restart only)
    if (testInstance == null) {
       ClientVmMgr.stop("Killing self (newMember): test restarts this VM on demand" , ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND);
    } else {
      testInstance.giiNewMember();
    }
  }

  /** initialize thread locals (for pausing) and increment pausing counter
   */
  private void giiNewMember() throws Exception {
    Log.getLogWriter().info("giiNewMember starts ...");
    testInstance.initializeInstancePerThread();
    
    long pausing = DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.pausing);
    if (pausing > 0) { // provider has signaled to pause
      if ((Boolean)threadIsPaused.get()) { // this thread has already paused, so don't increment counter
        Log.getLogWriter().info("Thread has paused");
      } else {
        Log.getLogWriter().info("This thread is pausing");
        threadIsPaused.set(new Boolean(true));
        DeltaGIIBB.getBB().getSharedCounters().increment(DeltaGIIBB.pausing);
      }
      MasterController.sleepForMs(5000);
    } else { // not pausing
      MasterController.sleepForMs(30000);
    }
  }

  //=================================================
  // Blackboard snapshot methods
  //=================================================

  public static void HydraTask_writeSnapshot() {
    testInstance.writeSnapshot();
  }

  /**
   * Write the region snapshot to the BB (includes only persistent regions)
   */
  protected void writeSnapshot() {
    Log.getLogWriter().info("Preparing to write snapshot for " + theRegion.getName());
    Map regionSnapshot = new HashMap();
    for (Object key: theRegion.keySet()) {
      Object value = null;
      if (theRegion.containsValueForKey(key)) { // won't invoke a loader (if any)
        value = theRegion.get(key);
      }
      if (value instanceof BaseValueHolder) {
        regionSnapshot.put(key, ((BaseValueHolder)value).myValue);
      } else if (value instanceof PdxInstance) {
        BaseValueHolder vh = PdxTest.toValueHolder(value);
        regionSnapshot.put(key, vh.myValue);
      } else {
        regionSnapshot.put(key, value);
      }
    } 
    Log.getLogWriter().info("Region snapshot for " + theRegion.getFullPath() + " is size " + regionSnapshot.size() + " and contains keys " + regionSnapshot.keySet());
    DeltaGIIBB.getBB().getSharedMap().put(DeltaGIIBB.regionSnapshotKey, regionSnapshot);
    Log.getLogWriter().info("vm_" + RemoteTestModule.getMyVmid() + " wrote snapshot for " + theRegion.getName() + " into blackboard at key " + DeltaGIIBB.regionSnapshotKey);
  }

  /** Verify region from the snapshot and wait for all threads to complete verify
   *
   */
  public static void HydraTask_verifyFromSnapshotAndSync() {
    PdxTest.initClassLoader();
    testInstance.verifyFromSnapshot();
    DeltaGIIBB.getBB().getSharedCounters().increment(DeltaGIIBB.doneVerifying);
    TestHelper.waitForCounter(DeltaGIIBB.getBB(), "DeltaGIIBB.doneVerifying",
        DeltaGIIBB.doneVerifying,  RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads(),
        true, -1, 2000);
  }
  
  /** Verify all regions from the snapshot
   * 
   */
  public static void HydraTask_verifyFromSnapshot() {
    PdxTest.initClassLoader();
    testInstance.verifyFromSnapshot();
  }

  /** For each region written to the blackboard, verify its state against the region
   *   in this vm. 
   * 
   */
  protected void verifyFromSnapshot() {
    Map regionSnapshot = (Map)DeltaGIIBB.getBB().getSharedMap().get(DeltaGIIBB.regionSnapshotKey);
    verifyFromSnapshot(theRegion, regionSnapshot);
  }

  /** Verify that the given region is consistent with the given snapshot.
   * 
   * @param theRegion The region to verify
   * @param snapshot The expected contents of theRegion.
   */
  public void verifyFromSnapshot(Region theRegion, Map regionSnapshot) {
    // init
    StringBuffer errStr = new StringBuffer();
    int snapshotSize = regionSnapshot.size();
    int regionSize = theRegion.size();
    long startVerifyTime = System.currentTimeMillis();
    Log.getLogWriter().info("Verifying " + theRegion.getFullPath() + " of size " + theRegion.size() +
        " against snapshot containing " + regionSnapshot.size() + " entries...");

    // verify
    if (snapshotSize != regionSize) {
      ((LocalRegion)theRegion).dumpBackingMap();
      errStr.append("Expected region " + theRegion.getFullPath() + " to be size " + snapshotSize + 
          ", but it is " + regionSize + "\n");
    }
    for (Object key: regionSnapshot.keySet()) {
      // validate using ParRegUtil calls even if the region is not PR; these calls do the desired job
      // containsKey
      try {
        ParRegUtil.verifyContainsKey(theRegion, key, true);
      } catch (TestException e) {
        errStr.append(e.getMessage() + "\n");
      }

      // containsValueForKey
      boolean containsValueForKey = theRegion.containsValueForKey(key);
      Object expectedValue = regionSnapshot.get(key);
      try {
        ParRegUtil.verifyContainsValueForKey(theRegion, key, (expectedValue != null));
      } catch (TestException e) {
        errStr.append(e.getMessage() + "\n");
      }

      // check the value
      if (containsValueForKey) {
        try {
          Object actualValue = theRegion.get(key);
          
          if (actualValue instanceof BaseValueHolder) {
            ParRegUtil.verifyMyValue(key, expectedValue, actualValue, ParRegUtil.EQUAL);
          } else if (actualValue instanceof PdxInstance) {
            actualValue = PdxTest.toValueHolder(actualValue);
            ParRegUtil.verifyMyValue(key, expectedValue, actualValue, ParRegUtil.EQUAL);
          } else {
            if ((actualValue instanceof byte[]) &&
                (expectedValue instanceof byte[])) {
              byte[] actual = (byte[])actualValue;
              byte[] expected = (byte[])expectedValue;
              if (actual.length != expected.length) {
                throw new TestException("Expected value for key " + key + " to be " +
                    TestHelper.toString(expectedValue) + ", but it is " +
                    TestHelper.toString(actualValue));
              }
            } else {
              throw new TestException("Expected value for key " + key + " to be " +
                  TestHelper.toString(expectedValue) + ", but it is " +
                  TestHelper.toString(actualValue));
            }
          }
        } catch (TestException e) {
          errStr.append(e.getMessage() + "\n");
        }
      } 
      // else value in region is null; the above check for containsValueForKey
      // checked that  the snapshot is also null
    }

    // check for extra keys in the region that were not in the snapshot
    Set theRegionKeySet = new HashSet(theRegion.keySet()); 
    Set snapshotKeySet = regionSnapshot.keySet();
    theRegionKeySet.removeAll(snapshotKeySet);
    if (theRegionKeySet.size() != 0) {
      errStr.append("Found the following unexpected keys in " + theRegion.getFullPath() + 
          ": " + theRegionKeySet + "\n");
    }

    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
    Log.getLogWriter().info("Done verifying " + theRegion.getFullPath() + " from snapshot containing " + snapshotSize + " entries, " +
        "verification took " + (System.currentTimeMillis() - startVerifyTime) + "ms");
  }

  /** Verify regions that are PRs 
   *
   */
  public static void HydraTask_verifyPR() {
    StringBuffer aStr = new StringBuffer();
    if (theRegion.getAttributes().getDataPolicy().withPartitioning()) {
      int redundantCopies = theRegion.getAttributes().getPartitionAttributes().getRedundantCopies();
      Log.getLogWriter().info("Verifying PR " + theRegion.getFullPath() + " with " + redundantCopies + " redundantCopies");
      // verify PR metadata 
      try {
        ParRegUtil.verifyPRMetaData(theRegion);
      } catch (Exception e) {
        aStr.append(TestHelper.getStackTrace(e) + "\n");
      } catch (TestException e) {
        aStr.append(TestHelper.getStackTrace(e) + "\n");
      }

      // verify primaries
      try {
        ParRegUtil.verifyPrimaries(theRegion, redundantCopies);
      } catch (Exception e) {
        aStr.append(e.toString() + "\n");
      }

      // verify PR data
      try {
        ParRegUtil.verifyBucketCopies(theRegion, redundantCopies);
      } catch (Exception e) {
        aStr.append(e.toString() + "\n");
      } catch (TestException e) {
        aStr.append(e.toString() + "\n");
      }

      if (aStr.length() > 0) {
        // shutdownHook will cause all members to dump partitioned region info
        throw new TestException(aStr.toString());
      }
      Log.getLogWriter().info("Done verifying PR internal consistency for " + theRegion.getFullPath());
    }
  }

  //=================================================
  // Operation HydraTasks
  //=================================================
  
  /** Task to do random operations
   * 
   */
  public static void HydraTask_doOperations() throws Exception {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    testInstance.doOperations(minTaskGranularityMS);
  }
  
  /** Do random operations on the test region
   * 
   *   @param msToRun The number of milliseconds to run operations before returning.
   */
  protected void doOperations(long msToRun) throws Exception {
     testInstance.doOperations(DeltaGIIPrms.operations, msToRun);
  }

  /** Do random operations on the test region
   * 
   *   @param opsPrm  Specifies which list of operations to execute (provider vs. requester operations)
   *   @param msToRun The number of milliseconds to run operations before returning.
   */
  protected void doOperations(Long opsPrm, long msToRun) throws Exception {
    Log.getLogWriter().info("In doOperations, running for " + msToRun + " milliseconds");
    long startTime = System.currentTimeMillis();
    int numOps = 0;
    do {
      try {
        int whichOp = getOperation(opsPrm, theRegion);
        switch (whichOp) {
        case ENTRY_ADD_OPERATION:
          addEntry(theRegion);
          break;
        case ENTRY_INVALIDATE_OPERATION:
          invalidateEntry(theRegion, false);
          break;
        case ENTRY_DESTROY_OPERATION:
          destroyEntry(theRegion, false);
          break;
        case ENTRY_UPDATE_OPERATION:
          updateEntry(theRegion);
          break;
        case ENTRY_GET_OPERATION:
          getExistingKey(theRegion);
          break;
        case ENTRY_GET_NEW_OPERATION:
          getNewKey(theRegion);
          break;
        case ENTRY_LOCAL_INVALIDATE_OPERATION:
          invalidateEntry(theRegion, true);
          break;
        case ENTRY_LOCAL_DESTROY_OPERATION:
          destroyEntry(theRegion, true);
          break;
        case ENTRY_PUTALL_OPERATION:
          putAll(theRegion);
          break;
        case CLEAR_REGION:
          theRegion = clearRegion();  // clearRegion returns the non-partitioned region it cleared
          break;
        default: {
          throw new TestException("Unknown operation " + whichOp);
        }
        }
        numOps++;
        Log.getLogWriter().info("Completed op " + numOps + " for this task");
      } catch (EntryExistsException e) {
        if (uniqueKeys) {
           throw new TestException(TestHelper.getStackTrace(e));
        } else {
           Log.getLogWriter().info("Caught " + e + ", expected with multiple writers (uniqueKeys==false), continuing test");
        }
      } catch (EntryNotFoundException e) {
        if (uniqueKeys) {
          boolean allowENFE = false;
          // if datahosts are being recycled, we could lose the only copy of the data
          long stoppingVMs = DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.stoppingVMs);
          if (stoppingVMs > 0) {
            allowENFE = true;
            Log.getLogWriter().info("Caught " + e + ", expected with HA, continuing test");
          } else {
            // This could happen with concurrent execution and Region.clear()
            long clearRegion = DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.clears);
            if (clearRegion > 0 && isClearedRegion(theRegion)) {
                allowENFE = true;
                Log.getLogWriter().info("Caught " + e + ", due to concurrent clear on " + theRegion.getFullPath() + ", continuing test");
            }
          }
          if (!allowENFE) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        } else {
           Log.getLogWriter().info("Caught " + e + ", expected with multiple writers (uniqueKeys==false), continuing test");
        }
      } catch (Exception e) {
          throw e;
      }
    } while ((System.currentTimeMillis() - startTime < msToRun));
    Log.getLogWriter().info("Done in doOperations");
  }

  /** Get an operation to perform on the region
   *  @param reg The region to get an operation for. 
   *  @returns A random operation.
   */
  protected int getOperation(Long opsPrm, Region theRegion) {
    Long upperThresholdOpsPrm = DeltaGIIPrms.upperThresholdOperations;
    Long lowerThresholdOpsPrm = DeltaGIIPrms.lowerThresholdOperations;
    int upperThreshold = DeltaGIIPrms.getUpperThreshold();
    int lowerThreshold = DeltaGIIPrms.getLowerThreshold();
    if (isBridgeClient) {
      opsPrm = DeltaGIIPrms.clientOperations; 
      upperThresholdOpsPrm = DeltaGIIPrms.upperThresholdClientOperations; 
      lowerThresholdOpsPrm = DeltaGIIPrms.lowerThresholdClientOperations; 
      upperThreshold = DeltaGIIPrms.getUpperThresholdClient();
      lowerThreshold = DeltaGIIPrms.getLowerThresholdClient();
    }
    if (opsPrm != DeltaGIIPrms.providerOperations) {
      int size = theRegion.size();
      if (size >= upperThreshold) {
        return getOperation(upperThresholdOpsPrm);
      } else if (size <= lowerThreshold) {
        return getOperation(lowerThresholdOpsPrm);
      } 
    }
    return getOperation(opsPrm);
  }      

  /** Get a random operation using the given hydra parameter.
   *
   *  @param whichPrm A hydra parameter which specifies random operations.
   *
   *  @returns A random operation.
   */
  protected int getOperation(Long whichPrm) {
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    if (operation.equals("add")) {
      op = ENTRY_ADD_OPERATION;
    } else if (operation.equals("update")) {
      op = ENTRY_UPDATE_OPERATION;
    } else if (operation.equals("invalidate")) {
      op = ENTRY_INVALIDATE_OPERATION;
    } else if (operation.equals("destroy")) {
      op = ENTRY_DESTROY_OPERATION;
    } else if (operation.equals("get")) {
      op = ENTRY_GET_OPERATION;
    } else if (operation.equals("getNew")) {
      op = ENTRY_GET_NEW_OPERATION;
    } else if (operation.equals("localInvalidate")) {
      op = ENTRY_LOCAL_INVALIDATE_OPERATION;
    } else if (operation.equals("localDestroy")) {
      op = ENTRY_LOCAL_DESTROY_OPERATION;
    } else if (operation.equals("putAll")) {
      op = ENTRY_PUTALL_OPERATION;
    } else if (operation.equals("cacheOperations")) {
      op = CACHE_OPERATIONS;
    } else if (operation.equals("clear")) {
      op = CLEAR_REGION;
    } else {
      throw new TestException("Unknown entry operation: " + operation);
    }
    return op;
  }

  /** Does a get operation on an existing key.
   *
   *  @param theRegion The region to perform the get on.
   *
   */
  protected void getExistingKey(Region theRegion) {
    Object key = obtainExistingKey(theRegion);
    if (key == null) {
      Log.getLogWriter().info("No keys available in " + theRegion.getFullPath() + ", cannot do get on existing key");
      return;
    }
    Log.getLogWriter().info("Getting existing key " + key + " from region " + theRegion.getFullPath());
    Object result = theRegion.get(key);
    Log.getLogWriter().info("Done getting existing key " + key + ", returned value " + result);
  }

  /** Add a new entry to the given region.
   *
   *  @param theRegion The region to use for adding a new entry.
   *
   *  @returns The key that was added.
   */
  protected Object addEntry(Region theRegion) {
    Object key = obtainNewKey();
    addEntry(theRegion, key);
    return key;
  }

  /** Add a new entry to the given region.
   *
   *  @param theRegion The region to use for adding a new entry.
   *  @param key The key to add.
   */
  protected void addEntry(Region theRegion, Object key) {
    Object anObj = getValueForKey(key);
    String callback = createCallbackPrefix + ProcessMgr.getProcessId();
    if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call with cacheWriter arg
        Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
            TestHelper.toString(anObj) + " cacheWriterParam is " + callback + ", region is " +
            theRegion.getFullPath());
        theRegion.create(key, anObj, callback);
        Log.getLogWriter().info("addEntry: done creating key " + key);
      } else { // use create with no cacheWriter arg
        Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
            TestHelper.toString(anObj) + ", region is " + theRegion.getFullPath());
        theRegion.create(key, anObj);
        Log.getLogWriter().info("addEntry: done creating key " + key);
      }
    } else { // use a put call
      Object returnVal = null;
      if (TestConfig.tab().getRandGen().nextBoolean()) { // use a put call with callback arg
        Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
            TestHelper.toString(anObj) + " callback is " + callback + ", region is " + theRegion.getFullPath());
        returnVal = theRegion.put(key, anObj, callback);
        Log.getLogWriter().info("addEntry: done putting key " + key + ", returnVal is " + returnVal);
      } else {
        Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
            TestHelper.toString(anObj) + ", region is " + theRegion.getFullPath());
        returnVal = theRegion.put(key, anObj);
        Log.getLogWriter().info("addEntry: done putting key " + key + ", returnVal is " + returnVal);
      }
    }
  }

  /** putall a map to the given region.
   *
   *  @param theRegion The region to use for putall a map.
   *
   */
  protected void putAll(Region theRegion) {
    // determine the number of new keys to put in the putAll
    Map mapToPut = getPutAllMap(theRegion);

    // do the putAll
    Log.getLogWriter().info("putAll: calling putAll with map of " + mapToPut.size() + " entries, region is " + theRegion.getFullPath());
    theRegion.putAll(mapToPut);
    Log.getLogWriter().info("putAll: done calling putAll with map of " + mapToPut.size() + " entries");
  }

  /** Get a map to use for putAll for the given region. Make sure the values in the
   *   map  honor unique keys.
   *
   * @param theRegion The region to consider for the putAll.
   * @return Map The map to use for the argument to putAll
   */
  protected Map getPutAllMap(Region theRegion) {
    // determine the number of new keys to put in the putAll
    int numPutAllNewKeys = DeltaGIIPrms.getNumPutAllNewKeys();

    // get a map to use with putAll; add new keys to it
    Map mapToPut = new HashMap();
    StringBuffer newKeys = new StringBuffer();
    for (int i = 1; i <= numPutAllNewKeys; i++) { // put new keys
      Object key = obtainNewKey();
      Object anObj = getValueForKey(key);
      mapToPut.put(key, anObj);
      newKeys.append(key + " ");
      if ((i % 10) == 0) {
        newKeys.append("\n");
      }
    }
    // add existing keys to the map
    int numPutAllExistingKeys = TestConfig.tab().intAt(DeltaGIIPrms.numPutAllExistingKeys);
    List<String> existingKeysList = obtainExistingKeys(theRegion, numPutAllExistingKeys);
    StringBuffer existingKeys = new StringBuffer();
    int count = 0;
    for (String key: existingKeysList) { // put existing keys
      Object anObj = getUpdateObject(theRegion, key);
      mapToPut.put(key, anObj);
      existingKeys.append(key + " ");
      count++;
      if ((count % 10) == 0) {
        existingKeys.append("\n");
      }
    }
    Log.getLogWriter().info(theRegion.getFullPath() + " size is " + theRegion.size() + ", map to use as argument to putAll is " +
        mapToPut.getClass().getName() + " containing " + numPutAllNewKeys + " new keys and " +
        numPutAllExistingKeys+ " existing keys (updates); total map size is " + mapToPut.size() +
        "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);
    return mapToPut;
  }

  /** Invalidate an entry in the given region.
   *
   *  @param theRegion The region to use for invalidating an entry.
   *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
   */
  protected void invalidateEntry(Region theRegion, boolean isLocalInvalidate) {
    Object key = obtainExistingKey(theRegion);
    if (key == null) {
      Log.getLogWriter().info("invalidateEntry: No keys in region " + theRegion.getFullPath());
      return;
    }
    invalidateEntry(theRegion, key, isLocalInvalidate);
  }

  /** Invalidate an entry in the given region.
   *
   *  @param theRegion The region to use for invalidating an entry.
   *  @param key The key to invalidate.
   *  @param isLocalInvalidate True if the invalidate should be local, false otherwise.
   */
  protected void invalidateEntry(Region theRegion, Object key, boolean isLocalInvalidate) {
    String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId();
    if (isLocalInvalidate) { // do a local invalidate
      if (TestConfig.tab().getRandGen().nextBoolean()) { // local invalidate with callback
        Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + " callback is " + callback
            + ", region is " + theRegion.getFullPath());
        theRegion.localInvalidate(key, callback);
        Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
      } else { // local invalidate without callback
        Log.getLogWriter().info("invalidateEntry: local invalidate for " + key + ", region is " + theRegion.getFullPath());
        theRegion.localInvalidate(key);
        Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
      }
    } else { // do a distributed invalidate
      if (TestConfig.tab().getRandGen().nextBoolean()) { // invalidate with callback
        Log.getLogWriter().info("invalidateEntry: invalidating key " + key + " callback is " + callback
            + ", region is " + theRegion.getFullPath());
        theRegion.invalidate(key, callback);
        Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
      } else { // invalidate without callback
        Log.getLogWriter().info("invalidateEntry: invalidating key " + key + ", theRegion is " + theRegion.getFullPath());
        theRegion.invalidate(key);
        Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
      }
    }
  }

  /** Destroy an entry in the given region.
   *
   *  @param theRegion The region to use for destroying an entry.
   *  @param isLocalDestroy True if the destroy should be local, false otherwise.
   */
  protected void destroyEntry(Region theRegion, boolean isLocalDestroy) {
    Object key = obtainExistingKey(theRegion);
    if (key == null) {
      Log.getLogWriter().info("destroyEntry: No keys in region " + theRegion.getFullPath());
      return;
    }
    destroyEntry(theRegion, key, isLocalDestroy);
  }

  /** Destroy an entry in the given region.
   *
   *  @param theRegion The region to use for destroying an entry.
   *  @param key The key to destroy.
   *  @param isLocalDestroy True if the destroy should be local, false otherwise.
   */
  protected void destroyEntry(Region theRegion, Object key, boolean isLocalDestroy) {
    String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
    if (isLocalDestroy) { // do a local destroy
      if (TestConfig.tab().getRandGen().nextBoolean()) { // local destroy with callback
        Log.getLogWriter().info("destroyEntry: local destroy for " + key + " callback is " + callback
            + ", region is " + theRegion.getFullPath());
        theRegion.localDestroy(key, callback);
        Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
      } else { // local destroy without callback
        Log.getLogWriter().info("destroyEntry: local destroy for " + key + ", region is " + theRegion.getFullPath());
        theRegion.localDestroy(key);
        Log.getLogWriter().info("destroyEntry: done with local destroy for " + key);
      }
    } else { // do a distributed destroy
      if (TestConfig.tab().getRandGen().nextBoolean()) { // destroy with callback
        Log.getLogWriter().info("destroyEntry: destroying key " + key + " callback is " + callback
            + ", region is " + theRegion.getFullPath());
        theRegion.destroy(key, callback);
        Log.getLogWriter().info("destroyEntry: done destroying key " + key);
      } else { // destroy without callback
        Log.getLogWriter().info("destroyEntry: destroying key " + key + ", region is " + theRegion.getFullPath());
        theRegion.destroy(key);
        Log.getLogWriter().info("destroyEntry: done destroying key " + key);
      }
    }
  }

  /** Update an existing entry in the given region. If there are
   *  no available keys in the region, then this is a noop.
   *
   *  @param theRegion The region to use for updating an entry.
   */
  protected void updateEntry(Region theRegion) {
    Object key = obtainExistingKey(theRegion);
    if (key == null) {
      Log.getLogWriter().info("updateEntry: No keys in region " + theRegion.getFullPath());
      return;
    }
    updateEntry(theRegion, key);
  }

  /** Update an existing entry in the given region. If there are
   *  no available keys in the region, then this is a noop.
   *
   *  @param theRegion The region to use for updating an entry.
   *  @param key The key to update.
   */
  protected void updateEntry(Region theRegion, Object key) {
    Object anObj = getUpdateObject(theRegion, (String)key);
    String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
    Object returnVal = null;
    if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
          TestHelper.toString(anObj) + ", callback is " + callback
          + ", region is " + theRegion.getFullPath());
      returnVal = theRegion.put(key, anObj, callback);
      Log.getLogWriter().info("Done with call to put (update), returnVal is " + returnVal);
    } else { // do a put without callback
      Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + TestHelper.toString(anObj)
          + ", region is " + theRegion.getFullPath());
      returnVal = theRegion.put(key, anObj, false);
      Log.getLogWriter().info("Done with call to put (update), returnVal is " + returnVal);
    }   

  }

  /** Get a new key int the given region.
   *
   *  @param theRegion The region to use for getting an entry.
   */
  protected void getNewKey(Region theRegion) {
    Object key = obtainNewKey();
    getThisNewKey(theRegion, key);
  }

  /** Do a get on the given new key.
   *
   *  @param theRegion The region to use for getting an entry.
   *  @param key A new key never used before.
   */
  protected void getThisNewKey(Region theRegion, Object key) {
    String callback = getCallbackPrefix + ProcessMgr.getProcessId();
    Object anObj;
    if (TestConfig.tab().getRandGen().nextBoolean()) { // get with callback
      Log.getLogWriter().info("getNewKey: getting new key " + key + ", callback is " + callback
          + ", region is " + theRegion.getFullPath());
      anObj = theRegion.get(key, callback);
    } else { // get without callback
      Log.getLogWriter().info("getNewKey: getting new key " + key + ", region is " + theRegion.getFullPath());
      anObj = theRegion.get(key);
    }
    Log.getLogWriter().info("getNewKey: done getting value for new key " + key + ": " + TestHelper.toString(anObj));
  }

  /** Return a random key from theRegion
   *
   * @param theRegion The region to get the existing key from.
   * @return A random key from theRegion.
   */
  protected Object obtainExistingKey(Region theRegion) {
    if (theRegion.size() == 0) {
      return null;
    }
    Object[] keyArr = theRegion.keySet().toArray();
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    int numThreads = TestHelper.getNumThreads();
    for (Object key: keyArr) {
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

  /** Return a random key from theRegion
   * 
   * @param theRegion The region to get the existing key from. 
   * @return A List of existing keys.
   */
  protected List<String> obtainExistingKeys(Region theRegion, int numToObtain) {
    List aList = new ArrayList();
    if (theRegion.size() == 0) {
      return aList;
    }
    Object[] keyArr = theRegion.keySet().toArray();
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    int numThreads = TestHelper.getNumThreads();
    for (Object key: keyArr) {
      if (uniqueKeys) {
        long keyIndex = NameFactory.getCounterForName(key);
        if ((keyIndex % numThreads) == myTid) {
          aList.add(key);
          if (aList.size() == numToObtain) {
            return aList;
          }
        }
      } else {
        aList.add(key);
        if (aList.size() == numToObtain) {
          return aList;
        }
      }
    }
    return aList;
  }

  /** Invoke region.clear() - only one region is cleared during any round of execution 
   *  Note that Partitioned Regions do not support clear
   */
  protected Region clearRegion() {
    long numClears = DeltaGIIBB.getBB().getSharedCounters().incrementAndRead(DeltaGIIBB.clears);
    if (numClears == 1) {
      if (theRegion != null) {
        String fullPathName = theRegion.getFullPath();
        DeltaGIIBB.getBB().addToClearedRegionList(fullPathName);
        Log.getLogWriter().info("Starting clearRegion on " + fullPathName);
        theRegion.clear();
        Log.getLogWriter().info("Completed clearRegion on " + fullPathName);
      } else {
        Log.getLogWriter().info("clearRegion invoked, but there are no non-PartitionedRegions in this test");
      }
    }
    return theRegion;
  }

  /** Whether or not the given Region was the target of a Region.clear().
   *
   */
  protected boolean isClearedRegion(Region theRegion) {
    boolean cleared = false;
    Set clearedRegions = (Set)DeltaGIIBB.getBB().getSharedMap().get(DeltaGIIBB.clearedRegions);
    if (clearedRegions != null) {
      if (clearedRegions.contains(theRegion.getFullPath())) {
        cleared = true;
      }
    } 
    return cleared;
  }

  /** Return a new key, never before used in the test.
   */
  protected Object obtainNewKey() {
    if (uniqueKeys) {
      int thisThreadID = RemoteTestModule.getCurrentThread().getThreadId();
      int anInt = (Integer)(DeltaGIIBB.getBB().getSharedMap().get(DeltaGIIBB.uniqueKeyIndex + thisThreadID));
      anInt += TestHelper.getNumThreads();
      DeltaGIIBB.getBB().getSharedMap().put(DeltaGIIBB.uniqueKeyIndex + thisThreadID, anInt);
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, anInt);
      return NameFactory.getObjectNameForCounter(anInt);
    } else {
      return NameFactory.getNextPositiveObjectName();
    }
  }

  /** Randomly choose between a ValueHolder and the object that is
   *  the extra value in the ValueHolder so the tests can put either
   *  ValueHolders or something else (such as byte[])
   */
  private Object getValueForKey(Object key) {
    String valueClassName = DeltaGIIPrms.getValueClassName();
    BaseValueHolder vh = null;
    if (valueClassName.equals(ValueHolder.class.getName())) {
      vh = new ValueHolder((String)key, randomValues);
    } else if (valueClassName.equals(VHDataSerializable.class.getName())) {
      vh = new VHDataSerializable((String)key, randomValues);
    } else if (valueClassName.equals(VHDataSerializableInstantiator.class.getName())) {
      vh = new VHDataSerializableInstantiator((String)key, randomValues);
    } else if ((valueClassName.equals("util.PdxVersionedValueHolder")) ||
               (valueClassName.equals("util.VersionedValueHolder"))) {
      vh = PdxTest.getVersionedValueHolder(valueClassName, (String)key, randomValues);
      
      return vh; // do not allow this method the option of returning the extraObject (below)
    } else {
      throw new TestException("Test does not support DeltaGIIPrms.valueClassName " + valueClassName);
    }
    if (TestConfig.tab().getRandGen().nextBoolean()) {
      Object anObj = vh.extraObject;
      if (anObj == null) {
        throw new TestException("Test config problem: ValueHolder does not have extraObj");
      }
      return anObj;
    } else {
      return vh;
    }
  }

  /** Return an object to be used to update the given key. If the
   *  value for the key is a ValueHolder, then get an alternate
   *  value which is similar to it's previous value (see
   *  ValueHolder.getAlternateValueHolder()).
   *
   *  @param theRegion The region which possible contains key.
   *  @param key The key to get a new value for.
   *  
   *  @returns An update to be used to update key in theRegion.
   */
  protected Object getUpdateObject(Region r, String key) {
    Object anObj = r.get(key);
    Object newObj = null;
    if ((anObj == null) || (!(anObj instanceof BaseValueHolder))) {
      newObj = getValueForKey(key);
    } else {
      newObj = ((BaseValueHolder)anObj).getAlternateValueHolder(randomValues);
    }
    if ((newObj instanceof BaseValueHolder) && TestConfig.tab().getRandGen().nextBoolean()) {
      return ((BaseValueHolder)newObj).extraObject;
    } else {
      return newObj;
    }
  }

  //=================================================
  // methods to access stats
  //=================================================

  /**
   * Returns the current stat value of CachePerfStats.tombstones This is done by getting the stat directly without going
   * to the archive file, thus the stat is local to this cache and cannot be combined across archives, but it is faster
   * than going to the archive.
   *
   * @return - the current value of CachePerfStats.tombstones
   */
  public static int getStat_tombstones(String regionName) {
    String regionStatName = "RegionStats-" + regionName;
    Log.getLogWriter().info("getStat_tombstones regionStatName=" + regionStatName);

    // Wait for the Cache to be created
    StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    while (statFactory == null) {
      statFactory = DistributedSystemHelper.getDistributedSystem();
    }

    // Wait for the regionStats to be created
    Statistics[] regionStats = statFactory.findStatisticsByTextId(regionStatName);
    while (regionStats.length == 0) {
      regionStats = statFactory.findStatisticsByTextId(regionStatName);
    }
    int tombstones = regionStats[0].getInt("tombstones");
    Log.getLogWriter().info("getStat_tombstones tombstones=" + tombstones);
    return tombstones;
  }

  //=================================================
  // other methods
  //=================================================

  /** Log the execution number of this serial task.
   */
  static protected long  logExecutionNumber() {
    long exeNum = DeltaGIIBB.getBB().getSharedCounters().incrementAndRead(DeltaGIIBB.executionNumber);
    Log.getLogWriter().info("Beginning task with execution number " + exeNum);
    return exeNum;
  }

  /** Check if we have run for the desired length of time. We cannot use 
   *  hydra's taskTimeSec parameter because of a small window of opportunity 
   *  for the test to hang due to the test's "concurrent round robin" type 
   *  of strategy. Here we set a blackboard counter if time is up and this
   *  is the last concurrent round.
   */
  protected static void checkForLastIteration() {
    // determine if this is the last iteration
    long numExecutions = DeltaGIIBB.getBB().getSharedCounters().read(DeltaGIIBB.executionNumber);
    if (numExecutions == DeltaGIIPrms.numExecutions()) {
      Log.getLogWriter().info("This is the last iteration of this task, numExecutions = " + numExecutions);
      DeltaGIIBB.getBB().getSharedCounters().increment(DeltaGIIBB.timeToStop);
    }
  }


  /**
   * Verify if initialized remotely (via gii) verify that we used deltaGII if expected.
   */
  public static void HydraTask_verifyDeltaGII() {
    boolean expectDeltaGII = DeltaGIIPrms.expectDeltaGII();
    Log.getLogWriter().info("HydraTask_verifyDeltaGII-expectDeltaGII(from Conf)=" + expectDeltaGII);

    // Tests which kill the requester depend on the the giiStatePrm (trigger for the callback to 
    // determine whether or not this should be a full or partial gii).
    String giiStatePrm = (String)DeltaGIIBB.getBB().getSharedMap().get(DeltaGIIBB.giiStatePrm);
    if (giiStatePrm != null) {
      if (giiStatePrm.equalsIgnoreCase("BeforeRequestRVV")              || 
          giiStatePrm.equalsIgnoreCase("AfterRequestRVV")               || 
          giiStatePrm.equalsIgnoreCase("AfterCalculatedUnfinishedOps")  ||
          giiStatePrm.equalsIgnoreCase("BeforeSavedReceivedRVV")        ||
          giiStatePrm.equalsIgnoreCase("AfterSentImageRequest")         ||
          giiStatePrm.equalsIgnoreCase("AfterSentImageReply")) {          
         expectDeltaGII = true;
      } else {
         //giiStatePrm.equalsIgnoreCase("AfterSavedReceivedRVV")        
         //giiStatePrm.equalsIgnoreCase("AfterReceivedRequestImage")    
         //giiStatePrm.equalsIgnoreCase("AfterReceivedImageReply")      
         //giiStatePrm.equalsIgnoreCase("DuringPackingImage") 
         // DuringApplyDelta
         // BeforeCleanExpiredTombstones
         // AfterSavedRVVEnd
         expectDeltaGII = false;
      }
    }
    Log.getLogWriter().info("HydraTask_verifyDeltaGII-expectDeltaGII(after giiStatePrm)=" + expectDeltaGII +
                            ". giiStatePrm=" + giiStatePrm);
    testInstance.verifyDeltaGII(expectDeltaGII);
  }

  /**
   * Verify that all regions were gii'ed as expected (delta or full gii).
   * throws a TestException if gii is not as configured via expectDeltaGII.
   */
  private void verifyDeltaGII(boolean expectDeltaGII) {
    String regionName = theRegion.getName();
    if (theRegion.getAttributes().getDataPolicy().withPartitioning()) {
      regionName = "partition-" + regionName;
    }

    // If the provider is explicitly doing a tombstone GC, verify that our tombstone count is less
    if (DeltaGIIPrms.doTombstoneGC()) {
      // CachePerfStats.tombstones (per thread) is stored on the BB (when requesters pause)
      String mapKey = DeltaGIIBB.tombstoneCount + RemoteTestModule.getCurrentThread().getThreadId();
      int beforeTombstones = (Integer)DeltaGIIBB.getBB().getSharedMap().get(mapKey);
      int tombstones = getStat_tombstones(regionName);
      if (tombstones >= beforeTombstones) {
        throw new TestException("Expected tombstone expiration in restarted vm (before recycle tombstone count was " + beforeTombstones +
                                " after restart, tombstone count was " + tombstones + ")");
      } else {
        Log.getLogWriter().info("After restart, requester vm has expected tombstones (before recycle tombstone count was " + beforeTombstones +
                                " after restart, tombstone count was " + tombstones + ")");
      }
    }

    StringBuffer aStr = new StringBuffer("verifyDeltaGII invoked with expectDeltaGII = " + expectDeltaGII + ". ");

    DiskRegion diskRegion = ((LocalRegion) theRegion).getDiskRegion();
    if (diskRegion != null && diskRegion.getStats().getRemoteInitializations() == 0) {
      aStr.append(regionName + " was recovered from disk (Remote Initializations = " +
                  diskRegion.getStats().getRemoteInitializations() + ").");
    } else {
      int giisCompleted = TestHelper.getStat_getInitialImagesCompleted(regionName);
      int deltaGiisCompleted = TestHelper.getStat_deltaGetInitialImagesCompleted(regionName);
      if ((expectDeltaGII && (deltaGiisCompleted == 0)) || (!expectDeltaGII && (deltaGiisCompleted == 1))) {
        throw new TestException("Did not perform expected type of GII. expectDeltaGII = " + expectDeltaGII +
                                " for region " + theRegion.getFullPath() +
                                " GIIsCompleted = " + giisCompleted +
                                " DeltaGIIsCompleted = " + deltaGiisCompleted);
      } else {
        aStr.append(regionName + " Remote Initialization (GII): GIIsCompleted = " + giisCompleted +
                    " DeltaGIIsCompleted = " + deltaGiisCompleted + ".");
      }
    }
    Log.getLogWriter().info(aStr.toString());
  }
}

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

package parReg.wbcl;

import hydra.blackboard.*;
import hydra.*;
import parReg.*;
import event.*;
import util.*;

import cacheperf.CachePerfClient;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;

import java.util.*;

import memscale.MemScalePrms;

/**
 * Extends parReg.ParRegTest to test GatewayEventListeners (WBCL).  
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.6.2
 */
public class ParRegWBCLTest extends ParRegTest {
  boolean isWBCLConfiguration = false;    // true => WBCL (vs. a true WAN Gateway)
  boolean isGateway = false;              // true => this is a Gateway or WBCL member
//  boolean rebalanceAsyncQueue = true;     // By default add async event queue for rebalance in parallel sender  
  public Region wbclRegion = null;

  public static boolean isDatastoreNewWanConfigured = false;
  
  /**
   * Starts a gateway hub in a VM that previously created one, after creating
   * gateways.
   */
  public static void startGatewayHubTask() {
    ((ParRegWBCLTest)testInstance).startGatewayHub(ConfigPrms.getGatewayConfig());
  }

  /**
   * Starts a gateway hub in a VM that previously created one, after creating
   * gateways.
   */
  protected void startGatewayHub(String gatewayConfig) {
    isWBCLConfiguration = (TestConfig.tab().stringAt(GatewayPrms.listeners, null) != null);
    isGateway = true;

    if (isWBCLConfiguration) {
       GatewayHubHelper.addWBCLGateway(gatewayConfig);
    } else {
       GatewayHubHelper.addGateways(gatewayConfig);
    }
    GatewayHubHelper.startGatewayHub();
  }

  /**
   * Creates a gateway hub using the {@link CacheServerPrms}.
   */
  protected void createGatewayHub() {
    String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
    if (gatewayHubConfig != null) {
      GatewayHubHelper.createGatewayHub(gatewayHubConfig);
    }
  }

  /**
   * Re-creates a gateway hub (within HAController method)
   */
  protected void createGatewayHub(String gatewayHubConfig) {
    if (gatewayHubConfig != null) {
      GatewayHubHelper.createGatewayHub(gatewayHubConfig);
    }
  }

  /**
   * Creates a async event queue using the {@link ConfigPrms#asyncEventQueueConfig}.
   */
  protected void createAsyncEventQueue() {
    String asyncEventQueueConfig = ConfigPrms.getAsyncEventQueueConfig();
    if (asyncEventQueueConfig != null) {
      AsyncEventQueueHelper.createAndStartAsyncEventQueue(asyncEventQueueConfig);
      isWBCLConfiguration = true;
    }
  }
  //============================================================================
  // INITTASKS (overrides)
  //============================================================================

  /** Creates and initializes the singleton instance of ParRegWBCLTest in this VM.
   *  Used for initializing accessor vms.
   */
  public synchronized static void HydraTask_initialize() {
     if (testInstance == null) {
        // initialize, hydra does not yet support cache xml generation for gateways, so avoid this
        PRObserver.installObserverHook();
        testInstance = new ParRegWBCLTest();
        ((ParRegWBCLTest)testInstance).aRegion = ((ParRegWBCLTest)testInstance).initializeRegion();
        ((ParRegWBCLTest)testInstance).createGatewayHub();
        ((ParRegWBCLTest)testInstance).initPdxDiskStore();
        ((ParRegWBCLTest)testInstance).initializeInstance();
        ((ParRegWBCLTest)testInstance).isDataStore = getIsDataStore();
     }
     setUniqueKeyIndex();
  }

  /** 
   * Create AsyncEventListener as defined by {@link ConfigPrms#asyncEventQueueConfig}
   * Create cache & region as defined by ConfigPrms.   
   * Used in newWan WBCL tests to create a separate local wbcl region
   */
  public synchronized static void HydraTask_initializeNewWanWBCL() {
    ((ParRegWBCLTest)testInstance).wbclRegion = ((ParRegWBCLTest)testInstance).initializeRegion();
    ((ParRegWBCLTest)testInstance).createAsyncEventQueue();
  }
  
  /**
   * Create cache & region as defined by ConfigPrms.  
   * Used in WBCL tests to create a separate local wbcl region
   */
  public static void HydraTask_initializeRegion() {
    ((ParRegWBCLTest)testInstance).wbclRegion = ((ParRegWBCLTest)testInstance).initializeRegion();
  }

  protected Region initializeRegion() {
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    Region region = RegionHelper.createRegion(ConfigPrms.getRegionConfig());

    // if configured as a bridgeServer, start the server
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
    }
  
    // edge clients register interest in ALL_KEYS
    if (region.getAttributes().getPoolName() != null) {
       region.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
       Log.getLogWriter().info("registered interest in ALL_KEYS for " + region.getName());
    }

    return region;
  }

  /** Creates and initializes the singleton instance of ParRegWBCLTest in this VM
   *  for HA testing with a PR accessor.
   */
  public synchronized static void HydraTask_HA_initializeAccessor() {
     if (testInstance == null) {
        String regionConfigName = getClientRegionConfigName();
        PRObserver.installObserverHook();
        testInstance = new ParRegWBCLTest();
        ((ParRegWBCLTest)testInstance).initializeRegion(regionConfigName, getCachePrmsName());
        ((ParRegWBCLTest)testInstance).initPdxDiskStore();
        ((ParRegWBCLTest)testInstance).initializeInstance();
        ((ParRegWBCLTest)testInstance).isDataStore = getIsDataStore();
     }
     setUniqueKeyIndex();
  }
  
  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing with a PR data store.
   */
  public synchronized static void HydraTask_HA_initializeDataStore() {
     if (testInstance == null) {
        String regionConfigName = getRegionConfigName();
        PRObserver.installObserverHook();
        testInstance = new ParRegWBCLTest();
        ((ParRegWBCLTest)testInstance).initializeRegion(regionConfigName, "cache1");
        ((ParRegWBCLTest)testInstance).initPdxDiskStore();
        ((ParRegWBCLTest)testInstance).initializeInstance();
        ((ParRegWBCLTest)testInstance).isDataStore = getIsDataStore();
     }
     setUniqueKeyIndex();
  }

  /**
   * Creates and initializes the singleton instance of ParRegWBCLTest in this VM.
   * Create AsyncEventListener as defined by {@link ConfigPrms#asyncEventQueueConfig}
   * Create cache & region as defined by ConfigPrms.   
   * Used in newWan WBCL tests to create a separate local wbcl region
   */
  public synchronized static void HydraTask_HA_initializeNewWanWBCL() { 
    if (testInstance == null) {            
      testInstance = new ParRegWBCLTest();
      ((ParRegWBCLTest)testInstance).wbclRegion = ((ParRegWBCLTest)testInstance).initializeRegion();
      ((ParRegWBCLTest)testInstance).createAsyncEventQueue();
   }
  }
  
  /**
   * Create cache & region as defined by ConfigPrms.   
   * Used for newwan WBCL HA test
   */
  public synchronized static void HydraTask_HA_initializeDatastoreNewWanWBCL() {
    if(!isDatastoreNewWanConfigured){
      isDatastoreNewWanConfigured = true;
      PRObserver.installObserverHook();
      String regionConfigName = getRegionConfigName();
      ((ParRegWBCLTest)testInstance).initializeRegion(regionConfigName, "cache1");
      ((ParRegWBCLTest)testInstance).initPdxDiskStore();
      ((ParRegWBCLTest)testInstance).initializeInstance();
      ((ParRegWBCLTest)testInstance).isDataStore = getIsDataStore();
    }
    setUniqueKeyIndex();
  }
  
  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing.
   */
  public synchronized static void HydraTask_HA_reinitializeAccessor() {
     if (testInstance == null) {
        PRObserver.installObserverHook();
        testInstance = new ParRegWBCLTest();
        ((ParRegWBCLTest)testInstance).HA_reinitializeRegion(getCachePrmsName());
        ((ParRegWBCLTest)testInstance).initPdxDiskStore();
        ((ParRegWBCLTest)testInstance).initializeInstance();
        ((ParRegWBCLTest)testInstance).isDataStore = getIsDataStore();
     }
     setUniqueKeyIndex();
  }

  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing.
   */
  public synchronized static void HydraTask_HA_reinitializeDataStore() {
     if (testInstance == null) {
        PRObserver.installObserverHook();
        PRObserver.initialize(RemoteTestModule.getMyVmid());
        testInstance = new ParRegWBCLTest();
        ((ParRegWBCLTest)testInstance).HA_reinitializeRegion("cache1");
        ((ParRegWBCLTest)testInstance).initPdxDiskStore();
        ((ParRegWBCLTest)testInstance).initializeInstance();
        ((ParRegWBCLTest)testInstance).isDataStore = getIsDataStore();
     }
     setUniqueKeyIndex();
  }

  public void initializeInstance() {
    super.initializeInstance();
    offHeapVerifyTargetCount = CachePerfClient.numThreads();
  }

  /**
   *  Re-Initialize a VM which has restarted by creating the appropriate region.
   *  For the wbcl members, also create the wbcl region.
   */
  public String HA_reinitializeRegion(String cachePrmsName) {

     String regionConfigName = "dataStoreRegion";
     if (isGateway) {     // hydra does not yet support cache xml generation for gateways
        // wbcl tests: re-initialize the partitioned region (regionConfig = dataStoreRegion)
        // wan p2pParRegHA and p2pParRegGatewayFailover: region config is clientRegion (PR Accessor)
        if (!isWBCLConfiguration) {    
          regionConfigName = "clientRegion";
        }
        CacheHelper.createCache("cache1");
        aRegion = RegionHelper.createRegion(regionConfigName);
        createGatewayHub("hub");
        startGatewayHub("gateway");
        initPdxDiskStore();
        Log.getLogWriter().info("After recreating " + aRegion.getFullPath() + ", size is " + aRegion.size());
     
        // re-initialize the TransactionManager (for the newly created cache) 
        if (getInitialImage.InitImagePrms.useTransactions()) {
           TxHelper.setTransactionManager();
        }
     
        // if we're in the wbcl vm, re-init the local wbcl region
        if (isWBCLConfiguration) {
           regionConfigName = "wbclRegion";
           wbclRegion = RegionHelper.createRegion(regionConfigName);
        }
     }else if (isNewWan()){
       CacheHelper.createCache("cache1");
       
       // if we're in the wbcl vm, re-init the local wbcl region
       if (isWBCLConfiguration) {         
         String wbclRegionConfigName = "wbclRegion";
         if(RegionHelper.getRegion(wbclRegionConfigName) == null){
           wbclRegion = RegionHelper.createRegion(wbclRegionConfigName);
         }
         createAsyncEventQueue();
       }
       
       aRegion = RegionHelper.createRegion(regionConfigName);       
       initPdxDiskStore();
       Log.getLogWriter().info("After recreating " + aRegion.getFullPath() + ", size is " + aRegion.size());
       
       // re-initialize the TransactionManager (for the newly created cache) 
       if (getInitialImage.InitImagePrms.useTransactions()) {
          TxHelper.setTransactionManager();
       }
     } else {    // this is a dataStore (only), use xml to reinitialize
        regionConfigName = super.HA_reinitializeRegion(cachePrmsName);
     } 
     return regionConfigName;
  }
  
  public static void HydraTask_HADoEntryOps() {
    testInstance.offHeapVerifyTargetCount = testInstance.getOffHeapVerifyTargetCount();
    ParRegTest.HydraTask_HADoEntryOps();
  }

  // Override for HAController(), so we also stop/start the local GatewayHub (in wbcl members)
  protected void HAController() {
    GatewayHub hub = null;
    List hubs = CacheHelper.getCache().getGatewayHubs();
    if (hubs.size() > 0) {
        hub = CacheHelper.getCache().getGatewayHubs().get(0);
    }

    // stop the gateway while we disconnect from the DS or close the PR
    if (hub != null) {
       Log.getLogWriter().info("Stopping GatewayHub " + hub.toString());
       hub.stop();
    }
    offHeapVerifyTargetCount = getOffHeapVerifyTargetCount();
  
    // run HAController (as is) ... note that the Gateway gets re-started as part of HA_reinitializeRegion
    super.HAController();
 
    // Note that the gateway is restarted through HA_reinitializeRegion()
  }

  /** Overrides ParRegTest.cycleVMsNoWait().
   *  When invoked in a GatewayVM ... do not cycle a gateway (because we don't want to stop one gateway and
   *  then kill the other gateway in a single WAN Site).  Cycle a dataStore if HAController is running in 
   *  a gateway.
   */
  protected void cycleVMsNoWait() {
    Log.getLogWriter().info("Cycling vms...");
    PRObserver.initialize();
    List<Integer> vmList = ClientVmMgr.getClientVmids();
    Log.getLogWriter().info("vmList is " + vmList);
    boolean stoppingAll = numVMsToStop >= vmList.size();
    cycleVMs_targetVMs = null;
    cycleVMs_notChosenVMs = new ArrayList();
  
    // When running HAController from a Gateway VM, do NOT recycle another Gateway
    // (since we have stopped the gateway before disconnecting from the cache, etc).
    String stopVMsMatchStr = TestConfig.tab().stringAt(ParRegPrms.stopVMsMatchStr, null);
    if (isGateway) {
       stopVMsMatchStr = "dataStore";
    } else if (isNewWan() && isWBCLConfiguration){
      stopVMsMatchStr = "dataStore";
    }
    Object[] anArr = null;
    if (stopVMsMatchStr == null) {
      anArr = StopStartVMs.getOtherVMs(numVMsToStop);
    } else {
      anArr = StopStartVMs.getOtherVMs(numVMsToStop, stopVMsMatchStr);
    }
    cycleVMs_targetVMs = (List<ClientVmInfo>)(anArr[0]);
    List stopModes = (List)(anArr[1]);
    cycleVMs_notChosenVMs = (List)(anArr[2]);
 
    clearBBCriticalState(cycleVMs_targetVMs);
    StopStartVMs.stopStartVMs(cycleVMs_targetVMs, stopModes);

    Log.getLogWriter().info("Done cycling vms");
  }

  // Override for concVerify(), so we also check the local wbclRegion
  protected void concVerify() {
    // wait for 30 seconds of silence to allow everything to be pushed across gateway and to other members
    // See util.SummaryLogListener (extends SilenceListener)
    SilenceListener.waitForSilence(30, 5000);
    super.concVerify();
    if (wbclRegion != null) {
      verifyWBCLRegion();
    }
  }

  /** Verify the region contents against the blackboard AND verify the
   *  internal state of the PR.
   */
  public void verifyFromSnapshot() {
    verifyFromSnapshotOnly();
    if (isWBCLConfiguration) {
      verifyInternalPRState();   // seeing an NPE in verifyInternalPRState test hook with Gateways
    }
  }

  /**
   * Waits for redundancy recovery after calling cycleVMsNoWait
   */  
  protected void waitForRecoveryAfterCycleVMs() {
    if(isNewWan()){
      // Wait for recovery to complete
      if (ParRegBB.getBB().getSharedCounters().read(ParRegBB.MaxRC) > 0) {
        int numPRs = 1;        
        // in newWan wbcl, the queue's shadow PR is added
        AsyncEventQueueDescription aed = AsyncEventQueueHelper.getAsyncEventQueueDescription("wbclQueue");
        if(aed != null && aed.getParallel().booleanValue()){
          numPRs = 2;
        }        
        long recoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("recoveryDelay"))).longValue();
        long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
        PRObserver.waitForRecovery(recoveryDelay, startupRecoveryDelay, 
            cycleVMs_targetVMs, cycleVMs_notChosenVMs, 1, numPRs, null, null);
      }
    }else{
      super.waitForRecoveryAfterCycleVMs();
    } 
  }
  
  /**
   * Waits for recovery in self vm after reconnect
   */
  protected void waitForSelfRecovery(){
    if(isNewWan()){
      RegionAttributes attr = aRegion.getAttributes();
      PartitionAttributes prAttr = attr.getPartitionAttributes();
      if(prAttr != null){      
        // in newWan wbcl, the queue's shadow PR is added
        AsyncEventQueueDescription aed = AsyncEventQueueHelper.getAsyncEventQueueDescription("wbclQueue");      
        int numPRs = 1;
        if(aed != null && aed.getParallel().booleanValue()){
          numPRs = 2;
        }
        
        List otherVMs = new ArrayList(ClientVmMgr.getOtherClientVmids());
        PRObserver.waitForRecovery(prAttr.getRecoveryDelay(), prAttr.getStartupRecoveryDelay(), 
                   new Integer(RemoteTestModule.getMyVmid()), otherVMs, 1, numPRs, null, null);  
      }  
    }else{
      super.waitForSelfRecovery();
    }
  }
  
  /**
   * Create cache & region as defined by ConfigPrms.  
   * Used in WBCL tests to verify the replicated wbcl region
   */
  public static void HydraTask_verifyWBCLRegion() {
    ((ParRegWBCLTest)testInstance).verifyWBCLRegion();
  }

  /** Verify wbcl local region against the region snapshot 
   */
  public void verifyWBCLRegion() {
    WBCLEventListener.waitForSilence(30, 5000);

    StringBuffer aStr = new StringBuffer();
    regionSnapshot = (Map)(ParRegBB.getBB().getSharedMap().get(ParRegBB.RegionSnapshot));
    destroyedKeys = (Set)(ParRegBB.getBB().getSharedMap().get(ParRegBB.DestroyedKeys));
    Set localKeySet = new HashSet(wbclRegion.keySet()); // must be wrapped in HashSet to invoke removeAll below
    int snapshotSize = regionSnapshot.size();
    int numLocalKeys = localKeySet.size();
    Log.getLogWriter().info("Verifying local wbclRegion keys from snapshot containing " + snapshotSize + " entries...");
    if (snapshotSize != numLocalKeys) {
      aStr.append("Expected number of keys in local wbclRegion to be " + snapshotSize + ", but it is " + numLocalKeys + "\n");
    }
    Iterator it = regionSnapshot.entrySet().iterator();
    while (it.hasNext()) { // iterating the expected keys
      Map.Entry entry = (Map.Entry)it.next();
      Object key = entry.getKey();
      Object expectedValue = entry.getValue();
      if (!localKeySet.contains(key)) {
        aStr.append("Expected key " + key + " to be in local wbclRegion, but it is missing\n");
      } else {
        Object value = wbclRegion.get(key);
        try {
           ParRegUtil.verifyMyValue(key, expectedValue, value, ParRegUtil.EQUAL);
        } catch (TestException e) {
           aStr.append(e.getMessage() + "\n");
        }
      }
    }

    // check that destroyedKeys are not in the server keys
    it = destroyedKeys.iterator();
    while (it.hasNext()) {
       Object key = it.next();
       if (localKeySet.contains(key)) {
         aStr.append("Destroyed key " + key + " was returned as a local wbclRegion key\n");
       }
    }
 
    // check for extra server keys that were not in the snapshot
    Set snapshotKeys = regionSnapshot.keySet();
    localKeySet.removeAll(snapshotKeys);
    if (localKeySet.size() != 0) {
       aStr.append("Found the following unexpected keys in local wbclRegion keys: " + 
                   ": " + localKeySet + "\n");
    }
 
    if (aStr.length() > 0) {
       // shutdownHook will cause all members to dump partitioned region info
       throw new TestException(aStr.toString());
    }
    Log.getLogWriter().info("Done verifying local wbclRegion keys from snapshot containing " + snapshotSize + " entries...");
  }
  
  public boolean isNewWan(){    
    boolean flag = false;    
    RegionDescription rd = RegionHelper.getRegionDescription("dataStoreRegion");
    
    //check for wbcl
    List asyncQueuesIds = rd.getAsyncEventQueueNames();
    if(asyncQueuesIds != null && asyncQueuesIds.size() > 0){
      flag = true;
    }
    
    //check for newwan sender
    List senderIds = rd.getGatewaySenderNames();
    if(senderIds != null && senderIds.size() > 0){
      flag = true;
    }
    
    return flag;
  }
  
  public int getOffHeapVerifyTargetCount() {
    return MemScalePrms.getOffHeapVerifyTargetCount();
  }
}

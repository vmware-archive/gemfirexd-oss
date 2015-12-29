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
package splitBrain;

import java.util.*;
import java.io.*;

import util.*;
import hydra.*;
import hydra.blackboard.*;
import perffmwk.*;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * 
 * ML Rio Use Case (SplitBrainTestSpec: 10.3.10)
 *
 * @author Lynn Hughes-Godfrey
 * @since 5.5
 */
public class MLRioTest extends util.OperationsClient {

  protected static final int ITERATIONS = 1000;

  // Single instance in this VM
  static protected MLRioTest testInstance;
  
  // instance variables
  protected RandomValues randomValues = null;
  protected int numRootRegions; 
  protected int numDynamicRegions;  
  protected boolean isNewWanConfig = (ConfigPrms.getGatewaySenderConfig() != null);

/* hydra task methods */
/* ======================================================================== */
// Server methods
    public synchronized static void HydraTask_initializeBridgeServer() {
       if (testInstance == null) {
          testInstance = new MLRioTest();
          testInstance.initializeOperationsClient();
          testInstance.initializeBridgeServer();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeServer() {

      String cacheXmlFile = "bridgeCache_" + RemoteTestModule.MyPid + ".xml";
      DistributedSystem ds = DistributedSystemHelper.connectWithXml(cacheXmlFile);

      // Establish dynamicRegionFactory (before creating the cache)
      File d = new File("DynamicRegionData" + ProcessMgr.getProcessId());
      d.mkdirs();
      DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(d, null));

      // Configure non-dynamic (parent) regions 
      List regionNames = new ArrayList();
      numRootRegions = TestConfig.tab().intAt(MLRioPrms.numRootRegions, 1);
      for (int i=0; i < numRootRegions; i++) {
         regionNames.add(NameFactory.REGION_NAME_PREFIX + (i+1));
      }
      
      // DynamicRegionFactory requires cache/region creation vs xml so that
      // parent regions will be available when dynamicRegions re-created
      CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(), null, ConfigPrms.getRegionConfig(), regionNames, ConfigPrms.getBridgeConfig(),ConfigPrms.getPoolConfig(), ConfigPrms.getDiskStoreConfig(), null, cacheXmlFile);
      CacheHelper.createCacheFromXml(cacheXmlFile);

      // Set-up the server to listen for client membership events
      BridgeMembershipListener membershipListener = MLRioPrms.getBridgeMembershipListener();
      if (membershipListener != null) {
         BridgeMembership.registerBridgeMembershipListener( membershipListener );
         Log.getLogWriter().info("Registered BridgeMembershipListener " + membershipListener);
      }

      // Create GatewayHub, note that the Gateway cannot be started until
      // all Gateways have been created (info placed on BB for hydra to 
      // connect appropriate VMs/ports
      if(!isNewWanConfig){
        createGatewayHub();
      }

      Log.getLogWriter().info("Installing MembershipNotifierHook");
      SBUtil.addMembershipHook(new MembershipNotifierHook());

      numDynamicRegions = TestConfig.tab().intAt(MLRioPrms.numDynamicRegions, 1);
      randomValues = new RandomValues();
   }
   
   /**
    * Add Gateways and start the GatewayHub
    *
    * Note:  This must be done after all servers have been initialized (and
    * invoked createGatewayHub(), so the hydra WAN blackboard knows about 
    * all the available servers and endpoints.
    */
    public synchronized static void HydraTask_startGatewayHubTask() {
       testInstance.startGatewayHub(ConfigPrms.getGatewayConfig());
    }

    /**
     * Starts a gateway hub in a VM that previously created one, after creating
     * gateways.
     */
    protected void startGatewayHub(String gatewayConfig) {
      GatewayHubHelper.addGateways(gatewayConfig);
      GatewayHubHelper.startGatewayHub();
    }

    /**
     * Creates GatewaySender ids based on the
     * {@link ConfigPrms#gatewaySenderConfig}.
     */
    public synchronized static void HydraTask_createGatewaySenderIds() {
      String senderConfig = ConfigPrms.getGatewaySenderConfig();
      GatewaySenderHelper.createGatewaySenderIds(senderConfig);
    }
    
    /**
     * Creates start new wan components i.e senders based on {@link ConfigPrms#gatewaySenderConfig}
     * and receivers based on {@link ConfigPrms#gatewayReceiverConfig}. 
     */
    public synchronized static void HydraTask_createAndStartNewWanComponents(){
      testInstance.createAndStartNewWanComponents();
    }
    
  protected void createAndStartNewWanComponents() {
    // create and start receivers
    String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
    if (receiverConfig != null) {
      GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
    }
    else {
      throw new TestException("New wan receiver config is null.");
    }

    // create and start sender
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    if (senderConfig != null) {
      GatewaySenderHelper.createAndStartGatewaySenders(senderConfig);
    }
    else {
      throw new TestException("New wan sender config is null.");
    }
  }
    
   /**
    *  Setup DynamicRegionFactory and create non-dynamic parent regions
    */
    public synchronized static void HydraTask_initializeBridgeClient() {
       if (testInstance == null) {
          testInstance = new MLRioTest();
          testInstance.initializeOperationsClient();
          testInstance.initializeBridgeClient();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeClient() {
       // create cache/region 
       if (CacheHelper.getCache() == null) {
         initCacheWithXml(ConfigPrms.getCacheConfig(), ConfigPrms.getRegionConfig(), ConfigPrms.getPoolConfig());
         
         
         DynamicRegionFactory.get().registerDynamicRegionListener(new MLRioDynamicRegionListener());
         MLRioBB.getBB().getSharedCounters().increment(MLRioBB.dynamicRegionClients);

         // Set-up the client to listen for server membership events
         BridgeMembershipListener membershipListener = MLRioPrms.getBridgeMembershipListener();
         if (membershipListener != null) {
           BridgeMembership.registerBridgeMembershipListener( membershipListener );
           Log.getLogWriter().info("Registered BridgeMembershipListener " + membershipListener);
         }
      }

      randomValues = new RandomValues();
   }

   /**
    * Create MLRioBB.numDynamicRegions (execute once in each WAN Site)
    * after servers and clients have been initialized (with non-dynamic
    * parent regions).
    */
   public static void HydraTask_createDynamicRegions() {
      testInstance.createDynamicRegions();
   }

   protected void createDynamicRegions() {
   Region parentRegion = null;
   Region aRegion = null;
   String drName = null;

      Set rootRegions = CacheHelper.getCache().rootRegions();
      for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
         parentRegion = (Region)it.next();
         numDynamicRegions = TestConfig.tab().intAt(MLRioPrms.numDynamicRegions, 1);
         for (int i=0; i < numDynamicRegions; i++) {
            drName = parentRegion.getName() + "_DYNAMIC" + "_" + (i+1);
            createDynamicRegion(parentRegion.getFullPath(), drName);
         }
      }
   }
   
   /**
    * Create MLRioBB.numDynamicRegions (execute once in each WAN Site)
    * after servers and clients have been initialized (with non-dynamic
    * parent regions).
    */
   public static void HydraTask_waitForDynamicRegions() {
      testInstance.waitForDynamicRegions();
   }

   private void waitForDynamicRegions() {
     int numDynamicRegions = TestConfig.tab().intAt(MLRioPrms.numDynamicRegions, 1);
     int numRootRegions = TestConfig.tab().intAt(MLRioPrms.numRootRegions);
     long numClients = MLRioBB.getBB().getSharedCounters().read(MLRioBB.dynamicRegionClients);
      
     long expectedRegions  = numDynamicRegions * numRootRegions * numClients;
     TestHelper.waitForCounter(MLRioBB.getBB(), "dynamicRegionCreates", MLRioBB.dynamicRegionCreates, expectedRegions, true, 120000);
  }

  /**
    * Initialize using XML based on the given cache and region configurations.
    */
   public void initCacheWithXml(String cacheConfig, String regionConfig, String poolConfig) { 
     // connect to the distributed system with the xml file (not generated yet)
     String cacheXmlFile = "edgeCache_" + RemoteTestModule.MyPid + ".xml";
     DistributedSystem ds = DistributedSystemHelper.connectWithXml(cacheXmlFile);

     // configure for dynamic regions, using the optional bridge writer
     File d = new File("DynamicRegionData" + RemoteTestModule.MyPid);
     d.mkdirs();

     String poolName = PoolHelper.getPoolDescription(ConfigPrms.getPoolConfig()).getName();
     DynamicRegionFactory.Config dynamicRegionConfig = new DynamicRegionFactory.Config(d, poolName, true, true);

     // generate the xml file
     List regionNames = new ArrayList();
     numRootRegions = TestConfig.tab().intAt(MLRioPrms.numRootRegions, 1);
     for (int i=0; i < numRootRegions; i++) {
        regionNames.add(NameFactory.REGION_NAME_PREFIX + (i+1));
     }

     CacheHelper.generateCacheXmlFile(cacheConfig, dynamicRegionConfig, regionConfig, regionNames, null, poolConfig, cacheXmlFile);

     // create the cache and its region using the xml file
     CacheHelper.createCacheFromXml(cacheXmlFile);

   }

   /* 
    * Creates the GatewayHub (if configured)
    */
   protected void createGatewayHub() {
      // Gateway initialization (if needed)
      String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
      if (gatewayHubConfig != null) {
         GatewayHubHelper.createGatewayHub(gatewayHubConfig);
      }
   }

    /** 
     *  Cause a server to beSick/playDead (but don't synchronize with
     *  bridgeClient operations (use healthController for coordination 
     *  with validation).
     *
     *  Once the forced disconnect has completed, (and validation completed), 
     *  re-initialize the cache and restart the Bridge/GatewayHub.
     */
    public static void HydraTask_forceDisconnect() {
       testInstance.forceDisconnect();
    }

    protected void forceDisconnect() {
      long executionNumber = MLRioBB.getBB().getSharedCounters().incrementAndRead(MLRioBB.executionNumber);
      Log.getLogWriter().info("forceDisconnect EXECUTION_NUMBER = " + executionNumber);
      DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
      Cache theCache = CacheHelper.getCache();

      // Expect RegionDestroyedEvent (FORCED_DISCONNECT) for each region in this VM
      int numRegionsInVm = numRootRegions + (numRootRegions * numDynamicRegions);

      // Force this VM to become unresponsive (even to ARE_YOU_DEAD messages)
      ControllerBB.enableSickness();
      ControllerBB.enablePlayDead();
      SBUtil.beSick(); 
      SBUtil.playDead();

      // Force a message distribution (so forcedDisconnect can be detected, 
      // even if publishers have finished.
      Region fdRegion = getRandomRootRegion();
      Log.getLogWriter().info("Calling put on " + fdRegion.getName() + " to cause a forced disconnect");
      try {
         fdRegion.put(new Long(executionNumber), new Long(System.currentTimeMillis()));
      } catch (Exception e) {
        if (!isCausedByForcedDisconnect(e)) {
           throw new TestException("Unexpected Exception " + e + " thrown on put to fdRegion " + fdRegion.getName() + " " + TestHelper.getStackTrace(e));
        }
      }
      Log.getLogWriter().info("Done calling put on " + fdRegion.getName() + " to cause a forced disconnect");

      // wait for a forced disconnect to be detected
      Log.getLogWriter().info("Waiting for " + numRegionsInVm + " ForcedDisconnect events");
      ForcedDiscUtil.waitForForcedDiscConditions(ds, theCache, numRegionsInVm);

      // clear all counters associated with beSick/playDead/forcedDisconnect
      ControllerBB.reset(RemoteTestModule.getMyVmid());

      // re-initialize
      Log.getLogWriter().info("Re-initializing BridgeServer and Gateway ..."); 
      initializeBridgeServer();
      
      if(isNewWanConfig){
        createAndStartNewWanComponents();
      }else{
        startGatewayHub(ConfigPrms.getGatewayConfig());  
      }
    }

    /** 
     *  Cause a server to beSick/playDead.  
     *  Once the forced disconnect has completed, (and validation completed), 
     *  re-initialize the cache and restart the Bridge/GatewayHub.
     */
    public static void HydraTask_healthController() {
       testInstance.healthController();
    }

    protected void healthController() {

      long executionNumber = MLRioBB.getBB().getSharedCounters().incrementAndRead(MLRioBB.executionNumber);
      Log.getLogWriter().info("healthController EXECUTION_NUMBER = " + executionNumber);
      int maxExecutions = TestConfig.tab().intAt(MLRioPrms.maxExecutions, 10);
      if (executionNumber == maxExecutions) {
         Log.getLogWriter().info("Last round of execution, maxExecutions = " + maxExecutions);
      }

      DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
      Cache theCache = CacheHelper.getCache();

      // Expect RegionDestroyedEvent (FORCED_DISCONNECT) for each region in this VM
      int numRegionsInVm = numRootRegions + (numRootRegions * numDynamicRegions);

      // wait for Publishers (putSequentialKeys task) to be active
      TestHelper.waitForCounter(MLRioBB.getBB(), "numPublishers", MLRioBB.numPublishers, 1, false, 500);
      long numPublishers = MLRioBB.getBB().getSharedCounters().read(MLRioBB.numPublishers);
      TestHelper.waitForCounter(MLRioBB.getBB(), "Publishing", MLRioBB.Publishing, numPublishers, true, 180000);

      // Reset counters for next (future) round of execution
      MLRioBB.getBB().clearMembershipCounters();

      // Force this VM to become unresponsive (even to ARE_YOU_DEAD messages)
      // enable in the BB so validation will know we expect a forced disconnect in the ForcedDiscListener
      ControllerBB.enableSickness();
      ControllerBB.enablePlayDead();
      SBUtil.beSick(); 
      SBUtil.playDead();

      // Force a message distribution (so forcedDisconnect can be detected, 
      // even if publishers have finished.
      Region fdRegion = getRandomRootRegion();
      Log.getLogWriter().info("Calling put on " + fdRegion.getName() + " to cause a forced disconnect");
      try {
         fdRegion.put(new Long(executionNumber), new Long(System.currentTimeMillis()));
      } catch (Exception e) {
        if (!isCausedByForcedDisconnect(e)) {
           throw new TestException("Unexpected Exception " + e + " thrown on put to fdRegion " + fdRegion.getName() + " " + TestHelper.getStackTrace(e));
        }
      }

      Log.getLogWriter().info("Done calling put on " + fdRegion.getName() + " to cause a forced disconnect");

      // wait for a forced disconnect to be detected
      Log.getLogWriter().info("Waiting for " + numRegionsInVm + " ForcedDisconnect events");
      ForcedDiscUtil.waitForForcedDiscConditions(ds, theCache, numRegionsInVm);

      // Expect (client) memberCrashed event for farSide primary connected to this (sick) Gateway endpoint
      String sWanSites = TestConfig.getInstance().getSystemProperty("wanSites");
      long numWanSites = new Long(sWanSites).longValue();
      String sEdgeVMsPerHost = TestConfig.getInstance().getSystemProperty("edgeVMsPerHost");
      long edgeVMsPerHost = new Long(sEdgeVMsPerHost).longValue();
      String sEdgeHostsPerSite = TestConfig.getInstance().getSystemProperty("edgeHostsPerSite");
      long edgeHostsPerSite = new Long(sEdgeHostsPerSite).longValue();

      if(isNewWanConfig){
        //in new wan, we can have more than one receivers from a site connected to same sender
        TestHelper.waitForCounter(MLRioBB.getBB(), "actualClientCrashedEvents", MLRioBB.actualClientCrashedEvents, numWanSites-1, false, 30000);
      }else{
        TestHelper.waitForCounter(MLRioBB.getBB(), "actualClientCrashedEvents", MLRioBB.actualClientCrashedEvents, numWanSites-1, true, 30000);
      }
 
      // Expected (server) memberCrashed events in each edgeClient for this server
      // lhughes 10/30/08 - relaxed this restriction to allow a memberDeparted or memberCrashed event
      TestHelper.waitForCounterSum(MLRioBB.getBB(), "actualServerCrashedEvents", "actualServerDepartedEvents", edgeHostsPerSite*edgeVMsPerHost, false, 60000);

      // Don't sync up until all edge clients have finished publishing
      TestHelper.waitForCounter(MLRioBB.getBB(), "donePublishing", MLRioBB.donePublishing, numPublishers, true, 60000);
      // Give the clients a few seconds to do SharedLock.getSharedCondition.await() (or we will not notify all clients to continue)
      MasterController.sleepForMs(5000);

      // Notify publishers that the we've been forcefully disconnected
      MLRioBB.getBB().getSharedCounters().zero(MLRioBB.validationComplete);
      Log.getLogWriter().info("Cleared MLRioBB.validationComplete counter");

      SharedLock hcLock = MLRioBB.getBB().getSharedLock();
      hcLock.lock();
      SharedCondition cond = hcLock.getCondition(MLRioBB.healthControllerComplete);
      Log.getLogWriter().info("notifying all VMs for MLRioBB.SharedLock.SharedCondition." + MLRioBB.healthControllerComplete);
      cond.signalAll();
      Log.getLogWriter().info("notifed all VMs for MLRioBB.SharedLock.SharedCondition." + MLRioBB.healthControllerComplete);
      hcLock.unlock();
      Log.getLogWriter().info("unlocked MLRioBB.SharedLock");

      // clear all counters associated with beSick/playDead/forcedDisconnect
      ControllerBB.reset(RemoteTestModule.getMyVmid());

      // re-initialize
      Log.getLogWriter().info("Re-initializing BridgeServer and Gateway ..."); 
      initializeBridgeServer();
      if(!isNewWanConfig){
        startGatewayHub(ConfigPrms.getGatewayConfig());  
      }else{
        createAndStartNewWanComponents();
      }      

      if (executionNumber == maxExecutions) {
         throw new StopSchedulingOrder("numExecutions = " + executionNumber);
      }
    }

    /** 
     *  Cause a network split between the primary and secondary gateway (servers)
     *  in WAN Site #1.
     *  After network partition causes ForcedDisconnects, recycle disconnected Gatweay/server
     *  re-initialize the cache and restart the Bridge/GatewayHub.
     *
     *  The networkController operations are coordinated with putSequentialKeys
     *  via the MLRioBB.healthControllerComplete SharedLock.SharedCondition.
     */
    public static void HydraTask_networkController() {
       testInstance.networkController();
    }

    protected void networkController() {

      long executionNumber = MLRioBB.getBB().getSharedCounters().incrementAndRead(MLRioBB.executionNumber);
      Log.getLogWriter().info("networkController EXECUTION_NUMBER = " + executionNumber);
      int maxExecutions = TestConfig.tab().intAt(MLRioPrms.maxExecutions, 10);
      if (executionNumber == maxExecutions) {
         Log.getLogWriter().info("Last round of execution, maxExecutions = " + maxExecutions);
      }

      // wait for Publishers (putSequentialKeys task) to be active
      TestHelper.waitForCounter(MLRioBB.getBB(), "numPublishers", MLRioBB.numPublishers, 1, false, 500);
      long numPublishers = MLRioBB.getBB().getSharedCounters().read(MLRioBB.numPublishers);
      TestHelper.waitForCounter(MLRioBB.getBB(), "Publishing", MLRioBB.Publishing, numPublishers, true, 180000);

      // Reset counters for next (future) round of execution
      MLRioBB.getBB().clearMembershipCounters();

      // Drop the network (between the 2 wan sites)
      SBUtil.dropConnection();

      // wait for a forced disconnect to be detected
      TestHelper.waitForCounter(MLRioBB.getBB(), "forcedDisconnects", MLRioBB.forcedDisconnects, 1, false, 60000);

      // Expect (client) memberCrashed events for both ends of each Gateway connected to the sick member
/*
      String sWanSites = TestConfig.getInstance().getSystemProperty("wanSites");
      long numWanSites = new Long(sWanSites).longValue();
      String sEdgeVMsPerHost = TestConfig.getInstance().getSystemProperty("edgeVMsPerHost");
      long edgeVMsPerHost = new Long(sEdgeVMsPerHost).longValue();
      String sEdgeHostsPerSite = TestConfig.getInstance().getSystemProperty("edgeHostsPerSite");
      long edgeHostsPerSite = new Long(sEdgeHostsPerSite).longValue();

      TestHelper.waitForCounter(MLRioBB.getBB(), "actualClientCrashedEvents", MLRioBB.actualClientCrashedEvents, numWanSites, false, 250);
 
      // Expected (server) memberCrashed events in each edgeClient for this server
      TestHelper.waitForCounter(MLRioBB.getBB(), "actualServerCrashedEvents", MLRioBB.actualServerCrashedEvents, edgeHostsPerSite*edgeVMsPerHost, false, 30000);
*/

      // wait for publishers to complete (and wait on healthController SharedLock Condition) ... then restore the connection and re-init losing side Gateway/Server
      TestHelper.waitForCounter(MLRioBB.getBB(), "donePublishing", MLRioBB.donePublishing, numPublishers, true, 60000);
      // Give the clients a few seconds to do SharedLock.getSharedCondition.await() (or we will not notify all clients to continue)
      MasterController.sleepForMs(5000);

      // restore the network connection
      SBUtil.restoreConnection();

      // stop/start losingSide Gateway VMs
      Log.getLogWriter().info("Re-starting losingSide BridgeServer/Gateway ..."); 
      Object[] losingSideServers = StopStartVMs.getOtherVMs(1, "wan_1_Lose");
      Log.getLogWriter().info("Stopping VMs : " + losingSideServers);
      List clientVmInfoList = (List)(losingSideServers[0]);
      List stopModeList = (List)(losingSideServers[1]);
      StopStartVMs.stopStartVMs(clientVmInfoList, stopModeList);
      Log.getLogWriter().info("Done restarting losingSide VMs");

      // Notify publishers that the we've been forcefully disconnected/restarted
      MLRioBB.getBB().getSharedCounters().zero(MLRioBB.validationComplete);
      Log.getLogWriter().info("Cleared MLRioBB.validationComplete counter");

      SharedLock hcLock = MLRioBB.getBB().getSharedLock();
      hcLock.lock();
      SharedCondition cond = hcLock.getCondition(MLRioBB.healthControllerComplete);
      Log.getLogWriter().info("notifying all VMs for MLRioBB.SharedLock.SharedCondition." + MLRioBB.healthControllerComplete);
      cond.signalAll();
      Log.getLogWriter().info("notifed all VMs for MLRioBB.SharedLock.SharedCondition." + MLRioBB.healthControllerComplete);
      hcLock.unlock();
      Log.getLogWriter().info("unlocked MLRioBB.SharedLock");

      // clear all counters associated with forcedDisconnect
      MLRioBB.getBB().getSharedCounters().zero(MLRioBB.forcedDisconnects);

      if (executionNumber == maxExecutions) {
         throw new StopSchedulingOrder("numExecutions = " + executionNumber);
      }
    }

    /**
     *  Performs ITERATIONS puts on a unique entry in TestRegion
     *  Each assignment of this task gets a new key from the NameFactory, and then
     *  updates that key with sequential values from 1 -> ITERATIONS.  This
     *  allows the client listeners to verify incremental updates on the key and 
     *  for a final CLOSETASK validator to verify that all clients have all keys with the 
     *  correct ending value (ITERATIONS).
     *  
     */
    public static void HydraTask_putSequentialKeys() {
       testInstance.putSequentialKeys();
       testInstance.validateSequentialKeys();
    }

    static Boolean firstTimePublishing = Boolean.TRUE;
    protected void putSequentialKeys() {

       // One time only, we need to advertise how many publishers are in this test
       synchronized (firstTimePublishing) {
          if (firstTimePublishing.equals(Boolean.TRUE)) {
             long numPublishers = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
             MLRioBB.getBB().getSharedCounters().setIfLarger(MLRioBB.numPublishers, numPublishers);
             firstTimePublishing = Boolean.FALSE;
          }
       }

       // Check for any Exceptions posted to BB by EventListeners
       TestHelper.checkForEventError(MLRioBB.getBB());

       long executionNumber = MLRioBB.getBB().getSharedCounters().read(MLRioBB.executionNumber);
       Log.getLogWriter().info("putSequentialKeys EXECUTION_NUMBER = " + executionNumber);
       int maxExecutions = TestConfig.tab().intAt(MLRioPrms.maxExecutions, 10);
       if (executionNumber == maxExecutions) {
          Log.getLogWriter().info("Last round of execution, maxExecutions = " + maxExecutions);
       }

       // Work: select randomRegion, nextKey and putValues 1->1000 for key
       boolean isDynamicRegion = (numDynamicRegions > 0);
       Region aRegion = getRandomRegion(!isDynamicRegion);
       // todo@lhughes -- should we need to do this?
       while (aRegion == null) {
          Log.getLogWriter().info("getRandomRegion returned null, sleeping 1 ms to waiting for dynamicRegionCreation");
          MasterController.sleepForMs(1000);
          aRegion = getRandomRegion(!isDynamicRegion);
       }
       Log.getLogWriter().info("Working on Region " + aRegion.getName());
       String key = NameFactory.getNextPositiveObjectName();
       Long val = new Long(NameFactory.getCounterForName(key));

       Log.getLogWriter().info("Working on key " + key + " in Region " + aRegion.getFullPath());
       for (int i = 1; i <= ITERATIONS; i++) {
          aRegion.put(key, new ValueHolder(val, randomValues, new Integer(i)));
          if (i == ITERATIONS/2) {
             // Notify healthController that we're publishing
             long ctr = MLRioBB.getBB().getSharedCounters().incrementAndRead(MLRioBB.Publishing);
             Log.getLogWriter().info("putSequentialKeys: incremented MLRioBB.Publishing = " + ctr);
          }
       }
       long ctr = MLRioBB.getBB().getSharedCounters().incrementAndRead(MLRioBB.donePublishing);
       Log.getLogWriter().info("putSequentialKeys: incremented MLRioBB.donePublishing = " + ctr);

       // healthController will let us know when we can continue again
       SharedLock hcLock = MLRioBB.getBB().getSharedLock();
       hcLock.lock();
       SharedCondition cond = hcLock.getCondition(MLRioBB.healthControllerComplete);
       Log.getLogWriter().info("Waiting to be notified for MLRioBB.SharedLock.SharedCondition." + MLRioBB.healthControllerComplete);
       try {
          cond.await();
          Log.getLogWriter().info("Notified for MLRioBB.SharedLock.SharedCondition." + MLRioBB.healthControllerComplete);
       } catch (InterruptedException e) {
          Log.getLogWriter().info("SharedCondition await() interrupted, continuing ...");
       } finally {
          hcLock.unlock();
          Log.getLogWriter().info("Unlocked for MLRioBB.SharedLock");
       }
       MLRioBB.getBB().getSharedCounters().zero(MLRioBB.Publishing);
       MLRioBB.getBB().getSharedCounters().zero(MLRioBB.donePublishing);
       Log.getLogWriter().info("putSequentialKeys: reset MLRioBB Publishing and donePublishing counters");

       if (executionNumber == maxExecutions) {
          throw new StopSchedulingOrder("numExecutions = " + executionNumber);
       }
    }

    /**
     *  Performs puts/gets on entries in TestRegion
     */
    public static void HydraTask_doEntryOperations() {
       testInstance.doEntryOperations();
    }

    protected void doEntryOperations() {
       Region parentRegion;
       Region aRegion; 

       GsRandom rng = TestConfig.tab().getRandGen();
       boolean isDynamicRegion = (numDynamicRegions > 0);
       aRegion = getRandomRegion(!isDynamicRegion);
       // todo@lhughes -- should we need to do this?
       while (aRegion == null) {
          Log.getLogWriter().info("getRandomRegion returned null, sleeping 1 ms to waiting for dynamicRegionCreation");
          MasterController.sleepForMs(1000);
          aRegion = getRandomRegion(!isDynamicRegion);
       }
       Log.getLogWriter().info("Working on Region " + aRegion.getName());
       doEntryOperations(aRegion);
    }

    /**
     * Close the cache (and disconnect from the DS)
     * This should prevent the ShutdownException: no down protocol available!
     * during Shutdown (if locators (client vms) are shut down prior to 
     * application client vms
     */
    public static void closeCacheAndDisconnectFromDS() {
       CacheHelper.closeCache();
       DistributedSystemHelper.disconnect();
    }

   /**
    *  This validation method coordinates client operations/validation with the
    *  healthController (forcedDisconnect of bridge/gateway).  
    *
    *  Called directly from putSequentialKeys task which coordinates 
    *  publishing, forcedDisconnect of server (healthController) and
    *  validation by pausing and waiting for activities to sync up.
    *
    *  Use validateKeysInRegion for concurrent (non-pausing) tests
    *  which use doEntryOperations() on randomKeys vs. putSequentialKeys/Values.
    */
    protected void validateSequentialKeys() {

       // Wait for Queues to drain
       SilenceListener.waitForSilence(30, 5000);

       Region aRegion = null;
       Set rootRegions = CacheHelper.getCache().rootRegions();
       for (Iterator rit = rootRegions.iterator(); rit.hasNext(); ) {
          aRegion = (Region)rit.next();
          Set subRegions = aRegion.subregions(true);
          for (Iterator sit = subRegions.iterator(); sit.hasNext();) {
             aRegion = (Region)sit.next();
             validateRegion(aRegion, true);
          }
       }

       Log.getLogWriter().info("Validation complete");

       MLRioBB.getBB().getSharedCounters().increment(MLRioBB.validationComplete);
       long numPublishers = MLRioBB.getBB().getSharedCounters().read(MLRioBB.numPublishers);
       TestHelper.waitForCounter(MLRioBB.getBB(), "validationComplete", MLRioBB.validationComplete, numPublishers, true, 120000);
    }

   /**
    *  This validation task does not do any coordination between the 
    *  publishing/forcedDisconnect or validation.  It should be used as a 
    *  one time call at the end of the test (as a CLOSETASK).
    *
    *  Verifies the region has the same keySet as the server region and
    *  that ValueHolder.myValue is correct for each key.
    *
    *  concurrent (non-pausing) tests should use HydraCloseTask_validateKeysInRegion().
    */
    public static void HydraCloseTask_validateKeysInRegion() {
       testInstance.validateKeysInRegion();
    }

    protected void validateKeysInRegion() {

       // Wait for Queues to drain
       SilenceListener.waitForSilence(30, 5000);

       Region aRegion = null;
       Set rootRegions = CacheHelper.getCache().rootRegions();
       for (Iterator rit = rootRegions.iterator(); rit.hasNext(); ) {
          aRegion = (Region)rit.next();
          Set subRegions = aRegion.subregions(true);
          for (Iterator sit = subRegions.iterator(); sit.hasNext();) {
             aRegion = (Region)sit.next();
             validateRegion(aRegion, false);
          }
       }
       Log.getLogWriter().info("Validation complete");
    }

    protected void validateRegion(Region aRegion, boolean verifyModVal) {

       Log.getLogWriter().info("Validating entries for Region " + aRegion.getName());

       // All clients/servers should have all keys
       Set serverKeys = aRegion.keySetOnServer();
       int expectedRegionSize = serverKeys.size();

       Set localKeys = aRegion.keySet();
       int regionSize = localKeys.size();
 
       // If region is empty (on client & server), there's no need to go further
       if ((regionSize == 0) && (expectedRegionSize == 0)) {
          return;
       }

       StringBuffer aStr = new StringBuffer();
       Set myEntries = aRegion.entrySet();
       Object[] entries = myEntries.toArray();
       Comparator myComparator = new RegionEntryComparator();
       java.util.Arrays.sort(entries, myComparator);

       Log.getLogWriter().info("Checking " + regionSize + " entries against " + expectedRegionSize + " entries in the server's region");

       aStr.append("Expecting " + expectedRegionSize + " entries in Region " + aRegion.getFullPath() + ", found " + entries.length + "\n");

       for (int i=0; i < entries.length; i++) {
          Region.Entry entry = (Region.Entry)entries[i];
          String key = (String)entry.getKey();
          BaseValueHolder val = (BaseValueHolder)entry.getValue();
          if (verifyModVal) {
             aStr.append("\t" + key + " : " + val.modVal + "\n");
          } else {
             if (val != null) {
                aStr.append("\t" + key + " : " + val.myValue + "\n");
             } else {
                aStr.append("\t" + key + " : " + " null\n");
             }
          }
       }

       // Okay to display for small region sizes
       if (regionSize <= 10) {
          Log.getLogWriter().info(aStr.toString());
       }
       
       aStr = new StringBuffer();
       if (regionSize != expectedRegionSize) {
          aStr.append("Expected " + expectedRegionSize + " keys in Region " + aRegion.getFullPath() + " but found " + regionSize + "\n");
       }

       Log.getLogWriter().info("Checking for missing or extra keys in client region");
       // Extra keys (not in the server region)?
       List unexpectedKeys = new ArrayList(localKeys);
       unexpectedKeys.removeAll(serverKeys);
       if (unexpectedKeys.size() > 0) {
          aStr.append("Extra keys (not found on server): " + unexpectedKeys + "\n");
       } 

       // Are we missing keys (found in server region)?
       List missingKeys= new ArrayList(serverKeys);
       missingKeys.removeAll(localKeys);
       if (missingKeys.size() > 0) { 
          aStr.append("Missing keys (found on server, but not locally) = " + missingKeys + "\n");
       }
 
       if (aStr.length() > 0) {
          throw new TestException(aStr.toString() + " " + TestHelper.getStackTrace());
       }

       // All entries should have value ITERATIONS (verifyModVal)
       // skipped/duplicate values will be detected/reported by Listeners
       for (int i=0; i < entries.length; i++) {
          Region.Entry entry = (Region.Entry)entries[i];
          String key = (String)entry.getKey();
          BaseValueHolder val = (BaseValueHolder)entry.getValue();

          if (verifyModVal) {
             if (val.modVal.intValue() != ITERATIONS) {
                throw new TestException("Mod value for key " + key + " is " + val.modVal + ", expected " + ITERATIONS);
             }
          } 
       }
    }

   /**
    * Selects random region (copied from dynamicReg/DynamicRegionTest.java)
    */
   protected Region getRandomRegion(boolean allowRootRegion) {
      // select a root region to work with
      Region rootRegion = getRandomRootRegion();
   
      Set subregionsSet = rootRegion.subregions(true);
      Log.getLogWriter().fine("getRandomRegion (" + rootRegion.getName() + " has the following subregions: " + subregionsSet);

      if (subregionsSet.size() == 0) {
        if (allowRootRegion) {
          return rootRegion;
        }
        else {
            return null;
        }
      }
      ArrayList aList = null;
      try {
        Object[] array = subregionsSet.toArray();
        aList = new ArrayList(array.length);
        for (int i=0; i<array.length; i++) {
          aList.add(array[i]);
        }
      } catch (NoSuchElementException e) {
         throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
      }
      if (allowRootRegion) {
         aList.add(rootRegion);
      }
      if (aList.size() == 0) { // this can happen because the subregionSet can change size after the toArray
         return null;
      }
      int randInt = TestConfig.tab().getRandGen().nextInt(0, aList.size() - 1);
      Region aRegion = (Region)aList.get(randInt);
      if (aRegion == null) {
         throw new TestException("Bug 30171 detected: aRegion is null");
      }
      return aRegion;
   }
   
   /**
    *  Creates a dynamic region (from dynamicReg/DynamicRegionTest.java)
    */
   
      protected Region createDynamicRegion(String parentName, String drName) {
   
       Region dr = null;
       // dynamic region inherits attributes of parent
       Log.getLogWriter().info("Creating dynamic region " + parentName + "/" + drName);
       try {
         dr = DynamicRegionFactory.get().createDynamicRegion(parentName, drName);
       } catch (CacheException ce) {
         throw new TestException(TestHelper.getStackTrace(ce));
       }
       Log.getLogWriter().info("Created dynamic region " + TestHelper.regionToString(dr, true));
       return dr;
   }
   
   /**
    *  Selects and returns a random rootRegion
    */
    protected Region getRandomRootRegion() {
       Set rootRegions = CacheHelper.getCache().rootRegions();
       Log.getLogWriter().fine("getRandomRootRegion found " + rootRegions);
       int randInt = TestConfig.tab().getRandGen().nextInt(0, rootRegions.size() - 1);
       Object[] regionList = rootRegions.toArray();
       Region rootRegion = (Region)regionList[randInt];
       return (rootRegion);
    }

    public synchronized static void HydraEndTask_verifyConflation() {
       if (testInstance == null) {
          testInstance = new MLRioTest();
          testInstance.verifyConflation();
       }
    }

    protected void verifyConflation() {
       // Display BB stats
       SharedCounters sc = MLRioBB.getBB().getSharedCounters();
       long missedUpdates = sc.read(MLRioBB.missedUpdates);
 
       Log.getLogWriter().info("verifyConflation: missedUpdates = " + missedUpdates);
       // Display gatewayStats.eventsNotQueuedConflated (total)
       double eventsConflated = getConflatedEventCount();

       Log.getLogWriter().info("verifyConflation: eventsConflated = " + eventsConflated);

       // Ensure that we actually did conflate (at the GatewayHub) 
       if (eventsConflated <= 0) {
          throw new TestException("Tuning required.  Test expected Gateway batchConflation, but GatewayStatistics.eventsNotQueuedConflated = " + eventsConflated);
       }
   }

   protected double getConflatedEventCount() {
     String spec = null;
     if(isNewWanConfig){
       spec = "*bridge* " // search all BridgeServer archives
         + "GatewaySenderStatistics "
         + "* " // match all instances
         + "eventsNotQueuedConflated "
         + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
         + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
         + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
     }else{
       spec = "*bridge* " // search all BridgeServer archives
         + "GatewayStatistics "
         + "* " // match all instances
         + "eventsNotQueuedConflated "
         + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
         + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
         + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
     }
     
       List aList = PerfStatMgr.getInstance().readStatistics(spec);
       if (aList == null) {
          Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
          return 0.0;
       }
       double eventsConflated = 0;
       for (int i = 0; i < aList.size(); i++) {
          PerfStatValue stat = (PerfStatValue)aList.get(i);
          eventsConflated += stat.getMax();
       }
       return eventsConflated;
    }

    /**
     * Check ShutdownExceptions and CacheClosedExceptions for underlying
     * ForcedDisconnectExceptions (Caused by within the reported exception
     * or if this VM processed a RegionDestroyedException with Operation
     * FORCED_DISCONNECT).
     *
     */
    static private boolean isCausedByForcedDisconnect(Exception e) {
       Log.getLogWriter().info("checkForForcedDisconnect processed Exception " + e);
       String errStr = e.toString();
       boolean causedByForcedDisconnect = errStr.indexOf("com.gemstone.gemfire.ForcedDisconnectException") >= 0;
       return (causedByForcedDisconnect);
    }
}

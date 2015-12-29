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
package durableClients;

import hct.ha.HAClientQueue;
import hct.ha.HAClientQueueBB;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientCacheHelper;
import hydra.ClientRegionHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.NetworkHelper;
import hydra.RegionHelper;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.BridgeServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.LocalRegionHelper;
import com.gemstone.gemfire.internal.cache.ha.HAHelper;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.HaHelper;

import cq.CQUtil;

/**
 * Contains the hydra task for the Durable Clients test
 * 
 * @author Aneesh Karayil
 * @since 5.2
 * 
 */
public class DurableClientsTest {

  // how cache is created (ClientCacheFactory vs. CacheFactory)
  static protected final boolean useClientCache = TestConfig.tab().booleanAt(DurableClientsPrms.useClientCache, false);

  // test region Name
  static protected final String REGION_NAME = TestConfig.tab().stringAt(
      DurableClientsPrms.regionName, "Region");

  static long killInterval = TestConfig.tab().intAt(
      DurableClientsPrms.killInterval, 60000);

  static Random rand = new Random();

  public final static String PARTIAL_LIST = "PARTIAL LIST : ";

  public final static String FULL_LIST = "FULL LIST : ";
  
  public final static String SLOW_START_TIME="80000";
   
  public static volatile boolean receivedLastKey = false;
  
  public final static int NETDOWN_TIME = 30; // Time in seconds
  
  private static PoolImpl myPool = null;
  
  public static volatile long lastEventReceivedTime = 0;
  

  /**
   * Initializes the test region in the cache server VM.
   */
  public static void initCacheServer() {
    
    if (TestConfig.tab().booleanAt(DurableClientsPrms.isExpirationTest, false)) {
      CacheClientProxy.isSlowStartForTesting = true;
      System.setProperty("slowStartTimeForTesting", SLOW_START_TIME);
      Log.getLogWriter().info(
          "Configuring the test with slowed down dispatcher.");
    }
    
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    int numOfRegion = TestConfig.tab().intAt(
        DurableClientsPrms.numberOfRegions, 1);

    for (int i = 0; i < numOfRegion; i++) {
      RegionHelper.createRegion(REGION_NAME + i, ConfigPrms.getRegionConfig());
    }
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
  }

  /**
   * Initializes the client caches and the regions
   * 
   */

  public static void initCacheClient() {
    synchronized (DurableClientsTest.class) {
      if (getCache() == null) {  // first thread
        // create the cache and region
        GemFireCache cache = createCache();

        int numOfRegion = TestConfig.tab().intAt(
            durableClients.DurableClientsPrms.numberOfRegions, 1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = createRegion(REGION_NAME + i);
          PoolImpl mybw = ClientHelper.getPool(region);
          DurableClientsTest.myPool = mybw;
          Log.getLogWriter().info(
              "The primary server endpoint is " + mybw.getPrimaryName());

          String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
              .getAnyInstance()).getConfig().getDurableClientId();
          Log.getLogWriter().info(" VM Durable Client Id is " + VmDurableId);
          if (!DurableClientsBB.getBB().getSharedMap().containsKey(VmDurableId)) {
            HashMap map = new HashMap();
            DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
          }
        }
        // client sending the ready message
        readyForEvents();
      }
    }

  }

  /**
   * Initializes the test region in the cache client VM and registers a CQ
   */
  public static void initCQClient() {
    synchronized (HAClientQueue.class) {
      if (getCache() == null) {
        // create the cache and region
        GemFireCache cache = createCache();

        // init CQ Service
        CQUtil.initialize();
        CQUtil.initializeCQService(useClientCache); 

        int numOfRegion = TestConfig.tab().intAt(
            durableClients.DurableClientsPrms.numberOfRegions, 1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = createRegion(REGION_NAME + i);
          PoolImpl mybw = ClientHelper.getPool(region);
          myPool = mybw;
          Log.getLogWriter().info(
              "The primary server endpoint is " + mybw.getPrimaryName());
          String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
              .getAnyInstance()).getConfig().getDurableClientId();
          Log.getLogWriter().info(" VM Durable Client Id is " + VmDurableId);
          if (!DurableClientsBB.getBB().getSharedMap().containsKey(VmDurableId)) {
            HashMap map = new HashMap();
            DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
          }
          Log.getLogWriter().info("Doing durable cq register");
          CQUtil.registerDurableCQ(region);
        }
        readyForEvents();
      }
    }
  }
  
  /**
   * Register Interest Hydra task for clients
   * 
   */

  public static void registerInterest() {

    String registerInterestKeys = TestConfig.tab().stringAt(
        durableClients.DurableClientsPrms.registerInterestKeys);

    if (registerInterestKeys.equalsIgnoreCase("allKeys")) {
      registerInterestALL_KEYS();
    }
    else if (registerInterestKeys.equalsIgnoreCase("partialKeysList")) {
      registerInterestPartial_List();
    }
    else if (registerInterestKeys.equalsIgnoreCase("fullKeysList")) {
      registerInterestFull_List();
    }
    else {
      throw new TestException(
          "Test issue - Invalid register interest configuration");
    }
  }

  /**
   * Register Interest ALL_KEYS
   * 
   */

  public static void registerInterestALL_KEYS() {
    Log.getLogWriter().info("Registering interest on all keys");
    int numOfRegion = TestConfig.tab().intAt(
        durableClients.DurableClientsPrms.numberOfRegions, 1);
    InterestResultPolicy interestResultPolicy = DurableClientsPrms.getInterestResultPolicy();

    for (int i = 0; i < numOfRegion; i++) {
      Region region = getRegion(REGION_NAME + i);
      region.registerInterest("ALL_KEYS", interestResultPolicy, true/* durable */);
      hydra.MasterController.sleepForMs(5000);
    }
  }

  /**
   * Register Interest on Partial List of Keys
   */
  public static void registerInterestPartial_List() {
    Log.getLogWriter().info("Registering interest on partial list of keys");
    int numOfRegion = TestConfig.tab().intAt(
        durableClients.DurableClientsPrms.numberOfRegions, 1);
    InterestResultPolicy interestResultPolicy = DurableClientsPrms.getInterestResultPolicy();

    for (int i = 0; i < numOfRegion; i++) {
      Region region = getRegion(REGION_NAME + i);
      Log.getLogWriter().info(
          "List size is :"
              + ((List)(durableClients.DurableClientsBB.getBB().getSharedMap()
                  .get(PARTIAL_LIST))).size());
      region.registerInterest(durableClients.DurableClientsBB.getBB()
          .getSharedMap().get(PARTIAL_LIST), interestResultPolicy, true);
      hydra.MasterController.sleepForMs(5000);
      if (TestConfig.tab().booleanAt(
          durableClients.DurableClientsPrms.putLastKey, false)) {
        region.registerInterest(durableClients.Feeder.LAST_KEY, interestResultPolicy, true);
      }
    }
  }

  /**
   * Register Interest on Full List of Keys
   */
  public static void registerInterestFull_List() {
    Log.getLogWriter().info("Registering interest on full list of keys");
    int numOfRegion = TestConfig.tab().intAt(
        durableClients.DurableClientsPrms.numberOfRegions, 1);
    InterestResultPolicy interestResultPolicy = DurableClientsPrms.getInterestResultPolicy();

    for (int i = 0; i < numOfRegion; i++) {
      Region region = getRegion(REGION_NAME + i);
      Log.getLogWriter().info(
          "List size is :"
              + ((List)(durableClients.DurableClientsBB.getBB().getSharedMap()
                  .get(FULL_LIST))).size());
      region.registerInterest(durableClients.DurableClientsBB.getBB()
          .getSharedMap().get(FULL_LIST), interestResultPolicy, true);
      hydra.MasterController.sleepForMs(5000);
      if (TestConfig.tab().booleanAt(
          durableClients.DurableClientsPrms.putLastKey, false)) {
        region.registerInterest(durableClients.Feeder.LAST_KEY, interestResultPolicy, true);
      }
    }

  }

  /**
   * Kill the clients
   * 
   */

  public static void killClient() {
    try {
      hydra.MasterController.sleepForMs(5000);
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }

  /**
   * Kill the clients with normal shutdown and client keep-alive option as true
   * 
   */
  public static void clientNormalShutDown() {
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      // close the cache and disconnect from the distributed system
      // closing the cache with the keep-alive option true for the durable
      // clients
      closeCache(true);
      //DistributedSystemHelper.getDistributedSystem().disconnect();
    }
    DurableClientsTest.initCacheClient();
    DurableClientsTest.registerInterest();

  }
  
  /**
   * Kill the cq clients with normal shutdown and client keep-alive option as true
   * 
   */

  public static void cqClientNormalShutDown() {
    hydra.MasterController.sleepForMs(3000);
    if (DistributedSystemHelper.getDistributedSystem() != null) {
      // close the cache and disconnect from the distributed system
      closeCache(true);
      //DistributedSystemHelper.getDistributedSystem().disconnect();
    }
    DurableClientsTest.initCQClient();

  }
  
  /**
   * Causing the client to temporarily disconnect from the server
   * 
   */
  public static void clientNetDown() {
    String remote1 = TestConfig.tab().stringAt(
        DurableClientsPrms.remoteMachine1);
    String remote2 = TestConfig.tab().stringAt(
        DurableClientsPrms.remoteMachine2);

    NetworkHelper.printConnectionState();
    NetworkHelper.dropConnectionTwoWay(remote1, remote2);
    Log.getLogWriter().info("Sleeping for " + NETDOWN_TIME + " Secs");
    hydra.MasterController.sleepForMs(NETDOWN_TIME * 1000);
    NetworkHelper.printConnectionState();
    NetworkHelper.restoreConnectionTwoWay(remote2, remote1);
    NetworkHelper.printConnectionState();
  }
  

  /**
   * Killing the Stable server
   * 
   */

  public static void killStableServer() throws ClientVmNotFoundException {

    killServer();

  }

  public static void killServer() throws ClientVmNotFoundException {

    Set dead;
    Set active;

    int numOfRegion = TestConfig.tab().intAt(
        durableClients.DurableClientsPrms.numberOfRegions, 1) - 1;
    Region region = getRegion(REGION_NAME + numOfRegion);

    active = ClientHelper.getActiveServers(region);

    int minServersRequiredAlive = TestConfig.tab().intAt(
        DurableClientsPrms.minServersRequiredAlive, 3);

    if (active.size() < minServersRequiredAlive) {
      Log.getLogWriter().info(
          "No kill executed , a minimum of " + minServersRequiredAlive
              + " servers have to be kept alive");
      return;
    }

    long now = System.currentTimeMillis();
    Long lastKill = (Long)DurableClientsBB.getBB().getSharedMap().get(
        "lastKillTime");
    long diff = now - lastKill.longValue();

    if (diff < killInterval) {
      Log.getLogWriter().info("No kill executed");
      return;
    }
    else {
      DurableClientsBB.getBB().getSharedMap()
          .put("lastKillTime", new Long(now));
    }

    List endpoints = BridgeHelper.getEndpoints();
    int index = rand.nextInt(endpoints.size() - 1);
    BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints
        .get(index);
    ClientVmInfo target = new ClientVmInfo(endpoint);
    target = ClientVmMgr.stop("Killing random cache server",
        ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND, target);
    Log.getLogWriter().info("Server Killed : " + target);

    int sleepSec = TestConfig.tab().intAt(DurableClientsPrms.restartWaitSec, 1);
    Log.getLogWriter().info("Sleeping for " + sleepSec + " seconds");
    MasterController.sleepForMs(sleepSec * 1000);


    active = ClientHelper.getActiveServers(region);

    ServerLocation server = new ServerLocation(endpoint.getHost(), endpoint.getPort());
    if (active.contains(server)) {
      Log.getLogWriter().info(
          "ERROR: Killed server " + server + " found in Active Server List: "
          + active);
    }

    ClientVmMgr.start("Restarting the cache server", target);

    int sleepMs = ClientHelper.getRetryInterval(region) + 1000;
    Log.getLogWriter().info("Sleeping for " + sleepMs + " ms");
    MasterController.sleepForMs(sleepMs);

    active = ClientHelper.getActiveServers(region);

    if (active.contains(server)) {
      // throw new HydraRuntimeException("ERROR: Restarted server "
      Log.getLogWriter().info(
          "ERROR: Restarted server " + server  + " not in Active Server List" + active);
    }

    return;

  }

  /**
   * Putting the stats
   * 
   */
  public static void putHAStatsInBlackboard() {

    DurableClientsBB.getBB().printSharedMap();
  }

  /**
   * Get the stats for conflated events for the conflation tests
   * 
   */
  public static void getConflationStats() {
    
    Cache cache = GemFireCacheImpl.getInstance();
    Iterator itr = cache.getCacheServers().iterator();
    BridgeServerImpl server = (BridgeServerImpl)itr.next();
    Iterator iter_prox = server.getAcceptor().getCacheClientNotifier()
        .getClientProxies().iterator();
    while (iter_prox.hasNext()) {
      CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
      if (HaHelper.checkPrimary(proxy)) {
        ClientProxyMembershipID proxyID = proxy.getProxyID();
        Log.getLogWriter().info("Proxy id : " + proxyID.toString());
        HARegion region = (HARegion)proxy.getHARegion();
        HARegionQueue haRegionQueue = HAHelper.getRegionQueue(region);
        HashMap statMap = new HashMap();
        statMap.put("eventsConflated", new Long(HAHelper.getRegionQueueStats(
            haRegionQueue).getEventsConflated()));

        Log.getLogWriter().info(
            "Events conflated = "
                + (HAHelper.getRegionQueueStats(haRegionQueue)
                    .getEventsConflated()));
        Log.getLogWriter().info(
            "Events Queued = "
                + new Long(HAHelper.getRegionQueueStats(haRegionQueue)
                    .getEventsEnqued()));
        DurableClientsBB.getBB().getSharedMap()
            .put(proxyID.toString(), statMap);
        DurableClientsBB.getBB().getSharedCounters().add(
            DurableClientsBB.NUM_GLOBAL_CONFLATE,
            (HAHelper.getRegionQueueStats(haRegionQueue).getEventsConflated()));
      }
    }

  }
  
  /**
   * This task is for the client Q conflation tests. This is to verify that
   * conflation do happened for this client events.(checks for individual
   * clients not just the total.
   */
  public static void verifyIndividualConflation() {
    ClientProxyMembershipID cpm = myPool.getProxyID();
    String proxyIdStr = cpm.toString();
    Log.getLogWriter().info("Proxy String : " + proxyIdStr);

    HashMap mp = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
        proxyIdStr);

    Log.getLogWriter().info(mp.toString());

    Long numOfConflated = (Long)mp.get("eventsConflated");

    Log.getLogWriter().info("value of numOfConflated : " + numOfConflated);

    long totalConflation = DurableClientsBB.getBB().getSharedCounters().read(
        DurableClientsBB.NUM_GLOBAL_CONFLATE);
    Log.getLogWriter().info("Total Conflation : " + totalConflation);
    if (numOfConflated.equals(new Long(0))) {
      throw new TestException(
          "Expected conflation to happen but has it did not happen");
    }

  }

  /**
   * This task is for the client Q conflation tests. This is to verify that no
   * conflation happened for this client events.
   */
  public static void verifyNoConflation() {
    ClientProxyMembershipID cpm = myPool.getProxyID();
    String proxyIdStr = cpm.toString();
    Log.getLogWriter().info("Proxy String : " + proxyIdStr);

    HashMap mp = (HashMap)DurableClientsBB.getBB().getSharedMap().get(
        proxyIdStr);

    Log.getLogWriter().info(mp.toString());

    Long numOfConflated = (Long)mp.get("eventsConflated");

    Log.getLogWriter().info("value of numOfConflated : " + numOfConflated);

    long totalConflation = DurableClientsBB.getBB().getSharedCounters().read(
        DurableClientsBB.NUM_GLOBAL_CONFLATE);
    Log.getLogWriter().info("Total Conflation : " + totalConflation);
    if (!numOfConflated.equals(new Long(0))) {
      throw new TestException("Expected no conflation to happen but has "
          + numOfConflated + " events");
    }

  }

  /**
   * Task to confirm conflation happening in the test
   * 
   */
  public static void checkForConflation() {

    long totalConflation = DurableClientsBB.getBB().getSharedCounters().read(
        DurableClientsBB.NUM_GLOBAL_CONFLATE);
    Log.getLogWriter().info("Total Conflation : " + totalConflation);
    if (totalConflation == 0) {
      throw new TestException("No conflation done - Test Issue - needs tuning ");
    }
  }

  /**
   * Verify the region entries with the feeder entries
   * 
   */

  public static void verifyDataInRegion() {

    // hydra.MasterController.sleepForMs(50000);

    // waitForLastKeyReceivedAtClient();
    checkBlackBoardForException();
    
    int numOfRegion = TestConfig.tab().intAt(
        durableClients.DurableClientsPrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      Region region = getRegion(REGION_NAME + i);
      Log.getLogWriter().info(
          "Validating the keys of the Region " + region.getFullPath() + " ...");
      if (region.isEmpty()) {
        throw new TestException(" Region has no entries to validate ");
      }

      Iterator iterator = region.entrySet(false).iterator();
      Region.Entry entry = null;
      Object key = null;
      Object value = null;
      while (iterator.hasNext()) {
        entry = (Region.Entry)iterator.next();
        key = entry.getKey();
        value = entry.getValue();
        if (value != null) {
          if (DurableClientsBB.getBB().getSharedMap().get(key) != null) {
            if (!DurableClientsBB.getBB().getSharedMap().get(key).equals(value)) {
              throw new TestException(" expected value to be "
                  + DurableClientsBB.getBB().getSharedMap().get(key)
                  + " for key " + key + " but is " + value);
            }
          }
          else {
            throw new TestException(
                " expected value to be present in the shared map but it is not for key "
                    + key);
          }
        }
        else {
          if (DurableClientsBB.getBB().getSharedMap().get(key) != null) {
            throw new TestException(
                " expected value to be null but it is not so for key " + key);
          }
        }
      }
    }
  }

  /**
   * Puts the events of the reference Vm in blackboard
   * 
   */

  public static void mentionReferenceInBlackboard() {

    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();
    Log.getLogWriter().info("Reference vm is : " + VmDurableId);

    DurableClientsBB.getBB().getSharedMap().put("REFERENCE VM:", VmDurableId);
    // Setting the time for the lastKillTime to zero
    DurableClientsBB.getBB().getSharedMap().put("lastKillTime", new Long(0));
  }

  /**
   * Validate the events received by the clients with the reference Vm
   * 
   * 
   */

  public static void validateEventsReceived() {

    // hydra.MasterController.sleepForMs(300000);
        
    // waitForLastKeyReceivedAtClient();
    checkBlackBoardForException();
    
    DurableClientsBB.getBB().printSharedMap();

    String ReferenceVm = (String)DurableClientsBB.getBB().getSharedMap().get(
        "REFERENCE VM:");

    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    HashMap currentVmMap = (HashMap)DurableClientsBB.getBB().getSharedMap()
        .get(VmDurableId);
    HashMap referenceMap = (HashMap)DurableClientsBB.getBB().getSharedMap()
        .get(ReferenceVm);

    if (currentVmMap.isEmpty()) {
      throw new TestException(" The map of threads is empty for the Vm "
          + VmDurableId);
    }

    if (referenceMap.isEmpty()) {
      throw new TestException(
          " The map of threads is empty for the Reference Vm " + ReferenceVm);
    }

    Iterator iterator = referenceMap.entrySet().iterator();
    Map.Entry entry = null;
    Object key = null;
    Object value = null;

    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      key = entry.getKey();
      value = entry.getValue();

      HashMap referenceThreadMap = (HashMap)value;

      Iterator iterator1 = referenceThreadMap.entrySet().iterator();
      Map.Entry entry1 = null;
      Object key1 = null;
      Object value1 = null;

      while (iterator1.hasNext()) {
        entry1 = (Map.Entry)iterator1.next();
        key1 = entry1.getKey();
        value1 = entry1.getValue();

        // Log.getLogWriter().info("Reference value =
        // "+((EntryEvent)value1).getKey()+ "
        // "+((EntryEvent)value1).getNewValue()+ "Client value "+
        // ((EntryEvent)((HashMap)currentVmMap.get(key)).get(key1)).getKey()+ "
        // "+((EntryEvent)((HashMap)currentVmMap.get(key))).getNewValue());

        if (((String)key1).startsWith("EVENT SR. No : ")) {
          if (currentVmMap.get(key) == null) {
            throw new TestException("No event map for the key " + key
                + " because client didnot receive any events for this key");
          }
          if (((HashMap)currentVmMap.get(key)).get(key1) == null) {

            throw new TestException(
                "Event not received by the client for the key " + key
                    + " for the event " + key1);
          }

          if (!value1.equals(((HashMap)currentVmMap.get(key)).get(key1))) {
            throw new TestException(" For the thread " + key + " the event no "
                + key1 + " has different events");
          }
        }

      }
    }

  }

  /**
   * Check the blackboard for any event of Exception thrown by the java threads
   * 
   */

  public static void checkBlackBoardForException() {
    long exceptionCount = DurableClientsBB.getBB().getSharedCounters().read(
        DurableClientsBB.NUM_EXCEPTION);
    long exceptionLoggingComplete = DurableClientsBB.getBB()
        .getSharedCounters().read(
            DurableClientsBB.NUM_COMPLETED_EXCEPTION_LOGGING); // to take care
                                                                // of 38583, if
                                                                // logging not
                                                                // complete will
                                                                // be caught
                                                                // next time.
    
    SharedMap sharedmap = DurableClientsBB.getBB().getSharedMap();

    if (exceptionCount > 0
        && (exceptionLoggingComplete > 0 || sharedmap.get(new Long(1)) != null)) {
      StringBuffer reason = new StringBuffer();
      reason.append("total exceptions = " + exceptionCount);
      reason.append("\n");

      for (long i = 1; i < exceptionCount + 1; i++) {
        reason.append("Reason for exception no. " + i + " : ");
        reason.append(sharedmap.get(new Long(i)));
        reason.append("\n");
      }

      throw new TestException(reason.toString());
    }
  }
  
  /**
   * Task to wait till the client receives its last key
   */
  public static void waitForLastKeyReceivedAtClient() {
    long hydraTaskWaitSec = TestConfig.tab()
        .longAt(hydra.Prms.maxResultWaitSec);
    long waitTime = 0;
    long maxWaitTime = 0;
    if (hydraTaskWaitSec < 300) {
      waitTime = 50;
    }
    else {
      waitTime = 100;
    }
    maxWaitTime = (hydraTaskWaitSec - waitTime) * 1000;
    Log.getLogWriter().info(
        "Setting the value for last key wait time to " + maxWaitTime + " ms");
    long start = System.currentTimeMillis();
    boolean checkedListnerActive = false;

    while (!receivedLastKey && !checkedListnerActive) { // wait until condition
                                                        // is
      // met
      if ((System.currentTimeMillis() - start) > maxWaitTime) {
        long currentTime = System.currentTimeMillis();
        long timeElapsedSinceLastEvent = currentTime - lastEventReceivedTime;
        checkedListnerActive = true;
        if (timeElapsedSinceLastEvent > 20000) { // 20 secs
          throw new TestException(
              "last_key was not received in "
                  + maxWaitTime
                  + " milliseconds, and listner is silent (this probably is not a late event arrival issue), could not proceed for further validation");
        }
        else {
          Log
              .getLogWriter()
              .info(
                  "Last key not yet received, but listner is still active!. So task will be rescheduled");
        }
      }
      try {
        Thread.sleep(5000);
      }
      catch (InterruptedException ignore) {
        Log
            .getLogWriter()
            .info(
                "waitForLastKeyReceivedAtClient : interrupted while waiting for validation");
      }
    }
    if (receivedLastKey) {
      throw new StopSchedulingTaskOnClientOrder(
          "Stopping the task schedule as the last_key received");
    }
    hydra.MasterController.sleepForMs(10000);

  }

  /**
   * This method is an CLOSETASK and does the following : <br>
   * toggle ha overflow status flag in shared black board
   * @since 5.7
   */
  public static void toggleHAOverflowFlag() {
    LocalRegionHelper.isHAOverflowFeaturedUsedInPrimaryPutOnShareBB();
  }
  
  /**
   * This method is an CLOSETASK and does the following : <br>
   * use ha overflow status flag in share black board for <br>
   * inference on HAOverFlow use in test
   * 
   * @throws TestException
   * @since 5.7
   */  
  public static void checkHAOverFlowUsedOnPrimary() {
    int status = (int)HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.HA_OVERFLOW_STATUS);
    // validate Overflow happened
    if (status == 0) {
      throw new TestException(
          "Test issue : Test need tuning - no overflow happened");
    }
  }

  /** 
   * Utility method to call readyForEvents() on either the Cache or ClientCache
   * (depending on test configuration).
   */
  public static void readyForEvents() {
    if (useClientCache) {
      ClientCacheHelper.getCache().readyForEvents();
    } else {
      CacheHelper.getCache().readyForEvents();
    }
  }

  /** 
   * Utility method to close cache (with keepAlive parameter) on either the 
   * Cache or ClientCache (depending on test configuration).
   */
  public static void closeCache(boolean keepAlive) {
    if (useClientCache) {
      ClientCacheHelper.getCache().close(keepAlive);
    } else {
      CacheHelper.getCache().close(keepAlive);
    }
  }

  /**
   * Utility method to get either the Cache or ClientCache (depending on 
   * test configuration).
   */
  public static GemFireCache getCache() {
      GemFireCache cache = null;
      if (useClientCache) {
        cache = ClientCacheHelper.getCache();
      } else {
        cache = CacheHelper.getCache();
      }
      return cache;
   }
  
  /** 
   * Utility method to create the cache or client cache (depending on 
   * test configuration).
   */
  public static GemFireCache createCache () {
    GemFireCache cache = null;
    if (useClientCache) {
      cache = ClientCacheHelper.createCache(ConfigPrms.getClientCacheConfig());
    } else {
      cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
    }
    return cache;
  }

  /**
   * Utility method to create the region via CacheHelepr or ClientCacheHelper
   * depending on test configuration (useClientCache).
   */
  public static Region createRegion(String regionName) {
    Region region = null;
    if (useClientCache) {
      region = ClientRegionHelper.createRegion(regionName, ConfigPrms.getClientRegionConfig());
    } else {
      region = RegionHelper.createRegion(regionName, ConfigPrms.getRegionConfig());
    }
    return region;
  }

  /** 
   * Utility method to get the Region from the Cache or the ClientCache
   *
   */
  private static Region getRegion(String regionName) {
    if (useClientCache) {
      return ClientRegionHelper.getRegion(regionName);
    } else {
      return RegionHelper.getRegion(regionName);
    }
  }
  
}

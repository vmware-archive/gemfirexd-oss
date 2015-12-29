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
package wan.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import objects.ObjectHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.newedge.account.domain.BackOfficeAccount;
import com.newedge.account.domain.BackOfficeAccountCollection;
import com.newedge.staticdata.domain.Product;

import diskRecovery.RecoveryPrms;
import diskRecovery.RecoveryTest;
import diskRecovery.StartupShutdownTest;

import event.EventPrms;
import hydra.CacheHelper;
import hydra.ConfigHashtable;
import hydra.GatewayHubHelper;
import hydra.GsRandom;
import hydra.HydraVector;
import hydra.Log;
import hydra.RegionHelper;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import query.QueryPrms;
import query.QueryTest;
import query.SerialQueryAndEntryOpsTest;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.ValueHolder;
import wan.CacheClientPrms;
import wan.CacheServerPrms;
import wan.WANClient;
import wan.WANClientVersionHelper;

public class WANQueryClient extends RecoveryTest {
  static final int UPDATE_OPERATION = 1;
  static final int QUERY_OPERATION = 2;
  static ConfigHashtable conftab = TestConfig.tab();
  class WANClientInner extends WANClient {
    public WANClientInner() {
      super();
    }
    
    @Override
    public Cache createCache(String cacheConfig) {
      return super.createCache(cacheConfig);
    }
    
    @Override
    public Region createRegion(String regionName, String regionConfig) {
      return super.createRegion(regionName, regionConfig);
    }

    
    public void initPdxDiskStore_1() {
      initPdxDiskStore();
    }
    
    @Override
    public CacheServer startBridgeServer(String bridgeConfig) {
      return super.startBridgeServer(bridgeConfig);
    }
    
    @Override
    public void createGatewayHub() {
      super.createGatewayHub();
    }
    
    /**
     * Registers interest in all keys using the client interest policy.
     */
    public void registerInterest(Region region) {
      InterestResultPolicy interestPolicy = CacheClientPrms.getInterestPolicy();
      LocalRegion localRegion = (LocalRegion)region;
     String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
          .getAnyInstance()).getConfig().getDurableClientId();

      if (!VmDurableId.equals("")) {
        Log.getLogWriter().info("Doing durable register interest");
        localRegion.registerInterest("ALL_KEYS", interestPolicy, true);
      }
      else {
        localRegion.registerInterest("ALL_KEYS", interestPolicy);
      }
      Log.getLogWriter().info("Initialized region " + region
         + "\nRegistered interest in ALL_KEYS with InterestResultPolicy = "
         + interestPolicy);
    }
  }

  /* hydra task methods */
  /* ======================================================================== */
 /* public synchronized static void HydraTask_initialize() {
    if (queryTest == null) {
      queryTest = new WANQueryClient();
    }
    queryTest.initialize();
  }
  
  @Override
  public void initialize() {
    maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, 20000);
    numOfRegions = TestConfig.getInstance().getRegionDescriptions().size()/2;
    ignoreQueryExecTimeOutException = TestConfig.tab().booleanAt(
        QueryPrms.ignoreTimeOutException, false);
  }

  /**
   * Initializes a server cache based on the {@link CacheServerPrms}.
   */
  /*public static void initServerCacheTask() {
    String cacheConfig = TestConfig.tasktab().stringAt(CacheServerPrms.cacheConfig, conftab.stringAt(CacheServerPrms.cacheConfig, null));
    HydraVector regionConfigs = TestConfig.tasktab().vecAt(CacheServerPrms.regionConfig, conftab.vecAt(CacheServerPrms.regionConfig, null));
    String bridgeConfig = TestConfig.tasktab().stringAt(CacheServerPrms.bridgeConfig, conftab.stringAt(CacheServerPrms.bridgeConfig, null));

   
    WANClientInner client = new WANQueryClient().new WANClientInner();
    client.createCache(cacheConfig);
    for (int i = 0; i < regionConfigs.size(); i++) {
      client.createRegion(REGION_NAME +i, (String)regionConfigs.get(i));      
    }
    client.initPdxDiskStore_1();
    client.startBridgeServer(bridgeConfig);
    client.createGatewayHub();
  }
  */
  public static void HydraTask_createGatewayHub() {
    WANClientInner client = new WANQueryClient().new WANClientInner();
    client.createGatewayHub();
  }
  
  public static void HydraTask_sleepForSometime() {
    try {
      Thread.sleep(60000);
    }
    catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  /**
   * Initializes a edge cache based on the {@link CacheClientPrms}.
   */
  /*public static void initEdgeClientCacheTask() {
    String cacheConfig = conftab.stringAt(CacheClientPrms.cacheConfig);
    HydraVector regionConfigs = conftab.vecAt(CacheClientPrms.regionConfig, null);

    WANClientInner client = new WANQueryClient().new WANClientInner();
    Cache cache = client.createCache(cacheConfig);
    for (int i = 0; i < regionConfigs.size(); i++) {
      Region region = client.createRegion(REGION_NAME + i, (String)regionConfigs.get(i));
      PoolImpl mybw = ClientHelper.getPool(region);

      ServerLocation primaryEndpoint = (ServerLocation )mybw.getPrimary();
      Log.getLogWriter()
          .info("The primary server endpoint is " + primaryEndpoint);
      client.registerInterest(region);
    }
  }*/

  public static void closeServerCacheTask() throws Exception {
    CacheHelper.closeCache();
  }
  
  public static void closeEdgeClientCacheTask() throws Exception {
    CacheHelper.closeCache();
  }
  
  /** Loads regions with data. This is a batched task, and each thread will repeatedly
   *  run this task until it loads RecoveryPrms.numToLoad new entries. Each invocation
   *  of this task will keep working until this thread loads the specified number of entries
   *  then it will throw a StopSchedulingTaskOnclientOrder exception.
   */
  public static void HydraTask_updateRegions() {
    int numToLoad = RecoveryPrms.getNumToLoad(); // number of keys to be put by each thread
    int CHUNK_SIZE = 50;
    RandomValues rv = new RandomValues();
    Set<Region<?, ?>> regionSet = CacheHelper.getCache().rootRegions();
    long maxKey = 1 * NameFactory.getPositiveNameCounter();
    Log.getLogWriter().info("MAX KEY :: " + maxKey);
    int PUT_ALL_SIZE = 50;
    GsRandom rand = TestConfig.tab().getRandGen();
    HashMap putAllMap = new HashMap();
    for (int i = 1; i <= PUT_ALL_SIZE; i++) {
      long randInt = rand.nextLong(1, maxKey);
      Object key = NameFactory.getObjectNameForCounter(randInt);
      Object value = new ValueHolder((String)key, rv);
      putAllMap.put(key, value);
    }

    Log.getLogWriter().info("Created putAll map of size " + putAllMap.size() + ", updating all regions with putAll...);");
    for (Region aRegion: regionSet) {
      String regName = aRegion.getName();
      if (!regName.equals("product") && (!regName.equals("backOfficeAccount")) && (!regName.equals("backOfficeAccountCollection"))) {
        Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
            aRegion.getFullPath());
        aRegion.putAll(putAllMap);
      }
    }
    
    Region aRegion = CacheHelper.getCache().getRegion("product");
    for (Object key: putAllMap.keySet()) {
      Product value = new Product();
      value.productCode = (String) key;
      value.instrumentId = (String) key;
      putAllMap.put(key, value);
    }
    Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
        aRegion.getFullPath());
    aRegion.putAll(putAllMap);
    
    aRegion = CacheHelper.getCache().getRegion("backOfficeAccount");
    for (Object key: putAllMap.keySet()) {
      BackOfficeAccount value = new BackOfficeAccount();
      value.account = (String) key;
      putAllMap.put(key, value);
    }
    Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
        aRegion.getFullPath());
    aRegion.putAll(putAllMap);
    
    aRegion = CacheHelper.getCache().getRegion("backOfficeAccountCollection");
    for (Object key: putAllMap.keySet()) {
      BackOfficeAccountCollection value = new BackOfficeAccountCollection();
      putAllMap.put(key, value);
    }
    Log.getLogWriter().info("Calling putAll with map of size " + putAllMap.size() + " with region " +
        aRegion.getFullPath());
    aRegion.putAll(putAllMap);

    Log.getLogWriter().info("Printing current region hierarchy with sizes");
    Log.getLogWriter().info(RecoveryTest.regionHierarchyToString());
  }
  
  /**
   * Do entry and query operations in parallel
   */
  public static void HydraTask_doEntryAndQueryOperations() {
    long startTime = System.currentTimeMillis();

    long minTaskGranularityMS = TestConfig.tab().longAt(
        TestHelperPrms.minTaskGranularitySec)
        * TestHelper.SEC_MILLI_FACTOR;
    do {
       try {
          int whichOp = getOp(QueryPrms.entryAndQueryOperations);
          switch (whichOp) {
             case QUERY_OPERATION:
                StartupShutdownTest.HydraTask_doQueries();
                break;
             case UPDATE_OPERATION:
               HydraTask_updateRegions();
                break;
            
             default: {
                throw new TestException("Unknown operation " + whichOp);
             }
          }
       } finally {
       }
     } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  }

  /**
   * Get an operation.
   * Picked up randomly
   * @param whichPrm
   * @return
   */
  protected static int getOp(Long whichPrm) {
    long limit = 60000;
    long startTime = System.currentTimeMillis();
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    if (operation.equals("update"))
       op =  UPDATE_OPERATION;
    else if (operation.equals("query"))
       op =  QUERY_OPERATION;
    else
       throw new TestException("Unknown entry operation: " + operation);
    if (System.currentTimeMillis() - startTime > limit) {
       // could not find a suitable operation in the time limit; there may be none available
       throw new TestException("Could not find an operation in " + limit + " check that the operations list has allowable choices");
    }
    return op;
  }
}

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
package hct.ha;

import hct.HctPrms;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.Iterator;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;

import cq.CQUtil;

/**
 * Contains Hydra tasks and supporting methods for testing the GemFire cache
 * server.It has methods for initializing and configuring the cache server and
 * cache clients.
 * 
 * @author Suyog Bhokare, Girish Thombare
 * 
 */

public class HAClientQueue
{
  static protected PoolImpl mypool;

  // test region name
  static protected final String REGION_NAME = TestConfig.tab().stringAt(
      HctPrms.regionName);
  
  /**
   * An arbitrary key ( used in some tests to do register/unregister interest on
   * keys other than test-keys set)
   */
  static private final String ARBITRARY_KEY = "AK1324Aasdfkkjhhcpwe";
  
  /**
   * Boolean to indicate that 'last_key' put by feeder has been received by
   * client
   */
  static public volatile boolean lastKeyReceived = false; 
  
  public final static String DEFAULT_DELAY_FOR_DISPATCHER_START = "100000";

  /**
   * Initializes the test region in the cache server VM.
   */
  public static void initCacheServer()
  {
    if (TestConfig.tab().booleanAt(hct.ha.HAClientQueuePrms.delayDispatcherStart,
        false)) {
      CacheClientProxy.isSlowStartForTesting = true;
      long delayMilis = TestConfig.tab().longAt(hct.ha.HAClientQueuePrms.delayDispatcherStartMilis,
          Long.valueOf(DEFAULT_DELAY_FOR_DISPATCHER_START));
      System.setProperty("slowStartTimeForTesting", String.valueOf(delayMilis));
      Log.getLogWriter().info(
          "Configured the test with delayed start for message dispatcher with delay time " + delayMilis + " milis.");
    }
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    int numOfRegion = TestConfig.tab().intAt(
        hct.ha.HAClientQueuePrms.numberOfRegions,1);
    for (int i = 0; i < numOfRegion; i++) {
      RegionHelper.createRegion(REGION_NAME + i, ConfigPrms.getRegionConfig());
    }
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
  }
 
  /**
	 * Initializes the test region in the cache client VM and registers
	 * interest.
	 */
  public static void initCacheClient()
  {
    synchronized (HAClientQueue.class) {
      if (CacheHelper.getCache() == null) { // first thread
        // create the cache and region
        CacheHelper.createCache(ConfigPrms.getCacheConfig());

        int numOfRegion = TestConfig.tab().intAt(
            hct.ha.HAClientQueuePrms.numberOfRegions,1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = RegionHelper.createRegion(REGION_NAME+i, ConfigPrms.getRegionConfig());

          // set the loader and writer statics (same for all regions)
          mypool = ClientHelper.getPool(region);

          // register interest in one thread to avoid possible performance hit
          try {
            if (TestConfig.tab().booleanAt(HctPrms.receiveValuesAsInvalidates)){
              Log.getLogWriter().info("Registering Interest for invalidates.");
              region.registerInterestRegex(".*", false, false);
            } else {
              region.registerInterest("ALL_KEYS");
            }
          }
          catch (CacheWriterException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        }
      }
    }
  }
  
  /**
   * Task for clients to sent ready for events (for durable clients)
   */
  public static void sendClientReadyForEvents(){
    Cache cache = CacheHelper.getCache();
    if(cache == null)
      throw new TestException("Cache is null");
    cache.readyForEvents();
  }
  
  /**
   * Task used for client Q conflation tests. The test currently uses the
   * product test hooks for the client Q conflation settings. Initializes the
   * test region in the cache client VM and registers interest.
   */
  public static void initCacheClientWithConflation() {
    synchronized (HAClientQueue.class) {
      if (CacheHelper.getCache() == null) { // first thread

        // create the cache and region
        Cache cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
        
        int numOfRegion = TestConfig.tab().intAt(
            hct.ha.HAClientQueuePrms.numberOfRegions, 1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = RegionHelper.createRegion(REGION_NAME + i, ConfigPrms
              .getRegionConfig());


          mypool = ClientHelper.getPool(region);
          
          Log.getLogWriter().info(
              "The primary server endpoint is " + mypool.getPrimaryName());

          // register interest in one thread to avoid possible performance hit
          try {
            region.registerInterest("ALL_KEYS");
          }
          catch (CacheWriterException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        }

      }
    }
  }
  
  /**
   * Initializes the test region in the cache client VM and registers a CQ
   */
  public static void initCQClient()
  {
    synchronized (HAClientQueue.class) {
      if (CacheHelper.getCache() == null) { // first thread
        // create the cache and region
        CacheHelper.createCache(ConfigPrms.getCacheConfig());

        // init CQ Service
        CQUtil.initialize();
        CQUtil.initializeCQService();

        int numOfRegion = TestConfig.tab().intAt(
            hct.ha.HAClientQueuePrms.numberOfRegions,1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = RegionHelper.createRegion(REGION_NAME+i, ConfigPrms.getRegionConfig());
          // register CQ in one thread to avoid possible performance hit
          CQUtil.registerCQ(region);
        }
      }
    }
  }
  
  /**
   * Registers the interest on an arbitrary key other than test-keys set.
   * 
   */
  public static void registerInterestOnArbitraryKey()
  {
    Region region = RegionHelper.getRegion(REGION_NAME + 0);
    region.registerInterest(ARBITRARY_KEY);
    Log.getLogWriter().info(
        "registered interest for ARBITRARY_KEY : " + ARBITRARY_KEY);
  }

  /**
   * Unregisters the interest on an arbitrary key other than test-keys set.
   * 
   */
  public static void unregisterInterestOnArbitraryKey()
  {
    Region region = RegionHelper.getRegion(REGION_NAME + 0);
    region.unregisterInterest(ARBITRARY_KEY);
    Log.getLogWriter().info(
        "unregistered interest for ARBITRARY_KEY : " + ARBITRARY_KEY);
  }
  
  public static void waitForLastKeyReceivedAtClient(){
    long maxWaitTime = 200000;
    long start = System.currentTimeMillis();
    while (!lastKeyReceived) { // wait until condition is
      // met

      if ((System.currentTimeMillis() - start) > maxWaitTime) {
        throw new TestException("last_key was not received in " + maxWaitTime
            + " milliseconds, could not proceed for further validation");
      }
      try {
        Thread.sleep(5000);
      }
      catch (InterruptedException ignore) {
        Log.getLogWriter().info(
            "waitForLastKeyReceivedAtClient : interrupted while waiting for validation");
      }
    }
    Validator.checkBlackBoardForException();
  }
  public static void verifyDataInRegion()
  {
    if (TestConfig.tab().booleanAt(hct.ha.HAClientQueuePrms.putLastKey, false)) {
      waitForLastKeyReceivedAtClient();
    }
    Cache cache = CacheFactory.getAnyInstance();
    int numOfRegion = TestConfig.tab().intAt(
        hct.ha.HAClientQueuePrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME + i);
      Log.getLogWriter().info(
          "Validating the keys of the Region " + region.getFullPath() + " ...");
      if (region.isEmpty()){
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
          if (HAClientQueueBB.getBB().getSharedMap().get(key) != null) {
            if (!HAClientQueueBB.getBB().getSharedMap().get(key).equals(value)) {
              throw new TestException(" expected value to be "
                  + HAClientQueueBB.getBB().getSharedMap().get(key)
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
          if (HAClientQueueBB.getBB().getSharedMap().get(key) != null) {
            throw new TestException(
                " expected value to be null but it is not so for key " + key);
          }
        }
      }
    }
  }
  
  /**
   * Kill the clients
   * 
   */
  public static void killClient() {
    int serverReconnectTime = 20000;
    try {
      hydra.MasterController.sleepForMs(5000);
      ClientVmInfo info = ClientVmMgr.stop("Killing the VM",
          ClientVmMgr.MEAN_KILL, serverReconnectTime);
    }
    catch (ClientVmNotFoundException e) {
      Log.getLogWriter().warning(" Exception while killing client ", e);
    }
  }
}

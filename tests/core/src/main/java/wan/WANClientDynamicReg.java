
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

package wan;

import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.RemoteTestModule;

import java.io.File;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.Region;

/**
 * Supports example tests for WAN distribution.
 *
 * @author Jean Farris
 * @since 4.3
 */
public class WANClientDynamicReg extends WANClient {

  // parent region for dynamically created region(s)
  private static final String REGION_NAME_PARENT = "parent";

  //============================================================================
  // INITTASKS
  //============================================================================

  /**
   * Initializes a server cache using {@link CacheServerPrms}.
   */
  public static void initServerCacheDynamicTask() {
    String cacheConfig = conftab.stringAt(CacheServerPrms.cacheConfig);
    String regionConfig = conftab.stringAt(CacheServerPrms.regionConfig);
    String bridgeConfig = conftab.stringAt(CacheServerPrms.bridgeConfig);

    WANClientDynamicReg client = new WANClientDynamicReg();
    client.configureDynamicRegionFactory(regionConfig); // before opening cache
    client.createCache(cacheConfig);
    client.createRegion(REGION_NAME_PARENT, regionConfig);
    client.startBridgeServer(bridgeConfig);
    client.startQueueMonitor();
    client.createGatewayHub();
  }

  /**
   * Initializes an edge client cache using {@link CacheClientPrms}.
   */
  public static void initEdgeClientCacheDynamicTask() {
    String cacheConfig = conftab.stringAt(CacheClientPrms.cacheConfig);
    String regionConfig = conftab.stringAt(CacheClientPrms.regionConfig);

    WANClientDynamicReg client = new WANClientDynamicReg();
    client.configureDynamicRegionFactory(regionConfig);
    client.createCache(cacheConfig);
    client.createRegion(REGION_NAME_PARENT, regionConfig);
  }

  /** Configures and opens DynamicRegionFactory */
  private void configureDynamicRegionFactory(String regionConfig) {
    File d = new File("DynamicRegionData" + hydra.ProcessMgr.getProcessId());
    d.mkdirs();
    DistributedSystemHelper.connect();
    DynamicRegionFactory.Config config = ClientHelper.getDynamicRegionConfig(d, RegionHelper
        .getRegionDescription(regionConfig), true, true);
    DynamicRegionFactory.get().open(config);
  }


  //============================================================================
  // TASKS
  //============================================================================


  /**
   * Puts objects into dynamic Region. Each logical hydra client puts an object
   * over and over at its own key.  The key is the hydra threadgroup id, which
   * runs from 0 to the size of the threadgroup, and is unique for each thread.
   * Sleeps a configurable amount of time before each operation.
   */
  public static void putTaskDynamic() {
    WANClientDynamicReg client = new WANClientDynamicReg();
    client.putDynamic(REGION_NAME_PARENT + "/" + REGION_NAME);
  }

/**   
 *  Create a dynamic region.  
 */
 private void createDynamicRegion(String parentName, String drName) {
    
    Region dr = null;
    // dynamic region inherits attributes of parent
    try {
      dr = DynamicRegionFactory.get().createDynamicRegion(parentName, drName);
    } catch (CacheException ce) {
      throw new TestException(TestHelper.getStackTrace(ce));
    }
    Log.getLogWriter().info("Created dynamic region " + 
        RegionHelper.regionAttributesToString(dr.getAttributes()));
}

  /** Puts objects into the cache. */
  private void putDynamic(String dynamicRegionPath) {
    Region region = RegionHelper.getRegion(dynamicRegionPath);
    if (region == null) {
      logger.info("Creating dynamic region: " + dynamicRegionPath);
      createDynamicRegion(REGION_NAME_PARENT, REGION_NAME);
      region = RegionHelper.getRegion(dynamicRegionPath);
    }
    if (region == null) {
	throw new TestException("Failed to get dynamic region: " + dynamicRegionPath);
    }
    int tid = RemoteTestModule.getCurrentThread().getThreadGroupId();
    logger.info("In putDynamic getThreadGroupId returns: " + tid);
    if(logger.fineEnabled()) {
      int pid = hydra.ProcessMgr.getProcessId();
      logger.fine("hydra.ProcessMgr.getProcessId() returns: " + pid);
    }
    //Integer key = new Integer(tid);
    int sleepMs = CacheClientPrms.getSleepSec() * 1000;
    for (int i = 1; i <= ITERATIONS; i++) {
      MasterController.sleepForMs(sleepMs);
      Integer key = new Integer(i);
      Integer val = new Integer(i);
      region.put(key, val);
      logger.info("Putting into region key: " + key + " val: " + val);
    }
    WANBlackboard.getInstance().getSharedCounters()
                 .setIfLarger(WANBlackboard.MaxKeys, tid);
  }
  

  //============================================================================
  // CLOSETASKS
  //============================================================================


  /**
   * Prints the contents of the client cache as-is.
   */
  public static void printTaskDynamic() {
    // sleep to allow all callbacks to complete
    MasterController.sleepForMs(5000);
    WANClientDynamicReg client = new WANClientDynamicReg();
    client.print(REGION_NAME_PARENT + "/" + REGION_NAME);
  }


  /**
   * Checks that all client caches see the same final result, being careful to
   * only look at the cache as-is.
   * <p>
   * Assumes no two writers ever write the same key concurrently; all writers
   * are in the same thread group, and write only to the key with their hydra
   * threadgroup id; this task is executed by the same threadgroup that did the
   * writing; and all clients are fully mirrored.
   */
  public static void validateTaskDynamic() throws Exception {
    try {
      Thread.interrupted();
      Thread.sleep(60000);
    } catch(Exception e) {
      e.printStackTrace();
    }
    WANClientDynamicReg client = new WANClientDynamicReg();
    client.validate(REGION_NAME_PARENT + "/" + REGION_NAME);
  }


  /**
   * Validates and closes a server cache.
   */
  public static void closeServerCacheDynamicTask() throws Exception {
    // sleep to allow all callbacks to complete
    //MasterController.sleepForMs(30000);
    Log.getLogWriter().info("Closing server cache");
    
    WANClientDynamicReg client = new WANClientDynamicReg();
    client.print(REGION_NAME_PARENT + "/" + REGION_NAME);
    client.validate(REGION_NAME_PARENT + "/" + REGION_NAME);
    client.closeCache();
  }

  /** Validates the contents of the local cache. */
  private void validate(String regionName) throws CacheException {
    Log.getLogWriter().info("Validating cache");

    // get the number of cache entries (one for each thread in the group)
    long size = WANBlackboard.getInstance().getSharedCounters()
                             .read(WANBlackboard.MaxKeys);
    Log.getLogWriter().info("Number of cache entries: " + size);
    // check the value of each entry in the local cache
    Region region = RegionHelper.getRegion(regionName);
    for (int i = 1; i < ITERATIONS; i++) {
      Integer key = new Integer(i);
      // use hook to do local get
      Object val = region.get(key);//DiskRegUtil.getValueInVM(region, key);
      if (val == null) {
        String s = "No value in cache at " + key;
        throw new TestException(s);
      } else if (val instanceof Integer) {
          Log.getLogWriter().info("Got value: " + ((Integer)val).intValue() +  // [bruce] moved from above to avoid NPE on null value
                              " for key: " + key);
          int ival = ((Integer)val).intValue();
          if (ival != key.intValue()) {
            String s = "Wrong value in cache at " + key +", expected "
                   + ITERATIONS + " but got " + ival;
            throw new TestException(s);
          }
      } else {
        String s = "Wrong type in cache at " + key +", expected Integer "
                 + "but got " + val + " of type " + val.getClass().getName();
        throw new TestException(s);
      }
    }
    Log.getLogWriter().info("Validated cache");
  }
}

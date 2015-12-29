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
package delta;

import hct.HctPrms;
import hct.ha.HAClientQueue;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;
import util.TestException;
import util.TestHelper;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientUpdater;

/**
 * Contains Hydra tasks and supporting methods for testing the GemFire cache
 * server.It has methods for initializing and configuring the cache server and
 * cache clients.
 * 
 * @author aingle
 * @since 6.1
 */

public class DeltaPropagation {
  static protected PoolImpl mypool;

  // test region name
  static protected final String REGION_NAME = TestConfig.tab().stringAt(
      HctPrms.regionName);

  public static String VmDurableId="";
  /**
   * Boolean to indicate that 'last_key' put by feeder has been received by
   * client
   */
  static public volatile boolean lastKeyReceived = false;

  /**
   * Initializes the test region in the cache server VM.
   */
  public static void initCacheServer() {
    if (TestConfig.tab().booleanAt(hct.ha.HAClientQueuePrms.delayDispatcherStart,
        false)) {
      CacheClientProxy.isSlowStartForTesting = true;
      long delayMilis = TestConfig.tab().longAt(hct.ha.HAClientQueuePrms.delayDispatcherStartMilis,
          Long.valueOf(HAClientQueue.DEFAULT_DELAY_FOR_DISPATCHER_START));
      System.setProperty("slowStartTimeForTesting", String.valueOf(delayMilis));
      Log.getLogWriter().info(
      "Configured the test with delayed start for message dispatcher with delay time " + delayMilis + " milis.");
    }
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    int numOfRegion = TestConfig.tab().intAt(
        delta.DeltaPropagationPrms.numberOfRegions, 1);
    for (int i = 0; i < numOfRegion; i++) {
      RegionHelper.createRegion(REGION_NAME + i, ConfigPrms.getRegionConfig());
    }
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
  }

  /**
   * Initializes the test region in the cache client VM and registers interest
   * only for non delta's.
   */
  public static void initCacheClientOld() {
    synchronized (DeltaPropagation.class) {
      if (CacheHelper.getCache() == null) { // first thread
        // create the cache and region
        CacheHelper.createCache(ConfigPrms.getCacheConfig());

        int numOfRegion = TestConfig.tab().intAt(
            delta.DeltaPropagationPrms.numberOfRegions, 1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = RegionHelper.createRegion(REGION_NAME + i, ConfigPrms
              .getRegionConfig());

          // set the loader and writer statics (same for all regions)
          mypool = ClientHelper.getPool(region);

          try {
            region.registerInterestRegex(".*_n_d_o");
            region.registerInterest("last_key");
            
          }
          catch (CacheWriterException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        }
      }
    }
  }

  public static void waitForLastKeyReceivedAtClient() {
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
        Log
            .getLogWriter()
            .info(
                "waitForLastKeyReceivedAtClient : interrupted while waiting for validation");
      }
    }
    Validator.checkBlackBoardForException();
  }

  /**
   * Task to verify PR bucket overflow to disk.
   */
  public static void verifyOverflowToDisk() {
    synchronized (DeltaPropagation.class) {
      int numOfRegion = TestConfig.tab().intAt(
        delta.DeltaPropagationPrms.numberOfRegions, 1);
      Cache theCache = CacheHelper.getCache();
      for (int i = 0; i < numOfRegion; i++) {
        Region aRegion = theCache.getRegion(REGION_NAME + i);      
        PartitionedRegion pr = (PartitionedRegion)aRegion;
        verifyOverflowToDisk(pr);
      }
    }  
  }

  public static void verifyOverflowToDisk(PartitionedRegion aRegion) {
    if (aRegion.getLocalMaxMemory() == 0) {
      Log.getLogWriter().info(
        "This is an accessor and hence eviction need not be verified");
      return;
    }

    long numOverflowToDisk = aRegion.getDiskRegionStats()
      .getNumOverflowOnDisk();

    long entriesInVm = aRegion.getDiskRegionStats().getNumEntriesInVM();

    long totalEntriesInBuckets = 0;
    Set bucketList = aRegion.getDataStore().getAllLocalBuckets();
    Log.getLogWriter().info("Number of buckets= " + bucketList.size());
    Iterator iterator = bucketList.iterator();
    while (iterator.hasNext()) {
      Map.Entry entry = (Map.Entry)iterator.next();
      BucketRegion localBucket = (BucketRegion)entry.getValue();
      if (localBucket != null) {
        totalEntriesInBuckets = totalEntriesInBuckets
          + localBucket.entryCount();

      }
    }
    if (bucketList.size() > 0) {
      if (numOverflowToDisk == 0) {
        throw new TestException("For the region " + aRegion.getName()
         + " no eviction happened ");
      }
      else {
        Log.getLogWriter().info(
          "For the region " + aRegion.getName()
          + " entries overflown to disk is " + numOverflowToDisk);
      }
    }
    if (totalEntriesInBuckets != (numOverflowToDisk + entriesInVm)) {
      throw new TestException(
        "Total Entries in bucket (actual value "
       + totalEntriesInBuckets
       + " ) is not the same as the sum of entries in disk and entries in vm ( "
       + numOverflowToDisk + " and " + entriesInVm + ")");
    }

    if (entriesInVm == 0) {
      throw new TestException( 
        " After eviction(overflow to disk) the entries in vm is zero but entries in disk is "
        + numOverflowToDisk);
    }
    Log.getLogWriter().info( "Total Entries in bucket (actual value "
      + totalEntriesInBuckets
      + " ) is the same as the sum of entries in disk and entries in vm ( "
      + numOverflowToDisk + " and " + entriesInVm + ")");
  }
  
  /**
   * Initializes the test region in the cache client VM and registers interest.
   */
  public static void initCacheClient() {
    synchronized (DeltaPropagation.class) {
      if (CacheHelper.getCache() == null) { // first thread
        // create the cache and region
        CacheHelper.createCache(ConfigPrms.getCacheConfig());   
        int numOfRegion = TestConfig.tab().intAt(
            delta.DeltaPropagationPrms.numberOfRegions, 1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = RegionHelper.createRegion(REGION_NAME + i, ConfigPrms
              .getRegionConfig());

          // set the loader and writer statics (same for all regions)
          mypool = ClientHelper.getPool(region);
          // to get full value request
          CacheClientUpdater.isUsedByTest = true;
          
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
   * Initializes the test region in the cache durable client VM and registers interest.
   */
  public static void initDurableCacheClient() {
    synchronized (DeltaPropagation.class) {
      if (CacheHelper.getCache() == null) { // first thread
        // create the cache and region
        Cache cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());   
        int numOfRegion = TestConfig.tab().intAt(
            delta.DeltaPropagationPrms.numberOfRegions, 1);
        for (int i = 0; i < numOfRegion; i++) {
          Region region = RegionHelper.createRegion(REGION_NAME + i, ConfigPrms
              .getRegionConfig());

          // set the loader and writer statics (same for all regions)
          mypool = ClientHelper.getPool(region);
          
          VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
              .getAnyInstance()).getConfig().getDurableClientId();
          Log.getLogWriter().info("VM Durable Client Id is " + VmDurableId);
          boolean isClientKilled = new Boolean(((String)DeltaPropagationBB
              .getBB().getSharedMap().get(DeltaPropagation.VmDurableId)));
          if (!isClientKilled) {
            // register interest in one thread to avoid possible performance hit
            try {
              region.registerInterest("ALL_KEYS", true/* durable */);
            }
            catch (CacheWriterException e) {
              throw new TestException(TestHelper.getStackTrace(e));
            }
          }else{
            DeltaDurableClientValidationListener.durableKeyMap.putAll((Map)DeltaPropagationBB
                .getBB().getSharedMap().getMap().get(DeltaPropagation.VmDurableId+"keyValueMap"));
          }
        }
        cache.readyForEvents();
      }
    }
  }
}

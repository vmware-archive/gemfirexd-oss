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
package gfxdperf.ycsb.gf;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import gfxdperf.PerfTestException;
import gfxdperf.ycsb.YCSBClient;
import gfxdperf.ycsb.core.DBException;
import gfxdperf.ycsb.core.WorkloadException;
import gfxdperf.ycsb.core.workloads.CoreWorkload;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RegionHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Client for measuring YCSB performance with GemFire.
 */
public class GFClient extends YCSBClient {

//------------------------------------------------------------------------------
// LOCATOR

  /**
   * Creates locator endpoints for use by {@link #startLocatorTask}.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Starts a locator in this JVM using the endpoints created in {@link
   * #createLocatorTask}.
   */
  public static void startLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

//------------------------------------------------------------------------------
// SERVER

  /**
   * Starts a server in this JVM.
   */
  public static void startServerTask() {
    DistributedSystemHelper.getDistributedSystem();
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
    RegionHelper.createRegion(CoreWorkloadPrms.getTableName(),
                              ConfigPrms.getRegionConfig());
  }

  /**
   * Stops a server in this JVM.
   */
  public static void stopServerTask() {
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.stopBridgeServer();
    }
  }

//------------------------------------------------------------------------------
// LOAD BALANCE

  /**
   * Creates all buckets to ensure perfect balance.
   */
  public static void createBucketsTask() throws InterruptedException {
    GFClient client = new GFClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.createBuckets();
    }
  }

  private void createBuckets() throws InterruptedException {
    Cache c = CacheHelper.getCache();
    if (c != null) {
      for (Region r : c.rootRegions()) {
        if (r.getAttributes().getDataPolicy().withPartitioning()) {
           Log.getLogWriter().info("Creating buckets for region "
                                   + r.getName());
           PartitionRegionHelper.assignBucketsToPartitions(r);
           RebalanceFactory rf = CacheHelper.getCache().getResourceManager()
                                            .createRebalanceFactory();
           RebalanceOperation ro = rf.start();
           RebalanceResults results = ro.getResults(); // blocking call
           Log.getLogWriter().info("Created buckets for region " + r.getName());
        }
      }
    }
  }

  /**
   * Checks the balance for buckets and primary buckets for each partitioned
   * region hosted by this datahost. Execute this task on every datahost. Use
   * {@link GFPrms#failOnLoadImbalance} to configure whether the test should
   * fail if data is imbalanced.
   */
  public static void checkBucketLoadBalanceTask() {
    GFClient client = new GFClient();
    client.initialize();
    if (client.jid == 0) {
      client.checkBucketLoadBalance();
    }
  }

  /**
   * Checks the balance for buckets and primary buckets for each partitioned
   * region hosted by this datahost.
   */
  protected void checkBucketLoadBalance() {
    for (PartitionedRegion pr : getPartitionedRegions()) {
      checkBucketLoadBalance(pr.getName());
    }
  }

  /**
   * Checks the bucket and primary bucket load balance for the given partitioned
   * region on this datahost.
   */
  protected void checkBucketLoadBalance(String regionName) {
    Statistics prstats = getPartitionedRegionStats(regionName);
    int buckets = 0;
    int primaries = 0;
    if (prstats != null) {
      List<String> errs = new ArrayList();
      int datahosts = getNumServersHosting(regionName);
      if (datahosts > 1) {
        Log.getLogWriter().info("Checking bucket load balance for " + regionName
                               + " with " + datahosts + " datahosts");
        primaries = prstats.getInt("primaryBucketCount");
        buckets = prstats.getInt("bucketCount");
        int copies = prstats.getInt("configuredRedundantCopies") + 1;
        int totalPrimaries = prstats.getInt("totalNumBuckets");
        int totalBuckets = totalPrimaries * copies;
        int primariesPerDatahost = totalPrimaries / datahosts;
        int bucketsPerDatahost = totalBuckets / datahosts;
        if (buckets != bucketsPerDatahost && (totalBuckets%datahosts == 0
            || buckets != bucketsPerDatahost + 1)) {
          errs.add(regionName + " has " + buckets + " buckets");
        }
        if (primaries != primariesPerDatahost && (totalPrimaries%datahosts == 0
            || primaries != primariesPerDatahost + 1)) {
          errs.add(regionName + " has " + primaries + " primary buckets");
        }
      }
      if (errs.size() > 0) {
        printBucketIdsAndSizes(regionName);
        String s = "Bucket load balance failures: " + errs;
        if (GFPrms.getFailOnLoadImbalance()) {
          Log.getLogWriter().warning(s);
        } else {
          throw new PerfTestException(s);
        }
      } else {
        Log.getLogWriter().info("Region " + regionName + " is bucket-balanced"
           + " with " + buckets + " buckets and " + primaries + " primaries");
      }
    }
  }

  /**
   * Checks the data load balance for each partitioned region hosted by this
   * datahost, in bytes. Execute this task on every datahost. Use {@link GFPrms
   * #failOnLoadImbalance} to configure whether the test should fail if data is
   * imbalanced.
   */
  public static void checkDataLoadBalanceTask() {
    GFClient client = new GFClient();
    client.initialize();
    if (client.jid == 0) {
      client.checkDataLoadBalance();
    }
  }

  /**
   * Checks the data load balance for each partitioned region hosted by this
   * datahost, in bytes.
   */
  protected void checkDataLoadBalance() {
    for (PartitionedRegion pr : getPartitionedRegions()) {
      checkDataLoadBalance(pr.getName());
    }
  }

  /**
   * Checks the data load balance for the given region. Complains if any two
   * datahosts differ by more than 10%.
   */
  protected void checkDataLoadBalance(String regionName) {
    List<String> errs = new ArrayList();
    for (PartitionRegionInfo pri : getPartitionRegionInfo()) {
      String regionPath = pri.getRegionPath();
      if (regionPath.contains(regionName)) {
        int datahosts = getNumServersHosting(regionName);
        if (datahosts > 1) {
          Log.getLogWriter().info("Checking entry load for " + regionName
                                 + " with " + datahosts + " datahosts");
          List<Long> sizes = new ArrayList();
          Set<PartitionMemberInfo> pmis = pri.getPartitionMemberInfo();
          long maxSize = 0;
          for (PartitionMemberInfo pmi : pmis) {
            long size = pmi.getSize();
            if (size < 0) {
              errs.add("Negative size for " + regionName + ": " + size);
            }
            if (size > maxSize) maxSize = size;
            sizes.add(size);
          }
          if (maxSize > 0) {
            for (Long size : sizes) {
              double ratio = size < maxSize ? (double)size/(double)maxSize
                                            : (double)maxSize/(double)size;
              if (ratio < 0.90) {
                errs.add(regionName + " is imbalanced: " + sizes);
                break;
              }
            }
          }
          Log.getLogWriter().info("Checked entry load for " + regionName
             + ", found entry sizes " + sizes + " bytes");
        }
      }
    }
    if (errs.size() > 0) {
      printBucketIdsAndSizes(regionName);
      String s = "Data load balance failures: " + errs;
      if (GFPrms.getFailOnLoadImbalance()) {
        Log.getLogWriter().warning(s);
      } else {
        throw new PerfTestException(s);
      }
    } else {
      Log.getLogWriter().info("Region " + regionName + " is load-balanced");
    }
  }

  /**
   * Prints the bucket ids and sizes for buckets and primary buckets for each
   * partitioned region hosted on this datahost. Execute this task on every
   * datahost.
   */
  public static void printBucketIdsAndSizesTask() {
    GFClient client = new GFClient();
    client.initialize();
    if (client.jid == 0) {
      client.printBucketIdsAndSizes();
    }
  }

  protected void printBucketIdsAndSizes() {
    for (PartitionedRegion pr : getPartitionedRegions()) {
      printBucketIdsAndSizes(pr.getName());
    }
  }

  /**
   * Prints a list of bucket and primary bucket ids for the given partitioned
   * region on this datahost, and the size of each bucket.
   */
  protected void printBucketIdsAndSizes(String regionName) {
    for (PartitionRegionInfo pri : getPartitionRegionInfo()) {
      String regionPath = pri.getRegionPath();
      if (regionPath.contains(regionName)) {
        StringBuilder sb = new StringBuilder();
        PartitionedRegion pr =
          (PartitionedRegion)CacheHelper.getCache().getRegion(regionPath);
        // buckets
        List<Integer> bids = (List<Integer>)pr.getLocalBucketsListTestOnly();
        sb.append("Buckets for " + regionName
          + " at " + regionPath + "/" + pr.getName() + ": " + bids + "\n");
        for (Integer bid : bids) {
          sb.append("size of bucket[" + bid + "]=")
            .append(pr.getDataStore().getBucketSize(bid) + "\n");
        }
        // primary buckets
        List<Integer> pbids =
                (List<Integer>)pr.getLocalPrimaryBucketsListTestOnly();
        sb.append("Primary buckets for " + regionName
          + " at " + regionPath + "/" + pr.getName() + ": " + pbids + "\n");
        for (Integer pbid : pbids) {
          sb.append("size of primary bucket[" + pbid + "]=")
            .append(pr.getDataStore().getBucketSize(pbid) + "\n");
        }
        Log.getLogWriter().info(sb.toString());
        return;
      }
    }
  }

  /**
   * Returns a list of all partitioned regions.
   */
  protected Set<PartitionedRegion> getPartitionedRegions() {
    GemFireCacheImpl c = (GemFireCacheImpl)CacheHelper.getCache();
    return c.getPartitionedRegions() ;
  }

  /**
   * Returns the PartitionedRegionStats for the given region.
   */
  protected Statistics getPartitionedRegionStats(String regionName) {
    Statistics[] stats = DistributedSystemHelper.getDistributedSystem()
      .findStatisticsByTextId(regionName);
    Statistics prStats = null;
    for (int i = 0; i < stats.length; i++) {
      if (stats[i].getType().getName().equals("PartitionedRegionStats")) {
        prStats = stats[i];
      }
    }
    return prStats;
  }

  /**
   * Returns info for each partitioned region.
   */
  protected Set<PartitionRegionInfo> getPartitionRegionInfo() {
    return PartitionRegionHelper.getPartitionRegionInfo(CacheHelper.getCache());
  }

  /**
   * Returns the number of datahosts hosting the given partitioned region.
   */
  protected int getNumServersHosting(String regionName) {
    for (PartitionRegionInfo pri : getPartitionRegionInfo()) {
      if (pri.getRegionPath().contains(regionName)) {
        Set<PartitionMemberInfo> pmis = pri.getPartitionMemberInfo();
        return pmis.size();
      }
    }
    return 0;
  }

//------------------------------------------------------------------------------
// DB initialization and cleanup

  public static void initDBTask() throws DBException, WorkloadException {
    GFClient client = new GFClient();
    client.initialize();
    client.initDB();
    client.updateHydraThreadLocals();
  }

  protected void initDB() throws DBException {
    this.db = new GFDB();
    this.db.init();
  }

  public static void cleanupDBTask() throws DBException, InterruptedException {
    GFClient client = new GFClient();
    client.initialize();
    client.cleanupDB();
    client.updateHydraThreadLocals();
  }

  protected void cleanupDB() throws DBException, InterruptedException {
    this.db.cleanup();
  }

//------------------------------------------------------------------------------
// Workload initialization

  public static void initWorkloadTask() throws WorkloadException {
    GFClient client = new GFClient();
    client.initialize();
    client.initWorkload();
    client.updateHydraThreadLocals();
  }

  protected void initWorkload() throws WorkloadException {
    this.workload = new CoreWorkload();
    this.workload.init(null, this.ttgid, this.numThreads);
    this.workloadstate = this.workload.initThread(null);
  }

//------------------------------------------------------------------------------
// DATA LOADING

  public static void loadDataTask() {
    GFClient client = new GFClient();
    client.initialize();
    client.loadData();
  }

//------------------------------------------------------------------------------
// WORKLOAD

  public static void doWorkloadTask() throws InterruptedException {
    GFClient client = new GFClient();
    client.initialize();
    client.doWorkload();
  }

//------------------------------------------------------------------------------
// MAJOR COMPACTION

  /**
   * {@inheritDoc}
   */
  protected long completeTask(String trimIntervalName) {
    return System.currentTimeMillis();
  }
}

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

package cacheperf.poc.useCase14;

import cacheperf.*;
import com.gemstone.gemfire.cache.*;
import distcache.gemfire.*;
import hydra.*;

import java.util.*;
import objects.*;
import perffmwk.*;

/**
 * Specially tuned client for data feed testing.
 */

public class UseCase14Client extends CachePerfClient {

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * TASK to add a region named {@link UseCase14Prms#addedRegionName}.
   */
  public static synchronized void addRegionTask() {
    UseCase14Client c = new UseCase14Client();
    c.initHydraThreadLocals();
    c.addRegion();
    c.updateHydraThreadLocals();
  }
  private void addRegion() {
    String regionName = GemFireCachePrms.getRegionName();
    String regionConfig = ConfigPrms.getRegionConfig();
    if (RegionHelper.getRegion(regionName) == null) {
      RegionHelper.createRegion(regionName, regionConfig);
    }
    Region region = RegionHelper.getRegion(regionName);
    EdgeHelper.addThreadLocalConnection(region);
  }

  /**
   * TASK to feed objects into the push and pull regions.
   */
  public static void feedDataTask() {
    UseCase14Client c = new UseCase14Client();
    c.initialize(CREATES);
    c.feedData();
  }
  private void feedData() {
    if (this.maxKeys % 100 != 0) {
      String s = "Need " + BasePrms.nameForKey(CachePerfPrms.maxKeys)
               + " to be a multiple of 100 for"
               + " cacheperf.poc.useCase14.UseCase14Prms.feedDataTask";
      throw new HydraConfigException(s);
    }
    Region pushRegion = RegionHelper.getRegion("PushRegion");
    Region pullRegion = RegionHelper.getRegion("PullRegion");
    int pushChunkSize = UseCase14Prms.getPushPercentage();
    int pullChunkSize = 100 - pushChunkSize;
    boolean batchDone = false;
    executeTaskTerminator();
    executeWarmupTerminator();
    do {
      for (int i = 0; i < pushChunkSize; i++) {
        feed(pushRegion, this.currentPushKey);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        this.currentPushKey += this.numThreads;
        this.currentKey = this.currentPushKey;
      }
      for (int i = 0; i < pullChunkSize; i++) {
        feed(pullRegion, this.currentPullKey);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        this.currentPullKey += this.numThreads;
        this.currentKey = this.currentPullKey;
      }
      batchDone = executeBatchTerminator();
    } while (!batchDone);
  }
  private void feed(Region region, int i) {
    long start = this.statistics.startCreate();
    Object key = ObjectHelper.createName(this.keyType, i);
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject(objectType, i);
    try {
      region.create(key, val);
    } catch (EntryExistsException e) {
      throw new HydraRuntimeException("Key already exists: " + key);
    }
    this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
  }

  /**
   *  TASK to create objects of type
   *  {@link cacheperf.CachePerfPrms#objectType}.
   *  Each client puts a new object at a new key.
   */
  public static void createTxDataTask() {
    UseCase14Client c = new UseCase14Client();
    c.initialize();
    c.createTxData();
  }
  private void createTxData() {
    Region region = RegionHelper.getRegion("TxRegion");
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      createTx(region, key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
    } while (!executeBatchTerminator());
  }
  private void createTx(Region region, int i) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );
    long start = this.statistics.startCreate();
    try {
      region.create(key, val);
    } catch (EntryExistsException e) {
      throw new HydraRuntimeException("Key already exists: " + key);
    }
    this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
  }

  /**
   *  TASK to put objects transactionally.
   */
  public static void putTxDataTask() {
    UseCase14Client c = new UseCase14Client();
    c.initialize(PUTS);
    c.putTxData();
  }
  private void putTxData() {
    Region region = RegionHelper.getRegion("TxRegion");
    boolean batchDone = false;
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      putTx(region, key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      batchDone = executeBatchTerminator();
    } while (!batchDone);
  }
  private void putTx(Region region, int i) {
    Object key = ObjectHelper.createName(this.keyType, i);
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject(objectType, i);
    long start = this.statistics.startPut();
    boolean committed = false;
    while (!committed) {
      this.tm.begin();
      region.put(key, val);
      try {
        this.tm.commit();
        committed = true;
      } catch (ConflictException e) {
        log().info("CONFLICT");
      }
    }
    this.statistics.endPut(start, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK to sleep {@link cacheperf.CachePerfPrms#sleepMs}.
   */
  public static void sleepTask() {
    int ms = CachePerfPrms.getSleepMs();
    log().info("Sleeping " + ms + " ms");
    MasterController.sleepForMs(ms);
  }

  /**
   * TASK to validate that no messages were dropped by bridge servers.
   */
  public static void validateMessagesFailedQueuedTask() {
    UseCase14Client c = new UseCase14Client();
    c.validateMessagesFailedQueued();
  }
  private void validateMessagesFailedQueued() {
    String type = "CacheClientProxyStatistics";
    String stat = "messagesFailedQueued";
    long failures = getStatTotalCount(type, stat);
    String s = type + "." + stat + " = " + failures;
    if (failures == 0) {
      log().info(s);
    } else {
      throw new HydraRuntimeException(s);
    }
  }

  /**
   * Returns the total value of the stat, which it gets by combining the
   * unfiltered max value of the stat across all archives.
   */
  private long getStatTotalCount(String type, String stat) {
    String spec = "* " // search all archives
                + type + " "
                + "* " // match all instances
                + stat + " "
                + StatSpecTokens.FILTER_TYPE + "="
                        + StatSpecTokens.FILTER_NONE + " "
                + StatSpecTokens.COMBINE_TYPE + "="
                        + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List psvs = PerfStatMgr.getInstance().readStatistics(spec);
    if (psvs == null) {
      Log.getLogWriter().warning("No stats found for " + spec);
      return 0;
    } else {
      double failures = 0;
      for (int i = 0; i < psvs.size(); i++) {
        PerfStatValue psv = (PerfStatValue)psvs.get(i);
        failures += psv.getMax();
      }
      return (long)failures;
    }
  }

  /**
   *  Gets the current push key for a thread's workload.
   */
  protected int getCurrentPushKey() {
    Integer n = (Integer) localcurrentpushkey.get();
    if ( n == null ) {
      n = new Integer(ttgid());
      localcurrentpushkey.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the current push key for a thread's workload.
   */
  protected void setCurrentPushKey( int n ) {
    localcurrentpushkey.set( new Integer( n ) );
  }
  /**
   *  Gets the current pull key for a thread's workload.
   */
  protected int getCurrentPullKey() {
    Integer n = (Integer) localcurrentpullkey.get();
    if ( n == null ) {
      n = new Integer(ttgid());
      localcurrentpullkey.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the current pull key for a thread's workload.
   */
  protected void setCurrentPullKey( int n ) {
    localcurrentpullkey.set( new Integer( n ) );
  }

  @Override
  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.currentPushKey = getCurrentPushKey();
    this.currentPullKey = getCurrentPullKey();
  }
  @Override
  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    setCurrentPushKey(this.currentPushKey);
    setCurrentPullKey(this.currentPullKey);
  }

  private static HydraThreadLocal localcurrentpushkey = new HydraThreadLocal();
  private static HydraThreadLocal localcurrentpullkey = new HydraThreadLocal();

  public int currentPushKey;
  public int currentPullKey;
}

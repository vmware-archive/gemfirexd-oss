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

package cacheperf;

import java.rmi.RemoteException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import cacheperf.gemfire.query.QueryPerfPrms;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.control.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;

import distcache.*;
import distcache.gemfire.GemFireCacheTestImpl;
import distcache.gemfire.GemFireCachePrms;
import distcache.hashmap.HashMapCacheImpl;
import hydra.*;
import hydra.blackboard.*;
import java.io.IOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import newWan.WANTestPrms;
import objects.*;
import perffmwk.*;
import query.QueryPrms;
import util.*;

/**
 *
 *  Client used to measure cache performance.
 *
 */
public class CachePerfClient {

  private static final Object closeCacheLock = new Object();

  //----------------------------------------------------------------------------
  //  Trim interval names
  //----------------------------------------------------------------------------

  protected static final int DATASTORECACHEOPENS = 100;
  protected static final int ACCESSORCACHEOPENS  = 101;
  protected static final int CACHEOPENS     = 0;
  protected static final int CREATES        = 1;
  protected static final int CREATEKEYS     = 2;
  protected static final int PUTS           = 3;
  protected static final int GETS           = 4;
  protected static final int PUTSCOMPARISON = 5;
  protected static final int GETSCOMPARISON = 6;
  protected static final int DESTROYS       = 7;
  protected static final int CREATEGETS     = 8;
  protected static final int PUTGETS        = 9;
  protected static final int USECASE12_UPDATES = 10;
  protected static final int COMBINEDPUTGETS = 11;
  protected static final int REGISTERINTERESTS = 12;
  protected static final int OPS            = 13; // used if isMainWorkload
  protected static final int CONNECTS       = 14;
  protected static final int QUERIES        = 15;
  protected static final int LOCKS          = 16;
  protected static final int UPDATES        = 17;
  protected static final int EXTRA_PUTS     = 18;
  protected static final int EXTRA_GETS     = 19;
  protected static final int SLEEP          = 20;

  protected static final String OPS_NAME = CachePerfStats.OPS;
  protected static final String DATASTORECACHEOPENS_NAME = "dataStoreCacheOpens";
  protected static final String ACCESSORCACHEOPENS_NAME = "accessorCacheOpens";
  protected static final String CACHEOPENS_NAME = "cacheOpens";
  protected static final String SLEEP_NAME = "sleep";
  protected static final String CREATES_NAME    = "creates";
  protected static final String CREATEKEYS_NAME = "createKeys";
  protected static final String PUTS_NAME       = "puts";
  protected static final String GETS_NAME       = "gets";
  protected static final String PUTSCOMPARISON_NAME = "putsComparison";
  protected static final String GETSCOMPARISON_NAME = "getsComparison";
  protected static final String DESTROYS_NAME   = "destroys";
  protected static final String CREATEGETS_NAME = "creategets";
  protected static final String PUTGETS_NAME    = "putgets";
  protected static final String USECASE12_UPDATES_NAME = "useCase12Updates";
  protected static final String COMBINEDPUTGETS_NAME = "combinedputgets";
  protected static final String REGISTERINTERESTS_NAME = "registerInterests";
  protected static final String CONNECTS_NAME   = "connects";
  protected static final String QUERIES_NAME    = "queries";
  protected static final String LOCKS_NAME      = "locks";
  protected static final String UPDATES_NAME    = "updates";
  protected static final String EXTRA_PUTS_NAME = "extraPuts";
  protected static final String EXTRA_GETS_NAME = "extraGets";

  protected String nameFor( int name ) {
    switch( name ) {
      case OPS:        return OPS_NAME;
      case DATASTORECACHEOPENS: return DATASTORECACHEOPENS_NAME;
      case SLEEP: return SLEEP_NAME;
      case ACCESSORCACHEOPENS: return ACCESSORCACHEOPENS_NAME;
      case CACHEOPENS: return CACHEOPENS_NAME;
      case CREATES:    return CREATES_NAME;
      case CREATEKEYS: return CREATEKEYS_NAME;
      case PUTS:       return PUTS_NAME;
      case GETS:       return GETS_NAME;
      case CREATEGETS: return CREATEGETS_NAME;
      case PUTGETS:    return PUTGETS_NAME;
      case COMBINEDPUTGETS: return COMBINEDPUTGETS_NAME;
      case PUTSCOMPARISON: return PUTSCOMPARISON_NAME;
      case GETSCOMPARISON: return GETSCOMPARISON_NAME;
      case DESTROYS:   return DESTROYS_NAME;
      case USECASE12_UPDATES: return USECASE12_UPDATES_NAME;
      case REGISTERINTERESTS: return REGISTERINTERESTS_NAME;
      case CONNECTS:   return CONNECTS_NAME;
      case QUERIES:    return QUERIES_NAME;
      case LOCKS:      return LOCKS_NAME;
      case UPDATES:    return UPDATES_NAME;
      case EXTRA_PUTS: return EXTRA_PUTS_NAME;
      case EXTRA_GETS: return EXTRA_GETS_NAME;
      default: throw new CachePerfException( "Unsupported trim interval: " + name );
    }
  }

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * TASK to create and start a locator and connect it to an admin distributed
   * system.
   */
  public static void createAndStartLocatorTask() {
    DistributedSystemHelper.createLocator();
    int parties = numThreads();
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(parties, "locator");
    log().info("Waiting for " + parties + " to meet at locator barrier");
    barrier.await();
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   * TASK to restart a locator and connect it to an admin distributed system.
   */
  public static void restartLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   * TASK to hook a vm to the cache without creating regions.
   */
   public static void createCacheTask() {
     CachePerfClient c = new CachePerfClient();
     c.initHydraThreadLocals();
     c.createCache();
     c.updateHydraThreadLocals();
   }
   protected void createCache() {
     if (this.cache == null) {
       log().info("Creating the cache");
       this.cache = DistCacheFactory.createInstance();
       this.cache.createCache();
       // currently only supported by distcache.gemfire
       // all other distcache impls will return null
       this.tm = (CacheTransactionManager)this.cache.getCacheTransactionManager();
       log().info("Created the cache");
     }
   }
 
   /**

   * TASK to create a pool.
   */
  public static void createPoolTask() {
    String poolConfig = ConfigPrms.getPoolConfig();
    PoolHelper.createPool(poolConfig);
  }

  /**
   *  TASK to hook a vm to the cache.
   */
  public static void openCacheTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.openCache();
    c.updateHydraThreadLocals();
  }
  private void openCache() {
    if ( this.cache == null ) {
      log().info( "Opening the cache" );
      this.cache = DistCacheFactory.createInstance();
      this.cache.open();
      // currently only supported by distcache.gemfire
      // all other distcache impls will return null
      this.tm = (CacheTransactionManager)this.cache.getCacheTransactionManager();
      if (DistCachePrms.getCacheVendor() == DistCachePrms.GEMFIRE) {
        GemFireCacheTestImpl impl = (GemFireCacheTestImpl)this.cache;
        EdgeHelper.addThreadLocalConnection(impl.getRegion());
      }
      log().info( "Opened the cache" );
    }
  }

  /**
   * INITTASK to assign buckets for a partitioned region.  This must
   * run only after all caches are open, and before creating data.
   */
  public static void assignBucketsTask() throws InterruptedException {
    CachePerfClient c = new CachePerfClient();
    c.initialize();
    // assign to only one thread just to save time
    if (c.ttgid == 0) {
      c.assignBuckets();
    }
  }
  protected void assignBuckets() throws InterruptedException {
    Cache c = CacheHelper.getCache();
    if (c != null) {
      for (Region r : c.rootRegions()) {
        Log.getLogWriter().info("Creating buckets for region " + r.getName());
        CachePerfClientVersion.assignBucketsToPartitions(r);
        Log.getLogWriter().info("Created buckets for region " + r.getName());
      }
    }
  }

  /**
   * TASK to run disk compaction.
   */
  public static void compactionTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.createCache();
    c.updateHydraThreadLocals();
    if (c.cache instanceof GemFireCacheTestImpl) {
      Region r = ((GemFireCacheTestImpl)c.cache).getRegion();
      DiskStore dStore = CacheHelper.getCache().findDiskStore(r.getAttributes().getDiskStoreName());
      boolean result = dStore.forceCompaction();
      if (!result) {
        throw new TestException("compaction returned " + result);
      }
    } else {
      String s = "Unknown cache for disk compaction";
      throw new HydraConfigException(s);
    }
  }

  /**
   * TASK to connect to the distributed system.
   */
  public static void connectTask() {
    DistributedSystemHelper.connect();
  }

  /**
   * TASK to disconnect from the distributed system.
   */
  public static void disconnectTask() {
    DistributedSystemHelper.disconnect();
  }

  /**
   * TASK to bounce a VM.  Selects a VM other than this one of type
   * {@link CachePerfPrms#clientNameToBounce}, sleeps {@link
   * CachePerfPrms#sleepMs}, then synchronously stops the VM using a nice exit,
   * or a mean kill if {@link CachePerfPrms#useMeanKill} is true.
   * The VM is restarted after {@link CachePerfPrms#restartWaitSec} seconds.
   * Set this to -1 to skip the restart altogether.
   * The task does not return until the restart is complete.  It also waits
   * for partitioned region redundancy recovery if {@link CachePerfPrms
   * #waitForRecovery} is true.  In that case, the same VM must not be
   * bounced until it has signalled recovery complete.
   * <p>
   * This task can be optionally terminated after a fixed number of executions
   * by setting {@link CachePerfPrms#maxExecutions} to a positive number.  When
   * this is reached, it sends a signal that other tasks can optionally detect
   * and use to terminate.
   */
  public static void bounceTask() throws ClientVmNotFoundException {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.bounce();
  }
  private void bounce() throws ClientVmNotFoundException {
    if (CachePerfPrms.waitForTrimSignal()) {
      // wait for trim signal from threadgroup doing cache ops
      SharedCounters sc = TaskSyncBlackboard.getInstance().getSharedCounters();
      while (sc.read(TaskSyncBlackboard.trimSignal) == 0) {
        MasterController.sleepForMs(250);
      }
    }

    int sleepMs = CachePerfPrms.getSleepMs();
    int killType = CachePerfPrms.useMeanKill() ? ClientVmMgr.MEAN_KILL
                                               : ClientVmMgr.NICE_EXIT;
    int restartWaitSec = CachePerfPrms.getRestartWaitSec();

    // choose a vm to bounce
    ClientVmInfo info = chooseVM();

    // sleep beforehand
    log().info("Sleeping " + sleepMs + " ms before bouncing " + info);
    MasterController.sleepForMs(sleepMs);

    // clear the recovery flag in case of any startup recoveries
    if (CachePerfPrms.waitForRecovery()) {
      Object tmp = TaskSyncBlackboard.getInstance().getSharedMap()
                                     .remove(TaskSyncBlackboard.RECOVERY_KEY);
      if (tmp != null) {
        Log.getLogWriter().info("Cleared recovery key from map"); 
      }
    }

    // stop the vm
    info = ClientVmMgr.stop(
        "Stopping " + info, killType, ClientVmMgr.ON_DEMAND, info);

    // optionally restart it
    if (restartWaitSec == -1) {
      log().info("Skipping restart of " + info);
    } else {
      restart(info, restartWaitSec);
    }

    // optionally wait for recovery
    if (CachePerfPrms.waitForRecovery()) {
      waitForRecovery();
    }

    int maxExecutions = CachePerfPrms.getMaxExecutions();
    if (maxExecutions > 0) {
      SharedCounters sc = TaskSyncBlackboard.getInstance().getSharedCounters();
      long executions = sc.incrementAndRead(TaskSyncBlackboard.executions);
      if (executions >= maxExecutions) {
        sc.increment(TaskSyncBlackboard.signal);
        String s = "Completed " + executions + " bounces";
        throw new StopSchedulingTaskOnClientOrder(s);
      }
    }
  }

  public static void restartTask() throws ClientVmNotFoundException {
    CachePerfClient c = new CachePerfClient();
    c.initialize(-1);
    c.initHydraThreadLocals();
    c.restart();
  }
  private void restart() throws ClientVmNotFoundException {
    // choose a vm to restart
    ClientVmInfo info = chooseVM();

    // restart the vm
    restart(info, CachePerfPrms.getRestartWaitSec());

    // optionally wait for recovery
    if (CachePerfPrms.waitForRecovery()) {
      waitForRecovery();
    }
  }
  private void restart(ClientVmInfo info, int restartWaitSec)
  throws ClientVmNotFoundException {
    // sleep before restart
    int restartWaitMs = restartWaitSec * 1000;
    log().info("Sleeping " + restartWaitMs + " ms before restarting " + info);
    MasterController.sleepForMs(restartWaitMs);

    // restart the vm
    info = ClientVmMgr.start("Restarting " + info, info);
  }

  private void waitForRecovery() {
    SharedMap map = TaskSyncBlackboard.getInstance().getSharedMap();
    log().info("Waiting for recovery");
    Long elapsed = null;
    while (true) {
      elapsed = (Long)map.get(TaskSyncBlackboard.RECOVERY_KEY);
      if (elapsed != null) {
        break;
      }
      MasterController.sleepForMs(250);
    }
    this.statistics.endRecovery(elapsed.longValue(),
                                this.isMainWorkload,
                                this.histogram);
    map.remove(TaskSyncBlackboard.RECOVERY_KEY);
    log().info("Recovery complete");
  }

  /**
   * Selects a VM of type {@link CachePerfPrms#clientNameToBounce}, if
   * specified, or else a random VM other than this one.
   */
  private static ClientVmInfo chooseVM() {
    ClientVmInfo info = null;
    String clientName = CachePerfPrms.getClientNameToBounce();
    if (clientName == null) {
      Vector others = ClientVmMgr.getOtherClientVmids();
      GsRandom rng = TestConfig.tab().getRandGen();
      Integer vmid = (Integer)others.get(rng.nextInt(0, others.size() - 1));
      info = new ClientVmInfo(vmid.intValue());
    } else {
      info = new ClientVmInfo(null, clientName, null);
    }
    return info;
  }

  /**
   * TASK to shut down a datahost.
   */
  public static void shutDownDataHostTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.shutDownDataHost();
    c.updateHydraThreadLocals();
  }
  private void shutDownDataHost() {
    Log.getLogWriter().info("Shutting down datahost...");

    long start = this.statistics.startCacheClose();
    closeCache(); // close cache and region and stop bridge server if any
    this.statistics.endCacheClose(start, this.isMainWorkload, this.histogram);

    start = this.statistics.startDisconnect();
    closeStatistics(); // disconnect
    this.statistics.endDisconnect(start, this.isMainWorkload, this.histogram);

    Log.getLogWriter().info("Done shutting down datahost");
  }

  /**
   * TASK to restart a datahost.
   */
  public static void restartDataHostTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.restartDataHost();
    c.updateHydraThreadLocals();
  }
  private void restartDataHost() {
    Log.getLogWriter().info("Restarting datahost...");

    long start = NanoTimer.getTime();
    openStatistics(); // connect
    this.statistics.endConnect(start, this.isMainWorkload, this.histogram);

    start = this.statistics.startCacheOpen();
    openCache(); // create cache and region and start bridge server if any
    this.statistics.endCacheOpen(start, this.isMainWorkload, this.cache, this.histogram);

    Log.getLogWriter().info("Done restarting datahost");
  }

  /**
   * TASK to add a new datahost and rebalance.  Runs once per VM.
   */
  public static void addDataHostTask() throws InterruptedException {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.addDataHost();
    c.updateHydraThreadLocals();
    
    throw new StopSchedulingTaskOnClientOrder("Done adding datahost");
  }
  private void addDataHost() throws InterruptedException {
    SharedCounters sc = TaskSyncBlackboard.getInstance().getSharedCounters();

    // wait for trim signal from threadgroup doing cache ops
    if (CachePerfPrms.waitForTrimSignal()) {
      while (sc.read(TaskSyncBlackboard.trimSignal) == 0) {
        MasterController.sleepForMs(500);
      }
    }

    // wait for my turn to start up
    this.ttgid = ttgid();
    while (sc.read(TaskSyncBlackboard.addDataHostSignal) != this.ttgid) {
      MasterController.sleepForMs(500);
    }

    // start up
    Log.getLogWriter().info("Starting datahost and rebalancing...");

    long start = NanoTimer.getTime();
    openStatistics(); // connect
    this.statistics.endConnect(start, this.isMainWorkload, this.histogram);

    start = this.statistics.startCacheOpen();
    openCache(); // create cache and region and start bridge server if any
    this.statistics.endCacheOpen(start, this.isMainWorkload, this.cache, this.histogram);

    // rebalance
    start = this.statistics.startRebalance();
    rebalance(); // rebalance buckets
    this.statistics.endRebalance(start, this.isMainWorkload, this.histogram);

    Log.getLogWriter().info("Done starting datahost and rebalancing");

    // let the next datahost proceed
    sc.increment(TaskSyncBlackboard.addDataHostSignal);

    // if last one, let threadgroup doing cache ops terminate
    if (this.ttgid == numThreads() - 1) {
      sc.increment(TaskSyncBlackboard.signal);
    }
  }

  public static void rebalanceTask() throws InterruptedException {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.doRebalance();
    c.updateHydraThreadLocals();
  }
  private void doRebalance() throws InterruptedException {
    Log.getLogWriter().info("Rebalancing...");
    long start = this.statistics.startRebalance();
    rebalance(); // rebalance buckets
    this.statistics.endRebalance(start, this.isMainWorkload, this.histogram);

    Log.getLogWriter().info("Done rebalancing");
  }
  protected void rebalance() throws InterruptedException {
    RebalanceFactory rf = CacheHelper.getCache().getResourceManager()
                                     .createRebalanceFactory();
    RebalanceOperation ro = rf.start();
    RebalanceResults results = ro.getResults(); // blocking call
    if (log().fineEnabled()) {
      log().fine("Rebalance results: " + results);
    }
  }

  /**
   * TASK to validate that the expected number of members has joined the
   * distributed system.  It can only be used when the members are actually
   * connected to their distributed systems.  It also assumes that all clients
   * that are configured to use a given distributed system do in fact use it.
   * @author lises
   * @since 5.0
   */
  public static void validateExpectedMembersTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.validateExpectedMembers();
    c.updateHydraThreadLocals();
  }
  private void validateExpectedMembers() {
    DistributedSystemHelper.connect();
    int expected = getExpectedMemberCount();
    int actual = 0;
    // looping to wait did not help when a locator was being used
    //for (int i=0; i<100; i++) {
      actual = getMemberCount();
      if (actual == expected) {
        log().info("Found expected number of members: " + expected);
        return;
      }
    //  try {
    //    Thread.currentThread().sleep(200);
    //  } catch (InterruptedException ie) {
    //    Thread.currentThread().interrupt();
    //    break;
    //  }
    //}
    String s = "Expected " + expected + " members, found " + actual;
    DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
    if (ds != null) {
      Set mbrs = DistributedSystemHelper.getMembers();
      StringBuffer b = new StringBuffer(1000);
      b.append(s);
      b.append(" [");
      for (Iterator it=mbrs.iterator(); it.hasNext(); ) {
        b.append(String.valueOf(it.next()));
        if (it.hasNext())
          b.append(", ");
      }
      b.append("]");
      s = b.toString();
    }
    throw new HydraRuntimeException(s);
  }
  private int getExpectedMemberCount() {
    String gemfireName = System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
    String dsName = TestConfig.getInstance().getGemFireDescription(gemfireName)
                              .getDistributedSystem();
    // count all client VMs configured to use this distributed system
    if (dsName.equals(GemFirePrms.LONER)) {
      return 1; // this VM is the one and only member
    }
    int count = 0;
    Map cds = TestConfig.getInstance().getClientDescriptions();
    for (Iterator i = cds.values().iterator(); i.hasNext();) {
      ClientDescription cd = (ClientDescription)i.next();
      if (cd.getGemFireDescription().getDistributedSystem().equals(dsName)) {
        count += cd.getVmQuantity();
      }
    }
    return count;
  }
  private int getMemberCount() {
    return DistributedSystemHelper.getMembers().size();
  }

  /**
   *  TASK to hook a vm to the cache and measure the time.
   */
  public static void timedOpenCacheTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( CACHEOPENS );
    c.timedOpenCache();
    c.updateHydraThreadLocals();
  }
  private void timedOpenCache() {
    if ( this.cache == null ) {
      log().info( "Opening the cache" );
      this.cache = DistCacheFactory.createInstance();
      cacheStartTrim( this.trimIntervals, this.trimInterval );
      long start = this.statistics.startCacheOpen();
      this.cache.open();
      this.statistics.endCacheOpen(start, this.isMainWorkload, this.cache, this.histogram);
      MasterController.sleepForMs( 2000 );
      cacheEndTrim( this.trimIntervals, this.trimInterval );
      if (DistCachePrms.getCacheVendor() == DistCachePrms.GEMFIRE) {
        GemFireCacheTestImpl impl = (GemFireCacheTestImpl)this.cache;
        EdgeHelper.addThreadLocalConnection(impl.getRegion());
      }
      log().info( "Opened the cache" );
    }
  }

  /**
   *  TASK to hook a datastore vm to the cache and measure the time.
   */
  public static void timedOpenDataStoreCacheTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( DATASTORECACHEOPENS );
    c.timedOpenDataStoreCache();
    c.updateHydraThreadLocals();
  }
  private void timedOpenDataStoreCache() {
    if ( this.cache == null ) {
      log().info( "Opening the cache" );
      cacheStartTrim( this.trimIntervals, this.trimInterval );

      String cacheConfig = ConfigPrms.getCacheConfig();
      CacheDescription cd = CacheHelper.getCacheDescription(cacheConfig);

      String regionConfig = ConfigPrms.getRegionConfig();
      RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
      String regionName = GemFireCachePrms.getRegionName();
      if (regionName == null) { // use name from region configuration
        regionName = RegionHelper.getRegionDescription(regionConfig)
                                 .getRegionName();
      }

      DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
      if (ds == null) {
         ds = DistributedSystemHelper.connect();
      }

      //------ START TIMER ------//
      log().info("Starting timer");
      long start = this.statistics.startCacheOpen();

      // create the cache
      Cache cache = null;
      try {
        cache = CacheFactory.create(ds);
      } catch (CacheException e) {
        String s = "Unable to create cache using: " + ds;
        throw new HydraRuntimeException(s, e);
      }
      cd.configure(cache);

      // create the region
      AttributesFactory factory = new AttributesFactory();
      rd.configure(factory, true); // connect
      RegionAttributes attributes = factory.create();
      Region region = cache.createRegion(regionName, attributes);

      this.statistics.endCacheOpen(start, this.isMainWorkload, this.cache, this.histogram);
      log().info("Stopped timer");
      //------  END TIMER  ------//

      MasterController.sleepForMs( 5000 );
      cacheEndTrim( this.trimIntervals, this.trimInterval );
      /*
      if (DistCachePrms.getCacheVendor() == DistCachePrms.GEMFIRE) {
        GemFireCacheImpl impl = (GemFireCacheImpl)this.cache;
        EdgeHelper.addThreadLocalConnection(impl.getRegion());
      }
      */
      log().info( "Opened the cache" );
    }
  }

  /**
   *  TASK to hook an accessor vm to the cache and measure the time.
   */
  public static void timedOpenAccessorCacheTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( ACCESSORCACHEOPENS );
    c.timedOpenAccessorCache();
    c.updateHydraThreadLocals();
  }
  private void timedOpenAccessorCache() {
    if ( this.cache == null ) {
      log().info( "Opening the cache" );
      cacheStartTrim( this.trimIntervals, this.trimInterval );

      String cacheConfig = ConfigPrms.getCacheConfig();
      CacheDescription cd = CacheHelper.getCacheDescription(cacheConfig);

      String regionConfig = ConfigPrms.getRegionConfig();
      RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
      String regionName = GemFireCachePrms.getRegionName();
      if (regionName == null) { // use name from region configuration
        regionName = RegionHelper.getRegionDescription(regionConfig)
                                 .getRegionName();
      }

      DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
      if (ds == null) {
         ds = DistributedSystemHelper.connect();
      }

      //------ START TIMER ------//
      log().info("Starting timer");
      long start = this.statistics.startCacheOpen();

      // create the cache
      Cache cache = null;
      try {
        cache = CacheFactory.create(ds);
      } catch (CacheException e) {
        String s = "Unable to create cache using: " + ds;
        throw new HydraRuntimeException(s, e);
      }
      cd.configure(cache);

      // create the region
      AttributesFactory factory = new AttributesFactory();
      rd.configure(factory, true); // connect
      RegionAttributes attributes = factory.create();
      Region region = cache.createRegion(regionName, attributes);

      this.statistics.endCacheOpen(start, this.isMainWorkload, this.cache, this.histogram);
      log().info("Stopped timer");
      //------  END TIMER  ------//

      MasterController.sleepForMs( 5000 );
      cacheEndTrim( this.trimIntervals, this.trimInterval );
      /*
      if (DistCachePrms.getCacheVendor() == DistCachePrms.GEMFIRE) {
        GemFireCacheImpl impl = (GemFireCacheImpl)this.cache;
        EdgeHelper.addThreadLocalConnection(impl.getRegion());
      }
      */
      log().info( "Opened the cache" );
    }
  }

  /**
   * TASK to create a gateway hub.
   */
  public static void createGatewayHubTask() {
    String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
    GatewayHubHelper.createGatewayHub(gatewayHubConfig);
  }

  /**
   * TASK to add gateways.
   */
  public static void addGatewaysTask() {
    String gatewayConfig = ConfigPrms.getGatewayConfig();
    GatewayHubHelper.addGateways(gatewayConfig);
  }

  /**
   * TASK to add a WBCL gateway.
   */
  public static void addWBCLGatewayTask() {
    String gatewayConfig = ConfigPrms.getGatewayConfig();
    GatewayHubHelper.addWBCLGateway(gatewayConfig);
  }

  /**
   * TASK to start a gateway hub.
   */
  public static void startGatewayHubTask() {
    GatewayHubHelper.startGatewayHub();
  }

  /**
   * TASK to stop a gateway hub.
   */
  public static void stopGatewayHubTask() {
    GatewayHubHelper.stopGatewayHub();
  }

  /**
   * Creates GatewaySender ids based on the
   * {@link ConfigPrms#gatewaySenderConfig}.
   * 
   * @since 7.0
   */
  public static void createGatewaySenderIdsTask() {
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    GatewaySenderHelper.createGatewaySenderIds(senderConfig);
  }
  
  /**
   * Initializes (create and start) GatewaySender based on the
   * {@link ConfigPrms#gatewaySenderConfig}.
   */
  public static void initGatewaySenderTask() {
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    GatewaySenderHelper.createAndStartGatewaySenders(senderConfig); 
  }
 
  /**
   * Initializes (create and start) GatewayReceiver based on the
   * {@link ConfigPrms#GatewayReceiverConfig}.
   */
  public static void initGatewayReceiverTask() {
    String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
    GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
  }
  
  /**
   * Stop gateway senders
   */
  public static void stopGatewaySenderTask() {
    GatewaySenderHelper.stopGatewaySenders();
   }

   /**
    * Stop gateway receivers
    */
  public static void stopGatewayReceiverTask() {
     GatewayReceiverHelper.stopGatewayReceivers();
   }
  
  /**
   *  TASK to unhook a vm from the cache.
   */
  public static void closeCacheTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.closeCache();
    c.updateHydraThreadLocals();
  }

  protected void closeCache() {
    synchronized (closeCacheLock) {
      Cache cache = CacheHelper.getCache();
      if (cache != null) {
        log().info("Closing the cache");
        if (DistCachePrms.getCacheVendor() == DistCachePrms.GEMFIRE) {
          Set rootRegions = cache.rootRegions();
          for (Iterator i = rootRegions.iterator(); i.hasNext();) {
            Region r = (Region)i.next();
            EdgeHelper.removeThreadLocalConnection(r);
          }
        }
        this.cache.close();
        this.cache = null;
        log().info("Closed the cache");
      }
    }
  }

  /**
   * TASK to register the cache performance and clock skew statistics objects.
   */
  public static void openStatisticsTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.openStatistics();
    c.updateHydraThreadLocals();
  }
  private void openStatistics() {
    if ( this.statistics == null ) {
      this.statistics = CachePerfStats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
  }

  /**
   * TASK to unregister the cache performance and clock skew statistics objects.
   */
  public static void closeStatisticsTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }
  protected void closeStatistics() {
    MasterController.sleepForMs( 2000 );
    if ( this.statistics != null ) {
      RemoteTestModule.closeClockSkewStatistics();
      this.statistics.close();
    }
  }

  /**
   *  TASK to create objects of type
   *  {@link cacheperf.CachePerfPrms#objectType}.
   *  Each client puts a new object at a new key.
   */
  public static void createDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( CREATES );
    c.createData();
  }
  protected void createData() {
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      create( key );
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  protected void create( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );
    long start = this.statistics.startCreate();
    this.cache.create( key, val );
    this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK using putAll to create a map containing {@link cacheperf.CachePerfPrms
   * #bulkOpMapSize} objects of type {@link cacheperf.CachePerfPrms#objectType}.
   */
  public static void createAllDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(CREATES);
    c.createAllData();
  }
  private void createAllData() {
    if (this.useTransactions) {
      this.begin();
    }
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    Map m = new HashMap();
    do {
      int size = CachePerfPrms.getBulkOpMapSize();
      for (int i = 0; i < size; i++) {
        int key = getNextKey();
        if (timeToExecuteTaskTerminator()) {
          createAll(r, m); // put partial map before terminating
          terminateTask(); // commits at task termination
          throw new HydraInternalException("Should not happen");
        }
        else if (timeToExecuteWarmupTerminator()) {
          createAll(r, m); // put partial map before syncing
          m.clear(); // reset for remainder
          terminateWarmup(); // commits at warmup termination
        }
        createMapEntry(m, key);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
        if (timeToExecuteBatchTerminator()) {
          createAll(r, m); // put partial map before returning
          terminateBatch(); // commits at batch termination
          return;
        }
      }
      createAll(r, m);
      m.clear();

    } while (true);
  }
  private void createAll(Region r, Map m) {
    boolean dummyBulkOps = CachePerfPrms.getDummyBulkOps();
    if (m.size() > 0) {
      long start = this.statistics.startCreate();
      if (dummyBulkOps) {
        for (Iterator i = m.keySet().iterator(); i.hasNext();) {
          Object key = i.next();
          r.create(key, m.get(key));
        }
      } else {
        r.putAll(m);
      }
      this.statistics.endCreate(start, m.size(), this.isMainWorkload,
                                                 this.histogram);
    }
  }

  /**
   *  TASK to create keys for objects of type
   *  {@link cacheperf.CachePerfPrms#objectType}.
   *  Each client puts a null object at a new key.
   */
  public static void createKeysTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( CREATEKEYS );
    c.createKeys();
  }
  private void createKeys() {
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      createKey( key );
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void createKey( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = CachePerfPrms.getObjectType();
    long start = this.statistics.startCreateKey();
    this.cache.createKey( key );
    this.statistics.endCreateKey(start, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK to create a map containing objects of type {@link
   * cacheperf.CachePerfPrms#objectType}.  Each client puts a new object
   * at a new key.  Used by {@link #putAllEntryMapTask}.
   */
  public static void createEntryMapTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize();
    c.createEntryMap();
  }
  private void createEntryMap() {
    Map entryMap = (Map)localentrymap.get();
    if (entryMap == null) {
      entryMap = new HashMap();
      localentrymap.set(entryMap);
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      createMapEntry(entryMap, key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator());
  }
  private void createMapEntry(Map entryMap, int i) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );
    entryMap.put(key, val);
    if (log().fineEnabled()) {
      log().fine("created map entry: " + key + "=" + val);
    }
  }
  private static HydraThreadLocal localentrymap = new HydraThreadLocal();

  /**
   *  TASK to putAll the map created in {@link #createEntryMapTask}.
   */
  public static void putAllEntryMapTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(PUTS);
    c.putAllEntryMap();
  }
  private void putAllEntryMap() {
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    Map m = (Map)localentrymap.get();
    if (this.useTransactions) {
      this.begin();
    }
    boolean batchDone = false;
    do {
      if ( this.sleepBeforeOp ) {
        MasterController.sleepForMs( CachePerfPrms.getSleepMs() );
      }
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      putAll(r, m);
      ++this.batchCount;
      ++this.count;
      ++this.iterationsSinceTxEnd;
      batchDone = executeBatchTerminator(); // commits at batch termination
    } while (!batchDone);
  }
  private void putAll(Region r, Map m) {
    boolean dummyBulkOps = CachePerfPrms.getDummyBulkOps();
    if (m.size() > 0) {
      long start = this.statistics.startPut();
      if (dummyBulkOps) {
        for (Iterator i = m.keySet().iterator(); i.hasNext();) {
          Object key = i.next();
          r.put(key, m.get(key));
        }
      } else {
        r.putAll(m);
      }
      this.statistics.endPutAll(start, m.size(), this.isMainWorkload,
                                                 this.histogram);
    }
  }

  /**
   * TASK using putAll to put a map containing {@link cacheperf.CachePerfPrms
   * #bulkOpMapSize} objects of type {@link cacheperf.CachePerfPrms#objectType}.
   */
  public static void putAllDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(PUTS);
    c.putAllData();
  }
  private void putAllData() {
    if (this.useTransactions) {
      this.begin();
    }
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    Map m = new HashMap();
    do {
      int size = CachePerfPrms.getBulkOpMapSize();
      for (int i = 0; i < size; i++) {
        int key = getNextKey();
        if (timeToExecuteTaskTerminator()) {
          putAll(r, m); // put partial map before terminating
          terminateTask(); // commits at task termination
          throw new HydraInternalException("Should not happen");
        }
        else if (timeToExecuteWarmupTerminator()) {
          putAll(r, m); // put partial map before syncing
          m.clear(); // reset for remainder
          terminateWarmup(); // commits at warmup termination
        }
        createMapEntry(m, key);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
        if (timeToExecuteBatchTerminator()) {
          putAll(r, m); // put partial map before returning
          terminateBatch(); // commits at batch termination
          return;
        }
      }
      putAll(r, m);
      m.clear();

    } while (true);
  }

  /**
   * TASK using getAll to get a map containing {@link cacheperf.CachePerfPrms
   * #bulkOpMapSize} objects of type {@link cacheperf.CachePerfPrms#objectType}.
   */
  public static void getAllDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(GETS);
    c.getAllData();
  }
  private void getAllData() {
    if (this.useTransactions) {
      this.begin();
    }
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    List l = new ArrayList();
    do {
      Map result = null;
      int size = CachePerfPrms.getBulkOpMapSize();
      for (int i = 0; i < size; i++) {
        int key = getNextKey();
        if (timeToExecuteTaskTerminator()) {
          result = getAll(r, l); // get partial map before terminating
          terminateTask(); // commits at task termination
          throw new HydraInternalException("Should not happen");
        }
        else if (timeToExecuteWarmupTerminator()) {
          result = getAll(r, l); // get partial map before syncing
          l.clear(); // reset for remainder
          terminateWarmup(); // commits at warmup termination
        }
        createListEntry(l, key);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
        if (timeToExecuteBatchTerminator()) {
          result = getAll(r, l); // get partial map before returning
          terminateBatch(); // commits at batch termination
          return;
        }
      }
      result = getAll(r, l);
      l.clear();
      if (result != null) result.clear();

    } while (true);
  }
  private Map getAll(Region r, List l) {
    boolean dummyGetAll = CachePerfPrms.getDummyBulkOps();
    Map result = null;
    if (l.size() > 0) {
      long start = this.statistics.startGet();
      if (dummyGetAll) {
        result = new HashMap();
        for (Iterator i = l.iterator(); i.hasNext();) {
          Object key = i.next();
          result.put(key, r.get(key));
        }
      } else {
        result = r.getAll(l);
      }
      this.statistics.endGetAll(start, l.size(), this.isMainWorkload,
                                                 this.histogram);

      for (Iterator i = result.keySet().iterator(); i.hasNext();) {
        Object key = i.next();
        Object val = result.get(key);
        if (val == null) {
          processNullValue(key);
        }
      }
    }
    return result;
  }
  private void createListEntry(List entryList, int i) {
    Object key = ObjectHelper.createName(this.keyType, i);
    if (!entryList.contains(key)) {
      entryList.add(key);
    }
    if (log().fineEnabled()) {
      log().fine("created list entry: " + key);
    }
  }

  /**
   *  CLOSETASK to put extra objects, untrimmed.
   */
  public static void putExtraDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(EXTRA_PUTS);
    c.putData();
  }

  /**
   *  TASK to put objects.
   */
  public static void putDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( PUTS );
    c.putData();
  }
  private void putData() {
    if (this.useTransactions) {
      this.begin();
    }
    boolean batchDone = false;
    do {
      int n = 1;
      if ( this.sleepBeforeOp ) {
        MasterController.sleepForMs( CachePerfPrms.getSleepMs() );
	n = CachePerfPrms.getSleepOpCount();
      }
      for ( int j = 0; j < n; j++ ) {
        int key = getNextKey();
        executeTaskTerminator();   // commits at task termination
        executeWarmupTerminator(); // commits at warmup termination
        put( key );
        this.batchCount += this.optimizationCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
        batchDone = executeBatchTerminator(); // commits at batch termination
      }
    } while (!batchDone);
  }
  protected void put( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );
    long start = this.statistics.startPut();
    if (this.optimizationCount == 1) {
      this.cache.put(key, val);
    } else {
      if (this.cache instanceof GemFireCacheTestImpl) {
        Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
        for (int j = 0; j < this.optimizationCount; j++) {
          r.put(key, val);
        }
      } else if (this.cache instanceof HashMapCacheImpl) {
        ConcurrentHashMap r = ((HashMapCacheImpl)this.cache).getMap();
        for (int j = 0; j < this.optimizationCount; j++) {
          r.put(key, val);
        }
      } else {
        String s = "Cannot optimize loop for non-gemfire cache";
        throw new HydraConfigException(s);
      }
    }
    this.statistics.endPut(start, this.optimizationCount, this.isMainWorkload,
                                                          this.histogram);
  }

  /**
   * TASK to update existing {@link objects.UpdatableObject}s by doing a get
   * on the object, invoking the update method on it, then putting it back.
   * @throws HydraConfigException if the object is not updatable 
   * @throws HydraRuntimeException if the key is not created
   */
  public static void updateDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(UPDATES);
    c.updateData();
  }
  private void updateData() {
    int numUpdates = CachePerfPrms.getNumUpdates();
    if (this.useTransactions) {
      this.begin();
    }
    boolean batchDone = false;
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      update(key, numUpdates);
      this.batchCount += numUpdates;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
      batchDone = executeBatchTerminator(); // commits at batch termination
    } while (!batchDone);
  }
  private void update(int i, int numUpdates) {
    Object key = ObjectHelper.createName(this.keyType, i);
    long start = this.statistics.startUpdate();
    Object val = this.cache.get(key);
    if (val == null) {
      String s = "Key has not been created: " + i;
      throw new HydraRuntimeException(s);
    }
    for (int n = 0; n < numUpdates; n++) {
      ObjectHelper.update(val);
      this.cache.put(key, val);
    }
    this.statistics.endUpdate(start, numUpdates, this.isMainWorkload,
                                                 this.histogram);
  }

  /**
   *  CLOSETASK to get extra objects, untrimmed.
   */
  public static void getExtraDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(EXTRA_GETS);
    c.getData();
  }

  /**
   *  TASK to get objects.
   */
  public static void getDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( GETS );
    c.getData();
  }
  protected void getData() {
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      if ( this.sleepBeforeOp ) {
        MasterController.sleepForMs( CachePerfPrms.getSleepMs() );
      }
      get( key );
      this.batchCount += this.optimizationCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  protected void get( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    long start = this.statistics.startGet();
    Object val = null;
    if (this.optimizationCount == 1) {
      val = this.cache.get(key);
      if ( this.invalidateAfterGet ) {
        if ( this.invalidateLocally ) {
          this.cache.localInvalidate( key );
        } else {
          this.cache.invalidate( key );
        }
      } else if ( this.destroyAfterGet ) {
        if ( this.destroyLocally ) {
          this.cache.localDestroy( key );
        } else {
          this.cache.destroy( key );
        }
      }
    } else {
      Region r = null;
      if (this.cache instanceof GemFireCacheTestImpl) {
        r = ((GemFireCacheTestImpl)this.cache).getRegion();
        for (int j = 0; j < this.optimizationCount; j++) {
          val = r.get(key);
          if ( this.invalidateAfterGet ) {
            if ( this.invalidateLocally ) {
              this.cache.localInvalidate( key );
            } else {
              this.cache.invalidate( key );
            }
          } else if ( this.destroyAfterGet ) {
            if ( this.destroyLocally ) {
              this.cache.localDestroy( key );
            } else {
              this.cache.destroy( key );
            }
          }
        }
      } else {
        String s = "Cannot optimize loop for non-gemfire cache";
        throw new HydraConfigException(s);
      }
    }
    if (val == null) {
      processNullValue(key);
    } else if (this.validateObjects) {
      validate(i, val);
    }
    this.statistics.endGet(start, this.optimizationCount, this.isMainWorkload,
                                                          this.histogram);
  }

  /**
   *  TASK to mix object creates and gets.
   */
  public static void mixCreateGetDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( CREATEGETS );
    c.mixCreateGetData();
  }
  private void mixCreateGetData() {
    int putPercentage = CachePerfPrms.getPutPercentage();
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      if ( this.sleepBeforeOp ) {
        MasterController.sleepForMs( CachePerfPrms.getSleepMs() );
      }
      mixCreateGet( key, putPercentage );
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void mixCreateGet( int i, int putPercentage ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    int n = this.rng.nextInt( 1, 100 );
    if ( n <= putPercentage ) {
      String objectType = CachePerfPrms.getObjectType();
      Object val = ObjectHelper.createObject( objectType, i );
      long start = this.statistics.startCreate();
      this.cache.create( key, val );
      this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
    } else {
      long start = this.statistics.startGet();
      Object val = this.cache.get( key );
      if (val == null) {
        processNullValue(key);
      } else if (this.validateObjects) {
        validate(i, val);
      }
      this.statistics.endGet(start, this.isMainWorkload, this.histogram);
    }
  }

  /**
   *  TASK to mix object puts and gets.
   */
  public static void mixPutGetDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( PUTGETS );
    c.mixPutGetData();
  }
  private void mixPutGetData() {
    int putPercentage = CachePerfPrms.getPutPercentage();
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      if ( this.sleepBeforeOp ) {
        MasterController.sleepForMs( CachePerfPrms.getSleepMs() );
      }
      mixPutGet( key, putPercentage );
      this.batchCount += this.optimizationCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void mixPutGet( int i, int putPercentage ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    int n = this.rng.nextInt( 1, 100 );
    if ( n <= putPercentage ) {
      String objectType = CachePerfPrms.getObjectType();
      Object val = ObjectHelper.createObject( objectType, i );
      long start = this.statistics.startPut();
      if (this.optimizationCount == 1) {
        this.cache.put(key, val);
      } else {
        Region r = null;
        if (this.cache instanceof GemFireCacheTestImpl) {
          r = ((GemFireCacheTestImpl)this.cache).getRegion();
          for (int j = 0; j < this.optimizationCount; j++) {
            r.put(key, val);
          }
        } else {
          String s = "Cannot optimize loop for non-gemfire cache";
          throw new HydraConfigException(s);
        }
      }
      this.statistics.endPut(start, this.optimizationCount, this.isMainWorkload,
                                                            this.histogram);

    } else {
      long start = this.statistics.startGet();
      Object val = null;
      if (this.optimizationCount == 1) {
        val = this.cache.get(key);
      } else {
        Region r = null;
        if (this.cache instanceof GemFireCacheTestImpl) {
          r = ((GemFireCacheTestImpl)this.cache).getRegion();
          for (int j = 0; j < this.optimizationCount; j++) {
            val = this.cache.get(key);
          }
        } else {
          String s = "Cannot optimize loop for non-gemfire cache";
          throw new HydraConfigException(s);
        }
      }
      if (val == null) {
        processNullValue(key);
      } else if (this.validateObjects) {
        validate(i, val);
      }
      this.statistics.endGet(start, this.optimizationCount, this.isMainWorkload,
                                                            this.histogram);
    }
  }

  /**
   *  TASK to do object put/gets.  The object is put, then immediately gotten.
   *
   *  @author lises
   *  @since 5.0
   */
  public static void combinePutGetDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( COMBINEDPUTGETS );
    c.combinePutGetData();
  }
  private void combinePutGetData() {
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      if ( this.sleepBeforeOp ) {
        MasterController.sleepForMs( CachePerfPrms.getSleepMs() );
      }
      combinePutGet( key );
      this.batchCount += this.optimizationCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void combinePutGet( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );
    long start = this.statistics.startCombinedPutGet();
    if (this.optimizationCount == 1) {
      this.cache.put(key, val);
      val = this.cache.get(key);
    } else {
      Region r = null;
      if (this.cache instanceof GemFireCacheTestImpl) {
        r = ((GemFireCacheTestImpl)this.cache).getRegion();
        for (int j = 0; j < this.optimizationCount; j++) {
          r.put(key, val);
          val = r.get(key);
        }
      } else {
        String s = "Cannot optimize loop for non-gemfire cache";
        throw new HydraConfigException(s);
      }
    }
    if (val == null) {
      processNullValue(key);
    } else if (this.validateObjects) {
      validate(i, val);
    }
    this.statistics.endCombinedPutGet(start, this.optimizationCount,
                                      this.isMainWorkload, this.histogram);
  }

  /**
   *  TASK to do gets with a certain percentage of recently used
   *  keys. Using this method, the test can be configured for 
   *  certain percentages of cache misses vs. cache hits.
   */
  public static void getRecentKeyDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( GETS );
    c.getRecentKeyData();
  }
  private void getRecentKeyData() {
    int recentKeysPercentage = CachePerfPrms.recentKeysPercentage(); 
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int randInt = this.batchCount % 100;
      int key;
      if (randInt <= recentKeysPercentage) { // use a recently used key
         key = getRecentKey();
         if (key == -1) // no recent keys available; get a normal key
            key = getNextKey();
      } else { // get a normal key
         key = getNextKey();
      }
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      get( key );
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }

  /**
   *  TASK to put objects for comparison purposes.
   */
  public static void putDataComparisonTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( PUTSCOMPARISON );
    c.putData();
  }

  /**
   *  TASK to get objects for comparison purposes.
   */
  public static void getDataComparisonTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( GETSCOMPARISON );
    c.getData();
  }

  /**
   * Signals the cache that the client is ready for events.  Run after
   * registering interest.
   */
  public static void readyForEventsTask() {
    CacheHelper.getCache().readyForEvents();
  }

  /**
   * TASK to register interest.  Registers interest in batches of size {@link
   * CachePerfPrms#interestBatchSize}.
   */
  public static void registerInterestTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(REGISTERINTERESTS);
    c.registerInterest();
  }
  private void registerInterest() {
    int interestBatchSize = CachePerfPrms.getInterestBatchSize();
    do {
      int key = getNextKey();
      executeTaskTerminator();   // registers remainder at task termination
      executeWarmupTerminator(); // registers remainder at warmup termination
      registerInterest(key, interestBatchSize);
      ++this.count;
      ++this.batchCount;
      ++this.keyCount;
    } while (!executeBatchTerminator()); // registers remainder at batch termination
  }
  protected void registerInterest(int i, int interestBatchSize) {
    Object key = ObjectHelper.createName(this.keyType, i);
    registerInterest(key, interestBatchSize);
  }
  protected void registerInterest(Object key, int interestBatchSize) {
    if (interestBatchSize > 1) {
      if (this.interestListBatch == null) {
        this.interestListBatch = new ArrayList();
      }
      this.interestListBatch.add(key);
      if (this.interestListBatch.size() == interestBatchSize) {
        registerInterest(INTEREST_LIST, this.interestListBatch);
      }
    } else {
      registerInterest(INTEREST_KEY, key);
    }
  }
  private void registerInterest(int type, Object keyOrList) {
    switch (type) {
      case INTEREST_KEY:
        if (keyOrList != null) {
          registerInterest(keyOrList);
        }
        break;
      case INTEREST_LIST:
        if (keyOrList != null && ((List)keyOrList).size() > 0) {
          registerInterest(keyOrList);
          keyOrList = null;
        }
      break;
    default:
      String s = "Unsupported interest object type: " + type;
      throw new HydraRuntimeException(s);
    }
  }
  private void registerInterest(Object keyOrList) {
    if (this.cache instanceof GemFireCacheTestImpl) {
      long start = this.statistics.startRegisterInterest();
      ((GemFireCacheTestImpl)this.cache).registerInterest(keyOrList,
                                     this.registerDurableInterest);
      this.statistics.endRegisterInterest(start, this.isMainWorkload,
                                                 this.histogram);
    }
  }
  private static final int INTEREST_KEY  = 0;
  private static final int INTEREST_LIST = 1;

  /**
   * TASK to register interest using a regular expression.  Requires that keys
   * have {@link CachePerfPrms#keyType} {@link objects.BatchString}.  The number
   * of keys registered is {@link objects.BatchStringPrms#batchSize}.  The
   * specific keys are those whose batch prefix equals the client tid modulo the
   * number of batches.  Requires that the batch size evenly divides {@link
   * CachePerfPrms#maxKeys}.
   */
  public static void registerInterestRegexTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(REGISTERINTERESTS);
    c.registerInterestRegex();
  }
  private void registerInterestRegex() {
    boolean logRegex = CachePerfPrms.logInterestRegex();
    int strBatchSize = BatchStringPrms.getBatchSize();
    if (this.maxKeys % strBatchSize != 0) {
      throw new HydraConfigException(BasePrms.nameForKey(BatchStringPrms.batchSize) + " does not evenly divide " + CachePerfPrms.nameForKey(CachePerfPrms.maxKeys));
    }
    int batches = this.maxKeys/strBatchSize;
    int batchNum = this.tid % batches;
    String regex = BatchString.getRegex(batchNum);
    if (this.cache instanceof GemFireCacheTestImpl) {
      if (logRegex) {
        log().info("Registering interest using regex \"" + regex + "\"");
      }
      long start = this.statistics.startRegisterInterest();
      ((GemFireCacheTestImpl)this.cache).registerInterestRegex(regex,
                                     this.registerDurableInterest);
      this.statistics.endRegisterInterest(start, this.isMainWorkload,
                                                 this.histogram);
    }
  }

  /**
   * TASK to register interest in all keys using a regular expression.
   */
  public static void registerInterestRegexAllTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(REGISTERINTERESTS);
    c.registerInterestRegexAll();
  }
  private void registerInterestRegexAll() {
    if (this.cache instanceof GemFireCacheTestImpl) {
      String regex = ".*";
      long start = this.statistics.startRegisterInterest();
      ((GemFireCacheTestImpl)this.cache).registerInterestRegex(regex,
                                     this.registerDurableInterest);
      this.statistics.endRegisterInterest(start, this.isMainWorkload,
                                                 this.histogram);
    }
  }

  /**
   * TASK to cycle interest in all keys using a regular expression.
   */
  public static void cycleRegisterInterestRegexTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(REGISTERINTERESTS);
    c.cycleRegisterInterestRegex();
  }
  private void cycleRegisterInterestRegex() {
    String regex = ".*";
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    InterestResultPolicy policy = GemFireCachePrms.getInterestResultPolicy();
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      cycleRegisterInterestRegex(r, regex, policy);
      ++this.batchCount;
      ++this.count;
    } while (!executeBatchTerminator());
  }
  private void cycleRegisterInterestRegex(Region r, String regex,
                                          InterestResultPolicy policy) {
    long start = this.statistics.startRegisterInterest();
    r.registerInterestRegex(regex, policy, this.registerDurableInterest);
    r.unregisterInterestRegex(regex);
    this.statistics.endRegisterInterest(start, this.isMainWorkload,
                                               this.histogram);
  }

  /**
   *  TASK to destroy objects.
   */
  public static void destroyDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( DESTROYS );
    c.destroyData();
  }
  protected void destroyData() {
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      try { // if this task uses random keys, we could destroy a key already destroyed 
        destroy( key );
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
      } catch( DistCacheException e ) {
        if (!(e.getCause() instanceof EntryNotFoundException)) { // skip this key
          throw e;
        } else { // cause is EntryNotFoundException
          if (this.keyAllocation == CachePerfPrms.PSEUDO_RANDOM_UNIQUE) { // keys are unique and should not get this
            throw e;
          }
        }
      }
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  protected void destroy( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    long start = this.statistics.startDestroy();
    this.cache.destroy( key );
    this.statistics.endDestroy(start, this.isMainWorkload, this.histogram);
  }
  
  /**
   *  TASK to signal that an event has occurred.
   */
  public static void signalTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize();
    c.signal();
  }
  private void signal() {
    TaskSyncBlackboard.getInstance().getSharedCounters().increment( TaskSyncBlackboard.signal );
  }

  /** Reset so that getting keys will retrieve the same pseudo-random unique keys as
   *  previously done.  So, a stream of pseudo-random unique keys can be replayed from the
   *  beginning by calling this prior to getNextKey().
   */
  public static void resetPseudoRandomUniqueKeysTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(-1);
    c.initHydraThreadLocals();
    c.currentSequenceNumber = c.getStartingSequenceNumber();
    c.setCurrentSequenceNumber(c.currentSequenceNumber);
    c.currentNumPseudoRandomKeys = 0;
    c.setCurrentNumPseudoRandomKeys(c.currentNumPseudoRandomKeys);
    CachePerfBlackboard.getInstance().getSharedCounters().zero(CachePerfBlackboard.counter);
  }
  
  /**
   * TASK to index objects with {@link CachePerfPrms#queryIndex} and {@link
   * CachePerfPrms#queryFromClause}.
   */
  public static void indexDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize();
    c.indexData();
  }
  protected void indexData() {
    String indexStr = CachePerfPrms.getQueryIndex();
    String fromClause = CachePerfPrms.getQueryFromClause();
    try {
      CacheHelper.getCache().getQueryService().createIndex("index",
        IndexType.FUNCTIONAL, indexStr, fromClause);
    } catch (IndexExistsException e) {
      log().info("index already created");
    } catch (IndexNameConflictException e) {
      log().info("index already created");
    } catch (QueryException e) {
      String s = "Problem creating index: " + indexStr + " " + fromClause;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * TASK to query objects with {@link CachePerfPrms#query} using region query.
   */
  public static void queryRegionDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(QUERIES);
    c.queryRegionData();
  }
  protected void queryRegionData() {
    String queryPredicate = CachePerfPrms.getQuery();
    String regionName = GemFireCachePrms.getRegionName();
    Region region = RegionHelper.getRegion(regionName);
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      queryRegion(region, queryPredicate);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator());
  }
  protected void queryRegion(Region region, String queryPredicate) {
    try {
      long start = this.statistics.startQuery();
      SelectResults results = (SelectResults)region.query(queryPredicate);
      int numResults = results.size();
      this.statistics.endQuery(start, numResults, this.isMainWorkload,
                                                  this.histogram);
      if (log().fineEnabled()) {
        log().fine("Query " + queryPredicate + " returned " + numResults);
        if (log().finerEnabled()) {
          log().finer(getResultString(results));
        }
      }
    } catch (QueryException e) {
      String s = "Problem executing query: " + queryPredicate;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * TASK to query objects with {@link CachePerfPrms#query} using region query
   * over a randomized range of size {@link CachePerfPrms#queryRangeSize}.
   */
  public static void queryRangeRegionDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(QUERIES);
    c.queryRangeRegionData();
  }
  protected void queryRangeRegionData() {
    GsRandom rng = new GsRandom(12); // need determinism
    String queryPredicate = CachePerfPrms.getQuery();
    long queryRangeMin = CachePerfPrms.getQueryRangeMin();
    long queryRangeMax = CachePerfPrms.getQueryRangeMax();
    long queryRangeSize = CachePerfPrms.getQueryRangeSize();
    String regionName = GemFireCachePrms.getRegionName();
    Region region = RegionHelper.getRegion(regionName);
    double random = -1;
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      String query = queryPredicate;
      long queryRangeStart = rng.nextLong(queryRangeMin, queryRangeMax);
      query = query.replaceAll("RANDOM_MIN",
              (new Double(queryRangeStart)).toString());
      query = query.replaceAll("RANDOM_MAX",
              (new Double(queryRangeStart + queryRangeSize)).toString());
      queryRangeRegion(region, query);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator());
  }
  protected void queryRangeRegion(Region region, String queryPredicate) {
    try {
      long start = this.statistics.startQuery();
      SelectResults results = (SelectResults)region.query(queryPredicate);
      int numResults = results.size();
      this.statistics.endQuery(start, numResults, this.isMainWorkload,
                                                  this.histogram);
      if (log().fineEnabled()) {
        log().fine("Query " + queryPredicate + " returned " + numResults);
        if (log().finerEnabled()) {
          log().finer(getResultString(results));
        }
      }
    } catch (QueryException e) {
      String s = "Problem executing query: " + queryPredicate;
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * TASK to query objects with {@link CachePerfPrms#query} using the query
   * service.
   */
  public static void queryDataTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(QUERIES);
    c.queryData();
  }
  protected void queryData() {
    String queryStr = CachePerfPrms.getQuery();
    Query query = CacheHelper.getCache().getQueryService().newQuery(queryStr);
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      query(query);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator());
  }
  protected void query(Query query) {
    try {
      long start = this.statistics.startQuery();
      SelectResults results = (SelectResults)query.execute();
      int numResults = results.size();
      this.statistics.endQuery(start, numResults, this.isMainWorkload,
                                                  this.histogram);
      if (log().fineEnabled()) {
        log().fine("Query " + query + " returned " + numResults);
        if (log().finerEnabled()) {
          log().finer(getResultString(results));
        }
      }
    } catch (QueryException e) {
      String s = "Problem executing query: " + query;
      throw new HydraRuntimeException(s, e);
    }
  }

  protected String getResultString(SelectResults results) {
    StringBuffer buf = new StringBuffer();
    buf.append("Query returned " + results.size() + " results\n");
    for (Iterator it = results.iterator(); it.hasNext();) {
      Struct struct = (Struct)it.next();
      String[] fieldNames = struct.getStructType().getFieldNames();
      Object[] vals = struct.getFieldValues();
      for (int i = 0; i < fieldNames.length; i++) {
        buf.append(fieldNames[i] + "=" + vals[i] + "\n");
      }
    }
    return buf.toString();
  }

  /**
   * Serves as the stat archiver for statless clients.  Samples at 1 second
   * intervals.  Reads the values of {@link TaskSyncBlackboard#ops} and {@link
   * TaskSyncBlackboard#opTime}, which are assumed to be written by the
   * stateless clients.  Relies on the usual terminators.
   */
  public static void statArchiverTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(OPS);
    c.statArchiver();
  }
  private void statArchiver() {
    SharedCounters counters =
                   TaskSyncBlackboard.getInstance().getSharedCounters();
    boolean batchDone = false;
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      archive(counters);
      ++this.batchCount;
      ++this.count;
      batchDone = executeBatchTerminator();
    } while (!batchDone);
  }
  private void archive(SharedCounters counters) {
    MasterController.sleepForMs(1000);
    long ops = counters.read(TaskSyncBlackboard.ops);
    long opTime = counters.read(TaskSyncBlackboard.opTime);
    long opTime1 = counters.read(TaskSyncBlackboard.opTime1);
    long opTime2 = counters.read(TaskSyncBlackboard.opTime2);
    long opTime3 = counters.read(TaskSyncBlackboard.opTime3);
    this.statistics.setOps((int)ops);
    this.statistics.setOpTime(opTime);
    this.statistics.setOpTime1(opTime1);
    this.statistics.setOpTime2(opTime2);
    this.statistics.setOpTime3(opTime3);
  }

  /**
   * TASK to cycle the connection on a distributed system, sidestepping the
   * distcache framework altogether.  Cannot be used with other task methods in
   * this class, except for {@link #statArchiverTask}.  Writes the values of
   * {@link TaskSyncBlackboard#ops} and {@link TaskSyncBlackboard#opTime},
   * which are needed by the stat archiver task.  Goes to great pains to avoid
   * logging overhead by minimal use of helper classes.
   */
  public static void cycleDistributedSystemConnectionOnlyTask()
  throws CacheException {
    CachePerfClient c = new CachePerfClient();
    c.initialize(CONNECTS);
    c.cycleDistributedSystemConnectionOnly();
  }
  private void cycleDistributedSystemConnectionOnly() throws CacheException {
    // get the configured properties
    Properties p = (Properties)cycleDistributedSystemConnectionProperties.get();
    SharedCounters counters =
          (SharedCounters)cycleDistributedSystemConnectionCounters.get();
    if (p == null) {
      // initialize cycling parameters
      p = DistributedSystemHelper.getDistributedSystemProperties(null);
      counters = TaskSyncBlackboard.getInstance().getSharedCounters();

      // save for next task run
      cycleDistributedSystemConnectionProperties.set(p);
      cycleDistributedSystemConnectionCounters.set(counters);
    }

    // do the cycling
    int ms = CachePerfPrms.getSleepMs();
    boolean batchDone = false;
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      cycleDistributedSystemConnection(p, counters);
      ++this.batchCount;
      ++this.count;
      batchDone = executeBatchTerminator();
      MasterController.sleepForMs(ms); // throttle
    } while (!batchDone);
  }
  private void cycleDistributedSystemConnection(Properties p,
               SharedCounters counters) throws CacheException {
    // cycle the distributed system
    long start = NanoTimer.getTime();
    long start1 = start;
    DistributedSystem ds = DistributedSystem.connect(p);
    long start2 = NanoTimer.getTime();
    long elapsed1 = start2 - start1;
    Cache c = CacheFactory.create(ds);
    c.close();
    long start3 = NanoTimer.getTime();
    long elapsed2 = start3 - start2;
    ds.disconnect();
    long end = NanoTimer.getTime();
    long elapsed3 = end - start3;
    long elapsed = end - start;

    // notify the remote statarchiver via the blackboard
    counters.increment(TaskSyncBlackboard.ops);
    counters.add(TaskSyncBlackboard.opTime, elapsed);
    counters.add(TaskSyncBlackboard.opTime1, elapsed1);
    counters.add(TaskSyncBlackboard.opTime2, elapsed2);
    counters.add(TaskSyncBlackboard.opTime3, elapsed3);

    // remove the stat archive and system/security logs to avoid inode overload
    String fn = p.getProperty(DistributionConfig.LOG_FILE_NAME);
    if (fn != null) {
      FileUtil.deleteFile(fn);
    }
    fn = p.getProperty(DistributionConfig.SECURITY_LOG_FILE_NAME);
    if (fn != null) {
      FileUtil.deleteFile(fn);
    }
    fn = p.getProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME);
    if (fn != null) {
      FileUtil.deleteFile(fn);
    }
  }
  private static HydraThreadLocal cycleDistributedSystemConnectionProperties = new HydraThreadLocal();
  private static HydraThreadLocal cycleDistributedSystemConnectionCounters = new HydraThreadLocal();

  /**
   * TASK to cycle the connection on a distributed system, creating a cache
   * and region from an XML file based on settings of {@link
   * hydra.ConfigPrms#cacheConfig} and {@link hydra.ConfigPrms#regionConfig},
   * sidestepping the distcache framework altogether.  Cannot be used with other
   * task methods in this class, except for {@link #statArchiverTask}.
   * Writes the values of {@link TaskSyncBlackboard#ops} and {@link
   * TaskSyncBlackboard#opTime}, which are needed by the stat archiver task.
   * Goes to great pains to avoid logging overhead by minimal use of helper
   * classes.
   */
  public static void cycleDistributedSystemConnectionTask()
  throws CacheException {
    CachePerfClient c = new CachePerfClient();
    c.initialize(CONNECTS);
    c.cycleDistributedSystemConnection();
  }
  private void cycleDistributedSystemConnection() throws CacheException {
    // get the configured and generated properties
    Properties p = (Properties)cycleDistributedSystemConnectionProperties.get();
    SharedCounters counters =
          (SharedCounters)cycleDistributedSystemConnectionCounters.get();
    if (p == null) {
      String cacheXmlFile = System.getProperty("user.dir") + "/"
                          + System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY)
                          + ".xml";

      // generate the XML file
      CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(),
                                       ConfigPrms.getRegionConfig(),
                                       cacheXmlFile);
      DistributedSystemHelper.disconnect();

      // initialize cycling parameters
      p = DistributedSystemHelper.getDistributedSystemProperties(cacheXmlFile);
      counters = TaskSyncBlackboard.getInstance().getSharedCounters();

      // save for next task run
      cycleDistributedSystemConnectionProperties.set(p);
      cycleDistributedSystemConnectionCounters.set(counters);
    }

    // do the cycling
    int ms = CachePerfPrms.getSleepMs();
    boolean batchDone = false;
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      cycleDistributedSystemConnection(p, counters);
      ++this.batchCount;
      ++this.count;
      batchDone = executeBatchTerminator();
      MasterController.sleepForMs(ms); // throttle
    } while (!batchDone);
  }

  /**
   * TASK to create and destroy a connection pool and region, sidestepping
   * the distcache framework altogether.  Cannot be used with other task methods
   * in this class.  Goes to great pains to avoid logging overhead by minimal
   * use of helper classes.
   */
  public static void cyclePoolAndRegionTask() throws CacheException {
    CachePerfClient c = new CachePerfClient();
    c.initialize(CONNECTS);
    c.cyclePoolAndRegion();
  }
  private void cyclePoolAndRegion() throws CacheException {
    // get the pool name
    String poolConfig = ConfigPrms.getPoolConfig();
    // get cache and configured region and pool factories
    Cache theCache = (Cache)cycleBridgeConnectionCache.get();
    AttributesFactory regionFactory =
              (AttributesFactory)cycleBridgeConnectionFactory.get();
    PoolFactory poolFactory = (PoolFactory)cyclePoolFactory.get();
    if (poolFactory == null) {
      // connect to the distributed system and open the cache and statistics
      DistributedSystem ds = DistributedSystemHelper.connect();
      theCache = CacheFactory.create(ds);
      openStatistics();

      // get the configured pool factory
      poolFactory = PoolHelper.getPoolFactory(poolConfig);

      // get the configured region factory
      String regionConfig = ConfigPrms.getRegionConfig();
      regionFactory = RegionHelper.getAttributesFactory(regionConfig);

      // save for next task run
      cycleBridgeConnectionCache.set(theCache);
      cycleBridgeConnectionFactory.set(regionFactory);
      cyclePoolFactory.set(poolFactory);
    }

    // do the cycling
    int ms = CachePerfPrms.getSleepMs();
    boolean batchDone = false;
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      cyclePoolAndRegion(theCache, regionFactory, poolFactory, poolConfig);
      ++this.batchCount;
      ++this.count;
      batchDone = executeBatchTerminator();
      MasterController.sleepForMs(ms); // throttle
    } while (!batchDone);
  }
  private void cyclePoolAndRegion(Cache theCache,
    AttributesFactory regionFactory, PoolFactory poolFactory, String poolName)
  throws CacheException {
    long start = this.statistics.startConnect();
    Pool pool = poolFactory.create(poolName);
    ((PoolImpl)pool).acquireConnection();
    RegionAttributes ratts = regionFactory.create();
    Region theRegion = theCache.createRegion("RecycledRegion", ratts);
    theRegion.close();
    pool.destroy();
    this.statistics.endConnect(start, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK to create and destroy a connection pool, over and over, sidestepping
   * the distcache framework altogether.  Cannot be used with other task methods
   * in this class.  Goes to great pains to avoid logging overhead by minimal
   * use of helper classes.
   */
  public static void cyclePoolTask() throws CacheException {
    CachePerfClient c = new CachePerfClient();
    c.initialize(CONNECTS);
    c.cyclePool();
  }
  private void cyclePool() throws CacheException {
    // get the configured pool factory
    String poolConfig = ConfigPrms.getPoolConfig();
    PoolFactory factory = (PoolFactory)cyclePoolFactory.get();
    if (factory == null) {
      DistributedSystem ds = DistributedSystemHelper.connect();
      openStatistics();
      factory = PoolHelper.getPoolFactory(poolConfig);
      // save for next task run
      cyclePoolFactory.set(factory);
    }

    // do the cycling
    int ms = CachePerfPrms.getSleepMs();
    boolean batchDone = false;
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      cyclePool(factory, poolConfig);
      ++this.batchCount;
      ++this.count;
      batchDone = executeBatchTerminator();
      MasterController.sleepForMs(ms); // throttle
    } while (!batchDone);
  }
  private void cyclePool(PoolFactory factory, String poolName)
  throws CacheException {
    long start = this.statistics.startConnect();
    Pool pool = factory.create(poolName);
    ((PoolImpl)pool).acquireConnection();
    pool.destroy();
    this.statistics.endConnect(start, this.isMainWorkload, this.histogram);
  }

  private static HydraThreadLocal cyclePoolFactory = new HydraThreadLocal();

  private static HydraThreadLocal cycleBridgeConnectionCache = new HydraThreadLocal();
  private static HydraThreadLocal cycleBridgeConnectionFactory = new HydraThreadLocal();
  
  /**
   * TASK to create a durable client and retrieve updates from the server,
   * ignoring the distcache framework altogether. The task alternates between a
   * client that puts values into the region and a client that retrieves those
   * updates from the region. Cannot be used with other task methods in this
   * class. This task must create a new distributed system for each cycle,
   * because it has to change the durable client id property.
   */
  public static void cycleDurableClientTask() throws CacheException,
      InterruptedException {
    CachePerfClient c = new CachePerfClient();
    c.initialize(CONNECTS);
    c.cycleDurableClient();
  }
  private void cycleDurableClient() throws CacheException, InterruptedException  {
    // connect to the distributed system and open statistics
    DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
    if (ds == null) {
      ds = DistributedSystemHelper.connect();
      openStatistics();
    }

    // get the configured pool factory
    String regionConfig = ConfigPrms.getRegionConfig();
    String poolName = RegionHelper.getRegionDescription(regionConfig)
                                  .getPoolDescription().getName();
    PoolFactory poolFactory = (PoolFactory)cyclePoolFactory.get();
    if (poolFactory == null) { // first time through
      poolFactory = PoolHelper.getPoolFactory(poolName);
      cyclePoolFactory.set(poolFactory);
    }

    // get the configured region factory
    AttributesFactory regionFactory = RegionHelper.getAttributesFactory(regionConfig);
    
    // do the cycling
    int ms = CachePerfPrms.getSleepMs();
    boolean batchDone = false;
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(TestHelper.getNumThreads(), "cachePerfBarrier");
    do {
      barrier.await();
      try {
        try {
          executeTaskTerminator();
        } catch(StopSchedulingTaskOnClientOrder e) {
          signal(); //signal the put task to stop.
          throw e;
        }
        executeWarmupTerminator();
        durableClientConnect(ds, poolFactory, poolName, regionFactory);
      } finally {
        barrier.await();
      }
      ++this.batchCount;
      ++this.count;
      batchDone = executeBatchTerminator();
      MasterController.sleepForMs(ms); // throttle
    } while (!batchDone);
  }
  private void durableClientConnect(DistributedSystem ds,
                                    PoolFactory poolFactory,
                                    String poolName,
                                    AttributesFactory regionFactory)
  throws CacheException, InterruptedException {
    // add a listener to the cache which will wait for previously queued events
    // to complete
    final BlockingQueue queue = new LinkedBlockingQueue();
    CacheListener listener = new CacheListenerAdapter() {
      AtomicInteger updateCount = new AtomicInteger();
      
      public void afterCreate(EntryEvent event) {
        updateCount.incrementAndGet();
      }

      public void afterUpdate(EntryEvent event) {
        updateCount.incrementAndGet();
      }

      public void afterRegionLive(RegionEvent event) {
        queue.add(new Integer(updateCount.get()));
      }
    };
    
    long start = this.statistics.startConnect();
    Cache theCache = CacheFactory.create(ds);
    Pool pool = poolFactory.create(poolName);
    regionFactory.addCacheListener(listener);
    RegionAttributes ratts = regionFactory.create();
    Region theRegion = theCache.createRegion(RegionPrms.DEFAULT_REGION_NAME, ratts);
    // register our durable interest again
    theRegion.registerInterestRegex(".*", true);
    theCache.readyForEvents();
    //wait for the region to finish initializing
    Integer updateCount = (Integer) queue.take();
    this.statistics.endConnect(start, false, this.histogram);
    this.statistics.incUpdateEvents(updateCount.intValue());
    this.statistics.setOpTime(this.statistics.getConnectTime());
    this.statistics.setOps(updateCount.intValue() + this.statistics.getOps());
    theCache.close(true);
    pool.destroy();
  }
  /**
   * Task that calls putDataTask, and then syncs twice on a barrier. This
   * is mostly for use with cycleDurableClient, but it could be useful for
   * other tests that need to put a bunch of entries in one VM and then
   * wait for another VM to process the results.
   */
  public static void putDataAndSyncTask() throws InterruptedException {
    putDataTask();
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(TestHelper.getNumThreads(), "cachePerfBarrier");
    //Signal all clients to start connecting
    barrier.await();
    //wait for all clients to finish connecting
    barrier.await();
  }

  /**
   *  TASK to put objects and wait for the gateway queues to drain.
   */
  public static void putDataGatewayTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( PUTS );
    c.putDataGateway();
  }
  private void putDataGateway() {
    if (this.useTransactions) {
      this.begin();
    }
    int gatewayQueueEntries = CachePerfPrms.getGatewayQueueEntries();
    boolean batchDone = false;
    do {
      // delay getting key until inside put loop
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      for (int i = 0; i < gatewayQueueEntries; i++) {
        int key = getNextKey();
        put(key);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
      }
      waitForGatewayQueuesToDrain();
      batchDone = executeBatchTerminator(); // commits at batch termination
    } while (!batchDone);
  }
  private void waitForGatewayQueuesToDrain() {
    long start = this.statistics.startGatewayQueueDrain();
    GatewayHub hub = GatewayHubHelper.getGatewayHub();
    List gateways = hub.getGateways();
    while (true) {
      MasterController.sleepForMs(1);
      int size = 0;
      for (Iterator i = gateways.iterator(); i.hasNext();) {
        Gateway gateway = (Gateway)i.next();
        size += gateway.getQueueSize();
      }
      if (size == 0) {
        break;
      }
    }
    this.statistics.endGatewayQueueDrain(start, 1, this.isMainWorkload,
                                                   this.histogram);
  }

  /**
   *  TASK to put objects with new wan and wait for the sender queues to drain.
   */
  public static void putDataGWSenderTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( PUTS );
    c.putDataGWSender();
  }
  private void putDataGWSender() {
    if (this.useTransactions) {
      this.begin();
    }
    int gatewayQueueEntries = CachePerfPrms.getGatewayQueueEntries();
    boolean batchDone = false;
    do {
      // delay getting key until inside put loop
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      for (int i = 0; i < gatewayQueueEntries; i++) {
        int key = getNextKey();
        put(key);
        ++this.batchCount;
        ++this.count;
        ++this.keyCount;
        ++this.iterationsSinceTxEnd;
      }
      waitForGWSenderQueuesToDrain();
      batchDone = executeBatchTerminator(); // commits at batch termination
    } while (!batchDone);
  }
  private void waitForGWSenderQueuesToDrain() {
    long start = this.statistics.startGatewayQueueDrain();    
    Set senders = GatewaySenderHelper.getGatewaySenders();
    while (senders != null) {
      MasterController.sleepForMs(1);
      int size = 0;
      int totalBatchSize = 0;
      for (Iterator i = senders.iterator(); i.hasNext();) {        
        GatewaySender sender = (GatewaySender)i.next();
        totalBatchSize += sender.getBatchSize();
        Set<RegionQueue> rqs = ((AbstractGatewaySender)sender).getQueues();
        for (RegionQueue rq: rqs){
          size += rq.size();
        }
      }
      if (size <= totalBatchSize) {
        break;
      }
    }
    this.statistics.endGatewayQueueDrain(start, 1, this.isMainWorkload,
                                                   this.histogram);
  }

  /**
   * TASK to cycle a gateway hub connection, avoiding logging.
   */
  public static void cycleGatewayHubConnectionTask()
  throws CacheException {
    CachePerfClient c = new CachePerfClient();
    c.initialize(CONNECTS);
    c.cycleGatewayHubConnection();
  }
  private void cycleGatewayHubConnection() {
    GatewayHub hub = GatewayHubHelper.getGatewayHub();
    int ms = CachePerfPrms.getSleepMs();
    boolean batchDone = false;
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      cycleGatewayHub(hub, ms);
      ++this.batchCount;
      ++this.count;
      batchDone = executeBatchTerminator();
    } while (!batchDone);
  }
  private void cycleGatewayHub(GatewayHub hub, int throttle) {
    long start = this.statistics.startConnect();
    try {
      hub.start();
    } catch (IOException e) {
      String s = "Problem starting gateway hub " + hub;
      throw new HydraRuntimeException(s, e);
    }
    this.statistics.endConnect(start, this.isMainWorkload, this.histogram);
    MasterController.sleepForMs(throttle); // throttle
    hub.stop();
  } 

  /**
   * TASK to lock and unlock a lock.
   */
  public static void cycleLockTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize(LOCKS);
    c.cycleLock();
  }
  private void cycleLock() {
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      cycleLock(key);
      this.batchCount += this.optimizationCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void cycleLock(int i) {
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    Object key = ObjectHelper.createName(this.keyType, i);
    Lock entryLock = r.getDistributedLock(key);
    long start = this.statistics.startLock();
    for (int j = 0; j < this.optimizationCount; j++) {
      entryLock.lock();
      try {
      } finally {
        entryLock.unlock();
      }
    }
    this.statistics.endLock(start, this.optimizationCount, this.isMainWorkload,
                                                           this.histogram);
  } 

  /**
   *  CLOSETASK to report trim intervals.
   */
  public static void reportTrimIntervalsTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.reportTrimIntervals();
  }
  private void reportTrimIntervals() {
    Map trimIntervals = getTrimIntervals();
    if ( trimIntervals.size() > 0 ) {
      PerfStatMgr.getInstance().reportTrimIntervals( trimIntervals );
    }
  }

  /**
   *  CLOSETASK to report extended trim intervals.
   */
  public static void reportExtendedTrimIntervalsTask() {
    CachePerfClient c = new CachePerfClient();
    c.initHydraThreadLocals();
    c.reportExtendedTrimIntervals();
  }
  private void reportExtendedTrimIntervals() {
    Map trimIntervals = getTrimIntervals();
    if ( trimIntervals.size() > 0 ) {
      PerfStatMgr.getInstance().reportExtendedTrimIntervals( trimIntervals );
    }
  }

  /**
   *  CLOSETASK to validate that the region entry count is equals to the maximum
   *  number of keys in the region.
   */
  public static void validateMaxKeysRegionEntriesTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize();
    c.validateMaxKeysRegionEntries();
  }
  private void validateMaxKeysRegionEntries() {
    int sz = this.cache.size();
    if ( sz == this.maxKeys ) {
      log().info( "Got the expected number of region entries: " + sz );
    } else {
      if (DistributionManager.VERBOSE && (this.cache instanceof GemFireCacheTestImpl)) {
        for (Iterator it = ((GemFireCacheTestImpl)this.cache).getRegion().entrySet().iterator();
        it.hasNext(); ) {
          Region.Entry re = (Region.Entry)it.next();
          Log.getLogWriter().info("(region dump) key = " + re.getKey());
        }
      }
      throw new CachePerfException( "Expected region with " + this.maxKeys + " entries but found " + sz );
    }
  }

  /**
   * TASK to do a gc.
   */
  public static void doGC() throws CacheException {
     Log.getLogWriter().info("Calling System.gc()");
     System.gc();
     Log.getLogWriter().info("Done calling System.gc()");
  }
  
  //----------------------------------------------------------------------------
  //  Value support
  //----------------------------------------------------------------------------

  public void processNullValue(Object key) {
    if (this.allowNulls) {
      this.statistics.incNulls();
    } else {
      String s = "Got null at key=" + key + ", maxKeys=" + this.maxKeys;
      throw new HydraRuntimeException(s);
    }
  }

  //----------------------------------------------------------------------------
  //  Key support
  //----------------------------------------------------------------------------

  protected int getNextKey() {
    int key;
    switch( this.keyAllocation ) {
      case CachePerfPrms.SAME_KEY:
        key = 0;
        break;
      case CachePerfPrms.SAME_KEYS:
        key = this.keyCount;
        break;
      case CachePerfPrms.SAME_KEYS_WRAP:
        key = this.keyCount % this.maxKeys;
        break;
      case CachePerfPrms.SAME_KEYS_RANDOM_WRAP:
        key = this.rng.nextInt( 0, this.maxKeys - 1 );
        break;
      case CachePerfPrms.OWN_KEY:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	  key = this.ttgid;
        } else {
          key = this.currentKey;
	}
        break;
      case CachePerfPrms.OWN_KEYS:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	  key = this.ttgid;
	} else {
          key = this.currentKey + this.numThreads;
	}
        break;
      case CachePerfPrms.OWN_KEYS_WRAP:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	  key = this.ttgid;
	} else {
          key = this.currentKey + this.numThreads;
	}
	if ( key >= this.maxKeys ) {
	  key = this.ttgid;
	}
        break;
      case CachePerfPrms.OWN_KEYS_RANDOM_WRAP:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	}
        int numKeys = (int) Math.ceil( this.maxKeys / this.numThreads );
        if ( this.ttgid >= this.maxKeys % this.numThreads ) {
          --numKeys;
        }
        key = this.ttgid + this.numThreads * this.rng.nextInt( 0, numKeys );
        break;
      case CachePerfPrms.OWN_KEYS_CHUNKED:
        if (this.currentKey == -1) {
	  checkSufficientKeys();
	  key = this.ttgid * this.keyAllocationChunkSize;
	} else if ((this.currentKey + 1) % this.keyAllocationChunkSize == 0) {
          key = this.currentKey + 1
              + (this.numThreads - 1) * this.keyAllocationChunkSize;
        } else {
          key = this.currentKey + 1;
	}
        break;
      case CachePerfPrms.OWN_KEYS_CHUNKED_RANDOM_WRAP:
        if (this.currentKey == -1) {
	  checkSufficientKeys();
          if (this.maxKeys % this.keyAllocationChunkSize != 0) {
            String s = BasePrms.nameForKey(CachePerfPrms.keyAllocationChunkSize)
                     + "=" + this.keyAllocationChunkSize
                     + " does not evenly divide "
                     + BasePrms.nameForKey(CachePerfPrms.maxKeys)
                     + "=" + this.maxKeys;
            throw new HydraConfigException(s);
          }
        }
        if (this.iterationsSinceTxEnd == 0) {
          // we committed a partial batch
          this.currentKey = -1;
        }
        if ((this.currentKey + 1) % this.keyAllocationChunkSize == 0) {
          // first pick a random "own" chunk number
          int numChunks = this.maxKeys/this.keyAllocationChunkSize;
          int numChunksPerThread = (int)Math.ceil(numChunks/this.numThreads);
          if (this.ttgid >= numChunks % this.numThreads) {
            --numChunksPerThread;
          }
          int chunk = this.ttgid + this.numThreads * this.rng.nextInt(0, numChunksPerThread);
          key = chunk * this.keyAllocationChunkSize;
	} else {
          // continue this chunk
          key = this.currentKey + 1;
	}
        break;
      case CachePerfPrms.DISJOINT_AS_POSSIBLE:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	  key = this.ttgid;
	} else {
          key = this.currentKey + this.numThreads;
	}
	if ( key >= this.maxKeys ) {
	  key = (this.ttgid + (int)Math.ceil(this.count/this.numThreads)) % this.numThreads;
          // this is less than maxKeys because we did checkSufficientKeys()
	}
        break;
      case CachePerfPrms.PSEUDO_RANDOM_UNIQUE:
        Long aKey = getNextPseudoRandomUniqueKey();
        if (aKey == null) { // no more unique keys to return
          updateHydraThreadLocals();
          throw new StopSchedulingTaskOnClientOrder();
        }
        key = (int)((long)aKey);
        break;
      case CachePerfPrms.ZIPFIAN:
        key = this.keyGenerator.nextInt();
        break;
      case CachePerfPrms.SCRAMBLED_ZIPFIAN:
        key = this.keyGenerator.nextInt();
        break;
      default:
        throw new HydraInternalException( "Should not happen" );
    }
    this.currentKey = key;
    return key;
  }
  protected int getRecentKey() {
    int maxRecentKeys = CachePerfPrms.maxRecentKeys(); 
    int key;
    switch( this.keyAllocation ) {
      case CachePerfPrms.SAME_KEY:
        key = 0;
        break;
      case CachePerfPrms.SAME_KEYS:
        throw new CachePerfException( "Unsupported recent key for sameKeys");
      case CachePerfPrms.SAME_KEYS_WRAP:
        throw new CachePerfException( "Unsupported recent key for sameKeysWrap");
      case CachePerfPrms.SAME_KEYS_RANDOM_WRAP:
        throw new CachePerfException( "Unsupported recent key for sameKeysRandomWrap");
      case CachePerfPrms.OWN_KEY:
        throw new CachePerfException( "Unsupported recent key for ownKey");
      case CachePerfPrms.OWN_KEYS:
        int backNumKeys = this.rng.nextInt(0, maxRecentKeys - 1);
        key = Math.max(this.ttgid, 
                      (this.currentKey - (backNumKeys * this.numThreads)));
        break;
      case CachePerfPrms.OWN_KEYS_WRAP:
        throw new CachePerfException( "Unsupported recent key for ownKeysWrap");
      case CachePerfPrms.OWN_KEYS_RANDOM_WRAP:
        throw new CachePerfException( "Unsupported recent key for ownKeysRandomWrap");
      case CachePerfPrms.OWN_KEYS_CHUNKED:
        throw new CachePerfException( "Unsupported recent key for ownKeysChunked");
      case CachePerfPrms.OWN_KEYS_CHUNKED_RANDOM_WRAP:
        throw new CachePerfException( "Unsupported recent key for ownKeysChunkedRandomWrap");
      case CachePerfPrms.DISJOINT_AS_POSSIBLE:
        throw new CachePerfException( "Unsupported recent key for disjointAsPossible");
      default:
        throw new HydraInternalException( "Should not happen" );
    }
    return key;
  }
  private void checkSufficientKeys() {
    if ( this.numThreads > this.maxKeys / this.keyAllocationChunkSize ) {
      throw new HydraConfigException
      (
	this.maxKeys + " keys are not enough for " +
	this.numThreads + " threads to have their own keys (or chunk of keys)"
      );
    }
  }

  /** A specialized use of keys where the keys are unique and pseudo-random. 
   *  This will not return the same key twice. When no more keys are left to
   *  be returned, this returns null.
   *  
   *  Keys are determined by using an arithmetic sequence:
   *  a<n>  = a<0>  + (n - 1)d
   *  where <> indicates subscripts, n is the nth number in the sequence, d is the difference between any two sequence numbers.
   *  
   *  The test chooses a different value of n as a starting point for each thread to simulate randomness
   *  but note that this strategy is not uniformly random (thus it is pseudo-random). This is so the test
   *  does not need to store the entire list of keys already returned.
   *  
   * @return The next pseudo-random unique key, or null if none left.
   */
  private Long getNextPseudoRandomUniqueKey() {
    int maxKeyValue = this.maxKeys-1; // keys are 0 based
    if (numPseudoRandomKeys == -1) { // define the hydra thread locals for pseudo-random unique keys
      long maxSeqNum = (int) Math.ceil((float)(this.maxKeys) / (float)(this.numThreads));
      long startingSequenceNum = this.rng.nextLong(1, maxSeqNum);
      setStartingSequenceNumber(startingSequenceNum);
      
      int percentage = CachePerfPrms.getKeyPercentage();
      long totalNumKeys = (long) Math.round(this.maxKeys * (percentage * 0.01));
      //Log.getLogWriter().info("xxx totalNumKeys (result of percentage) is " + totalNumKeys);
      long numKeysPerThread = totalNumKeys / this.numThreads; // throws away the remainder
      //Log.getLogWriter().info("xxx prior to adjustment, numKeysPerThread is " + numKeysPerThread);
      long remainder = totalNumKeys % this.numThreads;
      //Log.getLogWriter().info("xxx remainder is " + remainder);
      long numKeysAvailToThisThread = maxSeqNum;
      if (remainder > 0) { // each thread has a different number of keys
        numKeysAvailToThisThread--; // since we have a remainder, we don't yet know if this thread has a key in range of maxSeqNum (the initial value)
        long keyForMaxSequenceNum = this.ttgid + ((maxSeqNum - 1) * this.numThreads);
        //Log.getLogWriter().info("xxx keyforMaxSequenceNum is " + keyForMaxSequenceNum + ", ttgid is " + ttgid);
        if (keyForMaxSequenceNum <= maxKeyValue) { // this is valid key
          numKeysAvailToThisThread++;
        }
        //Log.getLogWriter().info("xxx numKeysAvailToThisThread is " + numKeysAvailToThisThread);
        
        // remainder number of threads must take on one more if they have a key available
        if (numKeysAvailToThisThread > numKeysPerThread) {
          long counter = CachePerfBlackboard.getInstance().getSharedCounters().incrementAndRead(CachePerfBlackboard.counter);
          if (counter <= remainder) {
            numKeysPerThread++; // adjust as not all threads will have the same number of keys available
            //Log.getLogWriter().info("xxx ADJUSTED, numKeysPerThread is " + numKeysPerThread);
          }
        }
      }
      setNumPseudoRandomKeys(numKeysPerThread);
      this.numPseudoRandomKeys = numKeysPerThread;
      setCurrentSequenceNumber(startingSequenceNum);
      this.currentSequenceNumber = startingSequenceNum;
      setCurrentNumPseudoRandomKeys(0);
      this.currentNumPseudoRandomKeys = 0;
      //Log.getLogWriter().info("xxx maxSeqNum = " + maxSeqNum + ", startingSequenceNum = " + startingSequenceNum + ", percentage = " + percentage + ", totalNumKeys = " + totalNumKeys
      //     + ", numKeysPerThread = " + numKeysPerThread);
    }
    //Log.getLogWriter().info("xxx numPseudoRandomKeys = " + numPseudoRandomKeys + 
    //    ", currentSequenceNumber = " + currentSequenceNumber + ", currentNumPseudoRandomKeys = " + currentNumPseudoRandomKeys);
    if (currentNumPseudoRandomKeys >= numPseudoRandomKeys) {
      return null;
    }
    //Log.getLogWriter().info("xxx calculating key, currentSequenceNumber is " + currentSequenceNumber + ", ttgid is " + ttgid + ", numthreads is " + this.numThreads);
    long key = this.ttgid + ((currentSequenceNumber - 1) * this.numThreads);
    if (key > maxKeyValue) {
      //Log.getLogWriter().info("xxx calculating (again) key AFTER WRAPPING, currentSequenceNum is 1");
      currentSequenceNumber = 1;
      key = this.ttgid;
    }
    currentNumPseudoRandomKeys++;
    currentSequenceNumber++;
    //Log.getLogWriter().info("xxx returning key " + key);
    return new Long(key);
  }
  
  //----------------------------------------------------------------------------
  //  Terminator support
  //----------------------------------------------------------------------------

  protected boolean executeBatchTerminator() {
    if (timeToExecuteBatchTerminator()) {
      terminateBatch();
      return true;
    }
    return false;
  }
  protected boolean timeToExecuteBatchTerminator() {
    // @todo lises croak if there is no batch terminator
    if (this.batchTerminator != null) {
      Object o = executeTerminator(this.batchTerminator);
      if (((Boolean)o).booleanValue()) {
        return true;
      }
      checkForTxEnd(TERMINATION_NONE);
    }
    return false;
  }
  protected void terminateBatch() {
    registerInterest(INTEREST_LIST, this.interestListBatch);
    checkForTxEnd(TERMINATION_BATCH);
    cacheEndTrim(this.trimIntervals, this.trimInterval);
    updateHydraThreadLocals();
    DistributedSystem.releaseThreadsSockets();
  }
  protected void executeWarmupTerminator() {
    if (timeToExecuteWarmupTerminator()) {
      terminateWarmup();
    }
  }
  protected boolean timeToExecuteWarmupTerminator() {
    if ( this.warmupTerminator != null && ! this.warmedUp ) {
      boolean check = checkFrequency( this.lastWarmupTerminatorTime,
                                      this.warmupTerminatorFrequency );
      if ( check ) {
	Object o = executeTerminator( this.warmupTerminator );
	if ( ( (Boolean) o ).booleanValue() ) {
          return true;
	}
        this.lastWarmupTerminatorTime = System.currentTimeMillis();
      }
    }
    return false;
  }
  protected void terminateWarmup() {
    registerInterest(INTEREST_LIST, this.interestListBatch);
    checkForTxEnd(TERMINATION_WARMUP);
    this.warmedUp = true;
    this.warmupCount = this.count;
    this.warmupTime = System.currentTimeMillis() - this.startTime;
    sync();
    if (this.useTransactions) {
      this.begin();
    }
    cacheStartTrim( this.trimIntervals, this.trimInterval );
  }
  protected void executeTaskTerminator() {
    if (timeToExecuteTaskTerminator()) {
      terminateTask();
    }
  }
  protected boolean timeToExecuteTaskTerminator() {
    if ( this.taskTerminator != null ) {
      boolean check = checkFrequency( this.lastTaskTerminatorTime,
                                      this.taskTerminatorFrequency );
      if ( check ) {
	Object o = executeTerminator( this.taskTerminator );
	if ( ( (Boolean) o ).booleanValue() ) {
          return true;
	}
        this.lastTaskTerminatorTime = System.currentTimeMillis();
      }
    }
    return false;
  }
  protected void terminateTask() {
    if ( this.warmupTerminator != null && ! this.warmedUp ) {
      String s = "Task terminator " + this.taskTerminator
               + " ran before warmup terminator " + this.warmupTerminator;
      throw new HydraConfigException(s);
    }
    registerInterest(INTEREST_LIST, this.interestListBatch);
    checkForTxEnd(TERMINATION_TASK);
    cacheEndTrim( this.trimIntervals, this.trimInterval );
    updateHydraThreadLocals();
    throw new StopSchedulingTaskOnClientOrder();
  }
  protected Method getMethod( String className, String methodName ) {
    if (( methodName == null ) || methodName.equals("null")) {
      return null;
    } else {
      try {
	Class cls = Class.forName( className );
	Class[] prmTypes = new Class[]{ cacheperf.CachePerfClient.class };
	return MethExecutor.getMethod( cls, methodName, prmTypes );
      } catch( ClassNotFoundException e ) {
	throw new HydraConfigException( className + " not found", e );
      } catch( NoSuchMethodException e ) {
	throw new HydraConfigException( className + "." + methodName + "(CachePerfClient) not found", e );
      }
    }
  }
  protected boolean checkFrequency( long lastCheckTime,
                                    CachePerfPrms.Frequency f ) {
    boolean check;
    switch( f.type ) {
      case CachePerfPrms.PER_TASK:
	check = this.count % this.batchSize == 0;
	break;
      case CachePerfPrms.PER_ITERATION:
	switch( f.frequency ) {
	  case 0:
	    check = false;
	    break;
	  case 1:
	    check = true;
	    break;
	  default:
	    check = this.count % f.frequency == 0;
	}
	break;
      case CachePerfPrms.PER_SECOND:
	switch( f.frequency ) {
	  case 0:
	    check = true;
	    break;
	  default:
	    long now = System.currentTimeMillis();
	    return lastCheckTime + (f.frequency * 1000) <= now;
	}
	break;
      default:
	throw new HydraRuntimeException( "Should not happen" );
    }
    return check;
  }
  protected Object executeTerminator( Method m ) {
    try {
      return m.invoke( null, this.terminatorArgs );
    } catch( ClassCastException e ) {
      throw new HydraConfigException( m + " does not return a String", e );
    } catch( IllegalAccessException e ) {
      throw new HydraConfigException( m + " cannot be accessed", e );
    } catch( InvocationTargetException e ) {
      throw new HydraConfigException( m + " cannot be invoked. " + "Exception is " + TestHelper.getStackTrace(e.getTargetException()), e );
    }
  }

  /**
   *  TASK to stop all other vms, then restart them
   */
  public static synchronized void stopOtherVMs() throws ClientVmNotFoundException {
    boolean useShutDownAllMembers = CachePerfPrms.useShutDownAllMembers();
    List<ClientVmInfo> stopVMs = null;
    if (useShutDownAllMembers) { 
      AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
      stopVMs = (List)(StopStartVMs.shutDownAllMembers(adminDS)[0]);
      // this call to hydra shutDownAllMembers calls the admin shutDownAllMembers which
      // disconnects the vms, then hydra shuts them down; when this returns the vms are gone
    } else { // prepare to stop the vms
      List<ClientVmInfo> allVMs = StopStartVMs.getAllVMs();
      if (allVMs.size() == 0) {
        throw new TestException("allVMs is empty; need to add init task for StopStartVMs.StopStart_initTask");
      }
      stopVMs = new ArrayList();
      for (ClientVmInfo info: allVMs) {
        if (info.getClientName().indexOf("locator") >= 0) {
          // skip
        } else if (info.getVmid() == RemoteTestModule.getMyVmid()) {
          // skip
        } else {
           stopVMs.add(info);
        }
      }

      // stop the vms
      List<String> stopModeList = new ArrayList();
      while (stopModeList.size() < stopVMs.size()) {
        stopModeList.add(ClientVmMgr.NiceExit);
      }
      StopStartVMs.stopVMs(stopVMs, stopModeList);
    }

    // restart to cause recovery (optional)
    if (CachePerfPrms.restartVMs()) {
      StopStartVMs.startVMs(stopVMs);
    }
  }

  //----------------------------------------------------------------------------
  //  Trim support
  //----------------------------------------------------------------------------

  /**
   *  Caches the start of a named trim interval for this thread.  Use
   *  {@link #reportTrimIntervalsTask} to report cached trim intervals.  This
   *  method can be invoked multiple times, but only the latest time will be
   *  cached.
   */
  protected void cacheStartTrim( Map intervals, int name ) {
    TrimInterval interval = getTrimInterval( intervals, name );
    TrimInterval mainInterval = getTrimInterval(intervals, OPS);
    if (interval != null || mainInterval != null) {
      long now = System.currentTimeMillis();
      if (interval != null) {
        interval.setStart(now);
      }
      if (mainInterval != null) {
        mainInterval.setStart(now);
      }
    }
  }
  
  /**
   *  Caches the end of a named trim interval for this thread.  Use
   *  {@link #reportTrimIntervalsTask} to report cached trim intervals.  This
   *  method can be invoked multiple times, but only the latest time will be
   *  cached.
   */
  protected void cacheEndTrim( Map intervals, int name ) {
    TrimInterval interval = getTrimInterval( intervals, name );
    TrimInterval mainInterval = getTrimInterval(intervals, OPS);
    if (interval != null || mainInterval != null) {
      long now = System.currentTimeMillis();
      if (interval != null) {
        interval.setEnd(now);
      }
      if (mainInterval != null) {
        mainInterval.setEnd(now);
      }
    }
  }
  /**
   *  Gets the trim interval with the given name.
   */
  protected TrimInterval getTrimInterval( Map intervals, int name ) {
    if (name != -1) {
      return (TrimInterval) intervals.get( nameFor( name ) );
    }
    return null; // interval is unspecified
  }

  //----------------------------------------------------------------------------
  //  Synchronization
  //----------------------------------------------------------------------------

  /**
   *  Synchronizes clients across vms before they start their timing run on a
   *  task.  Waits for all threads in all thread groups for the client's current
   *  task to meet at the barrier.  This method should be called only once per
   *  client per task selector.  For eligible tasks, see
   *  {@link cacheperf.TaskSyncBlackboard}.
   */
  protected void sync() {
    log().info( "syncing" );
    // @todo lises catch inadvertent repeat call for a given task
    // @todo lises include receiver, with underscores replacing dots
    Vector threadGroupNames = this.task.getThreadGroupNames();
    for ( Iterator i = threadGroupNames.iterator(); i.hasNext(); ) {

      // @todo take out of loop and use numThreads() method
      // find out how many threads are on tap for this task
      String threadGroupName = (String) i.next();
      HydraThreadGroup threadGroup = tc().getThreadGroup( threadGroupName );
      int totalThreads = threadGroup.getTotalThreads();

      // get the counter for this task
      SharedCounters counters = TaskSyncBlackboard.getInstance().getSharedCounters();
      String fieldName = null;
      Field field = null;
      try {
        fieldName = this.task.getSelector();
        field = TaskSyncBlackboard.class.getDeclaredField( fieldName );
      } catch( NoSuchFieldException e ) {
        throw new CachePerfException( "Need to add field " + fieldName + " to cacheperf.TaskSyncBlackboard", e );
      }
      int counter = -1;
      try {
        counter = ( (Integer) field.get( null ) ).intValue();
      } catch( IllegalAccessException e ) {
        throw new CachePerfException( "Unable to access field " + fieldName + " in TaskSyncBlackboard", e );
      }

      // poll until the expected number of threads meet at the barrier
      if ( log().fineEnabled() ) {
        log().fine( "Waiting for " + totalThreads + " threads"
	     + " at barrier for " + fieldName + " (" + counter + ")" );
      }
      long threadCount = counters.incrementAndRead( counter );
      while ( threadCount < totalThreads ) {
        MasterController.sleepForMs( 1000 );
        threadCount = counters.read( counter );
      }
    }
    if (this.syncSleepMs > 0) {
       log().info( "Sleeping after sync for " + syncSleepMs + " millis" );
       MasterController.sleepForMs( syncSleepMs );
    }
    log().info( "done syncing" );
  }
  
  public static void sleepTask() {
    MasterController.sleepForMs(CachePerfPrms.getSleepMs());
  }

  public static void dumpHeapTask() {
    CachePerfClient c = new CachePerfClient();
    c.initLocalVariables(-1);
    c.dumpHeap();
  }

  private void dumpHeap() {
    if (this.ttgid == 0) {
      log().severe(ProcessMgr.fgexec("sh heapdumprun.sh", 600));
    }
  }

  public static void sleepTaskWithTrim() {
    CachePerfClient c = new CachePerfClient();
    c.initialize( SLEEP );
    c.cacheStartTrim( c.trimIntervals, c.trimInterval );
    MasterController.sleepForMs(CachePerfPrms.getSleepMs());
    c.cacheEndTrim( c.trimIntervals, c.trimInterval );
    c.updateHydraThreadLocals();
  }
  
  public static void clearTask() {
    CachePerfClient c = new CachePerfClient();
    c.initialize();
    if (c.cache instanceof GemFireCacheTestImpl) {
      Region r = ((GemFireCacheTestImpl)c.cache).getRegion();
      r.clear();
    } else {
      String s = "clear() not supported for " + c.cache;
      throw new HydraConfigException(s);
    }
  }

  public static void startJProbeTask() {
  }

  public static void stopJProbeTask() {
  }

  //----------------------------------------------------------------------------
  //  Validation
  //----------------------------------------------------------------------------

  /**
   *  Validates that an indexed object has the right content.
   */
  public void validate( int index, Object obj ) {
    if ( log().fineEnabled() ) {
      if ( obj == null ) {
        log().fine( "Validating null:" + index + "=" + obj );
      } else {
        log().fine( "Validating " + obj.getClass().getName() + ":" + index + "=" + obj );
      }
    }

    ObjectHelper.validate( index, obj );

    if ( log().fineEnabled() ) {
      if ( obj == null ) {
        log().fine( "Validated null:" + index + "=" + obj );
      } else {
        log().fine( "Validated " + obj.getClass().getName() + ":" + index + "=" + obj );
      }
    }
  }

  //----------------------------------------------------------------------------
  //  Initialization
  //----------------------------------------------------------------------------

  // fixed task-local variables
  public int trimInterval = -1;
  public int jid;
  public int tid;
  public int tgid;
  public int ttgid;
  public int sttgid;
  public int numWanSites;
  public int numThreads;
  public int iterationsSinceTxEnd;
  public List interestListBatch;

  // other task-local variables
  public int batchCount;      // iterations since this batch started
  public long batchStartTime; // time when this batch started
  public boolean hadFullSizeTx; // whether a transaction of size txSize
                                // occurred since this batch started

  // fixed task-local parameters
  public int trimIterations;
  public int workIterations;
  public int numOperations;
  public int batchSize;
  public int trimSeconds;
  public int workSeconds;
  public int batchSeconds;
  public int syncSleepMs;
  public int txSize;
  public int commitPercentage;
  public int keyAllocation;
  public int keyAllocationChunkSize;
  public int maxKeys;
  public int optimizationCount;
  public boolean isMainWorkload;
  public boolean allowNulls;
  public boolean validateObjects;
  public boolean invalidateAfterGet;
  public boolean invalidateLocally;
  public boolean destroyAfterGet;
  public boolean destroyLocally;
  public boolean sleepBeforeOp;
  public boolean useTransactions;
  public boolean allowConflicts;
  public boolean registerDurableInterest;
  public Method batchTerminator;
  public Method warmupTerminator;
  public Method taskTerminator;
  public Object[] terminatorArgs;
  public CachePerfPrms.Frequency warmupTerminatorFrequency;
  public CachePerfPrms.Frequency taskTerminatorFrequency;

  protected void initialize() {
    initialize( -1 );
  }
  protected void initialize( int trimInterval ) {
    initLocalVariables( trimInterval );
    initLocalParameters();
    initHydraThreadLocals();
    initBatchVariables();
  }
  protected void initLocalVariables( int trimInterval ) {
    this.trimInterval = trimInterval;
    this.jid = jid();
    this.tid = tid();
    this.tgid = tgid();
    this.ttgid = ttgid();
    this.sttgid = sttgid();
    this.numWanSites = numWanSites();
    this.numThreads = numThreads();
    com.gemstone.gemfire.internal.Assert.assertTrue( this.numThreads > 0, "Have " + this.numThreads + " threads" );
    this.iterationsSinceTxEnd = 0;
  }
  protected void initBatchVariables() {
    this.batchCount = 0;
    this.batchStartTime = System.currentTimeMillis();
    this.hadFullSizeTx = false;
  }
  protected void initLocalParameters() {
    this.isMainWorkload = CachePerfPrms.isMainWorkload();
    this.trimIterations = CachePerfPrms.getTrimIterations();
    this.workIterations = CachePerfPrms.getWorkIterations();
    this.numOperations = CachePerfPrms.getNumOperations();
    this.batchSize = CachePerfPrms.getBatchSize();
    this.trimSeconds = CachePerfPrms.getTrimSeconds();
    this.workSeconds = CachePerfPrms.getWorkSeconds();
    this.batchSeconds = CachePerfPrms.getBatchSeconds();
    this.syncSleepMs = CachePerfPrms.syncSleepMs();
    this.commitPercentage = CachePerfPrms.getCommitPercentage();
    this.txSize = CachePerfPrms.getTxSize();
    this.keyAllocation = CachePerfPrms.getKeyAllocation();
    this.keyAllocationChunkSize = CachePerfPrms.getKeyAllocationChunkSize();
    this.maxKeys = CachePerfPrms.getMaxKeys();
    if ( log().fineEnabled() ) {
      if ( this.workIterations != -1 ) {
        log().fine( "trimIterations=" + this.trimIterations
                 + " workIterations=" + this.workIterations
                 + " batchSize=" + this.batchSize );
      } else if ( this.workSeconds != -1 ) {
        log().fine( "trimSeconds=" + this.trimSeconds
                 + " workSeconds=" + this.workSeconds
                 + " batchSeconds=" + this.batchSeconds );
      }
      log().fine( "maxKeys=" + this.maxKeys );
    }
    this.optimizationCount = CachePerfPrms.getOptimizationCount();
    this.allowNulls = CachePerfPrms.allowNulls();
    this.validateObjects = CachePerfPrms.validateObjects();
    this.invalidateAfterGet = CachePerfPrms.invalidateAfterGet();
    this.invalidateLocally = CachePerfPrms.invalidateLocally();
    this.destroyAfterGet = CachePerfPrms.destroyAfterGet();
    this.destroyLocally = CachePerfPrms.destroyLocally();
    this.sleepBeforeOp = CachePerfPrms.sleepBeforeOp();
    this.useTransactions = CachePerfPrms.useTransactions();
    this.allowConflicts = CachePerfPrms.allowConflicts();
    this.registerDurableInterest = CachePerfPrms.registerDurableInterest();

    this.batchTerminator  = getMethod( CachePerfPrms.getBatchTerminatorClass(),
                                       CachePerfPrms.getBatchTerminatorMethod() );
    this.warmupTerminator = getMethod( CachePerfPrms.getWarmupTerminatorClass(),
                                       CachePerfPrms.getWarmupTerminatorMethod() );
    this.taskTerminator   = getMethod( CachePerfPrms.getTaskTerminatorClass(),
                                       CachePerfPrms.getTaskTerminatorMethod() );

    this.terminatorArgs = new Object[]{ this };

    this.warmupTerminatorFrequency = CachePerfPrms.getWarmupTerminatorFrequency();
    this.taskTerminatorFrequency   = CachePerfPrms.getTaskTerminatorFrequency();
  }

  //----------------------------------------------------------------------------
  //  Hydra thread locals and their instance field counterparts
  //----------------------------------------------------------------------------

  public TestTask task;
  public CachePerfStats statistics;
  public HistogramStats histogram;
  public Map trimIntervals;
  public DistCache cache;
  public CacheTransactionManager tm; 
  public GsRandom rng;
  public Random keyGenerator;

  public boolean warmedUp; // whether this task has completed warmup
  public int warmupCount;  // iterations it took to warm up this task
  public long warmupTime;  // time it took to warm up this task, in ms
  public int count;        // iterations since this task started
  public long startTime;   // time when this task started
  public int keyCount;
  public int currentKey;
  public String keyType;
  public long lastWarmupTerminatorTime; // time warmup terminator last ran, in ms
  public long lastTaskTerminatorTime; // time task terminator last ran, in ms
  public long numPseudoRandomKeys;  // used for pseudo-random unique keys
  public long currentSequenceNumber;  // used for pseudo-random unique keys
  public long currentNumPseudoRandomKeys; // used for pseudo-random unique keys

  private static HydraThreadLocal localtask = new HydraThreadLocal();
  private static HydraThreadLocal localstatistics = new HydraThreadLocal();
  private static HydraThreadLocal localhistogram = new HydraThreadLocal();
  private static HydraThreadLocal localtrimintervals = new HydraThreadLocal();
  private static HydraThreadLocal localcache = new HydraThreadLocal();
  private static HydraThreadLocal localrng = new HydraThreadLocal();
  private static HydraThreadLocal localKeyGenerator = new HydraThreadLocal();
  private static HydraThreadLocal localwarmedup = new HydraThreadLocal();
  private static HydraThreadLocal localwarmupcount = new HydraThreadLocal();
  private static HydraThreadLocal localwarmuptime = new HydraThreadLocal();
  private static HydraThreadLocal localcount = new HydraThreadLocal();
  private static HydraThreadLocal localkeycount = new HydraThreadLocal();
  private static HydraThreadLocal localcurrentkey = new HydraThreadLocal();
  private static HydraThreadLocal localkeytype = new HydraThreadLocal();
  private static HydraThreadLocal locallastwarmupterminatortime = new HydraThreadLocal();
  private static HydraThreadLocal locallasttaskterminatortime = new HydraThreadLocal();
  private static HydraThreadLocal localstarttime = new HydraThreadLocal();
  private static HydraThreadLocal localtm = new HydraThreadLocal();
  // the following are used for pseudo-random unique keys
  private static HydraThreadLocal localstartingsequencenumber = new HydraThreadLocal();
  private static HydraThreadLocal localnumpseudorandomkeys = new HydraThreadLocal();
  private static HydraThreadLocal localcurrentsequencenum = new HydraThreadLocal();
  private static HydraThreadLocal localcurrentnumpseudorandomkeys = new HydraThreadLocal();

  protected void initHydraThreadLocals() {
    // read-only
    this.task = getTask();
    this.trimIntervals = getTrimIntervals();
    if ( this.trimInterval != -1 ) {
      String key = nameFor( this.trimInterval );
      if ( ! this.trimIntervals.containsKey( key ) ) {
        this.trimIntervals.put( key, new TrimInterval() );
      }
    }
    if (this.isMainWorkload) {
      String key = nameFor(OPS);
      if (!this.trimIntervals.containsKey(key)) {
        this.trimIntervals.put(key, new TrimInterval());
      }
    }
    this.rng = getRNG();
    this.keyGenerator = getKeyGenerator();

    // updated in special tasks
    this.statistics = getStatistics();
    this.histogram = getHistogram(); // derived
    this.cache = getCache();
    this.tm = getTxMgr();

    // updated in tasks
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    if (DistCachePrms.getCacheVendor() == DistCachePrms.GEMFIRE) {
      EdgeHelper.restoreThreadLocalConnections();
    }
    this.warmedUp = getWarmedUp();
    this.warmupCount = getWarmUpCount();
    this.warmupTime = getWarmUpTime();
    this.count = getCount();
    this.keyCount = getKeyCount();
    this.currentKey = getCurrentKey();
    this.keyType = getKeyType();
    this.lastWarmupTerminatorTime = getLastWarmupTerminatorTime();
    this.lastTaskTerminatorTime = getLastTaskTerminatorTime();
    this.startTime = getStartTime();
    this.numPseudoRandomKeys = getNumPseudoRandomKeys();
    this.currentSequenceNumber = getCurrentSequenceNumber();
    this.currentNumPseudoRandomKeys = getCurrentNumPseudoRandomKeys();
  }
  protected void resetHydraThreadLocals( boolean resetKeys ) {
    localwarmedup.set( null );
    localwarmupcount.set( null );
    localwarmuptime.set( null );
    localcount.set( null );
    if ( resetKeys ) {
      localkeycount.set( null );
      localcurrentkey.set( null );
    }
    // localkeytype.set( null ); // do not reset! want one key type per test
    locallastwarmupterminatortime.set( null );
    locallasttaskterminatortime.set( null );
    localstarttime.set( null );
    HistogramStats h = (HistogramStats)localhistogram.get();
    if (h != null) {
      h.close();
      localhistogram.set(null);
    }
  }
  protected void updateHydraThreadLocals() {
    setStatistics( this.statistics );
    setHistogram( this.histogram );
    setCache( this.cache );
    setTxMgr( this.tm );
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    if (DistCachePrms.getCacheVendor() == DistCachePrms.GEMFIRE) {
      EdgeHelper.saveThreadLocalConnections();
    }
    setWarmedUp( this.warmedUp );
    setWarmUpCount( this.warmupCount );
    setWarmUpTime( this.warmupTime );
    setCount( this.count );
    setKeyCount( this.keyCount );
    setCurrentKey( this.currentKey );
    setKeyType( this.keyType );
    setLastWarmupTerminatorTime( this.lastWarmupTerminatorTime );
    setLastTaskTerminatorTime( this.lastTaskTerminatorTime );
    setStartTime( this.startTime );
    setCurrentSequenceNumber( this.currentSequenceNumber );
    setCurrentNumPseudoRandomKeys( this.currentNumPseudoRandomKeys );
  }

  /**
   *  Gets the current task.
   */
  protected TestTask getTask() {
    TestTask oldTask = (TestTask) localtask.get();
    TestTask newTask = RemoteTestModule.getCurrentThread().getCurrentTask();
    if ( oldTask == null ) {
      localtask.set( newTask );
    } else if ( ! oldTask.equals( newTask ) ) {
      localtask.set( newTask );
      boolean resetKeysAfter = CachePerfPrms.resetKeysAfterTaskEnds( oldTask );
      boolean resetKeysBefore = CachePerfPrms.resetKeysBeforeTaskStarts( newTask );
      resetHydraThreadLocals( resetKeysAfter || resetKeysBefore );
    }
    return newTask;
  }
  /**
   *  Gets the per-thread cache performance statistics instance.
   */
  protected CachePerfStats getStatistics() {
    CachePerfStats stats = (CachePerfStats) localstatistics.get();
    return stats;
  }
  /**
   *  Sets the per-thread cache performance statistics instance.
   */
  protected void setStatistics( CachePerfStats stats ) {
    localstatistics.set( stats );
  }
  /**
   * Gets the per-thread cache histogram statistics instance.
   */
  protected HistogramStats getHistogram() {
    HistogramStats h = (HistogramStats)localhistogram.get();
    if (h == null && HistogramStatsPrms.enable()) {
      if (this.trimInterval != -1) {
        String instanceName = nameFor(this.trimInterval);
        log().info("Opening histogram for " + instanceName);
        h = HistogramStats.getInstance(instanceName);
        log().info("Opened histogram for " + instanceName);
      }
      localhistogram.set(h);
    }
    return h;
  }
  /**
   *  Sets the per-thread cache histogram instance.
   */
  protected void setHistogram(HistogramStats h) {
    localhistogram.set(h);
  }
  /**
   *  Gets the map of trim intervals.
   */
  protected Map getTrimIntervals() {
    Map trims = (Map) localtrimintervals.get();
    if ( trims == null ) {
      trims = new HashMap();
      localtrimintervals.set( trims );
    }
    return trims;
  }
  /**
   *  Gets the per-thread cache wrapper instance.
   */
  protected DistCache getCache() {
    DistCache c = (DistCache) localcache.get();
    return c;
  }
  /**
   *  Sets the per-thread cache wrapper instance.
   */
  protected void setCache( DistCache c ) {
    localcache.set( c );
  }
  /**
   *  Gets the per-thread transactionMgr instance
   */
  protected CacheTransactionManager getTxMgr() {
    CacheTransactionManager tm = (CacheTransactionManager) localtm.get();
    return tm;
  }
  /**
   *  Sets the per-thread CacheTransactionManager instance
   */
  protected void setTxMgr( CacheTransactionManager tm ) {
    localtm.set( tm );
  }
  /**
   *  Gets whether the thread is warmed up.
   */
  protected boolean getWarmedUp() {
    Boolean b = (Boolean) localwarmedup.get();
    if ( b == null ) {
      b = Boolean.FALSE;
      localwarmedup.set( b );
    }
    return b.booleanValue();
  }
  /**
   *  Sets whether the thread is warmed up.
   */
  protected void setWarmedUp( boolean b ) {
    localwarmedup.set(Boolean.valueOf(b));
  }
  /**
   *  Gets the number of iterations executed during warmup.
   */
  protected int getWarmUpCount() {
    Integer i = (Integer) localwarmupcount.get();
    if ( i == null ) {
      i = new Integer(-1);
      localwarmupcount.set( i );
    }
    return i.intValue();
  }
  /**
   *  Sets the number of iterations executed during warmup.
   */
  protected void setWarmUpCount( int i ) {
    localwarmupcount.set( new Integer( i ) );
  }
  /**
   *  Gets the time passed during warmup.
   */
  protected long getWarmUpTime() {
    Long t = (Long) localwarmuptime.get();
    if ( t == null ) {
      t = new Long( System.currentTimeMillis() );
      localwarmuptime.set( t );
    }
    return t.longValue();
  }
  /**
   *  Sets the time passed during warmup.
   */
  protected void setWarmUpTime( long i ) {
    localwarmuptime.set( new Long( i ) );
  }
  /**
   *  Gets the count for a thread's workload.
   */
  protected int getCount() {
    Integer n = (Integer) localcount.get();
    if ( n == null ) {
      n = new Integer(0);
      localcount.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the count for a thread's workload.
   */
  protected void setCount( int n ) {
    localcount.set( new Integer( n ) );
  }
  /**
   *  Gets the key count for a thread's workload.
   */
  protected int getKeyCount() {
    Integer n = (Integer) localkeycount.get();
    if ( n == null ) {
      n = new Integer(0);
      localkeycount.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the key count for a thread's workload.
   */
  protected void setKeyCount( int n ) {
    localkeycount.set( new Integer( n ) );
  }
  /**
   *  Gets the current key for a thread's workload.
   */
  protected int getCurrentKey() {
    Integer n = (Integer) localcurrentkey.get();
    if ( n == null ) {
      n = new Integer(-1);
      localcurrentkey.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the current key for a thread's workload.
   */
  protected void setCurrentKey( int n ) {
    localcurrentkey.set( new Integer( n ) );
  }
  /**
   *  Gets the key type for a thread's workload.
   */
  protected String getKeyType() {
    String t = (String) localkeytype.get();
    if ( t == null ) {
      t = CachePerfPrms.getKeyType();
      localkeytype.set( t );
    }
    return t;
  }
  /**
   *  Sets the key type for a thread's workload.
   */
  protected void setKeyType( String t ) {
    localkeytype.set(t);
  }
  /**
   *  Gets the last warmup terminator time for a thread's workload.
   */
  protected long getLastWarmupTerminatorTime() {
    Long t = (Long) locallastwarmupterminatortime.get();
    if ( t == null ) {
      t = new Long( System.currentTimeMillis() );
      locallastwarmupterminatortime.set( t );
    }
    return t.longValue();
  }
  /**
   *  Sets the last warmup terminator time for a thread's workload.
   */
  protected void setLastWarmupTerminatorTime( long t ) {
    locallastwarmupterminatortime.set( new Long( t ) );
  }
  /**
   *  Gets the last task terminator time for a thread's workload.
   */
  protected long getLastTaskTerminatorTime() {
    Long t = (Long) locallasttaskterminatortime.get();
    if ( t == null ) {
      t = new Long( System.currentTimeMillis() );
      locallasttaskterminatortime.set( t );
    }
    return t.longValue();
  }
  /**
   *  Sets the last task terminator time for a thread's workload.
   */
  protected void setLastTaskTerminatorTime( long t ) {
    locallasttaskterminatortime.set( new Long( t ) );
  }
  /**
   *  Gets the start time for a thread's workload.
   */
  protected long getStartTime() {
    Long t = (Long) localstarttime.get();
    if ( t == null ) {
      t = new Long( System.currentTimeMillis() );
      localstarttime.set( t );
    }
    return t.longValue();
  }
  /**
   *  Sets the start time for a thread's workload.
   */
  protected void setStartTime( long t ) {
    localstarttime.set( new Long( t ) );
  }
  /**
   *  Gets the random number generator.
   */
  protected GsRandom getRNG() {
    GsRandom r = (GsRandom) localrng.get();
    if ( r == null ) {
      r = tc().getParameters().getRandGen();
      localrng.set( r );
    }
    return r;
  }
  /**
   * Gets the key generator for zipfian and scrambledZipfian distribution
   */
  protected Random getKeyGenerator() {
    Random keyGenerator = (Random) localKeyGenerator.get();
    if (keyGenerator == null) {
      if (this.keyAllocation == CachePerfPrms.ZIPFIAN) {
        keyGenerator = new ZipfianGenerator(this.maxKeys);
      } else if (this.keyAllocation == CachePerfPrms.SCRAMBLED_ZIPFIAN) {
        keyGenerator = new ScrambledZipfianGenerator(this.maxKeys);
      }
      localKeyGenerator.set(keyGenerator);
    }
    return keyGenerator;
  }
  /**
   *  Gets the starting sequence number for pseudo-random unique keys
   */
  protected long getStartingSequenceNumber() {
    Long n = (Long) localstartingsequencenumber.get();
    if ( n == null ) {
      n = new Long(-1);
      localstartingsequencenumber.set( n );
    }
    return n.longValue();
  }
  /**
   *  Sets the starting sequence number for pseudo-random unique keys
   */
  protected void setStartingSequenceNumber( long i ) {
    localstartingsequencenumber.set( new Long( i ) );
  }
  /**
   *  Gets the desired number of pseudo-random ops for this thread
   */
  protected long getNumPseudoRandomKeys() {
    Long n = (Long) localnumpseudorandomkeys.get();
    if ( n == null ) {
      n = new Long(-1);
      localnumpseudorandomkeys.set( n );
    }
    return n.longValue();
  }
  /**
   *  Sets the desired number of pseudo-random keys for this thread.
   */
  protected void setNumPseudoRandomKeys( long i ) {
    localnumpseudorandomkeys.set( new Long( i ) );
  }
  /**
   *  Gets the current sequence number for pseudo-random ops.
   */
  protected long getCurrentSequenceNumber() {
    Long n = (Long) localcurrentsequencenum.get();
    if ( n == null ) {
      n = new Long(-1);
      localcurrentsequencenum.set( n );
    }
    return n.longValue();
  }
  /**
   *  Sets current sequence number for pseudo-random ops.
   */
  protected void setCurrentSequenceNumber( long i ) {
    localcurrentsequencenum.set( new Long( i ) );
  }
  
  /**
   *  Gets the current number of pseudo-random keys generated by this thread.
   */
  protected long getCurrentNumPseudoRandomKeys() {
    Long n = (Long) localcurrentnumpseudorandomkeys.get();
    if ( n == null ) {
      n = new Long(-1);
      localcurrentnumpseudorandomkeys.set( n );
    }
    return n.longValue();
  }
  /**
   *  Sets current number of pseudo-random keys generated by this thread.
   */
  protected void setCurrentNumPseudoRandomKeys( long i ) {
    localcurrentnumpseudorandomkeys.set( new Long( i ) );
  }
  //----------------------------------------------------------------------------
  //  Miscellaneous convenience methods
  //----------------------------------------------------------------------------

  /**
   * Gets the client's JVM thread id. Exactly one logical hydra thread in the
   * this JVM has a given jid. The jids are numbered consecutively starting
   * from 0.  Assumes that all threads in this JVM are in the same threadgroup.
   */
  protected int jid() {
    if (log().fineEnabled()) {
      log().fine("jid=" + tid() % RemoteTestModule.getMyNumThreads());
    }
    return tid() % RemoteTestModule.getMyNumThreads();
  }

  /**
   * Gets the client's hydra thread id. Exactly one logical hydra thread
   * in the whole test has a given tid. The tids are numbered consecutively
   * starting from 0.
   */
  protected int tid() {
    return RemoteTestModule.getCurrentThread().getThreadId();
  }

  /**
   * Gets the client's hydra threadgroup id. Exactly one logical hydra thread
   * in the threadgroup containing this thread has a given tgid. The tgids are
   * numbered consecutively starting from 0.
   */
  protected int tgid() {
    return RemoteTestModule.getCurrentThread().getThreadGroupId();
  }

  /**
   * Gets the client's hydra task threadgroup id.  Exactly one logical hydra
   * thread in all of the threadgroups assigned to the current task has a
   * given ttgid.  The ttgids are numbered consecutively starting from 0.
   */
  protected int ttgid() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    return task.getTaskThreadGroupId(tgname, tgid());
  }

  /**
   * Gets the client's hydra site-specific task threadgroup id.  Exactly one
   * logical hydra thread in all of the threads in the threadgroups assigned to
   * the current task threads that are in the wan site of this thread has a
   * given sttgid.  The sttgids for each wan site are numbers consecutively
   * starting from 0.
   * <p>
   * Assumes client names are of the form *_site_*, where site is the site
   * number.  If no site is found, then this method returns the {@link #ttgid}.
   */
  protected int sttgid() {
    List<List<String>> mappings = RemoteTestModule.getClientMapping();
    if (mappings == null) {
      String s = "Client mapping not found";
      throw new HydraRuntimeException(s);
    }

    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    String arr[] = clientName.split("_");
    if (arr.length != 3) {
      if (log().fineEnabled()) {
        log().fine("sttgid: single site, using ttgid=" + ttgid());
      }
      return ttgid(); // assume single site
    }
    String localsite = arr[1];
    String localthreadname = Thread.currentThread().getName();
    Vector threadgroups = RemoteTestModule.getCurrentThread().getCurrentTask()
                                          .getThreadGroupNames();
    if (log().fineEnabled()) {
      log().fine("sttgid: matching " + localthreadname + " for site="
                + localsite + " from threadgroups=" + threadgroups);
    }

    int count = 0;
    for (List<String> mapping : mappings) {
      String threadgroup = mapping.get(0);
      String threadname = mapping.get(1);
      if (log().fineEnabled()) {
        log().fine("sttgid: candidate threadgroup=" + threadgroup
                  + " threadname= " + threadname);
      }
      if (threadgroups.contains(threadgroup)) {
        // candidate thread is in the task thread group
        String carr[] = threadname.split("_");
        if (carr.length == 8) {
          if (localthreadname.startsWith(threadname)) {
            // this is the calling thread
            if (log().fineEnabled()) {
              log().fine("sttgid: returning match for " + localthreadname
                        + " as " + count);
            }
            return count;
          }
          String site = carr[5];
          if (site.equals(localsite)) {
            // candidate thread is in this site
            ++count;
            if (log().fineEnabled()) {
              log().fine("sttgid: counting match " + count + "=" + threadname);
            }
          } else {
            if (log().fineEnabled()) {
              log().fine("sttgid: candidate rejected=" + mapping
                        + " invalid site=" + site);
            }
          }
        } else {
          String s = "Unexpected array size from " + threadname;
          throw new HydraRuntimeException(s);
        }
      } else {
        if (log().fineEnabled()) {
          log().fine("sttgid: candidate rejected=" + mapping
                    + " invalid threadgroup=" + threadgroup);
        }
      }
    }
    String s = "Should not happen";
    throw new HydraInternalException(s);
  }

  /**
   * Gets the number of wan sites in the test.
   * <p>
   * Reads {@link CachePerfPrms#numWanSites}. If that returns 0 (not set),
   * assumes client names are of the form *_site_*, where site is the site
   * number.  If no site is found, then this method returns 1.
   */
  protected int numWanSites() {
    int maxSite = CachePerfPrms.getNumWanSites();
    if (maxSite != 0) {
      return maxSite;
    }
    // else compute it based on the client name
    Vector clientNames = TestConfig.getInstance().getClientNames();
    for (Iterator i = clientNames.iterator(); i.hasNext();) {
      String clientName = (String)i.next();
      String arr[] = clientName.split("_");
      if (arr.length != 3) {
        return 1; // assume single site
      }
      int site;
      try {
        site = Integer.parseInt(arr[1]);
      } catch (NumberFormatException e) {
        String s = clientName
                 + " is not in the form <name>_<wanSiteNumber>_<itemNumber>";
        throw new HydraRuntimeException(s, e);
      }
      maxSite = Math.max(site, maxSite);
    }
    if (maxSite == 0) {
      String s = "Should not happen";
      throw new HydraInternalException(s);
    }
    return maxSite;
  }

  /**
   *  Gets the total number of threads eligible to run the current task.
   */
  public static int numThreads() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    int t = task.getTotalThreads();
    return t;
  }

  /**
   *  Gets the log writer.
   */
  protected static LogWriter log() {
    return Log.getLogWriter();
  }

  /**
   *  Gets the test config.
   */
  protected TestConfig tc() {
    if ( this.tc == null ) {
      this.tc = TestConfig.getInstance();
    }
    return this.tc;
  }
  private TestConfig tc;

  /**
   *  Check for ending the transaction.
   *  @param terminationType the type of termination to handle.
   */
  protected void checkForTxEnd(int terminationType) {
    if (this.useTransactions) {
      switch (terminationType) {
        case TERMINATION_NONE:
          if (this.iterationsSinceTxEnd >= this.txSize) {
            this.iterationsSinceTxEnd = 0;
            this.endTx();
            this.begin();
          }
          this.hadFullSizeTx = true;
          break;
        case TERMINATION_BATCH:
          if (!this.hadFullSizeTx) {
            String t = "Batch ended before a transaction using " + txSize
                     + " operations had occurred.  Reconfigure either the"
                     + " batch granularity or the transaction size.";
            throw new HydraConfigException(t);
          }
          this.iterationsSinceTxEnd = 0;
          this.endTx();
          this.hadFullSizeTx = false;
          break;
        case TERMINATION_WARMUP:
          // leave this counter alone to do remainder on next round
          //this.iterationsSinceTxEnd = 0;
          this.endTx();
          // begin takes place after sync()
          break;
        case TERMINATION_TASK:
          this.iterationsSinceTxEnd = 0;
          this.endTx();
          break;
        default:
          String s = "Illegal termination type: " + terminationType;
          throw new HydraInternalException(s);
      }
    }
  }
  private static final int TERMINATION_NONE   = 0;
  private static final int TERMINATION_BATCH  = 1;
  private static final int TERMINATION_WARMUP = 2;
  private static final int TERMINATION_TASK   = 3;

  protected void begin() {
//     Log.getLogWriter().info("txBoundary: Begin");
     this.tm.begin();
  }

  protected void endTx() {
    int n = this.rng.nextInt( 1, 100 );
    if (n <= this.commitPercentage) {
      try {
//        Log.getLogWriter().info("txBoundary: Commit");
        this.tm.commit();
      } catch (RuntimeException e) {
        if (e.getClass().getName().contains("ConflictException")) {
          if (!allowConflicts) {
            throw new TestException("Detected ConflictException " + e, e);
          }
        }
        else {
          throw e;
        }
      }
    } else {
//      Log.getLogWriter().info("txBoundary: Rollback");
      tm.rollback();
    }
  }
}

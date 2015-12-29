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

package cacheperf.poc.useCase3_2;

import cacheperf.CachePerfException;
import cacheperf.poc.useCase3_2.UseCase3Prms.CacheOp;
import cacheperf.poc.useCase3_2.UseCase3Prms.ClientName;
import cacheperf.poc.useCase3_2.UseCase3Prms.RegionName;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.control.*;
import com.gemstone.gemfire.internal.NanoTimer;

import hydra.*;
import hydra.blackboard.*;
import java.lang.reflect.*;
import java.util.*;
import objects.*;
import perffmwk.*;
import util.*;

/**
 * Client used to model a specific UseCase3 use case from July 2010.
 */
public class UseCase3Client {

  private static LogWriter log = Log.getLogWriter();
  private static final String WORKLOAD_START_TIME = "workload_start_time";
  private static final InterestResultPolicy policy = InterestResultPolicy.NONE;

  private static Cache cache;

//------------------------------------------------------------------------------
// TASKS

  /**
   * INITTASK to create and start a locator and connect it to an admin
   * distributed system.
   */
  public static void createAndStartLocatorTask() {
    DistributedSystemHelper.createLocator();
    int parties = numThreads();
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(parties, "locator");
    log.info("Waiting for " + parties + " to meet at locator barrier");
    barrier.await();
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   * INITTASK to create and start an agent and connect it to an admin
   * distributed system.
   */
  public static void createAndStartAgentTask() {
    String agentConfig = ConfigPrms.getAgentConfig();
    AgentHelper.startConnectedAgent(agentConfig);
  }

  /**
   * INITTASK to connect to the distributed system.
   */
  public static void connectTask() {
    DistributedSystemHelper.connect();
  }

  /**
   * INITTASK to disconnect from the distributed system.
   */
  public static void disconnectTask() {
    DistributedSystemHelper.disconnect();
  }

  /**
   * INITTASK to register the cache performance and clock skew statistics
   * objects.
   */
  public static void openStatisticsTask() {
    UseCase3Client c = new UseCase3Client();
    c.initHydraThreadLocals();
    c.openStatistics();
    c.updateHydraThreadLocals();
  }
  private void openStatistics() {
    if ( this.statistics == null ) {
      this.statistics = UseCase3Stats.getInstance();
      incVMCount(this.statistics);
      RemoteTestModule.openClockSkewStatistics();
    }
  }
  private static synchronized void incVMCount(UseCase3Stats s) {
    if (!VMCounted) {
      s.incVMCount();
      VMCounted = true;
    }
  }
  private static boolean VMCounted = false;

  /**
   * INITTASK to unregister the cache performance and clock skew statistics
   * objects.
   */
  public static void closeStatisticsTask() {
    UseCase3Client c = new UseCase3Client();
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
   * INITTASK to create the cache.
   */
  public static synchronized void createCacheTask() {
    String cacheConfig = ConfigPrms.getCacheConfig();
    cache = CacheHelper.createCache(cacheConfig);
  }

  /**
   * INITTASK to close the cache.
   */
  public static void closeCacheTask() {
    /*
    Set rootRegions = cache.rootRegions();
    for (Iterator i = rootRegions.iterator(); i.hasNext();) {
      Region r = (Region)i.next();
      EdgeHelper.removeThreadLocalConnection(r);
    }
    */
    CacheHelper.closeCache();
    cache = null;
  }

  /**
   * INITTASK to create the regions.
   */
  public static synchronized void createRegionsTask() {
    List<String> regionConfigs = UseCase3Prms.getRegionConfigs();
    log.info("Creating regions using configs: " + regionConfigs);
    for (String regionConfig : regionConfigs) {
      RegionHelper.createRegion(regionConfig);
    }
  }

  /**
   * INITTASK to start the server.
   */
  public static void startServerTask() {
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    BridgeHelper.startBridgeServer(bridgeConfig);
  }

  /**
   * INITTASK to create a pool.
   */
  public static void createPoolTask() {
    String poolConfig = ConfigPrms.getPoolConfig();
    PoolHelper.createPool(poolConfig);
  }

  // @todo EdgeHelper.addThreadLocalConnection(impl.getRegion());
 
  // @todo multithread the registration
  /**
   * INITTASK to register interest.  Registers interest in {@link UseCase3Prms
   * #interestTotalKeys} in batches of size {@link UseCase3Prms#interestBatchSize}.
   * A single thread does all of the registrations.
   */
  public static void registerInterestTask() {
    UseCase3Client c = new UseCase3Client();
    c.initialize();
    if (c.tid() % c.numThreadsInVM() == 0) {
      c.registerInterest();
    }
  }
  private void registerInterest() {
    RegionName regionName = UseCase3Prms.getRegionName();
    this.maxKeys = this.regionSpec.get(regionName);
    Region region = cache.getRegion(regionName.toString());
    int interestTotalKeys = UseCase3Prms.getInterestTotalKeys();
    int interestBatchSize = UseCase3Prms.getInterestBatchSize();
    log.info("Registering interest in " + interestTotalKeys + " out of "
            + this.maxKeys + " keys in region " + regionName
            + " using batches of size " + interestBatchSize);

    int bins = interestTotalKeys;
    int keysPerBin = this.maxKeys/bins;
    int remainder = this.maxKeys - bins * keysPerBin;
    if (log.finerEnabled()) {
      log.finer("region: " + regionName
             + " maxKeys: " + this.maxKeys
             + " bins: " + bins + "keysPerBin: " + keysPerBin
             + " remainder " + remainder);
    }

    int totalKeys = 0;
    int registeredKeys = 0;
    List keys = new ArrayList();
    for (int bin = 0; bin < bins; bin++) {
      // define this bin
      int keysInThisBin = (bin < remainder) ? keysPerBin + 1 : keysPerBin;
      totalKeys += keysInThisBin;

      // pick a random key from this bin
      int rkey = this.rng.nextInt(0, keysInThisBin - 1);
      int key = rkey * bins + bin;
      keys.add(ObjectHelper.createName(this.keyType, key));
      if (log.finerEnabled()) {
        log.finer("bin #" + bin + " has " + keysInThisBin
               + " keys, selected rkey " + rkey + " key " + key);
      }
      // register in batches
      if (keys.size() == interestBatchSize) {
        registeredKeys += keys.size();
        registerInterest(keys, region, regionName);
      }
    }
    // register the remainder, if any
    if (keys.size() > 0) {
      registeredKeys += keys.size();
      registerInterest(keys, region, regionName);
    }
    if (registeredKeys == interestTotalKeys) {
      log.info("Registered " + registeredKeys + " total keys");
    } else {
      String s = "Registered wrong number of keys: " + registeredKeys;
      throw new CachePerfException(s);
    }
    if (totalKeys != this.maxKeys) {
      throw new CachePerfException("Wrong number of keys allocated to bins");
    }
  }
  private void registerInterest(List keys, Region region, RegionName regionName) {
    if (log.fineEnabled()) {
      log.fine("REGION=" + regionName + " OP=registerInterest KEYS=" + keys);
    }
    long start = this.statistics.startRegisterInterest();
    region.registerInterest(keys, policy, false);
    this.statistics.endRegisterInterest(start, keys.size(), regionName);
    keys.clear();
  }

  /**
   * INITTASK to register interest in all keys using a regular expression.
   */
  public static void registerInterestRegexAllTask() {
    UseCase3Client c = new UseCase3Client();
    c.initialize();
    if (c.tid() % c.numThreadsInVM() == 0) {
      c.registerInterestRegexAll();
    }
  }
  private void registerInterestRegexAll() {
    RegionName regionName = UseCase3Prms.getRegionName();
    this.maxKeys = this.regionSpec.get(regionName);
    Region region = cache.getRegion(regionName.toString());
    String regex = ".*";
    log.info("Registering interest in all " + this.maxKeys
            + " keys in region " + regionName);
    long start = this.statistics.startRegisterInterest();
    region.registerInterestRegex(regex, policy, false);
    this.statistics.endRegisterInterest(start, this.maxKeys, regionName);
  }

//------------------------------------------------------------------------------
// createDataTask

  /**
   *  INITTASK to create objects of type
   *  {@link cacheperf.UseCase3Prms#objectType}.
   *  Each client puts a new object at a new key.
   */
  public static void createDataTask() {
    UseCase3Client c = new UseCase3Client();
    c.initialize();
    c.createData();
  }
  private void createData() {
    RegionName regionName = UseCase3Prms.getRegionName();
    this.maxKeys = this.regionSpec.get(regionName);
    this.log.info("Creating data in region " + regionName
                 + " with " + this.maxKeys + " keys");
    Region region = cache.getRegion(regionName.toString());
    do {
      int key = getNextKey();
      executeTaskTerminator();
      create(key, region, regionName);
      ++this.count;
      ++this.keyCount;
    } while (!executeBatchTerminator());
  }
  private void create(int i, Region region, RegionName regionName) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = UseCase3Prms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );
    if (this.log.fineEnabled()) {
      this.log.fine("REGION=" + regionName + " OP=create KEY=" + key);
    }
    long start = this.statistics.startCreate();
    region.create( key, val );
    this.statistics.endCreate(start, 1, regionName);
  }

//------------------------------------------------------------------------------
// putDataTask

  /**
   * TASK to put objects.
   */
  public static void putDataTask() {
    UseCase3Client c = new UseCase3Client();
    c.initialize();
    c.putData();
  }
  private void putData() {
    RegionName regionName = UseCase3Prms.getRegionName();
    this.maxKeys = this.regionSpec.get(regionName);
    this.log.info("Putting data in region " + regionName
                 + " with " + this.maxKeys + " keys");
    Region region = cache.getRegion(regionName.toString());
    boolean batchDone = false;
    int sleepAfterOpMs = UseCase3Prms.getSleepAfterOpMs();
    do {
      int key = getNextKey();
      executeTaskTerminator();
      put(key, region, regionName);
      ++this.count;
      ++this.keyCount;
      if (sleepAfterOpMs > 0) {
        MasterController.sleepForMs(sleepAfterOpMs);
      }
      batchDone = executeBatchTerminator();
    } while (!batchDone);
  }

//------------------------------------------------------------------------------
// mixDataTask

  /**
   * TASK to mix objects.
   */
  public static void mixDataTask() {
    UseCase3Client c = new UseCase3Client();
    c.initialize();
    c.mixData();
  }
  private void mixData() {
    boolean batchDone = false;
    Map cacheOpSpec = UseCase3Prms.getCacheOpSpec();
    int sleepAfterOpMs = UseCase3Prms.getSleepAfterOpMs();
    do {
      RegionName regionName = UseCase3Prms.getRegionName();
      Region region = cache.getRegion(regionName.toString());
      this.maxKeys = this.regionSpec.get(regionName);
      int key = getNextKey();
      executeTaskTerminator();
      CacheOp op = getCacheOp(cacheOpSpec);
      int sz = 1;
      switch (op) {
        case get:
          get(key, region, regionName);
          break;
        case put:
          put(key, region, regionName);
          break;
        case getAll:
          sz = getAll(key, region, regionName);
          break;
        case putAll:
          sz = putAll(key, region, regionName);
          break;
        default:
          throw new CachePerfException("Unsupported cacheOp: " + op);
      }
      this.count += sz;
      this.keyCount += sz;
      if (sleepAfterOpMs > 0) {
        MasterController.sleepForMs(sleepAfterOpMs);
      }
      batchDone = executeBatchTerminator();
    } while (!batchDone);
  }
  private CacheOp getCacheOp(Map<CacheOp,Double> cacheOpSpec) {
    long n = this.rng.nextLong(0l, TOTAL_OPS - 1);
    double weight = 0d;
    for (CacheOp cacheOp : cacheOpSpec.keySet()) {
      weight += cacheOpSpec.get(cacheOp);
      if (n < weight * TOTAL_OPS) {
        return cacheOp;
      }
    }
    throw new CachePerfException("Should not happen");
  }
  private static final long TOTAL_OPS = 100000000;

//------------------------------------------------------------------------------
// cacheOps

  private void get(int i, Region region, RegionName regionName) {
    Object key = ObjectHelper.createName( this.keyType, i );
    if (this.log.fineEnabled()) {
      this.log.fine("REGION=" + regionName + " OP=get KEY=" + key);
    }
    long start = this.statistics.startGet(regionName, 1);
    Object val = region.get(key);
    if (val == null) {
      if (this.allowNulls) {
        this.statistics.incNulls(regionName);
      } else {
        String s = "Got null at key=" + key + " of maxKeys=" + this.maxKeys
                 + " for region " + regionName;
        throw new CachePerfException(s);
      }
    }
    this.statistics.endGet(start, 1, regionName);
  }

  private void put(int i, Region region, RegionName regionName) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = UseCase3Prms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );
    if (this.log.fineEnabled()) {
      this.log.fine("REGION=" + regionName + " OP=put KEY=" + key);
    }
    long start = this.statistics.startPut(regionName, 1);
    region.put(key, val);
    this.statistics.endPut(start, 1, regionName);
  }

  private int getAll(int i, Region region, RegionName regionName) {
    List l = new ArrayList();
    // add the first key
    l.add(ObjectHelper.createName(this.keyType, i));
    // add the remaining keys
    for (int j = 1; j < UseCase3Prms.getBulkOpMapSize(); j++) {
      int k = getNextKey();
      l.add(ObjectHelper.createName(this.keyType, k));
    }
    // do the operation
    if (this.log.fineEnabled()) {
      this.log.fine("REGION=" + regionName + " OP=getAll KEYS=" + l);
    }
    int sz = l.size();
    long start = this.statistics.startGetAll(regionName, sz);
    region.getAll(l);
    this.statistics.endGetAll(start, sz, regionName);
    return sz;
  }

  private int putAll(int i, Region region, RegionName regionName) {
    String objectType = UseCase3Prms.getObjectType();
    Map m = new HashMap();
    // add the first key/val
    m.put(ObjectHelper.createName(this.keyType, i),
          ObjectHelper.createObject(objectType, i));
    // add the remaining key/vals
    for (int j = 1; j < UseCase3Prms.getBulkOpMapSize(); j++) {
      int k = getNextKey();
      m.put(ObjectHelper.createName(this.keyType, k),
            ObjectHelper.createObject(objectType, k));
    }
    // do the operation
    if (this.log.fineEnabled()) {
      this.log.fine("REGION=" + regionName + " OP=putAll KEYS=" + m.keySet());
    }
    int sz = m.size();
    long start = this.statistics.startPutAll(regionName, sz);
    region.putAll(m);
    this.statistics.endPutAll(start, sz, regionName);
    return sz;
  }

//------------------------------------------------------------------------------
// bounceTask

  /**
   * TASK to bounce a server.  Selects a VM of type "server".  Sleeps {@link
   * UseCase3Prms#stopWaitSec}.  Synchronously stops the VM using a nice exit,
   * or a mean kill if {@link UseCase3Prms#useMeanKill} is true.  Restarts the
   * VM after {@link UseCase3Prms#restartWaitSec} seconds.
   */
  public static void bounceTask() throws ClientVmNotFoundException {
    UseCase3Client c = new UseCase3Client();
    c.initHydraThreadLocals();
    c.bounce();
  }
  private void bounce() throws ClientVmNotFoundException {
    int stopWaitSec = UseCase3Prms.getStopWaitSec();
    int restartWaitSec = UseCase3Prms.getRestartWaitSec();
    int killType = UseCase3Prms.useMeanKill() ? ClientVmMgr.MEAN_KILL
                                          : ClientVmMgr.NICE_EXIT;

    // select a random vm of the specified type
    ClientName clientNameToBounce = UseCase3Prms.getClientNameToBounce();
    Map cds = TestConfig.getInstance().getClientDescriptions();
    List<String> clientNames = new ArrayList();
    for (Iterator i = cds.values().iterator(); i.hasNext();) {
      ClientDescription cd = (ClientDescription)i.next();
      if (cd.getName().startsWith(clientNameToBounce.toString())) {
        clientNames.add(cd.getName());
      }
    }
    GsRandom rng = TestConfig.tab().getRandGen();
    String clientName = clientNames.get(rng.nextInt(0, clientNames.size() - 1));
    ClientVmInfo info = new ClientVmInfo(null, clientName, null);

    log.info("Bouncing " + clientName + "...");

    // sleep before stop
    if (stopWaitSec > 0) {
      log.info("Sleeping " + stopWaitSec + " sec before bouncing " + info);
      long startSleep = this.statistics.startSleep();
      MasterController.sleepForMs(stopWaitSec * 1000);
      this.statistics.endSleep(startSleep);
    }

    // stop the vm
    long startStop = this.statistics.startStop(clientNameToBounce);
    info = ClientVmMgr.stop(
        "Stopping " + info, killType, ClientVmMgr.ON_DEMAND, info);
    this.statistics.endStop(startStop, clientNameToBounce);

    // sleep before restart
    if (restartWaitSec > 0) {
      log.info("Sleeping " + restartWaitSec + " sec before restarting " + info);
      long startSleep = this.statistics.startSleep();
      MasterController.sleepForMs(restartWaitSec * 1000);
      this.statistics.endSleep(startSleep);
    }

    // restart the vm
    long startStart = this.statistics.startStart(clientNameToBounce);
    info = ClientVmMgr.start("Restarting " + info, info);
    this.statistics.endStart(startStart, clientNameToBounce);

    log.info("Bounced " + clientName);
  }

  public static void rebalanceTask() throws InterruptedException {
    UseCase3Client c = new UseCase3Client();
    c.initHydraThreadLocals();
    c.rebalance();
  }
  private void rebalance() throws InterruptedException {
    log.info("Rebalancing buckets...");
    long start = this.statistics.startRebalance();
    RebalanceFactory rf = CacheHelper.getCache().getResourceManager()
                                     .createRebalanceFactory();
    RebalanceOperation ro = rf.start();
    RebalanceResults results = ro.getResults(); // blocking call
    this.statistics.endRebalance(start);
    log.info("Rebalanced buckets");
  }

//------------------------------------------------------------------------------
//  Key support
//------------------------------------------------------------------------------

  protected int getNextKey() {
    int key;
    switch( this.keyAllocation ) {
      case UseCase3Prms.SAME_KEY:
        key = 0;
        break;
      case UseCase3Prms.SAME_KEYS:
        key = this.keyCount;
        break;
      case UseCase3Prms.SAME_KEYS_WRAP:
        key = this.keyCount % this.maxKeys;
        break;
      case UseCase3Prms.SAME_KEYS_RANDOM_WRAP:
        key = this.rng.nextInt( 0, this.maxKeys - 1 );
        break;
      case UseCase3Prms.OWN_KEY:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	  key = this.ttgid;
        } else {
          key = this.currentKey;
	}
        break;
      case UseCase3Prms.OWN_KEYS:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	  key = this.ttgid;
	} else {
          key = this.currentKey + this.numThreads;
	}
        break;
      case UseCase3Prms.OWN_KEYS_WRAP:
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
      case UseCase3Prms.OWN_KEYS_RANDOM_WRAP:
        if ( this.currentKey == -1 ) {
	  checkSufficientKeys();
	}
        int numKeys = (int) Math.ceil( this.maxKeys / this.numThreads );
        if ( this.ttgid >= this.maxKeys % this.numThreads ) {
          --numKeys;
        }
        key = this.ttgid + this.numThreads * this.rng.nextInt( 0, numKeys );
        break;
      case UseCase3Prms.OWN_KEYS_CHUNKED:
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
      default:
        throw new HydraInternalException( "Should not happen" );
    }
    this.currentKey = key;
    return key;
  }

  private void checkSufficientKeys() {
    if ( this.numThreads > this.maxKeys ) {
      throw new HydraConfigException(
	this.maxKeys + " keys are not enough for " +
	this.numThreads + " threads to have their own keys"
      );
    }
  }

  //----------------------------------------------------------------------------
  //  Terminator support
  //----------------------------------------------------------------------------

  protected boolean executeBatchTerminator() {
    if (this.batchTerminator != null) {
      Object o = executeTerminator(this.batchTerminator);
      if (((Boolean)o).booleanValue()) {
        updateHydraThreadLocals();
        DistributedSystem.releaseThreadsSockets();
        return true;
      }
    }
    return false;
  }
  protected void executeTaskTerminator() {
    if ( this.taskTerminator != null ) {
      Object o = executeTerminator( this.taskTerminator );
      if (((Boolean)o).booleanValue()) {
        updateHydraThreadLocals();
        throw new StopSchedulingTaskOnClientOrder();
      }
    }
  }
  protected Method getMethod( String className, String methodName ) {
    if (( methodName == null ) || methodName.equals("null")) {
      return null;
    } else {
      try {
	Class cls = Class.forName( className );
	Class[] prmTypes = new Class[]{ cacheperf.poc.useCase3_2.UseCase3Client.class };
	return MethExecutor.getMethod( cls, methodName, prmTypes );
      } catch( ClassNotFoundException e ) {
	throw new HydraConfigException( className + " not found", e );
      } catch( NoSuchMethodException e ) {
	throw new HydraConfigException( className + "." + methodName + "(UseCase3Client) not found", e );
      }
    }
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

  //----------------------------------------------------------------------------
  //  Initialization
  //----------------------------------------------------------------------------

  // fixed task-local variables
  public int tid;
  public int tgid;
  public int ttgid;
  public int numThreads;
  public List interestListBatch;

  // other task-local variables
  public long batchStartTime; // time when this batch started

  // fixed task-local parameters
  public boolean allowNulls;
  public Map<RegionName,Integer> regionSpec;
  public int batchSeconds;
  public int keyAllocation;
  public int keyAllocationChunkSize;
  public int maxKeys;  // this is initialized in each task
  public Method batchTerminator;
  public Method taskTerminator;
  public Object[] terminatorArgs;

  protected void initialize() {
    initLocalVariables();
    initLocalParameters();
    initHydraThreadLocals();
    initBatchVariables();
  }
  protected void initLocalVariables() {
    this.tid = tid();
    this.tgid = tgid();
    this.ttgid = ttgid();
    this.numThreads = numThreads();
    com.gemstone.gemfire.internal.Assert.assertTrue( this.numThreads > 0, "Have " + this.numThreads + " threads" );
  }
  protected void initBatchVariables() {
    this.batchStartTime = System.currentTimeMillis();
  }
  protected void initLocalParameters() {
    this.allowNulls = UseCase3Prms.allowNulls();
    this.regionSpec = UseCase3Prms.getRegionSpec();
    this.batchSeconds = UseCase3Prms.getBatchSeconds();
    this.keyAllocation = UseCase3Prms.getKeyAllocation();
    this.keyAllocationChunkSize = UseCase3Prms.getKeyAllocationChunkSize();

    this.batchTerminator  = getMethod( UseCase3Prms.getBatchTerminatorClass(),
                                       UseCase3Prms.getBatchTerminatorMethod() );
    this.taskTerminator   = getMethod( UseCase3Prms.getTaskTerminatorClass(),
                                       UseCase3Prms.getTaskTerminatorMethod() );

    this.terminatorArgs = new Object[]{ this };
  }

  //----------------------------------------------------------------------------
  //  Hydra thread locals and their instance field counterparts
  //----------------------------------------------------------------------------

  public TestTask task;
  public UseCase3Stats statistics;
  public GsRandom rng;

  public int count;        // iterations since this task started
  public long startTime;   // time when this task started
  public int keyCount;
  public int currentKey;
  public String keyType;
  public long lastTaskTerminatorTime; // time task terminator last ran, in ms
  public long workloadStartTime; // time when the TASK loop started

  private static HydraThreadLocal localtask = new HydraThreadLocal();
  private static HydraThreadLocal localstatistics = new HydraThreadLocal();
  private static HydraThreadLocal localrng = new HydraThreadLocal();
  private static HydraThreadLocal localcount = new HydraThreadLocal();
  private static HydraThreadLocal localkeycount = new HydraThreadLocal();
  private static HydraThreadLocal localcurrentkey = new HydraThreadLocal();
  private static HydraThreadLocal localkeytype = new HydraThreadLocal();
  private static HydraThreadLocal locallasttaskterminatortime = new HydraThreadLocal();
  private static HydraThreadLocal localstarttime = new HydraThreadLocal();
  private static HydraThreadLocal localworkloadstarttime = new HydraThreadLocal();
  private static HydraThreadLocal localtm = new HydraThreadLocal();

  protected void initHydraThreadLocals() {
    // read-only
    this.task = getTask();
    this.rng = getRNG();

    // updated in special tasks
    this.statistics = getStatistics();

    // updated in tasks
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    this.count = getCount();
    this.keyCount = getKeyCount();
    this.currentKey = getCurrentKey();
    this.keyType = getKeyType();
    this.lastTaskTerminatorTime = getLastTaskTerminatorTime();
    this.startTime = getStartTime();
    this.workloadStartTime = task.getTaskTypeString().equals("TASK")
                           ? getWorkloadStartTime() : -1;
  }
  protected void resetHydraThreadLocals() {
    localcount.set( null );
    localkeycount.set( null );
    localcurrentkey.set( null );
    locallasttaskterminatortime.set( null );
    localstarttime.set( null );
  }
  protected void updateHydraThreadLocals() {
    setStatistics( this.statistics );
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    setCount( this.count );
    setKeyCount( this.keyCount );
    setCurrentKey( this.currentKey );
    setKeyType( this.keyType );
    setLastTaskTerminatorTime( this.lastTaskTerminatorTime );
    setStartTime( this.startTime );
    if (this.workloadStartTime != -1) {
      setWorkloadStartTime(this.workloadStartTime);
    }
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
      resetHydraThreadLocals();
    }
    return newTask;
  }
  /**
   *  Gets the per-thread performance statistics instance.
   */
  protected UseCase3Stats getStatistics() {
    UseCase3Stats stats = (UseCase3Stats) localstatistics.get();
    return stats;
  }
  /**
   *  Sets the per-thread performance statistics instance.
   */
  protected void setStatistics( UseCase3Stats stats ) {
    localstatistics.set( stats );
  }
  /**
   *  Gets the count for a thread's workload.
   */
  protected int getCount() {
    Integer n = (Integer) localcount.get();
    if ( n == null ) {
      n = Integer.valueOf(0);
      localcount.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the count for a thread's workload.
   */
  protected void setCount( int n ) {
    localcount.set(Integer.valueOf(n));
  }
  /**
   *  Gets the key count for a thread's workload.
   */
  protected int getKeyCount() {
    Integer n = (Integer) localkeycount.get();
    if ( n == null ) {
      n = Integer.valueOf(0);
      localkeycount.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the key count for a thread's workload.
   */
  protected void setKeyCount( int n ) {
    localkeycount.set(Integer.valueOf(n));
  }
  /**
   *  Gets the current key for a thread's workload.
   */
  protected int getCurrentKey() {
    Integer n = (Integer) localcurrentkey.get();
    if ( n == null ) {
      n = Integer.valueOf(-1);
      localcurrentkey.set( n );
    }
    return n.intValue();
  }
  /**
   *  Sets the current key for a thread's workload.
   */
  protected void setCurrentKey( int n ) {
    localcurrentkey.set(Integer.valueOf(n));
  }
  /**
   *  Gets the key type for a thread's workload.
   */
  protected String getKeyType() {
    String t = (String) localkeytype.get();
    if ( t == null ) {
      t = UseCase3Prms.getKeyType();
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
   *  Gets the last task terminator time for a thread's workload.
   */
  protected long getLastTaskTerminatorTime() {
    Long t = (Long) locallasttaskterminatortime.get();
    if ( t == null ) {
      t = Long.valueOf(System.currentTimeMillis());
      locallasttaskterminatortime.set( t );
    }
    return t.longValue();
  }
  /**
   *  Sets the last task terminator time for a thread's workload.
   */
  protected void setLastTaskTerminatorTime( long t ) {
    locallasttaskterminatortime.set(Long.valueOf(t));
  }
  /**
   *  Gets the start time for a thread's workload.
   */
  protected long getStartTime() {
    Long t = (Long) localstarttime.get();
    if ( t == null ) {
      t = Long.valueOf(System.currentTimeMillis());
      localstarttime.set( t );
    }
    return t.longValue();
  }
  /**
   *  Sets the start time for a thread's workload.
   */
  protected void setStartTime( long t ) {
    localstarttime.set(Long.valueOf(t));
  }
  /**
   * Gets the workload start time for a thread's workload.
   */
  protected long getWorkloadStartTime() {
    Long t = (Long)localworkloadstarttime.get();
    if (t == null) {
      SharedMap map = TaskSyncBlackboard.getInstance().getSharedMap();
      t = (Long)map.get(WORKLOAD_START_TIME);
      if (t == null) {
        t = Long.valueOf(System.currentTimeMillis());
        map.put(WORKLOAD_START_TIME, t);
        log.info("Posted WORKLOAD_START_TIME=" + t);
      }
      localworkloadstarttime.set(t);
    }
    return t.longValue();
  }
  /**
   * Sets the workload start time for a thread's workload.
   */
  protected void setWorkloadStartTime(long t) {
    localworkloadstarttime.set(Long.valueOf(t));
  }
  /**
   *  Gets the random number generator.
   */
  protected GsRandom getRNG() {
    GsRandom r = (GsRandom) localrng.get();
    if ( r == null ) {
      r = TestConfig.getInstance().getParameters().getRandGen();
      localrng.set( r );
    }
    return r;
  }

//------------------------------------------------------------------------------
//  Test config functions
//--=---------------------------------------------------------------------------

  /**
   * Returns a string that maps each key (prefix1...prefixN) to the union of
   * subsets of each group of region configs, with a comma separating each
   * key-values pair.
   */
  public static String subset(String prefix, int n,
         int subsetSize1, String regionConfigs1,
         int subsetSize2, String regionConfigs2) {
    List<String> names = generateNames(prefix, n);
    List<String> configs1 = getTokens(regionConfigs1);
    if (configs1.size() < subsetSize1) {
      String s = "Cannot create a subset of size " + subsetSize1
               + " from " + configs1;
      throw new HydraConfigException(s);
    }
    List<String> configs2 = getTokens(regionConfigs2);
    if (configs2.size() < subsetSize2) {
      String s = "Cannot create a subset of size " + subsetSize2
               + " from " + configs2;
      throw new HydraConfigException(s);
    }
    return subset(names, subsetSize1, configs1, subsetSize2, configs2);
  }

  private static String subset(List<String> names,
                               int subsetSize1, List<String> regionConfigs1,
                               int subsetSize2, List<String> regionConfigs2) {
    String map = "";
    int j1 = 0;
    int j2 = 0;
    for (String name : names) {
      if (map.length() != 0) map += ", ";
      String configs = "";
      for (int i = 0; i < subsetSize1; i++) {
        configs += regionConfigs1.get((j1++ % regionConfigs1.size())) + " ";
      }
      for (int i = 0; i < subsetSize2; i++) {
        configs += regionConfigs2.get((j2++ % regionConfigs2.size())) + " ";
      }
      map += name + " " + configs.trim();
    }
    return map;
  }

  /**
   * Returns a string that maps each key (prefix1...prefixN) to all of
   * the region configs, with a comma separating each key-values pair.
   */
  public static String all(String prefix, int n, String regionConfigs) {
    List<String> names = generateNames(prefix, n);
    String map = "";
    for (String name : names) {
      if (map.length() != 0) map += ", ";
      map += name + " " + regionConfigs.trim();
    }
    return map;
  }

  private static List<String> generateNames(String prefix, int n) {
    List<String> names = new ArrayList();
    for (int i = 1; i <= n; i++) {
      names.add(prefix + i);
    }
    return names;
  }

  public static List<String> getTokens(String str) {
    List<String> tokens = new ArrayList();
    StringTokenizer tokenizer = new StringTokenizer(str, " ", false);
    while (tokenizer.hasMoreTokens()) {
      tokens.add(tokenizer.nextToken().trim());
    }
    return tokens;
  }

//------------------------------------------------------------------------------
//  Miscellaneous convenience methods
//------------------------------------------------------------------------------

  /**
   *  Gets the client's hydra thread id.
   */
  protected int tid() {
    return RemoteTestModule.getCurrentThread().getThreadId();
  }
  /**
   *  Gets the client's hydra threadgroup id.
   */
  protected int tgid() {
    return RemoteTestModule.getCurrentThread().getThreadGroupId();
  }
  /**
   *  Gets the client's hydra threadgroup id for the current task.
   */
  protected int ttgid() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    int id = task.getTaskThreadGroupId( tgname, tgid() );
    return id;
  }
  /**
   *  Gets the number of hydra threads in this vm.
   */
  protected static int numThreadsInVM() {
    return TestConfig.getInstance()
      .getClientDescription(RemoteTestModule.getMyClientName()).getVmThreads();
  }
  /**
   *  Gets the total number of threads eligible to run the current task.
   */
  protected static int numThreads() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    int t = task.getTotalThreads();
    return t;
  }
}

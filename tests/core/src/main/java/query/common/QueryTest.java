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
package query.common;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import event.*;
import parReg.ParRegUtil;
import pdx.PdxTest;
import pdx.PdxETListener;
import java.util.*;
import util.*;
import hydra.*;
import hydra.CachePrms;
import objects.*;

import cacheperf.gemfire.query.QueryPerfPrms;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryExecutionTimeoutException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;

/**
 * A Hydra test that concurrently performs a number of cache-related operations
 * and querying. This test is basically for testing the effect of querying on
 * various cache operations. It ensures that the querying works properly without
 * hang in distributed environment Along the way, it also uses the blackboards
 * {@link EventBB} and {@link EventCountersBB} to keep track of what has
 * happened and to validate that what has happened is expected. It requires that
 * the regions that it tests be distributed and mirrored.
 */
public class QueryTest {

  /* The singleton instance of QueryTest in this VM */
  static protected QueryTest queryTest;

  // operations
  static protected final int ADD_OPERATION = 1;
  static protected final int UPDATE_OPERATION = 2;
  static protected final int INVALIDATE_OPERATION = 3;
  static protected final int DESTROY_OPERATION = 4;
  static protected final int READ_OPERATION = 5;
  static protected final int LOCAL_INVALIDATE_OPERATION = 6;
  static protected final int LOCAL_DESTROY_OPERATION = 7;
  static protected final int QUERY_OPERATION = 8;
  static protected final int CREATE_INDEX_OPERATION = 9;
  static protected final int REMOVE_INDEX_OPERATION = 10;
  static protected final int COUNT_QUERY_OPERATION = 11;
  static protected final int PDXSTRING_QUERY_OPERATION = 12;
  protected static boolean pdxReadSerialized = false;
  // value of pdxReadSerialized in the cache
  protected static HydraThreadLocal origClassLoader = new HydraThreadLocal();
  // used to save the classLoader so that the test can ensure there is no domain
  // class classpath in used when a query executes AND pdxReadSerialized is true
  // (when pdxReadSerialized is false, the thread that executes a query must
  // have the domain class classpath)w
  protected int numOfRegions;
  // number of regions.
  protected boolean isSerialExecution;
  // cache whether this is serial execution
  protected boolean isCarefulValidation = false;
  // true if this test does careful validation
  protected boolean ignoreQueryExecTimeOutException;
  // whether to ignore query timeout exceptions
  protected int queryLimit = -1;
  // the limit to use in queries, -1 signifies no limit is to be used.
  static public final int MILLIS_TO_WAIT = 60000;
  // the number of millis to wait for an event to occur in serial execution
  // tests
  protected int numVMs;
  // the number of VMs in this test
  protected long minTaskGranularitySec;
  // the task granularity in seconds
  protected long minTaskGranularityMS;
  // the task granularity in milliseconds
  protected RandomValues randomValues = null;
  // for creating random objects
  protected int maxObjects;
  // the maximum number of objects to allow in the region
  protected DistributedLockService distLockService;
  // the distributed lock service for this VM
  // for entry event tests, indicates if the region this VM is operating on
  // is mirrored
  public Pool pool;
  // String prefixes for event callback object
  protected static final String createCallbackPrefix = "Create event originated in pid ";
  protected static final String updateCallbackPrefix = "Update event originated in pid ";
  protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
  protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";

  // lock names
  protected static String LOCK_SERVICE_NAME = "MyLockService";
  protected static String LOCK_NAME = "MyLock";

  // root region name
  public static final String REGION_NAME = "QueryRegion";

  private static Query[] queries = null;
  private static boolean queriesRead = false;
  // ========================================================================
  // initialization methods

  /**
   * Creates and {@linkplain #initialize initializes} the singleton instance of
   * <code>QueryTest</code> in this VM.
   */
  public synchronized static void HydraTask_initialize() {
    if (queryTest == null) {
      queryTest = new QueryTest();
      GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
      Log.getLogWriter().info("gfd.getSysDirName()::" + gfd.getSysDirName());
      Log.getLogWriter().info(
          "gfd.getSystemDirectoryStr()::" + gfd.getSystemDirectoryStr());
      Log.getLogWriter().info(
          "gfd.getSystemDirectory():: " + gfd.getSystemDirectory());
      queryTest.initialize();
      boolean isBridgeConfiguration = (TestConfig.tab().stringAt(
          BridgePrms.names, null) != null);
      if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
      }
    }
  }

  /**
   * Creates and {@linkplain #initialize initializes} the singleton instance of
   * <code>QueryTest</code> in this VM.
   */
  public synchronized static void HydraTask_initialize_xml() {
    if (queryTest == null) {
      queryTest = new QueryTest();
      String xmlFilename = TestConfig.tasktab().stringAt(QueryPrms.xmlFilename,
          TestConfig.tab().stringAt(QueryPrms.xmlFilename, null));
      GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
      Log.getLogWriter().info("gfd.getSysDirName()::" + gfd.getSysDirName());
      String baseDir = gfd.getSysDirName();
      baseDir = baseDir.substring(0, baseDir.lastIndexOf(File.separator));

      xmlFilename = xmlFilename.substring(0, xmlFilename.indexOf("."));
      int myId = RemoteTestModule.getMyVmid();
      Log.getLogWriter().info("My Id = " + myId);
      FileUtil.mkdir(baseDir + File.separator + "queryDiskStore" + myId);

      queryTest.initialize(xmlFilename + myId + ".xml");
      boolean isBridgeConfiguration = (TestConfig.tab().stringAt(
          BridgePrms.names, null) != null);
      if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
      }
    }
  }

  public static synchronized void HydraTask_initialize_clientCache() {
    if (queryTest == null) {
      queryTest = new QueryTest();
      queryTest.initialize_clientCache();
    }
  }

  public void initialize_clientCache() {

    Cache myCache = CacheHelper.createCache("cache1");

    maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, 20000);
    // used by rebalance tests only

    String regDescriptName = "accessorRegion";// TestConfig.getInstance().getRegionDescriptions();
    PoolDescription poolDescript = RegionHelper.getRegionDescription(
        regDescriptName).getPoolDescription();

    ParRegUtil.createDiskStoreIfNecessary(regDescriptName);
    RegionAttributes attr = RegionHelper.getRegionAttributes(regDescriptName);
    String regionName = RegionHelper.getRegionDescription(regDescriptName)
        .getRegionName();

    if (poolDescript != null) {
      String poolConfigName = RegionHelper
          .getRegionDescription(regDescriptName).getPoolDescription().getName();
      if (poolConfigName != null) {
        pool = PoolHelper.createPool(poolConfigName);
        pool.getQueryService();
      }
      // edgeClients always support ConcurrentMap
      // supportsConcurrentMap = true;
    }

    Region aRegion = CacheHelper.getCache().createRegion(regionName, attr);
    ParRegUtil.registerInterest(aRegion);
  }

  public void initialize() {
    String xmlFilename = TestConfig.tasktab().stringAt(QueryPrms.xmlFilename,
        TestConfig.tab().stringAt(QueryPrms.xmlFilename, null));
    initialize(xmlFilename);

  }

  
  /**
   * @see #HydraTask_initialize
   */
  public void initialize(String xmlFilename) {
    // numOfRegions = TestConfig.tab().intAt(QueryPrms.numOfRegions, 1);
    numOfRegions = TestConfig.getInstance().getRegionDescriptions().size();
    numOfRegions = numOfRegions == 0 ? 1 : numOfRegions;
    ignoreQueryExecTimeOutException = TestConfig.tab().booleanAt(
        QueryPrms.ignoreTimeOutException, false);
    Log.getLogWriter().info("Num of regions::" + numOfRegions);
    // createRootRegions();
    if (xmlFilename != null) {
      createCacheFromXml(xmlFilename);
      CacheUtil.setCache(CacheHelper.getCache());
      Set<Region<?,?>> regions = CacheHelper.getCache().rootRegions();
      numOfRegions = regions.size();
      for (Region region : regions) {
        numOfRegions += region.subregions(true).size();
      }
    } else {
      createRootRegions();
    }

    pdxReadSerialized = CacheHelper.getCache().getPdxReadSerialized();
    isSerialExecution = EventBB.isSerialExecution();
    isCarefulValidation = isCarefulValidation || isSerialExecution;
    queryLimit = TestConfig.tab().intAt(QueryPrms.queryLimit, -1);
    numVMs = 0;
    Vector gemFireNamesVec = TestConfig.tab().vecAt(GemFirePrms.names);
    Vector numVMsVec = TestConfig.tab().vecAt(ClientPrms.vmQuantities);
    if (gemFireNamesVec.size() == numVMsVec.size()) {
      for (int i = 0; i < numVMsVec.size(); i++) {
        numVMs = numVMs
            + (new Integer(((String) numVMsVec.elementAt(i)))).intValue();
      }
    } else {
      numVMs = new Integer((String) (numVMsVec.elementAt(0))).intValue()
          * gemFireNamesVec.size();
    }
    Log.getLogWriter().info("numVMs is " + numVMs);
    minTaskGranularitySec = TestConfig.tab().longAt(
        TestHelperPrms.minTaskGranularitySec);
    minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, 20000);
    randomValues = new RandomValues();
    if (xmlFilename == null) {
      createLockService();
    }
    EventBB.getBB().printSharedCounters();
    EventCountersBB.getBB().printSharedCounters();
  }


  /**
   * If necessary, creates the {@link DistributedLockService} used by this test.
   */
  static synchronized void createLockService() {
    if (queryTest.distLockService == null) {
      Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
      queryTest.distLockService = DistributedLockService.create(
          LOCK_SERVICE_NAME, DistributedSystemHelper.getDistributedSystem());
      Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
    }
  }

  // ========================================================================
  // hydra task methods

  /**
   * Load the regions under test with numToLoad entries spread across all the
   * regions.
   */
  public static void HydraTask_load() {
    final long LOG_INTERVAL_MILLIS = 10000;
    long lastLogTime = System.currentTimeMillis();
    long startTime = lastLogTime;
    int regionNumber = 0;
    List<Region> regionList = new ArrayList(CacheHelper.getCache()
        .rootRegions());
    do {
      regionNumber = ((new Random()).nextInt(queryTest.numOfRegions));
      regionNumber = (regionNumber + 1) % queryTest.numOfRegions;
      Region aRegion = regionList.get(regionNumber);
      Map aMap = new HashMap();
      for (int i = 1; i <= 100; i++) {
        String name = NameFactory
            .getNextPositiveObjectNameInLimit(queryTest.maxObjects);
        aMap.put(name, queryTest.getObjectToAdd(name));
      }
      aRegion.putAll(aMap);
      aMap = null;
      long entries = NameFactory.getPositiveNameCounter();
      if (entries >= queryTest.maxObjects) {
        TestHelper.checkForEventError(EventCountersBB.getBB());
        String aStr = "Done loading " + entries + " entries in "
            + queryTest.numOfRegions + " regions";
        throw new StopSchedulingTaskOnClientOrder(aStr);
      }
      if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
        Log.getLogWriter().info(
            "Added " + entries + " entries out of " + queryTest.maxObjects
                + " in " + queryTest.numOfRegions + " regions");
        lastLogTime = System.currentTimeMillis();
        TestHelper.checkForEventError(EventCountersBB.getBB());
      }
    } while (System.currentTimeMillis() - startTime < queryTest.minTaskGranularityMS);
  }

  /**
   * Populate regions with maxObjects number of region entries.
   */
  public static void HydraTask_populateRegions() {
    List<Region> regionList = new ArrayList(CacheHelper.getCache()
        .rootRegions());
    for (Region aRegion : regionList) {
      Map aMap = new HashMap();
      for (int j = 0; j < queryTest.maxObjects; j++) {
        String objectType = TestConfig.tab().stringAt(QueryPrms.objectType);
        Object val = ObjectHelper.createObject(objectType, j);
        aRegion.put(j + "", val);
      }
    }
  }

  /**
   * Performs randomly selected operations (add, invalidate, etc.) on the root
   * region based on the weightings in {@link QueryPrms#entryOperations}. The
   * operations will continue to be performed until the
   * {@linkplain TestHelperPrms#minTaskGranularitySec minimum task granularity}
   * has been reached.
   */
  public static void HydraTask_doEntryAndQueryOperations() {
    origClassLoader.set(Thread.currentThread().getContextClassLoader());
    PdxTest.initClassLoader();
    if (queryTest != null) {
      queryTest.doEntryAndQueryOperations();
    }
  }
  
  public Region getRandomRegionWithValueConstrains() {
    Region aRegion = null;

    Set<Region<?,?>> allRegions = new HashSet<Region<?,?>>();
    Set<Region<?,?>> rootRegions = CacheHelper.getCache().rootRegions();
    allRegions.addAll(rootRegions);
    for (Region region : rootRegions) {
      allRegions.addAll((Set<Region<?,?>>)region.subregions(true));
    }
    int attempts =  0;
    do {
      attempts++;
      int regionNumber = (new Random()).nextInt(numOfRegions);
      aRegion = (Region)CacheHelper.getCache().rootRegions().toArray()[regionNumber];
      Log.getLogWriter().info(
          "Obtained region for region operation: " + aRegion.getName()
              + " ValueConstraint for the region is: "
              + aRegion.getAttributes().getValueConstraint());
    } while (aRegion.getAttributes().getValueConstraint() != null && attempts < 100);
    if(attempts == 100) {
      throw new TestException("Could not obtain a region with ValueConstrainsts are 100 attempts");  
    }
    return aRegion;
  }

  /**
   * @see #HydraTask_doEntryOperations
   */
  protected void doEntryAndQueryOperations() {
    Region aRegion = getRandomRegionWithValueConstrains();
    long startTime = System.currentTimeMillis();
    if (isSerialExecution) {
      logExecutionNumber();
    }
    boolean isMirrored = aRegion.getAttributes().getMirrorType().isMirrored();
    
    boolean haveALock = false;
    do {
      TestHelper.checkForEventError(EventCountersBB.getBB());
      boolean useRandomLocks = TestConfig.tab().booleanAt(
          EventPrms.useRandomLocks);
      if (useRandomLocks) {
        Log.getLogWriter().info(
            "Trying to get distributed lock " + LOCK_NAME + "...");
        haveALock = distLockService.lock(LOCK_NAME, -1, -1);
        Log.getLogWriter().info(
            "Returned from trying to get distributed lock " + LOCK_NAME
                + ", lock acquired is " + haveALock);
        if (haveALock)
          Log.getLogWriter().info("Obtained distributed lock " + LOCK_NAME);
      }

      try {
        QueryBB.putQueryValidator(new QueryValidator());
        int whichOp = getOperation(QueryPrms.entryAndQueryOperations,
            isMirrored);
        switch (whichOp) {
          case ADD_OPERATION:
            addObject(aRegion, true);
            break;
          case INVALIDATE_OPERATION:
            invalidateObject(aRegion, false);
            break;
          case DESTROY_OPERATION:
            destroyObject(aRegion, false);
            break;
          case UPDATE_OPERATION:
            updateObject(aRegion);
            break;
          case READ_OPERATION:
            readObject(aRegion);
            break;
          case LOCAL_INVALIDATE_OPERATION:
            invalidateObject(aRegion, true);
            break;
          case LOCAL_DESTROY_OPERATION:
            destroyObject(aRegion, true);
            break;
          case QUERY_OPERATION:
            doQuery(true);
            break;
          default: {
            throw new TestException("Unknown operation " + whichOp);
          }
        }
      } finally {
        if (haveALock) {
          haveALock = false;
          distLockService.unlock(LOCK_NAME);
          Log.getLogWriter().info("Released distributed lock " + LOCK_NAME);
        }
      }
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  }

  public static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }

    public void beforeIndexLookup(Index index, int lowerBoundOperator,
        Object lowerBoundKey, int upperBoundOperator, Object upperBoundKey,
        Set NotEqualKeys) {
      indexesUsed.add(index.getName());
    }
  }

  protected void addObject(Region aRegion, boolean logAddition) {
    String name = NameFactory.getNextPositiveObjectNameInLimit(maxObjects);
    Object anObj = getObjectToAdd(aRegion.getAttributes().getValueConstraint().getName());
    String callback = createCallbackPrefix + ProcessMgr.getProcessId();
    if (logAddition)
      Log.getLogWriter().info(
          "addObject: calling put for name " + name + ", object "
              + TestHelper.toString(anObj) + " callback is " + callback
              + ", region is " + aRegion.getFullPath());
    try {
      aRegion.put(name, anObj, callback);
      QueryBB.putQueryValidator(new QueryValidator("add", name, true, true,
          anObj));
    } catch (RegionDestroyedException e) {
      handleRegionDestroyedException(aRegion, e);
    } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    long numPut = EventBB.incrementCounter("EventBB.NUM_CREATE",
        EventBB.NUM_CREATE);
  }

  protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
    Set aSet = aRegion.keySet();
    if (aSet.size() == 0) {
      Log.getLogWriter().info("invalidateObject: No names in region");
      return;
    }
    Iterator it = aSet.iterator();
    Object name = null;
    if (it.hasNext()) {
      name = it.next();
    } else { // has been destroyed cannot continue
      Log.getLogWriter().info(
          "invalidateObject: Unable to get name from region");
      return;
    }
    boolean containsValue = aRegion.containsValueForKey(name);
    boolean alreadyInvalidated = !containsValue;
    Log.getLogWriter().info("containsValue for " + name + ": " + containsValue);
    Log.getLogWriter().info(
        "alreadyInvalidated for " + name + ": " + alreadyInvalidated);
    try {
      String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId();
      if (isLocalInvalidate) {
        Log.getLogWriter().info(
            "invalidateObject: local invalidate for " + name + " callback is "
                + callback);
        aRegion.localInvalidate(name, callback);
        Log.getLogWriter().info(
            "invalidateObject: done with local invalidate for " + name);
        if (!alreadyInvalidated) {
          long numInvalidate = EventBB.incrementCounter(
              "EventBB.NUM_LOCAL_INVALIDATE", EventBB.NUM_LOCAL_INVALIDATE);
          QueryBB.putQueryValidator(new QueryValidator("localInvalidate", name,
              true, false, null));
        }
      } else {
        Log.getLogWriter().info(
            "invalidateObject: invalidating name " + name + " callback is "
                + callback);
        aRegion.invalidate(name, callback);
        Log.getLogWriter().info(
            "invalidateObject: done invalidating name " + name);
        if (!alreadyInvalidated) {
          long numInvalidate = EventBB.incrementCounter(
              "EventBB.NUM_INVALIDATE", EventBB.NUM_INVALIDATE);
          QueryBB.putQueryValidator(new QueryValidator("invalidate", name,
              true, false, null));
        }
      }
      if (isCarefulValidation)
        verifyObjectInvalidated(aRegion, name);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      if (isCarefulValidation)
        throw new TestException(TestHelper.getStackTrace(e));
      else {
        Log.getLogWriter()
            .info(
                "Caught "
                    + e
                    + " (expected with concurrent execution); continuing with test");
        return;
      }
    }
    // catch (CacheException e) {
    // throw new TestException(TestHelper.getStackTrace(e));
    // }
  }

  protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
    Set aSet = aRegion.keys();
    Iterator iter = aSet.iterator();
    if (!iter.hasNext()) {
      Log.getLogWriter().info("destroyObject: No names in region");
      return;
    }
    try {
      Object name = iter.next();
      destroyObject(aRegion, name, isLocalDestroy);
    } catch (NoSuchElementException e) {
      throw new TestException("Bug 30171 detected: "
          + TestHelper.getStackTrace(e));
    }
  }

  private void destroyObject(Region aRegion, Object name, boolean isLocalDestroy) {
    try {
      String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
      if (isLocalDestroy) {
        Log.getLogWriter().info(
            "destroyObject: local destroy for " + name + " callback is "
                + callback);
        aRegion.localDestroy(name, callback);
        Log.getLogWriter().info(
            "destroyObject: done with local destroy for " + name);
        long numDestroy = EventBB.incrementCounter("EventBB.NUM_LOCAL_DESTROY",
            EventBB.NUM_LOCAL_DESTROY);
        QueryBB.putQueryValidator(new QueryValidator("localDestroy", name,
            false, false, null));
      } else {
        Log.getLogWriter().info(
            "destroyObject: destroying name " + name + " callback is "
                + callback);
        aRegion.destroy(name, callback);
        Log.getLogWriter().info("destroyObject: done destroying name " + name);
        long numDestroy = EventBB.incrementCounter("EventBB.NUM_DESTROY",
            EventBB.NUM_DESTROY);
        QueryBB.putQueryValidator(new QueryValidator("destroy", name, false,
            false, null));
      }
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      if (isCarefulValidation)
        throw new TestException(TestHelper.getStackTrace(e));
      else {
        Log.getLogWriter()
            .info(
                "Caught "
                    + e
                    + " (expected with concurrent execution); continuing with test");
        return;
      }
    }
    // catch (CacheException e) {
    // throw new TestException(TestHelper.getStackTrace(e));
    // }
  }

  /**
   * Updates the "first" entry in a given region
   */
  protected void updateObject(Region aRegion) {
    Set aSet = aRegion.keys();
    Iterator iter = aSet.iterator();
    if (!iter.hasNext()) {
      Log.getLogWriter().info("updateObject: No names in region");
      return;
    }
    Object name = iter.next();
    updateObject(aRegion, name);
  }

  /**
   * Updates the entry with the given key (<code>name</code>) in the given
   * region.
   */
  protected void updateObject(Region aRegion, Object name) {
    Object anObj = null;
    try {
      anObj = aRegion.get(name);
    } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    Object newObj = getUpdateObject(aRegion.getAttributes().getValueConstraint().getName());
    try {
      String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
      Log.getLogWriter().info(
          "updateObject: replacing name " + name + " with "
              + TestHelper.toString(newObj) + "; old value is "
              + TestHelper.toString(anObj) + ", callback is " + callback);
      aRegion.put(name, newObj, callback);
      Log.getLogWriter().info("Done with call to put (update)");
    } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    QueryBB.putQueryValidator(new QueryValidator("update", name, true, true,
        newObj));
    long numUpdate = EventBB.incrementCounter("EventBB.NUM_UPDATE",
        EventBB.NUM_UPDATE);
  }

  /**
   * Fetches (reads) the value of a randomly selected entry in the given region.
   */
  protected void readObject(Region aRegion) {
    Set aSet = aRegion.keySet();
    Object anObj;
    if (aSet.size() == 0) {
      Log.getLogWriter().info("readObject: No names in region");
      return;
    }
    long maxNames = NameFactory.getPositiveNameCounter();
    if (maxNames <= 0) {
      Log.getLogWriter().info(
          "readObject: max positive name counter is " + maxNames);
      return;
    }
    Object name = NameFactory.getObjectNameForCounter(TestConfig.tab()
        .getRandGen().nextInt(1, (int) maxNames));
    Log.getLogWriter().info("readObject: getting name " + name);
    try {
      anObj = aRegion.get(name);
      Log.getLogWriter().info(
          "readObject: got value for name " + name + ": "
              + TestHelper.toString(anObj));
    } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    QueryBB.putQueryValidator(new QueryValidator("read", name, true, true,
        anObj));
  }

  private static void readQueries() {
    Map<String,Pool> nameToPoolMap = PoolManager.getAll();
    String poolName = TestConfig.tab().stringAt(PoolPrms.names, null);
    QueryService qs = CacheHelper.getCache().getQueryService();
    if (poolName != null) {
      if (nameToPoolMap.get(poolName) != null)  {
        qs = nameToPoolMap.get(poolName).getQueryService();
      }
    }
    HydraVector hvQueries = TestConfig.tab()
    .vecAt(QueryPerfPrms.query, null);

    if (hvQueries != null) {
      queries = new Query[hvQueries.size()];
      for (int i  = 0; i < hvQueries.size(); i++) {
        queries[i] = qs.newQuery((String)(hvQueries.get(i)));
      }
    }
 
    queriesRead = true;
  }
  
  /**
   * Executes the query over the region
   */
  protected void doQuery(boolean logAddition) {
    if(queries == null) {
      readQueries();
    }
    for (int i = 0; i < queries.length; i++) {
      Log.getLogWriter().info(" query = " + queries[i]);
      Query query = queries[i];
      try {
        // for pdx tests, we want the execute to occur without having domain
        // classes referenced from the class loader IF the test is using
        // pdxReadSerialized
        // if this is a pdx test, this thread currently has the domain class in
        // its class loader
        ClassLoader previousCL = Thread.currentThread().getContextClassLoader();
        if (pdxReadSerialized) { // remove the domain classes from this thread's
                                 // class loader
          Log.getLogWriter()
              .info(
                  "Setting class loader to remove domain classes in preparation of a query execute: "
                      + origClassLoader.get());
          Thread.currentThread().setContextClassLoader(
              (ClassLoader) origClassLoader.get());
        }
        Object result = null;
        try {
          Cache theCache = CacheHelper.getCache();
          if (theCache != null) { // could be null during HA
            // clear the pdx type registry so with Darrel's new 662 changes we
            // do not have domain classes available during the query
            // note that for a serial test, this will clear the pdx registry and
            // it will remain cleared during the query execution
            // but for concurrent tests, other threads might put domain classes
            // back into the registry while the query is executing
            Log.getLogWriter().info(
                "Clearing the pdx registry with a test hook");
            ((GemFireCacheImpl) theCache).getPdxRegistry().flushCache();
          }
          Log.getLogWriter().info("Executing query " + queries[i].toString());
          result = query.execute();
          Log.getLogWriter().info("Done executing " + queries[i].toString());
          
        } catch (QueryExecutionTimeoutException e1) {
          if (!ignoreQueryExecTimeOutException) {
            throw e1;
          } else {
            Log.getLogWriter().info(
                "Caught and ignored QueryExecutionTimeoutException:  "
                    + e1.getMessage());
            return;
          }
        } finally {
          if (pdxReadSerialized) { // set the class loader back to one that
                                   // includes pdx domain classes
            Log.getLogWriter().info(
                "Setting class loader back to one that refers to domain classes: "
                    + previousCL);
            Thread.currentThread().setContextClassLoader(previousCL);
          }
        }

      } catch (Exception e) {
        throw new TestException("Caught exception during query execution"
            + TestHelper.getStackTrace(e));
      }
    }
  }

  /**
   * Creates a new object with the given <code>name</code> to add to a region.
   * 
   * @see BaseValueHolder
   * @see RandomValues
   */
  protected Object getObjectToAdd(String classname) {
    Object obj = null;
    try {
      Class cls = Class.forName( classname, true, Thread.currentThread().getContextClassLoader());
      obj = cls.newInstance();
    }
    catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      throw new TestException(e.toString());
    }
    catch (InstantiationException e) {
      // TODO Auto-generated catch block
      throw new TestException(e.toString());
    }
    catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      throw new TestException(e.toString());
    }
    return obj;
  }

  /**
   * Returns the "updated" value of the object with the given <code>name</code>.
   * 
   * @see BaseValueHolder#getAlternateValueHolder
   */
  protected Object getUpdateObject(String classname) {
    Object obj = null;
    try {
      Class cls = Class.forName( classname, true, Thread.currentThread().getContextClassLoader());
      obj = cls.newInstance();
    }
    catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      throw new TestException(e.toString());
    }
    catch (InstantiationException e) {
      // TODO Auto-generated catch block
      throw new TestException(e.toString());
    }
    catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      throw new TestException(e.toString());
    }
    return obj;
  }

  /**
   * Returns the <code>CacheListener</code> that is installed on regions created
   * by this test.
   * 
   * @see ETListener
   */
  protected CacheListener getCacheListener() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType);
    if (objectType.indexOf("Pdx") > 0 || objectType.indexOf("Versioned") > 0) { // is
                                                                                // pdx
                                                                                // test
      return new PdxETListener(isCarefulValidation);
    } else {
      return new ETListener(isCarefulValidation);
    }
  }

  protected int getNumVMsWithListeners() {
    throw new TestException(
        "getNumVMsWithListeners must be implemented in a subclass");
  }

  /**
   * Creates the root region in the {@link Cache} used in this test according to
   * the configuration in a {@link RegionDefinition}.
   * 
   * @see RegionDefinition#createRegionDefinition()
   * @see RegionDefinition#createRootRegion
   */
  protected void createRootRegions() {
    CacheListener listener = queryTest.getCacheListener();
    CacheHelper.createCache("cache1");
    initPdxDiskStore();
    for (int i = 0; i < numOfRegions; i++) {
      String regionDescriptionName = "region" + (i + 1);
      AttributesFactory factory = RegionHelper
          .getAttributesFactory(regionDescriptionName);
      factory.addCacheListener(listener);
      RegionHelper.createRegion(REGION_NAME + ("" + (i)), factory);
    }
  }

  /**
   * Creates the cache based on the given xmlFile
   * 
   */
  protected void createCacheFromXml(String xmlFilename) {
    CacheHelper.createCacheFromXml(xmlFilename);
  }
  // ========================================================================
  // other methods

  /**
   * Return a random operation from the hydra parameter specified by whichPrm.
   * 
   * @param whichPrm
   *          - the hydra parameter that is a list of operations
   * @param disallowLocalEntryOps
   *          - if true, then do not return localInvalidate or localDestroy.
   *          This is used for regions that are mirrored, since localInvalidate
   *          and localDestroy are disallowed for mirrored regions.
   */
  protected int getOperation(Long whichPrm, boolean disallowLocalEntryOps) {
    long limit = 60000;
    long startTime = System.currentTimeMillis();
    int op = 0;
    do {
      String operation = TestConfig.tab().stringAt(whichPrm);
      if (operation.equals("add"))
        op = ADD_OPERATION;
      else if (operation.equals("update"))
        op = UPDATE_OPERATION;
      else if (operation.equals("invalidate"))
        op = INVALIDATE_OPERATION;
      else if (operation.equals("destroy"))
        op = DESTROY_OPERATION;
      else if (operation.equals("read"))
        op = READ_OPERATION;
      else if (operation.equals("localInvalidate"))
        op = LOCAL_INVALIDATE_OPERATION;
      else if (operation.equals("localDestroy"))
        op = LOCAL_DESTROY_OPERATION;
      else if (operation.equals("query"))
        op = QUERY_OPERATION;
      else if (operation.equals("countquery"))
        op = COUNT_QUERY_OPERATION;
      else if (operation.equals("createIndex"))
        op = CREATE_INDEX_OPERATION;
      else if (operation.equals("removeIndex"))
        op = REMOVE_INDEX_OPERATION;
      else if (operation.equals("pdxStringQuery"))
        op = PDXSTRING_QUERY_OPERATION;
      else
        throw new TestException("Unknown entry operation: " + operation);
      if (System.currentTimeMillis() - startTime > limit) {
        // could not find a suitable operation in the time limit; there may be
        // none available
        throw new TestException("Could not find an operation in " + limit
            + " millis; disallowLocalEntryOps is " + true
            + "; check that the operations list has allowable choices");
      }
    } while (disallowLocalEntryOps
        && ((op == LOCAL_INVALIDATE_OPERATION) || (op == LOCAL_DESTROY_OPERATION)));
    return op;
  }

  static protected void logExecutionNumber() {
    long exeNum = EventBB.getBB().getSharedCounters()
        .incrementAndRead(EventBB.EXECUTION_NUMBER);
    Log.getLogWriter().info("Beginning task with execution number " + exeNum);
  }

  /**
   * Called when the test gets a RegionDestroyedException. Sometimes the test
   * expects this exception, sometimes not. Check for error scenarios and throw
   * an error if the test should not get the RegionDestroyedException.
   * 
   * @param aRegion
   *          - the region that supposedly was destroyed and triggered the
   *          RegionDestroyedException
   * @param anException
   *          - the exception that was thrown.
   */
  protected void handleRegionDestroyedException(Region aRegion,
      RegionDestroyedException anException) {
    if (isCarefulValidation) {
      // no concurrent threads destroying regions, so should not get
      // RegionDestroyedException
      throw new TestException(TestHelper.getStackTrace(anException));
    } else {

      // make sure the region destroyed is this region
      if (!anException.getRegionFullPath().equals(aRegion.getFullPath())) {
        TestException te = new TestException(
            "Got a RegionDestroyedException when operating on region "
                + TestHelper.regionToString(aRegion, false)
                + ", but the region destroyed is '"
                + anException.getRegionFullPath() + "'");
        te.initCause(anException);
        throw te;
      }

      // Note: the test only creates a region with a given name once. Once that
      // region
      // has been destroyed, the test will never create another region with the
      // same name
      boolean isDestroyed = aRegion.isDestroyed();
      if (isDestroyed) {
        // Make sure it really is destoyed and is not causing the
        // RegionDestroyedException to be
        // thrown because one of its subregions was destroyed.
        Log.getLogWriter().info(
            "Got " + RegionDestroyedException.class.getName() + " on "
                + TestHelper.regionToString(aRegion, false)
                + "; exception expected, continuing test");
      } else { // the region was not destroyed, but we got
               // RegionDestroyedException anyway
        throw new TestException("Bug 30645 (likely): isDestroyed returned "
            + isDestroyed + " for region "
            + TestHelper.regionToString(aRegion, false)
            + ", but a region destroyed exception was thrown: "
            + TestHelper.getStackTrace(anException));
      }
    }
  }


  /**
   * Verify that the given key in the given region is invalid (has a null
   * value). If not, then throw an error. Checking is done locally, without
   * invoking a cache loader or doing a net search.
   * 
   * @param aRegion
   *          The region containing key
   * @param key
   *          The key that should have a null value
   */
  protected void verifyObjectInvalidated(Region aRegion, Object key) {
    StringBuffer errStr = new StringBuffer();
    boolean containsKey = aRegion.containsKey(key);
    if (!containsKey)
      errStr.append("Unexpected containsKey " + containsKey + " for key " + key
          + " in region " + TestHelper.regionToString(aRegion, false) + "\n");
    boolean containsValueForKey = aRegion.containsValueForKey(key);
    if (containsValueForKey)
      errStr.append("Unexpected containsValueForKey " + containsValueForKey
          + " for key " + key + " in region "
          + TestHelper.regionToString(aRegion, false) + "\n");
    Region.Entry entry = aRegion.getEntry(key);
    if (entry == null)
      errStr.append("getEntry for key " + key + " in region "
          + TestHelper.regionToString(aRegion, false) + " returned null\n");
    Object entryKey = entry.getKey();
    if (!entryKey.equals(key))
      errStr.append("getEntry.getKey() " + entryKey + " does not equal key "
          + key + " in region " + TestHelper.regionToString(aRegion, false)
          + "\n");
    Object entryValue = entry.getValue();
    if (entryValue != null)
      errStr.append("Expected getEntry.getValue() "
          + TestHelper.toString(entryValue) + " to be null.\n");
    if (errStr.length() > 0)
      throw new TestException(errStr.toString());
  }

  /**
   * Verify that the given key in the given region is destroyed (has no
   * key/value). If not, then throw an error. Checking is done locally, without
   * invoking a cache loader or doing a net search.
   * 
   * @param aRegion
   *          The region contains the destroyed key
   * @param key
   *          The destroyed key
   */
  protected void verifyObjectDestroyed(Region aRegion, Object key) {
    StringBuffer errStr = new StringBuffer();
    boolean containsKey = aRegion.containsKey(key);
    if (containsKey)
      errStr.append("Unexpected containsKey " + containsKey + " for key " + key
          + " in region " + TestHelper.regionToString(aRegion, false) + "\n");
    boolean containsValueForKey = aRegion.containsValueForKey(key);
    if (containsValueForKey)
      errStr.append("Unexpected containsValueForKey " + containsValueForKey
          + " for key " + key + " in region "
          + TestHelper.regionToString(aRegion, false) + "\n");
    Region.Entry entry = aRegion.getEntry(key);
    if (entry != null)
      errStr.append("getEntry for key " + key + " in region "
          + TestHelper.regionToString(aRegion, false)
          + " returned was non-null; getKey is " + entry.getKey()
          + ", value is " + TestHelper.toString(entry.getValue()) + "\n");
    if (errStr.length() > 0)
      throw new TestException(errStr.toString());
  }

  /**
   * Create the pdx disk store if one was specified.
   * 
   */
  private void initPdxDiskStore() {
    if (CacheHelper.getCache().getPdxPersistent()) {
      String pdxDiskStoreName = TestConfig.tab().stringAt(
          CachePrms.pdxDiskStoreName, null);
      if (pdxDiskStoreName != null) {// pdx disk store name was specified
        if (CacheHelper.getCache().findDiskStore(pdxDiskStoreName) == null) {
          DiskStoreHelper.createDiskStore(pdxDiskStoreName);
        }
      }
    }
  }
}

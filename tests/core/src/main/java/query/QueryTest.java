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
package query;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Random;
import java.util.Set;

import event.*;
import parReg.ParRegPrms;
import parReg.ParRegUtil;
import pdx.PdxTest;
import pdx.PdxETListener;
import pdx.PdxTestVersionHelper;
import query.index.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import util.*;
import hydra.*;
import hydra.CachePrms;
import objects.*;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.tools.gfsh.app.commands.index;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryExecutionTimeoutException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.functional.StructSetOrResultsSet;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.pdx.PdxInstance;

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

  public static final String ID_COMPACT_RANGE_INDEX = "idCompactRangeIndex";
  public static final String STATUS_COMPACT_RANGE_INDEX = "statusCompactRangeIndex";
  public static final String ID_HASH_INDEX = "idHashIndex";
  public static final String STATUS_HASH_INDEX = "statusHashIndex";
  public static final String ID_RANGE_INDEX = "idRangeIndex";
  public static final String STATUS_RANGE_INDEX = "statusRangeIndex";
  public static final String MAP_RANGE_INDEX_1 = "mapRangeIndex1";
  public static final String MAP_RANGE_INDEX_2 = "mapRangeIndex2";

  static protected final String enumWhereClause1 = " where aDay.name = 'Wednesday'";
  static protected final String enumWhereClause2 = " where aDay = null";

  protected static boolean pdxReadSerialized = false;
  // value of pdxReadSerialized in the cache
  static protected boolean statisticsWrittenToBB = false;
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
  protected boolean useEvictionController;
  // true if the test is using an eviction controller, false otherwise
  protected int maxObjects;
  // the maximum number of objects to allow in the region
  protected DistributedLockService distLockService;
  // the distributed lock service for this VM
  protected boolean isMirrored;
  // for entry event tests, indicates if the region this VM is operating on
  // is mirrored
  protected int[][] countQueryResults;
  // for validation of countresults in serial query tests.
  public Pool pool;
  // String prefixes for event callback object
  protected static final String createCallbackPrefix = "Create event originated in pid ";
  protected static final String updateCallbackPrefix = "Update event originated in pid ";
  protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
  protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
  protected static AtomicLong numQueries = new AtomicLong(0);
  // lock names
  protected static String LOCK_SERVICE_NAME = "MyLockService";
  protected static String LOCK_NAME = "MyLock";

  // root region name
  public static final String REGION_NAME = "QueryRegion";

  protected static final String REGION_WITHOUT_INDEX = "noIndex";

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

  public synchronized static void HydraTask_initialize_withIndexTestHook() {
    class IndexTestHook implements IndexManager.TestHook {
      public boolean indexCreatedAsPartOfGII;

      public void hook(int spot) throws RuntimeException {
        if (spot == 1) {
          throw new RuntimeException(
              "Index is not created as part of Region GII.");
        }
      }
    }
    ;
    IndexManager.testHook = new IndexTestHook();

    HydraTask_initialize();
    // Check if index are created.
    queryTest.validateIndexPresence();
  }

  private void validateIndexPresence() {
    String[] regions = new String[] { "QueryRegion0", "QueryRegion1",
        "QueryRegion2" };
    Cache c = CacheUtil.getCache();
    for (int i = 0; i < regions.length; i++) {
      Region r = c.getRegion(regions[i]);
      int numIndexes = r.getRegionService().getQueryService().getIndexes(r)
          .size();
      if (numIndexes != 2/* Number of indexes created in region */) {
        throw new TestException(
            "Expected number of indexes not found with region " + r.getName()
                + ", expected: 2 but found:" + numIndexes);
      }
    }
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
      // aRegion.putAll(aMap);
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

  public static void HydraTask_compareQueryResultsBetweenIndexRegions() {
    if (queryTest != null) {
      queryTest.compareQueryResultsBetweenIndexRegions();
    }
  }

  // methods to add/update/invalidate/destroy an object in a region

  /**
   * @see #HydraTask_doEntryOperations
   */
  protected void doEntryAndQueryOperations() {
    int regionNumber = (new Random()).nextInt(numOfRegions);
    Region aRegion = CacheHelper.getCache().getRegion(
        REGION_NAME + ("" + regionNumber));
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
            QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_REGION_OPS);
            break;
          case INVALIDATE_OPERATION:
            invalidateObject(aRegion, false);
            QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_REGION_OPS);
            break;
          case DESTROY_OPERATION:
            destroyObject(aRegion, false);
            QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_REGION_OPS);
            break;
          case UPDATE_OPERATION:
            updateObject(aRegion);
            QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_REGION_OPS);
            break;
          case READ_OPERATION:
            readObject(aRegion);
            break;
          case LOCAL_INVALIDATE_OPERATION:
            invalidateObject(aRegion, true);
            QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_REGION_OPS);
            break;
          case LOCAL_DESTROY_OPERATION:
            destroyObject(aRegion, true);
            QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_REGION_OPS);
            break;
          case QUERY_OPERATION:
            doQuery(true);
            break;
          case COUNT_QUERY_OPERATION:
            doCountQuery(true);
            break;
          case CREATE_INDEX_OPERATION:
            createIndex();
            break;
          case REMOVE_INDEX_OPERATION:
            removeIndex();
            break;
          case PDXSTRING_QUERY_OPERATION:
            doPdxStringQuery();
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

  protected void compareQueryResultsBetweenIndexRegions() {
    String[] regions = new String[] { "QueryRegion0", "QueryRegion1",
        "QueryRegion2" };
    Cache c = CacheUtil.getCache();
    String qStr = "SELECT * FROM /";
    String[] filter = new String[] { " p where p.ID > 10",
        " p where p.ID < 10", " p where p.ID = 10",
        " p where p.status = 'active'", " p where p.status != 'active'",
        " p where p.status = 'inactive'" };

    for (int x = 0; x < filter.length; x++) {
      String s[] = new String[2];
      for (int i = 0; i < regions.length; i++) {
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];

        Region r = c.getRegion(regions[i]);

        try {
          Query q = r.getRegionService().getQueryService()
              .newQuery(qStr + r.getName() + filter[x]);
          s[0] = q.getQueryString();
          Log.getLogWriter().info("Executing query: " + s[0]);
          sr[0][0] = (SelectResults) q.execute();
        } catch (Exception ex) {
          throw new TestException("Failed to execute the query. Query :" + s[0]
              + " Exception :" + ex.getMessage(), ex);
        }
        if (!observer.isIndexesUsed) {
          throw new TestException("Index not used for query. " + s[0]);
        }

        // Query using no index.
        try {
          Query q = r.getRegionService().getQueryService()
              .newQuery(qStr + r.getName() + REGION_WITHOUT_INDEX + filter[x]);
          s[1] = q.getQueryString();
          sr[0][1] = (SelectResults) q.execute();
        } catch (Exception ex) {
          throw new TestException(
              "Failed to execute the query on no index region.");
        }

        // compare.
        // ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1, s);
        compareQueryResultsWithoutAndWithIndexes(sr, 1, false, s);
      }
    }
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

  public void compareQueryResultsWithoutAndWithIndexes(Object[][] r, int len,
      boolean checkOrder, String queries[]) {

    Collection coll1 = null;
    Collection coll2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
    for (int j = 0; j < len; j++) {
      type1 = ((SelectResults) r[j][0]).getCollectionType().getElementType();
      type2 = ((SelectResults) r[j][1]).getCollectionType().getElementType();
      if ((type1.getClass().getName()).equals(type2.getClass().getName())) {
        Log.getLogWriter().info(
            "Both SelectResults are of the same Type i.e.--> "
                + ((SelectResults) r[j][0]).getCollectionType()
                    .getElementType());
      } else {
        Log.getLogWriter().info(
            "Classes are : " + type1.getClass().getName() + " "
                + type2.getClass().getName());
        throw new TestException(
            "FAILED:Select result Type is different in both the cases."
                + "; failed query=" + queries[j]);
      }
      if (((SelectResults) r[j][0]).size() == ((SelectResults) r[j][1]).size()) {
        Log.getLogWriter().info(
            "Both SelectResults are of Same Size i.e.  Size= "
                + ((SelectResults) r[j][1]).size());
      } else {
        Log.getLogWriter().info("#### SR[0] : \n" + Utils.printResult(r[j][0]));
        Log.getLogWriter().info("#### SR[1] : \n" + Utils.printResult(r[j][1]));

        throw new TestException(
            "FAILED:SelectResults size is different in both the cases. Size1="
                + ((SelectResults) r[j][0]).size() + " Size2 = "
                + ((SelectResults) r[j][1]).size() + "; failed query="
                + queries[j]);
      }
      if (checkOrder) {
        coll2 = (((SelectResults) r[j][1]).asList());
        coll1 = (((SelectResults) r[j][0]).asList());
      } else {
        coll2 = (((SelectResults) r[j][1]).asSet());
        coll1 = (((SelectResults) r[j][0]).asSet());
      }
      // boolean pass = true;
      itert1 = coll1.iterator();
      itert2 = coll2.iterator();
      while (itert1.hasNext()) {
        Object p1 = itert1.next();
        if (!checkOrder) {
          itert2 = coll2.iterator();
        }

        boolean exactMatch = false;
        while (itert2.hasNext()) {
          Object p2 = itert2.next();
          if (p1 instanceof Struct) {
            Object[] values1 = ((Struct) p1).getFieldValues();
            Object[] values2 = ((Struct) p2).getFieldValues();
            // assertEquals(values1.length, values2.length);
            if (values1.length != values2.length) {
              throw new TestException("Struct size is different");
            }
            boolean elementEqual = true;
            for (int i = 0; i < values1.length; ++i) {
              elementEqual = elementEqual
                  && ((values1[i] == values2[i]) || values1[i]
                      .equals(values2[i]));
            }
            exactMatch = elementEqual;
          } else {
            exactMatch = (p2 == p1) || p2.equals(p1);
          }
          if (exactMatch || checkOrder) {
            break;
          }
        }
        if (!exactMatch) {
          throw new TestException(
              "Atleast one element in the pair of SelectResults supposedly identical, is not equal "
                  + "; failed query=" + queries[j]);
        }
      }
    }
  }

  protected void createIndex() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    // if(!objectType.equals("objects.Portfolio")) {
    // return;
    // }
    IndexTest indexTest = new IndexTest();
    int regionNumber = (new Random()).nextInt(numOfRegions);
    indexTest.createIndex(REGION_NAME + ("" + regionNumber));
  }

  protected void createIndex_task() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    // if(!objectType.equals("objects.Portfolio")) {
    // return;
    // }
    IndexTest indexTest = new IndexTest();
    for (int i = 0; i < numOfRegions; i++) {
      indexTest.createIndex(i);
    }
  }

  protected void removeIndex() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    // if(!objectType.equals("objects.Portfolio")) {
    // return;
    // }
    IndexTest indexTest = new IndexTest();
    int regionNumber = (new Random()).nextInt(numOfRegions);
    indexTest.removeIndex(REGION_NAME + ("" + regionNumber));
  }

  protected void addObject(Region aRegion, boolean logAddition) {
    String name = NameFactory.getNextPositiveObjectNameInLimit(maxObjects);
    Object anObj = getObjectToAdd(name);
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
      Region rWithoutIndex = aRegion.getCache().getRegion(
          aRegion.getName() + REGION_WITHOUT_INDEX);
      if (rWithoutIndex != null) {
        rWithoutIndex.put(name, anObj, callback);
      }
    } catch (RegionDestroyedException e) {
      handleRegionDestroyedException(aRegion, e);
    } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    long numPut = EventBB.incrementCounter("EventBB.NUM_CREATE",
        EventBB.NUM_CREATE);

  }

  protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
    Set aSet = aRegion.keys();
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
        Region rWithoutIndex = aRegion.getCache().getRegion(
            aRegion.getName() + REGION_WITHOUT_INDEX);
        if (rWithoutIndex != null) {
          rWithoutIndex.localInvalidate(name, callback);
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
        Region rWithoutIndex = aRegion.getCache().getRegion(
            aRegion.getName() + REGION_WITHOUT_INDEX);
        if (rWithoutIndex != null) {
          rWithoutIndex.invalidate(name, callback);
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
        Region rWithoutIndex = aRegion.getCache().getRegion(
            aRegion.getName() + REGION_WITHOUT_INDEX);
        if (rWithoutIndex != null) {
          rWithoutIndex.localDestroy(name, callback);
        }
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
        Region rWithoutIndex = aRegion.getCache().getRegion(
            aRegion.getName() + REGION_WITHOUT_INDEX);
        if (rWithoutIndex != null) {
          rWithoutIndex.destroy(name, callback);
        }
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
    Object newObj = getUpdateObject((String) name);
    try {
      String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
      Log.getLogWriter().info(
          "updateObject: replacing name " + name + " with "
              + TestHelper.toString(newObj) + "; old value is "
              + TestHelper.toString(anObj) + ", callback is " + callback);
      aRegion.put(name, newObj, callback);
      Log.getLogWriter().info("Done with call to put (update)");
      Region rWithoutIndex = aRegion.getCache().getRegion(
          aRegion.getName() + REGION_WITHOUT_INDEX);
      if (rWithoutIndex != null) {
        rWithoutIndex.put(name, newObj, callback);
      }
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
    Set aSet = aRegion.keys();
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

  /**
   * Executes the query over the region
   */
  protected void doQuery(boolean logAddition) {
    String queries[] = null;
    final int threshold = 30000;
    
    String regionName1 = randRgn();
    Region region1 = RegionHelper.getRegion(regionName1);
    String regionName2 = randRgn();
    Region region2 = RegionHelper.getRegion(regionName2);
    String regionName3 = randRgn();
    Region region3 = RegionHelper.getRegion(regionName3);

    boolean isPdxTest = TestConfig.tab().stringAt(QueryPrms.objectType)
    .indexOf("VersionedPortfolio") >= 0;
    boolean isPartitioned = region1 instanceof PartitionedRegion
                             || region2 instanceof PartitionedRegion
                             || region3 instanceof PartitionedRegion;
    boolean aboveThresholdRegionSize = maxObjects > threshold && queryLimit == -1;
    // Allow queries to exceed the threshold if there is a limit in place

    if (aboveThresholdRegionSize) {
      // regions are large so make queries not return whole regions (or half
      // either!)
      if (isPdxTest) { // we have pdxID, pdxStatus etc fields
        queries = new String[] {
            "select distinct * from " + regionName1 + " where ID = 0",
            "select distinct * from " + regionName1 + " where ID <= 500",
            "select distinct * from " + regionName1 + " where ID <= 1000",
            "select distinct * from " + regionName1 + " where pdxID = 0",
            "select distinct * from " + regionName1 + " where pdxID <= 500",
            "select distinct * from " + regionName1 + " where pdxID <= 1000",
            "select distinct * from " + regionName1
                + " where ID = 0 and pdxID = 0",
            "select distinct * from " + regionName1
                + " where ID <= 500 and pdxID <= 500",
            "select distinct * from " + regionName1
                + " where ID <= 1000 and pdxID <= 1000" };
      } else {
        queries = new String[] {
            // IMPORTANT: If you add/delete queries make sure to change 
            // verifySanityForQueryResultsNonPdx() method accordingly
            "select distinct * from " + regionName1 + " where ID = 0",
            "select distinct * from " + regionName1 + " where ID <= 500",
            "select distinct * from " + regionName1 + " where ID <= 800",
            "select distinct * from " + regionName1 + " where ID <= 1000" };
      }
    } else {
      if (isPdxTest) {
        queries = new String[] {
            "select distinct * from " + regionName1 + enumWhereClause1,
            "select distinct * from " + regionName1 + enumWhereClause2,
            "select distinct * from " + regionName1 + " where ID = 0",
            "select distinct * from " + regionName1
                + " where status = 'active'",
            "select distinct * from " + regionName1 + ", " + regionName2,
            // Avoiding 3 region queries for the time being
            // "select distinct * from " + regionName1 + ", " + regionName2 +
            // ", " + regionName3,
            "select distinct * from " + regionName1
                + " where ID != 0 AND status = 'active'",
            "select distinct * from " + regionName1 + " where pdxID = 0",
            "select distinct * from " + regionName1
                + " where pdxStatus = 'active'",
            "select distinct * from " + regionName1
                + " where pdxID != 0 AND pdxStatus = 'active'",
            "select distinct * from " + regionName1
                + " where ID = 0 and pdxID = 0",
            "select distinct * from " + regionName1
                + " where status = 'active' and pdxID = 'active'",
            "select distinct * from "
                + regionName1
                + " where (ID != 0) AND (pdxID != 0) and (status = 'active') and (pdxStatus = 'active')" };
        if (isPartitioned) {
          // No Join queries for PR
          queries = new String[] {
              "select distinct * from " + regionName1 + " where ID = 0",
              "select distinct * from " + regionName1
                  + " where status = 'active'",
              "select distinct * from " + regionName1
                  + " where ID != 0 AND status = 'active'",
              "select distinct * from " + regionName1 + " where pdxID = 0",
              "select distinct * from " + regionName1
                  + " where pdxStatus = 'active'",
              "select distinct * from " + regionName1
                  + " where pdxID != 0 AND pdxStatus = 'active'",
              "select distinct * from " + regionName1
                  + " where ID = 0 and pdxID = 0",
              "select distinct * from " + regionName1
                  + " where status = 'active' and pdxID = 'active'",
              "select distinct * from "
                  + regionName1
                  + " where (ID != 0) AND (pdxID != 0) and (status = 'active') and (pdxStatus = 'active')" };
        }
      } else {
        // IMPORTANT: If you add/delete queries here make sure to change 
        // verifySanityForQueryResultsNonPdx() method accordingly
        queries = new String[] {
            "select distinct * from " + regionName1 + " where ID = 0",
            "select distinct * from " + regionName1
                + " where status = 'active'",
            "select distinct * from " + regionName1
                + " where ID != 0 AND status = 'active'",
            "select distinct * from " + regionName1 + ", " + regionName2,
            "select distinct * from " + regionName1 + " pf1, " + regionName2
                + " pf2 where pf1.ID != 0 AND pf2.status = 'active'" 
             // Avoiding 3 region queries for the time being
            // "select distinct * from " + regionName1 + ", " + regionName2 +
            // ", " + regionName3
                };
        if (isPartitioned) {
          // No Join queries for PR
          queries = new String[] {
              // IMPORTANT: If you add/delete queries here, make sure to change 
              // verifySanityForQueryResultsNonPdx() method accordingly
              "select distinct * from " + regionName1 + " where ID = 0",
              "select distinct * from " + regionName1
                  + " where status = 'active'",
              "select distinct * from " + regionName1
                  + " where ID != 0 AND status = 'active'" };
        }
      }
    }
    // Add the limitClause if configured to do so
    if (queryLimit != -1) {
      for (int q = 0; q < queries.length; q++) {
        queries[q] += " limit " + queryLimit;
      }
    }

    // int i = ((new Random()).nextInt(queries.length));
    for (int i = 0; i < queries.length; i++) {
      Log.getLogWriter().info(" query = " + queries[i]);
      Query query = CacheHelper.getCache().getQueryService()
          .newQuery(queries[i]);
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
          Log.getLogWriter().info("Executing query " + queries[i]);
          long start = System.nanoTime();
          result = query.execute();
          QueryBB.getBB().getSharedCounters().add(QueryBB.TOTAL_QUERY_EXEC_TIME, System.nanoTime() - start);
          QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_QUERY_EXECS);
          IndexBB.getBB().getSharedCounters().increment(IndexBB.TOTAL_USES);
          Log.getLogWriter().info("Done executing queryNum:" + numQueries.incrementAndGet() + ": " + queries[i]);
          if(!isPdxTest) {
            verifySanityForQueryResultsNonPdx(i, aboveThresholdRegionSize, ((SelectResults)result), queries[i]);            
          }
          
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

        // Log.getLogWriter().info(Utils.printResult(result));
        // We have a limit, so validate it
        validateQueryLimit(result);
        if (result != null && result instanceof Collection) {
          Collection coll = (Collection) result;
          Log.getLogWriter().info("Size of result is :" + coll.size());
          if (isSerialExecution) {
            validatePdxQuery(queries[i], coll);
          }
          int structCount = 0;
          int portfolioCount = 0;
          int portfolioInStructCount = 0;
          for (Object resultElement : coll) {
            // Log.getLogWriter().info("  result element of class " +
            // resultElement.getClass().getName() + ": " + resultElement);
            if (resultElement instanceof PdxInstance) {
              resultElement = PdxTestVersionHelper.toBaseObject(resultElement);
            }
            if (resultElement instanceof Struct) {
              structCount++;
              Struct aStruct = (Struct) resultElement;
              // Log.getLogWriter().info("aStruct.getClass() is " +
              // aStruct.getClass());
              // Log.getLogWriter().info("aStruct.getStructType() is " +
              // aStruct.getStructType());
              // Log.getLogWriter().info("aStruct.getFieldValues() is " +
              // Arrays.toString(aStruct.getFieldValues()));
              Object[] fieldValues = aStruct.getFieldValues();
              for (Object aValue : fieldValues) {
                portfolioInStructCount++;
                if (aValue instanceof PdxInstance) {
                  aValue = PdxTestVersionHelper.toBaseObject(aValue);
                }
                if (aValue instanceof Portfolio) {
                  aValue.toString(); // toString will access all fields in
                                     // Portfolio just to make sure we can
                } else {
                  throw new TestException(
                      "Query result contains struct that contains unexpected element: "
                          + aValue + ", struct element is of class "
                          + aValue.getClass().getName());
                }
              }
            } else if (resultElement instanceof Portfolio) {
              portfolioCount++;
              // Log.getLogWriter().info("Result is domain object: " +
              // resultElement);
            } else {
              throw new TestException("Query result element is unexpected: "
                  + resultElement + ", result element is of class "
                  + resultElement.getClass().getName());
            }
          }
          Log.getLogWriter().info(
              "Result of query contained " + portfolioCount
                  + " Portfolio objects, " + structCount
                  + " structs with all structs containing "
                  + portfolioInStructCount
                  + " Portfolio objects; total number of Portfolio objects: "
                  + (portfolioCount + portfolioInStructCount));
        } else {
          throw new TestException("Result of query is " + result + " of class "
              + result.getClass().getName() + ", but expected a Collection");
        }
      } catch (Exception e) {
        throw new TestException("Caught exception during query execution"
            + TestHelper.getStackTrace(e));
      }
    }
  }

  /**
   * 
   * @param queryNum
   * @param partitioned
   * @param aboveThresholdRegionSize
   * @param results
   * @param queryStr
   * @param expectedStatus
   */
  static void verifySanityForQueryResultsNonPdx(int queryNum,
      boolean aboveThresholdRegionSize,
      SelectResults results, String queryStr) {
    if (aboveThresholdRegionSize) {
      int[] id = new int[] { 0, 500, 800, 1000 };
      sanityCheckUsingId(results, id[queryNum], queryStr);
    } else  {
      if (queryNum == 0) {
        sanityCheckUsingId(results, 0, queryStr);
      } else {
        if (queryNum == 1) {
          sanityCheckUsingStatusAndId(results, -1 , "active", queryStr);
        } else if(queryNum == 2) {
          sanityCheckUsingStatusAndId(results, 0 , "active", queryStr);
        } else {
          // This is non-partitioned regions with multiple regions in fromclause.
          for (Object resultElement : results) {
            if (resultElement instanceof Struct) {
              Struct aStruct = (Struct) resultElement;
              Object[] fieldValues = aStruct.getFieldValues();
              for (Object aValue : fieldValues) {
                if (!(aValue instanceof Portfolio)) {
                  throw new TestException(
                      "Query result contains struct that contains unexpected element: "
                          + aValue + ", struct element is of class "
                          + aValue.getClass().getName());
                } 
              }
              if (queryNum == 4) {
                Portfolio pf1 = (Portfolio) fieldValues[0];
                Portfolio pf2 = (Portfolio) fieldValues[1];
                if (pf1.ID == 0) {
                  throw new TestException("Sanity check failed for query: " + queryStr
                      + " Expected pf1.ID to be != " + 0
                      + " for all objects but found to be 0 ");
                }
                if(!pf2.status.equals("active")) {
                  throw new TestException("Sanity check failed for query: " + queryStr
                      + " Expected pf2.status to be 'active' "
                      + " for all objects but found: " + pf2.status);
                }
              }
            } else {
              throw new TestException("Query result element is unexpected: "
                  + resultElement + ", result element is of class "
                  + resultElement.getClass().getName());
            }
          }
        }
      }
    }
  }
  
  /**
   * Sanity checks using ID
   * @param results
   * @param id
   * @param queryStr
   */
  static void sanityCheckUsingId(SelectResults results, int id, String queryStr) {
//    if (results.size() > (id + 1)) {
//      Log.getLogWriter().info("Result = " + Utils.printResult(results));
//      throw new TestException("Sanity check failed for query: " + queryStr
//          + " Expected size to be <=" + (id + 1) + " but actual: "
//          + results.size());
//    }
    for (Object resultObj : results) {
      if (((Portfolio) resultObj).ID > id) {
        Log.getLogWriter().info("Result = " + Utils.printResult(results));
        throw new TestException("Sanity check failed for query: " + queryStr
            + " Expected (Portfolio)resultObj).ID to be <= " + id
            + " for all objects but found: " + ((Portfolio) resultObj).ID);
      }
    }
  }
  
 /**
  * Sanity checks using status
  * @param results
  * @param id
  * @param status
  * @param queryStr
  */
 static void sanityCheckUsingStatusAndId(SelectResults results, int id, String status, String queryStr) {
   for (Object resultObj : results) {
     if (!((Portfolio) resultObj).status.equals(status)) {
       Log.getLogWriter().info("Result = " + Utils.printResult(results));
       throw new TestException("Sanity check failed for query: " + queryStr
           + " Expected ((Portfolio) resultObj).status to be equal to " + status
           + " for all objects but found: " + ((Portfolio) resultObj).status);
     }
     if (id != -1) {
       if (((Portfolio) resultObj).ID == id) {
         Log.getLogWriter().info("Result = " + Utils.printResult(results));
         throw new TestException("Sanity check failed for query: " + queryStr
             + " Expected ((Portfolio) resultObj).id to be != " + id
             + " for all objects but found: " + ((Portfolio) resultObj).ID);
       }
     }
   }
 }
  
  /**
   * Contains very specific validation for certain pdx queries.
   * 
   * @param string
   *          The query String.
   */
  private void validatePdxQuery(String qStr, Collection coll) {
    if ((qStr.indexOf("aDay") < 0) && (qStr.indexOf("pdx") >= 0)) {
      return; // this is not a pdx query
    }

    boolean pdx661Behavior;
    String propValue = System
        .getProperty("gemfire.loadClassOnEveryDeserialization");
    if (propValue == null) { // not set; default to 662 (and later) behavior
      pdx661Behavior = false;
    } else {
      pdx661Behavior = propValue.equals("true");
    }

    ClassLoader previousCL = Thread.currentThread().getContextClassLoader();
    if (pdx661Behavior) { // ok to change the class loader (see
                          // PdxTest.initClassLoader for more information)
      // we want to validate with version2 classloader so we can see all fields
      try {
        String alternateVersionClassPath = System.getProperty("JTESTS")
            + File.separator + ".." + File.separator + ".." + File.separator
            + "testsVersions" + File.separator + "version2" + File.separator
            + "classes/";

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        cl = ClassLoader.getSystemClassLoader();

        ClassLoader versionCL = new URLClassLoader(new URL[] { new File(
            alternateVersionClassPath).toURL() }, cl);
        Log.getLogWriter().info(
            "Setting class loader to version2 for validation "
                + alternateVersionClassPath);
        Thread.currentThread().setContextClassLoader(versionCL);
      } catch (MalformedURLException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } // else 662 behavior; all threads in this jvm are using one version and we
      // cannot change it

    try {
      Log.getLogWriter().info("Validating " + qStr);
      Set<Region<?, ?>> rootRegions = CacheHelper.getCache().rootRegions();
      // assume one region in the query
      Region qRegion = null;
      for (Region aRegion : rootRegions) {
        if (qStr.indexOf(aRegion.getFullPath()) >= 0) {
          qRegion = aRegion;
          break;
        }
      }
      Log.getLogWriter().info(
          "Validating from region " + qRegion.getFullPath() + " of size "
              + qRegion.size());

      Set expectedResults = new HashSet();
      boolean ableToReadAllFields = true;
      boolean isNullQuery = qStr.indexOf(enumWhereClause2) >= 0;
      if ((qStr.indexOf(enumWhereClause1) >= 0) || isNullQuery) {
        for (Object key : qRegion.keySet()) {
          Object value = qRegion.get(key);
          if (value instanceof PdxInstance) {
            value = ((PdxInstance) value).getObject();
          }
          if ((value.getClass().getName()
              .equals("objects.PdxVersionedPortfolio"))
              || (value.getClass().getName()
                  .equals("objects.VersionedPortfolio"))) {
            Portfolio pf = (Portfolio) value;
            // Log.getLogWriter().info("Considering " + pf);
            try {
              String myVersionValue = PdxTest.getFieldValue(pf, "myVersion");
              if (myVersionValue.indexOf("version2") >= 0) { // version2 is the
                                                             // only version
                                                             // that includes
                                                             // field aDay
                                                             // so version1
                                                             // objects don't
                                                             // qualify for the
                                                             // result
                String dayValue = PdxTest.getFieldValue(pf, "aDay");
                Log.getLogWriter().info("day value is " + dayValue);
                if (isNullQuery) {
                  if (dayValue == null) {
                    expectedResults.add(pf);
                    // Log.getLogWriter().info("xxx after adding, expected is size "
                    // + expectedResults.size());
                  }
                } else { // non-null query, looking for Wednesday
                  if ((dayValue != null) && (dayValue.equals("Wednesday"))) {
                    expectedResults.add(pf);
                    // Log.getLogWriter().info("xxx after adding, expected is size "
                    // + expectedResults.size());
                  }
                }
              }
            } catch (NoSuchFieldException e) {
              Log.getLogWriter().info("Caught NoSuchFieldException");
              // do not include in expected results if field is not present
              ableToReadAllFields = false;
            }
          }
        }
      } else if ((qStr.indexOf("aDay") >= 0) || (qStr.indexOf("pdx") >= 0)) {
        throw new TestException("Test does not know how to validate " + qStr);
      } else { // not a pdx query; this method does not currently support
               // validation for this query
        return;
      }

      logRegions();
      // we can check the exact size of the results if we were able to read all
      // fields (meaning this
      // thread is running with version2 objects) and pdxReadSerialized is
      // false; if pdxReadSerialized
      // is true then the result set can contain either (or both) of domain
      // objects and PdxInstances...
      // we do not support comparing for equality a PdxInstance and a domain
      // object even if they represent
      // the same object, so for distinct queries we can have two objects in the
      // result set, one a PdxInstance
      // and one a domain object, that represent the same object (even when the
      // query used "distinct")
      boolean checkExactSize = ableToReadAllFields && !pdxReadSerialized;
      Log.getLogWriter().info(
          "Size of result is :" + coll.size() + ", expected result size is "
              + expectedResults.size());
      if (checkExactSize) { // we should have an exact count of the expected
                            // result size
        if (coll.size() != expectedResults.size()) {
          Log.getLogWriter().info("Expected results: " + expectedResults);
          Log.getLogWriter().info("Actual results: " + coll);
          throw new TestException("Expected result size of " + qStr + " to be "
              + expectedResults.size() + ", but it is " + coll.size());
        }
      } else { // we should have at least the expected result size
        if (coll.size() < expectedResults.size()) {
          Log.getLogWriter().info("Expected results: " + expectedResults);
          Log.getLogWriter().info("Actual results: " + coll);
          throw new TestException("Expected result size of " + qStr
              + " to be at least " + expectedResults.size() + ", but it is "
              + coll.size());
        }
      }
      Log.getLogWriter().info("Validated " + qStr);
    } finally { // put the original classLoader back if we set it above
      if (pdx661Behavior) {
        Log.getLogWriter().info(
            "Setting class loader back to what it was prior to validation: "
                + previousCL);
        Thread.currentThread().setContextClassLoader(previousCL);
      }
    }
  }

  private void logRegions() {
    Cache theCache = CacheHelper.getCache();
    Set<Region<?, ?>> rootRegions = theCache.rootRegions();
    StringBuffer aStr = new StringBuffer();
    for (Region aRegion : rootRegions) {
      aStr.append(aRegion.getFullPath() + ":\n");
      for (Object key : aRegion.keySet()) {
        aStr.append("  " + key + " " + aRegion.get(key) + "\n");
      }
    }
    Log.getLogWriter().info(aStr.toString());
  }

  /**
   * Executes the query over the region
   */
  protected void doCountQuery(boolean logAddition) {
    String queries[] = null;
    final int threshold = 30000;
    boolean isPdxTest = TestConfig.tab().stringAt(QueryPrms.objectType)
        .equals("objects.PdxVersionedPortfolio");
    boolean isPr = TestConfig.tab().stringAt(RegionPrms.dataPolicy)
        .equals("partition");
    // Allow queries to exceed the threshold if there is a limit in place
    if (isPr) {
      if (isPdxTest) {
        queries = new String[] {
            "select count(*) from " + randRgn() + " where id = 0",
            "select count(*) from " + randRgn() + " where status = 'active'",
            "select count(*) from " + randRgn()
                + " where id != 0 AND status = 'active'",
            "select count(*) from " + randRgn() + " where pdxID = 0",
            "select count(*) from " + randRgn() + " where pdxStatus = 'active'",
            "select count(*) from " + randRgn()
                + " where pdxID != 0 AND pdxStatus = 'active'",
            "select count(*) from " + randRgn() + " where id = 0 and pdxID = 0",
            "select count(*) from " + randRgn()
                + " where status = 'active' and pdxID = 'active'",
            "select count(*) from "
                + randRgn()
                + " where (id != 0) AND (pdxID != 0) and (status = 'active') and (pdxStatus = 'active')" };
      } else {
        queries = new String[] {
            "select count(*) from " + randRgn() + " where id = 0",
            "select count(*) from " + randRgn() + " where status = 'active'",
            "select count(*) from " + randRgn()
                + " where id != 0 AND status = 'active'" };
      }
    } else {
      if (isPdxTest) {
        queries = new String[] {
            "select count(*) from " + randRgn() + " where ID = 0",
            "select count(*) from " + randRgn() + " where status = 'active'",
            "select count(*) from " + randRgn() + ", " + randRgn(),
            "select count(*) from " + randRgn() + ", " + randRgn() + ", "
                + randRgn(),
            "select count(*) from " + randRgn()
                + " where ID != 0 AND status = 'active'",
            "select count(*) from " + randRgn() + " where pdxID = 0",
            "select count(*) from " + randRgn() + " where pdxStatus = 'active'",
            "select count(*) from " + randRgn()
                + " where pdxID != 0 AND pdxStatus = 'active'",
            "select count(*) from " + randRgn() + " where ID = 0 and pdxID = 0",
            "select count(*) from " + randRgn()
                + " where status = 'active' and pdxID = 'active'",
            "select count(*) from "
                + randRgn()
                + " where (ID != 0) AND (pdxID != 0) and (status = 'active') and (pdxStatus = 'active')" };
      } else {
        queries = new String[] {
            "select count(*) from " + randRgn() + " where ID = 0",
            "select count(*) from " + randRgn() + " where status = 'active'",
            "select count(*) from " + randRgn() + ", " + randRgn(),
            "select count(*) from " + randRgn() + ", " + randRgn() + ", "
                + randRgn(),
            "select count(*) from " + randRgn()
                + " where ID != 0 AND status = 'active'" };
      }
    }
    // Add the limitClause if configured to do so
    if (queryLimit != -1) {
      for (int q = 0; q < queries.length; q++) {
        queries[q] += " limit " + queryLimit;
      }
    }
    countQueryResults = QueryBB.getCountResultsObject();
    if (countQueryResults == null) {
      countQueryResults = new int[queries.length][2];
    }
    int i = ((new Random()).nextInt(queries.length));
    Log.getLogWriter().info(" query = " + queries[i]);
    Query countReplacedQuery = null;
    if (queries[i].contains("count(*)")) {
      String countReplacedQueryStr = queries[i].replace("count(*)", "*");
      countReplacedQuery = CacheHelper.getCache().getQueryService()
          .newQuery(countReplacedQueryStr);
    }
    Query query = CacheHelper.getCache().getQueryService().newQuery(queries[i]);

    try {
      Object nonCountResult = null;
      // for pdx tests, we want the execute to occur without having domain
      // classes referenced from the
      // class loader IF the test is using pdxReadSerialized
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
      Object result;
      try {
        if (countReplacedQuery != null) {
          nonCountResult = countReplacedQuery.execute();
        }
        Cache theCache = CacheHelper.getCache();
        if (theCache != null) { // could be null during HA
          // clear the pdx type registry so with Darrel's new 662 changes we do
          // not have domain classes available during the query
          // note that for a serial test, this will clear the pdx registry and
          // it will remain cleared durin the query execution
          // but for concurrent tests, other threads might put domain classes
          // back into the registry while the query is executing
          Log.getLogWriter().info("Clearing the pdx registry with a test hook");
          ((GemFireCacheImpl) theCache).getPdxRegistry().flushCache();
        }
        Log.getLogWriter().info("Executing query " + queries[i]);
        result = query.execute();
        Log.getLogWriter().info("Done executing query " + queries[i]);
      } finally {
        if (pdxReadSerialized) { // set the class loader back to one that
                                 // includes pdx domain classes
          Log.getLogWriter().info(
              "Setting class loader back to one that refers to domain classes: "
                  + previousCL);
          Thread.currentThread().setContextClassLoader(previousCL);
        }
      }

      // Log.getLogWriter().info(Utils.printResult(result));
      // We have a limit, so validate it
      validateQueryLimit(result);
      if (result instanceof Collection) {
        Collection coll = (Collection) result;
        Log.getLogWriter().info("Size of result is :" + coll.size());
        int count = 0; // For count(*)
        for (Object resultElement : coll) {
          // Log.getLogWriter().info("  result element of class " +
          // resultElement.getClass().getName() + ": " + resultElement);
          if (resultElement instanceof Integer) {
            count = ((Integer) resultElement).intValue();
            countQueryResults[i][0] = count;
            countQueryResults[i][1] = ((SelectResults) nonCountResult).size();
            QueryBB.putCountStarResults(countQueryResults);
            if (count != ((SelectResults) nonCountResult).size()) {
              throw new TestException("Query result is unexpected: " + count
                  + ", result should be "
                  + ((SelectResults) nonCountResult).size());
            }
            // Log.getLogWriter().info("Result is domain object: " +
            // resultElement);
          } else {
            throw new TestException("Query result element is unexpected: "
                + resultElement + ", result element is of class "
                + resultElement.getClass().getName());
          }
        }
        Log.getLogWriter().info("Result of Count(*) query is" + count);
      } else {
        throw new TestException("Result of query is " + result + " of class "
            + result.getClass().getName() + ", but expected a Collection");
      }
    } catch (Exception e) {
      throw new TestException("Caught exception during query execution"
          + TestHelper.getStackTrace(e));
    }

  }

  protected void doPdxStringQuery() {
    String queries[] = null;
    final int threshold = 30000;
    boolean isPdxTest = TestConfig.tab().stringAt(QueryPrms.objectType)
        .equals("objects.PdxVersionedPortfolio");
    if (isPdxTest) {
      queries = new String[] {
          "select status from " + randRgn() + " where ID > 0",
          "select status from " + randRgn() + " where status > 'active'",
          "select status from " + randRgn() + " where status LIKE '%active'",
          "select status from " + randRgn()
              + " where ID != 0 AND status = 'active'",
          "select distinct status from " + randRgn() + " order by status" };
    } else {
      throw new TestException("Test only on pdx instances");
    }
    // Add the limitClause if configured to do so
    if (queryLimit != -1) {
      for (int q = 0; q < queries.length; q++) {
        queries[q] += " limit " + queryLimit;
      }
    }

    int i = ((new Random()).nextInt(queries.length));
    Log.getLogWriter().info(" query = " + queries[i]);

    Query query = CacheHelper.getCache().getQueryService().newQuery(queries[i]);
    Object result = null;

    try {
      try {
        Cache theCache = CacheHelper.getCache();
        Log.getLogWriter().info("Executing query: " + queries[i]);
        result = query.execute();
        Log.getLogWriter().info("Done executing query: " + queries[i]);
      } catch (Exception e) {
        Log.getLogWriter().error("Failed executing query: " + queries[i]);
        throw e;
      }

      validateQueryLimit(result);
      if (result != null && result instanceof Collection) {
        Collection coll = (Collection) result;
        Log.getLogWriter().info("Size of result is :" + coll.size());
        for (Object resultElement : coll) {
          if (!(resultElement instanceof String)) {
            throw new TestException("Query result element is unexpected: "
                + resultElement + ", result element is of class "
                + resultElement.getClass().getName());
          }
        }
      }
    } catch (Exception e) {
      throw new TestException("Caught exception during query execution: "
          + TestHelper.getStackTrace(e));
    }
  }

  /**
   * If {@link #queryLimit} is specified then validate that the limit was
   * obeyed. For tests that don't use a limit this is a noop. Bail out early if
   * the query returns a Tomen instead of a collection.
   * 
   * @param result
   *          the query results to check
   * @throws TestException
   *           if(results.size() > queryLimit)
   */
  protected void validateQueryLimit(Object result) {
    Log.getLogWriter().info("Validating Query LIMIT => " + queryLimit);
    if ((queryLimit != -1) && (result instanceof Collection)) {
      // We have a limit, so validate it
      int size = ((Collection) result).size();
      if (size > queryLimit) {
        String errorMsg = "Query Results exceeded the limit. "
            + "expected limit=" + queryLimit + " Actual size of the results="
            + size;
        throw new TestException(errorMsg);
      }
    }
  }

  protected String randRgn() {
    return ("/" + REGION_NAME + (new Random()).nextInt(numOfRegions));
  }

  // ========================================================================
  // methods that can be overridden for a more customized test

  /**
   * Must overridden in a subclass
   */
  protected void checkEventCounters() {
    throw new TestException(
        "checkEventCounters must be implmented in a subclass");
  }

  /**
   * Creates a new object with the given <code>name</code> to add to a region.
   * 
   * @see BaseValueHolder
   * @see RandomValues
   */
  protected Object getObjectToAdd(String name) {

    boolean useRandomValues = false;
    useRandomValues = TestConfig.tab().booleanAt(QueryPrms.useRandomValues,
        false);
    if (useRandomValues) {
      BaseValueHolder anObj = new ValueHolder(name, randomValues);
      return anObj;
    } else {
      long i = NameFactory.getPositiveNameCounter();
      int index = (int) (i % maxObjects);
      String objectType = TestConfig.tab().stringAt(QueryPrms.objectType);
      Object val = ObjectHelper.createObject(objectType, index);
      if (val instanceof Portfolio) {
        int payloadSize = TestConfig.tab().intAt(QueryPrms.payloadSize, -1);
        if (payloadSize > 0) {
          ((Portfolio) val).payload = new byte[payloadSize];
        }
      }
      return val;
    }
  }

  /**
   * Returns the "updated" value of the object with the given <code>name</code>.
   * 
   * @see BaseValueHolder#getAlternateValueHolder
   */
  protected Object getUpdateObject(String name) {
    Region rootRegion = CacheHelper.getCache().getRegion(REGION_NAME);
    boolean useRandomValues = false;
    useRandomValues = TestConfig.tab().booleanAt(QueryPrms.useRandomValues,
        false);
    if (useRandomValues) {
      BaseValueHolder anObj = null;
      BaseValueHolder newObj = null;
      try {
        anObj = (BaseValueHolder) rootRegion.get(name);
      } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      newObj = (anObj == null) ? new ValueHolder(name, randomValues) : anObj
          .getAlternateValueHolder(randomValues);
      return newObj;
    } else {
      String objectType = TestConfig.tab().stringAt(QueryPrms.objectType);
      Object val = ObjectHelper.createObject(objectType, 0);
      return val;
    }
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
      // for (int j = 0; j < numOfRegions; j++) {
      // RegionHelper.createRegion(REGION_NAME + ("" + j), factory);
      // }
      RegionHelper.createRegion(REGION_NAME + ("" + (i)), factory);
    }
  }

  // ========================================================================
  // end task methods
  public static void HydraTask_printBB() throws Throwable {
    CacheBB.getBB().print();
    EventBB.getBB().print();
    EventCountersBB.getBB().print();
    TestHelper.checkForEventError(EventCountersBB.getBB());
  }

  public static void HydraTask_iterate() throws Throwable {
    CacheHelper.createCache("cache1");
    RegionHelper.createRegion(REGION_NAME, "region1");
    CacheBB.getBB().print();
    EventBB.getBB().print();
    EventCountersBB.getBB().print();
    TestHelper.checkForEventError(EventCountersBB.getBB());
  }

  public static synchronized void HydraTask_closeTask_writeQueryStats_To_Blackboard() throws Throwable {
    if (!statisticsWrittenToBB) {
      GemFireCacheImpl cache = (GemFireCacheImpl)CacheHelper.getCache();
      CachePerfStats stats = cache.getCachePerfStats();
      Statistics statistics = stats.getStats();
      int value = statistics.getInt("queryExecutions");
      long value_time = statistics.getLong("queryExecutionTime");
      
      Log.getLogWriter().info("Value queryExecutions :  " + value);
      Log.getLogWriter().info("Value time queryExecutionTime:  " + value_time);
      long valueBB = QueryBB.getBB().getSharedCounters().read(QueryBB.NUM_QUERY_EXECS);
      int numVms = TestConfig.getInstance().getTotalVMs();
      
      Log.getLogWriter().info("Total VMs: " +numVms);
      Log.getLogWriter().info("Total VMs: " +numVms);
      if (CacheHelper.getCache().rootRegions().iterator().next() instanceof PartitionedRegion) {
        QueryBB.getBB().getSharedCounters().add(QueryBB.NUM_QUERY_EXECS, (numVms -1) * numQueries.get());
        IndexBB.getBB().getSharedCounters().add(IndexBB.TOTAL_USES, 112 * numQueries.get());
      }
      // Let the blackboard values stabilize
      Thread.sleep(10000);
      
      Log.getLogWriter().info("Num query executions as seen from Blackboard:  " + valueBB);
      Log.getLogWriter().info("Num query executions as per AtomicLong:  " + numQueries.get());
      QueryBB.getBB().getSharedCounters().add(QueryBB.NUM_QUERY_EXEC_STATS, value);
      QueryBB.getBB().getSharedCounters().add(QueryBB.TOTAL_QUERY_EXEC_TIME_STATS, value_time);
      QueryBB.getBB().getSharedCounters().add(QueryBB.NUM_QUERY_EXECS_ATOMIC_COUNTER_SUM, numQueries.get());
      
      boolean isIndexValidation = TestConfig.tab().booleanAt(
          QueryPrms.isIndexUsageValidation, false);
      if (isIndexValidation) {
        Collection<Index> indexes = CacheHelper.getCache().getQueryService()
            .getIndexes();
        Log.getLogWriter().info("Total indexes::  " + indexes.size());
        QueryBB.getBB().getSharedCounters().setIfLarger(QueryBB.TOTAL_INDEX_COUNT, indexes.size());
        long totalUsesInThisVM = 0;
        for (Index index : indexes) {
          Region region = index.getRegion();
          long keys = region.keySet().size();
          long numKeys = index.getStatistics().getNumberOfKeys();
          long numValues = index.getStatistics().getNumberOfValues();
          long numUpdates = index.getStatistics().getNumUpdates();
          long totalUses = index.getStatistics().getTotalUses();
          totalUsesInThisVM += totalUses;
          Log.getLogWriter().info("Index Name:  " + index.getName());
          Log.getLogWriter().info("Index stats numUsed:  " + totalUses);
          IndexBB.getBB().getSharedCounters().add(IndexBB.TOTAL_USES_STATS, totalUses);
          IndexBB.getBB().getSharedCounters().add(IndexBB.TOTAL_UPDATES_STATS, numUpdates);
        }
        Log.getLogWriter().info("Total uses in this VM : = " + totalUsesInThisVM);
      }
      statisticsWrittenToBB = true;
    }
  }
  
  public static void HydraTask_endTask_verifyQueryStats_With_Blackboard() throws Throwable {
    long valueBB = QueryBB.getBB().getSharedCounters().read(QueryBB.NUM_QUERY_EXECS);
    long valueStats = QueryBB.getBB().getSharedCounters().read(QueryBB.NUM_QUERY_EXEC_STATS);
    long valueTimeBB = QueryBB.getBB().getSharedCounters().read(QueryBB.TOTAL_QUERY_EXEC_TIME);
    long valueTimeStats = QueryBB.getBB().getSharedCounters().read(QueryBB.TOTAL_QUERY_EXEC_TIME_STATS);
    long valueSumAtomicLongBB = QueryBB.getBB().getSharedCounters().read(QueryBB.NUM_QUERY_EXECS_ATOMIC_COUNTER_SUM);
    long valueRegionOpsBB = QueryBB.getBB().getSharedCounters().read(QueryBB.NUM_REGION_OPS);
    long indexUpdatesStatsTotal = IndexBB.getBB().getSharedCounters().read(IndexBB.TOTAL_UPDATES_STATS);
   
    Log.getLogWriter().info(
        "For num queries valueSumAtomicLongBB = " + valueSumAtomicLongBB
            + " valuesBB = " + valueBB + " valueStats = " + valueStats);
    if (valueBB != valueStats) {
      throw new TestException("For num queries valuesBB = " + valueBB + " valueStats = " + valueStats);
    }
    Log.getLogWriter().info("For num queries valuesBB = " + valueBB + " valueStats = " + valueStats);
   /* if (valueTimeBB > valueTimeStats) {
      if (valueTimeStats < valueTimeBB * 0.9) {
        throw new TestException(
            "For queryExecTime, valueTimeBB > valueTimeStats and diff is more than 10% valueTimeBB * 0.9 = "
                + valueTimeBB * 0.9 + " valueTimeStats = " + valueTimeStats);
      }
    } else {
      if (valueTimeBB < valueTimeStats * 0.9) {
        throw new TestException(
            "For queryExecTime valueTimeStats > valueTimeBB and diff is more than 10% valueTimeStats * 0.9 = "
                + valueTimeStats * 0.9 + " valueTimeBB = " + valueTimeBB);
      }
    }*/
    boolean isIndexValidation = TestConfig.tab().booleanAt(
        QueryPrms.isIndexUsageValidation, false);
    if (isIndexValidation) {
      long indexTotalUsesBB = IndexBB.getBB().getSharedCounters().read(IndexBB.TOTAL_USES);
      long indexTotalUsesStats = IndexBB.getBB().getSharedCounters().read(IndexBB.TOTAL_USES_STATS);
      if (indexTotalUsesBB != indexTotalUsesStats) {
        throw new TestException("BUG 47149 For index totalUses indexTotalUsesBB = "
            + indexTotalUsesBB + " indexTotalUsesStats = "
            + indexTotalUsesStats);
      }
     
      long totalIndexCount = QueryBB.getBB().getSharedCounters().read(QueryBB.TOTAL_INDEX_COUNT);
      Log.getLogWriter().info("Total indexes::  " + totalIndexCount);
      if (indexUpdatesStatsTotal != totalIndexCount * valueRegionOpsBB) {
        //throw new TestException("For index indexUpdatesStatsTotal = "
        //    + indexUpdatesStatsTotal + " RegionOps = "
        //    + valueRegionOpsBB + " Num indexes = " + totalIndexCount + " Num index * regionOps = " + totalIndexCount * valueRegionOpsBB);
      }
    }
  }
  
  public static void HydraTask_endTask() throws Throwable {
    TestHelper.checkForEventError(EventCountersBB.getBB());
    CacheBB.getBB().print();
    EventBB.getBB().print();
    EventCountersBB.getBB().print();
    queryTest = new QueryTest();
    queryTest.initialize();
    StringBuffer errStr = new StringBuffer();
    try {
      queryTest.checkEventCounters();
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable e) {
      Log.getLogWriter().info(e.toString());
      errStr.append(e.toString());
    }
    if (errStr.length() > 0)
      throw new TestException(errStr.toString());
    TestHelper.checkForEventError(EventCountersBB.getBB());
  }

  /**
   * Creates the cache based on the given xmlFile
   * 
   */
  protected void createCacheFromXml(String xmlFilename) {
    CacheHelper.createCacheFromXml(xmlFilename);
    // CacheUtil.createCache(xmlFilename);
  }

  /**
   * Used for end task validation, iterate the keys/values in the given region,
   * checking that the key/value match according to the test strategy.
   * 
   * @param aRegion
   *          - the region to iterate.
   * @param allowZeroKeys
   *          - If the number of keys in the region is 0, then allow it if this
   *          is true, otherwise log an error to the return string.
   * @param allowZeroNonNullValues
   *          - If the number of non-null values in the region is 0, then allow
   *          it if this is true, otherwise log an error to the return string.
   * 
   * @return [0] the number of keys in aRegion [1] the number of non-null values
   *         in aRegion [2] a String containg a description of any errors
   *         detected, or "" if none.
   * 
   */
  protected Object[] iterateRegion(Region aRegion, boolean allowZeroKeys,
      boolean allowZeroNonNullValues) {
    StringBuffer errStr = new StringBuffer();
    Set keys = aRegion.keys();
    Log.getLogWriter().info(
        "For " + TestHelper.regionToString(aRegion, false) + ", found "
            + keys.size() + " keys");
    int numKeys = keys.size();
    if (numKeys == 0) {
      if (!allowZeroKeys)
        errStr.append("Region " + TestHelper.regionToString(aRegion, false)
            + " has " + numKeys + " keys\n");
    }
    int valueCount = 0;
    Iterator it = keys.iterator();
    while (it.hasNext()) {
      Object key = it.next();
      Object value;
      try {
        value = aRegion.get(key);
      } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      Log.getLogWriter().info("Checking key " + key + ", value " + value);
      boolean useRandomValues = false;
      useRandomValues = TestConfig.tab().booleanAt(QueryPrms.useRandomValues,
          false);
      if (useRandomValues) {
        if (value != null) {
          valueCount++;
          BaseValueHolder vh = (BaseValueHolder) value;
          String nameValue = "" + NameFactory.getCounterForName(key);
          String valueHolderValue = "" + vh.myValue;
          if (!nameValue.equals(valueHolderValue)) {
            String aStr = "Expected counter of key/value to match, key: " + key
                + ", value: " + vh.toString();
            Log.getLogWriter().info(aStr);
            errStr.append(aStr + "\n");
          }
        }
      } else {
        if (value != null) {
          valueCount++;
          ConfigurableObject cobj = (ConfigurableObject) value;
          int index = cobj.getIndex();
          long counter = NameFactory.getCounterForName(key);
          if (index != counter) {
            String aStr = "Expected counter of key/value to match, key: " + key
                + ", value: " + cobj.toString();
            Log.getLogWriter().info(aStr);
            errStr.append(aStr + "\n");
          }
        }
      }
    }
    if (valueCount == 0) {
      if (!allowZeroNonNullValues)
        errStr.append("Region " + TestHelper.regionToString(aRegion, false)
            + " has " + valueCount + " non-null values\n");
    }
    return new Object[] { new Integer(numKeys), new Integer(valueCount),
        errStr.toString() };
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
   * Given a region, return the number of names in the region and any of its
   * subregions.
   * 
   * @param aRegion
   *          The region to use to determin the number of names.
   * @param hasValueOnly
   *          True if the number of names returned should only include those
   *          names that return containsValueForKey true, false if all names
   *          should be counted.
   * 
   * @return The number of names in aRegion and its subregions.
   */
  protected int getNumNames(Region aRegion, boolean hasValueOnly) {
    int count = 0;
    StringBuffer aStr = new StringBuffer();
    aStr.append("Names in " + TestHelper.regionToString(aRegion, false)
        + " and its subregions:\n");
    Set aSet = new HashSet(aRegion.subregions(true));
    aSet.add(aRegion);
    Iterator it = aSet.iterator();
    while (it.hasNext()) {
      Region thisRegion = (Region) it.next();
      Iterator it2 = thisRegion.keys().iterator();
      while (it2.hasNext()) {
        Object key = it2.next();
        boolean containsValue = thisRegion.containsValueForKey(key);
        if (!hasValueOnly || (hasValueOnly && containsValue))
          count++;
        aStr.append("   " + key + " in "
            + TestHelper.regionToString(thisRegion, false)
            + " (containsValueForKey: " + containsValue + ")\n");
      }
    }
    Log.getLogWriter().info(aStr.toString());
    return count;
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

  /**
   * Verifies the count(*) query results for serial query tests.
   */
  public void verifyCountQueryResults() {
    int[][] countQueryResults = QueryBB.getCountResultsObject();
    // if (countQueryResults != null) {
    for (int i = 0; i < countQueryResults.length; i++) {
      if (countQueryResults[i][0] != countQueryResults[i][1]) {
        throw new TestException("Query result is incorrect: Should be "
            + countQueryResults[i][1] + ", but is " + countQueryResults[i][0]);
      }
    }
    // }
  }

}

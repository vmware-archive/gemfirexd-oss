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

import event.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import objects.PRPortfolio;
import objects.Portfolio;
import parReg.ParRegUtil;
import pdx.PdxTest;
import query.index.IndexBB;
import query.index.IndexPrms;
import query.index.IndexValidator;
import util.*;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.index.RangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexStore.IndexStoreEntry;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * A version of the <code>QueryTask</code> that performs operations serially. It
 * also validates the state of the cache after each operation. Note that this
 * test must be configured to use regions whose scope is
 * <code>DISTRIBUTED_ACK</code> or <code>GLOBAL</code>.
 */
public class SerialQueryAndEntryOpsTest extends QueryTest {
  private static SerialQueryAndEntryOpsTest serialTest = new SerialQueryAndEntryOpsTest();
  public static AtomicBoolean firstTimeInvocation = new AtomicBoolean(false);

  protected static String SELECT_STRING = "select distinct * from ";

  protected String queries[] = new String[] { 
      " where ID = 100",
      " where status = 'active'", 
      " pf, pf.positions.values where ID = 100",
      " pf, pf.positions.values where status = 'active'",
      " pf where pf.positions['SUN'] != null", 
      " pf where pf.status LIKE 'act%'",
      " pf where pf.status LIKE 'act_ve'"};

  final int threshold = 30000;

  Map<Integer, String> queryToIndexMap = new HashMap<Integer, String>() {
    {
      put(0, ID_COMPACT_RANGE_INDEX);
      put(1, STATUS_COMPACT_RANGE_INDEX);
      put(2, ID_RANGE_INDEX);
      put(3, STATUS_RANGE_INDEX);
      put(4, MAP_RANGE_INDEX_2);
      put(5, STATUS_COMPACT_RANGE_INDEX);
      put(6, STATUS_COMPACT_RANGE_INDEX);
    }
  };

  SelectResults r[][][] = null;

  protected String randRgn() {
    if (numOfRegions <= 0)
      numOfRegions = TestConfig.tab().intAt(QueryPrms.numOfRegions, 1);
//    if (numOfRegions != 1) {
//      throw new TestException(
//          "This test is no designed for more than one regions. Please set QueryPrms-numOfRegions = 1");
//    }
    return ("/" + REGION_NAME + (new Random()).nextInt(numOfRegions));
  }

  /* hydra task methods */
  /* ======================================================================== */
  public synchronized static void HydraTask_initialize() {
    if (queryTest == null) {
      queryTest = new SerialQueryAndEntryOpsTest();
      queryTest.initialize();
    }
  }

  static class QueryObserverImplBB extends QueryObserverAdapter {
    IndexValidator iv = null;

    public void beforeIndexLookup(Index index, int oper, Object key) {
      //iv = new IndexValidator(index.getName());
      Log.getLogWriter().info("BeforeIndexLoopkup :" + index.getName());
      //IndexBB.getBB().putIndexValidator(iv);
      IndexValidator iv = IndexBB.getBB().getIndexValidatorObject();
      iv.setIndexName(index.getName());
      IndexBB.getBB().putIndexValidator(iv);
    }

    public void afterIndexLookup(Collection results) {
      // if (results != null && results.size() != 0) {
      // IndexBB.getBB().putIndexValidator(iv);
      // }
    }
    
    public void beforeIndexLookup(Index index, int lowerBoundOperator,
        Object lowerBoundKey, int upperBoundOperator, Object upperBoundKey,
        Set NotEqualKeys) {
      Log.getLogWriter().info("BeforeIndexLoopkup :" + index.getName());
      IndexValidator iv = IndexBB.getBB().getIndexValidatorObject();
      iv.setIndexName(index.getName());
      IndexBB.getBB().putIndexValidator(iv);
    }
  }

  static class QueryObserverImplLocal extends QueryObserverAdapter {
    String indexName = null;

    public void beforeIndexLookup(Index index, int oper, Object key) {
      this.indexName = index.getName();
      Log.getLogWriter().info("BeforeIndexLoopkup :" + index.getName());
    }

    public void afterIndexLookup(Collection results) {
      // TODO: Check if this needs to be used as an extra validation
      // if (results != null && results.size() != 0) {
      // IndexBB.getBB().putIndexValidator(iv);
      // }
    }
    
    public void beforeIndexLookup(Index index, int lowerBoundOperator,
        Object lowerBoundKey, int upperBoundOperator, Object upperBoundKey,
        Set NotEqualKeys) {
      this.indexName = index.getName();
      Log.getLogWriter().info("BeforeIndexLoopkup :" + index.getName());
    }
  }

  public synchronized static void HydraTask_createIndexes() {
    if (queryTest == null) {
      queryTest = new SerialQueryAndEntryOpsTest();
    }
    queryTest.createIndex_task();
  }

  public synchronized static void HydraTask_attachQueryObserver() {
    QueryObserverImplBB observer = new QueryObserverImplBB();
    QueryObserverHolder.setInstance(observer);
  }

  /* override methods */
  /* ======================================================================== */
  protected int getNumVMsWithListeners() {
    return TestHelper.getNumVMs();
  }

  protected void addObject(Region aRegion, boolean aBoolean) {
    super.addObject(aRegion, aBoolean);
    validateQuery(aRegion);
  }

  protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
    super.invalidateObject(aRegion, isLocalInvalidate);
    validateQuery(aRegion);
  }

  protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
    super.destroyObject(aRegion, isLocalDestroy);
    validateQuery(aRegion);
  }

  protected void updateObject(Region aRegion) {
    super.updateObject(aRegion);
    validateQuery(aRegion);
  }

  protected void readObject(Region aRegion) {
    super.updateObject(aRegion);
    validateQuery(aRegion);
  }

  public static void HydraTask_validateCountStar() throws Throwable {
    new QueryTest().verifyCountQueryResults();
  }

  protected void validateQuery(Region aRegion) {
    try {
      Log.getLogWriter().info("Performing the query validation check");
      QueryValidator queryValidator = QueryBB.getQueryValidatorObject();
      if (queryValidator == null) {
        throw new TestException("QueryValidator found null");
      }
      Query query = CacheHelper.getCache().getQueryService()
          .newQuery("SELECT DISTINCT * FROM /" + aRegion.getName() + ".keys");
      Object key = queryValidator.getKey();
      if (key == null) {
        throw new TestException("key cannot be null");
      }
      boolean exists = queryValidator.getKeyExists();
      Object result = query.execute();
      if (exists) {
        if (((SelectResults)result).contains(key)) {
          Log.getLogWriter().info("Key found : " + key + " in Query result");
        }
        else {
          Log.getLogWriter().info(
              "Operation done : " + queryValidator.getOperation());
          throw new TestException("Key " + key
              + " not found in the Query result");
        }
        boolean hasValue = queryValidator.getHasValue();
        query = CacheHelper
            .getCache()
            .getQueryService()
            .newQuery(
                "SELECT DISTINCT itr.value FROM /" + aRegion.getName()
                    + ".entries itr where itr.key = $1");
        Object[] params = new Object[1];
        params[0] = key;
        result = query.execute(params);
        if (hasValue) {
          if (((Collection)result).size() != 1) {
            throw new TestException(
                "Size of result is not 1, and this key expected to have value");
          }
          Iterator iter = ((Collection)result).iterator();
          Object resultValue = iter.next();
          Object value = queryValidator.getValue();
          if (value instanceof Map) { // this is a pdx helper map
            // convert the entries in the map into an instance of
            // PdxVersionedPortfolio
            String className = TestConfig.tab().stringAt(QueryPrms.objectType);
            Portfolio portfolio = (Portfolio)PdxTest
                .getVersionedInstance(className);
            portfolio.restoreFromPdxHelperMap((Map)value);
            value = portfolio;
          }
          if (CacheHelper.getCache().getPdxReadSerialized() && resultValue instanceof PdxInstance) {
            resultValue = ((PdxInstance) resultValue).getObject();
          }
          String valueStr = value.toString();
          String resultValStr = resultValue.toString();
          Log.getLogWriter().info("result: " + resultValStr);
          Log.getLogWriter().info("valueStr: " + valueStr);
          if (!(valueStr.equals(resultValStr))) {
            Log.getLogWriter().info("Result = " + Utils.printResult(result));
            Log.getLogWriter().info("Value = " + value.toString());
            Log.getLogWriter().info("ResultValue = " + resultValue.toString());
            throw new TestException("Values do not match");
          }
        }
        else {
          Iterator iter = ((Collection)result).iterator();
          Object resultValue = iter.next();
          if (resultValue != null) {
            throw new TestException("Value is not null");
          }
        }

      }
      else {
        if (((SelectResults)result).contains(key)) {
          throw new TestException(
              "Key "
                  + key
                  + "found in the Query result, actually it is expected to be destroyed");
        }
        else {
          Log.getLogWriter().info(
              "Key : " + key
                  + " not found in Query result, so it is properly destroyed");
        }
      }

    }
    catch (Exception e) {
      throw new TestException("Caught exception during query validation"
          + TestHelper.getStackTrace(e));
    }

  }

  /**
   * Check all counters from EventCountersBB. This verifies that all events were
   * distributed to all VMs (full distribution) and that no region or local
   * events occurred.
   */
  protected void checkEventCounters() {
    checkEventCounters(true);
  }

  /**
   * Check event counters. If numCloseIsExact is true, then the number of close
   * events must be an exact match, otherwise allow numClose events to be the
   * minimum of the expected numClose counter. This is useful in tests where the
   * timing of shutting down the VMs/C clients may or may not cause a close
   * event.
   * 
   * @param numCloseIsExact
   *          True if the numClose event counters must exactly match the
   *          expected numClose value, false if the numClose event counters must
   *          be no less than the expected numClose counter.
   */
  protected void checkEventCounters(boolean numCloseIsExact) {
    SharedCounters counters = EventBB.getBB().getSharedCounters();
    long numCreate = counters.read(EventBB.NUM_CREATE);
    long numUpdate = counters.read(EventBB.NUM_UPDATE);
    long numDestroy = counters.read(EventBB.NUM_DESTROY);
    long numInval = counters.read(EventBB.NUM_INVALIDATE);
    long numRegionDestroy = counters.read(EventBB.NUM_REGION_DESTROY);
    long numRegionInval = counters.read(EventBB.NUM_REGION_INVALIDATE);
    long numLocalDestroy = counters.read(EventBB.NUM_LOCAL_DESTROY);
    long numLocalInval = counters.read(EventBB.NUM_LOCAL_INVALIDATE);
    long numLocalRegionDestroy = counters
        .read(EventBB.NUM_LOCAL_REGION_DESTROY);
    long numLocalRegionInval = counters
        .read(EventBB.NUM_LOCAL_REGION_INVALIDATE);
    long numClose = counters.read(EventBB.NUM_CLOSE);

    int numVmsWithList = getNumVMsWithListeners();

    Log.getLogWriter().info(
        "num VMs/C clients with listener installed: " + numVmsWithList);

    ArrayList al = new ArrayList();
    // afterCreate counters
    al.add(new ExpCounterValue("numAfterCreateEvents_isDist",
        (numCreate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotDist", 0));
    al.add(new ExpCounterValue("numAfterCreateEvents_isExp", 0));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp",
        (numCreate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterCreateEvents_isRemote",
        (numCreate * (numVmsWithList - 1))));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotRemote", numCreate));
    al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", 0));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad",
        (numCreate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", 0));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotLocalLoad",
        (numCreate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNetLoad", 0));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetLoad",
        (numCreate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNetSearch", 0));
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetSearch",
        (numCreate * numVmsWithList)));

    // afterDestroy counters
    al.add(new ExpCounterValue("numAfterDestroyEvents_isDist",
        (numDestroy * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNotDist", 0));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isExp", 0));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp",
        (numDestroy * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote",
        (numDestroy * (numVmsWithList - 1))));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNotRemote", numDestroy));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isLoad", 0));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLoad",
        (numDestroy * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isLocalLoad", 0));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLocalLoad",
        (numDestroy * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNetLoad", 0));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetLoad",
        (numDestroy * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNetSearch", 0));
    al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetSearch",
        (numDestroy * numVmsWithList)));

    // afterInvalidate counters
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isDist",
        (numInval * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotDist", 0));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isExp", 0));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp",
        (numInval * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote",
        (numInval * (numVmsWithList - 1))));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotRemote", numInval));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isLoad", 0));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLoad",
        (numInval * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isLocalLoad", 0));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLocalLoad",
        (numInval * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetLoad", 0));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetLoad",
        (numInval * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetSearch", 0));
    al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetSearch",
        (numInval * numVmsWithList)));

    // afterUpdate counters
    al.add(new ExpCounterValue("numAfterUpdateEvents_isDist",
        (numUpdate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotDist", 0));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isExp", 0));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp",
        (numUpdate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote",
        (numUpdate * (numVmsWithList - 1))));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotRemote", numUpdate));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", 0));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad",
        (numUpdate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", 0));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLocalLoad",
        (numUpdate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNetLoad", 0));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetLoad",
        (numUpdate * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNetSearch", 0));
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetSearch",
        (numUpdate * numVmsWithList)));

    // afterRegionDestroy counters
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", 0));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", 0));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", 0));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", 0));

    // afterRegionInvalidate counters
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist",
        (numRegionInval * numVmsWithList)));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist",
        numLocalRegionInval));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp",
        ((numRegionInval * numVmsWithList) + numLocalRegionInval)));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote",
        (numRegionInval * (numVmsWithList - 1))));
    al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote",
        (numRegionInval + numLocalRegionInval)));

    // afterRegionInvalidate counters
    al.add(new ExpCounterValue("numClose", numClose, numCloseIsExact));

    EventCountersBB.getBB().checkEventCounters(al);
  }

  /**
   * Method telling whether the region has range index associated with it
   * 
   * @param regionName
   * @return
   */
  private boolean isRangeIndexPresent(String regionName) {

    boolean present = false;
    Collection<Index> indexes = CacheHelper.getCache().getQueryService()
        .getIndexes();
    for (Index index : indexes) {
      if (index instanceof RangeIndex && !(index instanceof CompactRangeIndex)) {
        if (index.getCanonicalizedFromClause().startsWith(regionName)) {
          present = true;
          break;
        }
      }
    }
    Log.getLogWriter().info(
        "ISRANGEINDEXPRESENT " + regionName + " PRESENT = " + present);
    return present;
  }

  /**
   * This method will be called if this is a serial execution environment. If
   * QueryPrms-isIndexUsageValidation is set to true it will perform various
   * index usage checks. It will utilize IndexValidator and blackboard for this
   * purpose. If not, then it will call super.doQuery() which will perform
   * simple query results validation using query observer and blackboard.
   */
  protected void doQuery(boolean logAddition) {
    boolean isIndexValidation = TestConfig.tab().booleanAt(
        QueryPrms.isIndexUsageValidation, false);
    if (isIndexValidation) {

      //
      String indexType = TestConfig.tab().stringAt(IndexPrms.indexType,
          "compactRange");
      Set<Region<?, ?>> regions = CacheHelper.getCache().rootRegions();
      Log.getLogWriter().info(
          "Printing all root regions in the Cache: " + regions.toString());
      Collection<Index> indexes = CacheHelper.getCache().getQueryService()
          .getIndexes();
      Log.getLogWriter().info(
          "Printing all indexes in the Cache: " + indexes.toString());
      for (Index index : indexes) {
        Log.getLogWriter().info("Index name " + index.getName());
        Log.getLogWriter().info(
            "Index canonicalized from clause "
                + index.getCanonicalizedFromClause());
        Log.getLogWriter().info(
            "Index canonicalized index expression "
                + index.getCanonicalizedIndexedExpression());
        Log.getLogWriter().info(
            "Index canonicalized projection attributes "
                + index.getCanonicalizedProjectionAttributes());
      }

      // int i = ((new Random()).nextInt(queries.length));
      for (int i = 0; i < queries.length; i++) {
        Log.getLogWriter().info(" query = " + queries[i]);
        if (numOfRegions <= 0)
          numOfRegions = TestConfig.tab().intAt(QueryPrms.numOfRegions, 1);
        //int regionNum = (new Random()).nextInt(numOfRegions);
        for (int regionNum = 0; regionNum < numOfRegions; regionNum++) {
          boolean isOverflow = false;
          Region region = RegionHelper.getRegion("/" + REGION_NAME + regionNum);
          EvictionAttributes evAttr = region.getAttributes()
              .getEvictionAttributes();
          if (evAttr != null) {
            // We do not have range indexes so replace the expected indexes with
            // compact ones
            if (evAttr.getAction().equals(EvictionAction.OVERFLOW_TO_DISK)) {
              // Will come here if Region is overflow region
              // if (!isRangeIndexPresent("/" + REGION_NAME + regionNum)) {
              isOverflow = true;
              Log.getLogWriter().info(
                  "REPLACING EXPECTED INDEX Regionname: "
                      + ("/" + REGION_NAME + regionNum));
              queryToIndexMap.put(2, ID_COMPACT_RANGE_INDEX);
              queryToIndexMap.put(3, STATUS_COMPACT_RANGE_INDEX);
            }
          }
          if (indexType.equals("hashIndex") || indexType.equals("allAndHash")) {
            Log.getLogWriter().info(
                "REPLACING EXPECTED INDEX Regionname: "
                    + ("/" + REGION_NAME + regionNum + " due to hash index"));
            queryToIndexMap.put(0, ID_HASH_INDEX);
            queryToIndexMap.put(1, STATUS_HASH_INDEX);
            queryToIndexMap.put(2, ID_HASH_INDEX);
            queryToIndexMap.put(3, STATUS_HASH_INDEX);
          }

          Query query = CacheHelper
              .getCache()
              .getQueryService()
              .newQuery(
                  SELECT_STRING + ("/" + REGION_NAME + regionNum) + queries[i]);

          try {
            Log.getLogWriter().info(
                "Executing query :::" + query.getQueryString());
            IndexValidator iv = new IndexValidator(null);
            IndexBB.getBB().putIndexValidator(iv);
            long start = System.nanoTime();
            Object result = query.execute();
            QueryBB.getBB().getSharedCounters().add(QueryBB.TOTAL_QUERY_EXEC_TIME, System.nanoTime() - start);
            IndexBB.getBB().getSharedCounters().increment(IndexBB.TOTAL_USES);
            QueryBB.getBB().getSharedCounters().increment(QueryBB.NUM_QUERY_EXECS);
            Log.getLogWriter().info(
                "NumQueries :::" + numQueries.incrementAndGet());
            Log.getLogWriter().info("Query Result::: " + result.toString());
            Log.getLogWriter().info(
                "Query Result size::: " + ((Collection)result).size());
            // if it is a MapRange query and the region is overflow then there
            // will be no index usage
            if (!(i == 4 && isOverflow)) {
              
              Log.getLogWriter().info("i: " + i + "QueryToIndexMap.get(i)  "+ queryToIndexMap.get(i)) ;
              validateIndex(queryToIndexMap.get(i) + regionNum,
                  query.getQueryString());
            }
            queryToIndexMap.put(2, ID_RANGE_INDEX);
            queryToIndexMap.put(3, STATUS_RANGE_INDEX);

          } catch (QueryExecutionTimeoutException e) {
          	Log.getLogWriter().info("HERE Caught QueryExecutionTimeoutException " + e.getMessage());
          }
          catch (Exception e) {
            throw new TestException("Caught exception during query execution"
                + TestHelper.getStackTrace(e));
          }
        }
      }
    }
    else {
      super.doQuery(logAddition);
    }
  }

  /**
   * This method will be called if this is a serial execution environment. If
   * QueryPrms-isIndexUsageValidation is set to true then this method will be
   * called by doQuery(). It will perform various index usage checks. It will
   * utilize IndexValidator and blackboard for this purpose.
   */
  protected void validateIndex(String expectedIndexName, String query) {
    try {
      Log.getLogWriter().info("Performing the index usage validation check");
      IndexValidator indexValidator = IndexBB.getIndexValidatorObject();
      if (indexValidator == null) {
        throw new TestException("indexValidator found null");
      }
      String index = indexValidator.getIndexName();
      Log.getLogWriter().info("Expected Index name: " + expectedIndexName);
      Log.getLogWriter().info("Actual Index name " + index);
      if (index == null) {
        throw new TestException("No index was used by the query: " + query + " ExpectedIndexName: " + expectedIndexName);
      }
      if (!index.equals(expectedIndexName)) {
        // TODO: Check this, if the Compact and Non-compact both are available
        // should it pick
        // Range for bigger from clause queries?
        if (expectedIndexName.equals(ID_RANGE_INDEX)) {
          if (index.equals(ID_COMPACT_RANGE_INDEX) || index.equals(ID_HASH_INDEX)) {
            return;
          }
        }
        if (expectedIndexName.equals(STATUS_RANGE_INDEX)) {
          if (index.equals(STATUS_COMPACT_RANGE_INDEX)) {
            return;
          }
        }
        throw new TestException(
            "The name of the index used did not match Expected: "
                + expectedIndexName + " Actual "
                + indexValidator.getIndexName() + " query : " + query);
      }
    }
    catch (Exception e) {
      throw new TestException("Caught exception during query validation"
          + TestHelper.getStackTrace(e));
    }
  }

  /**
   * Hydra task to verify index data structures which were created in a node.
   * This also performs querying on the data.
   */
  public static void HydraTask_verifyIndexSize_QueryData_EatExpectedException() {
    try {
      HydraTask_verifyIndexSize_QueryData();
    } catch(CacheClosedException cce) { 
      Log.getLogWriter().warning("Caught CacheClosed exception.. ignoring");
    } catch (QueryInvocationTargetException e) {
      Log.getLogWriter().warning("Caught query invocation target exception.. ignoring");
    } catch (PartitionOfflineException e) {
      Log.getLogWriter().warning("Caught PartitionOfflineException.. ignoring");
    } catch (Exception e) {
      throw new TestException("Unexpectedly Caught Exception while executing query", e);
    }
  }
  
  public static void queryData_EatExpectedException() {
    try {
      queryData();
    } catch(CacheClosedException cce) { 
      Log.getLogWriter().warning("Caught CacheClosed exception.. ignoring");
    } catch (QueryInvocationTargetException e) {
      Log.getLogWriter().warning("Caught query invocation target exception.. ignoring");
    } catch (PartitionOfflineException e) {
      Log.getLogWriter().warning("Caught PartitionOfflineException.. ignoring");
    }catch (ServerOperationException e) {
      Log.getLogWriter().warning("Caught exception type: " + e.getClass().getName());
      Log.getLogWriter().warning("Caught exception cause: " + e.getCause());
      if (e.getCause() != null) {
        Log.getLogWriter().warning("Caught exception cause-messge: " + e.getCause().getMessage());
        Log.getLogWriter().warning("Caught exception cause-localizedmessge: " + e.getCause().getLocalizedMessage());
      }
      if (e.getCause() instanceof QueryInvocationTargetException) {
        Log.getLogWriter().warning("Caught ServerOperationException due to query invocation target exception.. ignoring");
      } else {
        throw new TestException("Unexpectedly Caught Exception while executing query", e);
      }
    } catch (Exception e) {
      throw new TestException("Unexpectedly Caught Exception while executing query", e);
     
    }
  }
  
  /**
   * Execute specific queries on the data and verify result sizes based on region sizes.
   * @throws Exception
   */
  public static void queryData() throws Exception {
    String query1 = "select * from /QueryRegion0 qr where qr.id = 1000";
    String query2 = "select * from /QueryRegion0 qr where qr.status = 'active'";
    String query3 = "select * from /QueryRegion0 qr where qr.status = 'active' AND qr.ID > 20000";
    boolean isBridgeConfiguration = (TestConfig.tab().stringAt(BridgePrms.names, null) != null);
    QueryService qs = null;
    if (!isBridgeConfiguration) {
      qs = CacheHelper.getCache().getQueryService();
    } else {
      if (queryTest.pool != null) {
        // This is edge VM in client-server test
        qs = queryTest.pool.getQueryService();
      } else {
        // This is bridge VM in client-server test
        qs = CacheHelper.getCache().getQueryService();
      }
    }
    
    Query query = qs.newQuery(query1);
    SelectResults sr = (SelectResults)query.execute();
    Region region = CacheHelper.getCache().getRegion("/QueryRegion0");
    
    //long regionSize = (queryTest.pool ==null) ? region.keySet().size():region.keySetOnServer().size();
    long regionSize = queryTest.maxObjects;
    Log.getLogWriter().info("Region size is: " + regionSize);
    if (regionSize != queryTest.maxObjects) {
      throw new TestException(
          "Region size expected to be queryTest.maxObjects i.e. "
              + queryTest.maxObjects + " but was found to be: " + regionSize);
    }

    if (sr.size() != 1) {
      throw new TestException("Unexpected result size  expected: 1 actual :" + sr.size());
    }
    PRPortfolio pfo = (PRPortfolio) sr.asList().get(0);
    if (pfo.ID != 1000) {
      throw new TestException("Query returned wrong result ID Expected :1000 Actual: " + pfo.ID );
    }
    
    query = qs.newQuery(query2);
    sr = (SelectResults)query.execute();

    if (sr.size() != regionSize/2) {
      throw new Exception("Unexpected result size Expected " + (regionSize/2) +" Actual: "
          + sr.size());
    }
    for (Object obj: sr) {
      PRPortfolio pf = (PRPortfolio) obj;
      if (!pf.status.equals("active")) {
        throw new TestException("Query returned wrong result status Expected 'active' for all objects, " +
        		"but for Portfolio Object id: " + pf.ID + " , status was found to be: " + pf.status );
      }
    }

    query = qs.newQuery(query3);
    sr = (SelectResults)query.execute();

    if (sr.size() != (regionSize - 20000)/2 - 1) {
      throw new Exception("Unexpected result size Expected "+ ((regionSize - 20000)/2 - 1) +" Actual: "
          + sr.size());
    }
    for (Object obj: sr) {
      PRPortfolio pf = (PRPortfolio) obj;
      if (!pf.status.equals("active")) {
        throw new TestException("Query returned wrong result, status Expected 'active' for all objects, " +
                        "but for Portfolio Object id: " + pf.ID + " , status was found to be: " + pf.status );
      }
      if (pf.ID <= 20000) {
        throw new TestException("Query returned wrong result, ID Expected >=20000 for all objects, " +
                        "but found Portfolio Object id: " + pf.ID );
      }
    }
  }
  
  /**
   * Verification for non-partitioned indexes (compactRange/Range)
   * @param index
   * @param region
   */
  static void verifyNonPartitionedIndex(Index index, Region region) {

    long keys = region.keySet().size();
    long numKeys = index.getStatistics().getNumberOfKeys();
    long numValues = index.getStatistics().getNumberOfValues();
    // 2 is for 'active' and 'inactive' status.
    verifyIndex(index, numKeys, numValues, keys, 2);
  }
  
  /**
   * Verify indexes. This is based on our knowledge of region data and 
   * expected index keys/values in as obtained from index statistics.
   * @param index
   * @param numKeys
   * @param numValues
   * @param keys
   * @param statusIndexExpectedKeys
   */
  static void verifyIndex(Index index, long numKeys, long numValues, long keys, long statusIndexExpectedKeys) {
    if (index.getName().startsWith(QueryTest.ID_COMPACT_RANGE_INDEX) || index.getName().startsWith("idCompactRangeEntryIndex")) {
      Log.getLogWriter().info("Compact index on ID: ");
      if (numKeys != keys) {
        Log.getLogWriter().info("NumKeys: " + numKeys +" LocalKeys: " + keys);
        throw new TestException("Local index keys and local region keys size mismatch for ID index");
      }
      if (numValues != keys) {
        Log.getLogWriter().info("NumValues: " + numValues +" LocalKeys: " + keys);
        throw new TestException("Local index values and local region keys size mismatch for ID index");
      }
    }
    else if (index.getName().startsWith(
        QueryTest.STATUS_COMPACT_RANGE_INDEX) || index.getName().startsWith("statusCompactRangeEntryIndex")) {
      Log.getLogWriter().info("Compact index on STATUS: ");
      if (numValues != keys) {
        Log.getLogWriter().info("NumValues: " + numValues +" LocalKeys: " + keys);
        throw new TestException("Local index values and local region keys size mismatch for status index");
      }
      if (numKeys != statusIndexExpectedKeys) {
        Log.getLogWriter().info("numKeys: " + numKeys );
        throw new TestException("Local index keys and local bucket IDs size mismatch for status index");
      }
    } else if (index.getName().startsWith(QueryTest.ID_RANGE_INDEX) || index.getName().startsWith("idRangeEntryIndex")) {
      
      Log.getLogWriter().info("Range index on ID: ");
      if (numKeys != keys) {
        Log.getLogWriter().info("NumKeys: " + numKeys +" LocalKeys: " + keys);
        throw new TestException("Local index keys and local region keys size mismatch for ID index");
      }
      if (numValues != 2 * keys) {
        Log.getLogWriter().info("NumValues: " + numValues +" LocalKeys: " + keys);
        throw new TestException("Local index values and local region keys size mismatch for ID index");
      }
    }
    else if (index.getName().startsWith(
        QueryTest.STATUS_RANGE_INDEX) || index.getName().startsWith("statusRangeEntryIndex")) {
      
      Log.getLogWriter().info("Range index on status: ");
      if (numValues != 2* keys) {
        Log.getLogWriter().info("NumValues: " + numValues +" LocalKeys: " + keys);
        throw new TestException("Local index values and local region keys size mismatch for status index");
      }
      if (numKeys != statusIndexExpectedKeys) {
        Log.getLogWriter().info("numKeys: " + numKeys );
        throw new TestException("Local index keys and local bucket IDs size mismatch for status index");
      }
    }
  }
  
  /**
   * Verify contents of the PartitionedIndex
   * @param index
   * @param region
   */
  static void verifyPartitionedIndex(Index index, Region region) {
    PartitionedRegion region1 = (PartitionedRegion)region;
        
     //bIds = region1.getLocalBucketsListTestOnly();
    Set<Integer> bIds = ((LocalDataSet)PartitionRegionHelper.getLocalData(region1)).getBucketSet();
    Log.getLogWriter().info(
        "Region contents - Local Bucket IDs for QueryRegion1 ::::"
            + bIds.toString());
    Log.getLogWriter().info("Bucket ids on this node:  " + bIds.toString());
    for (Object key : bIds) {
      BucketRegion br = region1.getBucketRegion(key);
      if (br != null) {
//        Log.getLogWriter().info(
//            "Server-side region contents -Bucket ID for bucket region: "
//                + br.getId());
        //Log.getLogWriter().info(
          //  "Server-side region contents - Keys in the bucket region for key "
            //    + key + ": " + br.keySet().toString());
      }
      else {
        Log.getLogWriter().info(
            "Server-side region contents  Bucket region not found for key:"
                + key);
      }
    }
    List bucketIndexes = ((PartitionedIndex)index).getBucketIndexes();
    Log.getLogWriter().info("Bucket indexes size: " + bucketIndexes.size());
    Log.getLogWriter().info("BIds size: " + bIds.size() );
    if (bIds.size() != bucketIndexes.size()) {
      Log.getLogWriter().info("Mistmatch occurred in bucket index count and bucket region count, " +
      		"starting a while loop to check if the data is still being populated");
      long initialBucketRegionCount = bIds.size();
      long initialBucketIndexCount = bucketIndexes.size();
      long newBucketIndexCount = -1;
      long newBucketRegionCount = -1;
      while (newBucketIndexCount != initialBucketIndexCount || newBucketRegionCount != initialBucketRegionCount) {
        Log.getLogWriter().info("Sleeping for 10 seconds.. ");
        try{Thread.sleep(10000);}catch (InterruptedException e){}
        bIds = ((LocalDataSet)PartitionRegionHelper.getLocalData(region1)).getBucketSet();
        bucketIndexes = ((PartitionedIndex)index).getBucketIndexes();
        initialBucketIndexCount = newBucketIndexCount;
        initialBucketRegionCount = newBucketRegionCount;
        newBucketIndexCount = bucketIndexes.size();
        newBucketRegionCount = bIds.size();
        Log.getLogWriter().info("After Sleeping for 5 seconds.. ");
        Log.getLogWriter().info("New bucket index count: " + newBucketIndexCount);
        Log.getLogWriter().info("New Bucket Region Count:  " + newBucketRegionCount );
      }
      if (newBucketIndexCount != newBucketRegionCount) throw new TestException("Bucket regions and bucket indexes size mismatch");
    }
    long numKeys = index.getStatistics().getNumberOfKeys();
    long numValues = index.getStatistics().getNumberOfValues();
    long localKeys = region1.getLocalSize();
    
    long regionKeysCount = region.keySet().size();
    Log.getLogWriter().info("Region size = " + regionKeysCount);
    Log.getLogWriter().info("NumBuckets = " + bIds.size());
    Log.getLogWriter().info("Local region size = " + localKeys);
    verifyIndex(index, numKeys, numValues, localKeys, 2 * bIds.size());

  }
  
  /**
   * HydraTask which verifies index datastructures and also queries data.
   * @throws Exception
   */
  public static void HydraTask_verifyIndexSize_QueryData() throws Exception {
    //HydraTask_calculateIndexSizesForPR();
    
    queryData();
    //Wait for data population if this method is invoked for the first time in this VM
    synchronized(SerialQueryAndEntryOpsTest.class){
    if (firstTimeInvocation.compareAndSet(false, true)) {
      try {Thread.sleep(30000);} catch(InterruptedException e) {/*ignore*/}
    }
    }
    Collection<Index> indexes = CacheHelper.getCache().getQueryService()
        .getIndexes();
    Log.getLogWriter().info("Printing all data For all indexes:  ");
    Log.getLogWriter().info("Total indexes::  " + indexes.size());
    for (Index index : indexes) {
      Region region = index.getRegion();
      long keys = region.keySet().size();
      long numKeys = index.getStatistics().getNumberOfKeys();
      long numValues = index.getStatistics().getNumberOfValues();
      long numInnerKeys = 0;
      long numInnerValues = 0;
      
      if (index instanceof CompactRangeIndex) {
        Log.getLogWriter().info("Index name : " + index.getName());
        Log.getLogWriter().info("Index type : CompactRangeIndex ");
        CompactRangeIndex crIndex = (CompactRangeIndex) index;
        numInnerKeys = crIndex.getIndexStorage().size();
        CloseableIterator<IndexStoreEntry> iterator = null;
        try{
          iterator = crIndex.getIndexStorage().iterator(null);
          while(iterator.hasNext()){
            iterator.next();
            numInnerValues ++;
          }
        } finally{
          if(iterator != null){
            iterator.close();
          }
        }
        //numInnerValues = crIndex.getValueToEntriesMap().values().size();
        // TODO: ASSERT: numInnerKeys = numKeys
        // TODO: ASSERT: numInnerValues = numValues
      } else if (index instanceof RangeIndex){
        Log.getLogWriter().info("Index name : " + index.getName());
        Log.getLogWriter().info("Index type : RangeIndex ");
        RangeIndex rangeIndex = (RangeIndex) index; 
        numInnerKeys = rangeIndex.getValueToEntriesMap().keySet().size();
        numInnerValues = rangeIndex.getValueToEntriesMap().values().size();
        // TODO: ASSERT: numInnerKeys = numKeys
        // TODO: ASSERT: numInnerValues = numValues
      } else if(index instanceof PartitionedIndex) {
        Log.getLogWriter().info("Index name : " + index.getName());
        Log.getLogWriter().info("Index type : PartitionedIndex ");
        PartitionedIndex pIndex = (PartitionedIndex) index;
        List bucketIndexes = pIndex.getBucketIndexes();
        Log.getLogWriter().info("Bucket indexes size: " + bucketIndexes.size());
        for (Object obj : bucketIndexes) {
           Index bucketIndex = (Index) obj;
           if (index instanceof CompactRangeIndex) {
             CompactRangeIndex crIndex = (CompactRangeIndex) index;
             numInnerKeys += crIndex.getIndexStorage().size();
             CloseableIterator<IndexStoreEntry> iterator = null;
             try{
               iterator = crIndex.getIndexStorage().iterator(null);
               while(iterator.hasNext()){
                 iterator.next();
                 numInnerValues ++;
               }
             } finally{
               if(iterator != null){
                 iterator.close();
               }
             }
             //numInnerValues += crIndex.getValueToEntriesMap().values().size();
           } else if (index instanceof RangeIndex){
             RangeIndex rangeIndex = (RangeIndex) index; 
             numInnerKeys += rangeIndex.getValueToEntriesMap().keySet().size();
             numInnerValues += rangeIndex.getValueToEntriesMap().values().size();
           }
        }
        // TODO: ASSERT: numInnerKeys = numKeys
        // TODO: ASSERT: numInnerValues = numValues
      }
    }
    
    if (indexes.size() != 8) {
      throw new TestException("Number of indexes was expected to be 8, found: " + indexes.size());
    }
    
    for (Index index : indexes) {
      Region region = index.getRegion();
      if (index instanceof PartitionedIndex) {
        verifyPartitionedIndex(index, region);
      } else {
        verifyNonPartitionedIndex(index, region);
      }
    }
    queryData();
  }
  
  /**
   * Calculate and verify index sizes for a partitioned region.
   */
  public static void HydraTask_calculateAndVerifyIndexSizesForPR() {
    long[] counterValues = IndexBB.getBB().getSharedCounters().getCounterValues();
    for (int i = 0; i < counterValues.length; i++) {
      IndexBB.getBB().getSharedCounters().zero(i);
    }
    HydraTask_calculateIndexSizesForPR();
    HydraTask_verifyIndexSizes();
  }

  /**
   * This method calculates the actual index sizes using index statistics and
   * compares them with the expected index sizes based on the data in region.
   * For partitioned region the index sizes are calculated by aggregating the
   * sizes of all bucket indexes.
   */
  public static void HydraTask_calculateIndexSizesForPR() {
    long statusRangeNumBuckets = 0;
    long statusCompactRangeNumBuckets = 0;
    long idRangeNumBuckets = 0;
    long idCompactNumBuckets = 0;
    long idRangeNumKeys = 0;
    long idCompactRangeNumKeys = 0;
    long statusRangeNumValues = 0;
    long statusCompactRangeNumValues = 0;
    long idRangeNumValues = 0;
    long idCompactRangeNumValues = 0;
    Set<Region<?, ?>> regions = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info(
        "Printing all root regions in the Cache: " + regions.toString());

    Collection<Index> indexes = CacheHelper.getCache().getQueryService()
        .getIndexes();

    // TODO: Add a check for number of indexes
    for (Index index : indexes) {
      long numKeys = 0;
      long numValues = 0;
      Region region = index.getRegion();
      SharedCounters counters = IndexBB.getBB().getSharedCounters();
      if (index instanceof PartitionedIndex) {
        List bucketIndexes = ((PartitionedIndex)index).getBucketIndexes();
        Log.getLogWriter().info("NumBuckets = " + bucketIndexes.size());
        if (index.getName().startsWith(QueryTest.ID_COMPACT_RANGE_INDEX)) {
          counters.add(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_NUMBUCKETS,
              bucketIndexes.size());
          Log.getLogWriter().info(
              "NUM Buckets counter: "
                  + counters
                      .read(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_NUMBUCKETS));
          idCompactNumBuckets += bucketIndexes.size();
        }
        else if (index.getName().startsWith(
            QueryTest.STATUS_COMPACT_RANGE_INDEX)) {
          counters.add(IndexBB.NUM_PR_COMPACTRANGE_STATUS_INDEX_NUMBUCKETS,
              bucketIndexes.size());
          statusCompactRangeNumBuckets += bucketIndexes.size();
        }
        else if (index.getName().startsWith(QueryTest.ID_RANGE_INDEX)) {
          counters.add(IndexBB.NUM_PR_RANGE_ID_INDEX_NUMBUCKETS,
              bucketIndexes.size());
          idRangeNumBuckets += bucketIndexes.size();
        }
        else if (index.getName().startsWith(QueryTest.STATUS_RANGE_INDEX)) {
          counters.add(IndexBB.NUM_PR_RANGE_STATUS_INDEX_NUMBUCKETS,
              bucketIndexes.size());
          statusRangeNumBuckets += bucketIndexes.size();
        }
        // for (Object indexObject : bucketIndexes) {
        // Index bucketIndex = (Index) indexObject;
        // numKeys = bucketIndex.getStatistics().getNumberOfKeys();
        // numValues = bucketIndex.getStatistics().getNumberOfValues();
        numKeys = index.getStatistics().getNumberOfKeys();
        numValues = index.getStatistics().getNumberOfValues();
        long regionKeysCount = region.keySet().size();
        Log.getLogWriter().info("Region size = " + regionKeysCount);
        if (index.getName().startsWith(ID_COMPACT_RANGE_INDEX)) {
          Log.getLogWriter().info("IndexName: " + index.getName());
          Log.getLogWriter().info(
              "IndexName: " + index.getName() + "  numKeys: " + numKeys);
          Log.getLogWriter().info(
              "IndexName: " + index.getName() + "  numValues: " + numValues);
          Log.getLogWriter().info(
              "IndexName: " + index.getName() + "  bucketSize"
                  + bucketIndexes.size());
          counters.add(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_KEYS, numKeys);
          counters.add(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_VALUES, numValues);
          counters.add(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_NUMBUCKETS, bucketIndexes.size());
          idCompactRangeNumKeys += numKeys;
          idCompactRangeNumValues += numValues;
        }
        else if (index.getName().startsWith(STATUS_COMPACT_RANGE_INDEX)) {
          Log.getLogWriter().info("IndexName: " + index.getName());
          Log.getLogWriter().info(
              "IndexName: " + STATUS_COMPACT_RANGE_INDEX + " numKeys: "
                  + numKeys);
          Log.getLogWriter().info(
              "IndexName: " + STATUS_COMPACT_RANGE_INDEX + " numValues: "
                  + numValues);
          Log.getLogWriter().info(
              "IndexName: " + STATUS_COMPACT_RANGE_INDEX + " bucketSize"
                  + bucketIndexes.size());
          counters.add(IndexBB.NUM_PR_COMPACTRANGE_STATUS_INDEX_VALUES,
              numValues);
          // if (numKeys > 2) {
          // Log.getLogWriter().info("WEIRD .............");
          // Log.getLogWriter().info(((CompactRangeIndex)bucketIndex).dump());
          // throw new TestException(
          // "The num Keys statusCompactRangeIndex cannot exceed 2");
          // }
          statusCompactRangeNumValues += numValues;
        }
        else if (index.getName().startsWith(ID_RANGE_INDEX)) {
          Log.getLogWriter().info("IndexName: " + index.getName());
          Log.getLogWriter().info(
              "IndexName: " + ID_RANGE_INDEX + " : numKeys: " + numKeys);
          Log.getLogWriter().info(
              "IndexName: " + ID_RANGE_INDEX + " : numValues: " + numValues);
          Log.getLogWriter().info(
              "IndexName: " + ID_RANGE_INDEX + " : bucketSize"
                  + bucketIndexes.size());
          counters.add(IndexBB.NUM_PR_RANGE_ID_INDEX_KEYS, numKeys);
          counters.add(IndexBB.NUM_PR_RANGE_ID_INDEX_VALUES, numValues);
          idRangeNumKeys += numKeys;
          idRangeNumValues += numValues;
        }
        else if (index.getName().startsWith(STATUS_RANGE_INDEX)) {
          Log.getLogWriter().info("IndexName: " + index.getName());
          Log.getLogWriter().info(
              "IndexName: " + STATUS_RANGE_INDEX + " :  numKeys: " + numKeys);
          Log.getLogWriter()
              .info(
                  "IndexName: " + STATUS_RANGE_INDEX + " : numValues: "
                      + numValues);
          Log.getLogWriter().info(
              "IndexName: " + STATUS_RANGE_INDEX + "  : bucketSize"
                  + bucketIndexes.size());
          counters.add(IndexBB.NUM_PR_RANGE_STATUS_INDEX_VALUES, numValues);
          // if (numKeys > 2) {
          // Log.getLogWriter().info("WEIRD .............");
          // Log.getLogWriter().info(((RangeIndex)bucketIndex).dump());
          // counters.add(IndexBB.NUM_PR_RANGE_STATUS_INDEX_VALUES, numValues);
          // throw new TestException("The num Keys "
          // + STATUS_RANGE_INDEX + "cannot exceed 2");
          // }
          statusRangeNumValues += numValues;
        }
        // }
      }
      else {
        numKeys = index.getStatistics().getNumberOfKeys();
        numValues = index.getStatistics().getNumberOfValues();
      }
    }
    Log.getLogWriter().info(
        "*******PRINTING BUCKET DATA FOR THIS VM***********");
    Log.getLogWriter().info(
        "* statusRangeNumBuckets = " + statusRangeNumBuckets);
    Log.getLogWriter().info(
        "*statusCompactRangeNumBuckets = " + statusCompactRangeNumBuckets);
    Log.getLogWriter().info("*idRangeNumBuckets = " + idRangeNumBuckets);
    Log.getLogWriter().info("*idCompactNumBuckets = " + idCompactNumBuckets);
    Log.getLogWriter().info(
        "*****************************************************");
    // Log.getLogWriter().info("* statusRangeNumKeys = " + statusRangeNumKeys);
    // Log.getLogWriter().info("*statusCompactRangeNumKeys =" +
    // statusCompactRangeNumKeys);
    Log.getLogWriter().info("*idRangeNumKeys = " + idRangeNumKeys);
    Log.getLogWriter()
        .info("*idCompactRangeNumKeys = " + idCompactRangeNumKeys);
    Log.getLogWriter().info(
        "*****************************************************");
    Log.getLogWriter().info("*statusRangeNumValues = " + statusRangeNumValues);
    Log.getLogWriter().info(
        "*statusCompactRangeNumValues = " + statusCompactRangeNumValues);
    Log.getLogWriter().info("*idRangeNumValues = " + idRangeNumValues);
    Log.getLogWriter().info(
        "*idCompactRangeNumValues = " + idCompactRangeNumValues);
    Log.getLogWriter().info(
        "*******END PRINTING BUCKET DATA FOR THIS VM***********");
  }

  /**
   * Verify index size based on the region size, for a non-partitioned region
   * 
   * @param index
   * @param region
   */
  public static void verifyIndexSizes(Index index, Region region) {
    long numKeys = index.getStatistics().getNumberOfKeys();
    long numValues = index.getStatistics().getNumberOfValues();
    long numUpdates = index.getStatistics().getNumUpdates();
    long regionKeysCount = region.keySet().size();
    Log.getLogWriter().info(index.getName() + " IndexName = "+ index.getName());
    Log.getLogWriter().info(index.getName() + " NumKeys = " + numKeys);
    Log.getLogWriter().info(index.getName() + " NumValues = " + numValues);
    Log.getLogWriter().info(index.getName() + " NumUpdates = " + numUpdates);
    Log.getLogWriter().info(
        index.getName() + " RegionKeyCount = " + regionKeysCount);

    if (index.getName().startsWith(QueryTest.ID_COMPACT_RANGE_INDEX)) {
      if (numKeys != regionKeysCount && numValues != regionKeysCount) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      if (numUpdates != regionKeysCount) {
        throw new TestException(
            "The num updates in index did not match with expectations. Expected = "
                + regionKeysCount + " Actual = " + numUpdates);
      }
    }
    else if (index.getName().startsWith(QueryTest.STATUS_COMPACT_RANGE_INDEX)) {
      if (numKeys != 2 && numValues != regionKeysCount) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      if (numUpdates != regionKeysCount) {
        throw new TestException(
            "The num updates in index did not match with expectations. Expected = "
                + regionKeysCount + " Actual = " + numUpdates);
      }
    }
    else if (index.getName().startsWith(QueryTest.ID_RANGE_INDEX)) {
      if (numKeys != regionKeysCount && numValues != regionKeysCount * 2) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      if (numUpdates != 2 * regionKeysCount) {
        throw new TestException(
            "The num updates in index did not match with expectations. Expected = "
                + 2 * regionKeysCount + " Actual = " + numUpdates);
      }

    }
    else if (index.getName().startsWith(QueryTest.STATUS_RANGE_INDEX)) {
      if (numKeys != 2 && numValues != regionKeysCount * 2) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      if (numUpdates != 2 * regionKeysCount) {
        throw new TestException(
            "The num updates in index did not match with expectations. Expected = "
                + 2 * regionKeysCount + " Actual = " + numUpdates);
      }
    }
  }

  /**
   * This method calculates the actual index sizes using index statistics and
   * compares them with the expected index sizes based on the data in region.
   * For partitioned region the index sizes are calculated by aggregating the
   * sizes of all bucket indexes.
   */
  public static void verifyIndexSizes_PR(PartitionedIndex index,
      PartitionedRegion region) {
    SharedCounters counters = IndexBB.getBB().getSharedCounters();

    // TODO: Add a check for number of indexes
    long numKeys = 0;
    long numValues = 0;
    long numBuckets = 0;
    /**********************************************************************************/
//    long regionKeysCount = region.keySet().size() * queryTest.numOfRegions;// Blackboard
//                                                                           // is
//                                                                           // updated
//                                                                           // numOfRegions
//                                                                           // times
    /**********************************************************************************/
    Set<Region<?, ?>> regions = CacheHelper.getCache().rootRegions();
    long keysPerRegion = region.keySet().size();
    long overallRegionKeysCount_withCopies = 0;  

    for (Region rgn : regions) {
      int redundantCopies = ((PartitionedRegion)rgn).getPartitionAttributes().getRedundantCopies();
      Log.getLogWriter().info("Before if  statement: overallRegionKeysCount_withCopies = " + overallRegionKeysCount_withCopies);
      Log.getLogWriter().info("Region name = " + rgn.getName());
      Log.getLogWriter().info("redundant copies = " + redundantCopies);
      Log.getLogWriter().info("RegionName: " + rgn.getName());
      EvictionAttributes evAttr = rgn.getAttributes().getEvictionAttributes();
      Log.getLogWriter().info("EvAttr: " + evAttr);
      Log.getLogWriter().info("evAttr.getAlgorithm() " + evAttr.getAlgorithm().toString());
      Log.getLogWriter().info("evAttr.getAction() " + evAttr.getAction().toString());
      boolean isOverflow = false;
      if (evAttr.getAlgorithm() != null && evAttr.getAction() != null) {
        if (!evAttr.getAlgorithm().equals(EvictionAlgorithm.NONE)
            && !evAttr.getAction().equals(EvictionAction.NONE)) {
          // This is an overflow region
          isOverflow = true;
        }
      }
      Log.getLogWriter().info("RegionName: " + rgn.getName() + " isOverflow:" + isOverflow);
      
      //If the region is overflow then there is no range index, so dont include in count.
      boolean isOverflowAndIndexIsRangeIndex = isOverflow
          && (index.getName().contains(STATUS_RANGE_INDEX) || index.getName()
              .contains(ID_RANGE_INDEX));
      if (!isOverflowAndIndexIsRangeIndex) {
        
        Log.getLogWriter().info("Inside if statement: overallRegionKeysCount_withCopies = " + overallRegionKeysCount_withCopies);
        Log.getLogWriter().info("Region name = " + rgn.getName());
        Log.getLogWriter().info("redundant copies = " + redundantCopies);
        overallRegionKeysCount_withCopies +=  keysPerRegion * (redundantCopies + 1);
      }
      Log.getLogWriter().info("After if  statement: overallRegionKeysCount_withCopies = " + overallRegionKeysCount_withCopies);
      Log.getLogWriter().info("Region name = " + rgn.getName());
      Log.getLogWriter().info("redundant copies = " + redundantCopies);
      
    }
    if (index.getName().startsWith(QueryTest.ID_COMPACT_RANGE_INDEX)) {
      numKeys = counters.read(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_KEYS);
      numValues = counters.read(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_VALUES);
      numBuckets = counters
          .read(IndexBB.NUM_PR_COMPACTRANGE_ID_INDEX_NUMBUCKETS);
      Log.getLogWriter().info(index.getName() + " numKeys: " + numKeys);
      Log.getLogWriter().info(index.getName() + " numValues: " + numValues);
      Log.getLogWriter().info(index.getName() + " numBuckets: " + numBuckets);
      Log.getLogWriter().info("RegionKeysCount: " + overallRegionKeysCount_withCopies);
      if (numKeys != overallRegionKeysCount_withCopies && numValues != overallRegionKeysCount_withCopies) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      // TODO: Verify numBuckets as well
      // if (numBuckets!=113) {
      // throw new TestException(
      // "NumBuckets was found to be: " + numBuckets);
      // }
    }
    else if (index.getName().startsWith(QueryTest.STATUS_COMPACT_RANGE_INDEX)) {
      numValues = counters
          .read(IndexBB.NUM_PR_COMPACTRANGE_STATUS_INDEX_VALUES);
      numBuckets = counters
          .read(IndexBB.NUM_PR_COMPACTRANGE_STATUS_INDEX_NUMBUCKETS);
      Log.getLogWriter().info(
          QueryTest.STATUS_COMPACT_RANGE_INDEX + " numValues: " + numValues);
      Log.getLogWriter().info(
          QueryTest.STATUS_COMPACT_RANGE_INDEX + " numBuckets: " + numBuckets);
      Log.getLogWriter().info("RegionKeysCount: " + overallRegionKeysCount_withCopies);
      if (numValues != overallRegionKeysCount_withCopies) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      // TODO: Verify numBuckets as well
      // if (numBuckets!=113) {
      // throw new TestException(
      // "NumBuckets was found to be: " + numBuckets);
      // }
    }
    else if (index.getName().startsWith(QueryTest.ID_RANGE_INDEX)) {
      numKeys = counters.read(IndexBB.NUM_PR_RANGE_ID_INDEX_KEYS);
      numValues = counters.read(IndexBB.NUM_PR_RANGE_ID_INDEX_VALUES);
      numBuckets = counters.read(IndexBB.NUM_PR_RANGE_ID_INDEX_NUMBUCKETS);
      Log.getLogWriter().info(index.getName() + " numKeys: " + numKeys);
      Log.getLogWriter().info(index.getName() + " numValues: " + numValues);
      Log.getLogWriter().info(index.getName() + " numBuckets: " + numBuckets);
      Log.getLogWriter().info("RegionKeysCount: " + overallRegionKeysCount_withCopies);
      if (numKeys != overallRegionKeysCount_withCopies && numValues != overallRegionKeysCount_withCopies * 2) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      // TODO: Verify numBuckets as well
      // if (numBuckets!=113) {
      // throw new TestException(
      // "NumBuckets was found to be: " + numBuckets);
      // }
    }
    else if (index.getName().startsWith(QueryTest.STATUS_RANGE_INDEX)) {
      numValues = counters.read(IndexBB.NUM_PR_RANGE_STATUS_INDEX_VALUES);
      numBuckets = counters.read(IndexBB.NUM_PR_RANGE_STATUS_INDEX_NUMBUCKETS);
      Log.getLogWriter().info(
          index.getName() + " numValues: " + numValues);
      Log.getLogWriter().info(
          index.getName() + " numBuckets: " + numBuckets);
      Log.getLogWriter().info("RegionKeysCount: " + overallRegionKeysCount_withCopies);
      if (numValues != overallRegionKeysCount_withCopies * 2) {
        throw new TestException(
            "The num Keys and values in index did not match with expectations");
      }
      // TODO: Verify numBuckets as well
      // if (numBuckets!=113) {
      // throw new TestException(
      // "NumBuckets was found to be: " + numBuckets);
      // }
    }
  }

  /**
   * This method calculates the actual index sizes using index statistics and
   * compares them with the expected index sizes based on the data in region.
   */
  public static void HydraTask_verifyIndexSizes() {
    Set<Region<?, ?>> regions = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info(
        "Printing all root regions in the Cache: " + regions.toString());

    Collection<Index> indexes = CacheHelper.getCache().getQueryService()
        .getIndexes();

    // TODO: Add a check for number of indexes
    for (Index index : indexes) {
      Region region = index.getRegion();
      if (region instanceof PartitionedRegion) {
        // Failing right now
         verifyIndexSizes_PR((PartitionedIndex) index,
         (PartitionedRegion)region);
      }
      else {
        verifyIndexSizes(index, region);
      }
    }

    Log.getLogWriter().info(
        "Printing all indexes in the Cache: " + indexes.toString());
    for (Index index : indexes) {
      Log.getLogWriter().info("Index name " + index.getName());
      Log.getLogWriter().info(
          "Index canonicalized from clause "
              + index.getCanonicalizedFromClause());
      Log.getLogWriter().info(
          "Index canonicalized index expression "
              + index.getCanonicalizedIndexedExpression());
      Log.getLogWriter().info(
          "Index canonicalized projection attributes "
              + index.getCanonicalizedProjectionAttributes());
    }
  }

  /**
   * Obtain the query results with indexes
   */
  public static void HydraTask_getQueryResultsWithIndexes() {
    serialTest.getQueryResultsWithIndexes();
  }

  /**
   * Obtain the query results with and without indexes and compare them. This is
   * general done in CLOSE TASK after doing various region operation like
   * add/destroy/update in TASK This provides an extra verification for various
   * index updates in Hydra concurrent environment.
   */
  public static void HydraTask_verifyQueryResultsWithAndWithoutIndexes() {
    serialTest.verifyQueryResultsWithAndWithoutIndexes();
  }

  /**
   * Obtain the query results with indexes
   */
  protected void getQueryResultsWithIndexes() {
    boolean isIndexValidation = TestConfig.tab().booleanAt(
        QueryPrms.isIndexUsageValidation, false);
    r = new SelectResults[this.numOfRegions][queries.length][2];
    final int threshold = 30000;

    try {
      String indexType = TestConfig.tab().stringAt(IndexPrms.indexType,
          "compactRange");
      if (!indexType.equals("all")) {
        queryToIndexMap.put(2, QueryTest.ID_COMPACT_RANGE_INDEX);
        queryToIndexMap.put(3, QueryTest.STATUS_COMPACT_RANGE_INDEX);
      }
      // Run queries with index
      for (int i = 0; i < queries.length; i++) {
        QueryObserverImplLocal qol = new QueryObserverImplLocal();
        QueryObserverHolder.setInstance(qol);
        for (int regionNum = 0; regionNum < numOfRegions; regionNum++) {
          Query query = CacheHelper
              .getCache()
              .getQueryService()
              .newQuery(
                  SELECT_STRING + ("/" + REGION_NAME + regionNum) + queries[i]);
          String regionName = ("/" + REGION_NAME + regionNum);
          Region region = CacheHelper.getCache().getRegion(regionName);
          Log.getLogWriter().info(
              "Obtaining index " + queryToIndexMap.get(i) + " on Region: "
                  + regionName);
          Index index = CacheHelper.getCache().getQueryService()
              .getIndex(region, queryToIndexMap.get(i) + regionNum);

          Collection<Index> indexes = CacheHelper.getCache().getQueryService()
              .getIndexes();
          if (index == null) {
            // BUG: 44192 causes index to be null in case of PartitionedRegion

            Log.getLogWriter().info(
                "Encountered BUG: 44192 while Obtaining index "
                    + queryToIndexMap.get(i) + " on Region: "
                    + region.getFullPath());
            Log.getLogWriter().info(
                "Encountered BUG: 44192 while Obtaining index "
                    + queryToIndexMap.get(i) + " on Region: "
                    + region.getName());
            Log.getLogWriter().info(
                "Encountered BUG: 44192 while Obtaining index "
                    + queryToIndexMap.get(i) + " on Region: " + regionName);
            Log.getLogWriter().info(
                "********************Printing all indexes in the Cache: "
                    + indexes.toString());
            if (indexes != null) {
              for (Index indexItr : indexes) {
                Log.getLogWriter().info("Index name " + indexItr.getName());
                if (indexItr.getName().equals(
                    queryToIndexMap.get(i) + regionNum)) {
                  index = indexItr;
                }
                Log.getLogWriter().info(
                    "Index type " + indexItr.getClass().getName());
                Log.getLogWriter().info(
                    "Index canonicalized from clause "
                        + indexItr.getCanonicalizedFromClause());
                Log.getLogWriter().info(
                    "Index canonicalized index expression "
                        + indexItr.getCanonicalizedIndexedExpression());
                Log.getLogWriter().info(
                    "Index canonicalized projection attributes "
                        + indexItr.getCanonicalizedProjectionAttributes());
              }
            }
          }
          if (index == null) {
            throw new TestException("Could not find index with name: "
                + queryToIndexMap.get(i));
          }

          if (!(index instanceof PartitionedIndex)) {
            long totalUses = index.getStatistics().getTotalUses();

            r[regionNum][i][0] = (SelectResults)query.execute();
            // TODO: Need to figure out a mechanism to use query observer to
            // validate exact index.
            // if (qol.indexName == null ||
            // !qol.indexName.equals(queryToIndexMap.get(i))) {
            // throw new TestException(
            // "The query did not use an index as expected. Indexname found to be:"
            // + qol.indexName + " Expected was: " + queryToIndexMap.get(i));
            // }
            if (index.getStatistics().getTotalUses() <= totalUses) {
              throw new TestException(
                  "Total use count is expected to be increased but found to be reduced or unaffected");
            }
          }
          else {
            // TODO: Need to figure out some mechanism to make sure index was
            // used
            // in case of partitioned region as well
            r[regionNum][i][0] = (SelectResults)query.execute();
          }
        }

      }

    }
    catch (Exception e) {
      throw new TestException("Caught exception during query execution"
          + TestHelper.getStackTrace(e));
    }

  }

  /**
   * Obtain the query results with and without indexes and compare them. This is
   * general done in CLOSE TASK after doing various region operation like
   * add/destroy/update in TASK This provides an extra verification for various
   * index updates in Hydra concurrent environment.
   */
  protected void verifyQueryResultsWithAndWithoutIndexes() {
    // TODO: Make sure that indexes are used for this query.
    Set<Region<?, ?>> regions = CacheHelper.getCache().rootRegions();
    Log.getLogWriter().info(
        "Printing all root regions in the Cache: " + regions.toString());
    synchronized (this) {
      for (Region region : regions) {
        Log.getLogWriter().info("Region name " + region.getName());
        Collection<Index> indexes = CacheHelper.getCache().getQueryService()
            .getIndexes();
        Log.getLogWriter().info(
            "All indexes on region: " + region.getName() + " Indexes : ="
                + indexes.toString());

        Log.getLogWriter().info(
            "Printing all indexes in the Cache: " + indexes.toString());
        if (indexes != null) {
          for (Index index : indexes) {
            Log.getLogWriter().info("Index name " + index.getName());
            Log.getLogWriter().info(
                "Index canonicalized from clause "
                    + index.getCanonicalizedFromClause());
            Log.getLogWriter().info(
                "Index canonicalized index expression "
                    + index.getCanonicalizedIndexedExpression());
            Log.getLogWriter().info(
                "Index canonicalized projection attributes "
                    + index.getCanonicalizedProjectionAttributes());
            CacheHelper.getCache().getQueryService().removeIndex(index);
          }

        }
      }
    }
    ;

    try {
      for (int i = 0; i < queries.length; i++) {
        QueryObserverImplLocal qol = new QueryObserverImplLocal();
        QueryObserverHolder.setInstance(qol);
        Query query = CacheHelper.getCache().getQueryService()
            .newQuery(SELECT_STRING + ("/" + REGION_NAME + 0) + queries[i]);
        for (int regionNum = 0; regionNum < numOfRegions; regionNum++) {
          r[regionNum][i][1] = (SelectResults)query.execute();

        }
        if (qol.indexName != null) {
          throw new TestException("The query unexpectedly used an index: "
              + qol.indexName);
        }
      }

    }
    catch (Exception e) {
      throw new TestException("Caught exception during query execution"
          + TestHelper.getStackTrace(e));
    }

    for (int regionNum = 0; regionNum < numOfRegions; regionNum++) {
      if (!CacheUtils.compareResultsOfWithAndWithoutIndex(r[regionNum])) {
        throw new TestException(
            "The results with and without indexes did not match");
      }
    }
  }

  /**
   * Randomly stop and restart vms.
  */
  public static void HydraTask_stopStartVMs_bridgeVMs() {
     int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);  
     Object[] tmpArr = StopStartVMs.getOtherVMsWithExclude(numVMsToStop, "locator");
     List vms = (List)(tmpArr[0]);
     List stopModes = (List)(tmpArr[1]);
     for (int i = 0; i < vms.size(); i++) {
        ClientVmInfo info = (ClientVmInfo)(vms.get(i));
        PRObserver.initialize(info.getVmid());
     }
     StopStartVMs.stopStartVMs(vms, stopModes);
  }

  /**
   * Randomly stop and restart vms and query on partitioned region data.
  */
  public static void HydraTask_stopStartVMs_queryPRData() {
    try {Thread.sleep(30000);} catch(InterruptedException e) {/**/}
    HydraTask_stopStartVMs_bridgeVMs();
    try {
      HydraTask_verifyIndexSize_QueryData();
    } catch(QueryInvocationTargetException e) {
      throw new TestException("Unexpectedly Caught QueryInvocationTargetException" + e);
    } catch (Exception e) {
      throw new TestException("Unexpectedly Caught " + e);
    }
  }
      
  /**
   * Randomly stop and restart vms and query on partitioned region data.
  */
  public static void HydraTask_stopVM_verifyIndex_queryPRData_startVM() {
     try {Thread.sleep(30000);} catch(InterruptedException e) {/**/}
     int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);  
     Object[] tmpArr = StopStartVMs.getOtherVMsWithExclude(numVMsToStop, "edge");
     List vms = (List)(tmpArr[0]);
     List stopModes = (List)(tmpArr[1]);
     for (int i = 0; i < vms.size(); i++) {
        ClientVmInfo info = (ClientVmInfo)(vms.get(i));
        PRObserver.initialize(info.getVmid());
     }
     //StopStartVMs.stopStartVMs(vms, stopModes);
     StopStartVMs.stopVMs(vms, stopModes);
     try {
       Region region = CacheHelper.getCache().getRegion("/QueryRegion0");
       try {Thread.sleep(30000);} catch(InterruptedException e) {/**/}
       ParRegUtil.doRebalance();
       HydraTask_verifyIndexSize_QueryData();
     } catch(QueryInvocationTargetException e) {
       throw new TestException("Unexpectedly Caught QueryInvocationTargetException" + e);
     } catch (Exception e) {
       throw new TestException("Unexpectedly Caught " + e);
     }
     StopStartVMs.startVMs(vms);
  }
  
}

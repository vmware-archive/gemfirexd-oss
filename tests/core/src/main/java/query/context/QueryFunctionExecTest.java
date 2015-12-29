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
package query.context;

import hydra.CacheHelper;

import hydra.HydraVector;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import objects.ObjectHelper;
import objects.PRPortfolio;
import parReg.ParRegUtil;
import query.QueryPrms;
import query.QueryTest;

import util.NameFactory;
import util.TestException;


import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import event.EventPrms;

/**
 * Backbone test class for testing Context based Querying using localDataSet API 
 * @author kdeshpan
 * @since 6.6.2
 */
public class QueryFunctionExecTest extends QueryTest {

  protected static int numOfRegions;
  protected static boolean isSerialExecution = false;
  static protected final int QUERY_NO_FILTER_OPERATION = 8;
  static protected final int QUERY_SINGLE_FILTER_OPERATION = 9;
  static protected final int QUERY_FILTER_OPERATION = 10;
  static protected final int REGION_CLOSE_REBLANCE_OPERATION = 11;
  
  /**
   * Initialize peer nodes in P2P topology
   */
  public static void HydraTask_initialize_peers() {
    queryTest = new QueryFunctionExecTest();
    isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);
    numOfRegions = TestConfig.getInstance().getRegionDescriptions().size();
    queryTest.initialize();
  }

  /**
   * Hydra task for populating region data
   */
  public static void HydraTask_populateRegions() {
    queryTest = new QueryFunctionExecTest();
    isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);
    ((QueryFunctionExecTest)queryTest).populateRegions();
  }
  
  /**
   * Populate regions
   */
  protected void populateRegions() {
    maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, 20000);
    List<Region> regionList = new ArrayList(CacheHelper.getCache()
        .rootRegions());
    for (Region aRegion : regionList) {
      Map aMap = new HashMap();
      for (int j = 0; j < maxObjects; j++) {
        String name = NameFactory.getNextPositiveObjectNameInLimit(maxObjects);
        aMap.put(new Integer(j), new PRPortfolio(name + j, j));
      }
      aRegion.putAll(aMap);
    }
  }

  /**
   * Verify region sizes and values
   */
  public static void HydraTask_verifyRegionSizeAndValues() {
    List<Region> regionList = new ArrayList(CacheHelper.getCache()
        .rootRegions());
    Region aRegion = regionList.get(0);
    if (!aRegion.getName().startsWith("QueryRegion")) {
      throw new TestException("Expected region name: QueryRegionX Actual:"
          + aRegion.getName());
    }
    int numObj = TestConfig.tab().intAt(EventPrms.maxObjects, 20000);
    if (aRegion.keySet().size() != numObj) {
      throw new TestException("Expected region size: " + numObj + " Actual:"
          + aRegion.keySet().size());
    }
  }

  @Override
  public void initialize() {
    maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, 20000);
    CacheHelper.createCache("cache1");
    String regionDescriptionName = "region1";
    AttributesFactory factory = RegionHelper
        .getAttributesFactory(regionDescriptionName);
    Region region1 = RegionHelper.createRegion(REGION_NAME + 1, factory);
    for (int j = 0; j < maxObjects; j++) {
      region1.put(new Integer(j), new PRPortfolio("name" + j, j));
    }
  }

  /**
   * INITTASK registering function for random function execution test
   */
  public static void HydraTask_initRegisterFunction() {
    Function prQueryFunction1 = new QueryFunction();
    FunctionService.registerFunction(prQueryFunction1);
  }

  /**
   * Do entry and query operations in parallel
   */
  public static void HydraTask_doEntryAndQueryOperations() {
    Log.getLogWriter().info("INSIDE HydraTask_doEntryAndQueryOperations " );
    if (queryTest == null) {
      queryTest = new QueryFunctionExecTest();
    }
    if (queryTest != null) {
      Log.getLogWriter().info("INSIDE HydraTask_doEntryAndQueryOperations IF " );
      ((QueryFunctionExecTest)queryTest).doEntryAndQueryOperations();
    }
 }

  /**
   * Perform various operations concurrently
   */
  protected void doEntryAndQueryOperations() {
    maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, 20000);
    Log.getLogWriter().info("INSIDE doEntryAndQueryOperations " );
    Region aRegion =  CacheFactory.getAnyInstance().getRegion(REGION_NAME + 1);
    long startTime = System.currentTimeMillis();
    long iter = 0;
    do {
      Log.getLogWriter().info("INSIDE DO iter = " + ++iter);
       try {
         Log.getLogWriter().info("INSIDE TRY iter = " + iter);
          int whichOp = getOperation(QueryPrms.entryAndQueryOperations);
          Log.getLogWriter().info("INSIDE TRY whichOp =  " + whichOp);
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
             case QUERY_NO_FILTER_OPERATION:
               Log.getLogWriter().info("Invoking function with no filter");
                HydraTask_ExecuteFunction_NoFilter();
                break;
             case QUERY_FILTER_OPERATION:
               Log.getLogWriter().info("Invoking function with filter keys");
               HydraTask_ExecuteFunction_Filter();
               break;
             case QUERY_SINGLE_FILTER_OPERATION:
               Log.getLogWriter().info("Invoking function with single filter key");
               HydraTask_ExecuteFunction_Filter_SingleKey();
               break;
             case REGION_CLOSE_REBLANCE_OPERATION:
               Log.getLogWriter().info("Invoking function PR close and rebalance");
               HydraTask_close_rebalancePR();
               break;
             default: {
                throw new TestException("Unknown operation " + whichOp);
             }
          }
       } finally {
       }
     } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  }
  
  /**
   * Get an operation.
   * Picked up randomly
   * @param whichPrm
   * @return
   */
  protected static int getOperation(Long whichPrm) {
    long limit = 60000;
    long startTime = System.currentTimeMillis();
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    if (operation.equals("add"))
       op =  ADD_OPERATION;
    else if (operation.equals("update"))
       op =  UPDATE_OPERATION;
    else if (operation.equals("invalidate"))
       op =  INVALIDATE_OPERATION;
    else if (operation.equals("destroy"))
       op =  DESTROY_OPERATION;
    else if (operation.equals("read"))
       op =  READ_OPERATION;
    else if (operation.equals("query_no_filter"))
       op = QUERY_NO_FILTER_OPERATION;
    else if (operation.equals("query_filter"))
      op = QUERY_FILTER_OPERATION;
   else if (operation.equals("query_single_filter"))
       op = QUERY_SINGLE_FILTER_OPERATION;
   else if (operation.equals("region_close_rebalance"))
     op = REGION_CLOSE_REBLANCE_OPERATION;
    else
       throw new TestException("Unknown entry operation: " + operation);
    if (System.currentTimeMillis() - startTime > limit) {
       // could not find a suitable operation in the time limit; there may be none available
       throw new TestException("Could not find an operation in " + limit + " check that the operations list has allowable choices");
    }
    return op;
  }
  
  /**
   * Get an Object to add
   * @param key
   * @return
   */
  protected Object getObjectToAdd(Long key) {
    long i = NameFactory.getPositiveNameCounter();
    int index = (int)(i % maxObjects);
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType);
    Object val = ObjectHelper.createObject( objectType, index );
    return val;
 }

  /**
   * Add new object
   * @param aRegion
   */
  protected void addObject(Region aRegion) {
    long key = NameFactory.getPositiveNameCounter();
    aRegion.put(key, getObjectToAdd(key));
  }

  /**
   * Invalidate object
   * @param aRegion
   */
  protected void invalidateObject(Region aRegion) {
    Set aSet = aRegion.keySet();
    if (aSet.size() == 0) {
       Log.getLogWriter().info("invalidateObject: No names in region");
       return;
    }
    Iterator it = aSet.iterator();
    Object key = null;
    if (it.hasNext()) {
      key = it.next();
    } else { // has been destroyed cannot continue
       Log.getLogWriter().info("invalidateObject: Unable to get key from region");
       return; 
    }
    boolean containsValue = aRegion.containsValueForKey(key);
    boolean alreadyInvalidated = !containsValue;
    if (!alreadyInvalidated) {
      aRegion.invalidate(key);
    }
  }
  
  /**
   * Destroy an entry
   * @param aRegion
   */
  protected void destroyObject(Region aRegion) {
    Set aSet = aRegion.keySet();
    if (aSet.size() == 0) {
       Log.getLogWriter().info("destroyObject: No names in region");
       return;
    }
    Iterator it = aSet.iterator();
    Object key = null;
    if (it.hasNext()) {
      key = it.next();
    } else {
       Log.getLogWriter().info("destroyObject: Unable to get key from region");
       return; 
    }
    aRegion.destroy(key);
  }
  
  /**
   * Get an object (value) for update purpose
   * @return
   */
  protected Object getUpdateObject() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType);
    Object val = ObjectHelper.createObject( objectType, 0 );
    return val;
  }
  
  /**
   * Update an object
   */
  @Override
  protected void updateObject(Region aRegion) {
    Set aSet = aRegion.keySet();
    if (aSet.size() == 0) {
       Log.getLogWriter().info("updateObject: No names in region");
       return;
    }
    Iterator it = aSet.iterator();
    Object key = null;
    if (it.hasNext()) {
      key = it.next();
    } else {
       Log.getLogWriter().info("updateObject: Unable to get key from region");
       return; 
    }
    aRegion.put(key, getUpdateObject());
  }
  
  /**
   * Read object
   */
  @Override
  protected void readObject(Region aRegion) {
    Set aSet = aRegion.keySet();
    if (aSet.size() == 0) {
       Log.getLogWriter().info("readObject: No names in region");
       return;
    }
    Iterator it = aSet.iterator();
    Object key = null;
    if (it.hasNext()) {
      key = it.next();
    } else {
       Log.getLogWriter().info("readObject: Unable to get key from region");
       return; 
    }
    aRegion.get(key);
  }
  
  /**
   * Execute function without any filter keys
   */
  public static void HydraTask_ExecuteFunction_NoFilter() {

    HydraVector hvExpResultSizes = TestConfig.tab().vecAt(
        QueryPrms.expectedQueryResultSizes, null);
    HydraVector hvQueries = TestConfig.tab()
        .vecAt(QueryPrms.queryStrings, null);
    if (hvQueries == null) {
      throw new TestException("No Queries specified ");
    }

    if (hvExpResultSizes == null) {
      throw new TestException(
          "Need expected query result sizes to run this method ");
    }
    if (hvExpResultSizes.size() != hvQueries.size()) {
      throw new TestException(
          "Query count and their result sizes count should be same in conf file ");
    }

    String[] expectedResultSizesStrs = new String[hvExpResultSizes.size()];
    Integer[] expectedResultSizes = new Integer[hvExpResultSizes.size()];
    String queries[] = new String[hvQueries.size()];
    hvQueries.copyInto(queries);
    hvExpResultSizes.copyInto(expectedResultSizesStrs);
    for (int i = 0; i < expectedResultSizesStrs.length; ++i) {
      expectedResultSizes[i] = Integer.parseInt(expectedResultSizesStrs[i]
          .trim());
    }
    int[] resultSizes = new int[queries.length];
    for (int i = 0; i < queries.length; i++) {
      SharedCounters counters = QueryFunctionContextBB.getBB()
          .getSharedCounters();
      if(isSerialExecution) {
        counters.zero(QueryFunctionContextBB.NUM_NODES);
      }
      Function function = new QueryFunction();
      ArrayList argList = new ArrayList();
      argList.add(queries[i]);
      argList.add(RemoteTestModule.getCurrentThread().getThreadId());
      Object result = FunctionService
          .onRegion(CacheFactory.getAnyInstance().getRegion(REGION_NAME + 1))
          .withArgs(argList).execute(function).getResult();
      
      ArrayList resultList = (ArrayList)result;
      resultList.trimToSize();
      List queryResults = null;
      Log.getLogWriter().info("Size of the resultsList: " + resultList.size());
      if (resultList.size() != 0 /* && resultList.get(0) instanceof ArrayList */) {
        queryResults = new ArrayList();
        for (Object obj : resultList) {
          if (obj != null) {
            queryResults.addAll((ArrayList)obj);
          }
        }
      }
      if (isSerialExecution) {
        try {
          //This sleep is added just to make sure that the shared blackboard counters
          // are updated before we verify the counts.
          Thread.sleep(15000);
        }
        catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        long numNodes = counters.read(QueryFunctionContextBB.NUM_NODES);
        if (numNodes != 3) {
          throw new TestException(
              "With no filters function call was not sent to all (3) PR nodes Actual count of nodes "
                  + numNodes);
        }
      }
      
      if (queryResults != null) {
        Log.getLogWriter().info(
            "CLIENT SIDE: Size of the query results: " + queryResults.size());
        resultSizes[i] = queryResults.size();
        verifyQueryResults(i, queryResults);
      }
      else {
        throw new TestException("UNEXPECTED queryResults are found to be empty");
      }
    }
    if (isSerialExecution) {
      Log.getLogWriter().info(
          "CLIENT SIDE: Query Result sizes: " + resultSizes.toString());
      for (int i = 0; i < resultSizes.length; i++) {
        if (resultSizes[i] != expectedResultSizes[i]) {
          throw new TestException("Expected result size mismatch for query # "
              + (i + 1) + " Expected size: " + expectedResultSizes[i]
              + "Actual size :" + resultSizes[i]);
        }
      }
    }
  }

  /** 
   * Execute function with single filter key
   */
  public static void HydraTask_ExecuteFunction_Filter_SingleKey() {
    Set<Integer> filter = new HashSet<Integer>();
    filter.add(1);

    String queries[] = new String[] { "select distinct * from /QueryRegion1 r1",
    /* "select distinct * from /QueryRegion1 r1, /QueryRegion2 r2 WHERE r1.ID = r2.ID" */};
    int[] resultSizes = new int[queries.length];
    for (int i = 0; i < queries.length; i++) {
      SharedCounters counters = QueryFunctionContextBB.getBB()
      .getSharedCounters();
      if (isSerialExecution) {
        counters.zero(QueryFunctionContextBB.NUM_NODES);
      }
      Function function = new QueryFunction();
      ArrayList argList = new ArrayList();
      argList.add(queries[i]);
      argList.add(RemoteTestModule.getCurrentThread().getThreadId());
      Object result = FunctionService
          .onRegion(CacheFactory.getAnyInstance().getRegion(REGION_NAME + 1))
          .withArgs(argList).withFilter(filter).execute(function)
          .getResult();
      
      ArrayList resultList = (ArrayList)result;
      resultList.trimToSize();
      List queryResults = null;
      Log.getLogWriter().info("Size of the resultsList: " + resultList.size());
      // Log.getLogWriter().info("Actual resultsList: " +
      // resultList.toString());
      if (resultList.size() != 0 /* && resultList.get(0) instanceof ArrayList */) {
        queryResults = new ArrayList();
        for (Object obj : resultList) {
          if (obj != null) {
            queryResults.addAll((ArrayList)obj);
          }
        }
      }
      if (isSerialExecution) {
        long numNodes = counters.read(QueryFunctionContextBB.NUM_NODES);
        if (numNodes != 1) {
          throw new TestException(
              "Single filter key function call was not sent to exactly one PR node. Actual count of nodes "
                  + numNodes);
        }
      }
      verifyQueryResults(i, queryResults);
      boolean key1exists = false;
      if (queryResults != null) {
        Log.getLogWriter().info(
            "CLIENT SIDE: Size of the query results: " + queryResults.size());
        resultSizes[i] = queryResults.size();
        for (Object obj : queryResults) {
          PRPortfolio pfo = null;
          if (obj instanceof StructImpl) {
            StructImpl structObj = (StructImpl)obj;
            Object[] fieldVals = structObj.getFieldValues();
            pfo = (PRPortfolio)fieldVals[0];
          }
          else {
            if (obj instanceof PRPortfolio) {
              pfo = (PRPortfolio)obj;
            }
            else {
              Log.getLogWriter().info("Unexpected objet type: " + obj.getClass().getName());
              continue;
            }
          }
          Log.getLogWriter().info("Portfolio ID: " + pfo.getId());
          int ID = pfo.getId();
          if (ID == 1) {
            key1exists = true;
          }
        }
      }
      else {
        throw new TestException("UNEXPECTED queryResults are found to be empty");
      }
      if (isSerialExecution) {
        if (!key1exists) {
          throw new TestException("UNEXPECTED queryResults did not contain the filter key = 1");
        }
      }
    }
    Log.getLogWriter().info(
        "CLIENT SIDE: Query Result sizes: " + resultSizes.toString());
  }

  /**
   * Execute function with filter keys
   */
  public static void HydraTask_ExecuteFunction_Filter() {
    int bitmap = 0; 
    // bit vector used to set bits as the expected keys are found in the result
    // Since we are calling filter execution for nine keys 1 though 9, we expect that 
    // the least significant 9 bits in the bitmap will be set.
    // So we expect that the bitmap value becomes 511 after going through all result objects.
    Set<Integer> filter = new HashSet<Integer>();
    for (int key = 1; key < 10; key++) {
      filter.add(key);
    }

    String queries[] = new String[] { "select distinct * from /QueryRegion1 r1",
    };
    int[] resultSizes = new int[queries.length];
    for (int i = 0; i < queries.length; i++) {
    	Log.getLogWriter().info(
          "CLIENT SIDE: Executing query: " + queries[i]);
      Function function = new QueryFunction();
      ArrayList argList = new ArrayList();
      argList.add(queries[i]);
      argList.add(RemoteTestModule.getCurrentThread().getThreadId());
      Object result = FunctionService
          .onRegion(CacheFactory.getAnyInstance().getRegion(REGION_NAME + 1))
          .withArgs(argList).withFilter(filter).execute(function)
          .getResult();
      ArrayList resultList = (ArrayList)result;
      resultList.trimToSize();
      List queryResults = null;
      Log.getLogWriter().info("Size of the resultsList: " + resultList.size());

      if (resultList.size() != 0) {
        queryResults = new ArrayList();
        for (Object obj : resultList) {
          if (obj != null) {
            queryResults.addAll((ArrayList)obj);
          }
        }
      }
      if (queryResults != null) {
        Log.getLogWriter().info(
            "CLIENT SIDE: Size of the query results: " + queryResults.size());
        // Log.getLogWriter().info("CLIENT SIDE: Printing all Portfolio IDs: " +
        // queryResults.toString());
        resultSizes[i] = queryResults.size();
        verifyQueryResults(i, queryResults);
        for (Object obj : queryResults) {
          PRPortfolio pfo = null;
          Log.getLogWriter().info("ACTUAL QUERY RESULT OBJ: " + obj.toString());
          Log.getLogWriter()
              .info(
                  "ACTUAL QUERY RESULT OBJ CLASS NAME: "
                      + obj.getClass().getName());
          if (obj instanceof StructImpl) {
            StructImpl structObj = (StructImpl)obj;
            Object[] fieldVals = structObj.getFieldValues();
            pfo = (PRPortfolio)fieldVals[0];
          }
          else {
            if (obj instanceof PRPortfolio) {
              pfo = (PRPortfolio)obj;
            }
            else {
              continue;
            }
          }
          Log.getLogWriter().info("Portfolio ID: " + pfo.getId());
          int ID = pfo.getId();
          if (ID <= 9 && ID >= 1) {
            bitmap = bitmap | (1 << (ID - 1));
          }
        }
        if (isSerialExecution) {
          if (bitmap != 511) {
            // the least significant 9 bits in the bitmap will be set.
            // So we expect that the bitmap value becomes 511 after going through all result objects.
            throw new TestException("BITMAP expected 511 Found: " + bitmap);
          }
          else {
            Log.getLogWriter().info("BITMAP = 511 !!! ");
          }
        }
      }
      else {
        throw new TestException("UNEXPECTED queryResults are found to be empty");
      }
    }
    Log.getLogWriter().info(
        "CLIENT SIDE: Query Result sizes: " + resultSizes.toString());
  }

  /**
   * Set query observer for destroying bucket region  
   */
  public static void HydraTask_Set_BucketRegion_Destroy_QueryObserver() {
    class MyQueryObserver extends IndexTrackingQueryObserver {

      @Override
      public void startQuery(Query query) {
        Region pr = CacheFactory.getAnyInstance().getRegion("QueryRegion1");
        Region KeyRegion = null;
        for (int i = 10; i < 20; i++) {
          KeyRegion = ((PartitionedRegion)pr).getBucketRegion(i/* key */);
          if (KeyRegion != null) {
            Log.getLogWriter().info("Destroying bucket region::");
            try {
              KeyRegion.destroyRegion();
            }
            catch (com.gemstone.gemfire.cache.RegionDestroyedException e) {
              Log.getLogWriter().info(
                  "OBSERVER::Ignoring RegionDestroyedException");
              continue;
            }
            catch (Exception e) {
              Log.getLogWriter().info(
                  "OBSERVER::Exception class is :" + e.getClass().getName());
              Log.getLogWriter().info(
                  "OBSERVER::Exception cause class is :"
                      + e.getCause().getClass().getName());
              throw new TestException(
                  "OBSERVER::Caught EXCEPTION during Bucket destroy "
                      + e.getMessage());
            }
            Log.getLogWriter().info("Destroyed bucket region successfully!!");
          }
        }
      }
    }
    ;

    QueryObserverHolder.setInstance(new MyQueryObserver());
  }

  /**
   * Set queryobserver which will invoke bucket rebalance upon startQuery() callback
   */
  public static void HydraTask_Set_BucketRegion_BucketRebalance_QueryObserver() {
    class MyQueryObserver extends IndexTrackingQueryObserver {
      @Override
      public void startQuery(Query query) {
        new Thread() {
          public void run() {
            ParRegUtil.doRebalance();
          }
        }.start();
      }
    }
    ;

    QueryObserverHolder.setInstance(new MyQueryObserver());
  }

  /**
   * Hydratask for verifying function execution when region destroy happens on
   * server
   */
  public static void HydraTask_ExecuteFunction_RegionDestroyUsingFilter() {
    Set<Integer> filter = new HashSet<Integer>();
    for (int key = 10; key < 20; key++) {
      filter.add(key);
    }

    String queries[] = new String[] { "select distinct * from /QueryRegion1 r1" };
    int[] resultSizes = new int[queries.length];
    try {
      Function function = new QueryFunction();
      ArrayList argList = new ArrayList();
      argList.add(queries[0]);
      argList.add(RemoteTestModule.getCurrentThread().getThreadId());
      Object result = FunctionService
          .onRegion(CacheFactory.getAnyInstance().getRegion(REGION_NAME + 1))
          .withArgs(argList).withFilter(filter).execute(function)
          .getResult();

      throw new TestException(
          "Should have received an QueryInvocationTargetException but no excpetion was thrown");
    }
    catch (FunctionException ex) {
      // ex.printStackTrace();
      if (!(ex.getCause() instanceof QueryInvocationTargetException)) {
        Log.getLogWriter().info(
            "Exception cause class is :" + ex.getCause().getClass().getName());
        throw new TestException(
            "Should have received an QueryInvocationTargetException but recieved"
                + ex.getMessage());
      }
    }
    catch (Exception e) {
      throw new TestException(
          "Caught EXCEPTION during Querying and Region Bucket destroy "
              + e.getMessage());
    }
  }
  
  /**
   * Verify query results for correctness
   * @param queryIndex
   * @param queryResults
   */
  public static void verifyQueryResults(int queryIndex, List queryResults) {
    PRPortfolio pfo = null;
    for (Object obj : queryResults) {
      if (obj instanceof StructImpl) {
        StructImpl structObj = (StructImpl)obj;
        Object[] fieldVals = structObj.getFieldValues();
        pfo = (PRPortfolio)fieldVals[0];
        performSanityChecks(queryIndex, new PRPortfolio[]{pfo,(PRPortfolio)fieldVals[1]});
      } else if (obj instanceof PRPortfolio) {
          pfo = (PRPortfolio)obj;
          performSanityChecks(queryIndex, new PRPortfolio[]{pfo,null});
      } else {
          continue;
      }
    }
  }
  
  /**
   * Perform sanity checks on the query results
   * @param queryIndex
   * @param pfo
   */
  public static void performSanityChecks(int queryIndex, PRPortfolio[] pfo) {
    switch (queryIndex) {
      case 0: break;
      case 1: if (pfo[0].ID < 100) {
          throw new TestException(
              "Result verification for the query failed ID expected below 100, but actually: "
                  + pfo[0].ID);
      }
      break;
      case 2: if(!pfo[0].status.equals("active")) {
          throw new TestException(
              "Result verification for the query failed status expected active, but actually: "
                  + pfo[0].status);
      }
      break;
      case 3: if((pfo[0].ID < 100) || !pfo[0].status.equals("active")) {
          throw new TestException(
              "Result verification for the query failed pfo[0].ID = "
                  + pfo[0].ID + " pfo[0].status = " + pfo[0].status);
      }
      break;
      case 4: if(pfo[0].ID != pfo[1].ID) {
          throw new TestException(
              "Result verification for the query failed pfo[0].ID = "
                  + pfo[0].ID + " pfo[1].ID = " + pfo[1].ID);
      }
      break;
      case 5: if (pfo[0].ID != pfo[1].ID || !pfo[0].status.equals("active")) {
        throw new TestException("Result verification for the query failed pfo[0].ID = "
                  + pfo[0].ID + " pfo[1].ID = " + pfo[1].ID + " pfo[0].status = " + pfo[0].status);
      }
      break;
      case 6: if (pfo[0].ID != pfo[1].ID || pfo[0].ID < 500 || !pfo[1].status.equals("active")) {
        throw new TestException("Result verification for the query failed pfo[0].ID = "
                  + pfo[0].ID + " pfo[1].ID = " + pfo[1].ID + " pfo[1].status = " + pfo[1].status);
      }
      break;
      default: throw new TestException("Unexpected query index");
    }
  }
  
  public static void HydraTask_close_rebalancePR() {
    Region aRegion = CacheFactory.getAnyInstance().getRegion(REGION_NAME + 1);

    RegionAttributes attrs = aRegion.getAttributes();
    PartitionAttributes pAttrs = attrs.getPartitionAttributes();
    
    int oldLocalMaxMem = pAttrs.getLocalMaxMemory();
    
    Log.getLogWriter().info("Initial local max memory = " + oldLocalMaxMem);
    
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setPartitionAttributes(attrs.getPartitionAttributes());
    PartitionAttributesFactory paFactory = new PartitionAttributesFactory(attrs.getPartitionAttributes());
    
    paFactory.setLocalMaxMemory(0);
    
    aRegion.close();
    factory.setPartitionAttributes(paFactory.create());
    aRegion = RegionHelper.createRegion(REGION_NAME + 1, factory.create());
    Log.getLogWriter().info(
        "Created partitioned region " + REGION_NAME + 1);
    
    ParRegUtil.doRebalance();
    
    aRegion.close();
    paFactory.setLocalMaxMemory(oldLocalMaxMem);
    factory.setPartitionAttributes(paFactory.create());
    aRegion = RegionHelper.createRegion(REGION_NAME + 1, factory.create());
    Log.getLogWriter().info(
        "Created partitioned region " + REGION_NAME + 1);

  }
}

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

package cacheperf.gemfire.query;

import cacheperf.CachePerfClient;
import cacheperf.CachePerfPrms;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.NanoTimer;

import distcache.DistCacheException;
import distcache.gemfire.GemFireCacheTestImpl;

import hydra.*;

import java.io.Serializable;
import java.util.*;

import perffmwk.PerfReportPrms;

import query.QueryPrms;
import util.TestException;

/**
 *
 *  Client used to measure cache performance.
 *
 */
public class QueryPerfClient extends CachePerfClient implements Serializable {
  
  static Query[] queries = null;
  static boolean queriesRead = false;
  static boolean individualQueryStats = false;
  static protected final int PUT_OPERATION = 1;
  static protected final int DESTROY_OPERATION = 2;
  static protected final int GET_OPERATION = 3;
  static protected final int QUERY_OPERATION = 4;
  static protected final int CREATE_OPERATION = 5;
  
  static class QueryObserverImpl extends QueryObserverAdapter {
    String indexName = null;

    public void beforeIndexLookup(Index index, int oper, Object key) {
      this.indexName = index.getName();
      Log.getLogWriter().info("BeforeIndexLoopkup :" + index.getName());
    }

    public void afterIndexLookup(Collection results) {
      Log.getLogWriter().info("AfterIndexLoopkup :" + indexName);
    }
  }

  //----------------------------------------------------------------------------
  //  Trim interval names
  //----------------------------------------------------------------------------
 
  protected String nameFor( int name ) {
    switch( name ) {
      default: return super.nameFor(name);
    }
  }

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  public static void readQueries() {
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

  public synchronized static void HydraTask_attachQueryObserver() {
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
  }

  /**
   * TASK to register the cache performance and clock skew statistics objects.
   */
  public static void openStatisticsTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initHydraThreadLocals();
    c.openStatistics();
    c.updateHydraThreadLocals();
  }
  private void openStatistics() {
    if ( this.statistics == null ) {
      this.statistics = QueryPerfStats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
  }
  /* TASK to unregister the cache performance and clock skew statistics objects.
  */
 public static void closeStatisticsTask() {
   QueryPerfClient c = new QueryPerfClient();
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
  
  public static void queryTask() {
    QueryPerfClient qpc = new QueryPerfClient();
    synchronized(QueryPerfClient.class) {
      if (!queriesRead) {
        Object obj = TestConfig.tab().get(
            PerfReportPrms.useAutoGeneratedStatisticsSpecification);
        
        individualQueryStats = (obj == null? false: Boolean.valueOf((String)obj));
        readQueries();
      }
    }
    qpc.initialize(QUERIES);
    qpc.queryBatch();
  }
  
  protected void queryBatch() {
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      query();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator());
  }
  
  protected void query() {
    if (queries != null)
      for (int i = 0; i < queries.length; i++) {
        /*int n = this.rng.nextInt( 1, 100 );
        String status = null; 
        if (n >= 50) {
          status = "active";
        } else {
          status = "inactive";
        }
        int key1 = getNextKey();
        int key2 = getNextKey();
        int bigger, smaller;
        if (key1 > key2) {
          bigger = key1; smaller = key2;
        } else {
          bigger = key2; smaller = key1;
        }
        */
        query(queries[i], null/*new Object[] {status, smaller, bigger}*/, i+1);
      }
  }
  public static void entryOpsAndQueryTask() {
    QueryPerfClient qpc = new QueryPerfClient();
    synchronized(QueryPerfClient.class) {
      if (!queriesRead) {
        readQueries();
      }
    }
    qpc.initialize(QUERIES);
    qpc.doEntryOpsAndQueryBatch();
  }
  
  protected void doEntryOpsAndQueryBatch() {
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      doEntryOpsAndQuery();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator());
  }
  
  protected void doEntryOpsAndQuery() {
    int key = -1;
    int whichOp = getOperation(QueryPrms.entryAndQueryOperations);
    switch (whichOp) {
      case CREATE_OPERATION:
        key = getNextKey();
        create(key);
        break;
      case PUT_OPERATION:
        key = getNextKey();
        put(key);
        break;
      case DESTROY_OPERATION:
        key = getNextKey();
        try {
          destroy(key);  
        } catch( DistCacheException e ) {
          if (!(e.getCause() instanceof EntryNotFoundException)) { // skip this key
             throw e;
          }
        }
        break;
      case GET_OPERATION:
        key = getNextKey();
        try {
        get(key);
        } catch (HydraRuntimeException e) {
          if (!e.getMessage().startsWith("Got null at key=")) {
            throw e;
          }
        }
        break;
      case QUERY_OPERATION:
        query();
        break;
      default: {
        throw new TestException("Unknown operation " + whichOp);
      }
    }
  }

  public static void entryOpsTask() {
    QueryPerfClient qpc = new QueryPerfClient();
    qpc.initialize(OPS);
    qpc.doEntryOpsAndQueryBatch();
  }
 
  protected int getOperation(Long whichPrm) {
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    if (operation.equals("put"))
      op = PUT_OPERATION;
    else if (operation.equals("destroy"))
      op = DESTROY_OPERATION;
    else if (operation.equals("get"))
      op = GET_OPERATION;
    else if (operation.equals("query"))
      op = QUERY_OPERATION;
    else if (operation.equals("create"))
      op = CREATE_OPERATION;

    else
      throw new TestException("Unknown entry operation: " + operation);
    return op;
  }
  
  /**
   * INITTASK registering function for random function execution test
   */
  public static void HydraTask_initRegisterFunction() {
    Function prQueryFunction1 = new QueryPerfClient().new QueryFunction();
    FunctionService.registerFunction(prQueryFunction1);
  }
  
  public static void queryContextTask() {
    QueryPerfClient qpc = new QueryPerfClient();
    synchronized(QueryPerfClient.class) {
      if (!queriesRead) {
        readQueries();
      }
    }
    qpc.initialize(QUERIES);
    qpc.queryContextBatch();
  }
  
  protected void queryContextBatch() {
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      queryContext();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator());
  }
  
  protected void queryContext() {
    if (queries != null)
      for (int i = 0; i < queries.length; i++) {
        /*int n = this.rng.nextInt( 1, 100 );
        String status = null; 
        if (n >= 50) {
          status = "active";
        } else {
          status = "inactive";
        }
        int key1 = getNextKey();
        int key2 = getNextKey();
        int bigger, smaller;
        if (key1 > key2) {
          bigger = key1; smaller = key2;
        } else {
          bigger = key2; smaller = key1;
        }*/
        queryContext(queries[i], /*new Object[] {status, smaller, bigger}*/null);
      }
  }
  
  protected void queryContext(Query query, Object[] bindParameters) {
    try {
      QueryPerfStats stat = (QueryPerfStats) this.statistics;
      Region region = ((GemFireCacheTestImpl)this.cache).getRegion();
      Function function = new QueryFunction();
      long start = this.statistics.startQuery();
      ArrayList argList = new ArrayList();
      argList.add(query.getQueryString());
      argList.add(RemoteTestModule.getCurrentThread().getThreadId());
      if (bindParameters != null) {
        argList.add(bindParameters);
      }
      
      Object result = FunctionService
          .onRegion(region)
          .withArgs(argList).execute(function).getResult();

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
      int numResults = queryResults.size();
      stat.endQuery(NanoTimer.getTime() - start, numResults, this.isMainWorkload,
                                                  this.histogram);

      if (log().fineEnabled()) {
        if (bindParameters != null) {
          Log.getLogWriter().info("Bindparameters: "  );
          for (int i = 0; i < bindParameters.length; i++) {
            Log.getLogWriter().info("param["+i+"] = " + bindParameters[i]);
          }
        }
        log().info("Query " + query.getQueryString() + " returned " + queryResults.size());
        log().fine("Query " + query.getQueryString() + " returned followin results: ");
        log().fine(queryResults.toString());
      }
    } catch (FunctionException e) {
      String s = "Problem executing query: " + query;
      throw new HydraRuntimeException(s, e);
    }
  }
  
  protected void query (Query query, Object[] bindParameters, int queryNum) {
   // log().info("Query " + query.getQueryString());
    if (bindParameters == null) {
      query(query, queryNum);
    } else {
      try {
        long start = this.statistics.startQuery();
        QueryPerfStats stat = (QueryPerfStats) this.statistics;
        SelectResults results = (SelectResults)query.execute(bindParameters);
        int numResults = results.size();
        if (individualQueryStats) {
          stat.endQuery(NanoTimer.getTime() - start, numResults, this.isMainWorkload,
              this.histogram, queryNum);
        } else {
          stat.endQuery(NanoTimer.getTime() - start, numResults, this.isMainWorkload,
              this.histogram);
        }
        if (log().fineEnabled()) {
          Log.getLogWriter().fine("Bindparameters: "  );
          for (int i = 0; i < bindParameters.length; i++) {
            Log.getLogWriter().fine("param["+i+"] = " + bindParameters[i]);
          }
          log().fine("Query " + query.getQueryString() + " returned " + numResults);

          log().fine(getResultString(results));
        }
      } catch (QueryException e) {
        String s = "Problem executing query: " + query;
        throw new HydraRuntimeException(s, e);
      }
    }
  }

  protected void query(Query query, int queryNum) {
    try {
      long start = this.statistics.startQuery();
      QueryPerfStats stat = (QueryPerfStats) this.statistics;
      SelectResults results = (SelectResults)query.execute();
      int numResults = results.size();
      if (individualQueryStats) {
        stat.endQuery(NanoTimer.getTime() - start, numResults, this.isMainWorkload,
            this.histogram, queryNum);
      } else {
        stat.endQuery(NanoTimer.getTime() - start, numResults, this.isMainWorkload, this.histogram);
      }
      
      if (log().fineEnabled()) {
        log().fine("Query " + query + " returned " + numResults);
        log().fine(getResultString(results));
      }
    } catch (QueryException e) {
      String s = "Problem executing query: " + query;
      throw new HydraRuntimeException(s, e);
    }
  }
  
  /**
   * TASK to index objects with {@link CachePerfPrms#queryIndex} and {@link
   * CachePerfPrms#queryFromClause}.
   */
  public static void indexDataTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initialize();
    c.indexData();
  }

  protected void indexData() {
    String []  indexes = null;
    String []  indexFromClauses = null;
    HydraVector hvIndexesExprs = TestConfig.tab()
    .vecAt(CachePerfPrms.queryIndex, null);

    if (hvIndexesExprs != null) {
      indexes = new String[hvIndexesExprs.size()];
      for (int i  = 0; i < hvIndexesExprs.size(); i++) {
        indexes[i] = (String)hvIndexesExprs.get(i);
      }
    }
    
    HydraVector hvIndexesFromClauses = TestConfig.tab()
    .vecAt(CachePerfPrms.queryFromClause, null);
    if (hvIndexesFromClauses != null) {
      indexFromClauses = new String[hvIndexesFromClauses.size()];
      for (int i  = 0; i < hvIndexesFromClauses.size(); i++) {
        indexFromClauses[i] = (String)hvIndexesFromClauses.get(i);
      }
    }
    if (hvIndexesExprs != null) {
      Assert
      .assertTrue(indexFromClauses.length == indexes.length,
          "Number of index expressions and index fromclauses in conf file are not same.");
      for (int i = 0; i < indexes.length; i++) {
        try {
          log().info("Creating index: " + "indexedExpression: " + indexes[i] + " fromClause: " + indexFromClauses[i]);
          CacheHelper.getCache().getQueryService()
              .createIndex("index" + i, IndexType.FUNCTIONAL, indexes[i], indexFromClauses[i]);
        }
        catch (IndexExistsException e) {
          log().info("index already created");
        }
        catch (IndexNameConflictException e) {
          log().info("index already created");
        }
        catch (QueryException e) {
          String s = "Problem creating index: " + indexes[i] + " " + indexFromClauses[i];
          throw new HydraRuntimeException(s, e);
        }
      }
    }
  }
  
  public class QueryFunction extends FunctionAdapter {

    @Override
    public void execute(FunctionContext context) {
      Log.getLogWriter().info(
          "QueryFunction.execute()::Executing QueryFunction on context ");
      Cache cache = CacheFactory.getAnyInstance();
      QueryService queryService = cache.getQueryService();
      ArrayList allQueryResults = new ArrayList();
      ArrayList arguments = (ArrayList)(context.getArguments());
      String qstr = (String)arguments.get(0);
      Object initiatingThreadID = arguments.get(1);
      Object [] bindParams = null;
      if (arguments.size() > 2) {
        bindParams = (Object[])arguments.get(2);
      }
      String aStr = "In execute with context " + context + " with query " + qstr +
         " initiated in hydra thread thr_" + initiatingThreadID + "_";
      for (int i = 2; i < arguments.size(); i++) {
         aStr = aStr + " additional arg: " + arguments.get(i);
      }
      Log.getLogWriter().info(aStr);

      Log.getLogWriter().info(
          "QueryFunction.execute()::Executing query:: " + qstr);
      try {
        Query query = queryService.newQuery(qstr);
        SelectResults result = null;
        if (bindParams != null) {
          result = (SelectResults)query
          .execute((RegionFunctionContext)context, bindParams);
        } else {
          result = (SelectResults)query
          .execute((RegionFunctionContext)context);
        }
        Log.getLogWriter().info(
            "QueryFunction.execute()::Context arguments :: "
                + context.getArguments().toString());
        Set filter = ((RegionFunctionContext)context).getFilter();
        if (filter != null) {
          Log.getLogWriter().info(
              "QueryFunction.execute()::Context FILTER on server side :: "
                  + filter.toString());
        }

        context.getResultSender().sendResult((ArrayList)result.asList());
        context.getResultSender().lastResult(null);
      }
      catch (Exception e) {
        throw new FunctionException(e);
      }
    }

    public String getId() {
      return "QueryFunction";
    }
  }
  
  
  
}

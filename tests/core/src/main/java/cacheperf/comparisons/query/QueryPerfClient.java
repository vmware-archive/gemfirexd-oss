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

package cacheperf.comparisons.query;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.Query;

import distcache.gemfire.GemFireCacheTestImpl;
import hydra.*;

import java.util.*;

import objects.*;
import perffmwk.*;
import cacheperf.*;

import parReg.query.index.*;

/**
 *
 *  Client used to measure cache performance.
 *
 */
public class QueryPerfClient extends cacheperf.CachePerfClient {

  //----------------------------------------------------------------------------
  //  Trim interval names
  //----------------------------------------------------------------------------

  protected static final int QUERIES     = 100;

  protected static final String QUERY_NAME = "queries";
  
  public boolean createIndex;
  protected static boolean exeQuery1 = false; 
  protected static boolean exeQuery2 = false;
  protected static boolean exeQuery3 = false;
  protected static boolean createIndex1 = false; 
  protected static boolean createIndex2 = false;
  protected static boolean createIndex3 = false;


  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------
  /**
   *  queryTask()
   */
  
  public static void createIndexTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initHydraThreadLocals();
    
    int createIndexNum = QueryPerfPrms.exeQuerNum(); //which index to be executed
    switch (createIndexNum) {
    case 1: 
      createIndex1 = true;
      break;
    case 2:
      createIndex2 = true;
      break;
    case 3:
      createIndex3 = true;
      break;
    default:
      throw new HydraInternalException( "Can't create such index." );
    }
    c.createIndex();
  }
  
  private void createIndex() {
    this.createIndex = QueryPerfPrms.createIndex();
    
    if (this.createIndex) {
      Region r = null;
      r = ((GemFireCacheTestImpl)super.cache).getRegion();
      
      IndexTest indexTest = new IndexTest();
      Log.getLogWriter().info("try to create index");     

     if (createIndex1) indexTest.createIndex1(r.getName());
     if (createIndex2) indexTest.createIndex2(r.getName());
     if (createIndex3) indexTest.createIndex3(r.getName());     
    }
  }
  
  public static void initQueryTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initHydraThreadLocals();
    
    int exeQueryNum = QueryPerfPrms.exeQuerNum(); //which query to be executed
    switch (exeQueryNum) {
    case 1: 
      exeQuery1 = true;
      break;
    case 2:
      exeQuery2 = true;
      break;
    case 3:
      exeQuery3 = true;
      break;
    default:
      throw new HydraInternalException( "Can't execute this query." );
    }
  }
  
  public static void queryTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initialize( QUERIES );
    c.executeQuery();
  }
  
  private void executeQuery() {
    if (log().fineEnabled()) log().fine("executing query");
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      
      if (exeQuery1)      query1( key );
      else if (exeQuery2) query2( key );
      else if (exeQuery3) query3( key );
      
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;

    } while (!executeBatchTerminator());
    if (log().fineEnabled()) log().fine("finishing executing query");
  }
  private void query1( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    long start;
    if (log().finerEnabled()) log().finer("query key = " + key);
    Region theRegion = ((GemFireCacheTestImpl)super.cache).getRegion();

    try {      
      start = this.querystats.startQuery();
      String queryString = "select distinct * from " + theRegion.getFullPath() + " where name = $1 ";
      Query query = CacheHelper.getCache().getQueryService().newQuery(queryString);
      
      Object [] params = new Object[1];
      params[0] = new Integer(i).toString();
      Object result = query.execute(params);
      
      if (! (result instanceof Collection)) {
        Log.getLogWriter().info("Result is not a collection." );
      }
      
    } catch(Exception e) {     // IllegalState or IllegalArgumentExceptions
      throw new CachePerfException( "Could not get query result", e );
    }
   
    this.querystats.endQuery( start );
    if (log().finerEnabled()) log().finer("finished query for key = " + key);

  }  
  
  private void query2( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    long start;
    if (log().finerEnabled()) log().finer("query key = " + key);
    Region theRegion = ((GemFireCacheTestImpl)super.cache).getRegion();

    try {      
      start = this.querystats.startQuery();
      String queryString = "select distinct * from " + theRegion.getFullPath() + " where id < $1 ";
      Query query = CacheHelper.getCache().getQueryService().newQuery(queryString);
      //Object result = query.execute();
      
      
      Object [] params = new Object[1];
      params[0] = new Integer(i%10 +1);
      Object result = query.execute(params);
      

      if (! (result instanceof Collection)) {
        Log.getLogWriter().info("Result is not a collection." );
      } 
   //   else Log.getLogWriter().info("Result size is " + ((Collection)result).size());
      
    } catch(Exception e) {     // IllegalState or IllegalArgumentExceptions
      throw new CachePerfException( "Could not get query result", e );
    }
   
    this.querystats.endQuery( start );
    if (log().finerEnabled()) log().finer("finished query for key = " + key);

  }
  
  private void query3( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    long start;
    if (log().finerEnabled()) log().finer("query key = " + key);
    Region theRegion = ((GemFireCacheTestImpl)super.cache).getRegion();

    try {      
      start = this.querystats.startQuery();
      String queryString = 
        "import parReg.\"query\".Position; select distinct r from " + theRegion.getFullPath() + " r, " +
        " r.positions.values pVal TYPE Position where pVal.mktValue  < $1";
      Query query = CacheHelper.getCache().getQueryService().newQuery(queryString);
      
      
      Object [] params = new Object[1];
      params[0] = new Double((i % 25) + 1.0);
      Object result = query.execute(params);
      

      if (! (result instanceof Collection)) {
        Log.getLogWriter().info("Result is not a collection." );
      } 
   //   else Log.getLogWriter().info("Result size is " + ((Collection)result).size());
      
    } catch(Exception e) {     // IllegalState or IllegalArgumentExceptions
      throw new CachePerfException( "Could not get query result", e );
    }
   
    this.querystats.endQuery( start );
    if (log().finerEnabled()) log().finer("finished query for key = " + key);

  }


  /**
   *  TASK to register the query performance statistics object.
   */
  public static void openStatisticsTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initHydraThreadLocals();
    c.openStatistics();
    c.updateHydraThreadLocals();
  }
  
  private void openStatistics() {
    if ( this.querystats == null ) {
      log().info( "Opening per-thread query performance statistics" );
      this.querystats = QueryPerfStats.getInstance();
      log().info( "Opened per-thread query performance statistics" );
    }
  }

  /**
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }
  protected void closeStatistics() {
    MasterController.sleepForMs( 2000 );
    if ( this.querystats != null ) {
      log().info( "Closing per-thread Query performance statistics" );
      this.querystats.close();
      log().info( "Closed per-thread Query performance statistics" );
    }
  }

  //----------------------------------------------------------------------------
  //  Hydra thread locals and their instance field counterparts
  //----------------------------------------------------------------------------

  public QueryPerfStats querystats;
  //public HashMap objectlist;

  private static HydraThreadLocal localquerystats = new HydraThreadLocal();
  //private static HydraThreadLocal localobjectlist = new HydraThreadLocal();

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();

    this.querystats = getQueryStats();
    //this.objectlist = getObjectList();

  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();

    setQueryStats( this.querystats );
    //setObjectList( this.objectlist );
  }

  /**
   *  Gets the per-thread QueryStats wrapper instance.
   */
  protected QueryPerfStats getQueryStats() {
    QueryPerfStats querystats = (QueryPerfStats) localquerystats.get();
    return querystats;
  }
  /**
   *  Sets the per-thread QueryStats wrapper instance.
   */
  protected void setQueryStats(QueryPerfStats querystats ) {
    localquerystats.set( querystats );
  }

  //----------------------------------------------------------------------------
  //  Overridden methods
  //----------------------------------------------------------------------------
  
  protected String nameFor( int name ) {
    switch (name) {
      case QUERIES:      return QUERY_NAME;
    }
    return super.nameFor(name);
  }
}

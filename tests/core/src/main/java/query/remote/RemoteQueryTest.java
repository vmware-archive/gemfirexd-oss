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
package query.remote;

import objects.PSTObject;
import objects.Portfolio;
import mapregion.MapBB;
import util.TestException;
import util.TestHelper;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.*;
import query.*;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;


/**
 * Contains Hydra tasks and supporting methods for testing remote OQL.It has
 * methods for initializing and configuring the cache server and cache clients.
 * All the validation methods are there.
 * 
 * @author Girish, Yogesh M
 */

public class RemoteQueryTest
{
  protected static String bridgeRegionName;

  protected static String edgeRegionName;

  protected static final int MAX_PUT = 50;  
  
  protected static final int MAX_PUT_CACHE_SCALABILITY = 10000;

  protected static final int NUM_OF_PUTS = 100;
  
  protected static final int MAX_PUT_EVICTION_OVERFLOW = 3000;  

  /** This array contains different types of queries * */
  private static volatile Query[] queries = null;
  
  //update region entries from client
  private static volatile long putKeyInt;

  //update region entries from client
  private static volatile long destroyKeyInt;

  private static volatile QueryResultsValidator rv = null;

  // initializes server cache
  public static void initServerRegion() {
    // create cache
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    // create region
    String regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();
    AttributesFactory attr = RegionHelper.getAttributesFactory(ConfigPrms.getRegionConfig());
    attr.setDataPolicy(DataPolicy.PRELOADED);
    Region reg = RegionHelper.createRegion(regionName, attr);
    // Region reg = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    bridgeRegionName = reg.getName();
    Log.getLogWriter().info("created cache and region in bridge");
    // start the bridge server
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
    Log.getLogWriter().info("started bridge server");
  }

  // initializes server cache
  public static void initServerRegion_WithReplicate() {
    // create cache
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    // create region
    String regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();
    AttributesFactory attr = RegionHelper.getAttributesFactory(ConfigPrms.getRegionConfig());
    attr.setDataPolicy(DataPolicy.REPLICATE);
    Region reg = RegionHelper.createRegion(regionName, attr);
    // Region reg = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    bridgeRegionName = reg.getName();
    Log.getLogWriter().info("created cache and region in bridge");
    // start the bridge server
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
    Log.getLogWriter().info("started bridge server");
  }

  // initializes client cache
  public static void initClientRegion() {
    // create cache using configuration of bridgeConfig specified in conf file
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    // create region
    Region reg = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    reg.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    edgeRegionName = reg.getName();
    Log.getLogWriter().info("created cache and region in edge");
  }
  
  public static void initClientRegionWithoutInterest()
  {
    // create cache using configuration of bridgeConfig specified in conf file
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    // create region
    Region reg = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    edgeRegionName = reg.getName();
    Log.getLogWriter().info("created cache and region in edge");
  }

  /** This function performs population of data from client side * */
  public static void updateRegionEntries() {
    try {
      Region reg = RegionHelper.getRegion(edgeRegionName);
      putKeyInt = MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
      int i = 0;
      Object key = null, val = null;
      do {
        key = "key" + putKeyInt;
        val = new Portfolio((int)putKeyInt);
        Log.getLogWriter().info("Putting key  : " + key + "    value : " + val);
        reg.put(key, val);
        i++;
        putKeyInt = MapBB.incrementCounter("MapBB.NUM_PUT",
            MapBB.NUM_PUT);
      } while (i < MAX_PUT);
      Log.getLogWriter().info("added entries in edge" + reg.size());

    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
 
  /** This function performs population of data from client side **/
  public static void updateRegionEntries_CacheScalability()
  {
    try {
      Region reg = RegionHelper.getRegion(edgeRegionName);
      putKeyInt = MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
      int i = 0;
      Object key = null, val = null;
      do {
        key = "key" + putKeyInt;
        val = new Portfolio((int)putKeyInt);
        Log.getLogWriter().info("Putting key  : " + key + "    value : " + val);
        reg.put(key, val);
        i++;
        putKeyInt = MapBB.incrementCounter("MapBB.NUM_PUT",
            MapBB.NUM_PUT);
      } while (i < MAX_PUT_CACHE_SCALABILITY);
      Log.getLogWriter().info("added entries in edge" + reg.size());
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  /** This function performs population of data from client side **/
  public static void updateRegionEntries_10KPayload()
  {
    try {
      Region reg = RegionHelper.getRegion(edgeRegionName);
      putKeyInt = MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
      // payLoad of size 10K , payLoad more than this causes OOM 
      //byte []payLoad = new byte[10*1024];
      //PSTObject also has a byte array and also fields to query.
      PSTObject payLoad = new PSTObject();
      payLoad.init(Integer.valueOf(String.valueOf(putKeyInt)));
      int i = 0;
      Object key = null, val = null;
      do {
        key = "key" + putKeyInt;
        reg.put(key, payLoad);
        i++;
        putKeyInt = MapBB.incrementCounter("MapBB.NUM_PUT",
            MapBB.NUM_PUT);
      } while (i < MAX_PUT);
      Log.getLogWriter().info("added entries in edge" + reg.size());
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  
  /** This function performs population of data from client side **/
  public static void updateRegionEntries_EvictionAndOverflow()
  {
    try {
      Region reg = RegionHelper.getRegion(edgeRegionName);
      putKeyInt = MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
      // payLoad of size 10K , payLoad more than this causes OOM 
      byte []payLoad = new byte[10*1024];
      int i = 0;
      Object key = null, val = null;
      do {
        key = "key" + putKeyInt;
        reg.put(key, payLoad);
        i++;
        putKeyInt = MapBB.incrementCounter("MapBB.NUM_PUT",
            MapBB.NUM_PUT);
      } while (i < MAX_PUT_EVICTION_OVERFLOW);
      Log.getLogWriter().info("added entries in edge" + reg.size());
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  /** This function is used to populate specific data from the client side * */
  public static void putRegionEntries()
  {
    try {
      Region reg = RegionHelper.getRegion(edgeRegionName);
      Object key = null, val = null;
      for (int i = 0; i < NUM_OF_PUTS; i++) {
        key = "key" + i;
        if (i < 10)
          val = new Portfolio(10);
        else if (i < 30)
          val = new Portfolio(20);
        else if (i < 60)
          val = new Portfolio(30);
        else
          val = new Portfolio(40);
        reg.put(key, val);
      }
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
 
 /**
   * Fucntion to store the Queries mentioned in conf file in the compile form
   */
  public static void initQuery()
  {
    HydraVector hv = TestConfig.tab().vecAt(QueryPrms.queryStrings, null);
    if (hv == null){
      throw new TestException("No Queries specified ");
    }
    
    String queryStrings[] = new String[hv.size()];
    hv.copyInto(queryStrings);
    queries = new Query[queryStrings.length];
    // QueryService qs = CacheHelper.getCache().getQueryService();
    String poolName = RegionHelper.getRegion(edgeRegionName).getAttributes().getPoolName();
    Pool pool = PoolManager.find(poolName);
    QueryService qs = null;
    qs = pool.getQueryService();
    Log.getLogWriter().info("Created QuerySevice using Pool.");

    for (int i = 0; i < queryStrings.length; ++i) {
      queries[i] = qs.newQuery(queryStrings[i]);
    }
    String validatorClass = TestConfig.tab().stringAt(
        QueryPrms.resultsValidator, null);
    try {
      if (validatorClass != null) {
        rv = (QueryResultsValidator)Class.forName(validatorClass).newInstance();
      }
      else {
        rv = new DefaultQueryResultsValidator(queries.length);
      }
    }
    catch (Exception e) {
      throw new TestException("Caught exception during query initialization"
          + TestHelper.getStackTrace(e));
    }
 }

  
  // perform querying operation on region
  public static void performQuery() {
    boolean executeQueryWithParams = false;
    HydraVector hv = TestConfig.tab().vecAt(QueryPrms.queryParametersLength, null);
    Object[][] allQueryParams = null;
    
    if (hv != null && hv.size() > 0){
      // Get query parameter length for each query.
      executeQueryWithParams = true;
      Object queryParametersLength[] = new Object[hv.size()];
      hv.copyInto(queryParametersLength);
      
      // Get query parameters.
      HydraVector hv1 = TestConfig.tab().vecAt(QueryPrms.queryParameters, null);
      Object queryParameters[] = new Object[hv1.size()];
      hv1.copyInto(queryParameters);
      
      allQueryParams = new Object[queryParametersLength.length][];
   
      int paramIndex = 0;
      for (int i=0; i < queryParametersLength.length; i++){
        int queryParamLength = Integer.valueOf(((String)queryParametersLength[i])).intValue();
        Object[] queryParams = new Object[queryParamLength];
        if (queryParamLength > 0) {
          for (int j=0; j < queryParamLength; j++){
            String p = (String)queryParameters[paramIndex++];
            if (p.length() > 3) {
              // String type
              queryParams[j] = p;
            } else {
              // Integer type
              queryParams[j] = new Integer(p);
            }
          }
        }
        allQueryParams[i] = queryParams;
      }      
    }
    
    boolean ok = false;
    try {
      Region reg = RegionHelper.getRegion(edgeRegionName);
      int whichQuery = TestConfig.tab().getRandGen().nextInt (0, queries.length - 1);
      if (whichQuery >= queries.length) {
        Log.getLogWriter().warning(
            "Which Query  :  " + whichQuery
                + " does not exist as maximum valid which Query index is "
                + (queries.length - 1));
        return;
      }
  
      Log.getLogWriter().info("Which Query  :  " + whichQuery + 
        " Query :" + queries[whichQuery].getQueryString() +
        (executeQueryWithParams ? 
        (" params :" + allQueryParams[whichQuery].length) : "" ));

      Object results = null;
      if (executeQueryWithParams && allQueryParams[whichQuery].length > 0) {
        String params = "";
        for (int i=0; i < allQueryParams[whichQuery].length; i++) {
          params += (allQueryParams[whichQuery][i] + ","); 
        }
        Log.getLogWriter().info("Using params :" + params);
 
        results = queries[whichQuery].execute(allQueryParams[whichQuery]);  
      } else {
        results = queries[whichQuery].execute();
      }
      
      ok = rv.validateQueryResults((SelectResults)results, whichQuery,
          queries[whichQuery].getQueryString());
    }
    catch (Exception e) {
      throw new TestException("Caught exception during query execution"
          + TestHelper.getStackTrace(e));
    }
    // Fix for Bug 44571. release threadLocalConnections
    String poolName = RegionHelper.getRegion(edgeRegionName).getAttributes().getPoolName();
    Pool pool = PoolManager.find(poolName);
    pool.releaseThreadLocalConnection();
    
    if (!ok) {
      throw new TestException("The validation was not successful");
    }
  } 
  
  
  /** Function to perform destroy operations from the client side */
  public static void performDestroyOperation()
  {
    Region reg = RegionHelper.getRegion(edgeRegionName);
    destroyKeyInt = MapBB.getBB().getSharedCounters().read(MapBB.NUM_DESTROY);
    String key = "key" + destroyKeyInt;
    try {
      reg.destroy(key);
    }
    catch (EntryNotFoundException ex) {
      // ignore this exception
    }
    catch (Exception ex) {
      throw new TestException("Destroy operation not successful "
          + ex.getStackTrace());
    }
    Log.getLogWriter().info("Destroying the key : " + key);
    MapBB.add("MapBB.NUM_DESTROY", MapBB.NUM_DESTROY, 2);
  }

  /** Function to verify compiled query size */
  public static void verifyCompiledQueries()
  {
    Log.getLogWriter().info("Verifying compiled queryies.");
    long compiledQueryCount = 0;
    for (int i=0; i < 10; i++){
      // Wait for sometime. So that the cleanup thread completes its job.
      compiledQueryCount = CacheClientNotifier.getInstance().getStats().getCompiledQueryCount();
      if (compiledQueryCount <= 0) {
        break;
      }
      try {
        Thread.sleep(2000);
      } catch (Exception ex){}
    }
    
    if (compiledQueryCount > 0){
      throw new TestException("Compiled Queries are not completely cleared. " +
          "Number of compiled queries present are: " + compiledQueryCount);
    }   
  }

}

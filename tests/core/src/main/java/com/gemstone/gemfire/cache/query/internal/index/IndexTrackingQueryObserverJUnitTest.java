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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;
import java.util.Map;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver.IndexInfo;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionQueryEvaluator.TestHook;

/**
 * @author shobhit
 *
 */
public class IndexTrackingQueryObserverJUnitTest extends TestCase {
  static QueryService qs;
  static Region region;
  static Index keyIndex1;
  static IndexInfo regionMap;
  
  private static final String queryStr = "select * from /portfolio where ID > 0";
  public static final int NUM_BKTS = 20;
  public static final String INDEX_NAME = "keyIndex1"; 
    
  @Override
  protected void setUp() throws Exception {
    System.setProperty("gemfire.Query.VERBOSE", "true");
    CacheUtils.startCache();
    QueryObserver observer = QueryObserverHolder.setInstance(new IndexTrackingQueryObserver());
  }

  @Override
  protected void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  public void testIndexInfoOnPartitionedRegion() throws Exception{
    //Query VERBOSE has to be true for the test
    assertEquals("true", System.getProperty("gemfire.Query.VERBOSE"));
    
    //Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(queryStr);

    //Inject TestHook in QueryObserver before running query.
    IndexTrackingTestHook th = new IndexTrackingTestHook(region, NUM_BKTS);
    QueryObserver observer = QueryObserverHolder.getInstance();
    assertTrue(QueryObserverHolder.hasObserver());
    
    ((IndexTrackingQueryObserver)observer).setTestHook(th);
    
    SelectResults results = (SelectResults)query.execute();
    
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    
        //Check results size of Map.
    regionMap = ((IndexTrackingTestHook)th).getRegionMap();
    Collection<Integer> rslts = regionMap.getResults().values();
    int totalResults = 0;
    for (Integer i : rslts){
      totalResults += i.intValue();
    }
    assertEquals(results.size(), totalResults);
    QueryObserverHolder.reset();    
  }

  public void testIndexInfoOnLocalRegion() throws Exception{
    //Query VERBOSE has to be true for the test
    assertEquals("true", System.getProperty("gemfire.Query.VERBOSE"));
    
    //Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(queryStr);

    //Inject TestHook in QueryObserver before running query.
    IndexTrackingTestHook th = new IndexTrackingTestHook(region, 0);
    QueryObserver observer = QueryObserverHolder.getInstance();
    assertTrue(QueryObserverHolder.hasObserver());
    
    ((IndexTrackingQueryObserver)observer).setTestHook(th);
    
    SelectResults results = (SelectResults)query.execute();
    
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    
    regionMap = ((IndexTrackingTestHook)th).getRegionMap();
    Object rslts = regionMap.getResults().get(region.getFullPath());
    assertTrue(rslts instanceof Integer);
    
    assertEquals(results.size(), ((Integer)rslts).intValue());
    QueryObserverHolder.reset();
  }

  /**
   * @author shobhit
   * TODO: Not implemented fully for all the hooks.
   *
   */
  public static class IndexTrackingTestHook implements TestHook {
    IndexInfo rMap;
    Region regn;
    int bkts;

    public IndexTrackingTestHook(Region region, int bukts) {
      this.regn = region;
      this.bkts = bukts;
    }


    public void hook(int spot) throws RuntimeException {

      QueryObserver observer = QueryObserverHolder.getInstance();
      assertTrue(observer instanceof IndexTrackingQueryObserver);
      IndexTrackingQueryObserver gfObserver = (IndexTrackingQueryObserver)observer;
      
      if (spot == 1) { //before index lookup
      } else if (spot == 2) { //before key range index lookup
      } else if (spot == 3) { //End of afterIndexLookup call
      } else if (spot == 4) { //Before resetting indexInfoMap
        Map map = gfObserver.getUsedIndexes();
        assertEquals(1, map.size());
        
        assertTrue(map.get(INDEX_NAME) instanceof IndexInfo);
        rMap = (IndexInfo)map.get(INDEX_NAME);
        
        if(this.regn instanceof PartitionedRegion){
          assertEquals(1, rMap.getResults().size());
        } else if (this.regn instanceof LocalRegion) {
          assertEquals(1, rMap.getResults().size());
        }
      }
    }
    
    public IndexInfo getRegionMap(){
      return rMap;
    }
  }
}

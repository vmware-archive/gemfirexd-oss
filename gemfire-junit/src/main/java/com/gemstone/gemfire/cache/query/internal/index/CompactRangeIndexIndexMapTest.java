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
package com.gemstone.gemfire.cache.query.internal.index;

import java.text.ParseException;
import java.util.HashMap;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;

public class CompactRangeIndexIndexMapTest extends TestCase {

  
  /**
   * @param name test name
   */
  public CompactRangeIndexIndexMapTest(String name) {
    super(name);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }

  protected void setUp() throws Exception {
    System.setProperty("gemfire.Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  protected void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(CompactRangeIndexIndexMapTest.class);
    return suite;
  }
  
  public void testCreateFromEntriesIndex() {
    
  }
  
  public void testCreateIndexAndPopulate() {
    
  }
  
  public void testLDMIndexCreation() throws Exception {
    Cache cache = CacheUtils.getCache();
    Region region = createLDMRegion("portfolios");
    QueryService queryService = cache.getQueryService();
    Index index = queryService.createIndex("IDIndex", "p.ID", "/portfolios p, p.positions ps");
    assertTrue(index instanceof CompactRangeIndex);
  }
  
  public void testFirstLevelEqualityQuery() throws Exception {
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID > 1");
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID < 10");
  }
  
  public void testSecondLevelEqualityQuery() throws Exception {
    boolean oldTestLDMValue = IndexManager.IS_TEST_LDM;
    boolean oldTestExpansionValue = IndexManager.IS_TEST_EXPANSION;
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select * from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select p.ID from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select p from /portfolios p where p.ID > 3");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select ps from /portfolios p, p.positions.values ps where ps.secId = 'VMW'");
    IndexManager.IS_TEST_LDM = oldTestLDMValue;
    IndexManager.IS_TEST_EXPANSION = oldTestExpansionValue;
  }
  
  public void testMultipleSecondLevelMatches() throws Exception {
    boolean oldTestLDMValue = IndexManager.IS_TEST_LDM;
    boolean oldTestExpansionValue = IndexManager.IS_TEST_EXPANSION;
    testIndexAndQuery("ps.secId", "/portfolios p, p.positions.values ps", "Select * from /portfolios p, p.positions.values ps where ps.secId = 'VMW'");
    IndexManager.IS_TEST_LDM = oldTestLDMValue;
    IndexManager.IS_TEST_EXPANSION = oldTestExpansionValue;
  }
  
  //executes queries against both no index and ldm index
  //compares size counts of both and compares results
  private void testIndexAndQuery(String indexExpression, String regionPath, String queryString) throws Exception {
    Cache cache = CacheUtils.getCache();
    int numEntries = 20;
    QueryService queryService = cache.getQueryService();
    IndexManager.IS_TEST_LDM = false;
    IndexManager.IS_TEST_EXPANSION = false;
    Region region = createReplicatedRegion("portfolios");
    createPortfolios(region, numEntries);
    
    //Test no index
    //Index index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    Query query = queryService.newQuery(queryString);
    SelectResults noIndexResults = (SelectResults) query.execute();
    //clean up
    queryService.removeIndexes();
    
    //creates indexes that may be used by the queries
    Index index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    query = queryService.newQuery(queryString);
    SelectResults memResults = (SelectResults) query.execute();
    //clean up
    queryService.removeIndexes();
    region.destroyRegion();
    
    //Now execute against a replicated region with regular indexes
    //we want to make sure we don't create and LDM index so undo the test hook
    IndexManager.IS_TEST_LDM = true;
    IndexManager.IS_TEST_EXPANSION = true;
    region = createLDMRegion("portfolios");
    createPortfolios(region, numEntries);

    index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    query = queryService.newQuery(queryString);
    SelectResults ldmResults = (SelectResults) query.execute();
    
    assertEquals("Size for no index and index results should be equal", noIndexResults.size(), memResults.size());
    assertEquals("Size for memory and ldm index results should be equal", memResults.size(), ldmResults.size());
    System.out.println("Size is:" + memResults.size());
    //now check elements for both
    for (Object o: ldmResults) {
      assertTrue(memResults.contains(o));
    }
    queryService.removeIndexes();
    region.destroyRegion();
    
  }
  
  
  //Should be changed to ldm region
  //Also should remove IS_TEST_LDM when possible
  private Region createLDMRegion(String regionName) throws ParseException {
    IndexManager.IS_TEST_LDM = true;
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }
  
  private Region createReplicatedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }
  
  private void createPortfolios(Region region, int num) {
    for (int i = 0; i < num; i++) {
      Portfolio p = new Portfolio(i);
      p.positions = new HashMap();
      p.positions.put("VMW", new Position("VMW", Position.cnt * 1000));
      p.positions.put("IBM", new Position("IBM", Position.cnt * 1000));
      p.positions.put("VMW_2", new Position("VMW", Position.cnt * 1000));
      region.put("" + i, p);
    }
  }
  
}

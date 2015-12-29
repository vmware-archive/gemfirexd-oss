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
import java.util.Collection;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;

public class HashIndexTest extends TestCase {

  private QueryService qs;
  private Region region;
  private Region joinRegion;
  private MyQueryObserverAdapter observer;
  private Index index;
  
  public HashIndexTest(String testName) {
    super(testName);
  }

  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    qs = CacheUtils.getQueryService();
    observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
    
  }
  
  private void createJoinTable(int numEntries) throws Exception {
    joinRegion = CacheUtils.createRegion("portfolios2", Portfolio.class);

    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i);
      joinRegion.put("" + i, p);
    }
  }

  protected void tearDown() throws java.lang.Exception {
    qs.removeIndexes();
    if (joinRegion != null) {
      joinRegion.close();
      joinRegion = null;
    }
    region.close();
    CacheUtils.closeCache();
  }
  
  /**
   * Helper that tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  
  private void helpTestHashIndexForQuery(String query)throws Exception {
    helpTestHashIndexForQuery(query,"p.ID", "/portfolios p");
  }
  
  private void helpTestHashIndexForQuery(String query, String indexedExpression, String regionPath) throws Exception {
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute();
    assertFalse(observer.indexUsed);

    index = (Index)qs.createHashIndex("idHash", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute();
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.indexUsed);
  }

  
  /*
   * helper method to test against a compact range index instead of hash index
   * @param query
   * @throws Exception
   */
  private void helpTestCRIndexForQuery(String query, String indexedExpression, String regionPath) throws Exception {
    SelectResults nonIndexedResults = (SelectResults)qs.newQuery(query).execute();
    assertFalse(observer.indexUsed);

    index = (Index)qs.createIndex("crIndex", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults)qs.newQuery(query).execute();
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index with And query for local region
   * @throws Exception
   */
  public void testHashIndexWithORQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID = 1 OR p.ID = 2", "p.ID", "/portfolios p");
  }
  
  /**
   * Tests that hash index with And query for local region
   * @throws Exception
   */
  public void testHashIndexWithNestedQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestCRIndexForQuery("SELECT * FROM (SELECT * FROM /portfolios p WHERE p.shortID = 1)", "p.shortID", "/portfolios p");
  }
  
  /**
   * Tests that hash index with Short vs Integer comparison
   * @throws Exception
   */
  public void testHashIndexWithNestedQueryWithShortVsIntegerCompareForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.shortID in (SELECT p2.ID FROM /portfolios p2 WHERE p2.shortID = 1)", "p.shortID", "/portfolios p");
  }
  

  /**
   * Tests that hash index with comparison between float and integer 
   * @throws Exception
   */
//  public void testHashIndexQueryWithFloatVsIntegerCompareForLocalRegion() throws Exception {
//    createLocalRegion("portfolios");
//    int numEntries = 1000;
//    int numIds = 100;
//    for (int i = 0; i < numEntries; i++) {
//      Portfolio p = new Portfolio(i % (numIds));
//      p.shortID = (short)i;
//      region.put("" + i, p);
//    }
//    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID = 1.0f", "p.ID", "/portfolios p");
//  }
  
  /**
   * Tests that hash index with comparison between float and integer 
   * @throws Exception
   */
//  public void testHashIndexNotEqualsWithFloatVsIntegerLocalRegion() throws Exception {
//    createLocalRegion("portfolios");
//    int numEntries = 1000;
//    int numIds = 100;
//    for (int i = 0; i < numEntries; i++) {
//      Portfolio p = new Portfolio(i % (numIds));
//      p.shortID = (short)i;
//      region.put("" + i, p);
//    }
//    helpTestCRIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID != 1.0f", "p.ID", "/portfolios p");
//  }
  
  /**
   * Tests that hash index with And query for local region
   * @throws Exception
   */
  public void testHashIndexWithAndQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios p WHERE p.ID = 1 AND p.shortID > 0", "p.ID", "/portfolios p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  public void testHashIndexWithLimitQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.entries p WHERE p.ID = 1 limit 3", "p.ID", "/portfolios.entries p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  public void testHashIndexEntriesQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.entries p WHERE p.ID = 1", "p.ID", "/portfolios.entries p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  public void testHashIndexValueQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.values p WHERE p.ID = 1", "p.ID", "/portfolios.values p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  public void testHashIndexKeySetQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("SELECT * FROM /portfolios.keySet p WHERE p.ID = 1", "p.ID", "/portfolios.keySet p");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for local region
   * @throws Exception
   */
  public void testHashIndexEqualsForSingleResultOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result for replicated region
   * @throws Exception
   */
  public void testHashIndexEqualsForSingleResultOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct result for partitioned region
   * @throws Exception
   */
  public void testHashIndexEqualsForSingleResultOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexAndEquiJoinForSingleResultQueryWithHashIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    createJoinTable(400);
    Index index = (Index)qs.createHashIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexAndEquiJoinForSingleResultQueryWithCompactRangeIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    createJoinTable(400);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexAndEquiJoinForSingleResultQueryWithRangeIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    createJoinTable(400);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2, p2.positions.values v");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
//  public void testHashIndexAndEquiJoinForSingleResultQueryWithMapRangeIndex() throws Exception {
//    createReplicatedRegion("portfolios");
//    createData(region, 1000);
//    createJoinTable(2000);
//    Index index = (Index)qs.createIndex("index2","p2.names[*]", "/portfolios2 p2");
//    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.names['1'] or p.names['2'] ) and p.names = p2.names", "p.names[*]", "/portfolios p");
//  }

  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexAndEquiJoinForSingleResultQueryWithHashIndexLessEntries() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 400);
    createJoinTable(200);
    Index index = (Index)qs.createHashIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexAndEquiJoinForSingleResultQueryWithCompactRangeIndexLessEntries() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 400);
    createJoinTable(200);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexAndEquiJoinForSingleResultQueryWithRangeIndexLessEntries() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 400);
    createJoinTable(200);
    Index index = (Index)qs.createIndex("index2","p2.ID", "/portfolios2 p2, p2.positions.values v");
    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where (p.ID = 1 or p.ID = 2 )and p.ID = p2.ID");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
//  public void testHashIndexAndEquiJoinForSingleResultQueryWithMapRangeIndexLessEntries() throws Exception {
//    createReplicatedRegion("portfolios");
//    createData(region, 1000);
//    createJoinTable(500);
//    Index index = (Index)qs.createIndex("index2","p2.positions[*]", "/portfolios2 p2");
//    helpTestHashIndexForQuery("Select * FROM /portfolios p, /portfolios2 p2 where p.positions['IBM'] and p.positions['IBM']=p2.positions['IBM']", "p.positions[*]", "/portfolios p");
//  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * on local region
   * @throws Exception
   */
  public void testHashIndexEqualsForMultipleResultQueryOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    //Create the data
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct number of results
   * on replicated region
   * @throws Exception
   */
  public void testHashIndexEqualsForMultipleResultQueryOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    //Create the data
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }

  /**
   * Tests that hash index is used and that it returns the correct number of results
   * on partitioned region
   * @throws Exception
   */ 
  public void testHashIndexEqualsForMultipleResultQueryOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    //Create the data
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID = 1");
  }
  
 
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * @throws Exception
   */
  public void testHashIndexEquiJoinForMultipleResultQueryWithHashIndex() throws Exception {
    createReplicatedRegion("portfolios");
    createJoinTable(400);
    index = qs.createHashIndex("idHash",
        "p.ID", "/portfolios p");
    Index index = (Index)qs.createHashIndex("index2","p2.ID", "/portfolios2 p2");

    
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p, /portfolios2 p2 where p.ID = 1 and p.ID = p2.ID").execute();
    assertEquals(numEntries/numIds, results.size());
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where only 1 value is using the key for partitioned regions
   * @throws Exception
   */
  public void testHashIndexRemoveOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    helpTestHashIndexRemove();
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where only 1 value is using the key for replicated regions
   * @throws Exception
   */
  public void testHashIndexRemoveOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestHashIndexRemove();
  }
  
  public void testHashIndexRemoveOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    region.destroy("1");
    SelectResults noIndexResults = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();

    region.destroyRegion();
    createPartitionedRegion("portfolios");
    createData(region, 200);
    region.destroy("1");
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();
    assertEquals(noIndexResults.size(), results.size());
    assertTrue(observer.indexUsed);
  }
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where only 1 value is using the key for partitioned regions
   * @throws Exception
   */
  private void helpTestHashIndexRemove() throws Exception {
    createData(region, 200);
    region.destroy("1");
    SelectResults noIndexResults = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();
    
    region.clear();
    createData(region, 200);
    region.destroy("1");
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 1").execute();
    assertEquals(noIndexResults.size(), results.size());
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where multiple entries are using the key on localRegion
   * @throws Exception
   */
  public void testHashIndexRemoveFromCommonKeyQueryOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    helpTestHashIndexRemoveFromCommonKeyQuery();
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where multiple entries are using the key on replicated region
   * @throws Exception
   */
  public void testHashIndexRemoveFromCommonKeyQueryOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestHashIndexRemoveFromCommonKeyQuery();
  }
  
  /**
   * Tests that hash index is used and that the value is correctly removed from the index
   * where multiple entries are using the key on partitioned region
   * @throws Exception
   */
  public void testHashIndexRemoveFromCommonKeyQueryOnPartitionedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestHashIndexRemoveFromCommonKeyQuery();
  }
  
  private void helpTestHashIndexRemoveFromCommonKeyQuery() throws Exception {
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    Portfolio p2 = new Portfolio(10000);
    region.put("2", p2);
    p2.ID = 1000;
    region.put("2", p2);
    SelectResults noIndexResult = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 2").execute();
  
    region.clear();
    index = qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    p2 = new Portfolio(10000);
    region.put("2", p2);
    p2.ID = 1000;
    region.put("2", p2);
    
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID = 2").execute();
    assertEquals(numEntries/numIds - 1, results.size());
    assertEquals(noIndexResult, results);
    assertTrue(observer.indexUsed);
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * on local region
   * @throws Exception
   */
  public void testHashIndexNotEqualsQueryOnLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * on replicated region
   * @throws Exception
   */
  public void testHashIndexNotEqualsQueryOnReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * on partitioned region
   * @throws Exception
   */
  public void testHashIndexNotEqualsQueryOnPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * for local region
   * @throws Exception
   */
  public void testHashIndexNotEqualsForMultipleResultQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * for replicated region
   * @throws Exception
   */
  public void testHashIndexNotEqualsForMultipleResultQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct number of results
   * for partitioned region
   * @throws Exception
   */
  public void testHashIndexNotEqualsForMultipleResultQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    int numEntries = 200;
    int numIds = 100;
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i % (numIds));
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexInQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID in set (1)");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexInQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID in set (1)");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  public void testHashIndexInQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID in set (1)");
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * for local region
   * @throws Exception
   */
  public void testHashIndexNotUsedInRangeQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexNotUsedInRangeQuery();
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * for replicated region
   * @throws Exception
   */
  public void testHashIndexNotUsedInRangeQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexNotUsedInRangeQuery();
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * for partitioned region
   * @throws Exception
   */
  public void testHashIndexNotUsedInRangeQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexNotUsedInRangeQuery();
  }
  
  /**
   * Tests that hash index is used and that it returns the correct result
   * @throws Exception
   */
  private void helpTestHashIndexNotUsedInRangeQuery() throws Exception {
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID < 2").execute();
    assertFalse(observer.indexUsed);
  }

  /**
   * Test order by asc query for local region using hash index
   * @throws Exception
   */
  public void testHashIndexOrderByAscQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByAscQuery();
  }
  
  /**
   * Test order by asc query for replicated region using hash index
   * @throws Exception
   */
  public void testHashIndexOrderByAscQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByAscQuery();
  }
  
  /**
   * Test order by asc query for partitioned region using hash index
   * @throws Exception
   */
  public void testHashIndexOrderByAscQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByAscQuery();
  }
  
  private void helpTestHashIndexOrderByAscQuery() throws Exception {
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID != 0 order by ID asc ").execute();
    assertEquals(199, results.size());
    assertTrue(observer.indexUsed);
    int countUp = 1;
    for (Object o: results) {
      Portfolio p = (Portfolio) o;
      assertEquals(countUp++, p.getID());
    }
  }
  
  /**
   * Test order by desc query for local region using hash index
   * @throws Exception
   */
  public void testHashIndexOrderByDescQueryForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByDescQuery();
  }
  
  /**
   * Test order by desc query for replicated region using hash index
   * @throws Exception
   */
  public void testHashIndexOrderByDescQueryForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByDescQuery();
  }
  
  /**
   * Test order by desc query for partitioned region using hash index
   * @throws Exception
   */
  public void testHashIndexOrderByDescQueryForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    createData(region, 200);
    helpTestHashIndexOrderByDescQuery();
  }
  
  /**
   * Tests that hash index on non sequential hashes
   * for local region
   * @throws Exception
   */
  public void testHashIndexOnNonSequentialHashForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    for (int i = 0; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 200; i < 300; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 500; i < 600; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index on non sequential hashes
   * for replicated region
   * @throws Exception
   */
  public void testHashIndexOnNonSequentialHashForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    for (int i = 0; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 200; i < 300; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 500; i < 600; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  /**
   * Tests that hash index on non sequential hashes
   * for partitioned region
   * @throws Exception
   */
  public void testHashIndexOnNonSequentialHashForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    for (int i = 0; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 200; i < 300; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    
    for (int i = 500; i < 600; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }
    helpTestHashIndexForQuery("Select * FROM /portfolios p where p.ID != 1");
  }
  
  private void helpTestHashIndexOrderByDescQuery() throws Exception {
    index = (Index)qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID != 0 order by ID desc ").execute();
    assertEquals(199, results.size());
    assertTrue(observer.indexUsed);
    int countDown = 199;
    for (Object o: results) {
      Portfolio p = (Portfolio) o;
      assertEquals(countDown--, p.getID());
    }
  }
  
  /**
   * test async exception for hash index using partitioned region
   * @throws Exception
   */
  public void testHashIndexAsyncMaintenanceExceptionForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios_async", false);
    helpTestAsyncMaintenance();
  }
  
  private void helpTestAsyncMaintenance() throws Exception {
    boolean expected = false;
    try {
      index = qs.createHashIndex("idHash", "p.ID", "/portfolios_async p");
    }
    catch ( UnsupportedOperationException e) {
      expected = true;
    }
    catch (IndexInvalidException e) {
      //for partition region execption;
      expected = true;
    }
  
    assertTrue(expected);
  }
  
  /**
   * test multiple iterators exception for hash index using local region
   * @throws Exception
   */
  public void testHashIndexMultipleIteratorsExceptionForLocalRegion() throws Exception {
    createLocalRegion("portfolios");
    helpTestMultipleIteratorsException();
  }
  
  /**
   * test multiple iterators exception for hash index using replicated region
   * @throws Exception
   */
  public void testHashIndexMultipleIteratorsExceptionForReplicatedRegion() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestMultipleIteratorsException();
  }
  
  /**
   * test multiple iterators exception for hash index using partiioned region
   * @throws Exception
   */
  public void testHashIndexMultipleIteratorsExceptionForPartitionedRegion() throws Exception {
    createPartitionedRegion("portfolios");
    helpTestMultipleIteratorsException();
  }
  
  private void helpTestMultipleIteratorsException() throws Exception {
    boolean expected = false;
    try {
      index = qs.createHashIndex("idHash",
          "p.ID", "/portfolios p, p.positions.values p");
    }
    catch ( UnsupportedOperationException e) {
      expected = true;
    }
    assertTrue(expected);
  }
  
  /**
   * test remove and not equals Query
   * @throws Exception
   */
  public void testRemoveAndNotEqualsQuery() throws Exception {
    createReplicatedRegion("portfolios");
    helpTestRemoveAndNotEqualsQuery();
  }
  
  private void helpTestRemoveAndNotEqualsQuery() throws Exception {
    int numEntries = 200;
    index = qs.createHashIndex("idHash", "p.ID", "/portfolios p");
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = (short)i;
      region.put("" + i, p);
    }

    region.destroy("1");
    
    SelectResults results = (SelectResults)qs.newQuery("Select * FROM /portfolios p where p.ID != 1").execute();
    assertEquals(numEntries - 1, results.size());
    assertTrue(observer.indexUsed);
  }
  
  
  
  private void createLocalRegion(String regionName) throws ParseException {
    createLocalRegion(regionName, true);
  }
  
  private void createLocalRegion(String regionName, boolean synchMaintenance) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    attributesFactory.setIndexMaintenanceSynchronous(synchMaintenance);
    RegionAttributes regionAttributes = attributesFactory.create();
    region = cache.createRegion(regionName, regionAttributes);
  }
  
  private void createReplicatedRegion(String regionName) throws ParseException {
    createReplicatedRegion(regionName, true);
  }
  
  private void createReplicatedRegion(String regionName, boolean synchMaintenance) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    attributesFactory.setIndexMaintenanceSynchronous(synchMaintenance);
    RegionAttributes regionAttributes = attributesFactory.create();
    region = cache.createRegion(regionName, regionAttributes);
  }
  
  private void createPartitionedRegion(String regionName) throws ParseException {
    createLocalRegion(regionName, true);
  }

  private void createPartitionedRegion(String regionName, boolean synchMaintenance) throws ParseException {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    attributesFactory.setIndexMaintenanceSynchronous(synchMaintenance);
    RegionAttributes regionAttributes = attributesFactory.create();
    region = cache.createRegion(regionName, regionAttributes);
  }
  
  private void createData(Region region, int numEntries) {
    for (int i = 0; i < numEntries; i++) {
      Portfolio p = new Portfolio(i);
      region.put("" + i, p);
    }
  }
  
  
  class MyQueryObserverAdapter extends QueryObserverAdapter {
    public boolean indexUsed = false;
    
    public void afterIndexLookup(Collection results){
      super.afterIndexLookup(results);
      indexUsed = true;
    }
    
   
  };
}
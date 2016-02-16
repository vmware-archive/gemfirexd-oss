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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;

/**
 * 
 * @author Tejas Nomulwar
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CompactRangeIndexTest  extends TestCase {
  private QueryTestUtils utils;
  private Index index;

  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(CompactRangeIndexTest.class);
    return suite;
  }
  
  public void setUp() {
    utils = new QueryTestUtils();
    utils.createCache(null);
    utils.createReplicateRegion("exampleRegion");
  }

  public void test000CompactRangeIndex() throws Exception{
    System.setProperty("index_elemarray_threshold", "3");
    index = utils.createIndex("type", "\"type\"", "/exampleRegion");
    putValues(9);
    isUsingIndexElemArray("type1");
    putValues(10);
    isUsingConcurrentHashSet("type1");
    utils.removeIndex("type", "/exampleRegion");
    executeQueryWithAndWithoutIndex(4);
    updateValues(2);
    executeQueryWithCount();
    executeQueryWithAndWithoutIndex(3);
    executeRangeQueryWithDistinct(8);
    executeRangeQueryWithoutDistinct(9);
  }
  
  /*
   * Tests adding entries to compact range index where the key is null
   * fixes bug 47151 where null keyed entries would be removed after being added
   */
  public void test001NullKeyCompactRangeIndex() throws Exception {
    index = utils.createIndex("indexName", "status", "/exampleRegion");
    Region region = utils.getCache().getRegion("exampleRegion");
   
    //create objects
    int numObjects = 10;
    for (int i = 1; i <= numObjects; i++) {
      Portfolio p = new Portfolio(i);
      p.status = null;
      region.put("KEY-"+ i, p);
    }
    //execute query and check result size
    QueryService qs = utils.getCache().getQueryService();
    SelectResults results = (SelectResults) qs.newQuery("Select * from /exampleRegion r where r.status = null").execute();
    assertEquals("Null matched Results expected", numObjects, results.size());
  }
  
 
  public void putValues(int num) {
    long start = System.currentTimeMillis();
    utils.createValuesStringKeys("exampleRegion", num);
  }
  
  private void updateValues(int num){
    utils.createDiffValuesStringKeys("exampleRegion", num);
  }
 
  public void executeQueryWithCount() throws Exception{
    String[] queries = { "520" };
    for (Object result :  utils.executeQueries(queries)) {
      if (result instanceof Collection) {
       for (Object e : (Collection) result) {
         if(e instanceof Integer) {
          assertEquals(10,((Integer) e).intValue());
         }
       }
     }
    }
  }
  
  private void isUsingIndexElemArray(String key){
    if(index instanceof CompactRangeIndex){
         assertEquals(true, getValuesFromMap(key) instanceof IndexElemArray);
    }
    else{
      fail("Should have used CompactRangeIndex");
    }
  }

  private void isUsingConcurrentHashSet(String key){
    if(index instanceof CompactRangeIndex){
      assertEquals(getValuesFromMap(key) instanceof IndexConcurrentHashSet, true);
    }
    else{
      fail("Should have used CompactRangeIndex");
    }
  }

  private Object getValuesFromMap(String key){
    MemoryIndexStore ind = (MemoryIndexStore) ((CompactRangeIndex)index).getIndexStorage();
    Map map = ind.valueToEntriesMap;
    Object entryValue = map.get(key);
    return entryValue;
  }
  
  public void executeQueryWithAndWithoutIndex(int expectedResults) {
    try {
      executeSimpleQuery(expectedResults);
    } catch (Exception e) {
      fail("Query execution failed. : "+e);
    }
    index = utils.createIndex("type", "\"type\"", "/exampleRegion");
    try {
      executeSimpleQuery( expectedResults);
    } catch (Exception e) {
      fail("Query execution failed. : " +e);
    }
    utils.removeIndex("type", "/exampleRegion");
  }
  
  private int executeSimpleQuery( int expResults) throws Exception{
    String[] queries = { "519" }; //SELECT * FROM /exampleRegion WHERE \"type\" = 'type1'
    int results = 0;
    for (Object result :  utils.executeQueries(queries)) {
      if (result instanceof SelectResults) {
       Collection<?> collection = ((SelectResults<?>) result).asList();
       results = collection.size();
       assertEquals(expResults, results);
       for (Object e : collection) {
         if(e instanceof Portfolio){
          assertEquals("type1",((Portfolio)e).getType());
         }
       }
     }
   }
    return results;
  }

  private int executeRangeQueryWithDistinct( int expResults) throws Exception{
    String[] queries = { "181" };
    int results = 0;
    for (Object result : utils.executeQueries(queries)) {
      if (result instanceof SelectResults) {
        Collection<?> collection = ((SelectResults<?>) result).asList();
        results = collection.size();
        assertEquals(expResults, results);
        int[] ids = {};
        List expectedIds = new ArrayList(Arrays.asList( 10, 9, 8, 7, 6, 5, 4, 3, 2 ));
        for (Object e : collection) {
          if (e instanceof Portfolio) {
            assertTrue(expectedIds.contains(((Portfolio) e).getID()));
            expectedIds.remove((Integer)((Portfolio) e).getID());
          }
        }
      }
    }
    return results;
  }
  
  private int executeRangeQueryWithoutDistinct( int expResults){
    String[] queries = { "181" };
    int results = 0;
    for (Object result :  utils.executeQueriesWithoutDistinct(queries)) {
      if (result instanceof SelectResults) {
       Collection<?> collection = ((SelectResults<?>) result).asList();
       results = collection.size();
       assertEquals(expResults, results);
       List expectedIds = new ArrayList(Arrays.asList( 10, 9, 8, 7, 6, 5, 4, 3, 3 ));
       for (Object e : collection) {
         if(e instanceof Portfolio){
           assertTrue(expectedIds.contains(((Portfolio) e).getID()));
           expectedIds.remove((Integer)((Portfolio) e).getID());
         }
       }
     }
   }
    return results;
  }
  
  @Override
  public void tearDown() throws Exception{
    utils.closeCache();
  }

}

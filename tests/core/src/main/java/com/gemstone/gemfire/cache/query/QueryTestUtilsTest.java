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
package com.gemstone.gemfire.cache.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndexTest;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * A sample test class using the QueryTestUtils
 * 
 * @author Tejas Nomulwar
 * 
 */
public class QueryTestUtilsTest extends TestCase {

  private static final long serialVersionUID = 1L;

  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(QueryTestUtilsTest.class);
    return suite;
  }

  public QueryTestUtilsTest(String name) {
    super(name);
  }

  QueryTestUtils utils;
  public void setUp() throws Exception {

    utils = new QueryTestUtils();
    utils.createCache(null);
    // create regions
    utils.createReplicateRegion("exampleRegion");
    utils.createReplicateRegion("numericRegion");
    // put entries in the region
    utils.createValuesStringKeys("exampleRegion", 10);
    utils.createNumericValuesStringKeys("numericRegion", 10);
  }

  public void testQueries()  {
    String[] queries = { "1" }; //SELECT * FROM /exampleRegion WHERE status = 'active'
    int results = 0;
    try {
      for (Object result :  utils.executeQueries(queries)) {
        if (result instanceof SelectResults) {
         Collection<?> collection = ((SelectResults<?>) result).asList();
         results = collection.size();
         assertEquals(5, results);
         for (Object e : collection) {
           if(e instanceof Portfolio){
            assertEquals(true,((Portfolio)e).isActive());
           }
         }
       }
  }
    } catch (Exception e) {
      fail("Query execution failed. : " + e);
    }
    // execute all the queries from the map
    // utils.executeAllQueries();
  }
  
  public void testQueriesWithoutDistinct() throws Exception{
    utils.createDiffValuesStringKeys("exampleRegion", 2);
    String[] queries = { "181" };
    int results = 0;
    for (Object result :  utils.executeQueriesWithoutDistinct(queries)) {
      if (result instanceof SelectResults) {
       Collection<?> collection = ((SelectResults<?>) result).asList();
       results = collection.size();
       assertEquals(9, results);
       List expectedIds = new ArrayList(Arrays.asList( 10, 9, 8, 7, 6, 5, 4, 3, 3 ));
       for (Object e : collection) {
         if (e instanceof Portfolio) {
           assertTrue(expectedIds.contains(((Portfolio) e).getID()));
           expectedIds.remove((Integer)((Portfolio) e).getID());
         }
       }
     }
   }
  }

  public void testQueriesWithDistinct() throws Exception{
    String[] queries = { "181" };
    int results = 0;
    int i = 7;
    for (Object result :  utils.executeQueriesWithDistinct(queries)) {
      if (result instanceof SelectResults) {
       Collection<?> collection = ((SelectResults<?>) result).asList();
       results = collection.size();
       assertEquals(8, results);
       List expectedIds = new ArrayList(Arrays.asList( 10, 9, 8, 7, 6, 5, 4, 3, 2 ));
       for (Object e : collection) {
         if (e instanceof Portfolio) {
           assertTrue(expectedIds.contains(((Portfolio) e).getID()));
           expectedIds.remove((Integer)((Portfolio) e).getID());
         }
       }
     }
   }    
  }
  
  @Override
  public void tearDown() throws Exception{
    utils.closeCache();
  }


}

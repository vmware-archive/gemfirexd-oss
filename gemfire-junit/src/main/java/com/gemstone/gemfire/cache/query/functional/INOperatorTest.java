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
/*
 * InOperatorTest.java
 *
 * Created on March 24, 2005, 5:08 PM
 */

package com.gemstone.gemfire.cache.query.functional;

import java.util.*;
import junit.framework.*;
import com.gemstone.gemfire.cache.query.*;
import java.util.ArrayList;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.internal.ResultsBag;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.data.Employee;

import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.CompiledSelect;
import com.gemstone.gemfire.cache.query.internal.CompiledJunction;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;


/**
 *
 * @author vikramj
 */
public class INOperatorTest extends TestCase {
  
  public INOperatorTest(String testName) {
    super(testName);
  }
  
  public static void main(java.lang.String[] args) {
    junit.textui.TestRunner.run(suite());
  }
  
  protected void setUp() throws Exception {
    CacheUtils.startCache();
  }
  
  protected void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  public static Test suite(){
    TestSuite suite = new TestSuite(INOperatorTest.class);
    return suite;
  }
  
  // For decomposition of IN expressions, need to unit-test the following:
  // 1) IN expr gets decomposed on indexed expression with func index (done)
  // 2) index gets used on IN clause (done)
  // 3) IN expr does not get decomposed on unindexed expression (not done)
  // 4) Decomposed IN expr works with bind parameters (not done)
  // 5) IN expr does get decomposed on indexed expression with pk index (not done)
  // 6) pk index gets used with IN expression (not done)
  // 7) decomposition (or not) of nested IN expressions (not done)
  // 8) test IN clauses with:
  //   a) zero elements (should shortcircuit to return false ideally) (not done)
  //   b) one element (not done)
  //   c) more than one element (done) 
   
  /**
   * Test the decomposition of IN SET(..) that gets decomposed
   * into ORs so an index can be used
   * @author Eric Zoerner
   */
  public void _testInDecompositionWithFunctionalIndex() throws Exception {
 
  }
  
  public void testRegionBulkGet() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();
    
    Region region = cache.createRegion("pos",regionAttributes);
    
    region.put("6", new Integer(6));
    region.put("10", new Integer(10));
    region.put("12", new Integer(10));
    
    QueryService qs = cache.getQueryService();
    
    Query q;
    SelectResults results;
    Object[] keys;
    Set expectedResults;
    
    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "5", "6", "10", "45" };
    results = (SelectResults)q.execute(new Object[] {keys});
    expectedResults = new HashSet();
    expectedResults.add(new Integer(6));
    expectedResults.add(new Integer(10));
    assertEquals(expectedResults, results.asSet());
    
    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "42" };
    results = (SelectResults)q.execute(new Object[] {keys});
    expectedResults = new HashSet();
    assertEquals(expectedResults, results.asSet());    
    
    for (int i = 0; i < 1000; i++) {
      region.put(String.valueOf(i), new Integer(i));
    }
    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "5", "6", "10", "45" };
    results = (SelectResults)q.execute(new Object[] {keys});
    expectedResults = new HashSet();
    expectedResults.add(new Integer(5));
    expectedResults.add(new Integer(6));
    expectedResults.add(new Integer(10));
    expectedResults.add(new Integer(45));
    assertEquals(expectedResults, results.asSet());
    
    q = qs.newQuery("SELECT e.key, e.value FROM /pos.entrySet e WHERE e.key IN $1");
    keys = new Object[] { "5", "6", "10", "45" };
    results = (SelectResults)q.execute(new Object[] {keys});
    assertEquals(4, results.size());    

    region.destroyRegion();
  }
  
  
  public void testIntSet() throws Exception {
    
    Query q = CacheUtils.getQueryService().newQuery("2 IN SET(1,2,3)");
    
    Object result = q.execute();
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for IN operator");
  }
  public void testStringSet() throws Exception {
    
    Query q = CacheUtils.getQueryService().newQuery("'a' IN SET('x','y','z')");
    
    Object result = q.execute();
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.FALSE))
      fail("Failed for StringSet with IN operator");
  }

  public void testShortNumSet() throws Exception {
    Short num = Short.valueOf("1");
    Object params[]=new Object[1];
    params[0]= num;
    
    Query q = CacheUtils.getQueryService().newQuery("$1 IN SET(1,2,3)");
    
    Object result = q.execute(params);
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for ShortNum with IN operator");
  }
 
  public void testCollection() throws Exception {
    Object e1 = new Object();
    Object e2 = new Object();
    Object e3 = new Object();
    HashSet C1 = new HashSet();
    C1.add(e1);
    C1.add(e2);
    C1.add(e3);
    Object params[]=new Object[3];
    params[0]= e1;
    params[1]= C1;
    params[2]= e2;
    
    Query q = CacheUtils.getQueryService().newQuery("$3 IN $2");
    Object result = q.execute(params);
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for Collection with IN operator");
  }
  
  public void testWithSet() throws Exception {
    String s1 = "Hello";
    String s2 = "World";
    HashSet H1 = new HashSet();
    H1.add(s1);
    H1.add(s2);
    Object params[]=new Object[2];
    params[0]= s1;
    params[1]= H1;
    Query q = CacheUtils.getQueryService().newQuery("$1 IN $2");
    Object result = q.execute(params);
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for String set with IN operator");
  }
  public void testArrayList() throws Exception {
    String s1 = "sss";
    String s2 = "ddd";
    ArrayList AL1 = new ArrayList();
    AL1.add(s1);
    AL1.add(s2);
    Object params[]=new Object[3];
    params[0]= s1;
    params[1]= s2;
    params[2]= AL1;
    Query q = CacheUtils.getQueryService().newQuery("$1 IN $3");
    Object result = q.execute(params);
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for ArrayList with IN operator");
  }
  
  public void testNULL() throws Exception {
    Query q = CacheUtils.getQueryService().newQuery(" null IN SET('x','y','z')");
    Object result = q.execute();
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.FALSE))
      fail("Failed for NULL in IN operator Test");
    
    q = CacheUtils.getQueryService().newQuery(" null IN SET(null)");
    result = q.execute();
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.TRUE))
      fail("Failed for NULL in IN operator Test");
    
  }
  
  public void testUNDEFINED() throws Exception {
    Query q = CacheUtils.getQueryService().newQuery(" UNDEFINED IN SET(1,2,3)");
    Object result = q.execute();
    System.out.println(Utils.printResult(result));
    if(!result.equals(Boolean.FALSE))
      fail("Failed for UNDEFINED with IN operator");
    
    q = CacheUtils.getQueryService().newQuery(" UNDEFINED IN SET(UNDEFINED)");
    result = q.execute();
    System.out.println(Utils.printResult(result));
    if(!result.equals(QueryService.UNDEFINED))
      fail("Failed for UNDEFINED with IN operator");
    
    q = CacheUtils.getQueryService().newQuery(" UNDEFINED IN SET(UNDEFINED,UNDEFINED)");
    result = q.execute();
    System.out.println(Utils.printResult(result));
    if(!result.equals(QueryService.UNDEFINED))
      fail("Failed for UNDEFINED with IN operator");
  }
  
  public void testMiscSet() throws Exception {
    Query q = CacheUtils.getQueryService().newQuery(" $1 IN SET(1, 'a', $2, $3, $4, $5)");
    Object params[] = {null, new Integer(0), "str", null, new Object()};
    
    for(int i=1;i<params.length;i++){
      params[0] = params[i];
      Object result = q.execute(params);
      System.out.println(Utils.printResult(result));
      if(!result.equals(Boolean.TRUE))
        fail("Failed for Mix set with IN operator");
    }
    
  }
  
  public void testIndexUsageWithIn() throws Exception {
	    Cache cache = CacheUtils.getCache();
	    AttributesFactory attributesFactory = new AttributesFactory();
	    RegionAttributes regionAttributes = attributesFactory.create();
	    
	    Region region = cache.createRegion("pos",regionAttributes);
	    
	    region.put("6", new Integer(6));
	    region.put("10", new Integer(10));
	    region.put("12", new Integer(10));
	    
	    QueryService qs = cache.getQueryService();
	    qs.createIndex("In Index", IndexType.FUNCTIONAL, "e.key","/pos.entrySet e");
	    Query q;
	    SelectResults results;
	    Object[] keys;
	    Set expectedResults;
	    
	    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "5", "6", "10", "45" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    expectedResults = new HashSet();
	    expectedResults.add(new Integer(6));
	    expectedResults.add(new Integer(10));
	    assertEquals(expectedResults, results.asSet());
	    
	    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "42" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    expectedResults = new HashSet();
	    assertEquals(expectedResults, results.asSet());    
	    
	    for (int i = 0; i < 1000; i++) {
	      region.put(String.valueOf(i), new Integer(i));
	    }
	    q = qs.newQuery("SELECT e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "5", "6", "10", "45" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    expectedResults = new HashSet();
	    expectedResults.add(new Integer(5));
	    expectedResults.add(new Integer(6));
	    expectedResults.add(new Integer(10));
	    expectedResults.add(new Integer(45));
	    assertEquals(expectedResults, results.asSet());
	    
	    q = qs.newQuery("SELECT e.key, e.value FROM /pos.entrySet e WHERE e.key IN $1");
	    keys = new Object[] { "5", "6", "10", "45" };
	    results = (SelectResults)q.execute(new Object[] {keys});
	    assertEquals(4, results.size());    
	  }
	  
}

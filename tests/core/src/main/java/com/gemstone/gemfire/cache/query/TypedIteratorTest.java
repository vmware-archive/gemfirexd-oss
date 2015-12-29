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
 * TypedIteratorTest.java
 * JUnit based test
 *
 * Created on March 22, 2005, 2:01 PM
 */

package com.gemstone.gemfire.cache.query;

import junit.framework.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.data.*;

/**
 *
 * @author ericz
 */
public class TypedIteratorTest extends TestCase {
  Region region;
  QueryService qs;
  Cache cache;
  
  public TypedIteratorTest(String testName) {
    super(testName);
  }
  
  public void testUntyped() throws QueryException {
    // one untyped iterator is now resolved fine
    Query q = this.qs.newQuery("SELECT DISTINCT * " +
                "FROM /pos " +
                "WHERE ID = 3 ");
    q.execute();
    
    // if there are two untyped iterators, then it's a problem, see bug 32251 and BugTest
    q = this.qs.newQuery("SELECT DISTINCT * FROM /pos, positions WHERE ID = 3");
    try {
      q.execute();
      fail("Expected a TypeMismatchException");
    }
    catch (TypeMismatchException e) {
      // pass
    }
  }
  
  public void testTyped() throws QueryException {
    Query q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM /pos TYPE Portfolio\n" +
      "WHERE ID = 3  ");
    Object r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));

    
    q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM /pos ptfo TYPE Portfolio\n" +
      "WHERE ID = 3  ");
    r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
  }  
  
  
  public void testTypeCasted() throws QueryException {
    Query q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT com.gemstone.gemfire.cache.\"query\".data.Portfolio;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM (collection<Portfolio>)/pos\n" +
      "WHERE ID = 3  ");
//    com.gemstone.gemfire.internal.util.DebuggerSupport.waitForJavaDebugger(this.cache.getLogger());
    Object r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
    
    q = this.qs.newQuery( // must quote "query" because it is a reserved word
      "IMPORT com.gemstone.gemfire.cache.\"query\".data.Position;\n" +       
      "SELECT DISTINCT *\n" +
      "FROM /pos p, (collection<Position>)p.positions.values\n" +
      "WHERE secId = 'IBM'");
//    com.gemstone.gemfire.internal.util.DebuggerSupport.waitForJavaDebugger(this.cache.getLogger());
    r = q.execute();
    CacheUtils.getLogger().fine(Utils.printResult(r));
  }
  
  protected void setUp() throws Exception {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
//    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();
    
    region = cache.createRegion("pos",regionAttributes);
    region.put("0",new Portfolio(0));
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(2));
    region.put("3",new Portfolio(3));
    
    qs = cache.getQueryService();
  }
  
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
}

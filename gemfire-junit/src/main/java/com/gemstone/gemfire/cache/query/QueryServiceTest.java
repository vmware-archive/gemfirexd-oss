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
 * QueryServiceTest.java
 * JUnit based test
 *
 * Created on March 8, 2005, 4:53 PM
 */

package com.gemstone.gemfire.cache.query;

import junit.framework.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;

/**
 *
 * @author vaibhav
 */
public class QueryServiceTest extends TestCase {
  
  public QueryServiceTest(String testName) {
    super(testName);
  }
  
  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for(int i=0;i<5;i++){
      region.put(i+"",new Portfolio(i));
    }
  }
  
  protected void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  public static junit.framework.Test suite() {
    junit.framework.TestSuite suite = new junit.framework.TestSuite(QueryServiceTest.class);
    
    return suite;
  }
  
  // tests to make sure no exception is thrown when a valid query is created
  // and an invalid query throws an exception
  public void testNewQuery() throws Exception {
    System.out.println("testNewQuery");
    QueryService qs = CacheUtils.getQueryService();
    qs.newQuery("SELECT DISTINCT * FROM /root");
    
    try {
      qs.newQuery("SELET DISTINCT * FROM /root");
      fail("Should have thrown an InvalidQueryException");
    }
    catch (QueryInvalidException e) {
      //pass
    }
  }
  
  public void testCreateIndex() throws Exception {
    System.out.println("testCreateIndex");
    QueryService qs = CacheUtils.getQueryService();
//    DebuggerSupport.waitForJavaDebugger(CacheUtils.getLogger());
    Index index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/Portfolios");
    
    try{
      index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/Portfolios");
      if(index != null)
        fail("QueryService.createIndex allows duplicate index names");
    }
    catch(IndexNameConflictException e) {
    }
    
    try{
      index = qs.createIndex("statusIndex1", IndexType.FUNCTIONAL,"status","/Portfolios");
      if(index != null)
        fail("QueryService.createIndex allows duplicate indexes");
    }
    catch(IndexExistsException e) {
    }
  }
  
  public void testIndexDefinitions() throws Exception{
    Object[][] testDataFromClauses = {
      {"status", "/Portfolios", Boolean.TRUE},
      {"status", "/Portfolios.entries", Boolean.FALSE},
      {"status", "/Portfolios.values", Boolean.TRUE},
      {"status", "/Portfolios.keys", Boolean.TRUE},
      {"status", "/Portfolios p", Boolean.TRUE},
      {"status", "/Portfolio", Boolean.FALSE},
      {"status", "/Portfolio.positions", Boolean.FALSE},
      {"status", "/Portfolios p, p.positions", Boolean.TRUE},
    };
    
    runCreateIndexTests(testDataFromClauses);
    
    Object[][] testDataIndexExpr = {
      {"positions", "/Portfolios", Boolean.FALSE},
      {"status.length", "/Portfolios", Boolean.TRUE},
      {"p.status", "/Portfolios p", Boolean.TRUE},
      {"p.getStatus()", "/Portfolios p", Boolean.TRUE},
      {"pos.value.secId", "/Portfolios p, p.positions pos", Boolean.TRUE},
      {"pos.getValue().getSecId()", "/Portfolios p, p.positions pos", Boolean.TRUE},
      {"pos.getValue.secId", "/Portfolios p, p.positions pos", Boolean.TRUE},
      {"secId", "/Portfolios p, p.positions", Boolean.FALSE},
      {"is_defined(status)", "/Portfolios", Boolean.FALSE},
      {"is_undefined(status)", "/Portfolios", Boolean.FALSE},
      {"NOT(status = null)", "/Portfolios", Boolean.FALSE},
      {"$1", "/Portfolios", Boolean.FALSE},
    };
    
    runCreateIndexTests(testDataIndexExpr);
  }
  
  private void runCreateIndexTests(Object testData[][]) throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    qs.removeIndexes();
    for(int i=0;i<testData.length;i++){
      //System.out.println("indexExpr="+testData[i][0]+" from="+testData[i][1]);
      Index index = null;
      try{
        String indexedExpr = (String)testData[i][0];
        String fromClause = (String)testData[i][1];
//        if (indexedExpr.equals("status") && i == 7) DebuggerSupport.waitForJavaDebugger(CacheUtils.getLogger());
        index = qs.createIndex("index"+i, IndexType.FUNCTIONAL, indexedExpr, fromClause);
        if(testData[i][2] == Boolean.TRUE && index == null){
          fail("QueryService.createIndex unable to  create index for indexExpr="+testData[i][0]+" from="+testData[i][1]);
        }else if(testData[i][2] == Boolean.FALSE && index != null){
          fail("QueryService.createIndex allows to create index for un-supported index definition (indexExpr="+testData[i][0]+" from="+testData[i][1]+")");
        }
        //System.out.println((index == null ? "" : index.toString()));
      }catch(Exception e){
        //e.printStackTrace();
        //System.out.println("NOT ALLOWDED "+e);
        if(testData[i][2] == Boolean.TRUE){
          e.printStackTrace();
          fail("QueryService.createIndex unable to  create index for indexExpr="+testData[i][0]+" from="+testData[i][1]);
        }
      } finally{
        if(index != null)
          qs.removeIndex(index);
      }
      //System.out.println("");
    }
  }
  
  public void atestGetIndex() throws Exception{
    System.out.println("testGetIndex");
    QueryService qs = CacheUtils.getQueryService();
    Object testData[][] ={
      {"status", "/Portfolios", Boolean.TRUE},
      {"status", "/Portfolios.values", Boolean.FALSE},
      {"status", "/Portfolios p", Boolean.TRUE},
      {"p.status", "/Portfolios p", Boolean.TRUE},
      {"status", "/Portfolios.values x", Boolean.FALSE},
      {"x.status", "/Portfolios.values x", Boolean.FALSE},
      {"status", "/Portfolio", Boolean.FALSE},
      {"p.status", "/Portfolios", Boolean.FALSE},
      {"ID", "/Portfolios", Boolean.FALSE},
      {"p.ID", "/Portfolios p", Boolean.FALSE},
      {"is_defined(status)", "/Portfolios", Boolean.FALSE},
    };
    
    Region r = CacheUtils.getRegion("/Portfolios");
    Index index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
    index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", "/Portfolios p");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
    index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios.values");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
    index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", "/Portfolios.values p");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
  }
  
    
  // no longer support getting indexes by fromClause, type, and indexedExpression, so this commented out
//  private void runGetIndexTests(Object testData[][]){
//    for(int i=0;i<testData.length;i++){
//      try{
//        Index index = CacheUtils.getQueryService().getIndex((String)testData[i][1],IndexType.FUNCTIONAL,(String)testData[i][0]);
//        if(testData[i][2] == Boolean.TRUE && index == null){
//          fail("QueryService.getIndex unable to  find index for indexExpr="+testData[i][0]+" from="+testData[i][1]);
//        }else if(testData[i][2] == Boolean.FALSE && index != null){
//          fail("QueryService.getIndex return non-matching index for indexExpr="+testData[i][0]+" from="+testData[i][1]);
//        }
//      }catch(Exception e){
//      }
//    }
//  }
  
  public void testRemoveIndex() throws Exception{
    System.out.println("testRemoveIndex");
    QueryService qs = CacheUtils.getQueryService();
    Index index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", "/Portfolios p");
    qs.removeIndex(index);
    index = qs.getIndex(CacheUtils.getRegion("/Portfolios"), "statusIndex");
    if(index !=null)
      fail("QueryService.removeIndex is not removing index");
  }
  
  public void testRemoveIndexes() throws Exception{
    System.out.println("testRemoveIndexes");
    CacheUtils.createRegion("Ptfs", Portfolio.class);
    CacheUtils.createRegion("Ptfs1", Portfolio.class);
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/Portfolios");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/Ptfs");
    qs.removeIndexes();
    Collection allIndexes = qs.getIndexes();
    if(allIndexes.size() != 0)
      fail("QueryService.removeIndexes() does not removes all indexes");
  }
  
  public void testGetIndexes() throws Exception{
    System.out.println("testGetIndexes");
    CacheUtils.createRegion("Ptfs", Portfolio.class);
    CacheUtils.createRegion("Ptfs1", Portfolio.class);
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/Portfolios");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/Ptfs");
    Collection allIndexes = qs.getIndexes();
    if(allIndexes.size() != 2)
      fail("QueryService.getIndexes() does not return correct indexes");
  }
  
}

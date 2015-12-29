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
 * IndexTest.java
 * JUnit based test
 *
 * Created on March 9, 2005, 3:30 PM
 */

package com.gemstone.gemfire.cache.query.internal.index;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;


/**
 *
 * @author vaibhav
 */
public class IndexTest extends TestCase {
  
  static Index index;
  static Region region;
  static final String indexName = "testIndex";
  static{
    try{
      CacheUtils.startCache();
      QueryService qs = CacheUtils.getQueryService();
      region = CacheUtils.createRegion("Portfolios", Portfolio.class);
      index = qs.createIndex(indexName, IndexType.FUNCTIONAL,"p.status","/Portfolios p");
    }catch(Exception e){
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }
  
  public IndexTest(String testName) {
    super(testName);
  }
  
  protected void setUp() throws java.lang.Exception {
  }
  
  protected void tearDown() throws java.lang.Exception {
  }
  
  public static junit.framework.Test suite() {
    junit.framework.TestSuite suite = new junit.framework.TestSuite(IndexTest.class);
    
    return suite;
  }
  
  public void testGetName() {
    System.out.println("testGetName");
    if(!index.getName().equals(indexName))
      fail("Index.getName does not return correct index name ");
  }
  
  public void testGetType() {
    System.out.println("testGetType");
    if(index.getType() != IndexType.FUNCTIONAL)
      fail("Index.getName does not return correct index type");
  }
  
  public void testGetRegion() {
    System.out.println("testGetRegion");
    if(index.getRegion() != region)
      fail("Index.getName does not return correct region");
  }
  
  public void testGetStatistics() {
    System.out.println("testGetStatistics");
    //fail("The test case is empty.");
  }
  
  public void testGetFromClause() {
    System.out.println("testGetCanonicalizedFromClause");
    if(!index.getCanonicalizedFromClause().equals("/Portfolios index_iter1"))
      fail("Index.getName does not return correct from clause");
  }
  
  public void testGetCanonicalizedIndexedExpression() {
    System.out.println("testGetCanonicalizedIndexedExpression");
    if(!index.getCanonicalizedIndexedExpression().equals("index_iter1.status"))
      fail("Index.getName does not return correct index expression");
  }
  
  public void testGetCanonicalizedProjectionAttributes() {
    System.out.println("testGetCanonicalizedProjectionAttributes");
    if(!index.getCanonicalizedProjectionAttributes().equals("*"))
      fail("Index.getName does not return correct projection attributes");
  }
}

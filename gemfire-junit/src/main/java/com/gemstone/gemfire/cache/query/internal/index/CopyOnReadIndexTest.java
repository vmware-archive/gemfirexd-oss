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

import java.util.HashMap;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;

/**
 * 
 * @author jhuynh
 *
 */
public class CopyOnReadIndexTest extends TestCase {
  
  QueryTestUtils utils;
  static String regionName = "portfolios";
  static final String indexName = "testIndex";
  String idQuery = "select * from /" + regionName + " p where p.ID = 1";
  String secIdPosition1Query = "select * from /" + regionName + " p, p.positions.values pv where pv.secId = '1'";
 
  
  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }
  
  public CopyOnReadIndexTest(String testName) {
    super(testName);
  }
  
  protected void setUp() throws java.lang.Exception {
    utils = new QueryTestUtils();
    java.util.Properties p = new java.util.Properties();
    p.put("log-level", "info");
    utils.createCache(p);
    utils.getCache().setCopyOnRead(true);
  }
  
  protected void tearDown() throws java.lang.Exception {
    utils.getCache().getQueryService().removeIndexes();
    utils.closeCache();
  }
  
  public static junit.framework.Test suite() {
    junit.framework.TestSuite suite = new junit.framework.TestSuite(CopyOnReadIndexTest.class);
    return suite;
  }
  
  private void createData(Region region) {
    for (int i = 0 ; i < 10; i++) {
      Portfolio p = new Portfolio(i);
      p.status = "testStatus";
      p.positions = new HashMap();
      p.positions.put("" + i, new Position("" + i, 20));
      region.put("key-" + i, p);
    }   
  }
  
  public void testCopyOnReadQuery() throws Exception {
    utils.createLocalRegion(regionName);
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithHashIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createHashIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithHashIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createHashIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithHashIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createHashIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithCompactRangeIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithCompactRangeIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithCompactRangeIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnRead(idQuery);
  }

  public void testCopyOnReadWithRangeIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithRangeIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithRangeIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithRangeIndexTupleWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnRead(secIdPosition1Query);
  }
  
  public void testCopyOnReadWithRangeIndexTupleWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnRead(secIdPosition1Query);
  }
  
  public void testCopyOnReadWithRangeIndexTupleWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnRead(secIdPosition1Query);
  }
 
  //Test copy on read with no index
  public void testCopyOnReadWithNoIndexWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithNoIndexWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadWithNoIndexWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    helpTestCopyOnRead(idQuery);
  }
  
  public void testCopyOnReadNoIndexTupleWithLocalRegion() throws Exception {
    utils.createLocalRegion(regionName);
    helpTestCopyOnRead(secIdPosition1Query);
  }
  
  public void testCopyOnReadNoRangeIndexTupleWithReplicatedRegion() throws Exception {
    utils.createReplicateRegion(regionName);
    helpTestCopyOnRead(secIdPosition1Query);
  }
  
  public void testCopyOnReadNoRangeIndexTupleWithPartitionedRegion() throws Exception {
    utils.createPartitionRegion(regionName, null);
    helpTestCopyOnRead(secIdPosition1Query);
  }
  
  //Test copy on read false
  public void testCopyOnReadFalseWithHashIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false);
    utils.createLocalRegion(regionName);
    utils.createHashIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithHashIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createHashIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithHashIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createHashIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithCompactRangeIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithCompactRangeIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithCompactRangeIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p");
    helpTestCopyOnReadFalse(idQuery);
  }

  public void testCopyOnReadFalseWithRangeIndexWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithRangeIndexWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithRangeIndexWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "p.ID", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnReadFalse(idQuery);
  }
  
  public void testCopyOnReadFalseWithRangeIndexTupleWithLocalRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createLocalRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnReadFalse(secIdPosition1Query);
  }
  
  public void testCopyOnReadFalseWithRangeIndexTupleWithReplicatedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createReplicateRegion(regionName);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnReadFalse(secIdPosition1Query);
  }
  
  public void testCopyOnReadFalseWithRangeIndexTupleWithPartitionedRegion() throws Exception {
    utils.getCache().setCopyOnRead(false); 
    utils.createPartitionRegion(regionName, null);
    utils.createIndex(indexName, "pv.secId", "/" + regionName + " p, p.positions.values pv");
    helpTestCopyOnReadFalse(secIdPosition1Query);
  }
  
  private void helpTestCopyOnRead(String queryString) throws Exception {
    Region region = utils.getCache().getRegion("/" + regionName);
    createData(region);
    
    //execute query
    QueryService qs = utils.getCache().getQueryService();
    Query query = qs.newQuery(queryString);
    SelectResults results = (SelectResults) query.execute();
    assertEquals("No results were found", 1, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        p.status = "discardStatus";
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        p.status = "discardStatus";
      }
    }
    
    results = (SelectResults) query.execute();
    assertEquals("No results were found", 1, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        assertEquals("status should not have been changed", "testStatus", p.status);
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        assertEquals("status should not have been changed", "testStatus", p.status);
      }
    }
  }
  
  private void helpTestCopyOnReadFalse(String queryString) throws Exception {
    Region region = utils.getCache().getRegion("/" + regionName);
    createData(region);
    
    //execute query
    QueryService qs = utils.getCache().getQueryService();
    Query query = qs.newQuery(queryString);
    SelectResults results = (SelectResults) query.execute();
    assertEquals("No results were found", 1, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        p.status = "discardStatus";
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        p.status = "discardStatus";
      }
    }
    
    results = (SelectResults) query.execute();
    assertEquals("No results were found", 1, results.size());
    for (Object o: results) {
      if (o instanceof Portfolio) {
        Portfolio p = (Portfolio) o;
        assertEquals("status should have been changed", "discardStatus", p.status);
      }
      else {
        Struct struct = (Struct)o;
        Portfolio p = (Portfolio) struct.getFieldValues()[0];
        assertEquals("status should have been changed", "discardStatus", p.status);
      }
    }
  }
}

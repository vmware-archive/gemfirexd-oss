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
package com.gemstone.gemfire.management;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.dunit.QueryUsingFunctionContextDUnitTest;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.internal.ManagementStrings;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.beans.BeanUtilFuncs;
import com.gemstone.gemfire.management.internal.beans.QueryDataFunction;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;

import dunit.SerializableRunnable;

/**
 * 
 * @author rishim
 * 
 */

// 1) Test Basic Json Strings for Partitioned Regions
// Test Basic Json Strings for Replicated Regions
// Test for all Region Types
// Test for primitive types
// Test for Nested Objects
// Test for Enums
// Test for collections
// Test for huge collection
// Test PDX types
// Test different projects type e.g. SelectResult, normal bean etc..
// Test Colocated Regions
// Test for Limit ( both row count and Depth)
// ORDER by orders
// Test all attributes are covered in an complex type

public class QueryDataDUnitTest extends ManagementTestBase {

  private static final long serialVersionUID = 1L;

  private static final int MAX_WAIT = 100 * 1000;

  private static final int cntDest = 30;

  private static final int cnt = 0;

  // PR 5 is co-located with 4
  static String PartitionedRegionName1 = "TestPartitionedRegion1"; // default
                                                                   // name
  static String PartitionedRegionName2 = "TestPartitionedRegion2"; // default
                                                                   // name
  static String PartitionedRegionName3 = "TestPartitionedRegion3"; // default
                                                                   // name
  static String PartitionedRegionName4 = "TestPartitionedRegion4"; // default
                                                                   // name
  static String PartitionedRegionName5 = "TestPartitionedRegion5"; // default
                                                                   // name

  static String repRegionName = "TestRepRegion"; // default name
  static String repRegionName2 = "TestRepRegion2"; // default name
  static String repRegionName3 = "TestRepRegion3"; // default name
  static String repRegionName4 = "TestRepRegion4"; // default name
  static String localRegionName = "TestLocalRegion"; // default name

  private static PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  public static String[] queries = new String[] {
      "select * from /" + PartitionedRegionName1 + " where ID>=0",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2 where r1.ID = r2.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2
          + " r2 where r1.ID = r2.ID AND r1.status = r2.status",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2, /"
          + PartitionedRegionName3 + " r3 where r1.ID = r2.ID and r2.ID = r3.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2, /"
          + PartitionedRegionName3 + " r3  , /" + repRegionName
          + " r4 where r1.ID = r2.ID and r2.ID = r3.ID and r3.ID = r4.ID",
      "Select * from /" + PartitionedRegionName4 + " r4 , /" + PartitionedRegionName5 + " r5 where r4.ID = r5.ID" };

  public static String[] nonColocatedQueries = new String[] {
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName4 + " r4 where r1.ID = r4.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName4 + " r4 , /"
          + PartitionedRegionName5 + " r5 where r1.ID = r42.ID and r4.ID = r5.ID" };

  public static String[] queriesForRR = new String[] { "<trace> select * from /" + repRegionName + " where ID>=0",
      "Select * from /" + repRegionName + " r1, /" + repRegionName2 + " r2 where r1.ID = r2.ID",
      "select * from /" + repRegionName3 + " where ID>=0", "select * from /" + repRegionName4};

  public QueryDataDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    initManagement(false);
    createRegionsInNodes();
    fillValuesInRegions();

  }

  public void tearDown2() throws Exception {
    super.tearDown2();

  }

  /**
   * This function puts portfolio objects into the created Region (PR or Local)
   * *
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRPuts(final String regionName,
      final Object[] portfolio, final int from, final int to) {
    SerializableRunnable puts = new CacheSerializableRunnable("Region Puts") {
      @Override
      public void run2() throws CacheException {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(regionName);
        for (int j = from; j < to; j++)
          region.put(new Integer(j), portfolio[j]);
        getLogWriter()
            .info(
                "PRQueryDUnitHelper#getCacheSerializableRunnableForPRPuts: Inserted Portfolio data on Region "
                    + regionName);
      }
    };
    return (CacheSerializableRunnable) puts;
  }

  /**
   * This function puts PDX objects into the created Region (REPLICATED) *
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPDXPuts(final String regionName) {
    SerializableRunnable puts = new CacheSerializableRunnable("Region Puts") {
      @Override
      public void run2() throws CacheException {
        putPdxInstances(regionName);

      }
    };
    return (CacheSerializableRunnable) puts;
  }
  
  /**
   * This function puts big collections to created Region (REPLICATED) *
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForBigCollPuts(final String regionName) {
    SerializableRunnable bigPuts = new CacheSerializableRunnable("Big Coll Puts") {
      @Override
      public void run2() throws CacheException {
        putBigInstances(regionName);

      }
    };
    return (CacheSerializableRunnable) bigPuts;
  }

  public void fillValuesInRegions() {
    // Create common Portflios and NewPortfolios
    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);

    // Fill local region
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(localRegionName, portfolio, cnt, cntDest));

    // Fill replicated region
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(repRegionName, portfolio, cnt, cntDest));
    managedNode2.invoke(getCacheSerializableRunnableForPRPuts(repRegionName2, portfolio, cnt, cntDest));

    // Fill Partition Region
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName1, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName2, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName3, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName4, portfolio, cnt, cntDest));
    managedNode1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName5, portfolio, cnt, cntDest));

    managedNode1.invoke(getCacheSerializableRunnableForPDXPuts(repRegionName3));
    managedNode1.invoke(getCacheSerializableRunnableForBigCollPuts(repRegionName4));
  }

  public void putPdxInstances(String regionName) throws CacheException {
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    Region r = getCache().getRegion(regionName);
    pf.writeInt("ID", 111);
    pf.writeString("status", "active");
    pf.writeString("secId", "IBM");
    PdxInstance pi = pf.create();
    r.put("IBM", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 222);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "YHOO");
    pi = pf.create();
    r.put("YHOO", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 333);
    pf.writeString("status", "active");
    pf.writeString("secId", "GOOGL");
    pi = pf.create();
    r.put("GOOGL", pi);

    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeInt("ID", 111);
    pf.writeString("status", "inactive");
    pf.writeString("secId", "VMW");
    pi = pf.create();
    r.put("VMW", pi);
  }
  
  public void putBigInstances(String regionName) throws CacheException {
    Region r = getCache().getRegion(regionName);
    List<String> bigColl1 = new ArrayList<String>();
    List<String> bigColl2 = new ArrayList<String>();
    List<String> bigColl3 = new ArrayList<String>();
    for(int i = 0; i< 200 ; i++){
      bigColl1.add("BigColl_1_ElemenNo_"+i);
      bigColl2.add("BigColl_2_ElemenNo_"+i);
      bigColl3.add("BigColl_3_ElemenNo_"+i);
    }
    r.put("BigColl_1", bigColl1);
    r.put("BigColl_2", bigColl2);
    r.put("BigColl_3", bigColl3);
    
    
  }

  private void createRegionsInNodes() {

    // Create local Region on servers
    managedNode1.invoke(QueryUsingFunctionContextDUnitTest.class, "createLocalRegion");

    // Create ReplicatedRegion on servers
    managedNode1.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    managedNode2.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    managedNode3.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    try {
      this.createDistributedRegion(managedNode2, repRegionName2);
      this.createDistributedRegion(managedNode1, repRegionName3);
      this.createDistributedRegion(managedNode1, repRegionName4);
    } catch (Exception e1) {
      fail("Test Failed while creating region " + e1.getMessage());
    }

    // Create two colocated PartitionedRegions On Servers.
    managedNode1.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");
    managedNode2.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");
    managedNode3.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");

    this.managingNode.invoke(new SerializableRunnable("Wait for all Region Proxies to get replicated") {

      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

        try {
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName1, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName2, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName3, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName4, 3);
          MBeanUtil.getDistributedRegionMbean("/" + PartitionedRegionName5, 3);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName, 3);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName2, 1);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName3, 1);
          MBeanUtil.getDistributedRegionMbean("/" + repRegionName4, 1);
        } catch (Exception e) {
          fail("Region proxies not replicated in time");
        }
      }
    });

  }

  public void testQueryOnPartitionedRegion() throws Exception {

    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    final DistributedMember member3 = getMember(managedNode3);
    this.managingNode.invoke(new SerializableRunnable("testQueryOnPartitionedRegion") {

      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();

        assertNotNull(bean);

        try {
          for (int i = 0; i < queries.length; i++) {
            String jsonString = null;
            if (i == 0) {
              jsonString = bean.queryData(queries[i], null, 10);
              if (jsonString.contains("result") && !jsonString.contains("No Data Found")) {
                GfJsonObject gfJsonObj = new GfJsonObject(jsonString);
                getLogWriter().info("testQueryOnPartitionedRegion" + queries[i] + " is = " + jsonString);
              } else {
                fail("Query On Cluster should have result");
              }
            } else {
              jsonString = bean.queryData(queries[i], member1.getId(), 10);
              if (jsonString.contains("member")) {
                GfJsonObject gfJsonObj = new GfJsonObject(jsonString);
                getLogWriter().info("testQueryOnPartitionedRegion" + queries[i] + " is = " + jsonString);
              } else {
                fail("Query On Member should have member");
              }
            }

            

          }
        } catch (GfJsonException e) {
          fail(e.getMessage());
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    });
  }

  public void testQueryOnReplicatedRegion() throws Exception {

    this.managingNode.invoke(new SerializableRunnable("Query Test For REPL1") {

      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          for (int i = 0; i < queriesForRR.length; i++) {
            String jsonString1 = null;
            if (i == 0) {
              jsonString1 = bean.queryData(queriesForRR[i], null, 10);
              if (jsonString1.contains("result") && !jsonString1.contains("No Data Found")) {
                GfJsonObject gfJsonObj = new GfJsonObject(jsonString1);
              } else {
                fail("Query On Cluster should have result");
              }
            } else {
              jsonString1 = bean.queryData(queriesForRR[i], null, 10);
              if (jsonString1.contains("result")) {
                GfJsonObject gfJsonObj = new GfJsonObject(jsonString1);
              } else {
                getLogWriter().info("Failed Test String" + queriesForRR[i] + " is = " + jsonString1);
                fail("Join on Replicated did not work.");
              }
            }
          }

        } catch (GfJsonException e) {
          fail(e.getMessage());
        } catch (IOException e) {
          fail(e.getMessage());
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    });
  }
  
  public void testMemberWise() throws Exception {

    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    
    this.managingNode.invoke(new SerializableRunnable("testMemberWise") {

      public void run() {
        Cache cache = getCache();
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          byte[] bytes = bean.queryDataForCompressedResult(queriesForRR[0], member1.getId() + "," + member2.getId(), 2);
          String jsonString = BeanUtilFuncs.decompress(bytes);
          GfJsonObject gfJsonObj = new GfJsonObject(jsonString);
          getLogWriter().info("testMemberWise " + queriesForRR[2] + " is = " + jsonString);

        } catch (GfJsonException e) {
          fail(e.getMessage());
        } catch (IOException e) {
          fail(e.getMessage());
        } catch (Exception e) {
          fail(e.getMessage());
        }
      }
    });
  }

  
  public void testLimit() throws Exception {
    this.managingNode.invoke(new SerializableRunnable("Test Limit") {
      public void run() {
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          String jsonString1 = null;
          byte[] bytes = bean.queryDataForCompressedResult(queriesForRR[2], null, 2);
          jsonString1 = BeanUtilFuncs.decompress(bytes);
          if (jsonString1.contains("result") && !jsonString1.contains("No Data Found")) {
            GfJsonObject gfJsonObj = new GfJsonObject(jsonString1);
            getLogWriter().info("testLimit" + queriesForRR[2] + " is = " + jsonString1);
            GfJsonArray arr = gfJsonObj.getJSONArray("result");
            assertEquals(2, arr.size());
          } else {
            fail("Query On Cluster should have result");
          }

        } catch (GfJsonException e) {
          fail(e.getMessage());
        } catch (IOException e) {
          fail(e.getMessage());
        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });
  }

  public void testErrors() throws Exception{
    
    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    final DistributedMember member3 = getMember(managedNode3);
    
    this.managingNode.invoke(new SerializableRunnable("Test Error") {
      public void run() {
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);

        try {
          Cache cache = getCache();
          try {
            byte[] bytes = bean.queryDataForCompressedResult("Select * from TestPartitionedRegion1", null, 2);
            fail("Should have thrown Exception : Region mentioned in query probably missing");
          } catch (Exception e) {
            Exception expectedException = new Exception(ManagementStrings.QUERY__MSG__INVALID_QUERY
                .toLocalizedString("Region mentioned in query probably missing /"));
            assertEquals(expectedException.getLocalizedMessage() , e.getLocalizedMessage());
          }
          
          try {
            String query = "Select * from /PartitionedRegionName9 r1, PartitionedRegionName2 r2 where r1.ID = r2.ID";
            byte[] bytes = bean.queryDataForCompressedResult(query, null, 2);
            fail("Should have thrown Exception : Cannot find regions <{0}> in any of the members");
          } catch (Exception e) {
            Exception expectedException = new Exception(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND.toLocalizedString("/PartitionedRegionName9"));
            assertEquals(expectedException.getLocalizedMessage(),e.getLocalizedMessage());
          }
          
          final String testTemp = "testTemp";
          try {
            RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
            
            rf.create(testTemp);
            String query = "Select * from /"+testTemp;
            
            bean.queryDataForCompressedResult(query, member1.getId(), 2);
            fail("Should have thrown Exception : Cannot find regions <{0}> in specified members");
          } catch (Exception e) {
            e.printStackTrace();
            Exception expectedException = new Exception(ManagementStrings.QUERY__MSG__REGIONS_NOT_FOUND_ON_MEMBERS
                .toLocalizedString("/"+testTemp));
            assertEquals(expectedException.getLocalizedMessage(),e.getLocalizedMessage());
          }
          
          try {
            String query = queries[1];            
            bean.queryDataForCompressedResult(query,null, 2);
            fail("Should have thrown Exception : Join operation can only be executed on targeted members, please give member input");
          } catch (Exception e) {
            e.printStackTrace();
            Exception expectedException = new Exception(ManagementStrings.QUERY__MSG__JOIN_OP_EX.toLocalizedString());
            assertEquals(expectedException.getLocalizedMessage(),e.getLocalizedMessage());
          }

        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });
  }
  
 public void testNormalRegions() throws Exception{
    
    final DistributedMember member1 = getMember(managedNode1);
    final DistributedMember member2 = getMember(managedNode2);
    final DistributedMember member3 = getMember(managedNode3);
    
    this.managingNode.invoke(new SerializableRunnable("Test Error") {
      public void run() {
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);
        final String testNormal = "testNormal";
        final String testTemp = "testTemp";
        
        final String testSNormal = "testSNormal"; // to Reverse order of regions while getting Random region in QueryDataFunction
        final String testATemp = "testATemp";
        
        try {
          Cache cache = getCache();
          RegionFactory rf = cache.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);          
          rf.create(testNormal);
          rf.create(testSNormal);
          
          
          Region region = cache.getRegion("/"+testNormal);
          assertTrue(region.getAttributes().getDataPolicy() == DataPolicy.NORMAL);
          
          RegionFactory rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE);
          rf1.create(testTemp);
          rf1.create(testATemp);
          String query1 = "Select * from /testTemp r1,/testNormal r2 where r1.ID = r2.ID";
          String query2 = "Select * from /testSNormal r1,/testATemp r2 where r1.ID = r2.ID";
          String query3 = "Select * from /testSNormal";
          
          try {
           
            bean.queryDataForCompressedResult(query1,null, 2);
            bean.queryDataForCompressedResult(query2,null, 2);
            bean.queryDataForCompressedResult(query3,null, 2);
          } catch (Exception e) {
            e.printStackTrace();
          }

        } catch (Exception e) {
          fail(e.getMessage());
        }

      }
    });
  }
 
}

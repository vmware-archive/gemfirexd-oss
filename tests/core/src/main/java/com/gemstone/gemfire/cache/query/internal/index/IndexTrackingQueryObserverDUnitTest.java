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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver.IndexInfo;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexTrackingQueryObserverJUnitTest.IndexTrackingTestHook;
import com.gemstone.gemfire.cache30.CacheTestCase;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * @author shobhit
 *
 */
public class IndexTrackingQueryObserverDUnitTest extends CacheTestCase {

  private final int NUM_BKTS = 10;
  private static final String queryStr = "select * from /portfolio where ID >= 0";
  protected static final int TOTAL_OBJECTS = 1000;

  public IndexTrackingQueryObserverDUnitTest(String name) {
    super(name);
  }

  //Commenting the test for #47768
  public void testIndexInfoOnRemotePartitionedRegion() throws Exception {
    /*
    final Host host = Host.getHost(0);
    VM ds0 = host.getVM(0);
    VM ds1 = host.getVM(1);

    createPR(ds0);
    createPR(ds1);

    createQueryIndex(ds0, true);
    createQueryIndex(ds1, false);

    // Populate region.
    initializeRegion(ds0);

    // Check query verbose on both VMs
    AsyncInvocation async1 = verifyQueryVerboseData(ds0, TOTAL_OBJECTS / 2);
    AsyncInvocation async2 = verifyQueryVerboseData(ds1, TOTAL_OBJECTS / 2);

    // Run query on one vm only.
    runQuery(ds1);

    async1.join();
    async2.join();

    ds0.invoke(new SerializableRunnable("Test Query Verbose Data") {
      public void run() {
        // Reset the observer.
        QueryObserverHolder.reset();
      }
    });
    ds1.invoke(new SerializableRunnable("Test Query Verbose Data") {

      public void run() {
        // Reset the observer.
        QueryObserverHolder.reset();
      }
    });

    if (async1.exceptionOccurred()) {
      fail("", async1.getException());
    }

    if (async1.exceptionOccurred()) {
      fail("", async1.getException());
    }
    */
  }

  /**
   * CReates a PR on a VM with NUM_BKTS buckets.
   * @param vm
   */
  private void createPR(VM vm){

    SerializableRunnable createDS = new SerializableRunnable("Creating PR Datastore") {
      
      public void run() {

        System.setProperty("gemfire.Query.VERBOSE", "true");
        QueryObserver observer = QueryObserverHolder.setInstance(new IndexTrackingQueryObserver());
        
        //Create Partition Region
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(NUM_BKTS);
        AttributesFactory af = new AttributesFactory();
        af.setPartitionAttributes(paf.create());

        Region region = getCache().createRegion("portfolio", af.create());
        
      }
    };
    
    vm.invoke(createDS);
  }
  
  private void initializeRegion(VM vm) {
    
    SerializableRunnable initRegion = new SerializableRunnable("Initialize the PR") {
      
      public void run() {

        Region region = getCache().getRegion("portfolio");
        
        if (region.size() == 0) {
          for (int i = 0; i < TOTAL_OBJECTS; i++) {
            region.put(Integer.toString(i), new Portfolio(i, i));
          }
        }
        assertEquals(TOTAL_OBJECTS, region.size());
        
      }
    };
    vm.invoke(initRegion);
  }
  
  private void createQueryIndex(VM vm, final boolean create){

    SerializableRunnable createIndex = new SerializableRunnable("Create index on PR") {
      
      public void run() {

      //Query VERBOSE has to be true for the test
        assertEquals("true", System.getProperty("gemfire.Query.VERBOSE"));
        
        QueryService qs = getCache().getQueryService();
        
        Index keyIndex1 = null;
        try {
          if(create){
            keyIndex1 = (IndexProtocol) qs.createIndex(IndexTrackingQueryObserverJUnitTest.INDEX_NAME,
              IndexType.FUNCTIONAL, "ID", "/portfolio ");
            assertNotNull(keyIndex1);
            assertTrue(keyIndex1 instanceof PartitionedIndex);
          }
        } catch (Exception e) {
          fail("While creating Index on PR", e);
        }
        Region region = getCache().getRegion("portfolio");
        //Inject TestHook in QueryObserver before running query.
        IndexTrackingTestHook th = new IndexTrackingTestHook(region, NUM_BKTS/2);
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());
        
        ((IndexTrackingQueryObserver)observer).setTestHook(th);
      }
    };
    
    vm.invoke(createIndex);
  }
  
  private void runQuery(VM vm) {
    
    SerializableRunnable runQuery = new SerializableRunnable("Run Query on PR") {
      
      public void run() {
        
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryStr);
        Region region = getCache().getRegion("portfolio");

        SelectResults results = null;
        try {
          results = (SelectResults) query.execute();
        } catch (Exception e) {
          fail("While running query on PR", e);
        }

        // The query should return all elements in region.
        assertEquals(region.size(), results.size());
      }
    };
    vm.invoke(runQuery);
  }
  
  private AsyncInvocation verifyQueryVerboseData(VM vm, final int results) {
    
    SerializableRunnable testQueryVerbose = new SerializableRunnable("Test Query Verbose Data") {
      
      public void run() {
        //Query VERBOSE has to be true for the test
        assertEquals("true", System.getProperty("gemfire.Query.VERBOSE"));
      
        // Get TestHook from observer.
        QueryObserver observer = QueryObserverHolder.getInstance();
        assertTrue(QueryObserverHolder.hasObserver());

        final IndexTrackingTestHook th = (IndexTrackingTestHook) ((IndexTrackingQueryObserver) observer)
            .getTestHook();

        waitForCriterion(new WaitCriterion() {

          public boolean done() {
            return th.getRegionMap() != null;
          }

          public String description() {
            return null;
          }
        }, 5000, 200, false);

        IndexInfo regionMap = th.getRegionMap();

        Collection<Integer> rslts = regionMap.getResults().values();
        int totalResults = 0;
        for (Integer i : rslts) {
          totalResults += i.intValue();
        }
        
        getLogWriter().fine("Index Info result size is " + totalResults);
        assertEquals(results, totalResults);
      }
    };
    AsyncInvocation asyncInv = vm.invokeAsync(testQueryVerbose);
    return asyncInv;
  }
}

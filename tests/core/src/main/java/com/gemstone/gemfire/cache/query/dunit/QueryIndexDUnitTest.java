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
 * QueryIndexDUnitTest.java
 *
 * Created on June 13, 2005, 4:44 PM
 */

package com.gemstone.gemfire.cache.query.dunit;

/**
 *
 * @author kdeshpan
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import util.CacheUtil;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.functional.StructSetOrResultsSet;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.distributed.DistributedSystem;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

public class QueryIndexDUnitTest extends DistributedTestCase {

  /** Creates a new instance of QueryIndexDUnitTest */
  public QueryIndexDUnitTest(String name) {
    super(name);
  }

  public void setUp(){
    int hostCount = Host.getHostCount();
    hydra.Log.getLogWriter().info( "Number of hosts = " + hostCount);
    for (int i = 0; i < hostCount; i++) {
      Host host = Host.getHost(i);
      //            int systemCount = host.getSystemCount();
      int vmCount = host.getVMCount();
      hydra.Log.getLogWriter().info( "Host number :" + i + "contains " 
          + vmCount + "Vms" );
      for (int j = 0; j < vmCount; j++) {
        VM vm = host.getVM(j);
        vm.invoke( QueryIndexDUnitTest.class, "createRegion" );
      }
    }
  }

  public void tearDown2(){
    int hostCount = Host.getHostCount();
    hydra.Log.getLogWriter().info( "Number of hosts = " + hostCount);
    for (int i = 0; i < hostCount; i++) {
      Host host = Host.getHost(i);
      //            int systemCount = host.getSystemCount();
      int vmCount = host.getVMCount();
      hydra.Log.getLogWriter().info( "Host number :" + i + "contains " 
          + vmCount + "Vms" );
      for (int j = 0; j < vmCount; j++) {
        VM vm = host.getVM(j);
        vm.invoke( QueryIndexDUnitTest.class, "destroyRegion" );
        vm.invoke( QueryIndexDUnitTest.class, "closeCache" );
      }
    }
  }

  public void testIndexCreationAndUpdates() throws InterruptedException {
    Object [] intArr = new Object[2];
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    AsyncInvocation ai1 = null;
    AsyncInvocation ai2 = null;

    vm0.invoke(QueryIndexDUnitTest.class, "createIndex");
    vm0.invoke(QueryIndexDUnitTest.class, "validateIndexUsage");
    VM vm3 = host.getVM(3);


    vm0.invoke(QueryIndexDUnitTest.class, "removeIndex");
    ai1 = vm3.invokeAsync(QueryIndexDUnitTest.class,"doPut");
    ai2 = vm0.invokeAsync(QueryIndexDUnitTest.class,
    "createIndex");
    DistributedTestCase.join(ai1, 30 * 1000, getLogWriter());
    DistributedTestCase.join(ai2, 30 * 1000, getLogWriter());
    intArr[0] =  new Integer(3);
    intArr[1] =  new Integer(2);
    vm0.invoke(QueryIndexDUnitTest.class,"validateIndexUpdate", intArr);


    vm0.invoke(QueryIndexDUnitTest.class, "removeIndex");
    ai1 = vm0.invokeAsync(QueryIndexDUnitTest.class,"doDestroy");
    ai2 = vm0.invokeAsync(QueryIndexDUnitTest.class,"createIndex");
    DistributedTestCase.join(ai1, 30 * 1000, getLogWriter());
    DistributedTestCase.join(ai2, 30 * 1000, getLogWriter());
    intArr[0] =  new Integer(1);
    intArr[1] =  new Integer(1);
    vm0.invoke(QueryIndexDUnitTest.class,"validateIndexUpdate", intArr);


    vm0.invoke(QueryIndexDUnitTest.class, "removeIndex");
    ai1 = vm0.invokeAsync(QueryIndexDUnitTest.class,"doPut");
    ai2 = vm0.invokeAsync(QueryIndexDUnitTest.class,"createIndex");
    DistributedTestCase.join(ai1, 30 * 1000, getLogWriter());
    DistributedTestCase.join(ai2, 30 * 1000, getLogWriter());
    intArr[0] =  new Integer(3);
    intArr[1] =  new Integer(2);
    vm0.invoke(QueryIndexDUnitTest.class,"validateIndexUpdate", intArr);


    vm0.invoke(QueryIndexDUnitTest.class, "removeIndex");
    ai1 = vm3.invokeAsync(QueryIndexDUnitTest.class,"doDestroy");
    ai2 = vm0.invokeAsync(QueryIndexDUnitTest.class,"createIndex");
    DistributedTestCase.join(ai1, 30 * 1000, getLogWriter());
    DistributedTestCase.join(ai2, 30 * 1000, getLogWriter());
    intArr[0] =  new Integer(1);
    intArr[1] =  new Integer(1);
    vm0.invoke(QueryIndexDUnitTest.class,"validateIndexUpdate", intArr);

    // Test for in-place update.
    vm0.invoke(QueryIndexDUnitTest.class, "removeIndex");
    ai1 = vm0.invokeAsync(QueryIndexDUnitTest.class,"doPut");
    ai2 = vm0.invokeAsync(QueryIndexDUnitTest.class,"createIndex");
    DistributedTestCase.join(ai1, 30 * 1000, getLogWriter());
    DistributedTestCase.join(ai2, 30 * 1000, getLogWriter());
    intArr[0] =  new Integer(3);
    intArr[1] =  new Integer(2);
    vm0.invoke(QueryIndexDUnitTest.class,"validateIndexUpdate", intArr);

    try {Thread.sleep(2000);} catch (Exception ex){}
    // Do an in-place update of the region entries.
    // This will set the Portfolio objects status to "active".
    String[] str = {"To get Update in synch thread."}; // Else test was exiting before the validation could finish.
    vm0.invoke(QueryIndexDUnitTest.class, "doInPlaceUpdate", str);
    intArr[0] =  new Integer(5);
    intArr[1] =  new Integer(0);
    vm0.invoke(QueryIndexDUnitTest.class,"validateIndexUpdate", intArr);
  }

  public void testIndexCreationonOverflowRegions() throws InterruptedException {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] { 
        host.getVM(0),
        host.getVM(1),
        host.getVM(2),
        host.getVM(3),
    };
    pause(1000);
    
    // Create and load regions on all vms.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "createAndLoadOverFlowRegions", 
          new Object[]{"testOf" + "vm" + i, new Boolean(true), new Boolean(true)}); 
    }
    pause(1000);
    
    // Create index on the regions.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "createIndexOnOverFlowRegions"); 
    }
    
    // execute query.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "executeQueriesUsingIndexOnOverflowRegions");
    }
    
    // reload the regions after index creation.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "createAndLoadOverFlowRegions", 
          new Object[]{"testOf" + "vm" + i, new Boolean(false), new Boolean(true)}); 
    }
    
    // reexecute the query.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "executeQueriesUsingIndexOnOverflowRegions");
    }
  }

  public void testIndexCreationonOverflowRegionsValidateResults() throws Exception {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] { 
        host.getVM(0),
        host.getVM(1),
    };
    pause(1000);
    
    // Create and load regions on all vms.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "createAndLoadOverFlowRegions", 
          new Object[]{"testOfValid" + "vm"+i, new Boolean(true), new Boolean(false)}); 
    }
    pause(1000);
    
   vms[0].invoke(new CacheSerializableRunnable("Execute query validate results") {
      public void run2() throws CacheException {
        Cache cache = null;
        String[] regionNames = new String[] {
            "replicateOverFlowRegion",
            "replicatePersistentOverFlowRegion",
            "prOverFlowRegion",
            "prPersistentOverFlowRegion",
        };

        try {
          if ((cache = CacheUtil.getCache()) == null) {
            QueryIndexDUnitTest qidt = new QueryIndexDUnitTest("temp");
            cache = qidt.createCache();
          }
        } catch (Exception ex) {
          fail("Failed to create cache");
        }

        QueryService qs = cache.getQueryService();
        Region region = null;

        int numObjects = 10;
        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i=0; i < regionNames.length; i++){
          region = cache.getRegion(regionNames[i]);
          for (int cnt=1; cnt < numObjects; cnt++){
            region.put(new Portfolio(cnt), new Portfolio(cnt));
          } 
        }

        String[] qString = new String[] {
          "SELECT * FROM /REGION_NAME pf WHERE pf.ID = 1",
          "SELECT ID FROM /REGION_NAME pf WHERE pf.ID = 1",
          "SELECT * FROM /REGION_NAME pf WHERE pf.ID > 5",
          "SELECT ID FROM /REGION_NAME pf WHERE pf.ID > 5",
          "SELECT * FROM /REGION_NAME.keys key WHERE key.ID = 1",
          "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID = 1",
          "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > 5",
          "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID > 5",
        };
        
        // Execute Query without index.
        SelectResults[] srWithoutIndex = new SelectResults[qString.length * regionNames.length];
        String[] queryString = new String[qString.length * regionNames.length];
        
        int r = 0;
        try {
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              queryString[r] = queryStr;
              srWithoutIndex[r] = (SelectResults)query.execute();
              r++;
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Create index.
        try {
          for (int i=0; i < regionNames.length; i++){ 
            region = cache.getRegion(regionNames[i]);
            String indexName = "idIndex" + regionNames[i];
            cache.getLogger().fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
            try {
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);  
                Index i1 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "pf.ID", "/" + regionNames[i] + " pf");
              }
              indexName = "keyIdIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
                Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "key.ID", "/" + regionNames[i] + ".keys key");
              }
            } catch (IndexNameConflictException ice) {
              // Ignore. The pr may have created the index through 
              // remote index create message from peer.
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to create index", ex);
          fail("Failed to create index.");
        }

        // Execute Query with index.
        SelectResults[] srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;       
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults)query.execute();
              if(!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];
        
        for (int i=0; i < srWithIndex.length; i++){
          sr[0][0] = srWithoutIndex[i]; 
          sr[0][1] = srWithIndex[i];
          hydra.Log.getLogWriter().info("Comparing the result for the query : " + queryString[i] + 
              " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1,queryString);
        }
        
        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i=0; i < regionNames.length; i++){
          region = cache.getRegion(regionNames[i]);
          for (int cnt=1; cnt < numObjects; cnt++){
            if (cnt %2 == 0){
              region.destroy(new Portfolio(cnt));
            }
          } 
          for (int cnt=10; cnt < numObjects; cnt++){
            if (cnt %2 == 0){
              region.put(new Portfolio(cnt), new Portfolio(cnt));
            }
          } 
        }

        // Execute Query with index.
        srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;       
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults)query.execute();
              if(!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

      }
    });
  }

  public void testIndexCreationonOverflowRegionsValidateResults2() throws Exception {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] { 
        host.getVM(0),
        host.getVM(1),
    };
    pause(1000);

    // Create and load regions on all vms.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "createAndLoadOverFlowRegions", 
          new Object[]{"testOfValid2" + "vm"+i, new Boolean(true), new Boolean(false)}); 
    }
    pause(1000);

    vms[0].invoke(new CacheSerializableRunnable("Execute query validate results") {
      public void run2() throws CacheException {
        Cache cache = null;
        String[] regionNames = new String[] {
            "replicateOverFlowRegion",
            "replicatePersistentOverFlowRegion",
            "prOverFlowRegion",
            "prPersistentOverFlowRegion",
        };

        try {
          if ((cache = CacheUtil.getCache()) == null) {
            QueryIndexDUnitTest qidt = new QueryIndexDUnitTest("temp");
            cache = qidt.createCache();
          }
        } catch (Exception ex) {
          fail("Failed to create cache");
        }

        QueryService qs = cache.getQueryService();
        Region region = null;

        int numObjects = 10;
        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i=0; i < regionNames.length; i++){
          region = cache.getRegion(regionNames[i]);
          for (int cnt=1; cnt < numObjects; cnt++){
            region.put(new Portfolio(cnt), new String("XX" + cnt));
          } 
        }

        String[] qString = new String[] {
            "SELECT * FROM /REGION_NAME pf WHERE pf = 'XX1'",
            //"SELECT pf.key.ID FROM /REGION_NAME.entries pf WHERE pf.value = 'XX1'",
            "SELECT * FROM /REGION_NAME pf WHERE pf IN SET( 'XX5', 'XX6', 'XX7')",
            "SELECT * FROM /REGION_NAME.values pf WHERE pf IN SET( 'XX5', 'XX6', 'XX7')",
            //"SELECT pf.key.ID FROM /REGION_NAME.entrySet pf WHERE pf.value IN SET( 'XX5', 'XX6', 'XX7')",
            "SELECT * FROM /REGION_NAME.keys k WHERE k.ID = 1",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID = 1",
            "SELECT ID, status FROM /REGION_NAME.keys WHERE ID = 1",
            "SELECT k.ID, k.status FROM /REGION_NAME.keys k WHERE k.ID = 1 and k.status = 'active'",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > 5",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID > 5 and key.status = 'active'",
            //"SELECT key.getID() FROM /REGION_NAME.keys key WHERE key.getID() > 5 and key.status = 'active'",
        };

        // Execute Query without index.
        SelectResults[] srWithoutIndex = new SelectResults[qString.length * regionNames.length];
        String[] queryString = new String[qString.length * regionNames.length];

        int r = 0;
        try {
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              queryString[r] = queryStr;
              srWithoutIndex[r] = (SelectResults)query.execute();
              r++;
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Create index.
        String indexName = "";
        try {
          for (int i=0; i < regionNames.length; i++){ 
            region = cache.getRegion(regionNames[i]);
            indexName = "idIndex" + regionNames[i];
            cache.getLogger().fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
            try {
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);  
                Index i1 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "pf", "/" + regionNames[i] + " pf");
              }
              indexName = "valueIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);  
                Index i1 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "pf", "/" + regionNames[i] + ".values pf");
              }
              indexName = "keyIdIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
                Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "key.ID", "/" + regionNames[i] + ".keys key");
              }
              indexName = "keyIdIndex2" + regionNames[i];
              /*
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
                Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "key.getID()", "/" + regionNames[i] + ".keys key");
              }*/
              
            } catch (IndexNameConflictException ice) {
              // Ignore. The pr may have created the index through 
              // remote index create message from peer.
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to create index", ex);
          fail("Failed to create index." + indexName);
        }

        // Execute Query with index.
        SelectResults[] srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;       
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults)query.execute();
              if(!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i=0; i < srWithIndex.length; i++){
          sr[0][0] = srWithoutIndex[i]; 
          sr[0][1] = srWithIndex[i];
          hydra.Log.getLogWriter().info("Comparing the result for the query : " + queryString[i] + 
              " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1,queryString);
        }

        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i=0; i < regionNames.length; i++){
          region = cache.getRegion(regionNames[i]);
          for (int cnt=1; cnt < numObjects; cnt++){
            if (cnt %2 == 0){
              region.destroy(new Portfolio(cnt));
            }
          } 
          for (int cnt=10; cnt < numObjects; cnt++){
            if (cnt %2 == 0){
              region.put(new Portfolio(cnt), new String("XX" + cnt));
            }
          } 
        }

        // Execute Query with index.
        srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;       
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults)query.execute();
              if(!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

      }
    });
  }
  
  public void testIndexCreationonOverflowRegionsValidateResultsUseParams() throws Exception {
    Host host = Host.getHost(0);
    VM[] vms = new VM[] { 
        host.getVM(0),
        host.getVM(1),
    };
    pause(1000);

    // Create and load regions on all vms.
    for (int i=0; i < vms.length; i++){
      vms[i].invoke(QueryIndexDUnitTest.class, "createAndLoadOverFlowRegions", 
          new Object[]{"testOfValidUseParams" + "vm"+i, new Boolean(true), new Boolean(false)}); 
    }
    pause(1000);

    vms[0].invoke(new CacheSerializableRunnable("Execute query validate results") {
      public void run2() throws CacheException {
        Cache cache = null;
        String[] regionNames = new String[] {
            "replicateOverFlowRegion",
            "replicatePersistentOverFlowRegion",
            "prOverFlowRegion",
            "prPersistentOverFlowRegion",
        };

        try {
          if ((cache = CacheUtil.getCache()) == null) {
            QueryIndexDUnitTest qidt = new QueryIndexDUnitTest("temp");
            cache = qidt.createCache();
          }
        } catch (Exception ex) {
          fail("Failed to create cache");
        }

        QueryService qs = cache.getQueryService();
        Region region = null;

        int numObjects = 10;
        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i=0; i < regionNames.length; i++){
          region = cache.getRegion(regionNames[i]);
          for (int cnt=1; cnt < numObjects; cnt++){
            region.put(new Portfolio(cnt), new Integer(cnt + 100));
          } 
        }

        String[] qString = new String[] {
            "SELECT * FROM /REGION_NAME pf WHERE pf = $1",
            //"SELECT pf.key.ID FROM /REGION_NAME.entries pf WHERE pf.value = 'XX1'",
            "SELECT * FROM /REGION_NAME pf WHERE pf > $1",
            "SELECT * FROM /REGION_NAME.values pf WHERE pf < $1",
            //"SELECT pf.key.ID FROM /REGION_NAME.entrySet pf WHERE pf.value IN SET( 'XX5', 'XX6', 'XX7')",
            "SELECT * FROM /REGION_NAME.keys k WHERE k.ID = $1",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID = $1",
            "SELECT ID, status FROM /REGION_NAME.keys WHERE ID = $1",
            "SELECT k.ID, k.status FROM /REGION_NAME.keys k WHERE k.ID = $1 and k.status = $2",
            "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > $1",
            "SELECT key.ID FROM /REGION_NAME.keys key WHERE key.ID > $1 and key.status = $2",
        };

        // Execute Query without index.
        SelectResults[] srWithoutIndex = new SelectResults[qString.length * regionNames.length];
        String[] queryString = new String[qString.length * regionNames.length];

        int r = 0;
        try {
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              queryString[r] = queryStr;
              srWithoutIndex[r] = (SelectResults)query.execute(new Object[] { new Integer(5), "active"});
              r++;
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Create index.
        String indexName = "";
        try {
          for (int i=0; i < regionNames.length; i++){ 
            region = cache.getRegion(regionNames[i]);
            indexName = "idIndex" + regionNames[i];
            cache.getLogger().fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
            try {
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);  
                Index i1 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "pf", "/" + regionNames[i] + " pf");
              }
              indexName = "valueIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);  
                Index i1 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "pf", "/" + regionNames[i] + ".values pf");
              }
              indexName = "keyIdIndex" + regionNames[i];
              if (qs.getIndex(region, indexName) == null) {
                cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
                Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "key.ID", "/" + regionNames[i] + ".keys key");
              }
            } catch (IndexNameConflictException ice) {
              // Ignore. The pr may have created the index through 
              // remote index create message from peer.
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to create index", ex);
          fail("Failed to create index." + indexName);
        }

        // Execute Query with index.
        SelectResults[] srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;       
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults)query.execute(new Object[] { new Integer(5), "active"});
              if(!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
        SelectResults[][] sr = new SelectResults[1][2];

        for (int i=0; i < srWithIndex.length; i++){
          sr[0][0] = srWithoutIndex[i]; 
          sr[0][1] = srWithIndex[i];
          hydra.Log.getLogWriter().info("Comparing the result for the query : " + queryString[i] + 
              " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1,queryString);
        }

        // Update the region and re-execute the query.
        // The index should get updated accordingly.
        for (int i=0; i < regionNames.length; i++){
          region = cache.getRegion(regionNames[i]);
          for (int cnt=1; cnt < numObjects; cnt++){
            if (cnt %2 == 0){
              region.destroy(new Portfolio(cnt));
            }
          } 
          // Add destroyed entries
          for (int cnt=1; cnt < numObjects; cnt++){
            if (cnt %2 == 0){
              region.put(new Portfolio(cnt), new Integer(cnt + 100));
            }
          } 
        }

        // Execute Query with index.
        srWithIndex = new SelectResults[qString.length * regionNames.length];
        try {
          r = 0;       
          for (int q=0; q < qString.length; q++){
            for (int i=0; i < regionNames.length; i++){
              QueryObserverImpl observer = new QueryObserverImpl();
              QueryObserverHolder.setInstance(observer);
              String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
              Query query = qs.newQuery(queryStr);
              srWithIndex[r++] = (SelectResults)query.execute(new Object[] { new Integer(5), "active"});
              if(!observer.isIndexesUsed) {
                fail("Index not used for query. " + queryStr);
              }
            }
          }
        } catch (Exception ex) {
          hydra.Log.getLogWriter().info("Failed to Execute query", ex);
          fail("Failed to Execute query.");
        }

        // Compare results with and without index.
        for (int i=0; i < srWithIndex.length; i++){
          sr[0][0] = srWithoutIndex[i]; 
          sr[0][1] = srWithIndex[i];
          hydra.Log.getLogWriter().info("Comparing the result for the query : " + queryString[i] + 
              " Index in ResultSet is: " + i);
          ssORrs.CompareQueryResultsWithoutAndWithIndexes(sr, 1,queryString);
        }
      }
    });
  }
  
  public static void doDestroy() {
    Region region = CacheUtil.getRegion("portfolios");
    if(region == null) {
      hydra.Log.getLogWriter().info("REGION IS NULL");
    }
    try {
      region.destroy("2");
      region.destroy("3");
      region.destroy("4");
    } catch(Exception e) {
      fail("Caught exception while trying to do put operation", e);
    }
  }

  public static void validateIndexUpdate(Integer a, Integer b) {
    QueryService qs = CacheUtil.getQueryService();
    Query q = qs.newQuery(
        "SELECT DISTINCT * FROM /portfolios where status = 'active'");
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    Object r;
    try {
      r = q.execute();
      int actual = ((Collection)r).size();
      if (actual != a.intValue()){
        fail("Active NOT of the expected size, found " + actual
            + ", expected " + a.intValue());
      }
    } catch (Exception e) {
      fail("Caught exception while trying to query", e);
    }
    if(!observer.isIndexesUsed) {
      fail("Index not used for query");
    }

    q = qs.newQuery(
    "SELECT DISTINCT * FROM /portfolios where status = 'inactive'");
    observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);

    try {
      r = q.execute();
      int actual = ((Collection)r).size();
      if (actual != b.intValue()){
        fail("Inactive NOT of the expected size, found " + actual
            + ", expected " + b.intValue());
      }
    } catch (Exception e) {
      fail("Caught exception while trying to query", e);
    }
    if(!observer.isIndexesUsed) {
      fail("Index not used for query");
    }
  }

  public static void doPut() {
    Region region = CacheUtil.getRegion("portfolios");
    if(region == null) {
      hydra.Log.getLogWriter().info("REGION IS NULL");
    }
    try {
      region.put("1",new Portfolio(1));
      region.put("2",new Portfolio(2));
      region.put("3",new Portfolio(3));
      region.put("4",new Portfolio(4));
    } catch(Exception e) {
      fail("Caught exception while trying to do put operation", e);
    }
  }

  public static void doInPlaceUpdate(String str) {
    Region region = CacheUtil.getRegion("portfolios");
    Portfolio p = null;
    if(region == null) {
      hydra.Log.getLogWriter().info("REGION IS NULL");
    }
    try {
      for (int i=0; i <= 4; i++) {

        p = (Portfolio)region.get("" + i);
        p.status = "active";
        region.put("" + i, p);
      }
    } catch(Exception e) {
      fail("Caught exception while trying to do put operation", e);
    }
  }

  public static void validateIndexUsage() {
    QueryService qs = CacheUtil.getQueryService();
    Query q = qs.newQuery(
        "SELECT DISTINCT * FROM /portfolios where status = 'active'");
    QueryObserverImpl observer = new QueryObserverImpl();
    QueryObserverHolder.setInstance(observer);
    Object r;
    try {
      r = q.execute();
      int actual = ((Collection)r).size();
      if (actual != 1){
        fail("Active NOT of the expected size, found " + actual
            + ", expected 1");
      }
    } catch (Exception e) {
      fail("Caught exception while trying to query", e);
    }
    if(!observer.isIndexesUsed) {
      fail("Index not used for query");
    }
  }

  public static void createIndex() {
    Region region = CacheUtil.getRegion("portfolios");
    if (region == null) {
      hydra.Log.getLogWriter().info("The region is not created properly");
    }
    else {
      if (!region.isDestroyed()) {
        hydra.Log.getLogWriter().info(
            "Obtained the region with name :" + "portfolios");
        QueryService qs = CacheUtil.getQueryService();
        if (qs != null) {
          try {
            qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
            "/portfolios");
            hydra.Log.getLogWriter().info(
            "Index statusIndex Created successfully");
          }
          catch (IndexNameConflictException e) {
            fail("Caught IndexNameConflictException", e);
          }
          catch (IndexExistsException e) {
            fail("Caught IndexExistsException", e);
          }
          catch (QueryException e) {
            fail("Caught exception while trying to create index", e);
          }
        }
        else {
          fail("Hey !!!!! Could not obtain QueryService for the cache ");
        }
      }
      else {
        fail(
            "Hey !!!!! Region.isDestroyed() returned true for region : "
            + "/portfolios");
      }
    }
  }
  private Cache createCache () throws Exception{
    Cache cache = null;
    DistributedSystem ds = getSystem();
    try {
      cache = CacheFactory.create( ds );
      if(cache == null) {
        fail( "CacheFactory.create() returned null ");
      }
      CacheUtil.setCache(cache);
    } catch( Exception e ) {
      String s = "Unable to create cache using: " + ds;
      fail(s, e );
    }
    return cache;
  }

  public static void createAndLoadOverFlowRegions(String vmName, final Boolean createRegions, final Boolean loadRegions) throws Exception {
    Cache cache = null;
    String[] regionNames = new String[] {
        "replicateOverFlowRegion",
        "replicatePersistentOverFlowRegion",
        "prOverFlowRegion",
        "prPersistentOverFlowRegion",
    };

    if ((cache = CacheUtil.getCache()) == null) {
      QueryIndexDUnitTest qidt = new QueryIndexDUnitTest("temp");
      cache = qidt.createCache();
    }
    
    hydra.Log.getLogWriter().info("CreateAndLoadOverFlowRegions() with vmName " + vmName 
        + " createRegions: " + createRegions + " And LoadRegions: " + loadRegions);
    
    if (createRegions.booleanValue()) {
      for (int i=0; i < regionNames.length; i++){
        hydra.Log.getLogWriter().info("Started creating region :" + regionNames[i]);
        String diskStore = regionNames[i] + vmName + "DiskStore";
        hydra.Log.getLogWriter().info("Setting disk store to: " + diskStore);
        cache.createDiskStoreFactory().create(diskStore);
        try {
          AttributesFactory attributesFactory = new AttributesFactory();
          //attributesFactory.setValueConstraint(Portfolio.class);
          attributesFactory.setDiskStoreName(diskStore);
          attributesFactory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
              10, EvictionAction.OVERFLOW_TO_DISK));
          if (i == 0){
            attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
          } else if(i == 1){
            attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          } else if (i == 2){
            attributesFactory.setDataPolicy(DataPolicy.PARTITION);
          } else {
            attributesFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          }
          cache.createRegion(regionNames[i], attributesFactory.create());
          hydra.Log.getLogWriter().info("Completed creating region :" + regionNames[i]);
        }
        catch (Exception e) {
          fail("Could not create region" + regionNames[i], e);
        }
      }
    }
    Region region = null;
    int numObjects = 50;
    if (loadRegions.booleanValue()) {      
      for (int i=0; i < regionNames.length; i++){
        region = cache.getRegion(regionNames[i]);
        // If its just load, try destroy some entries and reload.
        if (!createRegions.booleanValue()){
          hydra.Log.getLogWriter().info("Started destroying region entries:" + regionNames[i]);
          for (int cnt=0; cnt < numObjects/3; cnt++){
            region.destroy(new Portfolio(cnt * 2));
          } 
        }
        
        hydra.Log.getLogWriter().info("Started Loading region :" + regionNames[i]);
        for (int cnt=0; cnt < numObjects; cnt++){
          region.put(new Portfolio(cnt), new Portfolio(cnt));
        }
        hydra.Log.getLogWriter().info("Completed loading region :" + regionNames[i]);
      }
    }    
  }

  public static void createIndexOnOverFlowRegions() throws Exception {
    Cache cache = null;
    String[] regionNames = new String[] {
        "replicateOverFlowRegion",
        "replicatePersistentOverFlowRegion",
        "prOverFlowRegion",
        "prPersistentOverFlowRegion",
    };

    if ((cache = CacheUtil.getCache()) == null) {
      QueryIndexDUnitTest qidt = new QueryIndexDUnitTest("temp");
      cache = qidt.createCache();
    }

    cache.getLogger().fine("createIndexOnOverFlowRegions()");
    
    QueryService qs = cache.getQueryService();
    Region region = null;
    for (int i=0; i < regionNames.length; i++){ 
      region = cache.getRegion(regionNames[i]);
      String indexName = "idIndex" + regionNames[i];
      cache.getLogger().fine("createIndexOnOverFlowRegions() checking for index: " + indexName);
      try {
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);  
          Index i1 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "pf.ID", "/" + regionNames[i] + " pf");
        }
        indexName = "keyIdIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "key.ID", "/" + regionNames[i] + ".keys key");
        }
        indexName = "entryIdIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "entry.value.ID", "/" + regionNames[i] + ".entries entry");
        }
        indexName = "entryMethodIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "entry.getValue().getID()", "/" + regionNames[i] + ".entries entry");
        }
        indexName = "entryMethodWithArgIndex" + regionNames[i];
        if (qs.getIndex(region, indexName) == null) {
          cache.getLogger().fine("createIndexOnOverFlowRegions() Index doesn't exist, creating index: " + indexName);
          Index i2 = qs.createIndex(indexName, IndexType.FUNCTIONAL, "entry.getValue().boolFunction('active')", "/" + regionNames[i] + ".entries entry");
        }
      } catch (IndexNameConflictException ice) {
        // Ignore. The pr may have created the index through 
        // remote index create message from peer.
      }
    }
  }

  public static void executeQueriesUsingIndexOnOverflowRegions() throws Exception {
    Cache cache = null;
    String[] regionNames = new String[] {
        "replicateOverFlowRegion",
        "replicatePersistentOverFlowRegion",
        "prOverFlowRegion",
        "prPersistentOverFlowRegion",
    };

    if ((cache = CacheUtil.getCache()) == null) {
      QueryIndexDUnitTest qidt = new QueryIndexDUnitTest("temp");
      cache = qidt.createCache();
    }

    QueryService qs = cache.getQueryService();
    Region region = null;

    String[] qString = new String[] {
      "SELECT * FROM /REGION_NAME pf WHERE pf.ID = 1",
      "SELECT ID FROM /REGION_NAME pf WHERE pf.ID = 1",
      "SELECT * FROM /REGION_NAME pf WHERE pf.ID > 10",
      "SELECT ID FROM /REGION_NAME pf WHERE pf.ID > 10",
      "SELECT * FROM /REGION_NAME.keys key WHERE key.ID = 1",
      "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID = 1",
      "SELECT * FROM /REGION_NAME.keys key WHERE key.ID > 10",
      "SELECT ID FROM /REGION_NAME.keys key WHERE key.ID > 10",
      "SELECT entry.value FROM /REGION_NAME.entries entry WHERE entry.value.ID = 1",
      "SELECT entry.key FROM /REGION_NAME.entries entry WHERE entry.value.ID > 10",
      "SELECT entry.getValue() FROM /REGION_NAME.entries entry WHERE entry.getValue().getID() = 1",
      "SELECT entry.getKey() FROM /REGION_NAME.entries entry WHERE entry.getValue().getID() > 10",
      "SELECT entry.getValue() FROM /REGION_NAME.entries entry WHERE entry.getValue().boolFunction('active') = false",
    };
    
    for (int q=0; q < qString.length; q++){
      for (int i=0; i < regionNames.length; i++){
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        String queryStr = qString[q].replace("REGION_NAME", regionNames[i]);
        Query query = qs.newQuery(queryStr);
        SelectResults results = (SelectResults)query.execute();
        hydra.Log.getLogWriter().info("Executed query :" + queryStr + 
            " Result size: " + results.asList().size());
        if(!observer.isIndexesUsed) {
          fail("Index not used for query. " + queryStr);
        }
      }
    }
  }

  
  public static synchronized void createRegion() throws Exception {
    Cache cache = null;

    if ((cache = CacheUtil.getCache()) == null) {
      QueryIndexDUnitTest qidt = new QueryIndexDUnitTest("temp");
      cache = qidt.createCache();
    }

    Region region = CacheUtil.getRegion("portfolios");
    if (region == null) {
      try {
        AttributesFactory attributesFactory = new AttributesFactory();
        attributesFactory.setValueConstraint(Portfolio.class);
        attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
        attributesFactory.setIndexMaintenanceSynchronous(true);
        attributesFactory.setScope(Scope.GLOBAL);
        RegionAttributes regionAttributes = attributesFactory
        .create();
        region = cache.createRegion("portfolios", regionAttributes);
        region.put("0", new Portfolio(0));
      }
      catch (Exception e) {
        fail("Could not create region", e);
      }
    }
  }

  public static synchronized void destroyRegion() {
    try{
      Region region = CacheUtil.getRegion("portfolios");
      if(region != null) {
        region.destroyRegion();
      }
    } catch (Exception e) {
      fail("Could not destroy region", e);
    }
  }

  public static void closeCache() {
    try {
      CacheUtil.closeCache();

    } catch (Exception e) {
      fail("Could not close cache", e);
    }
  }

  public static void removeIndex() {
    Region region = CacheUtil.getRegion("portfolios");
    if (region == null) {
      fail("The region is not created properly");
    }
    else {
      hydra.Log.getLogWriter().info(
          "Obtained the region with name :" + "portfolios");
      QueryService qs = CacheUtil.getQueryService();
      if (qs != null) {
        try {
          Collection indexes = qs.getIndexes(region);
          if (indexes == null) {
            return; // no IndexManager defined
          }
          if (indexes.size() == 0) {
            return; // no indexes defined
          }
          Iterator iter = indexes.iterator();
          if (iter.hasNext()) {
            Index idx = (Index)(iter.next());
            String name = idx.getName();
            qs.removeIndex(idx);
            hydra.Log.getLogWriter().info(
                "Index " + name + " removed successfully");
          }
        }
        catch (Exception e) {
          fail("Caught exception while trying to remove index", e);
        }
      }
      else {
        fail("Hey !!!!! Could not obtain QueryService for the cache ");
      }

    }
  }

  public static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }
}


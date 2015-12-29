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
package com.gemstone.gemfire.internal.cache;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.cache.partitioned.QueryMessage;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * @author dsmith
 *
 */
public class PartitionedRegionQueryDUnitTest extends CacheTestCase {
  
  /**
   * @param name
   */
  public PartitionedRegionQueryDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }
  
  private static final AtomicReference<RebalanceResults> rebalanceResults = new AtomicReference<RebalanceResults>();

  /**
   * Test of bug 43102.
   * 1. Buckets are created on several nodes
   * 2. A query is started
   * 3. While the query is executing, several buckets are
   * moved.
   */
  public void testRebalanceDuringQueryEvaluation() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createAccessor(vm0);
    
    createPR(vm1);
    
    createBuckets(vm1);
    
    createPR(vm2);
    
    //Add a listener that will trigger a rebalance
    //as soon as the query arrives on this node.
    vm1.invoke(new SerializableRunnable("add listener") {
      
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof QueryMessage) {
              RebalanceOperation rebalance = getCache().getResourceManager().createRebalanceFactory().start();
              //wait for the rebalance
              try {
                rebalanceResults.compareAndSet(null, rebalance.getResults());
              } catch (CancellationException e) {
                //ignore
              } catch (InterruptedException e) {
              //ignore
              }
            }
          }
        });
        
      }
    });
    
    executeQuery(vm0);
    
    vm1.invoke(new SerializableRunnable("check rebalance happened") {
      
      public void run() {
        assertNotNull(rebalanceResults.get());
        assertEquals(5, rebalanceResults.get().getTotalBucketTransfersCompleted());
      }
    });
  }

  private void executeQuery(VM vm0) {
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        Query query = cache.getQueryService().newQuery("select * from /region r where r > 0");
        try {
          SelectResults results = (SelectResults) query.execute();
          assertEquals(new HashSet(Arrays.asList(new Integer[] { 1, 2, 3 ,4, 5, 6, 7, 8, 9 })), results.asSet());
        } catch (Exception e) {
          fail("Bad query", e);
        }
      }
    });

  }

  private void createBuckets(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        for(int i =0; i < 10; i++) {
          region.put(i, i);
        }
      }
    });
    
  }

  private void createPR(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {
      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(paf.create()).create("region");
      }
    });
  }

  private void createAccessor(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {
      
      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        paf.setLocalMaxMemory(0);
        cache.createRegionFactory(RegionShortcut.PARTITION_PROXY)
          .setPartitionAttributes(paf.create())
          .create("region");
      }

    });
  }

}

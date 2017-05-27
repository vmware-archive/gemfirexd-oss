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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author Asif
 *
 */
public class AsynchIndexMaintenanceTest extends TestCase {
  private QueryService qs;

  protected Region region;

  protected boolean indexUsed = false;
  protected volatile boolean exceptionOccured = false; 

  private Set idSet ;

  private void init() {
    idSet = new HashSet();
    try {
      CacheUtils.startCache();
      Cache cache = CacheUtils.getCache();
      region = CacheUtils.createRegion("portfolio", Portfolio.class, false);      
      
      qs = cache.getQueryService();

    }
    catch (Exception e) {
      e.printStackTrace();
    }

  }

  public AsynchIndexMaintenanceTest(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
    init();
  }

  protected void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(AsynchIndexMaintenanceTest.class);
    return suite;
  }

  private int getIndexSize(Index ri) {
    if (ri instanceof RangeIndex){
      return ((RangeIndex)ri).valueToEntriesMap.size();
    } else {
      return ((CompactRangeIndex)ri).getIndexStorage().size();
    }    
  }

  public void testIndexMaintenanceBasedOnThreshhold() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "50");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "0");
    final Index ri = qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    for( int i=0; i< 49; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    //assertEquals(0, getIndexSize(ri));
    region.put("50", new Portfolio(50));
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == 50);
      }
      public String description() {
        return "valueToEntriesMap never became 50";
      }
    };
    DistributedTestBase.waitForCriterion(ev, 3000, 200, true);
  }
  
  public void testIndexMaintenanceBasedOnTimeInterval() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "-1");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "10000");
    final Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    
    final int size = 5;
    for( int i=0; i<size ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    

    //assertEquals(0, getIndexSize(ri));

    WaitCriterion evSize = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == size);
      }
      public String description() {
        return "valueToEntriesMap never became size :" + size;
      }
    };

    DistributedTestBase.waitForCriterion(evSize, 17 * 1000, 200, true);
    
    // clear region.
    region.clear();
    
    WaitCriterion evClear = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == 0);
      }
      public String description() {
        return "valueToEntriesMap never became size :" + 0;
      }
    };
    DistributedTestBase.waitForCriterion(evClear, 17 * 1000, 200, true);
    
    // Add to region.
    for( int i=0; i<size ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    //assertEquals(0, getIndexSize(ri));
    DistributedTestBase.waitForCriterion(evSize, 17 * 1000, 200, true);
  }
  
  public void testIndexMaintenanceBasedOnThresholdAsZero() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "0");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "60000");
    final Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    for( int i=0; i<3 ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }  

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == 3);
      }
      public String description() {
        return "valueToEntries map never became size 3";
      }
    };
    DistributedTestBase.waitForCriterion(ev, 10 * 1000, 200, true);
  }
  
  public void testNoIndexMaintenanceBasedOnNegativeThresholdAndZeroSleepTime() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "-1");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "0");
    Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    
    int size = this.getIndexSize(ri);
    
    for( int i=0; i<3 ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    Thread.sleep(10000);
    //assertEquals(0, this.getIndexSize(ri));    
        
  }
  
  public void testConcurrentIndexMaintenanceForNoDeadlocks() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "700");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "500");
    qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    final int TOTAL_THREADS = 25;
    final int NUM_UPDATES = 25;
    final CyclicBarrier barrier = new CyclicBarrier(TOTAL_THREADS);
    Thread threads[] = new Thread[TOTAL_THREADS];
    for (int i = 0; i < TOTAL_THREADS; ++i) {
      final int k = i;
      threads[i] = new Thread(new Runnable() {
        public void run() {
          try {
            barrier.await();
            for (int i = 0; i < NUM_UPDATES; ++i) {
              try {
                region.put("" + (k + 1), new Portfolio(k + 1));
                Thread.sleep(10);
              } catch (IllegalStateException ie) {
                // If Asynchronous index queue is full. Retry.
                if (ie.getMessage().contains("Queue full")) {
                  // retry
                  i--;
                  continue;
                }
                throw ie;
              }
            }
          }
          catch (Exception e) {
            CacheUtils.getLogger().error(e);
            exceptionOccured = true;
          }
        }
      });
    }
    for (int i = 0; i < TOTAL_THREADS; ++i) {
      threads[i].start();
    }
    try {
      for (int i = 0; i < TOTAL_THREADS; ++i) {
        DistributedTestBase.join(threads[i], 30 * 1000, null);
      }
    }
    catch (Exception e) {
      CacheUtils.getLogger().error(e);
      exceptionOccured = true;
    }
    assertFalse(exceptionOccured);
  }  

}

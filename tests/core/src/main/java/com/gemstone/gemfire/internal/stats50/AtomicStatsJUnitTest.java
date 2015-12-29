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
package com.gemstone.gemfire.internal.stats50;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.concurrent.CF6Impl;

/**
 * @author dsmith
 *
 */
public class AtomicStatsJUnitTest extends TestCase {
  
  /**
   * Test for bug 41340. Do two gets at the same time of a dirty
   * stat, and make sure we get the correct value for the stat.
   * @throws Throwable
   */
  public void testConcurrentGets() throws Throwable {
    
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    //    props.setProperty("statistic-sample-rate", "60000");
    props.setProperty("statistic-sampling-enabled", "false");
    DistributedSystem ds = DistributedSystem.connect(props);
    
    String statName = "TestStats";
    String statDescription =
      "Tests stats";

    final String statDesc =
      "blah blah blah";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    StatisticsType type = f.createType(statName, statDescription,
       new StatisticDescriptor[] {
         f.createIntGauge("stat", statDesc, "bottles of beer on the wall"),
       });

    final int statId = type.nameToId("stat");

    try {

      final AtomicReference<Statistics> statsRef = new AtomicReference<Statistics>();
      final CyclicBarrier beforeIncrement = new CyclicBarrier(3);
      final CyclicBarrier afterIncrement = new CyclicBarrier(3);
      Thread thread1 = new Thread("thread1") {
        public void run() {
          try {
            while(true) {
              beforeIncrement.await();
              statsRef.get().incInt(statId, 1);
              afterIncrement.await();
            }
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      };
      Thread thread3 = new Thread("thread1") {
        public void run() {
          try {
            while(true) {
              beforeIncrement.await();
              afterIncrement.await();
              statsRef.get().getInt(statId);
            }
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (BrokenBarrierException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      };
      thread1.start();
      thread3.start();
      for(int i =0; i < 5000; i++) {
        Statistics stats = ds.createAtomicStatistics(type, "stats");
        statsRef.set(stats);
        beforeIncrement.await();
        afterIncrement.await();
        assertEquals("On loop " + i, 1, stats.getInt(statId));
        stats.close();
      }
    
    } finally {
      ds.disconnect();
    }
  }

  /**
   * This test does micro-benchmarking of Atomic*StatsImpl classes in particular
   * thread-local stats vs atomic ops.
   */
  public void DISABLED_testAtomicStatsPerformance() throws Exception {

    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("statistic-sampling-enabled", "false");

    final int numLoops = 500000;
    final int numThreads = 40;
    final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
    @SuppressWarnings("unchecked")
    final Region<String, byte[]>[] testLR = new Region[1];
    final Exception[] failure = new Exception[1];

    Runnable threadWork = new Runnable() {
      @Override
      public void run() {
        try {
          final Random rand = new Random();
          final Region<String, byte[]> lr = testLR[0];
          barrier.await();
          for (int i = 1; i <= numLoops; i++) {
            int rnd = rand.nextInt(2);
            int key = rand.nextInt(100000);
            switch (rnd) {
              case 0:
                lr.put("key-" + key, new byte[10]);
                break;
              default:
                lr.get("key-" + key);
                break;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          failure[0] = e;
        }
      }
    };

    Cache cache;
    Thread[] threads = new Thread[numThreads];
    long start, end, duration1, duration2;

    for (int cnt = 1; cnt <= 6; cnt++) {

      // first using the Atomic50StatisticsImpl
      System.setProperty("gemfire.disableManagement", "true");
      CF6Impl.STRIPED_STATS_ENABLED = true;
      cache = new CacheFactory(props).create();
      try {
        testLR[0] = cache.<String, byte[]> createRegionFactory(
            RegionShortcut.PARTITION).create("CFStats");

        for (int i = 0; i < numThreads; i++) {
          threads[i] = new Thread(threadWork);
        }
        for (int i = 0; i < numThreads; i++) {
          threads[i].start();
        }
        barrier.await();
        start = System.nanoTime();
        for (int i = 0; i < numThreads; i++) {
          threads[i].join();
        }
        end = System.nanoTime();
        duration1 = (end - start);

        // ignore first couple of warmup runs
        if (cnt > 2) {
          System.out.println("Time taken using Atomic50StatisticsImpl "
              + duration1 + " nanos");
        }

        if (failure[0] != null) {
          AssertionError ae = new AssertionError(failure[0].getMessage());
          ae.initCause(failure[0]);
          throw ae;
        }

      } finally {
        cache.close();
        System.clearProperty("gemfire.disableManagement");
        System.gc();
        System.runFinalization();
        System.gc();
        System.runFinalization();
        System.gc();
        System.runFinalization();
      }

      // next using the Atomic60StatisticsImpl
      System.setProperty("gemfire.disableManagement", "true");
      CF6Impl.STRIPED_STATS_ENABLED = false;
      cache = new CacheFactory(props).create();
      try {
        testLR[0] = cache.<String, byte[]> createRegionFactory(
            RegionShortcut.PARTITION).create("CFStats");

        for (int i = 0; i < numThreads; i++) {
          threads[i] = new Thread(threadWork);
        }
        for (int i = 0; i < numThreads; i++) {
          threads[i].start();
        }
        barrier.await();
        start = System.nanoTime();
        for (int i = 0; i < numThreads; i++) {
          threads[i].join();
        }
        end = System.nanoTime();
        duration2 = (end - start);

        // ignore first couple of warmup runs
        if (cnt > 2) {
          System.out.println("Time taken using Atomic60StatisticsImpl "
              + duration2 + " nanos");

          System.out.println("Percentage difference = "
              + ((100.0 * (duration1 - duration2)) / (double)duration2));
        }

        if (failure[0] != null) {
          AssertionError ae = new AssertionError(failure[0].getMessage());
          ae.initCause(failure[0]);
          throw ae;
        }

      } finally {
        cache.close();
        System.clearProperty("gemfire.disableManagement");
        System.gc();
        System.runFinalization();
        System.gc();
        System.runFinalization();
        System.gc();
        System.runFinalization();
      }
    }
  }
}

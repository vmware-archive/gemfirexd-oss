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
package parReg.eviction;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;
import parReg.ParRegTest;
import util.PRObserver;
import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;

public class DelayedDispatcherTest extends ParRegTest {

  public final static String SLOW_START_TIME = "100000";

  protected static InternalResourceManager irm;

  protected static EvictionThresholdListener listener;

  public static final String CRITICAL_HEAP_PERCENTAGE = "Critical Heap Percentage";

  public static final String EVICTION_HEAP_PERCENTAGE = "Eviction Heap Percentage";

  public static final float LOWER_HEAP_LIMIT_PERCENT = System.getProperty("java.vm.vendor").startsWith("IBM") ? 25:15 ;
    
  public static final float UPPER_HEAP_LIMIT_PERCENT = 25;

  public synchronized static void HydraTask_initializeBridgeServer() {
    if (testInstance == null) {
      CacheClientProxy.isSlowStartForTesting = true;
      System.setProperty("slowStartTimeForTesting", SLOW_START_TIME);
      Log.getLogWriter().info(
          "Configuring the test with slowed down dispatcher.");

      PRObserver.installObserverHook();
      testInstance = new ParRegTest();
      testInstance.initializeRegion("dataStoreRegion");
      testInstance.initializeInstance();
      BridgeHelper.startBridgeServer("bridge");
      testInstance.isBridgeClient = false;
      testInstance.isDataStore = true;

      Cache theCache = CacheHelper.getCache();
      irm = (InternalResourceManager)theCache.getResourceManager();
      Log.getLogWriter().info("Registering Listener");
      listener = new EvictionThresholdListener();
      irm.addResourceListener(ResourceType.HEAP_MEMORY, listener);
    }
    testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule
        .getCurrentThread().getThreadId()));
  }

  public synchronized static void HydraTask_updateBB() {
    if (irm == null) {
      throw new TestException("ResourceManager is null");
    }

    float criticalHeapPercentage = irm.getCriticalHeapPercentage();
    float evictionHeapPercentage = irm.getEvictionHeapPercentage();

    EvictionBB.getBB().getSharedMap().put(CRITICAL_HEAP_PERCENTAGE,
        criticalHeapPercentage);
    EvictionBB.getBB().getSharedMap().put(EVICTION_HEAP_PERCENTAGE,
        evictionHeapPercentage);

    EvictionBB.getBB().printSharedMap();
  }

  public synchronized static void HydraTask_verifyEvictionBehavior() {

    if (TestConfig.tab().booleanAt(EvictionPrms.verifyEvictionEvents, true)) {
      checkBlackBoardForException();
    }

    if (TestConfig.tab().booleanAt(EvictionPrms.verifyHeapUsage, true)) {
      verifyHeapUsage();
    }

  }

  public static void checkBlackBoardForException() {
    Log.getLogWriter().info("Checking BB for exceptions");
    long exceptionCount = EvictionBB.getBB().getSharedCounters().read(
        EvictionBB.NUM_EXCEPTION);
    long exceptionLoggingComplete = EvictionBB.getBB().getSharedCounters()
        .read(EvictionBB.NUM_COMPLETED_EXCEPTION_LOGGING);

    SharedMap sharedmap = EvictionBB.getBB().getSharedMap();

    EvictionBB.getBB().printSharedMap();

    if (exceptionCount > 5 //Wait for min 5 exceptions
        && (exceptionLoggingComplete > 0 || sharedmap.get(new Long(1)) != null)) {
      StringBuffer reason = new StringBuffer();
      reason.append("total exceptions = " + exceptionCount);
      reason.append("\n");

      for (long i = 1; i < exceptionCount + 1; i++) {
        reason.append("Reason for exception no. " + i + " : ");
        reason.append(sharedmap.get(new Long(i)));
        reason.append("\n");
      }

      throw new TestException(reason.toString());
    }
  }

  public static void verifyHeapUsage() {
    Log.getLogWriter().info("Checking Heap Usage");
    if (irm == null) {
      throw new TestException("Resource Manager is null");
    }

    if (listener.getEvictionThresholdCalls() > 0) { // Verify only if
      // for this vm,
      // eviction has
      // happened
      Cache theCache = CacheHelper.getCache();
      InternalResourceManager irm = (InternalResourceManager)theCache.getResourceManager();
      long currentHeapUsage = irm.getHeapMonitor().getBytesUsed();
      double maxTenuredBytes = HeapMemoryMonitor.getTrackedMaxMemory();

      float currentHeapUsagePercentage = (float)(currentHeapUsage / maxTenuredBytes) * 100;
      float evictionHeapPercentage = (Float)EvictionBB.getBB().getSharedMap()
          .get(EVICTION_HEAP_PERCENTAGE);
      float criticalHeapPercentage = (Float)EvictionBB.getBB().getSharedMap()
          .get(CRITICAL_HEAP_PERCENTAGE);

      float heapLowerBound = evictionHeapPercentage - LOWER_HEAP_LIMIT_PERCENT;
      float heapUpperBound = evictionHeapPercentage + UPPER_HEAP_LIMIT_PERCENT;

      if (currentHeapUsagePercentage < heapLowerBound) {
        throw new TestException(
            "Possible over eviction : Current heap utilization percent "
                + currentHeapUsagePercentage + " and eviction heap percent "
                + evictionHeapPercentage);
      }
      else if (currentHeapUsagePercentage > heapUpperBound) {
        throw new TestException(
            "Possibility of eviction not catching up : Current heap utilization percent "
                + currentHeapUsagePercentage + " and eviction heap percent "
                + evictionHeapPercentage);
      }
      else {
        Log.getLogWriter().info(
            "CurrentHeapUsagePercent " + currentHeapUsagePercentage
                + " in allowable limits");
      }
    }
    else {
      Log
          .getLogWriter()
          .info(
              "No eviction trigerred so far; hence not required to verify eviction behavior");
    }
  }

}

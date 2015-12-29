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

import hydra.CacheHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import parReg.ParRegBB;
import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import perffmwk.StatSpecTokens;
import util.BaseValueHolder;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;

public class ParRegHeapEvictionTest {

  protected static ParRegHeapEvictionTest testInstance;

  protected static List regionDescriptNames = new ArrayList();

  protected static Cache theCache;

  protected RandomValues randomValues = null;

  protected static InternalResourceManager irm;

  protected static EvictionThresholdListener listener;

  protected int numThreadsInClients;

  public static final int KEYS_TO_PUT = 500;
  
  protected static final int HEAVY_OBJECT_SIZE_VAL = 500;

  public static final String CRITICAL_HEAP_PERCENTAGE = "Critical Heap Percentage";

  public static final String EVICTION_HEAP_PERCENTAGE = "Eviction Heap Percentage";
  
  public static final String CRITICAL_OFF_HEAP_PERCENTAGE = "Critical Off-Heap Percentage";

  public static final String EVICTION_OFF_HEAP_PERCENTAGE = "Eviction Off-Heap Percentage";
  
  public static final float LOWER_HEAP_LIMIT_PERCENT = 30;

  public static final float UPPER_HEAP_LIMIT_PERCENT = 25;
  
  protected static long totalNumOverFlowToDisk = 0;

  protected static long totalEntriesInDisk = 0;

  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new ParRegHeapEvictionTest();
    }
    testInstance.initialize();
  }

  public synchronized static void HydraTask_createRegions() {
    if (testInstance == null) {
      testInstance = new ParRegHeapEvictionTest();
    }
    testInstance.createRegions();
  }

  public synchronized static void HydraTask_updateBB() {
    if (testInstance == null) {
      testInstance = new ParRegHeapEvictionTest();
    }
    testInstance.updateBB();
  }

  public synchronized static void HydraTask_populateRegions() {
    if (testInstance == null) {
      testInstance = new ParRegHeapEvictionTest();
    }
    testInstance.populateRegions();
  }
  
  public synchronized static void HydraTask_populateUniformHeavyEntries() {
    if (testInstance == null) {
      testInstance = new ParRegHeapEvictionTest();
    }
    testInstance.populateUniformHeavyEntries();
  }

  public synchronized static void HydraTask_doQuery() {
    testInstance.doQuery();
  }

  public synchronized static void HydraTask_populateMaxEntries() {
    if (testInstance == null) {
      testInstance = new ParRegHeapEvictionTest();
    }
    testInstance.populateMaxEntries();
  }

  public synchronized static void HydraTask_populateAndVerify() {
    testInstance.populateAndVerify();
  }

  public synchronized static void HydraTask_verifyEvictionBehavior() {
    if (testInstance == null) {
      testInstance = new ParRegHeapEvictionTest();
    }
    testInstance.verifyEvictionBehavior();
  }
  
 
  public static void HydraTask_verifyRegionFairness() {
    testInstance.verifyRegionFairness();
  }
  
  public static void HydraTask_verifyEviction() {
    testInstance.verifyEviction();
  }
  
  public static void HydraTask_verifyNoEviction() {
    testInstance.verifyNoEviction();
  }


  protected void initialize() {
    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);

    if (regionDescriptNames.size() == 0) {
      for (int i = 0; i < regionNames.size(); i++) {
        String regionDescriptName = (String)(regionNames.get(i));
        regionDescriptNames.add(regionDescriptName);
      }
      Log.getLogWriter().info("regionDescriptNames is " + regionDescriptNames);
      theCache = CacheHelper.createCache("cache1");
      randomValues = new RandomValues();

      irm = (InternalResourceManager)theCache.getResourceManager();
      Log.getLogWriter().info("Registering Listener");
      listener = new EvictionThresholdListener();
      irm.addResourceListener(ResourceType.MEMORY, listener);
    }
  }

  protected void createRegions() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      RegionHelper.createRegion(regionDescriptName);
    }
  }

  protected void updateBB() {
    if (irm == null) {
      throw new TestException("ResourceManager is null");
    }

    float criticalHeapPercentage = irm.getCriticalHeapPercentage();
    float evictionHeapPercentage = irm.getEvictionHeapPercentage();

    EvictionBB.getBB().getSharedMap().put(CRITICAL_HEAP_PERCENTAGE,
        criticalHeapPercentage);
    EvictionBB.getBB().getSharedMap().put(EVICTION_HEAP_PERCENTAGE,
        evictionHeapPercentage);

    float criticalOffHeapPercentage = irm.getCriticalOffHeapPercentage();
    float evictionOffHeapPercentage = irm.getEvictionOffHeapPercentage();

    EvictionBB.getBB().getSharedMap().put(CRITICAL_OFF_HEAP_PERCENTAGE,
        criticalOffHeapPercentage);
    EvictionBB.getBB().getSharedMap().put(EVICTION_OFF_HEAP_PERCENTAGE,
        evictionOffHeapPercentage);
    
    EvictionBB.getBB().printSharedMap();
  }

  protected void populateAndVerify() {
    logExecutionNumber();
    numThreadsInClients = RemoteTestModule.getCurrentThread().getCurrentTask()
        .getTotalThreads();
    Log.getLogWriter().info("numThreadsInClients = " + numThreadsInClients);
    long roundPosition = ParRegBB.getBB().getSharedCounters().incrementAndRead(
        EvictionBB.RoundPosition);
    Log.getLogWriter().info(
        "In populateAndVerify, roundPosition is " + roundPosition);
    if (roundPosition == numThreadsInClients) { // this is the last in the round
      Log.getLogWriter().info("In populateAndVerify, last in round");
      verifyEvictionBehavior();
      // now become the first in the round
      EvictionBB.getBB().getSharedCounters().zero(EvictionBB.RoundPosition);
      roundPosition = EvictionBB.getBB().getSharedCounters().incrementAndRead(
          EvictionBB.RoundPosition);
    }

    if (roundPosition == 1) { // first in round, do populate region
      long roundNumber = EvictionBB.getBB().getSharedCounters()
          .incrementAndRead(EvictionBB.RoundNumber);
      Log.getLogWriter().info(
          "In populateAndVerify, first in round, round number " + roundNumber);
      populateRegions();
    }
    else if (roundPosition != numThreadsInClients) { // neither first nor last
      Log.getLogWriter().info("In populateAndVerify, neither first nor last");
      verifyEvictionBehavior();
    }

  }
  
  protected void populateUniformHeavyEntries() {
    Integer keyObject = new Integer((int)ParRegBB.getBB().getSharedCounters()
        .incrementAndRead(ParRegBB.numOfPutOperations));

    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      populateUniformHeavyEntries(aRegion, keyObject);
    }
  }

  protected void populateUniformHeavyEntries(Region aRegion, Object key) {
    byte[] newVal = new byte[HEAVY_OBJECT_SIZE_VAL * HEAVY_OBJECT_SIZE_VAL];
    aRegion.put(key, newVal);
  }

  protected void populateRegions() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      populateRegion(aRegion);
    }
  }

  protected void populateRegion(Region aRegion) {
    for (int i = 0; i < KEYS_TO_PUT; i++) {
      Object key = NameFactory.getNextPositiveObjectName();
      BaseValueHolder value = new ValueHolder((String)key, randomValues);
      aRegion.put(key, value);
    }
  }

  protected void populateMaxEntries() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      populateMaxEntries(aRegion);
    }
  }

  protected void populateMaxEntries(Region aRegion) {
    long maxEntries = TestConfig.tab().longAt(EvictionPrms.maxEntries);
    int numThreadsInClients = RemoteTestModule.getCurrentThread()
        .getCurrentTask().getTotalThreads();
    long putCountInTheRegion = maxEntries
        / (regionDescriptNames.size() * numThreadsInClients);

    Log.getLogWriter().info(
        "Entries to be put " + putCountInTheRegion + " maxEntries "
            + maxEntries + " numThreadsInClients " + numThreadsInClients
            + " regionDescriptNames.size() " + regionDescriptNames.size());
    for (long i = 0; i < putCountInTheRegion; i++) {
      Object key = NameFactory.getNextPositiveObjectName();
      aRegion.put(key, new byte[1024 * 1024]);
    }

    Log.getLogWriter().info(
        "Completed populating region " + aRegion.getName() + " have "
            + aRegion.size() + " keys");
  }

  protected void doQuery() {
    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      doQuery(aRegion);
    }
  }

  protected void doQuery(Region aRegion) {
    String queryString = "select distinct * from " + aRegion.getFullPath();
    Query query = theCache.getQueryService().newQuery(queryString);
    try {
      Object result = query.execute();
      if ((result instanceof Collection)) {
        Log.getLogWriter().info(
            "Size of result is :" + ((Collection)result).size());
      }
    }
    catch (Exception e) {
      throw new TestException("Caught exception during query execution"
          + TestHelper.getStackTrace(e));
    }
  }

  protected void verifyEvictionBehavior() {
    if (TestConfig.tab().booleanAt(EvictionPrms.verifyEvictionEvents, true)) {
      checkBlackBoardForException();
    }

    if (TestConfig.tab().booleanAt(EvictionPrms.verifyHeapUsage, true)) {
      verifyHeapUsage();
    }
  }

  public void checkBlackBoardForException() {
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

  public void verifyHeapUsage() {
    Log.getLogWriter().info("Checking Heap Usage");
    if (irm == null) {
      throw new TestException("Resource Manager is null");
    }

    if (listener.getEvictionThresholdCalls() > 0) { // Verify only if for this
                                                      // vm, eviction has
                                                      // happened
      HeapMemoryMonitor hmm = ((InternalResourceManager) theCache.getResourceManager()).getHeapMonitor();
      long currentHeapUsage = hmm.getBytesUsed();
      double maxTenuredBytes = hmm.getTrackedMaxMemory();

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
  
  public void verifyEviction(){    
    double totalHeapEvictions = getNumHeapLRUEvictions();    
    if(totalHeapEvictions <= 0){
      throw new TestException("Test needs tuning - no eviction reported");
    }    
    Log.getLogWriter().info("Total eviction "+totalHeapEvictions);
  }
  
  public void verifyNoEviction(){    
    double totalHeapEvictions = getNumHeapLRUEvictions();    
    if(totalHeapEvictions > 0){
      throw new TestException("Test needs tuning, the test should not have evicted during inittask, but evicted "+totalHeapEvictions);
    }    
  }


 public static double getNumHeapLRUEvictions() {
    String spec = "* " // search all archives
                  + "HeapLRUStatistics "
                  + "* " // match all instances
                  + "lruEvictions "
                  + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                  + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                  + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List aList = PerfStatMgr.getInstance().readStatistics(spec);
    if (aList == null) {
       Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
       return 0.0;
    }
    double totalEvictions = 0;
    for (int i = 0; i < aList.size(); i++) {
       PerfStatValue stat = (PerfStatValue)aList.get(i);
       totalEvictions += stat.getMax();
    }
    return totalEvictions;
 }
 
  static protected void logExecutionNumber() {
    long exeNum = EvictionBB.getBB().getSharedCounters().incrementAndRead(
        EvictionBB.ExecutionNumber);
    Log.getLogWriter().info("Beginning task with execution number " + exeNum);
  }
  

  public synchronized void verifyRegionFairness() {

    long totalNumEntriesInDisk = 0;
    long totalNumEntriesInVm = 0;

    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      totalNumEntriesInDisk += pr.getDiskRegionStats().getNumOverflowOnDisk();
      totalNumEntriesInVm += pr.getDiskRegionStats().getNumEntriesInVM();
    }

    long averageEvictionPerRegion = totalNumEntriesInDisk
        / (regionDescriptNames.size());
    Log.getLogWriter().info("totalNumEntriesInDisk " + totalNumEntriesInDisk);
    Log.getLogWriter().info("totalNumEntriesInVm " + totalNumEntriesInVm);
    Log.getLogWriter().info(
        "AverageEvictionPerRegion = " + (int)averageEvictionPerRegion);

    for (int i = 0; i < regionDescriptNames.size(); i++) {
      String regionDescriptName = (String)(regionDescriptNames.get(i));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      Region aRegion = theCache.getRegion(regionName);
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      long numEntriesInDisk = pr.getDiskRegionStats().getNumOverflowOnDisk();
      long entriesInVm = pr.getDiskRegionStats().getNumEntriesInVM();

      // Checking with 25% tolerance on both sides
      if (numEntriesInDisk > averageEvictionPerRegion * 1.25
          || numEntriesInDisk < averageEvictionPerRegion * 0.75) {
        throw new TestException("For the region " + aRegion.getName()
            + " average expected eviction is " + averageEvictionPerRegion
            + " but is " + numEntriesInDisk);
      }
      else {
        Log.getLogWriter().info(
            "For the region " + aRegion.getName() + " num evicted is "
                + numEntriesInDisk + " which is within the expected limit "
                + averageEvictionPerRegion);
      }
    }

    if (totalNumEntriesInVm == 0 && totalNumEntriesInDisk > 0) {
      throw new TestException(
          "After eviction from all the regions the cache is empty");
    }

    // Aneesh: TO DO: After GC tuning
    // verifyHeapUsage();
  }
  
}

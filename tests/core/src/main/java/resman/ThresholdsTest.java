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
package resman;

import hydra.AsyncEventQueueHelper;
import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import parReg.ParRegBB;
import parReg.ParRegPrms;
import parReg.ParRegTest;
import parReg.ParRegUtil;
import resman.Indexable.ThresholdsTestStats;
import util.BaseValueHolder;
import util.PRObserver;
import util.RandomValues;
import util.StopStartBB;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.TxHelper;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.ConflictException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.TransactionDataNodeHasDepartedException;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.TransactionInDoubtException;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.SetUtils;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds;
import com.gemstone.gemfire.internal.lang.ThreadUtils;

public class ThresholdsTest extends ParRegTest {
  private static final String CLIENTVMINFO_TO_DM = "ClientVmInfo-to-DM";
  private static final String VMID_OF_LAST_KILLED_KEY = "VMIdOfLastKilled";
  public static final String ACCESSOR_NO_LOW_MEM_TIMESTAMP = "accessorNoLowMemTimestamp";
  public static final String ACCESSOR_LOW_MEM_TIMESTAMP = "accessorLowMemTimestamp";
  protected static Region noEvictRegion;
  public volatile ThresholdsTestStats testStats = null;
  public final Inspector inspector;
  private static final int NUM_EVICTION_ENTRIES = 20;
  private static final int NUM_CRITICAL_ENTRIES = 5;

  // Interface to switch inspector type
  public interface Inspector {
    public void reportNoLowMemoryException(Region r, Object key, Set keyOwners);
    public void reportLowMemoryException(Region r, Object key, LowMemoryException lme, Set keyOwners);
    public void expectLowMemoryException(final boolean expect, DistributedMember member);
    public void waitForAllMembersCriticalOrFail();
    public void waitForNoMembersCriticalOrFail();
  }
  
  private ThresholdsTest() {
    String it = ResourceManPrms.getInspectorType();
    if (it != null && it.equalsIgnoreCase("easy")) {
      this.inspector = new EasyInspector();
    } else {
      this.inspector = new SternInspector();
    }
  }

  private static ThresholdsTest getTestInstance() {
    return (ThresholdsTest) testInstance;
  }
  private static ThresholdsTestStats getThresholdTestStats() {
    return getTestInstance().testStats;
  }

  /** Creates and initializes the singleton instance of ParRegTest in this VM.
   */
  public synchronized static void HydraTask_initializeWithRegDef() {
     if (testInstance == null) {
        PRObserver.installObserverHook();
        testInstance = new ThresholdsTest();
        testInstance.initializeWithRegDef();
        testInstance.initializeInstance();
     }
     testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
  }
  
  /** Creates and initializes a data store PR in a bridge server.
   */
  public synchronized static void HydraTask_initializeBridgeServer() {
     if (testInstance == null) {
        PRObserver.installObserverHook();
        testInstance = new ThresholdsTest();
        testInstance.initializeRegion("dataStoreRegion");
        testInstance.initializeInstance();
        BridgeHelper.startBridgeServer("bridge");
        testInstance.isBridgeClient = false;
        testInstance.isDataStore = true;
     }
     testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
  }

  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing with a PR accessor.
   */
  public synchronized static void HydraTask_HA_initializeAccessor() {
     if (testInstance == null) {
        PRObserver.installObserverHook();
        testInstance = new ThresholdsTest();
        testInstance.initializeRegion("accessorRegion");
        testInstance.initializeInstance();
        testInstance.isDataStore = false;
        if (testInstance.isBridgeConfiguration) {
           testInstance.isBridgeClient = true;
           ParRegUtil.registerInterest(testInstance.aRegion);
        }
        ((ThresholdsTest) testInstance).initializeStats();
        DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
        ThresholdsTest.putDistributedMember(dm);
     }
     testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
  }
  private static ClientVmInfo getMyClientVMInfo() {
    return new ClientVmInfo(new Integer(RemoteTestModule.getMyVmid()),
        RemoteTestModule.getMyClientName(), RemoteTestModule.getMyLogicalHost());
  }
  private void initializeStats() {
    GemFireCacheImpl cache = (GemFireCacheImpl)CacheHelper.getCache();
    this.testStats = new ThresholdsTestStats(cache.getDistributedSystem());
  }
  
  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing with a PR data store.
   */
  public synchronized static void HydraTask_HA_initializeDataStore() {
    if (testInstance == null) {
        PRObserver.installObserverHook();
        testInstance = new ThresholdsTest();
        testInstance.initializeRegion("dataStoreRegion");
        testInstance.initializeInstance();
        testInstance.isDataStore = true;
        if (testInstance.isBridgeConfiguration) {
           testInstance.isBridgeClient = false;
           BridgeHelper.startBridgeServer("bridge");
        }
        // Indicate that the PR(s) are initialized before allowing another
        // member to proceed with the criticalThenDie task
        ((ThresholdsTest) testInstance).initCriticalThenDie();
        DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
        ThresholdsTest.putDistributedMember(dm);
        ((ThresholdsTest)testInstance).initNoEvictRegion();
     }
     testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
  }
  
  /** 
   * Create AsyncEventListener as defined by {@link ConfigPrms#asyncEventQueueConfig}
   * Create cache & region as defined by ConfigPrms.   
   * Used in newWan WBCL tests to create a separate local wbcl region
   */
  public synchronized static void HydraTask_initializeNewWanWBCL() {
    ((ThresholdsTest)testInstance).createAsyncEventQueue();
  }
  
  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing.
   */
  public synchronized static void HydraTask_HA_reinitializeAccessor() {
    if (testInstance == null) {
      PRObserver.installObserverHook();
      testInstance = new ThresholdsTest();
      testInstance.HA_reinitializeRegion();
      testInstance.initializeInstance();
      testInstance.isDataStore = false;
      if (testInstance.isBridgeConfiguration) {
        testInstance.isBridgeClient = true;
        ParRegUtil.registerInterest(testInstance.aRegion);
      }
      ((ThresholdsTest) testInstance).initializeStats();
      DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
      ThresholdsTest.putDistributedMember(dm);
    }
    testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
  }

  /** Creates and initializes the singleton instance of ParRegTest in this VM
   *  for HA testing.
   */
  public synchronized static void HydraTask_HA_reinitializeDataStore() {
     if (testInstance == null) {
        PRObserver.installObserverHook();
        PRObserver.initialize(RemoteTestModule.getMyVmid());
        testInstance = new ThresholdsTest();
        testInstance.HA_reinitializeRegion();
        testInstance.initializeInstance();
        testInstance.isDataStore = true;
        if (testInstance.isBridgeConfiguration) {
           testInstance.isBridgeClient = false;
           BridgeHelper.startBridgeServer("bridge");
        }

        Integer vmId = (Integer) ResourceManBB.getBB().getSharedMap().remove(VMID_OF_LAST_KILLED_KEY);
        if (vmId != null) {
          PRObserver.waitForRebalRecov(vmId, 1, 1, null, null, false);
        }
        DistributedMember dm = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
        ThresholdsTest.putDistributedMember(dm);
        ((ThresholdsTest)testInstance).initNoEvictRegion();

        // Requires that the PR(s) are initialized *and* recovered before allowing another
        // member to proceed with the criticalThenDie task
        ((ThresholdsTest) testInstance).initCriticalThenDie();
     }
     testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId()));
  }

  public static void HydraTask_InitFakeHeapThresholds() {
    Cache cache = GemFireCacheImpl.getInstance();
    InternalResourceManager irm = (InternalResourceManager)cache.getResourceManager();
    HeapMemoryMonitor hmm = irm.getHeapMonitor();
    //irm.setEvictionHeapPercentage(evictionThreshold);
    hmm.setTestMaxMemoryBytes(1000);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(500);
    irm.setCriticalHeapPercentage(90f);
  }

  public static void HydraTask_CriticalBounce() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache==null) {
      Log.getLogWriter().info("Tried to go critical in this vm, but no cache existed.");
      return;
    }
    try {
      goCritical(cache);
      Random r = new Random();
      int timeToStay = r.nextInt(10000); // Sleep for somewhere between 0 and 10 seconds

      if(timeToStay>0) {
        try {
          Log.getLogWriter().info("Hanging out in Critical state for:"+timeToStay+"ms");
            Thread.sleep(timeToStay);
            Log.getLogWriter().info("Done hanging out in Critical state");
        } catch(InterruptedException tie) {
          tie.printStackTrace();
        }
      }
      goBelowEviction(cache);
    } catch(CacheClosedException cce) {
      // this might happen yes, so lets ignore it.
      Log.getLogWriter().info("Possibly expected CCE during CriticalBounce",cce);
    } catch(com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException cce) {
      // this might happen yes, so lets ignore it.
      Log.getLogWriter().info("Possibly expected DSDE during CriticalBounce",cce);
    }
  }
  
  private static boolean isOffHeap() {
    return testInstance.aRegion.getAttributes().getEnableOffHeapMemory();
  }

  public static void goBelowEviction(GemFireCacheImpl cache) {
    InternalResourceManager irm = cache.
                                  getResourceManager();
    getTestInstance().inspector.expectLowMemoryException(false, cache.getMyId());
    if (isOffHeap()) {
      irm.getOffHeapMonitor().updateStateAndSendEvent(750);
    } else {
      irm.getHeapMonitor().updateStateAndSendEvent(750);
    }
    Log.getLogWriter().info("ThresholdsTest: Moved from Artifical Critical to below eviction threshold");
  }


  public static void goCritical(GemFireCacheImpl cache) {
    InternalResourceManager irm = (InternalResourceManager)cache.getResourceManager();
    getTestInstance().inspector.expectLowMemoryException(true, cache.getMyId());
    if (isOffHeap()) {
      irm.getOffHeapMonitor().updateStateAndSendEvent(950);
    } else {
      irm.getHeapMonitor().updateStateAndSendEvent(950);
    }
    Log.getLogWriter().info("ThresholdsTest: Artifically moved into Critical State");
  }

  public static class CriticalState implements Serializable {
    public final DistributedMember mem;
    public final boolean low;
    public final Long ts;
    public final Long ver;
    public CriticalState(Long timestamp, boolean expectLow, DistributedMember member, Long version) {
      ts = timestamp; low = expectLow; mem = member; ver=version;
    }
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {return true;}
      if (obj == null) {return false;}
      if (! getClass().equals(obj.getClass())) {return false;}
      CriticalState other = (CriticalState)obj;
      if (other.low != this.low) {return false;}
      if (! other.ver.equals(this.ver)) {return false;}
      if (! other.ts.equals(this.ts)) {return false;}
      if (! other.mem.equals(this.mem)) {return false;}
      return true;
    }
    @Override
    public String toString() {
      return new StringBuilder()
      .append(getClass().getSimpleName())
      .append("(ver=").append(ver)
      .append(", low=").append(low)
      .append(", ts=").append(ts)
      .append(", mem=").append(mem)
      .append(")").toString();
    }
  }
  @Override
  protected void clearBBCriticalState(List<ClientVmInfo> targetVMs) {
    CriticalState publishedState = (CriticalState)ResourceManBB.getBB().getSharedMap().get(LOW_MEMORY_STATE);
    if (publishedState != null) {
      for (ClientVmInfo cli: targetVMs) {
        DistributedMember memToClear = getDistributedMember(cli);
        if (publishedState.mem.equals(memToClear)) {
          inspector.expectLowMemoryException(false, memToClear);
        }
      }
    }
  }
  private static DistributedMember getDistributedMember(ClientVmInfo cli) {
    return (DistributedMember)ResourceManBB.getBB().getSharedMap().get(CLIENTVMINFO_TO_DM + cli.getVmid());
  }
  private static void putDistributedMember(DistributedMember dm) {
    ClientVmInfo cli = getMyClientVMInfo();
    Log.getLogWriter().info("Putting dm=" + dm + " for " + cli);
    ResourceManBB.getBB().getSharedMap().put(CLIENTVMINFO_TO_DM + cli.getVmid(), dm);
  }
  public static void HydraTask_makeHighUsage() throws Exception {
    Log.getLogWriter().info("making high usage");
    ((ThresholdsTest)testInstance).makeHighUsage();
  }

  public static void HydraTask_makeCriticalUsage() throws Exception {
    Log.getLogWriter().info("making critical usage");
    ((ThresholdsTest)testInstance).makeCriticalUsage();
  }

  public static void HydraTask_criticalThenDie() throws Exception {
    ((ThresholdsTest)testInstance).criticalThenDie();
  }

  private ThresholdsTest initCriticalThenDie() {
    long mv = ResourceManBB.getBB().getSharedCounters().incrementAndRead(ResourceManBB.criticalMembers);
    Log.getLogWriter().info("criticalMembers inc: " + mv);
    return this;
  }
  
  @SuppressWarnings("unchecked")
  private void criticalThenDie() throws Exception {
    logExecutionNumber();
    checkForLastIteration();
    inspector.waitForNoMembersCriticalOrFail();
    waitForPreviousCriticalMembersToRestart();
    ThreadUtils.sleep(10000);

    long counter = ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop);
    if (counter >= 1)
       throw new StopSchedulingOrder("Num criticalThenDie executions is " +
             ParRegBB.getBB().getSharedCounters().read(ParRegBB.ExecutionNumber));

    GemFireCacheImpl cache = (GemFireCacheImpl)CacheHelper.getCache();
    long criticalThresholdBytes = getThresholds().getCriticalThresholdBytes();
    long currentUsage = getBytesUsed();
    ThreadUtils.sleep(10000);
    if (currentUsage >= criticalThresholdBytes) {
      throw new TestException("Memory usage already critical, consider tuning the test");
    }
    
    inspector.expectLowMemoryException(true, cache.getMyId());
    
    if (currentUsage < getThresholds().getEvictionThresholdBytes()) {
      makeHighUsage();
    }

    System.gc();
   
    int size = (int) ((getThresholds().getCriticalThresholdBytes() * 1.05) - getBytesUsed());
    Log.getLogWriter().info("putting a byte[] of size :"+size);
    for(int i=0;i<NUM_CRITICAL_ENTRIES;i++) {
      noEvictRegion.put("highToCritical"+i, new byte[size/NUM_CRITICAL_ENTRIES]);
      System.gc();
    }

    long sleepTimeMs = TimeUnit.SECONDS.toMillis(ResourceManPrms.getTaskRemainCriticalSeconds());
    Log.getLogWriter().info("Sleeping for " + sleepTimeMs + "ms");
    Thread.sleep(sleepTimeMs);

    // Assume only one VM does this at a time
    long mv = ResourceManBB.getBB().getSharedCounters().decrementAndRead(ResourceManBB.criticalMembers);
    Log.getLogWriter().info("criticalMembers dec: " + mv);
    ResourceManBB.getBB().getSharedMap().put(VMID_OF_LAST_KILLED_KEY, new Integer(RemoteTestModule.getMyVmid()));

    inspector.expectLowMemoryException(false, cache.getMyId());

    int stopMode = ResourceManPrms.getTaskStopMode();
    ClientVmMgr.stopAsync("Stopping critical member", stopMode, ClientVmMgr.IMMEDIATE);
    // Expect this method to be re-scheduled again on another host
  }

  private void waitForPreviousCriticalMembersToRestart() {
    int totalDatastoreCount = ResourceManPrms.getTotalDatastoreCount();
    assert totalDatastoreCount > 0;
    TestHelper.waitForCounter(ResourceManBB.getBB(),
        "ResourceManBB.criticalMembers",
        ResourceManBB.criticalMembers,
        totalDatastoreCount,
        true,
        TimeUnit.SECONDS.toMillis(this.secondsToRun)/2,
        250);
  }

  public static void HydraTask_makeLowUsage() throws Exception {
    Log.getLogWriter().info("making low usage");
    ((ThresholdsTest)testInstance).makeLowUsage();
  }

  /**
   * Put the memory into a "high usage" state. High usage is
   * above Eviction threshold but below Critical threshold
   * @throws Exception
   */
  private void makeHighUsage() throws Exception {
    GemFireCacheImpl cache = (GemFireCacheImpl)CacheHelper.getCache();
    long evictionThresholdBytes = getThresholds().getEvictionThresholdBytes();
    long criticalThresholdBytes = getThresholds().getCriticalThresholdBytes();
    long currentUsage = getBytesUsed();
    
    if (currentUsage >= evictionThresholdBytes && currentUsage < criticalThresholdBytes) {
      throw new TestException("Memory already in high usage range, consider tuning the test");
    }
    
    if (noEvictRegion.size() > NUM_EVICTION_ENTRIES) {
      inspector.expectLowMemoryException(false, cache.getMyId());
      Log.getLogWriter().info("destroying highToCritical byte[]");
      for(int i=0;i<NUM_CRITICAL_ENTRIES;i++) {
        noEvictRegion.remove("highToCritical"+i);
      }
      System.gc();
      inspector.waitForNoMembersCriticalOrFail();
      
    } else {
      int size = (int) (evictionThresholdBytes + ((criticalThresholdBytes - evictionThresholdBytes) * .25) - currentUsage);
      Log.getLogWriter().info("Putting a byte[] of size :"+size);
      for(int i=0;i<NUM_EVICTION_ENTRIES;i++) {
        noEvictRegion.put("lowToHigh"+i, new byte[size/NUM_EVICTION_ENTRIES]);
      }
      System.gc();
    }
  }

  private void makeCriticalUsage() throws Exception {
    GemFireCacheImpl cache = (GemFireCacheImpl)CacheHelper.getCache();
    long currentUsage = getBytesUsed();
     if (currentUsage >= getThresholds().getCriticalThresholdBytes()) {
      throw new TestException("Memory usage already critical, consider tuning the test");
    }
     
    inspector.expectLowMemoryException(true, cache.getMyId());
     
    if (currentUsage < getThresholds().getEvictionThresholdBytes()){
      makeHighUsage();
      System.gc();
    }
    
    int size = (int) ((getThresholds().getCriticalThresholdBytes() * 1.05) - getBytesUsed());
    Log.getLogWriter().info("Putting a byte[] of size :"+size);
    for(int i=0;i<NUM_CRITICAL_ENTRIES;i++) {
      noEvictRegion.put("highToCritical"+i, new byte[size/NUM_CRITICAL_ENTRIES]);
      System.gc();
    }
    inspector.waitForAllMembersCriticalOrFail();
  }

  private void makeLowUsage() throws Exception {
    GemFireCacheImpl cache = (GemFireCacheImpl)CacheHelper.getCache();
    if (noEvictRegion.size() > NUM_EVICTION_ENTRIES) {
      inspector.expectLowMemoryException(false, cache.getMyId());
      for(int i=0;i<NUM_CRITICAL_ENTRIES;i++) {
        noEvictRegion.remove("highToCritical"+i);
      }
      System.gc();
      inspector.waitForNoMembersCriticalOrFail();
    }
    
    for(int i=0;i<NUM_EVICTION_ENTRIES;i++) {
      noEvictRegion.remove("lowToHigh"+i);
    }
    System.gc();
  }


  public static void HydraTask_turnOnGatewayDraining() throws TestException {
    
    // Drain if configured for old WAN
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    List<GatewayHub> hubs = cache.getGatewayHubs();
    for(GatewayHub hub : hubs) {
      List<Gateway> gws = hub.getGateways();
      for(Gateway gw : gws) {
        List<GatewayEventListener> lists = gw.getListeners();
        for(GatewayEventListener gel : lists) {
          ((BlockingGListener)gel).setDraining(true);
        }
      }
    }
    
    // Drain if configured for new WAN
    Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
    for (AsyncEventQueue queue : queues) {
      ((BlockingGListener) queue.getAsyncEventListener()).setDraining(true);
    }
  }
  
  /**
   * Creates a async event queue using the {@link ConfigPrms#asyncEventQueueConfig}.
   */
  protected void createAsyncEventQueue() {
    String asyncEventQueueConfig = ConfigPrms.getAsyncEventQueueConfig();
    AsyncEventQueueHelper.createAndStartAsyncEventQueue(asyncEventQueueConfig);
  }

  /** If this vm has a startup delay, wait for it.
   */
  public static void HydraTask_waitForStartupRecovery() {
     List startupVMs = new ArrayList(StopStartBB.getBB().getSharedMap().getMap().values());
     List vmsExpectingRecovery = StopStartVMs.getMatchVMs(startupVMs, "dataStore");
     vmsExpectingRecovery.addAll(StopStartVMs.getMatchVMs(startupVMs, "bridge"));
     if (vmsExpectingRecovery.size() == 0) {
        throw new TestException("No startup vms to wait for");
     }
     long startupRecoveryDelay = ((Long)(ParRegBB.getBB().getSharedMap().get("startupRecoveryDelay"))).longValue();
     if (startupRecoveryDelay >= 0) {
        PartitionAttributes prAttr = testInstance.aRegion.getAttributes().getPartitionAttributes();
        if (prAttr != null) {
           if (prAttr.getRedundantCopies() != 0) {
              PRObserver.waitForRebalRecov(vmsExpectingRecovery, 1, 1, null, null, false);
           } else {
              Log.getLogWriter().info("Not waiting for recovery because redundantCopies is 0");
           }
        } else {
           Log.getLogWriter().info("Not waiting for recovery because aRegion is not partitioned");
        }
     } else {
        Log.getLogWriter().info("Not waiting for recovery because region is not configured to run recovery at startup");
     }
  }

  public static void setCriticalPercentage(final float percentage) {
    if (isOffHeap()) {
      GemFireCacheImpl.getInstance().getResourceManager().setCriticalOffHeapPercentage(percentage);
    }

    GemFireCacheImpl.getInstance().getResourceManager().setCriticalHeapPercentage(percentage);
  }
  
  public static void setEvictionPercentage(final float percentage) {
    if (isOffHeap()) {
      GemFireCacheImpl.getInstance().getResourceManager().setEvictionOffHeapPercentage(percentage);
    }

    GemFireCacheImpl.getInstance().getResourceManager().setEvictionHeapPercentage(percentage);
  }
  
  public static void HydraTask_turnOnEvictionLate() throws TestException {
    setEvictionPercentage((float)ResourceManPrms.getTaskEvictionPercentage());
  }

  public static void HydraTask_waitForCriticalness() throws TestException {
    logCriticalUpNotificationTime(System.currentTimeMillis());
  }

  public void initNoEvictRegion() {
    Cache cache = CacheHelper.getCache();
    RegionFactory factory = ((GemFireCacheImpl) cache).createRegionFactory(RegionShortcut.LOCAL);
    factory.setEnableOffHeapMemory(isOffHeap());
    final EvictionAttributesImpl evictionAttrs = new EvictionAttributesImpl();
    evictionAttrs.setAlgorithm(EvictionAlgorithm.NONE);
    factory.setEvictionAttributes(evictionAttrs);
    noEvictRegion = factory.create("noEvictRegion");
  }
  
  private static void logCriticalUpNotificationTime(long start) throws TestException {
    long accessorTime = waitForAccessorTimestamp(ACCESSOR_LOW_MEM_TIMESTAMP);
    double millis = (accessorTime - start) * 10e-6;
    Log.getLogWriter().info("REPORT: Critical up detected after "+millis+" millis");
  }

  public static long waitForAccessorTimestamp(final String key) throws TestException {
    int MAX_WAIT_TIME = 30000;
    Blackboard bb = ResourceManBB.getBB();
    boolean done = bb.getSharedMap().get(key) != null;
    int counter = 0;
    int timeToWait = 0;
    while (!done) {
      counter++;
      timeToWait = MAX_WAIT_TIME-(counter*10);
      Log.getLogWriter().info("ThresholdsTest.waitForAccessorTimestamp: waiting for "+key+" for "+timeToWait);
      try {
        Thread.sleep(10);
      } catch(InterruptedException ie) {
        TestException te = new TestException("Interrupted!");
        te.initCause(ie);
      }
      done = bb.getSharedMap().get(key) != null;
      if (!done && timeToWait <= 0) {
        throw new TestException("Accessor did not register a timestamp with the " +
        		"blackboard for "+MAX_WAIT_TIME+" milliseconds");
      }
    }
    long accessorTs = (Long)bb.getSharedMap().get(key);
    bb.getSharedMap().remove(key);
    return accessorTs;
  }

  public static void HydraTask_waitForLowMemoryThenDrainGateways() {
    TestHelper.waitForCounter(ResourceManBB.getBB(),
        "ResourceManBB.lowMemoryFlag",
        ResourceManBB.lowMemoryFlag,
        1,
        true,
        ResourceManPrms.getTaskWaitForLowMemSec()*1000,
        1000);
    HydraTask_turnOnGatewayDraining();
    ResourceManBB.getBB().getSharedCounters().zero(ResourceManBB.lowMemoryFlag);
  }

  public static void HydraTask_waitForLowMemoryThenTurnOnEviction() {
    TestHelper.waitForCounter(ResourceManBB.getBB(),
        "ResourceManBB.lowMemoryFlag",
        ResourceManBB.lowMemoryFlag,
        1,
        true,
        ResourceManPrms.getTaskWaitForLowMemSec()*1000,
        1000);
    HydraTask_turnOnEvictionLate();
    ResourceManBB.getBB().getSharedCounters().zero(ResourceManBB.lowMemoryFlag);
  }

  public static void HydraTask_waitForLowMemoryThenDropIndexes() {
    TestHelper.waitForCounter(ResourceManBB.getBB(),
        "ResourceManBB.lowMemoryFlag",
        ResourceManBB.lowMemoryFlag,
        1,
        true,
        ResourceManPrms.getTaskWaitForLowMemSec()*1000,
        1000);
    HydraTask_dropIndexes();
    System.gc();
    Log.getLogWriter().info("DROPPED ALL THE INDEXES");

    ResourceManBB.getBB().getSharedCounters().zero(ResourceManBB.lowMemoryFlag);
  }

  public static void HydraTask_dropIndexes() throws TestException {
    QueryService qs = ((ThresholdsTest)testInstance).aRegion.getCache().getQueryService();
    qs.removeIndexes();
  }


  public static void HydraTask_doIndexablePutsUntilCriticalThenWaitForRecovery()  throws TestException {
    ((ThresholdsTest)testInstance).doPutsUntilCriticalThenWaitForRecovery(true);
  }

  public static void HydraTask_doPutsUntilCriticalThenWaitForRecovery()  throws TestException {
    ((ThresholdsTest)testInstance).doPutsUntilCriticalThenWaitForRecovery(false);
  }

  public void doPutsUntilCriticalThenWaitForRecovery(boolean useIndexable) throws TestException {
    boolean useCompression = false;
    Compressor compressor = aRegion.getAttributes().getCompressor();
    Log.getLogWriter().info("doPutsUntilCriticalThenWaitForRecovery compressor=" + (compressor == null ? null : compressor.getClass().getName()));
    if (compressor != null ) {
      useCompression = true;
    }
    int numPuts = ResourceManPrms.getTaskNumberOfPuts();
    boolean neverWentCritical = true;
    float minPct = ResourceManPrms.getTaskMinimumPutPercentage();
    for (int i = 0;i < numPuts;i++) {
      try {
        if(useIndexable) {
          simpleIndexablePut(i, useCompression);
        } else {
          simplePut(i, useCompression);
        }
      } catch(LowMemoryException lme) {
        float pct = (float)((float)i/(float)numPuts);
        if (pct > minPct || minPct < 1) {
          neverWentCritical = false;
          // this is ok, we have done 80% of the puts we expected to!
          // So now lets recover and see how long it takes!!!!
          ResourceManBB.getBB().getSharedCounters().zero(ResourceManBB.lowMemoryFlag);
          ResourceManBB.getBB().getSharedCounters().increment(ResourceManBB.lowMemoryFlag);

          TestHelper.waitForCounter(ResourceManBB.getBB(),
              "ResourceManBB.lowMemoryFlag",
              ResourceManBB.lowMemoryFlag,
              0,
              true,
              30000,
              1000);

          long start = System.currentTimeMillis();
          long end = System.currentTimeMillis()+1;
          boolean success = false;
          while((end-start)<(ResourceManPrms.getTaskTolerateLowMemSec()*1000)) { // lets wait to see if it works.
            try {
              if(useIndexable) {
                simpleIndexablePut(i, useCompression);
              } else {
                simplePut(i, useCompression);
              }
              success = true;
              break;
            } catch(LowMemoryException l) {
              // this is what we expect!
              try {
                Thread.sleep(1000);
              } catch(InterruptedException ie) {
                TestException te = new TestException("interrupted exception while sleeping!");
                te.initCause(ie);
                throw te;
              }
              end = System.currentTimeMillis();
            }
          }
          if(!success) {
           throw new TestException("We didn't get rescued from critical within "+(ResourceManPrms.getTaskTolerateLowMemSec())+" seconds");
          } else {
            return;
          }
        } else {
          TestException te = new TestException("Got a lowMemoryException earlier than we should! numPuts="+numPuts+" currentPut="+i+", we expected to start getting LME after 80% of puts, but we are only at "+pct);
          te.initCause(lme);
          throw te;
        }
      }
    }

    if(neverWentCritical) {
      throw new TestException("We never went critical, and we should have!");
    }
  }

  public static void HydraTask_createHeavyIndexes() {
    ((ThresholdsTest)testInstance).createHeavyIndexes();
  }


  public void createHeavyIndexes() {
    QueryService qs = aRegion.getCache().getQueryService();
      try {
        qs.createIndex("stateName-nma", IndexType.FUNCTIONAL, "r.a", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmb", IndexType.FUNCTIONAL, "r.b", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmc", IndexType.FUNCTIONAL, "r.c", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmd", IndexType.FUNCTIONAL, "r.d", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nme", IndexType.FUNCTIONAL, "r.e", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmf", IndexType.FUNCTIONAL, "r.f", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmg", IndexType.FUNCTIONAL, "r.g", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmh", IndexType.FUNCTIONAL, "r.h", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmi", IndexType.FUNCTIONAL, "r.i", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmj", IndexType.FUNCTIONAL, "r.j", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmk", IndexType.FUNCTIONAL, "r.k", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nml", IndexType.FUNCTIONAL, "r.l", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmm", IndexType.FUNCTIONAL, "r.m", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmn", IndexType.FUNCTIONAL, "r.n", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmo", IndexType.FUNCTIONAL, "r.o", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmp", IndexType.FUNCTIONAL, "r.p", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmq", IndexType.FUNCTIONAL, "r.q", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmr", IndexType.FUNCTIONAL, "r.r", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nms", IndexType.FUNCTIONAL, "r.s", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmt", IndexType.FUNCTIONAL, "r.t", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmu", IndexType.FUNCTIONAL, "r.u", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmv", IndexType.FUNCTIONAL, "r.v", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmw", IndexType.FUNCTIONAL, "r.w", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmx", IndexType.FUNCTIONAL, "r.x", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmy", IndexType.FUNCTIONAL, "r.y", "/"+aRegion.getName()+" r");
        qs.createIndex("stateName-nmz", IndexType.FUNCTIONAL, "r.z", "/"+aRegion.getName()+" r");

      } catch(IndexNameConflictException ince){
        TestException te = new TestException(ince.getMessage());
        te.initCause(ince);
        throw te;
      } catch(IndexExistsException iee){
        TestException te = new TestException(iee.getMessage());
        te.initCause(iee);
        throw te;
      } catch(RegionNotFoundException iee){
        TestException te = new TestException(iee.getMessage());
        te.initCause(iee);
        throw te;
      }
  }

  public static void HydraTask_populate() {
    ((ThresholdsTest)testInstance).populate();
  }

  public void populate() {
    int numPuts = ResourceManPrms.getTaskNumberOfPuts();
    for(int i=0;i<numPuts;i++) {
      simpleIndexablePut(i);
    }
  }



  public static void HydraTask_populateGatewayQueue() {
    ((ThresholdsTest)testInstance).populateGateways();
  }

  public void populateGateways() {
    int numPuts = ResourceManPrms.getTaskNumberOfPuts();
    for(int i=0;i<numPuts;i++) {
      try {
        simplePut(i);
        simpleDestroy(i);
      } catch(LowMemoryException lme) {
        Log.getLogWriter().info("Got potentially expected LME while populating gateways. lets bail out.");
        return;
      }
    }
  }

  private void doSimplePut(Object key, Object anObj) {
 
    boolean useTransactions = getInitialImage.InitImagePrms.useTransactions();
    boolean rolledback;

    rolledback = false;
    if (useTransactions) {
       TxHelper.begin();
    }

    try {
       aRegion.put(key, anObj);
      } catch (TransactionDataNodeHasDepartedException e) {
       if (!useTransactions) {
         throw new TestException("Unexpected TransactionDataNodeHasDepartedException " + TestHelper.getStackTrace(e));
       } else {
         Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
         Log.getLogWriter().info("Rolling back transaction.");
         try {
           TxHelper.rollback();
           Log.getLogWriter().info("Done Rolling back Transaction");
         } catch (TransactionException te) {
           Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching TransactionDataNodeHasDeparted during tx ops.  Expected, continuing test.");
         }
         rolledback = true;
       }
      } catch (TransactionDataRebalancedException e) {
       if (!useTransactions) {
         throw new TestException("Unexpected Exception " + e + ". " + TestHelper.getStackTrace(e));
       } else {
         Log.getLogWriter().info("Caught Exception " + e + ".  Expected with concurrent execution, continuing test.");
         Log.getLogWriter().info("Rolling back transaction.");
         try {
           TxHelper.rollback();
           Log.getLogWriter().info("Done Rolling back Transaction");
         } catch (TransactionException te) {
           Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching Exception " + e + " during tx ops.  Expected, continuing test.");
         }
         rolledback = true;
       }
     } catch (LowMemoryException e) {
       if (useTransactions) {
         Log.getLogWriter().info("Caught LowMemoryException, rolling back TX");
         TxHelper.rollback();
         rolledback = true;
         Log.getLogWriter().info("Done Rolling back Transaction");
       }
       throw e;
     }

     if (useTransactions && !rolledback) {
       try {
         TxHelper.commit();
       } catch (TransactionDataNodeHasDepartedException e) {
         Log.getLogWriter().info("Caught TransactionDataNodeHasDepartedException.  Expected with concurrent execution, continuing test.");
       } catch (TransactionDataRebalancedException e) {
         Log.getLogWriter().info("Caught Exception " + e + ".  Expected with concurrent execution, continuing test.");
       } catch (TransactionInDoubtException e) {
         Log.getLogWriter().info("Caught TransactionInDoubtException.  Expected with concurrent execution, continuing test.");
       } catch (ConflictException e) {
         // can occur with concurrent execution
         Log.getLogWriter().info("Caught ConflictException. Expected with concurrent execution, continuing test.");
       }
     }
  }

  public void simpleIndexablePut(int i) {
    simpleIndexablePut(i, false);
  }

  public void simpleIndexablePut(int i, boolean useCompression) {
    Object key = "Object_" + i;
    byte[] anObj = new byte[TestConfig.tab().intAt(util.RandomValuesPrms.elementSize)];

    if (useCompression) {
      TestConfig.tab().getRandGen().nextBytes(anObj);
    }

    Indexable idx = new Indexable(i, anObj);

    doSimplePut(key, idx);
    Log.getLogWriter().info("SimpleIndexablePut key=" + key + " value=" + anObj + " useCompression=" + useCompression);
  }

  public void simplePut(int i) {
    simplePut(i, false);
  }

  public void simplePut(int i, boolean useCompression) {
    Object key = "Object_" + i;
    RandomValues rv = new RandomValues();
    byte[] anObj = rv.getRandom_arrayOfBytes();

    if (useCompression) {
      TestConfig.tab().getRandGen().nextBytes(anObj);
    }

    doSimplePut(key, anObj);
    Log.getLogWriter().info("SimplePut key=" + key + " value=" + anObj + " useCompression=" + useCompression);
  }

  public void simpleDestroy(int i) {
    Object key = "Object_"+i;
    aRegion.destroy(key);
    Log.getLogWriter().info("SimpleDestroy key="+key);
  }


  @Override
  protected Object addEntry(Region r) {
    Object key = getNewKey();
    BaseValueHolder anObj = getValueForKey(key);
    String callback = createCallbackPrefix + ProcessMgr.getProcessId();
    int beforeSize = r.size();
    Set keyOwners = null;
    if (r instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion)r;
      pr.getOwnerForKey(pr.getKeyInfo(key));
    }
    if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call
       if (TestConfig.tab().getRandGen().nextBoolean()) { // use a create call with cacheWriter arg
          try {
            keyOwners = getOwnersForKey(key, r);
             Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
                TestHelper.toString(anObj) + " cacheWriterParam is " + callback + ", region is " +
                r.getFullPath()+" keyOwners:"+keyOwners);
             r.create(key, anObj, callback);
             inspector.reportNoLowMemoryException(r, key, keyOwners);
             Log.getLogWriter().info("addEntry: done creating key " + key);
          } catch (EntryExistsException e) {
             if (isSerialExecution) {
                // cannot get this exception; nobody else can have this key
                throw new TestException(TestHelper.getStackTrace(e));
             } else {
                Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
                // in concurrent execution, somebody could have updated this key causing it to exist
             }
          } catch (LowMemoryException lme) {
            inspector.reportLowMemoryException(r,
                key, lme, keyOwners);
          } catch(ServerOperationException soe) {
            if(soe.getCause() instanceof LowMemoryException) {
              LowMemoryException lme = (LowMemoryException)soe.getCause();
              inspector.reportLowMemoryException(r,
                  key, lme, keyOwners);
            }
          }
       } else { // use create with no cacheWriter arg
          try {
            keyOwners = getOwnersForKey(key, r);
             Log.getLogWriter().info("addEntry: calling create for key " + key + ", object " +
                TestHelper.toString(anObj) + ", region is " + r.getFullPath()+" keyOwners:"+keyOwners);
             r.create(key, anObj);
             inspector.reportNoLowMemoryException(r, key, keyOwners);
             Log.getLogWriter().info("addEntry: done creating key " + key);
          } catch (EntryExistsException e) {
             if (isSerialExecution) {
                // cannot get this exception; nobody else can have this key
                throw new TestException(TestHelper.getStackTrace(e));
             } else {
                Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
                // in concurrent execution, somebody could have updated this key causing it to exist
             }
          } catch (LowMemoryException lme) {
            inspector.reportLowMemoryException(r,
                key, lme, keyOwners);
          } catch(ServerOperationException soe) {
            if(soe.getCause() instanceof LowMemoryException) {
              LowMemoryException lme = (LowMemoryException)soe.getCause();
              inspector.reportLowMemoryException(r,
                  key, lme, keyOwners);
            }
          }
       }
    } else { // use a put call
       Object returnVal = null;
       try {
         if (TestConfig.tab().getRandGen().nextBoolean()) { // use a put call with callback arg
            keyOwners = getOwnersForKey(key, r);
            Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
                  TestHelper.toString(anObj) + " callback is " + callback + ", region is " + r.getFullPath()
                  +" keyOwners:"+keyOwners);
            returnVal = r.put(key, anObj, callback);
            inspector.reportNoLowMemoryException(r, key, keyOwners);
            Log.getLogWriter().info("addEntry: done putting key " + key + ", returnVal is " + returnVal);
         } else {
           keyOwners = getOwnersForKey(key, r);
            Log.getLogWriter().info("addEntry: calling put for key " + key + ", object " +
                  TestHelper.toString(anObj) + ", region is " + r.getFullPath()+" keyOwners:"+keyOwners);
            returnVal = r.put(key, anObj);
            inspector.reportNoLowMemoryException(r, key, keyOwners);
            Log.getLogWriter().info("addEntry: done putting key " + key + ", returnVal is " + returnVal);
         }
       } catch(LowMemoryException lme) {
         inspector.reportLowMemoryException(r,
             key, lme, keyOwners);
       } catch(ServerOperationException soe) {
         if(soe.getCause() instanceof LowMemoryException) {
           LowMemoryException lme = (LowMemoryException)soe.getCause();
           inspector.reportLowMemoryException(r,
               key, lme, keyOwners);
         }
       }
       if (isSerialExecution) { // put always returns NOT_AVAILABLE for PR
         if (isBridgeConfiguration) {
           // lynn - rework this when bug 36436 is fixed; meanwhile allow any return value for bridge
         }
         else {
           // TODO: what we *really* want is to know whether the above update occurred remotely.
           // isLocal() indicates whether or not the entry is hosted locally, which is *not*
           // the same thing, since we now allow reads to occur from secondary copies.
//           Entry entry = aRegion.getEntry(key);
//           RegionEntry re = ((PartitionedRegion.Entry)entry).getRegionEntry();
//           if (returnVal == null || !returnVal.equals(CacheEvent.NOT_AVAILABLE)) {
//             if (!((ParitionedRegion.Entry)entry).wasInitiallyLocal()) {
//               Log.getLogWriter().info("addEntry: regionEntry is " + re.getClass());
//               // if the returned object from put is a real value or null and the entry is not local...
//               throw new TestException("Expected return value from put to be CacheEvent.NOT_AVAILABLE, but it is " + returnVal);
//             }
//           }
//           else
//           if (returnVal.equals(CacheEvent.NOT_AVAILABLE)) {
//             if (((ParitionedRegion.Entry)entry).wasInitiallyLocal()) {
//               // if the returned object from put NOT_AVAILABLE and the entry local...
//               throw new TestException("Expected return value from put to be either null or a real value, but it is " + returnVal);
//             }
//           }
         }
       }
    }

    // validation
    if (isSerialExecution) {
       ParRegUtil.verifyContainsKey(r, key, true);
       ParRegUtil.verifyContainsValueForKey(r, key, true);
       ParRegUtil.verifySize(r, beforeSize+1);

       // record the current state
       regionSnapshot.put(key, anObj.myValue);
       destroyedKeys.remove(key);

    }
    return key;
  }

  private Set<? extends DistributedMember> getOwnersForKey(Object key, Region r) {
    final Set<? extends DistributedMember> keyOwners;
    if (PartitionRegionHelper.isPartitionedRegion(r)) {
      keyOwners = PartitionRegionHelper.getAllMembersForKey(r, key);
    } else if (r.getAttributes().getScope().isDistributed()) {
      DistributedRegion dr = (DistributedRegion) r;
      keyOwners = dr.getCacheDistributionAdvisor().adviseGeneric();
    } else if (r.getAttributes().getScope().isLocal()) {
      LocalRegion lr = (LocalRegion) r;
      if (lr.hasServerProxy()) {
        return null; // TODO figure out a way to determine the key owners on a server
      } else {
        keyOwners = Collections.singleton(((GemFireCacheImpl) lr.getCache()).getMyId());
      }
    } else {
      throw new TestException("Unable to determine owners for key=" + key + " and region=" + r);
    }
    return keyOwners;
  }

  private MemoryThresholds getThresholds() {
    if (isOffHeap()) {
      return GemFireCacheImpl.getInstance().getResourceManager().getOffHeapMonitor().getThresholds();
    }

    return GemFireCacheImpl.getInstance().getResourceManager().getHeapMonitor().getThresholds();
  }
  
  public long getBytesUsed() {
    if (isOffHeap()) {
      return GemFireCacheImpl.getInstance().getResourceManager().getOffHeapMonitor().getBytesUsed();
    }

    return GemFireCacheImpl.getInstance().getResourceManager().getHeapMonitor().getBytesUsed();
  }
  
  @Override
  protected void putAll(Region r) {
    // determine the number of new keys to put in the putAll
    int beforeSize = r.size();
    int numNewKeysToPut = 0;
    int numPutAllExistingKeys = 0;
    boolean limitPutAllToOne = ParRegPrms.getLimitPutAllToOne(); // if true overrides all other putAll settings
    if (limitPutAllToOne) { 
      if (TestConfig.tab().getRandGen().nextBoolean()) {
        numNewKeysToPut = 1;
      } else {
        numPutAllExistingKeys = 1;
      } 
    } else {
      numPutAllExistingKeys = TestConfig.tab().intAt(ParRegPrms.numPutAllExistingKeys);
      String numPutAllNewKeys = TestConfig.tab().stringAt(ParRegPrms.numPutAllNewKeys);
      if (numPutAllNewKeys.equalsIgnoreCase("useThreshold")) {
        numNewKeysToPut = upperThreshold - beforeSize;
        if (numNewKeysToPut <= 0) {
          numNewKeysToPut = 1;
        } else {
          int max = TestConfig.tab().intAt(ParRegPrms.numPutAllMaxNewKeys,
              numNewKeysToPut);
          max = Math.min(numNewKeysToPut, max);
          int min = TestConfig.tab().intAt(ParRegPrms.numPutAllMinNewKeys, 1);
          min = Math.min(min, max);
          numNewKeysToPut = TestConfig.tab().getRandGen().nextInt(min, max);
        }
      } else {
        numNewKeysToPut = Integer.valueOf(numPutAllNewKeys).intValue();
      }
    }

    // get a map to put
    Map mapToPut = null;
    int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
    if (randInt <= 25) {
       mapToPut = new HashMap();
    } else if (randInt <= 50) {
       mapToPut = new Hashtable();
    } else if (randInt <= 75) {
       mapToPut = new TreeMap();
    } else {
       mapToPut = new LinkedHashMap();
    }

    // add new keys to the map
    StringBuffer newKeys = new StringBuffer();
    for (int i = 1; i <= numNewKeysToPut; i++) { // put new keys
       Object key = getNewKey();
       BaseValueHolder anObj = getValueForKey(key);
       mapToPut.put(key, anObj);
       newKeys.append(key + " ");
       if ((i % 10) == 0) {
          newKeys.append("\n");
       }
    }

    // add existing keys to the map
    StringBuffer existingKeys = new StringBuffer();
    List keyList = new ArrayList();
    if (numPutAllExistingKeys > 0) {
      keyList = ParRegUtil.getExistingKeys(r, uniqueKeys, numThreadsInClients, numPutAllExistingKeys, false);
      if (keyList.size() != 0) { // no existing keys could be found
        for (int i = 0; i < keyList.size(); i++) { // put existing keys
          String key = (String)(keyList.get(i));
          Object anObj = getUpdateObject(r, key);
          mapToPut.put(key, anObj);
          existingKeys.append(key + " ");
          if (((i+1) % 10) == 0) {
            newKeys.append("\n");
          }
        }
      }
    }
    Log.getLogWriter().info("Region size is " + r.size() + ", map to use as argument to putAll is " +
        mapToPut.getClass().getName() + " containing " + numNewKeysToPut + " new keys and " +
        keyList.size() + " existing keys (updates); total map size is " + mapToPut.size() +
        "\nnew keys are: " + newKeys + "\n" + "existing keys are: " + existingKeys);

    // do the putAll
    Log.getLogWriter().info("putAll: calling putAll with map of " + mapToPut.size() + " entries");
    try {
      r.putAll(mapToPut);
      inspector.reportNoLowMemoryException(r, PUTALL_KEY, null);
    } catch(LowMemoryException lme) {
      inspector.reportLowMemoryException(r,
          PUTALL_KEY, lme, null); // TODO find member that owns the key
    } catch(ServerOperationException soe) {
      if(soe.getCause() instanceof LowMemoryException) {
        LowMemoryException lme = (LowMemoryException)soe.getCause();
        inspector.reportLowMemoryException(r,
            PUTALL_KEY, lme, null); // TODO find member that owns the key
      }
    }
    Log.getLogWriter().info("putAll: done calling putAll with map of " + mapToPut.size() + " entries");

    // validation
    if (isSerialExecution) {
       ParRegUtil.verifySize(r, beforeSize + numNewKeysToPut);
       Iterator it = mapToPut.keySet().iterator();
       while (it.hasNext()) {
          Object key = it.next();
          BaseValueHolder value = (BaseValueHolder)(mapToPut.get(key));
          ParRegUtil.verifyContainsKey(r, key, true);
          ParRegUtil.verifyContainsValueForKey(r, key, true);

          // record the current state
          regionSnapshot.put(key, value.myValue);
          destroyedKeys.remove(key);
       } // while
   }
 }

  @Override
  protected void updateEntry(Region r) {
    Object key = ParRegUtil.getExistingKey(r, uniqueKeys, numThreadsInClients, false);
    if (key == null) {
       int size = r.size();
       if (isSerialExecution && (size != 0))
          throw new TestException("getExistingKey returned " + key + ", but region size is " + size);
       Log.getLogWriter().info("updateEntry: No keys in region");
       return;
    }
    int beforeSize = r.size();
    Object anObj = getUpdateObject(r, (String)key);
    String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
    Object returnVal = null;
    Set keyOwners = null;
    try {
      if (TestConfig.tab().getRandGen().nextBoolean()) { // do a put with callback arg
         Log.getLogWriter().info("updateEntry: replacing key " + key + " with " +
            TestHelper.toString(anObj) + ", callback is " + callback);
         keyOwners = getOwnersForKey(key, r);
         returnVal = r.put(key, anObj, callback);
         inspector.reportNoLowMemoryException(r, key, keyOwners);
         Log.getLogWriter().info("Done with call to put (update), returnVal is " + returnVal);
      } else { // do a put without callback
         Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + TestHelper.toString(anObj));
         keyOwners = getOwnersForKey(key, r);
         returnVal = r.put(key, anObj, false);
         inspector.reportNoLowMemoryException(r, key, keyOwners);
         Log.getLogWriter().info("Done with call to put (update), returnVal is " + returnVal);
      }
    } catch (LowMemoryException lme) {
      inspector.reportLowMemoryException(r,
          key, lme, keyOwners);
    } catch(ServerOperationException soe) {
      if(soe.getCause() instanceof LowMemoryException) {
        LowMemoryException lme = (LowMemoryException)soe.getCause();
        inspector.reportLowMemoryException(r,
            key, lme, keyOwners);
      }
    }

    // validation
    if (isSerialExecution) {
       ParRegUtil.verifyContainsKey(r, key, true);
       ParRegUtil.verifyContainsValueForKey(r, key, true);
       ParRegUtil.verifySize(r, beforeSize);
       if (isBridgeConfiguration) {
          // lynn - rework this when bug 36436 is fixed; meanwhile allow any return value for bridge
       }
       else {
         // TODO: what we *really* want is to know whether the above update occurred remotely.
         // isLocal() indicates whether or not the entry is hosted locally, which is *not*
         // the same thing, since we now allow reads to occur from secondary copies.
//         Entry entry = aRegion.getEntry(key);
//         if (returnVal == null || ! returnVal.equals(CacheEvent.NOT_AVAILABLE)) {
//           if ( ! ((ParitionedRegion.Entry)entry).wasInitiallyLocal() ) {
//       Log.getLogWriter().info("updateEntry: entry is " + entry.getClass());
//             // if the returned object from put is a real value or null and the entry is not local...
//             throw new TestException("Expected return value from put to be CacheEvent.NOT_AVAILABLE, but it is " + returnVal);
//           }
//         }
//         else
//         if (returnVal.equals(CacheEvent.NOT_AVAILABLE)) {
//           if ( ((ParitionedRegion.Entry)entry).wasInitiallyLocal() ) {
//             // if the returned object from put NOT_AVAILABLE and the entry local...
//             throw new TestException("Expected return value from put to be either null or a real value, but it is " + returnVal);
//           }
//         }
       }

       // record the current state
       regionSnapshot.put(key, getValueForBB(anObj));
       destroyedKeys.remove(key);
    }
  }

  public static HydraThreadLocal localCriticalState = new HydraThreadLocal();

  public static class EasyInspector implements Inspector {
    public void reportLowMemoryException(Region r, Object key, LowMemoryException lme, Set keyOwners) {}
    public void reportNoLowMemoryException(Region r, Object key, Set keyOwners) {}
    public void expectLowMemoryException(boolean expect, DistributedMember member) {}
    public void waitForAllMembersCriticalOrFail() {}
    public void waitForNoMembersCriticalOrFail() {}
  }

  public static final String LOW_MEMORY_STATE = "expectLowMemoryState";
  private static final Object PUTALL_KEY = "PUTALL_KEY";

  public static class SternInspector implements Inspector {

    public void expectLowMemoryException(final boolean expect, DistributedMember member) {
      if (member == null) {
        throw new NullPointerException();
      }
      SharedMap sm = ResourceManBB.getBB().getSharedMap();
      long v = ResourceManBB.getBB().getSharedCounters().incrementAndRead(ResourceManBB.criticalStateVersion);
      CriticalState newms = new CriticalState(new Long(System.currentTimeMillis()), expect, member, new Long(v));
      Log.getLogWriter().info("Publishing: " + newms);
      CriticalState oldms = (CriticalState) sm.put(LOW_MEMORY_STATE, newms);
      // assert initial condition or serial state changes e.g. crit => no-crit => crit
      boolean serialState = oldms==null || oldms.low != newms.low;
      if (!serialState) {
        throw new IllegalStateException("More than one critical condtion at time not supported.  Old state=" + oldms + ", New state=" + newms);
      }
    }
    
    public void waitForNoMembersCriticalOrFail() {
      TestHelper.waitForCounter(ResourceManBB.getBB(),
          "ResourceManBB.lowCounter",
          ResourceManBB.lowCounter,
          0,
          true,
          TimeUnit.SECONDS.toMillis(ResourceManPrms.getTaskWaitForLowMemSec()),
          500);
    }

    public void waitForAllMembersCriticalOrFail() {
      int allThreads = 0;
      TestConfig tc = TestConfig.getInstance();
      for (String tgName: ResourceManPrms.getTaskThreadGroupNamesDoingEntryOps()) {
        allThreads =+ tc.getThreadGroup(tgName).getTotalThreads();
      }
      assert allThreads > 0;
      TestHelper.waitForCounter(ResourceManBB.getBB(),
          "ResourceManBB.lowCounter",
          ResourceManBB.lowCounter,
          allThreads,
          true,
          TimeUnit.SECONDS.toMillis(ResourceManPrms.getTaskWaitForLowMemSec()),
          500);
    }

    public void reportNoLowMemoryException(Region r, Object key, Set keyOwners) {
      reportLowMemoryException(r, key, null, keyOwners);
    }

    @SuppressWarnings("unchecked")
    public void reportLowMemoryException(Region r, Object key,
        LowMemoryException lme, Set p_keyOwners) {

      if (key == PUTALL_KEY) {
        return; // TODO figure out a way to determine which putAll key caused the exception
      }

      final Set<? extends DistributedMember> keyOwners;
      if (p_keyOwners == null) {
        return;// this is because we are on a client
      } else {
        keyOwners = p_keyOwners;
      }
      final Blackboard bb = ResourceManBB.getBB();
      final CriticalState cs = (CriticalState)bb.getSharedMap().get(LOW_MEMORY_STATE);
      final CriticalState lcs = (CriticalState) localCriticalState.get();
      // assert local critical state follows published critical state
      if (lcs != null && cs == null) {
        throw new IllegalStateException("Local critical state should be null");
      }
      if (lcs != null && cs !=null) {
        final long versionGap = Math.abs(lcs.ver.longValue() - cs.ver.longValue());
        if (versionGap > 1) { // this thread missed at least one full critical change
          Log.getLogWriter().info("Resetting local state, because of a version gap "
              + versionGap + ", local critical state " + lcs
              + " published critical state " + cs);
          localCriticalState.set(cs);
        } else {
          if (versionGap == 1 && lcs.low == cs.low) {
            throw new IllegalStateException("Local critical state " + lcs + " does not agree with published critical state "+ cs);
          }
        }
      }

      final boolean caughtException = lme != null;
      final boolean expectedException = cs != null && cs.low;

      if (!caughtException && !expectedException) {
        if (cs != null) {
          if (cs.low) {
            // confirm expectLowMemory conditions
            throw new IllegalStateException("Published critical state low unexpected value");
          }
          if (lcs != null) {
            localCriticalState.set(null);
            bb.getSharedMap().replace(ACCESSOR_NO_LOW_MEM_TIMESTAMP, null, new Long(System.currentTimeMillis()));
            long numLow = bb.getSharedCounters().decrementAndRead(ResourceManBB.lowCounter);
            //          if (numLow == (numTotalThreads-1)) {
            //            long millis = System.currentTimeMillis() - cs.ts.longValue();
            //            Log.getLogWriter().info("REPORT: Critical down detected after "+millis+" millis");
            //          }
          }
        }
        return;
      }
      if (caughtException && expectedException) {
        final ThresholdsTestStats tstats = getThresholdTestStats();
        if (tstats != null) {
          tstats.incNumLMEs();
        }
        if (! SetUtils.intersectsWith(keyOwners, lme.getCriticalMembers())) {
          throw new TestException("Owners for key " + key + " " + keyOwners 
              + " does not intersect with LME critical members", lme);
        }
        if (keyOwners.contains(cs.mem)) {
          if (lcs == null) {
            // The system is working as designed
            if (tstats != null) {
              long delta = System.currentTimeMillis() - cs.ts.longValue();
              tstats.setMaxLMEDiscoveryTime(delta);
            }
            localCriticalState.set(cs);
            bb.getSharedMap().replace(ThresholdsTest.ACCESSOR_LOW_MEM_TIMESTAMP, null, new Long(System.currentTimeMillis()));
            long numLow = bb.getSharedCounters().incrementAndRead(ResourceManBB.lowCounter);
            Log.getLogWriter().info("numLow:"+numLow);
            if (numLow == 1) {
              long millis = System.currentTimeMillis() - cs.ts.longValue();
              Log.getLogWriter().info("REPORT: Critical up detected after "+millis+" millis");
            }
          }
        } else {
          // Caught the expected exception for a member other than the one we made critical, test problem?
          throw new TestException("Caught exception for the wrong member, keyOwners=" + keyOwners + ", member=" + cs.mem, lme);
        }
        return;
      }
      if (caughtException && !expectedException) {
        final ThresholdsTestStats tstats = getThresholdTestStats();
        if (tstats != null) {
          tstats.incNumLMEs();
        }
        if (! SetUtils.intersectsWith(keyOwners, lme.getCriticalMembers())) {
          throw new TestException("Owners for key " + key + " " + keyOwners 
              + " does not intersect with LME critical members", lme);
        }
        if (cs == null) {
          // initial condition, system should be but is NOT healthy
          // re-set state for this thread
          localCriticalState.set(null);
          Log.getLogWriter().severe("Rethrowing unexpected LowMemoryException", new Exception(lme));
          throw lme; // fail
        } else {
          if (keyOwners.contains(cs.mem)) {
            assert !cs.low; // confirm expectLowMemory conditions
            // Assume the non-critical state has yet to reach this VM, wait for a period
            // of time since the non-critical was published before failing the test
            long tolerateMs = TimeUnit.SECONDS.toMillis(ResourceManPrms.getTaskTolerateLowMemSec());
            long deltaMs = System.currentTimeMillis() - cs.ts.longValue();
            if (deltaMs > tolerateMs) {
              localCriticalState.set(null);
              Log.getLogWriter().severe("Rethrowing unexpected LowMemoryException after being tolerant for " + deltaMs + "ms", new Exception(lme));
              throw lme; // fail
            }
          } else {
            // Caught an unexpected exception for a member other than the one which was made critical
            throw new TestException("Caught unexpected exception for the wrong member, keyOwners=" + keyOwners + ", member=" + cs.mem, lme);
          }
        }
        return;
      }
      if (!caughtException && expectedException) {
        if (keyOwners.contains(cs.mem)) {
          // Only throw (potentially) a test exception if the key is owned by the member
          // which is critical.

          // The critical state has yet to reach this VM, wait for a period
          // of time since the critical state was published on BB before failing the test
          long tolerateMs = TimeUnit.SECONDS.toMillis(ResourceManPrms.getTaskTolerateLowMemSec());
          long deltaMs = System.currentTimeMillis() - cs.ts.longValue();
          if (deltaMs > tolerateMs) {
            localCriticalState.set(null);
            throw new TestException("Expected LowMemoryException after being tolerant for " + deltaMs + "ms"); // fail
          }
        }
        return;
      }
    }
  }
  
  @Override
  public int getOffHeapVerifyTargetCount() {
    return ResourceManPrms.getOffHeapVerifyTargetCount();
  }
 }

class Indexable implements java.io.Serializable {
  public int a;
  public int b;
  public int c;
  public int d;
  public int e;
  public int f;
  public int g;
  public int h;
  public int i;
  public int j;
  public int k;
  public int l;
  public int m;
  public int n;
  public int o;
  public int p;
  public int q;
  public int r;
  public int s;
  public int t;
  public int u;
  public int v;
  public int w;
  public int x;
  public int y;
  public int z;

  public byte[] bytes;


  Indexable(int i,byte[] bytes) {
   this.a = i;
   this.b = i;
   this.c = i;
   this.d = i;
   this.e = i;
   this.f = i;
   this.g = i;
   this.h = i;
   this.i = i;
   this.j = i;
   this.k = i;
   this.l = i;
   this.m = i;
   this.n = i;
   this.o = i;
   this.p = i;
   this.q = i;
   this.r = i;
   this.s = i;
   this.t = i;
   this.u = i;
   this.v = i;
   this.w = i;
   this.x = i;
   this.y = i;
   this.z = i;

   this.bytes = bytes;
  }

  public static class ThresholdsTestStats {
    private static final StatisticsType type;
    private static final int maxLMEDiscoveryTimeId;
    private static final int numLMEsId;
    static {
      StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
      type = f.createType(
        "ThresholdTestStats",
        "Statistics for the ThresholdsTest Hydra test",
        new StatisticDescriptor[] {
            f.createLongGauge(
                "maxLMEDiscovyerTime",
                "The maximum time it takes to receive a LowMemoryException due to a member inspector.reporting a critical memory condition",
                "milliseconds"),
            f.createIntCounter(
                "numLMEs",
                "The number of LowMemoryExceptions received",
                "occurances")
        });
      maxLMEDiscoveryTimeId = type.nameToId("maxLMEDiscovyerTime");
      numLMEsId = type.nameToId("numLMEs");
    }

    private final Statistics stats;

    public ThresholdsTestStats(StatisticsFactory factory) {
      this.stats = factory.createAtomicStatistics(type, "ThresholdsTestStats");
    }

    public void close() {
      this.stats.close();
    }

    public void incNumLMEs() {
      this.stats.incInt(numLMEsId, 1);
    }

    public void setMaxLMEDiscoveryTime(long delta) {
      synchronized(this.stats) {
        if (delta > this.stats.getLong(maxLMEDiscoveryTimeId)) {
          this.stats.setLong(maxLMEDiscoveryTimeId, delta);
        }
      }
    }
  }
}

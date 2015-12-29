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
package rebalance;

import java.util.*;
import java.util.concurrent.CancellationException;

import util.*;
import hydra.*;
import hydra.blackboard.*;
import parReg.*;
import splitBrain.SplitBrainBB;
import cq.CQUtilBB;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.control.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.admin.*;

/** Test class for Basic Rebalancing test cases
 */
public class RebalanceTest {

static private final String cacheServerStr = "cacheserver";

/* The singleton instance of RebalanceTest in this VM */
static public RebalanceTest testInstance;

// instance fields (we cannot put the RebalanceOperation on the BB as 
// it is not serializable)
private RebalanceOperation  rebalanceOp;

// ========================================================================
// CacheServer specific Methods
// ========================================================================
// STARTTASK to generate cacheserver xml file 
public synchronized static void HydraTask_initCacheServerXml() {

   if (testInstance == null) {
      testInstance = new RebalanceTest();
      testInstance.createXml(ConfigPrms.getRegionConfig());
   }
}

/**
 *  Create an xml file (for this cache) without creating cache and regions. 
 *  This task is executed after creating the cache and the region.
 */
public static void HydraTask_createXml() {
  String key = cacheServerStr;
  String xmlFileName = key + ".xml";

   try {
     CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(), ConfigPrms.getRegionConfig(), xmlFileName);
   } catch (HydraRuntimeException e) {      
      throw e;
   }   
}

// TASK to start a cacheserver via the gemfire script using xml
// created by initCacheServerXml in earlier STARTTASK
public synchronized static void HydraTask_rebalanceWithCacheServer() {

   // What's the current state of the PR
   // VM should have already created PR via initialization task
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("HydraTask_rebalanceWithCacheServer() expects hydra client to have created cache and PR via initialization tasks");
   }

   ResourceManager rm = myCache.getResourceManager();
   Set<PartitionRegionInfo> before = PartitionRegionHelper.getPartitionRegionInfo(myCache);

   // clear rebalance/recovery counters
   RebalanceBB.getBB().getSharedCounters().zero(RebalanceBB.recoveryRegionCount);

   // Launch cacheServer to perform a rebalance
   String[] extraArgs = RebalancePrms.getExtraCacheServerArgs();
   String xmlFileName = cacheServerStr + ".xml";

   long numCacheServers = RebalanceBB.getBB().getSharedCounters().incrementAndRead(RebalanceBB.numCacheServers);
   String serverName = cacheServerStr + "_" + numCacheServers;
   Log.getLogWriter().info("Starting up cacheServer " + serverName + " with cache.xml file " + xmlFileName + " for Rebalance");
   GemFireDescription gfd = TestConfig.getInstance().getClientDescription( RemoteTestModule.getMyClientName() ).getGemFireDescription();  
   String offHeapMemorySize = gfd.getOffHeapMemorySize();
   if (offHeapMemorySize != null) {
     extraArgs = Arrays.copyOf(extraArgs, extraArgs.length+1);
     extraArgs[extraArgs.length-1] = " off-heap-memory-size=" + offHeapMemorySize;
   }
   CacheServerHelper.startCacheServer(serverName, xmlFileName, extraArgs);
   Long startTime = new Long(System.currentTimeMillis());
   
   // wait for cacheserver -rebalance to complete
   RebalanceUtil.waitForRecovery();

   Long stopTime = new Long(System.currentTimeMillis());
   Long rebalanceTime = stopTime - startTime;
   Log.getLogWriter().info("rebalance totalTime: " +  rebalanceTime + " ms");

   // How do things look after cacheServer rebalanced
   String regionPath = null;
   String regionName = null;
   for (PartitionRegionInfo prdBefore : before ) {
      Log.getLogWriter().info("Before Rebalance\n" + RebalanceUtil.partitionedRegionDetailsToString(prdBefore));
      regionPath = prdBefore.getRegionPath();
      regionName = RebalanceUtil.getRegionName(prdBefore);
      Region region = myCache.getRegion(regionName);
      PartitionRegionInfo prdAfter = PartitionRegionHelper.getPartitionRegionInfo(region);
      Log.getLogWriter().info("After Rebalance \n" + RebalanceUtil.partitionedRegionDetailsToString(prdAfter));

      if (RebalancePrms.verifyBalance()) {
         RebalanceUtil.isBalanceImproved(regionName, prdBefore.getPartitionMemberInfo(), prdAfter.getPartitionMemberInfo());
         Log.getLogWriter().info("PR for region " + regionName + " is balanced");
      }
   }

   // sleep to avoid conflict over cacheserver status file
   MasterController.sleepForMs(10000);

   // stop (or kill) the cacheServer
   CacheServerHelper.stopCacheServer(serverName);

   // If the hydra client VMs are finished with their work, then so is the rebalancer
   // To coordinate with ParReg and CQ tests
   long parRegCounter = ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop);
   long cqCounter = CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.TimeToStop);
   if (parRegCounter+cqCounter > 0) {
      throw new StopSchedulingOrder("Hydra clients have completed work, stop scheduling rebalance");
   }
}

/**
 *  Create an xml file (for this cache).  Returns vmName 
 *  based on RemoteTestModule info and an xml file for use in cache
 *  initialization.  Returns these two strings in an Array of String
 *    anArray[0] = vmName
 *    anArray[1] = xml file for use in initialization
 *
 *  @param regDescriptName The name of a region description.
 */
public void createXml(String regDescriptName) {
   String key = cacheServerStr;
   String xmlFileName = key + ".xml";

   CacheHelper.createCache(ConfigPrms.getCacheConfig()); 
   
   // create wan gateway senders if configured before creating xml. 
   if(TestConfig.tab().stringAt(GatewaySenderPrms.names, null) != null){     
     GatewaySenderHelper.createGatewaySenders(ConfigPrms.getGatewaySenderConfig()); 
   }
   
   try {
     RegionDescription regionDescr = RegionHelper.getRegionDescription(regDescriptName);
     CacheHelper.generateCacheXmlFile(ConfigPrms.getCacheConfig(), regDescriptName, xmlFileName);
   } catch (HydraRuntimeException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Cache XML file was already created") >= 0) {
         // initially created by STARTTASK
      } else {
         throw e;
      }
   }   
   // create the region (so rebalancing VM can also get PartitionedRegionDetails
   RegionHelper.createRegion(regDescriptName);
}

// ========================================================================
// ADMIN (including JMX) specific Methods
// ========================================================================
/**
 * Rebalance via the Admin API (placeholder)
 * todo@lhughes
 */
public static void HydraTask_adminRebalance() {
   AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
   if (adminDS == null) {
      throw new TestException("AdminDistributedSystem is null.  HydraTask_initializeAdminDS() must be invoked prior to HydraTask_adminRebalance().");
   }
  
   // Get a list of all SystemMembers
   SystemMember[] systemMembers = null;
   try {
      systemMembers = adminDS.getSystemMemberApplications();
   } catch (AdminException ae) {
      throw new TestException("getSystemMemberApplications threw " + ae);
   }

   if (systemMembers.length == 0) {
      Log.getLogWriter().info("Cannot do operations on SystemMembers ... none available");
      return;
   }

   // Display available system members 
   StringBuffer aStr = new StringBuffer();
   aStr.append("Discovered " + systemMembers.length + " SystemMembers");
   for (int i=0; i<systemMembers.length; i++ ) {
      aStr.append("\n");
      aStr.append("   SystemMembers[" + i + "] = " + systemMembers[i].toString());
   }
   Log.getLogWriter().info(aStr.toString());

   // select a random systemMember to target as rebalancer
   SystemMemberCache smCache = null;
   for (int i = 0; i < systemMembers.length; i++) {
      SystemMember sm = systemMembers[i];
      try {
         smCache = sm.getCache();
      } catch (AdminException ae) {
         throw new TestException("Caught exception " + ae);
      } catch (OperationCancelledException oce) {
         Log.getLogWriter().info("Caught OperationCancelledException, continuing with test");
         continue;
      }

      if (smCache == null) {
         // Race with administrating a VM whose connection has been
         // recycled and is still initializing its cache
         Log.getLogWriter().info("Member " + sm + " has no cache");
         continue;
      }

/* --------------------------------------------------------------------
   todo@lhughes - activate rebalance code once APIs available
      SystemMemberResourceManager mgr = smCache.getResourceManager();
      // todo@addResourceListener
      // admin version of RebalanceOperation?
      manager.createRebalanceFactory.start();
      //break;        // break out of loop once we've got a successful rebalance
   --------------------------------------------------------------------*/

      // Some code to execute until we get Rebalance ...
      Set rootRegions = smCache.getRootRegionNames();
      Log.getLogWriter().info("SystemMember " + sm + " Root Regions = " + rootRegions);

      // get a single list of all regions in the cache
      ArrayList aList = getAllRegionNames( smCache );
      StringBuffer displayList = new StringBuffer();
      for (int j = 0; j < aList.size(); j++) {
        displayList.append( aList.get(j) + " \n");
      }
      Log.getLogWriter().fine("regionList for member " + sm + " = \n" + displayList.toString());
   }
}


//---------------------------------------------------------------------------
// test utility methods - todo@lhughes - remove once implementation available
// these are just here to provide work in place of rebalancing
//---------------------------------------------------------------------------
protected static ArrayList getAllRegionNames( SystemMemberCache smCache ) {
   ArrayList regionList = new ArrayList();
   SystemMemberRegion smRegion = null;

   smCache.refresh();

   Set rootRegions = smCache.getRootRegionNames();
   regionList.addAll( rootRegions );

   for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
     String regionName = null;
     try {
       regionName = (String) it.next();
       smRegion = smCache.getRegion( regionName );
     } catch (AdminException ae) {
       throw new AdminTestException(" could not getRegion " + regionName, ae );
     } catch (RegionNotFoundException ex) {
       Log.getLogWriter().fine("Ignoring RegionNotFoundException for "  + regionName);
       continue;
     } catch (RegionDestroyedException de) {
       Log.getLogWriter().fine("Ignoring RegionDestroyedException for " + regionName);
       continue;
     } catch (OperationCancelledException oce) {
       Log.getLogWriter().fine("Ignoring OperationCancelledException for " + regionName);
       continue;
     } catch (Exception e) {
       Throwable cause = e.getCause();
       if (cause == null) {
         throw new TestException(TestHelper.getStackTrace(e));
       }
       if (cause.toString().contains("OperationCancelledException")) {
         Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
       } else {
         throw new TestException(TestHelper.getStackTrace(e));
       }
     }
     regionList.addAll( getRegionNames( smCache, smRegion ) );
   }
   return regionList;
}

protected static ArrayList getRegionNames( SystemMemberCache smCache, SystemMemberRegion smRegion ) {
  ArrayList aList = new ArrayList();
  String subregionName = null;

  Set subRegions = null;
  try {
    smRegion.refresh();
    subRegions = smRegion.getSubregionNames();
  } catch (Exception e) {
    Log.getLogWriter().fine("getRegionNames caught " + e);
    return aList;
  }

  for (Iterator it = subRegions.iterator(); it.hasNext();) {
    subregionName = new String( smRegion.getFullPath() + "/" + (String)it.next());
    aList.add( subregionName );

    SystemMemberRegion aRegion = null;
    try {
      aRegion = smCache.getRegion( subregionName );
    } catch (AdminException ae) {
      throw new AdminTestException(" Could not get subregion given subregionName " + subregionName, ae);
    } catch (RegionNotFoundException ex) {
      Log.getLogWriter().fine("Ignoring RegionNotFoundException for " + subregionName);
      continue;
    } catch (RegionDestroyedException dx) {
      Log.getLogWriter().fine("Ignoring RegionDestroyedException for " + subregionName);
      continue;
    }

    if (aRegion != null) {
      aList.addAll( getRegionNames(smCache, aRegion) );
    }
  }
  return aList;
}

// ========================================================================
// Cache API Specific Methods
// ========================================================================

/**
 * Rebalance via the cache api.  Verify that balance has improved.
 */
public static void HydraTask_rebalanceTask() {
   Log.getLogWriter().info("In HydraTask_rebalanceTask");
   if (testInstance == null) {
      testInstance = new RebalanceTest();
   }
   testInstance.rebalance();
}

public void rebalance() {

   // Check to see if time to stop (must be done at beginning for cases where
   // rebalancer is killed during a work-based test
   String taskType = RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString();
   if (taskType.equalsIgnoreCase("TASK")) {
      // If the hydra client VMs are finished with their work, then so is the rebalancer
      // To coordinate with both ParReg and CQ tests
      long parRegCounter = ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop);
      long cqCounter = CQUtilBB.getBB().getSharedCounters().read(CQUtilBB.TimeToStop);
      if (parRegCounter+cqCounter > 0) {
         throw new StopSchedulingOrder("Hydra clients have completed work, stop scheduling rebalance");
      }
   }

   // rebalance splitBrain tests which use the NetDownResourceObserver need a 
   // way to know when the network has been dropped (including time for recovery)
   SplitBrainBB.getBB().getSharedCounters().zero(SplitBrainBB.dropConnectionComplete);

   // VM should have already created PR via initialization task
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("HydraTask_rebalanceTask() expects hydra client to have created cache and PR via initialization tasks");
   }

   ResourceManager rm = myCache.getResourceManager();
   setResourceObserver();

   RebalanceFactory factory = rm.createRebalanceFactory();

   // How do things look before we rebalance
   Set<PartitionRegionInfo> prdSet = PartitionRegionHelper.getPartitionRegionInfo(myCache);
   for (PartitionRegionInfo prd : prdSet) {
      Log.getLogWriter().info(RebalanceUtil.partitionedRegionDetailsToString(prd));
   }

   RebalanceEventCountersBB.zeroAllCounters();   // clear out for next round of rebalance

   // we cannot call simulate with kill tests as simulate will also invoke
   // the ResourceObserver callbacks
   if (RebalancePrms.getTargetEvent() == null) {
      Log.getLogWriter().info("Calling simulate ...");
      RebalanceOperation simOp = factory.simulate();
      Log.getLogWriter().info("Called simulate ...");
      // what does the simulate operation say we'll do?
      RebalanceResults simResults = null;
      try {
         simResults = simOp.getResults();
         Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(simResults, "simulate"));
      } catch (CancellationException ce) {
           Log.getLogWriter().info("getResults() caught exception " + ce);
      } catch (InterruptedException ie) {
           Log.getLogWriter().info("getResults() caught exception " + ie);
      }

      // We shouldn't have gotten any events during simulate ...
      checkRebalanceEventCounters(simResults, false);

      RebalanceEventCountersBB.zeroAllCounters();   // clear out for next round of rebalance
   }

   // Save state (partitionedRegionDetails) to BB (before rebalance starts)
   prdSet = PartitionRegionHelper.getPartitionRegionInfo(myCache);
   Log.getLogWriter().info("Writing PartitionedRegionDetails to BB (before)");
   RebalanceBB.getBB().getSharedMap().put(RebalanceBB.PartitionedRegionDetails_before, prdSet);

   RebalanceBB.getBB().getSharedCounters().zero(RebalanceBB.processedStart);

   String action = RebalancePrms.getRebalanceAction();
   Long startTime = new Long(System.currentTimeMillis());
   if (action.equalsIgnoreCase("kill")) {
      killTargetVm(factory);
   } else if (action.equalsIgnoreCase("cancel")) {
      cancelRebalance(factory);
   } else {
     Log.getLogWriter().info("Starting rebalance ...");
     this.rebalanceOp = factory.start();
     Log.getLogWriter().info("Started rebalance.");
   }

   // always wait for results (even on killTarget and cancel)
   RebalanceResults rebalanceResults = null;
   try {
      rebalanceResults = this.rebalanceOp.getResults();
   } catch (CancellationException ce) {
      if (action.equalsIgnoreCase("cancel")) {
         Log.getLogWriter().info("getResults() caught exception " + ce + " expected, continuing test execution");
         return;
      } else {
         Log.getLogWriter().info("getResults() caught exception " + ce);
         throw new TestException("Unexpected exception " + TestHelper.getStackTrace(ce));
      }
   } catch (InterruptedException ie) {
      Log.getLogWriter().info("getResults() caught exception " + ie);
      throw new TestException("Unexpected exception " + TestHelper.getStackTrace(ie));
   }
   Long stopTime = new Long(System.currentTimeMillis());
   Long rebalanceTime = stopTime - startTime;
   Log.getLogWriter().info("Completed rebalance in " + rebalanceTime + " ms, results = " + this.rebalanceOp.toString());
   
   Log.getLogWriter().info(RebalanceUtil.RebalanceResultsToString(rebalanceResults, "rebalance"));
   
   if (RebalancePrms.verifyBalance()) {
      // compare to simulate predictions - todo@lhughes -> implement

      // this verifies both heap & primary counts
      RebalanceUtil.isBalanceImproved(rebalanceResults); 
      Log.getLogWriter().info("PRs are balanced");
   }

   // For netdown tests, we cannot return from rebalance until we know
   // that the async NetDownResourceObserver has dropped the network and
   // waited for SplitBrainPrms-dropTimeWaitSec.
   if (RebalancePrms.waitForNetworkDrop()) {
     TestHelper.waitForCounter(SplitBrainBB.getBB(), "dropConnectionComplete", SplitBrainBB.dropConnectionComplete, 1, true, 1830000);
   }
}

/**
 *  Verifies whether or not the expected PartitionRebalanceEvents were 
 *  processed.  If the operation was simulate, eventsExpected should be
 *  false and this method will verify that we did not receive any events.
 *  Otherwise, it verifies the bucket creates, destroys and transfers as
 *  well as the primaryTransfers for correctness.
 */
private static void checkRebalanceEventCounters(RebalanceResults results, boolean eventsExpected) {

   ArrayList aList = new ArrayList();
   aList.add(new ExpCounterValue(RebalanceEventCountersBB.BUCKET_CREATES, eventsExpected ? (long)results.getTotalBucketCreatesCompleted():0));
   aList.add(new ExpCounterValue(RebalanceEventCountersBB.BUCKET_TRANSFERS, eventsExpected ? (long)results.getTotalBucketTransfersCompleted():0));
   aList.add(new ExpCounterValue(RebalanceEventCountersBB.PRIMARY_TRANSFERS, eventsExpected ? (long)results.getTotalPrimaryTransfersCompleted():0));
   RebalanceEventCountersBB.checkEventCounters(aList);
}

public static void setResourceObserver() {
   // VM should have already created PR via initialization task
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("setResourceObserver() expects hydra client to have created cache and PR via initialization tasks");
   }

   ResourceManager rm = myCache.getResourceManager();

   // Setup test hooks to listen for rebalance/recovery start/finish
   InternalResourceManager.ResourceObserver ro = RebalancePrms.getResourceObserver();
   if (ro != null) {
      ((InternalResourceManager)rm).setResourceObserver(ro);
      Log.getLogWriter().info("Installed ResourceObserver " + ro.toString());
   }
}

public static void HydraTask_checkPartitionedRegionDetails() {
   Log.getLogWriter().info("In HydraTask_checkPartitionedRegionDetails");

   // Get state (prior to kill/restart)
   Set<PartitionRegionInfo> before = (Set<PartitionRegionInfo>)RebalanceBB.getBB().getSharedMap().get(RebalanceBB.PartitionedRegionDetails_before);

   // VM should have already created PR via initialization task
   Cache myCache = CacheHelper.getCache();
   if (myCache == null) {
     throw new TestException("HydraTask_checkPartitionedRegionDetails() expects hydra client to have created cache and PR via initialization tasks");
   }

   ResourceManager rm = myCache.getResourceManager();
   RebalanceFactory factory = rm.createRebalanceFactory();

   // How do things look upon re-start (we should be 'no worse off')
   String regionPath = null;
   String regionName = null; 
   for (PartitionRegionInfo prdBefore : before ) {
      Log.getLogWriter().info("Before Rebalancing \n" + RebalanceUtil.partitionedRegionDetailsToString(prdBefore));
      regionPath = prdBefore.getRegionPath();
      regionName = RebalanceUtil.getRegionName(prdBefore);
      Region region = myCache.getRegion(regionName);
      PartitionRegionInfo prdAfter = PartitionRegionHelper.getPartitionRegionInfo(region);
      Log.getLogWriter().info("After Rebalancing \n" + RebalanceUtil.partitionedRegionDetailsToString(prdAfter));
      if (RebalancePrms.verifyBalance()) {
         RebalanceUtil.isBalanceImproved(regionName, prdBefore.getPartitionMemberInfo(), prdAfter.getPartitionMemberInfo());
         RebalanceUtil.primariesBalanced(prdBefore);
         Log.getLogWriter().info("PR for region " + regionName + " is balanced");
      }
   }
}

//=============================================================================
// The HAResourceObserver executes a 'notify' on this syncObject to 
// coordinate between client vms and the ResourceObserver (listener)
//=============================================================================
public static Object listenerSyncObject = new Object();

/* killTargetVm when targetEvent is processed by the HAResourceObserver.
 * 
 */
public void killTargetVm(final RebalanceFactory factory) {
  String targetEvent = RebalancePrms.getTargetEvent();

  // start a thread to do the rebalance ... once started, wait on listenerSyncObject
  // RebalanceObserver will ntoify on listenerSyncObject once targetEvent processed
  Thread rebalanceThread = new Thread(new Runnable() {
    public void run() {

      // wait for thread to execute wait on our listenerSyncObject
      int actionDelaySecs = RebalancePrms.getActionDelaySecs();
      try {
        Thread.sleep(actionDelaySecs * 1000);
      } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      Log.getLogWriter().info("Starting rebalance ...");
      RebalanceTest.testInstance.rebalanceOp = factory.start();
      Log.getLogWriter().info("Started rebalance.");
    }
  });
  rebalanceThread.start();
   
  Log.getLogWriter().info("In killTargetVm with targetEvent " + targetEvent);

  synchronized(RebalanceTest.listenerSyncObject) {
    try {
      Log.getLogWriter().info("killTargetVm waiting on listenerSyncObject for " + targetEvent);
      RebalanceTest.listenerSyncObject.wait();
    } catch (InterruptedException e) {
      Log.getLogWriter().info("killTargetVm interrupted: VM not killed");
    }
  }

   ClientVmInfo targetVm = (ClientVmInfo)RebalanceBB.getBB().getSharedMap().get(RebalanceBB.targetVmInfo);
   Log.getLogWriter().info("killTargetVm notified of " + targetEvent + " for targetVm " + targetVm.toString());

   // If we are handling ANY ResourceObserver event, we know we've processed
   RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.processedStart);
   RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.numTargetEventsProcessed);

   // If the rebalancer is the target we must use async
   // stop/start (as we are killing the VM executing the stop)
   // Otherwise, use sync stop/start so we'll complete this action before 
   // killing the next source or destination target

   try {
      if (targetVm.getVmid() == RemoteTestModule.getMyVmid()) {
         // rebalancer VM
         ClientVmMgr.stopAsync("killTargetVm killing rebalancer " + targetVm.toString(),
                               ClientVmMgr.MEAN_KILL,  // kill -TERM
                               ClientVmMgr.IMMEDIATE,
                               targetVm);
      } else {
         // source or destination VM for movingBucket or movingPrimary
         ClientVmMgr.stop("killTargetVm killing " + targetVm.toString(),
                               ClientVmMgr.MEAN_KILL,  // kill -TERM
                               ClientVmMgr.ON_DEMAND,
                               targetVm);
         ClientVmMgr.start("killTargetVm restarting " + targetVm.toString(), targetVm);
      }
   } catch (ClientVmNotFoundException e) {
      Log.getLogWriter().info("Failed to kill " + targetVm.toString() + ", caught Exception " + e + " (expected with concurrent execution); continuing with test");
   }
}

/**  
 * Cancel rebalance when targetEvent is processed
 * See rebalance.HAResourceObserver which coordinates listener/vm
 * activities via the listenerSyncObject
 */
public void cancelRebalance(final RebalanceFactory factory) {
   String targetEvent = RebalancePrms.getTargetEvent();
   Log.getLogWriter().info("In cancelRebalance with targetEvent " + targetEvent);

  // start a thread to do the rebalance ... once started, wait on listenerSyncObject
  // RebalanceObserver will notify the listenerSyncObject once targetEvent processed
  Thread rebalanceThread = new Thread(new Runnable() {
    public void run() {
      // wait for thread to execute wait on our listenerSyncObject
      int actionDelaySecs = RebalancePrms.getActionDelaySecs();
      try {
        Thread.sleep(actionDelaySecs * 1000);
      } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      Log.getLogWriter().info("Starting rebalance ...");
      RebalanceTest.testInstance.rebalanceOp = factory.start();
      Log.getLogWriter().info("Started rebalance.");
    }
  });
  rebalanceThread.start();

  synchronized(RebalanceTest.listenerSyncObject) {
    try {
      Log.getLogWriter().info("cancelRebalance waiting on listenerSyncObject for " + targetEvent);
      RebalanceTest.listenerSyncObject.wait();
    } catch (InterruptedException e) {
      Log.getLogWriter().info("cancelRebalance interrupted: Rebalance not cancelled");
    }
  }

   // If we are handling ANY ResourceObserver event, we know we've processed
   RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.processedStart);
   RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.numTargetEventsProcessed);

   // Cancel active rebalance operation
   if (rebalanceOp.cancel()) {
      Log.getLogWriter().info("RebalanceOperation cancelled");
   } else {
      Log.getLogWriter().info("RebalanceOperation was NOT cancelled");
   }

   Log.getLogWriter().info("cancelRebalance notified of " + targetEvent);

   // Verify that we are "no worse off" than before 
   ResourceManager rm = CacheHelper.getCache().getResourceManager();
   Set<PartitionRegionInfo> before = (Set<PartitionRegionInfo>)RebalanceBB.getBB().getSharedMap().get(RebalanceBB.PartitionedRegionDetails_before);

   String regionPath = null;
   String regionName = null;
   for (PartitionRegionInfo prdBefore : before ) {
      Log.getLogWriter().info("Before Rebalance\n" + RebalanceUtil.partitionedRegionDetailsToString(prdBefore));
      regionPath = prdBefore.getRegionPath();
      regionName = RebalanceUtil.getRegionName(prdBefore);
      Region region = CacheHelper.getCache().getRegion(regionName);
      PartitionRegionInfo prdAfter = PartitionRegionHelper.getPartitionRegionInfo(region);
      Log.getLogWriter().info("After Rebalance \n" + RebalanceUtil.partitionedRegionDetailsToString(prdAfter));

      if (RebalancePrms.verifyBalance()) {
         RebalanceUtil.isBalanceImproved(regionName, prdBefore.getPartitionMemberInfo(), prdAfter.getPartitionMemberInfo());
         Log.getLogWriter().info("PR for region " + regionName + " is balanced");
      }
   }

   // Kill the targetVm (based on targetEvent) to ensure more work 
   // to do on next round.  See rebalance.HAResourceObserver.
   // Note that when we are running as a CLOSETASK we don't want to 
   // recycle the rebalancer (and we cannot execute dynamic stop/start
   // tasks from anything other than a TASK).
   String taskType = RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString();
   if (taskType.equalsIgnoreCase("TASK")) {
   // If the rebalancer is the target (rebalancingStarted), we must use async
   // stop/start (as we are killing the VM executing the stop)
   // Otherwise, use sync stop/start so we'll complete this action before 
   // killing the next source or destination target
      ClientVmInfo targetVm = (ClientVmInfo)RebalanceBB.getBB().getSharedMap().get(RebalanceBB.targetVmInfo);
      try {
         if (targetEvent.equalsIgnoreCase("rebalancingStarted")) {
            ClientVmMgr.stopAsync("cancelRebalance killing " + targetVm.toString(),
                                  ClientVmMgr.MEAN_KILL,  // kill -TERM
                                  ClientVmMgr.IMMEDIATE,
                                  targetVm);
         } else {
            ClientVmMgr.stop("cancelRebalance killing " + targetVm.toString(),
                                  ClientVmMgr.MEAN_KILL,  // kill -TERM
                                  ClientVmMgr.ON_DEMAND,
                                  targetVm);
            ClientVmMgr.start("cancelRebalance restarting" + targetVm.toString(), targetVm);
         }
      } catch (ClientVmNotFoundException e) {
         Log.getLogWriter().info("Failed to kill " + targetVm.toString() + ", caught Exception " + e + " (expected with concurrent execution); continuing with test");
      }
   }
}

public static void HydraTask_verifyTargetEventsProcessed() {
   String targetEvent = RebalancePrms.getTargetEvent();
   long numEvents = RebalanceBB.getBB().getSharedCounters().read(RebalanceBB.numTargetEventsProcessed);
   if (numEvents == 0) {
      throw new TestException("Test config or tuning issue: Test did not kill and VMs based on " + targetEvent + " events");
   } else {
      Log.getLogWriter().info("Executed " + numEvents + " kills based on " + targetEvent + " events");
   }
}

//=============================================================================

// ========================================================================
// Utility Methods
// ========================================================================
/**
 * Randomly stop and restart vms which are not rebalancing (rebalance must 
 * be part of the clientName).  Tests which invoke this must also call
 * util.StopStartVMs.StopStart_init as a INITTASK (to write clientNames
 * to the Blackboard).
*/
public static void HydraTask_stopStartNonAdminVMs() {
   int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
   int randInt =  TestConfig.tab().getRandGen().nextInt(1, numVMsToStop);
   Object[] objArr = StopStartVMs.getOtherVMsWithExclude(randInt, "rebalance");
   List clientVmInfoList = (List)(objArr[0]);
   List stopModeList = (List)(objArr[1]);
   StopStartVMs.stopStartVMs(clientVmInfoList, stopModeList);
}
}

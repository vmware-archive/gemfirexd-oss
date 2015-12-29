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
import java.io.*;

import util.*;
import hydra.*;
import hydra.blackboard.*;
import perffmwk.*;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * 
 * WAN (ParReg Redundancy) Recovery Test
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.0
 */
public class WANRecoveryTest extends util.OperationsClient {

  protected static final int ITERATIONS = 1000;

  // Single instance in this VM
  static protected WANRecoveryTest testInstance;
  
  // instance variables
  protected RandomValues randomValues = null;
  
  //============================================================================
  // LOCATOR INITTASKS/CLOSETASKS
  //============================================================================

  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its distributed system.
   */
  public static void startAndConnectLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   * Stops a locator.
   */
  public static void stopLocatorTask() {
    DistributedSystemHelper.stopLocator();
  }


/* ======================================================================== */
/* hydra task methods */
/* ======================================================================== */
// Server methods
    public synchronized static void HydraTask_initializeBridgeServer() {
       if (testInstance == null) {
          testInstance = new WANRecoveryTest();
          testInstance.initializeOperationsClient();
          testInstance.initializeBridgeServer();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeServer() {

      CacheHelper.createCache(ConfigPrms.getCacheConfig());
      RegionHelper.createRegion(ConfigPrms.getRegionConfig());
      BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());

      // Create GatewayHub, note that the Gateway cannot be started until
      // all Gateways have been created (info placed on BB for hydra to 
      // connect appropriate VMs/ports
      createGatewayHub();

      randomValues = new RandomValues();
   }
   
   /**
    * Add Gateways and start the GatewayHub
    *
    * Note:  This must be done after all servers have been initialized (and
    * invoked createGatewayHub(), so the hydra WAN blackboard knows about 
    * all the available servers and endpoints.
    */
    public synchronized static void HydraTask_startGatewayHubTask() {
       testInstance.startGatewayHub(ConfigPrms.getGatewayConfig());
    }

    /**
     * Starts a gateway hub in a VM that previously created one, after creating
     * gateways.
     */
    protected void startGatewayHub(String gatewayConfig) {
      GatewayHubHelper.addGateways(gatewayConfig);
      GatewayHubHelper.startGatewayHub();
 
      // only target the primary GatewayHub in WAN Site #1
      GatewayHub hub = GatewayHubHelper.getGatewayHub();
      if (hub.getStartupPolicy().equals(GatewayHub.STARTUP_POLICY_PRIMARY)) {
         parReg.ParRegBB.getBB().getSharedMap().put(RecoveryStopStart.DataStoreVmStr + RemoteTestModule.getMyVmid(), new Integer(RemoteTestModule.getMyVmid()));
      }
    }

   /**
    *  Setup DynamicRegionFactory and create non-dynamic parent regions
    */
    public synchronized static void HydraTask_initializeBridgeClient() {
       if (testInstance == null) {
          testInstance = new WANRecoveryTest();
          testInstance.initializeOperationsClient();
          testInstance.initializeBridgeClient();
       }
    }

    /* 
     * Creates cache and region (CacheHelper/RegionHelper)
     */
    protected void initializeBridgeClient() {
       // create cache/region 
       if (CacheHelper.getCache() == null) {
         CacheHelper.createCache(ConfigPrms.getCacheConfig());
         Region aRegion = RegionHelper.createRegion(ConfigPrms.getRegionConfig());
         aRegion.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS_VALUES);
       }
       randomValues = new RandomValues();
    }

   /* 
    * Creates the GatewayHub (if configured)
    */
   protected void createGatewayHub() {
      // Gateway initialization (if needed)
      String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();
      if (gatewayHubConfig != null) {
         GatewayHubHelper.createGatewayHub(gatewayHubConfig);
      }
   }

   /**
    * Recycle primary gateway in WAN Site #1
    * (to force redundancy recovery)
    */
    public static void HydraTask_recyclePrimary() {
       testInstance.recyclePrimary();
    }

    protected void recyclePrimary() {
      Log.getLogWriter().info("Invoked recyclePrimary");
      long executionNumber = WANRecoveryBB.getBB().getSharedCounters().incrementAndRead(WANRecoveryBB.executionNumber);
      Log.getLogWriter().info("recyclePrimary EXECUTION_NUMBER = " + executionNumber);
      int maxExecutions = WANRecoveryPrms.getMaxExecutions();
      if (executionNumber == maxExecutions) {
         Log.getLogWriter().info("Last round of execution, maxExecutions = " + maxExecutions);
      }

      DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
      Cache theCache = CacheHelper.getCache();

      // wait for Publishers (putSequentialKeys task) to be active
      TestHelper.waitForCounter(WANRecoveryBB.getBB(), "numPublishers", WANRecoveryBB.numPublishers, 1, false, 500);
      long numPublishers = WANRecoveryBB.getBB().getSharedCounters().read(WANRecoveryBB.numPublishers);
      TestHelper.waitForCounter(WANRecoveryBB.getBB(), "publishing", WANRecoveryBB.publishing, numPublishers, true, 180000);

      // recycle one of the dataStore VMs (not any particular one)
      RecoveryStopStart.HydraTask_stopStartDataStoreVm();

      // Don't sync up until all edge clients have finished publishing
      TestHelper.waitForCounter(WANRecoveryBB.getBB(), "donePublishing", WANRecoveryBB.donePublishing, numPublishers, true, 60000);
      // Give the clients a few seconds to do SharedLock.getSharedCondition.await() (or we will not notify all clients to continue)
      MasterController.sleepForMs(5000);

      // Notify publishers that the we've been forcefully disconnected
      WANRecoveryBB.getBB().getSharedCounters().zero(WANRecoveryBB.validationComplete);
      Log.getLogWriter().info("Cleared WANRecoveryBB.validationComplete counter");

      SharedLock hcLock = WANRecoveryBB.getBB().getSharedLock();
      hcLock.lock();
      SharedCondition cond = hcLock.getCondition(WANRecoveryBB.recyclePrimaryComplete);
      Log.getLogWriter().info("notifying all VMs for WANRecoveryBB.SharedLock.SharedCondition." + WANRecoveryBB.recyclePrimaryComplete);
      cond.signalAll();
      Log.getLogWriter().info("notifed all VMs for WANRecoveryBB.SharedLock.SharedCondition." + WANRecoveryBB.recyclePrimaryComplete);
      hcLock.unlock();
      Log.getLogWriter().info("unlocked WANRecoveryBB.SharedLock");

      if (executionNumber == maxExecutions) {
         throw new StopSchedulingOrder("numExecutions = " + executionNumber);
      }
    }

    /**
     *  Performs ITERATIONS puts on a unique entry in TestRegion
     *  Each assignment of this task gets a new key from the NameFactory, and then
     *  updates that key with sequential values from 1 -> ITERATIONS.  This
     *  allows the client listeners to verify incremental updates on the key and 
     *  for a final CLOSETASK validator to verify that all clients have all keys with the 
     *  correct ending value (ITERATIONS).
     *  
     */
    public static void HydraTask_putSequentialKeys() {
       testInstance.putSequentialKeys();
       testInstance.validateSequentialKeys();
    }

    static Boolean firstTimePublishing = Boolean.TRUE;
    protected void putSequentialKeys() {

       // One time only, we need to advertise how many publishers are in this test
       synchronized (firstTimePublishing) {
          if (firstTimePublishing.equals(Boolean.TRUE)) {
             long numPublishers = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
             WANRecoveryBB.getBB().getSharedCounters().setIfLarger(WANRecoveryBB.numPublishers, numPublishers);
             firstTimePublishing = Boolean.FALSE;
          }
       }

       // Check for any Exceptions posted to BB by EventListeners
       TestHelper.checkForEventError(WANRecoveryBB.getBB());

       long executionNumber = WANRecoveryBB.getBB().getSharedCounters().read(WANRecoveryBB.executionNumber);
       Log.getLogWriter().info("putSequentialKeys EXECUTION_NUMBER = " + executionNumber);
       int maxExecutions = WANRecoveryPrms.getMaxExecutions();
       if (executionNumber == maxExecutions) {
          Log.getLogWriter().info("Last round of execution, maxExecutions = " + maxExecutions);
       }

       // Work: select randomRegion, nextKey and putValues 1->1000 for key
       Region aRegion = getRandomRegion(true);
       Log.getLogWriter().info("Working on Region " + aRegion.getName());

       String key = NameFactory.getNextPositiveObjectName();
       Long val = new Long(NameFactory.getCounterForName(key));

       Log.getLogWriter().info("Working on key " + key + " in Region " + aRegion.getFullPath());
       for (int i = 1; i <= ITERATIONS; i++) {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
          aRegion.put(key, new ValueHolder(val, randomValues, new Integer(i)));
          } else {
             aRegion.replace(key, new ValueHolder(val, randomValues, new Integer(i)));
          }
          if (i == ITERATIONS/2) {
             // Notify recyclePrimary that we're publishing
             long ctr = WANRecoveryBB.getBB().getSharedCounters().incrementAndRead(WANRecoveryBB.publishing);
             Log.getLogWriter().info("putSequentialKeys: incremented WANRecoveryBB.publishing = " + ctr);
          }
       }
       long ctr = WANRecoveryBB.getBB().getSharedCounters().incrementAndRead(WANRecoveryBB.donePublishing);
       Log.getLogWriter().info("putSequentialKeys: incremented WANRecoveryBB.donePublishing = " + ctr);

       // recyclePrimary will let us know when we can continue again
       SharedLock hcLock = WANRecoveryBB.getBB().getSharedLock();
       hcLock.lock();
       SharedCondition cond = hcLock.getCondition(WANRecoveryBB.recyclePrimaryComplete);
       Log.getLogWriter().info("Waiting to be notified for WANRecoveryBB.SharedLock.SharedCondition." + WANRecoveryBB.recyclePrimaryComplete);
       try {
          cond.await();
          Log.getLogWriter().info("Notified for WANRecoveryBB.SharedLock.SharedCondition." + WANRecoveryBB.recyclePrimaryComplete);
       } catch (InterruptedException e) {
          Log.getLogWriter().info("SharedCondition await() interrupted, continuing ...");
       } finally {
          hcLock.unlock();
          Log.getLogWriter().info("Unlocked for WANRecoveryBB.SharedLock");
       }
       WANRecoveryBB.getBB().getSharedCounters().zero(WANRecoveryBB.publishing);
       WANRecoveryBB.getBB().getSharedCounters().zero(WANRecoveryBB.donePublishing);
       Log.getLogWriter().info("putSequentialKeys: reset WANRecoveryBB publishing and donePublishing counters");

       if (executionNumber == maxExecutions) {
          throw new StopSchedulingOrder("numExecutions = " + executionNumber);
       }
    }

    /**
     *  Performs puts/gets on entries in TestRegion
     */
    public static void HydraTask_doEntryOperations() {
       testInstance.doEntryOperations();
    }

    protected void doEntryOperations() {
       Region parentRegion;
       Region aRegion; 

       GsRandom rng = TestConfig.tab().getRandGen();
       aRegion = getRandomRegion(true);
       Log.getLogWriter().info("Working on Region " + aRegion.getName());
       doEntryOperations(aRegion);
    }

    /**
     * Close the cache (and disconnect from the DS)
     * This should prevent the ShutdownException: no down protocol available!
     * during Shutdown (if locators (client vms) are shut down prior to 
     * application client vms
     */
    public static void closeCacheAndDisconnectFromDS() {
       CacheHelper.closeCache();
       DistributedSystemHelper.disconnect();
    }

   /**
    *  This validation method coordinates client operations/validation with 
    *  recyclePrimary 
    *
    *  Called directly from putSequentialKeys task which coordinates 
    *  publishing, recyclePrimary and validation by pausing and waiting 
    *  for activities to sync up.
    *
    *  Use validateKeysInRegion for concurrent (non-pausing) tests
    *  which use doEntryOperations() on randomKeys vs. putSequentialKeys/Values.
    */
    protected void validateSequentialKeys() {

       // Wait for Queues to drain
       SilenceListener.waitForSilence(30, 5000);

       Set<Region<?,?>> rootRegions = CacheHelper.getCache().rootRegions();
       for (Region aRegion : rootRegions ) {
          validateRegion(aRegion, true);
       }

       Log.getLogWriter().info("Validation complete");

       WANRecoveryBB.getBB().getSharedCounters().increment(WANRecoveryBB.validationComplete);
       long numPublishers = WANRecoveryBB.getBB().getSharedCounters().read(WANRecoveryBB.numPublishers);
       TestHelper.waitForCounter(WANRecoveryBB.getBB(), "validationComplete", WANRecoveryBB.validationComplete, numPublishers, true, 120000);
    }

   /**
    *  This validation task does not do any coordination between the 
    *  publishing, recyclePrimary and validation.  It should be used as a 
    *  one time call at the end of the test (as a CLOSETASK).
    *
    *  Verifies the region has the same keySet as the server region and
    *  that ValueHolder.myValue is correct for each key.
    *
    *  concurrent (non-pausing) tests should use HydraCloseTask_validateKeysInRegion().
    */
    public static void HydraCloseTask_validateKeysInRegion() {
       testInstance.validateKeysInRegion();
    }

    protected void validateKeysInRegion() {

       // Wait for Queues to drain
       SilenceListener.waitForSilence(30, 5000);

       Region aRegion = null;
       Set rootRegions = CacheHelper.getCache().rootRegions();
       for (Iterator rit = rootRegions.iterator(); rit.hasNext(); ) {
          aRegion = (Region)rit.next();
          Set subRegions = aRegion.subregions(true);
          for (Iterator sit = subRegions.iterator(); sit.hasNext();) {
             aRegion = (Region)sit.next();
             validateRegion(aRegion, false);
          }
       }
       Log.getLogWriter().info("Validation complete");
    }

    protected void validateRegion(Region aRegion, boolean verifyModVal) {

       Log.getLogWriter().info("Validating entries for Region " + aRegion.getName());

       // All clients/servers should have all keys
       Set serverKeys = aRegion.keySetOnServer();
       int expectedRegionSize = serverKeys.size();

       Set localKeys = aRegion.keySet();
       int regionSize = localKeys.size();
 
       // If region is empty (on client & server), there's no need to go further
       if ((regionSize == 0) && (expectedRegionSize == 0)) {
          return;
       }

       StringBuffer aStr = new StringBuffer();
       Set myEntries = aRegion.entrySet();
       Object[] entries = myEntries.toArray();
       Comparator myComparator = new RegionEntryComparator();
       java.util.Arrays.sort(entries, myComparator);

       Log.getLogWriter().info("Checking " + regionSize + " entries against " + expectedRegionSize + " entries in the server's region");

       aStr.append("Expecting " + expectedRegionSize + " entries in Region " + aRegion.getFullPath() + ", found " + entries.length + "\n");

       for (int i=0; i < entries.length; i++) {
          Region.Entry entry = (Region.Entry)entries[i];
          String key = (String)entry.getKey();
          BaseValueHolder val = (BaseValueHolder)entry.getValue();
          if (verifyModVal) {
             aStr.append("\t" + key + " : " + val.modVal + "\n");
          } else {
             if (val != null) {
                aStr.append("\t" + key + " : " + val.myValue + "\n");
             } else {
                aStr.append("\t" + key + " : " + " null\n");
             }
          }
       }

       // Okay to display for small region sizes
       if (regionSize <= 10) {
          Log.getLogWriter().info(aStr.toString());
       }
       
       aStr = new StringBuffer();
       if (regionSize != expectedRegionSize) {
          aStr.append("Expected " + expectedRegionSize + " keys in Region " + aRegion.getFullPath() + " but found " + regionSize + "\n");
       }

       Log.getLogWriter().info("Checking for missing or extra keys in client region");
       // Extra keys (not in the server region)?
       List unexpectedKeys = new ArrayList(localKeys);
       unexpectedKeys.removeAll(serverKeys);
       if (unexpectedKeys.size() > 0) {
          aStr.append("Extra keys (not found on server): " + unexpectedKeys + "\n");
       } 

       // Are we missing keys (found in server region)?
       List missingKeys= new ArrayList(serverKeys);
       missingKeys.removeAll(localKeys);
       if (missingKeys.size() > 0) { 
          aStr.append("Missing keys (found on server, but not locally) = " + missingKeys + "\n");
       }
 
       if (aStr.length() > 0) {
          throw new TestException(aStr.toString() + " " + TestHelper.getStackTrace());
       }

       // All entries should have value ITERATIONS (verifyModVal)
       // skipped/duplicate values will be detected/reported by Listeners
       for (int i=0; i < entries.length; i++) {
          Region.Entry entry = (Region.Entry)entries[i];
          String key = (String)entry.getKey();
          BaseValueHolder val = (BaseValueHolder)entry.getValue();

          if (verifyModVal) {
             if (val.modVal.intValue() != ITERATIONS) {
                throw new TestException("Value for key " + key + " is " + val.modVal + ", expected " + ITERATIONS);
             }
          } 
       }
    }

   /**
    * Selects random region (copied from dynamicReg/DynamicRegionTest.java)
    */
   protected Region getRandomRegion(boolean allowRootRegion) {
      // select a root region to work with
      Region rootRegion = getRandomRootRegion();
   
      Set subregionsSet = rootRegion.subregions(true);
      Log.getLogWriter().fine("getRandomRegion (" + rootRegion.getName() + " has the following subregions: " + subregionsSet);

      if (subregionsSet.size() == 0) {
        if (allowRootRegion) {
          return rootRegion;
        }
        else {
            return null;
        }
      }
      ArrayList aList = null;
      try {
        Object[] array = subregionsSet.toArray();
        aList = new ArrayList(array.length);
        for (int i=0; i<array.length; i++) {
          aList.add(array[i]);
        }
      } catch (NoSuchElementException e) {
         throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
      }
      if (allowRootRegion) {
         aList.add(rootRegion);
      }
      if (aList.size() == 0) { // this can happen because the subregionSet can change size after the toArray
         return null;
      }
      int randInt = TestConfig.tab().getRandGen().nextInt(0, aList.size() - 1);
      Region aRegion = (Region)aList.get(randInt);
      if (aRegion == null) {
         throw new TestException("Bug 30171 detected: aRegion is null");
      }
      return aRegion;
   }
   
   /**
    *  Creates a dynamic region (from dynamicReg/DynamicRegionTest.java)
    */
   
      protected Region createDynamicRegion(String parentName, String drName) {
   
       Region dr = null;
       // dynamic region inherits attributes of parent
       Log.getLogWriter().info("Creating dynamic region " + parentName + "/" + drName);
       try {
         dr = DynamicRegionFactory.get().createDynamicRegion(parentName, drName);
       } catch (CacheException ce) {
         throw new TestException(TestHelper.getStackTrace(ce));
       }
       Log.getLogWriter().info("Created dynamic region " + TestHelper.regionToString(dr, true));
       return dr;
   }
   
   /**
    *  Selects and returns a random rootRegion
    */
    protected Region getRandomRootRegion() {
       Set rootRegions = CacheHelper.getCache().rootRegions();
       Log.getLogWriter().fine("getRandomRootRegion found " + rootRegions);
       int randInt = TestConfig.tab().getRandGen().nextInt(0, rootRegions.size() - 1);
       Object[] regionList = rootRegions.toArray();
       Region rootRegion = (Region)regionList[randInt];
       return (rootRegion);
    }

    public synchronized static void HydraEndTask_verifyConflation() {
       if (testInstance == null) {
          testInstance = new WANRecoveryTest();
          testInstance.verifyConflation();
       }
    }

    protected void verifyConflation() {
       // Display BB stats
       SharedCounters sc = WANRecoveryBB.getBB().getSharedCounters();
       long missedUpdates = sc.read(WANRecoveryBB.missedUpdates);
 
       Log.getLogWriter().info("verifyConflation: missedUpdates = " + missedUpdates);
       // Display gatewayStats.eventsNotQueuedConflated (total)
       double eventsConflated = getConflatedEventCount();

       Log.getLogWriter().info("verifyConflation: eventsConflated = " + eventsConflated);

       // Ensure that we actually did conflate (at the GatewayHub) 
       if (eventsConflated <= 0) {
          throw new TestException("Tuning required.  Test expected Gateway batchConflation, but GatewayStatistics.eventsNotQueuedConflated = " + eventsConflated);
       }
   }

   protected double getConflatedEventCount() {
       String spec = "*bridge* " // search all BridgeServer archives
                     + "GatewayStatistics "
                     + "* " // match all instances
                     + "eventsNotQueuedConflated "
                     + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                     + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                     + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
       List aList = PerfStatMgr.getInstance().readStatistics(spec);
       if (aList == null) {
          Log.getLogWriter().info("Getting stats for spec " + spec + " returned null");
          return 0.0;
       }
       double eventsConflated = 0;
       for (int i = 0; i < aList.size(); i++) {
          PerfStatValue stat = (PerfStatValue)aList.get(i);
          eventsConflated += stat.getMax();
       }
       return eventsConflated;
    }

    /**
     * Check ShutdownExceptions and CacheClosedExceptions for underlying
     * ForcedDisconnectExceptions (Caused by within the reported exception
     * or if this VM processed a RegionDestroyedException with Operation
     * FORCED_DISCONNECT).
     *
     */
    static private boolean isCausedByForcedDisconnect(Exception e) {
       Log.getLogWriter().info("checkForForcedDisconnect processed Exception " + e);
       String errStr = e.toString();
       boolean causedByForcedDisconnect = errStr.indexOf("com.gemstone.gemfire.ForcedDisconnectException") >= 0;
       return (causedByForcedDisconnect);
    }
}

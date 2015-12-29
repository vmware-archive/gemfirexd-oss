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
package expiration; 

import hydra.*;
import hydra.blackboard.*;
import util.*;
import java.util.*;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;

public class ExpirationTest {

// the one instance of ExpirationTest
static protected ExpirationTest testInstance;

// the region names used in this test; these are also the spec names in RegionDefPrms.regionSpecs
protected static String[] regionNames;
protected static final int TTLDestroyIndex = 0;
protected static final int TTLLocalDestroyIndex = 1;
protected static final int TTLInvalIndex = 2;
protected static final int TTLLocalInvalIndex = 3;
protected static final int IdleTODestroyIndex = 4;
protected static final int IdleTOLocalDestroyIndex = 5;
protected static final int IdleTOInvalIndex = 6;
protected static final int IdleTOLocalInvalIndex = 7;
protected static final int numRegions = 8;

// the class names of the listeners used for each region, parallel to regionNames
protected static String[] regionListenerClassNames;

// the names of the verify thread groups used for each region, parallel to regionNames
protected static String[] threadGroupNames;

static {
   // initialize the region names
   regionNames = new String[numRegions];
   regionNames[TTLDestroyIndex] = "TTLDestroy";
   regionNames[TTLLocalDestroyIndex] = "TTLLocalDestroy";
   regionNames[TTLInvalIndex] = "TTLInval";
   regionNames[TTLLocalInvalIndex] = "TTLLocalInval";
   regionNames[IdleTODestroyIndex] = "IdleTODestroy";
   regionNames[IdleTOLocalDestroyIndex] = "IdleTOLocalDestroy";
   regionNames[IdleTOInvalIndex] = "IdleTOInval";
   regionNames[IdleTOLocalInvalIndex] = "IdleTOLocalInval";

   // initialize the region listener class names
   regionListenerClassNames = new String[numRegions];
   regionListenerClassNames[TTLDestroyIndex] = TTLDestroyListener.class.getName();
   regionListenerClassNames[TTLLocalDestroyIndex] = TTLLocalDestroyListener.class.getName();
   regionListenerClassNames[TTLInvalIndex] = TTLInvalListener.class.getName();
   regionListenerClassNames[TTLLocalInvalIndex] = TTLLocalInvalListener.class.getName();
   regionListenerClassNames[IdleTODestroyIndex] = IdleTODestroyListener.class.getName();
   regionListenerClassNames[IdleTOLocalDestroyIndex] = IdleTOLocalDestroyListener.class.getName();
   regionListenerClassNames[IdleTOInvalIndex] = IdleTOInvalListener.class.getName();
   regionListenerClassNames[IdleTOLocalInvalIndex] = IdleTOLocalInvalListener.class.getName();

   // initialize the region listener class names
   threadGroupNames = new String[numRegions];
   threadGroupNames[TTLDestroyIndex] = "verifyTTLDestroyThreads";
   threadGroupNames[TTLLocalDestroyIndex] = "verifyTTLLocalDestroyThreads";
   threadGroupNames[TTLInvalIndex] = "verifyTTLInvalThreads";
   threadGroupNames[TTLLocalInvalIndex] = "verifyTTLLocalInvalThreads";
   threadGroupNames[IdleTODestroyIndex] = "verifyIdleTODestroyThreads";
   threadGroupNames[IdleTOLocalDestroyIndex] = "verifyIdleTOLocalDestroyThreads";
   threadGroupNames[IdleTOInvalIndex] = "verifyIdleTOInvalThreads";
   threadGroupNames[IdleTOLocalInvalIndex] = "verifyIdleTOLocalInvalThreads";
}

// ======================================================================== 
// initialization methods 

/**
 * Creates and initializes the singleton instance of ExpirationTest. 
 * All regions specified in RegionDefPrms.regionSpecs are created.
 */
public synchronized static void HydraTask_initializeControlThread() {
   if (testInstance == null) {
      testInstance = new ExpirationTest();
      testInstance.initializeControlThread();
   }
}

/** Initialize all 8 RegionDefintion instances for this run, and write
 *  them to the blackboard. 
 */
public static void StartTask_initialize() {
   for (int i = 0; i < regionNames.length; i++) { 
      RegionDefinition regDef = RegionDefinition.createRegionDefinition(
                                RegionDefPrms.regionSpecs, regionNames[i]);
      ExpirationBB.getBB().getSharedMap().put(regionNames[i], regDef);
   }
}

/** Create the regions specified in RegionDefPrms.regionSpecs. Writes
 *  each region definition to the blackboard.
 */
protected void initializeControlThread() {
   for (int i = 0; i < regionNames.length; i++) { 
      RegionDefinition regDef = (RegionDefinition)(ExpirationBB.getBB().getSharedMap().get(regionNames[i]));
      CacheListener listener = null;
      try {
         Class listenerClass = Class.forName(regionListenerClassNames[i]);
         listener = (CacheListener)(listenerClass.newInstance());
      } catch (ClassNotFoundException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalAccessException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (InstantiationException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      regDef.createRootRegion(CacheUtil.createCache(), regionNames[i], listener, null, null);

      // now write the region definition to be used in the verify threads
      // to the blackboard; verify threads use regions exactly like the
      // ones defined here, except they use mirror keysValues and no
      // expiration actions
      regDef.setEntryIdleTimeoutSec(null);
      regDef.setEntryIdleTimeoutAction(null);
      regDef.setEntryTTLSec(null);
      regDef.setEntryTTLAction(null);
      regDef.setRegionIdleTimeoutSec(null);
      regDef.setRegionIdleTimeoutAction(null);
      regDef.setRegionTTLSec(null);
      regDef.setRegionTTLAction(null);
      regDef.setMirroring(MirrorType.KEYS_VALUES);
      ExpirationBB.getBB().getSharedMap().put(regionNames[i], regDef);
   }
}

/**
 * Creates and initializes the singleton instance of ExpirationTest. 
 * All regions (of the same name created by the control threads) are created.
 * The region definitions for the regions created here are in the ExpirationBB
 * blackboard.
 */
public synchronized static void HydraTask_initializeVerifyThread() {
   if (testInstance == null) {
      testInstance = new ExpirationTest();
      testInstance.initializeVerifyThread();
   }
}

/** Create the regions specified in RegionDefPrms.regionSpecs. Writes
 *  each region definition to the blackboard.
 */
protected void initializeVerifyThread() {
   Map bbMap = ExpirationBB.getBB().getSharedMap().getMap();
   Iterator it = bbMap.keySet().iterator();
   while (it.hasNext()) {
      String regionName = (String)(it.next());
      RegionDefinition regDef = (RegionDefinition)(bbMap.get(regionName));
      CacheListener listener = null;
      try {
         String className = regionListenerClassNames[Arrays.asList(regionNames).indexOf(regionName)];
         Class listenerClass = Class.forName(className);
         listener = (CacheListener)(listenerClass.newInstance());
      } catch (ClassNotFoundException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalAccessException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (InstantiationException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }

      regDef.createRootRegion(CacheUtil.createCache(), regionName, listener, null, null);
   }
}

// ======================================================================== 
// hydra close tasks 
public static void HydraTask_logRegionContents() {
   for (int i = 0; i < regionNames.length; i++) {
      testInstance.logRegionContents(regionNames[i]);
   }
}

/** Task to wait for all control tasks to complete, then log the keys and values
 *  that are in the region.
 */
public static void HydraTask_syncAndLog() {
   // wait for control threads to finish tasks
   TestHelper.waitForCounter(ExpirationBB.getBB(), "DoneWithTask",
       ExpirationBB.DoneWithTask, numRegions, true, -1);

   // log the region contents
   HydraTask_logRegionContents();

   // signal that we are done with logging the region 
   ExpirationBB.getBB().getSharedCounters().increment(ExpirationBB.DoneLoggingRegion);

   // wait for everybody to finish logging region 
   TestHelper.waitForCounter(ExpirationBB.getBB(), "DoneLoggingRegion",
       ExpirationBB.DoneLoggingRegion, TestHelper.getNumThreads(), true, -1);
}

// ======================================================================== 
// hydra task methods for entry expiration

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLDestroy
 */
public static void HydraTask_controlEntryTTLDestroy() throws Throwable {
   try {
      testInstance.controlEntryTTLDestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLLocalDestroy
 */
public static void HydraTask_controlEntryTTLLocalDestroy() throws Throwable {
   try {
      testInstance.controlEntryTTLLocalDestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLInval
 */
public static void HydraTask_controlEntryTTLInval() throws Throwable {
   try {
      testInstance.controlEntryTTLInval();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLLocalInval
 */
public static void HydraTask_controlEntryTTLLocalInval() throws Throwable {
   try {
      testInstance.controlEntryTTLLocalInval();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTODestroy
 */
public static void HydraTask_controlEntryIdleTODestroy() throws Throwable {
   try {
      testInstance.controlEntryIdleTODestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTOLocalDestroy
 */
public static void HydraTask_controlEntryIdleTOLocalDestroy() throws Throwable {
   try {
      testInstance.controlEntryIdleTOLocalDestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTOInval
 */
public static void HydraTask_controlEntryIdleTOInval() throws Throwable {
   try {
      testInstance.controlEntryIdleTOInval();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTOLocalInval
 */
public static void HydraTask_controlEntryIdleTOLocalInval() throws Throwable {
   try {
      testInstance.controlEntryIdleTOLocalInval();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

// ======================================================================== 
// hydra task methods for region expiration

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLDestroy
 */
public static void HydraTask_controlRegionTTLDestroy() throws Throwable {
   try {
      testInstance.controlRegionTTLDestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLLocalDestroy
 */
public static void HydraTask_controlRegionTTLLocalDestroy() throws Throwable {
   try {
      testInstance.controlRegionTTLLocalDestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLInval
 */
public static void HydraTask_controlRegionTTLInval() throws Throwable {
   try {
      testInstance.controlRegionTTLInval();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region TTLLocalInval
 */
public static void HydraTask_controlRegionTTLLocalInval() throws Throwable {
   try {
      testInstance.controlRegionTTLLocalInval();
   }
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTODestroy
 */
public static void HydraTask_controlRegionIdleTODestroy() throws Throwable {
   try {
      testInstance.controlRegionIdleTODestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTOLocalDestroy
 */
public static void HydraTask_controlRegionIdleTOLocalDestroy() throws Throwable {
   try {
      testInstance.controlRegionIdleTOLocalDestroy();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTOInval
 */
public static void HydraTask_controlRegionIdleTOInval() throws Throwable {
   try {
      testInstance.controlRegionIdleTOInval();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}

/** Hydra task to control the thread that has an expiration action set
 *  in the region IdleTOLocalInval
 */
public static void HydraTask_controlRegionIdleTOLocalInval() throws Throwable {
   try {
      testInstance.controlRegionIdleTOLocalInval();
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw e;
   } finally {
      testInstance.wrapUp();
   }
}


// ======================================================================== 
// methods to do the work of hydra tasks for entry expiration

/** Controls the thread that has an expiration action set in the region
 *  TTLDestroy. Put entries into the region then wait for them to expire. 
 *  TTL is based on modify time. When finished, we should see all of
 *  the entries have been destroyed in the region, and in remote regions. 
 */
public void controlEntryTTLDestroy() {
   String regionName = regionNames[TTLDestroyIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(TTLDestroyBB.getBB(), "numAfterCreateEvents_isNotExp",
       TTLDestroyBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for remote caches to receive all destroy events
   TestHelper.waitForCounter(TTLDestroyBB.getBB(), "numAfterDestroyEvents_isRemote",
       TTLDestroyBB.numAfterDestroyEvents_isRemote, numRemoteVMs * numKeys,
       true, 600000);

   // Wait for this VM to receive all destroy events
   TestHelper.waitForCounter(TTLDestroyBB.getBB(), "numAfterDestroyEvents_isNotRemote",
       TTLDestroyBB.numAfterDestroyEvents_isNotRemote, numKeys, true, 600000);

   // Check that all were destroyed
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   int regionSize = aRegion.keys().size();
   if (regionSize != 0)
      throw new TestException("Expected region to have all its entries destroyed, but it has " +
                regionSize + " keys");

   // Check all counters
   checkEntryEventsForDestroy(TTLDestroyBB.getBB(), numKeys);
   TTLDestroyBB.getBB().printSharedCounters();
   TTLDestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set in the region
 *  TTLLocalDestroy. Put entries into the region then wait for them to expire. 
 *  TTL is based on modify time. When finished, we should see all of
 *  the entries have been destroyed in the region, but not in remote regions. 
 */
public void controlEntryTTLLocalDestroy() {
   String regionName = regionNames[TTLLocalDestroyIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
//   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(TTLLocalDestroyBB.getBB(), "numAfterCreateEvents_isNotExp",
       TTLLocalDestroyBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for this VM to receive all destroy events
   TestHelper.waitForCounter(TTLLocalDestroyBB.getBB(), "numAfterDestroyEvents_isNotRemote",
       TTLLocalDestroyBB.numAfterDestroyEvents_isNotRemote, numKeys, true, 600000);

   // Check that all were destroyed
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   int regionSize = aRegion.keys().size();
   if (regionSize != 0)
      throw new TestException("Expected region to have all its entries destroyed, but it has " +
                regionSize + " keys");

   // Check all counters
   checkEntryEventsForLocalDestroy(TTLLocalDestroyBB.getBB(), numKeys);
   TTLLocalDestroyBB.getBB().printSharedCounters();
   TTLLocalDestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region TTLInval
 */
public void controlEntryTTLInval() {
   String regionName = regionNames[TTLInvalIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(TTLInvalBB.getBB(), "numAfterCreateEvents_isNotExp",
       TTLInvalBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for remote caches to receive all invalidate events
   TestHelper.waitForCounter(TTLInvalBB.getBB(), "numAfterInvalidateEvents_isRemote",
       TTLInvalBB.numAfterInvalidateEvents_isRemote, numRemoteVMs * numKeys,
       true, 600000);

   // Wait for this VM to receive all invalidate events
   TestHelper.waitForCounter(TTLInvalBB.getBB(), "numAfterInvalidateEvents_isNotRemote",
       TTLInvalBB.numAfterInvalidateEvents_isNotRemote, numKeys, true, 600000);

   // Check that all were invalidated
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   Iterator it = aRegion.keys().iterator();
   while (it.hasNext()) { // it's ok to do gets, since no other cache has a value either
      key = it.next();
      try {
         Object value = aRegion.get(key);
         if (value != null)
            throw new TestException("Found key " + key + " with non-null value " + value);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }

   // Check all counters
   checkEntryEventsForInvalidate(TTLInvalBB.getBB(), numKeys);
   TTLInvalBB.getBB().printSharedCounters();
   TTLInvalBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region TTLLocalInval
 */
public void controlEntryTTLLocalInval() {
   String regionName = regionNames[TTLLocalInvalIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
//   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(TTLLocalInvalBB.getBB(), "numAfterCreateEvents_isNotExp",
       TTLLocalInvalBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for this VM to receive all invalidate events
   TestHelper.waitForCounter(TTLLocalInvalBB.getBB(), "numAfterInvalidateEvents_isNotRemote",
       TTLLocalInvalBB.numAfterInvalidateEvents_isNotRemote, numKeys, true, 600000);

   // Check all counters
   checkEntryEventsForLocalInvalidate(TTLLocalInvalBB.getBB(), numKeys);
   TTLLocalInvalBB.getBB().printSharedCounters();
   TTLLocalInvalBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set

 *  in the region IdleTODestroy
 */
public void controlEntryIdleTODestroy() {
   String regionName = regionNames[IdleTODestroyIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(IdleTODestroyBB.getBB(), "numAfterCreateEvents_isNotExp",
       IdleTODestroyBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for remote caches to receive all destroy events
   TestHelper.waitForCounter(IdleTODestroyBB.getBB(), "numAfterDestroyEvents_isRemote",
       IdleTODestroyBB.numAfterDestroyEvents_isRemote, numRemoteVMs * numKeys,
       true, 600000);

   // Wait for this VM to receive all destroy events
   TestHelper.waitForCounter(IdleTODestroyBB.getBB(), "numAfterDestroyEvents_isNotRemote",
       IdleTODestroyBB.numAfterDestroyEvents_isNotRemote, numKeys, true, 600000);

   // Check that all were destroyed
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   int regionSize = aRegion.keys().size();
   if (regionSize != 0)
      throw new TestException("Expected region to have all its entries destroyed, but it has " +
                regionSize + " keys");

   // Check all counters
   checkEntryEventsForDestroy(IdleTODestroyBB.getBB(), numKeys);
   IdleTODestroyBB.getBB().printSharedCounters();
   IdleTODestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region IdleTOLocalDestroy
 */
public void controlEntryIdleTOLocalDestroy() {
   String regionName = regionNames[IdleTOLocalDestroyIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
//   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(IdleTOLocalDestroyBB.getBB(), "numAfterCreateEvents_isNotExp",
       IdleTOLocalDestroyBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for this VM received all destroy events
   TestHelper.waitForCounter(IdleTOLocalDestroyBB.getBB(), "numAfterDestroyEvents_isNotRemote",
       IdleTOLocalDestroyBB.numAfterDestroyEvents_isNotRemote, numKeys, true, 600000);

   // Check that all were destroyed
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   int regionSize = aRegion.keys().size();
   if (regionSize != 0)
      throw new TestException("Expected region to have all its entries destroyed, but it has " +
                regionSize + " keys");

   // Check all counters
   checkEntryEventsForLocalDestroy(IdleTOLocalDestroyBB.getBB(), numKeys);
   IdleTOLocalDestroyBB.getBB().printSharedCounters();
   IdleTOLocalDestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region IdleTOInval
 */
public void controlEntryIdleTOInval() {
   String regionName = regionNames[IdleTOInvalIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(IdleTOInvalBB.getBB(), "numAfterCreateEvents_isNotExp",
       IdleTOInvalBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for remote caches to receive all invalidate events
   TestHelper.waitForCounter(IdleTOInvalBB.getBB(), "numAfterInvalidateEvents_isRemote",
       IdleTOInvalBB.numAfterInvalidateEvents_isRemote, numRemoteVMs * numKeys,
       true, 600000);

   // Wait for this VM received all invalidate events
   TestHelper.waitForCounter(IdleTOInvalBB.getBB(), "numAfterInvalidateEvents_isNotRemote",
       IdleTOInvalBB.numAfterInvalidateEvents_isNotRemote, numKeys, true, 600000);

   // Check that all were invalidated
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   Iterator it = aRegion.keys().iterator();
   while (it.hasNext()) { // it's ok to do gets, since no other cache has a value either
      key = it.next();
      try {
         Object value = aRegion.get(key);
         if (value != null)
            throw new TestException("Found key " + key + " with non-null value " + value);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }

   // Check all counters
   checkEntryEventsForInvalidate(IdleTOInvalBB.getBB(), numKeys);
   IdleTOInvalBB.getBB().printSharedCounters();
   IdleTOInvalBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region IdleTOLocalInval
 */
public void controlEntryIdleTOLocalInval() {
   String regionName = regionNames[IdleTOLocalInvalIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
//   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(IdleTOLocalInvalBB.getBB(), "numAfterCreateEvents_isNotExp",
       IdleTOLocalInvalBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for this VM received all invalidate events
   TestHelper.waitForCounter(IdleTOLocalInvalBB.getBB(), "numAfterInvalidateEvents_isNotRemote",
       IdleTOLocalInvalBB.numAfterInvalidateEvents_isNotRemote, numKeys, true, 600000);

   // Check all counters
   checkEntryEventsForLocalInvalidate(IdleTOLocalInvalBB.getBB(), numKeys);
   IdleTOLocalInvalBB.getBB().printSharedCounters();
   IdleTOLocalInvalBB.getBB().zeroAllCounters();
}

// ======================================================================== 
// methods to do the work of hydra tasks for region expiration

/** Controls the thread that has an expiration action set in the region
 *  TTLDestroy. 
 */
public void controlRegionTTLDestroy() {
   String regionName = regionNames[TTLDestroyIndex];
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   try {
      populateRegion(regionName);
   } catch (RegionDestroyedException e) {
      // the destroy event occurred; ok
   }

   // Wait for remote caches to receive all destroy events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(TTLDestroyBB.getBB(), "numAfterRegionDestroyEvents_isRemote",
       TTLDestroyBB.numAfterRegionDestroyEvents_isRemote, numRemoteVMs,
       true, 600000);

   // Wait for this VM to receive destroy event
   TestHelper.waitForCounter(TTLDestroyBB.getBB(), "numAfterRegionDestroyEvents_isNotRemote",
       TTLDestroyBB.numAfterRegionDestroyEvents_isNotRemote, 1, true, 600000);

   // Check that the region is destroyed
   try {
      aRegion.keys().size();
      throw new TestException("Expected exception for region destroy, but did not get one");
   } catch (RegionDestroyedException e) {
      // expected
   }

   // Check all counters
   checkRegionEventsForDestroy(TTLDestroyBB.getBB());
   TTLDestroyBB.getBB().printSharedCounters();
   TTLDestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set in the region
 *  TTLLocalDestroy. 
 */
public void controlRegionTTLLocalDestroy() {
   String regionName = regionNames[TTLLocalDestroyIndex];
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   try {
      populateRegion(regionName);
   } catch (RegionDestroyedException e) {
      // the destroy event occurred; ok
   }

   // Wait for this VM to receive destroy event
   TestHelper.waitForCounter(TTLLocalDestroyBB.getBB(), "numAfterRegionDestroyEvents_isNotRemote",
       TTLLocalDestroyBB.numAfterRegionDestroyEvents_isNotRemote, 1, true, 600000);

   // Check that the region is destroyed
   try {
      aRegion.keys().size();
      throw new TestException("Expected exception for region destroy, but did not get one");
   } catch (RegionDestroyedException e) {
      // expected
   }

   // Check all counters
   checkRegionEventsForLocalDestroy(TTLLocalDestroyBB.getBB());
   TTLLocalDestroyBB.getBB().printSharedCounters();
   TTLLocalDestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region TTLInval
 */
public void controlRegionTTLInval() {
   String regionName = regionNames[TTLInvalIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(TTLInvalBB.getBB(), "numAfterCreateEvents_isNotExp",
       TTLInvalBB.numAfterCreateEvents_isNotExp, numVMs * numKeys,
       true, 600000);

   // Wait for remote caches to receive invalidate events
   TestHelper.waitForCounter(TTLInvalBB.getBB(), "numAfterRegionInvalidateEvents_isRemote",
       TTLInvalBB.numAfterRegionInvalidateEvents_isRemote, numRemoteVMs,
       true, 600000);

   // Wait for this VM to receive invalidate event
   TestHelper.waitForCounter(TTLInvalBB.getBB(), "numAfterRegionInvalidateEvents_isNotRemote",
       TTLInvalBB.numAfterRegionInvalidateEvents_isNotRemote, 1, true, 600000);

   // Check that all were invalidated
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   Iterator it = aRegion.keys().iterator();
   while (it.hasNext()) { // it's ok to do gets, since no other cache has a value either
      key = it.next();
      try {
         Object value = aRegion.get(key);
         if (value != null)
            throw new TestException("Found key " + key + " with non-null value " + value);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }

   // Check all counters
   checkRegionEventsForInvalidate(TTLInvalBB.getBB(), numKeys);
   TTLInvalBB.getBB().printSharedCounters();
   TTLInvalBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region TTLLocalInval
 */
public void controlRegionTTLLocalInval() {
   String regionName = regionNames[TTLLocalInvalIndex];
   int numKeys = populateRegion(regionName);
   Object key = regionName + "_numKeys";
   Log.getLogWriter().info("Putting " + key + ", " + numKeys + " into ExpirationBB"); 
   ExpirationBB.getBB().getSharedMap().put(key, new Integer(numKeys));

   // Wait for remote caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(TTLLocalInvalBB.getBB(), "numAfterCreateEvents_isRemote",
       TTLLocalInvalBB.numAfterCreateEvents_isRemote, numRemoteVMs * numKeys,
       true, 600000);

   // Wait for this VM to receive invalidate event
   TestHelper.waitForCounter(TTLLocalInvalBB.getBB(), "numAfterRegionInvalidateEvents_isNotRemote",
       TTLLocalInvalBB.numAfterRegionInvalidateEvents_isNotRemote, 1, true, 600000);

   // Check all counters
   checkRegionEventsForLocalInvalidate(TTLLocalInvalBB.getBB(), numKeys);
   TTLLocalInvalBB.getBB().printSharedCounters();
   TTLLocalInvalBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set

 *  in the region IdleTODestroy
 */
public void controlRegionIdleTODestroy() {
   String regionName = regionNames[IdleTODestroyIndex];
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   try {
      populateRegion(regionName);
   } catch (RegionDestroyedException e) {
      // the destroy event occurred; ok
   }

   // Wait for remote caches to receive destroy events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(IdleTODestroyBB.getBB(), "numAfterRegionDestroyEvents_isRemote",
       IdleTODestroyBB.numAfterRegionDestroyEvents_isRemote, numRemoteVMs,
       true, 600000);

   // Wait for this VM to receive destroy event
   TestHelper.waitForCounter(IdleTODestroyBB.getBB(), "numAfterRegionDestroyEvents_isNotRemote",
       IdleTODestroyBB.numAfterRegionDestroyEvents_isNotRemote, 1, true, 600000);

   // Check that the region is destroyed
   try {
      aRegion.keys().size();
      throw new TestException("Expected exception for region destroy, but did not get one");
   } catch (RegionDestroyedException e) {
      // expected
   }

   // Check all counters
   checkRegionEventsForDestroy(IdleTODestroyBB.getBB());
   IdleTODestroyBB.getBB().printSharedCounters();
   IdleTODestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region IdleTOLocalDestroy
 */
public void controlRegionIdleTOLocalDestroy() {
   String regionName = regionNames[IdleTOLocalDestroyIndex];
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   try {
      populateRegion(regionName);
   } catch (RegionDestroyedException e) {
      // the destroy event occurred; ok
   }

   // Wait for this VM to receive destroy event
   TestHelper.waitForCounter(IdleTOLocalDestroyBB.getBB(), "numAfterRegionDestroyEvents_isNotRemote",
       IdleTOLocalDestroyBB.numAfterRegionDestroyEvents_isNotRemote, 1, true, 600000);

   // Check that the region is destroyed
   try {
      aRegion.keys().size();
      throw new TestException("Expected exception for region destroy, but did not get one");
   } catch (RegionDestroyedException e) {
      // expected
   }

   // Check all counters
   checkRegionEventsForLocalDestroy(IdleTOLocalDestroyBB.getBB());
   IdleTOLocalDestroyBB.getBB().printSharedCounters();
   IdleTOLocalDestroyBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region IdleTOInval
 */
public void controlRegionIdleTOInval() {
   String regionName = regionNames[IdleTOInvalIndex];
   int numKeys = populateRegion(regionName);

   // Wait for remote caches to receive all invalidate events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(IdleTOInvalBB.getBB(), "numAfterRegionInvalidateEvents_isRemote",
       IdleTOInvalBB.numAfterRegionInvalidateEvents_isRemote, numRemoteVMs,
       true, 600000);

   // Wait for this VM to receive invalidate event
   TestHelper.waitForCounter(IdleTOInvalBB.getBB(), "numAfterRegionInvalidateEvents_isNotRemote",
       IdleTOInvalBB.numAfterRegionInvalidateEvents_isNotRemote, 1, true, 600000);

   // Check that all were invalidated
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   Iterator it = aRegion.keys().iterator();
   while (it.hasNext()) { // it's ok to do gets, since no other cache has a value either
      Object key = it.next();
      try {
         Object value = aRegion.get(key);
         if (value != null)
            throw new TestException("Found key " + key + " with non-null value " + value);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheLoaderException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }

   // Check all counters
   checkRegionEventsForInvalidate(IdleTOInvalBB.getBB(), numKeys);
   IdleTOInvalBB.getBB().printSharedCounters();
   IdleTOInvalBB.getBB().zeroAllCounters();
}

/** Controls the thread that has an expiration action set
 *  in the region IdleTOLocalInval
 */
public void controlRegionIdleTOLocalInval() {
   String regionName = regionNames[IdleTOLocalInvalIndex];
   int numKeys = populateRegion(regionName);

   // Wait for remote caches to receive all create events
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm
   TestHelper.waitForCounter(IdleTOLocalInvalBB.getBB(), "numAfterCreateEvents_isRemote",
       IdleTOLocalInvalBB.numAfterCreateEvents_isRemote, numRemoteVMs * numKeys,
       true, 600000);

   // Wait for this VM received all invalidate events
   TestHelper.waitForCounter(IdleTOLocalInvalBB.getBB(), "numAfterRegionInvalidateEvents_isNotRemote",
       IdleTOLocalInvalBB.numAfterRegionInvalidateEvents_isNotRemote, 1, true, 600000);

   // Check all counters
   checkRegionEventsForLocalInvalidate(IdleTOLocalInvalBB.getBB(), numKeys);
   IdleTOLocalInvalBB.getBB().printSharedCounters();
   IdleTOLocalInvalBB.getBB().zeroAllCounters();
}


// ======================================================================== 
// other methods 

/** Populate a region with keys and values.
 *
 *  @param regionName - the name of the region to populate
 *
 *  @returns - the number of keys/values put in the region.
 */
protected int populateRegion(String regionName) {
   Cache theCache = CacheUtil.getCache();
   Region aRegion = theCache.getRegion(regionName);
   Log.getLogWriter().info("Populating region " + regionName + ", aRegion is " + 
       TestHelper.regionToString(aRegion, true));
   String keyPrefix = regionName + "_";

   // put for a time period
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long msToRun = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("Putting for " + msToRun + " ms");
   int numKeys = 0;
   long startTimeMs = System.currentTimeMillis();
   do {
      String key = keyPrefix + (++numKeys);
      Object value = new Long(numKeys);
      try {
         Log.getLogWriter().info("Putting key " + key + ", value " + value + " into region " + 
             regionName);
         if (TestConfig.tab().booleanAt(ExpirPrms.useTransactions, false)) 
            TxHelper.begin();
         long putStart = System.currentTimeMillis();
         aRegion.put(key, value);
         long putEnd = System.currentTimeMillis();
         long duration = putEnd - putStart;
         Log.getLogWriter().info("Done putting key " + key + ", value " + value + " into region " +
             regionName + ", put took " + duration + " ms");
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (CacheWriterException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } finally {
        if (TxHelper.exists())
           TxHelper.commitExpectSuccess();
      }
   } while (System.currentTimeMillis() - startTimeMs < msToRun);
   Log.getLogWriter().info("Done putting into " + regionName + " for " + msToRun + 
                           " ms, put " + numKeys + " keys/values");
   return numKeys;
}

/** Check all event counters for expiration of destroy.
 *
 *  @param bb - the blackboard containing the event counters.
 *  @param numEntries - the number of entries that should have been
 *         destroyed.
 */
protected void checkEntryEventsForDestroy(Blackboard bb, int numEntries) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   long[] expectedValues = new long[] {
      // afterCreate counters
         (numVMs * numEntries), 0, 
         0, (numVMs * numEntries), 
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterDestroy counters
         (numVMs * numEntries), 0, 
         (numVMs * numEntries), 0,
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterInvalidate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         0, 0, 0, 0, 0, 0,
      // afterRegionInvalidate counters
         0, 0, 0, 0, 0, 0,
      // close counters
      0
   };
   if (bb instanceof TTLDestroyBB)
      ((TTLDestroyBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTODestroyBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Check all event counters for expiration of local destroy.
 *
 *  @param bb - the blackboard containing the event counters.
 *  @param numEntries - the number of entries that should have been
 *         locally destroyed.
 */
protected void checkEntryEventsForLocalDestroy(Blackboard bb, int numEntries) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   long[] expectedValues = new long[] {
      // afterCreate counters
         (numVMs * numEntries), 0, 
         0, (numVMs * numEntries), 
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterDestroy counters
         0, numEntries, 
         numEntries, 0,
         0, numEntries,          
         0, numEntries, 
         0, numEntries, 
         0, numEntries, 
         0, numEntries,
      // afterInvalidate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         0, 0, 0, 0, 0, 0,
      // afterRegionInvalidate counters
         0, 0, 0, 0, 0, 0,
      // close counters
      0
   };
   if (bb instanceof TTLLocalDestroyBB)
      ((TTLLocalDestroyBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTOLocalDestroyBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Check all event counters for expiration of invalidate.
 *
 *  @param bb - the blackboard containing the event counters.
 *  @param numEntries - the number of entries that should have been
 *         invalidated.
 */
protected void checkEntryEventsForInvalidate(Blackboard bb, int numEntries) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   long[] expectedValues = new long[] {
      // afterCreate counters
         (numVMs * numEntries), 0, 
         0, (numVMs * numEntries), 
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterDestroy counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterInvalidate counters
         (numVMs * numEntries), 0, 
         (numVMs * numEntries), 0,
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         0, 0, 0, 0, 0, 0,
      // afterRegionInvalidate counters
         0, 0, 0, 0, 0, 0,
      // close counters
      0
   };
   if (bb instanceof TTLInvalBB)
      ((TTLInvalBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTOInvalBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Check all event counters for expiration of local invalidate.
 *
 *  @param bb - the blackboard containing the event counters.
 *  @param numEntries - the number of entries that should have been
 *         locally invalidated.
 */
protected void checkEntryEventsForLocalInvalidate(Blackboard bb, int numEntries) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   long[] expectedValues = new long[] {
      // afterCreate counters
         (numVMs * numEntries), 0, 
         0, (numVMs * numEntries), 
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterDestroy counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterInvalidate counters
         0, numEntries, 
         numEntries, 0,
         0, numEntries,          
         0, numEntries,
         0, numEntries,
         0, numEntries,
         0, numEntries,
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         0, 0, 0, 0, 0, 0,
      // afterRegionInvalidate counters
         0, 0, 0, 0, 0, 0,
      // close counters
      0
   };
   if (bb instanceof TTLLocalInvalBB)
      ((TTLLocalInvalBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTOLocalInvalBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Check all event counters for expiration of destroy.
 *
 *  @param bb - the blackboard containing the event counters.
 */
protected void checkRegionEventsForDestroy(Blackboard bb) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 

   // the region might have been destroyed while populating the region,
   // so we don't know exactly how many entries were in it when the
   // region was destroyed
   long[] expectedValues = new long[] {
      // afterCreate counters
         -1, 0, 
         0, -1,
         -1, -1,          
         0, -1,
         0, -1,
         0, -1,
         0, -1,
      // afterDestroy counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterInvalidate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         numVMs, 0, 
         numVMs, 0, 
         numRemoteVMs, 1,
      // afterRegionInvalidate counters
         0, 0, 0, 0, 0, 0,
      // close counters
      numVMs
   };
   if (bb instanceof TTLDestroyBB)
      ((TTLDestroyBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTODestroyBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Check all event counters for expiration of local destroy.
 *
 *  @param bb - the blackboard containing the event counters.
 */
protected void checkRegionEventsForLocalDestroy(Blackboard bb) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
//   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   long[] expectedValues = new long[] {
      // afterCreate counters
         -1, 0, 
         0, -1, 
         -1, -1,          
         0, -1, 
         0, -1, 
         0, -1, 
         0, -1,
      // afterDestroy counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterInvalidate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         0, 1, 
         1, 0, 
         0, 1,
      // afterRegionInvalidate counters
         0, 0, 0, 0, 0, 0,
      // close counters
      1
   };
   if (bb instanceof TTLLocalDestroyBB)
      ((TTLLocalDestroyBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTOLocalDestroyBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Check all event counters for expiration of invalidate.
 *
 *  @param bb - the blackboard containing the event counters.
 *  @param numEntries - the number of entries that should have been
 *         invalidated.
 */
protected void checkRegionEventsForInvalidate(Blackboard bb, int numEntries) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   long[] expectedValues = new long[] {
      // afterCreate counters
         (numVMs * numEntries), 0, 
         0, (numVMs * numEntries), 
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterDestroy counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterInvalidate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         0, 0, 0, 0, 0, 0,
      // afterRegionInvalidate counters
         numVMs, 0, 
         numVMs, 0, 
         numRemoteVMs, 1,
      // close counters
      0
   };
   if (bb instanceof TTLInvalBB)
      ((TTLInvalBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTOInvalBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Check all event counters for expiration of local invalidate.
 *
 *  @param bb - the blackboard containing the event counters.
 *  @param numEntries - the number of entries that should have been
 *         locally invalidated.
 */
protected void checkRegionEventsForLocalInvalidate(Blackboard bb, int numEntries) {
   Log.getLogWriter().info("Checking event counters in " + bb.getClass().getName());
   int numVMs = TestHelper.getNumVMs();
   int numRemoteVMs = numVMs - 1; // number of VMs remote to this controller vm

   // Check all counters
   // counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   //    isLoad/isNotLoad isLocalLoad/isNotLocalLoad isNetLoad/isNotNetLoad  isNetSearch/isNotNetSearch
   // Region counters ordered by isDist/isNotDist, isExp/isNotExp, isRemote/isNotRemote
   // Followed by numClose 
   long[] expectedValues = new long[] {
      // afterCreate counters
         (numVMs * numEntries), 0, 
         0, (numVMs * numEntries), 
         (numRemoteVMs * numEntries), numEntries,          
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries), 
         0, (numVMs * numEntries),
      // afterDestroy counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterInvalidate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterUpdate counters
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      // afterRegionDestroy counters
         0, 0, 0, 0, 0, 0,
      // afterRegionInvalidate counters
         0, 1, 
         1, 0, 
         0, 1,
      // close counters
      0
   };
   if (bb instanceof TTLLocalInvalBB)
      ((TTLLocalInvalBB)bb).checkEventCounters(expectedValues);
   else
      ((IdleTOLocalInvalBB)bb).checkEventCounters(expectedValues);
   Log.getLogWriter().info("Done checking event counters in " + bb.getClass().getName() +
                           ", no errors detected");
}

/** Log the region contents by showing which keys and values that were 
 *  orginally put into the region are still there.
 *
 *  @param regionName - the region to display
 */
protected void logRegionContents(String regionName) {
   Region aRegion = CacheUtil.getCache().getRegion(regionName);
   if (aRegion == null) {
      Log.getLogWriter().info("Cannot log region " + regionName + 
          ", aRegion is null (it does not exist)");
      return;
   }
   String key = aRegion.getName() + "_numKeys";
   Object anObj = ExpirationBB.getBB().getSharedMap().get(key);
   if (anObj == null) {
//      ExpirationBB.getBB().printSharedMap();
      Log.getLogWriter().info("Blackboard map for " + key + " is null");
   } else {
      int numKeys = ((Integer)(anObj)).intValue();
      Log.getLogWriter().info("Logging region contents for region " + aRegion.getName() +
          " based on numKeys " + numKeys + "\n" + regionContentsToString(aRegion, numKeys));
   }
}

/** Return a string showing the region contents by showing which keys and values 
 *  that were orginally put into the region are still there.
 *
 *  @param aRegion - the region to display
 *  @param numKeys - the number of keys orginally put into the region. They might
 *                   have been destroyed or invalidated by now.
 */
protected String regionContentsToString(Region aRegion, int numKeys) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("Region " + aRegion.getName() + " has " + aRegion.keys().size() + " keys\n");
   int startRange = 1;
   boolean currentContainsKey = true;
   boolean currentContainsValueForKey = true;
   for (int i = 1; i <= numKeys; i++) {
      String key = aRegion.getName() + "_" + i;
      boolean containsKey = aRegion.containsKey(key);
      boolean containsValueForKey = aRegion.containsValueForKey(key);
      if (i == 1) { // is first time through
         currentContainsKey = containsKey;
         currentContainsValueForKey = containsValueForKey;
         continue;
      } else if (i == numKeys) { // is last time through
         aStr.append(startRange + " - " + i + ", containsKey: " + currentContainsKey +
                     ", containsValue: " + currentContainsValueForKey + "\n"); 
         continue;
      }
      if (containsKey == currentContainsKey) {
         if (containsValueForKey == currentContainsValueForKey) {
            // is the same as previous value for i; do nothing
         } else { // containsValueForKey is different
            aStr.append(startRange + " - " + (i-1) + ", containsKey: " + currentContainsKey +
                        ", containsValue: " + currentContainsValueForKey + "\n"); 
            startRange = i;
         }
      } else { // containsKey is different
         aStr.append(startRange + " - " + (i-1) + ", containsKey: " + currentContainsKey +
                     ", containsValue: " + currentContainsValueForKey + "\n"); 
         startRange = i;
      }
      currentContainsKey = containsKey;
      currentContainsValueForKey = containsValueForKey;
   }
   return aStr.toString();
}

/** Method to sync up with all other controller threads and display the
 *  test regions existing keys and values.
 */
protected void wrapUp() {
   // signal that we are done with the task
   ExpirationBB.getBB().getSharedCounters().increment(ExpirationBB.DoneWithTask);

   // wait for everybody else to finish task
   TestHelper.waitForCounter(ExpirationBB.getBB(), "DoneWithTask",
       ExpirationBB.DoneWithTask, numRegions, true, -1);

   // log the region contents
   HydraTask_logRegionContents();

   // signal that we are done with logging the region 
   ExpirationBB.getBB().getSharedCounters().increment(ExpirationBB.DoneLoggingRegion);

   // wait for everybody to finish logging region 
//   TestHelper.waitForCounter(ExpirationBB.getBB(), "DoneLoggingRegion",
//       ExpirationBB.DoneLoggingRegion, TestHelper.getNumThreads(), true, -1);
}

}

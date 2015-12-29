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
package mirror;

import diskReg.*;
import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import java.util.*;
import java.io.File;

public class MirrorTest {

// constants used to specify the nature of the keys/values in a region
static final int POSITIVE = 0;
static final int NEGATIVE = 1;
static final int BOTH = 2;
static final int NEITHER = 3;

// constants used to specify how to validate
static final int CHECK_IF_PRESENT = 0;
static final int CHECK_ALWAYS = 1;
static final int waitTimeMS = 300000;

// other static fields
static RandomValues randomValues = null;
public static final String REGION_NAME = "mirrorTestRegion";

/* INITTASK/STARTTASK methods */
/* ======================================================================== */
public static synchronized void HydraTask_initWithListener() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
   if (randomValues == null) {
      randomValues = new RandomValues();
      String listenerClassName = TestConfig.tasktab().stringAt(MirrorPrms.listenerClassName, null);
      CacheListener listener = null;
      if (listenerClassName != null) {
         Class listenerClass = Class.forName(listenerClassName);
         listener = (CacheListener)listenerClass.newInstance();
      }

      // Let's look for dataPolicy first, if we don't find it, we can fall back to 
      // deprecated mirroring attribute.
      DataPolicy dataPolicy = null;
      MirrorType mirroring = null;

      String dataPolicyAttribute = TestConfig.tasktab().stringAt(util.CachePrms.dataPolicyAttribute, null);
      if (dataPolicyAttribute != null) {
         dataPolicy = TestHelper.getDataPolicy(dataPolicyAttribute);
       } else {
         String mirrorAttribute = TestConfig.tasktab().stringAt(util.CachePrms.mirrorAttribute, null);
         if (mirrorAttribute != null) 
            mirroring = TestHelper.getMirrorType(mirrorAttribute);
       } 

       boolean expectErrorOnRegionConfig = TestConfig.tab().booleanAt(MirrorPrms.expectErrorOnRegionConfig, false);
       RegionDefinition regDef = RegionDefinition.createRegionDefinition();
      // if this test specified disk region attributes, then merge them into the regDef
      String result = TestConfig.tab().stringAt(DiskRegPrms.diskAttrSpecs, "not specified");
      if (!result.equals("not specified"))
         processDiskRegionAttrs(regDef);

      if (dataPolicy != null) {
         regDef.setDataPolicy(dataPolicy);
      } else if (mirroring != null) {
         regDef.setMirroring(mirroring);
      }

      Log.getLogWriter().info("Using RegionDefinition " + regDef + " to create region");
      try {
         regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, listener, null, null); 
         if (expectErrorOnRegionConfig) {
            throw new TestException("Expected error on region configuration, but did not receive an error");
         }
      } catch (IllegalStateException e) {
         if (expectErrorOnRegionConfig) {
            String errStr = e.toString();
            if ((errStr.indexOf("Local Scope is incompatible with mirroring") >= 0) ||
                (errStr.indexOf("Local Scope is incompatible with replication") >= 0)) { // OK; this was expected
               Log.getLogWriter().info("Caught expected error " + e + " " + TestHelper.getStackTrace(e));
            } else {
               throw e;
            }
         } else { // no error expected
            throw e;
         }
      }
      List diskDirList = regDef.getDiskDirList();
      Boolean persist = regDef.getPersistBackup();
      if ((diskDirList != null) && (diskDirList.size() != 0) && (persist != null) && (persist.booleanValue())) {
         regDef.setDiskDirList(diskDirList);
         String dirListKey = new String(RemoteTestModule.getMyClientName() + "_" + DiskRegBB.BACKUP_DIR_LIST);
         DiskRegBB.getBB().getSharedMap().put(dirListKey, regDef.getDiskDirList());
         Log.getLogWriter().info("put BACKUP_DIR_LIST to BB sharedMap " + regDef.getDiskDirList() + " with key = " + dirListKey);
      }
      DiskRegBB.getBB().print();
   }
}

/* TASK methods to update */
/* ======================================================================== */
public static void HydraTask_updatePositive() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   int totalNumObjectsToMirror = TestConfig.tab().intAt(MirrorPrms.totalNumObjectsToMirror);
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_updatePositive, running for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   if (MirrorPrms.useTransactions()) {
      TxHelper.begin();
   }
   do {
      String key = NameFactory.getNextPositiveObjectName();
      update(key);
      long total = NameFactory.getCounterForName(key) + Math.abs(NameFactory.getNegativeNameCounter());
      if (total >= totalNumObjectsToMirror) {
         if (TxHelper.exists()) {
            TxHelper.commitExpectSuccess();
         }
         throw new StopSchedulingOrder("Test has mirrored " + total + 
                   " objects, min required is " + totalNumObjectsToMirror);
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   if (TxHelper.exists()) {
      TxHelper.commitExpectSuccess();
   }
}

public static void HydraTask_updateNegative() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   int totalNumObjectsToMirror = TestConfig.tab().intAt(MirrorPrms.totalNumObjectsToMirror);
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In HydraTask_updateNegative, running for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   if (MirrorPrms.useTransactions()) {
      TxHelper.begin();
   }
   do {
      String key = NameFactory.getNextNegativeObjectName();
      update(key);
      long total = Math.abs(NameFactory.getCounterForName(key)) + NameFactory.getPositiveNameCounter();
      if (total >= totalNumObjectsToMirror) {
         if (TxHelper.exists()) {
            TxHelper.commitExpectSuccess();
         }
         throw new StopSchedulingOrder("Test has mirrored " + total + 
                   " objects, min required is " + totalNumObjectsToMirror);
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   if (TxHelper.exists()) {
      TxHelper.commitExpectSuccess();
   }
}

protected static void update(Object key) {
   BaseValueHolder newValue = new ValueHolder((String)key, randomValues);
   Region aRegion = CacheUtil.getCache().getRegion(REGION_NAME);
   Log.getLogWriter().info("Putting key " + key + " with value " + TestHelper.toString(newValue));
   try {
      aRegion.put(key, newValue);
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } 
   Log.getLogWriter().info("Done putting key " + key + " with value " + TestHelper.toString(newValue));
}

/* TASK methods to read */
/* ======================================================================== */

/** Iterate through the root region expecting to find positive keys and non-null values. 
 */
public static void HydraTask_readPositive() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   Validator val = new Validator();
      val.setContainsKeyCategory(Validator.POSITIVE);
      val.setContainsValueForKeyCategory(Validator.POSITIVE);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(MirrorBB.isSerialExecution());
      val.setWaitForContainsKey(true);
      val.setWaitForContainsValueForKey(true);
      val.setThrowOnFirstError(true);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(CacheUtil.getCache().getRegion(REGION_NAME));
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

/** Iterate through the root region expecting to find negative keys and non-null values. 
 */
public static void HydraTask_readNegative() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   Validator val = new Validator();
      val.setContainsKeyCategory(Validator.NEGATIVE);
      val.setContainsValueForKeyCategory(Validator.NEGATIVE);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(MirrorBB.isSerialExecution());
      val.setWaitForContainsKey(true);
      val.setWaitForContainsValueForKey(true);
      val.setThrowOnFirstError(true);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(CacheUtil.getCache().getRegion(REGION_NAME));
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

/** Iterate through the root region expecting to find positive and negative keys and non-null values. 
 */
public static void HydraTask_readPositiveAndNegative() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   Validator val = new Validator();
      val.setContainsKeyCategory(Validator.BOTH);
      val.setContainsValueForKeyCategory(Validator.BOTH);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(MirrorBB.isSerialExecution());
      val.setWaitForContainsKey(true);
      val.setWaitForContainsValueForKey(true);
      val.setThrowOnFirstError(true);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(CacheUtil.getCache().getRegion(REGION_NAME));
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

/** Iterate through the root region expecting to find positive and negative keys. 
 *  Values may be null or non-null.
 */
public static void HydraTask_readKeys() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   Validator val = new Validator();
      val.setContainsKeyCategory(Validator.BOTH);
      val.setContainsValueForKeyCategory(Validator.DONT_CHECK);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(MirrorBB.isSerialExecution());
      val.setWaitForContainsKey(true);
      val.setWaitForContainsValueForKey(true);
      val.setThrowOnFirstError(true);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(CacheUtil.getCache().getRegion(REGION_NAME));
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

/** Iterate through the root region expecting to find positive and negative keys with null values. 
 */
public static void HydraTask_readKeysButNoValues() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   Validator val = new Validator();
      val.setContainsKeyCategory(Validator.BOTH);
      val.setContainsValueForKeyCategory(Validator.NONE);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.ALWAYS);
      val.setStableKeys(MirrorBB.isSerialExecution());
      val.setWaitForContainsKey(true);
      val.setWaitForContainsValueForKey(true);
      val.setThrowOnFirstError(true);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(CacheUtil.getCache().getRegion(REGION_NAME));
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

/** Iterate through the root region expecting to find positive and negative keys 
 *  with null values for the positive keys, and non-null values for the negative keys.
 */
public static void HydraTask_readKeysButNoPositiveValues() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   Validator val = new Validator();
      val.setContainsKeyCategory(Validator.BOTH);
      val.setContainsValueForKeyCategory(Validator.NEGATIVE);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(MirrorBB.isSerialExecution());
      val.setWaitForContainsKey(true);
      val.setWaitForContainsValueForKey(true);
      val.setThrowOnFirstError(true);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(CacheUtil.getCache().getRegion(REGION_NAME));
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

/** Iterate through the root region expecting to find positive and negative keys 
 *  with null values for the negative keys, and non-null values for the positive keys.
 */
public static void HydraTask_readKeysButNoNegativeValues() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   Validator val = new Validator();
      val.setContainsKeyCategory(Validator.BOTH);
      val.setContainsValueForKeyCategory(Validator.POSITIVE);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(MirrorBB.isSerialExecution());
      val.setWaitForContainsKey(true);
      val.setWaitForContainsValueForKey(true);
      val.setThrowOnFirstError(true);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(CacheUtil.getCache().getRegion(REGION_NAME));
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

/** Verify that there are no keys in the root region 
 */
public static void HydraTask_noKeys() {
   if (MirrorBB.isSerialExecution())
      logExecutionNumber();
   // get the KeySet for the region 
   Region aRegion = CacheUtil.getCache().getRegion(REGION_NAME);
   Set keySet = aRegion.keys();
   int size = keySet.size();
   Iterator it = keySet.iterator();
   while (it.hasNext())
      Log.getLogWriter().info("Found key " + it.next());
   Log.getLogWriter().info("HydraTask_noKeys: got keySet of size " + keySet.size());
   if (size > 0) {
      throw new TestException("Expected no keys to be propagated, but keySet size is " + size + "\n" +
                              NameFactory.getKeySetDiscrepancies(aRegion, keySet, 0, 0));
   }
}

/* ENDTASK or CLOSE methods */
/* ======================================================================== */

/** Validate end task for mirror NONE or LOCAL scope. 
 */
private static void validateNoPropagation(
        CacheListener listener, 
        int numAfterCreateEventsForListener) {
   StringBuffer errStr = new StringBuffer();
   boolean scopeIsLocal = RegionDefinition.createRegionDefinition().getScope().isLocal();
   EndTaskValidator val = new EndTaskValidator();
      val.setContainsKeyCategory(Validator.NONE);
      val.setContainsValueForKeyCategory(Validator.NONE);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(scopeIsLocal ? Validator.ALWAYS : Validator.NEVER);
      val.setStableKeys(true);
      val.setWaitForContainsKey(false);
      val.setWaitForContainsValueForKey(false);
      val.setThrowOnFirstError(false);
      val.setBlackboard(CacheBB.getBB());
      val.setListener(listener);
      val.setNumAfterCreateEventsForListener(numAfterCreateEventsForListener);
      val.setNumAfterCreateEventsForOtherListeners(scopeIsLocal ? 0 : -1);
      val.setCreationKeySetSize(0);
      val.setEndKeySetSize(0);
   errStr.append(val.endTaskValidate());
// lynn
//   errStr.append(val.validateMirrorNoneEvents());
   CacheBB.getBB().printSharedCounters();
   MirrorBB.getMirrorBB().printSharedCounters();
   if (errStr.length() > 0)
      throw new TestException(errStr.toString());
}
public static void HydraTask_validateNoPropagation() {
   boolean scopeIsLocal = RegionDefinition.createRegionDefinition().getScope().isLocal();
   validateNoPropagation(getNextEndTaskListener(), 0);
}
public static void HydraTask_validateNoPropagationPositive() {
   boolean scopeIsLocal = RegionDefinition.createRegionDefinition().getScope().isLocal();
   int numNames = scopeIsLocal ? 0: Math.abs((int)NameFactory.getNegativeNameCounter());
   validateNoPropagation(getNextEndTaskListener(), numNames);
}
public static void HydraTask_validateNoPropagationNegative() {
   boolean scopeIsLocal = RegionDefinition.createRegionDefinition().getScope().isLocal();
   int numNames = scopeIsLocal ? 0: Math.abs((int)NameFactory.getPositiveNameCounter());
   validateNoPropagation(getNextEndTaskListener(), numNames);
}
   
/** Validate end task VMs for MIRROR_KEYS */
private static String validateMirrorKeys(int numAfterCreateEventsForListener,
                                         int containsKeyCategory,
                                         int containsValueForKeyCategory,
                                         int creationKeySetSize) {
   EndTaskValidator val = new EndTaskValidator();
      val.setContainsKeyCategory(containsKeyCategory); 
      val.setContainsValueForKeyCategory(containsValueForKeyCategory); 
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(true);
      val.setWaitForContainsKey(false);
      val.setWaitForContainsValueForKey(false);
      val.setThrowOnFirstError(false);
      val.setBlackboard(CacheBB.getBB());
      val.setListener(getNextEndTaskListener());
      val.setNumAfterCreateEventsForListener(numAfterCreateEventsForListener);
      val.setNumAfterCreateEventsForOtherListeners(-1);
      val.setCreationKeySetSize(creationKeySetSize);
      val.setEndKeySetSize(creationKeySetSize);
      val.setValidatePositiveNameCounter(true);
      val.setValidateNegativeNameCounter(true);
   StringBuffer errStr = new StringBuffer();
// lynn
//   errStr.append(val.validateMirrorKeysEvents());
   errStr.append(val.endTaskValidate());
   return errStr.toString();
}
public static void HydraTask_validateMirrorKeysPositive() {
   String errStr = validateMirrorKeys(
                   (int)Math.abs(NameFactory.getNegativeNameCounter()), Validator.POSITIVE, Validator.POSITIVE,
                   (int)NameFactory.getPositiveNameCounter());
   if (errStr.length() > 0)
      throw new TestException(errStr);
}
public static void HydraTask_validateMirrorKeysNegative() {
   String errStr = validateMirrorKeys(
                   (int)(NameFactory.getPositiveNameCounter()), Validator.NEGATIVE, Validator.NEGATIVE,
                   (int)Math.abs(NameFactory.getNegativeNameCounter()));
   if (errStr.length() > 0)
      throw new TestException(errStr);
}
public static void HydraTask_validateMirrorKeys() {
   String errStr = validateMirrorKeys( 
                   (int)NameFactory.getTotalNameCounter(), Validator.BOTH, Validator.NONE,
                   (int)NameFactory.getTotalNameCounter());
   if (errStr.length() > 0)
      throw new TestException(errStr);
}

/** Validate end task VMs for mirror keysValues */
public static void HydraTask_validateMirrorKeysValues() {
   StringBuffer errStr = new StringBuffer();
   EndTaskValidator val = new EndTaskValidator();
      val.setContainsKeyCategory(Validator.BOTH);
      val.setContainsValueForKeyCategory(Validator.BOTH);
      val.setDoGetStrategy(Validator.ALWAYS);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(true);
      val.setWaitForContainsKey(false);
      val.setWaitForContainsValueForKey(false);
      val.setThrowOnFirstError(false);
      val.setBlackboard(CacheBB.getBB());
      val.setListener(new EndTaskListener1());
      val.setNumAfterCreateEventsForListener(0);
      val.setNumAfterCreateEventsForOtherListeners(-1);
      val.setCreationKeySetSize((int)NameFactory.getTotalNameCounter());
      val.setEndKeySetSize((int)NameFactory.getTotalNameCounter());
      val.setValidatePositiveNameCounter(true);
      val.setValidateNegativeNameCounter(true);
   errStr.append(val.endTaskValidate());
// lynn
//   errStr.append(val.validateMirrorKeysValuesEvents());
   if (errStr.length() > 0)
      throw new TestException(errStr);
}

/* private methods */
/* ======================================================================== */
static void logExecutionNumber() {
   long exeNum = MirrorBB.getMirrorBB().getSharedCounters().incrementAndRead(MirrorBB.EXECUTION_NUMBER);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
}

private static CacheListener getNextEndTaskListener() {
   long nextListenerNum = MirrorBB.getMirrorBB().getSharedCounters().incrementAndRead(MirrorBB.NEXT_END_TASK_LISTENER);
   String className = "mirror.EndTaskListener" + nextListenerNum;
   try {
      Class listenerClass = Class.forName(className);
      CacheListener listener = (CacheListener)listenerClass.newInstance();
      return listener;
   } catch (ClassNotFoundException e) {
      throw new TestException("Attempt to use non-existent listener " + className);
   } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Read the disk region attributes and merge them into regDef.
*  Initialize this test instance's fields with the disk region attributes
*
*  @param regDef The region definition to put the disk region attributes into.
*/
public static void processDiskRegionAttrs(RegionDefinition regDef) {
   // Get the specific disk region attributes for this VM, a different one for each VM potentially
   int whichDiskRegionAttrs = (int)(DiskRegBB.getBB().getSharedCounters().
       incrementAndRead(DiskRegBB.WHICH_DISK_REGION_ATTRS));
   ArrayList diskRegionSpecNames = new ArrayList( RegionDefinition.getSpecNames(DiskRegPrms.diskAttrSpecs));
   whichDiskRegionAttrs = whichDiskRegionAttrs % diskRegionSpecNames.size();
   String specName = (String)(diskRegionSpecNames.get(whichDiskRegionAttrs));
   RegionDefinition diskRegionAttrs = RegionDefinition.createRegionDefinition(DiskRegPrms.diskAttrSpecs, specName);
   Log.getLogWriter().info("Using disk region attributes from spec: " + specName);

   // merge the selected disk attributes into the region definition
   regDef.setPersistBackup(diskRegionAttrs.getPersistBackup());
   regDef.setEvictionLimit(diskRegionAttrs.getEvictionLimit());
   regDef.setIsSynchronous(diskRegionAttrs.getIsSynchronous());
   regDef.setTimeInterval(diskRegionAttrs.getTimeInterval());
   regDef.setBytesThreshold(diskRegionAttrs.getBytesThreshold());
   regDef.setNumDiskDirs(diskRegionAttrs.getNumDiskDirs());
   regDef.setDiskDirList(diskRegionAttrs.getDiskDirList());
   regDef.setEvictionAction(diskRegionAttrs.getEvictionAction());
   ////added by Prafulla for disk region implementation changes
   regDef.setRollOplogs(diskRegionAttrs.getRollOplogs());
   regDef.setMaxOplogSize(diskRegionAttrs.getMaxOplogSize());
   // Allow override of RegionDefinition specification for using backupDiskDirs
   // (saved in BB shared map) or using list specified by task specific Attributes.
   boolean useBackupDiskDirs = TestConfig.tasktab().booleanAt(DiskRegPrms.useBackupDiskDirs,
                              TestConfig.tab().booleanAt(DiskRegPrms.useBackupDiskDirs, false));
   ArrayList diskDirList = null;
   if (useBackupDiskDirs) { // get the existing backup from the blackboard
     String dirListKey = new String(RemoteTestModule.getMyClientName() + "_" + DiskRegBB.BACKUP_DIR_LIST);
     diskDirList = (ArrayList)DiskRegBB.getBB().getSharedMap().get(dirListKey);
     Log.getLogWriter().fine("got BACKUP_DIR_LIST from DiskRegBB " + diskDirList + " with key = " + dirListKey);
	   
     if (diskDirList == null) {
       String s = "No disk dir list found in blackboard under name "+ dirListKey;
         throw new TestException(s);	   
     } else {
       regDef.setDiskDirList(diskDirList);
        regDef.setNumDiskDirs(diskDirList.size());
     }
	   
     } else {
      String runID = (new File(System.getProperty("user.dir"))).getName();
      Vector aVec = TestConfig.tasktab().vecAt(DiskRegPrms.diskDirNames,
      TestConfig.tab().vecAt(DiskRegPrms.diskDirNames, new HydraVector()));
      diskDirList = new ArrayList();
      for (int i = 0; i < aVec.size(); i++) {
          String dirName = ((String)(aVec.elementAt(i))) + "-" + runID;
           diskDirList.add(dirName);
   }	   
   if (diskDirList.size() != 0) {
       regDef.setDiskDirList(diskDirList);
    }
   }
    
   regDef.setEviction(diskRegionAttrs.getEviction());
   regDef.setEvictionLimit(diskRegionAttrs.getEvictionLimit());
   regDef.setEvictionAction(diskRegionAttrs.getEvictionAction());
   regDef.setObjectSizerClass(diskRegionAttrs.getObjectSizerClass());
}

}

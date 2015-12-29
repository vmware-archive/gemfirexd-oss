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
package diskReg;

import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.DiskStoreHelper;
import hydra.Log;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import util.BaseValueHolder;
import util.CacheBB;
import util.CacheUtil;
import util.NameBB;
import util.NameFactory;
import util.RandomValues;
import util.RegionDefinition;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.Validator;
import util.ValueHolder;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;

public abstract class DiskRegTest implements java.io.Serializable {

// single static instance of the test class
public static DiskRegTest testInstance; 
protected static RegionAttributes attr;
protected static String regionConfig;

// final fields
protected static final String REGION_NAME = "diskRegion";
protected static final int KEY_LENGTH = 20;
protected static final int NO_DISK = 1;
protected static final int DISK_FOR_OVRFLW = 2;           
protected static final int DISK_FOR_PERSIST = 3;        
protected static final int DISK_FOR_OVRFLW_PERSIST = 4; 


// operations
static protected final int ADD_OPERATION = 1;
static protected final int UPDATE_OPERATION = 2;
static protected final int INVALIDATE_OPERATION = 3;
static protected final int DESTROY_OPERATION = 4;
static protected final int READ_OPERATION = 5;
static protected final int LOCAL_INVALIDATE_OPERATION = 6;
static protected final int LOCAL_DESTROY_OPERATION = 7;
static protected final int PUT_IF_ABSENT = 8;
static protected final int REPLACE = 9; 
static protected final int REMOVE = 10;

protected int lowerThreshold;           //Value of MapRegion.lowerThreshold
protected int upperThreshold;           //Value of MapRegion.upperThreshold 

// instance fields
protected Region aRegion;
protected RandomValues randomValues;
protected int regionKind;
protected int numVMs;

// cache the values of diskRegion attributes in this VM's instance of this class
protected boolean persistBackup;
protected int evictionLimit;
protected long numDiskDirs;
protected String lruKind;
protected boolean isReplicated;

// static fields
protected static String spaceKey = null;              // a key with all spaces

static {
   StringBuffer sb = new StringBuffer();
   for (int i = 0; i < KEY_LENGTH; i++) sb.append(" ");
   spaceKey = sb.toString();
}

// initialization methods
// ================================================================================
/** Initialize this test instance */
protected void initialize() {
  if (!(this instanceof DiskRegRecoveryTest)) {
    boolean useComplexObject = TestConfig.tab().booleanAt(DiskRegPrms.useComplexObject);
    DiskRegBB.getBB().getSharedMap().put("useComplexObject", new Boolean(useComplexObject));
    randomValues = new RandomValues();
  }

  numVMs = TestHelper.getNumVMs();

  ////////////////////////////////////////////
  CacheHelper.createCache(ConfigPrms.getCacheConfig());
  regionConfig = (String)(DiskRegBB.getBB().getSharedMap().get(DiskRegBB.REGION_CONFIG_NAME));
  attr = RegionHelper.getRegionAttributes(regionConfig);  
  AttributesFactory attrFact = RegionHelper.getAttributesFactory(regionConfig);

  boolean useBackupDiskDirs = TestConfig.tasktab().booleanAt(DiskRegPrms.useBackupDiskDirs, TestConfig.tab().booleanAt(DiskRegPrms.useBackupDiskDirs, false));   
  if (useBackupDiskDirs){
    // see if there is a persistent backup available from running this test

    String dirListKey = new String(RemoteTestModule.getMyClientName() + "_" + DiskRegBB.BACKUP_DIR_LIST);
    File [] diskDirArr = new File [((File []) DiskRegBB.getBB().getSharedMap().get(dirListKey)).length];
    diskDirArr = (File []) DiskRegBB.getBB().getSharedMap().get(dirListKey);
    Log.getLogWriter().info("In HydraTask_recoveryEndTask: got BACKUP_DIR_LIST from BB " + diskDirArr + " with key = " + dirListKey);
    if(diskDirArr != null) {
      DiskStoreFactory dsFactory = DiskStoreHelper.getDiskStoreFactory(attr.getDiskStoreName());
      Log.getLogWriter().info("Setting disk dirs to " + diskDirArr);
      dsFactory.setDiskDirs(diskDirArr);
      attrFact.setDiskStoreName(attr.getDiskStoreName());
      dsFactory.create(attr.getDiskStoreName());
    }   
  } else {
    DiskStoreFactory dsFactory = DiskStoreHelper.getDiskStoreFactory(attr.getDiskStoreName());
    DiskStore ds = dsFactory.create(attr.getDiskStoreName());
    if (ds.getDiskDirs() != null) {
      String dirListKey = new String(RemoteTestModule.getMyClientName() + "_" + DiskRegBB.BACKUP_DIR_LIST);
      DiskRegBB.getBB().getSharedMap().put(dirListKey, ds.getDiskDirs());
      Log.getLogWriter().info("put BACKUP_DIR_LIST to DiskRegBB sharedMap " + ds.getDiskDirs() + " with key = " + dirListKey);
    }
  }
  attr = attrFact.create();
  RegionDescription reg  = RegionHelper.getRegionDescription(regionConfig);
  String regionName = reg.getRegionName();
  aRegion = CacheHelper.getCache().createRegion(regionName, attr);
  Log.getLogWriter().info("A region with the following attributes is created:-" + attr.toString());   

  // set the selected disk attributes in this test instance's field
  this.persistBackup = false;    
  this.persistBackup = attr.getDataPolicy().isPersistentReplicate();

  this.evictionLimit = 0;
  try {
    evictionLimit = attr.getEvictionAttributes().getMaximum();
  } catch(Exception e) {
    evictionLimit = (int)aRegion.getCache().getResourceManager().getEvictionHeapPercentage();
  }

  this.numDiskDirs = 0;
  numDiskDirs = DiskStoreHelper.getDiskStore(attr.getDiskStoreName()).getDiskDirs().length;

  //lruKind = attr.getEvictionAttributes().getAlgorithm().toString();

  // set the type of region for this VM
  if (persistBackup == true) {
    if (evictionLimit <= 0)
      regionKind = DISK_FOR_PERSIST;
    else
      regionKind = DISK_FOR_OVRFLW_PERSIST;
  } else {
    if (evictionLimit <= 0)
      regionKind = NO_DISK;
    else
      regionKind = DISK_FOR_OVRFLW;
  }

  isReplicated = false;
  if (attr.getDataPolicy().withPersistence()) {
    isReplicated = true;
  }
}
   
// ================================================================================
// Hydra task methods

public static void HydraTask_doEntryOperations() {
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   int endTestOnNumKeysInRegion = TestConfig.tab().intAt(DiskRegPrms.endTestOnNumKeysInRegion, -1);
   Log.getLogWriter().info("In HydraTask_doEntryOperations for " + timeToRunSec + " seconds, endTestOnNumKeysInRegion is " + endTestOnNumKeysInRegion);
   long startTime = System.currentTimeMillis();
   do {
      testInstance.doEntryOperations();
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In HydraTask_doEntryOperations, done running for " + timeToRunSec + " seconds");
   if (endTestOnNumKeysInRegion > -1) {
      int numKeys = testInstance.aRegion.keySet().size();
      Log.getLogWriter().info("Current numKeys is " + numKeys + ", endTestOnNumKeysInRegion is " +
          endTestOnNumKeysInRegion);
      if (numKeys >= endTestOnNumKeysInRegion) {
         throw new StopSchedulingTaskOnClientOrder("Workload based test has " + numKeys + 
            " keys in region, endTestOnNumKeysInRegion is " + endTestOnNumKeysInRegion);
      }
   }
}

// method to do work
// ================================================================================

protected void doEntryOperations() {
  long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
  long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
  long startTime = System.currentTimeMillis();
   
  lowerThreshold = TestConfig.tab().intAt(DiskRegPrms.lowerThreshold, -1);
  upperThreshold = TestConfig.tab().intAt(DiskRegPrms.upperThreshold, Integer.MAX_VALUE);  
  
 
  do {
      checkForEventError();

      int whichOp = getOperation(DiskRegPrms.entryOperations);
     
      int size = aRegion.size();
 
      if (size >= upperThreshold) {
         whichOp = getOperation(DiskRegPrms.upperThresholdOperations);
      }else if (size <= lowerThreshold) {
         whichOp = getOperation(DiskRegPrms.lowerThresholdOperations);
      }

      switch (whichOp) {
         case ADD_OPERATION: 
            addEntry();
            break;
         case INVALIDATE_OPERATION:
            invalidateEntry(false);
            break;
         case DESTROY_OPERATION:
            destroyEntry(false);
            break;
         case UPDATE_OPERATION:
            updateEntry();
            break;
         case READ_OPERATION:
            readEntry();
            break;
         case LOCAL_INVALIDATE_OPERATION:
            invalidateEntry(true);
            break;
         case LOCAL_DESTROY_OPERATION:
            destroyEntry(true);
            break;
         case PUT_IF_ABSENT:
            putIfAbsent();
            break;
         case REPLACE:
            replace();
            break;
         case REMOVE:
            remove();
            break;
         default: {
            throw new TestException("Unknown operation " + whichOp);
         }
      }
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

/** Add multiple entries to the region 
 *  This adds new keys never added before.
 *
 */
protected void addEntries() {
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In addEntries, adding for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      if (TestConfig.tab().getRandGen().nextBoolean()) {
         putIfAbsent();
      } else {
      addEntry();
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In addEntries, done running for " + timeToRunSec + " seconds");
}

/** Add an entry to the region 
 *  adds a new key, never added before.
 *
 *  @return [0] the key that was added, [1] the value that was added 
 *          [2] true if the value was added via a get (cacheLoader)
 */
protected Object[] addEntry() {
   Object key = getNewKey();
   Object value = null;
   boolean useCacheLoader = useCacheLoader();
   if (useCacheLoader) { // use a get
      String aStr = key + " in " + REGION_NAME;
      Log.getLogWriter().info("Getting " + aStr);
      value = aRegion.get(key);
      Log.getLogWriter().info("Done getting " + aStr);
   } else { // use a put
      value = getNewValue(key);
      String aStr = "\"" + key + "\" with value " + TestHelper.toString(value) + " in " + REGION_NAME;
      Log.getLogWriter().info("Putting " + aStr);
      aRegion.put(key, value);
      Log.getLogWriter().info("Done putting " + aStr);
   } 
   checkForEventError();
   return new Object[] {key, value, new Boolean(useCacheLoader)};
}

/** Add an entry to the region using the putIfAbsent API
 *  adds a new key, never added before.
 *
 *  @return [0] the key that was added, [1] the value that was added 
 *          [2] true if the value was added via a get (cacheLoader)
 */
protected Object[] putIfAbsent() {
   Object key = getNewKey();
   Object value = null;
   value = getNewValue(key);
   String aStr = "\"" + key + "\" with value " + TestHelper.toString(value) + " in " + REGION_NAME;
   Log.getLogWriter().info("executing putIfAbsent " + aStr);
   Object oldVal = aRegion.putIfAbsent(key, value);
   Log.getLogWriter().info("putIfAbsent " + aStr + " returns " + oldVal);
   checkForEventError();
   return new Object[] {key, value, new Boolean(false)};
}

/** Invalidate an entry */
protected void invalidateEntry(boolean isLocalInvalidate) {
   Object key = getOldOrRecentKey();
   if (key == null) {
      Log.getLogWriter().info("invalidateEntry: Unable to get key from region");
      return; 
   }
   try {
      if (isLocalInvalidate) {
         Log.getLogWriter().info("invalidateEntry: local invalidate for " + key);
         aRegion.localInvalidate(key);
         Log.getLogWriter().info("invalidateEntry: done with local invalidate for " + key);
      } else {
         Log.getLogWriter().info("invalidateEntry: invalidating key " + key);
         aRegion.invalidate(key);
         Log.getLogWriter().info("invalidateEntry: done invalidating key " + key);
      }
   } catch (EntryNotFoundException e) {
     Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
     return;
   } 
}

/** destroy an entry */
protected void destroyEntry(boolean isLocalDestroy) {
   Object key = getOldOrRecentKey();
   if (key == null) {
      Log.getLogWriter().info("destroyEntry: Unable to get key from region");
      return; 
   }
   try {
      if (isLocalDestroy) {
         Log.getLogWriter().info("destroyObject: local destroy for " + key);
         aRegion.localDestroy(key);
         Log.getLogWriter().info("destroyObject: done with local destroy for " + key);
      } else {
         Log.getLogWriter().info("destroyObject: destroying key " + key);
         aRegion.destroy(key);
         Log.getLogWriter().info("destroyObject: done destroying key " + key);
      }
   } catch (EntryNotFoundException e) {
     Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
     return;
   } 
}

/** remove an entry */
protected void remove() {
   Object key = getOldOrRecentKey();
   if (key == null) {
      Log.getLogWriter().info("remove: Unable to get key from region");
      return; 
   }
   try {
      Log.getLogWriter().info("remove: removing key " + key);
      boolean removed = aRegion.remove(key, aRegion.get(key));
      Log.getLogWriter().info("remove " + key + " returned " + removed);
   } catch (EntryNotFoundException e) {
     Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
     return;
   } 
}

/** update an entry */
protected void updateEntry() {
   Object key = getOldOrRecentKey();
   if (key == null) {
      Log.getLogWriter().info("updateEntry: Unable to get key from region");
      return; 
   }
   Log.getLogWriter().info("In updateEntry, getting value for key " + key);
   Object anObj = aRegion.get(key);
   Log.getLogWriter().info("In updateEntry, done getting value for key " + key);

   Object newObj = getUpdateObject((String)key);
   Log.getLogWriter().info("updateEntry: replacing key " + key + " with " + 
       TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj));
   aRegion.put(key, newObj);
   Log.getLogWriter().info("Done with call to put (update)");
}

/** replace an entry */
protected void replace() {
   Object key = getOldOrRecentKey();
   if (key == null) {
      Log.getLogWriter().info("replace: Unable to get key from region");
      return; 
   }
   Log.getLogWriter().info("In replace, getting value for key " + key);
   Object anObj = aRegion.get(key);
   Log.getLogWriter().info("In replace, done getting value for key " + key);

   Object newObj = getUpdateObject((String)key);
   Log.getLogWriter().info("replace: replacing key " + key + " with " + 
       TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj));
   aRegion.replace(key, newObj);
   Log.getLogWriter().info("Done with call to replace");
}

/** read an object */
protected void readEntry() {
   final int RANGE = 50;
   randomGet(RANGE);
}

/** Do a get in order to try to bring in entries from other distributed
 *  caches. This get may or may not actually bring in a new entry.
 *
 *  @param range How far back in previously created keys to look. 
 *
 *  @returns [0] Object, the key for the get 
 *           [1] Object, the value for the get.
 *           [2] Boolean, containsKey before the get occurred; this may not be
 *               accurate in concurrent tests as the key may not be there
 *               when containsKey is called, but there when the get occurs.
 *            
 */
protected Object[] randomGet(int range) {
   Object key = getPreviousKey(range);
   if (key == null) // there is no previous key
       return null;
   boolean containsKeyBeforeGet = aRegion.containsKey(key);
   String aStr = key + " in " + REGION_NAME;
   Log.getLogWriter().info("Getting previous key " + aStr);
   Object value = aRegion.get(key);
   Log.getLogWriter().info("Done getting " + aStr);
   return new Object[] {key, value, new Boolean(containsKeyBeforeGet)};
}

public Object getNewKey() {
   String key = getFixedLengthKeyForName(NameFactory.getNextPositiveObjectName());
   return key;
}

public Object getNewValue(Object key) {
   if (((Boolean)(DiskRegBB.getBB().getSharedMap().get("useComplexObject"))).booleanValue()) {
      return new ValueHolder((String)key, randomValues);
   } else {
      return randomValues.getRandomObjectGraph();
   }
}

public CacheListener getEventListener() {
   return new DiskRegListener();
}

// method that can be overridden in subclasses for a more customized test
// ================================================================================
public Object getPreviousKey(int range) {
   long currentNameCounter = NameFactory.getPositiveNameCounter();
   if (currentNameCounter <= 0) // there is no previous key
      return null;
   long lowEnd = currentNameCounter - range;
   if (lowEnd <= 0)
      lowEnd = 1;
   long previousNameCounter = TestConfig.tab().getRandGen().nextLong(lowEnd, currentNameCounter);
   return getFixedLengthKeyForName(NameFactory.getObjectNameForCounter(previousNameCounter)); 
}

static protected void checkForEventError() {
   TestHelper.checkForEventError(DiskRegBB.getBB());
}

protected CacheLoader getCacheLoader() {
   return new DiskRegLoader();
}

protected static String getFixedLengthKeyForName(String name) {
   StringBuffer sb = new StringBuffer(spaceKey);
   sb.replace(0, name.length(), name);
   if (sb.length() != KEY_LENGTH)
      throw new TestException("Expected key to be length " + KEY_LENGTH + ", but it is " + 
            TestHelper.toString(sb));
   return sb.toString();
}

//========================================================================
// string methods

/** Return a string describing this test instance.
 *
 */
public String toVerboseString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("Test instance " + super.toString() + "\n");
   aStr.append("     persistBackup: " + persistBackup + "\n");
   aStr.append("     evictionLimit: " + evictionLimit + "\n");
   aStr.append("     numDiskDirs: " + numDiskDirs + "\n");
   aStr.append("     regionKind: " + regionKindToString(regionKind) + "\n");
   aStr.append("     lruKind: " + lruKind + "\n");
   return aStr.toString();
}

/** Return a string describing the given regionKind.
 *
 *  @params regionKind The kind of region used by this VM. Can be one of
 *          NO_DISK, DISK_FOR_OVRFLW, DISK_FOR_PERSIST, or DISK_FOR_OVRFLW_PERSIST.
 *
 *  @returns A string describing regionKind
 */
protected String regionKindToString(int kind) {
   switch (kind) {
      case NO_DISK: return "No disk";
      case DISK_FOR_OVRFLW: return "Disk for overflow";
      case DISK_FOR_PERSIST: return "Disk for persistence";
      case DISK_FOR_OVRFLW_PERSIST: return "Disk for overflow and persistence";
      default: throw new TestException("unknown region kind " + kind);
   }
} 

//========================================================================
// other methods
protected int getOperation(Long whichPrm) {
   long limit = 60000;
   long startTime = System.currentTimeMillis();
   int op = 0;
   do {
      String operation = TestConfig.tab().stringAt(whichPrm);
      if (operation.equals("add")) 
         op =  ADD_OPERATION;
      else if (operation.equals("update"))
         op =  UPDATE_OPERATION;
      else if (operation.equals("invalidate"))
         op =  INVALIDATE_OPERATION;
      else if (operation.equals("destroy"))
         op =  DESTROY_OPERATION;
      else if (operation.equals("read"))
         op =  READ_OPERATION;
      else if (operation.equals("localInvalidate"))
         op =  LOCAL_INVALIDATE_OPERATION;
      else if (operation.equals("localDestroy"))
         op =  LOCAL_DESTROY_OPERATION;
      else if (operation.equalsIgnoreCase("putIfAbsent"))
         op =  PUT_IF_ABSENT;
      else if (operation.equalsIgnoreCase("replace")) 
         op =  REPLACE;
      else if (operation.equalsIgnoreCase("remove"))
         op = REMOVE;
      else
         throw new TestException("Unknown operation: " + operation);
      if (System.currentTimeMillis() - startTime > limit) {
         // could not find a suitable operation in the time limit; there may be none available
         throw new TestException("Could not find an operation in " + limit + " millis; disallowLocalEntryOps is " + true + "; check that the operations list has allowable choices");
      }
   } while (isReplicated && ((op == LOCAL_INVALIDATE_OPERATION) || (op == LOCAL_DESTROY_OPERATION)));
   return op;
}

/** Get an existing key. Randomly choose whether to get an older key
 *  by returning the first iterated key, or by choosing a  most recently
 *  used key.
 */
protected Object getOldOrRecentKey() {
   final int RANGE = 50;
   Object key = null;
   if (TestConfig.tab().getRandGen().nextBoolean()) {
      Log.getLogWriter().info("Getting key by iteration...");
      // get a key by iteration; probably and old one
      Set aSet = aRegion.keys();
      if (aSet.size() == 0) {
         Log.getLogWriter().info("No keys in region");
         return null;
      }
      Iterator it = aSet.iterator();
      if (it.hasNext()) {
         key = it.next();
      } else { // has been destroyed cannot continue
         Log.getLogWriter().info("Unable to get key from region");
         return null; 
      }
   } else { // get a recent key
      Log.getLogWriter().info("Getting recent key...");
      key = getPreviousKey(RANGE);
      if (key == null) {
         Log.getLogWriter().info("Unable to get key from region");
         return null; 
      }
   }
   Log.getLogWriter().info("Returning key from getOldOrRecentKey " + key);
   return key;
}

protected Object getUpdateObject(String key) {
   boolean useComplexObject = ((Boolean)(DiskRegBB.getBB().getSharedMap().get("useComplexObject"))).booleanValue();
   if (useComplexObject) { 
      BaseValueHolder anObj = (BaseValueHolder)aRegion.get(key);
      BaseValueHolder newObj = (anObj == null) ?
                                  new ValueHolder(key, randomValues) :
                                  anObj.getAlternateValueHolder(randomValues);
      return newObj;
   } else {
      return getNewValue(key);
   }
}

/** Called to remove disk region files after a run. When running overnight
 *  the leftover disk region files can fill up the disk
 */
protected static void removeDiskRegionFiles() {
   FilenameFilter dirFilter = new FilenameFilter() {
      public boolean accept(File dir, String name) {
         File aFile = new File(name);
         return (aFile.isDirectory());
      }
   };
   FilenameFilter htreeFileFilter = new FilenameFilter() {
      public boolean accept(File dir, String name) {
         return (name.endsWith(".lg") || name.endsWith(".db"));
      }
   };

   String currDirStr = System.getProperty("user.dir");
   File currDir = new File(currDirStr);
   File[] dirList = currDir.listFiles(dirFilter);
   for (int i = 0; i < dirList.length; i++) { // iterate all subdirectories of the current dir
      File aDir = dirList[i];
      File[] htreeFileList = aDir.listFiles(htreeFileFilter);
      for (int j = 0; j < htreeFileList.length; j++) { 
          File htreeFile = htreeFileList[j];
          Log.getLogWriter().info(htreeFile.getAbsolutePath() + " is " + htreeFile.length() + " bytes");
          boolean result = htreeFile.delete();
          if (result) 
             Log.getLogWriter().info("Removed " + htreeFile.getAbsolutePath());
          else
             Log.getLogWriter().info("Was unable to remove " + htreeFile.getAbsolutePath());
      }
   }
}

// end task methods
//========================================================================
public static void HydraTask_endTask() {
   DiskRegTest.removeDiskRegionFiles(); 
   CacheHelper.createCache(ConfigPrms.getCacheConfig());
   NameBB.getBB().printSharedCounters();
   DiskRegBB.getBB().printSharedMap();
   DiskRegBB.getBB().printSharedCounters();
   checkForEventError();
}

/** If there is a backup disk region from this test run, use it to create a 
 *  region with persist backup so it can be recovered.
 */
protected static Region setupForRecovery(String regionName) {
   CacheUtil.createCache();   
   NameBB.getBB().printSharedCounters();
   DiskRegBB.getBB().printSharedMap();
   DiskRegBB.getBB().printSharedCounters();
   checkForEventError();

   // see if there is a persistent backup available from running this test
   String dirListKey = new String(RemoteTestModule.getMyClientName() + "_" + DiskRegBB.BACKUP_DIR_LIST);
   List diskDirList = (ArrayList)DiskRegBB.getBB().getSharedMap().get(dirListKey);
   Log.getLogWriter().fine("In HydraTask_recoveryEndTask: got BACKUP_DIR_LIST from BB " + diskDirList + " with key = " + dirListKey);
   if (diskDirList == null) { // there was no backup file for this test
      Log.getLogWriter().info("There is no backup directory for this VM to attach to");
      return null;
   } else {      
      // create a region with persist backup (for recovery) and any kind of overflow
      // to prevent the VM from filling up
      RegionDefinition regDef = RegionDefinition.createRegionDefinition();
      regDef.setPersistBackup(true);
      regDef.setDiskDirList(diskDirList);
      regDef.setNumDiskDirs(diskDirList.size());
      regDef.setEviction("overflowToDisk");
      regDef.setEvictionLimit(10);
      Region aRegion = regDef.createRootRegion(CacheUtil.getCache(), regionName, null, null, null);
      return aRegion;
   } 
}

public static void HydraTask_mirrorKeysEndTask() {
   Region aRegion = setupForRecovery(mirror.MirrorTest.REGION_NAME);
   if (aRegion == null) // no backup for this VM to attach to
      return;
   Validator val = new Validator();
      if (RemoteTestModule.getMyClientName().equals("negativeClient"))
         val.setContainsKeyCategory(Validator.NEGATIVE);
      else if (RemoteTestModule.getMyClientName().equals("positiveClient"))
         val.setContainsKeyCategory(Validator.POSITIVE);
      else
         val.setContainsKeyCategory(Validator.BOTH);
      val.setContainsValueForKeyCategory(Validator.NONE);
      val.setDoGetStrategy(Validator.IF_HAS_VALUE);
      val.setValueShouldBeNull(Validator.NEVER);
      val.setStableKeys(true);
      val.setWaitForContainsKey(false);
      val.setWaitForContainsValueForKey(false);
      val.setThrowOnFirstError(false);
      val.setBlackboard(CacheBB.getBB());
   Object[] objArr = val.validateRegionContents(aRegion);
   int numKeys = ((Integer)objArr[0]).intValue();
   int numValues = ((Integer)objArr[1]).intValue();
   Log.getLogWriter().info("Validated " + numKeys + " entries");
}

public boolean useCacheLoader() {
   if (DiskRegBB.fixUseCacheLoader())
      return DiskRegBB.useCacheLoader();
   else   
      return TestConfig.tab().booleanAt(DiskRegPrms.useCacheLoader);
}

/** Get a blackboard key to store a region definition. We want it to
 *  be unique by VM.
 */
/*public static String getRegDefKey() {
   String regDefKey = Thread.currentThread().getName();
   regDefKey = regDefKey.substring(0, regDefKey.lastIndexOf("_")); // strip off thread ID
   regDefKey = regDefKey + "_" + DiskRegBB.REGION_DEFINITION;
   return regDefKey;
}*/

}

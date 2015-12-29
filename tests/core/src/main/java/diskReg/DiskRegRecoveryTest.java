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

import hydra.*;
import hydra.blackboard.*;
import java.lang.reflect.*;
import objects.*;
import java.util.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;
import util.TestException;

public class DiskRegRecoveryTest extends DiskRegTest {

// used in calculating ranges for operation types
private static final int CREATE = 0;
private static final int DESTROY = 1;
private static final int INVALIDATE = 2;
private static final int UPDATE = 3;

// initialization methods
// ================================================================================
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      testInstance = new DiskRegRecoveryTest();
      testInstance.initialize();

      // display SharedMap in Blackboard
      DiskRegBB.getBB().printSharedMap();
   }
}

public synchronized static void HydraTask_initializeMirroredRegion() {
   if (testInstance == null) {
      testInstance = new DiskRegRecoveryTest();
      ((DiskRegRecoveryTest)testInstance).syncOnCacheReady();
      testInstance.initialize();
   }
}

/** initialize fields in this class and its superclass */
protected void initialize() {
  // In recovery tests we only have one VM per client type.
  // Furthermore, we have an END task VM that needs to have the same
  // disk dirs as one of the client VMs.  So, the "parent" directory
  // for GFX disk regions has to be based on client type.
  String cwd = System.getProperty("user.dir");
  String dir = cwd + java.io.File.separator +
    RemoteTestModule.getMyClientName() + "_diskDir";
  System.setProperty(util.RegionDefinition.GFX_DISK_DIR_PARENT_PROP, dir);

  super.initialize();
}

// task methods
//========================================================================

/** 
 *Hydra task to create a range of entries in the cache.
 */
public static void HydraTask_createEntries() {
   int range;
//   int low;
//   int high;

   Log.getLogWriter().info("In createEntries ...");
   int maxKeys = TestConfig.tab().intAt(DiskRecoveryPrms.maxKeys, 100);
   range = maxKeys / 4;
   
   // CREATE maxKey entries
   Log.getLogWriter().info("CREATING " + maxKeys + " entries");
   for (int i=0; i < maxKeys; i++) {
     ((DiskRegRecoveryTest)testInstance).createEntry(i);
   }
   Log.getLogWriter().info("CREATED " + maxKeys + " entries in the range of 0 to " + (maxKeys-1));
 
   /* Normalize this, so we only count the creates that won't be altered */
   DiskRegBB.getBB().getSharedCounters().add(DiskRegBB.completedOperations, range);

   // wait 30 seconds for distribution to complete
   MasterController.sleepForMs(30000);

   // Announce that the cache is ready (for tasks sync-ing on that signal
   ((DiskRegRecoveryTest)testInstance).setCacheReady();
}

/** 
 *Hydra task to create a range of entries in the cache.
 */
public static void HydraTask_doEntryOperations() {
   int range;
   int low;
   int high;

   Log.getLogWriter().info("In doEntryOperations ...");
   int maxKeys = TestConfig.tab().intAt(DiskRecoveryPrms.maxKeys, 100);
   range = maxKeys / 4;
   
   // DESTROY 

   low = range * DESTROY;            
   high = low + range;
   Log.getLogWriter().info("DESTROYING " + range + " entries\n"); 
   for (int i=low; i < high; i++) {
     ((DiskRegRecoveryTest)testInstance).destroyEntry(i);
     DiskRegBB.getBB().getSharedCounters().increment(DiskRegBB.completedOperations);
   }
   Log.getLogWriter().info("DESTROYED " + range + " entries in the range of " + low + " to " + (high-1));

   // INVALIDATE
   low =  INVALIDATE * range;  
   high = low + range;

   Log.getLogWriter().info("INVALIDATING " + range + " entries\n"); 
   for (int i=low; i < high; i++) {
     ((DiskRegRecoveryTest)testInstance).invalidateEntry(i);
     DiskRegBB.getBB().getSharedCounters().increment(DiskRegBB.completedOperations);
   }
   Log.getLogWriter().info("INVALIDATED " + range + " entries in the range of "+ low + " to " + (high-1));

   // UPDATE
   low = UPDATE * range;  
   high = low + range;

   Log.getLogWriter().info("UPDATING " + range + " entries\n"); 
   for (int i=low; i < high; i++) {
     ((DiskRegRecoveryTest)testInstance).updateEntry(i);
     DiskRegBB.getBB().getSharedCounters().increment(DiskRegBB.completedOperations);
   }
   Log.getLogWriter().info("UPDATED " + range + " entries in the range of "+ low + " to " + (high-1));

}

/** 
 *Hydra task to update a range of entries in the cache.
 */
public static void HydraTask_updateEntries() {
   int range;
   int low;
   int high;

   Log.getLogWriter().info("In updateEntries ...");
   int maxKeys = TestConfig.tab().intAt(DiskRecoveryPrms.maxKeys, 100);
   range = maxKeys / 4;
   
   // UPDATE
   low = UPDATE * range;  // 75-99
   high = low + range;

   Log.getLogWriter().info("UPDATING " + range + " entries\n"); 
   for (int i=low; i < high; i++) {
     ((DiskRegRecoveryTest)testInstance).updateEntry(i);
   }
}

/** 
 *Hydra task to update a range of entries in the cache.
 */
public static void HydraTask_readEntries() {
   int range;
   int low;
   int high;

   Log.getLogWriter().info("In readEntries ...");
   int maxKeys = TestConfig.tab().intAt(DiskRecoveryPrms.maxKeys, 100);
   range = maxKeys / 4;
   
   // read in the create range
   low = CREATE * range;  // 0-24
   high = low + range;

   Log.getLogWriter().info("READING " + range + " entries\n"); 
   for (int i=low; i < high; i++) {
     ((DiskRegRecoveryTest)testInstance).readEntry(i);
   }

   // read in the update 
   low = UPDATE * range;  // 75-99
   high = low + range;

   Log.getLogWriter().info("READING " + range + " entries\n"); 
   for (int i=low; i < high; i++) {
     ((DiskRegRecoveryTest)testInstance).readEntry(i);
   }
}


/** 
 *Hydra task to stop a VM
 */
public static void HydraTask_stopClientVm() throws ClientVmNotFoundException {
   Log.getLogWriter().info("In stopClientVm ...");

   long numOps = DiskRegBB.getBB().getSharedCounters().read(DiskRegBB.completedOperations);
   int maxKeys = TestConfig.tab().intAt( DiskRecoveryPrms.maxKeys, 100 );
   int minimumNumOpsBeforeStop = TestConfig.tab().intAt( DiskRecoveryPrms.minimumNumOpsBeforeStop, maxKeys / 2);
   int stopType = DiskRecoveryPrms.getStopType();
   Log.getLogWriter().info("stopClientVm: numOps = " + numOps + " with minimumNumOpsBeforeStop = " + minimumNumOpsBeforeStop);

   if (numOps > minimumNumOpsBeforeStop) {
     ClientVmMgr.stopAsync(
       "Completed more than " + minimumNumOpsBeforeStop + " ops",
       stopType,
       ClientVmMgr.NEVER       // never allow this VM to restart
     );
     return;
   }
   else {
     long timeToSleep = 250 * (minimumNumOpsBeforeStop - numOps);
     Log.getLogWriter().info("stopClientVm: have not met miniumumNumOps, napping for " + timeToSleep + " ms");
     MasterController.sleepForMs((int)timeToSleep);
   }
}

/** 
 *Hydra task to display DiskRegionStats in log
 */
public static void HydraTask_checkStats() {
   Log.getLogWriter().info("In checkStats() ...");
   ((DiskRegRecoveryTest)testInstance).checkStats();
   
   // display SharedMap in Blackboard
   DiskRegBB.getBB().printSharedMap();
}

/** 
 *Hydra task to validate cache after recovery
 */
public static void HydraTask_validateRecovery() {
//   Object val;

   Log.getLogWriter().info("In validateRecovery ...");

   ((DiskRegRecoveryTest)testInstance).checkStats();

   // validate correctness of each entry
   // create & updated must have correct value
   // destroyed should not containKey
   // invalidated should not containValueForKey
   long completedOperations = DiskRegBB.getBB().getSharedCounters().read(DiskRegBB.completedOperations);
   Log.getLogWriter().info("validateRecovery verifying " + completedOperations + " keys");
   for (int i=0; i < completedOperations; i++) {
     ((DiskRegRecoveryTest)testInstance).validateEntry(i);
   }

   // check that the basic region APIs are unaffected
   // Region.entries(), Region.keys(), Region.values()
   ((DiskRegRecoveryTest)testInstance).checkRegionAPIs();

   // cleanup diskRegion files (to save space)
   DiskRegTest.removeDiskRegionFiles();

   Log.getLogWriter().info("Exiting validateRecovery.");
}

private void createEntry(int i) {
   String key = (String)ObjectHelper.createName(i);
   Object val = null;

   String objectType = TestConfig.tab().stringAt(DiskRecoveryPrms.objectType, "objects.ArrayOfByte");

   Log.getLogWriter().info("creating entries of type " + objectType);
   val = ObjectHelper.createObject( objectType, i );

   if (TestConfig.tab().getRandGen().nextBoolean()) {
      Log.getLogWriter().info( "CREATING (putIfAbsent) key = " + key );
      aRegion.putIfAbsent(key, val);
   } else {
      Log.getLogWriter().info( "CREATING (put) key = " + key );
   aRegion.put(key, val);
   }
}

private void destroyEntry(int i) {
   String key = (String)ObjectHelper.createName(i);

   try {
      if (TestConfig.tab().getRandGen().nextBoolean()) {
        Log.getLogWriter().info( "REMOVING entry with key = " + key );
        aRegion.remove(key);
      } else {
   Log.getLogWriter().info( "DESTROYING entry with key = " + key );
     aRegion.destroy(key);
      }
   } catch( EntryNotFoundException e ) {
     throw new TestException( "Exception destroying entry " + i, e );
   }
}

private void invalidateEntry(int i) {
   String key = (String)ObjectHelper.createName(i);
   Log.getLogWriter().info( "INVALIDATING entry with key = " + key );
   try {
     aRegion.invalidate(key);
   } catch( EntryNotFoundException e ) {
     throw new TestException( "Exception invalidating entry " + i, e );
   }
}

private void updateEntry(int i) {
   String key = (String)ObjectHelper.createName(i);
   Object val = null;
   String objectType = TestConfig.tab().stringAt(DiskRecoveryPrms.objectType, "objects.ArrayOfByte");
   Log.getLogWriter().info("updating entries of type " + objectType);
   val = ObjectHelper.createObject( objectType, i );
   try {
      if (TestConfig.tab().getRandGen().nextBoolean()) {
        Log.getLogWriter().info( "REPLACING key = " + key);
        aRegion.replace(key, val);
      } else {
   Log.getLogWriter().info( "UPDATING key = " + key);
     aRegion.put(key, val);
      }
   } catch( CancelException e ) {
     // Don't fail because the cache has been closed.
     return;
   }
}

private void readEntry(int i) {
   String key = (String)ObjectHelper.createName(i);

   Log.getLogWriter().info( "READING key = " + key);
   Object val = aRegion.get(key);
}

private void checkStats() {
   Log.getLogWriter().info( "CHECKING stats ...");

   Log.getLogWriter().info("getNumOverflowOnDisk = " + DiskRegUtil.getNumOverflowOnDisk(aRegion));
   Log.getLogWriter().info("numWrites = " + DiskRegUtil.getNumEntriesWrittenToDisk(aRegion));
   Log.getLogWriter().info("numEntries in VM = " + DiskRegUtil.getNumEntriesInVM(aRegion));
}

private void checkRegionAPIs() {
   Log.getLogWriter().info( "CHECKING regionAPIs (entries, keys, values) ...");

   Set entries = aRegion.entries(false);
   Log.getLogWriter().info("Region API entries() returns " + entries.size() + " entries");

   Set keys = aRegion.keys();
   Log.getLogWriter().info("RegionAPI keys() returns " + keys.size() + " entries");

   Collection values = aRegion.values();
   Log.getLogWriter().info("RegionAPI values() returns " + values.size() + " entries");

}

private void validateEntry(int i) {
   Object val = null;

   Log.getLogWriter().info( "VALIDATING correctness of entry with key = " + i);

   String key = (String)ObjectHelper.createName(i);
   int range = getRange(i);
   switch(range) {
     case CREATE:
       // Use test hooks to check values in VM and on Disk
       val = DiskRegUtil.getValueOnDiskOrBuffer(aRegion, key);
       ObjectHelper.validate(i, val);

       // verify application interface work as expected
       val = aRegion.get(key);
       ObjectHelper.validate(i, val);
       break;

     case DESTROY:
       // verify application interface work as expected
       if (aRegion.containsKey(key)) {
         throw new HydraRuntimeException( "Exception in RecoveryValidation for entry " + i + " -- should be destroyed but key exists");
       }
       break;

     case INVALIDATE:
       // Use test hooks to check values in VM and on Disk
       val = DiskRegUtil.getValueOnDiskOrBuffer(aRegion, key);
       if (!LocalRegionHelper.isInvalidToken(val)) {
         throw new HydraRuntimeException( "Exception RecoveryValidation for entry " + i + " -- should be INVALID, but is class " + val.getClass().getName() );
       }

       // as of 6.5 the value obtained from getValueInVM will also be the invalid token
       // if a user did a get, they would see a null, but the test will not do a get here
       // because it could invoke a loader rather than return null; here we just check
       // for the invalid token
       val = DiskRegUtil.getValueInVM(aRegion, key);
       if (!LocalRegionHelper.isInvalidToken(val)) {
         throw new HydraRuntimeException( "Exception RecoveryValidation for entry " + i + " -- should be INVALID, but has value" + val + ", val is invalid token: " + LocalRegionHelper.isInvalidToken(val));
       }

       // verify application interface work as expected
       if (aRegion.containsValueForKey(key)) {
         throw new HydraRuntimeException( "Exception RecoveryValidation for entry " + i + " -- should be invalidated, but has a value");
       }
       break;

     case UPDATE:
       // this might be a vm that did disk recovery or a vm that did a gii from the other
       // vm who did disk recovery because it had the latest disk files
       val = aRegion.get(key);
       ObjectHelper.validate(i, val);

       break;

     default:
       throw new HydraRuntimeException( "Exception during RecoveryValidation -- key is out of range, entry = " + i);
   }
}

private static int getRange(int x) {
int maxKeys = TestConfig.tab().intAt(DiskRecoveryPrms.maxKeys);
int range = maxKeys / 4;
int operationRange;
   
   if (x < DESTROY * range) {
     operationRange = CREATE;
   }
   else if (x < INVALIDATE * range) {
     operationRange = DESTROY;
   }
   else if (x < UPDATE * range) {
     operationRange = INVALIDATE;
   }
   else {
     operationRange = UPDATE;
   }

   return(operationRange);
}

  /**
   *  Helper methods to synchronize clients across vms 
   *  Client who prepares the cache invokes setCacheReady(),
   *  clients synchronizing on this invoke syncOnCacheReady().
   */
  private void syncOnCacheReady() {

    // get the counter for this task
    SharedCounters counters = DiskRegBB.getBB().getSharedCounters();
    String fieldName = new String("cacheReady");
    Field field = null;
    try {
        field = DiskRegBB.class.getDeclaredField( fieldName );
    } catch( NoSuchFieldException e ) {
      throw new HydraRuntimeException( "Need to add field " + fieldName + " to DiskRegBB", e );
    }

    int counter = -1;
    try {
      counter = ( (Integer) field.get( null ) ).intValue();
    } catch( IllegalAccessException e ) {
      throw new HydraRuntimeException( "Unable to access field " + fieldName + " in DiskRegBB", e );
    }

    // poll until the cacheReady value is set to 1
    Log.getLogWriter().fine( "SYNC -- waiting for cacheReady" );
    long cacheReady = 0;
    while ( cacheReady != 1 ) {
      MasterController.sleepForMs( 200 );
      cacheReady = counters.read( counter );
    }

  }

  private void setCacheReady() {
    SharedCounters counters = DiskRegBB.getBB().getSharedCounters();
    String fieldName = new String("cacheReady");
    Field field = null;
    try {
      field = DiskRegBB.class.getDeclaredField( fieldName );
    } catch( NoSuchFieldException e ) {
      throw new HydraRuntimeException( "Need to add field " + fieldName + " to DiskRegBB", e );
    }

    int counter = -1;
    try {
      counter = ( (Integer) field.get( null ) ).intValue();
      } catch( IllegalAccessException e ) {
      throw new HydraRuntimeException( "Unable to access field " + fieldName + " in DiskRegBB", e );
    }

    counters.incrementAndRead( counter );

    Log.getLogWriter().fine( "SYNC ( " + fieldName + " ) counter set!");
  }

}

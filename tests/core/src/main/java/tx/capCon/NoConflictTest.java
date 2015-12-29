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
package tx.capCon; 

import util.*;
import hydra.*;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import java.util.*;
import tx.*;

/**
 * A class to test that capacity controllers do not cause tx conflicts.
 */
public abstract class NoConflictTest {

// single instance of this test class
static public NoConflictTest testInstance = null;

// final fields
protected static final String REGION_NAME = "capConRegion";

// instance fields
protected Region aRegion;

// abstract methods
protected abstract void verifyCapacity();
protected abstract void fill();

// ======================================================================== 
// initialization

/** Initialize by creating a root region.
 */
protected void initialize() {
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();
   Log.getLogWriter().info("Using RegionDefinition " + regDef + " to create region");
   aRegion = regDef.createRootRegion(CacheUtil.createCache(), REGION_NAME, 
                    new event.LogListener(), 
                    new TxLoader(), 
                    null); 
}

// ======================================================================== 
// hydra tasks

/** Hydra task to fill the region up to but not over its capacity.
 *  This is called as an init task.
 */
public static void HydraTask_fill() {
   testInstance.fill();
}

/** Hydra task to do random operations in a transaction */
public static void HydraTask_doOpsInTx() {
   testInstance.doOpsInTx();
}

/** Hydra task to add new entries to the region */
public static void HydraTask_addNewEntries() {
   testInstance.addNewEntries();
}

// ======================================================================== 
// methods that implement the test tasks

/** Do random operations in a transaction for minTaskGranularitySec seconds.
 *  Do numOps operations per transaction.
 */
protected void doOpsInTx() {
   Vector operations = TestConfig.tab().vecAt(TxPrms.operations);
   int numOps = TestConfig.tab().intAt(TxPrms.numOps);
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In txThread, doing random operations for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   do {
      TxHelper.begin();
      int numOpsCompleted = 0;
      while (numOpsCompleted != numOps) {
         try {
            TxUtil.txUtilInstance.doOperations(operations, 1); 
            numOpsCompleted++;
         } 
         catch (VirtualMachineError e) {
           SystemFailure.initiateFailure(e);
           throw e;
         }
         catch (Throwable e) {
            String errStr = e.toString();
            if (errStr.indexOf(EntryNotFoundException.class.getName()) >= 0) {
               // After doOperations selects a key to operation on, threads adding to the
               // region could evict it, then doOperations does its operation and gets
               // EntryNotFoundException
               Log.getLogWriter().info("Caught " + errStr + "; continuing test");
            } else {
               throw new TestException(TestHelper.getStackTrace(e));
            }
         }
      }
      TxHelper.commitExpectSuccess();
      verifyCapacity();
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In doOpsInTx, done running for " + timeToRunSec + " seconds");

}

/** Add new entries to the test's region using unique keys for every new entry.
 *  These operations occur outside a transaction.
 */
protected void addNewEntries() {
   long timeToRunSec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * TestHelper.SEC_MILLI_FACTOR;
   Log.getLogWriter().info("In addNewEntries, creating new entries for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   GsRandom rand = TestConfig.tab().getRandGen();
   do {
      if (rand.nextBoolean()) 
         TxUtil.txUtilInstance.createEntry(aRegion, false);
      else
         TxUtil.txUtilInstance.getEntryWithNewKey(aRegion);
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
   Log.getLogWriter().info("In addEntries, done running for " + timeToRunSec + " seconds");
}

}

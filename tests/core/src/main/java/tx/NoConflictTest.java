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
package tx; 

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

/**
 * A class to test no conflicts in transactions, achieved by each tx
 * using a unique set of keys.
 */
public class NoConflictTest {

// single instance of this test class
static public NoConflictTest testInstance = null;

static final String REGION_NAME = "NoConflictRegion";

// ======================================================================== 
// initialization

/** Hydra task to create a forest of region hierarchies. This task
 *  can be called from more than one thread in the same VM.
 */
public synchronized static void HydraTask_initializeConcTest() {
   if (testInstance == null) {
      testInstance = new NoConflictTest();
      testInstance.initialize();
      TxUtil.txUtilInstance = new TxUtilKeyRange();
   }
   testInstance.initEachThread();
}

/** Initialize method to be invoked once per VM */
protected void initialize() {
   Cache aCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
   CacheUtil.setCache(aCache);   // Required by TxUtil and splitBrain/distIntegrityFD.conf
   Region aRegion = RegionHelper.createRegion(REGION_NAME, ConfigPrms.getRegionConfig());
}

/** Initialize method to be invoked once per thread */
protected void initEachThread() {
   TxUtil.txUtilInstance.initialize();
   Region aRegion = CacheUtil.getCache().getRegion(REGION_NAME);
   int lowerKeyIndex = ((Integer)(((TxUtilKeyRange)(TxUtil.txUtilInstance)).lowerKeyRange.get())).intValue();
   int upperKeyIndex = ((Integer)(((TxUtilKeyRange)(TxUtil.txUtilInstance)).upperKeyRange.get())).intValue();
   for (int i = lowerKeyIndex; i <= upperKeyIndex; i++) {
      Object key = NameFactory.getObjectNameForCounter(i);
      NameBB.getBB().getSharedCounters().setIfLarger(NameBB.POSITIVE_NAME_COUNTER, i);
      BaseValueHolder value = (BaseValueHolder)(TxUtil.txUtilInstance.getNewValue(key));
      TxUtil.txUtilInstance.putEntry(aRegion, key, value, Operation.ENTRY_CREATE);
   }
}

// ======================================================================== 
// Hydra tasks

/** Hydra task for serial conflict test */
public static void HydraTask_concNoConflictTest() {
   testInstance.concNoConflictTest();
}

// ======================================================================== 
// methods that implement the test tasks

public void concNoConflictTest() {
   GsRandom rand = TestConfig.tab().getRandGen(); 
   int tasksInTxPercentage = TxPrms.getTasksInTxPercentage();
   if (rand.nextInt(1, 100) <= tasksInTxPercentage) { // run this task inside a transaction
      TxHelper.begin();
   } else { // run outside a transaction
      TxBB.inc(TxBB.NOT_IN_TRANS);
   }

   // do random operations
   TxUtil.doOperations();  // does numOps operations

   // if in a transaction; decide whether to commit or rollback
   if (TxHelper.exists()) {
      int commitPercentage = TxPrms.getCommitPercentage();
      if (rand.nextInt(1, 100) <= commitPercentage) { // do a commit
         TxHelper.commitExpectSuccess();
         TxBB.inc(TxBB.TX_SUCCESS);
      } else { // do a rollback
         TxHelper.rollback();
         TxBB.inc(TxBB.TX_ROLLBACK);
      }
   }
}

}

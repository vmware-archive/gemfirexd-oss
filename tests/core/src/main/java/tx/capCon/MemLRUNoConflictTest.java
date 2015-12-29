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
import capCon.*;
import tx.TxUtil;

/**
 * A class to test that capacity controllers do not cause conflicts.
 */
public class MemLRUNoConflictTest extends NoConflictTest {

// instance fields
protected MemLRUTest memLRUTestInstance;    

/** Hydra task to initialize a test instance */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      TxUtil.txUtilInstance = new TxUtil();
      testInstance = new MemLRUNoConflictTest();
      testInstance.initialize();
   }
}

/** Initialize this test instance */
protected void initialize() {
   memLRUTestInstance = (MemLRUTest)(CapConTest.testInstance);
   super.initialize();
   TxUtil.txUtilInstance = new TxUtil();
   TxUtil.txUtilInstance.initialize();
   Log.getLogWriter().info(this.toString());
   aRegion = CacheUtil.getRegion(REGION_NAME);
}

/** Verify the number of bytes used for this region. In this test keys are
 *  all strings of size KEY_LENGTH and values are all ByteArrays of size
 *  byteArraySize.
 */
protected void verifyCapacity() {
//   memLRUTestInstance.verifyMemCapacity(memLRUTestInstance.memLRUParams.getTotalAllowableBytes());
}

/** Fill the region to its LRU capacity, but do not go over */
protected void fill() {
//   MemLRUCapacityController capCon = (MemLRUCapacityController)(aRegion.getAttributes().getCapacityController());
//   int maxEntries = memLRUTestInstance.memLRUParams.getMaxEntries(capCon);
//   Log.getLogWriter().info("Filling with " + maxEntries + " entries...");
//   for (long i = 1; i <= maxEntries; i++) {
//      TxUtil.txUtilInstance.createEntry(aRegion);
//      verifyCapacity();
//   }
}

}

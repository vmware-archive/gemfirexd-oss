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
import tx.TxUtil;

/**
 * A class to test that capacity controllers do not cause conflicts.
 */
public class LRUNoConflictTest extends NoConflictTest {


// instance fields
protected int maximumEntries = -1;            // LRU capacity
protected int highestAllowableEntries = -1;   // allowance for LRU to go above maximumEntries


/** Hydra task to initialize a test instance */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      TxUtil.txUtilInstance = new TxUtil();
      testInstance = new LRUNoConflictTest();
      testInstance.initialize();
   }
}

/** Initialize this test instance */
protected void initialize() {
   super.initialize();
   maximumEntries = CacheUtil.getRegion(REGION_NAME).getAttributes().getEvictionAttributes().getMaximum();
   // LRU can deviate by +/- number of threads in this VM + 1 more for the ceiling 
   // because LRU adds before it evicts.
   String clientName = System.getProperty("clientName");
   ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
   int numThreads = cd.getVmThreads();
   highestAllowableEntries = maximumEntries + numThreads + 2; // a little extra room 
          // -1 because 1 thread is doing transactions that do not add entries to the region
   Log.getLogWriter().info("Setting limits: numThreads = " + numThreads + 
                           " maximumEntries = " + maximumEntries + 
                           ", highestAllowableEntries = " + highestAllowableEntries);
}

/** Verify that the current capacity of the region does not exceed its
 *  allowable upper limit
 */
protected void verifyCapacity() {
   int numKeys = aRegion.keys().size();
   Log.getLogWriter().info("In verifyNumKeys, numKeys is " + numKeys);
   if (numKeys > highestAllowableEntries)
      throw new TestException("Expected num keys to be <= " +
         highestAllowableEntries + ", but num keys for " + aRegion.getFullPath() + " is " + numKeys + ", " + this.toString());
}

/** Fill the region to its LRU capacity, but do not go over */
protected void fill() {
   Log.getLogWriter().info("Filling with " + maximumEntries + " entries...");
   for (int i = 1; i <= maximumEntries; i++) {
      TxUtil.txUtilInstance.createEntry(aRegion, false);
      verifyCapacity();
   }
}

}

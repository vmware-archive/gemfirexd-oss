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
import java.util.*;
import com.gemstone.gemfire.internal.cache.*;

/**
 * A class to test conflicts in PR Transactions.
 */
public class PRConflictTest extends ConflictTest {

// ======================================================================== 
// hydra tasks

/** Hydra task to create a forest of region hierarchies. This task
 *  can be called from more than one thread in the same VM.
 */
public synchronized static void HydraTask_initializeConcTest() {
   if (testInstance == null) {
      testInstance = new PRConflictTest();
      testInstance.initialize();
   }
}

/** Hydra task to create a forest of region hierarchies. This task
 *  can be called from more than one thread in the same VM.
 */
public synchronized static void HydraTask_initializeSerialTest() {
   if (testInstance == null) {
      testInstance = new PRConflictTest();
      testInstance.testConfiguration = UNDEFINED;
      int numVMs = TestHelper.getNumVMs();
      if (numVMs == 1) { 
         // test has 1 VM and multi-threads (at least 2, test cannot work with 1 VM and 1 thread)
         if (TestHelper.getNumThreads() == 1)
            throw new TestException("Test cannot run with 1 VM and 1 thread");
         testInstance.testConfiguration = MULTI_THREADS_PER_ONE_VM;
      } else { // test has > 1 VM
         int numThreads = TestHelper.getNumThreads();
         if (TestHelper.getNumThreads() != numVMs) 
            throw new TestException("Test cannot run with multi-VMs (" + numVMs + 
                                    ") having > 1 threads (" + numThreads + ")");
         testInstance.testConfiguration = ONE_THREAD_PER_MULTI_VMS;
      }
      testInstance.initialize();
   }
}

/** Return a decision on whether or not this configuration supports the 2nd
 *  (conflicting thread) running in a tx.  
 *  
 *  With PartitionedRegions, we cannot target a thread in a remote VM, 
 *  so there is no way to create a conflict if both threads are transactional (because
 *  their keySets won't intersect). 
 */
 protected boolean executeInTx() {
   boolean useTx = false;
   if (testInstance.testConfiguration == MULTI_THREADS_PER_ONE_VM && TestConfig.tab().getRandGen().nextBoolean()) {
     useTx = true;
   }
   return useTx;
}

}

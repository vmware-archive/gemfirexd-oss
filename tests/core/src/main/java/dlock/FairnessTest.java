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
package dlock; 

import hydra.*;
import hydra.blackboard.*;
import util.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.distributed.*;
import java.util.*;

public class FairnessTest {

protected static final String ObjectToLock = "objectToLock";

/** Hydra task to test distributed lock fairness */
public static void HydraTask_fairnessTest() {
   FairnessTest testInstance = new FairnessTest();
   testInstance.doFairnessTest();
}

/** Hydra task to verify distributed lock fairness */
public static void HydraTask_verify() {
   FairnessTest testInstance = new FairnessTest();
   testInstance.verify();
}

/** Fairness test. Wait for all thread to be ready to lock, then
 *  loop while continually locking and unlocking. All threads try 
 *  to lock the same String. When finished, each thread should have
 *  obtained the lock roughly an equal number of times.
 */
private void doFairnessTest() {
   // initialize
   DistributedLockService dls = DLockUtil.getLockService();
   
   // exclude the lock grantor because he is optimized to not be fair...
   makeSureDLSIsReady(dls); // and lock grantor has been picked
   if (dls.isLockGrantor()) return;
   
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   
   // exclude the lock grantor threads from the test results...
   int totalVMs = TestHelper.getNumVMs();
   int totalThreads = TestHelper.getNumThreads();
   int threadsPerVM = totalThreads / totalVMs; 
   int numThreads = totalThreads - threadsPerVM;

   // wait for all threads to be ready to get the lock
   Blackboard bb = DLockBlackboard.getInstance();
   SharedCounters sc = bb.getSharedCounters();
   long counterValue = sc.incrementAndRead(DLockBlackboard.ReadyToLock);
   if (counterValue > numThreads)
      throw new TestException("This task might have been invoked more than once; " +
                              "it is intented to run once per thread");
   TestHelper.waitForCounter(bb, "DLockBlackboard.ReadyToLock", DLockBlackboard.ReadyToLock,
              numThreads, true, 300000);

   // loop to lock/unlock
   long startLoopTime = System.currentTimeMillis();
   int lockCount = 0;
   do {
// lynn - remove timing and logging after 31279 is fixed
//      long startTime = System.currentTimeMillis();
//Log.getLogWriter().info("About to lock");
      dls.lock(ObjectToLock, -1, -1);
//Log.getLogWriter().info("Got lock");
//      long grantTime = System.currentTimeMillis();
      dls.unlock(ObjectToLock);
//Log.getLogWriter().info("Unlocked");
//      long endTime = System.currentTimeMillis();
      lockCount++;
//      Log.getLogWriter().info("Locking took " + (grantTime - startTime) + " ms, unlocking took " + 
//          (endTime - grantTime) + " ms");
   } while (System.currentTimeMillis() - startLoopTime < minTaskGranularityMS);

   Log.getLogWriter().info("Done with task, obtained lock " + lockCount + " times");
   DLockBlackboard.getInstance().getSharedMap().put(Thread.currentThread().getName(), new Integer(lockCount));
}

/** Sleep until DLS is initialized which means a lock grantor is known */
private void makeSureDLSIsReady(DistributedLockService dls) {
  com.gemstone.gemfire.distributed.internal.locks.DLockService dlock = 
      (com.gemstone.gemfire.distributed.internal.locks.DLockService) dls;
  // force dlock to pick a lock grantor now...
  dlock.getLockGrantorId();
}

/** Check how many times each thread obtained the lock, checking for fairness. */
protected void verify() {
   // print out the results and total the number of locks obtained in the whole test
   Map aMap = DLockBlackboard.getInstance().getSharedMap().getMap();
   int totalLocks = 0;
   Iterator it = (new TreeMap(aMap)).keySet().iterator();
   StringBuffer aStr = new StringBuffer();
   while (it.hasNext()) {
      String threadId = (String)(it.next());
      int numLocks = ((Integer)(aMap.get(threadId))).intValue();
      aStr.append(threadId  + " obtained " + numLocks + " locks \n");
      totalLocks += numLocks;
   }
   Log.getLogWriter().info(aStr.toString());

   // check for fairness
   aStr = new StringBuffer();

   // exclude the lock grantor threads from the test results...
   int totalVMs = TestHelper.getNumVMs();
   int totalThreads = TestHelper.getNumThreads();
   int threadsPerVM = totalThreads / totalVMs; 
   int numThreads = totalThreads - threadsPerVM;

   it = aMap.keySet().iterator();
   int expectedLocks = totalLocks / numThreads;

   double devMultiplier = tab().doubleAt(DLockPrms.fairnessDeviationMultiplier);
   if (devMultiplier == 0.0) {
     devMultiplier = 0.20;
   }
   
   int deviation = (int)(expectedLocks * devMultiplier);
   int lowThreshold = expectedLocks - deviation;
   int highThreshold = expectedLocks + deviation;
   
   Log.getLogWriter().info("Checking for fairness, numThreads is " + numThreads + 
       ", totalLocks is " + totalLocks + 
       ", expectedLocks is " + expectedLocks + 
       ", deviation multiplier is " + devMultiplier + 
       ", deviation is " + deviation + 
       ", lowThreshold is " + lowThreshold + 
       ", highThreshold is " + highThreshold);
   while (it.hasNext()) {
      String threadId = (String)(it.next());
      int numLocks = ((Integer)(aMap.get(threadId))).intValue();
      if ((numLocks < lowThreshold) || (numLocks > highThreshold))
         aStr.append("Expected thread " + threadId + " to have obtained between " + lowThreshold + 
              " and " + highThreshold + " locks, but it obtained " + numLocks + "\n");
   }
   if (aStr.length() > 0)
      throw new TestException(aStr.toString());
}

  protected static ConfigHashtable tab() {
    return TestConfig.tab();
  }
  protected static LogWriter log() {
    return Log.getLogWriter();
  }
}


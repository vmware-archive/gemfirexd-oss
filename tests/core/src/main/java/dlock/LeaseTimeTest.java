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
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import java.util.*;

public class LeaseTimeTest {

protected static final String ObjectToLock = "objectToLock";

// indexes for results array
protected static final int lockOrderIndex = 0;
protected static final int startTimeIndex = 1;
protected static final int grantTimeIndex = 2;
protected static final int releaseTimeIndex = 3;
protected static final int leaseTimeIndex = 4;
protected static final int resultsArrSize = 5;

/** Hydra task to record lease times */
public static void HydraTask_leaseTimeTest() {
   LeaseTimeTest testInstance = new LeaseTimeTest();
   testInstance.doLeaseTimeTest();
}

/** Hydra task to verify lease times */
public static void HydraTask_validateResults() {
   LeaseTimeTest testInstance = new LeaseTimeTest();
   testInstance.checkResultsInBlackboard();
}

/** Test lease time. Wait for all thread to be ready to lock, then
 *  all threads try to lock the same String using a random lease time.
 *  Each thread should get the lock one at a time, then automatically
 *  release the lock when the lease time expires, causing some other
 *  thread to get the lock. This method waits at the end of the task for
 *  all threads to finish.
 */
private void doLeaseTimeTest() {
   // initialize
   Blackboard bb = DLockBlackboard.getInstance();
   bb.getSharedMap().put(Thread.currentThread().getName(), "empty");
   hydra.blackboard.SharedCounters sc = bb.getSharedCounters();
   int numThreads = TestHelper.getNumThreads();
   int leaseTime = TestConfig.tab().intAt(DLockPrms.leaseTime);
   Log.getLogWriter().info("numThreads is " + numThreads + "; lease time is " + leaseTime + " millis");
   DistributedLockService dls = DLockUtil.getLockService();
   DM dm = ((DLockService)dls).getDistributionManager();
   int waitLimit = 360000;

   // wait for all threads to be ready to get the lock
   sc.increment(DLockBlackboard.ReadyToLock);
   TestHelper.waitForCounter(bb, "DLockBlackboard.ReadyToLock", DLockBlackboard.ReadyToLock,
              numThreads, true, waitLimit, 0);
   // now sleep to allow everybody to be done checking bb readyToLock; hopefully, everbody
   // will start getting locks at roughly the same time
   Log.getLogWriter().info("Sleeping for 15 seconds");
   MasterController.sleepForMs(15000);
   Log.getLogWriter().info("Done Sleeping for 15 seconds");

   // get the lock
   long startTime = dm.cacheTimeMillis();
   boolean result = dls.lock(ObjectToLock, -1, leaseTime);
   long grantTime = dm.cacheTimeMillis();
   long lockOrder = sc.incrementAndRead(DLockBlackboard.LockOrder);
   Log.getLogWriter().info("Got the lock, result is " + result + ", lockOrder is " + lockOrder);

   // wait for lock to automatically release because of the lease time
   while (dls.isHeldByCurrentThread(ObjectToLock)) {
      try {
         Thread.sleep(1);
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
   long releaseTime = dm.cacheTimeMillis();

   // now somebody else can get the lock; we don't have it anymore; log results
   long waitTimeForLock = grantTime - startTime;
   long actualLeaseTime = releaseTime - grantTime;
   long diff = Math.abs(actualLeaseTime - leaseTime);
   Log.getLogWriter().info("Waited " + waitTimeForLock + " millis for lock; set lease time " +
       leaseTime + " millis; actual lease time " + actualLeaseTime + 
       " millis (difference is " + diff + "); order of getting lock was " + 
       lockOrder + " out of " + numThreads);
   if (!result)
      throw new TestException("Result of getting lock is " + result);

   // save the results in the blackboard
   Object[] resultsArr = new Object[resultsArrSize];
   resultsArr[lockOrderIndex] = new Long(lockOrder);
   resultsArr[startTimeIndex] = new Long(startTime);
   resultsArr[grantTimeIndex] = new Long(grantTime);
   resultsArr[releaseTimeIndex] = new Long(releaseTime);
   resultsArr[leaseTimeIndex] = new Long(leaseTime);
   bb.getSharedMap().put(Thread.currentThread().getName(), resultsArr);
}
   
/** Check the wait times for each lock that was granted. Lock times and
 *  ordering of locks granted were put in the blackboard during the test.
 *  Look at the times now to make sure that each lock granted did not wait
 *  too long past the previous lock's lease expiration time.
 */
protected void checkResultsInBlackboard() {
   // Construct parallel arrays: The arrays are in the order that the locks were granted.
   //    1) the thread id (String),
   //    2) start time; System.currentTimeMillis() that the thread requested the lock.
   //    3) grant time; System.currentTimeMillis() that the thread got the lock.
   //    4) release time; System.currentTimeMillis() that the thread's lease expired.
   //    5) lease time; The lease time used to obtain the lock.
   //    6) wait time; The time the thread waited for the lock, in ms.
   //    7) actual lease time; The time the thread held the lock, in ms.
   //    8) lease time diff; The difference between the lease time and the actual lease time.
   //    9) wait time while lock was available; The time it took to get the lock after 
   //       the the lock became available.
   Map aMap = DLockBlackboard.getInstance().getSharedMap().getMap();
   int numLocks = aMap.size();
   String[] threadIDArr = new String[numLocks];
   long[] startTimeArr = new long[numLocks];
   long[] grantTimeArr = new long[numLocks];
   long[] releaseTimeArr = new long[numLocks];
   long[] leaseTimeArr = new long[numLocks];
   long[] waitTimeArr = new long[numLocks];
   long[] actualLeaseTimeArr = new long[numLocks];
   long[] leaseTimeDiffArr = new long[numLocks];
   long[] waitTimeAfterAvailArr = new long[numLocks];
   Iterator it = aMap.keySet().iterator();
   long minStartTime = Long.MAX_VALUE;
   long maxStartTime = Long.MIN_VALUE;
   while (it.hasNext()) {
      String key = (String)it.next();
      Object[] anArr = (Object[])(aMap.get(key));
      int  lockOrder   = (int)(((Long)(anArr[lockOrderIndex])).longValue());
      long startTime   =       ((Long)(anArr[startTimeIndex])).longValue();
      long grantTime   =       ((Long)(anArr[grantTimeIndex])).longValue();
      long releaseTime =       ((Long)(anArr[releaseTimeIndex])).longValue();
      long leaseTime   =       ((Long)(anArr[leaseTimeIndex])).longValue();
      int lockOrderIndex = lockOrder - 1;
      threadIDArr[lockOrderIndex] = key;
      startTimeArr[lockOrderIndex] = startTime;
      grantTimeArr[lockOrderIndex] = grantTime;
      releaseTimeArr[lockOrderIndex] = releaseTime;
      leaseTimeArr[lockOrderIndex] = leaseTime;
      waitTimeArr[lockOrderIndex] = grantTime - startTime;
      actualLeaseTimeArr[lockOrderIndex] = releaseTime - grantTime;
      leaseTimeDiffArr[lockOrderIndex] = Math.abs(actualLeaseTimeArr[lockOrderIndex] - leaseTime);
      minStartTime = Math.min(minStartTime, startTime);
      maxStartTime = Math.max(maxStartTime, startTime);
   }

   // print out the parallel arrays
   StringBuffer aStr = new StringBuffer();
   for (int i = 0; i < threadIDArr.length; i++) {
      if (i == 0) { // processing first lock granted
         waitTimeAfterAvailArr[i] = grantTimeArr[i] - startTimeArr[i];
      } else { // not the first lock granted
         waitTimeAfterAvailArr[i] = grantTimeArr[i] - releaseTimeArr[i-1];
      }
      aStr.append("Lock " + (i+1) + " granted to " + threadIDArr[i] + "\n" + 
           "   lease time: " + leaseTimeArr[i] + " ms\n" +
           "   start time: " + startTimeArr[i] + "\n" +
           "   grant time: " + grantTimeArr[i] + "\n" +
           "   release time: " + releaseTimeArr[i] + "\n" +
           "   lock wait time: " + waitTimeArr[i] + "\n" +
           "   actual lease time: " + actualLeaseTimeArr[i] + "\n" +
           "   difference between lease time and actual: " + leaseTimeDiffArr[i] + " ms\n" +
           "   time it took to get the lock after the lock became available: " + waitTimeAfterAvailArr[i] + " ms\n");
   }
   aStr.append("Min start time: " + minStartTime + "\n");
   aStr.append("Max start time: " + maxStartTime + "\n");
   long diff = maxStartTime - minStartTime;
   aStr.append("Difference between min and max start time: " + diff + "ms\n");
   Log.getLogWriter().info(aStr.toString());

   // check the times
   aStr = new StringBuffer();
   long upperThreshold = 1000; // millis
   long lowerThreshold = 500; // millis
   for (int i = 0; i < threadIDArr.length; i++) {

      // check the lease time, the time the lock was held by each thread
      if (actualLeaseTimeArr[i] < (leaseTimeArr[i] - lowerThreshold)) { // lease expired too soon
         aStr.append("Bug 33006 detected; For lock acquisition " + (i+1) + ", thread " + threadIDArr[i] +
                     ", lease was not held long enough, actual lease time was " + actualLeaseTimeArr[i] + 
                     ", but lease time is " + leaseTimeArr[i] + "\n");
      } else if (leaseTimeDiffArr[i] > upperThreshold) { // lease expired too late
         aStr.append("For lock acquisition " + (i+1) + ", thread " + threadIDArr[i] + 
                     ", actual lease time was " + leaseTimeArr[i] + " ms, but lock was held for " + 
                     actualLeaseTimeArr[i] + " ms, difference in time is " +
                     leaseTimeDiffArr[i] + "\n");
      }

      // check how long it took for the first lock to be granted
      if (i == 0) { // this is the first lock granted
         if (waitTimeAfterAvailArr[i] > upperThreshold) {
            aStr.append("For lock acquisition " + (i+1) + ", wait time in thread " + 
                threadIDArr[i] + " took " + waitTimeAfterAvailArr[i] + " millis (too long!)\n");
         }
      }
      else { // this is not the first lock granted (nor is it the 2nd or 3rd lock)

         // check how long it took to get the lock after it became available
         if (waitTimeAfterAvailArr[i] > upperThreshold) {
            aStr.append("For lock acquistion " + (i+1) + ", previous thread " + threadIDArr[i-1] + 
                " released its lock at " + releaseTimeArr[i-1] + ", thread " + threadIDArr[i] + 
                " obtained the lock at " + grantTimeArr[i] + ", difference in time is " + 
                waitTimeAfterAvailArr[i] + " ms (too long!)\n");
         }
      }
   }
   if (aStr.length() > 0)
      throw new TestException(aStr.toString());
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    }
    catch (InterruptedException e) {
      throw new TestException("Test was interrupted", e);
    }
  }

}


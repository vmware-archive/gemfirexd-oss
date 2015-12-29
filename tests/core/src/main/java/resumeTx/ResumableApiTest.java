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
package resumeTx;

import hydra.CacheHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;
import hydra.blackboard.SharedMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import util.NameFactory;
import util.TestException;
import util.TestHelper;
import util.TxHelper;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionId;

public class ResumableApiTest {

  public static ResumableApiTest testInstance = null;

  // instance fields to hold information about this test run
  private CacheTransactionManager ctm;
  private SharedCounters sc;

  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new ResumableApiTest();
      testInstance.createRegions();
      testInstance.initializeInstance();
    }
  }

  /** Initialize this test instance
   * 
   */
  private void initializeInstance() {
    ctm = CacheHelper.getCache().getCacheTransactionManager();
    sc = ResumeTxBB.getBB().getSharedCounters();
  }

  /** Create regions specified in RegionPrms.names
   * 
   */
  private void createRegions() {
    CacheHelper.createCache("cache1");
    Vector<String> aVec = TestConfig.tab().vecAt(RegionPrms.names);
    for (String regionConfigName: aVec) {
      Log.getLogWriter().info("Creating region " + regionConfigName);
      RegionHelper.createRegion(regionConfigName);
    }
  }

  /** Initialize the extra vm.
   */
  public static void HydraTask_initExtraVm() {
    // put transaction ids on the blackboard
    SharedMap sm = ResumeTxBB.getBB().getSharedMap();

    // Transaction that is suspended
    TxHelper.begin();
    sm.put("ExtraVmTx_SuspendedTx", testInstance.ctm.getTransactionId());
    TxHelper.suspend();

    // Transaction that has committed
    TxHelper.begin();
    sm.put("ExtraVmTx_CommittedTx", testInstance.ctm.getTransactionId());
    TxHelper.commit();

    // Transaction that has rolled back
    TxHelper.begin();
    sm.put("ExtraVmTx_RolledBackTx", testInstance.ctm.getTransactionId());
    TxHelper.rollback();

    // Transaction that has begun and remains open
    TxHelper.begin();
    sm.put("ExtraVmTx_BegunTx", testInstance.ctm.getTransactionId());

  }

  /** Test api calls with a txId that does not exist in this jvm.
   *  This tests the following api calls:
   *     tryResume(TransactionId, long, TimeUnit)
   *     tryResume(TransctionId)
   *     exists()
   *     exists(TransactionId)
   *     isSuspended(TransactionId)
   *     resume(TransactionId)
   */
  public static void HydraTask_txDoesNotExist() {
    // get transactionIds out of the blackboard map that are/were hosted in a vm other than this one
    // we expect all tryResume calls for these txIds to return false immediately
    Map aMap = ResumeTxBB.getBB().getSharedMap().getMap();
    for (Object key: aMap.keySet()) {
      if (key.toString().startsWith("ExtraVmTx_")) {
        TransactionId txId = (TransactionId) aMap.get(key);
        // TimeUnit.DAYS not available in java 5      testInstance.verifyImmediate(txId, 1, TimeUnit.DAYS, false);
        // TimeUnit.HOURS not available in java 5     testInstance.verifyImmediate(txId, 1, TimeUnit.HOURS, false);
        // TimeUnit.MINUTES not available in java 5   testInstance.verifyImmediate(txId, 1, TimeUnit.MINUTES, false);
        testInstance.verifyTryResumeImmediate(txId, 60, TimeUnit.SECONDS, false);
        testInstance.verifyTryResumeImmediate(txId, 60000, TimeUnit.MILLISECONDS, false);
        testInstance.verifyTryResumeImmediate(txId, 6000000, TimeUnit.MICROSECONDS, false);
        testInstance.verifyTryResumeImmediate(txId, 600000000, TimeUnit.NANOSECONDS, false);

        testInstance.verifyTryResume(txId, false);
        testInstance.verifyExists(false);
        testInstance.verifyExists(txId, false);
        testInstance.verifyIsSuspended(txId, false);
        testInstance.verifyResume(txId, false);
      }
    }
  }

  /** Test api calls on a TransactionId that is busy in this jvm.
   *  This tests the following api calls:
   *     tryResume(TransactionId, long, TimeUnit)
   *     tryResume(TransctionId)
   *     exists()
   *     exists(TransactionId)
   *     isSuspended(TransactionId)
   *     resume(TransactionId)
   */
  public static void HydraTask_txIsBusy() throws Throwable {
    TxHelper.begin();
    final TransactionId txId = testInstance.ctm.getTransactionId();

    // test api calls on the current thread
    testInstance.verifyTryResumeImmediate(txId, 10, TimeUnit.SECONDS, false);
    testInstance.verifyTryResumeImmediate(txId, 10000, TimeUnit.MILLISECONDS, false);
    testInstance.verifyTryResumeImmediate(txId, 10000000, TimeUnit.MICROSECONDS, false);
    testInstance.verifyTryResumeImmediate(txId, 1000000000, TimeUnit.NANOSECONDS, false);
    testInstance.verifyTryResume(txId, false);
    testInstance.verifyExists(true);
    testInstance.verifyExists(txId, true);
    testInstance.verifyIsSuspended(txId, false);
    try {
      Log.getLogWriter().info("Calling resume(" + txId + ")...");
      testInstance.ctm.resume(txId);
      Log.getLogWriter().info("resume(" + txId + ") succeeded");
      throw new TestException("Expected resume(" + txId + ") to throw " + IllegalStateException.class.getName() + ", but it succeeded");
    } catch (IllegalStateException e) {
      Log.getLogWriter().info("resume(" + txId + ") resulted in " + e);
      if (e.getMessage().indexOf("cannot resume another transaction") >= 0) {
        throw new TestException("Trying to resume " + txId + " which this thread already owns, but error message refers to 'another transaction': " + e);
      }
    }

    // test api calls in a different thread
    final List<Throwable> failureList = new ArrayList();
    Thread testThread = new Thread(new Runnable() {
      public void run() {
        try {
          testInstance.verifyTryResumeWaits(txId, 10, TimeUnit.SECONDS, false);
          testInstance.verifyTryResumeWaits(txId, 10000, TimeUnit.MILLISECONDS, false);
          testInstance.verifyTryResumeWaits(txId, 10000000, TimeUnit.MICROSECONDS, false);
          testInstance.verifyTryResumeWaits(txId, 1000000000, TimeUnit.NANOSECONDS, false);
          testInstance.verifyTryResume(txId, false);
          testInstance.verifyExists(false);
          testInstance.verifyExists(txId, true);
          testInstance.verifyIsSuspended(txId, false);
          testInstance.verifyResume(txId, false);
        }
        catch (Throwable e) {
          failureList.add(e);
        }
      }
    });
    testThread.start();
    try {
      testThread.join();
    } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    if (failureList.size() > 0) {
      StringBuffer aStr = new StringBuffer();
      for (Throwable t: failureList) {
        aStr.append(TestHelper.getStackTrace(t) + "\n");
      }
      throw new TestException(aStr.toString());
    }
  }

  /** Test api calls on a TransactionId that completes by committing.
   */
  public static void HydraTask_txCommits() {
    final int numExecutions = 10;
    for (int i = 1; i <= numExecutions; i++) {
      Log.getLogWriter().info("Execution number " + i);
      testInstance.txCommits();
    }
  }

  /** Test api calls on a TransactionId that completes by committing.
   *  This tests the following api calls:
   *     tryResume(TransactionId, long, TimeUnit)
   *     tryResume(TransctionId)
   *     exists()
   *     exists(TransactionId)
   *     isSuspended(TransactionId)
   *     resume(TransactionId)
   */
  private void txCommits() throws TestException {
    List<Map> resultList = endTryResumeEarly("commit");
    final int thresholdMs = 1000;
    StringBuffer errStr = new StringBuffer();
    TransactionId txId = (TransactionId)(resultList.get(0).get("txId")); // txId of the committed transaction, stored in each Map in resultList

    // validate the timing results
    for (Map aMap: resultList) { 
      long timeDiffMs = (Long)aMap.get("timeDiffMs");
      if (timeDiffMs > thresholdMs) {
        errStr.append(aMap.get("statusStr") + "\n");
      }
    }
    if (errStr.length() > 0) {
      throw new TestException("Bug 43802 detected; Expected tryResume to complete when the transaction completed: " + errStr.toString());
    }

    // validate the return value
    for (Map aMap: resultList) { 
      boolean result = (Boolean)aMap.get("result");
      if (result) {
        errStr.append(aMap.get("statusStr") + "\n");
      }
    }
    if (errStr.length() > 0) {
      throw new TestException("Expected tryResume to return false: " + errStr.toString());
    }

    // test other api calls on committed txId
    verifyTryResume(txId, false);
    verifyExists(false);
    verifyExists(txId, false);
    verifyIsSuspended(txId, false);
    verifyResume(txId, false);
  }

  /** Test api calls on a TransactionId that completes by rolling back.
   */
  public static void HydraTask_txRollsBack() {
    final int numExecutions = 10;
    for (int i = 1; i <= numExecutions; i++) {
      Log.getLogWriter().info("Execution number " + i);
      testInstance.txRollsBack();
    }
  }

  /** Test api calls on a TransactionId that completes by rolling back.
   *  This tests the following api calls:
   *     tryResume(TransactionId, long, TimeUnit)
   *     tryResume(TransctionId)
   *     exists()
   *     exists(TransactionId)
   *     isSuspended(TransactionId)
   *     resume(TransactionId)
   */
  private void txRollsBack() throws TestException {
    List<Map> resultList = endTryResumeEarly("rollback");
    final int thresholdMs = 1000;
    StringBuffer errStr = new StringBuffer();
    TransactionId txId = (TransactionId)(resultList.get(0).get("txId")); // txId of the rolledBack transaction, stored in each Map in resultList

    // validate the timing results
    for (Map aMap: resultList) { 
      long timeDiffMs = (Long)aMap.get("timeDiffMs");
      if (timeDiffMs > thresholdMs) {
        errStr.append(aMap.get("statusStr") + "\n");
      }
    }
    if (errStr.length() > 0) {
      throw new TestException("Bug 43802 detected; Expected tryResume to complete when the transaction completed: " + errStr.toString());
    }

    // validate the return value
    for (Map aMap: resultList) { 
      boolean result = (Boolean)aMap.get("result");
      if (result) {
        errStr.append(aMap.get("statusStr") + "\n");
      }
    }
    if (errStr.length() > 0) {
      throw new TestException("Expected tryResume to return false: " + errStr.toString());
    }

    // test other api calls on rolled back txId
    verifyTryResume(txId, false);
    verifyExists(false);
    verifyExists(txId, false);
    verifyIsSuspended(txId, false);
    verifyResume(txId, false);
  }

  /** Test api calls on a TransactionId that completes by suspending.
   */
  public static void HydraTask_txSuspends() {
    final int numExecutions = 10;
    for (int i = 1; i <= numExecutions; i++) {
      Log.getLogWriter().info("Execution number " + i);
      testInstance.txSuspends();
    }
  }

  /** Test api calls on a TransactionId that completes by suspending.
   *  This tests the following api calls:
   *     tryResume(TransactionId, long, TimeUnit)
   *     tryResume(TransctionId)
   *     exists()
   *     exists(TransactionId)
   *     isSuspended(TransactionId)
   *     resume(TransactionId)
   */
  private void txSuspends() throws TestException {
    List<Map> resultList = endTryResumeEarly("suspend");
    final int thresholdMs = 1000;

    // divide the results by threads that ended tryResume early and those that didn't
    List<Map> endedEarly = new ArrayList();
    List<Map> others = new ArrayList();
    for (Map aMap: resultList) {
      Long timeDiffMs = (Long)(aMap.get("timeDiffMs"));
      if (timeDiffMs <= thresholdMs) {
        endedEarly.add(aMap);
      } else {
        others.add(aMap);
      }
    }

    // validate the timing results
    if (endedEarly.size() == 0) {
      StringBuffer errStr = new StringBuffer();
      for (Map aMap: others) {
        errStr.append(aMap.get("statusStr") + "\n");
      }
      throw new TestException("No tryResume call ended its wait when suspend was called: \n" +
          errStr.toString());
    } else if (endedEarly.size() > 1) {
      StringBuffer errStr = new StringBuffer();
      for (Map aMap: endedEarly) {
        errStr.append(aMap.get("statusStr") + "\n");
      }
      throw new TestException("More than 1 tryResume call ended its wait early when suspend was called: \n" +
          errStr.toString());
    } else {
      Log.getLogWriter().info("Exactly 1 tryResume call ended its wait early as expected");
    }

    // validate the return results (at this point we know that only 1 thread ended early and it must return true
    // all others must return false
    for (Map aMap: endedEarly) {
      boolean result = (Boolean)aMap.get("result");
      if (!result) {
        throw new TestException(aMap.get("statusStr") + "; expected result to be true");
      }
    }
    for (Map aMap: others) {
      boolean result = (Boolean)aMap.get("result");
      if (result) {
        throw new TestException(aMap.get("statusStr") + "; expected result to be false");
      }
    }

    // test other api calls on a suspended txId
    TxHelper.begin();
    TransactionId suspendedTxId = ctm.getTransactionId();
    TxHelper.suspend();
    verifyTryResume(suspendedTxId, true);
    TxHelper.suspend(); // suspend again
    verifyExists(false);
    verifyExists(suspendedTxId, true);
    verifyIsSuspended(suspendedTxId, true);
    verifyResume(suspendedTxId, true);
    TxHelper.suspend(); // suspend again
    verifyTryResumeImmediate(suspendedTxId, 30, TimeUnit.SECONDS, true);
    TxHelper.rollback(); // end this tx
  }

  /** Begin a transaction, spawn a number of threads all trying to execute tryResume(TransactionId, long, TimeUnit)
   *  then end the transaction with either commit, rollback or suspend. Record the results of each tryResume
   *  thread and return it so the caller can validate appropriately.
   *  
   * @param howToEndTx How to end the transaction. Can be either "commit", "rollback" or "suspend".
   * @return List of Maps. Each Map is the result of one of the threads that executed tryResume. The keys
   *         of each Map are as follows:
   *         "result" (Boolean) The return value of tryResume
   *         "duration" (Long) The number of milliseconds it took tryResume to execute
   *         "timeAtCompletion" (Long) The value of System.currentTimeMillis() after the tryResume call completed
   *         "threadName" (String) The name of the spawned thread that executed the tryResume call.
   *         "timeAtTxCompletion" (Long) The value of System.currentTimeMillis() after the transaction ended.
   *         "timeDiffMs" (Long) The number of milliseconds between the time the transaction ended and the time
   *                             tryResume ended.
   *         "txId" (TransactionId) The TransactionId tryResume is waiting for.
   *         "statusStr" (String) A String describing the tryResume call, its time and result (a String
   *                              description of the Map values described here)
   *         
   */
  private List<Map> endTryResumeEarly(String howToEndTx) {
    final int numThreads = 10; // the number of threads to start that attempt tryResume(TransactionId, long, TimeUnit)

    // begin a transaction, do some puts
    TxHelper.begin();
    Region region1 = CacheHelper.getCache().getRegion("region1");
    Region region2 = CacheHelper.getCache().getRegion("region2");
    Object anObj = NameFactory.getNextPositiveObjectName();
    region1.put(anObj, anObj);
    Log.getLogWriter().info("Put key " + anObj + ", value " + anObj + " into " + region1.getFullPath());
    region2.put(anObj, anObj);
    Log.getLogWriter().info("Put key " + anObj + ", value " + anObj + " into " + region2.getFullPath());
    final TransactionId txId = ctm.getTransactionId();

    // start threads to resume with a 30 second wait
    final long waitTime = 30;
    final List<Throwable> failureList = new ArrayList();
    final List<Map> resultList = Collections.synchronizedList(new ArrayList());
    List<Thread> threadList = new ArrayList();
    // start the threads
    for (int i = 1; i <= numThreads; i++) {
      Thread testThread = new Thread(new Runnable() {
        public void run() {
          try {
            Object[] resultArr = tryResume(txId, waitTime, TimeUnit.SECONDS);
            Map aMap = new HashMap();
            aMap.put("result", resultArr[0]);
            aMap.put("duration", resultArr[1]);
            aMap.put("timeAtCompletion", resultArr[2]);
            aMap.put("threadName", Thread.currentThread().getName());
            aMap.put("txId", txId);
            resultList.add(aMap);
          }
          catch (Throwable e) {
            failureList.add(e);
          }
        }
      });
      testThread.setName("tryResumeThread_" + i);
      testThread.start();
      threadList.add(testThread);
    }

    // sleep for 15 seconds, then end and wait according to howToEndTx
    final int sleepTime = (int)(waitTime / 2);
    Log.getLogWriter().info("Sleeping for " + sleepTime + " seconds, then doing " + howToEndTx + "...");
    MasterController.sleepForMs(sleepTime * 1000);
    if (howToEndTx.equals("suspend")) {
      ctm.suspend();
    } else if (howToEndTx.equals("commit")) {
      ctm.commit();
    } else if (howToEndTx.equals("rollback")) {
      ctm.rollback();
    } else {
      throw new TestException("Unknown howToEndTx: " + howToEndTx);
    }
    long timeAtTxCompletion = System.currentTimeMillis();
    Log.getLogWriter().info(howToEndTx + " completed for " + txId);

    // wait for all threads to complete
    try {
      for (Thread aThread: threadList) {
        aThread.join();
      }
    } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    // look for any exceptions thrown by the thread
    if (failureList.size() > 0) {
      StringBuffer aStr = new StringBuffer();
      for (Throwable t: failureList) {
        aStr.append(TestHelper.getStackTrace(t) + "\n");
      }
      throw new TestException(aStr.toString());
    }

    // record the timings for each tryResume thread
    for (Map aMap: resultList) {
      boolean resultOfTryResume = (Boolean)aMap.get("result");
      long timeAtTryResumeCompletion = (Long)aMap.get("timeAtCompletion");
      String threadName = (String)aMap.get("threadName");
      long timeDiffMs = Math.abs(timeAtTxCompletion - timeAtTryResumeCompletion);
      aMap.put("timeDiffMs", timeDiffMs);
      aMap.put("timeAtTxCompletion", timeAtTxCompletion);
      String aStr = "For thread " + threadName + ", time of completion of " + howToEndTx + 
      " is " + timeAtTxCompletion + ", time of completion of tryResume(" + txId + ", " + waitTime + 
      ", SECONDS) is " + timeAtTryResumeCompletion + ", difference is " + timeDiffMs + "ms" +
      ", result is " + resultOfTryResume;
      aMap.put("statusStr", aStr);
      Log.getLogWriter().info(aStr);
    }
    return resultList;
  }

  /** Test to have many threads trying to resume a suspended thread. Eventually
   *  all threads should resume the transaction.
   * 
   */
  public static void HydraTask_concSuspend() {
    testInstance.concSuspend();
  }

  /** Start many threads which call tryResume. Each tryResume that returns should return true
   *  (it resumed) then it suspends and the next thread gets the transaction until all threads
   *  have been able to resume the transaction.
   */
  private void concSuspend() {
    final int numThreads = 1000;

    // begin a transaction, do some puts, suspend it
    TxHelper.begin();
    final TransactionId txId = testInstance.ctm.getTransactionId();
    Region region1 = CacheHelper.getCache().getRegion("region1");
    Region region2 = CacheHelper.getCache().getRegion("region2");
    Object anObj = NameFactory.getNextPositiveObjectName();
    region1.put(anObj, anObj);
    Log.getLogWriter().info("Put key " + anObj + ", value " + anObj + " into " + region1.getFullPath());
    region2.put(anObj, anObj);
    Log.getLogWriter().info("Put key " + anObj + ", value " + anObj + " into " + region2.getFullPath());

    // allow another thread to resume
    // important to suspend before starting the tryResume threads because then we have tryResumes
    // starting to wait while other threads are already resuming; this helps to show off bug 43802
    TxHelper.suspend();
    long timeAtFirstSuspendCompletion = System.currentTimeMillis();

    // start the tryResume threads
    final long waitTime = Integer.MAX_VALUE;
    final List<Throwable> failureList = new ArrayList();
    List<Thread> threadList = new ArrayList();
    final List<Map> resultList = new ArrayList();
    // start the threads
    for (int i = 1; i <= numThreads; i++) {
      Thread testThread = new Thread(new Runnable() {
        public void run() {
          try {
            Object[] resultArr = tryResume(txId, waitTime, TimeUnit.SECONDS);
            boolean result = (Boolean)(resultArr[0]);
            Map aMap = new HashMap();
            resultList.add(aMap);
            aMap.put("result", resultArr[0]);
            aMap.put("duration", resultArr[1]);
            aMap.put("timeAtCompletion", resultArr[2]);
            aMap.put("threadName", Thread.currentThread().getName());
            aMap.put("resumeOrder", resultArr[3]);
            TxHelper.suspend(); // allow another thread to resume
            aMap.put("timeAtSuspendCompletion", System.currentTimeMillis());
            if (!result) {
              throw new TestException(Thread.currentThread().getName() + " returned " + result + " from tryResume(" +
                  txId + ", " + waitTime + ", TimeUnit.SECONDS) but expected true");
            }
          }
          catch (Throwable e) {
            failureList.add(e);
          }
          Log.getLogWriter().info(Thread.currentThread().getName() + " is terminating");
        }
      });
      testThread.setName("tryResumeThread_" + i);
      testThread.start();
      threadList.add(testThread);
    }

    // wait for all threads to complete
    int numAliveThreads = logAliveThreads(threadList);
    while (numAliveThreads > 0) {
      MasterController.sleepForMs(10000);
      numAliveThreads = logAliveThreads(threadList);
      Log.getLogWriter().info(txId + " is suspended: " + ctm.isSuspended(txId));
    }

    // look for any exceptions thrown by the thread
    if (failureList.size() > 0) {
      StringBuffer aStr = new StringBuffer();
      for (Throwable t: failureList) {
        aStr.append(TestHelper.getStackTrace(t) + "\n");
      }
      throw new TestException(aStr.toString());
    }

    verifyTryResume(txId, true);
    TxHelper.commitExpectSuccess();

    verifyResumeTimes(timeAtFirstSuspendCompletion, resultList);
  }

  /** Verify the amount of time it takes for each tryResume to get the transaction
   *  after the previous thread suspends.
   *  
   * @param firstSuspendTime The value of System.currentTimeMillis() when the 
   *                         very first suspend completed.
   * @param tryResumeResultList A List of Maps containing the results of 
   *                            a tryResume call. See concSuspend() to see
   *                            the keys/values expected in the Map.
   */
  private void verifyResumeTimes(long firstSuspendTime, List<Map> tryResumeResultList) {
    // put the Map results in resumeOrder
    Vector<Map> resumeOrderList = new Vector();
    for (int i = 0; i < tryResumeResultList.size(); i++) {
      Map aMap = tryResumeResultList.get(i);
      int resumeOrder = ((Long)aMap.get("resumeOrder")).intValue();
      int index = resumeOrder-1;
      if (resumeOrder > resumeOrderList.size()) {
        resumeOrderList.setSize(resumeOrder);
      }
      resumeOrderList.set(index, aMap);
    }

    // check the timing of suspend/resume
    StringBuffer aStr = new StringBuffer();
    long currentSuspendCompletionTime = firstSuspendTime;
    final int thresholdMs = 1000;
    StringBuffer errStr = new StringBuffer();
    for (Map aMap: resumeOrderList) {
      long currentResumeTime = (Long)(aMap.get("timeAtCompletion"));
      long diff = Math.abs(currentSuspendCompletionTime - currentResumeTime);
      currentSuspendCompletionTime = (Long)(aMap.get("timeAtSuspendCompletion"));
      String logStr = aMap + "; took " + diff + " ms to resume\n";
      aStr.append(logStr);
      if (diff > thresholdMs) {
        errStr.append("tx took too long to resume after previous suspend: " + logStr + "\n");
      }
    }
    Log.getLogWriter().info(aStr.toString());
// @todo lynn threads start at random times so this check might not be valid
//    if (errStr.length() > 0) {
//      throw new TestException(errStr.toString());
//    }
  }

  /** Log the number of thread in threadList that are alive and return the number
   *  of threads that are alive.
   * @param threadList A list of Threads.
   * @return The number of threads in threadList that are alive (and running)
   */
  private int logAliveThreads(List<Thread> threadList) {
    int aliveThreadCount = 0;
    StringBuffer aStr = new StringBuffer();
    for (Thread aThread: threadList) {
      if (aThread.isAlive()) {
        aStr.append(aThread.getName() + " is alive\n");
        aliveThreadCount++;
      }
    }
    aStr.insert(0, aliveThreadCount + " of " + threadList.size() + " threads are still alive\n");
    Log.getLogWriter().info(aStr.toString());
    return aliveThreadCount;
  }

  /** Verify that tryResume(TransactionId, int, TimeUnit) returns immediately with the 
   *  given time value. Also verify that tryResume(TransactionId) returns with the
   *  given value. 
   * 
   * @param txId The transaction ID to attempt t resume. 
   * @param timeValue A tryResume argument.
   * @param unit A tryResume argument.
   *
   * @returns [0] (Boolean) The result of tryResume(TransactionId, long, TimeUnit)
   *          [1] (Long) The duration of the tryResume call in milliseconds.
   *          [2] (Long) The value of System.currentTimeMillis() when the tryResume call completed
   *          [3] (Long) The value of ResumeTxBB.resumeOrder counter after incrementing
   */
  private Object[] tryResume(TransactionId txId, long timeValue, TimeUnit unit) {
    Log.getLogWriter().info("Calling tryResume(" + txId + ", " + timeValue + ", " + unit + ")");
    long start = System.currentTimeMillis();
    boolean result = ctm.tryResume(txId, timeValue, unit);
    long end = System.currentTimeMillis();
    long resumeOrder = sc.incrementAndRead(ResumeTxBB.resumeOrder);
    long duration = end - start;
    Log.getLogWriter().info("tryResume(" + txId + ", " + timeValue + ", " + unit + ") returned in " + duration + "ms, result is " + result);
    return new Object[] {result, duration, end, resumeOrder};
  }

  /** Verify that tryResume(TransactionId, long, TimeUnit) returns immediately with the 
   *  given time value. Also verify that tryResume(TransactionId) returns with the
   *  given value. 
   * 
   * @param txId The transaction ID to attempt t resume. 
   * @param timeValue A tryResume argument.
   * @param unit A tryResume argument.
   * @param expectedResult The expected result from tryResume.
   */
  private void verifyTryResumeImmediate(TransactionId txId, long timeValue, TimeUnit unit, boolean expectedResult) {
    final int immediateLimitMs = 1000; // tryResume must execute in <= this many milliseconds to be considered an immediate return
    Object[] tmp = tryResume(txId, timeValue, unit);
    boolean result = (Boolean)tmp[0];
    long duration = (Long)tmp[1];
    if (duration > immediateLimitMs) {
      throw new TestException("Expected tryResume(" + txId + ", " + timeValue + ", " + unit + ") to return immediately, " +
          "but it took " + duration + "ms");
    }
    if (result != expectedResult) {
      throw new TestException("Expected tryResume(" + txId + ", " + timeValue + ", " + unit + ") to return " +
          expectedResult + ", but it returned " + result);
    }
  }

  /** Verify that tryResume(TransctionId, long, TimeUnit) waits the full time and returns the expected
   *  value. 
   * 
   * @param txId The transaction ID to attempt to resume. 
   * @param timeValue A tryResume argument.
   * @param unit A tryResume argument.
   * @param expectedResult The expected result from tryResume.
   */
  private void verifyTryResumeWaits(TransactionId txId, long timeValue, TimeUnit unit, boolean expectedResult) {
    Object[] tmp = tryResume(txId, timeValue, unit);
    boolean result = (Boolean)tmp[0];
    long duration = (Long)tmp[1];

    // verify the wait
    long expectedWaitMs = timeValue;
    if (unit == TimeUnit.SECONDS) {
      expectedWaitMs = timeValue * 1000;
    } else if (unit == TimeUnit.MICROSECONDS) {
      expectedWaitMs = timeValue / 1000;
    } else if (unit == TimeUnit.NANOSECONDS) {
      expectedWaitMs = timeValue / 1000000;
    } else if (unit == TimeUnit.MILLISECONDS){
      // already set
    } else {
      throw new TestException("Unknown TimeUnit " + unit);
    }
    if (duration < expectedWaitMs) {
      throw new TestException("Expected tryResume(" + txId + ", " + timeValue + ", " + unit + ") to wait for " +
          expectedWaitMs + "ms but it took " + duration + "ms");
    }
    if (result != expectedResult) {
      throw new TestException("Expected tryResume(" + txId + ", " + timeValue + ", " + unit + ") to return " +
          expectedResult + ", but it returned " + result);
    }
  }

  /** Verify that tryResume(TransactionId) returns the expected value.
   * 
   * @param txId The transaction ID to attempt to resume. 
   * @param expectedResult The expected result from tryResume.
   */
  private void verifyTryResume(TransactionId txId, boolean expectedResult) {
    boolean result = ctm.tryResume(txId);
    Log.getLogWriter().info("tryResume(" + txId + ") returned " + result);
    if (result != expectedResult) {
      throw new TestException("Expected tryResume(" + txId + ") to return " +
          expectedResult + ", but it returned " + result);
    }
  }

  /** Verify that exists() returnes the expected result
   *
   * @param expectedResult The expected return result from exists().
   */
  private void verifyExists(boolean expectedResult) {
    boolean result = ctm.exists();
    Log.getLogWriter().info("exists() returned " + result);
    if (result != expectedResult) {
      throw new TestException("Expected exists() to return " + expectedResult +
          ", but it returned " + result);
    }
  }

  /** Verify that exists(TransactionId) returns the expected result
   *
   * @param txId The argument to use for exists(TransactionId)
   * @param expectedResult The expected return result from exists(TransactionId).
   */
  private void verifyExists(TransactionId txId, boolean expectedResult) {
    boolean result = ctm.exists(txId);
    Log.getLogWriter().info("exists(" + txId + ") returned " + result);
    if (result != expectedResult) {
      throw new TestException("Expected exists(" + txId + ") to return " + expectedResult +
          ", but it returned " + result);
    }
  }

  /** Verify that isSuspended(TransactionId) returns the expected result
   *
   * @param txId The argument to use for isSuspended(TransactionId)
   * @param expectedResult The expected return result from isSuspended(TransactionId).
   */
  private void verifyIsSuspended(TransactionId txId, boolean expectedResult) {
    boolean result = ctm.isSuspended(txId);
    Log.getLogWriter().info("isSuspended(" + txId + ") returned " + result);
    if (result != expectedResult) {
      throw new TestException("Expected isSuspended(" + txId + ") to return " + expectedResult +
          ", but it returned " + result);
    }
  }

  /** Verify that resume(TransactionId) succeeds or throws an exception as designated by
   *  the given argument.
   *
   * @param txId The argument to use for resume(TransactionId)
   * @param expectSuccess If true, then resume(TransactionId) should succeed, if
   *                      false then it should throw an IllegalStateException.
   */
  private void verifyResume(TransactionId txId, boolean expectSuccess) {
    try {
      Log.getLogWriter().info("Calling resume(" + txId + ")...");
      ctm.resume(txId);
      Log.getLogWriter().info("resume(" + txId + ") succeeded");
      if (!expectSuccess) {
        throw new TestException("Expected resume(" + txId + ") to throw " + IllegalStateException.class.getName() + ", but it succeeded");
      }
    } catch (IllegalStateException e) {
      Log.getLogWriter().info("resume(" + txId + ") resulted in " + e);
      if (expectSuccess) {
        throw new TestException("resume(" + txId + ") resulted in " + e + " but expected it to succeed");
      }
    }
  }
}

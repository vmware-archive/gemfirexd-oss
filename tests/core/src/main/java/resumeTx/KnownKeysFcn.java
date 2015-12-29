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
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import util.BaseValueHolder;
import util.KeyIntervals;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.TxHelper;
import util.ValueHolder;

import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;

/** Function to execute on servers for known-keys style tests.
 * 
 * @author lynn
 *
 */
public class KnownKeysFcn implements Function {

  static final int MAX_TXNS = 10;      // the max number of transactions this jvm will suspend/resume
  static final int OPS_PER_FCN = 10;   // the number of ops to do in each function execution
  
  // parallel lists
  static List<TransactionId> txIdList = null;  // list of resumable transactions
  static List<Integer> suspendCount = null;    // number of times each transaction has been suspended
  
  // other fields used in the test
  static List keysToDoList = null;             // keys yet to have ops done to them
  static RandomValues rv = new RandomValues(); // for optional payload

  public boolean hasResult() {
    return true;
  }

  /** execute method looks in the argument for a command string. The function will
   *  execute according to the command string.
   */
  public void execute(FunctionContext context) {
    Log.getLogWriter().info("Executing KnownKeysFcn");
    List args = (List)(context.getArguments());
    Log.getLogWriter().info("In KnownKeysFcn with args: " + args);
    String clientOrigin = (String)(args.get(0));
    Log.getLogWriter().info("In KnownKeysFcn.execute, originated from " + clientOrigin);
    String command = (String)(args.get(1));
    Log.getLogWriter().info("Executing KnownKeysFcn, command is " + command);
    if (command.equals("initialize")) {
      initialize();
      context.getResultSender().lastResult("initialize completed");
    } else if (command.equals("ops")) {
      TransactionId txId = (TransactionId)args.get(2);
      Log.getLogWriter().info("TxId is " + txId);
      CacheTransactionManager ctm = CacheHelper.getCache().getCacheTransactionManager();
      if (!ctm.exists(txId)) {
        throw new TestException("onTransaction chose vmId " + RemoteTestModule.getMyVmid() + " to execute function for " + txId + 
            " but exists(" + txId + ") returned false");
      }
      boolean resumed = ctm.tryResume(txId, Integer.MAX_VALUE, TimeUnit.SECONDS);
      if (!resumed) {
        throw new TestException(txId + " did not resume");
      } else {
        Log.getLogWriter().info("Resumed " + txId);
        boolean opsCompleted = doOps(OPS_PER_FCN);
        TxHelper.suspend();
        int index = txIdList.indexOf(txId);
        synchronized (suspendCount) {
          suspendCount.set(index, ((suspendCount.get(index)).intValue())+1); 
        }
        HashMap resultMap = new HashMap();
        resultMap.put("suspendCount", suspendCount.get(index));
        resultMap.put("opsCompleted", opsCompleted);
        if (opsCompleted) {
          Object key = ResumableKnownKeysTest.txListKey + RemoteTestModule.getMyVmid();
          ResumeTxBB.getBB().getSharedMap().remove(key);
        }
        Log.getLogWriter().info("Returning " + resultMap + " from KnownKeysFcn with origin " + clientOrigin);
        context.getResultSender().lastResult(resultMap);
      }
    } else {
      throw new TestException("Unknown command: " + command);
    }
  }

  public String getId() {
    return "KnownKeysFcn";
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean isHA() {
    return false;
  }

  /** Initialize static fields for doing operations
   * 
   */
  protected static synchronized void initialize() {
    if (txIdList == null) {
      txIdList = Collections.synchronizedList(new ArrayList()); // list of resumable transactions
      keysToDoList = Collections.synchronizedList(new ArrayList()); // keys yet to have ops done to them
      suspendCount = Collections.synchronizedList(new ArrayList());

      // make sure the PRs are really colocated and have the same local data
      Set baseLocalKeys = null;
      String baseRegionName = null;
      Set localKeys = new HashSet();
      for (Region<?, ?> aRegion: CacheHelper.getCache().rootRegions()) {
        if (aRegion.getAttributes().getDataPolicy().withPartitioning()) { // is a PR
          Set primaryKeys = PartitionRegionHelper.getLocalPrimaryData(aRegion).keySet();
          if (baseLocalKeys == null) {
            baseLocalKeys = new HashSet();
            baseLocalKeys.addAll(primaryKeys);
            baseRegionName = aRegion.getFullPath();
          } else {
            if (!baseLocalKeys.equals(primaryKeys)) {
              Set missing = new HashSet(baseLocalKeys);
              missing.removeAll(primaryKeys);
              Set unexpected = new HashSet(primaryKeys);
              unexpected.removeAll(baseLocalKeys);
              throw new TestException("Local key set for " + aRegion.getFullPath() + " is not equal to key set of " +
                  baseRegionName + "; keys missing in " + aRegion.getFullPath() + ": " + missing + ", keys unexpected: " +
                  unexpected);
            }
          }
          localKeys.addAll(primaryKeys);
        }
      }
      
      // construct keysToDoList
      keysToDoList.addAll(localKeys);
      // now add new keys that should land in this data store when they are created
      int numKeyIntervalKeys = ResumableKnownKeysTest.testInstance.keyIntervals.getNumKeys();
      DistributedMember myDM = DistributedSystemHelper.getDistributedSystem().getDistributedMember();
      Region aRegion = CacheHelper.getCache().getRegion("region1"); // region1 is a PR
      for (int i = numKeyIntervalKeys+1; i <= numKeyIntervalKeys + ResumableKnownKeysTest.testInstance.numNewKeys; i++) {
        Object key = NameFactory.getObjectNameForCounter(i);
        DistributedMember primaryDM = PartitionRegionHelper.getPrimaryMemberForKey(aRegion, key);
        if (primaryDM != null && primaryDM.equals(myDM)) { // key will reside in this jvm when it is created
          keysToDoList.add(key);
        }
      }
      //Log.getLogWriter().info("keysToDoList is " + keysToDoList);

      // Create transactions
      CacheTransactionManager ctm = CacheHelper.getCache().getCacheTransactionManager();
      for (int i = 1; i < MAX_TXNS; i++) {
        TxHelper.begin();
        TransactionId txId = ctm.getTransactionId();
        txIdList.add(txId);
        TxHelper.suspend();
        suspendCount.add(new Integer(1));
      }
      
      // write the list of open transactions to the blackbaord
      Object key = ResumableKnownKeysTest.txListKey + RemoteTestModule.getMyVmid();
      try {
        ResumeTxBB.getBB().getSharedMap().put(key, RtxUtilVersionHelper
            .convertTransactionIDListToByteArrayList(txIdList));
      } catch (Exception e) {
        throw new TestException("Exception occurred while writing transaction Id list to Blackboard" + e);
      }
    }
  }

  /** Look for a transaction to resume
   * 
   * @return true if a transaction was resumed, false otherwise
   */
  protected boolean resumeATransaction() {
    Log.getLogWriter().info("Looking for a txn to resume...");
    CacheTransactionManager ctm = CacheHelper.getCache().getCacheTransactionManager();
    final int numFullAttempts = 10; // number of times to search txIdList for a suspended transaction
    for (int i = 1; i <= numFullAttempts; i++) {
      for (TransactionId txId: txIdList) {
        if (txId != null) {
          if (ctm.tryResume(txId)) {
            return true;
          }
        }
      }
    }
    return false;
  }
  
  /** Execute numOps ops on all regions (or fewer if there aren't enough ops remaining).
   * 
   * @param numOps The desired number of ops to execute. This might execute fewer ops
   *               if there aren't enough ops remaining.
   * @return true if there are ops remaining, false otherwise. 
   */
  protected boolean doOps(int numOps) {
    KeyIntervals ki = ResumableKnownKeysTest.testInstance.keyIntervals;
    Log.getLogWriter().info("Doing " + numOps + " ops");
    for (int i = 1; i <= numOps; i++) {
      // get a key if any are available
      Object key = null;
      synchronized (keysToDoList) {
        if (keysToDoList.size() > 0) {
          key = keysToDoList.remove(TestConfig.tab().getRandGen().nextInt(0, keysToDoList.size()-1));
        } else {
          Log.getLogWriter().info("No ops remaining");
          return true; // no ops left to do
        }
      }
      Log.getLogWriter().info("Obtained key " + key);
      
      int op = ki.opForKeyIndex(NameFactory.getCounterForName(key));
      switch (op) {
        case KeyIntervals.NONE: {
          // do nothing
          break;
        }
        case KeyIntervals.INVALIDATE: {
          for (Region aRegion: CacheHelper.getCache().rootRegions()) {
            Log.getLogWriter().info("Operation: invalidating " + key + " in region " + aRegion.getFullPath());
            aRegion.invalidate(key);
          }
          break;
        }
        case KeyIntervals.LOCAL_INVALIDATE: {
          for (Region aRegion: CacheHelper.getCache().rootRegions()) {
            Log.getLogWriter().info("Operation: locally invalidating " + key + " in region " + aRegion.getFullPath());
            aRegion.localInvalidate(key);
          }
          break;
        }
        case KeyIntervals.DESTROY: {
          for (Region aRegion: CacheHelper.getCache().rootRegions()) {
            Log.getLogWriter().info("Operation: destroying " + key + " in region " + aRegion.getFullPath());
            aRegion.destroy(key);
          }
          break;
        }
        case KeyIntervals.LOCAL_DESTROY: {
          for (Region aRegion: CacheHelper.getCache().rootRegions()) {
            Log.getLogWriter().info("Operation: locally destroying " + key + " in region " + aRegion.getFullPath());
            aRegion.localDestroy(key);
          }
          break;
        }
        case KeyIntervals.UPDATE_EXISTING_KEY: {
          BaseValueHolder newValue = new ValueHolder((String)key, rv);
          newValue.myValue = "updated_" + NameFactory.getCounterForName(key);
          for (Region aRegion: CacheHelper.getCache().rootRegions()) {
            Log.getLogWriter().info("Operation: updating " + key + " to value " + TestHelper.toString(newValue) + " in region " + aRegion.getFullPath());
            aRegion.put(key, newValue);
          }
          break;
        }
        case KeyIntervals.GET: {
          for (Region aRegion: CacheHelper.getCache().rootRegions()) {
            Log.getLogWriter().info("Operation: getting " + key + " in region " + aRegion.getFullPath());
            aRegion.get(key);
          }
          break;
        }
        case -1: { // this is a new key
          BaseValueHolder newValue = new ValueHolder((String)key, rv);
          for (Region aRegion: CacheHelper.getCache().rootRegions()) {
            Log.getLogWriter().info("Operation: creating " + key + " in region " + aRegion.getFullPath());
            aRegion.put(key, newValue);
          }
          break;
        }
        default: {
          throw new TestException("Unknown operation: " + op);
        }
      }
    }
    Log.getLogWriter().info("Done doing " + numOps + " ops");
    return false;
  }
  
  /** Write the List of TransactionIds for this jvm to the blackboard
   * 
   */
  public static void HydraTask_writeTxnsToBlackboard() throws Exception {
    Object key = ResumableKnownKeysTest.txListKey + RemoteTestModule.getMyVmid();
    ResumeTxBB
        .getBB()
        .getSharedMap()
        .put(
            key,
            RtxUtilVersionHelper
                .convertTransactionIDListToByteArrayList(txIdList));
  }
}

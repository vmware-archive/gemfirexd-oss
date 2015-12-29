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

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.*;

import hydra.*;
import hydra.blackboard.*;

import util.*;

/**
 * A test to validate what different threads/VMs see in terms of
 * transactional state (when a tx is in progress) as well as non-tx
 * operations.
 */

public class ViewTest {

//-----------------------------------------------------------------------
// Class variables
//-----------------------------------------------------------------------
static protected ViewTest viewTest;

//-----------------------------------------------------------------------
// Instance variables
//-----------------------------------------------------------------------
protected boolean isSerialExecution;
protected GsRandom rng;
protected boolean txInProgress = false;
protected Integer myVmPid;
protected boolean isScopeLocal = false;
protected boolean isBridgeClient = false;
protected boolean isProxy;
protected InterestPolicy interestPolicy;
protected boolean initializeTxInfo = true;
protected boolean checkEventCounters;

// Used for communication between commit internal callbacks and our
// VM killer thread.  The VM killer thread is waiting on this;
// the internal callback will notify when executed.
static public Object killSyncObject = new Object();

// because hydra executes each task in a new HydraThread, the transaction
// manager doesn't associate our commits with our begins (its not truly
// the same thread.  We can use suspend to get the current TXState (after
// begin() ... and then upon re-entering to either commit or rollback
// re-instate that context.
protected TXStateInterface txContext = null;

  /*
   *  Initialize info for test (random number generator, isSerialExecution,
   *  pid of this VM and isScopeLocal (derived from regionAttributes).
   */
  public static void HydraTask_initialize() {
    Log.getLogWriter().info("In HydraTask_initialize");
    if (viewTest == null) {
      viewTest = new ViewTest();
      viewTest.initialize();
    }
    // set to true when TxListener processed the afterCommit (for distIntegrity tests)
    TxBB.getBB().getSharedMap().put(TxBB.afterCommitProcessed, new Boolean(false));
  }

  public void initialize() {
    // establish our randomNumberGenerator (for commit vs. rollback)
    this.rng = TestConfig.tab().getRandGen();

    // is this a serialExecution/carefulValidation test?
    this.isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution, false);
    if (this.isSerialExecution) {
      Log.getLogWriter().info("ViewTest is running in SERIAL_EXECUTION mode");
    }

    // cache this VM PID 
    this.myVmPid = new Integer(RemoteTestModule.getMyPid());

    // cache info on SCOPE (isScopeLocal) need to determine isVisible
    Set root = CacheUtil.getCache().rootRegions();
    Iterator it = root.iterator();
    if (it.hasNext()) {
      Region aRegion = (Region)it.next();
      RegionAttributes ratts = aRegion.getAttributes();

      // set isScopeLocal field ... since tx initially supported only for p2p
      // the meaning of this field is whether or not tx updates will be viewed
      // in other VMs.  For edgeClients, tx updates will be distributed (unlike
      // local scope in p2p vms) even though the scope is local
      this.isBridgeClient = (ratts.getPoolName() != null) ? true : false;
      if (!isBridgeClient) {
         this.isScopeLocal = (ratts.getScope() == Scope.LOCAL) ? true : false;
      } 
      this.isProxy = (ratts.getDataPolicy().isEmpty());
    } else {
      throw new TestException("no regions found at initialization -- cannot setup test without established forest of regions!");
    }

    // cache boolean indicating whether or not to track cache & txListener events
    // based for now on whether or not a cacheListener is configured.
    checkEventCounters = TxPrms.checkEventCounters();
    Log.getLogWriter().info("isScopeLocal = " + isScopeLocal + ", isProxy = " + isProxy + ", myVmPid = " + myVmPid + ", checkEventCounters = " + checkEventCounters);

    // initialize timeToStop (see waitForDist/killCommittor)
    TxBB.getBB().getSharedMap().put(TxBB.timeToStop, new Boolean(false));
  }

  /*
   * Initialize information specific to transactional thread
   * (txVmPid is written to BB)
   */
  private void initializeTxInfo() {
    // This is the transactional thread -- let's put the VM PID into
    // the BB for use with local validation (since only local validators
    // will see this committed state with scope = LOCAL.
    Integer txVmPid = new Integer(RemoteTestModule.getMyPid());
    TxBB.getBB().getSharedMap().put(TxBB.TX_VM_PID, txVmPid);
    Log.getLogWriter().info("Putting txVmPid into BB at " + TxBB.TX_VM_PID + " with value = " + txVmPid);

    // write the clientName of the txVm to the BB (for use in OpList.java) by ViewTests. 
    String myClientName = RemoteTestModule.getMyClientName();
    TxBB.getBB().getSharedMap().put(TxBB.TX_VM_CLIENTNAME, myClientName);
    Log.getLogWriter().info("Putting clientVmName of txVm into BB at " + TxBB.TX_VM_CLIENTNAME + " with value " + myClientName);

    // Some view tests (mixed with non-tx threads) don't use the 
    // TransactionListener or Writer.  Write default value to the BB for
    // those tests.  (The TxWriter will overwrite this if/when invoked.
    TxBB.getBB().getSharedMap().put(TxBB.TxWriterAction, TxBB.TXACTION_NONE);

    this.initializeTxInfo = false;
  }

  /*
   * Execute a transaction ... every other invocation either does a beginTx()
   * or finishTx().  Details shown below.
   * 
   * BEGIN:
   *  - clear out last known NON_TX_OPLIST (so we don't try to validate
   *    against it)
   *  - clear out any CacheListener_op lists from previous transaction
   *  - doOperations and post to BB
   *  - validate state (this thread should see tx state)
   *  - post info to BB for validator threads to use (TX_IN_PROGRESS, etc).
   *
   * FINISH:
   *  - validate state (this thread should see tx state)
   *  - commit or rollback (determined by TxPrms.commitPercentage)
   *  - validate state (based on commit vs. rollback)
   *  - update BB info (tx complete, completion action, etc).
   *
   */
  public static void HydraTask_executeTx() {
    // check for any listener exceptions thrown by previous method execution
    TestHelper.checkForEventError(TxBB.getBB());

    // Either setup or commit/rollback a transaction
    viewTest.executeTx();
  }

  protected void executeTx() {
    // on first invocation, save txVmPid in BB & obtain scope
    if (this.initializeTxInfo) {
      initializeTxInfo();
    }

    // take appropriate action (start or complete the tx)
    if (this.txInProgress) {
      TxHelper.internalResume(this.txContext);
      finishTx();
    } else {
      beginTx();
      this.txContext = TxHelper.internalSuspend();
    }

    // toggle between begin & commit/rollback
    this.txInProgress = !this.txInProgress;
  }

  /* 
   * txThread: start a transaction, perform operations and validation
   * tx view 
   */
  protected void beginTx() {
    // TX_NUMBER tracks current transaction (from begin through commit)
    long txNum = TxBB.getBB().getSharedCounters().incrementAndRead(TxBB.TX_NUMBER);
    Log.getLogWriter().info("In beginTx() for TX_NUMBER = " + txNum);

    // Let the listeners know that we're now ready to process events
    // wait a bit so any creates from createDestroyedRegionsFromBB(true)
    // have a chance to come through
    MasterController.sleepForMs(10000);
    TxBB.getBB().getSharedCounters().increment(TxBB.PROCESS_EVENTS);

    // clear out last known NON_TX_OPLIST since we're starting fresh
    // on this TX_NUMBER (removes KEY from SharedMap).
    Log.getLogWriter().fine("Clearing BB NON_TX_OPLIST");
    TxBB.getBB().getSharedMap().remove(TxBB.NON_TX_OPLIST);

    // clear out cacheListeners records (create, destroy, invalidate, put)
    Log.getLogWriter().info("Removing CacheListener operation lists from BB");
    TxBB.getBB().getSharedMap().remove(TxBB.LocalListenerOpListPrefix + (txNum-1));
    TxBB.getBB().getSharedMap().remove(TxBB.RemoteListenerOpListPrefix + (txNum-1));

    // clear the destroyedRegionsList once done
    TxViewUtil.txUtilInstance.clearDestroyedRegions();

    // Set-up the txUtilInstance to suspend the transaction when getting a randomKey
    // Otherwise, getting a random key can bring all region entries into the readSet
    TxViewUtil.txUtilInstance.suspendResume = true;

    // validate cacheListener & txListener event counters from 
    // the previous transaction; match the expected counts established
    // from our collapsedOps list to the events processed
    // Note that this uses TxBB.COMPLETION_ACTION (TXACTION_COMMIT)
    // to determine if we expected events to be distributed.
    checkAndClearEventCounters();

    // random entry operations to be done here
    TxHelper.begin();

    // do random region/entry operations as part of our transaction
    // Save operation list to BB for other threads to use in validation
    // Proxy clients should not perform 'get' operations.  Since there
    // is no local storage, it always invokes the loader and oldValue is
    // always reported as null (which doesn't allow view validation).
    boolean allowGetOperations = (isProxy) ? false: true;
    OpList opList = TxViewUtil.doOperations(allowGetOperations);

    // save the list of operations
    TxBB.putOpList(opList);

    // setup callbacks to allow tracking of commit status 
    // via internal callbacks.  Used in HA Tests to allow
    // us to kill the vm at a specific time during the commit
    // process.  The thread waiting to initiate the kill
    // is waiting on our static killSyncObject, the Runnable issues
    // a notify on our killSyncObject and then sleeps for 30 seconds
    // before returning control to the caller 
    String commitStatePrm = TestConfig.tab().stringAt(TxPrms.commitStateTrigger, null);
    if (commitStatePrm != null) {
      setupCommitTestCallbacks(commitStatePrm);
    }

    // post the key to get to the opList (for other threads to use)
    TxBB.getBB().getSharedMap().put(TxBB.TX_OPLIST_KEY, TxBB.getOpListKey());

    // verify we (txThread) see new tx state
    // note that region apis (containsKey(), containsValueForKey() and
    // getValueInVM() will operate on transaction state in this thread
    Log.getLogWriter().info("validating tx state");
    validateTxOps(true);

    // update expected counts (for listener tests)
    setExpectedEventCounts();

    // announce tx in progress to other threads
    SharedCounters sc = TxBB.getBB().getSharedCounters();
    sc.zero(TxBB.TX_COMPLETED);
    sc.increment(TxBB.TX_READY_FOR_VALIDATION);
  }

  /*
   * txThread: commit or rollback current transaction 
   */
  protected void finishTx() {

    // either commit or rollback & verify accordingly
    int n = this.rng.nextInt(1, 100);
    int commitPercentage = TxPrms.getCommitPercentage();

    boolean failedCommit = false;
    boolean txAborted = false;
    boolean isCausedByTransactionWriterException = false;
    boolean isCausedByHA = false;

    if (n <= commitPercentage) {
      try {
        TxHelper.commit();
      } catch (ConflictException e) {
        failedCommit = true;
        Log.getLogWriter().info("finishTx() caught " + e.toString());

        // Was this caused by a TxWriter aborting the TX?
        Throwable causedBy = e.getCause();
        if (causedBy instanceof TransactionWriterException) {
          isCausedByTransactionWriterException = true;
        } else {
          Log.getLogWriter().info("causedBy is not a TransactionWriterException, but is " + causedBy, causedBy);
        }
      } catch (TransactionDataNodeHasDepartedException e) {
        failedCommit = true;
        Log.getLogWriter().info("finishTx() caught " + e.toString());

        // verify this was caused by a failure on a remote dataStore (see parRegIntegrityRemote.conf)
        String errStr = e.toString();
        isCausedByHA = errStr.indexOf("PartitionResponse got memberDeparted event") >=0;
      } catch (TransactionInDoubtException e) {
        failedCommit = true;
        Log.getLogWriter().info("finishTx() caught " + e.toString());

        // verify this was caused by a failure on a remote dataStore (see parRegIntegrityRemote.conf)
        String errStr = e.toString();
        isCausedByHA = errStr.indexOf("ReplyException: Failed to deliver message to members") >=0
            || errStr.indexOf("memberDeparted") >= 0;
      }

      // if a true CommitConflict, we should not have invoked the TxWriter
      if (failedCommit && !isCausedByTransactionWriterException) {
        if (!TxBB.getBB().getSharedMap().get(TxBB.TxWriterAction).equals(TxBB.TXACTION_NONE)) {
          throw new TestException("Invoked TransactionWriter after conflict detected " + TestHelper.getStackTrace());

        }
      }

      // The TransactionWriter (TxWriter) will occassionally abort the tx
      // If so, expect ConflictException Caused by TransactionWriterException
      // and invocation of afterFailedCommit()
      if (TxBB.getBB().getSharedMap().get(TxBB.TxWriterAction).equals(TxBB.TXACTION_ABORT)) {
        txAborted = true;
      }
      if (txAborted) {
        if (!failedCommit || !isCausedByTransactionWriterException) {
          throw new TestException("TransactionWriter threw TransactionWriterException (to abort tx), but caller did not process ConflictException Caused by TransactionWriterException " + TestHelper.getStackTrace());
        }
      }

      if (failedCommit && isCausedByTransactionWriterException) {
        if (!txAborted) {
          throw new TestException("Missed expected TX ABORT " + TestHelper.getStackTrace());
        }
      }

      if (failedCommit) {
        TxBB.inc(TxBB.NUM_EXPECTED_FAILED_COMMIT);
        if (txAborted) {
          TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_ABORT);
        } else {
          TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_ROLLBACK);
        }

        boolean isVisible = false;
        // parRegIntegrityRemote - adjust isVisible based on commitTrigger after remote dataStore killed
        if (isCausedByHA) {   // TransactionInDoubtException thrown after remote data store killed
            // the 'Origin Departed Commit' thread may continue the tx after
            // detecting that the tx coordinator has departed.  This is 
            // asynchronous, so give it a few seconds to run.
            MasterController.sleepForMs(3000);
            isVisible = updatesVisible();
        }
        validateCommittedState(isVisible);
      } else {
        TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_COMMIT);
        validateCommittedState(true);
      }
    } else {
      TxBB.inc(TxBB.NUM_EXPECTED_ROLLBACK);
      try {
         TxHelper.rollback();
      } catch (TransactionDataNodeHasDepartedException e) {
        Log.getLogWriter().info("finishTx() caught " + e.toString() + " on rollback");
      }

      TxBB.getBB().getSharedMap().put(TxBB.COMPLETION_ACTION, TxBB.TXACTION_ROLLBACK);
      // verify after rollback invoked
      validateCommittedState(false);
    }

    // update shared state info in BB
    SharedCounters sc = TxBB.getBB().getSharedCounters();
    sc.zero(TxBB.TX_READY_FOR_VALIDATION);
    sc.increment(TxBB.TX_COMPLETED);

    // see waitForDist/killCommittor
    TxBB.getBB().getSharedMap().put(TxBB.timeToStop, new Boolean(true));
  }

  protected void verifyCallbackInvocation(int numVmsWithTxListener) {
    // If there are no entry ops (all region ops), only the local
    // listener will be invoked
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
    boolean hasEntryOps = false;
    for (int i=0; i < txOpList.numOps(); i++) {
      Operation op = txOpList.getOperation(i);
      if (op.isEntryOperation()) {
        hasEntryOps = true;
      }
    }
    if (!hasEntryOps) {
       Log.getLogWriter().info("OpList does not contain any entry operations, only the local TxListener will be invoked.  Setting numVmsWithTxListener to 1");
       numVmsWithTxListener = 1;
    }

    SharedCounters sc = TxBB.getBB().getSharedCounters();
    long expectedFailedCommits = sc.read(TxBB.NUM_EXPECTED_FAILED_COMMIT) * numVmsWithTxListener;
    long expectedRollbacks = sc.read(TxBB.NUM_EXPECTED_ROLLBACK) * numVmsWithTxListener;
  
    long actualFailedCommits = sc.read(TxBB.TX_FAILURE);
    long actualRollbacks = sc.read(TxBB.TX_ROLLBACK);
  
    StringBuffer aStr = new StringBuffer();
  
    if (actualFailedCommits != expectedFailedCommits) {
       aStr.append("Expected " + expectedFailedCommits + " afterFailedCommit() listener invocations, but found " + actualFailedCommits);
    }
    if (actualRollbacks != expectedRollbacks) {
       aStr.append("Expected " + expectedRollbacks + " afterRollback() listener invocations, but found " + actualRollbacks);
    }
    if (aStr.length() > 0) {
       throw new TestException(aStr.toString() + " " + TestHelper.getStackTrace());
    }
  }

  /*
   * txThread: validates final view (after commit/rollback)
   */
  protected void validateCommittedState(boolean txCommitted) {

    // PROXY clients cannot validate their committed state (they
    // have no local data to access
    if (this.isProxy) {
       return;
    }

    if (txCommitted) {
      Log.getLogWriter().info("TX COMPLETED: validating tx committed");
      validateCombinedOpsAfterCommit();
    } else {
      // We had a conflict, validate nonTx + any verifiable rolled back txOps
      Log.getLogWriter().info("TX COMPLETED: validating tx rollback");
      validateCombinedOpsAfterRollback();
    }
  }

  /* 
   * Used after commit to combine nonTxOpList & txOpList (since txOps
   * may have more recent values than the nonTxOpList, for example if
   * the nonTx op is a get and the txOp is an update on the same entry
   */
  protected void validateCombinedOpsAfterCommit() {
    OpList nonTxOpList = (OpList)TxBB.getBB().getSharedMap().get(TxBB.NON_TX_OPLIST);
    // filter out any no-ops (eg: invalidating an already invalid entry)
    if (nonTxOpList != null) {
       for (int i=0; i < nonTxOpList.numOps(); i++) {
         Operation nonTxOp = nonTxOpList.getOperation(i);
         if (nonTxOp.isDoubleInvalidate()) {
            Log.getLogWriter().info("removing double invalidate from nonTxList " + nonTxOp.toString());
            nonTxOpList.remove(i);
         }
       }
    }

    if (nonTxOpList == null) {          // simply validate txOps committed
      validateTxOps(true, TxBB.TXACTION_COMMIT);  // committed=>visible
      return;
    }
    
    // update newValues in nonTxOperations for any updates + 
    // add txOps (that are not updates) so they can be verified
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    OpList fullList = new OpList();
    // update newValues in nonTxOperations for any updates in txOps
    for (int i=0; i < nonTxOpList.numOps(); i++) {
      Operation nonTxOp = nonTxOpList.getOperation(i);
      for (int j=0; j < txOpList.numOps(); j++) {
        Operation txOp = txOpList.getOperation(j);
        if (txOp.isEntryGet()) {       // don't update ops newValue to earlier 'get' value
          continue;
        }
        if (nonTxOp.affectedBy(txOp)) {
          // if we destroyed a nonTx operation target entry in the tx, we
          // can't verify the nonTx op, don't add it to the list!
          if (txOp.isEntryDestroy()) {
            nonTxOp = null;
            break;
          }
          nonTxOp.setNewValue(txOp.getNewValue());
        } 
      }
      if (nonTxOp != null) {
        fullList.add(nonTxOp);
      }
    }

    // add verifiable txOps to our list to verify
    for (int i=0; i < txOpList.numOps(); i++) {
      Operation txOp = txOpList.getOperation(i);
      for (int j=0; j < nonTxOpList.numOps(); j++) {
        Operation nonTxOp = nonTxOpList.getOperation(j);
        // We can validate a destroy which affected a nonTx operation target entry
        if (txOp.affectedBy(nonTxOp)) {
          if (!txOp.isEntryDestroy()) {
            txOp = null;
            break;
          }
        } 
      }
      if (txOp != null) {
        fullList.add(txOp);
      }
    }
    Log.getLogWriter().fine("COMBINED NON-TX & TX OPS = " + fullList);
    validateState(fullList, true);
  }

  /*
   * Used in case of rollback to combine nonTxOpList with verifiable
   * operations from Tx.  (Operations that were rolled back and whose
   * values we're overridden by the nonTxOps).  Don't include any txOps
   * whose keys are not unique with respect to nonTx ops).
   */
  protected void validateCombinedOpsAfterRollback() {
    OpList nonTxOpList = (OpList)TxBB.getBB().getSharedMap().get(TxBB.NON_TX_OPLIST);
    if (nonTxOpList == null) {          // simply validate txOps rolledback
      validateTxOps(false, TxBB.TXACTION_ROLLBACK);  // rolledBack=>not visible
      return;
    }

    // verify nonTxOps - happened at operation time, should be committed state
    validateNonTxOps();

    // get the txOps not overridden by nonTxOps & verify NOT in current state
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    OpList shortList = new OpList();
    for (int i=0; i < txOpList.numOps(); i++) {
      Operation txOp = txOpList.getOperation(i);
      for (int j=0; j < nonTxOpList.numOps(); j++) {
        Operation nonTxOp = nonTxOpList.getOperation(j);
        if (txOp.affectedBy(nonTxOp)) {
          txOp = null;
          break;
        }
      }
      if (txOp != null) {
        shortList.add(txOp);
      }
    }
    Log.getLogWriter().fine("TXOPS NOT AFFECTED BY NON-TX OPS = " + shortList);
    validateState(shortList, false);
  }

  /*
   * Used by validator threads to check the final state
   * after the Tx has been committed.
   * Uses BB counters and sharedMap to determine if commit vs. rollback
   *
   * This thread may also perform non-transactional operations 
   * as configured by TxPrms.executeNonTxOperations.  If so, it 
   * performs the operations, then writes to a well-known map entry
   * (NON_TX_OPLIST) in the BB.  All validation checks will verify
   * this state (in distributed configurations) as well as the tx
   * state (provided by an OpListKey in the BB).
   */
  public static void HydraTask_checkCommittedState() {
    Log.getLogWriter().info("HydraTask_checkCommittedState(TX_NUMBER = " + TxBB.getBB().getSharedCounters().read(TxBB.TX_NUMBER) + ")" );
    // Hold off and allow distributions to occur
    MasterController.sleepForMs(1000);
    viewTest.checkCommittedState();

    // If we're done with the TX, replace any lost regions
    if (TxBB.getBB().getSharedCounters().read(TxBB.TX_COMPLETED) == 1) {
       // turn off listener processing (we'll be getting create events
       // from the createDestroyedRegionsFromBB() that we aren't interested in)
       // This gets set again by beginTx()
       TxBB.getBB().getSharedCounters().zero(TxBB.PROCESS_EVENTS);

       // re-create any destroyed regions for this VM
       // allow a little extra time for events to complete processing before
       // re-enabling PROCESS_EVENTS
       TxViewUtil.txUtilInstance.createDestroyedRegionsFromBB(true);
    }
  }

  protected void checkCommittedState() {

    // Note that proxy clients cannot validate their committed state
    // as they have no local storage
    if (this.isProxy) {
      Log.getLogWriter().info("Cannot validate state with DataPolicy.EMPTY, no local storage/state, returning ...");
      return;
    } 

    // If executeTx(begin) has executed, check that our validator
    // only sees the committed state and not the tx state
    if (TxBB.getBB().getSharedCounters().read(TxBB.TX_READY_FOR_VALIDATION) == 1) {
      // executeNonTxOperations - task attribute for non-tx threads
      // they can either be simple validators OR one additional thread
      // could also generate non-tx operations.
      boolean executeNonTxOperations = TxPrms.getExecuteNonTxOperations();
      Log.getLogWriter().info("TxPrms.getExecuteNonTxOperations() = " + executeNonTxOperations);

      if (executeNonTxOperations) {
        Log.getLogWriter().info("executing non-tx operations");
        OpList opList = TxViewUtil.doOperations();
        TxBB.getBB().getSharedMap().put(TxBB.NON_TX_OPLIST, opList);
        Log.getLogWriter().info("non-tx operations written to BB " + opList.toString());
      } else {
        Log.getLogWriter().info("check view, TX in progress, we should only see committed state");
      }
      // If we have a nonTx list, we can't validate that we see original 
      // values for txOps (because we may be causing conflicts with our 
      // nonTxOps.  Validate txOps when we can; just nonTxOps otherwise.
      if (!validateNonTxOps()) {
        validateTxOps(false);
      }
      return;
    }

    if (TxBB.getBB().getSharedCounters().read(TxBB.TX_COMPLETED) == 1) {

      // get the completion type (commit/rollback)
      String txAction = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
      if (txAction.equals(TxBB.TXACTION_COMMIT)) {
        Log.getLogWriter().info("validate tx committed");
        validateCombinedOpsAfterCommit();   
      } else {
        Log.getLogWriter().info("validate tx rollback");
        validateCombinedOpsAfterRollback();   
      }
      return;
    }
    Log.getLogWriter().info("checkCommittedState: no tx state to verify, returning");
  }

  /*
   *  Validates both non-tx opList (if one exists) and the current
   *  state vs. the tx oplist.  Note that the non-tx operations should
   *  be seen by everyone (with distributed scope), while the tx operations
   *  may or not be seen based on the situation.  Our txOps validation is 
   *  therefore controlled by the boolean, isTxVisible.
   *
   *  @param - isTxVisible is only used while validating the current
   *           state vs. the tx opList.  It denotes whether the tx state
   *           should be seen by the thread performing validation.
   */
  protected void validateAllStates(boolean isTxVisible) {
    validateNonTxOps();
    validateTxOps(isTxVisible);
  }

  /*
   *  Validates non-tx opList (if one exists) 
   *
   *  @returns boolean to indicate if nonTxOps existed & were verified
   */
  protected boolean validateNonTxOps() {

    boolean nonTxOpsVerified = false;

    // Check non-tx state (if exists) -- note that non-tx operations
    // should always be visible (as long as distributed scope)
    // validateState() handles local scope differences
    OpList nonTxOpList = (OpList)TxBB.getBB().getSharedMap().get(TxBB.NON_TX_OPLIST);
    if (nonTxOpList != null) {
      Log.getLogWriter().info("validateAllStates() - verifying state with nonTxOpList");
      validateState(nonTxOpList, true);
      nonTxOpsVerified = true;
    }
    return nonTxOpsVerified;
  }

  /*
   *  Validates txOpList (may or may not be seen based on situation)
   *  Our validation is therefore controlled by the isTxVisible boolean.
   *
   *  @param - isTxVisible used while validating the current
   *           state vs. the tx opList.  It denotes whether the tx state
   *           should be seen by the thread performing validation.
   *  @return - boolean returned indicates that TxOps were found & verified
   */
  protected boolean validateTxOps(boolean isTxVisible) {
     return validateTxOps(isTxVisible, TxBB.TXACTION_NONE);
  }

  protected boolean validateTxOps(boolean isTxVisible, String txAction) {
    // check tx state (if exists)
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
    Log.getLogWriter().info("validateAllStates() - verifying state with txOpList");
    validateState(txOpList, isTxVisible, txAction);
    return true;
  }

  /*
   *  Validates values in the cache vs. operations recently 
   *  executed as part of a transaction.
   *
   *  @param OpList opList - the opList to verify (may be Tx or non-tx
   *         opList from BB.
   *  @param boolean isVisible - should this thread 'see' the same state
   *         indicated by the transaction data in the BB?
   *         For example, 
   *           if tx not yet committed or rolledback invoke with isVisible=false
   *           to verify cache values against opList oldValue
   *         if tx committed, invoke with isVisible=true to verify cache values
   *           match those in newVal of opList
   */
  protected void validateState(OpList opList, boolean isVisible) {
     validateState(opList, isVisible, TxBB.TXACTION_NONE); 
  }

  protected void validateState(OpList opList, boolean isVisible, String txAction) {

    // We cannot validate state for proxy (cacheless) clients
    if (this.isProxy) {
       return;
    }

    // replace with collapsed opList (multiple entry operations on same
    // region/key or region operations which affect region in current operation
    opList = opList.collapse(txAction);

    for (int i=0; i < opList.numOps(); i++) {
      Operation op = opList.getOperation(i);

      // if local operation and we are not in the txVm, should not see mods
      if (op.isLocalOperation() && !TxViewUtil.inTxVm()) {
        Log.getLogWriter().fine("validateState() - isVisible forced to false");
        isVisible = false;
      }

      // whether or not this change is visible depends on tx state 
      // not yet committed, committed or rolledback AND the 
      // scope/mirror (scope = local won't be seen in separate VMs) and 
      // (scope = dist*/mirror = none won't be seen
      // For scope = dist/mirror=keysValues we can't have localDestroy or invalidate
      if (op.isEntryOperation()) {
        // if we aren't looking at NEWVAL, we need to adjust OLDVAL
        // with any affects of region operations before we get expectedValues
/*
        if (!isVisible) {
          adjustOldValue(op, opList);
        }
*/

        EntryValidator expectedValues = EntryValidator.getExpected(op, isVisible);
        // if isLocalScope & this is a newly created object (by the txThread)
        // none of the remote VMs will ever see this new object (not the
        // create or any subsequent operations) ... update expectedValues
        // accordingly (keyExists = false, etc).
        if (this.isScopeLocal && !TxViewUtil.inTxVm()) {
          if (NameFactory.getCounterForName(op.getKey()) > TestConfig.tab().intAt(TxPrms.maxKeys)-1) {
            Log.getLogWriter().fine("override expected values -- scope is local and key " + op.getKey() + " is only visible in txVm");
            expectedValues.setKeyExists(false);
            expectedValues.setHasValue(false);
            expectedValues.setCacheValue(EntryValidator.DONT_CARE);
          } else {
              // with local scope & key < maxKeys we know the key has
              // to be there and must have a value (since invalidates
              // and destroys won't be distributed ... reset expectedValues
              // we just can't guarentee the value (since oldValue is 
              // maintained with respect to the txThreads view.
              expectedValues.setKeyExists(true);
              expectedValues.setHasValue(true);
              expectedValues.setCacheValue(EntryValidator.DONT_CARE);
          }
        }

        EntryValidator actualValues = EntryValidator.getActual(op);
        expectedValues.compare(actualValues);

      } else if (op.isRegionOperation()) {   
        // Most region operations are immediately visible to all, EXCEPT
        // to remote VMs (if operation was region-local-destroy or region
        // local-invalidate OR scope is local).
        boolean regionOpsVisible = true;
        if (!TxViewUtil.inTxVm()) {
          if (this.isScopeLocal || op.isLocalOperation()) {
            regionOpsVisible = false;
          }
        }

        RegionValidator expectedValues = RegionValidator.getExpected(op, regionOpsVisible);
        RegionValidator actualValues = RegionValidator.getActual(op);
        expectedValues.compare(actualValues);

      } else if (op.getOpName().equalsIgnoreCase(Operation.CACHE_CLOSE)) {
        validateCacheClosed(isVisible);
      } else { // unknown operation
       throw new TestException("Unknown operation " + op);
      }
    }
  }

  protected void validateCacheClosed(boolean isVisible) {
   Log.getLogWriter().info("Entering validateCacheClosed");
   boolean cacheClosed = CacheUtil.getCache().isClosed();
   /* Logic table (isVisible, cacheClosed)
    * 0 0 - not visible, not closed = ok
    * 0 1 - not visible, closed = throw an exception
    * 1 0 - visible, not closed = throw an exception
    * 1 1 - visible, closed = ok
    * So, if the two values are not the same (both 0's or both 1's)
    * we need to throw an error.
    */
   if (isVisible != cacheClosed) {
     throw new TestException("Validation failure in cacheClosed");
   } 
  }

  protected void adjustOldValue(Operation op, OpList opList) {
    
    Log.getLogWriter().info("adjustOldValue received op = " + op.toString());

    // Update expected values based on any region operations since 
    // these occurred during the transaction and were applied immediately 
    // For example, hasValue & cacheValue will both be affected if a region 
    // invalidate occurred
    for (int j=0; j < opList.numOps(); j++) {
      Operation tmpOp = opList.getOperation(j);
      if (tmpOp.isRegionOperation()) {
        if (op.getRegionName().startsWith(tmpOp.getRegionName())) {
          if (tmpOp.isLocalOperation()) {
            if (TxViewUtil.inTxVm()) {
              // update oldValue to reflect region impact to this operation
              Object updatedOldVal = null;     // if regionDestroy -> null
              if (tmpOp.getOpName().endsWith("inval")) {
                updatedOldVal = new String("LOCAL_INVALID");
              } 
              op.setOldValue(updatedOldVal);
            }
          } else {           // distributed operation
            Object updatedOldVal = null;       // if destroy -> null
            if (tmpOp.getOpName().endsWith("inval")) {
                updatedOldVal = new String("INVALID");
            }
            op.setOldValue(updatedOldVal);
          }
        }
      }
    }
    Log.getLogWriter().info("adjustOldValue returns op = " + op.toString());
  }

  // Code to support event counters
  // There are three different types of counters (expected, cacheListener, txListener)
  // Each type has counters for create, destroy, invalidate and update
  // At executeTx(begin), all counters are cleared and the expected counts built
  // based on the collapsed opList (*numVMs)
  // cacheListener - updated as cacheListener events processed
  // txListeners - updated on receipt of TxEvent
  // validation is done as the last part of executeTx(finish).

  static final int LOCAL_EVENTS = 0;
  static final int REMOTE_EVENTS = 1;
  private void setExpectedEventCounts() {

    if (!this.checkEventCounters) {
      return;
    }
 
    // clear existing counters
    SharedCounters sc = TxBB.getBB().getSharedCounters();
    TxBB.getBB().zeroEventCounters();

    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
 
    // update expected counters specific to the local (tx) VM listener
    incrementEventCounters(txOpList, LOCAL_EVENTS);

    // update expected counters for the remote VMs listeners
    incrementEventCounters(txOpList.getEntriesWithUniqueKeys(txOpList.getEntryOps()), REMOTE_EVENTS);
  }


  /** updatedInThisTx returns a boolean which indicates whether
   *  or not an update on this same Object (region, key match) occurred
   *  after the get/load in this same transaction.
   */
  private boolean updatedInThisTx(Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();

    // find our 'get' (op) and see if updated after the load
    int i = 0;
    for (i=0; i < txOpList.numOps(); i++) {
      Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;  // only interested in what happened BEFORE op
    }

    // we are now indexing the passed in get/load op, move past this and continue
    for (i++; i < txOpList.numOps(); i++) {
      Operation tmpOp = txOpList.getOperation(i);
      if (tmpOp.isEntryUpdate() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("updatedInThisTx returning TRUE + updateOperation = " + tmpOp.toString() + " occurred after get/loadOp = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** invalidatedInThisTx returns a boolean which indicates whether
   *  or not a invalidate on this same Object (region, key match) occurred
   * in this same transaction (prior to op)
   */
  private boolean invalidatedInThisTx(Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    for (int i=0; i < txOpList.numOps(); i++) {
      Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;  // only interested in what happened BEFORE op
      if (tmpOp.isEntryInvalidate() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("invalidatedInThisTx returning TRUE + invalidateOperation = " + tmpOp.toString() + " occurred before op = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** createdInThisTx returns a boolean which indicates whether
   *  or not an entry was created and destroyed within the same tx (in that order).
   *  The destroyOperation is passed in, true is returned if that same entry
   *  was created earlier in the same tx.
   *  (Note that remote VMs will not get a TxEvent for entries which are
   *  created and destroyed within the same tx).
   */
  private boolean createdInThisTx(Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    for (int i=0; i < txOpList.numOps(); i++) {
      Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;   // only interested in creates prior to DESTROY
      if (tmpOp.isEntryCreate() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("createdInThisTx returning TRUE, createOperation = " + tmpOp.toString() + " occurred prior to destroyOp = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** destroyedBeforeLoadInThisTx returns a boolean which indicates whether
   *  or not an entry was destroyed prior to a get/load within the same tx.
   *  The get operation is passed in, true is returned if that same entry
   *  was destroyed earlier in the same tx.
   *  Note that this appears as a create to the local (tx) VM and as an 
   *  update to remote VMs (since they just get the update their previously
   *  (non-destroyed) entry.
   */
  private boolean destroyedBeforeLoadInThisTx(Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    for (int i=0; i < txOpList.numOps(); i++) {
      Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;   // only interested in destroy prior to LOAD
      if (tmpOp.isEntryDestroy() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("destroyedBeforeLoad returning TRUE + destroyOperation = " + tmpOp.toString() + " occurred prior to getOperation = " + op.toString());
        rc = true;
        break;
      }
    }
    return rc;
  }

  /** destroyedInThisTx returns a boolean which indicates whether
   *  or not an entry was created and destroyed within the same tx (in that order).
   *  The createOperation is passed in, true is returned if that same entry
   *  was subsequently destroyed in the same tx.
   *  (Note that remote VMs will not get a TxEvent for entries which are
   *  created and destroyed within the same tx).
   */
  private boolean destroyedInThisTx(Operation op) {
    boolean rc = false;

    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);

    String regionName = op.getRegionName();
    String key = (String)op.getKey();
    // start with the create operation, then look for subsequent destroy
    int i = 0;
    Log.getLogWriter().fine("Searching for original (create) operation in OpList, op = " + op.toString());
    while (i < txOpList.numOps()) {
      Operation tmpOp = txOpList.getOperation(i);
      if (op.equals(tmpOp)) break;   // only interested in creates followed by destroy
      Log.getLogWriter().fine("operation " + tmpOp.toString() + " does not match createOp = " + op.toString());
      i++;
    }
    Log.getLogWriter().fine("Found original operation in opList " + op.toString());

    // txOpList[i] = the createOperation (passed in)
    // We are now looking at the createOperation, get past that to next op
    Log.getLogWriter().fine("Searching for an entry-destroy on same region/key as createOp  " + op.toString());
    for (i++; i< txOpList.numOps(); i++) {
      Operation tmpOp = txOpList.getOperation(i);
      if (tmpOp.isEntryDestroy() && tmpOp.usesSameRegionAndKey(op)) {
        Log.getLogWriter().info("destroyedInThisTx returning TRUE + destroyOperation = " + tmpOp.toString() + " occurred prior to createOp = " + op.toString());
        rc = true;
        break;
      }
      Log.getLogWriter().fine("Operation " + tmpOp.toString() + " does not match createOp = " + op.toString());
    }
    return rc;
  }

  /** hostRegionDestroyedInThisTx returns a boolean which indicates whether
   *  or not the region (or any parent regions) was destroyed in this transaction.
   *  Since region destroys propogated to remote VMs, but region creates are not,
   *  its possible that a region could be destroyed and then re-created and finally
   *  an entry created in that region.  In this case, the remote VMs will not get
   *  the createRegionEvent (since the region no longer exists in the remote VMs).
   */
  private boolean hostRegionDestroyedInThisTx(Operation op) {
    boolean rc = false;
                                                                                         
    // Get the original & complete opList
    String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
    OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
                                                                                         
    int i = 0;
    for (i=0; i < txOpList.numOps(); i++) {
      Operation tmpOp = txOpList.getOperation(i);
      if (tmpOp.isRegionDestroy()) {
        if (op.getRegionName().startsWith(tmpOp.getRegionName())) {
          Log.getLogWriter().info("hostRegionDestroyedInThisTx found regionDestroy = " + tmpOp.toString() + " op = " + op.toString());
          rc = true;
          break;
        }
      }
    }
    return rc;
  }

  public void incrementEventCounters(OpList txOpList, int counterType) {
   
    String counterTypeName = (counterType == LOCAL_EVENTS) ? "LOCAL" : "REMOTE";
    Log.getLogWriter().fine("In incrementEventCounters, counterType = " + counterTypeName);
    for (int i=0; i < txOpList.numOps(); i++) {
      Operation op = txOpList.getOperation(i);
      String opName = op.getOpName();
      // We need to differentiate between isLoad & notLoad events!
      boolean isLoad = false;

      // handle gets specifically: may end up as no-op (exists), create (new) or update (if previously invalidated)
      if (op.isEntryGet()) {
        if (op.isPreviouslyInvalid() || invalidatedInThisTx(op)) {   // invalid, so key exists
          opName = Operation.ENTRY_UPDATE;
          isLoad = true;
        } else if (destroyedBeforeLoadInThisTx(op)) {
           // If we did a destroy prior to get (load) in this same Tx, it would
           // appear as a create in the local VM, but as updates in the remote VMs
           opName = (counterType==LOCAL_EVENTS) ? Operation.ENTRY_CREATE : Operation.ENTRY_UPDATE;
        } else if (op.getOldValue() == null) {         // destroyed => load & new value
          opName = Operation.ENTRY_CREATE;
          isLoad = true;

          // get/load + update = distributed create (isNotLoad)
          if (counterType == REMOTE_EVENTS) {
            if (updatedInThisTx(op)) {
              isLoad = false;
            }
          }
        } 
      }

      // update appropriate counter
      // Note that we aren't interested in region ops or in gets that
      // didn't result in a loader operation (create or update).
      SharedCounters sc = TxBB.getBB().getSharedCounters();
      if (opName.equalsIgnoreCase(Operation.ENTRY_CREATE)) {
         // if entry's region was destroyed in this transaction, then remote VMs
         // will not get the entry create events (since the region no longer exists
         // for those remote VMs.  Therefore, we only update the remote counters
         // if the host region wasn't destroyed in the tx.  
         if (counterType == REMOTE_EVENTS) {   // operates on uniqueKeys in opList
           if (hostRegionDestroyedInThisTx(op)) {
             sc.increment((isLoad) ? TxBB.CREATE_IN_DESTROYED_REGION_ISLOAD : TxBB.CREATE_IN_DESTROYED_REGION);
           } else {
             sc.increment((isLoad) ? TxBB.REMOTE_CREATE_ISLOAD : TxBB.REMOTE_CREATE);
           } 
         } else {                              // LOCAL_EVENTS: operates on all ops
           if (destroyedInThisTx(op)) {
             sc.increment(TxBB.CONFLATED_CREATE_DESTROY);
           } 
           sc.increment((isLoad) ? TxBB.LOCAL_CREATE_ISLOAD : TxBB.LOCAL_CREATE);
         }

         // transactions will not invoke netLoad
      } else if (opName.equalsIgnoreCase(Operation.ENTRY_UPDATE)) {
         //transactions will not invoke netLoad
         if (isLoad) {
           sc.increment((counterType == REMOTE_EVENTS) ? TxBB.REMOTE_UPDATE_ISLOAD : TxBB.LOCAL_UPDATE_ISLOAD );
         } else {
           sc.increment((counterType == REMOTE_EVENTS) ? TxBB.REMOTE_UPDATE : TxBB.LOCAL_UPDATE);
         }
      } else if (opName.equalsIgnoreCase(Operation.ENTRY_DESTROY)) {
         // if an entry is created and destroyed within the same tx, remote vms
         // will NOT see either in the TxEvent
         if (counterType == REMOTE_EVENTS) {
            if (!createdInThisTx(op)) {
               sc.increment(TxBB.REMOTE_DESTROY);
            }
         } else sc.increment(TxBB.LOCAL_DESTROY);
      } else if (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_DESTROY)) {
         if (counterType == LOCAL_EVENTS) {
            sc.increment(TxBB.LOCAL_LOCAL_DESTROY);
         }
      } else if (opName.equalsIgnoreCase(Operation.ENTRY_INVAL)) {
         if (!op.isDoubleInvalidate()) {
           sc.increment((counterType == REMOTE_EVENTS) ? TxBB.REMOTE_INVALIDATE : TxBB.LOCAL_INVALIDATE);
         }
      } else if (opName.equalsIgnoreCase(Operation.ENTRY_LOCAL_INVAL)) {
         if (!op.isDoubleInvalidate()) {
           sc.increment((counterType == REMOTE_EVENTS) ? TxBB.REMOTE_LOCAL_INVALIDATE : TxBB.LOCAL_LOCAL_INVALIDATE);
         }
      } 
    }
    TxBB.getBB().printSharedCounters();
  }

  private void checkAndClearEventCounters() {

    if (!this.checkEventCounters) {
      return;
    }

    // CacheWriter events
    checkWriterCounters();

    // CacheListener events
    checkEventCounters();

    // Transaction Listener events
    checkTxEventCounters(TxEventCountersBB.getBB());
     
    // Transaction Writer events
    checkTxEventCounters(TxWriterCountersBB.getBB());

    // clear event counters (local & remote) 
    EventCountersBB.getBB().zeroAllCounters();
    WriterCountersBB.getBB().zeroAllCounters();
    TxEventCountersBB.getBB().zeroAllCounters();
    TxWriterCountersBB.getBB().zeroAllCounters();

    // clear TxWriterAction
    TxBB.getBB().getSharedMap().put(TxBB.TxWriterAction, TxBB.TXACTION_NONE);
    
  }

/** 
 *  Check CacheWriter Event Counters
 */
protected void checkWriterCounters() {
   SharedCounters counters = TxBB.getBB().getSharedCounters();
   // "LOCAL" counters reflect Writer operations (the local tx vm will see all)
   long localCreate = counters.read(TxBB.LOCAL_CREATE);
   long localCreateIsLoad = counters.read(TxBB.LOCAL_CREATE_ISLOAD);
   long localUpdate = counters.read(TxBB.LOCAL_UPDATE);
   long localUpdateIsLoad = counters.read(TxBB.LOCAL_UPDATE_ISLOAD);
   long localDestroy = counters.read(TxBB.LOCAL_DESTROY);
   long localInval = counters.read(TxBB.LOCAL_INVALIDATE);
   long localLocalDestroy = counters.read(TxBB.LOCAL_LOCAL_DESTROY);
   long localLocalInval = counters.read(TxBB.LOCAL_LOCAL_INVALIDATE);

   ArrayList al = new ArrayList();
     // beforeCreate counters
        al.add(new ExpCounterValue("numBeforeCreateEvents_isDist", (localCreate+localCreateIsLoad)));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotExp", (localCreate+localCreateIsLoad)));
        if (this.isBridgeClient || this.isProxy) { // for clients and empty peers, events are fired in servers/datahosts
          al.add(new ExpCounterValue("numBeforeCreateEvents_isRemote", (localCreate+localCreateIsLoad)));
          al.add(new ExpCounterValue("numBeforeCreateEvents_isNotRemote", 0));
        } else {
          al.add(new ExpCounterValue("numBeforeCreateEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeCreateEvents_isNotRemote", (localCreate+localCreateIsLoad)));
        }
        al.add(new ExpCounterValue("numBeforeCreateEvents_isLoad", localCreateIsLoad));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotLoad", localCreate));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isLocalLoad", localCreateIsLoad));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotLocalLoad", localCreate));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotNetLoad", (localCreate+localCreateIsLoad)));
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numBeforeCreateEvents_isNotNetSearch", (localCreate+localCreateIsLoad)));

      // beforeDestroy counters
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isDist", localDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotExp", localDestroy));
        if (this.isBridgeClient || this.isProxy) {  // for clients and empty peers, events are fired in servers/datahosts
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isRemote", localDestroy));
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotRemote", 0));
        } else {
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotRemote", localDestroy));
        }
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotLoad", localDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isLocalLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotLocalLoad", localDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotNetLoad", localDestroy));
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotNetSearch", localDestroy));

     // beforeUpdate counters
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isDist", (localUpdate+localUpdateIsLoad)));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotExp", (localUpdate+localUpdateIsLoad)));
        if (this.isBridgeClient || this.isProxy) {  // for clients and empty peers, events are fired in servers/datahosts
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isRemote", (localUpdate+localUpdateIsLoad)));
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotRemote", 0));
        } else {
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotRemote", (localUpdate+localUpdateIsLoad)));
        }
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isLoad", localUpdateIsLoad));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotLoad", localUpdate));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isLocalLoad", localUpdateIsLoad));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotLocalLoad", localUpdate));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNetLoad", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotNetLoad", (localUpdate+localUpdateIsLoad)));
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNetSearch", 0)); 
        al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotNetSearch", (localUpdate+localUpdateIsLoad)));

     // beforeRegionDestroy counters
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isDist", 0));
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotDist", 0)); 
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isExp", 0)); 
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotExp", 0));
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isRemote", 0));
        al.add(new ExpCounterValue("numBeforeRegionDestroyEvents_isNotRemote", 0));

   WriterCountersBB.getBB().checkEventCounters(al);
}

/** 
 *  Check CacheListener Event Counters
 */
protected void checkEventCounters() {
   SharedCounters counters = TxBB.getBB().getSharedCounters();

   // "REMOTE" counters reflect the events based on the collapsed opList
   // which now (with prTx) applies to CacheListeners (invoked at commit time)
   // These will now be the same for the local and remote VMs
   long remoteCreate = counters.read(TxBB.REMOTE_CREATE);
   long remoteCreateIsLoad = counters.read(TxBB.REMOTE_CREATE_ISLOAD);
   long remoteUpdate = counters.read(TxBB.REMOTE_UPDATE);
   long remoteUpdateIsLoad = counters.read(TxBB.REMOTE_UPDATE_ISLOAD);
   long remoteDestroy = counters.read(TxBB.REMOTE_DESTROY);
   long remoteInval = counters.read(TxBB.REMOTE_INVALIDATE);
   long remoteLocalDestroy = counters.read(TxBB.REMOTE_LOCAL_DESTROY);
   long remoteLocalInval = counters.read(TxBB.REMOTE_LOCAL_INVALIDATE);

   // allow for conflated entry create/destroy which is seen simply as a destroy
   // in Tx VM (on commit only) when tx vm has DataPolicy.EMPTY
   long numConflatedCreateDestroy = counters.read(TxBB.CONFLATED_CREATE_DESTROY);
   if (!this.isProxy) {
      numConflatedCreateDestroy = 0;
   }

   long numClose = 0;

   // handle situation with rollback or commitConflict
   // remote listeners won't get any events in this case!
   String endResult = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
   // We don't actually have a result yet
   if (endResult == null) 
     return;

   int numVmsWithList = TestHelper.getNumVMs();
   if (this.isProxy) {
     numVmsWithList = TestHelper.getNumVMs() - 1;   // no events fired in empty peer
   }
   Log.getLogWriter().info("num VMs with listener installed: " + numVmsWithList);

   // On Rollback, we shouldn't get any CacheListener events (set all expected counters to 0)
   if (!endResult.equals(TxBB.TXACTION_COMMIT)) {
      remoteCreate = 0;
      remoteCreateIsLoad = 0;
      remoteUpdate = 0;
      remoteUpdateIsLoad = 0;
      remoteDestroy = 0;
      remoteInval = 0;
      remoteLocalDestroy = 0;
      remoteLocalInval = 0;
      numConflatedCreateDestroy = 0;
   } 

   ArrayList al = new ArrayList();
        // afterCreate counters
           al.add(new ExpCounterValue("numAfterCreateEvents_isDist", ((remoteCreate + remoteCreateIsLoad) * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", ((remoteCreate + remoteCreateIsLoad) * numVmsWithList))); 
           if (this.isProxy) { // see BUG 45556 (all events fired on datahost)
             al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", ((remoteCreate+remoteCreateIsLoad) * (numVmsWithList))));
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", ((remoteCreate+remoteCreateIsLoad) * (numVmsWithList-1))));
             al.add(new ExpCounterValue("numAfterCreateEvents_isNotRemote", (remoteCreate+remoteCreateIsLoad)));
           }
           al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", (remoteCreateIsLoad * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad", (remoteCreate * numVmsWithList)));
           // once 40870 is fixed, this will need to be adjusted, 
           // right now all get => load => LOCAL_LOAD_CREATE (isLocalLoad = true)
           al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", (remoteCreateIsLoad * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLocalLoad", (remoteCreate * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetLoad", ((remoteCreate+remoteCreateIsLoad) * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotNetSearch", ((remoteCreate+remoteCreateIsLoad) * numVmsWithList))); 

        // afterDestroy counters
           al.add(new ExpCounterValue("numAfterDestroyEvents_isDist", ((remoteDestroy * numVmsWithList)+numConflatedCreateDestroy)));
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotExp", ((remoteDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           if (this.isProxy) { // see BUG 45556 (all events fired on datahost)
             al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", (remoteDestroy * (numVmsWithList))));
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", (remoteDestroy * (numVmsWithList-1))));
             al.add(new ExpCounterValue("numAfterDestroyEvents_isNotRemote", (remoteDestroy+numConflatedCreateDestroy)));
           }
           al.add(new ExpCounterValue("numAfterDestroyEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLoad", ((remoteDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotLocalLoad", ((remoteDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetLoad", ((remoteDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterDestroyEvents_isNotNetSearch", ((remoteDestroy * numVmsWithList)+numConflatedCreateDestroy))); 

        // afterInvalidate counters
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isDist", (remoteInval * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotExp", (remoteInval * numVmsWithList))); 
           if (this.isProxy) {  // see BUG 45556, all events fired in dataHosts
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", (remoteInval * (numVmsWithList))));
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", (remoteInval * (numVmsWithList-1))));
             al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotRemote", remoteInval));
           }
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLoad", (remoteInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotLocalLoad", (remoteInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetLoad", (remoteInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isNotNetSearch", (remoteInval * numVmsWithList))); 

        // afterUpdate counters
           al.add(new ExpCounterValue("numAfterUpdateEvents_isDist", ((remoteUpdate + remoteUpdateIsLoad) * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", ((remoteUpdate + remoteUpdateIsLoad) * numVmsWithList))); 
           if (this.isProxy) {  // see BUG 45556, events are fired in dataHosts
             al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote", ((remoteUpdate+remoteUpdateIsLoad) * (numVmsWithList))));
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", (remoteUpdateIsLoad * numVmsWithList))); 
             al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad", (remoteUpdate * numVmsWithList)));
           }
           // once 40870 is fixed, this will need to be adjusted, 
           // right now all get => load => LOCAL_LOAD_CREATE (isLocalLoad = true)
           al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", (remoteUpdateIsLoad * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLocalLoad", (remoteUpdate * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetLoad", ((remoteUpdate + remoteUpdateIsLoad) * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotNetSearch", ((remoteUpdate + remoteUpdateIsLoad) * numVmsWithList))); 

        // afterRegionDestroy counters
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isDist", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotExp", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isRemote", 0));
           al.add(new ExpCounterValue("numAfterRegionDestroyEvents_isNotRemote", 0));

        // afterRegionInvalidate counters
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isDist", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotDist", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotExp", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isRemote", 0));
           al.add(new ExpCounterValue("numAfterRegionInvalidateEvents_isNotRemote", 0));

        // after(Region) close counters
           al.add(new ExpCounterValue("numClose", numClose, false));

   EventCountersBB.getBB().checkEventCounters(al);
}

/** 
 *  Check Transaction Listener (or Writer) Event Counters
 *  
 *  @param - the blackboard to verify agains (TxEventCountersBB vs. TxWriterCountersBB)
 */
protected void checkTxEventCounters(Blackboard bb) {
   // Note that "REMOTE" counters are the expected counts from the 
   // collapsed list of operations.
   SharedCounters counters = TxBB.getBB().getSharedCounters();
   long numCreate = counters.read(TxBB.REMOTE_CREATE);
   long numCreateIsLoad = counters.read(TxBB.REMOTE_CREATE_ISLOAD);
   long numUpdate = counters.read(TxBB.REMOTE_UPDATE);
   long numUpdateIsLoad = counters.read(TxBB.REMOTE_UPDATE_ISLOAD);
   long numDestroy = counters.read(TxBB.REMOTE_DESTROY);
   long numInval = counters.read(TxBB.REMOTE_INVALIDATE);
   long numLocalDestroy = counters.read(TxBB.REMOTE_LOCAL_DESTROY);
   long numLocalInval = counters.read(TxBB.REMOTE_LOCAL_INVALIDATE);

   // allow for conflated entry create/destroy which is seen simply as a destroy
   // in Tx VM (on commit only)
   long numConflatedCreateDestroy = counters.read(TxBB.CONFLATED_CREATE_DESTROY);

   // allow for entry-creates which are cancelled out by a region-destroy
   // Note that these cause a CommitConflict/FailedCommit
   long numCreateInDestroyedRegion = counters.read(TxBB.CREATE_IN_DESTROYED_REGION);
   long numCreateInDestroyedRegion_isLoad = counters.read(TxBB.CREATE_IN_DESTROYED_REGION_ISLOAD);

   int numVmsWithList = TestHelper.getNumVMs();
   if (this.isBridgeClient) {     // for client/server configuration, TxListener only fired in server vms
       numVmsWithList = BridgeHelper.getEndpoints().size();
   } else if (this.isProxy) {  // See BUG 45556, events only fired in datahosts
       numVmsWithList = TestHelper.getNumVMs()-1;
   }
   if (bb.equals(TxWriterCountersBB.getBB())) {   // TransactionWriter
     numVmsWithList = 1;    // Writer only fired in txVm (distributed regions)
   } 

   String endResult = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
   // No result to check, try again later
   if (endResult == null) {
     return;
   }

   // If the tx fails or is rolled back, the remote listeners (cache & tx)
   // won't get any events, so take this into consideration via numVmsWithList.
   // The TransactionWriter won't be invoked on Rollback, so we should expect
   // no events.
   if (!endResult.equals(TxBB.TXACTION_COMMIT)) {
     if (bb.equals(TxWriterCountersBB.getBB())) {   // TransactionWriter
       // If rollback, no invocation/events
       // If the TransactionWriter aborted, we should have checked TxEvents
       if (endResult.equals(TxBB.TXACTION_ROLLBACK)) {
         numCreate = 0;
         numCreateIsLoad = 0;
         numUpdate = 0;
         numUpdateIsLoad = 0;
         numDestroy = 0;
         numInval = 0;
         numLocalDestroy = 0;
         numLocalInval = 0;
         numConflatedCreateDestroy = 0;
         numCreateInDestroyedRegion = 0;
         numCreateInDestroyedRegion_isLoad = 0;
       }
     } else {  // TransactionListener
       numVmsWithList = 1;
       Log.getLogWriter().info("Commit failed or was rolled back, remote TxListeners will not be included in counts, numVmsWithList = " + numVmsWithList);
       // don't count conflated create/destroy (we won't see it in rollback) unless we are using DataPolicy.EMPTY
       if (!this.isProxy) {
         numConflatedCreateDestroy = 0;
       }
     }
   }

   Log.getLogWriter().info("num VMs with listener installed: " + numVmsWithList);
   // verify TX Listener Counters for callback invocation (afterCommit, afterFailedCommit, afterRollback)
   if (bb.equals(TxEventCountersBB.getBB())) {
      verifyCallbackInvocation(numVmsWithList);
   }

   ArrayList al = new ArrayList();
        // afterCreate counters
           al.add(new ExpCounterValue("numCreateTxEvents_isDist", (((numCreate+numCreateIsLoad) * numVmsWithList)+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad)));
           al.add(new ExpCounterValue("numCreateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotExp", (((numCreate+numCreateIsLoad) * numVmsWithList)+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad))); 
           if (this.isBridgeClient || this.isProxy) {  // TxEvents only in servers/datahosts, all remote to initiating client
             al.add(new ExpCounterValue("numCreateTxEvents_isRemote", ((numCreate+numCreateIsLoad) * numVmsWithList)));
             al.add(new ExpCounterValue("numCreateTxEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numCreateTxEvents_isRemote", ((numCreate+numCreateIsLoad) * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numCreateTxEvents_isNotRemote", ((numCreate+numCreateIsLoad)+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad)));
           }
           al.add(new ExpCounterValue("numCreateTxEvents_isLoad", ((numCreateIsLoad * numVmsWithList)+numCreateInDestroyedRegion_isLoad))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotLoad", ((numCreate * numVmsWithList)+numCreateInDestroyedRegion)));
           al.add(new ExpCounterValue("numCreateTxEvents_isLocalLoad", ((numCreateIsLoad * numVmsWithList)+numCreateInDestroyedRegion_isLoad))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotLocalLoad", ((numCreate * numVmsWithList)+numCreateInDestroyedRegion)));
           al.add(new ExpCounterValue("numCreateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotNetLoad", (((numCreate+numCreateIsLoad) * numVmsWithList)+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotNetSearch", (((numCreate+numCreateIsLoad) * numVmsWithList))+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad)); 

        // afterDestroy counters
           al.add(new ExpCounterValue("numDestroyTxEvents_isDist", (((numDestroy * numVmsWithList))+numConflatedCreateDestroy)));
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotExp", ((numDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           if (this.isBridgeClient || this.isProxy) {  // TxEvents only in servers/datahosts, all remote to initiating client
             al.add(new ExpCounterValue("numDestroyTxEvents_isRemote", (numDestroy * numVmsWithList)));
             al.add(new ExpCounterValue("numDestroyTxEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numDestroyTxEvents_isRemote", (numDestroy * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numDestroyTxEvents_isNotRemote", (numDestroy+numConflatedCreateDestroy)));
           }
           al.add(new ExpCounterValue("numDestroyTxEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotLoad", ((numDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotLocalLoad", ((numDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetLoad", ((numDestroy * numVmsWithList)+numConflatedCreateDestroy))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetSearch", ((numDestroy * numVmsWithList)+numConflatedCreateDestroy))); 

        // afterInvalidate counters
           al.add(new ExpCounterValue("numInvalidateTxEvents_isDist", (numInval * numVmsWithList)));
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotExp", (numInval * numVmsWithList))); 
           if (this.isBridgeClient || this.isProxy) {  // TxEvents only in servers/datahosts, all remote to initiating client
             al.add(new ExpCounterValue("numInvalidateTxEvents_isRemote", (numInval * numVmsWithList)));
             al.add(new ExpCounterValue("numInvalidateTxEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numInvalidateTxEvents_isRemote", (numInval * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numInvalidateTxEvents_isNotRemote", numInval));
           }
           al.add(new ExpCounterValue("numInvalidateTxEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotLoad", (numInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotLocalLoad", (numInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotNetLoad", (numInval * numVmsWithList))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotNetSearch", (numInval * numVmsWithList))); 

        // afterUpdate counters
           al.add(new ExpCounterValue("numUpdateTxEvents_isDist", ((numUpdate+numUpdateIsLoad) * numVmsWithList)));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotExp", ((numUpdate+numUpdateIsLoad) * numVmsWithList))); 
           if (this.isBridgeClient || this.isProxy) {  // TxEvents only in servers/datahosts, all remote to initiating client
             al.add(new ExpCounterValue("numUpdateTxEvents_isRemote", ((numUpdate+numUpdateIsLoad) * numVmsWithList)));
             al.add(new ExpCounterValue("numUpdateTxEvents_isNotRemote", 0));
           } else {
             al.add(new ExpCounterValue("numUpdateTxEvents_isRemote", ((numUpdate+numUpdateIsLoad) * (numVmsWithList - 1))));
             al.add(new ExpCounterValue("numUpdateTxEvents_isNotRemote", (numUpdate+numUpdateIsLoad)));
           }
           al.add(new ExpCounterValue("numUpdateTxEvents_isLoad", (numUpdateIsLoad * numVmsWithList)));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotLoad", (numUpdate * numVmsWithList))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isLocalLoad", (numUpdateIsLoad * numVmsWithList)));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotLocalLoad", (numUpdate * numVmsWithList))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetLoad", ((numUpdate+numUpdateIsLoad) * numVmsWithList))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetSearch", ((numUpdate+numUpdateIsLoad) * numVmsWithList))); 

   if (bb instanceof TxEventCountersBB) {
      ((TxEventCountersBB)bb).checkEventCounters(al);
   } else {
      ((TxWriterCountersBB)bb).checkEventCounters(al);
   }
   
}

// Local class used to provide a Runnable to the product via
// internal apis.  
static final class CommitTestCallback {
  public static void run() {
    synchronized(ViewTest.killSyncObject) {
      Log.getLogWriter().info("product invoked CommitTestCallback, killSyncObject.notify() ...");
      ViewTest.killSyncObject.notify();
    }
    // prevent the commit process from continuing until the VM stops
    // We're going to do a MEAN_KILL, so it shouldn't take long
    // In the case of distIntegrityFD (splitBrain), we need to wait for
    // FD Detection (memberTimeout(15) + ackSevereAlertThreshold (10))
    MasterController.sleepForMs(30000);
  }
}

public void setupCommitTestCallbacks(String commitStatePrm) {

  if (this.isProxy && TxPrms.killRemoteTxVm()) {
     setupRemoteCallbacks(commitStatePrm);
  } else {

    // select the commitStatus to trigger the VM kill
    // callbacks are setup via the product TXState class
    //TXStateProxy txStateProxy = TxHelper.getTxState();
    //TXStateInterface txState = ((TXStateProxyImpl)txStateProxy).getRealDeal(null,null);
    TXStateInterface txState = TxHelper.getTxState();
  
    int commitStateTrigger = TxPrms.getCommitStateTrigger(commitStatePrm);
    // Store on BB for CloseTasks to use (determines isVisible setting)
    TxBB.getBB().getSharedMap().put(TxBB.CommitStateTrigger, new Integer(commitStateTrigger));
    Log.getLogWriter().info("TxPrms.commitStateTrigger = " + commitStatePrm + "(" + commitStateTrigger + ")");
  
    // establish the appropriate trigger point
    // based on this trigger point, 
    switch (commitStateTrigger) {
      case TxPrms.CommitState_afterReservation:
        throw new TestException("TODO: Not valid with new model; remove");
  
      case TxPrms.CommitState_afterConflictCheck:
        throw new TestException("TODO: Not valid with new model; remove");
  
      case TxPrms.CommitState_beforeSend:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void beforeSend(TXStateProxy tx, boolean rollback) {
            CommitTestCallback.run();
          }
        });
        break;

      case TxPrms.CommitState_duringIndividualSend:
        throw new TestException("TODO: Not valid with new model; remove");
  
      case TxPrms.CommitState_afterIndividualSend:
        throw new TestException("TODO: Not valid with new model; remove");
  
      case TxPrms.CommitState_duringIndividualCommit:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void duringIndividualCommit(TXStateProxy tx, Object callbackArg) {
            CommitTestCallback.run();
          }
        });
        break;
      case TxPrms.CommitState_afterIndividualCommit:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterIndividualCommit(TXStateProxy tx, Object callbackArg) {
            CommitTestCallback.run();
          }
        });
        break;

      case TxPrms.CommitState_afterApplyChanges:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterApplyChanges(TXStateProxy tx) {
            CommitTestCallback.run();
          }
        });
        break;

      case TxPrms.CommitState_afterReleaseLocalLocks:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterReleaseLocalLocks(TXStateProxy tx) {
            CommitTestCallback.run();
          }
        });
        break;

      case TxPrms.CommitState_afterSend:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterSend(TXStateProxy tx, boolean rollback) {
            CommitTestCallback.run();
          }
        });
        break;

      // kill local tx thread when targeting remote entries (TXStateProxy) on commit
      case TxPrms.CommitState_afterSendCommit:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterSend(TXStateProxy tx, boolean rollback) {
            if (!rollback) {
              CommitTestCallback.run();
            }
          }
        });
        break;

      // kill local tx thread when targeting remote entries (TXStateProxy) on rollback
      case TxPrms.CommitState_afterSendRollback:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterSend(TXStateProxy tx, boolean rollback) {
            if (rollback) {
              CommitTestCallback.run();
            }
          }
        });
        break;
  
      default:
        throw new TestException("Unexpected commitStateTrigger " +  TxPrms.toStringForCommitStateTrigger(commitStateTrigger));
    } // end switch 
  }
}

/** 
 *  kill the VM if we are the transactional thread and a commit
 *  is in Progress (see TxBB.commitInProgress)
 */
public static void HydraTask_killCommittor() throws ClientVmNotFoundException {
   Log.getLogWriter().info("In killCommittor ...");

   synchronized(ViewTest.killSyncObject) {
     try {
       ViewTest.killSyncObject.wait();
     } catch (InterruptedException e) {
       Log.getLogWriter().info("killCommittor interrupted, did not kill VM");
     }
   }

   Boolean timeToStop = (Boolean)TxBB.getBB().getSharedMap().get(TxBB.timeToStop);
   int stopMode = ClientVmMgr.MEAN_KILL;

   // BUG 43538 
   // - afterIndividualCommit trigger needs to use a NICE_KILL (to allow socket buffer flush)
   String commitStatePrm = TestConfig.tab().stringAt(TxPrms.commitStateTrigger, null);
   if (commitStatePrm != null) {
      if (TxPrms.getCommitStateTrigger(commitStatePrm) == TxPrms.CommitState_afterIndividualCommit) {
         stopMode = ClientVmMgr.NICE_KILL;
      }
   }

   if (timeToStop.equals(Boolean.FALSE)) {
     // this is the VM we intend to kill via the callback
     ClientVmMgr.stop(
       "product invoked CommitTestCallback, killing comittor",
       stopMode,  
       ClientVmMgr.NEVER       // never allow this VM to restart
     );
   } else {
     Log.getLogWriter().info("killCommittor invoked, signalled by waitForDist to allow test to continue normally.  Not killing this VM");
   }
}

/**
 * Task for threads whose sole purpose is to wait for commit distribution
 * Basically sleeping ...
 */
public static void HydraTask_waitForDist() {
  MasterController.sleepForMs(60000);

  // support killCommit threads in multiple VMs (some that won't be killed by the commitTestCallbacks)
  Boolean timeToStop = (Boolean)TxBB.getBB().getSharedMap().get(TxBB.timeToStop);
  if (timeToStop.equals(Boolean.TRUE)) {
    // let killCommittor thread know that its time to give up 
    synchronized(ViewTest.killSyncObject) {
      Log.getLogWriter().info("waitForDist signalling killCommittor to stop waiting, commit completed");
      ViewTest.killSyncObject.notify();
    }
  }
}

/**
 *  Hydra CLOSETASK to ensure that we can access the same entries
 *  that were accessed in the original transaction (to ensure all
 *  locking resources were released).
 */
public static void HydraCloseTask_verifyResourcesReleased() {
  Log.getLogWriter().info("HydraCloseTask_verifyResourcesReleased(TX_NUMBER = " + TxBB.getBB().getSharedCounters().read(TxBB.TX_NUMBER) + ")" );
  viewTest.verifyResourcesReleased();
}

/**
 *  Hydra CLOSETASK to verify that resources (e.g. locks) have been
 *  released after a client which was committing a transaction is killed.
 *  This cannot be invoked in the case of the distributed commit succeeding
 *  as we likely can't execute the exact operation again.  Either check this
 *  case, or provide functions to randomly create these operations with a 
 *  given Region & Key.
 */
protected void verifyResourcesReleased() {
  String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
  OpList opList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
  OpList txOpList = opList.collapse();

  Log.getLogWriter().info("Entered verifyResourcesReleased, reusing operations from " + txOpList.toString());

  boolean isVisible = updatesVisible();

  CollectionsTest validator = new CollectionsTest();
  for (int i = 0; i < txOpList.numOps(); i++) {
    Operation op = txOpList.getOperation(i);
    Log.getLogWriter().info("Executing operation " + op);

    // beginTx
    TxHelper.begin();

    // If isVisible, everyone should have received & updated so we can't
    // execute the same opList.  
    //
    // To simplify actions on the same keyset,
    // Modify any entry-destroys => create; change everything else to an update. 
    if (isVisible) {
      if (op.isEntryDestroy()) {
        op.setOpName(Operation.ENTRY_CREATE);
      } else if (op.isEntryInvalidate()) {
        op.setOpName(Operation.ENTRY_UPDATE);
      } else {
        op.setOpName(Operation.ENTRY_UPDATE);
      }
      op.setNewValue(new Integer(0));  // Integer modVal
    }

    // op = entryDestroy => entryCreate
    // else do an update
    validator.executeOperation(op);

    try {
      TxHelper.commit();
    } catch (ConflictException e) {
      throw new TestException("Unexpected ConflictException " + TestHelper.getStackTrace(e));
    }

  }
}

/**
 *  Hydra CLOSETASK to verify that either all or no VMs received 
 *  the distribution of a transaction (even though the tx vm was 
 *  killed mid-way through the operation.  Either everyone has all
 *  the updates or no one does!
 */
public static void HydraCloseTask_validateTxConsistency() {
  Log.getLogWriter().info("HydraCloseTask_validateTxConsistency(TX_NUMBER = " + TxBB.getBB().getSharedCounters().read(TxBB.TX_NUMBER) + ")" );
  // Hold off and allow distributions to occur
  viewTest.validateTxConsistency();
}

  protected void validateTxConsistency() {
    Log.getLogWriter().info("Entered validateTxConsistency(TX_NUMBER = " + TxBB.getBB().getSharedCounters().read(TxBB.TX_NUMBER) + ")" );
  
    try {
      validateTxOps(updatesVisible());
    } catch (TestException e) {
      Log.getLogWriter().info("Listing current values of each entry involved in the commit");
      String opListKey = (String)TxBB.getBB().getSharedMap().get(TxBB.TX_OPLIST_KEY);
      OpList txOpList = (OpList)TxBB.getBB().getSharedMap().get(opListKey);
      for (int i = 0; i < txOpList.numOps(); i++) {
        Operation op = txOpList.getOperation(i);
        String regionName = op.getRegionName();
        Region aRegion = CacheUtil.getCache().getRegion(regionName);
        String key = (String)op.getKey();
        Log.getLogWriter().info("Operation(" + i + ") = " + op.toString());
        Log.getLogWriter().info("Current value of <" + regionName + "><" + key + "> = <" + getValueInVM(aRegion, key) + ">");
      }
      throw e;
    }
  }

  /** Depending on the commitStateTrigger (the point at which the committing
   *  VM was killed), should we be able to see the tx updates?
   *
   *  @returns - true => updates should be visible
   *
   *  Note that this implementation covers replicated regions.
   *  ParRegViewTest overrides this for PR specific behavior.
   */
  protected boolean updatesVisible() {
    boolean isVisible = false;
    Integer mapValue = (Integer)TxBB.getBB().getSharedMap().get(TxBB.CommitStateTrigger);
    int commitStateTrigger = mapValue.intValue();
    Boolean listenerProcessedCommit = (Boolean)TxBB.getBB().getSharedMap().get(TxBB.afterCommitProcessed);

    if (((commitStateTrigger == TxPrms.CommitState_afterSend) ||
        (commitStateTrigger == TxPrms.CommitState_afterReleaseLocalLocks) ||
        (commitStateTrigger == TxPrms.CommitState_afterIndividualCommit)) ||
        listenerProcessedCommit) {
      isVisible = true;
    }
    Log.getLogWriter().info("updatesVisible(" + TxPrms.toStringForCommitStateTrigger(commitStateTrigger) + ") returning " + isVisible);
    return isVisible;
  }

  /** Call getValueInVM on the given region.
   *
   *  @param aRegion - The region to use for the call.
   *  @param key - The key to use for the call.
   *
   *  @returns The value in the VM of key.
   */
  public static Object getValueInVM(Region aRegion, Object key) {
     Object value = null;
     Region.Entry entry = aRegion.getEntry(key);
     if (entry != null) {
       value = entry.getValue();
       if (value == null) {    // keyExists with null value => INVALID
         value = Token.INVALID;
       }
     }
     return value;
  }

  /** For empty peers, transactions are remoted to another member 
   *  which has data.   (See BUG 45556 for more information).
   */
  private void setupRemoteCallbacks(String commitStatePrm) {
  
    // callbacks are setup via the product TXState class
    TXStateInterface txState = TxHelper.getTxState();
    TransactionId txId = TxHelper.getTransactionId();
    Set targetDMs = txState.getProxy().getFinishMessageRecipientsForTest();
    Log.getLogWriter().info("Invoking setupRemoteTestCallbacks with " + txId + " and trigger " + commitStatePrm + " on " + targetDMs);
  
    DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
  
    //Function f = FunctionService.getFunction("parReg.tx.SetupRemoteTestCallbacks");
    Function f = new parReg.tx.SetupRemoteTestCallbacks();
  
    // build args ArrayList (op: key, value)
    ArrayList aList = new ArrayList();
    aList.add(txId);
    aList.add(commitStatePrm);
    Execution e = FunctionService.onMembers(targetDMs).withArgs(aList);
  
    Log.getLogWriter().info("executing " + f.getId() + " on members " + targetDMs); 
    ResultCollector rc = e.execute(f);
    Log.getLogWriter().info("executed " + f.getId());
  
    Object result = rc.getResult();
  }
}


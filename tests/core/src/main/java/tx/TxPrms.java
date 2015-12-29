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

public class TxPrms extends BasePrms {

  /**
   *  Name of region to exclude from TxUtil (getRandomRegions, doOperations, etc).
   *  This is useful when extending existing tests and an additional region is
   *  needed for other purposes.  This prevents the 'new' region from 
   *  interfering with the original test code.
   */
  public static Long excludeRegionName;

  //---------------------------------------------------------------------------
  // TransactionListener
  //--------------------------------------------------------------------------- 
  /**
   *  Class name of transaction listener to use.  Defaults to null.
   */
  public static Long txListener;
  public static TransactionListener getTxListener() {
    Long key = txListener;
    String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
    try {
      return (TransactionListener)instantiate( key, val );
    } catch( ClassCastException e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement TransactionListener", e );
    }
  }                                                                                
  //---------------------------------------------------------------------------
  // TransactionWriter
  //--------------------------------------------------------------------------- 
  /**
   *  Class name of transaction writer to use.  Defaults to null.
   */
  public static Long txWriter;
  public static TransactionWriter getTxWriter() {
    Long key = txWriter;
    String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
    try {
      return (TransactionWriter)instantiate( key, val );
    } catch( ClassCastException e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement TransactionWriter", e );
    }
  }                                                                                
    /** (Vector of Strings) A list of the operations on a region OR region entries
     *  that this test is allowed to do.  Can be one or more of:
     *     entry-create - create a new key/value in a region.
     *     entry-update - update an entry in a region with a new value.
     *     entry-destroy - destroy an entry in a region.
     *     entry-localDestroy - local destroy of an entry in a region.
     *     entry-inval - invalidate an entry in a region.
     *     entry-localInval - local invalidate of an entry in a region.
     *     entry-get - get the value of an entry.
     *     
     *     region-create - create a new region.
     *     region-destroy - destroy a region.
     *     region-localDestroy - locally destroy a region.
     *     region-inval - invalidate a region.
     *     region-localInval - locally invalidate a region.
     *     
     *     cache-close - close the cache.
     */
    public static Long operations;
    
    /** (boolean) isPartitionedRegion
     *  For peer tests, true indicates that the peer client is using partitioned regions
     *  For client/server tests, true indicates that the server is hosting a partitioned region
     *  Defaults to false.
     */
    public static Long isPartitionedRegion;
    public static boolean isPartitionedRegion() {
      Long key = isPartitionedRegion;
      return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
    }
    /** (boolean) A task attribute to control whether the doOperations() method 
     *  performs random operations (not part of a transaction).  This allows us 
     *  Used in ViewTests with a non-tx thread performing operations.  In some 
     *  tests, these are performed in the same VM with the tx thread -- in 
     *  other tests, the non-tx operations are performed in a different VM.
     */
    public static Long executeNonTxOperations;
    public static boolean getExecuteNonTxOperations() {
      Long key = executeNonTxOperations;
      return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
    }

    /** (boolean) Boolean to indicate if event counters (CacheListener & TxListener)
     *  are to be maintained & validated.  To be used with distributed View Tests.
     *  Defaults to false.
     */
    public static Long checkEventCounters;
    public static boolean checkEventCounters() {
      Long key = checkEventCounters;
      return tasktab().booleanAt( key, tab().booleanAt( key, false ) );
    }

    /** (boolean) Boolean to indicate if event counters (CacheListener & TxListener)
     *  are to be maintained & validated.  To be used with distributed View Tests.
     *  Defaults to true.
     */
    public static Long checkTxEventOrder;
    public static boolean checkTxEventOrder() {
      Long key = checkTxEventOrder;
      return tasktab().booleanAt( key, tab().booleanAt( key, true) );
    }

    /** (int) The number of operations to do when TxUtil.doOperations() is called.
     */
    public static Long numOps;

    /** 
     *  (int) 
     *  Percentage of tasks that run in a transaction.
     */
    public static Long tasksInTxPercentage;
    public static int getTasksInTxPercentage() {
      Long key = tasksInTxPercentage;
      int val = tasktab().intAt( key, tab().intAt( key, 100 ) );
      if ( val < 0 || val > 100 ) {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
      return val;
    }

    /** 
     *  (int) 
     *  Percentage of transactions to commit (vs. rollback)
     */
    public static Long commitPercentage;
    public static int getCommitPercentage() {
      Long key = commitPercentage;
      int val = tasktab().intAt( key, tab().intAt( key, 100 ) );
      if ( val < 0 || val > 100 ) {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
      return val;
    }

    /** 
     *  (int) 
     *  Percentage of operations that are region operations (ie region inval, destroy)
     *  Note that this is not honored for calling TxUtil.doOperations.
     */
    public static Long regionOpPercentage;
    public static int getRegionOpPercentage() {
      Long key = regionOpPercentage;
      int val = tasktab().intAt( key, tab().intAt( key, 100 ) );
      if ( val < 0 || val > 100 ) {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
      }
      return val;
    }

    /** 
     *  (int) 
     *  Number of entries to create (per region) initially
     */
    public static Long maxKeys;

    /**
     *  (int)
     *  number of root regions to generate
     */
    public static Long numRootRegions;
                                                                                
    /**
     *  (int)
     *  number of subregions to generate for each region
     */
    public static Long numSubRegions;
                                                                                
    /**
     *  (int)
     *  depth of each region tree
     */
    public static Long regionDepth;

    /**
     *  (String)
     *  How the test should handle making copies of objects for updates.
     *  Can be one of:
     *     useCopyHelper - use the CopyHelper class for copies on updates
     *     useCopyOnRead - use the copyOnRead setting on instance of Cache.
     */
    public static Long updateStrategy;
    public static final String USE_COPY_HELPER = "useCopyHelper";
    public static final String USE_COPY_ON_READ = "useCopyOnRead";

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------
                                                                               
  /** Get a valid value for the update strategy */
  public static String getUpdateStrategy() {
     String strategy = TestConfig.tab().stringAt(updateStrategy); 
     if ((!strategy.equalsIgnoreCase(USE_COPY_HELPER)) &&
         (!strategy.equalsIgnoreCase(USE_COPY_ON_READ)))
        throw new TestException("Unknown TxPrms.updateStrategy " + strategy);
     return strategy;
  }

  /** If defined, get the commitStatus to trigger a VM kill */
  public static Long commitStateTrigger;
  public static final int CommitState_afterReservation          = 0;
  public static final int CommitState_afterConflictCheck        = 1;
  public static final int CommitState_beforeSend                = 2;
  public static final int CommitState_duringIndividualSend      = 3;
  public static final int CommitState_afterIndividualSend       = 4;
  public static final int CommitState_duringIndividualCommit    = 5;
  public static final int CommitState_afterIndividualCommit     = 6;
  public static final int CommitState_afterApplyChanges         = 7;
  public static final int CommitState_afterReleaseLocalLocks    = 8;
  public static final int CommitState_afterSend                 = 9;
  // following callback trigger points target the local TXStateStub when
  // remoting tx to (nonlocal) dataStore
  public static final int CommitState_afterSendCommit           = 10;
  public static final int CommitState_afterSendRollback         = 11;

  static final String AFTER_RESERVATION         = "afterReservation";
  static final String AFTER_CONFLICT_CHECK      = "afterConflictCheck";
  static final String BEFORE_SEND               = "beforeSend";
  static final String AFTER_APPLY_CHANGES       = "afterApplyChanges";
  static final String AFTER_RELEASE_LOCAL_LOCKS = "afterReleaseLocalLocks";
  static final String DURING_INDIVIDUAL_SEND    = "duringIndividualSend";
  static final String AFTER_INDIVIDUAL_SEND     = "afterIndividualSend";
  static final String DURING_INDIVIDUAL_COMMIT  = "duringIndividualCommit";
  static final String AFTER_INDIVIDUAL_COMMIT   = "afterIndividualCommit";
  static final String AFTER_SEND                = "afterSend";
  static final String AFTER_SEND_COMMIT         = "afterSendCommit";
  static final String AFTER_SEND_ROLLBACK       = "afterSendRollback";
  
  public static int getCommitStateTrigger(String commitStatePrm) {
    int commitStateTrigger = -1;

    if (commitStatePrm.equalsIgnoreCase(AFTER_RESERVATION)) {
      commitStateTrigger = CommitState_afterReservation;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_CONFLICT_CHECK)) {
      commitStateTrigger = CommitState_afterConflictCheck;
    } else if (commitStatePrm.equalsIgnoreCase(BEFORE_SEND)) {
      commitStateTrigger = CommitState_beforeSend;
    } else if (commitStatePrm.equalsIgnoreCase(DURING_INDIVIDUAL_SEND)) {
      commitStateTrigger = CommitState_duringIndividualSend;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_INDIVIDUAL_SEND)) {
      commitStateTrigger = CommitState_afterIndividualSend;
    } else if (commitStatePrm.equalsIgnoreCase(DURING_INDIVIDUAL_COMMIT)) {
      commitStateTrigger = CommitState_duringIndividualCommit;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_INDIVIDUAL_COMMIT)) {
      commitStateTrigger = CommitState_afterIndividualCommit;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_APPLY_CHANGES)) {
      commitStateTrigger = CommitState_afterApplyChanges;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_RELEASE_LOCAL_LOCKS)) {
      commitStateTrigger = CommitState_afterReleaseLocalLocks;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_SEND)) {
      commitStateTrigger = CommitState_afterSend;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_SEND_COMMIT)) {
      commitStateTrigger = CommitState_afterSendCommit;
    } else if (commitStatePrm.equalsIgnoreCase(AFTER_SEND_ROLLBACK)) {
      commitStateTrigger = CommitState_afterSendRollback;
    } else {
        throw new TestException("Unknown TxPrms.commitStateTrigger " + commitStateTrigger);
    }
    return commitStateTrigger;
  }

  public static String toStringForCommitStateTrigger(int commitStateTrigger) {
    String s = null;

    switch (commitStateTrigger) {
      case (CommitState_afterReservation):
        s = AFTER_RESERVATION;
        break;
      case (CommitState_afterConflictCheck):
        s = AFTER_CONFLICT_CHECK;
        break;
       case (CommitState_beforeSend):
        s = BEFORE_SEND;
        break;
      case (CommitState_duringIndividualSend):
        s = DURING_INDIVIDUAL_SEND;
        break;
      case (CommitState_afterIndividualSend):
        s= AFTER_INDIVIDUAL_SEND;
        break;
      case (CommitState_duringIndividualCommit):
        s = DURING_INDIVIDUAL_COMMIT;
        break;
      case (CommitState_afterIndividualCommit):
        s = AFTER_INDIVIDUAL_COMMIT;
        break;
      case (CommitState_afterApplyChanges):
        s = AFTER_APPLY_CHANGES;
        break;
      case (CommitState_afterReleaseLocalLocks):
        s = AFTER_RELEASE_LOCAL_LOCKS;
        break;
      case (CommitState_afterSend):
        s = AFTER_SEND;
        break;
      case (CommitState_afterSendCommit):
        s = AFTER_SEND_COMMIT;
        break;
      case (CommitState_afterSendRollback):
        s = AFTER_SEND_ROLLBACK;
        break;
      default:
        s = "Invalid commitStateTrigger (" + commitStateTrigger + ")";
    }
    return s;
  }

  /** (boolean) Which VM to kill during proxyDistIntegrity tests.
   *  This defaults to false as normally (with replicated regions), we kill the initiator of the transaction.
   *  With 7.0, empty peers now "remote their transactions to members hosting data, so we will need to kill
   *  the remote (transactional) vm for most proxyDistIntegrity tests.
   *
   *  However, there are two callbacks (afterSendCommit, afterSendRollback) which are executed on the
   *  empty peer.  For these tests (proxyDistIntegrityKillTxVm.conf) we want to kill the empty peer.
   */
  public static Long killRemoteTxVm;
  public static boolean killRemoteTxVm() {
     Long key = killRemoteTxVm;
     boolean val = tab().booleanAt(key, false);
     return val;
  }
  
  private static Object instantiate( Long key, String classname ) {
    if ( classname == null ) {
      return null;
    }
    try {
      Class cls = Class.forName( classname );
      return cls.newInstance();
    } catch( Exception e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": cannot instantiate " + classname, e );
    }
  }

/** (String) Indicates the dataPolicy attributes of the 3 distributed caches
 *  that the View Tests will use for its regions. Can be any combination of
 *  proxy and cached dataPolicies.  This string contains 3 dataPolicies 
 *  strings separated by hyphens '-'.
 */
public static Long viewDataPolicies;

// ================================================================================
static {
   BasePrms.setValues(TxPrms.class);
}

}

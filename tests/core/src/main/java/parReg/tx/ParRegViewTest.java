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
   
package parReg.tx;

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.*;

import hydra.*;
import hydra.blackboard.*;
import util.*;
import tx.*;

/**
 * A version of <code>ViewTest</code> that performs operations 
 * and checks event counters for PartitionedRegions.
 *
 * Overrides checkEventCounters, checkTxEventCounters, etc from 
 * tx/ViewTest.
 *
 * @see parRegSerialView.conf
 */

public class ParRegViewTest extends tx.ViewTest {

  static int numVmsWithListeners = 1;  // default case is cache_content (primary only)
  static int numEdgeClients = 0;  // default is for p2p tests
  static boolean useLocalKeySet;

  public synchronized static void HydraTask_initialize() {
    if (viewTest == null) {
      viewTest = new ParRegViewTest();
      viewTest.initialize();
      ((ParRegViewTest)viewTest).initialize(ConfigPrms.getRegionConfig());
    }
  }

  public void initialize(String regionConfig) {

    // establish numVmsWithListeners for checkEventCounters
    //   cache_content = 1 (primary)
    //   gemfire.BucketRegion.alwaysFireLocalListeners = redundantCopies + 1
    StringBuffer aStr = new StringBuffer();
    boolean alwaysFireLocalListeners = Boolean.getBoolean("gemfire.BucketRegion.alwaysFireLocalListeners");
    aStr.append("alwaysFireLocalListeners= " + alwaysFireLocalListeners + " ");
    PrTxBB.getBB().getSharedMap().put(PrTxBB.alwaysFireLocalListeners, new Boolean(alwaysFireLocalListeners));

    RegionAttributes ratts = RegionHelper.getRegionAttributes(regionConfig);
    PartitionAttributes patts = ratts.getPartitionAttributes();
    int redundantCopies = patts.getRedundantCopies();
    aStr.append("redundantCopies = " + redundantCopies + " ");
    PrTxBB.getBB().getSharedMap().put(PrTxBB.redundantCopies, new Integer(redundantCopies));
    if (alwaysFireLocalListeners) {   
      numVmsWithListeners = redundantCopies + 1;   // primary + copies
      aStr.append("calculated numVmsWithListeners = " + numVmsWithListeners + " ");
    }

    // consider edge clients for client/server tests (e.g. prBridgeSerialView)
    int totalVMs = TestConfig.getInstance().getTotalVMs();
    int bridgeVMs = BridgeHelper.getEndpoints().size();
    if (bridgeVMs > 0)  {
      // don't want to set this for peer tests
      numEdgeClients = totalVMs - bridgeVMs;
    }
    Log.getLogWriter().info("numEdgeClients = " + numEdgeClients + " numVmsWithListeners = " + numVmsWithListeners);
    
    // useLocalKeySet Prms sets expectation for isRemote events
    useLocalKeySet = PrTxPrms.useLocalKeySet();
    aStr.append("useLocalKeySet = " + useLocalKeySet);
    Log.getLogWriter().info(aStr.toString());

    TxBB.getBB().getSharedMap().put(TxBB.afterCommitProcessed, new Boolean(false));
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
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numBeforeCreateEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeCreateEvents_isNotRemote", (localCreate+localCreateIsLoad)));
        } else {
          al.add(new ExpCounterValue("numBeforeCreateEvents_isRemote", (localCreate + localCreateIsLoad)));
          al.add(new ExpCounterValue("numBeforeCreateEvents_isNotRemote", 0));
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
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotRemote", localDestroy));
        } else {
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isRemote", localDestroy));
          al.add(new ExpCounterValue("numBeforeDestroyEvents_isNotRemote", 0));
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
        if (useLocalKeySet) {
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isRemote", 0));
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotRemote", (localUpdate+localUpdateIsLoad)));
        } else {
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isRemote", (localUpdate+localUpdateIsLoad)));
          al.add(new ExpCounterValue("numBeforeUpdateEvents_isNotRemote", 0));
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

   // for client/server tests, expect CacheListener events in the edgeClients as well
   int numVmsWithList = numVmsWithListeners + numEdgeClients;
   Log.getLogWriter().info("expect callbacks from " + numVmsWithList + " VMs based on numVmsWithListeners(" + numVmsWithListeners + ") + numEdgeClients(" + numEdgeClients + ")");

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
           al.add(new ExpCounterValue("numAfterCreateEvents_isRemote", "numAfterCreateEvents_isNotRemote", ((remoteCreate+remoteCreateIsLoad) * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterCreateEvents_isLoad", (remoteCreateIsLoad * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterCreateEvents_isNotLoad", (remoteCreate * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterCreateEvents_isLocalLoad", (remoteCreateIsLoad*numVmsWithList)));
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
           al.add(new ExpCounterValue("numAfterDestroyEvents_isRemote", "numAfterDestroyEvents_isNotRemote", ((remoteDestroy * numVmsWithList)+numConflatedCreateDestroy)));
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
           al.add(new ExpCounterValue("numAfterInvalidateEvents_isRemote", "numAfterInvalidateEvents_isNotRemote", (remoteInval * numVmsWithList)));
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
           al.add(new ExpCounterValue("numAfterUpdateEvents_isRemote", "numAfterUpdateEvents_isNotRemote", ((remoteUpdate+remoteUpdateIsLoad) * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isLoad", (remoteUpdateIsLoad * numVmsWithList))); 
           al.add(new ExpCounterValue("numAfterUpdateEvents_isNotLoad", (remoteUpdate * numVmsWithList)));
           al.add(new ExpCounterValue("numAfterUpdateEvents_isLocalLoad", (remoteUpdateIsLoad*numVmsWithList)));
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

   // assume that we're dealing with TxListener event counters
   // note that in client/server tests, these events are only fired in servers
   int numTxCallbacks = numVmsWithListeners; 
   // for TransactionWriters, the callbacks are only invoked in 1 VM (the dataStore VM)
   if (bb.equals(TxWriterCountersBB.getBB())) {
      numTxCallbacks = 1;
   }
   String endResult = (String)TxBB.getBB().getSharedMap().get(TxBB.COMPLETION_ACTION);
   // No result to check, try again later
   if (endResult == null) {
     return;
   }

   // If the tx fails or is rolled back, the remote listeners (cache & tx)
   // won't get any events, so take this into consideration via numTxCallbacks.
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
       numTxCallbacks = 1;
       Log.getLogWriter().info("Commit failed or was rolled back, remote TxListeners will not be included in counts, numTxCallbacks = " + numTxCallbacks);
       // don't count conflated create/destroy (we won't see it in rollback) unless we are using DataPolicy.EMPTY
       if (!this.isProxy) {
         numConflatedCreateDestroy = 0;
       }
     }
   }

   Log.getLogWriter().info("expect callback invocations from " + numTxCallbacks + " VMs");
   // verify TX Listener Counters for callback invocation (afterCommit, afterFailedCommit, afterRollback)
   if (bb.equals(TxEventCountersBB.getBB())) {
      verifyCallbackInvocation(numTxCallbacks);
   }

   ArrayList al = new ArrayList();
        // afterCreate counters
           al.add(new ExpCounterValue("numCreateTxEvents_isDist", (((numCreate+numCreateIsLoad) * numTxCallbacks)+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad)));
           al.add(new ExpCounterValue("numCreateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotExp", (((numCreate+numCreateIsLoad) * numTxCallbacks)+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isRemote", "numCreateTxEvents_isNotRemote", (((numCreate+numCreateIsLoad) * numTxCallbacks)+numCreateInDestroyedRegion + numCreateInDestroyedRegion_isLoad)));
           al.add(new ExpCounterValue("numCreateTxEvents_isLoad", ((numCreateIsLoad * numTxCallbacks)+numCreateInDestroyedRegion_isLoad))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotLoad", ((numCreate * numTxCallbacks)+numCreateInDestroyedRegion)));
           al.add(new ExpCounterValue("numCreateTxEvents_isLocalLoad", ((numCreateIsLoad * numTxCallbacks)+numCreateInDestroyedRegion_isLoad))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotLocalLoad", ((numCreate * numTxCallbacks)+numCreateInDestroyedRegion)));
           al.add(new ExpCounterValue("numCreateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotNetLoad", (((numCreate+numCreateIsLoad) * numTxCallbacks)+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad))); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numCreateTxEvents_isNotNetSearch", (((numCreate+numCreateIsLoad) * numTxCallbacks))+numCreateInDestroyedRegion+numCreateInDestroyedRegion_isLoad)); 

        // afterDestroy counters
           al.add(new ExpCounterValue("numDestroyTxEvents_isDist", ((numDestroy * numTxCallbacks))));
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotExp", (numDestroy * numTxCallbacks))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isRemote", "numDestroyTxEvents_isNotRemote", (numDestroy * numTxCallbacks)));
           al.add(new ExpCounterValue("numDestroyTxEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotLoad", (numDestroy * numTxCallbacks))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotLocalLoad", (numDestroy * numTxCallbacks))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetLoad", (numDestroy * numTxCallbacks))); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numDestroyTxEvents_isNotNetSearch", (numDestroy * numTxCallbacks))); 

        // afterInvalidate counters
           al.add(new ExpCounterValue("numInvalidateTxEvents_isDist", (numInval * numTxCallbacks)));
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotExp", (numInval * numTxCallbacks))); 
             al.add(new ExpCounterValue("numInvalidateTxEvents_isRemote", "numInvalidateTxEvents_isNotRemote", (numInval * numTxCallbacks)));
           al.add(new ExpCounterValue("numInvalidateTxEvents_isLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotLoad", (numInval * numTxCallbacks))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isLocalLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotLocalLoad", (numInval * numTxCallbacks))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotNetLoad", (numInval * numTxCallbacks))); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numInvalidateTxEvents_isNotNetSearch", (numInval * numTxCallbacks))); 

        // afterUpdate counters
           al.add(new ExpCounterValue("numUpdateTxEvents_isDist", ((numUpdate+numUpdateIsLoad) * numTxCallbacks)));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotDist", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isExp", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotExp", ((numUpdate+numUpdateIsLoad) * numTxCallbacks))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isRemote", "numUpdateTxEvents_isNotRemote", ((numUpdate+numUpdateIsLoad) * numTxCallbacks)));
           al.add(new ExpCounterValue("numUpdateTxEvents_isLoad", (numUpdateIsLoad * numTxCallbacks)));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotLoad", (numUpdate * numTxCallbacks))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isLocalLoad", (numUpdateIsLoad * numTxCallbacks)));
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotLocalLoad", (numUpdate * numTxCallbacks))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNetLoad", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetLoad", ((numUpdate+numUpdateIsLoad) * numTxCallbacks))); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNetSearch", 0)); 
           al.add(new ExpCounterValue("numUpdateTxEvents_isNotNetSearch", ((numUpdate+numUpdateIsLoad) * numTxCallbacks))); 

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
      Log.getLogWriter().info(
          "product invoked CommitTestCallback, killSyncObject.notify() ...");
      ViewTest.killSyncObject.notify();
    }
    // prevent the commit process from continuing until the VM stops
    // We're going to do a MEAN_KILL, so it shouldn't take long
    // In the case of distIntegrityFD (splitBrain), we need to wait for
    // FD Detection (memberTimeout(15) + ackSevereAlertThreshold (10))
    MasterController.sleepForMs(30000);
  }
}

//======================================================================
// override ViewTest version (suitable for replicated regions and local partitionRegion entries only)
//======================================================================
@Override
public void setupCommitTestCallbacks(String commitStatePrm) {

  // callbacks are setup via the product TXState class
  if (!PrTxPrms.killLocalTxVm()) {
     setupRemoteCallbacks(commitStatePrm);
  } else {

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
          public void duringIndividualCommit(TXStateProxy tx,
              Object callbackArg) {
            CommitTestCallback.run();
          }
        });
        break;
  
      case TxPrms.CommitState_afterIndividualCommit:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterIndividualCommit(TXStateProxy tx,
              Object callbackArg) {
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

      // kill local tx thread when targeting remote entries (TXStateStub) on commit
      case TxPrms.CommitState_afterSendCommit:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterSend(TXStateProxy tx, boolean rollback) {
            CommitTestCallback.run();
          }
        });
        break;

      // kill local tx thread when targeting remote entries (TXStateStub) on rollback
      case TxPrms.CommitState_afterSendRollback:
        txState.setObserver(new TransactionObserverAdapter() {
          @Override
          public void afterSend(TXStateProxy tx, boolean rollback) {
            CommitTestCallback.run();
          }
        });
        break;
  
      default:
        throw new TestException("Unexpected commitStateTrigger " +  TxPrms.toStringForCommitStateTrigger(commitStateTrigger));
    } // end switch 
  }
}
  
private void setupRemoteCallbacks(String commitStatePrm) {

  // callbacks are setup via the product TXState class
  TXStateInterface txState = TxHelper.getTxState();
  TransactionId txId = TxHelper.getTransactionId();
  Log.getLogWriter().info("Invoking setupRemoteTestCallbacks with " + txId + " and trigger " + commitStatePrm);

  DistributedSystem ds = CacheHelper.getCache().getDistributedSystem();
  DistributedMember targetDM = (DistributedMember)PrTxBB.getBB().getSharedMap().get(PrTxBB.targetDM);

  Function f = FunctionService.getFunction("parReg.tx.SetupRemoteTestCallbacks");

  // build args ArrayList (op: key, value)
  ArrayList aList = new ArrayList();
  aList.add(txId);
  aList.add(commitStatePrm);
  Execution e = FunctionService.onMember(ds, targetDM).withArgs(aList);

  Log.getLogWriter().info("executing " + f.getId() + " on member " + targetDM); 
  ResultCollector rc = e.execute(f.getId());
  Log.getLogWriter().info("executed " + f.getId());

  Object result = rc.getResult();
}

  /** Depending on the commitStateTrigger (the point at which the committing
   *  VM was killed), should we be able to see the tx updates?
   *
   *  @returns - true => updates should be visible
   *
   *  Note that this implementation overrides the replicate region behavior
   *  from tx/ViewTest.java.
   */
  protected boolean updatesVisible() {
    Integer mapValue = (Integer)TxBB.getBB().getSharedMap().get(TxBB.CommitStateTrigger);
    int commitStateTrigger = mapValue.intValue();
    boolean isVisible = false;
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
}

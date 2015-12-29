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

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;

import hydra.*;

/**
 * A test to validate what different threads/VMs see in terms of
 * transactional state (when a tx is in progress) as well as non-tx
 * operations.
 *
 * This version is used in client/server distIntegrity tests.  Variants exist to kill
 * the tx edge client, the delegate or a non-delegate server.
 */

public class BridgeViewTest extends ViewTest {

  /*
   *  Initialize info for test (random number generator, isSerialExecution,
   *  pid of this VM and isScopeLocal (derived from regionAttributes).
   */
  public static void HydraTask_initialize() {
    Log.getLogWriter().info("In HydraTask_initialize");
    if (viewTest == null) {
      viewTest = new BridgeViewTest();
      viewTest.initialize();
    }
    TxBB.getBB().getSharedMap().put(TxBB.afterCommitProcessed, new Boolean(false));
  }

/* Install commitTestCallbacks in the local tx vm.  
 * The killCommittor, killDelegate or killNonDelgate thread will kill the killTarget VM
 * which is determined in this method and written to the BB.
 */
@Override
public void setupCommitTestCallbacks(String commitStatePrm) {

  // Local class used to provide a Runnable to the product via
  // internal apis.  
  class CommitTestCallback implements Runnable {
    public void run() {
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

  // select the commitStatus to trigger the VM kill
  int commitStateTrigger = TxPrms.getCommitStateTrigger(commitStatePrm);
  // Store on BB for CloseTasks to use (determines isVisible setting)
  TxBB.getBB().getSharedMap().put(TxBB.CommitStateTrigger, new Integer(commitStateTrigger));
  Log.getLogWriter().info("TxPrms.commitStateTrigger = " + commitStatePrm + "(" + commitStateTrigger + ")");

  // callbacks are setup via the product TXState class
    /* TODO: merge: below code for client TX
    TXStateInterface txStateProxy = TxHelper.getTxState();
    ClientTXStateStub txState = (ClientTXStateStub)((TXStateProxyImpl)txStateProxy)
        .getRealDeal(null, null);

  postClientVmInfoToBB();

  switch (commitStateTrigger) {
    // kill local tx thread when targeting remote entries (TXStateStub) on commit
    case TxPrms.CommitState_afterSendCommit:
      txState.setAfterSendCommit(new CommitTestCallback());
      break;

    // kill local tx thread when targeting remote entries (TXStateStub) on rollback
    case TxPrms.CommitState_afterSendRollback:
      txState.setAfterSendRollback(new CommitTestCallback());
      break;

    default:
      throw new TestException("Unexpected commitStateTrigger " +  TxPrms.toStringForCommitStateTrigger(commitStateTrigger));
  } // end switch 
  */
}

/**
 *  Write ClientVmInfo for delegate and another (nonDelegate) host to BB for future use
 *  with kill methods above.
 */
  protected void postClientVmInfoToBB() {
    // use test hooks to determine Endpoint of delegate
    PoolImpl pool = (PoolImpl)PoolManager.find("brloader");
    ServerLocation delegate  = pool.getServerAffinityLocation();
    int delegatePort = delegate.getPort();
    String delegateHost = delegate.getHostName();
    // be careful of fully qualified hostnames (xxx.gemstone.com)
    int dot = delegateHost.indexOf(".");
    if (dot != -1) {
      delegateHost = delegateHost.substring(0, dot);
    }
    Log.getLogWriter().info("Delegate is " + delegateHost + ":" + delegatePort);

    // use this information to get the Endpoint based ClientVmInfo and post to BB
    ClientVmInfo delegateVmInfo = null;
    ClientVmInfo nonDelegateVmInfo = null;

    List endpoints = BridgeHelper.getEndpoints();
    for (Iterator i = endpoints.iterator(); i.hasNext();) {
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)i.next();
      if ((endpoint.getHost().equalsIgnoreCase(delegateHost)) && (endpoint.getPort() == delegatePort)) {
        delegateVmInfo = new ClientVmInfo(endpoint);
      } else {
        nonDelegateVmInfo = new ClientVmInfo(endpoint);
      }
    }
    TxBB.getBB().getSharedMap().put(TxBB.delegate, delegateVmInfo);
    TxBB.getBB().getSharedMap().put(TxBB.nonDelegateServer, nonDelegateVmInfo);

    Log.getLogWriter().info("Writing delegateVmInfo to BB: " + delegateVmInfo);
    Log.getLogWriter().info("Writing nonDelegateVmInfo to BB: " + nonDelegateVmInfo);
  }

/** 
 *  kill the VM if we are the transactional thread and a commit
 *  is in Progress (see TxBB.commitInProgress)
 */
public static void HydraTask_killCommittor() throws ClientVmNotFoundException {
   Log.getLogWriter().info("In killCommittor ... killing tx edge client vm");

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
       "product invoked CommitTestCallback, killing committor",
       stopMode,  
       ClientVmMgr.NEVER       // never allow this VM to restart
     );
   } else {
     Log.getLogWriter().info("killCommittor invoked, signalled by waitForDist to allow test to continue normally.  Not killing this VM");
   }
}

/** 
 *  kill the delegate (killTarget on BB) if we are the transactional thread and a commit
 *  is in Progress (see TxBB.commitInProgress)
 */
public static void HydraTask_killDelegate() throws ClientVmNotFoundException {
   Log.getLogWriter().info("In killDelegate ... ");

   synchronized(ViewTest.killSyncObject) {
     try {
       ViewTest.killSyncObject.wait();
     } catch (InterruptedException e) {
       Log.getLogWriter().info("killCommittor interrupted, did not kill VM");
     }
   }
   
   Boolean timeToStop = (Boolean)TxBB.getBB().getSharedMap().get(TxBB.timeToStop);
   if (timeToStop.equals(Boolean.FALSE)) {
     // this is the VM we intend to kill via the callback
     ClientVmMgr.stop(
       "product invoked CommitTestCallback, killing committor",
       ClientVmMgr.MEAN_KILL,  // kill -TERM
       ClientVmMgr.NEVER,      // never allow this VM to restart
       (ClientVmInfo)TxBB.getBB().getSharedMap().get(TxBB.delegate)
     );
   } else {
     Log.getLogWriter().info("killCommittor invoked, signalled by waitForDist to allow test to continue normally.  Not killing this VM");
   }
}

/** 
 *  kill a non-delegate server if the VM if we are the transactional thread and a commit
 *  is in Progress (see TxBB.commitInProgress)
 */
public static void HydraTask_killNonDelegate() throws ClientVmNotFoundException {
   Log.getLogWriter().info("In killNonDelegate ...");

   synchronized(ViewTest.killSyncObject) {
     try {
       ViewTest.killSyncObject.wait();
     } catch (InterruptedException e) {
       Log.getLogWriter().info("killCommittor interrupted, did not kill VM");
     }
   }
   
   Boolean timeToStop = (Boolean)TxBB.getBB().getSharedMap().get(TxBB.timeToStop);
   if (timeToStop.equals(Boolean.FALSE)) {
     // this is the VM we intend to kill via the callback
     ClientVmMgr.stop(
       "product invoked CommitTestCallback, killing committor",
       ClientVmMgr.MEAN_KILL,  // kill -TERM
       ClientVmMgr.NEVER,      // never allow this VM to restart
       (ClientVmInfo)TxBB.getBB().getSharedMap().get(TxBB.nonDelegateServer)
     );
   } else {
     Log.getLogWriter().info("killCommittor invoked, signalled by waitForDist to allow test to continue normally.  Not killing this VM");
   }
}

}

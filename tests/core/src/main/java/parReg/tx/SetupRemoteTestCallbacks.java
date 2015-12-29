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
import com.gemstone.gemfire.internal.cache.*;

import hydra.*;
import util.*;
import tx.*;

public class SetupRemoteTestCallbacks implements Function, Declarable {

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
  
  public void execute(FunctionContext context) {

    ArrayList argumentList = (ArrayList) context.getArguments();
    TXId txId = (TXId)argumentList.get(0);
    String commitStatePrm = (String)argumentList.get(1);
    Log.getLogWriter().info("SetupRemoteTestCallbacks invoked on " + CacheHelper.getCache().getDistributedSystem().getDistributedMember() + " for " + txId + " with trigger " + commitStatePrm);

    // callbacks are setup via the product TXState class
    //TXStateProxy txStateProxy = ((TXManagerImpl)CacheHelper.getCache().getCacheTransactionManager()).getHostedTXStateForTest(txId);
    //TXState txState = (TXState)((TXStateProxyImpl)txStateProxy).getRealDeal(null,null);
    TXState txState = ((TXManagerImpl)CacheHelper.getCache().getCacheTransactionManager()).getHostedTXState(txId).getLocalTXState();
    int commitStateTrigger = TxPrms.getCommitStateTrigger(commitStatePrm);
    // Store on BB for CloseTasks to use (determines isVisible setting)
    TxBB.getBB().getSharedMap().put(TxBB.CommitStateTrigger, new Integer(commitStateTrigger));

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
  
      default:
        throw new TestException("Unexpected commitStateTrigger " +  TxPrms.toStringForCommitStateTrigger(commitStateTrigger));
    } // end switch 

    ResultSender rs = context.getResultSender();
    rs.lastResult(true);

  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties props) {
  }

  public boolean isHA() {
    return false;
  }
}

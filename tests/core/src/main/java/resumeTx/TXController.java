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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import hydra.*;
import hydra.blackboard.*;

import util.*;
import tx.*;

/** TXController
 *  A Function to execute a complete transaction (begin, ops, commit) without ever suspending.
 *  BB counters are used to coordinate activities with threads executing TryResume (with wait).
 *  
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class TXController implements Function, Declarable {
  
   public void execute(FunctionContext context){

      SharedCounters sc = ResumeTxBB.getBB().getSharedCounters();
      DistributedMember dm = CacheHelper.getCache().getDistributedSystem().getDistributedMember();
      // Arguments are an ArrayList with a clientIdString, number of threads executing tasks and TxInfo (with routingInfo)
      ArrayList argumentList = (ArrayList) context.getArguments();
      String forDM = (String)argumentList.get(0);
      int numThreads = ((Integer)argumentList.get(1)).intValue();
      TxInfo txInfo = (TxInfo)argumentList.get(2);
      boolean status = true;
    
      Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + dm + ", invoked from " + forDM + " and " + txInfo);

      TxHelper.begin();
      TransactionId txId = TxHelper.getTransactionId();
      txInfo.setTxId(txId);

      // put this on the BB for other threads to find tx info
      //Map activeTxns = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.ACTIVE_TXNS);
      Map activeTxnsWrapped = (Map)ResumeTxBB.getBB().getSharedMap().get(ResumeTxBB.ACTIVE_TXNS);
      try {
        Map activeTxns = RtxUtilVersionHelper.convertWrapperMapToActiveTxMap(activeTxnsWrapped);
        activeTxns.clear();
        activeTxns.put(txId, txInfo);
        ResumeTxBB.getBB().getSharedMap().put(ResumeTxBB.ACTIVE_TXNS, RtxUtilVersionHelper.convertActiveTxMapToWrapperMap(activeTxns));
      } catch (Exception e) {
        throw new TestException(e.toString() + e.getStackTrace().toString());
      } 
      // we have an active transaction now so signal other threads in TryResume function to 
      // get the transaction from the BB and try to resume it (with wait)
      sc.zero(ResumeTxBB.inTryResume);
      sc.increment(ResumeTxBB.readyToTryResume);
 
      // execute ops (via function execution) some random number of times
      int minExecutions =  TestConfig.tab().getRandGen().nextInt(10, ResumeTxPrms.getMinExecutions());
      for (int i=0; i < minExecutions; i++) {
            RtxUtil.doOperations();
      }
    
      // wait for all threads to have entered the TryResume Function
      TestHelper.waitForCounter(ResumeTxBB.getBB(), 
                                "ResumeTxBB.inTryResume",
                                ResumeTxBB.inTryResume,
                                numThreads - 1,
                                true,
                                5000);
   
      // since we know the ResumeTx threads are ready (inTryResume), we can go ahead and clear this counter
      sc.zero(ResumeTxBB.readyToTryResume);   

      long startTime = System.currentTimeMillis();
      // do something to free all the waiting threads
      // 0 - commit
      // 1 - rollback
      // 2 - suspend (then resume and commit)
      int action = TestConfig.tab().getRandGen().nextInt(0, 2);

      switch (action) {
         case 0:
            TxHelper.commit();
            break;
         case 1:
            TxHelper.rollback();
            break;
         case 2:
            TxHelper.suspend();
            break;
         default: 
            throw new TestException("Unknown action, expected commit(0), rollback(1) or suspendd(2)");
      }
   
      // wait for all threads to have exited the TryResume Function
      // waitForCounter throws an Exception if condition not met within time given (5 minutes)
      TestHelper.waitForCounter(ResumeTxBB.getBB(), 
                                "ResumeTxBB.inTryResume",
                                ResumeTxBB.inTryResume,
                                0,
                                true,
                                300000);

      if (action == 2) { // we need to clean this tx up -- go ahead and commit
         if (TxHelper.isSuspended(txId)) {
            TxHelper.resume(txId);
            TxHelper.commit();
         } else {
            throw new TestException("TXController unable to resume suspended transaction " + txId);
         }
      }
    
      long timeDelay = System.currentTimeMillis() - startTime;
      Log.getLogWriter().info("Time delay for all threads to return from tryResume(with wait) " + timeDelay + "ms");

      // return back whether or not ops were completed successfully (tx still active)
      Log.getLogWriter().info("TXController returning " + status);
      context.getResultSender().lastResult(new Boolean(status));
    }
    
    public String getId() {
       return this.getClass().getName();
    }

    public boolean hasResult() {
       return true;
    }

    public boolean optimizeForWrite() {
       return true;
    }

    public void init(Properties props) {
    }
 
    public boolean isHA() {
       return false;
    }
}   

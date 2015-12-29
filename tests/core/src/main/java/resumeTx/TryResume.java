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

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import hydra.*;

import util.*;
import tx.*;

/** TryResume
 *  A Function invoke tryResume on a given Transaction.
 *  This function is invoked with a TxInfo (containing TxId) in the argList.  It attempts to 
 *  resume the given transaction with a wait.
 *  
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class TryResume implements Function, Declarable {
  
  public void execute(FunctionContext context) {

    DistributedMember dm = CacheHelper.getCache().getDistributedSystem().getDistributedMember();

    // Arguments are an ArrayList with a clientIdString, TransactionId
    ArrayList argumentList = (ArrayList) context.getArguments();
    String forDM = (String)argumentList.get(0);
    TransactionId txId = (TransactionId)argumentList.get(1);
    
    Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + dm + ", invoked from " + forDM + " on txId " + txId);

    ResumeTxBB.getBB().getSharedCounters().increment(ResumeTxBB.inTryResume);
    boolean txResumed = false;
    if (TxHelper.tryResume(txId, 3600L, TimeUnit.SECONDS)) {
       Log.getLogWriter().fine("TryResume RESUMED " + txId);
       txResumed = true;
       // we likely got here because the TXController suspended the tx
       // we will need to do the same to let everyone else resume as well
       // TXController will wait for all threads to resume/suspend and then it will commit the tx
       TxHelper.suspend();
    } else {
       Log.getLogWriter().info("tryResume with wait failed, tx exists = " + TxHelper.exists(txId));
    }

    // return back whether or not ops were completed successfully (tx still active)
    Log.getLogWriter().info("TryResume returning " + txResumed +  " for txId = " + txId + 
 " exists = " + TxHelper.exists(txId));
    context.getResultSender().lastResult(new Boolean(txResumed));
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

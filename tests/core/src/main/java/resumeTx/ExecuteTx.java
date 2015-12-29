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

/** ExecuteTx
 *  A Function to do random operations as part of a resumeable transaction.
 *  This function is invoked with a TrannsactionId in the argList.  It resumes the
 *  given transaction id and then executes random operations (see TxUtil.doOperations) before
 *  suspending the transaction.
 *  
 * @author Lynn Hughes-Godfrey
 * @since 6.6
 */
public class ExecuteTx implements Function, Declarable {
  
  public void execute(FunctionContext context) {

    DistributedMember dm = CacheHelper.getCache().getDistributedSystem().getDistributedMember();
    boolean isHA = ResumeTxPrms.getHighAvailability();
    boolean isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);

    // Arguments are an ArrayList with clientIdString, TransactionId
    ArrayList argumentList = (ArrayList) context.getArguments();
    String forDM = (String)argumentList.get(0);
    TransactionId txId = (TransactionId)argumentList.get(1);
    
    Log.getLogWriter().info("executing " + this.getClass().getName() + " in member " + dm + ", invoked from " + forDM + " on txId " + txId);

    OpList opList = null;
    boolean executedOps = false;
    if (TxHelper.tryResume(txId, 10L, TimeUnit.SECONDS)) {
       Log.getLogWriter().fine("ExecuteTx RESUMED " + txId);
       try {
          opList = RtxUtil.doOperations();
          executedOps = true;
       } catch (TransactionDataNodeHasDepartedException e) {
          if (!isHA) {
            throw new TestException("Unexpected Exception " + e + " while executing tx ops on " + txId + ", " + TestHelper.getStackTrace(e));
          } else {
            Log.getLogWriter().info("Caught Exception " + e + ".  Expected with HA, continuing test.");  
            Log.getLogWriter().info("Rolling back transaction.");
            try {
              TxHelper.rollback();
              Log.getLogWriter().info("Done Rolling back Transaction");
            } catch (Exception te) {
              Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching TransactionDataNodeHasDeparted during tx ops.  Expected, continuing test.");
            } 
          }
       } catch (TransactionDataRebalancedException e) {
          if (!isHA) {
            throw new TestException("Unexpected Exception " + e + " while executing tx ops on " + txId + ", " + TestHelper.getStackTrace(e));
          } else {
            Log.getLogWriter().info("Caught Exception " + e + ".  Expected with HA, continuing test.");  
            Log.getLogWriter().info("Rolling back transaction.");
            try {
              TxHelper.rollback();
              Log.getLogWriter().info("Done Rolling back Transaction");
            } catch (Exception te) {
              Log.getLogWriter().info("Caught exception " + te + " on rollback() after catching Exception " + e + " during tx ops.  Expected, continuing test."); 
            } 
          }
       } catch (TestException e) {
          // TxUtil and RtxUtil can wrap the TransactionDataRebalancedExceptions, etc as TestExceptions
          if (isHA) {
            Log.getLogWriter().info("Caught Exception " + e + ".  Expected with HA, continuing test.");  
          } else {
            Log.getLogWriter().info("Unexpected Exception " + e + " caught while executing tx ops for " + txId + ", " + TestHelper.getStackTrace(e));
            throw e;
          }
       } finally {
          TxHelper.suspend();
       }
       if (isSerialExecution && executedOps) {
          ResumeTxBB.updateOpList(opList);
       }
    } else {
       Log.getLogWriter().fine(txId + " is not hosted in this VM, returning executedOps = " + executedOps);
    }
    // return back whether or not ops were completed successfully (tx still active)
    Log.getLogWriter().info("ExecuteTx returning " + executedOps +  " for txId = " + txId);
    context.getResultSender().lastResult(new Boolean(executedOps));
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

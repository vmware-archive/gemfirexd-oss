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
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.cache.DistributedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
/**
 * 
 * @author ymahajan
 *
 */
public final class DistributedRegionFunctionResultSender implements
    InternalResultSender {

  private final DistributedRegionFunctionStreamingMessage msg;

  private final DM dm;

  private final TXStateInterface txState;

  private ResultCollector rc;

  private boolean isLocal;

  private ServerToClientFunctionResultSender sender;
  
  private final Function functionObject;

  private boolean enableOrderedResultStreaming;

  private boolean localLastResultRecieved = false;

  /**
   * Have to combine next two construcotr in one and make a new class which will
   * send Results back.
   * 
   * @param msg
   * @param dm
   */
  public DistributedRegionFunctionResultSender(DM dm,
      DistributedRegionFunctionStreamingMessage msg, Function function) {
    this.msg = msg;
    this.dm = dm;
    this.txState = msg.getTXState();
    this.functionObject = function;
  }

  /**
   * Have to combine next two construcotr in one and make a new class which will
   * send Results back.
   *
   */
  public DistributedRegionFunctionResultSender(DM dm, TXStateInterface tx,
      ResultCollector rc, Function function,
      final ServerToClientFunctionResultSender sender) {
    this.msg = null;
    this.dm = dm;
    this.txState = tx;
    this.isLocal = true;  
    this.rc = rc;
    this.functionObject = function;
    this.sender = sender;
  }

  public final void lastResult(final Object oneResult) {
    lastResult(oneResult, true, true, true);
  }

  public final void lastResult(final Object oneResult,
      final boolean doTXFlush, final boolean sendTXChanges,
      final boolean finishTXRead) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    // flush any pending transactional operations
    if (doTXFlush) {
      flushTXPendingOps();
    }

    if (this.localLastResultRecieved){
      return;
    }
    this.localLastResultRecieved = true;
    if (this.sender != null) { // Client-Server
      sender.lastResult(oneResult);
      if(this.rc != null) {
        this.rc.endResults();
      }
    }
    else {
      if (isLocal) {
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        this.rc.endResults();
        FunctionStats.getFunctionStats(functionObject.getId(), this.dm.getSystem()).incResultsReceived();
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true,
              enableOrderedResultStreaming, sendTXChanges);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(),
              this.dm.getSystem()).incResultsReturned();
    }
  
  }
  
  public void lastResult(Object oneResult, DistributedMember memberID) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    this.localLastResultRecieved = true;
    if (this.sender != null) { // Client-Server
      sender.lastResult(oneResult, memberID);
      if (this.rc != null) {
        this.rc.endResults();
      }
    }
    else {
      if (isLocal) {
        this.rc.addResult(memberID, oneResult);
        this.rc.endResults();
        FunctionStats.getFunctionStats(functionObject.getId(),
            this.dm == null ? null : this.dm.getSystem()).incResultsReceived();
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, true,
              enableOrderedResultStreaming, true);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(),
          this.dm == null ? null : this.dm.getSystem()).incResultsReturned();
    }

  }

  public synchronized void sendResult(Object oneResult) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.sender != null) { // Client-Server
      sender.sendResult(oneResult);
    }
    else {
      if (isLocal) {
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(functionObject.getId(),
                this.dm.getSystem()).incResultsReceived();
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false,
              enableOrderedResultStreaming, false);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(),
              this.dm.getSystem()).incResultsReturned();
    }
  }
  
  public synchronized void sendResult(Object oneResult,
      DistributedMember memberID) {
    if (!this.functionObject.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.sender != null) { // Client-Server
      sender.sendResult(oneResult, memberID);
    }
    else {
      if (isLocal) {
        this.rc.addResult(memberID, oneResult);
        FunctionStats.getFunctionStats(functionObject.getId(),
            this.dm == null ? null : this.dm.getSystem()).incResultsReceived();
      }
      else {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false,
              enableOrderedResultStreaming, false);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      // incrementing result sent stats.
      FunctionStats.getFunctionStats(functionObject.getId(),
          this.dm == null ? null : this.dm.getSystem()).incResultsReturned();
    }
  }
  
  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(
        exception);
    this.lastResult(iFunxtionException);
    this.localLastResultRecieved = true;
  }
  
  public void setException(Throwable exception) {
    if (this.sender != null) {
      this.sender.setException(exception);
      //this.sender.lastResult(exception);
    }
    else {
      ((LocalResultCollector)this.rc).setException(exception);
      //this.lastResult(exception);
      this.dm.getLoggerI18n().severe(LocalizedStrings.DistributedRegionFunctionResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE,
              exception);
    }
    this.rc.endResults();
    this.localLastResultRecieved = true;
  }

  public void enableOrderedResultStreaming(boolean enable) {
    this.enableOrderedResultStreaming = enable;
  }

  public boolean isLocallyExecuted() {
    return this.msg == null;
  }

  public boolean isLastResultReceived() {
    return this.localLastResultRecieved ;
  }

  protected final void flushTXPendingOps() {
    final TXStateInterface tx = this.txState;
    if (tx != null) {
      // flush all batched TXStateProxy ops before sending lastResult
      tx.flushPendingOps(this.dm);
    }
    else {
      // check if there was a TX started within function body then wait for
      // commit before returning
      final TXManagerImpl.TXContext txContext = TXManagerImpl
          .currentTXContext();
      if (txContext != null) {
        txContext.waitForPendingCommit();
      }
    }
  }
}

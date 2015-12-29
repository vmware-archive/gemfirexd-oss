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
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.MemberFunctionStreamingMessage;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
/**
 * 
 * @author ymahajan
 *
 */
public final class MemberFunctionResultSender implements InternalResultSender {

  private final MemberFunctionStreamingMessage msg;

  private final DM dm;

  private final TXStateInterface txState;

  private ResultCollector rc;

  private final Function function;

  private boolean localLastResultRecieved = false;

  private boolean onlyLocal = false;

  private boolean onlyRemote = false;

  private boolean completelyDoneFromRemote = false;
  private boolean enableOrderedResultStreming;

  private ServerToClientFunctionResultSender serverSender;

  /**
   * Have to combine next two construcotr in one and make a new class which will 
   * send Results back.
   * @param msg
   * @param dm
   */
  public MemberFunctionResultSender(DM dm, 
      MemberFunctionStreamingMessage msg, Function function) {
    this.msg = msg;
    this.dm = dm;
    this.txState = msg.getTXState();
    this.function = function;
  }

  /**
   * Have to combine next two construcotr in one and make a new class which will
   * send Results back.
   * 
   * @param dm
   * @param rc
   */
  public MemberFunctionResultSender(DM dm, TXStateInterface tx,
      ResultCollector rc, Function function, boolean onlyLocal,
      boolean onlyRemote, ServerToClientFunctionResultSender sender) {
    this.msg = null;
    this.dm = dm;
    this.txState = tx;
    this.rc = rc;
    this.function = function;
    this.onlyLocal = onlyLocal;
    this.onlyRemote = onlyRemote;
    this.serverSender = sender;
  }

  public final void lastResult(final Object oneResult) {
    lastResult(oneResult, true, true, true);
  }

  public final void lastResult(final Object oneResult,
      final boolean doTXFlush, final boolean sendTXChanges,
      final boolean finishTXRead) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (doTXFlush) {
      // flush any pending transactional operations
      flushTXPendingOps();
    }

    if (this.serverSender != null) { // client-server
      if (this.localLastResultRecieved) {
        return;
      }
      if (onlyLocal) {
        this.serverSender.lastResult(oneResult);
        this.rc.endResults();
        this.localLastResultRecieved = true;
      }
      else {
        lastResult(oneResult, rc, false, true, this.dm.getId());
      }
    }
    else { // P2P
      if (this.msg != null) {
        try {
        this.msg.sendReplyForOneResult(dm, oneResult, true,
            enableOrderedResultStreming, sendTXChanges);
        }
        catch (QueryException e) {
          throw new FunctionException(e);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        if (this.localLastResultRecieved) {
          return;
        }
        if (onlyLocal) {
        this.rc.addResult(this.dm.getDistributionManagerId(), oneResult);
        this.rc.endResults();
        this.localLastResultRecieved = true;
      }
        else {
        //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, rc, false, true, this.dm
            .getDistributionManagerId());
        }
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
    }
    FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();    
  }

  private synchronized void lastResult(Object oneResult,
      ResultCollector collector, boolean lastRemoteResult,
      boolean lastLocalResult, DistributedMember memberID) {

    if(lastRemoteResult){
      this.completelyDoneFromRemote = true;
    }    
    if(lastLocalResult) {
      this.localLastResultRecieved = true;
      
    }
    if (this.serverSender != null) { // Client-Server
      if (this.completelyDoneFromRemote && this.localLastResultRecieved) {
        this.serverSender.lastResult(oneResult, memberID);
        collector.endResults();
      }
      else {
        this.serverSender.sendResult(oneResult, memberID);
      }
    }
    else { // P2P
      if (this.completelyDoneFromRemote && this.localLastResultRecieved) {
        collector.addResult(memberID, oneResult);
        collector.endResults();
      }
      else {
        collector.addResult(memberID, oneResult);
      }
    }
  }

  public void lastResult(Object oneResult, boolean completelyDone,
      ResultCollector reply, DistributedMember memberID) {
    if (this.serverSender != null) { // Client-Server
      if (completelyDone) {
        if (onlyRemote) {
          this.serverSender.lastResult(oneResult, memberID);
          reply.endResults();
        }
        else {
          lastResult(oneResult, reply, true, false, memberID);
        }
      }
      else {
        this.serverSender.sendResult(oneResult, memberID);
      }
    }
    else { // P2P
      if (completelyDone) {
        if (this.onlyRemote) {
          reply.addResult(memberID, oneResult);
          reply.endResults();
        }
        else {
        //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, reply, true, false, memberID);
        }
      }
      else {
        reply.addResult(memberID, oneResult);
      }
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
    }
    FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
  }
      
  public void sendResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.serverSender != null) { // Client-Server
      if(dm.getLoggerI18n().fineEnabled()){
        dm.getLoggerI18n().fine("MemberFunctionResultSender sending result from local node to client " + oneResult);
      }
      this.serverSender.sendResult(oneResult);
    }
    else { // P2P
      if (this.msg != null) {
        try {
          this.msg.sendReplyForOneResult(dm, oneResult, false,
              enableOrderedResultStreming, false);
        }
        catch (QueryException e) {
          throw new FunctionException(e);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        this.rc.addResult(this.dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReceived();
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(function.getId(), this.dm.getSystem()).incResultsReturned();
    }
  }
  
  
  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(
        exception);
    this.lastResult(iFunxtionException);
    this.localLastResultRecieved = true;
  }
  
  public void setException(Throwable exception) {
    ((LocalResultCollector)this.rc).setException(exception);
    //this.lastResult(exception);
    this.dm
        .getLoggerI18n()
        .severe(
            LocalizedStrings.MemberResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE,
            exception);
    this.rc.endResults();
    this.localLastResultRecieved = true;
  }

  public void enableOrderedResultStreaming(boolean enable) {
    this.enableOrderedResultStreming = enable;
  }

  public boolean isLocallyExecuted() {
    return this.msg == null;
  }

  public boolean isLastResultReceived() {
    return this.localLastResultRecieved;
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

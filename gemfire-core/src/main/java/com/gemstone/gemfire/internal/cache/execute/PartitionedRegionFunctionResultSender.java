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

import java.util.Set;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionedRegionFunctionStreamingMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * ResultSender needs ResultCollector in which to add results one by one.
 * In case of localExecution it just adds result to the resultCollector whereas for remote cases it 
 * takes help of PRFunctionExecutionStreamer to send results to the calling node. The results will be received 
 * in the ResultReciever.
 * ResultSender will be instantiated in executeOnDatastore and set in FunctionContext.
 * 
 * @author skumar
 */

public final class PartitionedRegionFunctionResultSender implements
    InternalResultSender {

  private final PartitionedRegionFunctionStreamingMessage msg;

  private final DM dm;

  private final TXStateInterface txState;

  private final PartitionedRegion pr;

  private final long time;

  private final boolean forwardExceptions;
  
  private ResultCollector rc;

  private ServerToClientFunctionResultSender serverSender;

  private boolean localLastResultRecieved = false;

  private boolean onlyLocal = false;

  private boolean onlyRemote = false;

  private boolean completelyDoneFromRemote = false;

  private final Function function;
  
  private LogWriterI18n logger = null ;

  private boolean enableOrderedResultStreming;

  private final Set<Integer> bucketSet;
  
  /**
   * Have to combine next two constructor in one and make a new class which will 
   * send Results back.
   * @param msg
   * @param dm
   * @param pr
   * @param time
   */
  public PartitionedRegionFunctionResultSender(DM dm, PartitionedRegion pr,
      long time, PartitionedRegionFunctionStreamingMessage msg,
      Function function, Set<Integer> bucketSet) {
    this.msg = msg;
    this.dm = dm;
    this.txState = msg.getTXState();
    this.pr = pr;
    this.time = time;
    this.function = function;
    this.logger = pr.getLogWriterI18n();
    this.bucketSet = bucketSet;
    
    forwardExceptions = false;
  }

  /**
   * Have to combine next two constructor in one and make a new class which will
   * send Results back.
   * @param dm
   * @param partitionedRegion
   * @param time
   * @param rc
   */
  public PartitionedRegionFunctionResultSender(DM dm, TXStateInterface tx,
      PartitionedRegion partitionedRegion, long time, ResultCollector rc,
      ServerToClientFunctionResultSender sender, boolean onlyLocal,
      boolean onlyRemote, boolean forwardExceptions, Function function,
      Set<Integer> bucketSet) {
    this.msg = null;
    this.dm = dm;
    this.txState = tx;
    this.pr = partitionedRegion;
    this.time = time;
    this.rc = rc;
    this.serverSender = sender;
    this.onlyLocal = onlyLocal;
    this.onlyRemote = onlyRemote;
    this.forwardExceptions = forwardExceptions;
    this.function = function;
    this.logger = pr.getLogWriterI18n();
    this.bucketSet = bucketSet;
  }

  public final void lastResult(final Object oneResult) {
    lastResult(oneResult, true, true, true);
  }

  public final void lastResult(final Object oneResult,
      final boolean doTXFlush, final boolean sendTXChanges,
      final boolean finishTXRead) {
    if (doTXFlush) {
      // flush any pending transactional operations
      flushTXPendingOps();
    }

    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    final int movedBucket;
    if (!(forwardExceptions && oneResult instanceof Throwable) 
        && (movedBucket = pr.getDataStore().areAllBucketsHosted(bucketSet,
            this.function.optimizeForWrite())) >= 0) {
      throw new BucketMovedException(
          LocalizedStrings.FunctionService_BUCKET_MIGRATED_TO_ANOTHER_NODE
              .toLocalizedString(),
          movedBucket, this.pr.getFullPath());
    }
    if (this.serverSender != null) { // Client-Server
      if(this.localLastResultRecieved){
        return;
      }
      if (onlyLocal) {
        lastClientSend(dm.getDistributionManagerId(), oneResult);
        this.rc.endResults();
        this.localLastResultRecieved = true;
      }
      else {
      //call a synchronized method as local node is also waiting to send lastResult 
        lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
      }
    }
    else { // P2P

      if (this.msg != null) {
        try {          
          this.msg.sendReplyForOneResult(dm, pr, time, oneResult, true,
              enableOrderedResultStreming, sendTXChanges);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        if(this.localLastResultRecieved){
          return;
        }
        if (onlyLocal) {
          this.rc.addResult(dm.getDistributionManagerId(), oneResult);
          this.rc.endResults();
          this.localLastResultRecieved = true;
        }
        else {
        //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, rc, false, true, dm.getDistributionManagerId());
        }
        FunctionStats.getFunctionStats(function.getId(),
            this.dm.getSystem()).incResultsReceived();
      }
      // incrementing result sent stats.
      // Bug : remote node as well as local node calls this method to send
      // the result When the remote nodes are added to the local result collector at that
      // time the stats for the result sent is again incremented : Once the PR team comes with the concept of the Streaming FunctionOperation
      // for the partitioned Region then it will be simple to fix this problem.
      FunctionStats.getFunctionStats(function.getId(),
          this.dm.getSystem()).incResultsReturned();
    }
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
        lastClientSend(memberID, oneResult);
        collector.endResults();
      }
      else {
        clientSend(oneResult, memberID);
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

    if(logger.fineEnabled()){
      logger.fine("PartitionedRegionFunctionResultSender Sending lastResult  " + oneResult);
    }

    if (this.serverSender != null) { // Client-Server
      
      if (completelyDone) {
        if (this.onlyRemote) {
          lastClientSend(memberID, oneResult);
          reply.endResults();
        }
        else {
          //call a synchronized method as local node is also waiting to send lastResult 
          lastResult(oneResult, reply, true, false, memberID);
        }
      }
      else {
        clientSend(oneResult, memberID);
      }
    }
    else{
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
      FunctionStats.getFunctionStats(function.getId(),
          this.dm == null ? null : this.dm.getSystem()).incResultsReceived();
    }
    FunctionStats.getFunctionStats(function.getId(),
        this.dm == null ? null : this.dm.getSystem()).incResultsReturned();
  }
      
  public void sendResult(Object oneResult) {
    if (!this.function.hasResult()) {
      throw new IllegalStateException(
          LocalizedStrings.ExecuteFunction_CANNOT_0_RESULTS_HASRESULT_FALSE
              .toLocalizedString("send"));
    }
    if (this.serverSender != null) {
      if(logger.fineEnabled()){
        logger.fine("PartitionedRegionFunctionResultSender sending result from local node to client " + oneResult);
      }
      clientSend(oneResult, dm.getDistributionManagerId());
    }
    else { // P2P
      if (this.msg != null) {
        try {
          if(logger.fineEnabled()){
            logger.fine("PartitionedRegionFunctionResultSender sending result from remote node " + oneResult);
          }
          this.msg.sendReplyForOneResult(dm, pr, time, oneResult, false,
              enableOrderedResultStreming, false);
        }
        catch (ForceReattemptException e) {
          throw new FunctionException(e);
        }
        catch (InterruptedException e) {
          throw new FunctionException(e);
        }
      }
      else {
        if(logger.fineEnabled()){
          logger.fine("PartitionedRegionFunctionResultSender adding result to ResultCollector on local node " + oneResult);
        }
        this.rc.addResult(dm.getDistributionManagerId(), oneResult);
        FunctionStats.getFunctionStats(function.getId(),
            this.dm.getSystem()).incResultsReceived();
      }
    //incrementing result sent stats.
      FunctionStats.getFunctionStats(function.getId(),
          this.dm.getSystem()).incResultsReturned();
    }
  }  
  
  private void clientSend(Object oneResult, DistributedMember memberID) {
    this.serverSender.sendResult(oneResult, memberID);
  }

  private void lastClientSend(DistributedMember memberID,
      Object lastResult) {
    this.serverSender.lastResult(lastResult, memberID);
  }

  public void sendException(Throwable exception) {
    InternalFunctionException iFunxtionException = new InternalFunctionException(
        exception);
    this.lastResult(iFunxtionException);
    this.localLastResultRecieved = true;
  }
  
  public void setException(Throwable exception) {
    if (this.serverSender != null) {
      this.serverSender.setException(exception);
    }
    else {
      ((LocalResultCollector)this.rc).setException(exception);
      this.dm
          .getLoggerI18n()
          .severe(
              LocalizedStrings.PartitionedRegionFunctionResultSender_UNEXPECTED_EXCEPTION_DURING_FUNCTION_EXECUTION_ON_LOCAL_NODE,
              exception);
    }
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
    return localLastResultRecieved;
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

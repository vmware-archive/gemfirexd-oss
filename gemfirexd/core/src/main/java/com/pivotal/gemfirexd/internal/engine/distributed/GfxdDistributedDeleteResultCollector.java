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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.
    InternalDistributedMember;
import com.gemstone.gnu.trove.TObjectIntHashMap;
import com.gemstone.gnu.trove.TObjectIntProcedure;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.
    ReferencedKeyCheckerMessage.ReferencedKeyReplyMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

/**
 * This collector provides support for distributed delete with referenced key
 * checking (currently only for DELETE RESTRICT).
 * 
 * The strategy to the above is:
 * 
 * a) Target nodes will first return the result of reference key checks that
 * will be done after the delete scan itself by deferring the actual delete in
 * <code>DeleteResultSet</code>.
 * 
 * b) After first set of replies from all nodes are in, this collector will send
 * a reply message to all the nodes waiting after reference key checks. The
 * nodes will wait on a ReplyProcessor21 to verify that reference key check was
 * successful on all the nodes (which is communicated by this node to stores).
 * 
 * c) The first set of replies from the nodes will have the processor ID on
 * which the node will wait for reply, so it will be stored in a map.
 * 
 * d) If an exception is received from any of the nodes (including node going
 * down), then send an exception type reply message to remaining nodes. Else
 * send a success type reply message to all nodes.
 * 
 * e) Subsequently on receiving the delete counts, add and return the final
 * result.
 * 
 * This allows us to do distributed referenced key checking efficiently in two
 * passes, with second pass already having the rows to be deleted on each data
 * store. For the case of transactions even the wait is not required and each
 * node can individually go ahead and do the deletes after reference key
 * checking since the TX is currently rolled back as a whole in case of
 * constraint violations. In future when savepoint support is added, then
 * transactions will rollback all changes done by the current statement
 * so no waiting after first phase will be required in any case with txns.
 * 
 * @author swale
 * @since 7.0
 */
public final class GfxdDistributedDeleteResultCollector extends
    GfxdQueryResultCollector {

  private static final long serialVersionUID = -284948494861032158L;

  private boolean hasReferencedKeys;

  private boolean waitForReferencedKeyResults;

  private volatile TObjectIntHashMap memberProcessorIds;

  private int numExpectedResults;

  private final AtomicInteger numDeletes;

  private GemFireTransaction tran;
  
  public GfxdDistributedDeleteResultCollector(final boolean hasReferencedKeys,
      final GemFireTransaction gtxn) {
    // we don't use super's array list
    super(0);
    this.hasReferencedKeys = hasReferencedKeys
        && (gtxn == null || !gtxn.isTransactional());
    this.tran = gtxn;
    this.numDeletes = new AtomicInteger(0);
  }

  @Override
  public void setProcessor(final ReplyProcessor21 processor) {
    if (GemFireXDUtils.TraceQuery) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, toString()
          + ": initial members: " + processor.membersToString());
    }
    //Fixes #43878 - start looking for member departures early.
    processor.startWait();
  }

  @Override
  public void setNumRecipients(final int n) {
    if (this.hasReferencedKeys) {
      if (n <= 1) {
        this.waitForReferencedKeyResults = false;
        this.memberProcessorIds = null;
      }
      else {
        this.waitForReferencedKeyResults = true;
        this.memberProcessorIds = new TObjectIntHashMap();
      }
    }
    else {
      this.waitForReferencedKeyResults = false;
      this.memberProcessorIds = null;
    }
    this.numExpectedResults = n;
    if (GemFireXDUtils.TraceQuery) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, toString()
          + ": expected number of results = " + n);
    }
  }

  @Override
  public void addResult(final DistributedMember member,
      final Object resultOfSingleExecution) {
    if (resultOfSingleExecution instanceof Integer) {
      if (GemFireXDUtils.TraceRSIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER, toString()
            + "#addResult: adding result=" + resultOfSingleExecution + '('
            + (this.memberProcessorIds != null ? "PROCESSORID" : "NUMDELETES")
            + ") from member: " + member);
      }
      final int v = ((Integer)resultOfSingleExecution).intValue();
      if (this.memberProcessorIds != null) {
        synchronized (this) {
          if (this.memberProcessorIds != null) {
            if (this.memberProcessorIds.putIfAbsent(member, v,
                Integer.MIN_VALUE) == Integer.MIN_VALUE) {
              signalDeletesAtEndOfReferenceKeyCheck();
            }
          }
          else {
            this.numDeletes.addAndGet(v);
          }
        }
      }
      else {
        this.numDeletes.addAndGet(v);
      }
      this.helper.addResultMember(member);
    }
    else {
      super.addResult(member, resultOfSingleExecution);
    }
  }

  @Override
  public Integer getResult() throws FunctionException, ReplyException {
    super.getResult();
    return Integer.valueOf(this.numDeletes.get());
  }

  @Override
  protected void addExceptionFromMember(Throwable t,
      final DistributedMember member) {
    super.addExceptionFromMember(t, member);
    if (this.memberProcessorIds != null) {
      //Fix for 43877 - make sure we don't double count
      //crashed members that also send a reply.
      //indicate that the number failed. -1 is not a valid reply processor
      //id - see ProcessorKeeeper21.put.
      this.memberProcessorIds.put(member, -1);
      signalDeletesAtEndOfReferenceKeyCheck();
      if (GemFireXDUtils.TraceRSIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER, toString()
            + "#addExceptionFromMember: added -1 expected processorId "
            + "due to exception " + t + " from member " + member);
      }
    }
    else {
      if (GemFireXDUtils.TraceRSIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER, toString()
            + "#addExceptionFromMember: received exception " + t
            + " from member " + member + " waitForReferencedKeyResults="
            + this.waitForReferencedKeyResults);
      }
    }
  }

  @Override
  protected void clearData() {
    super.clearData();
    if (this.memberProcessorIds != null) {
      this.memberProcessorIds = new TObjectIntHashMap();
    }
    this.numExpectedResults = 0;
    this.numDeletes.set(0);
  }

  private void signalDeletesAtEndOfReferenceKeyCheck() {
    assert Thread.holdsLock(this): "expected synchronized on 'this'";

    if (this.memberProcessorIds.size() >= this.numExpectedResults) {
      if (GemFireXDUtils.TraceQuery) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, toString()
            + ": signalling delete at end of reference " + "check for "
            + this.memberProcessorIds.toString());
      }
      // Misc will return a DS will non-null DM
      final DM dm = Misc.getDistributedSystem().getDM();
      final boolean success = (this.functionException == null);
      // got all results; send the replies to all waiting stores
      this.memberProcessorIds.forEachEntry(new TObjectIntProcedure() {
        @Override
        public final boolean execute(final Object m, final int procId) {
          if (procId != -1) {
            if (GemFireXDUtils.TraceQuery) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB,
                  toString() + ": sending reference check end to " + m);
            }
            ReferencedKeyReplyMessage.send((InternalDistributedMember)m,
                procId, dm, success);
          }
          return true;
        }
      });
      this.memberProcessorIds = null;
    }
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public GfxdResultCollector<Object> cloneCollector() {
    return new GfxdDistributedDeleteResultCollector(this.hasReferencedKeys, this.tran);
  }
}

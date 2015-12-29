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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.util.concurrent.StoppableCountDownLatch;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

/**
 * {@link ResultCollector} used for query executions in GemFireXD. For
 * streaming result support use {@link GfxdQueryStreamingResultCollector}.
 * 
 * @author swale
 */
public class GfxdQueryResultCollector extends ArrayList<Object> implements
    GfxdResultCollector<Object> {

  private static final long serialVersionUID = 8434972705144037747L;

  protected final CancelCriterion stopper;

  protected volatile StoppableCountDownLatch latch;

  protected volatile FunctionException functionException;

  protected final GfxdResultCollectorHelper helper;

  public GfxdQueryResultCollector() {
    this(8);
  }

  public GfxdQueryResultCollector(int initialCapacity) {
    this(initialCapacity, new GfxdResultCollectorHelper());
  }

  private GfxdQueryResultCollector(int initialCapacity,
      GfxdResultCollectorHelper helper) {
    super(initialCapacity);
    this.stopper = Misc.getGemFireCache().getCancelCriterion();
    this.latch = new StoppableCountDownLatch(this.stopper, 1);
    this.helper = helper;
  }

  public final void setResultMembers(Set<DistributedMember> members) {
    this.helper.setResultMembers(members);
  }

  public final Set<DistributedMember> getResultMembers() {
    return this.helper.getResultMembers();
  }

  public final boolean setupContainersToClose(
      Collection<GemFireContainer> containers, GemFireTransaction tran)
      throws StandardException {
    // non-streaming collector will not have anything to do with releasing locks
    return false;
  }

  public void setNumRecipients(int n) {
  }

  public GfxdResultCollectorHelper getStreamingHelper() {
    return null;
  }

  public void setProcessor(ReplyProcessor21 processor) {
    // nothing to be done for non-streaming collector
  }

  public ReplyProcessor21 getProcessor() {
    // not required to be implemented for non-streaming collector
    return null;
  }

  /**
   * Adds a single function execution result from a remote node to the
   * ResultCollector
   * 
   * @param resultOfSingleExecution
   */
  // !!ezoerner added argument member, but not yet used in implementation
  // [sumedh] now using member argument
  public void addResult(DistributedMember member,
      Object resultOfSingleExecution) {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER, toString()
          + "#addResult: adding result: " + resultOfSingleExecution
          + " from member: " + member);
    }
    if (resultOfSingleExecution == null) {
      return;
    }
    boolean addMember = true;
    Throwable t = null;
    if (resultOfSingleExecution instanceof Throwable) {
      t = (Throwable)resultOfSingleExecution;
      addException(t, member);
      // exceptions can be fatal ones like low memory exception, so never
      // add the member as having sent a valid reply (#44762)
      addMember = false;
    }
    else if (resultOfSingleExecution instanceof ResultHolder) {
      final ResultHolder rh = (ResultHolder)resultOfSingleExecution;
      // apply TX changes for ResultHolder
      rh.applyRemoteTXChanges(member);
      t = rh.getException();
      if (t != null) {
        addException(t, member);
        addMember = false;
      }
    }
    if (addMember) {
      this.helper.addResultMember(member);
    }
    if (t == null) {
      synchronized (this) {
        final long count = this.latch.getCount();
        if (count != 1) {
          SanityManager.THROWASSERT("unexpected latch count=" + count);
        }
        add(resultOfSingleExecution);
      }
    }
  }

  /**
   * Waits if necessary for the computation to complete, and then retrieves its
   * result.<br>
   * If {@link Function#hasResult()} is false, upon calling
   * {@link ResultCollector#getResult()} throws {@link FunctionException}.
   * 
   * @return the computed result
   * @throws FunctionException
   *           if something goes wrong while retrieving the result
   */
  public Object getResult() throws FunctionException, ReplyException {
    try {
      this.latch.await();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      this.stopper.checkCancelInProgress(ie);
    }
    if (this.functionException != null) {
      throw this.functionException;
    }
    return this;
  }

  /**
   * Call back provided to caller, which is called after function execution is
   * complete and caller can retrieve results using
   * {@link ResultCollector#getResult()}
   * 
   */
  public final void endResults() {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER, toString()
          + "#endResults: ending results");
    }
    this.latch.countDown();
  }

  /**
   * Waits if necessary for at most the given time for the computation to
   * complete, and then retrieves its result, if available. <br>
   * If {@link Function#hasResult()} is false, upon calling
   * {@link ResultCollector#getResult()} throws {@link FunctionException}.
   * 
   * @param timeout
   *          the maximum time to wait
   * @param unit
   *          the time unit of the timeout argument
   * @return computed result
   * @throws FunctionException
   *           if something goes wrong while retrieving the result
   */
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException {
    throw new FunctionException(
        "getResult with timeout not expected to be invoked in GemFireXD");
  }

  public void setException(Throwable exception) {
    addException(exception, Misc.getGemFireCache().getMyId());
  }

  public final void clearResults() {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER, toString()
          + "#clearResults: resetting the latch and clearing results");
    }
    synchronized (this) {
      clearData();
    }
  }

  protected void clearData() {
    this.latch = new StoppableCountDownLatch(this.stopper, 1);
    this.functionException = null;
    if (GemFireXDUtils.isOffHeapEnabled()) {
      Iterator<Object> resultsIter = this.iterator();
      while (resultsIter.hasNext()) {
        Object obj = resultsIter.next();
        if (obj instanceof ResultHolder) {
          OffHeapReleaseUtil.freeOffHeapReference((ResultHolder) obj);
        }
        resultsIter.remove();
      }
    } else {
      this.clear();
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + '@'
        + Integer.toHexString(System.identityHashCode(this));
  }

  protected final void addException(Throwable t, DistributedMember member) {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER, toString()
          + "#processException: from member [" + member + "] got exception", t);
    }
    StandardException.fixUpRemoteException(t, member);
    synchronized (this) {
      addExceptionFromMember(t, member);
    }
  }

  protected void addExceptionFromMember(Throwable t,
      final DistributedMember member) {
    if (this.functionException == null) {
      if (t instanceof FunctionException) {
        this.functionException = (FunctionException)t;
        if (t.getCause() != null) {
          t = t.getCause();
        }
      }
      else {
        this.functionException = new FunctionException(t);
      }
    }
    this.functionException.addException(t);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GfxdResultCollector<Object> cloneCollector() {
    return new GfxdQueryResultCollector(8);
  }
}

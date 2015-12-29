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
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * A simple list {@link ResultCollector} that just adds the received results to
 * a list used by GemFireXD.
 * 
 * @author swale
 */
public class GfxdListResultCollector extends ArrayList<Object> implements
    ResultCollector<Object, Object> {

  private static final long serialVersionUID = -9058302896893928570L;
  
  private final Object ignoreToken;
  
  private final boolean getMemberInformation;
  
  protected boolean throwException = false;
  
  public GfxdListResultCollector() {
    this.ignoreToken = null;
    this.getMemberInformation = false;
  }

  public GfxdListResultCollector(Object ignoreToken,
      boolean memberInformation) {
    this.ignoreToken = ignoreToken;
    this.getMemberInformation = memberInformation;
  }

  public void addResult(DistributedMember memberID,
      Object resultOfSingleExecution) {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "ResultCollector received result from member "
              + memberID + ": " + (resultOfSingleExecution == null
                  ? "null" : resultOfSingleExecution));
    }
    if (this.ignoreToken != null
        && this.ignoreToken.equals(resultOfSingleExecution)) {
      return;
    }
    if (GemFireXDUtils.TraceFunctionException) {
      if (resultOfSingleExecution instanceof Throwable) {
        throwException = true;
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_FUNCTION_EX,
            "ResultCollector received unexpected throwable in addResult "
                + "from member " + memberID,
                (Throwable)resultOfSingleExecution);
      }
    }

    if (this.getMemberInformation) {
      /* Note:
       * @ see GemFireResultSet#getNextFromGetAll()
       * Though an overhead, but its once each member only since remote
       * calls only lastResult. 
       * @see GetAllExecutorMessage.execute()
       * If remote starts calling sendResult, this may need change.
       */
      add(new ListResultCollectorValue(resultOfSingleExecution, memberID));
    }
    else {
      add(resultOfSingleExecution);
    }
  }

  public ArrayList<Object> getResult() throws FunctionException {
    if (GemFireXDUtils.TraceFunctionException) {
      if (this.throwException) {
        throw new AssertionError("unexpected Throwable ");
      }
    }
    return this;
  }

  public ArrayList<Object> getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    throw new AssertionError(
        "getResult with timeout not expected to be invoked for GemFireXD");
  }

  public void clearResults() {
    if (GemFireXDUtils.isOffHeapEnabled()) {
      Iterator<Object> resultsIter = this.iterator();
      while (resultsIter.hasNext()) {
        Object obj = resultsIter.next();
        OffHeapReleaseUtil.freeOffHeapReference(obj);
        resultsIter.remove();
      }
    } else {
      this.clear();
    }
  }

  public void endResults() {
  }

  /**
   * Result item of GfxdListResultCollector
   */
  public static final class ListResultCollectorValue {

    public final Object resultOfSingleExecution;

    public final DistributedMember memberID;

    private ListResultCollectorValue(Object r, DistributedMember m) {
      resultOfSingleExecution = r;
      memberID = m;
    }
  }
}

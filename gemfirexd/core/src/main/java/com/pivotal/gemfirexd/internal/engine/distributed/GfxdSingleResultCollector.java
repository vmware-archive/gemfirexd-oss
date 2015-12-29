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

import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * A simple single element {@link ResultCollector} expecting a non-null result
 * used in GemFireXD for single node targeted functions.
 * 
 * @author swale
 */
public final class GfxdSingleResultCollector implements
    ResultCollector<Object, Object> {

  private volatile Object result;

  private final Object ignoreToken;

  public GfxdSingleResultCollector() {
    this.ignoreToken = null;
  }

  public GfxdSingleResultCollector(Object ignoreToken) {
    this.ignoreToken = ignoreToken;
  }

  public void addResult(DistributedMember memberID,
      Object resultOfSingleExecution) {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "received addResult from member " + memberID + ": "
              + resultOfSingleExecution);
    }
    if (this.ignoreToken != null
        && this.ignoreToken.equals(resultOfSingleExecution)) {
      return;
    }
    if (this.result != null) {
      throw new GemFireXDRuntimeException(toString() + ": unexpected result "
          + resultOfSingleExecution + " with existing result " + this.result);
    }
    if (GemFireXDUtils.TraceFunctionException) {
      if (resultOfSingleExecution instanceof Throwable) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_FUNCTION_EX,
            "unexpected throwable received in addResult from member "
                + memberID, (Throwable)resultOfSingleExecution);
      }
    }
    this.result = resultOfSingleExecution;
  }

  public Object getResult() throws FunctionException {
    if (GemFireXDUtils.TraceFunctionException) {
      if (this.result instanceof Throwable) {
        throw new AssertionError("unexpected Throwable as result: "
            + this.result);
      }
    }
    return this.result;
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    throw new AssertionError(
        "getResult with timeout not expected to be invoked for GemFireXD");
  }

  public void clearResults() {
    if( GemFireXDUtils.isOffHeapEnabled() && this.result instanceof ResultHolder) {
      ((ResultHolder)this.result).freeOffHeapForCachedRowsAndCloseResultSet();
    }        
    this.result = null;
    
  }

  public void endResults() {
  }
}

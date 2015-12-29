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
package parReg.execute;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;

@SuppressWarnings("unchecked")
public class MemberResultsCollector implements ResultCollector {

  private HashMap result = new HashMap();
  
  /**
   * Adds a single function execution result from a remote node to the
   * ResultCollector
   * 
   * @param resultOfSingleExecution
   * @param memberID
   */

  public  void addResult(DistributedMember memberID,
      Object resultOfSingleExecution) {
      if(result.get(memberID.getId()) == null){
        List memberResults = new ArrayList();
        memberResults.addAll((List)resultOfSingleExecution);
        result.put(memberID.getId(), memberResults);
      }else{
        List memberResults = (ArrayList)result.get(memberID.getId());
        memberResults.addAll((List)resultOfSingleExecution);
      }
  }

  /**
   * Waits if necessary for the computation to complete, and then retrieves its
   * result.<br>
   * If {@link Function#hasResult()} is false, upon calling
   * {@link ResultCollector#getResult()} throws {@link FunctionException}.
   * 
   * @return the Object computed result
   * @throws FunctionException
   *                 if something goes wrong while retrieving the result
   */
  public Object getResult() throws FunctionException {
    return this.result;
  }

  /**
   * Call back provided to caller, which is called after function execution is
   * complete and caller can retrieve results using
   * {@link ResultCollector#getResult()}
   * 
   */
  public void endResults() {

  }

  /**
   * Waits if necessary for at most the given time for the computation to
   * complete, and then retrieves its result, if available. <br>
   * If {@link Function#hasResult()} is false, upon calling
   * {@link ResultCollector#getResult()} throws {@link FunctionException}.
   * 
   * @param timeout
   *                the maximum time to wait
   * @param unit
   *                the time unit of the timeout argument
   * @return Object computed result
   * @throws FunctionException
   *                 if something goes wrong while retrieving the result
   */
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException {
    return this.result;
  }

  /**
   * GemFire will invoke this method before re-executing function (in case of
   * Function Execution HA) This is to clear the previous execution results from
   * the result collector
   * 
   * @since 6.5
   */
  public void clearResults() {
    this.result.clear();
  }
  
  /**
   * Method to receive the results (which will be partial in the tests)
   * 
   * @return Object results
   */
  public Object getPartialResults() {
    return this.result;
  }
}


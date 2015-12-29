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
package query.remote;

import hydra.Log;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;

public class DynamicResultsValidator implements QueryResultsValidator 
{
  public DynamicResultsValidator(){
    super();
  }
  
  public boolean validateQueryResults(Object results, int queryIndex,
      String queryString)
  {
    boolean validate = false;
    Log.getLogWriter().info("Dynamic results validator validating ...");
    EntryEvent evnt = ((EntryEventImpl)QueryExecutorThread.eventLocal.get());
    Log.getLogWriter().info("This is my evnt boss"  + evnt) ;
    Object key = ((EntryEventImpl)QueryExecutorThread.eventLocal.get()).getKey();
    Log.getLogWriter().info("This is my key boss"  + key) ;
    try{
    if ((results instanceof SelectResults)){
      if (!((SelectResults)results).contains(key) ){
        validate = true;        
      }
    }     
    Log.getLogWriter().info("Dynamic results validator validate --> " + validate);
    }
    catch (Exception e) {
      throw new TestException("Caught exception during query execution"
          + TestHelper.getStackTrace(e));
    }
    if (!validate) {
      throw new TestException("The validation was not successful");
    }
    return validate;
  }
}

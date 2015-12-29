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

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
/**
 * Validates query result which is fired from a afterDestroy call back 
 * of cache listener attatched to the region. It verifies that the
 * key which is destroyed should not present in the result set.
 * 
 * @author Yogesh Mahajan
 *
 */
public class DynamicQueryResultsValidator implements QueryResultsValidator
{
  public DynamicQueryResultsValidator() {
    super();
  }

  public boolean validateQueryResults(Object results, int queryIndex,
      String queryString)
  {
    boolean validate = false;
    // get EntryEvent which is set into thread local of this thread
    Object key = ((EntryEventImpl)QueryExecutorThread.eventLocal.get())
        .getKey();
    try {
      if ((results instanceof SelectResults)) {
        if (!((SelectResults)results).contains(key)) {
          validate = true;
        }
      }
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

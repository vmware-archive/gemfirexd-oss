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

import mapregion.MapBB;
import hydra.Log;
import objects.PSTObject;
import util.TestException;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;

/**
 * 
 * Validates the resultSet returned from query.  
 * Checks the query result against region size 
 * 
 * @author Girish,Asif,Yogesh M.
 * 
 */
public class DataVerificationQueryResultsValidator implements QueryResultsValidator
{

  public DataVerificationQueryResultsValidator() {
    super();
  }
  public boolean validateQueryResults(Object results, int queryIndex, String queryString) {
    Log.getLogWriter().info("Executed query :  " + queryString);
    boolean validated = false;
    if (results instanceof SelectResults) {
      int resultSetSize = ((SelectResults)results).size();
      Log.getLogWriter().info("Result set size:" + resultSetSize);
      switch (queryIndex) {
      case 0:
          validated = true;
          break;
      case 1:
      case 2:
        validated = validateResults(queryIndex, (SelectResults)results);          
        break;
      //Cases 3 falling through to 6 are for validating select * queries 
      case 3:       
      case 4:
      case 5:
      case 6:
        validated = validateSelectStarResults(queryIndex, (SelectResults) results, queryString);

        break;
      default:
        throw new TestException("Not valid Query");
      }
    }
    else {
      Log.getLogWriter().info("Result set not instanceof SelectResults");
    }
    return validated;
  }

  private boolean validateResults(int queryIndex, SelectResults results) {
    boolean valid = true;
    for (Object elem : results) {
      if (elem instanceof Portfolio) {
        Portfolio p = (Portfolio)elem;
        if (queryIndex == 1 && !p.status.equalsIgnoreCase("active")) valid = false;
        if (queryIndex == 2 && !p.status.equalsIgnoreCase("inactive")) valid = false;
      } else if (elem instanceof PSTObject) {
        PSTObject p = (PSTObject)elem;
        if (queryIndex == 1 && (p.getIndex()<=0)) valid = false;
      } 
    }
    return valid;
  }
  
  private boolean validateSelectStarResults(int queryIndex,
      SelectResults results, String queryString) {
    long puts = MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
    Log.getLogWriter().info(
        "Comparing resultset of size: " + results.size() + " to region size: "
            + puts + " for query: " + queryString);
    if (results.size() != puts) {
      Log.getLogWriter().info(
          "Size of resultset: " + results.size()
              + " does not match region size: " + puts + " for query: "
              + queryString);
      return false;
    }
    for (Object elem : results) {
      if (!(elem instanceof objects.Portfolio)) {
        Log.getLogWriter().info(
            " Expected Portfolio object but is " + elem.getClass());
        return false;
      }
    }
    return true;
  }

}

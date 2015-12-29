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


import com.gemstone.gemfire.cache.query.SelectResults;
import hydra.HydraVector;
import hydra.Log;
import hydra.TestConfig;
import util.TestException;
import query.*;

/**
 * 
 * Validates the resultSet returned from query.  
 * This is the default implementation of QueryResultsValidator
 * 
 * @author Girish, Yogesh
 * 
 */
public class DefaultQueryResultsValidator implements QueryResultsValidator
{ 
  private int expectedResultSet[] = null;
  
  private String expectedResultType[] = null;
  
  public DefaultQueryResultsValidator(int numOfQueries) {
      
      HydraVector hvSize = TestConfig.tab().vecAt(QueryPrms.expectedResultsSize);
      HydraVector hvType = TestConfig.tab().vecAt(QueryPrms.expectedResultsType);
      String queryResult[] = new String[hvSize.size()];
      this.expectedResultType = new String[hvType.size()];
      hvSize.copyInto(queryResult);
      hvType.copyInto(this.expectedResultType);
      this.expectedResultSet = new int[queryResult.length];
      
      if(this.expectedResultType.length != 1 && this.expectedResultSet.length != this.expectedResultType.length)
        throw new TestException("Size of expectedResultSet and expectedResultType are not same");
      
      if(expectedResultSet.length != numOfQueries)
        throw new TestException("Size of expectedResultSet is not same as numOfQueries");
      
      for (int i = 0; i < queryResult.length ; i++ ){        
        this.expectedResultSet[i] = Integer.parseInt(queryResult[i]);  
      }   
   }
  
  
  public boolean validateQueryResults(Object results, int queryIndex,
      String queryString)
  {
    boolean validate = false;
    Log.getLogWriter().info("Executed query : " + queryString);
    if (  (this.expectedResultType.length == 1 && this.expectedResultType[0].equalsIgnoreCase("SelectResults"))
        || this.expectedResultType[queryIndex].equalsIgnoreCase("SelectResults")) {
      
      if ((results instanceof SelectResults)){
        if (((SelectResults)results).size() == expectedResultSet[queryIndex]){
          validate = true ;
        }
        else {
          throw new TestException("Size of the results "
              + ((SelectResults)results).size()
              + " is not same as the expected size "
              + expectedResultSet[queryIndex]);
        }
      }
      else {
        throw new TestException("Query Results not instance of SelectResults");
      }
    }
    else {
      throw new TestException(
          "Test Configuration Issue in setting expected result type and result sizes in the conf");
    }
    //TODO: Yogesh : Add suuport for StructSet as well
    return validate;
  }
}

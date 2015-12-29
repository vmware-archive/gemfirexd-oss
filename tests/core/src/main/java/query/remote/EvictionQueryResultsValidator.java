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
import hydra.RegionHelper;
import util.TestException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.SelectResults;

/**
 * Validates results of query which is fired on a region with eviction
 * attributes It also validates the resultSet size against maximum value used by
 * the EvictionAlgorithm
 * 
 * @author Yogesh Mahajan
 * 
 */
public class EvictionQueryResultsValidator implements QueryResultsValidator
{
  public EvictionQueryResultsValidator(){
    super();
  }

  public boolean validateQueryResults(Object resultSet, int queryIndex,
      String queryString)
  {
    Log.getLogWriter().info("Executed query : " + queryString);
    Region reg = RegionHelper.getRegion(RemoteQueryTest.edgeRegionName);
    //maximum value used by the EvictionAlgorithm which
    //determines when the EvictionAction is performed.
    int lruMemorySize = reg.getAttributes().getEvictionAttributes().getMaximum();
    boolean validated = false;
    if (resultSet instanceof SelectResults) {
      int resultSetSize = ((SelectResults)resultSet).size();
      switch (queryIndex) {
      case 0:
        if (resultSetSize == lruMemorySize)
          validated = true;
          break;
      case 1:
      case 2:
        if (resultSetSize == lruMemorySize / 2)
          validated = true;          
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
}

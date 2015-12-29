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
package partitionedRegionQuerying;

import java.util.ArrayList;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;

/**
 * This function executes a query using its RegionFunctionContext which provides
 * a filter on data which should be queried.
 * 
 */
public class QueryingFunction extends FunctionAdapter implements Declarable {
  
  @Override
  public void execute(FunctionContext context) {
    // Get an existing cache 
    Cache cache = CacheFactory.getAnyInstance();
    // Get queryservice from cache
    QueryService queryService = cache.getQueryService();
    // Get the query string passed as an argument to the function
    String qstr = (String) context.getArguments();

    try {
      Query query = queryService.newQuery(qstr);
      // If function is executed on region, context is RegionFunctionContext
      RegionFunctionContext rContext = (RegionFunctionContext) context;
      // Execute the query
      cache.getLogger().info("Executing query: " + qstr);
      SelectResults results = (SelectResults) query.execute(rContext);
      cache.getLogger().info("Query returned " + results.size() + " results");
      
      // Send the results to function caller node.
      context.getResultSender().sendResult((ArrayList) (results).asList());
      context.getResultSender().lastResult(null);

    } catch (Exception e) {
      throw new FunctionException(e);
    }
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public String getId() {
    return getClass().getName();
  }

  @Override
  public void init(Properties props) {
   
  }
}

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

package query.context;

import hydra.Log;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import objects.PRPortfolio;
import query.index.IndexBB;
import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Class defining Function execution code for testing Context based querying
 * using localDataSet API
 * 
 * @author kdeshpan
 * 
 */
public class QueryFunction extends FunctionAdapter {

  @Override
  public void execute(FunctionContext context) {
    Log.getLogWriter().info(
        "QueryFunction.execute()::Executing QueryFunction on context ");
    String regionName = query.QueryTest.REGION_NAME;
    Cache cache = CacheFactory.getAnyInstance();

    try {
      PartitionedRegion region1 = (PartitionedRegion)CacheFactory
          .getAnyInstance().getRegion("QueryRegion1");
      if (region1 != null) {
        List bIds = region1.getLocalBucketsListTestOnly();
        Log.getLogWriter().info(
            "Server-side region contents - Local Bucket IDs for QueryRegion1 ::::" + bIds.toString());
        // LocalDataSet localDataSet = (LocalDataSet) ((RegionFunctionContext)
        // context).getDataSet();
        // Set = localDataSet.getBucketSet();
        for (Object key : bIds) {
          BucketRegion br = region1.getBucketRegion(key);
          if (br != null) {
            Log.getLogWriter().info(
                "Server-side region contents -Bucket ID for bucket region: " + br.getId());
            Log.getLogWriter().info(
                "Server-side region contents - Keys in the bucket region for key " + key + ": "
                    + br.keySet().toString());
          }
          else {
            Log.getLogWriter().info(
                "Server-side region contents  Bucket region not found for key:" + key);
          }
        }
      }
    }
    catch (Exception e1) {
      // TODO Auto-generated catch block
      //throw new TestException(e1.getMessage());
    }

    QueryService queryService = cache.getQueryService();
    ArrayList allQueryResults = new ArrayList();
    //String qstr = (String)context.getArguments();
    ArrayList arguments = (ArrayList)(context.getArguments());
    String qstr = (String)arguments.get(0);
    Object initiatingThreadID = arguments.get(1);
    String aStr = "In execute with context " + context + " with query " + qstr +
       " initiated in hydra thread thr_" + initiatingThreadID + "_";
    for (int i = 2; i < arguments.size(); i++) {
       aStr = aStr + " additional arg: " + arguments.get(i);
    }
    Log.getLogWriter().info(aStr);
    PartitionedRegion region = (PartitionedRegion)CacheFactory.getAnyInstance()
        .getRegion(regionName);
    Log.getLogWriter().info(
        "QueryFunction.execute()::Executing query:: " + qstr);
    try {
      Query query = queryService.newQuery(qstr);
      SelectResults result = (SelectResults)query
          .execute((RegionFunctionContext)context);
      Log.getLogWriter().info(
          "QueryFunction.execute()::Context arguments :: "
              + context.getArguments().toString());
      Set filter = ((RegionFunctionContext)context).getFilter();
      if (filter != null) {
        Log.getLogWriter().info(
            "QueryFunction.execute()::Context FILTER on server side :: "
                + filter.toString());
      }
      SharedCounters counters = QueryFunctionContextBB.getBB()
          .getSharedCounters();
      counters.increment(QueryFunctionContextBB.NUM_NODES);
      ArrayList arrayResult = (ArrayList)result.asList();
      // context.getResultSender().sendResult(arrayResult);
      // context.getResultSender().lastResult(null);
      context.getResultSender().sendResult((ArrayList)result.asList());
      context.getResultSender().lastResult(null);
    }
    catch (Exception e) {
      throw new FunctionException(e);
      // throw new FunctionException(e.getMessage());
    }
  }

  public String getId() {
    return "QueryFunction";
  }

}

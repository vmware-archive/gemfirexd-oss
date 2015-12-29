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

package cacheperf.comparisons.replicated.execute;

import cacheperf.CachePerfClient;
import cacheperf.CachePerfPrms;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.execute.*;
import distcache.gemfire.GemFireCacheTestImpl;
import hydra.Log;
import hydra.MasterController;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import objects.ObjectHelper;

/**
 * Client used to measure performance of function execution.
 */
public class ExecuteClient extends CachePerfClient {

  private static final Function function = ExecutePrms.getFunction();

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * Registers the function given in {@link ExecutePrms#function}.
   */
  public static void registerFunctionTask() {
    FunctionService.registerFunction(function);
  }

  /**
   * TASK to put objects using the {@link ExecutePrms#function}.  The filter
   * is the set containing the key.
   */
  public static void putFunctionExecutionDataTask() throws Exception {
    ExecuteClient c = new ExecuteClient();
    c.initialize(PUTS);
    c.putFunctionExecutionData();
  }
  private void putFunctionExecutionData() throws Exception {
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      putFunctionExecution(r, key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void putFunctionExecution(Region r, int i) throws Exception {
    Object key = ObjectHelper.createName(this.keyType, i);
    String objectType = CachePerfPrms.getObjectType();
    Serializable val = (Serializable)ObjectHelper.createObject(objectType, i);
    long start = this.statistics.startPut();
    FunctionService.onRegion(r)
                   .withFilter(Collections.singleton(key))
                   .withArgs(val)
                   .execute(function.getId())
                   .getResult();
    this.statistics.endPut(start, 1, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK to get objects using the {@link ExecutePrms#function}.  The filter
   * is the set containing the key.
   */
  public static void getFunctionExecutionDataTask() throws Exception {
    ExecuteClient c = new ExecuteClient();
    c.initialize(GETS);
    c.getFunctionExecutionData();
  }
  private void getFunctionExecutionData() throws Exception {
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      getFunctionExecution(r, key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void getFunctionExecution(Region r, int i) throws Exception {
    Object key = ObjectHelper.createName(this.keyType, i);
    long start = this.statistics.startGet();
    List resultSet = (List)FunctionService.onRegion(r)
                          .withFilter(Collections.singleton(key))
                          .execute(function.getId())
                          .getResult();
    if (resultSet.size() == 0) {
      this.processNullValue(key);
    } else {
      Object val = resultSet.iterator().next();
      if (this.validateObjects) {
        validate(i, val);
      }
    }
    this.statistics.endGet(start, 1, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK to put and get objects using the {@link ExecutePrms#function}.
   */
  public static void putAndGetFunctionExecutionDataTask() throws Exception {
    ExecuteClient c = new ExecuteClient();
    c.initialize(GETS);
    c.putAndGetFunctionExecutionData();
  }
  private void putAndGetFunctionExecutionData() throws Exception {
    Region r = ((GemFireCacheTestImpl)this.cache).getRegion();
    if (this.useTransactions) {
      this.begin();
    }
    do {
      int key = getNextKey();
      executeTaskTerminator();   // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      putAndGetFunctionExecution(r, key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void putAndGetFunctionExecution(Region r, int i) throws Exception {
    Serializable key = (Serializable)ObjectHelper.createName(this.keyType, i);
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject(objectType, i);

    // initial put
    /*
    long start = this.statistics.startPut();
    r.put(key, val);
    this.statistics.endPut(start, 1, this.isMainWorkload, this.histogram);
    */

    for (int j = 0; j < 4; j++) {
      if (this.sleepBeforeOp) {
        MasterController.sleepForMs(CachePerfPrms.getSleepMs());
      }
      // put
      long start = this.statistics.startPut();
      r.put(key, val);
      this.statistics.endPut(start, 1, this.isMainWorkload, this.histogram);

      // get using function
      start = this.statistics.startGet();
      Object result = retrieveUsingRegionFunction(r, key);
      this.statistics.endGet(start, 1, this.isMainWorkload, this.histogram);
    }
  }

  private Object retrieveUsingRegionFunction(Region r, Serializable key) {
    Set keys = new HashSet();
    keys.add(key);

    Execution execution = FunctionService.onRegion(r).withFilter(keys);
    Object result = null;
    int attempt = 0;
    while (attempt < 3) {
      try {
        result = retrieveWithFunction(execution);
        break;
      } catch (ServerConnectivityException e) {
        handleException(key, e);
        attempt++;
      }
    }
    return result;
  }

  private Object retrieveWithFunction(Execution execution) {
    ResultCollector collector = execution.execute(function.getId(),
                    true /*hasResult*/, true /*isHA*/,
                    true /*isOptimizeForWrite*/);
    List allResults = (List)collector.getResult();
    return allResults.get(0);
  }
  private void handleException(Object key, ServerConnectivityException e) {
    if (!(e.getCause() instanceof IOException)) {
      throw e;
    }
    String message = "Caught exception attempting to execute function on "
                   + key + ". Retrying.";
    Log.getLogWriter().warning(message, e);
  }
}

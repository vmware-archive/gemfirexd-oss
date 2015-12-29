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


import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import java.util.Properties;

/**
 * Function that puts the value from the argument at the key passed in through
 * the filter.
 */
public class PutKeyFunction extends FunctionAdapter implements Declarable {

  private static final String ID = "PutKeyFunction";

  public void execute(FunctionContext context) {
    RegionFunctionContext regionContext = (RegionFunctionContext)context;
    Region dataSet = regionContext.getDataSet();
    Object key = regionContext.getFilter().iterator().next();
    Object value = regionContext.getArguments();
    dataSet.put(key, value);
    context.getResultSender().lastResult(Boolean.TRUE);
  }

  public String getId() {
    return ID;
  }

  public boolean hasResult() {
    return true;
  }

  public void init(Properties p) {
  }

  public boolean optimizeForWrite() {
    return true;
  }
  public boolean isHA() {
    return false;
  }
}

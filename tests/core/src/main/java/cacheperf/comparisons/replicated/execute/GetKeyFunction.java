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

import java.io.Serializable;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import java.util.Properties;

/**
 * Function that returns the value of the key passed in through the filter,
 * using the default result collector.
 */
public class GetKeyFunction extends FunctionAdapter implements Declarable {

  private static final String ID = "GetKeyFunction";

  public void execute(FunctionContext context) {
    RegionFunctionContext regionContext = (RegionFunctionContext)context;
    Region dataSet = regionContext.getDataSet();
    Object key = regionContext.getFilter().iterator().next();
    context.getResultSender().lastResult((Serializable)dataSet.get(key));
  }

  public String getId() {
    return ID;
  }

  public boolean hasResult() {
    return true;
  }

  public void init(Properties p) {
  }
  public boolean isHA() {
    return false;
  }

}

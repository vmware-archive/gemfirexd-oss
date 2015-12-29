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
package parReg.execute;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import util.TestException;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;

public class NonPRHctTestFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
      regionContext = (RegionFunctionContext)context;
    }

    Region region = regionContext.getDataSet();
    // verify that the execution happens on replicated region and not one with
    // empty datapolicy
    if (region.getAttributes().getDataPolicy() != DataPolicy.REPLICATE) {
      throw new TestException(
          "For non PR region the function execution on region happened on non replicate region");
    }

    Set<Object> keySet = region.keySet();
    ArrayList<Object> list = new ArrayList<Object>();
    Iterator<Object> iterator = keySet.iterator();

    while (iterator.hasNext()) {
      list.add(region.get(iterator.next()));
    }
    context.getResultSender().lastResult(list);

  }

  public String getId() {
    return this.getClass().getName();
  }

  public boolean hasResult() {
    return true;
  }
  public boolean isHA() {
    return false;
  }

}

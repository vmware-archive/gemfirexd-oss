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
package security;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class SecurityFunction extends FunctionAdapter {

  private boolean isHA = false;

  public SecurityFunction() {
  }

  public SecurityFunction(boolean isHA) {
    this.isHA = isHA;
  }

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    RegionFunctionContext regionContext = null;
    ArrayList<Object> list = new ArrayList<Object>();
    if (isRegionContext) {
      regionContext = (RegionFunctionContext)context;
      Region region = regionContext.getDataSet();
      Set<Object> keys = region.keySet();
      Iterator<Object> iterator = keys.iterator();
      while (iterator.hasNext()) {
        list.add(iterator.next());
      }

      if (regionContext.getArguments() != null) {
        HashSet<Object> args = (HashSet)regionContext.getArguments();
        list.removeAll(args);
      }
    }
    else {
      DistributionConfig dc = ((InternalDistributedSystem)InternalDistributedSystem
          .getAnyInstance()).getConfig();
      list.add(dc.getCacheXmlFile());
      list.add(dc.getName());

      if (context.getArguments() != null) {
        list.add("Insecure item");
      }
    }
    context.getResultSender().lastResult(list);
  }

  public String getId() {
    return "SecureFunction";
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return false;
  }
  public boolean isHA() {
    return this.isHA;
  }

}

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
package parReg.execute.useCase1;

import java.util.*;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import hydra.*;

public class UseCase1Function extends FunctionAdapter {

  ArrayList list = new ArrayList();

  public void execute(FunctionContext context) {
    if (context instanceof RegionFunctionContext) {
      Log.getLogWriter().info("Executing function ");
      RegionFunctionContext prContext = (RegionFunctionContext)context;
      if (prContext.getArguments() != null
          && prContext.getArguments() instanceof Integer) {
        PartitionedRegion pr = (PartitionedRegion)prContext.getDataSet();
        int instrumentId = ((Integer)prContext.getArguments()).intValue();
        Set prKeys = pr.keys();
        Iterator iterator = prKeys.iterator();
        while (iterator.hasNext()) {
          RiskPartitionKey key = (RiskPartitionKey)iterator.next();
          if (key.getInstrumentId().intValue() == instrumentId) {
            if (pr.get(key) != null) {
              Log.getLogWriter().info("Adding to the list " + pr.get(key));
              list.add(pr.get(key));
            }
          }
        }
        context.getResultSender().lastResult(list);
      }else{
        context.getResultSender().lastResult(null);
      }
    }else{
      context.getResultSender().lastResult(null);
    }
  }

  public String getId() {
    return "UseCase1Function";
  }

  public boolean hasResult() {
    return true;
  }
  public boolean isHA() {
    return false;
  }

}

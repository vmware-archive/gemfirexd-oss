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
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;

/**
 * @author Suranjan Kumar
 *
 */
public class PRContainsValueFunction extends FunctionAdapter implements InternalEntity {

  @Override
  public void execute(FunctionContext context) {

    RegionFunctionContext prContext = (RegionFunctionContext)context;
    Region dataSet = prContext.getDataSet();
    Object values = context.getArguments();
    
    Iterator itr = dataSet.values().iterator();
    while (itr.hasNext()) {
      Object val = itr.next();
      if(val.equals(values)) {
        prContext.getResultSender().lastResult(Boolean.TRUE);
        return;
      }
    }
    prContext.getResultSender().lastResult(Boolean.FALSE);
  }
  
  @Override
  public String getId() {
    return getClass().getName();
  }
  
  @Override
  public boolean optimizeForWrite() {
    return false;
  }
}

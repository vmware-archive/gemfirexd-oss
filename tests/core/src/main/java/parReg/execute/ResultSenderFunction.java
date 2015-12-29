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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import parReg.ParRegBB;
import util.TestHelper;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;


public class ResultSenderFunction extends FunctionAdapter{

  public void execute(FunctionContext context) {
    final boolean isRegionContext = context instanceof RegionFunctionContext;
    RegionFunctionContext regionContext = null;
    if (isRegionContext) {
      regionContext = (RegionFunctionContext)context;
    }
    
    if(regionContext != null){
      if(regionContext.getFilter() != null && regionContext.getFilter() instanceof Set){
        ArrayList filter = new ArrayList();
        filter.addAll((Set)regionContext.getFilter());
        Iterator argsIterator = filter.iterator();
        int addResultCounter = 0;
        
        while(argsIterator.hasNext()){
          regionContext.getResultSender().sendResult((Serializable)argsIterator.next());
          addResultCounter++;
          TestHelper.waitForCounter(ParRegBB.getBB(), "ParRegBB.resultSenderCounter", ParRegBB.resultSenderCounter,
              addResultCounter, true, 30000);
        }
        regionContext.getResultSender().lastResult(null);
      }
    }
  }

  public String getId() {
    return "ResultSenderFunction";
  }
  public boolean isHA() {
    return false;
  }

}
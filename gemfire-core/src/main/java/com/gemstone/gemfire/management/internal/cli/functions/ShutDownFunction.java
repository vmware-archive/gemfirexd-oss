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
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.admin.remote.ShutdownAllRequest;

/**
 * 
 * Class for Shutdown function
 * 
 * @author apande
 *  
 * 
 */
public class ShutDownFunction implements Function, InternalEntity {
  public static final String ID = ShutDownFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    Object[] args = (Object[]) context.getArguments();
    long timeout = ((Number) args[0]).intValue();
    
    try{
      Cache cache = CacheFactory.getAnyInstance();
      InternalDistributedSystem ds = (InternalDistributedSystem) cache
          .getDistributedSystem();     
      if (timeout > 0) {
        ShutdownAllRequest.send(ds.getDistributionManager(), timeout);
      } else {
        ShutdownAllRequest.send(ds.getDistributionManager(), -1);
      }
      context.getResultSender().lastResult("succeeded in shutting down");
    }catch(Exception ex){
      context.getResultSender().lastResult("ShutDownFunction could not locate cache " +ex.getMessage());
    }   
  }

  @Override
  public String getId() {
    return ShutDownFunction.ID;

  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    // no need of optimization since read-only.
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

}
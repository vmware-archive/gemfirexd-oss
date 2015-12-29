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
package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

/**
 * A listener which will wipe out the PDX registry on the client side if the
 * entire server distributed system was lost and came back on line. <br>
 * <br>
 * TODO - There is a window in which all of the servers could crash and come
 * back up and we would connect to a new server before realizing that all the
 * servers crashed. To fix this, we would need to get some kind of birthdate of
 * the server ds we connect and use that to decide if we need to recover
 * the PDX registry.
 * 
 * We can also lose connectivity with the servers, even if the servers are still
 * running. Maybe for the PDX registry we need some way of telling if the PDX
 * registry was lost at the server side in the interval. 
 * 
 * 
 * @author dsmith
 * 
 */
public class PdxRegistryRecoveryListener extends EndpointManager.EndpointListenerAdapter {
  private final AtomicInteger endpointCount = new AtomicInteger();
  private final InternalPool pool;
  
  public PdxRegistryRecoveryListener(InternalPool pool) {
    this.pool = pool;
  }
  
  @Override
  public void endpointCrashed(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if(pool.getLoggerI18n().fineEnabled()) {
      pool.getLoggerI18n().fine("PdxRegistryRecoveryListener - EndpointCrashed. Now have " + count  + " endpoints");
    }
  }

  @Override
  public void endpointNoLongerInUse(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if(pool.getLoggerI18n().fineEnabled()) {
      pool.getLoggerI18n().fine("PdxRegistryRecoveryListener - EndpointNoLongerInUse. Now have " + count  + " endpoints");
    }
  }

  @Override
  public void endpointNowInUse(Endpoint endpoint) {
    int count  = endpointCount.incrementAndGet();
    if(pool.getLoggerI18n().fineEnabled()) {
      pool.getLoggerI18n().fine("PdxRegistryRecoveryListener - EndpointNowInUse. Now have " + count  + " endpoints");
    }
    if(count == 1) {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache == null) {
        return;
      }
      TypeRegistry registry = cache.getPdxRegistry();
      
      if(registry == null) {
        return;
      }
      registry.clear();
    }
  }
}

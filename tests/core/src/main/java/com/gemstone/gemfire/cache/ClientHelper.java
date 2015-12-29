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
package com.gemstone.gemfire.cache;

import hydra.PoolHelper;
import hydra.RegionDescription;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.DynamicRegionFactory.Config;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerProxy;
import com.gemstone.gemfire.cache.util.BridgeWriter;
import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * Provides methods for getting at the 
 * bridge client and connection proxies used by a
 * region.
 * 
 * @author dsmith
 *
 */
public class ClientHelper {
  
  public static PoolImpl getPool(Region region) {
    ServerProxy proxy = ((LocalRegion)region).getServerProxy();
    if(proxy == null) {
      return null;
    } else {
      return (PoolImpl) proxy.getPool();
    }
  }
  
  public static Set getActiveServers(Region region) {
    return new HashSet(getPool(region).getCurrentServers());
  }
  
//   public static Set getDeadServers(Region region) {
//   }
  
  private ClientHelper() {
    
  }
  
  public static int getRetryInterval(Region region) {
    return (int)(getPool(region).getPingInterval());
  }

  /**
   * @param region
   */
  public static void release(Region region) {
    
    PoolImpl pool = getPool(region);
    if(pool != null) {
      pool.releaseThreadLocalConnection();
    }
  }

  public static Config getDynamicRegionConfig(File diskDir,
      RegionDescription regionDescription, boolean persistBackup, boolean registerInterest) {
    if(regionDescription.getPoolDescription() != null) {
      String poolName = regionDescription.getPoolDescription().getName();
      PoolHelper.createPool(poolName);
      return new DynamicRegionFactory.Config(diskDir, poolName, persistBackup, registerInterest);
    }
    else {
      BridgeWriter writer = (BridgeWriter) regionDescription.getCacheWriterInstance(true);
      return new DynamicRegionFactory.Config(diskDir, writer, persistBackup, registerInterest);
    }
  }
}

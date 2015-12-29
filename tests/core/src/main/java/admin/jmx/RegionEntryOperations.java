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
package admin.jmx;

import hydra.ConfigHashtable;
import hydra.DistributedSystemHelper;
import hydra.TestConfig;

import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.HARegion;

public class RegionEntryOperations {
  
  static ConfigHashtable conftab = TestConfig.tab();
  
    
  public static void invokeRegionEntryDestroy() {
    
    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);
        
    Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
    Object[] regionList = cache.rootRegions().toArray();
    int numRegs = regionList.length;

    for (int i = 0; i < numRegs; i++) {
      Region reg = (Region)regionList[i];
      if (!(reg instanceof HARegion)) {
        Set keys = reg.keySet();
        
        if(keys.size() > noOfEntity) {
          Iterator ite=keys.iterator();
          int count = keys.size() - noOfEntity ;
          for (int j = 0; ite.hasNext() && j < count ; j++ )
            reg.destroy(ite.next());
        }
      }
    }
  }
  
  public static void invokeRegionEntryGet() {
    
    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);
    
    Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
    Object[] regionList = cache.rootRegions().toArray();
    int numRegs = regionList.length;

    for (int i = 0; i < numRegs; i++) {
      Region reg = (Region)regionList[i];
      if (!(reg instanceof HARegion)) {
        Set keys = reg.keySet();
        Iterator ite=keys.iterator();
        for (int j = 0; ite.hasNext() && j < noOfEntity ; j++ )
          reg.get(ite.next());
        }
      }
  }
  
  public static void invokeRegionEntryMiss() {
    
    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);
    
    Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
    Object[] regionList = cache.rootRegions().toArray();
    int numRegs = regionList.length;

    for (int i = 0; i < numRegs; i++) {
      Region reg = (Region)regionList[i];
      if (!(reg instanceof HARegion)) {
        int count = reg.keySet().size() - noOfEntity;
        for (int j = 0; j < count ; j++ )
          reg.get("notexist"+j);
        }
      }
  }
    
  public static void invokeRegionEntryPut() {
    
    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);
    
    Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
    Object[] regionList = cache.rootRegions().toArray();
    int numRegs = regionList.length;

    for (int i = 0; i < numRegs; i++) {
      Region reg = (Region)regionList[i];
      if (!(reg instanceof HARegion)) {
        Set keys = reg.keySet();
        Iterator ite=keys.iterator();
        for (int j = 0; ite.hasNext() && j < noOfEntity ; j++ ) {
          Object key = ite.next();
          reg.put(key,key);
         }
       }
     }
  }  
  


  public static void invokeRegionEntryInvalidate() {

    int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);

    Cache cache = CacheFactory.getInstance(DistributedSystemHelper
        .getDistributedSystem());
    Object[] regionList = cache.rootRegions().toArray();
    int numRegs = regionList.length;

    for (int i = 0; i < numRegs; i++) {
      Region reg = (Region)regionList[i];
      if (!(reg instanceof HARegion)) {
        Set keys = reg.keySet();
        Iterator ite = keys.iterator();
        for (int j = 0; ite.hasNext() && j < noOfEntity; j++) {
          Object key = ite.next();
          reg.invalidate(key);
        }
      }
    }
  }  
  
}

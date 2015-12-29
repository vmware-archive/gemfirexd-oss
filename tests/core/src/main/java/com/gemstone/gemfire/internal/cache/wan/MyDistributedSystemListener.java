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
package com.gemstone.gemfire.internal.cache.wan;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;

public class MyDistributedSystemListener implements DistributedSystemListener {

  public int addCount;
  public int removeCount;
  Cache cache;
  
  public MyDistributedSystemListener() {
  }
  
  /**
   * Please note that dynamic addition of the sender id to region is not yet available.  
   */
  public void addedDistributedSystem(int remoteDsId) {
     addCount++;
     List<Locator> locatorsConfigured = Locator.getLocators();
     Locator locator = locatorsConfigured.get(0);
     Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = ((InternalLocator)locator).getAllLocatorsInfo();
     System.out.println("Added : allSiteMetaData : " + allSiteMetaData);
  }
  
  public void removedDistributedSystem(int remoteDsId) {
    removeCount++;
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    Map<Integer,Set<DistributionLocatorId>> allSiteMetaData = ((InternalLocator)locator).getAllLocatorsInfo();
    System.out.println("Removed : allSiteMetaData : " + allSiteMetaData);
  }

  public int getAddCount() {
    return addCount;
  }
  
  public int getRemoveCount() {
    return removeCount;
  }
   
 
}

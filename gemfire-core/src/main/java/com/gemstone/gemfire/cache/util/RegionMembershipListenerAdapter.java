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
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionMembershipListener;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Utility class that implements all methods in <code>RegionMembershipListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods for the events of interest.
 * 
 * @author Darrel Schneider
 * 
 * @since 5.0
 */
public abstract class RegionMembershipListenerAdapter<K,V> 
extends CacheListenerAdapter<K,V> 
implements RegionMembershipListener<K,V> {
  public void initialMembers(Region<K,V> r, DistributedMember[] initialMembers) {
  }
  public void afterRemoteRegionCreate(RegionEvent<K,V> event) {
  }
  public void afterRemoteRegionDeparture(RegionEvent<K,V> event) {
  }
  public void afterRemoteRegionCrash(RegionEvent<K,V> event) {
  }
}

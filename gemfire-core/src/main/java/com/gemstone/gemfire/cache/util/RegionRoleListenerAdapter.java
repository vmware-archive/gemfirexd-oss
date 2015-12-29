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

import com.gemstone.gemfire.cache.RegionRoleListener;
import com.gemstone.gemfire.cache.RoleEvent;

/**
 * Utility class that implements all methods in 
 * <code>RegionRoleListener</code> with empty implementations. 
 * Applications can subclass this class and only override the methods for 
 * the events of interest.
 * 
 * @author Kirk Lund
 * @since 5.0
 */
public abstract class RegionRoleListenerAdapter<K,V> 
extends RegionMembershipListenerAdapter<K,V>
implements RegionRoleListener<K,V> {

  public void afterRoleGain(RoleEvent<K,V> event) {}
  
  public void afterRoleLoss(RoleEvent<K,V> event) {}

}


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

import java.util.Set;

/** 
 * Contains information about an event affecting a region reliability, 
 * including its identity and the circumstances of the event. This is 
 * passed in to {@link RegionRoleListener}.
 *
 * @author Kirk Lund
 * @see RegionRoleListener
 * @since 5.0
 */
public interface RoleEvent<K,V> extends RegionEvent<K,V> {
  
  /**
   * Returns the required roles that were lost or gained because of this
   * event.
   */
  public Set<String> getRequiredRoles();
  
}

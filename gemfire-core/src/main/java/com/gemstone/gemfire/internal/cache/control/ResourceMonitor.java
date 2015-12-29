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
package com.gemstone.gemfire.internal.cache.control;

import java.util.Set;

import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;

/**
 * Implemented by classes that the ResourceManager creates in order to monitor a
 * specific type of resource (heap memory, off-heap memory, disk, etc.).
 * 
 * @author David Hoots
 * @since 7.5
 */
interface ResourceMonitor {

  /**
   * Ask the monitor to notify the given listeners of the given event.
   * 
   * @param listeners
   *          Set of listeners of notify.
   * @param event
   *          Event to send to the listeners.
   */
  public void notifyListeners(final Set<ResourceListener> listeners, final ResourceEvent event);

  /**
   * Ask the monitor to stop monitoring.
   */
  public void stopMonitoring();
  
  /**
   * Populate the fields in the profile that are appropriate for this monitor.
   * 
   * @param profile
   *          The profile to populate.
   */
  public void fillInProfile(final ResourceManagerProfile profile);
}

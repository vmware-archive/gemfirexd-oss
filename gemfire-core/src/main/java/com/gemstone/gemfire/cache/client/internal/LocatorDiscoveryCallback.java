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

import java.util.List;

/**
 * A callback to receive notifications about locator discovery. Currently 
 * only used internally.
 * @author dsmith
 * @since 5.7
 */
public interface LocatorDiscoveryCallback {
  
  /**
   * Called to indicate that new locators
   * have been discovered
   * @param locators a list of InetSocketAddresses of new
   * locators that have been discovered.
   */
  void locatorsDiscovered(List locators);
  
  /**
   * Called to indicated that locators
   * have been removed from the list
   * of available locators.
   * @param locators a list of InetSocketAddresses
   * of locators that have been removed
   */
  void locatorsRemoved(List locators);
  

}

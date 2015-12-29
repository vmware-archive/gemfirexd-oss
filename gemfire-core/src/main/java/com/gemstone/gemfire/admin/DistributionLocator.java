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

package com.gemstone.gemfire.admin;

/**
 * Represents a single distribution locator server, of which a
 * distributed system may use zero or many.  The distributed system
 * will be configured to use either multicast discovery or locator
 * service.
 *
 * @see DistributionLocatorConfig
 *
 * @author    Kirk Lund
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface DistributionLocator extends ManagedEntity {
  
  /** 
   * Returns the identity name for this locator.
   */
  public String getId();
  
  /**
   * Returns the configuration object for this distribution locator.
   *
   * @since 4.0
   */
  public DistributionLocatorConfig getConfig();

}


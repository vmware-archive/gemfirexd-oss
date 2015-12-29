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
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.distributed.Locator;

/**
 * MBean that provides access to information and management functionality for a
 * {@link Locator}.
 * 
 * @author rishim
 * @since 7.0
 */
public interface LocatorMXBean {

  /**
   * Returns the port on which this Locator listens for connections.
   */
  public int getPort();

  /**
   * Returns a string representing the IP address or host name that this Locator
   * will listen on.
   */
  public String getBindAddress();

  /**
   * Returns the name or IP address to pass to the client as the location
   * where the Locator is listening.
   */
  public String getHostnameForClients();

  /**
   * Returns whether the Locator provides peer location services to members.
   * 
   * @return True if the Locator provides peer locations services, false otherwise.
   */
  public boolean isPeerLocator();

  /**
   * Returns whether the Locator provides server location services To clients.
   * 
   * @return True if the Locator provides server location services, false otherwise.
   */
  public boolean isServerLocator();

  /**
   * Returns the most recent log entries for the Locator.
   */
  public String viewLog();

  /**
   * Returns a list of servers on which the manager service may be started
   * either by a Locator or users.
   */
  public String[] listPotentialManagers();

  /**
   * Returns the list of current managers.
   */
  public String[] listManagers();
}

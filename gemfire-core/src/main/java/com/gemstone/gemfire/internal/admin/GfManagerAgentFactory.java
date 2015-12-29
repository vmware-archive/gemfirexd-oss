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
   
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.internal.admin.remote.*;

/**
 * A factory for GfManagerAgent instances.  This is the main entry
 * point for the admin API.
 *
 * @author    Pete Matern
 * @author    Darrel Schneider
 * @author    Kirk Lund
 *
 */
public class GfManagerAgentFactory {
 
  /**
   * Creates a GfManagerAgent for managing a distributed system.
   *
   * @param config  definition of the distributed system to manage
   */
  public static GfManagerAgent getManagerAgent(GfManagerAgentConfig config) {
    return new RemoteGfManagerAgent(config);
  }

}

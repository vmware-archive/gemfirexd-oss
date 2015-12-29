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

package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.ManagedEntity;
import com.gemstone.gemfire.internal.ClassPathLoader;

/**
 * Creates ManagedEntityController for administration (starting, stopping, etc.) 
 * of GemFire {@link ManagedEntity}s.
 * 
 * @author Kirk Lund
 */
public class ManagedEntityControllerFactory {

  private static final String ENABLED_MANAGED_ENTITY_CONTROLLER_CLASS_NAME = "com.gemstone.gemfire.admin.internal.EnabledManagedEntityController";
  
  static ManagedEntityController createManagedEntityController(final AdminDistributedSystem system) {
    if (isEnabledManagedEntityController()) {
      system.getLogWriter().config("Local and remote OS command invocations are enabled for the Admin API.");
      return createEnabledManagedEntityController(system);
    } else {
      system.getLogWriter().config("Local and remote OS command invocations are disabled for the Admin API.");
      return new DisabledManagedEntityController(system);
    }
  }

  public static boolean isEnabledManagedEntityController() {
    try {
      ClassPathLoader.getLatest().forName(ENABLED_MANAGED_ENTITY_CONTROLLER_CLASS_NAME);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
  
  private static ManagedEntityController createEnabledManagedEntityController(final AdminDistributedSystem system) {
    return new EnabledManagedEntityController(system);
  }
}

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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.DistributedSystemConfig;

/**
 * This is a disabled implementation of ManagedEntityController for bug #47909.
 *
 * The old ManagedEntityController was a concrete class which has been renamed
 * to ManagedEntityControllerImpl. The build.xml now skips building
 * ManagedEntityControllerImpl. If ManagedEntityControllerImpl is not found
 * in the classpath then the code uses DisabledManagedEntityController as a
 * place holder.
 *
 * @author Kirk Lund
 */
class DisabledManagedEntityController implements ManagedEntityController {

  private static final String VERBOSE = "gemfire.DisabledManagedEntityController.VERBOSE";

  private static final String EXCEPTION_MESSAGE = "Local and remote OS command invocations are disabled for the Admin API.";
  
  private final boolean verbose = Boolean.getBoolean(VERBOSE);

  private final LogWriter log;
  
  DisabledManagedEntityController(AdminDistributedSystem system) {
    this.log = system.getLogWriter();
  }
  
  @Override
  public void start(InternalManagedEntity entity) {
    if (this.verbose){
      this.log.warning("DisabledManagedEntityController#start " + EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public void stop(InternalManagedEntity entity) {
    if (this.verbose){
      this.log.warning("DisabledManagedEntityController#stop " + EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean isRunning(InternalManagedEntity entity) {
    if (this.verbose){
      this.log.warning("DisabledManagedEntityController#isRunning " + EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public String getLog(DistributionLocatorImpl locator) {
    if (this.verbose){
      this.log.warning("DisabledManagedEntityController#getLog " + EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public String buildSSLArguments(DistributedSystemConfig config) {
    if (this.verbose){
      this.log.warning("DisabledManagedEntityController#buildSSLArguments " + EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public String getProductExecutable(InternalManagedEntity entity, String executable) {
    if (this.verbose){
      this.log.warning("DisabledManagedEntityController#getProductExecutable " + EXCEPTION_MESSAGE);
    }
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }
}

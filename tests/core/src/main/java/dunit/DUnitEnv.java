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
/**
 * 
 */
package dunit;

import hydra.GemFireDescription;

import java.util.Properties;

import dunit.eclipse.EclipseDUnitEnv;

/**
 * This class provides an abstraction over the environment
 * that is used to run dunit. This will delegate to the hydra
 * or to the eclipse dunit launcher as needed.
 * 
 * Any dunit tests that rely on hydra configuration should go
 * through here, so that we can separate them out from depending on hydra
 * and run them on a different VM launching system.
 *   
 * @author dsmith
 *
 */
public abstract class DUnitEnv {
  
  public static DUnitEnv instance = new HydraDUnitEnv();
  
  public static final DUnitEnv get() {
    return instance;
  }
  
  public static void set(DUnitEnv eclipseDUnitEnv) {
    instance = eclipseDUnitEnv;
  }
  
  public abstract String getLocatorString();
  
  public abstract String getLocatorAddress();

  public abstract int getLocatorPort();
  
  public abstract Properties getDistributedSystemProperties();

  public abstract GemFireDescription getGemfireDescription();
  
  public abstract int getPid();

  
}

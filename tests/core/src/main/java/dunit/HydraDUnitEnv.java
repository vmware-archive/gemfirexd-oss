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
package dunit;

import hydra.GemFireDescription;
import hydra.GemFirePrms;
import hydra.HostHelper;
import hydra.HydraConfigException;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.Properties;

public class HydraDUnitEnv extends DUnitEnv {


  @Override
  public String getLocatorString() {
//    final GemFireDescription gfd = getGemfireDescription();
//    return TestConfig.getInstance().getMasterDescription()
//    .getLocator(gfd.getDistributedSystem());
    
    java.util.List endpoints = hydra.DistributedSystemHelper
        .getSystemEndpoints();
    hydra.DistributedSystemHelper.Endpoint ep = (hydra.DistributedSystemHelper.Endpoint)endpoints
        .get(0);
    return ep.getId();
  }
  
  @Override
  public String getLocatorAddress() {
    java.util.List endpoints = hydra.DistributedSystemHelper
        .getSystemEndpoints();
    hydra.DistributedSystemHelper.Endpoint ep =
        (hydra.DistributedSystemHelper.Endpoint)endpoints.get(0);
    return ep.getAddress();
  }

  @Override
  public int getLocatorPort() {
    java.util.List endpoints = hydra.DistributedSystemHelper.getSystemEndpoints();
    hydra.DistributedSystemHelper.Endpoint ep = (hydra.DistributedSystemHelper.Endpoint)endpoints.get(0);
    return ep.getPort();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    String gemfireName =
        System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
      if ( gemfireName == null ) {
        String s = "No gemfire name has been specified";
        throw new HydraConfigException(s);
      } else {
        GemFireDescription gfd =
          TestConfig.getInstance().getGemFireDescription(gemfireName);

        String hostName = gfd.getHostDescription().getCanonicalHostName();
        if (HostHelper.isLocalHost(hostName)) {
          return gfd.getDistributedSystemProperties();
        }
        else {
          String s = gemfireName + " is on remote system " + hostName;
          throw new HydraConfigException(s);
        }
      }
  }

  @Override
  public GemFireDescription getGemfireDescription() {
    final String gemFireName = getGemFireName();
    final TestConfig tc = TestConfig.getInstance();
    return tc.getGemFireDescription(gemFireName);
  }
  
  private String getGemFireName() {
    // Get the name of distributed system
    String gemFireName = System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
    if (gemFireName == null) {
      throw new HydraConfigException("No gemfire name has been specified");
    }
    return gemFireName;
  }

  @Override
  public int getPid() {
    return RemoteTestModule.getMyPid();
  }
  
  
}

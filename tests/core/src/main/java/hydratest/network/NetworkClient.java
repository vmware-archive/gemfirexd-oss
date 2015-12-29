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

package hydratest.network;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import hydra.*;

/**
 * Tasks for testing hydra network configuration support.
 */
public class NetworkClient {

  public static void hostIsNotUsedTask() {
    String localhost = HostHelper.getLocalHost();
    try {
      NetworkHelper.dropConnectionOneWay(localhost, "frip");
      String s = "Failed to catch unknown host";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Target host not in use";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
    try {
      NetworkHelper.dropConnectionTwoWay(localhost, "frip");
      String s = "Failed to catch unknown host";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Target host not in use";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
  }

  public static void sourceIsMasterTask() {
    String masterhost = TestConfig.getInstance().getMasterDescription()
                                  .getVmDescription().getHostDescription()
                                  .getHostName();
    String remotehost = TestConfig.getInstance().getHostDescription("remote")
                                  .getHostName();
    try {
      NetworkHelper.dropConnectionOneWay(masterhost, remotehost);
      String s = "Failed to catch source=master";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Source host is the master host";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
    try {
      NetworkHelper.dropConnectionTwoWay(masterhost, remotehost);
      String s = "Failed to catch source=master";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Source host is the master host";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
  }

  public static void targetIsMasterTask() {
    String masterhost = TestConfig.getInstance().getMasterDescription()
                                  .getVmDescription().getHostDescription()
                                  .getHostName();
    String remotehost = TestConfig.getInstance().getHostDescription("remote")
                                  .getHostName();
    try {
      NetworkHelper.dropConnectionOneWay(remotehost, masterhost);
      String s = "Failed to catch target=master";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Target host is the master host";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
    try {
      NetworkHelper.dropConnectionTwoWay(remotehost, masterhost);
      String s = "Failed to catch target=master";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Target host is the master host";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
  }

  public static void sourceIsTargetTask() {
    String remotehost = TestConfig.getInstance().getHostDescription("remote")
                                  .getHostName();
    try {
      NetworkHelper.dropConnectionOneWay(remotehost, remotehost);
      String s = "Failed to catch source=target";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Source host equals target host";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
    try {
      NetworkHelper.dropConnectionTwoWay(remotehost, remotehost);
      String s = "Failed to catch source=target";
      throw new HydraRuntimeException(s);
    } catch (IllegalArgumentException e) {
      Log.getLogWriter().warning(e.getMessage());
      String err = "Source host equals target host";
      if (e.getMessage().indexOf(err) == -1) {
        throw e;
      }
    }
  }

  public static void dropRestoreTask() {
    String remote1 = TestConfig.getInstance().getHostDescription("remote1")
                               .getHostName();
    String remote2 = TestConfig.getInstance().getHostDescription("remote2")
                               .getHostName();
    NetworkHelper.printConnectionState();
    NetworkHelper.dropConnectionTwoWay(remote1, remote2);
    NetworkHelper.printConnectionState();
    NetworkHelper.restoreConnectionTwoWay(remote2, remote1);
    NetworkHelper.printConnectionState();
  }

  public static void dropRestoreTask2() {
    String remote1 = TestConfig.getInstance().getHostDescription("remote1")
                               .getHostName();
    String remote2 = TestConfig.getInstance().getHostDescription("remote2")
                               .getHostName();
    String remote3 = TestConfig.getInstance().getHostDescription("remote3")
                               .getHostName();
    NetworkHelper.printConnectionState();
    NetworkHelper.dropConnectionTwoWay(remote1, remote2);
    NetworkHelper.printConnectionState();
    NetworkHelper.dropConnectionTwoWay(remote1, remote3);
    NetworkHelper.printConnectionState();
    NetworkHelper.dropConnectionOneWay(remote2, remote3);
    NetworkHelper.printConnectionState();
    NetworkHelper.restoreConnectionOneWay(remote2, remote1);
    NetworkHelper.printConnectionState();
    NetworkHelper.restoreConnectionOneWay(remote1, remote2);
    NetworkHelper.printConnectionState();
    NetworkHelper.restoreConnectionTwoWay(remote3, remote1);
    NetworkHelper.printConnectionState();
    NetworkHelper.restoreConnectionOneWay(remote2, remote3);
    NetworkHelper.printConnectionState();
  }

  public static void dropClientServerTask() {
    NetworkHelper.showConnectionState();
    String bridge = TestConfig.getInstance().getHostDescription("bridge")
                              .getHostName();
    String edge   = TestConfig.getInstance().getHostDescription("edge")
                              .getHostName();
    NetworkHelper.printConnectionState();
    NetworkHelper.dropConnectionTwoWay(bridge, edge);
    NetworkHelper.printConnectionState();
    NetworkHelper.showConnectionState();
  }

  public static void restoreClientServerTask() {
    NetworkHelper.showConnectionState();
    String bridge = TestConfig.getInstance().getHostDescription("bridge")
                              .getHostName();
    String edge   = TestConfig.getInstance().getHostDescription("edge")
                              .getHostName();
    NetworkHelper.printConnectionState();
    NetworkHelper.restoreConnectionTwoWay(bridge, edge);
    NetworkHelper.printConnectionState();
    NetworkHelper.showConnectionState();
  }

  public static void openCacheTask() {
    NetworkHelper.showConnectionState();
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    RegionHelper.createRegion(ConfigPrms.getRegionConfig());
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    if (bridgeConfig != null) {
      BridgeHelper.startBridgeServer(bridgeConfig);
    }
    NetworkHelper.showConnectionState();
  }

  public static void putDataNetDownTask() {
    NetworkHelper.showConnectionState();
    Region r = RegionHelper.getRegion("DefaultRegion");
    try {
      r.put(new Integer(0), new Integer(0));
      String s = "Failed to get exception";
      throw new HydraRuntimeException(s);
    } catch (NoAvailableServersException e) {
    }
    Log.getLogWriter().info("Got expected exception");
  }

  public static void dummyTask() {
  }
}

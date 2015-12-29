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

package hydratest.security;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import hydra.*;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Tasks for testing hydra security support.
 */
public class SecurityTestClient {

  private static Region TheRegion;

  public static void connectTask() {
    DistributedSystem d = DistributedSystemHelper.connect();
    Properties p = DistributedSystemHelper.getDistributedSystem()
                                          .getSecurityProperties();
    String s = "Connected with security properties: " + p;
    Log.getLogWriter().info(s);
    d.getLogWriter().info(s);
    d.getSecurityLogWriter().info(s);
  }

  public static synchronized void openCacheTask() {
    if (TheRegion == null) {
      String cacheConfig = ConfigPrms.getCacheConfig();
      String regionConfig = ConfigPrms.getRegionConfig();
      String bridgeConfig = ConfigPrms.getBridgeConfig();

      Cache cache = CacheHelper.createCache(cacheConfig);
      TheRegion = RegionHelper.createRegion(regionConfig);

      if (bridgeConfig != null) {
        BridgeHelper.startBridgeServer(bridgeConfig);
      }
    }
  }

  public static synchronized void closeCacheTask() {
    if (TheRegion != null) {
      BridgeHelper.stopBridgeServer();
      CacheHelper.closeCache();
      TheRegion = null;
    }
  }

  public static void putTask() {
    for (int i = 0; i < 10000; i++) {
      TheRegion.put("key" + i, "value" + i);
    }
  }

  public static void checkClientPropertiesTask() {
    Properties p = DistributedSystemHelper.getDistributedSystem().getSecurityProperties();
    Log.getLogWriter().info("Using security properties: " + p);

    Long key;
    String val;

    key = ClientPrms.clientNoDefaultNoSetNoTask;
    val = p.getProperty(ClientPrms.CLIENT_NO_DEFAULT_NO_SET_NO_TASK_NAME);
    checkProperty(key, val, "Client");
    key = ClientPrms.clientHasDefaultNoSetNoTask;
    val = p.getProperty(ClientPrms.CLIENT_HAS_DEFAULT_NO_SET_NO_TASK_NAME);
    checkProperty(key, val, "Client");
    key = ClientPrms.clientNoDefaultHasSetNoTask;
    val = p.getProperty(ClientPrms.CLIENT_NO_DEFAULT_HAS_SET_NO_TASK_NAME);
    checkProperty(key, val, "Client");
    key = ClientPrms.clientHasDefaultHasSetNoTask;
    val = p.getProperty(ClientPrms.CLIENT_HAS_DEFAULT_HAS_SET_NO_TASK_NAME);
    checkProperty(key, val, "Client");
    key = ClientPrms.clientNoDefaultNoSetHasTask;
    val = p.getProperty(ClientPrms.CLIENT_NO_DEFAULT_NO_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Client");
    key = ClientPrms.clientNoDefaultHasSetHasTask;
    val = p.getProperty(ClientPrms.CLIENT_NO_DEFAULT_HAS_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Client");
    key = ClientPrms.clientHasDefaultNoSetHasTask;
    val = p.getProperty(ClientPrms.CLIENT_HAS_DEFAULT_NO_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Client");
    key = ClientPrms.clientHasDefaultHasSetHasTask;
    val = p.getProperty(ClientPrms.CLIENT_HAS_DEFAULT_HAS_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Client");
  }

  public static void checkPeerPropertiesTask() {
    Properties p = DistributedSystemHelper.getDistributedSystem().getSecurityProperties();
    Log.getLogWriter().info("Using security properties: " + p);

    Long key;
    String val;

    key = PeerPrms.peerNoDefaultNoSetNoTask;
    val = p.getProperty(PeerPrms.PEER_NO_DEFAULT_NO_SET_NO_TASK_NAME);
    checkProperty(key, val, "Peer");
    key = PeerPrms.peerHasDefaultNoSetNoTask;
    val = p.getProperty(PeerPrms.PEER_HAS_DEFAULT_NO_SET_NO_TASK_NAME);
    checkProperty(key, val, "Peer");
    key = PeerPrms.peerNoDefaultHasSetNoTask;
    val = p.getProperty(PeerPrms.PEER_NO_DEFAULT_HAS_SET_NO_TASK_NAME);
    checkProperty(key, val, "Peer");
    key = PeerPrms.peerHasDefaultHasSetNoTask;
    val = p.getProperty(PeerPrms.PEER_HAS_DEFAULT_HAS_SET_NO_TASK_NAME);
    checkProperty(key, val, "Peer");
    key = PeerPrms.peerNoDefaultNoSetHasTask;
    val = p.getProperty(PeerPrms.PEER_NO_DEFAULT_NO_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Peer");
    key = PeerPrms.peerNoDefaultHasSetHasTask;
    val = p.getProperty(PeerPrms.PEER_NO_DEFAULT_HAS_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Peer");
    key = PeerPrms.peerHasDefaultNoSetHasTask;
    val = p.getProperty(PeerPrms.PEER_HAS_DEFAULT_NO_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Peer");
    key = PeerPrms.peerHasDefaultHasSetHasTask;
    val = p.getProperty(PeerPrms.PEER_HAS_DEFAULT_HAS_SET_HAS_TASK_NAME);
    checkProperty(key, val, "Peer");
  }

  private static void checkProperty(Long key, String actualVal, String prefix) {
    String expectedVal;
    String name = BasePrms.nameForKey(key);
    if (name.indexOf("HasTask") != -1) {
      expectedVal = prefix + "TaskVal";
    } else if (name.indexOf("HasSet") != -1) {
      expectedVal = prefix + "SetVal";
    } else if (name.indexOf("HasDefault") != -1) {
      expectedVal = prefix + "DefaultVal";
    } else {
      expectedVal = "";
    }
    if (actualVal == null || !actualVal.equals(expectedVal)) {
      String s = name + " got value: " + actualVal + ", expected: " + expectedVal;
      throw new HydraRuntimeException(s);
    }
  }

  public static void validatePathTask() throws IOException {
    Properties p = DistributedSystemHelper.getDistributedSystem()
                                          .getSecurityProperties();
    String path, expectedPath;

    path = p.getProperty("security-path");
    expectedPath = System.getProperty("JTESTS") + File.separator
                 + "hydratest" + File.separator + "security" + File.separator
                 + RemoteTestModule.getCurrentThread().getThreadGroupName()
                 + ".txt";
    validatePath(path, expectedPath);

    path = p.getProperty("security-authz-xml-uri");
    expectedPath = EnvHelper.expandEnvVars(PathPrms.DEFAULT_AUTHZ_XML_URI);
    validatePath(path, expectedPath);
  }

  private static void validatePath(String path, String expectedPath) {
    if (path == null) {
      String s = "Path is null";
      throw new HydraRuntimeException(s);
    }
    if (path.length() == 0) {
      String s = "Path is not set";
      throw new HydraRuntimeException(s);
    }
    if (!FileUtil.isAbsoluteFilename(path)) {
      String s = "Path is not absolute: " + path;
      throw new HydraRuntimeException(s);
    }
    Log.getLogWriter().info("Actual path is: " + path);
    Log.getLogWriter().info("Expected path: " + expectedPath);
    if (!path.equals(expectedPath)) {
      String s = "Got path value: " + path + ", expected: " + expectedPath;
      throw new HydraRuntimeException(s);
    }
  }
}

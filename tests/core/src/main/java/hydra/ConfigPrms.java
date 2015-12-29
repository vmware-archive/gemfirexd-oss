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

package hydra;

/**
 * A class used to store keys for test configuration settings, and to provide
 * accessors for reading the keys so that they can be set as regular parameters
 * or as task attributes.  See individual javadocs for the meaning of each key.
 * <p>
 * Values of a parameter can use oneof, range, or robing to get a different
 * value each time the parameter is accessed.
 * <p>
 * This class is offered as a convenience to test developers and is not used by
 * the hydra master controller.
 */
public class ConfigPrms extends BasePrms {

  //----------------------------------------------------------------------------
  // cacheConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link CachePrms#names} giving the cache configuration.
   */
  public static Long cacheConfig;

  /**
   * Returns the value of {@link #cacheConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link CachePrms#names}.
   */
  public static String getCacheConfig() {
    return getString(cacheConfig);
  }

  //----------------------------------------------------------------------------
  // clientCacheConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link ClientCachePrms#names} giving the client cache
   * configuration.
   */
  public static Long clientCacheConfig;

  /**
   * Returns the value of {@link #clientCacheConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link ClientCachePrms#names}.
   */
  public static String getClientCacheConfig() {
    return getString(clientCacheConfig);
  }

  //----------------------------------------------------------------------------
  // regionConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link RegionPrms#names} giving the region configuration.
   */
  public static Long regionConfig;

  /**
   * Returns the value of {@link #regionConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link RegionPrms#names}.
   */
  public static String getRegionConfig() {
    return getString(regionConfig);
  }

  //----------------------------------------------------------------------------
  // clientRegionConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link ClientRegionPrms#names} giving the client region
   * configuration.
   */
  public static Long clientRegionConfig;

  /**
   * Returns the value of {@link #clientRegionConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link ClientRegionPrms#names}.
   */
  public static String getClientRegionConfig() {
    return getString(clientRegionConfig);
  }

  //----------------------------------------------------------------------------
  // bridgeConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link BridgePrms#names} giving the bridge configuration.
   */
  public static Long bridgeConfig;

  /**
   * Returns the value of {@link #bridgeConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link BridgePrms#names}.
   */
  public static String getBridgeConfig() {
    return getString(bridgeConfig);
  }

  //----------------------------------------------------------------------------
  // gatewayReceiverConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link GatewayReceiverPrms#names} giving the gateway receiver
   * configuration.
   */
  public static Long gatewayReceiverConfig;

  /**
   * Returns the value of {@link #gatewayReceiverConfig}, or null if it is not
   * set.
   * @throws HydraConfigException if not found in {@link GatewayReceiverPrms
   *                              #names}.
   */
  public static String getGatewayReceiverConfig() {
    return getString(gatewayReceiverConfig);
  }

  //----------------------------------------------------------------------------
  // gatewaySenderConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link GatewaySenderPrms#names} giving the gateway sender
   * configuration.
   */
  public static Long gatewaySenderConfig;

  /**
   * Returns the value of {@link #gatewaySenderConfig}, or null if it is not
   * set.
   * @throws HydraConfigException if not found in {@link GatewaySenderPrms
   *                              #names}.
   */
  public static String getGatewaySenderConfig() {
    return getString(gatewaySenderConfig);
  }

  //----------------------------------------------------------------------------
  // asyncEventQueueConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link AsyncEventQueuePrms#names} giving the async event queue
   * configuration.
   */
  public static Long asyncEventQueueConfig;

  /**
   * Returns the value of {@link #asyncEventQueueConfig}, or null if it is not
   * set.
   * @throws HydraConfigException if not found in {@link AsyncEventQueuePrms
   *                              #names}.
   */
  public static String getAsyncEventQueueConfig() {
    return getString(asyncEventQueueConfig);
  }

  //----------------------------------------------------------------------------
  // gatewayConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link GatewayPrms#names} giving the gateway configuration.
   */
  public static Long gatewayConfig;

  /**
   * Returns the value of {@link #gatewayConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link GatewayPrms#names}.
   */
  public static String getGatewayConfig() {
    return getString(gatewayConfig);
  }

  //----------------------------------------------------------------------------
  // gatewayHubConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link GatewayHubPrms#names} giving the gateway hub
   * configuration.
   */
  public static Long gatewayHubConfig;

  /**
   * Returns the value of {@link #gatewayHubConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link GatewayHubPrms#names}.
   */
  public static String getGatewayHubConfig() {
    return getString(gatewayHubConfig);
  }

  //----------------------------------------------------------------------------
  // diskConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link DiskStorePrms#names} giving the disk store
   * configuration.
   */
  public static Long diskStoreConfig;

  /**
   * Returns the value of {@link #diskConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link DiskStorePrms#names}.
   */
  public static String getDiskStoreConfig() {
    return getString(diskStoreConfig);
  }

  //----------------------------------------------------------------------------
  // poolConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link PoolPrms#names} giving the pool configuration.
   */
  public static Long poolConfig;

  /**
   * Returns the value of {@link #poolConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link PoolPrms#names}.
   */
  public static String getPoolConfig() {
    return getString(poolConfig);
  }

  //----------------------------------------------------------------------------
  // adminConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link AdminPrms#names} giving the admin configuration.
   */
  public static Long adminConfig;

  /**
   * Returns the value of {@link #adminConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link AdminPrms#names}.
   */
  public static String getAdminConfig() {
    return getString(adminConfig);
  }

  //----------------------------------------------------------------------------
  // agentConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link AgentPrms#names} giving the agent configuration.
   */
  public static Long agentConfig;

  /**
   * Returns the value of {@link #agentConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link AgentPrms#names}.
   */
  public static String getAgentConfig() {
    return getString(agentConfig);
  }

  //----------------------------------------------------------------------------
  // hadoopConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link HadoopPrms#names} giving the hadoop configuration.
   */
  public static Long hadoopConfig;

  /**
   * Returns the value of {@link #hadoopConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link HadoopPrms#names}.
   */
  public static String getHadoopConfig() {
    return getString(hadoopConfig);
  }

  //----------------------------------------------------------------------------
  // hdfsStoreConfig 
  //----------------------------------------------------------------------------

  /**
   * (String)
   * An entry in {@link HDFSStorePrms#names} giving the HDFS store
   * configuration.
   */
  public static Long hdfsStoreConfig;

  /**
   * Returns the value of {@link #hdfsStoreConfig}, or null if it is not set.
   * @throws HydraConfigException if not found in {@link HDFSStorePrms#names}.
   */
  public static String getHDFSStoreConfig() {
    return getString(hdfsStoreConfig);
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  /**
   * Returns the string at the given key, checking task attributes as well as
   * the regular configuration hashtable.
   */
  public static String getString(Long key) {
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      return null;

    } else {
      Object description;
      if (key == cacheConfig) {
        description = TestConfig.getInstance().getCacheDescription(val);

      } else if (key == clientCacheConfig) {
        description = TestConfig.getInstance().getClientCacheDescription(val);

      } else if (key == regionConfig) {
        description = TestConfig.getInstance().getRegionDescription(val);

      } else if (key == clientRegionConfig) {
        description = TestConfig.getInstance().getClientRegionDescription(val);

      } else if (key == bridgeConfig) {
        description = TestConfig.getInstance().getBridgeDescription(val);

      } else if (key == gatewayReceiverConfig) {
        description = TestConfig.getInstance().getGatewayReceiverDescription(val);

      } else if (key == gatewaySenderConfig) {
        description = TestConfig.getInstance().getGatewaySenderDescription(val);

      } else if (key == asyncEventQueueConfig) {
        description = TestConfig.getInstance().getAsyncEventQueueDescription(val);

      } else if (key == gatewayConfig) {
        description = TestConfig.getInstance().getGatewayDescription(val);

      } else if (key == gatewayHubConfig) {
        description = TestConfig.getInstance().getGatewayHubDescription(val);

      } else if (key == diskStoreConfig) {
        description = TestConfig.getInstance().getDiskStoreDescription(val);

      } else if (key == poolConfig) {
        description = TestConfig.getInstance().getPoolDescription(val);

      } else if (key == adminConfig) {
        description = TestConfig.getInstance().getAdminDescription(val);

      } else if (key == agentConfig) {
        description = TestConfig.getInstance().getAgentDescription(val);

      } else if (key == hadoopConfig) {
        description = TestConfig.getInstance().getHadoopDescription(val);

      } else if (key == hdfsStoreConfig) {
        description = TestConfig.getInstance().getHDFSStoreDescription(val);

      } else {
        throw new HydraInternalException("Unknown key: " + nameForKey(key));
      }
      if (description == null) {
        String s = "Description for configuration name \"" + nameForKey(key)
                 + "\" not found: " + val;
        throw new HydraConfigException(s);
      }
    }
    return val;
  }

  static {
    setValues(ConfigPrms.class);
  }
  public static void main(String args[]) {
    dumpKeys();
  }
}

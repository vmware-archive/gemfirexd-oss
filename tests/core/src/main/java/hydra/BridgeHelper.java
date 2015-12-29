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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Provides support for bridge servers running in hydra-managed client VMs.
 */
public class BridgeHelper {

//------------------------------------------------------------------------------
// CacheServer
//------------------------------------------------------------------------------

  /** Name of the bridge description used to create the bridge server */
  private static String TheBridgeConfig;

  /**
   * Creates and starts a bridge server in the current cache using the given
   * bridge configuration.
   * @see #createBridgeServer(String)
   * @see #startBridgeServer
   */
  public static synchronized CacheServer startBridgeServer(
                                              String bridgeConfig) {
    CacheServer bridge = createBridgeServer(bridgeConfig);
    startBridgeServer();
    return bridge;
  }

  /**
   * Creates a bridge server in the current cache.  The bridge is configured
   * using the {@link BridgeDescription} corresponding to the given bridge
   * configuration from {@link BridgePrms#names}.
   * <p>
   * Upon initial creation, selects a random port and registers the {@link
   * Endpoint} in the {@link BridgeBlackboard} map.  Starts of the bridge
   * server will always use this port.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing bridge server.
   */
  public static synchronized CacheServer createBridgeServer(
                                                String bridgeConfig) {
    CacheServer bridge = getBridgeServer();
    if (bridge == null) {

      // look up the bridge configuration
      BridgeDescription bd = getBridgeDescription(bridgeConfig);

      // create the bridge
      log("Adding bridge server to cache");
      bridge = CacheFactory.getAnyInstance().addCacheServer();

      // configure the bridge
      log("Determining bridge server port");
      int port = getEndpoint(bd.getGroups()).getPort();
      log("Configuring bridge server with port: " + port);
      bd.configure(bridge, port);
      log("Configured bridge server: " + bridgeServerToString(bridge));

      // save the bridge config for future reference
      TheBridgeConfig = bridgeConfig;

    } else if (TheBridgeConfig == null) {
      // block attempt to create bridge in multiple ways
      String s = "Bridge server was already created without BridgeHelper using"
               + " an unknown, and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheBridgeConfig.equals(bridgeConfig)) {
      // block attempt to recreate bridge with clashing configuration name
      String s = "Bridge server already exists using logical bridge server"
               + " configuration named " + TheBridgeConfig
               + ", cannot also use " + bridgeConfig;
      throw new HydraRuntimeException(s);

    } // else it was already created with this configuration, which is fine

    return bridge;
  }

  /**
   * Starts the existing bridge server in the current cache.
   * <p>
   * Lazily creates the gateway disk store specified by {@link BridgePrms
   * #diskStoreName}, using {@link DiskStoreHelper.createDiskStore(String)}.
   *
   * @throws HydraRuntimeException if the bridge has not been created.
   */
  public static synchronized CacheServer startBridgeServer() {
    CacheServer bridge = getBridgeServer();
    if (bridge == null) {
      String s = "Bridge server has not been created yet";
      throw new HydraRuntimeException(s);
    }
    // start the bridge if necessary
    if (!bridge.isRunning()) {
      log("Starting bridge server: " + bridgeServerToString(bridge));
      BridgeVersionHelper.createDiskStore(bridge);
      long startTimeMs = System.currentTimeMillis();
      while (true) {
        try {
          bridge.start();
          log("Started bridge server: " + bridgeServerToString(bridge));
          break;
        } catch (IOException e) {
          if (!PortHelper.retrySocketBind(e, startTimeMs)) {
            String s = "Problem starting bridge" + bridgeServerToString(bridge);
            throw new HydraRuntimeException(s, e);
          }
        }
      }
    }
    return bridge;
  }

  /**
   * Returns the bridge server for the current cache, or null if no bridge
   * server or cache exists.
   */
  public static synchronized CacheServer getBridgeServer() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      return null;
    } else {
      Collection bridges = cache.getCacheServers();
      if (bridges == null || bridges.size() == 0) {
        return null;
      } else if (bridges.size() == 1) {
        return (CacheServer)bridges.iterator().next();
      } else {
        throw new UnsupportedOperationException("Multiple bridge servers");
      }
    }
  }

  /**
   * Stops the bridge server in the current cache, if it exists.
   */
  public static synchronized void stopBridgeServer() {
    CacheServer bridge = getBridgeServer();
    if (bridge != null) {
      if (bridge.isRunning()) {
        log("Stopping bridge server: " + bridgeServerToString(bridge));
        bridge.stop();
        // TheBridgeConfig = null; // cannot remove server, so config is fixed
        log("Stopped bridge server: " + bridgeServerToString(bridge));
      }
    }
  }

  /**
   * Returns the given bridge server as a string.
   */
  public static String bridgeServerToString(CacheServer bridge) {
    return BridgeDescription.bridgeServerToString(bridge);
  }

//------------------------------------------------------------------------------
// BridgeDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link BridgeDescription} with the given configuration name
   * from {@link BridgePrms#names}.
   */
  public static BridgeDescription getBridgeDescription(String bridgeConfig) {
    if (bridgeConfig == null) {
      throw new IllegalArgumentException("bridgeConfig cannot be null");
    }
    log("Looking up bridge server config: " + bridgeConfig);
    BridgeDescription bd = TestConfig.getInstance()
                                     .getBridgeDescription(bridgeConfig);
    if (bd == null) {
      String s = bridgeConfig + " not found in "
               + BasePrms.nameForKey(BridgePrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up bridge server config:\n" + bd);
    return bd;
  }

//------------------------------------------------------------------------------
// Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns all bridge server endpoints from the {@link BridgeBlackboard} map,
   * a possibly empty list.  This includes all servers that have ever started,
   * regardless of their distributed system or current active status.
   */
  public static synchronized List<Endpoint> getEndpoints() {
    return new ArrayList(BridgeBlackboard.getInstance()
                                         .getSharedMap().getMap().values());
  }

  /**
   * Returns all bridge server endpoints) for the specified
   * distributed system from the {@link BridgeBlackboard} map, a
   * possibly empty list.  This includes all servers that have ever
   * started for the system, regardless of their current active status.
   */
  public static synchronized List getEndpoints(String distributedSystemName) {
    List endpoints = new ArrayList();
    for (Iterator i = getEndpoints().iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint)i.next();
      if (endpoint.getDistributedSystemName().equals(distributedSystemName)) {
        endpoints.add(endpoint);
      }
    }
    return endpoints;
  }

  /**
   * Returns all bridge server endpoints) for the specified
   * distributed system in the given server group from the {@link
   * BridgeBlackboard} map, a possibly empty list.  This includes all servers
   * that have ever started for the system, regardless of their current active
   * status.
   */
  public static synchronized List getEndpointsInGroup(
         String distributedSystemName, String group) {
    if (group== null) {
      String s = "Group cannot be null";
      throw new IllegalArgumentException(s);
    }
    List<Endpoint> endpoints = new ArrayList();
    for (Endpoint endpoint : getEndpoints()) {
      if (endpoint.getDistributedSystemName().equals(distributedSystemName) &&
          endpoint.getGroups().contains(group)) {
        endpoints.add(endpoint);
      }
    }
    return endpoints;
  }

  /**
   * Returns all remote bridge server endpoints from the {@link
   * BridgeBlackboard} map, a possibly empty list.  This includes all servers
   * that have ever started, except for the caller (if it is a server),
   * regardless of their distributed system or current active status.
   */
  public static synchronized List getRemoteEndpoints() {
    List endpoints = new ArrayList();
    int myVmid = RemoteTestModule.getMyVmid();
    for (Iterator i = getEndpoints().iterator(); i.hasNext();) {
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)i.next();
      if (endpoint.getVmid() != myVmid) {
        endpoints.add(endpoint);
      }
    }
    return endpoints;
  }

  /**
   * Returns all remote bridge server endpoints for the specified distributed
   * system from the {@link BridgeBlackboard} map, a possibly empty list.
   * This includes all servers that have ever started, except for the caller
   * (if it is a server), regardless of their current active status.
   */
  public static synchronized List getRemoteEndpoints(String distributedSystem) {
    List endpoints = new ArrayList();
    int myVmid = RemoteTestModule.getMyVmid();
    for (Iterator i = getEndpoints().iterator(); i.hasNext();) {
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)i.next();
      if (endpoint.getVmid() != myVmid && 
          endpoint.getDistributedSystemName().equals(distributedSystem)) {
        endpoints.add(endpoint);
      }
    }
    return endpoints;
  }

  /**
   * Gets the bridge server endpoint for this VM from the shared {@link
   * BridgeBlackboard} map using the given server groups.  Lazily selects
   * a random port and registers the endpoint in the map.
   */
  protected static synchronized Endpoint getEndpoint(String[] groups) {
    int vmid = RemoteTestModule.getMyVmid();
    Endpoint endpoint =
      (Endpoint)BridgeBlackboard.getInstance().getSharedMap().get(vmid);
    if (endpoint == null) {
      int port = PortHelper.getRandomPort();
      endpoint = generateEndpoint(vmid, port, groups);
    }
    return endpoint;
  }

  /**
   * Gets the bridge server endpoint for this VM from the shared {@link
   * BridgeBlackboard} map using the given port and server groups.
   * Registers the endpoint in the map.
   */
  protected static synchronized Endpoint getEndpoint(int port,
                                                     String[] groups) {
    int vmid = RemoteTestModule.getMyVmid();
    Endpoint endpoint =
      (Endpoint)BridgeBlackboard.getInstance().getSharedMap().get(vmid);
    if (endpoint == null) {
      endpoint = generateEndpoint(vmid, port, groups);
    }
    return endpoint;
  }

  /**
   * Gets the bridge server endpoint for this VM from the shared {@link
   * BridgeBlackboard} map using the given port and server groups.
   * Registers the endpoint in the map.
   */
  private static synchronized Endpoint generateEndpoint(int vmid, int port,
                                                        String[] groups) {
    String name = RemoteTestModule.getMyClientName();
    String host = HostHelper.getLocalHost();
    String addr = HostHelper.getHostAddress();

    String ds = DistributedSystemHelper.getDistributedSystemName();
    Endpoint endpoint = new Endpoint(name, vmid, host, addr, port, ds,
                                     Arrays.asList(groups));
    BridgeBlackboard.getInstance().getSharedMap().put(vmid, endpoint);
    log("Generated bridge server endpoint: " + endpoint + " in ds: " + ds
       + " with groups: " + endpoint.getGroups());
    return endpoint;
  }

  /**
   * Represents the endpoint for a bridge server.
   */
  public static class Endpoint implements Contact, Serializable {
    String id, name, host, addr, ds;
    int vmid, port;
    List<String> groups;

    /**
     * Creates an endpoint for a bridge server.
     *
     * @param name the logical hydra client VM name from {@link
     *             ClientPrms#names} found via the {@link
     *             ClientPrms#CLIENT_NAME_PROPERTY} system property.
     * @param vmid the logical hydra client VM ID found via {@link
     *             RemoteTestModule#getMyVmid}.
     * @param host the bridge server host.
     * @param addr the bridge server address.
     * @param port the bridge server port.
     * @param ds   the bridge server distributed system name.
     * @param groups the bridge server groups.
     */
    public Endpoint(String name, int vmid, String host,
                    String addr, int port, String ds, List<String> groups) {
      if (name == null) {
        throw new IllegalArgumentException("name cannot be null");
      }
      if (host == null) {
        throw new IllegalArgumentException("host cannot be null");
      }
      if (addr == null) {
        throw new IllegalArgumentException("addr cannot be null");
      }
      if (ds == null) {
        throw new IllegalArgumentException("ds cannot be null");
      }

      this.name = name;
      this.vmid = vmid;
      this.host = host;
      this.addr = addr;
      this.port = port;
      this.ds   = ds;
      this.groups = groups;

      this.id = "vm_" + this.vmid + "_" + this.name + "_" + this.host;
    }

    /**
     * Returns the unique bridge server logical endpoint ID.
     */
    public String getId() {
      return this.id;
    }

    /**
     * Returns the bridge server logical VM name.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns the bridge server logical VM ID.
     */
    public int getVmid() {
      return this.vmid;
    }

    /**
     * Returns the bridge server host.
     */
    public String getHost() {
      return this.host;
    }

    /**
     * Returns the bridge server address.
     */
    public String getAddress() {
      return this.addr;
    }

    /**
     * Returns the bridge server port.
     */
    public int getPort() {
      return this.port;
    }

    /**
     * Returns the bridge server distributed system name.
     */
    public String getDistributedSystemName() {
      return this.ds;
    }

    /**
     * Returns the bridge server groups.
     */
    public List<String> getGroups() {
      return this.groups;
    }

    public boolean equals(Object obj) {
      if (obj instanceof Endpoint) {
        Endpoint endpoint = (Endpoint)obj;
        return endpoint.getName().equals(this.getName())
            && endpoint.getVmid() == this.getVmid()
            && endpoint.getHost().equals(this.getHost())
            && endpoint.getAddress().equals(this.getAddress())
            && endpoint.getPort() == this.getPort()
            && endpoint.getDistributedSystemName() == this.getDistributedSystemName();
      }
      return false;
    }

    public int hashCode() {
      return this.port;
    }

    /**
     * Returns the endpoint as a string suitable for use in endpoint properties.
     */
    public String toString() {
      return this.id + "=" + addr + ":" + port;
    }
  }

//------------------------------------------------------------------------------
// Log
//------------------------------------------------------------------------------

  private static LogWriter log;
  private static synchronized void log(String s) {
    if (log == null) {
      log = Log.getLogWriter();
    }
    if (log.infoEnabled()) {
      log.info(s);
    }
  }
}

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
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Provides support for gateway hubs running in hydra-managed client VMs.
 */
public class GatewayHubHelper {

//------------------------------------------------------------------------------
// GatewayHub
//------------------------------------------------------------------------------

  /** Name of the gateway hub description used to create the gateway hub */
  private static String TheGatewayHubConfig;

  /** Name of the gateway description used to create the gateways */
  private static String TheGatewayConfig;

  /** Name of the gateway description used to create the WBCLs */
  private static String TheWBCLGatewayConfig;

  /**
   * Creates a gateway hub in the current cache.  The hub is configured using
   * the {@link GatewayHubDescription} corresponding to the given hub
   * configuration from {@link GatewayHubPrms#names}.
   * <p>
   * Upon initial creation, if {@link GatewayHubPrms#acceptGatewayConnections}
   * is true, selects a random port and registers the {@link Endpoint} in the
   * {@link GatewayHubBlackboard} map for use by gateways.  Starts of the hub
   * will always use this port.
   * <p>
   * If {@link GatewayHubPrms#haEnabled} is true, the hub ID is the name of its
   * distributed system.  Otherwise the hub is given a unique ID.
   * <p>
   * Gateways are added using {@link addGateways(String)}.  The hub is
   * started using {@link startGatewayHub()}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing gateway hub.
   */
  public static synchronized GatewayHub createGatewayHub(
                                        String gatewayHubConfig) {
    GatewayHub hub = getGatewayHub();
    if (hub == null) {

      // look up the gateway hub configuration
      GatewayHubDescription ghd = getGatewayHubDescription(gatewayHubConfig);

      // set system property for asynchronous distribution BEFORE creating hub
      ghd.setAsynchronousGatewayDistributionEnabledProperty();

      String id = getId(ghd);
      int port = getPort(ghd, id);

      // create the hub
      log("Creating gateway hub with id: " + id + " and port: " + port);
      hub = CacheFactory.getAnyInstance().addGatewayHub(id, port);

      // configure the hub
      log("Configuring gateway hub: " + gatewayHubToString(hub));
      ghd.configure(hub);
      log("Configured gateway hub: " + gatewayHubToString(hub));

      // save the gateway hub config for future reference
      TheGatewayHubConfig = gatewayHubConfig;

    } else if (TheGatewayHubConfig == null) {
      // block attempt to create gateway hub in multiple ways
      String s = "Gateway hub was already created without GatewayHubHelper"
               + " using an unknown, and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheGatewayHubConfig.equals(gatewayHubConfig)) {
      // block attempt to recreate gateway hub with clashing configuration name
      String s = "Gateway hub already exists using logical gateway hub"
               + " configuration named " + TheGatewayHubConfig
               + ", cannot also use " + gatewayHubConfig;
      throw new HydraRuntimeException(s);

    } // else it was already created with this configuration, which is fine

    return hub;
  }

  /**
   * Adds gateways to the current gateway hub, one per unique distributed system
   * different than the hub's.  The gateways are configured using the {@link
   * GatewayDescription} corresponding to the given gateway configuration from
   * {@link GatewayPrms#names}.
   * <p>
   * IMPORTANT: This method must be invoked after all hub VMs have completed
   * execution of {@link #createGatewayHub(String)}, so that their endpoints
   * are available for discovery.  This is most easily done by creating gateway
   * hubs in an INITTASK and adding gateways in a later task, allowing the
   * hydra task scheduler to provide the necessary synchronization.
   *
   * @throws HydraRuntimeException if the gateway hub has not been created.
   */
  public static synchronized void addGateways(String gatewayConfig) {
    GatewayHub hub = getGatewayHub();
    if (hub == null) {
      throw new HydraRuntimeException("Gateway hub not created yet");
    }
    if (hub.getGateways().size() == 0) {
      log("Adding gateways to hub using gateway config: " + gatewayConfig);
      GatewayDescription gd =
                GatewayHelper.getGatewayDescription(gatewayConfig);
      List endpoints = getEndpoints();
      String ownds = DistributedSystemHelper.getDistributedSystemName();
      Set dsnames = getDistributedSystemNames(endpoints);
      for (Iterator i = dsnames.iterator(); i.hasNext();) {
        String dsname = (String)i.next();
        if (!dsname.equals(ownds)) {
          int vmid = RemoteTestModule.getMyVmid();
          String id = dsname; // remote distributed system

          log("Configuring gateway with id: " + id);
          Gateway gateway = GatewayHubVersionHelper.addGateway(hub, id, gd);
          gd.configure(gateway, dsname, endpoints, true);
          log("Configured gateway: " + GatewayHelper.gatewayToString(gateway));
        }
      }
      // save the gateway config for future reference
      TheGatewayConfig = gatewayConfig;

      log("Added gateways to hub");

    } else if (TheGatewayConfig == null) {
      // block attempt to create gateway in multiple ways
      String s = "Gateways were already created without GatewayHubHelper"
               + " using an unknown, and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheGatewayConfig.equals(gatewayConfig)) {
      // block attempt to recreate gateways with clashing configuration name
      String s = "Gateways already exist using logical gateway"
               + " configuration named " + TheGatewayConfig
               + ", cannot also use " + gatewayConfig;
      throw new HydraRuntimeException(s);

    } // else they were already created with this configuration, which is fine
  }

  /**
   * Adds {@link GatewayPrms#concurrencyLevel} WBCL gateways to the current
   * gateway hub.  The gateways are configured using the {@link
   * GatewayDescription} corresponding to the given gateway configuration from
   * {@link GatewayPrms#names}.
   *
   * @throws HydraRuntimeException if the gateway hub has not been created.
   */
  public static synchronized void addWBCLGateway(String gatewayConfig) {
    GatewayHub hub = getGatewayHub();
    if (hub == null) {
      throw new HydraRuntimeException("Gateway hub not created yet");
    }
    if (hub.getGateways().size() == 0) {
      log("Adding WBCL gateway to hub using gateway config: " + gatewayConfig);
      GatewayDescription gd =
                GatewayHelper.getGatewayDescription(gatewayConfig);
      String id = "wbcl";
      log("Configuring WBCL gateway with id: " + id);
      Gateway gateway = GatewayHubVersionHelper.addGateway(hub, id, gd);
      gd.configure(gateway, null, null, true);
      log("Configured WBCL gateway: " + GatewayHelper.gatewayToString(gateway));

      // save the gateway config for future reference
      TheWBCLGatewayConfig = gatewayConfig;

      log("Added WBCL gateways to hub");

    } else if (TheWBCLGatewayConfig == null) {
      // block attempt to create gateway in multiple ways
      String s = "WBCL gateway was already created without GatewayHubHelper"
               + " using an unknown, and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheWBCLGatewayConfig.equals(gatewayConfig)) {
      // block attempt to recreate gateways with clashing configuration name
      String s = "WBCL gateway already exists using logical gateway"
               + " configuration named " + TheWBCLGatewayConfig
               + ", cannot also use " + gatewayConfig;
      throw new HydraRuntimeException(s);

    } // else it was already created with this configuration, which is fine
  }

  /**
   * Starts the gateway hub in the current cache, if it exists.
   * <p>
   * Lazily creates the gateway disk stores specified by {@link GatewayPrms
   * #diskStoreName}, using {@link DiskStoreHelper.createDiskStore(String)}.
   */
  public static synchronized void startGatewayHub() {
    GatewayHub hub = getGatewayHub();
    if (hub == null) {
      String s = "Gateway hub has not been created yet";
      throw new HydraRuntimeException(s);
    }
    if (!GatewayHubVersionHelper.isRunning(hub)) {
      // regions must be created before starting hub
      Set regions = CacheHelper.getCache().rootRegions();
      if (regions == null || regions.size() == 0) {
        String s = "Gateway hub cache has no regions yet";
        throw new HydraRuntimeException(s);
      }
      log("Starting gateway hub: " + gatewayHubToString(hub));
      GatewayHubVersionHelper.createDiskStores(hub);
      long startTimeMs = System.currentTimeMillis();
      while (true) {
        try {
          hub.start();
          // TheGatewayHubConfig = null; // cannot remove hub, so config is fixed
          log("Started gateway hub: " + hub);
          break;
        } catch (IOException e) {
          if (!PortHelper.retrySocketBind(e, startTimeMs)) {
            String s = "Problem starting gateway hub" + gatewayHubToString(hub);
            throw new HydraRuntimeException(s, e);
          }
        }
      }
    }
  }

  /**
   * Returns the gateway hub for the current cache, or null if no gateway
   * hub exists.
   */
  public static synchronized GatewayHub getGatewayHub() {
    return CacheHelper.getCache().getGatewayHub();
  }

  /**
   * Stops the gateway hub in the current cache, if it exists.
   */
  public static synchronized void stopGatewayHub() {
    GatewayHub hub = getGatewayHub();
    if (hub != null) {
      if (GatewayHubVersionHelper.isRunning(hub)) {
        log("Stopping gateway hub: " + gatewayHubToString(hub));
        hub.stop();
        // TheGatewayHubConfig = null; // cannot remove hub, so config is fixed
        log("Stopped gateway hub: " + gatewayHubToString(hub));
      }
    }
  }

  /**
   * Returns the given gateway hub as a string.
   */
  public static String gatewayHubToString(GatewayHub hub) {
    return GatewayHubDescription.gatewayHubToString(hub);
  }

//------------------------------------------------------------------------------
// GatewayHubDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link GatewayHubDescription} with the given configuration name
   * from {@link GatewayHubPrms#names}.
   */
  public static GatewayHubDescription getGatewayHubDescription(
                                  String gatewayHubConfig) {
    if (gatewayHubConfig == null) {
      throw new IllegalArgumentException("gatewayHubConfig cannot be null");
    }
    log("Looking up gateway hub config: " + gatewayHubConfig);
    GatewayHubDescription ghd = TestConfig.getInstance()
                                .getGatewayHubDescription(gatewayHubConfig);
    if (ghd == null) {
      String s = gatewayHubConfig + " not found in "
               + BasePrms.nameForKey(GatewayHubPrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up gateway hub config:\n" + ghd);
    return ghd;
  }

//------------------------------------------------------------------------------
// DistributedSystem
//------------------------------------------------------------------------------

  /**
   * Returns the set of distributed system names for all gateway hub endpoints,
   * a possibly empty list.
   */
  public static Set getDistributedSystemNames(List endpoints) {
    Set names = new HashSet();
    for (Iterator i = endpoints.iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint)i.next();
      names.add(endpoint.getDistributedSystemName());
    }
    return names;
  }

//------------------------------------------------------------------------------
// Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns all gateway hub endpoints from the {@link GatewayHubBlackboard}
   * map, a possibly empty list.  This includes all hubs that have ever started,
   * regardless of their current active status.
   */
  public static synchronized List getEndpoints() {
    return new ArrayList(GatewayHubBlackboard.getInstance()
                                   .getSharedMap().getMap().values());
  }

  /**
   * Returns all remote gateway hub endpoints from the {@link
   * GatewayHubBlackboard} map, a possibly empty list.  This includes all hubs
   * that have ever started, except for the caller (if it is a hub), regardless
   * of their current active status.
   */
  public static synchronized List getRemoteEndpoints() {
    List endpoints = new ArrayList();
    int myVmid = RemoteTestModule.getMyVmid();
    for (Iterator i = getEndpoints().iterator(); i.hasNext();) {
      GatewayHubHelper.Endpoint endpoint = (GatewayHubHelper.Endpoint)i.next();
      if (endpoint.getVmid() != myVmid) {
        endpoints.add(endpoint);
      }
    }
    return endpoints;
  }

  /**
   * Gets the gateway hub endpoint for this VM from the shared {@link
   * GatewayHubBlackboard} map.  Lazily selects a random port and registers
   * the endpoint in the map.
   */
  protected static synchronized Endpoint getEndpoint(String id) {
    Integer vmid = new Integer(RemoteTestModule.getMyVmid());
    Endpoint endpoint =
      (Endpoint)GatewayHubBlackboard.getInstance().getSharedMap().get(vmid);
    if (endpoint == null) {
      String name = RemoteTestModule.getMyClientName();
      String host = HostHelper.getLocalHost();
      String addr = HostHelper.getHostAddress();
      int port = PortHelper.getRandomPort();
      String ds = DistributedSystemHelper.getDistributedSystemName();
      endpoint = new Endpoint(id, name, vmid.intValue(), host, addr, port, ds);
      log("Generated gateway hub endpoint: " + endpoint);
      GatewayHubBlackboard.getInstance().getSharedMap().put(vmid, endpoint);
    }
    return endpoint;
  }

  protected static String getId(GatewayHubDescription ghd) {
    // determine the id and optional port
    String dsname = DistributedSystemHelper.getDistributedSystemName();
    String id = dsname; // this distributed system
    if (!ghd.getHAEnabled()) {
      // make the id unique in this distributed system
      id += "_" + RemoteTestModule.getMyVmid();
    }
    return id;
  }

  protected static int getPort(GatewayHubDescription ghd, String id) {
    log("Determining gateway hub port");
    int port = GatewayHub.DEFAULT_PORT;
    if (ghd.getAcceptGatewayConnections()) {
      port = getEndpoint(id).getPort();
    }
    return port;
  }

  /**
   * Represents the endpoint for a gateway hub.
   */
  public static class Endpoint implements Serializable {
    String id, name, host, addr, ds;
    int vmid, port;

    /**
     * Creates an endpoint for a gateway hub.
     *
     * @param id   the gateway hub id.
     * @param name the logical hydra client VM name from {@link
     *             ClientPrms#names} found via the {@link
     *             ClientPrms#CLIENT_NAME_PROPERTY} system property.
     * @param vmid the logical hydra client VM ID found via {@link
     *             RemoteTestModule#getMyVmid}.
     * @param host the gateway hub host.
     * @param addr the gateway hub IP address.
     * @param port the gateway hub port.
     * @param ds   the gateway hub distributed system name.
     */
    public Endpoint(String id, String name, int vmid, String host, String addr,
                    int port, String ds) {
      if (id == null) {
        throw new IllegalArgumentException("id cannot be null");
      }
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

      this.id   = id;   // happens to be the same as this.ds
      this.name = name;
      this.vmid = vmid;
      this.host = host;
      this.addr = addr;
      this.port = port;
      this.ds   = ds;
    }

    /**
     * Returns the unique gateway hub logical endpoint ID.
     */
    public String getId() {
      return this.id;
    }

    /**
     * Returns the gateway hub logical VM name.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns the gateway hub logical VM ID.
     */
    public int getVmid() {
      return this.vmid;
    }

    /**
     * Returns the gateway hub host.
     */
    public String getHost() {
      return this.host;
    }

    /**
     * Returns the gateway hub address.
     */
    public String getAddress() {
      return this.addr;
    }

    /**
     * Returns the gateway hub port.
     */
    public int getPort() {
      return this.port;
    }

    /**
     * Returns the gateway hub distributed system name.
     */
    public String getDistributedSystemName() {
      return this.ds;
    }

    public boolean equals(Object obj) {
      if (obj instanceof Endpoint) {
        Endpoint endpoint = (Endpoint)obj;
        return endpoint.getId().equals(this.getId())
            && endpoint.getName().equals(this.getName())
            && endpoint.getVmid() == this.getVmid()
            && endpoint.getHost().equals(this.getHost())
            && endpoint.getAddress().equals(this.getAddress())
            && endpoint.getPort() == this.getPort()
            && endpoint.getDistributedSystemName()
                       .equals(this.getDistributedSystemName());
      }
      return false;
    }

    public int hashCode() {
      return this.port;
    }

    /**
     * Returns the endpoint as a string.
     */
    public String toString() {
      return this.id + " " + addr + "[" + port + "]"
                     + "(vm_" + vmid + "_" + name + "_" + host + ")";
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

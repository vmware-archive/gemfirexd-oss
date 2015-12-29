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

package hydra.gemfirexd;

import com.gemstone.gemfire.LogWriter;
import com.pivotal.gemfirexd.FabricService;
import com.pivotal.gemfirexd.FabricServiceManager;
import com.pivotal.gemfirexd.NetworkInterface;
import hydra.BasePrms;
import hydra.EnvHelper;
import hydra.HostDescription;
import hydra.HostHelper;
import hydra.HydraRuntimeException;
import hydra.HydraTimeoutException;
import hydra.Log;
import hydra.MasterController;
import hydra.PortHelper;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;
import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.*;

/**
 * Provides support for network servers running in hydra-managed client VMs.
 */
public class NetworkServerHelper {

  /** The types of servers */
  private static enum Type {
    locator, server;
  }

  /** Name of the network server description used to create the servers */
  private static String TheNetworkServerConfig;

  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// NetworkServer
//------------------------------------------------------------------------------

  /**
   * Starts network servers in this VM.  The network servers are configured
   * using the {@link NetworkServerDescription} corresponding to the given
   * network server configuration from {@link NetworkServerPrms#names}.
   * <p>
   * When NetworkServerPrms#reusePorts} is true, selects random ports upon
   * initial creation and registers the {@link Endpoint}s in the {@link
   * NetworkServerBlackboard} map.  Restarts of the network servers will always
   * use the same ports.  Otherwise, random ports are generated for each start.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure
   *         existing network servers.
   */
  public static List<NetworkInterface> startNetworkServers(
                                      String networkServerConfig) {
    return startNetworkServers(networkServerConfig, Type.server);
  }

  /**
   * Starts network locators in this VM.  The network servers are configured
   * using the {@link NetworkServerDescription} corresponding to the given
   * network locator configuration from {@link NetworkServerPrms#names}.
   * <p>
   * When NetworkServerPrms#reusePorts} is true, selects random ports upon
   * initial creation and registers the {@link Endpoint}s in the {@link
   * NetworkServerBlackboard} map.  Restarts of the network locators will always
   * use the same ports.  Otherwise, random ports are generated for each start.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure
   *         existing network locators.
   */
  protected static List<NetworkInterface> startNetworkLocators(
                                         String networkServerConfig) {
    return startNetworkServers(networkServerConfig, Type.locator);
  }

  /**
   * Starts network servers of the given type using the given configuration.
   */
  private static synchronized List<NetworkInterface> startNetworkServers(
                              String networkServerConfig, Type type) {
    List<NetworkInterface> servers = getNetworkServers();
    if (servers.size() == 0) {

      // look up the network server configuration
      NetworkServerDescription nsd =
                   getNetworkServerDescription(networkServerConfig);

      int numServers = nsd.getNumServers();
      log.info("Starting " + numServers + " network " + type + "s "
             + "using configuration: " + networkServerConfig);

      // start the servers
      for (int i = 0; i < numServers; i++) {
        log.info("Starting network " + type + " #" + i + " of " + numServers);
        NetworkInterface server = startNetworkServer(i, nsd, type);
        servers.add(server);
        log.info("Started network server #" + i + " of " + numServers);
      }

      // save the network server config for future reference
      TheNetworkServerConfig = networkServerConfig;

    } else if (TheNetworkServerConfig == null) {
      // block attempt to create network servers in multiple ways
      String s = "Network servers were already created without "
               + "NetworkServerHelper using an unknown, "
               + "and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheNetworkServerConfig.equals(networkServerConfig)) {
      // block attempt to recreate server with clashing configuration name
      String s = "Network servers already exist using logical configuration "
               + TheNetworkServerConfig
               + ", cannot also use " + networkServerConfig;
      throw new HydraRuntimeException(s);

    } // else they were already created with this configuration, which is fine

    return servers;
  }

  /**
   * Starts a network server using the given description.
   */
  private static synchronized NetworkInterface startNetworkServer(int index,
                              NetworkServerDescription nsd, Type type) {
    NetworkInterface server;

    // generate an endpoint
    Endpoint endpoint = getEndpoint(index, type, nsd.getReusePorts());

    // start the server
    log.info("Starting network " + type + " \"" + endpoint + "\"");
    FabricService service = FabricServiceManager.currentFabricServiceInstance();
    if (service == null) {
      throw new HydraRuntimeException("No FabricService started yet.");
    }
    try {
      server = service.startNetworkServer(endpoint.getAddress(),
          endpoint.getPort(), nsd.getNetworkProperties());
    } catch (SQLException e) {
      String s = "Problem starting network server at " + endpoint + " for " + nsd;
      throw new HydraRuntimeException(s, e);
    }

    // wait for the server to start
    int timeoutSec = nsd.getMaxStartupWaitSec();
    log.info("Waiting "  + timeoutSec + " seconds for network server to start");
    long timeout = System.currentTimeMillis() + timeoutSec * 1000;
    while (!server.status()) {
      MasterController.sleepForMs(250);
      if (System.currentTimeMillis() > timeout) {
        String s = "Timed out after waiting " + timeoutSec
                 + " seconds for network server to start";
        throw new HydraTimeoutException(s);
      }
    }

    // configure the server
    log.info("Configuring network server");
    nsd.configure(server);

    log.info("Started network server: " + networkServerToString(server));
    return server;
  }

  /**
   * Returns the network servers for this VM, a possibly empty list.
   */
  public static synchronized List<NetworkInterface> getNetworkServers() {
    FabricService service = FabricServiceManager.currentFabricServiceInstance();
    if (service != null) {
      return new ArrayList<NetworkInterface>(service.getAllNetworkServers());
    }
    return new ArrayList<NetworkInterface>();
  }

  /**
   * Stops the network servers for this VM, if they exist.
   */
  public static synchronized void stopNetworkServers() {
    log.info("Stopping network servers");
    FabricService fs = FabricServiceManager.currentFabricServiceInstance();
    if (fs != null) {
      fs.stopAllNetworkServers();
    }
    TheNetworkServerConfig = null;
    log.info("Stopped network servers");
  }

  /**
   * Returns the given network server as a string.
   */
  public static String networkServerToString(NetworkInterface server) {
    return NetworkServerDescription.networkServerToString(server);
  }

  /**
   * Issues "gfxd run" on the specified file to a network locator in the
   * specified distributed system. Waits the given number of seconds for
   * the run command to complete.
   *
   * @throws HydraRuntimeException if no network locator is found.
   */
  public static synchronized void executeSQLCommands(
         String dsName, String fn, int waitSec) {
    log.info("Issuing run command to " + dsName + " using " + fn);
    List<Endpoint> locators = getNetworkServerEndpoints(dsName);
    if (locators.size() == 0) {
      throw new HydraRuntimeException("No network locators found");
    }
    Endpoint endpoint = locators.get(0);
    String cmd = FabricServerHelper.getGFXDCommand()
               + "run -file=" + fn
               + " -client-bind-address=" + endpoint.getAddress()
               + " -client-port=" + endpoint.getPort();
    String output = ProcessMgr.fgexec(cmd, waitSec);
    log.info("Issued run command to " + dsName + " using " + fn);
  }

//------------------------------------------------------------------------------
// NetworkServerDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link NetworkServerDescription} with the given configuration
   * name from {@link NetworkServerPrms#names}.
   */
  public static NetworkServerDescription getNetworkServerDescription(
                                         String networkServerConfig) {
    if (networkServerConfig == null) {
      throw new IllegalArgumentException("networkServerConfig cannot be null");
    }
    log.info("Looking up network server config: " + networkServerConfig);
    NetworkServerDescription nsd =
      GfxdTestConfig.getInstance()
                    .getNetworkServerDescription(networkServerConfig);
    if (nsd == null) {
      String s = networkServerConfig + " not found in "
               + BasePrms.nameForKey(NetworkServerPrms.names);
      throw new HydraRuntimeException(s);
    }
    log.info("Looked up network server config:\n" + nsd);
    return nsd;
  }

//------------------------------------------------------------------------------
// Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns all network locator endpoints for this WAN site from the {@link
   * NetworkServerBlackboard} map, a possibly empty list.  This includes all
   * network locators that have ever started, regardless of their distributed
   * system or current active status.
   * <p>
   * NOTE: Assumes use of <code>hydraconfig/gemfirexd/topology_wan_*.inc</code>.
   */
  public static List<Endpoint> getNetworkLocatorEndpointsInWanSite() {
    List<Endpoint> endpoints = getNetworkLocatorEndpoints();
    return getEndpointsInWanSite(endpoints);
  }

  /**
   * Returns all network server endpoints for this WAN site from the {@link
   * NetworkServerBlackboard} map, a possibly empty list.  This includes all
   * network servers that have ever started, regardless of their distributed
   * system or current active status.
   * <p>
   * NOTE: Assumes use of <code>hydraconfig/gemfirexd/topology_wan_*.inc</code>.
   */
  public static List<Endpoint> getNetworkServerEndpointsInWanSite() {
    List<Endpoint> endpoints = getNetworkServerEndpoints();
    return getEndpointsInWanSite(endpoints);
  }

  /**
   * Returns all endpoints for this WAN site in the list.
   * <p>
   * NOTE: Assumes use of <code>hydraconfig/gemfirexd/topology_wan_*.inc</code>.
   */
  private static List<Endpoint> getEndpointsInWanSite(List<Endpoint> endpoints) {
    int wanSite = toWanSite(RemoteTestModule.getMyClientName());
    List<Endpoint> matchingEndpoints = new ArrayList();
    for (Endpoint endpoint : endpoints) {
      if (toWanSite(endpoint.getName()) == wanSite) {
        matchingEndpoints.add(endpoint);
      }
    }
    return matchingEndpoints;
  }

  /**
   * Extracts the WAN site number from a value of {@link ClientPrms#names}
   * as generated by <code>hydraconfig/gemfirexd/topology_wan_*.inc</code>.
   */
  public static int toWanSite(String clientName) {
    String arr[] = clientName.split("_");
    if (arr.length != 3) {
      String s = clientName
               + " is not in the form <name>_<wanSiteNumber>_<itemNumber>";
      throw new HydraRuntimeException(s);
    }
    try {
      return Integer.parseInt(arr[1]);
    } catch (NumberFormatException e) {
      String s = clientName
               + " is not in the form <name>_<wanSiteNumber>_<itemNumber>";
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Returns all network locator endpoints from the {@link
   * NetworkServerBlackboard} map, a possibly empty list.  This includes all
   * network locators that have ever started, regardless of their distributed
   * system or current active status.
   */
  public static List getNetworkLocatorEndpoints() {
    return getEndpoints(Type.locator);
  }

  /**
   * Returns all network server endpoints from the {@link
   * NetworkServerBlackboard} map, a possibly empty list.  This includes all
   * network servers that have ever started, regardless of their distributed
   * system or current active status.
   */
  public static List getNetworkServerEndpoints() {
    return getEndpoints(Type.server);
  }

  /**
   * Returns all endpoints of the given type.
   */
  private static synchronized List<Endpoint> getEndpoints(Type type) {
    List<Endpoint> endpoints = new ArrayList();
    Map<String,List<Endpoint>> map =
               NetworkServerBlackboard.getInstance().getSharedMap().getMap();
    log.info("Complete endpoint map contains: " + map);
    for (String key : map.keySet()) {
      if (key.startsWith(type.toString())) {
        endpoints.addAll(((Map)map.get(key)).values());
      }
    }
    log.info("Returning endpoint map: " + endpoints);
    return endpoints;
  }

  /**
   * Returns all network server endpoints for the specified distributed system
   * from the {@link NetworkServerBlackboard} map, a possibly empty list.
   * This includes all network servers that have ever started for the system,
   * regardless of their current active status.
   */
  public static List getNetworkServerEndpoints(String distributedSystemName) {
    return getEndpoints(Type.server, distributedSystemName);
  }

  /**
   * Returns all network locator endpoints for the specified distributed system
   * from the {@link NetworkServerBlackboard} map, a possibly empty list.
   * This includes all network locators that have ever started for the system,
   * regardless of their current active status.
   */
  public static List getNetworkLocatorEndpoints(String distributedSystemName) {
    return getEndpoints(Type.locator, distributedSystemName);
  }

  /**
   * Returns all endpoints of the given type in the given distributed system.
   */
  private static synchronized List getEndpoints(Type type,
                                                String distributedSystemName) {
    List endpoints = new ArrayList();
    for (Endpoint endpoint : getEndpoints(type)) {
      if (endpoint.getDistributedSystemName().equals(distributedSystemName)) {
        endpoints.add(endpoint);
      }
    }
    return endpoints;
  }

  /**
   * Returns the i'th network endpoint of the given type for this VM.  Lazily
   * selects a random port and registers the endpoint in the {@link
   * NetworkServerBlackboard} map.  Optionally reuses the port on restart.
   */
  private static synchronized Endpoint getEndpoint(int index, Type type,
                                                   boolean reusePorts) {
    SharedMap map = NetworkServerBlackboard.getInstance().getSharedMap();
    String key = getKey(type, RemoteTestModule.getMyVmid());
    Map<Integer,Endpoint> endpoints = (Map<Integer,Endpoint>)map.get(key);
    if (endpoints == null) {
      endpoints = new HashMap();
    }
    Endpoint endpoint = endpoints.get(index);
    if (endpoint == null) {
      log.info("Generating network " + type + " endpoint" );
    } else if (reusePorts) {
      log.info("Reusing network " + type + " endpoint: " + endpoint);
    } else {
      log.info("Regenerating network " + type + " endpoint: "  + endpoint);
      endpoints.remove(index);
      endpoint = null;
    }
    if (endpoint == null) {
      endpoint = generateEndpoint();
      endpoints.put(index, endpoint);
      map.put(key, endpoints);
      log.info("Generated network " + type + " endpoint: " + endpoint);
    }
    return endpoint;
  }

  private static Endpoint generateEndpoint() {
    Integer vmid = RemoteTestModule.getMyVmid();
    String name = RemoteTestModule.getMyClientName();
    String host = HostHelper.getLocalHost();
    String addr = HostHelper.getHostAddress();
    InetAddress iaddr = HostHelper.getIPAddress();
    log.info("Generating random port");
    int port = PortHelper.getRandomPort();
    log.info("Generated network port: " + port);
    String ds = FabricServerHelper.TheFabricServerDescription
                                  .getDistributedSystem();
    return new Endpoint(name, vmid.intValue(), host, addr, iaddr, port, ds);
  }

  private static String getKey(Type type, int vmid) {
    return type + "_" + vmid;
  }

  /**
   * Represents the endpoint for a network server.
   */
  public static class Endpoint implements Serializable {
    String id, name, host, addr, ds;
    InetAddress iaddr;
    int vmid, port;

    /**
     * Creates an endpoint for a network server.
     *
     * @param name the logical hydra client VM name from {@link
     *             ClientPrms#names} found via the {@link
     *             ClientPrms#CLIENT_NAME_PROPERTY} system property.
     * @param vmid the logical hydra client VM ID found via {@link
     *             RemoteTestModule#getMyVmid}.
     * @param host the network server host.
     * @param addr the network server address.
     * @param iaddr the network server inet address.
     * @param port the network server port.
     * @param ds   the network server distributed system name.
     */
     public Endpoint(String name, int vmid, String host, String addr,
                    InetAddress iaddr, int port, String ds) {
      if (name == null) {
        throw new IllegalArgumentException("name cannot be null");
      }
      if (host == null) {
        throw new IllegalArgumentException("host cannot be null");
      }
      if (addr == null) {
        throw new IllegalArgumentException("addr cannot be null");
      }
      if (iaddr == null) {
        throw new IllegalArgumentException("iaddr cannot be null");
      }
      if (ds == null) {
        throw new IllegalArgumentException("ds cannot be null");
      }

      this.name = name;
      this.vmid = vmid;
      this.host = host;
      this.addr = addr;
      this.iaddr = iaddr;
      this.port = port;
      this.ds   = ds;

      this.id = "vm_" + this.vmid + "_" + this.name + "_" + this.host;
    }

    /**
     * Returns the unique network server logical endpoint ID.
     */
    public String getId() {
      return this.id;
    }

    /**
     * Returns the network server logical VM name.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns the network server logical VM ID.
     */
    public int getVmid() {
      return this.vmid;
    }

    /**
     * Returns the network server host.
     */
    public String getHost() {
      return this.host;
    } 
        
    /**
     * Returns the network server host address.
     */ 
    public String getAddress() {
      return this.addr; 
    }   
      
    /**
     * Returns the network server address.
     */
    public InetAddress getInetAddress() {
      return this.iaddr;
    } 
      
    /**
     * Returns the network server port.
     */
    public int getPort() {
      return this.port; 
    }

    /**
     * Returns the network server distributed system name.
     */
    public String getDistributedSystemName() {
      return this.ds;
    }

    public boolean equals(Object obj) {
      if (obj instanceof Endpoint) {
        Endpoint endpoint = (Endpoint)obj;
        return endpoint.getName().equals(this.getName())
            && endpoint.getVmid() == this.getVmid()
            && endpoint.getHost().equals(this.getHost())
            && endpoint.getAddress().equals(this.getAddress())
            && endpoint.getInetAddress().equals(this.getInetAddress())
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
      return addr + ":" + port;
    }
  }  
}

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
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Helps hydra clients manage jmx managers. Methods are thread-safe.
 */
public class JMXManagerHelper {

  protected static final int DEFAULT_JMX_MANAGER_PORT = 0;
  protected static final int DEFAULT_JMX_MANAGER_HTTP_PORT = 0;

  /** Perpetual endpoint for the JMX manager. */
  private static Endpoint TheJMXManagerEndpoint;

//------------------------------------------------------------------------------
// JMX Manager Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns the JMX manager endpoint for this JVM from the shared {@link
   * JMXManagerBlackboard} map, if it exists. Otherwise returns null.
   */
  protected static synchronized Endpoint getEndpoint() {
    if (TheJMXManagerEndpoint == null) {
      Integer vmid = new Integer(RemoteTestModule.getMyVmid());
      TheJMXManagerEndpoint = (Endpoint)JMXManagerBlackboard.getInstance()
                                       .getSharedMap().get(vmid);
    }
    return TheJMXManagerEndpoint;
  }

  /**
   * Returns all JMX manager endpoints from the {@link JMXManagerBlackboard}
   * map, a possibly empty list.  This includes all JMX managers that have ever
   * generated an endpoint, regardless of their distributed system or current
   * active status.
   */
  public static synchronized List<Endpoint> getEndpoints() {
    List<Endpoint> endpoints = new ArrayList();
    endpoints.addAll(JMXManagerBlackboard.getInstance()
                    .getSharedMap().getMap().values());
    return endpoints;
  }

  /**
   * Returns all JMX manager endpoints for this JVM's distributed system from
   * the {@link JMXManagerBlackboard} map, a possibly empty list.  This
   * includes all JMX managers that have ever generated an endpoint, regardless
   * of their current active status.
   */
  public static synchronized List getSystemEndpoints() {
    return getEndpoints(DistributedSystemHelper.getDistributedSystemName());
  }

  /**
   * Returns all JMX manager endpoints for the specified distributed system
   * from the {@link JMXManagerBlackboard} map, a possibly empty list.
   * This includes all JMX managers that have ever generated an endpoint,
   * regardless of their current active status.
   */
  public static synchronized List getEndpoints(String distributedSystemName) {
    List jmxManagerEndpoints = new ArrayList();
    for (Iterator i = getEndpoints().iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint)i.next();
      if (endpoint.getDistributedSystemName().equals(distributedSystemName)) {
        jmxManagerEndpoints.add(endpoint);
      }
    }
    return jmxManagerEndpoints;
  }

  /**
   * Returns the JMX manager endpoint for this JVM from the shared {@link
   * JMXManagerBlackboard} map. Generates one if it does not already exist.
   */
  protected static synchronized Endpoint getEndpoint(boolean generatePort,
                                                     boolean generateHttpPort) {
    Endpoint endpoint = getEndpoint();
    if (endpoint == null) {
      endpoint = generateEndpoint(generatePort, generateHttpPort);
    }
    return endpoint;
  }

  /**
   * Generates a JMX manager endpoint with random ports for this JVM and
   * stores it in the shared {@link JMXManagerBlackboard} map, if needed.
   * Caches the result.
   */
  private static Endpoint generateEndpoint(boolean generatePort,
                                           boolean generateHttpPort) {
    log("Generating JMX manager endpoint");
    Integer vmid = new Integer(RemoteTestModule.getMyVmid());
    String name = RemoteTestModule.getMyClientName();
    String host = HostHelper.getCanonicalHostName();
    String addr = HostHelper.getHostAddress();
    int port = generatePort ? PortHelper.getRandomPort()
                            : DEFAULT_JMX_MANAGER_PORT;
    int httpPort = generateHttpPort ? PortHelper.getRandomPort()
                                    : DEFAULT_JMX_MANAGER_HTTP_PORT;
    GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
    String ds = gfd.getDistributedSystem();
    Endpoint endpoint = new Endpoint(name, vmid.intValue(),
                                     host, addr, port, httpPort, ds);
    log("Generated JMX manager endpoint: " + endpoint);
    JMXManagerBlackboard.getInstance().getSharedMap().put(vmid, endpoint);
    TheJMXManagerEndpoint = endpoint; // cache it for later use
    return endpoint;
  }

  /**
   * Represents the endpoint for a JMX manager.
   */
  public static class Endpoint implements Serializable {
    String name, host, addr, ds;
    int vmid, port, httpPort;

    /**
     * Creates an endpoint for a JMX manager.
     *
     * @param name the logical hydra client VM name from {@link
     *             ClientPrms#names} found via the {@link
     *             ClientPrms#CLIENT_NAME_PROPERTY} system property.
     * @param vmid the logical hydra client VM ID found via {@link
     *             RemoteTestModule#getMyVmid}.
     * @param host the JMX manager host.
     * @param addr the JMX manager address.
     * @param port the JMX manager port.
     * @param httpPort the JMX manager http port.
     * @param ds   the JMX manager distributed system name.
     */
    public Endpoint(String name, int vmid,
                    String host, String addr, int port, int httpPort, String ds) {
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
      this.httpPort = httpPort;
      this.ds   = ds;
    }

    /**
     * Returns the JMX manager logical VM name.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns the JMX manager logical VM ID.
     */
    public int getVmid() {
      return this.vmid;
    }

    /**
     * Returns the JMX manager host.
     */
    public String getHost() {
      return this.host;
    }

    /**
     * Returns the JMX manager address.
     */
    public String getAddress() {
      return this.addr;
    }

    /**
     * Returns the JMX manager port.
     */
    public int getPort() {
      return this.port;
    }

    /**
     * Returns the JMX manager http port.
     */
    public int getHttpPort() {
      return this.httpPort;
    }

    /**
     * Returns the JMX manager distributed system name.
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
            && endpoint.getPort() == this.getPort()
            && endpoint.getHttpPort() == this.getHttpPort()
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
      return this.host + ":" + this.port + ":" + this.httpPort
           + "(" + this.ds + ":vm_" + this.vmid + "_" + this.name + ")";
     
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

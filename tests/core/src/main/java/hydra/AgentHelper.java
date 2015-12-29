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
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.jmx.Agent;
import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.gemstone.gemfire.admin.jmx.AgentFactory;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.*;
import javax.management.MalformedObjectNameException;

/**
 * Provides support for management agents running in hydra client VMs.
 */
public class AgentHelper {

  /** Name of the agent description used to create the agent */
  private static String TheAgentConfig;

  /** The created agent */
  private static Agent TheAgent;

//------------------------------------------------------------------------------
// Agent
//------------------------------------------------------------------------------

  /**
   * Creates, starts, and connects an agent using the given agent configuration.
   * @see #createAgent(String)
   * @see #startAgent
   * @see #connectAgent
   */
  public static synchronized Agent startConnectedAgent(String agentConfig) {
    Agent agent = createAgent(agentConfig);
    startAgent();
    connectAgent();
    return agent;
  }

  /**
   * Creates an unstarted and unconnected agent.  The agent is configured
   * using the {@link AgentDescription} corresponding to the given agent
   * configuration from {@link AgentPrms#names}.
   * <p>
   * Upon initial creation, selects a random port and registers the {@link
   * Endpoint} in the {@link AgentBlackboard} map for use by tools.  Starts
   * of any agent in this VM will always use this port.
   * <p>
   * To make modifications before connecting, invoke <code>getConfig()/code>
   * to get the {@link com.gemstone.gemfire.admin.jmx.AgentConfig} and
   * invoke its mutator methods.
   * <p>
   * To start the agent, use {@link #startAgent()}.  To connect it, use {@link
   * #connectAgent()}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing agent or there is an exception when creating it.
   */
  public static synchronized Agent createAgent(String agentConfig) {
    Agent agent = getAgent();
    if (agent == null) {
      log("Creating agent from config: " + agentConfig);

      // look up the agent configuration
      AgentDescription ad = getAgentDescription(agentConfig);

      // configure the config
      log("Configuring agent from config: " + agentConfig);
      AgentConfig agConfig = AgentFactory.defineAgent();
      String ds = ad.getAdminDescription().getDistributedSystem();
      Endpoint endpoint = getEndpoint(ds);
      ad.configure(agConfig, endpoint.getPort());
      if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
        //Specify rmiBindAddress see bug 40500
        String addr = HostHelper.getHostAddress();
        agConfig.setRmiBindAddress(addr);
      }
      log("Configured agent: " + AgentDescription.agentToString(agConfig));

      // create the agent
      try {
        agent = AgentFactory.getAgent(agConfig);
      } catch (AdminException e) {
        String s = "Unable to create agent: " + agentConfig;
        throw new HydraRuntimeException(s, e);
      }

      // save the agent and the config it was based on for future reference
      TheAgent = agent;
      TheAgentConfig = agentConfig;

      log("Created agent: " + agent);
    }
    else if (!TheAgentConfig.equals(agentConfig)) {
      // block attempt to recreate agent with clashing configuration name
      String s = "Agent already exists using logical agent configuration named "
               + TheAgentConfig + ", cannot also use " + agentConfig;
      throw new HydraRuntimeException(s);
    }
    // else it was already created with this configuration, which is fine
    return agent;
  }

  /**
   * Returns the current agent if it is created, otherwise null.  Note that
   * the agent might not be connected or started.
   */
  public static synchronized Agent getAgent() {
    return TheAgent;
  }

  /**
   * Starts the agent whose endpoint was created by {@link #createAgent}, if it
   * is not already started.  This starts the adaptors but does not connect it
   * to a distributed system.  Use {@link #connectAgent} to connect the agent.
   */
  public static synchronized Agent startAgent() {
    Agent agent = getAgent();
    if (agent == null) {
      String s = "Agent has not been created yet";
      throw new HydraRuntimeException(s);
    }
    log("Starting agent:" + agent);
    agent.start();
    log("Started agent: " + agent);
    return agent;
  }

  /**
   * Stops the agent, if it is running.  This stops the adaptors and
   * disconnects the agent from the distributed system.
   */
  public static synchronized void stopAgent() {
    Agent agent = getAgent();
    if (agent == null) {
      String s = "Agent has not been created yet";
      throw new HydraRuntimeException(s);
    }
    log("Stopping agent: " + agentToString(agent));
    agent.stop();
    log("Stopped agent: " + agentToString(agent));
    TheAgentConfig = null;
    TheAgent = null;
  }

  /**
   * Connects the agent whose endpoint was created by {@link #createAgent} to
   * an admin distributed system, if it is not already connected, but does not
   * start it.  To start the agent, use {@link #startAgent}.
   *
   * @throws HydraRuntimeException if the agent has not been created or an
   *                               exception occurs when connecting it.
   */
  public static synchronized Agent connectAgent() {
    Agent agent = getAgent();
    if (agent == null) {
      String s = "Agent has not been created yet";
      throw new HydraRuntimeException(s);
    }
    AdminDistributedSystem ads = agent.getDistributedSystem();
    if (ads == null) {
      log("Connecting agent to admin distributed system: " + agent);
      try {
        agent.connectToSystem();
      } catch (AdminException e) {
        try {
          ProcessMgr.logNetworkStatistics();
        } finally {
          String s = "Unable to connect agent: " + agent;
          throw new HydraRuntimeException(s, e);
        }
      } catch (MalformedObjectNameException e) {
        String s = "Unable to connect agent: " + agent;
        throw new HydraRuntimeException(s, e);
      }
      ads = agent.getDistributedSystem();
      log("Connected agent: " + agent + " to "
                              + DistributedSystemHelper.adminToString(ads));
    }
    return agent;
  }

  /**
   * Disconnects the agent if it is connected, leaving the adaptors running.
   * To stop the adaptors, use {@link #stopAgent}.
   */
  public static synchronized void disconnectAgent() {
    Agent agent = getAgent();
    if (agent == null) {
      String s = "Agent has not been created yet";
      throw new HydraRuntimeException(s);
    }
    if (agent.isConnected()) {
      log("Disconnecting agent: " + agent);
      agent.disconnectFromSystem();
      log("Disconnected agent");
    }
  }

  /**
   * Returns the given agent as a string.
   */
  public static String agentToString(Agent agent) {
    if (agent == null) {
      String s = "agent cannot be null";
      throw new IllegalArgumentException(s);
    }
    return AgentDescription.agentToString(agent.getConfig());
  }

//------------------------------------------------------------------------------
// AgentDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link AgentDescription} with the given configuration name
   * from {@link AgentPrms#names}.
   */
  public static AgentDescription getAgentDescription(String agentConfig) {
    if (agentConfig == null) {
      throw new IllegalArgumentException("agentConfig cannot be null");
    }
    log("Looking up agent config: " + agentConfig);
    AgentDescription ad = TestConfig.getInstance()
                                    .getAgentDescription(agentConfig);
    if (ad == null) {
      String s = agentConfig + " not found in "
               + BasePrms.nameForKey(AgentPrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up agent config:\n" + ad);
    return ad;
  }

//------------------------------------------------------------------------------
// Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns all agent endpoints from the {@link AgentBlackboard} map,
   * a possibly empty list.  This includes all agents that have ever been
   * created, regardless of their current status.
   */
  public static synchronized List getEndpoints() {
    return new ArrayList(AgentBlackboard.getInstance()
                                        .getSharedMap().getMap().values());
  }

  /**
   * Gets the agent endpoint for this VM from the shared {@link AgentBlackboard}
   * map.  Lazily selects a random port and registers the endpoint in the map.
   */
  private static synchronized Endpoint getEndpoint(String ds) {
    Integer vmid = new Integer(RemoteTestModule.getMyVmid());
    Endpoint endpoint =
      (Endpoint)AgentBlackboard.getInstance().getSharedMap().get(vmid);
    if (endpoint == null) {
      String name = RemoteTestModule.getMyClientName();
      String host = RemoteTestModule.getMyHost();
      String addr = HostHelper.getHostAddress();
      int port = PortHelper.getRandomPort();
      endpoint = new Endpoint(name, vmid.intValue(), host, addr, port, ds);
      log("Generated agent endpoint: " + endpoint + " in ds: " + ds);
      AgentBlackboard.getInstance().getSharedMap().put(vmid, endpoint);
      try {
        RemoteTestModule.Master.recordAgent(endpoint);
      } catch (RemoteException e) {
        String s = "While recording agent endpoint";
        throw new HydraRuntimeException(s, e);
      }
    }
    return endpoint;
  }

  /**
   * Represents the endpoint for an agent.
   */
  public static class Endpoint implements Serializable {
    String id, name, host, addr, ds;
    int vmid, port;

    /**
     * Creates an endpoint for an agent.
     *
     * @param name the logical hydra client VM name from {@link
     *             ClientPrms#names} found via the {@link
     *             ClientPrms#CLIENT_NAME_PROPERTY} system property.
     * @param vmid the logical hydra client VM ID found via {@link
     *             RemoteTestModule#getMyVmid}.
     * @param host the agent host.
     * @param addr the agent address.
     * @param port the agent port.
     * @param ds   the agent distributed system name.
     */
    public Endpoint(String name, int vmid, String host, String addr,
                    int port, String ds) {
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

      this.id = "vm_" + this.vmid + "_" + this.name;
    }

    /**
     * Returns the unique agent logical endpoint ID.
     */
    public String getId() {
      return this.id;
    }

    /**
     * Returns the agent logical VM name.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns the agent logical VM ID.
     */
    public int getVmid() {
      return this.vmid;
    }

    /**
     * Returns the agent host.
     */
    public String getHost() {
      return this.host;
    }

    /**
     * Returns the agent addr.
     */
    public String getAddr() {
      return this.addr;
    }

    /**
     * Returns the agent port.
     */
    public int getPort() {
      return this.port;
    }

    /**
     * Returns the agent distributed system name.
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
            && endpoint.getAddr().equals(this.getAddr())
            && endpoint.getPort() == this.getPort()
            && endpoint.getDistributedSystemName() == this.getDistributedSystemName();
      }
      return false;
    }

    public int hashCode() {
      return this.port;
    }

    /**
     * Returns the endpoint info as a properties object containing a single
     * property with the test-wide unique key equal to the agent endpoint id
     * and value equal to {@link #toString}.
     */
    public Properties toProperty() {
      Properties p = new Properties();
      p.setProperty(String.valueOf(this.vmid), this.toString());
      return p;
    }

    /**
     * Records agent information in a properties file for use when manually
     * generating GUI tests.
     */
    public void record() {
      String data = this.id + "," + this.ds + "," + this.host + ","
                  + this.addr + "," + this.port;
      FileUtil.appendToFile("agents.dat", data + "\n" );
    }

    /**
     * Returns the endpoint info as a string that can be parsed on = and ,.
     */
    public String toString() {
      return "vmid=" + this.vmid + ",name=" + this.name
          + ",host=" + this.host + ",addr=" + this.addr + ",port=" + this.port
          + ",ds=" + this.ds;
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

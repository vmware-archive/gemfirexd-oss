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

package admin.jmx;

import hydra.AgentHelper;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigPrms;
import hydra.HydraRuntimeException;
import hydra.Log;

import java.io.IOException;
import java.net.MalformedURLException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.NotificationFilterSupport;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.ServiceNotFoundException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.jmx.internal.AdminDistributedSystemJmxImpl;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;

/**
 * Intended to serve as a helper for JMX Clients
 * wishing to use JMXAgent functionality
 *
 * @author hkhanna
 */
public class JMXHelper
{
  public static void main(String[] args)
    throws Exception
  {
    String host = args[0];
    int port = Integer.valueOf(args[1]).intValue();

    connectToDS(host, port);
  }
  
  /**
   * Starts and connects a JMX agent using the {@link hydra.AgentPrms}
   * configuration specified in {@link hydra.ConfigPrms#agentConfig}.
   */
  public static void startJMXAgent()
  {
    // Let's start the Agent first
    startJMXAgentButDoNotConnect();

    // And now connect. If there was an exception in start or create
    // that would've thrown a HydraRuntime Exception. A check for null
    // Agent is not necessary
     AgentHelper.connectAgent();
  }

  /**
   * Starts a JMX agent using the {@link hydra.AgentPrms}
   * configuration specified in {@link hydra.ConfigPrms#agentConfig}.
   * This method does not connect the Agent to the DS
   */
  public static void startJMXAgentButDoNotConnect()
  {
    String agentConfig = ConfigPrms.getAgentConfig();

    // Create Agent
    AgentHelper.createAgent(agentConfig);

    // Start Agent
    AgentHelper.startAgent();
  }

  /**
   * If there's an existing connection to the DS, then disconnect it
   * Subsequently re-connect again. If we keep connecting without
   * first disconnecting it is possible to starve on the jmx sockets
   */
  public static void disconnectAndThenReconnectToDSNoListeners() throws Exception
  {
    // If the MBeanServer Connection is null, we're not connected
    if (connector==null) {
      connectToDSButDoNotAttachListeners();
      return;
    }

    // Remove the notification listener. If there's an exception here
    // that'll be thrown back. It is not expected that listener be Null
    if (listener!=null) {
      mbsc.removeNotificationListener(distributedSys, listener);
    }

    // Disconnect
    disconnectFromDS();
   
    // Close the connector
    connector.close();

    // Now we can reconnect
    connectToDSButDoNotAttachListeners();
  }

  /*
   * Only establish a conection to DS. Do not attach any Notification listeners
   * This is helpful when we're testing only connects to a DS and not
   * establishing notification listeners for recieving notifications
   */
  public static void connectToDSButDoNotAttachListeners() throws Exception
  {
    int port = -1;
    String host = "";
    
    // get the list of all agent endpoints
    List vals = AgentHelper.getEndpoints();
    
    // 1st agent, a fix for multi- agents here for future
    for (Iterator iter=vals.iterator(); iter.hasNext(); ) {
      AgentHelper.Endpoint endpoint = (AgentHelper.Endpoint)iter.next();
      port = endpoint.getPort();
      host = endpoint.getHost();
      break;
    }
    
    connectToDS(host, port);
  }

  /**
   * the INITTASKS calling connectToDS across ThreadGroups 
   * should be executed with the keyword SEQUENTIAL
   * <p>
   * Must be invoked after the JMX agents have been created, started, and
   * connected using {@link hydra.AgentHelper}.
   */
  public static void connectToDS() throws Exception
  {
    // Connect To DS
    connectToDSButDoNotAttachListeners();

    //Register the stat alert notification listener as well.
    listener = new StatAlertNotificationListener(40);
    NotificationFilterSupport support = new NotificationFilterSupport();
    support.enableType(AdminDistributedSystemJmxImpl.NOTIF_STAT_ALERT);
    mbsc.addNotificationListener(distributedSys, listener, support, new Object());
    
    logger.info("Connected to JMX ADS " + distributedSys);
  }

  public static void connectToDS(String host, int port) throws Exception
  {
    String url =MessageFormat.format(JMX_URI,  new Object[] { host, 
                                              String.valueOf(port) });
      
    try {
      JMXServiceURL jmxurl = new JMXServiceURL(url);
      connector = JMXConnectorFactory.connect(jmxurl, null);
      mbsc = connector.getMBeanServerConnection();

      String[] domains = mbsc.getDomains();
      String domain = null;
      for (int i = 0; i < domains.length; i++) {
        if (domains[i].equalsIgnoreCase(MBEAN_DOMAIN_GEMFIRE_NAME)) {
          domain = domains[i];
          break;
        }
      }
      
      Set set = mbsc.queryNames(new ObjectName(domain + ":*"), null);
      ObjectName on = null;
      for (Iterator iter = set.iterator(); iter.hasNext();) {
        on = (ObjectName)iter.next();
        String onType = on.getKeyProperty(MBEAN_PROPERTY_BEAN_TYPE);
        if (MBEAN_AGENT_TYPE.equalsIgnoreCase(onType))
          break;
      }
      agent = on;
      
      if (agent==null)
        throw new ServiceNotFoundException( MBEAN_AGENT_TYPE + 
                                           " could not be connected");

      String[] params = {}, signature = {};
      String method = "connectToSystem";
      ObjectName ret = null;
      
      ret = (ObjectName) mbsc.invoke(agent, method, params, signature);
      logger.info("Connected DS client");

      if (ret != null &&
          MBEAN_DISTRIBUTED_SYSTEM_TYPE.
            equalsIgnoreCase(ret.getKeyProperty(MBEAN_PROPERTY_BEAN_TYPE))) {
        distributedSys = ret;
      } else {
        ServiceNotFoundException ex = new ServiceNotFoundException(
            MBEAN_DISTRIBUTED_SYSTEM_TYPE + " could not be connected");
        throw ex;
      }
    }
    catch (NullPointerException nullEx) {
      logger.error(url + " is construed NULL", nullEx);
      throw nullEx;
    }
    catch (MalformedURLException malfEx) {
      logger.error(url + " is construed Malformed", malfEx);
      throw malfEx;
    }
    catch (SecurityException secuEx) {
      logger.error("Connection denied due to Security reasons", secuEx);
      throw secuEx;
    }
    catch (InstanceNotFoundException instEx) {
      logger.error("Did not find MBean Instance", instEx);
      throw instEx;
    }
    catch (MBeanException beanEx) {
      logger.error("Exception in MBean", beanEx.getCause());
      throw beanEx;
    }
    catch (ReflectionException reflEx) {
      logger.error("Could not get MBean Info", reflEx);
      throw reflEx;
    }
    catch (IOException ioEx) {
      logger.error("JMX Connection problem. Attempt opeartion Again", ioEx);
      throw ioEx;
    } finally {
      //shLock.unlock();
    }
  }
  
  public static void disconnectFromDS() throws Exception
  {
    try {
      String[] domains = mbsc.getDomains();
      String domain = null;
      for (int i = 0; i < domains.length; i++) {
        if (domains[i].equalsIgnoreCase(MBEAN_DOMAIN_GEMFIRE_NAME)) {
          domain = domains[i];
          break;
        }
      }
      
      Set set = mbsc.queryNames(new ObjectName(domain + ":*"), null);
      ObjectName on = null;
      for (Iterator iter = set.iterator(); iter.hasNext();) {
        on = (ObjectName)iter.next();
        String onType = on.getKeyProperty(MBEAN_PROPERTY_BEAN_TYPE);
        if (MBEAN_AGENT_TYPE.equalsIgnoreCase(onType))
          break;
      }
      agent = on;
      
      if (agent==null)
        throw new ServiceNotFoundException( MBEAN_AGENT_TYPE + 
                                           " could not be contacted");

      String[] params = {}, signature = {};
      String method = "disconnectFromSystem";
      ObjectName ret = null;
      
      mbsc.invoke(agent, method, params, signature);
      logger.info("DisConnected DS client");
    }
    catch (SecurityException secuEx) {
      logger.error("Connection denied due to Security reasons", secuEx);
      throw secuEx;
    }
    catch (InstanceNotFoundException instEx) {
      logger.error("Did not find MBean Instance", instEx);
      throw instEx;
    }
    catch (MBeanException beanEx) {
      logger.error("Exception in MBean", beanEx.getCause());
      throw beanEx;
    }
    catch (ReflectionException reflEx) {
      logger.error("Could not get MBean Info", reflEx);
      throw reflEx;
    }
    catch (IOException ioEx) {
      logger.error("JMX Connection problem. Attempt opeartion Again", ioEx);
      throw ioEx;
    } finally {
      //shLock.unlock();
    }
  }
  
  public static void registerStatAlertDefinition(StatAlertDefinition def)
      throws Exception {

    Object[] params = { def };
    String[] signature = { "com.gemstone.gemfire.internal.admin.StatAlertDefinition" };

    try {
      mbsc.invoke(distributedSys, "updateAlertDefinition", params, signature);
    }
    catch (Exception ex) {
      throw ex;
    }
  }

  public static void killClient() throws Exception {
    try {
      ClientVmMgr.stop("killing myself", ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
      return;
    } catch (ClientVmNotFoundException ex) {
      throw new HydraRuntimeException("I should always be able to kill myself", ex);
    }
  }

  private static ObjectName agent, distributedSys;
  private static JMXConnector connector;
  private static MBeanServerConnection mbsc;
  private static NotificationListener listener;
  static LogWriter logger = Log.getLogWriter();

  static final String JMX_URI = "service:jmx:rmi://{0}/jndi/rmi://{0}:{1}/jmxconnector";

  static final String MBEAN_DOMAIN_GEMFIRE_NAME = "GemFire";
  static final String MBEAN_AGENT_TYPE = "Agent";
  static final String MBEAN_PROPERTY_BEAN_TYPE = "type";
  static final String MBEAN_DISTRIBUTED_SYSTEM_TYPE = "AdminDistributedSystem"; 
}

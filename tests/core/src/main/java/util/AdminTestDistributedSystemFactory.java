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

package util;

import hydra.*;
//import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.jmx.Agent;
import java.util.*;
import javax.management.*;
import javax.management.remote.*;

// This class is in the tests source tree
import com.gemstone.gemfire.admin.jmx.JMXAdminDistributedSystem;

/**
 *  Supports creating AdminDistributedSystem of various types 
 *  (GemFire internal admin interface vs. admin/jmx/internal).
 */
public class AdminTestDistributedSystemFactory {

  /**
   *  Creates a {@link DistributedSystem} backed by an implementation of the type
   *  configured via {@link AdminHelperPrms#adminInterface}.
   *
   *  @throws AdminTestException if any problem was encountered.
   */
  public static AdminDistributedSystem getDistributedSystem( DistributedSystemConfig dsConfig ) {
    try {
//      Class cls = null;
      switch( AdminHelperPrms.getAdminInterface() ) {
	case AdminHelperPrms.ADMIN:
	  Log.getLogWriter().fine( "Creating an ADMIN AdminDistributedSystem implementation" );
          return AdminDistributedSystemFactory.getDistributedSystem(dsConfig);

	case AdminHelperPrms.JMX:
	  Log.getLogWriter().fine( "Creating a JMX AdminDistributedSystem implementation" );
          return createJMXDistributedSystem(dsConfig);

      default:
        String s = "Unknown Admin Interface type: " +
          AdminHelperPrms.getAdminInterface();
        throw new HydraConfigException(s);
      }

    } catch( Exception e ) {
      String s = "Problem creating AdminDistributedSystem implementation";
      throw new AdminTestException( s, e );
    }
  }

  /**
   * Connects to the first configured JMX Agent and returns an
   * implementation of <code>DistributedSystem</code> that performs
   * all of its actions remotely using JMX.
   */
  private static AdminDistributedSystem
    createJMXDistributedSystem(DistributedSystemConfig config) {

    List agents = AgentHelper.getEndpoints();
    if (agents.isEmpty()) {
      String s = "No JMX Agents have been configured";
      throw new HydraConfigException(s);
    }

    AgentHelper.Endpoint agent = (AgentHelper.Endpoint)agents.iterator().next();

    // Put together this ugly URL for connecting to the JMX Agent
    String agentProtocol = "rmi";
    String urlString = "service:jmx:" + agentProtocol + "://" + 
      agent.getHost() + "/jndi/rmi://" + agent.getHost() + ":" + 
      agent.getPort() + Agent.JNDI_NAME;

    try {
      JMXServiceURL url = new JMXServiceURL(urlString);
      JMXConnector conn = JMXConnectorFactory.connect(url);
      MBeanServerConnection mbs = conn.getMBeanServerConnection();
      ObjectName bean = new ObjectName("GemFire:type=Agent");
      
      String bindAddress = HostHelper.getHostAddress(HostHelper.getIPAddress());
      return new JMXAdminDistributedSystem(config.getMcastAddress(),
                                      config.getMcastPort(),
                                      config.getLocators(),
                                      bindAddress,
                                      config.getRemoteCommand(),
                                      mbs, conn, bean);

    } catch (Exception ex) {
      String s = "While connecting to JMX Agent";
      throw new HydraRuntimeException(s, ex);
    }
  }

}

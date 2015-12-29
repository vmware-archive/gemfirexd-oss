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
package com.gemstone.gemfire.admin.jmx;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.admin.internal.DistributedSystemConfigImpl;
import com.gemstone.gemfire.admin.internal.ManagedEntityConfigXmlGenerator;
import com.gemstone.gemfire.admin.internal.ManagedEntityControllerFactory;
//import com.gemstone.gemfire.admin.jmx.internal.AgentImpl;
import com.gemstone.gemfire.admin.jmx.internal.ManagedResource;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import dunit.*;
import java.io.*;
import java.util.*;
import javax.management.*;
import javax.management.remote.*;

/**
 * Tests JMX {@link Agent}s that are co-located in a GemFire
 * application VM.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class CoLocatedAgentDUnitTest extends CacheTestCase {

//  /** The JMX Agent started in this VM */
//  private static Agent agent;

  /** The XML file used to configure the agent */
  private String xmlFile = null;

  ////////  Constructors

  /**
   * Creates a new <code>CoLocatedAgentDUnitTest</code>
   */
  public CoLocatedAgentDUnitTest(String name) {
    super(name);
  }

  /**
   * Returns the distribution configuration of the system this test
   * would connect to.
   */
  DistributionConfig getDistributionConfig() {
    return this.getSystem().getConfig();
  }

  ////////  Helper methods

  /**
   * Returns the name of the XML file used to initialize the JMX
   * agent.
   */
  public String getEntityConfigXMLFile() {
    return this.xmlFile;
  }

  ////////  Test Methods

  /**
   * Tests that a JMX remote client can access information about a
   * cache that is hosted in a VM that also contains the JMX agent.
   */
  public void testRemoteAccess() throws Exception {
    disconnectAllFromDS();
   try{
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);

    final String rootName = "Root";

    vm.invoke(new CacheSerializableRunnable("Create Cache") {
        public void run2() throws CacheException {
          DistributedSystem system = getSystem();
          Cache cache = CacheFactory.create(system);
          AttributesFactory factory = new AttributesFactory();
          cache.createRegion(rootName,
                               factory.create());
        }
      });
    String urlString = JMXHelper.startRMIAgentNoAdmin(this, vm);

    // Connect to the JMX Agent using RMI
    JMXServiceURL url = new JMXServiceURL(urlString);
    JMXConnector conn = JMXConnectorFactory.connect(url);
    MBeanServerConnection mbs = conn.getMBeanServerConnection();
    assertNotNull(mbs);
      
    ObjectName objectName =
      new ObjectName(ManagedResource.MBEAN_NAME_PREFIX + "Agent");
    assertTrue(mbs.isRegistered(objectName));

    DistributionConfig dsConfig = this.getSystem().getConfig();
    disconnectFromDS();

    DistributedSystemConfig config =
      AdminDistributedSystemFactory.defineDistributedSystem();
    config.setMcastPort(dsConfig.getMcastPort());
    config.setMcastAddress(dsConfig.getMcastAddress().getHostName());
    config.setLocators(dsConfig.getLocators());
    JMXAdminDistributedSystem system =
      new JMXAdminDistributedSystem(config, mbs, conn,objectName);

    system.connect();
    assertTrue(system.waitToBeConnected(20 * 1000));

    SystemMember[] apps = system.getSystemMemberApplications();
    assertEquals(1, apps.length);

    SystemMember app = apps[0];
    assertTrue(app.hasCache());
    SystemMemberCache cache = app.getCache();
    Set roots = cache.getRootRegionNames();
    assertEquals(1, roots.size());
    assertEquals(rootName, roots.iterator().next());
    
    system.disconnect();
    system.closeRmiConnection();

    JMXHelper.stopRMIAgent(this, vm);
    }catch(Exception e) {
	    DistributedTestCase.fail("Exception in CoLocatedAgentDUnitTest  testRemoteAcess method", e);
    }
   }

  /**
   * Tests that a JMX agent can be initialized using a
   * <code>ds.xml</code> file.  This isn't really a co-located Agent
   * test, but whatever.
   */
  public void testAgentWithXMLFile() throws Exception {
    if (!ManagedEntityControllerFactory.isEnabledManagedEntityController()) {
      return;
    }
    disconnectAllFromDS();
    try{
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);

    DistributionConfig dsConfig = this.getSystem().getConfig();
    disconnectFromDS();

    final String command = "ssh {HOST} {CMD}";
    DistributedSystemConfig config =
      new DistributedSystemConfigImpl(dsConfig, command);
    AdminDistributedSystem system =
      AdminDistributedSystemFactory.getDistributedSystem(config);

    DistributionLocatorConfig locator =
      system.addDistributionLocator().getConfig();
    String string = dsConfig.getLocators();
    
    int portStartIdx = string.indexOf('[');
    int portEndIdx = string.indexOf(']');
    int bindIdx = string.lastIndexOf('@');
    if (bindIdx < 0) {
      bindIdx = string.lastIndexOf(':');
    }
    
    String hostname = string.substring(0, bindIdx > -1 ? bindIdx : portStartIdx);

    if (hostname.indexOf(':') >= 0) {
      bindIdx = string.lastIndexOf('@');
      hostname = string.substring(0, bindIdx > -1 ? bindIdx : portStartIdx);
    }    
    
    String port = string.substring(portStartIdx+1, portEndIdx);
    locator.setHost(hostname);
    locator.setPort(Integer.parseInt(port));
    
    CacheServerConfig server = system.addCacheServer().getConfig();
    final String classpath = System.getProperty("user.dir");
    server.setClassPath(classpath);

    // Generate the XML file
    File file = new File(this.getUniqueName() + ".xml");
    PrintWriter pw = new PrintWriter(new FileWriter(file), true);
    ManagedEntityConfigXmlGenerator.generate(system, pw);
    pw.flush();
    this.xmlFile = file.getCanonicalPath();

    system.disconnect();

//     vm.invoke(new CacheSerializableRunnable("Create Cache") {
//         public void run2() throws CacheException {
//           DistributedSystem system = getSystem();
//         }
//       });
    String urlString = JMXHelper.startRMIAgentNoAdmin(this, vm);

    // Connect to the JMX Agent using RMI
    JMXServiceURL url = new JMXServiceURL(urlString);
    JMXConnector conn = JMXConnectorFactory.connect(url);
    MBeanServerConnection mbs = conn.getMBeanServerConnection();
    assertNotNull(mbs);
      
    ObjectName objectName =
      new ObjectName(ManagedResource.MBEAN_NAME_PREFIX + "Agent");
    assertTrue(mbs.isRegistered(objectName));

    config = AdminDistributedSystemFactory.defineDistributedSystem();
    config.setMcastPort(dsConfig.getMcastPort());
    config.setMcastAddress(dsConfig.getMcastAddress().getHostName());
    config.setLocators(dsConfig.getLocators());
    system = new JMXAdminDistributedSystem(config, mbs, conn, objectName);

    system.connect();
    assertTrue(system.waitToBeConnected(20 * 1000));

    assertEquals(command, system.getRemoteCommand());

    CacheServer[] servers = system.getCacheServers();
    assertEquals(1, servers.length);
    CacheServerConfig serverConfig = servers[0].getConfig();
    assertEquals(classpath, serverConfig.getClassPath());
    
    system.disconnect();
    ((JMXAdminDistributedSystem)system).closeRmiConnection();

    JMXHelper.stopRMIAgent(this, vm);

    vm.invoke(new SerializableRunnable("Close connection") {
        public void run() {
          DistributedSystem system2 =
            InternalDistributedSystem.getAnyInstance();
          while (system2 != null) {
            system2.disconnect();
            system2 = InternalDistributedSystem.getAnyInstance();
          }
        }
      });
    }catch(Exception e ) {
	    DistributedTestCase.fail ("CoLocatedAgentDUnitTest:testAgentWithXMFile test failed", e);
    }
  }

}

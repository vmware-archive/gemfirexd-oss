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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.text.MessageFormat;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.shell.JmxOperationInvoker;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.internal.engine.Misc;

@SuppressWarnings("serial")
public class GfxdManagementTestBase extends DistributedSQLTestBase {
//  private static boolean configureManager = false;
//  private static boolean configureManagerStart = false;
//
//  private int     managerJmxPort;
//  private String  managerJmxBindAddress;
  private JMXConnector connector;

  public GfxdManagementTestBase(String name) {
    super(name);
  }

//  // To be 'invoked' on the VM which is required to be configured as a Manager
//  public static void configureManager(boolean startManager) {
//    configureManager = true;
//    configureManagerStart = startManager;
//  }
//
//  // To be 'invoked' on the VM which is not required to be a Manager any more.
//  public static void deconfigureManager() {
//    configureManager = configureManagerStart = false;
//  }

  @Override
  protected String reduceLogging() {
    return LogWriterImpl.levelToString(LogWriterImpl.FINE_LEVEL);
  }

//  @Override
//  protected void setOtherCommonProperties(Properties props, int mcastPort,
//      String serverGroups) {
//    super.setOtherCommonProperties(props, mcastPort, serverGroups);
//    setGFXDProperty(props, com.pivotal.gemfirexd.Attribute.TABLE_DEFAULT_PARTITIONED, "false");
//
//    if (GfxdManagementTestBase.configureManager) {
//      props.putAll(getManagementConfig());
//    }
//  }
//
//  protected Properties getManagementConfig() {
//    Properties props = new Properties();
//    props.put(DistributionConfig.JMX_MANAGER_NAME, String.valueOf(GfxdManagementTestBase.configureManager));
//    props.put(DistributionConfig.JMX_MANAGER_START_NAME, String.valueOf(GfxdManagementTestBase.configureManagerStart));
//
//    this.managerJmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
//    props.put(DistributionConfig.JMX_MANAGER_PORT_NAME, this.managerJmxPort);
//    try {
//      this.managerJmxBindAddress = SocketCreator.getLocalHost().getHostName();
//    } catch (UnknownHostException e) {
//      this.managerJmxBindAddress = "localhost";
//    }
//    props.put(DistributionConfig.JMX_MANAGER_BIND_ADDRESS_NAME, managerJmxBindAddress);
//
//    return props;
//  }
//
//  public JMXServiceURL getJMXServiceURL() {
//    return getJMXServiceURL(this.managerJmxBindAddress, this.managerJmxPort);
//  }

  public static JMXServiceURL getJMXServiceURL(String managerJmxBindAddress, int managerJmxPort) {
    JMXServiceURL url = null;
    try {
      url = new JMXServiceURL(MessageFormat.format(JmxOperationInvoker.JMX_URL_FORMAT, new Object[] {managerJmxBindAddress, ""+managerJmxPort}));
    } catch (MalformedURLException e) {
      url = null;
    }

    return url;
  }

  public MBeanServerConnection startJmxClient(String managerJmxBindAddress, int managerJmxPort) throws IOException {
    this.connector = JMXConnectorFactory.connect(getJMXServiceURL(managerJmxBindAddress, managerJmxPort));
    return this.connector.getMBeanServerConnection();
  }

  public void stopJmxClient() throws IOException {
    if (this.connector != null) {
      this.connector.close();
    }
  }

  public static ConnectionEndpoint retrieveJmxHostPort() {
    try {
      DistributionConfig config = Misc.getDistributedSystem().getConfig();

      return new ConnectionEndpoint(config.getJmxManagerBindAddress(), config.getJmxManagerPort());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void tearDown2() throws Exception {
    stopJmxClient();
    this.connector = null;
//    deconfigureManager();
    super.tearDown2();
  }

  protected static ObjectName getObjectName(String objectNameString) {
    return MBeanJMXAdapter.getObjectName(objectNameString);
  }

  protected static String getMemberNameOrId() {
    return MBeanJMXAdapter.getMemberNameOrId(Misc.getDistributedSystem().getDistributedMember());
  }

  protected static ObjectName getMemberObjectNamePattern() {
    return getMemberObjectNamePattern(getMemberNameOrId());
  }

  protected static ObjectName getMemberObjectNamePattern(String memberNameOrId) {
    return ManagementUtils.getMemberMBeanNamePattern(memberNameOrId);
  }

  protected static void logInfo(String statementToLog) {
    LogWriterI18n loggerI18n = InternalDistributedSystem.getLoggerI18n();
    if (loggerI18n != null) {
      loggerI18n.convertToLogWriter().info(statementToLog);
    } else {
      System.out.println(statementToLog);
    }
  }

  protected static void logFine(String statementToLog) {
    LogWriterI18n loggerI18n = InternalDistributedSystem.getLoggerI18n();
    if (loggerI18n != null) {
      loggerI18n.convertToLogWriter().fine(statementToLog);
    } else {
      System.out.println(statementToLog);
    }
  }

  protected static class ConnectionEndpoint implements Serializable {
    private static final long serialVersionUID = 4850423084173302704L;

    public String host;
    public int port;

    public ConnectionEndpoint(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }
}

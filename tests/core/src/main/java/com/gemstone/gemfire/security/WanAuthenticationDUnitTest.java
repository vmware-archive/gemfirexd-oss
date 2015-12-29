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
/**
 * 
 */
package com.gemstone.gemfire.security;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import security.CredentialGenerator;

import dunit.Host;
import dunit.VM;
import dunit.DistributedTestCase;

/**
 * @author kneeraj
 * 
 */
public class WanAuthenticationDUnitTest extends ClientAuthorizationTestBase {
  // public class WanAuthenticationDUnitTest extends DistributedTestCase {

  public WanAuthenticationDUnitTest(String name) {
    super(name);
  }

  private static Locator locator = null;

  private static String site1 = "site1";

  private static String site2 = "site2";

  private VM locator1 = null;

  private VM server1 = null;

  private VM locator2 = null;

  private VM server2 = null;

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    locator1 = host.getVM(0);
    server1 = host.getVM(1);
    locator2 = host.getVM(2);
    server2 = host.getVM(3);
  }

  private static Properties buildProperties(String clientauthenticator,
      String clientAuthInit, String accessor, Properties extraAuthProps,
      Properties extraAuthzProps) {

    Properties authProps = new Properties();
    if (clientauthenticator != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME,
          clientauthenticator);
    }
    if (accessor != null) {
      authProps.setProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME,
          accessor);
    }
    if (clientAuthInit != null) {
      authProps.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
          clientAuthInit);
    }
    if (extraAuthProps != null) {
      authProps.putAll(extraAuthProps);
    }
    if (extraAuthzProps != null) {
      authProps.putAll(extraAuthzProps);
    }
    return authProps;
  }

  public static void startLocator(String name, Integer port) {
    File logFile = new File(name + "-locator" + port.intValue() + ".log");
    try {
      Properties props = new Properties();

      props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props.setProperty(DistributionConfig.LOCATORS_NAME, 
                        DistributedTestCase.getIPLiteral() + "[" + port + "]");

      locator = Locator
          .startLocatorAndDS(port.intValue(), logFile, null, props);
    }
    catch (IOException ex) {
      fail("While starting locator on port " + port.intValue(), ex);
    }
  }

  public static void stopLocator() {
    try {
      locator.stop();
    }
    catch (Exception ex) {
      fail("While stopping locator", ex);
    }
  }

  public static void setupHubAndAddGateway(String site, String hub,
      Integer hubEP, String gateway, Integer gatewayEP, Boolean expectException) {

    try {
      Cache cache = CacheFactory.getAnyInstance();
      GatewayHub gatewayhub = cache.addGatewayHub(hub, hubEP.intValue());
      gatewayhub.setSocketBufferSize(25600);
      Gateway tmpgateway = gatewayhub.addGateway(gateway);
      setDiskStoreForGateway(cache, gateway, tmpgateway.getQueueAttributes());

      tmpgateway.addEndpoint(site, 
                             DistributedTestCase.getIPLiteral(), 
                             gatewayEP.intValue());
      gatewayhub.start();
      tmpgateway.start();
      if (expectException.equals(Boolean.TRUE)) {
        fail("Exception was expected while starting the gateway hub and gateway");
      }
    }
    catch (Exception ex) {
      if (expectException.equals(Boolean.FALSE)) {
        fail("While starting the gateway hub and gateway", ex);
      }
      else {
        getLogWriter().info(
            "setupHubAndAddGateway - was an expected exception, msg = "
                + ex.getMessage() + ", exception = " + expectException);
      }
    }
  }

  public void testWanAuthValidCredentials() {
    disconnectAllFromDS();
    Iterator iter = SecurityTestUtil.getAllGenerators().iterator();
    while (iter.hasNext()) {
      int mcastport1 = AvailablePort
          .getRandomAvailablePort(AvailablePort.JGROUPS);
      int mcastport2 = AvailablePort
          .getRandomAvailablePort(AvailablePort.JGROUPS);

      CredentialGenerator gen = (CredentialGenerator)iter.next();
      Properties extraProps = gen.getSystemProperties();

      String clientauthenticator = gen.getAuthenticator();
      String clientauthInit = gen.getAuthInit();

      Properties credentials1 = gen.getValidCredentials(1);
      if (extraProps != null) {
        credentials1.putAll(extraProps);
      }
      Properties javaProps1 = gen.getJavaProperties();

      Properties credentials2 = gen.getValidCredentials(2);
      if (extraProps != null) {
        credentials2.putAll(extraProps);
      }
      Properties javaProps2 = gen.getJavaProperties();

      Properties props1 = buildProperties(clientauthenticator, clientauthInit,
          null, credentials1, null);
      Properties props2 = buildProperties(clientauthenticator, clientauthInit,
          null, credentials2, null);

      int serverport1 = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);

      server1.invoke(SecurityTestUtil.class, "createCacheServer",
          new Object[] { props1, javaProps1, new Integer(mcastport1), "",
              new Integer(serverport1),
              new Integer(SecurityTestUtil.NO_EXCEPTION) });

      int serverport2 = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server2.invoke(SecurityTestUtil.class, "createCacheServer",
          new Object[] { props2, javaProps2, new Integer(mcastport2), "",
              new Integer(serverport2),
              new Integer(SecurityTestUtil.NO_EXCEPTION) });

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      server1.invoke(WanAuthenticationDUnitTest.class, "setupHubAndAddGateway",
          new Object[] { site1, DistributedTestCase.getIPLiteral(), 
                         new Integer(hubep1), "gateway1",
              new Integer(hubep2), Boolean.FALSE });

      server2.invoke(WanAuthenticationDUnitTest.class, "setupHubAndAddGateway",
          new Object[] { site2, DistributedTestCase.getIPLiteral(),
                         new Integer(hubep2), "gateway2",
              new Integer(hubep1), Boolean.FALSE });

    }
  }

  public void testWanAuthInvalidCredentials() {
    disconnectAllFromDS();
    Iterator iter = SecurityTestUtil.getAllGenerators().iterator();
    while (iter.hasNext()) {
      int mcastport1 = AvailablePort
          .getRandomAvailablePort(AvailablePort.JGROUPS);
      int mcastport2 = AvailablePort
          .getRandomAvailablePort(AvailablePort.JGROUPS);
      CredentialGenerator gen = (CredentialGenerator)iter.next();

      Properties extraProps = gen.getSystemProperties();

      String clientauthenticator = gen.getAuthenticator();
      String clientauthInit = gen.getAuthInit();

      Properties credentials1 = gen.getInvalidCredentials(1);
      if (extraProps != null) {
        credentials1.putAll(extraProps);
      }
      Properties javaProps1 = gen.getJavaProperties();
      Properties credentials2 = gen.getInvalidCredentials(2);
      if (extraProps != null) {
        credentials2.putAll(extraProps);
      }
      Properties javaProps2 = gen.getJavaProperties();

      Properties props1 = buildProperties(clientauthenticator, clientauthInit,
          null, credentials1, null);
      Properties props2 = buildProperties(clientauthenticator, clientauthInit,
          null, credentials2, null);

      int serverport1 = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);

      server1.invoke(SecurityTestUtil.class, "createCacheServer",
          new Object[] { props1, javaProps1, new Integer(mcastport1), "",
              new Integer(serverport1),
              new Integer(SecurityTestUtil.NO_EXCEPTION) });

      int serverport2 = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);

      server2.invoke(SecurityTestUtil.class, "createCacheServer",
          new Object[] { props2, javaProps2, new Integer(mcastport2), "",
              new Integer(serverport2),
              new Integer(SecurityTestUtil.NO_EXCEPTION) });

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      server1.invoke(WanAuthenticationDUnitTest.class, "setupHubAndAddGateway",
          new Object[] { site1, DistributedTestCase.getIPLiteral(),
                         new Integer(hubep1), "gateway1",
              new Integer(hubep2), Boolean.FALSE });

      server2.invoke(WanAuthenticationDUnitTest.class, "setupHubAndAddGateway",
          new Object[] { site2, DistributedTestCase.getIPLiteral(),
                         new Integer(hubep2), "gateway2",
              new Integer(hubep1), Boolean.TRUE });
    }
  }

  public void testWanAuthFirstInvalidSecondValidCredentials() {
    disconnectAllFromDS();
    Iterator iter = SecurityTestUtil.getAllGenerators().iterator();
    while (iter.hasNext()) {
      int mcastport1 = AvailablePort
          .getRandomAvailablePort(AvailablePort.JGROUPS);
      int mcastport2 = AvailablePort
          .getRandomAvailablePort(AvailablePort.JGROUPS);
      CredentialGenerator gen = (CredentialGenerator)iter.next();

      Properties extraProps = gen.getSystemProperties();

      String clientauthenticator = gen.getAuthenticator();
      String clientauthInit = gen.getAuthInit();

      Properties credentials1 = gen.getInvalidCredentials(1);
      if (extraProps != null) {
        credentials1.putAll(extraProps);
      }
      Properties javaProps1 = gen.getJavaProperties();
      Properties credentials2 = gen.getValidCredentials(2);
      if (extraProps != null) {
        credentials2.putAll(extraProps);
      }
      Properties javaProps2 = gen.getJavaProperties();

      Properties props1 = buildProperties(clientauthenticator, clientauthInit,
          null, credentials1, null);
      Properties props2 = buildProperties(clientauthenticator, clientauthInit,
          null, credentials2, null);

      int serverport1 = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);

      server1.invoke(SecurityTestUtil.class, "createCacheServer",
          new Object[] { props1, javaProps1, new Integer(mcastport1), "",
              new Integer(serverport1),
              new Integer(SecurityTestUtil.NO_EXCEPTION) });

      int serverport2 = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);

      server2.invoke(SecurityTestUtil.class, "createCacheServer",
          new Object[] { props2, javaProps2, new Integer(mcastport2), "",
              new Integer(serverport2),
              new Integer(SecurityTestUtil.NO_EXCEPTION) });

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      server1.invoke(WanAuthenticationDUnitTest.class, "setupHubAndAddGateway",
          new Object[] { site1, DistributedTestCase.getIPLiteral(),
                         new Integer(hubep1), "gateway1",
              new Integer(hubep2), Boolean.FALSE });

      server2.invoke(WanAuthenticationDUnitTest.class, "setupHubAndAddGateway",
          new Object[] { site2, DistributedTestCase.getIPLiteral(),
                         new Integer(hubep2), "gateway2",
              new Integer(hubep1), Boolean.TRUE });

    }
  }

  public void tearDown2() throws Exception {

    super.tearDown2();
    // close the clients first
    locator1.invoke(SecurityTestUtil.class, "closeCache");
    locator2.invoke(SecurityTestUtil.class, "closeCache");
    SecurityTestUtil.closeCache();
    // then close the servers
    server1.invoke(SecurityTestUtil.class, "closeCache");
    server2.invoke(SecurityTestUtil.class, "closeCache");
  }
}

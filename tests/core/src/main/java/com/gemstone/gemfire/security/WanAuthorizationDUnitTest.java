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
package com.gemstone.gemfire.security;

import java.util.Iterator;
import java.util.Properties;

import security.AuthzCredentialGenerator;
import security.CredentialGenerator;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;

import dunit.Host;
import dunit.VM;
import dunit.DistributedTestCase;

public class WanAuthorizationDUnitTest extends ClientAuthorizationTestBase {
  private static Cache cache = null;

  private static DistributedSystem dsys = null;

  VM site1 = null;

  VM site2 = null;

  private static final String[] gatewayExpectedExceptions = {
      NotAuthorizedException.class.getName(),
      AuthenticationFailedException.class.getName() };

  /** constructor */
  public WanAuthorizationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    site1 = host.getVM(1);
    site2 = host.getVM(2);
  }

  public void testOneSiteValidOtherInvalidCredentials() {
    Iterator iter = getAllGeneratorCombos().iterator();
    while (iter.hasNext()) {
      AuthzCredentialGenerator gen = (AuthzCredentialGenerator)iter.next();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      Properties props1 = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);

      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      Properties javaProps1 = cGen.getJavaProperties();

      Properties invalidCreateCredentials = cGen.getInvalidCredentials(1);
      Properties javaProps2 = cGen.getJavaProperties();

      getLogWriter().info(
          "testOneSiteValidOtherInvalidCredentials: For first client credentials: "
              + createCredentials);
      if (authInit != null) {
        props1.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
            authInit);
      }
      // authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props1.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "finest");

      Properties props2 = new Properties();
      props2.putAll(props1);

      props1.putAll(createCredentials);

      props2.putAll(invalidCreateCredentials);

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      site1.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props1, javaProps1, "site1", 
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep1), "gateway1", new Integer(hubep2),
              gatewayExpectedExceptions });

      site2.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props2, javaProps2, "site2",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep2), "gateway2", new Integer(hubep1),
              gatewayExpectedExceptions });

      pause(1000);

      site1.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(0) });

      site2.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(2) });

      pause(5000);

      site1.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(2) });

      site2.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(2) });

      site1.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
      site2.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
    }

  }

  public void testBothSiteValidAuthzCredentials() {
    Iterator iter = getAllGeneratorCombos().iterator();
    while (iter.hasNext()) {
      AuthzCredentialGenerator gen = (AuthzCredentialGenerator)iter.next();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      Properties props1 = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);

      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      Properties javaProps = cGen.getJavaProperties();
      getLogWriter().info(
          "testBothSiteValidAuthzCredentials: For first client credentials: "
              + createCredentials);
      if (authInit != null) {
        props1.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
            authInit);
      }
      // authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props1.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "finest");

      props1.putAll(createCredentials);

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      site1.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props1, javaProps, "site1",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep1), "gateway1", new Integer(hubep2),
              gatewayExpectedExceptions });

      site2.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props1, javaProps, "site2",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep2), "gateway2", new Integer(hubep1),
              gatewayExpectedExceptions });

      pause(1000);

      site1.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(0) });

      site2.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(2) });

      pause(5000);

      site1.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(4) });

      site2.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(4) });

      site1.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
      site2.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
    }
  }

  public void testOneSiteValidOtherInvalidAuthzCredentials() {
    Iterator iter = getAllGeneratorCombos().iterator();
    while (iter.hasNext()) {
      AuthzCredentialGenerator gen = (AuthzCredentialGenerator)iter.next();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      Properties props1 = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);

      Properties createCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      Properties javaProps1 = cGen.getJavaProperties();

      Properties invalidCreateCredentials = gen.getDisallowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      Properties javaProps2 = cGen.getJavaProperties();

      getLogWriter().info(
          "testOneSiteValidOtherInvalidAuthzCredentials: For first client credentials: "
              + createCredentials);
      if (authInit != null) {
        props1.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
            authInit);
      }
      // authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props1.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "finest");

      Properties props2 = new Properties();
      props2.putAll(props1);

      props1.putAll(createCredentials);

      props2.putAll(invalidCreateCredentials);

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      site1.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props1, javaProps1, "site1",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep1), "gateway1", new Integer(hubep2),
              gatewayExpectedExceptions });

      site2.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props2, javaProps2, "site2",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep2), "gateway2", new Integer(hubep1),
              gatewayExpectedExceptions });

      pause(1000);

      site1.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(0) });

      site2.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(2) });

      pause(5000);

      site1.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(2) });

      site2.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(4) });

      site1.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
      site2.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
    }

  }

  public void testBothSiteInvalidAuthzCredentials() {
    Iterator iter = getAllGeneratorCombos().iterator();
    while (iter.hasNext()) {
      AuthzCredentialGenerator gen = (AuthzCredentialGenerator)iter.next();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      Properties props1 = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);
      Properties javaProps1 = cGen.getJavaProperties();

      Properties invalidCreateCredentials = gen.getDisallowedCredentials(
          new OperationCode[] { OperationCode.PUT },
          new String[] { regionName }, 1);
      Properties javaProps2 = cGen.getJavaProperties();

      getLogWriter().info(
          "testBothSiteInvalidAuthzCredentials: For first client credentials: "
              + invalidCreateCredentials);
      if (authInit != null) {
        props1.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
            authInit);
      }
      // authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props1.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "finest");

      props1.putAll(invalidCreateCredentials);

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      site1.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props1, javaProps1, "site1",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep1), "gateway1", new Integer(hubep2),
              gatewayExpectedExceptions });

      site2.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props1, javaProps2, "site2",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep2), "gateway2", new Integer(hubep1),
              gatewayExpectedExceptions });

      pause(1000);

      site1.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(0) });

      site2.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(2), new Integer(2) });

      pause(5000);

      site1.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(2) });

      site2.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(2) });

      site1.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
      site2.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
    }
  }

  public void testValidInvalidDestroyAuthzCredentials() {
    Iterator iter = getAllGeneratorCombos().iterator();
    while (iter.hasNext()) {
      AuthzCredentialGenerator gen = (AuthzCredentialGenerator)iter.next();
      CredentialGenerator cGen = gen.getCredentialGenerator();
      Properties extraAuthProps = cGen.getSystemProperties();
      Properties extraAuthzProps = gen.getSystemProperties();
      String authenticator = cGen.getAuthenticator();
      String authInit = cGen.getAuthInit();
      String accessor = gen.getAuthorizationCallback();

      Properties props1 = buildProperties(authenticator, accessor, false,
          extraAuthProps, extraAuthzProps);

      Properties validDestroyCredentials = gen.getAllowedCredentials(
          new OperationCode[] { OperationCode.DESTROY },
          new String[] { regionName }, 1);
      Properties javaProps1 = cGen.getJavaProperties();

      Properties invalidDestroyCredentials = gen.getDisallowedCredentials(
          new OperationCode[] { OperationCode.DESTROY },
          new String[] { regionName }, 1);
      Properties javaProps2 = cGen.getJavaProperties();
      getLogWriter().info(
          "testValidInvalidDestroyAuthzCredentials: For first client credentials: "
              + invalidDestroyCredentials);
      if (authInit != null) {
        props1.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
            authInit);
      }
      // authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props1.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "finest");

      Properties props2 = new Properties();
      props2.putAll(props1);
      props1.putAll(validDestroyCredentials);
      props2.putAll(invalidDestroyCredentials);

      int hubep1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      int hubep2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

      site1.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props1, javaProps1, "site1",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep1), "gateway1", new Integer(hubep2),
              gatewayExpectedExceptions });

      site2.invoke(WanAuthorizationDUnitTest.class, "createTheSite",
          new Object[] { props2, javaProps2, "site2",
              DistributedTestCase.getIPLiteral(),
              new Integer(hubep2), "gateway2", new Integer(hubep1),
              gatewayExpectedExceptions });

      pause(1000);

      site1.invoke(WanAuthorizationDUnitTest.class, "doNPuts", new Object[] {
          new Integer(4), new Integer(0) });

      pause(5000);

      site1.invoke(WanAuthorizationDUnitTest.class, "doNDestroys",
          new Object[] { new Integer(2), new Integer(0) });

      site2.invoke(WanAuthorizationDUnitTest.class, "doNDestroys",
          new Object[] { new Integer(2), new Integer(2) });

      pause(5000);

      site1.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(2) });

      site2.invoke(WanAuthorizationDUnitTest.class, "verifyNInCache",
          new Object[] { new Integer(0) });

      site1.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
      site2.invoke(WanAuthorizationDUnitTest.class, "closeTheSite",
                   new Object[] { gatewayExpectedExceptions } );
    }
  }

  public static void closeTheSite(String[] expectedExceptions) {
    LogWriter logger = dsys.getLogWriter();
    if (logger != null) {
      SecurityTestUtil.removeExpectedExceptions(expectedExceptions, logger);
    }
    cache.close();
    dsys.disconnect();
  }

  public static void createTheSite(Properties props, Object javaProps,
      String site, String hub, Integer hubEP, String gateway, Integer gatewayEP,
      String[] expectedExceptions) {

    SecurityTestUtil.clearStaticSSLContext();
    SecurityTestUtil.setJavaProps((Properties)javaProps);

    int mcastport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    Integer mport = new Integer(mcastport);
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, mport.toString());
    props.setProperty("log-level", "finest");

    dsys = DistributedSystem.connect(props);
    cache = CacheFactory.create(dsys);

    SecurityTestUtil.addExpectedExceptions(expectedExceptions, dsys.getLogWriter());

    try {
      AttributesFactory factory = new AttributesFactory();
      // factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setScope(Scope.LOCAL);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setEnableGateway(true);
      RegionAttributes attrs = factory.create();
      cache.createRegion(regionName, attrs);

      GatewayHub gatewayhub = cache.addGatewayHub(hub, hubEP.intValue());
      gatewayhub.setSocketBufferSize(25600);
      Gateway tmpgateway = gatewayhub.addGateway(gateway);
      setDiskStoreForGateway(cache, gateway, tmpgateway.getQueueAttributes());
      tmpgateway.addEndpoint(site, DistributedTestCase.getIPLiteral(), 
                             gatewayEP.intValue());
      gatewayhub.start();
      tmpgateway.start();

    }
    catch (Exception ex) {
      getLogWriter()
          .fine("exception received while starting gateway ignorable");
    }
  }

  public static void doNPuts(Integer n, Integer offset) {
    Region reg = cache.getRegion(regionName);
    for (int i = offset.intValue(); i < (n.intValue() + offset.intValue()); i++) {
      reg.put(SecurityTestUtil.keys[i], SecurityTestUtil.values[i]);
    }
  }

  public static void doNDestroys(Integer n, Integer offset) {
    Region reg = cache.getRegion(regionName);
    for (int i = offset.intValue(); i < (n.intValue() + offset.intValue()); i++) {
      reg.destroy(SecurityTestUtil.keys[i]);
    }
  }

  public static void verifyNInCache(Integer n) {
    Region reg = cache.getRegion(regionName);
    long start = System.currentTimeMillis();
    long waitTime = 60000;
    while(n.intValue() != reg.keySet().size()) {
      if(System.currentTimeMillis() - start > waitTime) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        fail("interrupted");
      }
    }
    assertEquals("entry count mismatch", n.intValue(), reg.keySet().size());
  }

  protected static Properties buildProperties(String authenticator,
      String accessor, boolean isAccessorPP, Properties extraAuthProps,
      Properties extraAuthzProps) {

    Properties authProps = new Properties();
    if (authenticator != null) {
      authProps.setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, authenticator);
    }
    if (accessor != null) {
      if (isAccessorPP) {
        authProps.setProperty(
            DistributionConfig.SECURITY_CLIENT_ACCESSOR_PP_NAME, accessor);
      }
      else {
        authProps.setProperty(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME,
            accessor);
      }
    }
    return SecurityTestUtil.concatProperties(new Properties[] { authProps,
        extraAuthProps, extraAuthzProps });
  }

  /**
   * close the clients and teh servers
   */
  public void tearDown2() throws Exception {
    super.tearDown2();
    site1.invoke(getClass(), "closeCache");
    site2.invoke(getClass(), "closeCache");
  }

  /**
   * close the cache
   *
   */
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}

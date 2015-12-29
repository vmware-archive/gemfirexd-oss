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
package com.gemstone.gemfire.internal.cache.wan.misc;

import java.util.Iterator;
import java.util.Properties;

import security.CredentialGenerator;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.SecurityTestUtil;

public class NewWanAuthenticationDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public NewWanAuthenticationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Authentication test for new WAN with valid credentials. Although, nothing
   * related to authentication has been changed in new WAN, this test case is
   * added on request from QA for defect 44650.
   */
  public void testWanAuthValidCredentials() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    getLogWriter().info("Created locator on local site");

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    getLogWriter().info("Created locator on remote site");

    Iterator iter = SecurityTestUtil.getAllGenerators().iterator();
    while (iter.hasNext()) {
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

      vm2.invoke(WANTestBase.class, "createSecuredCache", new Object[] {
          props1, javaProps1, lnPort });
      getLogWriter().info("Created secured cache in vm2");

      vm3.invoke(WANTestBase.class, "createSecuredCache", new Object[] {
          props2, javaProps2, nyPort });
      getLogWriter().info("Created secured cache in vm3");

      vm2.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          false, 100, 10, false, false, null, true });
      getLogWriter().info("Created sender in vm2");

      vm3.invoke(WANTestBase.class, "createReceiverInSecuredCache",
          new Object[] { nyPort });
      getLogWriter().info("Created receiver in vm3");

      vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
          testName + "_RR", "ln", isOffHeap() });
      getLogWriter().info("Created RR in vm2");
      vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
          testName + "_RR", null, isOffHeap() });
      getLogWriter().info("Created RR in vm3");

      vm2.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm2.invoke(WANTestBase.class, "waitForSenderRunningState",
          new Object[] { "ln" });
      getLogWriter().info("Done successfully.");
    }

  }

  /**
   * Test authentication with new WAN with invalid credentials. Although,
   * nothing related to authentication has been changed in new WAN, this test
   * case is added on request from QA for defect 44650.
   */
  public void testWanAuthInvalidCredentials() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    getLogWriter().info("Created locator on local site");

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    getLogWriter().info("Created locator on remote site");

    Iterator iter = SecurityTestUtil.getAllGenerators().iterator();
    while (iter.hasNext()) {
      CredentialGenerator gen = (CredentialGenerator)iter.next();
      getLogWriter().info("Picked up credential: " + gen);

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

      getLogWriter().info("Done building auth properties");

      vm2.invoke(WANTestBase.class, "createSecuredCache", new Object[] {
          props1, javaProps1, lnPort });
      getLogWriter().info("Created secured cache in vm2");

      vm3.invoke(WANTestBase.class, "createSecuredCache", new Object[] {
          props2, javaProps2, nyPort });
      getLogWriter().info("Created secured cache in vm3");

      vm2.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          false, 100, 10, false, false, null, true });
      getLogWriter().info("Created sender in vm2");

      vm3.invoke(WANTestBase.class, "createReceiverInSecuredCache",
          new Object[] { nyPort });
      getLogWriter().info("Created receiver in vm3");

      vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
          testName + "_RR", "ln", isOffHeap() });
      getLogWriter().info("Created RR in vm2");
      vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
          testName + "_RR", null, isOffHeap() });
      getLogWriter().info("Created RR in vm3");

      try {
        vm2.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
        fail("Authentication Failed: While starting the sender, an exception should have been thrown");
      } catch (Exception e) {
        if (!(e.getCause().getCause() instanceof AuthenticationFailedException)) {
          fail("Authentication is not working as expected");
        }
      }
    }
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
}

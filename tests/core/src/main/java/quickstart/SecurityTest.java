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
package quickstart;

import java.util.Properties;

/**
 * This quickstart Test tests the Security Functionality.
 * Following is the brief explanation:
 * 1) Starting the Server with security properties (SecurityServer.java)
 * 2) Starting the Client with valid PUT credential (SecurityClient.java)
 * 3) Client does some put and get.
 * 4) Client expect NotAuthorizedException for get operation
 *    which is not authorized.
 * 
 * @author Rajesh Kumar
 */

public class SecurityTest extends QuickstartTestCase {

  protected ProcessWrapper client;
  protected ProcessWrapper server;

  public SecurityTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.client != null)
      this.client.destroy();
    if (this.server != null)
      this.server.destroy();
  }

  @Override
  protected Properties createProperties() {
    Properties props = super.createProperties();
    props.setProperty("gemfire.security-log-level", "warning");
    return props;
  }
  
  public void testSecurity() throws Exception {
    // start up SecurityServer
    getLogWriter().info("[testSecurity] start up SecurityServer");
    String serverTempArg[] = new String[] { "ldap",
        "ou=ldapTesting,dc=pune,dc=gemstone,dc=com" };
    this.server = new ProcessWrapper(SecurityServer.class, serverTempArg);
    this.server.execute(createProperties());
    
    this.server.waitForOutputToMatch("Please start the security client, "
        + "and press Enter when the client finishes all the operations\\.");

    // start up SecurityClient
    getLogWriter().info("[testSecurity] start up SecurityClient");
    String clientTempArg[] = new String[] { "gemfire6", "gemfire6" };
    this.client = new ProcessWrapper(SecurityClient.class, clientTempArg);
    this.client.execute(createProperties());

    this.client.waitFor();
    printProcessOutput(this.client, "CLIENT");

    // Stopping the server
    this.server.sendInput();
    this.server.waitFor();
    printProcessOutput(this.server, "SERVER");

    assertOutputMatchesGoldenFile(this.client.getOutput(), "SecurityClient.txt");
    assertOutputMatchesGoldenFile(this.server.getOutput(), "SecurityServer.txt");
  }
}

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

public class MultiuserSecurityTest extends QuickstartTestCase {

  protected ProcessWrapper client;
  protected ProcessWrapper server;
  protected Thread clientThread;
  protected Thread serverThread;

  public MultiuserSecurityTest(String name) {
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

  public void testPutAndGetOperations() throws Exception {
    startup("[testPutAndGetOperations]");
    this.client.waitForOutputToMatch(" Your selection: ");
    this.client.sendInput("1");
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();

    this.client.waitFor();
    printProcessOutput(this.client, "testPutAndGetOperations-CLIENT");

    // Stopping the server
    this.server.sendInput();
    this.server.waitForOutputToMatch("Closing the cache and disconnecting\\.");
    this.server.waitForOutputToMatch("Closed the Server Cache");
    this.server.waitFor();
    printProcessOutput(this.server, "testPutAndGetOperations-SERVER");

    assertOutputMatchesGoldenFile(this.client.getOutput(), "MultiuserSecurityClient.txt");
  }

  public void testFunctionExecution() throws Exception {
    startup("[testFunctionExecution]");
    this.client.waitForOutputToMatch(" Your selection: ");
    this.client.sendInput("2");
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();

    this.client.waitFor();
    printProcessOutput(this.client, "testFunctionExecution-CLIENT");

    // Stopping the server
    this.server.sendInput();
    this.server.waitForOutputToMatch("Closing the cache and disconnecting\\.");
    this.server.waitForOutputToMatch("Closed the Server Cache");
    this.server.waitFor();
    printProcessOutput(this.server, "testFunctionExecution-SERVER");

    assertOutputMatchesGoldenFile(this.client.getOutput(), "MultiuserSecurityClientFE.txt");
  }

  public void testCQs() throws Exception {
    startup("[testCQs]");
    this.client.waitForOutputToMatch(" Your selection: ");
    this.client.sendInput("3");
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();
    this.client.waitForOutputToMatch("Press Enter to continue\\.");
    this.client.sendInput();

    this.client.waitFor();
    printProcessOutput(this.client, "testCQs-CLIENT");

    // Stopping the server
    this.server.sendInput();
    this.server.waitForOutputToMatch("Closing the cache and disconnecting\\.");
    this.server.waitForOutputToMatch("Closed the Server Cache");
    this.server.waitFor();
    printProcessOutput(this.server, "testCQs-SERVER");

    assertOutputMatchesGoldenFile(this.client.getOutput(), "MultiuserSecurityClientCQ.txt");
  }

  private void startup(String testName) throws Exception {
    // start up MultiuserSecurityServer
    getLogWriter().info(testName + " start up MultiuserSecurityServer");
    this.server = new ProcessWrapper(MultiuserSecurityServer.class);
    this.server.execute(createProperties());

    this.server.waitForOutputToMatch("This example demonstrates Security "
        + "functionalities\\.");
    this.server.waitForOutputToMatch("This program is a server, listening "
        + "on a port for client requests\\.");
    this.server.waitForOutputToMatch("The client in this example is configured "
        + "with security properties\\.");
    this.server.waitForOutputToMatch("Setting security properties for server");
    this.server.waitForOutputToMatch("Connecting to the distributed system "
        + "and creating the cache\\.");
    this.server.waitForOutputToMatch("Example region, /exampleRegion, created "
        + "in cache\\.");
    this.server.waitForOutputToMatch("Example region, /functionServiceExampleRegion,"
        + " created in cache and populated\\.");
    this.server.waitForOutputToMatch("Please start the security client "
        + "and press Enter when the client finishes all the operations\\.");

    // start up MultiuserSecurityClient
    getLogWriter().info(testName + " start up MultiuserSecurityClient");
    this.client = new ProcessWrapper(MultiuserSecurityClient.class);
    this.client.execute(createProperties());
  }
}

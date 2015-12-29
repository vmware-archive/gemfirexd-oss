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

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerLauncher;

/**
 * Tests the functionality of the continuous query quickstart example.
 */
public class CqClientTest extends QuickstartTestCase {

  protected ProcessWrapper clientVM;
  protected ProcessWrapper serverVM;

  public CqClientTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.clientVM != null) this.clientVM.destroy();
    if (this.serverVM != null) this.serverVM.destroy();
  }
  
  public void testCqClient() throws Exception {
    boolean serverStarted = false;
    try {
      getLogWriter().info("[testCqClient] start the server");
      // TODO: int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      String[] args = { "start", "cache-xml-file=xml/CqServer.xml", "-J-Dgemfire.mcast-port=0", "-J-Dgemfire.log-level=fine"};//, "server-port=" + serverPort};
      this.serverVM = new ProcessWrapper(CacheServerLauncher.class, args, false);
      this.serverVM.execute();

      this.serverVM.waitForOutputToMatch("CacheServer pid: [0-9]* status: running");
      this.serverVM.waitFor();
      getLogWriter().info("[testCqClient] server started");
      serverStarted = true;

      // start up continuous querying
      getLogWriter().info("[testCqClient] start up CqClient");
      this.clientVM = new ProcessWrapper(CqClient.class);
      Properties clientProps = createProperties();
      // TODO: clientProps.setProperty("gemfire.mcast-port", String.valueOf(serverPort));
      this.clientVM.execute(clientProps);

      getLogWriter().info("[testCqClient] waiting for 1st \"Press Enter to continue.\"");
      this.clientVM.waitForOutputToMatch("Press Enter to continue.");
      this.clientVM.sendInput();
      getLogWriter().info("[testCqClient] waiting for 2nd \"Press Enter to continue.\"");
      this.clientVM.waitForOutputToMatch("Press Enter to continue.");
      this.clientVM.sendInput();
      getLogWriter().info("[testCqClient] waiting for 3rd \"Press Enter to continue.\"");
      this.clientVM.waitForOutputToMatch("Press Enter to continue.");
      this.clientVM.sendInput();
      getLogWriter().info("[testCqClient] waiting for 4th \"Press Enter to continue.\"");
      this.clientVM.waitForOutputToMatch("Press Enter to continue.");
      this.clientVM.sendInput();
      getLogWriter().info("[testCqClient] waiting for 5th \"Press Enter to continue.\"");
      this.clientVM.waitForOutputToMatch("Press Enter to continue.");
      this.clientVM.sendInput();
      getLogWriter().info("[testCqClient] joining to CqClient");
      this.clientVM.waitFor();
      printProcessOutput(this.clientVM);

      // validate output from process
      assertOutputMatchesGoldenFile(this.clientVM.getOutput(), "CqClient.txt");
    }
    finally {
      if (serverStarted) {
        ProcessWrapper stop = new ProcessWrapper(CacheServerLauncher.class, new String[] { "stop" });
        stop.execute();
        stop.waitForOutputToMatch("The CacheServer has stopped\\.");
        stop.waitFor();
      }
    }
  }
}


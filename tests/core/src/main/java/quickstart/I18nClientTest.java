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

import com.gemstone.gemfire.internal.cache.CacheServerLauncher;

public class I18nClientTest extends QuickstartTestCase {
  
  protected ProcessWrapper client;
  protected ProcessWrapper server;
  
  public I18nClientTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.client != null) this.client.destroy();
    if (this.server != null) this.server.destroy();
  }
  
  public void testI18nClient() throws Exception {
    boolean serverStarted = false;
    try {
      getLogWriter().info("[testI18nClient] start up CacheServer");
      String[] args = { "start", "cache-xml-file=xml/I18nServer_utf8.xml", "-J-Dgemfire.mcast-port=0", "-J-Dgemfire.log-level=fine" };
      this.server = new ProcessWrapper(CacheServerLauncher.class, args);
      this.server.execute();

      this.server.waitForOutputToMatch("CacheServer pid: [0-9]* status: running");
      this.server.waitFor();
      serverStarted = true;

      getLogWriter().info("[testI18nClient] start up I18nClient");
      this.client = new ProcessWrapper(I18nClient.class);
      this.client.execute(createProperties());

      getLogWriter().info("[testI18nClient] joining to I18nClient");
      this.client.waitFor();
      printProcessOutput(this.client);

      // validate output from process
      assertOutputMatchesGoldenFile(this.client.getOutput(), "I18nClient.txt");
    }
    finally {
      if (serverStarted) {
        ProcessWrapper pw = new ProcessWrapper(CacheServerLauncher.class,
            new String[] { "stop" });
        pw.execute(createProperties());
        pw.waitForOutputToMatch("The CacheServer has stopped.");
        pw.waitFor();
      }
    }
  }
}

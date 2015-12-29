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

public class ServerManagedCachingTest extends QuickstartTestCase {

  protected ProcessWrapper server;
  protected ProcessWrapper worker;
  protected ProcessWrapper consumer;

  public ServerManagedCachingTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.worker != null)
      this.worker.destroy();
    if (this.consumer != null)
      this.consumer.destroy();
    if (this.server != null)
      this.server.destroy();
  }

  public void testServerManagedCaching() throws Exception {
    boolean serverStarted = false;
    try {
      // start up MultiuserSecurityServer
      getLogWriter().info("[testServerManagedCaching] start up CacheServer");
      String[] args = { "start", "cache-xml-file=xml/Server.xml", "-J-Dgemfire.mcast-port=0", "-J-Dgemfire.log-level=fine" };
      this.server = new ProcessWrapper(CacheServerLauncher.class, args);
      this.server.execute(createProperties());

      this.server.waitForOutputToMatch("CacheServer pid: [0-9]* status: running");
      this.server.waitFor();
      serverStarted = true;    

      getLogWriter().info("[testServerManagedCaching]  start up ClientConsumer");
      this.consumer = new ProcessWrapper(ClientConsumer.class, new String[] {"all-keys"});
      this.consumer.execute(createProperties());
      this.consumer.waitForOutputToMatch("Connecting to the distributed system and creating the cache\\.");
      this.consumer.waitForOutputToMatch("Example region \"/exampleRegion\" created in cache\\. ");
      this.consumer.waitForOutputToMatch("Asking the server to send me all data additions, updates, and destroys\\. "); 
      this.consumer.waitForOutputToMatch("The data region has a listener that reports all changes to standard out\\.");
      this.consumer.waitForOutputToMatch("");
      this.consumer.waitForOutputToMatch("Please run the worker client in another session\\. It will update the");
      this.consumer.waitForOutputToMatch("cache and the server will forward the updates to me. Note the listener");
      this.consumer.waitForOutputToMatch("output in this session\\.");
      this.consumer.waitForOutputToMatch("");
      this.consumer.waitForOutputToMatch("When the other client finishes, hit Enter to exit this program\\.");
    
      getLogWriter().info("[testServerManagedCaching]  start up ClientWorker");
      this.worker = new ProcessWrapper(ClientWorker.class);
      this.worker.execute(createProperties());
      this.worker.waitForOutputToMatch("Connecting to the distributed system and creating the cache\\.");
      this.worker.waitForOutputToMatch("Example region \"/exampleRegion\" created in cache\\.");
      this.worker.waitForOutputToMatch("Getting three values from the cache server\\.");
      this.worker.waitForOutputToMatch("This will cause the server's loader to run, which will add the values");
      this.worker.waitForOutputToMatch("to the server cache and return them to me. The values will also be");
      this.worker.waitForOutputToMatch("forwarded to any other client that has subscribed to the region\\.");
      this.worker.waitForOutputToMatch("Getting key key0");
      this.worker.waitForOutputToMatch("    Received afterCreate event for entry: key0, LoadedValue0");
      this.worker.waitForOutputToMatch("Getting key key1");
      this.worker.waitForOutputToMatch("    Received afterCreate event for entry: key1, LoadedValue1");
      this.worker.waitForOutputToMatch("Getting key key2");
      this.worker.waitForOutputToMatch("    Received afterCreate event for entry: key2, LoadedValue2");
      this.worker.waitForOutputToMatch("Note the other client's region listener in response to these gets\\.");
      this.worker.waitForOutputToMatch("Press Enter to continue\\.");
      this.worker.sendInput();
      this.worker.waitForOutputToMatch("Changing the data in my cache - all destroys and updates are forwarded");
      this.worker.waitForOutputToMatch("through the server to other clients. Invalidations are not forwarded\\.");
      this.worker.waitForOutputToMatch("Putting new value for key0");
      this.worker.waitForOutputToMatch("    Received afterUpdate event for entry: key0, ClientValue0");
      this.worker.waitForOutputToMatch("Invalidating key1");
      this.worker.waitForOutputToMatch("    Received afterInvalidate event for entry: key1");
      this.worker.waitForOutputToMatch("Destroying key2");
      this.worker.waitForOutputToMatch("    Received afterDestroy event for entry: key2");
      this.worker.waitForOutputToMatch("Closing the cache and disconnecting\\.");
      this.worker.waitForOutputToMatch("In the other session, please hit Enter in the Consumer client");
      this.worker.waitForOutputToMatch("and then stop the cacheserver with 'gfsh stop server --dir=<serverDirectory>'\\.");

      this.consumer.waitForOutputToMatch("    Received afterCreate event for entry: key0, LoadedValue0");
      this.consumer.waitForOutputToMatch("    Received afterCreate event for entry: key1, LoadedValue1");
      this.consumer.waitForOutputToMatch("    Received afterCreate event for entry: key2, LoadedValue2");
      this.consumer.waitForOutputToMatch("    Received afterUpdate event for entry: key0, ClientValue0");
      this.consumer.waitForOutputToMatch("    Received afterDestroy event for entry: key2");
      this.consumer.sendInput();
      this.consumer.waitForOutputToMatch("Closing the cache and disconnecting\\.");

      this.worker.waitFor();
      printProcessOutput(this.worker, "WORKER");

      this.consumer.waitFor();
      printProcessOutput(this.consumer, "CONSUMER");
    } finally {
      if (serverStarted) {
        ProcessWrapper pw = new ProcessWrapper(CacheServerLauncher.class,
            new String[] {"stop"});
        pw.execute(createProperties());
        pw.waitForOutputToMatch("The CacheServer has stopped\\.");
        pw.waitFor();
      }
    }
  }
}

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

/**
 * This quickstart test tests the Durable Functionality. The steps are as
 * follows:
 * 
 * 1) Start the Server (DurableServer.java). 2) Start the Client
 * (DurableClient.java). It expects a Y or N for registering clients. 3) As the
 * client has started for the first time we need to register interest and hence
 * Y is given. In DurableClient only 2 keys K3 & K4 have registered interest as
 * durable. For K1 & K2 interests are registered as nonDurable. 4) Update values
 * in the Server. 5) Client goes down. 6) Server is updated again. 7) Client
 * comes up. 8) Client doesn't register interest. 9) Updating server. 10) Print
 * the output of DurableClient. NOTE: we get values for only K3 & K4 (durable
 * ones) and not for K1 & K2. 11) Print the output of DurableServer 12) Compare
 * the outputs of client and server wrt the standard ones.
 * 
 * @author Deepkumar Varma
 */
public class DurableTest extends QuickstartTestCase {

  /**
   * The Region scope is DISTRIBUTED_NO_ACK. Without this sleep the client
   * intermittently receives the queued durable events twice, causing test
   * failures.
   */
  private static final long SLEEP_BEFORE_CLOSING_CLIENT_MILLIS = 5000;
  
  private ProcessWrapper clientFirstTime;
  private ProcessWrapper clientSecondTime;
  private ProcessWrapper server;
  
  public DurableTest(String name) {
    super(name);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.clientFirstTime != null) this.clientFirstTime.destroy();
    if (this.clientSecondTime != null) this.clientSecondTime.destroy();
    if (this.server != null) this.server.destroy();
  }
  
  @Override
  protected GoldenComparator createGoldenComparator() {
    return new StringGoldenComparator(expectedProblemLines());
  }
  
  public void testDurable() throws Exception { // TODO: remove sleep calls
    // Start the Server (DurableServer.java).
    writeLogMessage("Starting up the DurableServer...");
    this.server = new ProcessWrapper(DurableServer.class);
    this.server.execute(createProperties());
    this.server.waitForOutputToMatch("\\[DurableServer\\] Example region.*has been created in the cache.");
    this.server.waitForOutputToMatch("\\[DurableServer\\] Please start the DurableClient now...");
    writeLogMessage("Done starting up the DurableServer.");
    
    // Start the Client (DurableClient.java).
    writeLogMessage("Starting up the DurableClient...");
    this.clientFirstTime = new ProcessWrapper(DurableClient.class);
    this.clientFirstTime.execute(createProperties());
    this.clientFirstTime.failIfOutputMatches("\\[DurableClient\\] Region.*does not exist, exiting...", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] Press Enter in the server window to do an update on the server.", 10000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] Then press Enter in the client window to continue.", 10000);
    writeLogMessage("Done starting up the DurableClient.");
    
    // Initialize values in server cache.
    writeLogMessage("Initializing the values in the DurableServer...");
    this.server.sendInput(); // create value1
    this.server.waitForOutputToMatch("\\[DurableServer\\] Press Enter in the server window to update the values in the cache, or 'Exit' to shut down\\.", 10000);
    writeLogMessage("Done updating the values in the DurableServer.");

    writeLogMessage("Receiving the new values from /exampleRegion in the DurableClient.");
    this.clientFirstTime.waitForOutputToMatch("    Received afterRegionLive event, sent to durable clients after the server has finished replaying stored events.", 2000);
    this.clientFirstTime.waitForOutputToMatch("    Received afterCreate event for entry: key1, value1", 2000);
    this.clientFirstTime.waitForOutputToMatch("    Received afterCreate event for entry: key2, value2", 2000);
    this.clientFirstTime.waitForOutputToMatch("    Received afterCreate event for entry: key3, value3", 2000);
    this.clientFirstTime.waitForOutputToMatch("    Received afterCreate event for entry: key4, value4", 2000);
    writeLogMessage("DurableClient has received the new values.");

    // Update values in server while client is up.
    writeLogMessage("Updating the values in the DurableServer...");
    this.server.sendInput(); // update to value11
    this.server.waitForOutputToMatch("\\[DurableServer\\] Press Enter in the server window to update the values in the cache, or 'Exit' to shut down\\.", 10000);
    writeLogMessage("Done updating the values in the DurableServer.");

    writeLogMessage("Receiving the new values from /exampleRegion in the DurableClient.");
    this.clientFirstTime.waitForOutputToMatch("    Received afterUpdate event for entry: key1, value11", 2000);
    this.clientFirstTime.waitForOutputToMatch("    Received afterUpdate event for entry: key2, value22", 2000);
    this.clientFirstTime.waitForOutputToMatch("    Received afterUpdate event for entry: key3, value33", 2000);
    this.clientFirstTime.waitForOutputToMatch("    Received afterUpdate event for entry: key4, value44", 2000);
    writeLogMessage("DurableClient has received the new values.");

    writeLogMessage("Showing the new values from /exampleRegion in the DurableClient and closing.");
    sleep(SLEEP_BEFORE_CLOSING_CLIENT_MILLIS);
    this.clientFirstTime.sendInput(); // close client with keepalive=true
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\]", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] After the update on the server, the region contains:", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] key1 => value11", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] key2 => value22", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] key3 => value33", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] key4 => value44", 2000);
    writeLogMessage("DurableClient has displayed the new values.");
    
    writeLogMessage("Closing DurableClient with keepalive=true.");
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\]", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] Closing the cache and disconnecting from the distributed system...", 2000);
    this.clientFirstTime.waitForOutputToMatch("\\[DurableClient\\] Finished disconnecting from the distributed system. Exiting...", 10000);
    this.clientFirstTime.waitFor();
    this.clientFirstTime.destroy();
    writeLogMessage("DurableClient has closed with keepalive=true.");

    // Update values in the server while client is down.
    writeLogMessage("Updating the values in the DurableServer...");
    this.server.sendInput(); // update to value111
    this.server.waitForOutputToMatch("\\[DurableServer\\] Press Enter in the server window to update the values in the cache, or 'Exit' to shut down\\.", 10000);
    writeLogMessage("Done updating the values in the DurableServer.");

    writeLogMessage("Restarting the DurableClient...");
    this.clientSecondTime = new ProcessWrapper(DurableClient.class);
    this.clientSecondTime.execute(createProperties());
    this.clientSecondTime.failIfOutputMatches("\\[DurableClient\\] Region.*does not exist, exiting...", 2000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] Press Enter in the server window to do an update on the server.", 10000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] Then press Enter in the client window to continue.", 10000);
    writeLogMessage("Done restarting the DurableClient.");

    // Update values in the server.
    writeLogMessage("Updating the values in the DurableServer");
    this.server.sendInput(); // update to value111
    this.server.waitForOutputToMatch("\\[DurableServer\\] Press Enter in the server window to update the values in the cache, or 'Exit' to shut down\\.", 10000);
    writeLogMessage("Done updating the values in the DurableServer.");

    writeLogMessage("Receiving the new values from /exampleRegion in the DurableClient.");
    this.clientSecondTime.waitForOutputToMatch("    Received afterUpdate event for entry: key3, value333", 10000);
    this.clientSecondTime.waitForOutputToMatch("    Received afterUpdate event for entry: key4, value444", 10000);
    this.clientSecondTime.waitForOutputToMatch("    Received afterRegionLive event, sent to durable clients after the server has finished replaying stored events.", 10000);
    this.clientSecondTime.waitForOutputToMatch("    Received afterUpdate event for entry: key1, value1111", 10000);
    this.clientSecondTime.waitForOutputToMatch("    Received afterUpdate event for entry: key2, value2222", 10000);
    this.clientSecondTime.waitForOutputToMatch("    Received afterUpdate event for entry: key3, value3333", 10000);
    this.clientSecondTime.waitForOutputToMatch("    Received afterUpdate event for entry: key4, value4444", 10000);
    writeLogMessage("DurableClient has received the new values.");

    writeLogMessage("Showing the new values from /exampleRegion in the DurableClient and closing.");
    sleep(SLEEP_BEFORE_CLOSING_CLIENT_MILLIS);
    this.clientSecondTime.sendInput("CloseCache");
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\]", 2000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] After the update on the server, the region contains:", 2000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] key1 => value1111", 2000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] key2 => value2222", 2000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] key3 => value3333", 2000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] key4 => value4444", 2000);
    writeLogMessage("DurableClient has displayed the new values.");

    // Stop the client.
    writeLogMessage("Closing DurableClient with keepalive=false.");
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] Closing the cache and disconnecting from the distributed system...", 2000);
    this.clientSecondTime.waitForOutputToMatch("\\[DurableClient\\] Finished disconnecting from the distributed system. Exiting...", 10000);
    this.clientSecondTime.waitFor();
    this.clientSecondTime.destroy();
    writeLogMessage("DurableClient has closed with keepalive=false.");

    // Stop the server.
    writeLogMessage("Stopping the DurableServer");
    this.server.sendInput("Exit");
    this.server.waitForOutputToMatch("\\[DurableServer\\] Finished disconnecting from the distributed system. Exiting...", 10000);
    this.server.waitFor();
    writeLogMessage("DurableServer has stopped.");

    // Print the output of the DurableServer.
    printProcessOutput(clientFirstTime, "clientFirstTime");
    printProcessOutput(clientSecondTime, "clientSecondTime");
    printProcessOutput(server, "server");

    // Validate the process outputs
    assertOutputMatchesGoldenFile(this.clientFirstTime.getOutput(), "DurableClientFirst.txt");
    assertOutputMatchesGoldenFile(this.clientSecondTime.getOutput(), "DurableClientSecond.txt");
    assertOutputMatchesGoldenFile(this.server.getOutput(), "DurableServer.txt");
  }

  private void writeLogMessage(Object message) {
    getLogWriter().info("[DurableTest] " + message);
  }
}

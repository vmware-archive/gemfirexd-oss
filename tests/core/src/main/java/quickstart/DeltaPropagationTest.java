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

public class DeltaPropagationTest extends QuickstartTestCase {
  
  protected ProcessWrapper feederVM;
  protected ProcessWrapper receiverVM;
  protected ProcessWrapper serverVM;
  
  public DeltaPropagationTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.feederVM != null)
      this.feederVM.destroy();
    if (this.receiverVM != null)
      this.receiverVM.destroy();
    if (this.serverVM != null)
      this.serverVM.destroy();
  }
  
  public void testDeltaPropagation() throws Exception {
    
    getLogWriter().info("[testDeltaPropagation] start DeltaPropagationTest Server");
    
    this.serverVM = new ProcessWrapper(DeltaPropagationServer.class);
    this.serverVM.execute(createProperties());
    this.serverVM.waitForOutputToMatch("This example demonstrates delta propagation. This program is a server,");
    this.serverVM.waitForOutputToMatch("listening on a port for client requests. The client program connects and");
    this.serverVM.waitForOutputToMatch("requests data. The client in this example is also configured to produce/consume");
    this.serverVM.waitForOutputToMatch("information on data destroys and updates\\.");
    this.serverVM.waitForOutputToMatch("To stop the program, press Ctrl c in console\\.");
    this.serverVM.waitForOutputToMatch("Connecting to the distributed system and creating the cache\\.\\.\\.");
    this.serverVM.waitForOutputToMatch("Connected to the distributed system\\.");
    this.serverVM.waitForOutputToMatch("Created the cache\\.");
    this.serverVM.waitForOutputToMatch("Please press Enter to stop the server\\.");
    
    getLogWriter().info("[testDeltaPropagation] start DeltaPropagationTest Feeder");
    
    this.feederVM = new ProcessWrapper(DeltaPropagationClientFeeder.class);
    this.feederVM.execute(createProperties());
    this.feederVM.waitForOutputToMatch("Connecting to the distributed system and creating the cache\\.");
    this.feederVM.waitForOutputToMatch("Delta is 50%\\.");
    this.feederVM.waitForOutputToMatch("Please press Enter to start the feeder\\.");
    
    getLogWriter().info(
    "[testDeltaPropagation] start DeltaPropagationTest Receiver");
    this.receiverVM = new ProcessWrapper(DeltaPropagationClientReceiver.class);
    this.receiverVM.execute(createProperties());
    this.receiverVM.waitForOutputToMatch("Connecting to the distributed system and creating the cache\\.");
    this.receiverVM.waitForOutputToMatch("Please press Enter to stop the receiver\\.");
    
    this.feederVM.sendInput();
    getLogWriter().info("[testDeltaPropagation] joining to feeder");
    this.feederVM.waitFor();
    printProcessOutput(this.feederVM, "FEEDER");
    assertOutputMatchesGoldenFile(this.feederVM.getOutput(), "DeltaPropagationFeeder.txt");
    Thread.sleep(1000);
    
    this.receiverVM.sendInput();
    getLogWriter().info("[testDeltaPropagation] joining to receiver");
    this.receiverVM.waitFor();
    printProcessOutput(this.receiverVM, "RECEIVER");
    assertOutputMatchesGoldenFile(this.receiverVM.getOutput(), "DeltaPropagationReceiver.txt");
    
    this.serverVM.sendInput();
    getLogWriter().info("[testDeltaPropagation] joining to server");
    this.serverVM.waitFor();
    printProcessOutput(this.serverVM, "SERVER");
    assertOutputMatchesGoldenFile(this.serverVM.getOutput(), "DeltaPropagationServer.txt");
  }
}

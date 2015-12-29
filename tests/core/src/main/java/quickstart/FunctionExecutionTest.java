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

public class FunctionExecutionTest extends QuickstartTestCase {

  protected ProcessWrapper regionVM1;
  protected ProcessWrapper regionVM2;

  public FunctionExecutionTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.regionVM1 != null)
      this.regionVM1.destroy();
    if (this.regionVM2 != null)
      this.regionVM2.destroy();
  }

  public void testFunctionExecution() throws Exception {
    // create partition region in VM1
    getLogWriter().info("[testFunctionExecution] start PartitionRegion on Peer1");
    this.regionVM1 = new ProcessWrapper(FunctionExecutionPeer1.class);
    this.regionVM1.execute(createProperties());
    this.regionVM1.waitForOutputToMatch("^Please start Other Peer And Then Press Enter to continue\\.$");

    // create partition region in VM2
    this.regionVM2 = new ProcessWrapper(FunctionExecutionPeer2.class);
    this.regionVM2.execute(createProperties());
    this.regionVM2.waitForOutputToMatch("^Press Enter to continue\\.$");
    this.regionVM2.sendInput();
    
    this.regionVM2.waitForOutputToMatch("^Press Enter to continue\\.$");
    this.regionVM2.sendInput();

    this.regionVM2.waitForOutputToMatch("^Press Enter to continue\\.$");
    this.regionVM2.sendInput();
    
    this.regionVM2.waitForOutputToMatch("^Closing the cache and disconnecting\\.$");
    
    this.regionVM1.sendInput();
    this.regionVM1.waitForOutputToMatch("^Closing the cache and disconnecting\\.$");
    
    this.regionVM1.waitFor();
    this.regionVM2.waitFor();
    
    printProcessOutput(this.regionVM1, "PEER1");
    printProcessOutput(this.regionVM2, "PEER2");
    
    assertOutputMatchesGoldenFile(this.regionVM1.getOutput(), "FunctionExecutionPeer1.txt");
    assertOutputMatchesGoldenFile(this.regionVM2.getOutput(), "FunctionExecutionPeer2.txt");
  }
}

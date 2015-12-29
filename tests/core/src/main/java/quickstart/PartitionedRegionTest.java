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
 * Tests the functionality of the partition region quickstart example.
 * 
 * @author gthombar
 * 
 */
public class PartitionedRegionTest extends QuickstartTestCase {

  protected ProcessWrapper regionVM1;
  protected ProcessWrapper regionVM2;

  public PartitionedRegionTest(String name) {
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

  @Override
  protected String[] expectedProblemLines() {
    return new String[] { "^\\[warning.*PartitionedRegion.*PartitionedRegion Message Processor.*\\] .*Redundancy has dropped below 1 configured copies to 0 actual copies for /PartitionedRegion" };
  }
  
  public void testPartitionedRegion() throws Exception {
    // create partition region in VM1
    getLogWriter().info("[testPartitionRegion] start up PartitionRegion");
    this.regionVM1 = new ProcessWrapper(PartitionedRegionVM1.class);
    this.regionVM1.execute(createProperties());
    this.regionVM1.waitForOutputToMatch("^Please start VM2\\.$");

    // create partition region in VM2
    this.regionVM2 = new ProcessWrapper(PartitionedRegionVM2.class);
    this.regionVM2.execute(createProperties());
    this.regionVM2.waitForOutputToMatch("^Please press Enter in VM1\\.$");

    // Put entries in partition region from VM1
    this.regionVM1.sendInput();
    this.regionVM1.waitForOutputToMatch("^Please press Enter in VM2\\.$");

    // Read entries from partition region
    this.regionVM2.sendInput();
    this.regionVM2.waitForOutputToMatch("^Please press Enter in VM1 again\\.$");

    // Perform destroy and invalidate operations on partition region from VM1
    this.regionVM1.sendInput();
    this.regionVM1.waitForOutputToMatch("^Please press Enter in VM2 again\\.$");

    // Read entries from partition region and close cache in the VM2
    this.regionVM2.sendInput();
    this.regionVM1.waitForOutputToMatch(".*Redundancy has dropped.*");
    this.regionVM1.sendInput();

    this.regionVM1.waitFor();
    this.regionVM2.waitFor();
    printPartitionOutPut(this.regionVM1, "PartitionRegion in VM1");
    printPartitionOutPut(this.regionVM2, "PartitionRegion in VM2");

    assertOutputMatchesGoldenFile(this.regionVM1.getOutput(), "PartitionRegionVM1.txt");
    assertOutputMatchesGoldenFile(this.regionVM2.getOutput(), "PartitionRegionVM2.txt");
  }

  private void printPartitionOutPut(ProcessWrapper process, String title) {
    System.out.println("\n------------------ BEGIN " + title + " ------------------");
    System.out.println(process.getOutput());
    System.out.println("------------------- END " + title + " -------------------");
  }
}

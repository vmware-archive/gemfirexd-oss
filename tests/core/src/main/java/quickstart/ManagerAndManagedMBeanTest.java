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
 * Tests the functionality of the Manager and Managed MXBean quickstart example.
 * 
 * @author GemStone Systems, Inc.
 * @since 7.0
 */
public class ManagerAndManagedMBeanTest extends QuickstartTestCase {

  protected ProcessWrapper managedNode;
  protected ProcessWrapper managerNode;

  public ManagerAndManagedMBeanTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.managedNode != null)
      this.managedNode.destroy();
    if (this.managerNode != null)
      this.managerNode.destroy();
  }

  public void testManagingManagedMXBean() throws Exception {

    // start up Managed Node
    getLogWriter().info("[testManagingManagedMXBean] start up ManagedNode");
    this.managedNode = new ProcessWrapper(ManagedNode.class);
    this.managedNode.execute(createProperties());

    this.managedNode.waitForOutputToMatch("Start Manager Node and wait till it completes");

    // start up Managing Node
    getLogWriter().info("[testManagingManagedMXBean] start up managerNode");
    this.managerNode = new ProcessWrapper(ManagerNode.class);
    this.managerNode.execute(createProperties());

    this.managerNode.waitForOutputToMatch("Press enter in Managed Node");

    // complete Managing Node
    getLogWriter().info("[testManagingManagedMXBean] joining to managerNode");
    this.managerNode.waitFor();
    printProcessOutput(this.managerNode, "MANAGER");

    // complete Managed Node
    this.managedNode.sendInput();
    this.managedNode.waitForOutputToMatch("Closing the cache and disconnecting.");

    getLogWriter().info("[testManagingManagedMXBean] joining to managedNodeThread");
    this.managedNode.waitFor();   
    printProcessOutput(this.managerNode, "managedNode");
    // TODO: this test should be using GoldenComparator to compare output to a golden file

    getLogWriter().info("[testManagingManagedMXBean] End of Test testManagingManagedMXBean");
  }
}

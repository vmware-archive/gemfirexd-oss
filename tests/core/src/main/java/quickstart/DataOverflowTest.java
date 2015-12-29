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

import java.util.*;
import java.io.File;

/**
 * Tests the functionality of the DataOverflow quickstart example.
 * 
 * @author Kirk Lund
 * @since 4.1.1
 */
public class DataOverflowTest extends QuickstartTestCase {

  private ProcessWrapper process;

  public DataOverflowTest(String name) {
    super(name);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.process != null) this.process.destroy();
  }
  
  public void testDataOverflow() throws Exception {

    // Create the overflow directory.
    File overflowDir = new File("overflowData1");
    overflowDir.mkdir();

    // Start up the DataOverflow quickstart example.
    getLogWriter().info("[testDataOverflow] start up DataOverflow");
    this.process = new ProcessWrapper(DataOverflow.class);
    this.process.execute(createProperties());
    this.process.waitForOutputToMatch("Press Enter in this shell to continue\\.");

    // Validate the contents of overflowDir.
    assertTrue("overflowDir is missing", overflowDir.exists());
    File dbFile = new File(overflowDir, "DRLK_IFds1.lk");
    assertTrue(dbFile + " is missing files=" + Arrays.asList(overflowDir.list()), dbFile.exists());

    this.process.sendInput();
    this.process.waitForOutputToMatch("Press Enter in this shell to continue\\.");

    this.process.sendInput();
    getLogWriter().info("[testDataOverflow] joining to DataOverflow");
    this.process.waitFor();
    printProcessOutput(this.process);
    assertFalse(dbFile.toString() + " still exists", dbFile.exists());

    // Validate output from the process.
    assertOutputMatchesGoldenFile(this.process.getOutput(), "DataOverflow.txt");

    overflowDir.delete();
  }
}

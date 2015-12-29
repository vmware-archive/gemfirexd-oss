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

import java.io.IOException;

import junit.framework.AssertionFailedError;

/**
 * Abstract test case for tests verifying that quickstart test output with a
 * log message of warning/error/severe will cause expected failures.
 * 
 * @author Kirk Lund
 */
public abstract class FailWithProblemInOutputTestCase extends FailOutputTestCase {

  FailWithProblemInOutputTestCase(String name) {
    super(name);
  }
  
  @Override
  protected String[] expectedProblemLines() {
    return new String[] { ".*" + name() + ".*" };
  }
  
  public void testFailWithProblemLogMessageInOutput() throws InterruptedException, IOException {
    this.process = new ProcessWrapper(getClass());
    this.process.execute(createProperties());
    this.process.waitForOutputToMatch("Begin " + name() + "\\.main");
    this.process.waitForOutputToMatch("Press Enter to continue\\.");
    this.process.sendInput();
    this.process.waitForOutputToMatch("End " + name() + "\\.main");
    this.process.waitFor();
    printProcessOutput(process);
    String goldenString = "Begin " + name() + ".main" + "\n" 
        + "Press Enter to continue." + "\n" 
        + "End " + name() + ".main" + "\n";
    innerPrintOutput(goldenString, "GOLDEN");
    try {
      assertOutputMatchesGoldenFile(this.process.getOutput(), goldenString);
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionFailedError expected) {
//      System.out.println("Problem: " + problem());
//      System.out.println("AssertionFailedError message: " + expected.getMessage());
      assertTrue(expected.getMessage().contains(problem()));
    }
  }
}

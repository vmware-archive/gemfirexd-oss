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
 * Verifies that quickstart test output missing an expected line (at the middle 
 * of the golden file) will fail with that line as the failure message.
 * 
 * @author Kirk Lund
 */
public class FailWithLineMissingFromMiddleOfOutputJUnitTest extends FailOutputTestCase {

  public FailWithLineMissingFromMiddleOfOutputJUnitTest() {
    super("FailWithLineMissingFromMiddleOfOutputJUnitTest");
  }
  
  @Override
  String problem() {
    return "This line is missing in actual output.";
  }
  
  @Override
  void outputProblem(String message) {
    // this tests that the message is missing from output
  }
  
  public void testFailWithLineMissingFromEndOfOutput() throws InterruptedException, IOException {
    this.process = new ProcessWrapper(getClass());
    this.process.execute(createProperties());
    this.process.waitForOutputToMatch("Begin " + name() + "\\.main");
    this.process.waitForOutputToMatch("Press Enter to continue\\.");
    this.process.sendInput();
    this.process.waitFor();
    printProcessOutput(this.process);
    String goldenString = "Begin " + name() + ".main" + "\n" 
        + "Press Enter to continue." + "\n" 
        + problem() + "\n"
        + "End " + name() + ".main" + "\n";
    innerPrintOutput(goldenString, "GOLDEN");
    try {
      assertOutputMatchesGoldenFile(this.process.getOutput(), goldenString);
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionFailedError expected) {
      assertTrue("AssertionFailedError message should contain \"" + problem() + "\"", 
          expected.getMessage().contains(problem()));
    }
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithLineMissingFromMiddleOfOutputJUnitTest().execute();
  }
}

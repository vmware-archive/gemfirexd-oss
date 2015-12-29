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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Abstract test case for quickstart testing framework. This provides basis for
 * unit tests which involve an example that is expected to always pass.
 * 
 * @author Kirk Lund
 */
public abstract class PassWithExpectedProblemTestCase extends QuickstartTestCase {

  private ProcessWrapper process;
  
  PassWithExpectedProblemTestCase(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.process != null) this.process.destroy();
  }
  
  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  @Override
  protected String[] expectedProblemLines() {
    return new String[] { ".*" + name() + ".*", "^\\[" + problem() + ".*\\] This is an expected problem in the output" };
  }
  
  String name() {
    return getClass().getSimpleName();
  }
  
  abstract String problem();
  
  abstract void outputProblem(String message);
  
  public void testPassWithExpectedProblem() throws InterruptedException, IOException {
    // output has an expected warning/error/severe message and should pass
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
        + "\n"
        + "^\\[" + problem() + ".*\\] This is an expected problem in the output" + "\n"
        + "End " + name() + ".main" + "\n";
    innerPrintOutput(goldenString, "GOLDEN");
    String[] printMe = expectedProblemLines();
    for (String str : printMe) {
      System.out.println(str);
    }
    assertOutputMatchesGoldenFile(this.process.getOutput(), goldenString);
  }
  
  void execute() throws IOException {
    System.out.println("Begin " + name() + ".main");
    System.out.println("Press Enter to continue.");
    BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
    inputReader.readLine();
    outputProblem("This is an expected problem in the output");
    System.out.println("End " + name() + ".main");
  }
}

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
 * Basic unit testing of the quickstart testing framework. This tests an 
 * example which is expected to always pass.
 * 
 * @author Kirk Lund
 */
public class PassJUnitTest extends QuickstartTestCase {
  
  private ProcessWrapper process;
  
  public PassJUnitTest() {
    super("PassJUnitTest");
  }

  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  public void setUp() throws Exception {
    super.setUp();
  }
  
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.process != null) this.process.destroy();
  }

  String name() {
    return getClass().getSimpleName();
  }

  public void testPass() throws InterruptedException, IOException {
    // output has no problems and should pass
    this.process = new ProcessWrapper(getClass());
    this.process.execute(createProperties());
    assertTrue(this.process.isAlive());
    this.process.waitForOutputToMatch("Begin " + name() + "\\.main");
    this.process.waitForOutputToMatch("Press Enter to continue\\.");
    this.process.sendInput();
    this.process.waitForOutputToMatch("End " + name() + "\\.main");
    this.process.waitFor();
    printProcessOutput(process);
    String goldenString = "Begin " + name() + ".main" + "\n" 
        + "Press Enter to continue." + "\n" 
        + "End " + name() + ".main" + "\n";
    assertOutputMatchesGoldenFile(this.process, goldenString);

    assertFalse(this.process.isAlive());
    //assertFalse(this.process.getOutputReader());
    assertFalse(this.process.getStandardOutReader().isAlive());
    assertFalse(this.process.getStandardErrorReader().isAlive());
  }

  void execute() throws IOException {
    System.out.println("Begin " + name() + ".main");
    System.out.println("Press Enter to continue.");
    BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
    inputReader.readLine();
    System.out.println("End " + name() + ".main");
  }
  
  public static void main(String[] args) throws Exception {
    new PassJUnitTest().execute();
  }
}

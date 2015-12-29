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
 * Abstract test case for tests verifying that quickstart test output will
 * cause expected failures.
 * 
 * @author Kirk Lund
 */
public abstract class FailOutputTestCase extends QuickstartTestCase {
  
  protected ProcessWrapper process;
  
  FailOutputTestCase(String name) {
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

  String name() {
    return getClass().getSimpleName();
  }
  
  abstract String problem();
  
  abstract void outputProblem(String message);
  
  void execute() throws IOException {
    System.out.println("Begin " + name() + ".main");
    System.out.println("Press Enter to continue.");
    BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
    inputReader.readLine();
    outputProblem(problem());
    System.out.println("End " + name() + ".main");
  }
}

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

import junit.framework.AssertionFailedError;

public class FailWithTimeoutOfWaitForOutputToMatchJUnitTest extends FailOutputTestCase {
  
  public FailWithTimeoutOfWaitForOutputToMatchJUnitTest() {
    super("FailWithTimeoutOfWaitForOutputToMatchJUnitTest");
  }
  
  @Override
  String problem() {
    return "This is an extra line";
  }
  
  @Override
  void outputProblem(String message) {
    System.out.println(message);
  }
  
  public void testFailWithTimeoutOfWaitForOutputToMatch() throws Exception {
    // output has an extra line and should fail
    this.process = new ProcessWrapper(getClass());
    this.process.execute(createProperties());
    this.process.waitForOutputToMatch("Begin " + name() + "\\.main");
    try {
      this.process.waitForOutputToMatch(problem());
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionFailedError expected) {
      assertTrue(expected.getMessage().contains(problem()));
    }
    // the following should generate no failures if timeout and tearDown are all working properly
    assertNotNull(this.process);
    assertTrue(this.process.isAlive());
    tearDown();
    this.process.waitFor();
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithTimeoutOfWaitForOutputToMatchJUnitTest().execute();
  }
}

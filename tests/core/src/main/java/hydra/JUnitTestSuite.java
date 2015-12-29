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
package hydra;

import junit.framework.*;

/**
 * A <code>JUnitTestSuite</code> is a <code>TestSuite</code> that
 * can detect when tests are hung.
 *
 * @see dunit.DistributedTestCase
 */
public class JUnitTestSuite extends TestSuite {
  final int maxRunSecs = Integer.getInteger("JUnitTestSuite.maxRunSecs", 0).intValue();
  
  public JUnitTestSuite() {
    super();
  }
  public void runTest(final Test test, final TestResult result) {
    if (maxRunSecs <= 0) {
      test.run(result);
    } else {
      Runnable r = new Runnable() {
          public void run() {
            test.run(result);
          }
        };
      Thread t = new Thread(r);
      t.start();
      try {
        t.join(maxRunSecs * 1000);
      } catch (InterruptedException ex) {
      }
      if (t.isAlive()) {
        t.interrupt();
        result.stop();
        throw new JUnitTestTimedOutException("Test " + test
                                             + " timed out after "
                                             + maxRunSecs + " seconds.");
      }
    }
  }
  
}

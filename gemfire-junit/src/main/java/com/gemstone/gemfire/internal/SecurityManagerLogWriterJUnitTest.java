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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.GemFireTestCase;
import com.gemstone.gemfire.internal.shared.NativeCalls;
import io.snappydata.test.util.TestException;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This class tests refactored ManagerLogWriter, SecurityManagerLogWriter
 * and GemFireStatSampler 
 * {@link com.gemstone.gemfire.internal.security.SecurityManagerLogWriter}.
 *
 * @author Gester Zhou
 *
 */
public class SecurityManagerLogWriterJUnitTest extends TestCase {

  public SecurityManagerLogWriterJUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  ////////  Test Methods

  public final Logger logger = LogManager.getLogger(getClass());

  /**
   * run the script and check the result.
   */
  public void testScript() throws Throwable {
    String localPath = GemFireTestCase.getResourcesDir() +
        "/bin/testManagerLogWriter.sh";
    String gemfire = System.getProperty("gemfire.home");
    if (gemfire == null) {
      gemfire = GemFireTestCase.getResourcesDir() + "/../../product";
    }
    String command = localPath + " GEMFIRE=" + gemfire;

    if (NativeCalls.getInstance().getOSType().isWindows()) {
      return;
    }

    logger.setLevel(Level.INFO);
    logger.info("Calling " + command);
    final Process proc = Runtime.getRuntime().exec(command);
    String result = GemFireTestCase.getProcessOutput(proc, 0, 120000, null);
    logger.info("Done calling script, result is " + result);
    for (int i=1; i<=13; i++) {
      if (result.indexOf("Test "+i+" PASSED") < 0) { // did not return normal result for test N
        throw new TestException("Test "+i+" failed");
      }
    }
  }
}

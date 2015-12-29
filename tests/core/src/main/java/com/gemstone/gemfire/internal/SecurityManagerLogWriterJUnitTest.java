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

import hydra.HostHelper;
import hydra.Log;
import hydra.ProcessMgr;

import util.TestException;
import junit.framework.*;

/**
 * This class tests refactored ManagerLogWriter, SecurityManagerLogWriter
 * and GemFireStatSampler 
 * {@link SecurityManagerLogWriter}.
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

  /**
   * run the script and check the result.
   */
  public void testScript() throws Throwable {
    String localPath =  System.getProperty("JTESTS")+"/bin/testManagerLogWriter.sh";
    String gemfire = System.getProperty("gemfire.home");
    if (gemfire == null) {
      gemfire = System.getProperty("JTESTS")+"/../../product";
    }
    String command = localPath + " GEMFIRE=" + gemfire;

    if (HostHelper.isWindows()) {
      return;
    }

    Log.createLogWriter("my.log", "info");
    Log.getLogWriter().info("Calling "+command);
    String result = ProcessMgr.fgexec(command, 0);
    Log.getLogWriter().info("Done calling script, result is " + result);
    for (int i=1; i<=13; i++) {
      if (result.indexOf("Test "+i+" PASSED") < 0) { // did not return normal result for test N
        throw new TestException("Test "+i+" failed");
      }
    }
  }
}

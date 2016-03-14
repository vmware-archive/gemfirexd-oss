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
package com.gemstone.gemfire;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import junit.framework.*;

/**
 * This is an abstract superclass for classes that test GemFire.  It
 * has setUp() and tearDown() methods that create and initialize a
 * GemFire connection.
 *
 * @author davidw
 *
 */
public abstract class GemFireTestCase extends TestCase {

  /**
   * Thank you, JUnit 3.8
   */
  public GemFireTestCase() {
  }

  public GemFireTestCase(String name) {
    super(name);
  }

  ////////  Test life cycle methods

  protected void setUp() throws Exception {
    Properties p = new Properties();
    // make it a loner
    p.setProperty("mcast-port", "0");
    p.setProperty("locators", "");
    p.setProperty(DistributionConfig.NAME_NAME, this.getName());
    DistributedSystem.connect(p);
  }

  protected void tearDown() throws Exception {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if (ds != null) {
      ds.disconnect();
    }
  }

  /////////  Helper methods

  public static String getResourcesDir() {
    String testDir = System.getProperty("store.test.resourceDir");
    if (testDir != null && testDir.length() > 0) {
      return testDir;
    } else {
      // for IDEA runs
      return "../src/test/resources";
    }
  }

  public static String getProcessOutput(final Process p,
      final int expectedExitValue, final int maxWaitMillis,
      final int[] exitValue) throws IOException, InterruptedException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(
        p.getInputStream()));
    final StringBuilder res = new StringBuilder();
    final int loopWaitMillis = 100;
    int maxTries = maxWaitMillis / loopWaitMillis;
    boolean doExit = false;
    final char[] cbuf = new char[1024];
    while (maxTries-- > 0) {
      if (doExit || reader.ready()) {
        int readChars;
        if (reader.ready() && (readChars = reader.read(cbuf)) > 0) {
          res.append(cbuf, 0, readChars);
          if (doExit) {
            // check for any remaining data
            while ((readChars = reader.read(cbuf)) > 0) {
              res.append(cbuf, 0, readChars);
            }
            break;
          }
        }
        else {
          if (doExit) {
            break;
          }
          try {
            if (exitValue != null) {
              exitValue[0] = p.exitValue();
            }
            else {
              assertEquals(expectedExitValue, p.exitValue());
            }
            doExit = true;
            // try one more time for any remaining data
            continue;
          } catch (IllegalThreadStateException itse) {
            // continue in the loop
            Thread.sleep(loopWaitMillis);
          }
        }
      }
      else {
        try {
          if (exitValue != null) {
            exitValue[0] = p.exitValue();
          }
          else {
            assertEquals(expectedExitValue, p.exitValue());
          }
          doExit = true;
          // try one more time for any remaining data
          continue;
        } catch (IllegalThreadStateException itse) {
          // continue in the loop
          Thread.sleep(loopWaitMillis);
        }
      }
    }
    return res.toString();
  }

  /**
   * Strip the package off and gives just the class name.
   * Needed because of Windows file name limits.
   */
  private String getShortClassName() {
    String result = this.getClass().getName();
    int idx = result.lastIndexOf('.');
    if (idx != -1) {
      result = result.substring(idx+1);
    }
    return result;
  }
  
  /**
   * Returns a unique name for this test method.  It is based on the
   * name of the class as well as the name of the method.
   */
  protected String getUniqueName() {
    return getShortClassName() + "_" + this.getName();
  }

  /**
   * Assert an Invariant condition on an object.
   * @param inv the Invariant to assert. If null, this method just returns
   * @param obj object to assert the Invariant on.
   */
  protected void assertInvariant(Invariant inv, Object obj) {
    if (inv == null) return;
    InvariantResult result = inv.verify(obj);
    assertTrue(result.message, result.valid);
  }
}

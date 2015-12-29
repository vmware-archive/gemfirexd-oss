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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

/**
 * Tests the quickstart testing framework.
 * 
 * @author Kirk Lund
 */
public class QuickstartTestCaseJUnitTest extends TestCase {

  public QuickstartTestCaseJUnitTest(String name) {
    super(name);
  }

//  public void testFailingDueToOutput() {
//    // execute FailingDueToOutput and ensure that everything fails with proper failure message
//  }
  
  
    public void testFailWithUnexpectedWarningInOutput() {
      // output has an unexpected warning message and should fail
    }
    
    public void testFailWithUnexpectedErrorInOutput() {
      // output has an unexpected error message and should fail
    }
    
    public void testFailWithUnexpectedSevereInOutput() {
      // output has an unexpected severe message and should fail
    }
    
    public void testFailWithExceptionInOutput() {
      // output has an exception stack trace and should fail
    }
  
  // TODO: launches a process that hangs -- ensure it times out and destroys the process
  
}

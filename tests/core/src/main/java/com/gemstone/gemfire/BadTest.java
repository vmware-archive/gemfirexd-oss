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

import junit.framework.*;

/**
 * This test provides examples of a test failing and a test getting an
 * error.  We use it to test JUnit failure reporting.
 */
public class BadTest extends TestCase {

  public BadTest(String name) {
    super(name);
  }

  ////////  Test Methods

  public void testFailure() {
    fail("I'm failing away...");
  }

  public void testError() {
    String s = "I've failed";
    throw new RuntimeException(s);
  }

}

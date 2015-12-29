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
   * @param the obj to assert the Invariant on.
   */
  protected void assertInvariant(Invariant inv, Object obj) {
    if (inv == null) return;
    InvariantResult result = inv.verify(obj);
    assertTrue(result.message, result.valid);
  }
}

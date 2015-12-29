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
package com.gemstone.gemfire.admin.jmx;

//import com.gemstone.gemfire.admin.*;
//import com.gemstone.gemfire.admin.jmx.internal.*;
//import dunit.Host;
//import javax.management.*;
//import javax.management.remote.*;

/**
 * Tests the functionality of the <code>GemFireHealth</code> admin
 * interface via JMX MBeans accessed remotely using RMI.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class GemFireHealthJMXRMIDUnitTest 
  extends GemFireHealthJMXDUnitTest {

  ////////  Constructors

  /**
   * Creates a new <code>GemFireHealthJMXRMIDUnitTest</code>
   */
  public GemFireHealthJMXRMIDUnitTest(String name) {
    super(name);
  }

  protected boolean isRMI() {
    return true;
  }

}

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

import com.gemstone.gemfire.admin.*;

/**
 * Tests the functionality of the <code>SystemMember</code> admin
 * interface via JMX MBeans.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class SystemMemberJMXDUnitTest extends SystemMemberDUnitTest {

  ////////  Constructors

  /**
   * Creates a new <code>SystemMemberJMXDUnitTest</code>
   */
  public SystemMemberJMXDUnitTest(String name) {
    super(name);
  }

  protected boolean isJMX() {
    return true;
  }
  
}

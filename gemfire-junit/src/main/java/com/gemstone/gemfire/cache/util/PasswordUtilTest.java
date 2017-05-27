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
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.internal.ConnectionCountProbe;
import com.gemstone.gemfire.cache.server.internal.ServerMetricsImpl;
import com.gemstone.gemfire.internal.util.PasswordUtil;

import junit.framework.Assert;
import junit.framework.TestCase;

public class PasswordUtilTest extends TestCase {
  public void testPasswordUtil() {
    String x = "password";
    String z = null;
    try {
      //System.out.println(x);
      String y = PasswordUtil.encrypt(x);
      //System.out.println(y);
      y = "encrypted(" + y + ")";
      z = PasswordUtil.decrypt(y);
      //System.out.println(z);
      assertEquals(x, z);
    }
    catch (Exception ex) {
      fail("Test failed due to " + ex);
    }
  }
}

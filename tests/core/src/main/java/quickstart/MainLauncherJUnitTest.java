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

import java.lang.reflect.Method;

import junit.framework.TestCase;

/**
 * Quick sanity tests to make sure MainLauncher is functional.
 * 
 * @author Kirk Lund
 */
public class MainLauncherJUnitTest extends TestCase {

  private static boolean flag = false;
  
  public MainLauncherJUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() {
    flag = false;
  }
  
  @Override
  public void tearDown() {
    flag = false;
  }
  
  public void testInvokeMainWithNullArgs() throws Exception {
    assertFalse(flag);
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = null;
    mainMethod.invoke(null, new Object[] { args });
    assertTrue(flag);
  }
  
  public void testInvokeMainWithEmptyArgs() throws Exception {
    assertFalse(flag);
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[0];
    mainMethod.invoke(null, new Object[] { args });
    assertTrue(flag);
  }
  
  public void testInvokeMainWithOneArg() throws Exception {
    assertFalse(flag);
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[] { "arg0" };
    mainMethod.invoke(null, new Object[] { args });
    assertTrue(flag);
  }
  
  public void testInvokeMainWithTwoArgs() throws Exception {
    assertFalse(flag);
    Class<?> clazz = getClass();
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[] { "arg0", "arg1" };
    mainMethod.invoke(null, new Object[] { args });
    assertTrue(flag);
  }
  
  public void testInvokeMainWithMainLauncherWithNoArgs() throws Exception {
    assertFalse(flag);
    Class<?> clazz = MainLauncher.class;
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[] { getClass().getName() };
    mainMethod.invoke(null, new Object[] { args });
    assertTrue(flag);
  }
  
  public void testInvokeMainWithMainLauncherWithOneArg() throws Exception {
    assertFalse(flag);
    Class<?> clazz = MainLauncher.class;
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[] { getClass().getName(), "arg0" };
    mainMethod.invoke(null, new Object[] { args });
    assertTrue(flag);
  }
  
  public void testInvokeMainWithMainLauncherWithTwoArgs() throws Exception {
    assertFalse(flag);
    Class<?> clazz = MainLauncher.class;
    Method mainMethod = clazz.getMethod("main", String[].class);
    String[] args = new String[] { getClass().getName(), "arg0", "arg1" };
    mainMethod.invoke(null, new Object[] { args });
    assertTrue(flag);
  }
  
  public static void main(String... args) throws Exception {
    flag = true;
  }
}

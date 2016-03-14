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

import java.util.*;
import junit.framework.*;

/**
 * This test prints out the version information obtained from the
 * {@link GemFireVersion} class.  It provides a record of what version
 * of GemFire (and the JDK) was used to run the unit tests.
 */
public class GemFireVersionTest extends TestCase {

  /**
   * Prints both the GemFire version info and the system properties.
   * We have to print both 
   */
  public void testPrintInfo() {
    GemFireVersion.print(System.out);
    System.out.flush();
  }

  public void testSystemProperties() {
    // Can't use props.list(System.out) with JUnit under Ant, plus it
    // truncates the values.

    Properties props = System.getProperties();
    Enumeration theEnum = props.propertyNames();
    while (theEnum.hasMoreElements()) {
      String name = (String) theEnum.nextElement();
      String value = props.getProperty(name);
      System.out.println(name + "=" + value);
    }
    System.out.flush();
  }

  /**
   * Ant closes System.out after it runs a test method.  See bug
   * 29041.
   */
  public void testSystemOutNotClosed() {
    assertTrue(!System.out.checkError());
  }
  
  public void testMajorMinorVersions() {
    assertEquals(1, GemFireVersion.getMajorVersion("1.0.3"));
    assertEquals(33, GemFireVersion.getMajorVersion("33.0.3"));
    
    assertEquals(7, GemFireVersion.getMinorVersion("1.7.3"));
    assertEquals(79, GemFireVersion.getMinorVersion("1.79.3"));
    assertEquals(0, GemFireVersion.getMinorVersion("1.RC1"));
    assertEquals(5, GemFireVersion.getMinorVersion("1.5Beta2"));

    assertEquals(13, GemFireVersion.getBuild("7.0.2.13"));
    assertEquals(0, GemFireVersion.getBuild("1.7.3"));
    assertEquals(0, GemFireVersion.getBuild("1.79.3"));
    assertEquals(0, GemFireVersion.getBuild("1.RC1"));
    assertEquals(0, GemFireVersion.getBuild("1.5Beta2"));

    assertTrue("7.0 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0", "7.0.2.14", true) < 0);
    assertTrue("7.0.0 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.0", "7.0.2.14", true) < 0);
    assertTrue("7.0.2 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.2", "7.0.2.14", true) < 0);
    assertTrue("7.0.3 should be > 7.0.2.14", GemFireVersion.compareVersions("7.0.3", "7.0.2.14", true) > 0);
    assertTrue("7.0.1.15 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.1.15", "7.0.2.14", true) < 0);
    assertTrue("7.0.2.13 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.2.13", "7.0.2.14", true) < 0);
    assertTrue("7.0.2.14 should be > 7.0.2.13", GemFireVersion.compareVersions("7.0.2.14", "7.0.2.13", true) > 0);
    assertTrue("7.0.2.14 should be == 7.0.2.14", GemFireVersion.compareVersions("7.0.2.14", "7.0.2.14", true) == 0);
    assertTrue("7.0.2.12 should be < 7.0.2.13", GemFireVersion.compareVersions("7.0.2.12", "7.0.2.13", true) < 0);
    assertTrue("7.0.2.13 should be == 7.0.2.13", GemFireVersion.compareVersions("7.0.2.13", "7.0.2.13", true) == 0);
    assertTrue("7.0.2.15 should be > 7.0.2.13", GemFireVersion.compareVersions("7.0.2.14", "7.0.2.13", true) > 0);
  }
}

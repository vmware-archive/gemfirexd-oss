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
package com.gemstone.gemfire.internal.util;

import java.lang.reflect.Constructor;
//import java.io.*;
import java.net.*;
import junit.framework.*;

/**
 * Tests the ability to load
 * "user" classes from the current context class loader.  See bug
 * 29637.
 *
 * @author David Whitlock
 *
 * @since 2.0.2
 */
public class DeserializerTest extends TestCase {

  /** The old context class loader before the test was run */
  private ClassLoader oldCCL;

  public DeserializerTest(String name) {
    super(name);
  }

  ////////  Test life-cycle methods

  /**
   * Sets the current context class loader to a loader that can load
   * a special class.
   */
  public void setUp() throws MalformedURLException {
    this.oldCCL = Thread.currentThread().getContextClassLoader();
    assertTrue(this.oldCCL instanceof URLClassLoader);

    // Grep through the URLs of the CCL to find $JTESTS
    URL otherURL = null;

    URLClassLoader ccl = (URLClassLoader) this.oldCCL;
    URL[] urls = ccl.getURLs();
    for (int i = 0; i < urls.length; i++) {
      URL url = urls[i];
      if (url.getFile().indexOf("tests/classes") != -1) {
        otherURL = new URL(url, "../other/");
      }
    }

    assertNotNull(otherURL);

    ClassLoader cl =
      URLClassLoader.newInstance(new URL[] { otherURL},
                                 this.oldCCL);
    Thread.currentThread().setContextClassLoader(cl);
  }

  /**
   * Resets the current context class loader to what it was before the
   * test was run.
   */
  public void tearDown() {
    Thread.currentThread().setContextClassLoader(this.oldCCL);
  }

  /**
   * Tests serializing an object loaded with the current context class
   * loader (whose parent is the loader that loads GemFire and test
   * classes).
   */
  public void testDeserializingObjectFromDifferentCL()
    throws java.io.IOException, InstantiationException,
           IllegalAccessException {

    String name =
      "com.gemstone.gemfire.internal.util.SerializableImpl";

    // First load the class
    Class c;
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      c = Class.forName(name, true, cl);

    } catch (ClassNotFoundException ex) {
      URL[] urls = ((URLClassLoader) cl).getURLs();
      StringBuffer sb = new StringBuffer("Could not load \"");
      sb.append(name);
      sb.append("\" from ");
      for (int i = 0; i < urls.length; i++) {
        sb.append(urls[i]);
        sb.append(" ");
      }
      sb.append(": ");
      sb.append(ex);
      fail(sb.toString());
      return;
    }

    Object o = c.newInstance();
    byte[] bytes = BlobHelper.serializeToBlob(o/*, 42*/);

    try {
      BlobHelper.deserializeBlob(bytes);

    } catch (ClassNotFoundException ex) {
      fail("Couldn't load class for serialized bytes: " + ex);
    }
  }

  /**
   * Tests that the deserialized object has the correct state
   */
  public void testDeserializingObjectWithState() throws Exception {
    String name =
      "com.gemstone.gemfire.internal.util.SerializableImplWithValue";
    // First load the class
    Class c;
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      c = Class.forName(name, true, cl);

    } catch (ClassNotFoundException ex) {
      URL[] urls = ((URLClassLoader) cl).getURLs();
      StringBuffer sb = new StringBuffer("Could not load \"");
      sb.append(name);
      sb.append("\" from ");
      for (int i = 0; i < urls.length; i++) {
        sb.append(urls[i]);
        sb.append(" ");
      }
      sb.append(": ");
      sb.append(ex);
      fail(sb.toString());
      return;
    }

    Constructor init =
      c.getConstructor(new Class[] { Object.class });
    Valuable o =
      (Valuable) init.newInstance(new Object[] { new Integer(123) });
    byte[] bytes = BlobHelper.serializeToBlob(o/*, 42*/);

    try {
      Valuable o2 = (Valuable) BlobHelper.deserializeBlob(bytes);
      assertEquals(o.getValue(), o2.getValue());

    } catch (ClassNotFoundException ex) {
      fail("Couldn't load class for serialized bytes: " + ex);
    }

  }

}

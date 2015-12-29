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
package cacheperf.comparisons.connection.preload;

import com.gemstone.gemfire.internal.NanoTimer;
import java.io.*;

public class TimingClassLoader extends ClassLoader {

  private static final int BUFFER_SIZE = 8192;

  private PrintWriter writer;
  private boolean verbose;

  long bytesTime = 0;
  long defineTime = 0;
  long resolveTime = 0;
  long parentTime = 0;

  public TimingClassLoader(PrintWriter pw, boolean verbose) {
    super(TimingClassLoader.class.getClassLoader());
    this.writer = pw;
    this.verbose = verbose;
  }

  protected synchronized Class loadClass(String className, boolean resolve)
  throws ClassNotFoundException {

    if (verbose) {
      this.writer.println(className + "...loading");
    }

    // 1. is this class already loaded?
    Class cls = findLoadedClass(className);
    if (cls != null) {
      if (verbose) {
        this.writer.println(className + "...already loaded");
      }
      return cls;
    }

    // 2. get class file name from class name
    String clsFile = className.replace('.', '/') + ".class";

    // 3. get bytes for class
    long start = NanoTimer.getTime();
    byte[] classBytes = null;
    try {
      InputStream in = getResourceAsStream(clsFile);
      byte[] buffer = new byte[BUFFER_SIZE];
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      int n = -1;
      while ((n = in.read(buffer, 0, BUFFER_SIZE)) != -1) {
        out.write(buffer, 0, n);
      }
      classBytes = out.toByteArray();
    }
    catch (IOException e) {
      throw new ClassNotFoundException("Cannot load class: " + className
                                      + e.getMessage());
    }
    if (classBytes == null) {
      throw new ClassNotFoundException("Cannot load class: " + className);
    }
    bytesTime += NanoTimer.getTime() - start;

    // 4. turn the byte array into a Class
    try {
      start = NanoTimer.getTime();
      cls = defineClass(className, classBytes, 0, classBytes.length);
      defineTime += NanoTimer.getTime() - start;

      if (resolve) {
        start = NanoTimer.getTime();
        resolveClass(cls);
        resolveTime += NanoTimer.getTime() - start;
      }
    }
    catch (SecurityException e) {
      // loading core java classes such as java.lang.String
      // is prohibited, throws java.lang.SecurityException.
      // delegate to parent if not allowed to load class
      start = NanoTimer.getTime();
      cls = super.loadClass(className, resolve);
      parentTime += NanoTimer.getTime() - start;
      if (verbose) {
        this.writer.println(className + " loaded by parent class loader");
      }
    }

    return cls;
  }

  public void printBreakdown() {
    this.writer.println("TOTAL TIME: bytesTime = " + bytesTime);
    this.writer.println("TOTAL TIME: defineTime = " + defineTime);
    this.writer.println("TOTAL TIME: resolveTime = " + resolveTime);
    this.writer.println("TOTAL TIME: parentTime = " + parentTime);
  }
}

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
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Supports preloading a resource such as gemfire.jar.
 */
public class Preload {

  String resourceFile;
  PrintWriter pw;
  boolean initializeClasses;
  boolean verbose;
  ClassLoader loader;

  public Preload(String resourceFile,
                 boolean useTimingClassLoader, boolean initializeClasses,
                 String outputFile, boolean verbose) throws IOException {
    this.resourceFile = resourceFile;
    this.initializeClasses = initializeClasses;
    this.pw = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)));
    this.verbose = verbose;

    if (useTimingClassLoader) {
      loader = new TimingClassLoader(pw, verbose);
    } else {
      loader = this.getClass().getClassLoader();
    }
  }

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  public void preloadResource()
  throws IOException {
    pw.println("Preloading " + resourceFile + " using " + loader);

    long start = NanoTimer.getTime();
    try {
      if (resourceFile.indexOf(".jar") == -1) {
        preloadClassList();
      } else {
        preloadJar();
      }
      long elapsed = NanoTimer.getTime() - start;

      if (loader instanceof TimingClassLoader) {
        ((TimingClassLoader)loader).printBreakdown();
      }
      pw.println("Preloaded " + resourceFile + " in " + elapsed + " ns");
    } finally {
      pw.close();
    }
  }
  private void preloadJar() throws IOException {
    JarFile jarFile = new JarFile(this.resourceFile);
    Enumeration en = jarFile.entries();
    while (en.hasMoreElements()) {
      preloadJarEntry(en.nextElement());
    }
  }
  private void preloadJarEntry(Object obj) {
    long start, elapsed;
    JarEntry entry = (JarEntry)obj;
    String name = entry.getName();
    int classIndex = name.indexOf(".class");
    if (classIndex != -1) {
      long size = entry.getSize();
      //long compressedSize = entry.getCompressedSize();
      String classFileName = name.replace('/', '.');
      String className = classFileName.substring(0, classIndex);
      preloadClass(className, size);
    }
  }
  private void preloadClassList() throws FileNotFoundException, IOException {
    String classList = getContentsWithSpace(this.resourceFile);
    StringTokenizer tokenizer = new StringTokenizer(classList, " ", false);
    while (tokenizer.hasMoreTokens()) {
      String className = tokenizer.nextToken();
      preloadClass(className, -1);
    }
  }
  private void preloadClass(String className, long size) {
    if (verbose) {
      if (size == -1) {
        pw.println("Loading " + className + "(" + size + " bytes)");
      } else {
        pw.println("Loading " + className);
      }
    }
    try {
      if (className.indexOf("AvailablePortTask") == -1 &&
          className.indexOf("VMStats") == -1) {
        long start = NanoTimer.getTime();
        Class.forName(className, initializeClasses, loader);
        long elapsed = NanoTimer.getTime() - start;
        if (verbose) {
          pw.println("Loaded " + className + " in " + elapsed + " ns");
        }
      }
    }
    catch (ClassNotFoundException e) {
      pw.println("Class not found: " + className);
    }
  }

  /**
   * Read the file into a string with a space where newlines have been.
   */
  private String getContentsWithSpace(String fn)
  throws FileNotFoundException, IOException {
    StringBuffer buf = new StringBuffer(1000);
    BufferedReader br = new BufferedReader(new FileReader(fn));
    String s;
    boolean firstLine = true;
    while ((s = br.readLine()) != null) {
      if (!firstLine) {
        buf.append(" ");
      }
      buf.append(s);
      firstLine = false;
    }
    br.close();
    return buf.toString();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      String usage = "cacheperf.comparisons.connection.preload.Preload <resourceFile> <useTimingClassLoader> <initializeClasses> <outputFile> <verbose>";
      throw new Exception(usage);
    }
    Preload p = new Preload(
      /* resourceFile */         args[0],
      /* useTimingClassLoader */ Boolean.valueOf(args[1]).booleanValue(),
      /* initializeClasses */    Boolean.valueOf(args[2]).booleanValue(),
      /* outputFile */           args[3],
      /* verbose */              Boolean.valueOf(args[4]).booleanValue());
    p.preloadResource();
  }
}

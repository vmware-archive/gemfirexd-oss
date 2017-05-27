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

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.lang.ObjectUtils;

/**
 * Simple verification of OSGi headers in MANIFEST.MF of gemfire.jar.
 *
 * @author Kirk Lund
 * @author John Blum
 * @since 6.6
 */
@SuppressWarnings("unused")
public class SimpleOsgiJUnitTest extends TestCase {

  public SimpleOsgiJUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    if (false) { // set true and edit if running inside Eclipse
      System.setProperty("GEMFIRE", "C:/build/gemfire/trunk/build-artifacts/win/product");
      System.setProperty("gemfire.osgi.version", "7.0");
      System.setProperty("antlr.version", "2.7.4");
    }
  }

  @Override
  public void tearDown() throws Exception {
  }

  protected String getGemFireJarFileLocation() {
    final StringBuilder buffer = new StringBuilder(
        ObjectUtils.defaultIfNull(System.getProperty("GEMFIRE"), System.getenv("GEMFIRE")));
    buffer.append(File.separator);
    buffer.append("lib");
    buffer.append(File.separator);
    buffer.append("gemfire.jar");
    return buffer.toString();
  }

  /**
   * Verifies expected OSGi headers in MANIFEST.MF. Changes to imported or
   * exported packages may cause failures that require updates to this test.
   * Please talk to Kirk Lund about how to resolve such failures.
   */
  public void testGemFireManifest() throws Exception {
    File gemfireJarFile = new File(getGemFireJarFileLocation());
    assertTrue(gemfireJarFile.exists());

    JarFile gemfireJar = new JarFile(gemfireJarFile);
    assertNotNull(gemfireJar);

    /*
    URLClassLoader cl = new URLClassLoader(new URL[]{gemfireJar.toURI().toURL()}, null);

    InputStream is = cl.getResourceAsStream("META-INF/MANIFEST.MF");
    assertNotNull(is);

    Manifest manifest = new Manifest(is);
    */

    Manifest manifest = gemfireJar.getManifest();

    Attributes attributes = manifest.getMainAttributes();
    assertNotNull(attributes);
    printAttributes(attributes);

    assertEquals("1.0", attributes.getValue("Manifest-Version"));

    //assertEquals("16.0-b13 (Sun Microsystems Inc.)", attributes.getValue("Created-By"));
    assertNotNull(attributes.getValue("Created-By"));
    //assertEquals("Apache Ant 1.7.1", attributes.getValue("Ant-Version"));
    assertNotNull(attributes.getValue("Ant-Version"));
    //assertEquals("Plexus Archiver", attributes.getValue("Archiver-Version"));
    assertNotNull(attributes.getValue("Archiver-Version"));

    assertEquals("Pivotal Software, Inc.", attributes.getValue("Built-By"));
    assertEquals("com.gemstone.gemfire.internal.GemFireVersion", attributes.getValue("Main-Class"));
    assertEquals("Bundlor 1.0.0.RELEASE", attributes.getValue("Tool"));
    assertEquals("GemFire", attributes.getValue("Bundle-Name"));
    assertEquals("Pivotal Software, Inc.", attributes.getValue("Bundle-Vendor"));
    assertEquals(".", attributes.getValue("Bundle-Classpath"));

    // 6.6.0 was first version
    String bundleVersion = System.getProperty("gemfire.osgi.version");
    assertEquals(bundleVersion, attributes.getValue("Bundle-Version"));

    assertEquals("2", attributes.getValue("Bundle-ManifestVersion"));
    assertEquals("Pivotal GemFire bundle", attributes.getValue("Bundle-Description"));
    assertEquals("com.gemstone.gemfire;singleton:=\"true\";fragment-attachment:=\"always\"",
                 attributes.getValue("Bundle-SymbolicName"));

    String exportPackageStr = attributes.getValue("Export-Package");
    Set<String> exportedPackages = parseExportedPackages(exportPackageStr);
    printPackages("Export-Package contains: ", exportedPackages);

    assertNotExportedPackage(exportedPackages, "com.gemstone.gnu");
    assertNotExportedPackage(exportedPackages, "com.gemstone.java");
    assertNotExportedPackage(exportedPackages, "com.gemstone.jdbm");
    assertNotExportedPackage(exportedPackages, "com.gemstone.org");
    assertNotExportedPackage(exportedPackages, "com.springsource");
    assertNotExportedPackage(exportedPackages, "com.vmware");
    assertNotExportedPackage(exportedPackages, "javax");

    assertExportedPackage(exportedPackages,"com.gemstone.gemfire");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.admin");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.admin.jmx");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.asyncqueue");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.client");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.control");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.execute");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.hdfs");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.operations");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.partition");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.persistence");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.query");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.query.internal");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.query.internal.index");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.query.internal.parse");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.query.internal.types");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.query.types");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.server");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.snapshot");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.util");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.cache.wan");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.compression");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.distributed");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.i18n");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.lang");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.management");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.management.cli");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.memcached");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.pdx");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.ra");
    assertExportedPackage(exportedPackages,"com.gemstone.gemfire.security");

    assertEquals("Export-Package has unexpected packages: " + exportedPackages, 0, exportedPackages.size());

    String importedPackageStr = attributes.getValue("Import-Package");
    Set<String> importedPackages = parseImportedPackages(importedPackageStr);
    printPackages("Import-Package contains: ", importedPackages);

    assertImportedPackage(importedPackages,"antlr");
    assertImportedPackage(importedPackages,"antlr.collections");
    assertImportedPackage(importedPackages,"antlr.collections.impl");
    assertImportedPackage(importedPackages,"antlr.debug.misc");

    assertImportedPackage(importedPackages, "com.google.common.util.concurrent");
    assertImportedPackage(importedPackages, "com.sun.tools.attach");

    assertImportedPackage(importedPackages,"javax.crypto");
    assertImportedPackage(importedPackages,"javax.crypto.spec");
    assertImportedPackage(importedPackages,"javax.mail");
    assertImportedPackage(importedPackages,"javax.mail.internet");
    assertImportedPackage(importedPackages,"javax.management");
    assertImportedPackage(importedPackages,"javax.management.loading");
    assertImportedPackage(importedPackages,"javax.management.modelmbean");
    assertImportedPackage(importedPackages,"javax.management.openmbean");
    assertImportedPackage(importedPackages,"javax.management.remote");
    assertImportedPackage(importedPackages,"javax.management.remote.rmi");
    assertImportedPackage(importedPackages,"javax.management.timer");
    assertImportedPackage(importedPackages,"javax.naming");
    assertImportedPackage(importedPackages,"javax.naming.directory");
    assertImportedPackage(importedPackages,"javax.naming.spi");
    assertImportedPackage(importedPackages,"javax.net");
    assertImportedPackage(importedPackages,"javax.net.ssl");
    assertImportedPackage(importedPackages,"javax.print.attribute");
    assertImportedPackage(importedPackages,"javax.rmi.ssl");
    assertImportedPackage(importedPackages,"javax.security.auth");
    assertImportedPackage(importedPackages,"javax.sql");
    assertImportedPackage(importedPackages,"javax.swing");
    assertImportedPackage(importedPackages,"javax.swing.table");
    assertImportedPackage(importedPackages,"javax.transaction");
    assertImportedPackage(importedPackages,"javax.transaction.xa");
    assertImportedPackage(importedPackages,"javax.xml.parsers");
    assertImportedPackage(importedPackages,"javax.xml.bind.annotation");
    assertImportedPackage(importedPackages,"javax.xml.transform");
    assertImportedPackage(importedPackages,"javax.xml.transform.sax");
    assertImportedPackage(importedPackages,"javax.xml.transform.stream");

    assertImportedPackage(importedPackages,"jline");

    assertImportedPackage(importedPackages,"mx4j");
    assertImportedPackage(importedPackages,"mx4j.log");
    assertImportedPackage(importedPackages,"mx4j.persist");
    assertImportedPackage(importedPackages,"mx4j.tools.adaptor");
    assertImportedPackage(importedPackages,"mx4j.tools.adaptor.http");
    assertImportedPackage(importedPackages,"mx4j.util");
    
    assertImportedPackage(importedPackages,"nu.xom");
    
    assertImportedPackage(importedPackages,"org.apache.commons.io");
    assertImportedPackage(importedPackages,"org.apache.commons.io.filefilter");
    assertImportedPackage(importedPackages,"org.apache.commons.lang");
    assertImportedPackage(importedPackages,"org.apache.commons.logging");
    assertImportedPackage(importedPackages,"org.apache.commons.modeler");
    assertImportedPackage(importedPackages,"org.apache.hadoop.conf");
    assertImportedPackage(importedPackages,"org.apache.hadoop.fs");
    assertImportedPackage(importedPackages,"org.apache.hadoop.security");
    assertImportedPackage(importedPackages,"com.pivotal.org.apache.hadoop.hbase.io.hfile");
    assertImportedPackage(importedPackages,"com.pivotal.org.apache.hadoop.hbase.regionserver");
    assertImportedPackage(importedPackages,"com.pivotal.org.apache.hadoop.hbase.regionserver.metrics");
    assertImportedPackage(importedPackages,"com.pivotal.org.apache.hadoop.hbase.util");
    assertImportedPackage(importedPackages,"org.apache.hadoop.io");
    assertImportedPackage(importedPackages,"org.apache.hadoop.mapreduce");
    assertImportedPackage(importedPackages,"org.apache.hadoop.mapreduce.lib.input");
    assertImportedPackage(importedPackages,"org.apache.hadoop.mapred");
    assertImportedPackage(importedPackages,"org.apache.hadoop.util");
    assertImportedPackage(importedPackages,"org.apache.hadoop.mapreduce.lib.output");
    assertImportedPackage(importedPackages,"org.apache.hadoop.io.compress");
    assertImportedPackage(importedPackages,"org.apache.hadoop.ipc");
    
    assertImportedPackage(importedPackages,"org.apache.tomcat.util");
    assertImportedPackage(importedPackages,"org.apache.tools.ant");

    assertImportedPackage(importedPackages,"org.codehaus.jackson");
    assertImportedPackage(importedPackages,"org.codehaus.jackson.impl");

    assertImportedPackage(importedPackages, "org.eclipse.jetty.server");
    assertImportedPackage(importedPackages, "org.eclipse.jetty.server.nio");
    assertImportedPackage(importedPackages, "org.eclipse.jetty.server.handler");
    assertImportedPackage(importedPackages, "org.eclipse.jetty.webapp");

    assertImportedPackage(importedPackages,"org.springframework.core.io");
    assertImportedPackage(importedPackages,"org.springframework.http");
    assertImportedPackage(importedPackages,"org.springframework.http.client");
    assertImportedPackage(importedPackages,"org.springframework.http.converter");
    assertImportedPackage(importedPackages,"org.springframework.shell.core");
    assertImportedPackage(importedPackages,"org.springframework.shell.core.annotation");
    assertImportedPackage(importedPackages,"org.springframework.shell.event");
    assertImportedPackage(importedPackages,"org.springframework.shell.support.util");
    assertImportedPackage(importedPackages,"org.springframework.util");
    assertImportedPackage(importedPackages,"org.springframework.web.client");
    assertImportedPackage(importedPackages,"org.springframework.web.multipart");
    assertImportedPackage(importedPackages,"org.springframework.web.util");

    assertImportedPackage(importedPackages,"org.w3c.dom");

    assertImportedPackage(importedPackages,"org.xerial.snappy");

    assertImportedPackage(importedPackages,"org.xml.sax");
    assertImportedPackage(importedPackages,"org.xml.sax.ext");
    assertImportedPackage(importedPackages,"org.xml.sax.helpers");

    assertImportedPackage(importedPackages, "com.sun.jna");
    assertImportedPackage(importedPackages, "com.sun.jna.ptr");
    assertImportedPackage(importedPackages, "com.sun.jna.win32");

    assertImportedPackage(importedPackages, "org.slf4j");

    assertEquals("Import-Package has unexpected packages: " + importedPackages,
        0, importedPackages.size());
  }

  protected String getAntlrJarFileLocation() {
    final StringBuilder buffer = new StringBuilder(
        ObjectUtils.defaultIfNull(System.getProperty("GEMFIRE"), System.getenv("GEMFIRE")));
    buffer.append(File.separator);
    buffer.append("lib");
    buffer.append(File.separator);
    buffer.append("antlr.jar");
    return buffer.toString();
  }

  public void testAntlrManifest() throws Exception {
    File antlrJarFile = new File(getAntlrJarFileLocation());
    assertTrue(antlrJarFile.exists());

    JarFile antlrJar = new JarFile(antlrJarFile, false, JarFile.OPEN_READ);
    assertNotNull(antlrJar);

    /*
    URLClassLoader cl = new URLClassLoader(new URL[]{antlrJarFile.toURI().toURL()}, null);

    InputStream is = cl.getResourceAsStream("META-INF/MANIFEST.MF");
    assertNotNull(is);

    Manifest manifest = new Manifest(is);
    */

    Manifest manifest = antlrJar.getManifest();

    Attributes attributes = manifest.getMainAttributes();
    assertNotNull(attributes);
    printAttributes(attributes);

    assertEquals("1.0", attributes.getValue("Manifest-Version"));

    assertEquals("1.4.2_03 (Apple Computer, Inc.)", attributes.getValue("Created-By"));

    assertEquals("Bundlor 1.0.0.RELEASE", attributes.getValue("Tool"));
    assertEquals("ANTLR", attributes.getValue("Bundle-Name"));
    assertEquals("Pivotal Software, Inc.", attributes.getValue("Bundle-Vendor"));
    assertEquals(".", attributes.getValue("Bundle-Classpath"));

    // 2.7.4 was first version
    String bundleVersion = System.getProperty("antlr.version");
    assertEquals(bundleVersion, attributes.getValue("Bundle-Version"));

    assertEquals("2", attributes.getValue("Bundle-ManifestVersion"));
    assertEquals("com.gemstone.antlr", attributes.getValue("Bundle-SymbolicName"));

    String exportPackageStr = attributes.getValue("Export-Package");
    Set<String> exportedPackages = parseExportedPackages(exportPackageStr);
    printPackages("Export-Package contains: ", exportedPackages);

    assertTrue(exportedPackages.remove("antlr"));
    assertTrue(exportedPackages.remove("antlr.actions.cpp"));
    assertTrue(exportedPackages.remove("antlr.actions.csharp"));
    assertTrue(exportedPackages.remove("antlr.actions.java"));
    assertTrue(exportedPackages.remove("antlr.build"));
    assertTrue(exportedPackages.remove("antlr.collections"));
    assertTrue(exportedPackages.remove("antlr.collections.impl"));
    assertTrue(exportedPackages.remove("antlr.debug"));
    assertTrue(exportedPackages.remove("antlr.debug.misc"));
    assertTrue(exportedPackages.remove("antlr.preprocessor"));

    assertEquals("Export-Package has unexpected packages: " + exportedPackages,
        0, exportedPackages.size());

    String importedPackageStr = attributes.getValue("Import-Package");
    Set<String> importedPackages = parseImportedPackages(importedPackageStr);
    printPackages("Import-Package contains: ", importedPackages);

    assertTrue(importedPackages.remove("javax.swing"));
    assertTrue(importedPackages.remove("javax.swing.event"));
    assertTrue(importedPackages.remove("javax.swing.tree"));
    assertTrue(importedPackages.remove("com.gemstone.gemfire.cache.query.internal"));
    assertTrue(importedPackages.remove("com.gemstone.gemfire.cache.query.internal.index"));
    assertTrue(importedPackages.remove("com.gemstone.gemfire.cache.query.internal.parse"));
    assertTrue(importedPackages.remove("com.gemstone.gemfire.cache.query.internal.types"));

    assertEquals("Export-Package has unexpected packages: " + importedPackages,
        0, importedPackages.size());
  }

  private void assertImportedPackage(Set<String> importedPackages, String pkg) {
    assertTrue("Expected imported package not found: " + pkg, importedPackages.remove(pkg));
  }

  private void assertExportedPackage(Set<String> exportedPackages, String pkg) {
    if (!pkg.contains("query")) {
      assertFalse("Do not export 'internal' packages!", pkg.contains("internal"));
    }
    assertTrue("Expected exported package not found: " + pkg, exportedPackages.remove(pkg));
  }
  
  private void assertNotExportedPackage(Set<String> exportedPackages, String pkg) {
    assertFalse("Exported packages should NOT include: " + pkg, 
        containsSubString(exportedPackages, pkg));
  }
  
  private void printAttributes(Attributes attributes) {
    System.out.println("\nAttributes includes:");
    for (Iterator<?> iter = attributes.keySet().iterator(); iter.hasNext();) {
      Attributes.Name key = (Attributes.Name)iter.next();
      String value = attributes.getValue(key);
      System.out.println(key + "=" + value);
    }
  }

  private void printPackages(String header, Set<String> pkgs) {
    System.out.println("\n" + header);
    int i = 0;
    for (String pkg : pkgs) {
      i++;
      System.out.println("pkg-" + i + ": " + pkg);
    }
  }

  private boolean containsSubString(Set<String> exportedPackages, String string) {
    for (String pkg : exportedPackages) {
      if (pkg.contains(string)) {
        return true;
      }
    }
    return false;
  }

  private Set<String> parseExportedPackages(String exportPackageStr) {
    Set<String> exportedPackages = new HashSet<String>();

    StringTokenizer tokenizer = new StringTokenizer(exportPackageStr, ";");
    while (tokenizer.hasMoreTokens()) {
      String string = tokenizer.nextToken();
      string.replace(" ", "");
      if (string.contains("version=\"") || string.startsWith("uses:=\"")) {
        int beginIndex = string.indexOf("\",") + 2;
        if (beginIndex > 1) {
          String pkg = string.substring(beginIndex);
          exportedPackages.add(pkg);
        }
      }
      else {
        String pkg = string;
        exportedPackages.add(pkg);
      }
    }

    return exportedPackages;
  }

  private Set<String> parseImportedPackages(String importedPackageStr) {
    Set<String> importedPackages = new HashSet<String>();

    StringTokenizer tokenizer = new StringTokenizer(importedPackageStr, ",");
    while (tokenizer.hasMoreTokens()) {
      String string = tokenizer.nextToken();
      if (string.contains(";")) {
        int endIndex = string.indexOf(";");
        String pkg = string.substring(0, endIndex);
        if (!pkg.contains(")")) {
          importedPackages.add(pkg);
        }
      }
      else {
        String pkg = string;
        importedPackages.add(pkg);
      }
    }

    return importedPackages;
  }

}

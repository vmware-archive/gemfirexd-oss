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
package com.pivotal.gemfirexd.internal.engine.store;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.pivotal.gemfirexd.TestUtil;
import junit.framework.TestCase;

public class GFECompatibilityTest extends TestCase {

  private final String exclusions[] = new String[] { "NonLocalRegionEntry",
      "NonLocalRegionEntryWithStats", "ProxyRegionEntry",
      "GfxdTXEntryState", "OplogDiskEntry", "ValidatingDiskEntry",
      "HDFSEventRowLocationRegionEntry" };

  public GFECompatibilityTest(String name) {
    super(name);
  }

  public void testExtendedRegionEntryClasses() throws Exception {
    TestUtil.getConnection();
    String gfeJar = GemFireCacheImpl.class.getProtectionDomain()
        .getCodeSource().getLocation().getFile();
    String gemfirexdJar = GemFireStore.class.getProtectionDomain()
        .getCodeSource().getLocation().getFile();
    // strip off leading '/' for Windows (#42037)
    if (System.getProperty("os.name").contains("Windows")) {
      if (gfeJar.charAt(0) == '/') {
        gfeJar = gfeJar.substring(1);
      }
      if (gemfirexdJar.charAt(0) == '/') {
        gemfirexdJar = gemfirexdJar.substring(1);
      }
    }

    //The GFE classes are now included in the gemfirexd jar, so we 
    //need to make sure we look at only the gemfirexd classes.
    List<Class> gfeREClasses = getRegionEntryClassesFromJar(gfeJar,
        "com.gemstone.gemfire.internal.cache");
    List<Class> gfxdREClasses = getRegionEntryClassesFromJar(gemfirexdJar,
        "com.pivotal.gemfirexd.internal.engine.store.entry");
    assertFalse(gfeREClasses.isEmpty());
    assertFalse(gfxdREClasses.isEmpty());
    Iterator<Class> gfeClassesItr = gfeREClasses.iterator();
    // For each class of GFE there should be two classes in gemfirexd
    // Right now there are no equivalent of Version* classes
    while(gfeClassesItr.hasNext()) {
      Class<?> gfeClass = gfeClassesItr.next();
      Iterator<Class> gfxdClassItr = gfxdREClasses.iterator();
      int numFound = 0;
      Map<Class, Class> assignableClasses = new HashMap<>();
      while(gfxdClassItr.hasNext()) {
        Class<?> gfxdClass = gfxdClassItr.next();
        // get the non-GFXD super class
        Class<?> gfxdSuper = gfxdClass.getSuperclass();
        while (gfxdSuper.getName().contains("RowLocation")) {
          gfxdSuper = gfxdSuper.getSuperclass();
        }
        if (gfeClass.equals(gfxdSuper)) {
          ++numFound;
          assignableClasses.put(gfxdClass, gfeClass);
          gfxdClassItr.remove();
        }
      }
      String gfeClassName = gfeClass.getName();
      // GFXD classes don't extend *Heap or *OffHeap classes rather
      // their abstract base classes
      if (gfeClassName.endsWith("OffHeap")
          || gfeClassName.endsWith("Heap")) {
        assertEquals("Assignable classes: " + assignableClasses + " for "
            + gfeClassName, 0, numFound);
      } else {
        assertEquals("Assignable classes: " + assignableClasses + " for "
            + gfeClassName, 9, numFound);
      }
      gfeClassesItr.remove();
    }
    assertTrue(gfxdREClasses.isEmpty());
    assertTrue(gfeREClasses.isEmpty());
  }

  private List<Class> getRegionEntryClassesFromJar(String jarFile, String pkg) throws Exception {

    LinkedList<Class> regionEntryClasses = new LinkedList<>();
    JarFile gfJar = new JarFile(jarFile, true);
    Enumeration<JarEntry> enm = gfJar.entries();
    while (enm.hasMoreElements()) {
      JarEntry je = enm.nextElement();
      String name = je.getName().replace('/', '.');
      if (name.startsWith(pkg)
          && name.endsWith(".class")) {
        Class<?> jeClass = Class.forName(name.replaceAll(".class", ""));
        if (!jeClass.isInterface()
            && RegionEntry.class.isAssignableFrom(jeClass)
            && !isInExclusionList(jeClass)
            && !jeClass.getSimpleName().startsWith("Abstract")) {
          regionEntryClasses.add(jeClass);
        }
      }
    }
    return regionEntryClasses;
  }

  private boolean isInExclusionList(Class jeClass) {
    String name = jeClass.getSimpleName();

   for(String toExclude:exclusions) {
      if(toExclude.equals(name)) {
        return true;
      }
   }
   return false;
  }
}

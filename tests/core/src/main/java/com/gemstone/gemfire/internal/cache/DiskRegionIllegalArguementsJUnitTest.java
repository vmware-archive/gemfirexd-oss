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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;

/**
 * This test tests Illegal arguements being passed to create disk regions. The
 * creation of the DWA object should throw a relevant exception if the
 * arguements specified are incorrect.
 * 
 * @author mbid
 *  
 */
public class DiskRegionIllegalArguementsJUnitTest extends TestCase
{

  protected static Cache cache = null;

  protected static DistributedSystem ds = null;
  protected static Properties props = new Properties();

  static {
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("log-level", "config"); // to keep diskPerf logs smaller
    props.setProperty("statistic-sampling-enabled", "true");
    props.setProperty("enable-time-statistics", "true");
    props.setProperty("statistic-archive-file", "stats.gfs");
  }

  protected void setUp() throws Exception {
    super.setUp();
    cache = new CacheFactory(props).create();
    ds = cache.getDistributedSystem();
  }

  protected void tearDown() throws Exception {
    cache.close();
    super.tearDown();
  }
  
  public DiskRegionIllegalArguementsJUnitTest(String name) {
    super(name);
  }

  /**
   * test Illegal max oplog size
   */

  public void testMaxOplogSize()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setMaxOplogSize(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
    dsf.setMaxOplogSize(1);
    assertEquals(1, dsf.create("test").getMaxOplogSize());
  }

  public void testCompactionThreshold() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();

    try {
      dsf.setCompactionThreshold(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    try {
      dsf.setCompactionThreshold(101);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    dsf.setCompactionThreshold(0);
    dsf.setCompactionThreshold(100);
    assertEquals(100, dsf.create("test").getCompactionThreshold());
  }

  public void testAutoCompact()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(true);
    assertEquals(true, dsf.create("test").getAutoCompact());

    dsf.setAutoCompact(false);
    assertEquals(false, dsf.create("test2").getAutoCompact());
  }

  public void testAllowForceCompaction()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAllowForceCompaction(true);
    assertEquals(true, dsf.create("test").getAllowForceCompaction());

    dsf.setAllowForceCompaction(false);
    assertEquals(false, dsf.create("test2").getAllowForceCompaction());
  }
  
  public void testDiskDirSize()
  {

    File file1 = new File("file1");

    File file2 = new File("file2");
    File file3 = new File("file3");
    File file4 = new File("file4");
    file1.mkdir();
    file2.mkdir();
    file3.mkdir();
    file4.mkdir();
    file1.deleteOnExit();
    file2.deleteOnExit();
    file3.deleteOnExit();
    file4.deleteOnExit();

    File[] dirs = { file1, file2, file3, file4 };

    int[] ints = { 1, 2, 3, -4 };

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setDiskDirsAndSizes(dirs, ints);
      fail("expected IllegalArgumentException");
    }
    catch (IllegalArgumentException ok) {
    }

    int[] ints1 = { 1, 2, 3 };

    try {
      dsf.setDiskDirsAndSizes(dirs, ints1);
      fail("expected IllegalArgumentException");
    }
    catch (IllegalArgumentException ok) {
    }
    ints[3] = 4;
    dsf.setDiskDirsAndSizes(dirs, ints);
  }

  public void testDiskDirs()
  {
    File file1 = new File("file6");

    File file2 = new File("file7");
    File file3 = new File("file8");
    File file4 = new File("file9");

    File[] dirs = { file1, file2, file3, file4 };

    int[] ints = { 1, 2, 3, 4 };

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setDiskDirsAndSizes(dirs, ints);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    int[] ints1 = { 1, 2, 3 };
    file1.mkdir();
    file2.mkdir();
    file3.mkdir();
    file4.mkdir();
    file1.deleteOnExit();
    file2.deleteOnExit();
    file3.deleteOnExit();
    file4.deleteOnExit();

    try {
      dsf.setDiskDirsAndSizes(dirs, ints1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
    dsf.setDiskDirsAndSizes(dirs, ints);
  }

  public void testQueueSize()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setQueueSize(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    dsf.setQueueSize(1);
    assertEquals(1, dsf.create("test").getQueueSize(), 1);
  }

  public void testTimeInterval()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setTimeInterval(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }

    dsf.setTimeInterval(1);
    assertEquals(dsf.create("test").getTimeInterval(), 1);
  }

}

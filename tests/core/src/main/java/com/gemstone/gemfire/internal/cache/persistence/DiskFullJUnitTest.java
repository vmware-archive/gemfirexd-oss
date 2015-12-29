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
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.InsufficientDiskSpaceException;
import com.gemstone.gemfire.internal.cache.TestUtils;

public class DiskFullJUnitTest extends TestCase {
  private File img = new File("test.img");
  File dir;
  
  private Cache cache;
  private DiskStore ds;
  private Region<Long, Object> region;
  
  private static final long SIZE = 4l * 1024 * 1024 * 1024;
  
  // Tests what happens when the disk runs out of space using a limited size
  // disk partition.
  //
  // 1) Write entries until it runs out of space.
  // 2) Closes/reopens the cache and disk store.
  // 3) Recovers and verifies entries up to the last entry successfully written.
  // 
  public void testFullDisk() throws Exception {
    if (!isLinux()) {
      return;
    }
    
    TestUtils.addExpectedException(DiskAccessException.class.getName());
    TestUtils.addExpectedException(InsufficientDiskSpaceException.class.getName());
    TestUtils.addExpectedException(IOException.class.getName());
    
    int entrySize = 1024 * 1024;
    long count = 10 * SIZE / entrySize;
    
    long key = 0;
    try {
      while (key < count) {
        byte[] val = new byte[entrySize];
        Arrays.fill(val, (byte) key);
        
        cache.getLogger().info("Inserting entry " + key);
        region.put(key, val);
        
        key++;
      }
    } catch (InsufficientDiskSpaceException e) {
      
      cache.close();
      // copy files to normal partition
      File newdir = new File("mydisk");
      newdir.mkdir();
      try {
        FileUtil.copy(dir, newdir);
        ds.destroy();
        
        setUpGem(newdir);
        
        // recover
        assertEquals(key, region.size());
        for (long i = 0; i < key; i++) {
          cache.getLogger().info("Checking entry " + i);
          byte[] val = (byte[]) region.get(i);
          for (int j = 0; j < entrySize; j++) {
            assertEquals((byte) i, val[j]);
          }
        }
        return;
      } finally {
        FileUtil.delete(newdir);
      }
    }
    fail("Expected a disk full exception");
  }
  
  public void setUp() throws Exception {
    dir = createDisk();
    setUpGem(dir);
  }
  
  public void tearDown() throws Exception {
    cache.close();
    ds.destroy();
    
    if (isLinux()) {
      cmd("sudo /export/localnew/scripts/umount-file.sh " + dir.getName());
      img.delete();
    }
  }

  private void setUpGem(File diskdir) {
    CacheFactory cf = new CacheFactory()
        .set("mcast-port", "0");
    cache = cf.create();

    ds = cache.createDiskStoreFactory()
        .setDiskDirs(new File[] { diskdir })
        .setMaxOplogSize(SIZE / 16 / 1024 / 1024)
        .create("store1");
  
    region = cache
      .<Long, Object>createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
      .setDiskStoreName(ds.getName())
      .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
      .create("test");
  }
  
  private File createDisk() throws Exception {
    if (!isLinux()) {
      return new File(".");
    }
    
    String user = System.getProperty("user.name");
    String testimg = img.getAbsolutePath();
    String mount = UUID.randomUUID().toString();
    long blocks = SIZE / 1024;
    
    cmd("dd if=/dev/zero of=" + testimg + " bs=1024 count=" + blocks);
    cmd("sudo /export/localnew/scripts/umount-file.sh", false);
    cmd("sudo /export/localnew/scripts/mount-file.sh " 
        + testimg + " " 
        + mount + " " 
        + user);
    
    return new File("/mnt/" + mount);
  }

  private boolean isLinux() {
    return System.getProperty("os.name").toLowerCase().contains("linux");
  }

  private void cmd(String cmdline) throws IOException, InterruptedException {
    cmd(cmdline, true);
  }
  
  private void cmd(String cmdline, boolean throwErr) throws IOException, InterruptedException {
    System.out.println("Executing " + cmdline);
    
    String[] args = cmdline.split(" ");
    List<String> line = Arrays.asList(args);
    
    ProcessBuilder pb = new ProcessBuilder(line);
    pb.redirectErrorStream(true);
    Process p = pb.start();
    
    int size = 0;
    byte[] buf = new byte[1024];
    while ((size = p.getInputStream().read(buf)) != -1) {
      System.out.write(buf, 0, size);
    }
    
    int exitCode = p.waitFor();
    if (throwErr && exitCode != 0) {
      throw new IOException("exit: " + exitCode);
    }
  }
}

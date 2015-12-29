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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.Builder;
import org.apache.hadoop.util.ShutdownHookManager;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.HoplogReader;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;
import com.gemstone.gemfire.internal.util.BlobHelper;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HdfsErrorHandlingJunitTest extends BaseHoplogTestCase {
  MiniDFSCluster cluster;
  private String HDFS_DIR = "hdfs-test-cluster";
  private int CLUSTER_PORT = AvailablePortHelper.getRandomAvailableTCPPort();
  private File configFile;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    cache.getLogger().info("<ExpectedException action=add>java.io.IOException</ExpectedException>");
  }
  
  /*
   * This test validates no tmphop files exist on hdfs if xceiver error occurs.
   */
  public void test000HoplogDeletionOnFailure() throws Exception {
    Configuration hconf = getMiniClusterConf();
    hconf.setInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, 1);

    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);

    final AtomicInteger errorCount = new AtomicInteger(0);
    final AtomicInteger hoplogCount = new AtomicInteger(0);
    
    class Client implements Runnable {
      private int num;

      public Client(int i) {
        this.num = i;
      }

      public void run() {
        cache.getLogger().info("<ExpectedException action=add>PriviledgedActionException</ExpectedException>");
        cache.getLogger().info("<ExpectedException action=add>java.io.FileNotFoundException</ExpectedException>");
        try {
          byte[] keyBytes1 = BlobHelper.serializeToBlob("key-1");
          final HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(
              regionManager, num);
          try {
            int count = 50000;
            ArrayList<TestEvent> items = new ArrayList<TestEvent>();
            String value = getlongString();
            for (int i = 0; i < count; i++) {
              items.add(new TestEvent(("key-" + i), (value + System.nanoTime())));
            }
            cache.getLogger().info("<ExpectedException action=add>java.io.IOException</ExpectedException>");
            organizer.flush(items.iterator(), count);
          } catch (Exception e) {
            // expecting all but one writes to fail
            errorCount.incrementAndGet();
            assertNull(organizer.read(keyBytes1));
          }
          cache.getLogger().info("<ExpectedException action=remove>java.io.IOException</ExpectedException>");
          cache.getLogger().info("<ExpectedException action=remove>PriviledgedActionException</ExpectedException>");
          cache.getLogger().info("<ExpectedException action=remove>java.io.FileNotFoundException</ExpectedException>");
        } catch (IOException e) {
          e.printStackTrace();
        }        
      }

      private String getlongString() {
        String value = "v";
        for (int i =0;  i < 2048; i++ )
          value += "v";
        return value;
      }
    }

    // create parallel threads
    Thread[] clients = new Thread[2];
    for (int i = 0; i < clients.length; i++) {
      clients[i] = new Thread(new Client(i));
    }
    for (int i = 0; i < clients.length; i++) {
      clients[i].start();
    }
    for (int i = 0; i < clients.length; i++) {
      clients[i].join();
    }
    assertEquals(clients.length - 1, errorCount.get());
    
    for (int num = 0; num < clients.length; num++) {
    	final HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(
          regionManager, num);
    	// delete failed files
    	organizer.performMaintenance();

    	FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + num, "");
    	hoplogCount.addAndGet(hoplogs.length);
    }
    
    // check file existence in bucket directory
    assertEquals(1, hoplogCount.get());
  }

  /*
   * A hadoop user may delete region directory directly. This test validates
   * that doing so does not result in creation of large number of hoplogs
   */
  public void test001DirectDirDeletion() throws Exception {
    Configuration hconf = getMiniClusterConf();

    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);

    byte[] keyBytes1 = BlobHelper.serializeToBlob("key-1");
    final HoplogOrganizer<SortedHoplogPersistedEvent> organizer = new HdfsSortedOplogOrganizer(
        regionManager, 1);
    
    int count = 10;
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    organizer.flush(items.iterator(), count);
    assertTrue(((String) organizer.read(keyBytes1).getValue())
        .startsWith("value-"));

    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + 1, "");
    assertEquals(1, hoplogs.length);

    FileStatus[] testDir = cluster.getFileSystem().listStatus(testDataDir);
    assertNotNull(testDir);
    //Note that we also have a "cleanUpInterval" file
    assertEquals(2, testDir.length);
    cluster.getFileSystem().delete(testDataDir, true);
    
    organizer.flush(items.iterator(), count);
    
    hoplogs = getBucketHoplogs(getName() + "/" + 1, "");
    assertEquals(1, hoplogs.length);
  }

  /*
   * This test validates that the number of open hdfs files per bucket is
   * less than or equal to the max configured
   */
  public void test002MaxHdfsOpenFiles() throws Exception {
    // datanode will have 2 transfer threads
    Configuration hconf = getMiniClusterConf();
    hconf.setInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY, 5);
    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);
    
    HdfsSortedOplogOrganizer bucket1 = new HdfsSortedOplogOrganizer(regionManager, 1);
    HdfsSortedOplogOrganizer bucket2 = new HdfsSortedOplogOrganizer(regionManager, 2);
    
    int count = 10;
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int j = 0; j < 3; j++) {
      items.clear();
      for (int i = 0; i < count; i++) {
        items.add(new TestEvent(("key-" + j + "-" + i), ("value")));
      }
      bucket1.flush(items.iterator(), count);
      bucket2.flush(items.iterator(), count);
    }
    
    // block more than half readers by reading oldest key in bucket1
    byte[] keyBytes = BlobHelper.serializeToBlob("key-0-1");
    assertTrue(((String) bucket1.read(keyBytes).getValue()).equals("value"));
    
    // bucket 2 read will fail
    cache.getLogger().info("<ExpectedException action=add>java.io.EOFException</ExpectedException>");
    cache.getLogger().info("<ExpectedException action=add>org.apache.hadoop.hdfs.BlockMissingException</ExpectedException>");
    try {
      assertTrue(((String) bucket2.read(keyBytes).getValue()).equals("value"));
      fail();
    } catch (IOException e) {
      // expected
    }
    cache.getLogger().info("<ExpectedException action=remove>java.io.EOFException</ExpectedException>");
    cache.getLogger().info("<ExpectedException action=remove>org.apache.hadoop.hdfs.BlockMissingException</ExpectedException>");
    
    // restart test after max open file setting, now both reads should pass
    bucket1.close();
    bucket2.close();
    System.setProperty("hoplog.bucket.max.open.files", "1");
    bucket1 = new HdfsSortedOplogOrganizer(regionManager, 1);
    bucket2 = new HdfsSortedOplogOrganizer(regionManager, 2);
    
    // block more than half readers by reading oldest key in bucket1
    assertTrue(((String) bucket1.read(keyBytes).getValue()).equals("value"));
    
    // bucket 2 read will pass this time
    assertTrue(((String) bucket2.read(keyBytes).getValue()).equals("value"));
  }


  public void test003DataReadErrorByFSClose() throws Exception {
    Configuration hconf = getMiniClusterConf();

    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);
    hdfsStore.getFileSystem().delete(testDataDir, true);
    
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    organizer.flush(items.iterator(), items.size());

    byte[] keyBytes = BlobHelper.serializeToBlob("1");
    
    List<TrackedReference<Hoplog>> list = organizer.getSortedOplogs();
    assertEquals(1, list.size());
    Hoplog hoplog = list.get(0).get();
    HoplogReader reader = hoplog.getReader();
    reader.getBloomFilter().mightContain(keyBytes);

    ShutdownHookManager mgr = ShutdownHookManager.get();
    Field field = ShutdownHookManager.class.getDeclaredField("shutdownInProgress");
    field.setAccessible(true);
    field.set(mgr, new AtomicBoolean(true));
    
    hdfsStore.getFileSystem().close();
    hdfsStore = null;
    
    try {
      assertTrue(reader.getBloomFilter().mightContain(keyBytes));
      organizer.read(keyBytes);
      fail();
    } catch (CacheClosedException e) {
      // expected
    }
    field.set(mgr, new AtomicBoolean(false));
  }
  
  public void test004ScanReadErrorByFSClose() throws Exception {
    Configuration hconf = getMiniClusterConf();
    
    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);
    hdfsStore.getFileSystem().delete(testDataDir, true);
    
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    
    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    organizer.flush(items.iterator(), items.size());
    
    byte[] keyBytes = BlobHelper.serializeToBlob("1");
    
    List<TrackedReference<Hoplog>> list = organizer.getSortedOplogs();
    assertEquals(1, list.size());
    Hoplog hoplog = list.get(0).get();
    HoplogReader reader = hoplog.getReader();
    reader.getBloomFilter().mightContain(keyBytes);
    
    ShutdownHookManager mgr = ShutdownHookManager.get();
    Field field = ShutdownHookManager.class.getDeclaredField("shutdownInProgress");
    field.setAccessible(true);
    field.set(mgr, new AtomicBoolean(true));
    
    hdfsStore.getFileSystem().close();
    hdfsStore = null;
    
    try {
      assertTrue(reader.getBloomFilter().mightContain(keyBytes));
      organizer.scan();
      fail();
    } catch (CacheClosedException e) {
      // expected
    }
    field.set(mgr, new AtomicBoolean(false));
  }

  public void test005BloomReadErrorByFSClose() throws Exception {
    Configuration hconf = getMiniClusterConf();

    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);
    hdfsStore.getFileSystem().delete(testDataDir, true);
    
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    organizer.flush(items.iterator(), items.size());

    List<TrackedReference<Hoplog>> list = organizer.getSortedOplogs();
    assertEquals(1, list.size());
    Hoplog hoplog = list.get(0).get();
    HoplogReader reader = hoplog.getReader();

    ShutdownHookManager mgr = ShutdownHookManager.get();
    Field field = ShutdownHookManager.class.getDeclaredField("shutdownInProgress");
    field.setAccessible(true);
    field.set(mgr, new AtomicBoolean(true));
    
    hdfsStore.getFileSystem().close();
    hdfsStore = null;

    byte[] keyBytes = BlobHelper.serializeToBlob("1");
    try {
      organizer.read(keyBytes);
      fail();
    } catch (CacheClosedException e) {
      // expected
    }
    field.set(mgr, new AtomicBoolean(false));
  }

  public void test006HopCloseErrorByFSClose() throws Exception {
    Configuration hconf = getMiniClusterConf();

    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);
    hdfsStore.getFileSystem().delete(testDataDir, true);

    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(
        regionManager, 0);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    organizer.flush(items.iterator(), items.size());
    
    byte[] keyBytes = BlobHelper.serializeToBlob("1");
    assertNotNull(organizer.read(keyBytes));

    ShutdownHookManager mgr = ShutdownHookManager.get();
    Field field = ShutdownHookManager.class
        .getDeclaredField("shutdownInProgress");
    field.setAccessible(true);
    field.set(mgr, new AtomicBoolean(true));

    hdfsStore.getFileSystem().close();
    hdfsStore = null;

    organizer.close();
    field.set(mgr, new AtomicBoolean(false));
  }
  
  public void test007HopWriteErrByClusterDown() throws Exception {
    cache.getLogger().info("<ExpectedException action=add>java.io.IOException</ExpectedException>");
    cache.getLogger().info("<ExpectedException action=add>java.lang.InterruptedException</ExpectedException>");
    Configuration hconf = getMiniClusterConf();
    
    // my iter is needed so that cluster can be closed after creating writer and
    // before calling close
    final AtomicInteger counter = new AtomicInteger(100);      
    class MyIter implements Iterator<TestEvent> {
      public boolean hasNext() {
        return counter.get() >= 0;
      }
      public TestEvent next() {
        try {
          int i = counter.incrementAndGet();
          i = i > 0 ? i : 1000;
          TestEvent event = new TestEvent(("" + i), ("" + System.currentTimeMillis()));
          TimeUnit.MILLISECONDS.sleep(20);
          return event;
        } catch (Exception e) {
        }
        return null;
      }
      public void remove() {
      }
    };
    
    int numDataNodes = 1;
    initMiniCluster(hconf, numDataNodes);
    hdfsStore.getFileSystem().delete(testDataDir, true);
    
    final HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(
        regionManager, 0);
    
    final MyIter iter = new MyIter();
    final AtomicInteger status = new AtomicInteger(0);
    Executors.newSingleThreadScheduledExecutor().execute(new Runnable() {
      public void run() {
        try {
          organizer.flush(iter, 100);
          status.set(-1);
        } catch (IOException e) {
          status.set(1);
          return;
        } catch (Exception e) {
          // not expected
          status.set(-1);
        }
      }
    });
    
    while (counter.get() < 105) {
      TimeUnit.MILLISECONDS.sleep(20);
    }
    
    cluster.shutdown();
    cluster = null;
    counter.set(-100);
    
    int wait = 0;
    while (status.get() == 0 && wait < 1000) {
      TimeUnit.MILLISECONDS.sleep(20);
      wait += 20;
    }
    assertEquals(1, status.get());
    cache.getLogger().info("<ExpectedException action=remove>java.io.IOException</ExpectedException>");
    cache.getLogger().info("<ExpectedException action=remove>java.lang.InterruptedException</ExpectedException>");
    initMiniCluster(hconf, 1);
  }

  private Configuration getMiniClusterConf() {
    System.setProperty("test.build.data", HDFS_DIR);
    Configuration hconf = new HdfsConfiguration();
    // hconf.set("hadoop.log.dir", "/tmp/hdfs/logs");
    hconf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    return hconf;
  }

  private void initMiniCluster(Configuration hconf, int numDataNodes)
      throws IOException {
    Builder builder = new MiniDFSCluster.Builder(hconf);
    builder.numDataNodes(numDataNodes);
    builder.nameNodePort(CLUSTER_PORT);
    cluster = builder.build();
  }

  @Override
  protected void configureHdfsStoreFactory() throws Exception {
    super.configureHdfsStoreFactory();
    
    configFile = new File("testMaxHdfsOpenFiles-config");
    String hadoopClientConf = "<configuration>\n" + 
                "  <property>\n" + 
                "    <name>dfs.client.max.block.acquire.failures</name>\n" + 
                "    <value>0</value>\n" + 
                "  </property>\n" + 
                "  <property>\n" + 
                "    <name>dfs.client.retry.window.base</name>\n" + 
                "    <value>10</value>\n" + 
                "  </property>\n" + 
                "</configuration>";
    BufferedWriter bw = new BufferedWriter(new FileWriter(configFile));
    bw.write(hadoopClientConf);
    bw.close();

    hsf.setNameNodeURL("hdfs://127.0.0.1:" + CLUSTER_PORT);
    hsf.setHDFSClientConfigFile("testMaxHdfsOpenFiles-config");
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if (cluster != null) {
      cluster.shutdown();
    }
    cluster = null;
    FileUtils.deleteDirectory(new File(HDFS_DIR));
    configFile.delete();
    cache.getLogger().info("<ExpectedException action=remove>java.io.IOException</ExpectedException>");
  }
}


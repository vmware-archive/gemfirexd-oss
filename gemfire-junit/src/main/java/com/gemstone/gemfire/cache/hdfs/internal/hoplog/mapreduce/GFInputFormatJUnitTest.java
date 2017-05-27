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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.BaseHoplogTestCase;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HdfsSortedOplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;
import io.snappydata.test.dunit.AvailablePortHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class GFInputFormatJUnitTest extends BaseHoplogTestCase {
  MiniDFSCluster cluster;
  private int CLUSTER_PORT = AvailablePortHelper.getRandomAvailableTCPPort();
  private File configFile;
  Path regionPath = null;

  /*
   * Tests if a file split is created for each hdfs block occupied by hoplogs of
   * a region
   */
  public void testNBigFiles1Dn() throws Exception {
    cluster = initMiniCluster(CLUSTER_PORT, 1);

    int count = 30;
    HdfsSortedOplogOrganizer bucket1 = new HdfsSortedOplogOrganizer(
        regionManager, 1);
    int hopCount = 3;
    for (int j = 0; j < hopCount; j++) {
      ArrayList<TestEvent> items = new ArrayList<TestEvent>();
      for (int i = 0; i < count; i++) {
        items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
      }
      bucket1.flush(items.iterator(), count);
    }

    Path bucketPath = new Path(regionPath, "1");

    int expectedBlocks = 0;
    long remainder = 0;
    long blockSize = 2048;
    List<TrackedReference<Hoplog>> hoplogs = bucket1.getSortedOplogs();
    for (TrackedReference<Hoplog> hop : hoplogs) {
      Path hopPath = new Path(bucketPath, hop.get().getFileName());
      FileStatus status = hdfsStore.getFileSystem().getFileStatus(hopPath);
      blockSize = status.getBlockSize();
      long len = status.getLen();
      assertNotSame(blockSize, len);
      while (len > status.getBlockSize()) {
        expectedBlocks ++;
        len -= blockSize;
      };
      remainder += len;
    }
    expectedBlocks += remainder / blockSize;
    assertTrue(expectedBlocks > 1);

    Configuration conf = hdfsStore.getFileSystem().getConf();

    GFInputFormat gfInputFormat = new GFInputFormat();
    Job job = Job.getInstance(conf, "test");

    conf = job.getConfiguration();
    conf.set(GFInputFormat.INPUT_REGION, getName());
    conf.set(GFInputFormat.HOME_DIR, testDataDir.getName());
    conf.setBoolean(GFInputFormat.CHECKPOINT, false);

    List<InputSplit> splits = gfInputFormat.getSplits(job);
    assertTrue(Math.abs(expectedBlocks - splits.size()) <= 1);
    assertTrue(hopCount < splits.size());
    
    for (InputSplit split : splits) {
      assertEquals(1, split.getLocations().length);
    }
  }

  public void testNSmallFiles1Dn() throws Exception {
    cluster = initMiniCluster(CLUSTER_PORT, 1);
    
    int count = 1;
    HdfsSortedOplogOrganizer bucket1 = new HdfsSortedOplogOrganizer(
        regionManager, 1);
    int hopCount = 3;
    for (int j = 0; j < hopCount; j++) {
      ArrayList<TestEvent> items = new ArrayList<TestEvent>();
      for (int i = 0; i < count; i++) {
        items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
      }
      bucket1.flush(items.iterator(), count);
    }
    
    Path bucketPath = new Path(regionPath, "1");
    
    int expectedBlocks = 0;
    long remainder = 0;
    long blockSize = 2048;
    List<TrackedReference<Hoplog>> hoplogs = bucket1.getSortedOplogs();
    for (TrackedReference<Hoplog> hop : hoplogs) {
      Path hopPath = new Path(bucketPath, hop.get().getFileName());
      FileStatus status = hdfsStore.getFileSystem().getFileStatus(hopPath);
      blockSize = status.getBlockSize();
      long len = status.getLen();
      while (len > status.getBlockSize()) {
        expectedBlocks ++;
        len -= blockSize;
      };
      remainder += len;
    }
    expectedBlocks += remainder / blockSize;
    
    Configuration conf = hdfsStore.getFileSystem().getConf();
    
    GFInputFormat gfInputFormat = new GFInputFormat();
    Job job = Job.getInstance(conf, "test");
    
    conf = job.getConfiguration();
    conf.set(GFInputFormat.INPUT_REGION, getName());
    conf.set(GFInputFormat.HOME_DIR, testDataDir.getName());
    conf.setBoolean(GFInputFormat.CHECKPOINT, false);
    
    List<InputSplit> splits = gfInputFormat.getSplits(job);
    assertEquals(expectedBlocks, splits.size());
    assertTrue(hopCount > splits.size());
    for (InputSplit split : splits) {
      assertEquals(1, split.getLocations().length);
    }
  }
  
  /*
   * Test splits cover all the data in the hoplog
   */
  public void testHfileSplitCompleteness() throws Exception {
    cluster = initMiniCluster(CLUSTER_PORT, 1);

    int count = 40;
    HdfsSortedOplogOrganizer bucket1 = new HdfsSortedOplogOrganizer(
        regionManager, 1);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    bucket1.flush(items.iterator(), count);

    Configuration conf = hdfsStore.getFileSystem().getConf();
    GFInputFormat gfInputFormat = new GFInputFormat();
    Job job = Job.getInstance(conf, "test");

    conf = job.getConfiguration();
    conf.set(GFInputFormat.INPUT_REGION, getName());
    conf.set(GFInputFormat.HOME_DIR, testDataDir.getName());
    conf.setBoolean(GFInputFormat.CHECKPOINT, false);

    List<InputSplit> splits = gfInputFormat.getSplits(job);
    assertTrue(1 < splits.size());

    long lastBytePositionOfPrevious = 0;
    for (InputSplit inputSplit : splits) {
      CombineFileSplit split = (CombineFileSplit) inputSplit;
      assertEquals(1, split.getPaths().length);
      assertEquals(lastBytePositionOfPrevious, split.getOffset(0));
      lastBytePositionOfPrevious += split.getLength();
      assertEquals(1, split.getLocations().length);
    }

    Path bucketPath = new Path(regionPath, "1");
    Path hopPath = new Path(bucketPath, bucket1.getSortedOplogs().iterator()
        .next().get().getFileName());
    FileStatus status = hdfsStore.getFileSystem().getFileStatus(hopPath);
    assertEquals(status.getLen(), lastBytePositionOfPrevious);
  }

  @Override
  protected void configureHdfsStoreFactory() throws Exception {
    super.configureHdfsStoreFactory();

    configFile = new File("testGFInputFormat-config");
    String hadoopClientConf = "<configuration>\n             "
        + "  <property>\n                                    "
        + "    <name>dfs.block.size</name>\n                 "
        + "    <value>2048</value>\n                         "
        + "  </property>\n                                   "
        + "  <property>\n                                    "
        + "    <name>fs.default.name</name>\n                "
        + "    <value>hdfs://127.0.0.1:" + CLUSTER_PORT + "</value>\n"
        + "  </property>\n                                   "
        + "  <property>\n                                    "
        + "    <name>dfs.replication</name>\n                "
        + "    <value>1</value>\n                            "
        + "  </property>\n                                   "
        + "</configuration>";
    BufferedWriter bw = new BufferedWriter(new FileWriter(configFile));
    bw.write(hadoopClientConf);
    bw.close();
    hsf.setHDFSClientConfigFile(configFile.getName());
  }

  @Override
  protected void setUp() throws Exception {
    CLUSTER_PORT = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    super.setUp();
    regionPath = new Path(testDataDir, getName());
  }

  @Override
  protected void tearDown() throws Exception {
    if (configFile != null) {
      configFile.delete();
    }
    super.tearDown();
    if (cluster != null) {
      cluster.shutdown();
    }
    deleteMiniClusterDir();
  }
}

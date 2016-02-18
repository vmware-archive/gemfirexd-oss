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
import java.util.HashSet;
import java.util.List;

import io.snappydata.test.dunit.AvailablePortHelper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.BaseHoplogTestCase;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HFileSortedOplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HdfsSortedOplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.internal.util.BlobHelper;

public class GFRecordReaderJUnitTest extends BaseHoplogTestCase {
  MiniDFSCluster cluster;
  private int CLUSTER_PORT = AvailablePortHelper.getRandomAvailableTCPPort();
  private File configFile;
  Path regionPath = null;

  /*
   * Test splits cover all the data in the hoplog
   */
  public void testGFRecordReader1HopNBlocks() throws Exception {
    cluster = super.initMiniCluster(CLUSTER_PORT, 1);

    int count = 200;
    HdfsSortedOplogOrganizer bucket1 = new HdfsSortedOplogOrganizer(
        regionManager, 1);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    bucket1.flush(items.iterator(), count);
    HFileSortedOplog hoplog = (HFileSortedOplog) bucket1.getSortedOplogs().iterator().next().get();
    
    int blockCount = (int) (hoplog.getSize() / 4096) + 1;
    assertTrue(1 < blockCount);

    Configuration conf = hdfsStore.getFileSystem().getConf();
    GFInputFormat gfInputFormat = new GFInputFormat();
    Job job = Job.getInstance(conf, "test");

    conf = job.getConfiguration();
    conf.set(GFInputFormat.INPUT_REGION, getName());
    conf.set(GFInputFormat.HOME_DIR, testDataDir.getName());
    conf.setBoolean(GFInputFormat.CHECKPOINT, false);

    List<InputSplit> splits = gfInputFormat.getSplits(job);
    assertEquals(blockCount, splits.size());

    CombineFileSplit split = (CombineFileSplit) splits.get(1);

    HoplogIterator<byte[], byte[]> directIter = hoplog.getReader().scan(
        split.getOffset(0), split.getLength(0));

    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
        new TaskAttemptID());
    RecordReader<GFKey, PersistedEventImpl> reader = gfInputFormat
        .createRecordReader(splits.get(1), context);
    reader.initialize(splits.get(1), context);

    for (; directIter.hasNext();) {
      assertTrue(reader.nextKeyValue());
      directIter.next();

      assertEquals(BlobHelper.deserializeBlob(directIter.getKey()),
          reader.getCurrentKey().getKey());
    }

    assertFalse(reader.nextKeyValue());

    hoplog.close();
  }
  
  public void testGFRecordReaderNHop1Split() throws Exception {
    cluster = super.initMiniCluster(CLUSTER_PORT, 1);
    
    int entryCount = 2;
    int bucketCount = 3;
    HashSet<String> keySet = new HashSet<String>();
    
    for (int j = 0; j < bucketCount; j++) {
      HdfsSortedOplogOrganizer bucket = new HdfsSortedOplogOrganizer(
          regionManager, j);
      ArrayList<TestEvent> items = new ArrayList<TestEvent>();
      for (int i = 0; i < entryCount; i++) {
        String key = "key - " + j + " : " + i;
        items.add(new TestEvent(key, ("value-" + System.nanoTime())));
        keySet.add(key);
      }
      bucket.flush(items.iterator(), entryCount);
    }
    
    assertEquals(entryCount * bucketCount, keySet.size());
    
    Configuration conf = hdfsStore.getFileSystem().getConf();
    GFInputFormat gfInputFormat = new GFInputFormat();
    Job job = Job.getInstance(conf, "test");
    
    conf = job.getConfiguration();
    conf.set(GFInputFormat.INPUT_REGION, getName());
    conf.set(GFInputFormat.HOME_DIR, testDataDir.getName());
    conf.setBoolean(GFInputFormat.CHECKPOINT, false);
    
    List<InputSplit> splits = gfInputFormat.getSplits(job);
    assertEquals(1, splits.size());
    
    CombineFileSplit split = (CombineFileSplit) splits.get(0);
    assertEquals(bucketCount, split.getNumPaths());
    
    TaskAttemptContext context = new TaskAttemptContextImpl(conf,
        new TaskAttemptID());
    RecordReader<GFKey, PersistedEventImpl> reader = gfInputFormat
        .createRecordReader(split, context);
    reader.initialize(split, context);
    
    while (reader.nextKeyValue()) {
      keySet.remove(reader.getCurrentKey().getKey());
    }
    assertEquals(0, keySet.size());
    
    reader.close();
  }

  @Override
  protected void configureHdfsStoreFactory() throws Exception {
    super.configureHdfsStoreFactory();

    System.setProperty(HoplogConfig.HFILE_BLOCK_SIZE_CONF,
        String.valueOf(1 << 8));

    configFile = new File("testGFInputFormat-config");
    String hadoopClientConf = "<configuration>\n             "
        + "  <property>\n                                    "
        + "    <name>dfs.block.size</name>\n                 "
        + "    <value>4096</value>\n                         "
        + "  </property>\n                                   "
        + "  <property>\n                                    "
        + "    <name>fs.default.name</name>\n                "
        + "    <value>hdfs://127.0.0.1:" + CLUSTER_PORT + "</value>\n"
        + "  </property>\n                                   "
        + "</configuration>";
    BufferedWriter bw = new BufferedWriter(new FileWriter(configFile));
    bw.write(hadoopClientConf);
    bw.close();
    hsf.setHDFSClientConfigFile(configFile.getName());
  }

  @Override
  protected void setUp() throws Exception {
    CLUSTER_PORT = AvailablePortHelper.getRandomAvailableTCPPort();
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
      FileUtils.deleteDirectory(new File("hdfs-test-cluster"));
    }    
  }
}

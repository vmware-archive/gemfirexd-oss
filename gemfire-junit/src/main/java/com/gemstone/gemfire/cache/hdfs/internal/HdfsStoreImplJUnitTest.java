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
package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import io.snappydata.test.dunit.AvailablePortHelper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSCompactionConfigMutator;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator.HDFSEventQueueAttributesMutator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.BaseHoplogTestCase;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HFileSortedOplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HdfsSortedOplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.util.BlobHelper;

public class HdfsStoreImplJUnitTest extends BaseHoplogTestCase {
  public void testAlterAttribute() throws Exception {
    assertEquals(HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE, hdfsStore.getMaxFileSize());
    assertEquals(HDFSStore.DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL, hdfsStore.getFileRolloverInterval());
    
    HDFSCompactionConfig compConfig = hdfsStore.getHDFSCompactionConfig();
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT, compConfig.getMaxInputFileCount());
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_SIZE_MB, compConfig.getMaxInputFileSizeMB());
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_THREADS, compConfig.getMaxThreads());
    assertEquals(HDFSCompactionConfig.DEFAULT_MIN_INPUT_FILE_COUNT, compConfig.getMinInputFileCount());
    assertFalse(compConfig.getAutoCompaction());

    assertEquals(HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS, compConfig.getMajorCompactionIntervalMins());
    assertEquals(HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_MAX_THREADS, compConfig.getMajorCompactionMaxThreads());
    assertFalse(compConfig.getAutoMajorCompaction());
    
    assertEquals(HDFSCompactionConfig.DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS, compConfig.getOldFilesCleanupIntervalMins());
    
    HDFSEventQueueAttributes qAttr = hdfsStore.getHDFSEventQueueAttributes();
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB, qAttr.getBatchSizeMB());
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_TIME_INTERVAL_MILLIS, qAttr.getBatchTimeInterval());
    
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    HDFSCompactionConfigMutator compMutator = mutator.getCompactionConfigMutator();
    HDFSEventQueueAttributesMutator qMutator = mutator.getHDFSEventQueueAttributesMutator();
    
    mutator.setMaxFileSize(234);
    mutator.setFileRolloverInterval(121);
    
    compMutator.setMaxInputFileCount(87);
    compMutator.setMaxInputFileSizeMB(45);
    compMutator.setMinInputFileCount(34);
    compMutator.setMaxThreads(843);
    compMutator.setAutoCompaction(true);

    compMutator.setMajorCompactionIntervalMins(26);
    compMutator.setMajorCompactionMaxThreads(92);
    compMutator.setAutoMajorCompaction(true);
    
    compMutator.setOldFilesCleanupIntervalMins(328);
    
    qMutator.setBatchSizeMB(985);
    qMutator.setBatchTimeInterval(695);
    
    hdfsStore.alter(mutator);
    
    assertEquals(234, hdfsStore.getMaxFileSize());
    assertEquals(121, hdfsStore.getFileRolloverInterval());
    
    compConfig = hdfsStore.getHDFSCompactionConfig();
    assertEquals(87, compConfig.getMaxInputFileCount());
    assertEquals(45, compConfig.getMaxInputFileSizeMB());
    assertEquals(843, compConfig.getMaxThreads());
    assertEquals(34, compConfig.getMinInputFileCount());
    assertTrue(compConfig.getAutoCompaction());

    assertEquals(26, compConfig.getMajorCompactionIntervalMins());
    assertEquals(92, compConfig.getMajorCompactionMaxThreads());
    assertTrue(compConfig.getAutoMajorCompaction());
    
    assertEquals(328, compConfig.getOldFilesCleanupIntervalMins());
    
    qAttr = hdfsStore.getHDFSEventQueueAttributes();
    assertEquals(985, qAttr.getBatchSizeMB());
    assertEquals(695, qAttr.getBatchTimeInterval());
  }
  
  public void testSameHdfsMultiStore() throws Exception {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    MiniDFSCluster cluster = initMiniCluster(port ,1);
    // create store with config file
    hsf.setHomeDir("Store-1");
    File confFile = new File("HdfsStoreImplJUnitTest-Store-1");
    String conf = "<configuration>\n             "
        + "  <property>\n                                    "
        + "    <name>dfs.block.size</name>\n                 "
        + "    <value>1024</value>\n                         "
        + "  </property>\n                                   "
        + "  <property>\n                                    "
        + "    <name>fs.default.name</name>\n                "
        + "    <value>hdfs://127.0.0.1:" + port + "</value>\n"
        + "  </property>\n                                   "
        + "</configuration>";
    setConfigFile(hsf, confFile, conf);
    HDFSStoreImpl store1 = (HDFSStoreImpl) hsf.create("Store-1");
    confFile.delete();

    // create region with store
    regionfactory.setHDFSStoreName(store1.getName());
    Region<Object, Object> region1 = regionfactory.create("region-1");

    // populate data and check block size
    Path path = new Path("Store-1/region-1/0/1-1-1.hop");
    Hoplog oplog = new HFileSortedOplog(store1, path,
        store1.getBlockCache(), director.getHdfsRegionStats("/region-1"),
        store1.getStats());
    createHoplog(10, oplog);
    
    FileStatus[] status = store1.getFileSystem().listStatus(path);
    assertEquals(1024, status[0].getBlockSize());

    // create store with config file
    hsf.setHomeDir("Store-2");
    confFile = new File("HdfsStoreImplJUnitTest-Store-2");
    conf = "<configuration>\n             "
        + "  <property>\n                                    "
        + "    <name>dfs.block.size</name>\n                 "
        + "    <value>2048</value>\n                         "
        + "  </property>\n                                   "
        + "  <property>\n                                    "
        + "    <name>fs.default.name</name>\n                "
        + "    <value>hdfs://127.0.0.1:" + port + "</value>\n"
        + "  </property>\n                                   "
        + "</configuration>";
    setConfigFile(hsf, confFile, conf);
    HDFSStoreImpl store2 = (HDFSStoreImpl) hsf.create("Store-2");
    confFile.delete();
    
    // create region with store
    regionfactory.setHDFSStoreName(store2.getName());
    Region<Object, Object> region2 = regionfactory.create("region-2");
    
    // populate data and check block size
    path = new Path("Store-2/region-2/0/1-1-1.hop");
    oplog = new HFileSortedOplog(store2, path,
        store2.getBlockCache(), director.getHdfsRegionStats("/region-2"),
        store2.getStats());
    createHoplog(10, oplog);
    
    status = store2.getFileSystem().listStatus(path);
    assertEquals(2048, status[0].getBlockSize());
    
    // cleanup
    region1.destroyRegion();
    store1.destroy();
    region2.destroyRegion();
    store2.destroy();
    cleanupCluster(cluster);
  }
  
  public void testSameHdfsMultiStoreOneClose() throws Exception {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    MiniDFSCluster cluster = initMiniCluster(port ,1);
    byte[] keyBytes5 = BlobHelper.serializeToBlob("5");

    hsf.setHomeDir("Store-1");
    hsf.setNameNodeURL("hdfs://127.0.0.1:" + port);
    HDFSStoreImpl store1 = (HDFSStoreImpl) hsf.create("Store-1");
    // create region with store
    regionfactory.setHDFSStoreName(store1.getName());
    Region<Object, Object> region1 = regionfactory.create("region-1");
    HdfsRegionManager regionManager1 = ((LocalRegion)region1).getHdfsRegionManager();
    HdfsSortedOplogOrganizer organizer1 = new HdfsSortedOplogOrganizer(regionManager1, 0);
    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 100; i++) {
      items.add(new TestEvent("" + i, "" + i));
    }
    organizer1.flush(items.iterator(), items.size());
    assertEquals("5", organizer1.read(keyBytes5).getValue());
    
    hsf.setHomeDir("Store-2");
    hsf.setNameNodeURL("hdfs://127.0.0.1:" + port);
    HDFSStoreImpl store2 = (HDFSStoreImpl) hsf.create("Store-2");
    // create region with store
    regionfactory.setHDFSStoreName(store2.getName());
    Region<Object, Object> region2 = regionfactory.create("region-2");
    HdfsRegionManager regionManager2 = ((LocalRegion)region2).getHdfsRegionManager();
    HdfsSortedOplogOrganizer organizer2 = new HdfsSortedOplogOrganizer(regionManager2, 0);
    organizer2.flush(items.iterator(), items.size());

    // close region one and then check if region 2 is still readable
    region1.destroyRegion();
    store1.destroy();
    assertEquals("5", organizer2.read(keyBytes5).getValue());
    
    region2.destroyRegion();
    store2.destroy();
    cleanupCluster(cluster);
  }
  
  public void testCheckAndFixFs() throws Exception {
    deleteMiniClusterDir();
    
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    MiniDFSCluster cluster = initMiniCluster(port ,1);
    // create store with config file
    hsf.setHomeDir("Store-1");
    File confFile = new File(getName() + ".xml");
    String conf = "<configuration>\n             "
        + "  <property>\n                                    "
        + "    <name>dfs.block.size</name>\n                 "
        + "    <value>1024</value>\n                         "
        + "  </property>\n                                   "
        + "  <property>\n                                    "
        + "    <name>fs.default.name</name>\n                "
        + "    <value>hdfs://127.0.0.1:" + port + "</value>\n"
        + "  </property>\n                                   "
        + "</configuration>";
    setConfigFile(hsf, confFile, conf);
    HDFSStoreImpl store1 = (HDFSStoreImpl) hsf.create("Store-1");
    
    assertTrue(store1.getFileSystem().exists(new Path("/")));
    assertTrue(store1.getCachedFileSystem().exists(new Path("/")));

    store1.getFileSystem().close();
    cache.getLogger().info("<ExpectedException action=add>java.io.IOException</ExpectedException>");
    try {
      store1.getCachedFileSystem().exists(new Path("/"));
      fail();
    } catch (IOException e) {
      // expected
    }
    
    store1.checkAndClearFileSystem();
    cache.getLogger().info("<ExpectedException action=remove>java.io.IOException</ExpectedException>");
    
    assertTrue(store1.getFileSystem().exists(new Path("/")));
    assertTrue(store1.getCachedFileSystem().exists(new Path("/")));
    confFile.delete();
    cleanupCluster(cluster);
  }

  void cleanupCluster(MiniDFSCluster cluster) throws Exception {
    cluster.shutdown();
    FileUtils.deleteDirectory(new File("hdfs-test-cluster"));
  }
}

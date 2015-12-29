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
package com.pivotal.gemfirexd.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;

/**
 * @author Ashvin
 */

public class AlterHDFSStoreTest extends JdbcTestBase {
  public AlterHDFSStoreTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    System.setProperty(HDFSStoreImpl.ALLOW_STANDALONE_HDFS_FILESYSTEM_PROP, "true");
    deleteIfDirExists();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    deleteIfDirExists();
  }

  private void deleteIfDirExists() throws IOException {
    String dirs[] = {"MYHDFS", "MYHDFS1", "MYHDFS2"};
    for (String dir : dirs) {
      File hdfsDir = new File(dir);
      if (hdfsDir.exists()) {
        FileUtils.deleteDirectory(hdfsDir);
      }
    }
  }

  public void testAlterAllMutableAttributes() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();

    // create a default store and verify the values
    st.execute("create hdfsstore myhdfs namenode 'localhost'");
    
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ArrayList<HDFSStoreImpl> stores = cache.getAllHDFSStores();
    assertEquals(1, stores.size());
    
    HDFSStoreImpl hdfsStore = cache.findHDFSStore("MYHDFS");
    assertNotNull(hdfsStore);
    assertEquals(HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE, hdfsStore.getMaxFileSize());
    assertEquals(HDFSStore.DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL, hdfsStore.getFileRolloverInterval());
    
    HDFSCompactionConfig compConfig = hdfsStore.getHDFSCompactionConfig();
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT, compConfig.getMaxInputFileCount());
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_SIZE_MB, compConfig.getMaxInputFileSizeMB());
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_THREADS, compConfig.getMaxThreads());
    assertEquals(HDFSCompactionConfig.DEFAULT_MIN_INPUT_FILE_COUNT, compConfig.getMinInputFileCount());
    assertTrue(compConfig.getAutoCompaction());

    assertEquals(HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS, compConfig.getMajorCompactionIntervalMins());
    assertEquals(HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_MAX_THREADS, compConfig.getMajorCompactionMaxThreads());
    assertTrue(compConfig.getAutoMajorCompaction());
    
    assertEquals(HDFSCompactionConfig.DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS, compConfig.getOldFilesCleanupIntervalMins());
    
    HDFSEventQueueAttributes qAttr = hdfsStore.getHDFSEventQueueAttributes();
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB, qAttr.getBatchSizeMB());
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_TIME_INTERVAL_MILLIS, qAttr.getBatchTimeInterval());

    // alter values and verify change in hdfsstore
    st.execute("alter hdfsstore myhdfs "
        + "SET MaxWriteOnlyFileSize 47 "
        + "SET WriteOnlyFileRolloverInterval 347 minutes "
        + "SET MaxInputFileCount 98 "
        + "SET MaxInputFileSize 38 "
        + "SET MinorCompactionThreads 35 "
        + "SET MinInputFileCount 24 "
        + "SET MinorCompact false "
        + "SET MajorCompactionInterval 372 minutes "
        + "SET MajorCompactionThreads 29 "
        + "SET MajorCompact false "
        + "SET PurgeInterval 2392 minutes "
        + "SET BatchSize 23 "
        + "SET BatchTimeInterval 2 seconds "
        );
    
    assertEquals(47, hdfsStore.getMaxFileSize());
    assertEquals(347 * 60, hdfsStore.getFileRolloverInterval());
    
    compConfig = hdfsStore.getHDFSCompactionConfig();
    assertEquals(98, compConfig.getMaxInputFileCount());
    assertEquals(38, compConfig.getMaxInputFileSizeMB());
    assertEquals(35, compConfig.getMaxThreads());
    assertEquals(24, compConfig.getMinInputFileCount());
    assertFalse(compConfig.getAutoCompaction());

    assertEquals(372, compConfig.getMajorCompactionIntervalMins());
    assertEquals(29, compConfig.getMajorCompactionMaxThreads());
    assertFalse(compConfig.getAutoMajorCompaction());
    
    assertEquals(2392, compConfig.getOldFilesCleanupIntervalMins());
    
    qAttr = hdfsStore.getHDFSEventQueueAttributes();
    assertEquals(23, qAttr.getBatchSizeMB());
    assertEquals(2000, qAttr.getBatchTimeInterval());
  }
  
  public void testAlterTwoHdfsStores() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    
    // create a default store and verify the values
    st.execute("create hdfsstore myhdfs1 namenode 'localhost'");
    st.execute("create hdfsstore myhdfs2 namenode 'localhost'");
    
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ArrayList<HDFSStoreImpl> stores = cache.getAllHDFSStores();
    assertEquals(2, stores.size());
    
    HDFSStoreImpl hdfsStore1 = cache.findHDFSStore("MYHDFS1");
    assertNotNull(hdfsStore1);
    assertEquals(HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE, hdfsStore1.getMaxFileSize());
    HDFSCompactionConfig compConfig = hdfsStore1.getHDFSCompactionConfig();
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT, compConfig.getMaxInputFileCount());
    HDFSEventQueueAttributes qAttr = hdfsStore1.getHDFSEventQueueAttributes();
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB, qAttr.getBatchSizeMB());
    
    HDFSStoreImpl hdfsStore2 = cache.findHDFSStore("MYHDFS2");
    assertNotNull(hdfsStore2);
    assertEquals(HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE, hdfsStore2.getMaxFileSize());
    compConfig = hdfsStore2.getHDFSCompactionConfig();
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT, compConfig.getMaxInputFileCount());
    qAttr = hdfsStore2.getHDFSEventQueueAttributes();
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB, qAttr.getBatchSizeMB());
    
    // alter values and verify change in hdfsstore
    st.execute("alter hdfsstore myhdfs1 "
        + "SET MaxWriteOnlyFileSize 47 "
        + "SET MaxInputFileCount 98 "
        + "SET BatchSize 23 "
        );
    
    // alter values and verify change in hdfsstore
    st.execute("alter hdfsstore myhdfs2 "
        + "SET MaxWriteOnlyFileSize 74 "
        + "SET MaxInputFileCount 89 "
        + "SET BatchSize 32 "
        );
    
    assertEquals(47, hdfsStore1.getMaxFileSize());
    compConfig = hdfsStore1.getHDFSCompactionConfig();
    assertEquals(98, compConfig.getMaxInputFileCount());
    qAttr = hdfsStore1.getHDFSEventQueueAttributes();
    assertEquals(23, qAttr.getBatchSizeMB());

    assertEquals(74, hdfsStore2.getMaxFileSize());
    compConfig = hdfsStore2.getHDFSCompactionConfig();
    assertEquals(89, compConfig.getMaxInputFileCount());
    qAttr = hdfsStore2.getHDFSEventQueueAttributes();
    assertEquals(32, qAttr.getBatchSizeMB());
  }
  
  public void testAlterOneOfTwoHdfsStores() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    
    // create a default store and verify the values
    st.execute("create hdfsstore myhdfs1 namenode 'localhost'");
    st.execute("create hdfsstore myhdfs2 namenode 'localhost'");
    
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ArrayList<HDFSStoreImpl> stores = cache.getAllHDFSStores();
    assertEquals(2, stores.size());
    
    HDFSStoreImpl hdfsStore1 = cache.findHDFSStore("MYHDFS1");
    assertNotNull(hdfsStore1);
    assertEquals(HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE, hdfsStore1.getMaxFileSize());
    HDFSCompactionConfig compConfig = hdfsStore1.getHDFSCompactionConfig();
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT, compConfig.getMaxInputFileCount());
    HDFSEventQueueAttributes qAttr = hdfsStore1.getHDFSEventQueueAttributes();
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB, qAttr.getBatchSizeMB());
    
    HDFSStoreImpl hdfsStore2 = cache.findHDFSStore("MYHDFS2");
    assertNotNull(hdfsStore2);
    assertEquals(HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE, hdfsStore2.getMaxFileSize());
    compConfig = hdfsStore2.getHDFSCompactionConfig();
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT, compConfig.getMaxInputFileCount());
    qAttr = hdfsStore2.getHDFSEventQueueAttributes();
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB, qAttr.getBatchSizeMB());
    
    // alter values and verify change in hdfsstore
    st.execute("alter hdfsstore myhdfs1 "
        + "SET MaxWriteOnlyFileSize 47 "
        + "SET MaxInputFileCount 98 "
        + "SET BatchSize 23 "
        );
    
    assertEquals(47, hdfsStore1.getMaxFileSize());
    compConfig = hdfsStore1.getHDFSCompactionConfig();
    assertEquals(98, compConfig.getMaxInputFileCount());
    qAttr = hdfsStore1.getHDFSEventQueueAttributes();
    assertEquals(23, qAttr.getBatchSizeMB());
    
    assertNotNull(hdfsStore2);
    assertEquals(HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE, hdfsStore2.getMaxFileSize());
    compConfig = hdfsStore2.getHDFSCompactionConfig();
    assertEquals(HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT, compConfig.getMaxInputFileCount());
    qAttr = hdfsStore2.getHDFSEventQueueAttributes();
    assertEquals(HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB, qAttr.getBatchSizeMB());
  }
  
  public void testAlterInvalidValue() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    
    // create a default store and verify the values
    st.execute("create hdfsstore myhdfs namenode 'localhost'");
    
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ArrayList<HDFSStoreImpl> stores = cache.getAllHDFSStores();
    assertEquals(1, stores.size());
    HDFSStoreImpl hdfsStore = cache.findHDFSStore("MYHDFS");
    assertNotNull(hdfsStore);
        
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET MaxWriteOnlyFileSize -1 "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }

    conn = TestUtil.getConnection();
    st = conn.createStatement();
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET WriteOnlyFileRolloverInterval 347 "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
    
    conn = TestUtil.getConnection();
    st = conn.createStatement();
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET MinorCompact noop "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
    
    conn = TestUtil.getConnection();
    st = conn.createStatement();
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET MajorCompactionInterval 5 milliseconds "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
  }
  
  public void testAlterImmutableAttributes() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    
    // create a default store and verify the values
    st.execute("create hdfsstore myhdfs namenode 'localhost'");
    
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ArrayList<HDFSStoreImpl> stores = cache.getAllHDFSStores();
    assertEquals(1, stores.size());
    HDFSStoreImpl hdfsStore = cache.findHDFSStore("MYHDFS");
    assertNotNull(hdfsStore);
    
    // alter immutable attribute and statement will fail
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET MaxQueueMemory 47 "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
    
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET DispatcherThreads 47 "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
    
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET QueuePersistent false "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }

    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET DiskSynchronous false "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
    
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET BlockCacheSize 1 "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
    
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET ClientConfigFile 'abcd' "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
  }
  
  public void testAlterOfMissingStore() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ArrayList<HDFSStoreImpl> stores = cache.getAllHDFSStores();
    assertEquals(0, stores.size());
    HDFSStoreImpl hdfsStore = cache.findHDFSStore("MYHDFS");
    assertNull(hdfsStore);
    
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET BatchSize 47 "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { /*expected*/ }
  }
  
  public void testAlterAfterDropStore() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    
    // create a default store and verify the values
    st.execute("create hdfsstore myhdfs namenode 'localhost'");
    
    GemFireCacheImpl cache = Misc.getGemFireCache();
    ArrayList<HDFSStoreImpl> stores = cache.getAllHDFSStores();
    assertEquals(1, stores.size());   
    HDFSStoreImpl hdfsStore = cache.findHDFSStore("MYHDFS");
    assertNotNull(hdfsStore);
    
    st.execute("drop hdfsstore myhdfs");
    stores = cache.getAllHDFSStores();
    assertEquals(0, stores.size());
    hdfsStore = cache.findHDFSStore("MYHDFS");
    assertNull(hdfsStore);
    
    try {
      st.execute("alter hdfsstore myhdfs "
          + "SET BatchSize 47 "
          );
      fail("alter statement should have failed");
    } catch (SQLException e) { e.printStackTrace();/*expected*/ }
  }
  
  public void testDDLConflation() throws Exception {
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.put("mcast-port", String.valueOf(mcastPort));    
    props.put(DistributionConfig.MCAST_TTL_NAME, "0");
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();    

    st.execute("create hdfsstore myhdfs namenode 'localhost' "); 
    st.execute("alter hdfsstore myhdfs "
        + "SET BatchSize 47 "
        );
    st.execute("drop hdfsstore myhdfs");
    shutDown();    
    //DDL replay
    conn = TestUtil.getConnection(props);
    st = conn.createStatement();
    
   
  }
  
}

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

package hydra;

import batterytest.BatteryTest;

import java.io.IOException;
import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory.HDFSCompactionConfigFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;

/**
 * Encodes information needed to describe and create an HDFS store.
 */
public class HDFSStoreDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this HDFS store description and actual id */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Boolean autoCompaction;
  private Boolean autoMajorCompaction;
  private Integer batchSizeMB;
  private Integer batchTimeInterval;
  private Float blockCacheSize;
  private String compactionStrategy;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String diskStoreName;
  private Boolean diskSynchronous;
  private Integer fileRolloverInterval;
  private String hadoopName;
  private HadoopDescription hadoopDescription; // from hadoopName
  private String hdfsClientConfigFile;
  private String homeDir;
  private Integer majorCompactionIntervalMins;
  private Integer majorCompactionMaxThreads;
  private Integer maxFileSize;
  private Integer maxInputFileCount;
  private Integer maxInputFileSizeMB;
  private Integer maxThreads;
  private Integer maximumQueueMemory;
  private Integer minInputFileCount;
  private Integer oldFilesCleanupIntervalMins;
  private Boolean persistent;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public HDFSStoreDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this HDFS store description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this HDFS store description.
   */
  private void setName(String str) {
    this.name = str;
  }

  public Boolean getAutoCompaction() {
    return this.autoCompaction;
  }

  private void setAutoCompaction(Boolean bool) {
    this.autoCompaction = bool;
  }

  public Boolean getAutoMajorCompaction() {
    return this.autoMajorCompaction;
  }

  private void setAutoMajorCompaction(Boolean bool) {
    this.autoMajorCompaction = bool;
  }

  public Integer getBatchSizeMB() {
    return this.batchSizeMB;
  }

  private void setBatchSizeMB(Integer i) {
    this.batchSizeMB = i;
  }

  public Integer getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  private void setBatchTimeInterval(Integer i) {
    this.batchTimeInterval = i;
  }

  public Float getBlockCacheSize() {
    return this.blockCacheSize;
  }

  private void setBlockCacheSize(Float f) {
    this.blockCacheSize = f;
  }

  public String getCompactionStrategy() {
    return this.compactionStrategy;
  }

  private void setCompactionStrategy(String str) {
    this.compactionStrategy = str;
  }

  /**
   * Returns the disk store description name.
   */
  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  /**
   * Sets the disk store description name.
   */
  private void setDiskStoreName(String str) {
    this.diskStoreName = str;
  }

  public DiskStoreDescription getDiskStoreDescription() {
    return this.diskStoreDescription;
  }

  private void setDiskStoreDescription(DiskStoreDescription dsd) {
    this.diskStoreDescription = dsd;
  }

  public Boolean getDiskSynchronous() {
    return this.diskSynchronous;
  }

  private void setDiskSynchronous(Boolean bool) {
    this.diskSynchronous = bool;
  }

  public Integer getFileRolloverInterval() {
    return this.fileRolloverInterval;
  }

  private void setFileRolloverInterval(Integer i) {
    this.fileRolloverInterval = i;
  }

  /**
   * Returns the Hadoop cluster name.
   */
  public String getHadoopName() {
    return this.hadoopName;
  }

  /**
   * Sets the Hadoop cluster name.
   */
  private void setHadoopName(String str) {
    this.hadoopName = str;
  }

  public HadoopDescription getHadoopDescription() {
    return this.hadoopDescription;
  }

  private void setHadoopDescription(HadoopDescription hd) {
    this.hadoopDescription = hd;
  }

  public String getHDFSClientConfigFile() {
    return this.hdfsClientConfigFile;
  }

  private void setHDFSClientConfigFile(String str) {
    this.hdfsClientConfigFile = str;
  }

  public String getHomeDir() {
    return this.homeDir;
  }

  private void setHomeDir(String str) {
    this.homeDir = str;
  }

  public Integer getMajorCompactionIntervalMins() {
    return this.majorCompactionIntervalMins;
  }

  private void setMajorCompactionIntervalMins(Integer i) {
    this.majorCompactionIntervalMins = i;
  }

  public Integer getMajorCompactionMaxThreads() {
    return this.majorCompactionMaxThreads;
  }

  private void setMajorCompactionMaxThreads(Integer i) {
    this.majorCompactionMaxThreads = i;
  }

  public Integer getMaxFileSize() {
    return this.maxFileSize;
  }

  private void setMaxFileSize(Integer i) {
    this.maxFileSize = i;
  }

  public Integer getMaxInputFileCount() {
    return this.maxInputFileCount;
  }

  private void setMaxInputFileCount(Integer i) {
    this.maxInputFileCount = i;
  }

  public Integer getMaxInputFileSizeMB() {
    return this.maxInputFileSizeMB;
  }

  private void setMaxInputFileSizeMB(Integer i) {
    this.maxInputFileSizeMB = i;
  }

  public Integer getMaxThreads() {
    return this.maxThreads;
  }

  private void setMaxThreads(Integer i) {
    this.maxThreads = i;
  }

  public Integer getMaximumQueueMemory() {
    return this.maximumQueueMemory;
  }

  private void setMaximumQueueMemory(Integer i) {
    this.maximumQueueMemory = i;
  }

  public Integer getMinInputFileCount() {
    return this.minInputFileCount;
  }

  private void setMinInputFileCount(Integer i) {
    this.minInputFileCount = i;
  }

  public Integer getOldFilesCleanupIntervalMins() {
    return this.oldFilesCleanupIntervalMins;
  }

  private void setOldFilesCleanupIntervalMins(Integer i) {
    this.oldFilesCleanupIntervalMins = i;
  }

  public Boolean getPersistent() {
    return this.persistent;
  }

  private void setPersistent(Boolean bool) {
    this.persistent = bool;
  }

//------------------------------------------------------------------------------
// HDFS store configuration
//------------------------------------------------------------------------------

  /**
   * Configures an HDFS store using this description.
   */
  protected void configure(HDFSStoreFactory f) {
    if (this.getBlockCacheSize() != null) {
      f.setBlockCacheSize(this.getBlockCacheSize().floatValue());
    }
    f.setFileRolloverInterval(this.getFileRolloverInterval().intValue());
    if (this.getHDFSClientConfigFile() != null) {
      String clientName = RemoteTestModule.getMyClientName();
      HostDescription hd = TestConfig.getInstance()
                                     .getClientDescription(clientName)
                                     .getVmDescription().getHostDescription();
      String fn = EnvHelper.expandEnvVars(this.getHDFSClientConfigFile(), hd);
      f.setHDFSClientConfigFile(fn);
    }
    f.setHomeDir(this.getHomeDir());
    f.setMaxFileSize(this.getMaxFileSize().intValue());
    f.setNameNodeURL(this.getHadoopDescription().getNameNodeURL());

    HDFSCompactionConfigFactory cf = f.createCompactionConfigFactory(this.getCompactionStrategy());
    cf.setAutoCompaction(this.getAutoCompaction());
    cf.setAutoMajorCompaction(this.getAutoMajorCompaction());
    cf.setMajorCompactionIntervalMins(this.getMajorCompactionIntervalMins());
    cf.setMajorCompactionMaxThreads(this.getMajorCompactionMaxThreads());
    cf.setMaxThreads(this.getMaxThreads());
    cf.setMaxInputFileCount(this.getMaxInputFileCount());
    cf.setMaxInputFileSizeMB(this.getMaxInputFileSizeMB());
    cf.setMinInputFileCount(this.getMinInputFileCount());
    cf.setOldFilesCleanupIntervalMins(this.getOldFilesCleanupIntervalMins());
    f.setHDFSCompactionConfig(cf.create());
    //f.setHDFSCompactionConfig(cf.getConfigView());

    HDFSEventQueueAttributesFactory qf = new HDFSEventQueueAttributesFactory();
    qf.setBatchSizeMB(this.getBatchSizeMB().intValue());
    qf.setBatchTimeInterval(this.getBatchTimeInterval().intValue());
    if (this.getDiskStoreDescription() != null) {
      qf.setDiskStoreName(this.getDiskStoreDescription().getName());
    }
    qf.setDiskSynchronous(this.getDiskSynchronous().booleanValue());
    qf.setMaximumQueueMemory(this.getMaximumQueueMemory().intValue());
    qf.setPersistent(this.getPersistent().booleanValue());

    HDFSEventQueueAttributes qatts = qf.create();
    f.setHDFSEventQueueAttributes(qatts);
  }

  /**
   * Returns the HDFS store as a string. For use only by {@link HDFSStoreHelper
   * #hdfsStoreToString(HDFSStore)}.
   */
  protected static synchronized String hdfsStoreToString(HDFSStore h) {
    return hdfsStoreToString(h.getName(), h);
  }

  /**
   * Returns the HDFS store as a string.  Serves calling methods for both
   * factory attributes and actual HDFS store.
   */
  protected static synchronized String hdfsStoreToString(String hdfsStoreName,
                                       HDFSStore h) {
    HDFSEventQueueAttributes q = h.getHDFSEventQueueAttributes();
    HDFSCompactionConfig c = h.getHDFSCompactionConfig();
    StringBuffer buf = new StringBuffer();
    buf.append("\n  hdfsStoreName: " + hdfsStoreName);
    buf.append("\n  autoCompaction: " + c.getAutoCompaction());
    buf.append("\n  autoMajorCompaction: " + c.getAutoMajorCompaction());
    buf.append("\n  batchSizeMB: " + q.getBatchSizeMB());
    buf.append("\n  batchTimeInterval: " + q.getBatchTimeInterval());
    buf.append("\n  blockCacheSize: " + h.getBlockCacheSize());
    buf.append("\n  compactionStrategy: " + c.getCompactionStrategy());
    buf.append("\n  diskStoreName: " + q.getDiskStoreName());
    buf.append("\n  diskSynchronous: " + q.isDiskSynchronous());
    buf.append("\n  fileRolloverInterval: " + h.getFileRolloverInterval());
    buf.append("\n  hdfsClientConfigFile: " + h.getHDFSClientConfigFile());
    buf.append("\n  homeDir: " + h.getHomeDir());
    buf.append("\n  majorCompactionIntervalMins: " + c.getMajorCompactionIntervalMins());
    buf.append("\n  majorCompactionMaxThreads: " + c.getMajorCompactionMaxThreads());
    buf.append("\n  maxFileSize: " + h.getMaxFileSize());
    buf.append("\n  maxInputFileCount: " + c.getMaxInputFileCount());
    buf.append("\n  maxInputFileSizeMB: " + c.getMaxInputFileSizeMB());
    buf.append("\n  maxThreads: " + c.getMaxThreads());
    buf.append("\n  maximumQueueMemory: " + q.getMaximumQueueMemory());
    buf.append("\n  minInputFileCount: " + c.getMinInputFileCount());
    buf.append("\n  oldFilesCleanupIntervalMins: " + c.getOldFilesCleanupIntervalMins());
    buf.append("\n  nameNodeURL: " + h.getNameNodeURL());
    buf.append("\n  persistent: " + q.isPersistent());
    return buf.toString();
  }

  /**
   * Returns whether the two HDFS stores have the same attributes.
   */
  protected static boolean equals(HDFSStore h1, HDFSStore h2) {
    HDFSEventQueueAttributes hq1 = h1.getHDFSEventQueueAttributes();
    HDFSEventQueueAttributes hq2 = h2.getHDFSEventQueueAttributes();
    HDFSCompactionConfig hc1 = h1.getHDFSCompactionConfig();
    HDFSCompactionConfig hc2 = h2.getHDFSCompactionConfig();
    if (hc1.getAutoCompaction() != hc2.getAutoCompaction() ||
        hc1.getAutoMajorCompaction() != hc2.getAutoMajorCompaction() ||
        hq1.getBatchSizeMB() != hq2.getBatchSizeMB() ||
        hq1.getBatchTimeInterval() != hq2.getBatchTimeInterval() ||
        h1.getBlockCacheSize() != h2.getBlockCacheSize() ||
        !hc1.getCompactionStrategy().equals(hc2.getCompactionStrategy()) ||
        !hq1.getDiskStoreName().equals(hq2.getDiskStoreName()) ||
        hq1.isDiskSynchronous() != hq2.isDiskSynchronous() ||
        h1.getFileRolloverInterval() != h2.getFileRolloverInterval() ||
        !h1.getHomeDir().equals(h2.getHomeDir()) ||
        hc1.getMajorCompactionIntervalMins() != hc2.getMajorCompactionIntervalMins() ||
        hc1.getMajorCompactionMaxThreads() != hc2.getMajorCompactionMaxThreads() ||
        hc1.getMaxThreads() != hc2.getMaxThreads() ||
        h1.getMaxFileSize() != h2.getMaxFileSize() ||
        hc1.getMaxInputFileCount() != hc2.getMaxInputFileCount() ||
        hc1.getMaxInputFileSizeMB() != hc2.getMaxInputFileSizeMB() ||
        hc1.getMinInputFileCount() != hc2.getMinInputFileCount() ||
        hq1.getMaximumQueueMemory() != hq2.getMaximumQueueMemory() ||
        !h1.getNameNodeURL().equals(h2.getNameNodeURL()) ||
        hc1.getOldFilesCleanupIntervalMins() != hc2.getOldFilesCleanupIntervalMins() ||
        hq1.isPersistent() != hq2.isPersistent()) {
      return false;
    }
    if (h1.getHDFSClientConfigFile() == null && h2.getHDFSClientConfigFile() != null) {
      return false;
    } else if (h1.getHDFSClientConfigFile() != null && h2.getHDFSClientConfigFile() == null) {
      return false;
    } else if (h1.getHDFSClientConfigFile() != null && h2.getHDFSClientConfigFile() != null && !h1.getHDFSClientConfigFile().equals(h2.getHDFSClientConfigFile())) {
      return false;
    }
    return true;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "autoCompaction", this.getAutoCompaction());
    map.put(header + "autoMajorCompaction", this.getAutoMajorCompaction());
    map.put(header + "batchSizeMB", this.getBatchSizeMB());
    map.put(header + "batchTimeInterval", this.getBatchTimeInterval());
    map.put(header + "blockCacheSize", this.getBlockCacheSize());
    map.put(header + "compactionStrategy", this.getCompactionStrategy());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "diskSynchronous", this.getDiskSynchronous());
    map.put(header + "fileRolloverInterval", this.getFileRolloverInterval());
    map.put(header + "hadoopName", this.getHadoopName());
    map.put(header + "hdfsClientConfigFile", this.getHDFSClientConfigFile());
    map.put(header + "homeDir", this.getHomeDir());
    map.put(header + "majorCompactionIntervalMins", this.getMajorCompactionIntervalMins());
    map.put(header + "majorCompactionMaxThreads", this.getMajorCompactionMaxThreads());
    map.put(header + "maxFileSize", this.getMaxFileSize());
    map.put(header + "maxInputFileCount", this.getMaxInputFileCount());
    map.put(header + "maxInputFileSizeMB", this.getMaxInputFileSizeMB());
    map.put(header + "maxThreads", this.getMaxThreads());
    map.put(header + "maximumQueueMemory", this.getMaximumQueueMemory());
    map.put(header + "minInputFileCount", this.getMinInputFileCount());
    map.put(header + "persistent", this.getPersistent());
    map.put(header + "oldFilesCleanupIntervalMins", this.getOldFilesCleanupIntervalMins());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates HDFS store descriptions from the HDFS store parameters
   * in the test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each HDFS store name
    Vector names = tab.vecAt(HDFSStorePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create HDFS store description from test configuration parameters
      HDFSStoreDescription hsd = createHDFSStoreDescription(
                                                        name, config, i);

      // save configuration
      config.addHDFSStoreDescription(hsd);
    }
  }

  /**
   * Creates the HDFS store description using test configuration parameters
   * and product defaults.
   */
  private static HDFSStoreDescription createHDFSStoreDescription(
                 String name, TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    HDFSStoreDescription hsd = new HDFSStoreDescription();
    hsd.setName(name);

    // autoCompaction
    {
      Long key = HDFSStorePrms.autoCompaction;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = HDFSCompactionConfig.DEFAULT_AUTO_COMPACTION;
      }
      hsd.setAutoCompaction(bool);
    }
    // autoMajorCompaction
    {
      Long key = HDFSStorePrms.autoMajorCompaction;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = HDFSCompactionConfig.DEFAULT_AUTO_MAJOR_COMPACTION;
      }
      hsd.setAutoMajorCompaction(bool);
    }
    // batchSizeMB
    {
      Long key = HDFSStorePrms.batchSizeMB;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB;
      }
      hsd.setBatchSizeMB(i);
    }
    // batchTimeInterval
    {
      Long key = HDFSStorePrms.batchTimeInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSEventQueueAttributesFactory.DEFAULT_BATCH_TIME_INTERVAL_MILLIS;
      }
      hsd.setBatchTimeInterval(i);
    }
    // blockCacheSize
    {
      Long key = HDFSStorePrms.blockCacheSize;
      Double d = tab.getDouble(key, tab.getWild(key, index, null));
      Float f = (d == null) ? HDFSStore.DEFAULT_BLOCK_CACHE_SIZE
                            : Float.valueOf(d.floatValue());
      hsd.setBlockCacheSize(f);
    }
    // compactionStrategy
    {
      Long key = HDFSStorePrms.compactionStrategy;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = HDFSCompactionConfig.DEFAULT_STRATEGY;
      }
      hsd.setCompactionStrategy(str);
    }
    // diskStoreName (generates DiskStoreDescription)
    {
      Long key = HDFSStorePrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        hsd.setDiskStoreDescription(getDiskStoreDescription(str, key, config));
        hsd.setDiskStoreName("DiskStoreDescription." + str);
      }
    }
    // diskSynchronous
    {
      Long key = HDFSStorePrms.diskSynchronous;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_DISK_SYNCHRONOUS;
      }
      hsd.setDiskSynchronous(bool);
    }
    // fileRolloverInterval
    {
      Long key = HDFSStorePrms.fileRolloverInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSStore.DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL;
      }
      hsd.setFileRolloverInterval(i);
    }
    // hadoopName (generates HadoopDescription)
    {
      Long key = HDFSStorePrms.hadoopName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        String s = BasePrms.nameForKey(key) + " is a required parameter";
        throw new HydraConfigException(s);
      }
      hsd.setHadoopDescription(getHadoopDescription(str, key, config));
      hsd.setHadoopName("HadoopDescription." + str);
    }
    // hdfsClientConfigFile
    {
      Long key = HDFSStorePrms.hdfsClientConfigFile;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hsd.setHDFSClientConfigFile(str);
      }
    }
    // homeDir
    {
      Long key = HDFSStorePrms.homeDir;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) { // default to the test directory name
        String userDir = config.getMasterDescription().getVmDescription()
                               .getHostDescription().getUserDir();
        str = FileUtil.filenameFor(userDir);
      }
      hsd.setHomeDir(str);
    }
    // majorCompactionIntervalMins
    {
      Long key = HDFSStorePrms.majorCompactionIntervalMins;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS;
      }
      hsd.setMajorCompactionIntervalMins(i);
    }
    // majorCompactionMaxThreads
    {
      Long key = HDFSStorePrms.majorCompactionMaxThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_MAX_THREADS;
      }
      hsd.setMajorCompactionMaxThreads(i);
    }
    // maxFileSize
    {
      Long key = HDFSStorePrms.maxFileSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE;
      }
      hsd.setMaxFileSize(i);
    }
    // maxInputFileCount
    {
      Long key = HDFSStorePrms.maxInputFileCount;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_COUNT;
      }
      hsd.setMaxInputFileCount(i);
    }
    // maxInputFileSizeMB
    {
      Long key = HDFSStorePrms.maxInputFileSizeMB;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_SIZE_MB;
      }
      hsd.setMaxInputFileSizeMB(i);
    }
    // maxThreads
    {
      Long key = HDFSStorePrms.maxThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAX_THREADS;
      }
      hsd.setMaxThreads(i);
    }
    // maximumQueueMemory
    {
      Long key = HDFSStorePrms.maximumQueueMemory;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;
      }
      hsd.setMaximumQueueMemory(i);
    }
    // minInputFileCount
    {
      Long key = HDFSStorePrms.minInputFileCount;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MIN_INPUT_FILE_COUNT;
      }
      hsd.setMinInputFileCount(i);
    }
    // oldFilesCleanupIntervalMins
    {
      Long key = HDFSStorePrms.oldFilesCleanupIntervalMins;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS;
      }
      hsd.setOldFilesCleanupIntervalMins(i);
    }
    // persistent
    {
      Long key = HDFSStorePrms.persistent;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;
      }
      hsd.setPersistent(bool);
    }
    HadoopDescription hdd = hsd.getHadoopDescription();
    if (hdd.useExistingCluster()) {
      // connecting to existing cluster so add special cleanup
      boolean moveHadoopData = Boolean.getBoolean(BatteryTest.MOVE_HADOOP_DATA);
      Nuker.getInstance().recordHDFSDir(hdd.getHadoopDist(),
            hdd.getNameNodeURL(), moveHadoopData);
    }
    return hsd;
  }

//------------------------------------------------------------------------------
// Disk store configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the disk store description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         DiskStorePrms#names}.
   */
  private static DiskStoreDescription getDiskStoreDescription(String str,
                                                  Long key, TestConfig config) {
    DiskStoreDescription dsd = config.getDiskStoreDescription(str);
    if (dsd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(DiskStorePrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return dsd;
    }
  }

//------------------------------------------------------------------------------
// Hadoop configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the hadoop description for the given string.
   * @throws HydraConfigException if the given string is not listed in {@link
   *         HadoopPrms#names}.
   */
  private static HadoopDescription getHadoopDescription(String str, Long key,
                                                        TestConfig config) {
    HadoopDescription hd = config.getHadoopDescription(str);
    if (hd == null) {
      String s = BasePrms.nameForKey(key) + " not found in "
               + BasePrms.nameForKey(HadoopPrms.names) + ": " + str;
      throw new HydraConfigException(s);
    } else {
      return hd;
    }
  }

//------------------------------------------------------------------------------
// Version support
//------------------------------------------------------------------------------

  /**
   * Custom deserialization.
   */
  private void readObject(java.io.ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    this.autoCompaction = (Boolean)in.readObject();
    this.autoMajorCompaction = (Boolean)in.readObject();
    this.batchSizeMB = (Integer)in.readObject();
    this.batchTimeInterval = (Integer)in.readObject();
    this.blockCacheSize = (Float)in.readObject();
    this.compactionStrategy = (String)in.readObject();
    this.diskStoreDescription = (DiskStoreDescription)in.readObject();
    this.diskStoreName = (String)in.readObject();
    this.diskSynchronous = (Boolean)in.readObject();
    this.fileRolloverInterval = (Integer)in.readObject();
    this.hadoopDescription = (HadoopDescription)in.readObject();
    this.hadoopName = (String)in.readObject();
    this.hdfsClientConfigFile = (String)in.readObject();
    this.homeDir = (String)in.readObject();
    this.majorCompactionIntervalMins = (Integer)in.readObject();
    this.majorCompactionMaxThreads = (Integer)in.readObject();
    this.maxFileSize = (Integer)in.readObject();
    this.maxInputFileCount = (Integer)in.readObject();
    this.maxInputFileSizeMB = (Integer)in.readObject();
    this.maxThreads = (Integer)in.readObject();
    this.maximumQueueMemory = (Integer)in.readObject();
    this.minInputFileCount = (Integer)in.readObject();
    this.name = (String)in.readObject();
    this.oldFilesCleanupIntervalMins = (Integer)in.readObject();
    this.persistent = (Boolean)in.readObject();
  }

  /**
   * Custom serialization.
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.autoCompaction);
    out.writeObject(this.autoMajorCompaction);
    out.writeObject(this.batchSizeMB);
    out.writeObject(this.batchTimeInterval);
    out.writeObject(this.blockCacheSize);
    out.writeObject(this.compactionStrategy);
    out.writeObject(this.diskStoreDescription);
    out.writeObject(this.diskStoreName);
    out.writeObject(this.diskSynchronous);
    out.writeObject(this.fileRolloverInterval);
    out.writeObject(this.hadoopDescription);
    out.writeObject(this.hadoopName);
    out.writeObject(this.hdfsClientConfigFile);
    out.writeObject(this.homeDir);
    out.writeObject(this.majorCompactionIntervalMins);
    out.writeObject(this.majorCompactionMaxThreads);
    out.writeObject(this.maxFileSize);
    out.writeObject(this.maxInputFileCount);
    out.writeObject(this.maxInputFileSizeMB);
    out.writeObject(this.maxThreads);
    out.writeObject(this.maximumQueueMemory);
    out.writeObject(this.minInputFileCount);
    out.writeObject(this.name);
    out.writeObject(this.oldFilesCleanupIntervalMins);
    out.writeObject(this.persistent);
  }
}

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

package hydra.gemfirexd;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import batterytest.BatteryTest;

import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStore.HDFSCompactionConfig;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.pivotal.gemfirexd.internal.impl.sql.compile.CreateHDFSStoreNode;

import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.EnvHelper;
import hydra.FileUtil;
import hydra.HadoopDescription;
import hydra.HadoopPrms;
import hydra.HostDescription;
import hydra.HostHelper;
import hydra.HostPrms;
import hydra.HydraConfigException;
import hydra.HydraVector;
import hydra.Nuker;
import hydra.RemoteTestModule;
import hydra.TestConfig;

/**
 * Encodes information needed to describe and create an HDFS store.
 */
public class HDFSStoreDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String SPACE = " ";
  private static final String TIC = "'";

  /** The logical name of this HDFS store description and actual id */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Integer batchSize;
  private Integer batchTimeInterval;
  private Integer blockCacheSize;
  private String clientConfigFile;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String diskStoreName;
  private Boolean diskSynchronous;
  private Integer dispatcherThreads;
  private HadoopDescription hadoopDescription; // from hadoopName
  private String hadoopName;
  private String homeDir;
  private Boolean majorCompact;
  private Integer majorCompactionInterval;
  private Integer majorCompactionThreads;
  private Integer maxInputFileCount;
  private Integer maxInputFileSize;
  private Integer maxQueueMemory;
  private Integer maxWriteOnlyFileSize;
  private Integer minInputFileCount;
  private Boolean minorCompact;
  private Integer minorCompactionThreads;
  private Integer purgeInterval;
  private Boolean queuePersistent;
  private Integer writeOnlyFileRolloverInterval;

  private transient String DDL;

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

  public Integer getBatchSize() {
    return this.batchSize;
  }

  private void setBatchSize(Integer i) {
    this.batchSize = i;
  }

  public Integer getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  private void setBatchTimeInterval(Integer i) {
    this.batchTimeInterval = i;
  }

  public Integer getBlockCacheSize() {
    return this.blockCacheSize;
  }

  private void setBlockCacheSize(Integer i) {
    this.blockCacheSize = i;
  }

  public String getClientConfigFile() {
    return this.clientConfigFile;
  }

  private void setClientConfigFile(String str) {
    this.clientConfigFile = str;
  }

  public DiskStoreDescription getDiskStoreDescription() {
    return this.diskStoreDescription;
  }

  private void setDiskStoreDescription(DiskStoreDescription dsd) {
    this.diskStoreDescription = dsd;
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

  public Boolean getDiskSynchronous() {
    return this.diskSynchronous;
  }

  private void setDiskSynchronous(Boolean bool) {
    this.diskSynchronous = bool;
  }

  public Integer getDispatcherThreads() {
    return this.dispatcherThreads;
  }

  private void setDispatcherThreads(Integer i) {
    this.dispatcherThreads = i;
  }

  public HadoopDescription getHadoopDescription() {
    return this.hadoopDescription;
  }

  private void setHadoopDescription(HadoopDescription hd) {
    this.hadoopDescription = hd;
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

  public String getHomeDir() {
    return this.homeDir;
  }

  private void setHomeDir(String str) {
    this.homeDir = str;
  }

  public Boolean getMajorCompact() {
    return this.majorCompact;
  }

  private void setMajorCompact(Boolean bool) {
    this.majorCompact = bool;
  }

  public Integer getMajorCompactionInterval() {
    return this.majorCompactionInterval;
  }

  public Integer getMajorCompactionThreads() {
    return this.majorCompactionThreads;
  }

  private void setMajorCompactionThreads(Integer i) {
    this.majorCompactionThreads = i;
  }

  private void setMajorCompactionInterval(Integer i) {
    this.majorCompactionInterval = i;
  }

  public Integer getMaxInputFileCount() {
    return this.maxInputFileCount;
  }

  private void setMaxInputFileCount(Integer i) {
    this.maxInputFileCount = i;
  }

  public Integer getMaxInputFileSize() {
    return this.maxInputFileSize;
  }

  private void setMaxInputFileSize(Integer i) {
    this.maxInputFileSize = i;
  }

  public Integer getMaxQueueMemory() {
    return this.maxQueueMemory;
  }

  private void setMaxQueueMemory(Integer i) {
    this.maxQueueMemory = i;
  }

  public Integer getMaxWriteOnlyFileSize() {
    return this.maxWriteOnlyFileSize;
  }

  private void setMaxWriteOnlyFileSize(Integer i) {
    this.maxWriteOnlyFileSize = i;
  }

  public Integer getMinInputFileCount() {
    return this.minInputFileCount;
  }

  private void setMinInputFileCount(Integer i) {
    this.minInputFileCount = i;
  }

  public Boolean getMinorCompact() {
    return this.minorCompact;
  }

  private void setMinorCompact(Boolean bool) {
    this.minorCompact = bool;
  }

  public Integer getMinorCompactionThreads() {
    return this.minorCompactionThreads;
  }

  private void setMinorCompactionThreads(Integer i) {
    this.minorCompactionThreads = i;
  }

  public Integer getPurgeInterval() {
    return this.purgeInterval;
  }

  private void setPurgeInterval(Integer i) {
    this.purgeInterval = i;
  }

  public Boolean getQueuePersistent() {
    return this.queuePersistent;
  }

  private void setQueuePersistent(Boolean bool) {
    this.queuePersistent = bool;
  }

  public Integer getWriteOnlyFileRolloverInterval() {
    return this.writeOnlyFileRolloverInterval;
  }

  private void setWriteOnlyFileRolloverInterval(Integer i) {
    this.writeOnlyFileRolloverInterval = i;
  }

//------------------------------------------------------------------------------
// HDFS store configuration
//------------------------------------------------------------------------------

  /** Returns DDL for HDFS STORE based on this description. */
  protected synchronized String getDDL() {
    if (DDL == null) {
      StringBuffer buf = new StringBuffer();
      buf.append("CREATE HDFSSTORE ").append(this.getName())
         .append(" NAMENODE '").append(this.getHadoopDescription().getNameNodeURL()).append(TIC)
         .append(SPACE).append(CreateHDFSStoreNode.HOMEDIR).append(SPACE).append(TIC).append(this.getHomeDir()).append(TIC)
         .append(SPACE).append(CreateHDFSStoreNode.BATCHSIZE).append(SPACE).append(this.getBatchSize())
         .append(SPACE).append(CreateHDFSStoreNode.BATCHTIMEINTERVAL).append(SPACE).append(this.getBatchTimeInterval()).append(" milliseconds")
         .append(SPACE).append(CreateHDFSStoreNode.BLOCKCACHESIZE).append(SPACE).append(this.getBlockCacheSize())
         .append(SPACE).append(CreateHDFSStoreNode.DISKSTORENAME).append(SPACE).append(this.getDiskStoreDescription().getName())
         .append(SPACE).append(CreateHDFSStoreNode.DISKSYNCHRONOUS).append(SPACE).append(this.getDiskSynchronous())
         .append(SPACE).append(CreateHDFSStoreNode.DISPATCHERTHREADS).append(SPACE).append(this.getDispatcherThreads())
         .append(SPACE).append(CreateHDFSStoreNode.MAJORCOMPACT).append(SPACE).append(this.getMajorCompact())
         .append(SPACE).append(CreateHDFSStoreNode.MAJORCOMPACTIONINTERVAL).append(SPACE).append(this.getMajorCompactionInterval()).append(" minutes")
         .append(SPACE).append(CreateHDFSStoreNode.MAJORCOMPACTIONTHREADS).append(SPACE).append(this.getMajorCompactionThreads())
         .append(SPACE).append(CreateHDFSStoreNode.MAXINPUTFILECOUNT).append(SPACE).append(this.getMaxInputFileCount())
         .append(SPACE).append(CreateHDFSStoreNode.MAXINPUTFILESIZE).append(SPACE).append(this.getMaxInputFileSize())
         .append(SPACE).append(CreateHDFSStoreNode.MAXQUEUEMEMORY).append(SPACE).append(this.getMaxQueueMemory())
         .append(SPACE).append(CreateHDFSStoreNode.MAXWRITEONLYFILESIZE).append(SPACE).append(this.getMaxWriteOnlyFileSize())
         .append(SPACE).append(CreateHDFSStoreNode.MINORCOMPACT).append(SPACE).append(this.getMinorCompact())
         .append(SPACE).append(CreateHDFSStoreNode.MINORCOMPACTIONTHREADS).append(SPACE).append(this.getMinorCompactionThreads())
         .append(SPACE).append(CreateHDFSStoreNode.MININPUTFILECOUNT).append(SPACE).append(this.getMinInputFileCount())
         .append(SPACE).append(CreateHDFSStoreNode.PURGEINTERVAL).append(SPACE).append(this.getPurgeInterval()).append(" minutes")
         .append(SPACE).append(CreateHDFSStoreNode.QUEUEPERSISTENT).append(SPACE).append(this.getQueuePersistent())
         .append(SPACE).append(CreateHDFSStoreNode.WRITEONLYFILEROLLOVERINTERVAL).append(SPACE).append(this.getWriteOnlyFileRolloverInterval()).append(" seconds");
      ;
      String ccf = this.getClientConfigFile();
      if (ccf != null && ccf.length() > 0) {
        buf.append(SPACE).append(CreateHDFSStoreNode.CLIENTCONFIGFILE).append(SPACE).append(TIC).append(expandClientConfigFile(ccf)).append(TIC);
      }
      DDL = buf.toString();
    }
    return DDL;
  }

  /**
   * Expand the path based on the server environment.
   */
  private String expandClientConfigFile(String fn) {
    HostDescription rhd = null; // representative host description
    Map<String,FabricServerDescription> fsds = GfxdTestConfig.getInstance().getFabricServerDescriptions();
    for (FabricServerDescription fsd : fsds.values()) {
      if (fsd.getHostData()) {
        for (String clientName : fsd.getClientNames()) {
          HostDescription hd =
            TestConfig.getInstance().getClientDescription(clientName).getVmDescription().getHostDescription();
          if (rhd == null) {
            rhd = hd;
          }
          if (fn.contains("$PWD") && !hd.getUserDir().equals(rhd.getUserDir())) {
            String s = "Unsure how to expand " + HDFSStorePrms.nameForKey(HDFSStorePrms.clientConfigFile) + "=" + fn
                     + " with servers setting " + HostPrms.nameForKey(HostPrms.userDirs) + " to both " + hd.getUserDir() + " and " + rhd.getUserDir();
            throw new HydraConfigException(s);
          }
          if (fn.contains("$JTESTS") && hd.getTestDir() != rhd.getTestDir()) {
            String s = "Unsure how to expand " + HDFSStorePrms.nameForKey(HDFSStorePrms.clientConfigFile) + "=" + fn
                     + " with servers setting " + HostPrms.nameForKey(HostPrms.testDirs) + " to both " + hd.getTestDir() + " and " + rhd.getTestDir();
            throw new HydraConfigException(s);
          }
        }
      }
    }
    if (rhd == null) {
      // allow this in case the test is not using hydra support for fabric servers
      rhd = TestConfig.getInstance().getClientDescription(RemoteTestModule.getMyClientName())
                      .getVmDescription().getHostDescription();
    }

    String tmpfn = EnvHelper.expandEnvVars(fn, rhd);
    if (rhd.getOSType() == HostHelper.OSType.windows) {
      tmpfn = tmpfn.replace("\\", "/");
    }
    return tmpfn;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "batchSize", this.getBatchSize());
    map.put(header + "batchTimeInterval", this.getBatchTimeInterval());
    map.put(header + "blockCacheSize", this.getBlockCacheSize());
    map.put(header + "clientConfigFile", this.getClientConfigFile());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "diskSynchronous", this.getDiskSynchronous());
    map.put(header + "dispatcherThreads", this.getDispatcherThreads());
    map.put(header + "hadoopName", this.getHadoopName());
    map.put(header + "homeDir", this.getHomeDir());
    map.put(header + "majorCompact", this.getMajorCompact());
    map.put(header + "majorCompactionInterval", this.getMajorCompactionInterval());
    map.put(header + "majorCompactionThreads", this.getMajorCompactionThreads());
    map.put(header + "maxInputFileCount", this.getMaxInputFileCount());
    map.put(header + "maxInputFileSize", this.getMaxInputFileSize());
    map.put(header + "maxQueueMemory", this.getMaxQueueMemory());
    map.put(header + "maxWriteOnlyFileSize", this.getMaxWriteOnlyFileSize());
    map.put(header + "minInputFileCount", this.getMinInputFileCount());
    map.put(header + "minorCompact", this.getMinorCompact());
    map.put(header + "minorCompactionThreads", this.getMinorCompactionThreads());
    map.put(header + "purgeInterval", this.getPurgeInterval());
    map.put(header + "queuePersistent", this.getQueuePersistent());
    map.put(header + "writeOnlyFileRolloverInterval", this.getWriteOnlyFileRolloverInterval());
    return map;
  }

  //------------------------------------------------------------------------------
  // Configuration
  //------------------------------------------------------------------------------

  /**
   * Creates HDFS store descriptions from the HDFS store parameters
   * in the test configuration.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each HDFS store name
    SortedMap<String,HDFSStoreDescription> hsds = new TreeMap();
    Vector names = tab.vecAt(HDFSStorePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create HDFS store description from test configuration parameters
      HDFSStoreDescription hsd = createHDFSStoreDescription(name, config, sconfig, i);

      // save configuration
      hsds.put(name, hsd);
    }
    return hsds;
  }

  /** Creates the HDFS store description using test configuration parameters and product defaults. */
  private static HDFSStoreDescription createHDFSStoreDescription(String name,
                                                                 TestConfig config,
                                                                 GfxdTestConfig sconfig,
                                                                 int index) {
    ConfigHashtable tab = config.getParameters();

    HDFSStoreDescription hsd = new HDFSStoreDescription();
    hsd.setName(name);

    // batchSize
    {
      Long key = HDFSStorePrms.batchSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSEventQueueAttributesFactory.DEFAULT_BATCH_SIZE_MB;
      }
      hsd.setBatchSize(i);
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
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Float.valueOf(HDFSStore.DEFAULT_BLOCK_CACHE_SIZE).intValue();
      }
      hsd.setBlockCacheSize(i);
    }
    // clientConfigFile
    {
      Long key = HDFSStorePrms.clientConfigFile;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null) {
        hsd.setClientConfigFile(str);
      }
    }
    // diskStoreName (generates DiskStoreDescription)
    {
      Long key = HDFSStorePrms.diskStoreName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str != null && !str.equalsIgnoreCase(BasePrms.NONE)) {
        hsd.setDiskStoreDescription(getDiskStoreDescription(str, key, sconfig));
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
    // dispatcherThreads
    {
      Long key = HDFSStorePrms.dispatcherThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_HDFS_DISPATCHER_THREADS;
      }
      hsd.setDispatcherThreads(i);
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
    // majorCompact
    {
      Long key = HDFSStorePrms.majorCompact;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = HDFSCompactionConfig.DEFAULT_AUTO_MAJOR_COMPACTION;
      }
      hsd.setMajorCompact(bool);
    }
    // majorCompactionInterval
    {
      Long key = HDFSStorePrms.majorCompactionInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS;
      }
      hsd.setMajorCompactionInterval(i);
    }
    // majorCompactionThreads
    {
      Long key = HDFSStorePrms.majorCompactionThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAJOR_COMPACTION_MAX_THREADS;
      }
      hsd.setMajorCompactionThreads(i);
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
    // maxInputFileSize
    {
      Long key = HDFSStorePrms.maxInputFileSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAX_INPUT_FILE_SIZE_MB;
      }
      hsd.setMaxInputFileSize(i);
    }
    // maxQueueMemory
    {
      Long key = HDFSStorePrms.maxQueueMemory;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY;
      }
      hsd.setMaxQueueMemory(i);
    }
    // maxWriteOnlyFileSize
    {
      Long key = HDFSStorePrms.maxWriteOnlyFileSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSStore.DEFAULT_MAX_WRITE_ONLY_FILE_SIZE;
      }
      hsd.setMaxWriteOnlyFileSize(i);
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
    // minorCompact
    {
      Long key = HDFSStorePrms.minorCompact;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = HDFSCompactionConfig.DEFAULT_AUTO_COMPACTION;
      }
      hsd.setMinorCompact(bool);
    }
    // minorCompactionThreads
    {
      Long key = HDFSStorePrms.minorCompactionThreads;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_MAX_THREADS;
      }
      hsd.setMinorCompactionThreads(i);
    }
    // purgeInterval
    {
      Long key = HDFSStorePrms.purgeInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSCompactionConfig.DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS;
      }
      hsd.setPurgeInterval(i);
    }
    // queuePersistent
    {
      Long key = HDFSStorePrms.queuePersistent;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = GatewaySender.DEFAULT_PERSISTENCE_ENABLED;
      }
      hsd.setQueuePersistent(bool);
    }
    // writeOnlyFileRolloverInterval
    {
      Long key = HDFSStorePrms.writeOnlyFileRolloverInterval;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = HDFSStore.DEFAULT_WRITE_ONLY_FILE_ROLLOVER_INTERVAL;
      }
      hsd.setWriteOnlyFileRolloverInterval(i);
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
   * @throws hydra.HydraConfigException if the given string is not listed in {@link
   *         DiskStorePrms#names}.
   */
  private static DiskStoreDescription getDiskStoreDescription(String str, Long key, GfxdTestConfig config) {
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
   * @throws hydra.HydraConfigException if the given string is not listed in {@link
   *         hydra.HadoopPrms#names}.
   */
  private static HadoopDescription getHadoopDescription(String str, Long key, TestConfig config) {
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
// Custom serialization
//------------------------------------------------------------------------------

  private void readObject(java.io.ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    this.batchSize = (Integer)in.readObject();
    this.batchTimeInterval = (Integer)in.readObject();
    this.blockCacheSize = (Integer)in.readObject();
    this.clientConfigFile = (String)in.readObject();
    this.diskStoreDescription = (DiskStoreDescription)in.readObject();
    this.diskStoreName = (String)in.readObject();
    this.diskSynchronous = (Boolean)in.readObject();
    this.dispatcherThreads = (Integer)in.readObject();
    this.hadoopDescription = (HadoopDescription)in.readObject();
    this.hadoopName = (String)in.readObject();
    this.homeDir = (String)in.readObject();
    this.majorCompact = (Boolean)in.readObject();
    this.majorCompactionInterval = (Integer)in.readObject();
    this.majorCompactionThreads = (Integer)in.readObject();
    this.maxInputFileCount = (Integer)in.readObject();
    this.maxInputFileSize = (Integer)in.readObject();
    this.maxQueueMemory = (Integer)in.readObject();
    this.maxWriteOnlyFileSize = (Integer)in.readObject();
    this.minInputFileCount = (Integer)in.readObject();
    this.minorCompact = (Boolean)in.readObject();
    this.minorCompactionThreads = (Integer)in.readObject();
    this.name = (String)in.readObject();
    this.purgeInterval = (Integer)in.readObject();
    this.queuePersistent = (Boolean)in.readObject();
    this.writeOnlyFileRolloverInterval = (Integer)in.readObject();
  }

  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.writeObject(this.batchSize);
    out.writeObject(this.batchTimeInterval);
    out.writeObject(this.blockCacheSize);
    out.writeObject(this.clientConfigFile);
    out.writeObject(this.diskStoreDescription);
    out.writeObject(this.diskStoreName);
    out.writeObject(this.diskSynchronous);
    out.writeObject(this.dispatcherThreads);
    out.writeObject(this.hadoopDescription);
    out.writeObject(this.hadoopName);
    out.writeObject(this.homeDir);
    out.writeObject(this.majorCompact);
    out.writeObject(this.majorCompactionInterval);
    out.writeObject(this.majorCompactionThreads);
    out.writeObject(this.maxInputFileCount);
    out.writeObject(this.maxInputFileSize);
    out.writeObject(this.maxQueueMemory);
    out.writeObject(this.maxWriteOnlyFileSize);
    out.writeObject(this.minInputFileCount);
    out.writeObject(this.minorCompact);
    out.writeObject(this.minorCompactionThreads);
    out.writeObject(this.name);
    out.writeObject(this.purgeInterval);
    out.writeObject(this.queuePersistent);
    out.writeObject(this.writeOnlyFileRolloverInterval);
  }
}

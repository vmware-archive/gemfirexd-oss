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

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.LogWriterImpl;
import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.FileUtil;
import hydra.HostDescription;
import hydra.HydraConfigException;
import hydra.HydraInternalException;
import hydra.HydraRuntimeException;
import hydra.HydraVector;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.VmDescription;
import java.io.File;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Encodes information needed to describe and create a loner distributed system.
 */
public class LonerDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The logical name of this loner description */
  private String name;

  /** Remaining parameters, in alphabetical order */

  private Integer archiveDiskSpaceLimit; // GFE
  private Integer archiveFileSizeLimit; // GFE
  private List<String> clientNames; // HYDRA
  private Boolean enableTimeStatistics; // GFE
  private Integer logDiskSpaceLimit; // GFE
  private transient String logFile; // GFE
  private Integer logFileSizeLimit; // GFE
  private String logLevel; // GFE
  private transient String nameOfMember; // GFE
  private transient String resourceDir; // HYDRA path to sysDir
  private transient String statisticArchiveFile; // GFE
  private Integer statisticSampleRate; // GFE
  private Boolean statisticSamplingEnabled; // GFE
  private transient String sysDir; // HYDRA path to system.log/statArchive.gfs

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public LonerDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in alphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this loner description.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this loner description.
   */
  private void setName(String str) {
    this.name = str;
  }

  public Integer getArchiveDiskSpaceLimit() {
    return this.archiveDiskSpaceLimit;
  }
  private void setArchiveDiskSpaceLimit(Integer i) {
    this.archiveDiskSpaceLimit = i;
  }

  public Integer getArchiveFileSizeLimit() {
    return this.archiveFileSizeLimit;
  }
  private void setArchiveFileSizeLimit(Integer i) {
    this.archiveFileSizeLimit = i;
  }

  public List<String> getClientNames() {
    return this.clientNames;
  }
  private void setClientNames(List<String> list) {
    this.clientNames = list;
  }

  public Boolean getEnableTimeStatistics() {
    return this.enableTimeStatistics;
  }
  private void setEnableTimeStatistics(Boolean bool) {
    this.enableTimeStatistics = bool;
  }

  public Integer getLogDiskSpaceLimit() {
    return this.logDiskSpaceLimit;
  }
  private void setLogDiskSpaceLimit(Integer i) {
    this.logDiskSpaceLimit = i;
  }

  /**
   * Client-side transient.  Returns absolute path to log file.
   */
  private synchronized String getLogFile() {
    if (this.logFile == null) {
      this.logFile = this.getSysDir() + "/system.log";
    }
    return this.logFile;
  }

  public Integer getLogFileSizeLimit() {
    return this.logFileSizeLimit;
  }
  private void setLogFileSizeLimit(Integer i) {
    this.logFileSizeLimit = i;
  }

  public String getLogLevel() {
    return this.logLevel;
  }
  private void setLogLevel(String str) {
    this.logLevel = str;
  }

  /**
   * Client-side transient.  Returns the name of the member.
   *
   * This is really the "name" property, but that was taken by the field
   * for the logical description name.
   */
  private synchronized String getNameOfMember() {
    if (this.nameOfMember == null) {
      this.nameOfMember = RemoteTestModule.getCurrentThread() == null
                        ? this.name
                        : this.name + "_" + RemoteTestModule.getMyHost()
                                    + "_" + RemoteTestModule.getMyPid();
    }
    return this.nameOfMember;
  }

  /**
   * Client-side transient.  Returns absolute path to the system directories.
   */
  private synchronized String getResourceDir() {
    if (this.resourceDir == null) {
      HostDescription hd = getHostDescription();
      String dir = hd.getResourceDir();
      if (dir == null) {
        dir = hd.getUserDir();
      }
      this.resourceDir = dir;
    }
    return this.resourceDir;
  }

  /**
   * Client-side transient.  Returns absolute path to statistic archive file.
   */
  private synchronized String getStatisticArchiveFile() {
    if (this.statisticArchiveFile == null) {
      this.statisticArchiveFile = this.getSysDir() + "/statArchive.gfs";
    }
    return this.statisticArchiveFile;
  }

  public Integer getStatisticSampleRate() {
    return this.statisticSampleRate;
  }
  private void setStatisticSampleRate(Integer i) {
    this.statisticSampleRate = i;
  }

  public Boolean getStatisticSamplingEnabled() {
    return this.statisticSamplingEnabled;
  }
  private void setStatisticSamplingEnabled(Boolean bool) {
    this.statisticSamplingEnabled = bool;
  }

  /**
   * Client-side transient.  Returns absolute path to system directory.
   * Lazily creates and registers it.  This directory contains system logs
   * and statistics archives.
   */
  protected synchronized String getSysDir() {
    if (this.sysDir == null) {
      HostDescription hd = getHostDescription();
      String dir = this.getResourceDir() + "/"
                 + "vm_" + RemoteTestModule.getMyVmid()
                 + "_" + RemoteTestModule.getMyClientName()
                 + "_" + hd.getHostName()
                 + "_" + RemoteTestModule.getMyPid();
      FileUtil.mkdir(new File(dir));
      try {
        RemoteTestModule.Master.recordDir(hd, getName(), dir);
      } catch (RemoteException e) {
        String s = "Unable to access master to record directory: " + dir;
        throw new HydraRuntimeException(s, e);
      }
      this.sysDir = dir;
    }
    return this.sysDir;
  }

  /**
   * Returns the host description for this hydra client VM, after validating
   * that it is legal for it to use this description.
   */
  private HostDescription getHostDescription() {
    // make sure this is a hydra client
    String clientName = RemoteTestModule.getMyClientName();
    if (clientName == null) {
      String s = "This method can only be invoked from hydra client VMs";
      throw new HydraInternalException(s);
    }
    // make sure this client belongs to this description
    if (!this.clientNames.contains(clientName)) {
      String s = "Logical client " + clientName
               + " is not in the list of clients supported by "
               + BasePrms.nameForKey(LonerPrms.names) + "=" + getName();
      throw new HydraRuntimeException(s);
    }
    return TestConfig.getInstance().getClientDescription(clientName)
                                   .getVmDescription().getHostDescription();
  }

//------------------------------------------------------------------------------
// Properties
//------------------------------------------------------------------------------

  /**
   * Returns the distributed system properties for this loner description.
   */
  protected Properties getLonerProperties() {
    Properties p = new Properties();

    p.setProperty(DistributionConfig.NAME_NAME,
                  this.getNameOfMember());

    p.setProperty(DistributionConfig.ARCHIVE_DISK_SPACE_LIMIT_NAME,
                  getArchiveDiskSpaceLimit().toString());
    p.setProperty(DistributionConfig.ARCHIVE_FILE_SIZE_LIMIT_NAME,
                  getArchiveFileSizeLimit().toString());
    p.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME,
                  getEnableTimeStatistics().toString());
    p.setProperty(DistributionConfig.LOCATORS_NAME,
                  "");
    p.setProperty(DistributionConfig.LOG_DISK_SPACE_LIMIT_NAME,
                  getLogDiskSpaceLimit().toString());
    p.setProperty(DistributionConfig.LOG_FILE_NAME,
                  getLogFile());
    p.setProperty(DistributionConfig.LOG_FILE_SIZE_LIMIT_NAME,
                  getLogFileSizeLimit().toString());
    p.setProperty(DistributionConfig.LOG_LEVEL_NAME,
                  getLogLevel());
    p.setProperty(DistributionConfig.MCAST_PORT_NAME,
                  String.valueOf(0));
    p.setProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME,
                  getStatisticArchiveFile());
    p.setProperty(DistributionConfig.STATISTIC_SAMPLE_RATE_NAME,
                  getStatisticSampleRate().toString());
    p.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME,
                  getStatisticSamplingEnabled().toString());

    return p;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap<String,Object> toSortedMap() {
    SortedMap<String,Object> map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "archiveDiskSpaceLimit", getArchiveDiskSpaceLimit());
    map.put(header + "archiveFileSizeLimit", getArchiveFileSizeLimit());
    map.put(header + "clientNames", getClientNames());
    map.put(header + "enableTimeStatistics", getEnableTimeStatistics());
    map.put(header + "logDiskSpaceLimit", getLogDiskSpaceLimit());
    map.put(header + "logFile", "autogenerated: system.log in gemfire system directory");
    map.put(header + "logFileSizeLimit", getLogFileSizeLimit());
    map.put(header + "logLevel", getLogLevel());
    map.put(header + "statisticArchiveFile", "autogenerated: statArchive.gfs in gemfire system directory");
    map.put(header + "statisticSampleRate", getStatisticSampleRate());
    map.put(header + "statisticSamplingEnabled", getStatisticSamplingEnabled());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates loner descriptions from the test configuration parameters.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();
    List<String> usedClients = new ArrayList();

    // create a description for each loner name
    SortedMap<String,LonerDescription> lds = new TreeMap();
    Vector names = tab.vecAt(LonerPrms.names, new HydraVector());
    for (int i = 0; i < names.size(); i++) {
      String name = (String)names.elementAt(i);
      if (lds.containsKey(name)) {
        String s = BasePrms.nameForKey(LonerPrms.names)
                 + " contains duplicate entries: " + names;
        throw new HydraConfigException(s);
      }
      LonerDescription ld = createLonerDescription(name, config, i,
                                                   usedClients);
      lds.put(name, ld);
    }
    return lds;
  }

  /**
   * Creates the loner description using test configuration parameters
   * and product defaults.
   */
  private static LonerDescription createLonerDescription(
                 String name, TestConfig config, int index,
                 List<String> usedClients) {

    ConfigHashtable tab = config.getParameters();

    LonerDescription ld = new LonerDescription();
    ld.setName(name);

    // archiveDiskSpaceLimit
    {
      Long key = LonerPrms.archiveDiskSpaceLimit;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = DistributionConfig.DEFAULT_ARCHIVE_DISK_SPACE_LIMIT;
      }
      ld.setArchiveDiskSpaceLimit(i);
    }
    // archiveFileSizeLimit
    {
      Long key = LonerPrms.archiveFileSizeLimit;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = DistributionConfig.DEFAULT_ARCHIVE_FILE_SIZE_LIMIT;
      }
      ld.setArchiveFileSizeLimit(i);
    }
    // clientNames
    {
      Long key = LonerPrms.clientNames;
      Vector strs = tab.vecAtWild(key, index, null);
      if (strs == null) {
        String s = BasePrms.nameForKey(key)
                 + " is a required field and has no default value";
        throw new HydraConfigException(s);
      }
      List<String> cnames = getClientNames(strs, key, usedClients, tab);
      ld.setClientNames(cnames);

      // add product jar to class paths
      for (String cname : cnames) {
        VmDescription vmd = config.getClientDescription(cname)
                                  .getVmDescription();
        HostDescription hd = vmd.getHostDescription();
        List<String> classPaths = new ArrayList();
        String classPath = hd.getGemFireHome()
                         + hd.getFileSep() + ".."
                         + hd.getFileSep() + "product-gfxd"
                         + hd.getFileSep() + "lib"
                         + hd.getFileSep() + "gemfirexd.jar";
        classPaths.add(classPath);
        vmd.setGemFireXDClassPaths(classPaths);
      }
    }
    // enableTimeStatistics
    {
      Long key = LonerPrms.enableTimeStatistics;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.TRUE; // overrides product default
      }
      ld.setEnableTimeStatistics(bool);
    }
    // logDiskSpaceLimit
    {
      Long key = LonerPrms.logDiskSpaceLimit;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = DistributionConfig.DEFAULT_LOG_DISK_SPACE_LIMIT;
      }
      ld.setLogDiskSpaceLimit(i);
    }
    // defer logFile
    // logFileSizeLimit
    {
      Long key = LonerPrms.logFileSizeLimit;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = DistributionConfig.DEFAULT_LOG_FILE_SIZE_LIMIT;
      }
      ld.setLogFileSizeLimit(i);
    }
    // logLevel
    {
      Long key = LonerPrms.logLevel;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        str = LogWriterImpl.levelToString(DistributionConfig.DEFAULT_LOG_LEVEL);
      }
      ld.setLogLevel(str);
    }
    // defer name (of member)
    // defer resourceDir
    // defer statisticArchiveFile
    // statisticSampleRate
    {
      Long key = LonerPrms.statisticSampleRate;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = DistributionConfig.DEFAULT_STATISTIC_SAMPLE_RATE;
      }
      ld.setStatisticSampleRate(i);
    }
    // statisticSamplingEnabled
    {
      Long key = LonerPrms.statisticSamplingEnabled;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.TRUE; // overrides product default
      }
      ld.setStatisticSamplingEnabled(bool);
    }
    // defer sysDir
    return ld;
  }

//------------------------------------------------------------------------------
// clientNames configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the client names for the given string.
   */
  private static List<String> getClientNames(Vector strs, Long key,
                                             List<String> usedClients,
                                             ConfigHashtable tab) {
    // make sure there are no duplicates
    for (Iterator i = strs.iterator(); i.hasNext();) {
      String str = tab.getString(key, i.next());
      if (usedClients.contains(str)) {
        String s = BasePrms.nameForKey(key) + " contains duplicate: " + str;
        throw new HydraConfigException(s);
      }
      usedClients.add(str);
    }
    return new ArrayList(strs);
  }
}

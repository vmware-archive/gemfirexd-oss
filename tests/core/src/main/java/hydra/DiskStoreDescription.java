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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.internal.cache.DiskStoreFactoryImpl;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

/**
 * Encodes information needed to describe and create a disk store.
 */
public class DiskStoreDescription extends AbstractDescription
implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The logical name of this disk store description and actual name of the
   * disk store.
   */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Boolean allowForceCompaction;
  private Boolean autoCompact;
  private Integer compactionThreshold;
  private Map<String,List> diskDirBases;
  private Integer diskDirNum;
  private int[] diskDirSizes;
  private transient File[] diskDirs;
  private Long maxOplogSize;
  private Integer queueSize;
  private Long timeInterval;
  private Integer writeBufferSize;

//------------------------------------------------------------------------------
// Constructors
//------------------------------------------------------------------------------

  public DiskStoreDescription() {
  }

//------------------------------------------------------------------------------
// Accessors, in aphabetical order after name
//------------------------------------------------------------------------------

  /**
   * Returns the logical name of this disk store description and actual name
   * of the disk store.
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the logical name of this disk store description and actual name
   * of the disk store.
   */
  private void setName(String str) {
    this.name = str;
  }

  /**
   * Returns the allow force compaction.
   */
  private Boolean getAllowForceCompaction() {
    return this.allowForceCompaction;
  }

  /**
   * Sets the allow force compaction.
   */
  private void setAllowForceCompaction(Boolean bool) {
    this.allowForceCompaction = bool;
  }

  /**
   * Returns the auto compact.
   */
  private Boolean getAutoCompact() {
    return this.autoCompact;
  }

  /**
   * Sets the auto compact.
   */
  private void setAutoCompact(Boolean bool) {
    this.autoCompact = bool;
  }

  /**
   * Returns the compaction threshold.
   */
  private Integer getCompactionThreshold() {
    return this.compactionThreshold;
  }

  /**
   * Sets the compaction threshold.
   */
  private void setCompactionThreshold(Integer i) {
    this.compactionThreshold = i;
  }

  /**
   * Returns the number of disk directories.
   */
  private Integer getDiskDirNum() {
    return this.diskDirNum;
  }

  /**
   * Sets the number of disk directories.
   */
  private void setDiskDirNum(Integer i) {
    this.diskDirNum = i;
  }

  /*
   * Returns the disk directories.
   * <p>
   * Autogenerates directory names.  They are numbered consecutively from 1
   * to {@link DiskStorePrms#diskDirNum} and use the user-defined bases for the
   * local host, if specified, or the same path as the system directory for the
   * process cache, which is the default.
   * <p>
   * Example: /export/.../test-0328-112342/vm_3_client2_disk_1
   *          /export/.../test-0328-112342/vm_3_client2_disk_2
   *
   * @throws HydraRuntimeException if a directory cannot be created.
   */
  public synchronized File[] getDiskDirs() {
    if (this.diskDirs == null) {
      if (this.diskDirNum.intValue() == 0) {
        File[] dirs = new File[0];
        this.diskDirs = dirs;
      } else {
        GemFireDescription gfd =
          DistributedSystemHelper.getGemFireDescription();
        List<String> bases = this.getDiskDirBases(gfd);
        File[] dirs = new File[this.diskDirNum.intValue()];
        for (int i = 0; i < dirs.length; i++) {
          // do not add pid here since dirs must survive pid change
          String path = bases.get(i%bases.size());
          dirs[i] = new File(path + File.separator
                  + "vm_" + RemoteTestModule.getMyVmid()
                  + "_" + RemoteTestModule.getMyClientName()
                  + "_disk_" + (i+1));
          try {
            FileUtil.mkdir(dirs[i]);
            HostDescription hd = gfd.getHostDescription();
            String dir = dirs[i].toString();
            try {
              RemoteTestModule.Master.recordDir(hd, gfd.getName(), dir);
            } catch (RemoteException e) {
              String s = "Unable to access master to record directory: " + dir;
              throw new HydraRuntimeException(s, e);
            }
          } 
          catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          }
          catch (Error e) {
            String s = "Unable to create directory: " + dirs[i];
            throw new HydraRuntimeException(s);
          }
        }
        this.diskDirs = dirs;
      }
    }
    return this.diskDirs;
  }

  /**
   * Sets the disk directories.
   */
  private void setDiskDirs(File[] dirs) {
    this.diskDirs = dirs;
  }

  /**
   * Returns the disk directory bases.
   */
  private Map<String,List> getDiskDirBases() {
    return this.diskDirBases;
  }

  /**
   * Returns the disk directory bases for the given gemfire description.  First
   * look for user-defined bases, then default to the system directory path.
   */
  private List<String> getDiskDirBases(GemFireDescription gfd) {
    Map<String,List> diskDirBases = this.getDiskDirBases();
    List<String> basesForHost = null;
    if (diskDirBases != null) {
      String hostName = gfd.getHostDescription().getHostName();
      basesForHost = getDiskDirBases().get(hostName);
    }
    String sysdir = gfd.getSystemDirectoryStr();
    if (basesForHost == null || basesForHost.size() == 0) {
      String path = (new File(sysdir)).getParent();
      basesForHost = new ArrayList();
      basesForHost.add(path);
    } else {
      String testDir = (new File(sysdir)).getParentFile().getName();
      List<String> newBasesForHost = new ArrayList();
      for (String baseForHost : basesForHost) {
        String path = baseForHost + "/" + testDir;
        newBasesForHost.add(path);
      }
      basesForHost = newBasesForHost;
    }
    return basesForHost;
  }

  /**
   * Sets the disk directory bases.
   */
  private void setDiskDirBases(Map<String,List> bases) {
    this.diskDirBases = bases;
  }

  /**
   * Returns the disk directory sizes.
   */
  private int[] getDiskDirSizes() {
    return this.diskDirSizes;
  }

  /**
   * Sets the disk directory sizes.
   */
  private void setDiskDirSizes(int[] sizes) {
    this.diskDirSizes = sizes;
  }

  /**
   * Returns the max oplog size.
   */
  private Long getMaxOplogSize() {
    return this.maxOplogSize;
  }

  /**
   * Sets the max oplog size.
   */
  private void setMaxOplogSize(Long l) {
    this.maxOplogSize = l;
  }

  /**
   * Returns the queue size.
   */
  private Integer getQueueSize() {
    return this.queueSize;
  }

  /**
   * Sets the queue size.
   */
  private void setQueueSize(Integer i) {
    this.queueSize = i;
  }

  /**
   * Returns the time interval.
   */
  private Long getTimeInterval() {
    return this.timeInterval;
  }

  /**
   * Sets the time interval.
   */
  private void setTimeInterval(Long l) {
    this.timeInterval = l;
  }

  /**
   * Returns the write buffer size.
   */
  private Integer getWriteBufferSize() {
    return this.writeBufferSize;
  }

  /**
   * Sets the write buffer size.
   */
  private void setWriteBufferSize(Integer i) {
    this.writeBufferSize = i;
  }

//------------------------------------------------------------------------------
// Disk store configuration
//------------------------------------------------------------------------------

  /**
   * Configures the disk store factory using this disk store description.
   */
  public void configure(DiskStoreFactory f) {
    f.setAllowForceCompaction(this.getAllowForceCompaction().booleanValue());
    f.setAutoCompact(this.getAutoCompact().booleanValue());
    f.setCompactionThreshold(this.getCompactionThreshold().intValue());
    f.setDiskDirsAndSizes(this.getDiskDirs(), this.getDiskDirSizes());
    f.setMaxOplogSize(this.getMaxOplogSize().longValue());
    f.setQueueSize(this.getQueueSize().intValue());
    f.setTimeInterval(this.getTimeInterval().longValue());
    f.setWriteBufferSize(this.getWriteBufferSize().intValue());
  }

  /**
   * Returns the disk store as a string.  For use only by {@link DiskStoreHelper
   * #diskStoreToString(DiskStore)}.
   */
  protected static synchronized String diskStoreToString(DiskStore d) {
    return diskStoreToString(d.getName(), d);
  }

  /**
   * Returns the disk store as a string.  Serves calling methods for both
   * factory attributes and actual disk store.
   */
  private static synchronized String diskStoreToString(String diskStoreName,
                                                       DiskStore d) {
    StringBuffer buf = new StringBuffer();
    buf.append("\n  diskStoreName: " + diskStoreName);
    buf.append("\n  allowForceCompaction: " + d.getAllowForceCompaction());
    buf.append("\n  autoCompact: " + d.getAutoCompact());
    buf.append("\n  compactionThreshold: " + d.getCompactionThreshold());
    buf.append("\n  diskDirSizes: " + asList(d.getDiskDirSizes()));
    buf.append("\n  diskDirs: " + Arrays.asList(d.getDiskDirs()));
    buf.append("\n  maxOplogSize: " + d.getMaxOplogSize());
    buf.append("\n  queueSize: " + d.getQueueSize());
    buf.append("\n  timeInterval: " + d.getTimeInterval());
    buf.append("\n  writeBufferSize: " + d.getWriteBufferSize());
    return buf.toString();
  }

  /**
   * Returns the named disk store factory as a string.  For use only by {@link
   * DiskStoreHelper#diskStoreFactoryToString(String, DiskStoreFactory)}.
   */
  protected static synchronized String diskStoreFactoryToString(
                                String diskStoreName, DiskStoreFactory f) {
    DiskStore da = ((DiskStoreFactoryImpl)f).getDiskStoreAttributes();
    return diskStoreToString(diskStoreName, da);
  }

  /**
   * Returns whether the factory attributes are the same as the disk store.
   */
  protected static boolean equals(DiskStoreFactory f, DiskStore d) {
    DiskStore da = ((DiskStoreFactoryImpl)f).getDiskStoreAttributes();
    if (da.getAllowForceCompaction() != d.getAllowForceCompaction() ||
        da.getAutoCompact() != d.getAutoCompact() ||
        da.getCompactionThreshold() != d.getCompactionThreshold() ||
        da.getMaxOplogSize() != d.getMaxOplogSize() ||
        da.getQueueSize() != d.getQueueSize() ||
        da.getTimeInterval() != d.getTimeInterval() ||
        da.getWriteBufferSize() != d.getWriteBufferSize()) {
      return false;
    }
    if (da.getDiskDirSizes().length != d.getDiskDirSizes().length) {
      return false;
    }
    for (int i = 0; i < da.getDiskDirSizes().length; i++) {
      if (da.getDiskDirSizes()[i] != d.getDiskDirSizes()[i]) {
        return false;
      }
    }
    if (da.getDiskDirs().length != d.getDiskDirs().length) {
      return false;
    }
    for (int i = 0; i < da.getDiskDirs().length; i++) {
      if (!da.getDiskDirs()[i].equals(d.getDiskDirs()[i])) {
        return false;
      }
    }
    return true;
  }

//------------------------------------------------------------------------------
// Printing
//------------------------------------------------------------------------------

  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    String header = this.getClass().getName() + "." + this.getName() + ".";
    map.put(header + "allowForceCompaction", this.getAllowForceCompaction());
    map.put(header + "autoCompact", this.getAutoCompact());
    map.put(header + "compactionThreshold", this.getCompactionThreshold());
    map.put(header + "diskDirBases", this.getDiskDirBases());
    map.put(header + "diskDirNum", this.getDiskDirNum());
    map.put(header + "diskDirSizes", asList(this.getDiskDirSizes()));
    if (this.diskDirNum.intValue() == 0) {
      map.put(header + "diskDirs", "[]");
    } else {
      map.put(header + "diskDirs", "autogenerated: same path as system directory");
    }
    map.put(header + "maxOplogSize", this.getMaxOplogSize());
    map.put(header + "queueSize", this.getQueueSize());
    map.put(header + "timeInterval", this.getTimeInterval());
    map.put(header + "writeBufferSize", this.getWriteBufferSize());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates disk store descriptions from the disk store parameters in the
   * test configuration.
   */
  protected static void configure(TestConfig config) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each disk store name
    Vector names = tab.vecAt(DiskStorePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create disk store description from test configuration parameters
      DiskStoreDescription dsd = createDiskStoreDescription(name, config, i);

      // save configuration
      config.addDiskStoreDescription(dsd);
    }
  }

  /**
   * Creates the disk store description using test configuration parameters
   * and product defaults.
   */
  private static DiskStoreDescription createDiskStoreDescription(String name,
                                      TestConfig config, int index) {

    ConfigHashtable tab = config.getParameters();

    DiskStoreDescription dsd = new DiskStoreDescription();
    dsd.setName(name);

    // allowForceCompaction
    {
      Long key = DiskStorePrms.allowForceCompaction;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION);
      }
      dsd.setAllowForceCompaction(bool);
    }
    // autoCompact
    {
      Long key = DiskStorePrms.autoCompact;
      Boolean bool = tab.getBoolean(key, tab.getWild(key, index, null));
      if (bool == null) {
        bool = Boolean.valueOf(DiskStoreFactory.DEFAULT_AUTO_COMPACT);
      }
      dsd.setAutoCompact(bool);
    }
    // compactionThreshold
    {
      Long key = DiskStorePrms.compactionThreshold;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD);
      }
      dsd.setCompactionThreshold(i);
    }
    // diskDirNum
    {
      Long key = DiskStorePrms.diskDirNum;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(1);
      }
      dsd.setDiskDirNum(getDiskDirNum(i, key));
    }
    // defer diskDirs
    // diskDirBases and diskDirBaseMapFileName
    {
      Map<String,List> bases;
      Long key = DiskStorePrms.diskDirBaseMapFileName;
      String str = tab.getString(key, tab.getWild(key, index, null));
      if (str == null) {
        key = DiskStorePrms.diskDirBases;
        bases = getDiskDirBases(key, tab.vecAtWild(key, index, null));
      } else {
        bases = getDiskDirBases(key, str);
      }
      dsd.setDiskDirBases(bases);
    }
    // diskDirSizes (depends on diskDirNum)
    {
      Long key = DiskStorePrms.diskDirSizes;
      int[] sizes = getDiskDirSizes(key, tab.vecAtWild(key, index, null), tab,
                                    dsd.getDiskDirNum());
      dsd.setDiskDirSizes(sizes);
    }
    // maxOplogSize
    {
      Long key = DiskStorePrms.maxOplogSize;
      Long l = tab.getLong(key, tab.getWild(key, index, null));
      if (l == null) {
        l = Long.valueOf(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE);
      }
      dsd.setMaxOplogSize(l);
    }
    // queueSize
    {
      Long key = DiskStorePrms.queueSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(DiskStoreFactory.DEFAULT_QUEUE_SIZE);
      }
      dsd.setQueueSize(i);
    }
    // timeInterval
    {
      Long key = DiskStorePrms.timeInterval;
      Long l = tab.getLong(key, tab.getWild(key, index, null));
      if (l == null) {
        l = Long.valueOf(DiskStoreFactory.DEFAULT_TIME_INTERVAL);
      }
      dsd.setTimeInterval(l);
    }
    // writeBufferSize
    {
      Long key = DiskStorePrms.writeBufferSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i == null) {
        i = Integer.valueOf(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE);
      }
      dsd.setWriteBufferSize(i);
    }
    return dsd;
  }

//------------------------------------------------------------------------------
// Disk store configuration support
//------------------------------------------------------------------------------

  /**
   * Returns the number of disk directories from the given integer.
   * @throws HydraConfigException if the value is negative.
   */
  private static Integer getDiskDirNum(Integer val, Long key) {
    return val == null ? Integer.valueOf(1)
                       : getNonnegativeIntegerFor(val, key);
  }

  /**
   * Returns the mapping of hosts to disk directory bases from the given file,
   * by reading the file and calling {@link getDiskDirBases(Long, Vector)}.
   * @throws HydraConfigException if the file is not found.
   */
  private static Map<String,List> getDiskDirBases(Long key, String str) {
    try {
      List<String> tokens = FileUtil.getTextAsTokens(str);
      return getDiskDirBases(key, new Vector(tokens));
    } catch (IOException e) {
      String s = "Problem reading " + BasePrms.nameForKey(key) + ": " + str;
      throw new HydraConfigException(s, e);
    }
  }

  /**
   * Returns the mapping of hosts to disk directory bases from the given vector,
   * which is expected to provide a list of hosts where each host is followed
   * by the bases for that host, or null if no bases are given.
   * @throws HydraConfigException if the value is malformed or a field contains
   *         an illegal value or type.
   */
  private static Map<String,List> getDiskDirBases(Long key,
                                                  Vector<String> val) {
    if (val == null || val.size() == 0) {
      return null;
    } else {
      SortedMap<String,List> bases = new TreeMap();
      String currentHost = null;
      List<String> currentBases = null;
      for (String token : val) {
        if (token.indexOf("/") == -1 && token.indexOf("\\") == -1) {
          currentHost = token;
          currentBases = new ArrayList();
          bases.put(currentHost, currentBases);
        } else {
          currentBases = bases.get(currentHost);
          if (currentBases == null) {
            String s = BasePrms.nameForKey(key) + " missing host for: " + token;
            throw new HydraConfigException(s);
          } else {
            currentBases.add(token);
          }
        }
      }
      return bases;
    }
  }

  /**
   * Returns the given number of disk directory sizes from the given vector,
   * which is expected to hold a list of longs, or null if there are no sizes
   * given.
   * @throws HydraConfigException if the value is malformed or a field contains
   *         an illegal value or type.
   */
  private static int[] getDiskDirSizes(Long key, Vector val,
          ConfigHashtable tab, Integer diskDirNum) {
    int[] sizes = new int[diskDirNum.intValue()];
    for (int i = 0; i < sizes.length; i++) {
      if (val == null) {
        sizes[i] = DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;
      } else {
        Integer size = tab.getInteger(key,
                                      ConfigHashtable.getWild(val, i, null));
        if (size == null) {
          sizes[i] = DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;
        } else {
          sizes[i] = size.intValue();
        }
      }
    }
    return sizes;
  }
}

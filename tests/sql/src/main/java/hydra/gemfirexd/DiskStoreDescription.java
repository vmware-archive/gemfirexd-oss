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

import com.gemstone.gemfire.cache.DiskStoreFactory;

import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.ConfigHashtable;
import hydra.HydraConfigException;
import hydra.HydraVector;
import hydra.TestConfig;

import java.io.Serializable;
import java.util.ArrayList;
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

  private static final String COMMA = ",";
  private static final String SPACE = " ";
  private static final String TIC = "'";

  /**
   * The logical name of this disk store description and actual name of the
   * disk store.
   */
  private String name;

  /** Remaining parameters, in alphabetical order */
  private Boolean allowForceCompaction;
  private Boolean autoCompact;
  private Integer compactionThreshold;
  private List<String> dirNames;
  private Integer maxDirSize;
  private Long maxLogSize;
  private Integer queueSize;
  private Long timeInterval;
  private Integer writeBufferSize;

  private transient String DDL;

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

  private void setDirNames(List<String> l) {
    this.dirNames = l;
  }

  private List<String> getDirNames() {
    return this.dirNames;
  }

  private void setMaxDirSize(Integer i) {
    this.maxDirSize = i;
  }

  private Integer getMaxDirSize() {
    return this.maxDirSize;
  }

  /**
   * Returns the max oplog size.
   */
  private Long getMaxLogSize() {
    return this.maxLogSize;
  }

  /**
   * Sets the max oplog size.
   */
  private void setMaxLogSize(Long l) {
    this.maxLogSize = l;
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

  /** Returns DDL for DISK STORE based on this description. */
  protected synchronized String getDDL() {
    if (DDL == null) {
      StringBuffer buf = new StringBuffer();
      buf.append("CREATE DISKSTORE ").append(this.getName())
         .append(SPACE).append("ALLOWFORCECOMPACTION").append(SPACE).append(this.getAllowForceCompaction())
         .append(SPACE).append("AUTOCOMPACT").append(SPACE).append(this.getAutoCompact())
         .append(SPACE).append("COMPACTIONTHRESHOLD").append(SPACE).append(this.getCompactionThreshold())
         .append(SPACE).append("MAXLOGSIZE").append(SPACE).append(this.getMaxLogSize())
         .append(SPACE).append("QUEUESIZE").append(SPACE).append(this.getQueueSize())
         .append(SPACE).append("TIMEINTERVAL").append(SPACE).append(this.getTimeInterval())
         .append(SPACE).append("WRITEBUFFERSIZE").append(SPACE).append(this.getWriteBufferSize())
      ;

      List<String> dirs = getDirNames();
      Integer size = getMaxDirSize();
      buf.append(SPACE).append("(");
      for (int i = 0; i < dirs.size(); i++) {
        if (i > 0) buf.append(COMMA).append(SPACE);
        buf.append(TIC).append(dirs.get(i)).append(TIC);
        if (size != null) {
          buf.append(SPACE).append(size);
        }
      }
      buf.append(")");
      DDL = buf.toString();
    }
    return DDL;
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
    map.put(header + "dirNames", this.getDirNames());
    if (this.getMaxDirSize() == null) {
      map.put(header + "maxDirSize", "unlimited");
    } else {
      map.put(header + "maxDirSize", this.getMaxDirSize());
    }
    map.put(header + "maxLogSize", this.getMaxLogSize());
    map.put(header + "queueSize", this.getQueueSize());
    map.put(header + "timeInterval", this.getTimeInterval());
    map.put(header + "writeBufferSize", this.getWriteBufferSize());
    return map;
  }

//------------------------------------------------------------------------------
// Configuration
//------------------------------------------------------------------------------

  /**
   * Creates disk store descriptions from the disk store parameters
   * in the test configuration.
   */
  protected static Map configure(TestConfig config, GfxdTestConfig sconfig) {

    ConfigHashtable tab = config.getParameters();

    // create a description for each disk store name
    SortedMap<String,DiskStoreDescription> dsds = new TreeMap();
    Vector names = tab.vecAt(DiskStorePrms.names, new HydraVector());

    for (int i = 0; i < names.size(); i++) {

      String name = (String)names.elementAt(i);

      // create disk store description from test configuration parameters
      DiskStoreDescription dsd = createDiskStoreDescription(name, config, sconfig, i);

      // save configuration
      dsds.put(name, dsd);
    }
    return dsds;
  }

  /** Creates the disk store description using test configuration parameters and product defaults. */
  private static DiskStoreDescription createDiskStoreDescription(String name,
                                                                 TestConfig config,
                                                                 GfxdTestConfig sconfig,
                                                                 int index) {
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
    // dirNames
    {
      Long key = DiskStorePrms.dirNames;
      List<String> l = getDirNames(key, tab.vecAtWild(key, index, null), tab);
      dsd.setDirNames(l);
    }
    // maxDirSize
    {
      Long key = DiskStorePrms.maxDirSize;
      Integer i = tab.getInteger(key, tab.getWild(key, index, null));
      if (i != null) {
        dsd.setMaxDirSize(i);
      }
    }
    // maxLogSize
    {
      Long key = DiskStorePrms.maxLogSize;
      Long l = tab.getLong(key, tab.getWild(key, index, null));
      if (l == null) {
        l = Long.valueOf(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE);
      }
      dsd.setMaxLogSize(l);
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
   * Returns the directory names from the given vector.
   * @throws HydraConfigException if there are no valid names given.
   */
  private static List<String> getDirNames(Long key, Vector val,
                                          ConfigHashtable tab) {
    List<String> dirs = new ArrayList();
    if (val != null) {
      for (int i = 0; i < val.size(); i++) {
        String dir = tab.getString(key, ConfigHashtable.getWild(val, i, null));
        if (dir != null) {
          dirs.add(dir);
        }
      }
    }
    if (dirs.size() == 0) {
      dirs.add("data");
    }
    return dirs;
  }
}

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
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

import com.pivotal.gemfirexd.internal.impl.sql.compile.CreateHDFSStoreNode;

import hydra.AbstractDescription;
import hydra.EnvHelper;
import hydra.HadoopDescription;

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
  private Integer blockCacheSize;
  private String clientConfigFile;
  private DiskStoreDescription diskStoreDescription; // from diskStoreName
  private String diskStoreName;
  private Boolean diskSynchronous;
  private HadoopDescription hadoopDescription; // from hadoopName
  private String hadoopName;
  private String homeDir;
  private Boolean majorCompact;
  private Integer majorCompactionThreads;
  private Integer maxInputFileCount;
  private Integer maxInputFileSize;
  private Integer maxQueueMemory;
  private Integer maxWriteOnlyFileSize;
  private Integer minInputFileCount;
  private Boolean minorCompact;
  private Integer minorCompactionThreads;
  private Boolean queuePersistent;

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

  public Integer getBatchSize() {
    return this.batchSize;
  }

  public Integer getBlockCacheSize() {
    return this.blockCacheSize;
  }

  public String getClientConfigFile() {
    return this.clientConfigFile;
  }

  public DiskStoreDescription getDiskStoreDescription() {
    return this.diskStoreDescription;
  }

  /**
   * Returns the disk store description name.
   */
  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public Boolean getDiskSynchronous() {
    return this.diskSynchronous;
  }

  public HadoopDescription getHadoopDescription() {
    return this.hadoopDescription;
  }

  /**
   * Returns the Hadoop cluster name.
   */
  public String getHadoopName() {
    return this.hadoopName;
  }

  public String getHomeDir() {
    return this.homeDir;
  }

  public Boolean getMajorCompact() {
    return this.majorCompact;
  }

  public Integer getMajorCompactionThreads() {
    return this.majorCompactionThreads;
  }

  public Integer getMaxInputFileCount() {
    return this.maxInputFileCount;
  }

  public Integer getMaxInputFileSize() {
    return this.maxInputFileSize;
  }

  public Integer getMaxQueueMemory() {
    return this.maxQueueMemory;
  }

  public Integer getMaxWriteOnlyFileSize() {
    return this.maxWriteOnlyFileSize;
  }

  public Integer getMinInputFileCount() {
    return this.minInputFileCount;
  }

  public Boolean getMinorCompact() {
    return this.minorCompact;
  }

  public Integer getMinorCompactionThreads() {
    return this.minorCompactionThreads;
  }

  public Boolean getQueuePersistent() {
    return this.queuePersistent;
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
         .append(SPACE).append(CreateHDFSStoreNode.BLOCKCACHESIZE).append(SPACE).append(this.getBlockCacheSize())
         .append(SPACE).append(CreateHDFSStoreNode.DISKSTORENAME).append(SPACE).append(this.getDiskStoreDescription().getName())
         .append(SPACE).append(CreateHDFSStoreNode.DISKSYNCHRONOUS).append(SPACE).append(this.getDiskSynchronous())
         .append(SPACE).append(CreateHDFSStoreNode.MAJORCOMPACT).append(SPACE).append(this.getMajorCompact())
         .append(SPACE).append(CreateHDFSStoreNode.MAJORCOMPACTIONTHREADS).append(SPACE).append(this.getMajorCompactionThreads())
         .append(SPACE).append(CreateHDFSStoreNode.MAXINPUTFILECOUNT).append(SPACE).append(this.getMaxInputFileCount())
         .append(SPACE).append(CreateHDFSStoreNode.MAXINPUTFILESIZE).append(SPACE).append(this.getMaxInputFileSize())
         .append(SPACE).append(CreateHDFSStoreNode.MAXQUEUEMEMORY).append(SPACE).append(this.getMaxQueueMemory())
         .append(SPACE).append(CreateHDFSStoreNode.MAXWRITEONLYFILESIZE).append(SPACE).append(this.getMaxWriteOnlyFileSize())
         .append(SPACE).append(CreateHDFSStoreNode.MINORCOMPACT).append(SPACE).append(this.getMinorCompact())
         .append(SPACE).append(CreateHDFSStoreNode.MINORCOMPACTIONTHREADS).append(SPACE).append(this.getMinorCompactionThreads())
         .append(SPACE).append(CreateHDFSStoreNode.MININPUTFILECOUNT).append(SPACE).append(this.getMinInputFileCount())
         .append(SPACE).append(CreateHDFSStoreNode.QUEUEPERSISTENT).append(SPACE).append(this.getQueuePersistent())
         ;
      ;
      String ccf = this.getClientConfigFile();
      if (ccf != null && ccf.length() > 0) {
        ccf = EnvHelper.expandPath(this.getClientConfigFile());
        buf.append(SPACE).append(CreateHDFSStoreNode.CLIENTCONFIGFILE).append(SPACE).append(TIC).append(ccf).append(TIC);
      }
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
    map.put(header + "batchSize", this.getBatchSize());
    map.put(header + "blockCacheSize", this.getBlockCacheSize());
    map.put(header + "clientConfigFile", this.getClientConfigFile());
    map.put(header + "diskStoreName", this.getDiskStoreName());
    map.put(header + "diskSynchronous", this.getDiskSynchronous());
    map.put(header + "hadoopName", this.getHadoopName());
    map.put(header + "homeDir", this.getHomeDir());
    map.put(header + "majorCompact", this.getMajorCompact());
    map.put(header + "majorCompactionThreads", this.getMajorCompactionThreads());
    map.put(header + "maxInputFileCount", this.getMaxInputFileCount());
    map.put(header + "maxInputFileSize", this.getMaxInputFileSize());
    map.put(header + "maxQueueMemory", this.getMaxQueueMemory());
    map.put(header + "maxWriteOnlyFileSize", this.getMaxWriteOnlyFileSize());
    map.put(header + "minInputFileCount", this.getMinInputFileCount());
    map.put(header + "minorCompact", this.getMinorCompact());
    map.put(header + "minorCompactionThreads", this.getMinorCompactionThreads());
    map.put(header + "queuePersistent", this.getQueuePersistent());
    return map;
  }

//------------------------------------------------------------------------------
// Custom serialization
//------------------------------------------------------------------------------

  private void readObject(java.io.ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    this.batchSize = (Integer)in.readObject();
    /* this.batchTimeInterval = */ in.readObject();
    this.blockCacheSize = (Integer)in.readObject();
    this.clientConfigFile = (String)in.readObject();
    this.diskStoreDescription = (DiskStoreDescription)in.readObject();
    this.diskStoreName = (String)in.readObject();
    this.diskSynchronous = (Boolean)in.readObject();
    /* this.dispatcherThreads = */ in.readObject();
    this.hadoopDescription = (HadoopDescription)in.readObject();
    this.hadoopName = (String)in.readObject();
    this.homeDir = (String)in.readObject();
    this.majorCompact = (Boolean)in.readObject();
    /* this.majorCompactionInterval = */ in.readObject();
    this.majorCompactionThreads = (Integer)in.readObject();
    this.maxInputFileCount = (Integer)in.readObject();
    this.maxInputFileSize = (Integer)in.readObject();
    this.maxQueueMemory = (Integer)in.readObject();
    this.maxWriteOnlyFileSize = (Integer)in.readObject();
    this.minInputFileCount = (Integer)in.readObject();
    this.minorCompact = (Boolean)in.readObject();
    this.minorCompactionThreads = (Integer)in.readObject();
    this.name = (String)in.readObject();
    /* this.purgeInterval = */ in.readObject();
    this.queuePersistent = (Boolean)in.readObject();
    /* this.writeOnlyFileRolloverInterval = */ in.readObject();
  }
}

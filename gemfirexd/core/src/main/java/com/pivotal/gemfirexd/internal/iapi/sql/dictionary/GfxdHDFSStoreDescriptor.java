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
package com.pivotal.gemfirexd.internal.iapi.sql.dictionary;

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;

/**
 * 
 * @author jianxiachen
 *
 */

public class GfxdHDFSStoreDescriptor extends TupleDescriptor
{

  private final String hdfsStoreName;

  private final String nameNode;

  private final String homeDir;

  private final int maxQueueMemory;

  private final int batchSize;

  private final int batchInterval;

  private final boolean isPersistenceEnabled;

  private final String diskStoreName;

  private final boolean diskSynchronous;

  private final boolean autoCompact;

  private final boolean autoMajorCompact;

  private final int maxInputFileSize;

  private final int minInputFileCount;

  private final int maxInputFileCount;

  private final int maxConcurrency;

  private final int majorCompactionInterval;

  private final int majorCompactionConcurrency;

  private float blockCacheSize;
  
  private String hdfsClientConfigFile;
  
  private int maxFileSizeWriteOnly;
  
  private int fileRolloverInterval;
  
  private int purgeInterval;
  
  private int dispatcherThreads;
  
  private final UUID id;

  public GfxdHDFSStoreDescriptor(DataDictionary dd, UUID id, HDFSStore hs) {
    super(dd);
    this.id = id;
    this.hdfsStoreName = hs.getName();
    this.nameNode = hs.getNameNodeURL();
    this.homeDir = hs.getHomeDir();
    this.maxQueueMemory = hs.getHDFSEventQueueAttributes().getMaximumQueueMemory();
    this.batchSize = hs.getHDFSEventQueueAttributes().getBatchSizeMB();
    this.batchInterval = hs.getHDFSEventQueueAttributes().getBatchTimeInterval();
    this.isPersistenceEnabled = hs.getHDFSEventQueueAttributes().isPersistent();
    this.diskSynchronous = hs.getHDFSEventQueueAttributes().isDiskSynchronous();
    this.diskStoreName = hs.getHDFSEventQueueAttributes().getDiskStoreName();
    this.autoCompact = hs.getHDFSCompactionConfig().getAutoCompaction();
    this.autoMajorCompact = hs.getHDFSCompactionConfig().getAutoMajorCompaction();
    this.maxInputFileSize = hs.getHDFSCompactionConfig().getMaxInputFileSizeMB();
    this.minInputFileCount = hs.getHDFSCompactionConfig().getMinInputFileCount();
    this.maxInputFileCount = hs.getHDFSCompactionConfig().getMaxInputFileCount();
    this.maxConcurrency = hs.getHDFSCompactionConfig().getMaxThreads();
    this.majorCompactionInterval = hs.getHDFSCompactionConfig().getMajorCompactionIntervalMins();
    this.majorCompactionConcurrency = hs.getHDFSCompactionConfig().getMajorCompactionMaxThreads();
    this.hdfsClientConfigFile = hs.getHDFSClientConfigFile();
    this.blockCacheSize = hs.getBlockCacheSize();
    this.maxFileSizeWriteOnly = hs.getMaxFileSize();
    this.fileRolloverInterval = hs.getFileRolloverInterval();
    this.purgeInterval = hs.getHDFSCompactionConfig().getOldFilesCleanupIntervalMins();
    this.dispatcherThreads = hs.getHDFSEventQueueAttributes().getDispatcherThreads();
  }
  
  
  public GfxdHDFSStoreDescriptor(DataDictionary dd, UUID id, String hdfsStoreName,
      String nameNode, String homeDir, int maxQueueMemory, int batchSize, int batchInterval,
      boolean isPersistent, boolean isDiskSync, String diskStore, boolean autoCompact, boolean autoMajorCompact,
      int maxInputFileSize, int minInputFileCount, int maxInputFileCount, int maxConcurrency,
      int majorCompactionInterval, int majorCompactionConcurrency, String hdfsClientConfigFile, float blockCacheSize, 
      int maxFileSizeWriteOnly, int fileRolloverInterval, int purgeInterval, int dispatcherThreads) {
    super(dd);
    this.id = id;
    this.hdfsStoreName = hdfsStoreName;
    this.nameNode = nameNode;
    this.homeDir = homeDir;
    this.maxQueueMemory = maxQueueMemory;
    this.batchSize = batchSize;
    this.batchInterval = batchInterval;
    this.isPersistenceEnabled = isPersistent;
    this.diskSynchronous = isDiskSync;
    this.diskStoreName = diskStore;
    this.autoCompact = autoCompact;
    this.autoMajorCompact = autoMajorCompact;
    this.maxInputFileSize = maxInputFileSize;
    this.minInputFileCount = minInputFileCount;
    this.maxInputFileCount = maxInputFileCount;
    this.maxConcurrency = maxConcurrency;
    this.majorCompactionInterval = majorCompactionInterval;
    this.majorCompactionConcurrency = majorCompactionConcurrency;
    this.hdfsClientConfigFile = hdfsClientConfigFile;
    this.blockCacheSize = blockCacheSize;
    this.maxFileSizeWriteOnly = maxFileSizeWriteOnly;
    this.fileRolloverInterval = fileRolloverInterval;
    this.purgeInterval = purgeInterval;
    this.dispatcherThreads = dispatcherThreads;
  }

  public UUID getUUID()
  {
    return this.id;
  }

  /*----- getter functions for rowfactory ------*/

  public String getHDFSStoreName()
  {
    return this.hdfsStoreName;
  }
  
  public String getNameNode()
  {
    return this.nameNode;
  }
  
  public String getHomeDir()
  {
    return this.homeDir;
  }
  
  public int getMaxQueueMemory()
  {
    return this.maxQueueMemory;
  }
  
  public int getBatchSize() {
    return this.batchSize;
  }


  public int getBatchInterval() {
    return this.batchInterval;
  }


  public boolean isPersistenceEnabled() {
    return this.isPersistenceEnabled;
  }


  public String getDiskStoreName() {
    return this.diskStoreName;
  }


  public boolean isDiskSynchronous() {
    return this.diskSynchronous;
  }


  public boolean isAutoCompact() {
    return this.autoCompact;
  }


  public boolean isAutoMajorCompact() {
    return this.autoMajorCompact;
  }


  public int getMaxInputFileSize() {
    return this.maxInputFileSize;
  }


  public int getMinInputFileCount() {
    return this.minInputFileCount;
  }


  public int getMaxInputFileCount() {
    return this.maxInputFileCount;
  }


  public int getMaxConcurrency() {
    return this.maxConcurrency;
  }


  public int getMajorCompactionInterval() {
    return this.majorCompactionInterval;
  }


  public int getMajorCompactionConcurrency() {
    return this.majorCompactionConcurrency;
  }

  public String getHDFSClientConfigFile()
  {
    return this.hdfsClientConfigFile;
  }
  
  public float getBlockCacheSize()
  {
    return this.blockCacheSize;
  }

  public String getDescriptorType() 
  {
          return "HDFSStore";
  }
  
  public int getMaxFileSizeWriteOnly()
  {
    return this.maxFileSizeWriteOnly;
  }
  
  public int getFileRolloverInterval()
  {
    return this.fileRolloverInterval;
  }
  
  public int getPurgeInterval()
  {
    return this.purgeInterval;
  }
  
  public int getDispatcherThreads()
  {
  	return this.dispatcherThreads;  	
  }
  
  /** @see TupleDescriptor#getDescriptorName */
  public String getDescriptorName() { 
    return this.hdfsStoreName; 
  }
  
  public String toString()
  {
    return "HDFS Store: name = " + hdfsStoreName + ", nameNode = " + nameNode
        + ", homeDir = " + homeDir;
  }

}

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


import com.gemstone.gemfire.cache.DiskStore;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;

/**
 * 
 * @author ashahid
 * 
 */
public class GfxdDiskStoreDescriptor extends TupleDescriptor
{

  final private String diskStoreName;

  final private long maxLogSize;

  private final String autoCompact;

  private final String allowForceCompaction;

  private final int compactionThreshold;

  private final long timeInterval;

  private final int writeBufferSize;

  final private int queueSize;

  private final String dirAndSize;

  private final UUID id;

  public GfxdDiskStoreDescriptor(DataDictionary dd, UUID id,DiskStore ds, 
      String dirAndSize) {
    super(dd);
    this.id = id;
    this.diskStoreName = ds.getName();
    this.maxLogSize = ds.getMaxOplogSize();
    this.autoCompact = Boolean.toString(ds.getAutoCompact());
    this.allowForceCompaction = Boolean.toString(ds.getAllowForceCompaction());
    this.compactionThreshold = ds.getCompactionThreshold();
    this.timeInterval = ds.getTimeInterval();
    this.writeBufferSize = ds.getWriteBufferSize();
    this.queueSize = ds.getQueueSize();
    this.dirAndSize = dirAndSize;
  }
  
  
  public GfxdDiskStoreDescriptor(DataDictionary dd, UUID id, String diskStoreName,
      long maxLogSize, String autoCompact, String allowForceCompaction,
      int compactionThreshold, long timeInterval, int writeBufferSize,
      int queueSize, String dirAndSize) {
    super(dd);
    this.id = id;
    this.diskStoreName = diskStoreName;
    this.maxLogSize = maxLogSize;
    this.autoCompact = autoCompact;
    this.allowForceCompaction = allowForceCompaction;
    this.compactionThreshold = compactionThreshold;
    this.timeInterval = timeInterval;
    this.writeBufferSize = writeBufferSize;
    this.queueSize = queueSize;
    this.dirAndSize = dirAndSize;
  }

  public UUID getUUID()
  {
    return this.id;
  }

  /*----- getter functions for rowfactory ------*/

  public String getDiskStoreName()
  {
    return this.diskStoreName;
  }

  public long getMaxLogSize()
  {
    return this.maxLogSize;
  }

  public String getAutocompact()
  {
    return this.autoCompact;
  }

  public String getAllowForceCompaction()
  {
    return this.allowForceCompaction;
  }

  public int getCompactionThreshold()
  {
    return this.compactionThreshold;
  }

  public long getTimeInterval()
  {
    return this.timeInterval;
  }

  public int getWriteBufferSize()
  {
    return this.writeBufferSize;
  }

  public int getQueueSize()
  {
    return this.queueSize;
  }

  public String getDirPathAndSize()
  {
    return this.dirAndSize;
  }
  
  public String getDescriptorType() 
  {
          return "DiskStore";
  }
  
  /** @see TupleDescriptor#getDescriptorName */
  public String getDescriptorName() { 
    return this.diskStoreName; 
  }
  
  public String toString()
  {
    return "DiskStore: name=" + diskStoreName + ",maxLogSize=" + maxLogSize
        + ",autoCompact=" + autoCompact + ",allowForceCompaction="
        + allowForceCompaction + ",compactionThreshold=" + compactionThreshold
        + ",timeInterval=" + timeInterval + ",writeBufferSize="
        + writeBufferSize + ",queueSize=" + queueSize + ",dirPathAndSize="
        + this.dirAndSize;
  }

}

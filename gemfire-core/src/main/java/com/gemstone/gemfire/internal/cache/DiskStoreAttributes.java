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
/**
 *
 */
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.Serializable;
import java.util.UUID;

import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.internal.InsufficientDiskSpaceException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Creates an attribute object for DiskStore.
 * </p>
 * @author Gester
 * @since prPersistSprint2
 */
public class DiskStoreAttributes implements Serializable, DiskStore {
  private static final long serialVersionUID = 1L;
  
  public boolean allowForceCompaction;
  public boolean autoCompact;

  public int compactionThreshold;
  public int queueSize;
  public int writeBufferSize;

  private long maxOplogSizeInBytes;
  private boolean maxOplogSizeSet;
  public long timeInterval;

  public int[] diskDirSizes;

  public File[] diskDirs;

  public String name;

  public boolean syncWrites;
  
  public DiskStoreAttributes() {
    // set all to defaults
    this.autoCompact = DiskStoreFactory.DEFAULT_AUTO_COMPACT;
    this.compactionThreshold = DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD;
    this.allowForceCompaction = DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION;
    this.maxOplogSizeInBytes = DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE * (1024*1024);
    this.timeInterval = DiskStoreFactory.DEFAULT_TIME_INTERVAL;
    this.writeBufferSize = DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE;
    this.queueSize = DiskStoreFactory.DEFAULT_QUEUE_SIZE;
    this.diskDirs = DiskStoreFactory.DEFAULT_DISK_DIRS;
    this.diskDirSizes = DiskStoreFactory.DEFAULT_DISK_DIR_SIZES;
    this.syncWrites = DiskStoreFactory.DEFAULT_SYNC_WRITES;
  }

  public UUID getDiskStoreUUID() {
    throw new UnsupportedOperationException("Not Implemented!");
  }

  /* (non-Javadoc)
  * @see com.gemstone.gemfire.cache.DiskStore#getAllowForceCompaction()
  */
  public boolean getAllowForceCompaction() {
    return this.allowForceCompaction;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getAutoCompact()
   */
  public boolean getAutoCompact() {
    return this.autoCompact;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getCompactionThreshold()
   */
  public int getCompactionThreshold() {
    return this.compactionThreshold;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getDiskDirSizes()
   */
  public int[] getDiskDirSizes() {
    int[] result = new int[this.diskDirSizes.length];
    System.arraycopy(this.diskDirSizes, 0, result, 0, this.diskDirSizes.length);
    return result;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getDiskDirs()
   */
  public File[] getDiskDirs() {
    File[] result = new File[this.diskDirs.length];
    System.arraycopy(this.diskDirs, 0, result, 0, this.diskDirs.length);
    return result;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getMaxOplogSize()
   */
  public long getMaxOplogSize() {
    // TODO Auto-generated method stub
    return this.maxOplogSizeInBytes / (1024 * 1024);
  }

  /**
   * Used by unit tests
   */
  public long getMaxOplogSizeInBytes() {
    return this.maxOplogSizeInBytes;
  }

  public void setMaxOplogSizeInBytes(long sizeInBytes) {
    this.maxOplogSizeInBytes = sizeInBytes;
    this.maxOplogSizeSet = true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getName()
   */
  public String getName() {
    return this.name;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getQueueSize()
   */
  public int getQueueSize() {
    return this.queueSize;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getTimeInterval()
   */
  public long getTimeInterval() {
    return this.timeInterval;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.DiskStore#getWriteBufferSize()
   */
  public int getWriteBufferSize() {
    return this.writeBufferSize;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public boolean getSyncWrites() {
    return this.syncWrites;
  }

  void validateAndAdjust() {
    // oplog size should be greater than size of every diskDir to be able to
    // create the oplog file (#51632)
    File[] dirs = this.diskDirs;
    int[] dirSizes = this.diskDirSizes;
    int length = dirs.length;
    long oplogSize = getMaxOplogSize();
    for (int index = 0; index < length; index++) {
      int diskDirSize = dirSizes[index];
      if (diskDirSize != 0 && oplogSize > diskDirSize) {
        // for the default case, reduce the max oplog size
        if (this.maxOplogSizeSet) {
          throw new InsufficientDiskSpaceException(LocalizedStrings
              .DiskStoreImpl_OplogSize_Exceeds_DiskDirSize.toLocalizedString(
                  oplogSize, diskDirSize, dirs[index].getName()), null, this);
        }
        else {
          this.maxOplogSizeInBytes = 1024L * 1024L * diskDirSize;
          oplogSize = diskDirSize;
        }
      }
    }
  }

  public void flush() {
    // nothing needed
  }

  public void forceRoll() {
    // nothing needed
  }

  public boolean forceCompaction() {
    return false;
  }

  @Override
  public void destroy() {
    // nothing needed
  }
}

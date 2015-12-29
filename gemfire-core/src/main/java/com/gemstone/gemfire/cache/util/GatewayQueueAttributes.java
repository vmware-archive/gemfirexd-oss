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

package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Class <code>GatewayQueueAttributes</code> contains attributes
 * related to the <code>Gateway</code> queue such as batch size and
 * disk directories.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
public class GatewayQueueAttributes {

  /**
   * The name of the directory in which to store overflowed and/or persisted
   * queue entries (if persistence is enabled)
   * @deprecated as of 6.5
   */
  private String _overflowDirectory;
  
  /**
   * The disk store name for overflow
   */
  private String _diskStoreName;

  /**
   * The maximum amount of memory (MB) to allow in the queue before
   * overflowing entries to disk
   */
  protected int _maximumQueueMemory;

  /**
   * The batch size for messages sent between this <code>Gateway</code>
   * and its corresponding <code>Gateway</code>.
   */
  private int _batchSize;

  /**
   * The maximum time interval in milliseconds to wait before sending an
   * incomplete batch of messages between this <code>Gateway</code>
   * and its corresponding <code>Gateway</code>.
   */
  private int _batchTimeInterval;

  /**
   * Whether to enable batch conflation for batches of messages sent
   * between this <code>Gateway</code>and its corresponding
   * <code>Gateway</code>.
   */
  private boolean _batchConflation;

  /**
   * Whether to enable persistence for this <code>Gateway</code>'s queue.
   */
  private boolean _enablePersistence;

  /**
   * Whether to enable oplog compaction for this <code>Gateway</code>'s queue.
   * @deprecated as of 6.5
   */
  private boolean enableOplogCompaction;

  /**
   * The time in milliseconds an event is in this <code>Gateway</code>'s queue
   * after which to log an alert
   */
  private int _alertThreshold;
  
  private boolean hasEnableOplogCompaction = false;
  private boolean hasOverflowDirectory = false;

  /**
   * The default overflow directory
   */
  public static final String DEFAULT_OVERFLOW_DIRECTORY = ".";

  /**
   * The default maximum amount of memory (MB) to allow in the queue
   * before overflowing entries to disk
   */
  public static final int DEFAULT_MAXIMUM_QUEUE_MEMORY = 100;

  /**
   * The default batch size
   */
  public static final int DEFAULT_BATCH_SIZE = 100;

  /**
   * The default batch time interval in milliseconds
   */
  public static final int DEFAULT_BATCH_TIME_INTERVAL = 1000;

  /**
   * The default batch conflation
   */
  public static final boolean DEFAULT_BATCH_CONFLATION = false;

  /**
   * The default enable persistence
   */
  public static final boolean DEFAULT_ENABLE_PERSISTENCE = false;

  /**
   * The default oplog compaction behaviour
   * @since 6.5
   */
  public static final boolean DEFAULT_ENABLE_COMPACTION = true;
  /**
   * The default oplog compaction behaviour
   * @deprecated as of 6.5 use DEFAULT_ENABLE_COMPACTION instead.
   * 
   */
  public static final boolean DEFAULT_ENABLE_ROLLING = DEFAULT_ENABLE_COMPACTION;

  /**
   * The default alert threshold in milliseconds
   */
  public static final int DEFAULT_ALERT_THRESHOLD = 0;


  /**
   * Default constructor.
   *
   */
  public GatewayQueueAttributes() {
    this._overflowDirectory = DEFAULT_OVERFLOW_DIRECTORY;
    this._maximumQueueMemory = DEFAULT_MAXIMUM_QUEUE_MEMORY;
    this._batchSize = DEFAULT_BATCH_SIZE;
    this._batchTimeInterval = DEFAULT_BATCH_TIME_INTERVAL;
    this._batchConflation = DEFAULT_BATCH_CONFLATION;
    this._enablePersistence = DEFAULT_ENABLE_PERSISTENCE;
    this.enableOplogCompaction = DEFAULT_ENABLE_COMPACTION;
    this._alertThreshold = DEFAULT_ALERT_THRESHOLD;
  }

  /**
   * Constructor.
   *
   * @param overflowDirectory The name of the directory in which to store
   * overflowed and/or persisted queue entries (if persistence is enabled)
   * @param maximumQueueMemory The maximum heap memory to be used by the
   * queue
   * @param batchSize The batch size for messages sent between
   * <code>Gateway</code>s
   * @param batchTimeInterval The maximum time interval in milliseconds
   * to wait before sending an incomplete batch of messages between
   * <code>Gateway</code>s
   * @param batchConflation Whether to enable conflation for 
   * batches of messages sent between <code>Gateway</code>s
   * @param enablePersistence Whether to enable persistence for this
   * <code>Gateway</code>'s queue
   * @param enableCompaction Whether to oplog compaction for this
   * <code>Gateway</code>'s queue
   * @param alertThreshold The alert threshold in milliseconds for entries in
   * this <code>Gateway</code>'s queue
   * @deprecated use {@link GatewayQueueAttributes#GatewayQueueAttributes(String, int, int, int, boolean, boolean, int)} 
   * to specify a disk store name, rather than a directory name.
   */
  public GatewayQueueAttributes(
    String overflowDirectory,
    int maximumQueueMemory,
    int batchSize,
    int batchTimeInterval,
    boolean batchConflation,
    boolean enablePersistence,
    boolean enableCompaction,
    int alertThreshold)
  {
    this._overflowDirectory = overflowDirectory;
    this._maximumQueueMemory = maximumQueueMemory;
    this._batchSize = batchSize;
    this._batchTimeInterval = batchTimeInterval;
    this._batchConflation = batchConflation;
    this._enablePersistence = enablePersistence;
    this.enableOplogCompaction = enableCompaction;
    this._alertThreshold = alertThreshold;
  }
  
  /**
   * Constructor.
   *
   * @param diskStoreName The name of the disk store in which to store
   * overflowed and/or persisted queue entries (if persistence is enabled)
   * @param maximumQueueMemory The maximum heap memory to be used by the
   * queue
   * @param batchSize The batch size for messages sent between
   * <code>Gateway</code>s
   * @param batchTimeInterval The maximum time interval in milliseconds
   * to wait before sending an incomplete batch of messages between
   * <code>Gateway</code>s
   * @param batchConflation Whether to enable conflation for 
   * batches of messages sent between <code>Gateway</code>s
   * @param enablePersistence Whether to enable persistence for this
   * <code>Gateway</code>'s queue
   * @param alertThreshold The alert threshold in milliseconds for entries in
   * this <code>Gateway</code>'s queue
   * @since 6.5
   */
  public GatewayQueueAttributes(
    String diskStoreName,
    int maximumQueueMemory,
    int batchSize,
    int batchTimeInterval,
    boolean batchConflation,
    boolean enablePersistence,
    int alertThreshold)
  {
    this._diskStoreName= diskStoreName;
    this._maximumQueueMemory = maximumQueueMemory;
    this._batchSize = batchSize;
    this._batchTimeInterval = batchTimeInterval;
    this._batchConflation = batchConflation;
    this._enablePersistence = enablePersistence;
    this._alertThreshold = alertThreshold;
  }

  /**
   * Sets the overflow directory for a <code>Gateway</code> queue's overflowed
   * and/or persisted queue entries.
   * @param overflowDirectory the overflow directory for a <code>Gateway</code>
   * queue's overflowed and/or persisted queue entries
   * @deprecated as of 6.5 use {@link #setDiskStoreName(String)} instead.
   */
  public void setOverflowDirectory(String overflowDirectory) {
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setOverflowDirectory", this.getDiskStoreName()}));
    }
    this._overflowDirectory = overflowDirectory;
    setHasOverflowDirectory(true);
  }

  /**
   * Answers the overflow directory for a <code>Gateway</code> queue's
   * overflowed and/or persisted queue entries.
   * @return the overflow directory for a <code>Gateway</code> queue's
   * overflowed and/or persisted queue entries
   * @deprecated as of 6.5 use {@link #getDiskStoreName()} instead.
   */
  public String getOverflowDirectory() {
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"getOverflowDirectory", this.getDiskStoreName()}));
    }
    return this._overflowDirectory;
  }

  /**
   * Sets the disk store name for overflow
   * @param diskStoreName
   * @since 6.5
   */
  public void setDiskStoreName(String diskStoreName) {
    if (hasEnableOplogCompaction() || hasOverflowDirectory()) {
      throw new IllegalStateException(LocalizedStrings.DiskStore_Deprecated_API_0_Cannot_Mix_With_DiskStore_1
          .toLocalizedString(new Object[] {"setDiskStoreName", this.getDiskStoreName()}));
    }
    this._diskStoreName = diskStoreName;
  }

  /**
   * Gets the disk store name for overflow
   * @return disk store name
   * @since 6.5
   */
  public String getDiskStoreName() {
    return this._diskStoreName;
  }
  /**
   * Sets the maximum amount of memory (in MB) for a <code>Gateway</code>'s
   * queue.
   * @param maximumQueueMemory The maximum amount of memory (in MB) for a
   * <code>Gateway</code>'s queue.
   */
  public void setMaximumQueueMemory(int maximumQueueMemory) {
    this._maximumQueueMemory = maximumQueueMemory;
  }

  /**
   * Answers maximum amount of memory (in MB) for a <code>Gateway</code>'s
   * queue.
   * @return maximum amount of memory (in MB) for a <code>Gateway</code>'s
   * queue
   */
  public int getMaximumQueueMemory() {
    return this._maximumQueueMemory;
  }

  /**
   * Sets the batch size for a <code>Gateway</code>'s queue.
   * @param batchSize The size of batches sent from a <code>Gateway</code>
   * to its corresponding <code>Gateway</code>.
   */
  public void setBatchSize(int batchSize) {
    this._batchSize = batchSize;
  }

  /**
   * Answers the batch size for a <code>Gateway</code>'s queue.
   * @return the batch size for a <code>Gateway</code>'s queue
   */
  public int getBatchSize() {
    return this._batchSize;
  }

  /**
   * Sets the batch time interval for a <code>Gateway</code>'s queue.
   * @param batchTimeInterval The maximum time interval that can elapse
   * before a partial batch is sent from a <code>Gateway</code>
   * to its corresponding <code>Gateway</code>.
   */
  public void setBatchTimeInterval(int batchTimeInterval) {
    this._batchTimeInterval = batchTimeInterval;
  }

  /**
   * Answers the batch time interval for a <code>Gateway</code>'s queue.
   * @return the batch time interval for a <code>Gateway</code>'s queue
   */
  public int getBatchTimeInterval() {
    return this._batchTimeInterval;
  }

  /**
   * Sets whether to enable batch conflation for a <code>Gateway</code>'s
   * queue.
   * @param batchConflation Whether or not to enable batch conflation
   * for batches sent from a <code>Gateway</code> to its corresponding
   * <code>Gateway</code>.
   */
  public void setBatchConflation(boolean batchConflation) {
    this._batchConflation = batchConflation;
  }

  /**
   * Answers whether to enable batch conflation for a <code>Gateway</code>'s
   * queue.
   * @return whether to enable batch conflation for batches sent from a
   * <code>Gateway</code> to its corresponding <code>Gateway</code>.
   */
  public boolean getBatchConflation() {
    return this._batchConflation;
  }

  /**
   * Sets whether to enable persistence for a <code>Gateway</code>'s
   * queue.
   * @param enablePersistence Whether to enable persistence for a
   * <code>Gateway</code>'s queue
   */
  public void setEnablePersistence(boolean enablePersistence) {
    this._enablePersistence = enablePersistence;
  }

  /**
   * Answers whether to enable persistence for a <code>Gateway</code>'s
   * queue.
   * @return whether to enable persistence for a <code>Gateway</code>'s
   * queue
   */
  public boolean getEnablePersistence() {
    return this._enablePersistence;
  }


  /**
   * Answers whether to enable oplog rolling for a <code>Gateway</code>'s
   * queue.
   * @return whether to enable oplog rolling for a <code>Gateway</code>'s
   * queue
   * @deprecated as of 6.5 use {@link #getDiskStoreName()} instead. 
   * On the disk store, use {@link DiskStore#getAutoCompact()}
   */
  public boolean isRollOplogs() {
    return this.enableOplogCompaction;
  }
  /**
   * Sets whether to enable oplog rolling for a <code>Gateway</code>'s
   * queue.
   * @param rollingEnabled Whether to enable oplog rolling for a
   * <code>Gateway</code>'s queue
   * @deprecated as of 6.5 use {@link #setDiskStoreName(String)} instead. 
   * When creating the disk store, use {@link DiskStoreFactory#setAutoCompact(boolean)} to
   * control compaction.
   */
  public void setRollOplogs( boolean rollingEnabled) {
    this.enableOplogCompaction = rollingEnabled;
    setHasEnableOplogCompaction(true);
  }

  /**
   * Sets the alert threshold for entries in a <code>Gateway</code>'s queue.
   * @param threshold the alert threshold for entries in a <code>Gateway</code>'s
   * queue
   *
   * @since 6.0
   */
  public void setAlertThreshold(int threshold) {
    this._alertThreshold = threshold;
  }

  /**
   * Returns the alert threshold for entries in a <code>Gateway</code>'s queue.
   * @return the alert threshold for entries in a <code>Gateway</code>'s queue
   *
   * @since 6.0
   */
  public int getAlertThreshold() {
    return this._alertThreshold;
  }

  /**
   * Returns <code>true</code> if {@link #setOverflowDirectory} has been called.
   * Returns <code>false</code> if the overflow directory has never been explicitly set.
   * @deprecated as of 6.5 use {@link #getDiskStoreName()} instead.
   */
  public boolean hasOverflowDirectory()
  {
    return this.hasOverflowDirectory;
  }
  private void setHasOverflowDirectory(boolean hasOverflowDirectory)
  {
    this.hasOverflowDirectory = hasOverflowDirectory;
  }
  private boolean hasEnableOplogCompaction()
  {
    return this.hasEnableOplogCompaction;
  }
  private void setHasEnableOplogCompaction(boolean enableOplogCompaction)
  {
    this.hasEnableOplogCompaction = enableOplogCompaction;
  }
}

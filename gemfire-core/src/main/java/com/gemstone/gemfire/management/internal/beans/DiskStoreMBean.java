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
package com.gemstone.gemfire.management.internal.beans;

import javax.management.NotificationBroadcasterSupport;

import com.gemstone.gemfire.management.DiskStoreMXBean;

/**
 * DiskStore MBean represent a DiskStore which provides disk storage for one or
 * more regions. The regions in the same disk store will share the same disk
 * persistence attributes. A region without a disk store name belongs to the
 * default disk store.
 * 
 * @author rishim
 * 
 */
public class DiskStoreMBean extends NotificationBroadcasterSupport implements
    DiskStoreMXBean {

  private DiskStoreMBeanBridge bridge;

  public DiskStoreMBean(DiskStoreMBeanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public boolean forceCompaction() {
    return bridge.forceCompaction();

  }

  @Override
  public void forceRoll() {
    bridge.forceRoll();

  }

  @Override
  public int getCompactionThreshold() {
    return bridge.getCompactionThreshold();
  }

  @Override
  public String[] getDiskDirectories() {
    return bridge.getDiskDirectories();
  }

  @Override
  public long getDiskReadsAvgLatency() {
    return bridge.getDiskReadsAvgLatency();
  }

  @Override
  public float getDiskReadsRate() {
    return bridge.getDiskReadsRate();
  }

  @Override
  public long getDiskWritesAvgLatency() {
    return bridge.getDiskWritesAvgLatency();
  }

  @Override
  public float getDiskWritesRate() {
    return bridge.getDiskWritesRate();
  }

  @Override
  public long getFlushTimeAvgLatency() {
    return bridge.getFlushTimeAvgLatency();
  }

  @Override
  public long getMaxOpLogSize() {
    return bridge.getMaxOpLogSize();
  }

  @Override
  public String getName() {
    return bridge.getName();
  }

  @Override
  public int getQueueSize() {
    return bridge.getQueueSize();
  }

  @Override
  public long getTimeInterval() {
    return bridge.getTimeInterval();
  }

  @Override
  public int getTotalBackupInProgress() {
    return bridge.getTotalBackupInProgress();
  }

  @Override
  public long getTotalBytesOnDisk() {
    return bridge.getTotalBytesOnDisk();
  }

  @Override
  public int getTotalQueueSize() {
    return bridge.getTotalQueueSize();
  }

  @Override
  public int getTotalRecoveriesInProgress() {
    return bridge.getTotalRecoveriesInProgress();
  }

  @Override
  public int getWriteBufferSize() {
    return bridge.getWriteBufferSize();
  }

  @Override
  public boolean isAutoCompact() {
    return bridge.isAutoCompact();
  }

  @Override
  public boolean isForceCompactionAllowed() {
    return bridge.isForceCompactionAllowed();
  }

  @Override
  public void flush() {
    bridge.flush();

  }

  @Override
  public int getTotalBackupCompleted() {
    return bridge.getTotalBackupCompleted();
  }
  
  public DiskStoreMBeanBridge getBridge(){
    return bridge;
  }
  
  public void stopMonitor(){
    bridge.stopMonitor();
  }

}

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

import com.pivotal.gemfirexd.internal.catalog.UUID;

public class GfxdGatewaySenderDescriptor extends TupleDescriptor {

  private final UUID id;

  public String senderId;

  public int remoteDs;

  public String serverGroup;

  public int socketBufferSize;

  public boolean manualStart;

  public int socketReadTimeout;

  public boolean isBatchConflationEnabled;

  public int batchSize;

  public int batchTimeInterval;

  public boolean isPersistenceEnabled;
  
  public boolean diskSynchronous;

  public String diskStoreName;

  public int maximumQueueMemory;

  public int alertThreshold;
  
  public boolean isStarted;

  public GfxdGatewaySenderDescriptor(DataDictionary dd, UUID id,
      String senderId, Integer remoteDs, String serverGroup,
      Integer socketBufferSize, Boolean manualStart,
      Integer socketReadTimeout, Boolean isBatchConflationEnabled,
      Integer batchSize, Integer batchTimeInterval,
      Boolean isPersistenceEnabled, Boolean diskSynchronous, String diskStoreName,
      Integer maximumQueueMemory, Integer alertThreshold, Boolean isStarted) {

    super(dd);
    this.id = id;
    this.senderId = senderId;
    this.remoteDs = remoteDs;
    this.serverGroup = serverGroup;
    this.socketBufferSize = socketBufferSize;
    this.manualStart = manualStart;
    this.socketReadTimeout = socketReadTimeout;
    this.batchSize = batchSize;
    this.batchTimeInterval = batchTimeInterval;
    this.isBatchConflationEnabled = isBatchConflationEnabled;
    this.isPersistenceEnabled = isPersistenceEnabled;
    this.diskSynchronous = diskSynchronous;
    this.diskStoreName = diskStoreName;
    this.maximumQueueMemory = maximumQueueMemory;
    this.alertThreshold = alertThreshold;
    this.isStarted = isStarted;

  }

  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public int getSocketReadTimeout() {
    return this.socketReadTimeout;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public int getMaximumQueueMemory() {
    return this.maximumQueueMemory;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  public int getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  public boolean isBatchConflationEnabled() {
    return this.isBatchConflationEnabled;
  }

  public boolean isPersistenceEnabled() {
    return this.isPersistenceEnabled;
  }

  public boolean isDiskSynchronous() {
    return this.diskSynchronous;
  }
  
  public int getAlertThreshold() {
    return this.alertThreshold;
  }

  public boolean isManualStart() {
    return this.manualStart;
  }

  public String getSenderId() {
    return this.senderId;
  }

  public int getRemoteDistributedSystemId() {
    return this.remoteDs;
  }

  public String getServerGroup() {
    return this.serverGroup;
  }

  public UUID getUUID() {
    return this.id;
  }

  public boolean isStarted() {
    return this.isStarted;
  }
  
  public String getDescriptorType() 
  {
          return "GatewaySender";
  }
  
  /** @see TupleDescriptor#getDescriptorName */
  public String getDescriptorName() { 
    return this.senderId; 
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GfxdGatewaySenderDescriptor{");
    sb.append("id=" + getSenderId());
    sb.append(",remoteDsId=" + getRemoteDistributedSystemId());
    sb.append("}");
    return sb.toString();
  }
}

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

package com.gemstone.gemfire.internal.cache.control;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds.MemoryState;

/**
 * @author sbawaska
 * @author David Hoots
 */
public class MemoryEvent implements ResourceEvent {
  private final ResourceType type;
  private volatile MemoryState state;
  private final MemoryState previousState;
  private final DistributedMember member;
  private final long bytesUsed;
  private final boolean isLocal;
  private final MemoryThresholds thresholds;
  private final long eventTime;
 
  public MemoryEvent(final ResourceType type, final MemoryState previousState, final MemoryState state,
      final DistributedMember member, final long bytesUsed, final boolean isLocal, final MemoryThresholds thresholds) {
    this.type = type;
    this.previousState = previousState;
    this.state = state;
    this.member = member;
    this.bytesUsed = bytesUsed;
    this.isLocal = isLocal;
    this.thresholds = thresholds;
    this.eventTime = System.currentTimeMillis();
  }

  @Override
  public ResourceType getType() {
    return this.type;
  }
  
  public MemoryState getPreviousState() {
    return this.previousState;
  }
  
  public MemoryState getState() {
    return this.state;
  }

  @Override
  public DistributedMember getMember() {
    return this.member;
  }

  public long getBytesUsed() {
    return this.bytesUsed;
  }
  
  @Override
  public boolean isLocal() {
    return this.isLocal;
  }
  
  public long getEventTime() {
    return this.eventTime;
  }

  public MemoryThresholds getThresholds() {
    return this.thresholds;
  }
  
  @Override
  public String toString() {
    return new StringBuilder().append("MemoryEvent@")
        .append(System.identityHashCode(this))
        .append("[Member:" + this.member)
        .append(",type:" + this.type)
        .append(",previousState:" + this.previousState)
        .append(",state:" + this.state)
        .append(",bytesUsed:" + this.bytesUsed)
        .append(",isLocal:" + this.isLocal)
        .append(",eventTime:" + this.eventTime)
        .append(",thresholds:" + this.thresholds + "]")
        .toString();
  }
}


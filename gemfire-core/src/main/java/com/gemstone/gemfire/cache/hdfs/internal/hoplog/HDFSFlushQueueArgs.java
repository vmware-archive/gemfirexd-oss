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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.VersionedDataSerializable;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * Defines the arguments to the flush queue request.
 * 
 * @author bakera
 */
@SuppressWarnings("serial")
public class HDFSFlushQueueArgs implements VersionedDataSerializable {

  private static Version[] serializationVersions = new Version[]{ Version.GFXD_10 };

  private HashSet<Integer> buckets;

  private long maxWaitTimeMillis;

  public HDFSFlushQueueArgs() {
  }

  public HDFSFlushQueueArgs(Set<Integer> buckets, long maxWaitTime) {
    this.buckets = new HashSet<Integer>(buckets);
    this.maxWaitTimeMillis = maxWaitTime;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashSet(buckets, out);
    out.writeLong(maxWaitTimeMillis);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    this.buckets = DataSerializer.readHashSet(in);
    this.maxWaitTimeMillis = in.readLong();
  }

  @Override
  public Version[] getSerializationVersions() {
    return serializationVersions;
  }

  public Set<Integer> getBuckets() {
    return (Set<Integer>) buckets;
  }

  public void setBuckets(Set<Integer> buckets) {
    this.buckets = new HashSet<Integer>(buckets);
  }

  public boolean isSynchronous() {
    return maxWaitTimeMillis == 0;
  }

  public long getMaxWaitTime() {
    return this.maxWaitTimeMillis;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getCanonicalName()).append("@")
    .append(System.identityHashCode(this))
    .append(" buckets:").append(buckets)
    .append(" maxWaitTime:").append(maxWaitTimeMillis);
    return sb.toString();
  }
}

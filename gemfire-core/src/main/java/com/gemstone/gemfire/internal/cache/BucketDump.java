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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * This is a class used for test hooks that get the contents of buckets
 * and then compare them. It contains the region version vector for the bucket
 * as well as all of the entries.
 * @author dsmith
 *
 */
public class BucketDump {
  /**
   * The version vector for this bucket
   */
  private final RegionVersionVector rvv;
  
  /**
   * The contents of the bucket
   */
  private final Map<Object, Object> values;
  
  /**
   * The contents of the bucket
   */
  private final Map<Object, VersionTag> versions;
  
  private final int bucketId;
  
  private final InternalDistributedMember member;

  public BucketDump(int bucketId, InternalDistributedMember member,
      RegionVersionVector rvv, Map<Object, Object> values,
      Map<Object, VersionTag> versions) {
    this.bucketId = bucketId;
    this.member = member;
    this.rvv = rvv;
    this.values = values;
    this.versions = versions;
  }

  public RegionVersionVector getRvv() {
    return rvv;
  }


  public Map<Object, Object> getValues() {
    return values;
  }

  public Map<Object, VersionTag> getVersions() {
    return versions;
  }

  public int getBucketId() {
    return bucketId;
  }

  public InternalDistributedMember getMember() {
    return member;
  }

  @Override
  public String toString()
  {
//    int sz; 
//    synchronized(this) {
//      sz = this.size();
//    }
    return "Bucket id = " + bucketId + " from member = "
        + member
        + ": " + super.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((values == null) ? 0 : values.hashCode());
    result = prime * result + ((versions == null) ? 0 : versions.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BucketDump other = (BucketDump) obj;
    if (values == null) {
      if (other.values != null)
        return false;
    } else if (!values.equals(other.values))
      return false;
    if (versions == null) {
      if (other.versions != null)
        return false;
    } else if (!versions.equals(other.versions))
      return false;
    return true;
  }
}

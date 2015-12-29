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
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;

/**
 * Provides load and bucket level details for internal use. Extends 
 * {@link com.gemstone.gemfire.cache.partition.PartitionMemberInfo}.
 * 
 * @author Kirk Lund
 */
public interface InternalPartitionDetails 
extends PartitionMemberInfo, Comparable<InternalPartitionDetails> {

  /**
   * Returns the load for the partitioned region.
   * 
   * @return the load for the partitioned region
   */
  public PRLoad getPRLoad();
  
  /**
   * Returns the size of the bucket in bytes.
   * 
   * @param bucketId the identity of the bucket from 0 to number of buckets -1
   * @return the size of the bucket in bytes
   */
  public long getBucketSize(int bucketId);
  
}

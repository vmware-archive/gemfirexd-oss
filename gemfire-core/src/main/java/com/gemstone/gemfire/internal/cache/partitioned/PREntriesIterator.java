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

import java.util.Iterator;

import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * This interface is implemented by the partitioned region iterators to provide
 * current bucket information during iteration.Currently used by GemFireXD to
 * obtain information of the bucket ID from which the current local entry is
 * being fetched from as also the bucket region.
 * 
 * @author Asif, swale
 */
public interface PREntriesIterator<T> extends Iterator<T> {

  /**
   * @return the PartitionedRegion being iterated
   */
  public PartitionedRegion getPartitionedRegion();

  /**
   * @return int bucket ID of the bucket in which the current entry resides
   */
  public int getBucketId();

  /**
   * Bucket in which the current entry resides.
   */
  public Bucket getBucket();

  /**
   * If the current entry resides in local hosted BucketRegion then return that
   * else null.
   */
  public BucketRegion getHostedBucketRegion();
}

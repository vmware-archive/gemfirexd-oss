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
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;
import java.util.Arrays;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Composite date type used to distribute attributes for a {@link PartitionedRegion}.
 * 
 * @author rishim
 * @since 7.0
 */
public class PartitionAttributesData {

  private int redundantCopies;
  private long totalMaxMemory;
  // Total number of buckets for whole region
  private int totalNumBuckets;
  private int localMaxMemory;
  private String colocatedWith;
  private String partitionResolver;
  private long recoveryDelay;
  private long startupRecoveryDelay;
  private String[] partitionListeners;

  @ConstructorProperties( { "redundantCopies", "totalMaxMemory",
      "totalNumBuckets", "localMaxMemory", "colocatedWith",
      "partitionResolver", "recoveryDelay", "startupRecoveryDelay",
      "partitionListeners" })
  public PartitionAttributesData(int redundantCopies, long totalMaxMemory,
      int totalNumBuckets, int localMaxMemory, String colocatedWith,
      String partitionResolver, long recoveryDelay, long startupRecoveryDelay,
      String[] partitionListeners) {

    this.redundantCopies = redundantCopies;
    this.totalMaxMemory = totalMaxMemory;
    this.totalNumBuckets = totalNumBuckets;
    this.localMaxMemory = localMaxMemory;
    this.colocatedWith = colocatedWith;
    this.partitionResolver = partitionResolver;
    this.recoveryDelay = recoveryDelay;
    this.startupRecoveryDelay = startupRecoveryDelay;
    this.partitionListeners = partitionListeners;
   }

  /**
   * Returns the number of redundant copies for this PartitionedRegion.
   */
  public int getRedundantCopies() {
    return redundantCopies;
  }

  /**
   * Returns the maximum total size (in megabytes) of the Region.
   */
  public long getTotalMaxMemory() {
    return totalMaxMemory;
  }

  /**
   * Returns the total number of buckets for the whole region.
   */
  public int getTotalNumBuckets() {
    return totalNumBuckets;
  }

  /**
   * Returns the maximum amount of local memory that can be used by the region.
   */
  public int getLocalMaxMemory() {
    return localMaxMemory;
  }

  /**
   * Returns the name of the PartitionedRegion that this PartitionedRegion is
   * colocated with.
   */
  public String getColocatedWith() {
    return colocatedWith;
  }

  /**
   * Returns a list of Classes that are configured as resolvers for the Region.
   */
  public String getPartitionResolver() {
    return partitionResolver;
  }

  /**
   * Returns the delay (in milliseconds) that a member will wait while trying
   * to satisfy the redundancy of data hosted on other members.
   */
  public long getRecoveryDelay() {
    return recoveryDelay;
  }

  /**
   * Returns the delay (in milliseconds) that a new member will wait while trying
   * to satisfy the redundancy of data hosted on other members.
   */
  public long getStartupRecoveryDelay() {
    return startupRecoveryDelay;
  }

  /**
   * Returns a list of Classes that are configured as listeners for the Region.
   */
  public String[] getPartitionListeners() {
    return partitionListeners;
  }

  @Override
  public String toString() {
    return "PartitionAttributesData [colocatedWith=" + colocatedWith
        + ", localMaxMemory=" + localMaxMemory + ", partitionListeners="
        + Arrays.toString(partitionListeners) + ", partitionResolver="
        + partitionResolver + ", recoveryDelay=" + recoveryDelay
        + ", redundantCopies=" + redundantCopies + ", startupRecoveryDelay="
        + startupRecoveryDelay + ", totalMaxMemory=" + totalMaxMemory
        + ", totalNumBuckets=" + totalNumBuckets + "]";
  }
}

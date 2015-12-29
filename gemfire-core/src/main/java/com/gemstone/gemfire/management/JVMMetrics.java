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

import com.gemstone.gemfire.cache.Region;


/**
 * Composite data type used to distribute metrics for the JVM running
 * a GemFire member.
 * 
 * @author rishim
 * @since 7.0
 *
 */
public class JVMMetrics {

  /**
   * Number of GCs performed
   */
  private long gcCount;

  /**
   * Total gc time in milliseconds
   */
  private long gcTimeMillis;

  /**
   * Initial memory the vm requested from the operating system for this area in
   * bytes
   */
  private long initMemory;

  /**
   * The amount of used memory for this area, measured in bytes
   */
  private long committedMemory;

  /**
   * The amount of used memory for this area, measured in bytes
   */
  private long usedMemory;

  /**
   * represents the maximum amount of memory (in bytes) that can be used for memory
   * management
   */
  private long maxMemory;

  /**
   * Total number of threads
   */
  private int totalThreads;

  @ConstructorProperties( { "gcCount", "gcTimeMillis", "initMemory",
      "committedMemory", "usedMemory", "maxMemory", "totalThreads"
  })
  public JVMMetrics(long gcCount, long gcTimeMillis, long initMemory,
      long committedMemory, long usedMemory, long maxMemory,
      int totalThreads) {
    this.gcCount = gcCount;
    this.gcTimeMillis = gcTimeMillis;
    this.initMemory = initMemory;
    this.committedMemory = committedMemory;
    this.usedMemory = usedMemory;
    this.maxMemory = maxMemory;
    this.totalThreads = totalThreads;
  }

  /**
   * Returns the number of times garbage collection has occured.
   */
  public long getGcCount() {
    return gcCount;
  }

  /**
   * Returns the amount of time (in milliseconds) spent on garbage collection.
   */
  public long getGcTimeMillis() {
    return gcTimeMillis;
  }

  /**
   * Returns the initial number of megabytes of memory requested from the
   * operating system.
   */
  public long getInitMemory() {
    return initMemory;
  }

  /**
   * Returns the current number of megabytes of memory allocated.
   */
  public long getCommittedMemory() {
    return committedMemory;
  }
  
  /**
   * Returns the current number of megabytes of memory being used.
   */
  public long getUsedMemory() {
    return usedMemory;
  }

  /**
   * Returns the maximum number of megabytes of memory available from the
   * operating system.
   */
  public long getMaxMemory() {
    return maxMemory;
  }

  /**
   * Returns the number of threads in use.
   */
  public int getTotalThreads() {
    return totalThreads;
  }

  @Override
  public String toString() {
    return "{JVMMetrics : gcCount = " + gcCount + " gcTimeMillis = "
        + gcTimeMillis + " initMemory = " + initMemory + " committedMemory = "
        + committedMemory + " usedMemory = " + usedMemory + " maxMemory = "
        + maxMemory + " totalThreads = " + totalThreads + "}";
  }
}

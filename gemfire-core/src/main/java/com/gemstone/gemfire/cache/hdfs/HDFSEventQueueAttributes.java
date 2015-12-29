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

package com.gemstone.gemfire.cache.hdfs;

/**
 * {@link HDFSEventQueueAttributes} represents the attributes of the buffer where events are 
 * accumulated before they are persisted to HDFS  
 * 
 * @author Hemant Bhanawat
 */
public interface HDFSEventQueueAttributes {

  /**
   * The Disk store that is required for overflow and persistence
   * @return    String
   */
  public String getDiskStoreName();

  /**
   * The maximum memory after which the data needs to be overflowed to disk
   * @return    int
   */
  public int getMaximumQueueMemory();

  /**
   * Represents the size of a batch per bucket that gets delivered
   * from the HDFS Event Queue to HDFS. A higher value means that 
   * less number of bigger batches are persisted to HDFS and hence 
   * big files are created on HDFS. But, bigger batches consume memory.  
   *  
   * This value is an indication. Batches per bucket with size less than the specified
   * are sent to HDFS if interval specified by {@link #getBatchTimeInterval()}
   * has elapsed.
   * @return    int
   */
  public int getBatchSizeMB();
  
  /**
   * Represents the batch time interval for a HDFS queue. This is the maximum time interval
   * that can elapse before a batch of data from a bucket is sent to HDFS.
   *
   * @return  int
   */
  public int getBatchTimeInterval();
  
  /**
   * Represents whether the  HDFS Event Queue is configured to be persistent or non-persistent
   * @return    boolean
   */
  public boolean isPersistent();

  /**
   * Represents whether or not the writing to the disk is synchronous.
   * 
   * @return boolean 
   */
  public boolean isDiskSynchronous();
  
  /**
   * Number of threads in VM consuming the events.
   * default is one.
   * 
   * @return int
   */
  public int getDispatcherThreads();
}

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

package com.pivotal.gemfirexd.internal.engine.ddl;

/**
 * A key-value pair containing the cached data in a {@link GfxdDDLRegionQueue}.
 * This object's operations (except for{Entry#setValue()}), are not distributed,
 * do not acquire any locks, and do not affect <code>CacheStatistics</code>.
 */
public interface GfxdDDLQueueEntry {

  /**
   * Returns the key for this entry.
   * 
   * @return the key for this entry
   */
  public Long getKey();

  /**
   * Returns the value of this entry in the local cache. Does not invoke a
   * <code>CacheLoader</code>, does not do a netSearch, netLoad, etc.
   * 
   * @return the value of this entry
   */
  public Object getValue();

  /**
   * Return the sequence ID of this entry in the queue.
   */
  public long getSequenceId();

  /**
   * Set the sequence ID of this entry in the queue.
   */
  public void setSequenceId(long seqId);
}

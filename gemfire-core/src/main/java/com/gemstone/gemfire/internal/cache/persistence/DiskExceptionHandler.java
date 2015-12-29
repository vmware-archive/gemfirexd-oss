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
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;

/**
 * An interface for handling exceptions that occur at the disk layer, used
 * by the {@link DiskRegion} class. The exception handler is expected to close
 * the region. This interface exists so that ProxyBucketRegions can handle
 * disk access exceptions by passing them on to the parent partition region.
 * @author dsmith
 *
 */
public interface DiskExceptionHandler {
  
  /**
   * @param dae DiskAccessException encountered by the thread
   * @param stopBridgeServers boolean which indicates that apart from destroying the
   * region should the bridge servers running, if any, should also be stopped or not.
   * So if the DiskAccessException occurs during region creation ( while recovering
   * from the  disk or GII) , then the servers are not stopped as the clients would
   * not have registered any interest. But if the Exception occurs during entry operations
   * then the bridge servers need to be stopped.
   * @see LocalRegion#handleDiskAccessException(DiskAccessException, boolean)
   */
  void handleDiskAccessException(DiskAccessException dae, boolean stopBridgeServers);
}

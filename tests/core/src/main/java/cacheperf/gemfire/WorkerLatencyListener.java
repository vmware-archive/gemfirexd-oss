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

package cacheperf.gemfire;

import com.gemstone.gemfire.cache.EntryEvent;

/**
 *  A cache listener that records message latency statistics on updates and then
 *  does non-cache work for a specified duration.
 *
 *  @author jfarris
 *  @since 5.0
 */

public class WorkerLatencyListener extends LatencyListener {

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  /**
   *  Creates a worker latency listener.
   */
  public WorkerLatencyListener() {
    super();
  }

  //----------------------------------------------------------------------------
  // CacheListener API
  //----------------------------------------------------------------------------

  /**
   * Overrides to do some work as part of the update.
   */
  public void afterUpdate(EntryEvent event) {
    super.afterUpdate(event);
    doNonCacheWork();
  }
}

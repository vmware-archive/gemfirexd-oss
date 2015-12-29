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

package cacheperf.poc.useCase3.mars;

import cacheperf.CachePerfClient;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.RegionHelper;

/**
 * Tasks for TIMPR servers and clients.
 */
public class MarsTasks extends CachePerfClient {

  //----------------------------------------------------------------------------
  // Cache Tasks
  //----------------------------------------------------------------------------

  /**
   * Opens the cache.
   */
  public static void openCacheTask() {
    String cacheConfig = ConfigPrms.getCacheConfig();
    CacheHelper.createCache(cacheConfig);
  }

  /**
   * Closes the cache.
   */
  public static void closeCacheTask() {
    CacheHelper.closeCache();
  }

  //----------------------------------------------------------------------------
  // Region Tasks
  //----------------------------------------------------------------------------

  /**
   * Creates the region.
   */
  public static void createRegionTask() {
    String regionConfig = ConfigPrms.getRegionConfig();
    RegionHelper.createRegion(regionConfig);
  }
}

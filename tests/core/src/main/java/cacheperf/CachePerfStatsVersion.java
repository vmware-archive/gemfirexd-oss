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

package cacheperf;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import distcache.DistCache;
import distcache.gemfire.GemFireCacheTestImpl;
import hydra.CacheHelper;

import java.util.Set;

/**
 * Version support for cacheperf.CachePerfStats.
 */
public class CachePerfStatsVersion {

  /**
   * increase the time on disk recovery
   */
  private static void incLocalRecoveryTime(long amount, Statistics stats) {
    stats.incLong(CachePerfStats.LOCALRECOVERY_TIME, amount);
  }
  /**
   * increase the time on remote recovery
   */
  private static void incRemoteRecoveryTime(long amount, Statistics stats) {
    stats.incLong(CachePerfStats.REMOTERECOVERY_TIME, amount);
  }
  /**
   * @param record stat for end of recovery
   */
  public static void endDiskRecovery(long elapsed, DistCache theCache, Statistics stats) {
    if (theCache instanceof GemFireCacheTestImpl) {
      Cache gfCache = CacheHelper.getCache();
      Set<Region<?, ?>> aSet = gfCache.rootRegions();
      for (Region aRegion: aSet) {
        DiskRegion dr = ((LocalRegion)aRegion).getDiskRegion();
        if (dr != null) {
          DiskRegionStats diskStats = dr.getStats();
          long localInit = diskStats.getLocalInitializations();
          long remoteInit = diskStats.getRemoteInitializations();
          if (localInit > 0) {
            incLocalRecoveryTime(elapsed, stats);
          } 
          if (remoteInit > 0) {
            incRemoteRecoveryTime(elapsed, stats);
          }
        }
      }
    }
  }
}

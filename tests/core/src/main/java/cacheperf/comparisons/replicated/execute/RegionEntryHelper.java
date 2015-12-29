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
package cacheperf.comparisons.replicated.execute;

import java.io.Serializable;

import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

public class RegionEntryHelper {

  /**
   * Returns the value for the specified key.
   * @param region
   * @param key
   * @return
   */
  protected static Serializable getValue(Region region, Serializable key) {
    Serializable value = null;
    // Get the value associated with that key
    if (isValid(region, key)) {
      // Get the value at that key (to update its timestamp)
      value = (Serializable) region.get(key);
    }
    return value;
  }

  /**
   * Returns whether the entry at the key is valid. If it should have expired
   * but hasn't, log a warning.
   * @param region
   * @param key
   * @return
   */
  private static boolean isValid(Region region, Serializable key) {
    boolean valid = true;
    Region.Entry entry = region.getEntry(key);
    if (entry != null) {
      CacheStatistics statistics = entry.getStatistics();
      GemFireCacheImpl cache = (GemFireCacheImpl) region.getCache(); 
      long msSinceLastAccessed = cache.cacheTimeMillis() - statistics.getLastAccessedTime();
      long timeout = region.getAttributes().getEntryIdleTimeout().getTimeout() * 1000;
      if (msSinceLastAccessed > timeout) {
        StringBuilder builder = new StringBuilder();
        builder
            .append("It has been ")
            .append(msSinceLastAccessed)
            .append(" ms since entry at key=")
            .append(key)
            .append(" was accessed. It should be expired but isn't.");
        region.getCache().getLogger().warning(builder.toString());
        valid = false;
      }
    }
    return valid;
  }
}

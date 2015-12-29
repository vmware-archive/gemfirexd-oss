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
package cacheRunner;

import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheListener</code> that logs information about the events it
 * receives.
 * 
 * @author GemStone Systems, Inc.
 * @since 4.0
 */
public class LoggingCacheListener<K, V> extends LoggingCacheCallback implements
    CacheListener<K, V> {

  public void afterCreate(EntryEvent<K, V> event) {
    log("CacheListener.afterCreate", event);
  }

  public void afterUpdate(EntryEvent<K, V> event) {
    log("CacheListener.afterUpdate", event);
  }

  public void afterInvalidate(EntryEvent<K, V> event) {
    log("CacheListener.afterInvalidate", event);
  }

  public void afterDestroy(EntryEvent<K, V> event) {
    log("CacheListener.afterDestroy", event);
  }

  public void afterRegionInvalidate(RegionEvent<K, V> event) {
    log("CacheListener.afterRegionInvalidate", event);
  }

  public void afterRegionDestroy(RegionEvent<K, V> event) {
    log("CacheListener.afterRegionDestroy", event);
  }

  public void afterRegionClear(RegionEvent<K, V> event) {
    log("CacheListener.afterRegionClear", event);
  }
  
  public void afterRegionCreate(RegionEvent<K, V> event) {
	  log("CacheListener.afterRegionCreate", event);
  }

  public void afterRegionLive(RegionEvent<K, V> event) {
	  log("CacheListener.afterRegionLive", event);
  }
}

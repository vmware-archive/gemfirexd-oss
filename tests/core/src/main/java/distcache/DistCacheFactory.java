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

package distcache;

import hydra.Log;

/**
 *  Supports creating distributed caches of various types.
 */
public class DistCacheFactory {

  /**
   *  Creates a {@link DistCache} backed by an implementation of the type
   *  configured via {@link DistCachePrms#cacheVendor}.
   *
   *  @throws DistCacheException if any problem was encountered.
   */
  public static DistCache createInstance() {
    try {
      Class cls = null;
      switch( DistCachePrms.getCacheVendor() ) {
	case DistCachePrms.GEMFIRE:
	  Log.getLogWriter().fine( "Creating a GemFire cache implementation" );
	  cls = Class.forName( "distcache.gemfire.GemFireCacheTestImpl" );
	  break;
	case DistCachePrms.HASHMAP:
	  Log.getLogWriter().fine( "Creating a HashMap cache implementation" );
	  cls = Class.forName( "distcache.hashmap.HashMapCacheImpl" );
	  break;
	default:
	  throw new DistCacheException( "Should not happen" );
      }
      DistCache cache = (DistCache) cls.newInstance();
      Log.getLogWriter().fine( "Created the cache implementation" );
      return cache;
    } catch( DistCacheException e ) {
      throw e;
    } catch( Exception e ) {
      throw new DistCacheException( "Problem creating cache implementation", e );
    }
  }
}

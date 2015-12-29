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

import java.util.*;

/**
 *  Interface for a member of a distributed cache of objects.
 */
public interface DistCache {

  /**
   * Creates the local cache (with no regions).
   */
  public void createCache()
  throws DistCacheException;

  /**
   *  Opens the local cache.
   */
  public void open()
  throws DistCacheException;

  /**
   *  Closes the local cache.
   */
  public void close()
  throws DistCacheException;

  /**
   *  Gets the name of this member of the distributed cache.
   */
  public String getCacheName();

  /**
   *  Gets the CacheTransactionManager of the distributed cache.
   *  returns null if not implemented
   */
  public Object getCacheTransactionManager();

  /**
   *  Notifies this member about other members of the distributed cache.
   */
  public void addMembers( Map caches )
  throws DistCacheException;

  /**
   *  Puts the specified key, with no value, into the cache.
   *  @throws DistCacheException if the key already exists.
   */
  public void createKey( Object key )
  throws DistCacheException;

  /**
   *  Puts the specified key and value into the cache.
   *  @throws DistCacheException if the key already exists.
   */
  public void create( Object key, Object value )
  throws DistCacheException;

  /**
   *  Puts the specified value into the cache at the specified key.
   *  @throws DistCacheException if the value is null.
   */
  public void put( Object key, Object value )
  throws DistCacheException;

  /**
   *  Puts the specified value in the cache at the specified key
   *  under the protection of a distributed lock.
   *  @throws DistCacheException if the key does not exist.
   */
  public void synchPut( Object key, Object value )
  throws DistCacheException;

  /**
   *  Gets the value with the specified key from the cache.
   *  @return the value (or null).
   */
  public Object get( Object key )
  throws DistCacheException;

  /**
   *  Gets the value with the specified key from the local cache.
   *  @return the value (or null).
   */
  public Object localGet( Object key )
  throws DistCacheException;

  /**
   *  Removes the value from the cache, but keeps the key.
   */
  public void invalidate( Object key )
  throws DistCacheException;

  /**
   *  Removes the value from the local cache, but keeps the key.
   */
  public void localInvalidate( Object key )
  throws DistCacheException;

  /**
   *  Removes the specified key and its value from the cache.
   */
  public void destroy( Object key )
  throws DistCacheException;

  /**
   *  Removes the specified key and its value from the local cache.
   */
  public void localDestroy( Object key )
  throws DistCacheException;

  /**
   *  Returns the number of entries in the local cache.
   */
  public int size()
  throws DistCacheException;
}

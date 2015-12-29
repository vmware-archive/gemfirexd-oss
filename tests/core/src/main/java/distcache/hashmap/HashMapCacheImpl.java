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

package distcache.hashmap;

import com.gemstone.gemfire.LogWriter;
import distcache.*;
import hydra.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  Implements the {@link distcache.DistCache} interface.  Holds onto a
 *  {@link java.util.HashMap}.
 */
public class HashMapCacheImpl implements DistCache{

  private String cacheName;
  private ConcurrentHashMap cache;
  private LogWriter log;
  private boolean fine;

  //----------------------------------------------------------------------------
  //  Constructors
  //----------------------------------------------------------------------------

  /**
   *  Creates a {@link DistCache} backed by a {@link java.util.ConcurrentHashMap}.
   */
  public HashMapCacheImpl() {
    this.cacheName = "HashMapCache";
    this.log = Log.getLogWriter();
    this.fine = this.log.fineEnabled();
  }

  /**
   * Returns the underlying map;
   */
  public ConcurrentHashMap getMap() {
    return this.cache;
  }

  //----------------------------------------------------------------------------
  //  Interface methods
  //----------------------------------------------------------------------------

  /**
   *  See {@link distcache.DistCache#createCache}.
   */
  public void createCache()
  throws DistCacheException {
    throw new UnsupportedOperationException();
  }

  /**
   *  See {@link distcache.DistCache#open}.
   */
  public void open()
  throws DistCacheException {
    try {
      synchronized( this.getClass() ) {
        if ( this.cache == null ) {
          int initialCapacity = HashMapCachePrms.getInitialCapacity();
          double loadFactor = HashMapCachePrms.getLoadFactor();
          int concurrencyLevel = HashMapCachePrms.getConcurrencyLevel();
          this.cache = new ConcurrentHashMap(initialCapacity, (float)loadFactor,
                                             concurrencyLevel);
          this.log.info( "Created cache " + this.cache );
	}
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /** 
   *  See {@link distcache.DistCache#close}.
   */
  public void close() {
    synchronized( this.getClass() ) {
      if ( this.cache != null ) {
        this.log.info( "Releasing the cache" );
        this.cache = null;
      }
    }
  }

  /**
   *  See {@link distcache.DistCache#getCacheName}.
   */
  public String getCacheName() {
    return this.cacheName;
  }

  /**
   *  See {@link distcache.DistCache#getCacheTransactionManager}.
   */
  public Object getCacheTransactionManager() {
    return null;
  }

  /**
   *  See {@link distcache.DistCache#addMembers}.
   */
  public void addMembers( Map caches )
  throws DistCacheException {
    // noop
  }

  /**
   *  See {@link distcache.DistCache#createKey}.
   */
  public void createKey( Object key )
  throws DistCacheException {
    // @todo lises make this complain if key already exists
    try {
      this.cache.put( key, null );
      if ( this.fine ) {
        this.log.fine( "Created key " + key );
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#put(Object, Object)}.
   */
  public void create( Object key, Object value )
  throws DistCacheException {
    try {
      // @todo lises make this complain if key already exists
      this.cache.put( key, value );
      if ( this.fine ) {
        this.log.fine( "Created " + key + "=" + value );
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#put}.
   */
  public void put( Object key, Object value )
  throws DistCacheException {
    if ( value == null ) {
      throw new DistCacheException( "Cannot put null value" );
    }
    try {
      this.cache.put( key, value );
      if ( this.fine ) {
        this.log.fine( "Put key " + key + "=" + value );
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#synchPut}.
   */
  public void synchPut( Object key, Object value )
  throws DistCacheException {
    throw new UnsupportedOperationException();
  }

  /**
   *  See {@link distcache.DistCache#get}.
   */
  public Object get( Object key )
  throws DistCacheException {
    try {
      Object value = this.cache.get( key );
      if ( this.fine ) {
        this.log.fine( "Got key " + key + "=" + value );
      }
      return value;
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#localGet}.
   */
  public Object localGet( Object key ) {
    try {
      Object value = this.cache.get( key );
      if ( this.fine ) {
        this.log.fine( "Got key " + key + "=" + value );
      }
      return value;
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#invalidate}.
   */
  public void invalidate( Object key )
  throws DistCacheException {
    try {
      this.cache.put( key, null );
      if ( this.fine ) {
        this.log.fine( "Invalidated key " + key );
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#localInvalidate}.
   */
  public void localInvalidate( Object key )
  throws DistCacheException {
    try {
      this.cache.put( key, null );
      if ( this.fine ) {
        this.log.fine( "Invalidated key " + key );
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#destroy}.
   */
  public void destroy( Object key )
  throws DistCacheException {
    try {
      this.cache.remove( key );
      if ( this.fine ) {
        this.log.fine( "Destroyed key " + key );
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#localDestroy}.
   */
  public void localDestroy( Object key )
  throws DistCacheException {
    try {
      this.cache.remove( key );
      if ( this.fine ) {
        this.log.fine( "Destroyed key " + key );
      }
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#size}.
   */
  public int size()
  throws DistCacheException {
    // @todo lises make sure this only includes entries in the local cache
    try {
      return this.cache.size();
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }
}

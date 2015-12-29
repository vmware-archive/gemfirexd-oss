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

package distcache.gemfire;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;

import distcache.*;
import hydra.*;

import java.util.*;

/**
 *  Implements the {@link distcache.DistCache} interface.  Holds onto a
 *  {@link com.gemstone.gemfire.cache.Region}.
 */
public class GemFireCacheTestImpl implements DistCache {

  private String cacheConfig;
  private String regionConfig;
  private String bridgeConfig;
  private String senderConfig;
  private String receiverConfig;
  private String asyncEventQueueConfig;
  private String cacheName;
  private String regionName;

  private Region region;
  private InterestResultPolicy policy;
  private CacheTransactionManager tm = null;

  private LogWriter log;
  private boolean fine;

  ////  CONSTRUCTORS  //////////////////////////////////////////////////////////

  /**
   *  Creates a {@link DistCache} backed by a {@link com.gemstone.gemfire.cache.Region}.
   */
  public GemFireCacheTestImpl() {
    this.log = Log.getLogWriter();
    this.fine = this.log.fineEnabled();
  }

  ////  INTERFACE METHODS  /////////////////////////////////////////////////////

  /**
   *  See {@link distcache.DistCache#createCache}.
   */
  public void createCache()
  throws DistCacheException {
    this.cacheConfig = ConfigPrms.getCacheConfig();
    Cache cache = CacheHelper.createCache(cacheConfig);
  }

  /**
   *  See {@link distcache.DistCache#open}.
   */
  public void open()
  throws DistCacheException {
    try {
      this.cacheConfig = ConfigPrms.getCacheConfig();
      this.regionConfig = ConfigPrms.getRegionConfig();
      this.bridgeConfig = ConfigPrms.getBridgeConfig();
      this.senderConfig = ConfigPrms.getGatewaySenderConfig();
      this.receiverConfig = ConfigPrms.getGatewayReceiverConfig();
      this.asyncEventQueueConfig = ConfigPrms.getAsyncEventQueueConfig();
      
      this.cacheName = "GemFireCacheImpl";
      this.regionName = GemFireCachePrms.getRegionName();
      if (this.regionName == null) { // use name from region configuration
        this.regionName = RegionHelper.getRegionDescription(this.regionConfig)
                                      .getRegionName();
      }

      // create the cache
      Cache cache = CacheHelper.createCache(cacheConfig);

      //create gateway senders (this needs to be created before region)      
      if (this.senderConfig != null){
        GatewaySenderHelper.createAndStartGatewaySenders(senderConfig);
      }
      
      //create async event listener
      if (this.asyncEventQueueConfig != null){
        AsyncEventQueueHelper.createAndStartAsyncEventQueue(asyncEventQueueConfig);
      }
        
      // create the region
      this.region = RegionHelper.createRegion(this.regionName, this.regionConfig);

      // create the bridge server, if needed
      if (this.bridgeConfig != null) {
        BridgeHelper.startBridgeServer(this.bridgeConfig);
      }
      
      // create gateway receivers, if needed
      if (this.receiverConfig != null){
        GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
      }

      // set other fields
      this.policy = GemFireCachePrms.getInterestResultPolicy();
      this.tm = cache.getCacheTransactionManager();

    } catch( Exception e ) {
      throw new DistCacheException(e);
    }
  }

  /** 
   *  See {@link distcache.DistCache#close}.
   */
  public void close()
  throws DistCacheException {
    try {
      BridgeHelper.stopBridgeServer();
      CacheHelper.closeCache();
      this.region = null;

    } catch (Exception e) {
      throw new DistCacheException(e);
    }
  }

  /**
   *  See {@link distcache.DistCache#getCacheName}.
   */
  public String getCacheName() {
    return this.cacheName;
  }

  /**
   *  See {@link distcache.DistCache#getCacheName}.
   */
  public Object getCacheTransactionManager() {
    return this.tm;
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
    try {
      if ( this.fine ) {
        this.log.fine( "Creating key " + key );
      }
      this.region.create( key, null );
      if ( this.fine ) {
        this.log.fine( "Created key " + key );
      }
    } catch( EntryExistsException e ) {
      throw new DistCacheException( key + " already exists", e );
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   *  See {@link distcache.DistCache#create}.
   */
  public void create( Object key, Object value )
  throws DistCacheException {
    try {
      if ( this.fine ) {
        this.log.fine( "Creating key " + key + "=" + value );
      }
      this.region.create( key, value );
      if ( this.fine ) {
        this.log.fine( "Created key " + key + "=" + value );
      }
    } catch( EntryExistsException e ) {
      throw new DistCacheException( key + " already exists", e );
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
      if ( this.fine ) {
        this.log.fine( "Putting key " + key + "=" + value );
      }
      this.region.put( key, value );
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
    throw new UnsupportedOperationException( "Not implemented yet" );
  }

  /**
   *  See {@link distcache.DistCache#get}.
   */
  public Object get( Object key )
  throws DistCacheException {
    try {
      if ( this.fine ) {
        this.log.fine( "Getting key " + key );
      }
      Object value = this.region.get( key );
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
      if ( this.fine ) {
        this.log.fine( "Locally getting key " + key );
      }
      Object value = this.region.getEntry( key ).getValue();
      if ( this.fine ) {
        this.log.fine( "Locally got key " + key + "=" + value );
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
      if ( this.fine ) {
        this.log.fine( "Invalidating key " + key );
      }
      this.region.invalidate( key );
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
      if ( this.fine ) {
        this.log.fine( "Locally invalidating key " + key );
      }
      this.region.localInvalidate( key );
      if ( this.fine ) {
        this.log.fine( "Locally invalidated key " + key );
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
      if ( this.fine ) {
        this.log.fine( "Destroying key " + key );
      }
      this.region.destroy( key );
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
      if ( this.fine ) {
        this.log.fine( "Locally destroying key " + key );
      }
      this.region.localDestroy( key );
      if ( this.fine ) {
        this.log.fine( "Locally destroyed key " + key );
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
    try {
      return this.region.size();
    } catch( Exception e ) {
      throw new DistCacheException( e );
    }
  }

  /**
   * Returns the <code>Region</code> that underlies this
   * <code>GemFireCacheImpl</code>. 
   *
   * @since 3.5
   */
  public Region getRegion() {
    return this.region;
  }

  //----------------------------------------------------------------------------
  //  Register interest
  //----------------------------------------------------------------------------

  /**
   * Registers interest in the key or list of keys using the result policy
   * configured via {@link GemFireCachePrms#interestResultPolicy}.
   */
  public void registerInterest(Object keyOrList, boolean durable) {
    if (this.fine) {
      if (durable) {
        this.log.fine("Registering durable interest in list/key " + keyOrList);
      } else {
        this.log.fine("Registering interest in list/key " + keyOrList);
      }
    }
    this.region.registerInterest(keyOrList, this.policy, durable);
    if (durable) {
      CacheHelper.getCache().readyForEvents();
    }
    if (this.fine) {
      if (durable) {
        this.log.fine("Registered durable interest in list/key " + keyOrList);
      } else {
        this.log.fine("Registered interest in list/key " + keyOrList);
      }
    }
  }

  /**
   * Registers interest in the key or list of keys using the specified regular
   * expression and the result policy configured via {@link
   * GemFireCachePrms#interestResultPolicy}.
   */
  public void registerInterestRegex(String regex, boolean durable) {
    if (this.fine) {
      if (durable) {
        this.log.fine("Registering durable interest in regex " + regex);
      } else {
        this.log.fine("Registering interest in regex " + regex);
      }
    }
    this.region.registerInterestRegex(regex, this.policy, durable);
    if (durable) {
      CacheHelper.getCache().readyForEvents();
    }
    if (this.fine) {
      if (durable) {
        this.log.fine("Registered durable interest in regex " + regex);
      } else {
        this.log.fine("Registered interest in regex " + regex);
      }
    }
  }
}

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

package util;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.cache.query.QueryService;
import hydra.*;
import java.util.*;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 *
 *  Provides convenience methods for cache and region operations.
 *  Intended for use by hydra client VMs.
 *
 */

public class CacheUtil {

  /** The default region attributes */
  private static final RegionAttributes DEFAULT_ATTRIBUTES = 
    (new AttributesFactory()).createRegionAttributes();

  // cache created by this vm
  private static Cache TheCache;

  /**
   *  Creates and returns a {@link Cache}.  Returns the existing cache if it was
   *  previously created and is still open.  This method is synchronized and is
   *  safe for use by concurrent threads.
   *  <p>
   *  The distributed system used is the one specified for the hydra client VM
   *  via {@link ClientPrms#gemfireNames}.
   *
   *  @throws HydraRuntimeException if there is a problem creating the cache.
   *  @return the created cache, or the existing cache if was previously created.  */
  public static Cache createCache(String cacheXmlFile) {
    if ( TheCache == null ) {
      _createCache(cacheXmlFile); 
    }
    return TheCache;
  }

  /**
   *  Creates and returns a {@link Cache}.  Returns the existing cache if it was
   *  previously created and is still open.  This method is synchronized and is
   *  safe for use by concurrent threads.
   *  <p>
   *  The distributed system used is the one specified for the hydra client VM
   *  via {@link ClientPrms#gemfireNames}.
   *
   *  @throws HydraRuntimeException if there is a problem creating the cache.
   *  @return the created cache, or the existing cache if was previously created.  */
  public static Cache createCache() {
    return createCache(null);
  }

  /*  Initialize the cache.
   */
  public static void setCache( Cache c ) {
    TheCache = c;
  }

  private static synchronized void _createCache(String cacheXmlFile) {
    if ( TheCache == null ) {
      DistributedSystem ds = DistributedConnectionMgr.connect(cacheXmlFile);
      try {
        TheCache = CacheFactory.create( ds );
        int searchTimeout = TestConfig.tab().intAt(CachePrms.searchTimeout, -1);
        if (searchTimeout > 0) {
          TheCache.setSearchTimeout(searchTimeout);
        }
      } catch( CacheException e ) {
        String s = "Unable to create cache using: " + ds;
	throw new HydraRuntimeException( s, e );
      }
    } else {
      if (cacheXmlFile != null)
         throw new HydraRuntimeException("Cannot honor " + cacheXmlFile + " because the cache is already open");
    }
  }

  /**
   *  Returns the {@link Cache} for this VM, if any, or null if it does not exist.
   */
  public static Cache getCache() {
    return TheCache;
  }

  /**
   *  Closes the {@link Cache}.  Does nothing if the cache does not exist.
   */
  public static synchronized void closeCache() {
    if ( TheCache != null ) {
      TheCache.close();
      TheCache = null;
    }
  }
  
  
  /**
   *  Disconnect from the distributed system.
   */
  public static synchronized void disconnect() {
     DistributedConnectionMgr.disconnect();
     TheCache = null;
  }

  /**
   * Returns a collection of the current distribution members
   * @see com.gemstone.gemfire.distributed.DistributedMember
   */
  public static java.util.Set getMembers() {
    return _getDM().getNormalDistributionManagerIds();
  }
  
  /**
   * Returns the DistributedMember for the current cache
   */
  public static DistributedMember getMemberId() {
    return _getDM().getDistributionManagerId();
  }
  
  
  /** return the distribution manager for the current cache (TheCache) */
  private static com.gemstone.gemfire.distributed.internal.DM _getDM() {
    return
      (
        (com.gemstone.gemfire.distributed.internal.InternalDistributedSystem)
          (
            (com.gemstone.gemfire.internal.cache.GemFireCacheImpl)TheCache
          )
          .getDistributedSystem()
      )
      .getDistributionManager();
  }
  
  /**
   * Returns true if the given DistributedMember is admin-only
   */
  public static boolean isAdminOnlyMember(DistributedMember mbr) {
    return ((InternalDistributedMember)mbr).getVmKind() == DistributionManager.ADMIN_ONLY_DM_TYPE;
  }

  /**
   * Returns true if the given DistributedMember is a locator
   */
  public static boolean isLocatorMember(DistributedMember mbr) {
    return ((InternalDistributedMember)mbr).getVmKind() == DistributionManager.LOCATOR_DM_TYPE;
  }
  
  /**
   * Returns true if the given DistributedMember is normal (not admin or loner)
   */
  public static boolean isNormalMember(DistributedMember mbr) {
    return ((InternalDistributedMember)mbr).getVmKind() == DistributionManager.NORMAL_DM_TYPE;
  }
  
  /**
   * Returns true if the given DistributedMember is loner (no peer-to-peer cache)
   */
  public static boolean isLonerMember(DistributedMember mbr) {
    return ((InternalDistributedMember)mbr).getVmKind() == DistributionManager.LONER_DM_TYPE;
  }
  
  
  
  /**
   *  Creates and returns a {@link Region} with the specified simple name
   *  and default region attributes in the cache, if one does not already
   *  exist with the same name and attributes.  This method is synchronized
   *  and is safe for use by concurrent threads.
   *
   *  @param regionName the name of the region.
   *  @param regionType the type of the region.
   *  @throws HydraRuntimeException if the cache has not been created, or
   *          there is a problem creating the region, or
   *          the region already exists but has different attributes.
   *  @return the region.
   */
  public static synchronized Region createRegion( String regionName ) {
    return createRegion( regionName, null );
  }


  /**
   *  Creates and returns a root {@link Region} with the specified simple name
   *  and region attributes in the cache, if one does not already exist
   *  with the same name and attributes.  This method is synchronized and is safe
   *  for use by concurrent threads.
   *
   *  @param regionName the name of the region.
   *  @param regionAttributes the attributes for the region.
   *  @throws HydraRuntimeException if the cache has not been created, or
   *          there is a problem creating the region, or
   *          the region already exists but has different attributes.
   *  @return the region.
   */
  public static synchronized Region createRegion( String regionName,
                                                  RegionAttributes regionAttributes ) {
    if ( TheCache == null )
      throw new HydraRuntimeException( "The cache has not been created" );
    if ( regionName == null )
      throw new IllegalArgumentException( "Region name is null" );

    try {
      if ( regionAttributes == null ) {
	regionAttributes = DEFAULT_ATTRIBUTES;
      }
      return TheCache.createVMRegion( regionName, regionAttributes );
    } catch( CacheException e ) {
      throw new HydraRuntimeException( "Unable to create region \"" + regionName + "\"", e );
    }
  }

  /**
   *  Creates and returns a {@link Region} with the specified simple name
   *  and region attributes as a subregion of the specified region, if one does
   *  not already exist with the same name and attributes.  This method is
   *  synchronized and is safe for use by concurrent threads.
   *
   *  @param parentRegion the region in which to create the subregion.
   *  @param regionName the name of the region.
   *  @param regionAttributes the attributes for the region.
   *  @throws HydraRuntimeException if there is a problem creating the region or if
   *          the region already exists but has different attributes.
   *  @return the region.
   */
  public static synchronized Region createRegion( Region parentRegion,
                                                  String regionName,
                                                  RegionAttributes regionAttributes ) {
    if ( parentRegion == null )
      throw new IllegalArgumentException( "Parent region is null" );

    if ( regionName == null )
      throw new IllegalArgumentException( "Region name is null" );

    try {
      if ( regionAttributes == null )
        return parentRegion.createSubregion( regionName, DEFAULT_ATTRIBUTES );
      else
        return parentRegion.createSubregion( regionName, regionAttributes );

    } catch( RegionExistsException e ) {
        Region region = null;
        region = parentRegion.getSubregion( regionName );

        if (region == null) {
          throw new HydraRuntimeException( "Unable to get region \"" + regionName + "\"" );
        }

        if ( ! region.getAttributes().equals( regionAttributes ) )
          throw new HydraRuntimeException( "Region \"" + regionName + "\" already exists with different attributes" );

	return region;

    } 
//    catch( CacheException e ) {
//      throw new HydraRuntimeException( "Unable to create region \"" + regionName + "\"", e );
//    }
  }

/** Create a region either by creating the cache with a declarative xml file
 *  or create the region programmatically. Which method chosen depends on the
 *  hydra parameter CachePrms.useDeclarativeXmlFile and whether this VM already
 *  has a connection to the distributed system.
 *
 *  @param cacheDef The CacheDefinition to use to create the cache.
 *  @param regDef The RegionDefinition to use if the region is created
 *         programmatically.
 *  @param xmlFile The xmlFile to use if the region is created with the xml file.
 *
 *  Note: This can only honor CachePrms.useDeclarativeXmlFile or the xmlFile
 *        parameter if we are not already connected. If we already have a connection 
 *        to the distributed system, then creating the cache will use any declarative
 *        xml file used when the connection was established, if any declarative xml file
 *        was set during the connect. See the comments in the code below.
 *
 */ 
public static Region createRegion(CacheDefinition cacheDef, RegionDefinition regDef, String xmlFile) {
   Region createdRegion = null;
   if (DistributedConnectionMgr.isConnected()) {
      // we are already connected to the distributed system, thus the declarative
      // file (if any) was already "set" when we connected; at this point, all we
      // can do is open the cache and if there was a declarative xml file set already
      // then the region will be created when the cache is opened, otherwise we must
      // create the region programmatically
      Cache cache = cacheDef.createCache();
      createdRegion = CacheUtil.getRegion(regDef.getRegionName());
      if (createdRegion == null)
         createdRegion = regDef.createRootRegion(cache, null, null, null, null);
   } else { // not connected; we have the opportunity to "set" any declarative, if desired
      if (TestConfig.tab().booleanAt(CachePrms.useDeclarativeXmlFile)) {
         // create the region using a declarative xml file; creating the cache creates the region
         // when an xml file is passed in, assuming the xmlFile specifies a region
         Log.getLogWriter().info("Creating " + regDef.getRegionName() + " with " + xmlFile);
         cacheDef.createCache(xmlFile);
         createdRegion = CacheUtil.getRegion(regDef.getRegionName());
         if (createdRegion == null)
            throw new TestException("Unexpected null region");
         Log.getLogWriter().info("Finished creating " + TestHelper.regionToString(createdRegion, true) +
             " by using " + xmlFile);
      } else { // create the region programmatically
         createdRegion = regDef.createRootRegion(cacheDef.createCache(), null, null, null, null);
      }
   } 
   return createdRegion;
}

/** Create a region either by creating the cache with a declarative xml file
 *  or create the region programmatically. Which method chosen depends on the
 *  hydra parameter CachePrms.useDeclarativeXmlFile and whether this VM already
 *  has a connection to the distributed system.
 *
 *  @param cacheDescriptName The name of the cache description to use to create 
 *         the cache.
 *  @param regionDescriptName The name of the region description to use if the 
 *         region is created programmatically.
 *  @param xmlFile The xmlFile to use if the region is created with the xml file.
 *
 *  Note: This can only honor CachePrms.useDeclarativeXmlFile or the xmlFile
 *        parameter if we are not already connected. If we already have a connection 
 *        to the distributed system, then creating the cache will use any declarative
 *        xml file used when the connection was established, if any declarative xml file
 *        was set during the connect. See the comments in the code below.
 *
 */ 
public static Region createRegion(String cacheDescriptName, String regionDescriptName, String xmlFile) {
   Region createdRegion = null;
   RegionDescription regDescript = RegionHelper.getRegionDescription(regionDescriptName);
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   if ((ds != null) && ds.isConnected()) {
      Cache theCache = CacheHelper.getCache();
      if (theCache == null) {
         // we are already connected to the distributed system, thus the declarative
         // file (if any) was already "set" when we connected; at this point, all we
         // can do is open the cache and if there was a declarative xml file set already
         // then the region will be created when the cache is opened, otherwise we must
         // create the region programmatically
         theCache = CacheHelper.createCache(cacheDescriptName);
         createdRegion = theCache.getRegion(regDescript.getRegionName());
         if (createdRegion == null) {
            createdRegion = RegionHelper.createRegion(regionDescriptName);
         }
      } else { // we are connected and already have a cache, just create the region
         createdRegion = RegionHelper.createRegion(regionDescriptName);
      }
   } else { // not connected; we have the opportunity to "set" any declarative, if desired
      if (TestConfig.tab().booleanAt(util.CachePrms.useDeclarativeXmlFile)) {
         // create the region using a declarative xml file; creating the cache creates the region
         // when an xml file is passed in, assuming the xmlFile specifies a region
         Log.getLogWriter().info("Creating " + regDescript.getRegionName() + " with " + xmlFile);
         Cache theCache = CacheHelper.createCacheFromXml(xmlFile);
         if (theCache == null)
            throw new TestException("Unexpected null cache");
         createdRegion = theCache.getRegion(regDescript.getRegionName());
         if (createdRegion == null)
            throw new TestException("Unexpected null region");
         Log.getLogWriter().info("Finished creating " + createdRegion.getFullPath() +
             " by using " + xmlFile);
      } else { // create the region programmatically
         Cache theCache = CacheHelper.createCache(cacheDescriptName);
         createdRegion = RegionHelper.createRegion(regionDescriptName);
      }
   } 
   return createdRegion;
}

/** Create a region either by creating the client cache with a declarative xml file
 *  or create the region programmatically. Which method chosen depends on the
 *  hydra parameter CachePrms.useDeclarativeXmlFile and whether this VM already
 *  has a connection to the distributed system.
 *
 *  @param cacheDescriptName The name of the client cache description to use to create 
 *         the cache.
 *  @param regionDescriptName The name of the client region description to use if the 
 *         region is created programmatically.
 *  @param xmlFile The xmlFile to use if the region is created with the xml file.
 *
 *  Note: This can only honor CachePrms.useDeclarativeXmlFile or the xmlFile
 *        parameter if we are not already connected. If we already have a connection 
 *        to the distributed system, then creating the cache will use any declarative
 *        xml file used when the connection was established, if any declarative xml file
 *        was set during the connect. See the comments in the code below.
 *
 */ 
public static Region createClientRegion(String cacheDescriptName, String regionDescriptName, String xmlFile) {
   Region createdRegion = null;
   ClientRegionDescription regDescript = ClientRegionHelper.getClientRegionDescription(regionDescriptName);
   DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
   if ((ds != null) && ds.isConnected()) {
      ClientCache theCache = ClientCacheHelper.getCache();
      if (theCache == null) {
         // we are already connected to the distributed system, thus the declarative
         // file (if any) was already "set" when we connected; at this point, all we
         // can do is open the cache and if there was a declarative xml file set already
         // then the region will be created when the cache is opened, otherwise we must
         // create the region programmatically
         theCache = ClientCacheHelper.createCache(cacheDescriptName);
         createdRegion = theCache.getRegion(regDescript.getRegionName());
         if (createdRegion == null) {
            createdRegion = ClientRegionHelper.createRegion(regionDescriptName);
         }
      } else { // we are connected and already have a cache, just create the region
         createdRegion = ClientRegionHelper.createRegion(regionDescriptName);
      }
   } else { // not connected; we have the opportunity to "set" any declarative, if desired
      if (TestConfig.tab().booleanAt(util.CachePrms.useDeclarativeXmlFile)) {
         // create the region using a declarative xml file; creating the cache creates the region
         // when an xml file is passed in, assuming the xmlFile specifies a region
         Log.getLogWriter().info("Creating " + regDescript.getRegionName() + " with " + xmlFile);
         ClientCache theCache = ClientCacheHelper.createCacheFromXml(xmlFile);
         if (theCache == null)
            throw new TestException("Unexpected null client cache");
         createdRegion = theCache.getRegion(regDescript.getRegionName());
         if (createdRegion == null)
            throw new TestException("Unexpected null region");
         Log.getLogWriter().info("Finished creating " + createdRegion.getFullPath() +
             " by using " + xmlFile);
      } else { // create the region programmatically
         ClientCache theCache = ClientCacheHelper.createCache(cacheDescriptName);
         createdRegion = ClientRegionHelper.createRegion(regionDescriptName);
      }
   } 
   return createdRegion;
}

  /**
   *  Returns the {@link Region} with the specified simple or global name
   *  from the cache.
   *
   *  @param regionName the name of the region.
   *  @return the region, or null if it does not exist.
   */
  public static Region getRegion( String regionName ) {
    if ( TheCache == null )
      throw new HydraRuntimeException( "The cache has not been created" );
    if ( regionName == null )
      throw new IllegalArgumentException( "Region name is null" );

    return TheCache.getRegion( regionName );
  }

  /**
   *  Returns the {@link Region} with the specified simple or global name
   *  from the specified parent region.
   *
   *  @param region the parent region from which to get the region.
   *  @param regionName the name of the region.
   *  @return the region, or null if it does not exist.
   */
  public static Region getRegion( Region region,
                                  String regionName ) {
    if ( region == null )
      throw new IllegalArgumentException( "Region is null" );
    if ( regionName == null )
      throw new IllegalArgumentException( "Region name is null" );

    return region.getSubregion( regionName );
  }
  /**
   *  Returns the QueryService from the current cache
   *
   *  @return the QueryService.
   */
  public  static QueryService getQueryService(){
    if ( TheCache == null )
      throw new HydraRuntimeException( "The cache has not been created" );
    return TheCache.getQueryService();
  }
  /**
   *  Returns the object with the specified name from the specified {@link Region}.
   *
   *  @param region the region from which to get the object.
   *  @param name the name of the object to get.
   *  @return the object, or null if it is not found.
   */
  public static Object get( Region region,
                            Object name ) {
    if ( region == null )
      throw new IllegalArgumentException( "Region is null" );
    if ( name == null )
      throw new IllegalArgumentException( "Name is null" );

    try {
      return region.get( name );

    } catch( Exception e ) {
      String s = "Unable to get object \"" + region.getFullPath()
        + Region.SEPARATOR + name + "\"";
      throw new HydraRuntimeException( s, e );
    }
  }

  /**
   *  Puts the specified object at the specified name in the specified
   *  {@link Region}
   *
   *  @param region the region in which to put the object.
   *  @param name the name of the object.
   *  @param obj the object to put at the name.
   */
  public static void put( Region region,
                          Object name,
			  Object obj ) {
    if ( region == null )
      throw new IllegalArgumentException( "Region is null" );
    if ( name == null )
      throw new IllegalArgumentException( "Name is null" );
    if ( obj == null )
      throw new IllegalArgumentException( "Object is null" );

    try {
      region.put( name, obj );

    } catch( Exception e ) {
      String s = "Unable to put object \"" + region.getFullPath() +
        Region.SEPARATOR + name + "\"" + " with " + obj;
      throw new HydraRuntimeException( s, e );
    }
  }


  /**
   *  Replaces the object with the specified name from the specified {@link Region}
   *  with a different object.  Complains if any cache-related exception occurs.
   *
   *  @param region the region from which to replace the object.
   *  @param name the name of the object to replace.
   *  @param obj the object to install at the name.
   *
   *  @throws HydraRuntimeException if any CacheException occurs.
   *
   * @see #put(Region, Object, Object)
   */
  public static void replace( Region region,
                              Object name,
			      Object obj ) {
    if ( region == null )
      throw new IllegalArgumentException( "Region is null" );
    if ( name == null )
      throw new IllegalArgumentException( "Name is null" );
    if ( obj == null )
      throw new IllegalArgumentException( "Object is null" );

    try {
      region.put( name, obj );

    } catch( Exception e ) {
      String s = "Unable to replace object \"" + region.getFullPath() +
        Region.SEPARATOR + name + "\"" + " with " + obj;
      throw new HydraRuntimeException( s, e );
    }
  }

  /**
   *  Replaces the object with the specified name from the specified {@link Region}
   *  with a different object.  Gives the application a chance to retry on stale
   *  reads and ownership timeouts.
   *
   *  @param region the region from which to replace the object.
   *  @param name the name of the object to replace.
   *  @param obj the object to install at the name.
   *
   *  @throws StaleReadException if a stale read occurs due to contention, to give
   *                             the app a chance to retry.
   *  @throws OwnershipException if a timeout occurs getting ownership, to give
   *                             the app a chance to retry.
   *  @throws HydraRuntimeException if any other CacheException occurs.
   */
  public static void recoverableReplace( Region region,
                                         Object name,
			                 Object obj ) {
    if ( region == null )
      throw new IllegalArgumentException( "Region is null" );
    if ( name == null )
      throw new IllegalArgumentException( "Name is null" );
    if ( obj == null )
      throw new IllegalArgumentException( "Object is null" );

    String s = "Don't know how to implement this behavior in 3.0";
    throw new UnsupportedOperationException(s);
  }

  /**
   *  Gets the prettified contents of the specified region as a string.
   *
   *  @param region the region whose contents to print.
   *  @param recursive whether or not to print subregions recursively.
   */
  public static String getContentString( Region region, boolean recursive ) {
    if ( region == null )
      throw new IllegalArgumentException( "Region is null" );
    StringBuffer buf = new StringBuffer();
    Set set = region.entries( recursive );

    for ( Iterator i = set.iterator(); i.hasNext(); ) {
      Region.Entry entry = (Region.Entry) i.next();
      buf.append( "[" );
      buf.append( entry.getRegion().getFullPath() );
      buf.append( "]" );
      buf.append( entry.getKey() ).append( "=" );
      buf.append( entry.getValue() ).append( "\n" );
    }
    return buf.toString();
  }

  /**
   *  Defines an entry in the specified region with the specified
   *  name, using the default attributes for the region.  Note that in
   *  3.0, this method does a <code>region.create(name, null)</code>.
   *
   * @param region the region in which to define the object entry.
   * @param name the name to give the object entry.
   */
  public static void defineObjectAttributes( Region region, Object name ) {
    try {
      region.create( name, null );

    } catch ( CacheException e ) {
      String s = "Unable to define object attributes for \"" +
        region.getFullPath() + Region.SEPARATOR + name + "\"";
      throw new HydraRuntimeException( s, e );
    }
  }

  /**
   *  Returns the names (keys) of a given <code>Region</code>
   *
   * @param region the region on which to call keys()
   * @param aBool 
   *        Whether or not to recurse over subregions.  Note that this
   *        functionality is not support in GemFire 3.0.
   *
   * @see Region#keys
   */
  public static Set nameSet( Region region, boolean aBool ) {
    if (aBool) {
      String s = "Recursive name set is not supported in 3.0";
      throw new UnsupportedOperationException(s);
    }

    return region.keys();
  }

  /**
   *  Calls subregions() on the given region.
   *
   * @param region the region on which to call subregions()
   * @param aBool the argument to subregions()
   */
  public static Set regionSet( Region region, boolean aBool ) {
    return region.subregions( aBool );
  }

  /**
   *  Calls getRegionAttributes() on the given region.
   *
   * @param region the region on which to call getRegionAttributes()
   */
  public static RegionAttributes getRegionAttributes( Region region ) {
    return region.getAttributes( );
  }

  /**
   *  Calls invalidateRegion() on the given region.
   *
   * @param region the region on which to call invalidateRegion()
   */
  public static void invalidateRegion( Region region ) {
   try {
      region.invalidateRegion( );
   } catch ( Exception e ) {
      throw new HydraRuntimeException( TestHelper.getStackTrace(e));
    }
  }

  /**
   *  Calls destroyRegion() on the given region.
   *
   * @param region the region on which to call destroyRegion()
   */
  public static void destroyRegion( Region region ) {
    try {
      region.destroyRegion( );
    } catch ( Exception e ) {
      throw new HydraRuntimeException( TestHelper.getStackTrace(e));
    }
  }

}

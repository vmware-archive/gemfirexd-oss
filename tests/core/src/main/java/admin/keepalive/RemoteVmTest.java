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
package admin.keepalive;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.admin.*;
import java.util.*;

import hydra.HydraRuntimeException;
import hydra.Log;
import util.*;

import admin.*;
import cacheLoader.smoke.*;

/**
 *
 * Basic test of using Admin API to create regions in remote VM. 
 * The tasks for for preloading, reading value, and invalidating
 * are from cacheLoader.smoke.Client
 *
 * @since 4.0
 */
public class RemoteVmTest extends CacheServerAdminTest {

  // the single instance of this test class
  protected static RemoteVmTest remoteVmTest = null;

  protected static final String REGION_NAME = cacheLoader.smoke.Util.REGION_NAME;
  protected static final String MASTER_REGION_NAME = cacheLoader.smoke.Util.MASTER_REGION_NAME;
  protected static final String CACHED_REGION_NAME = cacheLoader.smoke.Util.CACHED_REGION_NAME;;

  /**
   * Hydra task to initialize an  for hosting a
   * <code>Cache</code> and for admin of the distributed system in the 
   * same VM.
   */
  public synchronized static void initializeForAdminTask() {
    if (remoteVmTest == null) {
      remoteVmTest = new RemoteVmTest();
      CacheUtil.createCache();
      remoteVmTest.initializeForAdmin();
    }
  }

/* task to add, configure and start a dedicated cache server
    in a remote VM.
 */
 public static void startCacheServerTask() {
   remoteVmTest.startCacheServer();
  }


  /** 
   * Create the cache and define regions and entries,
   * including setting the cache loader for CachedData.
   */
  public static void defineCacheServerRegionsTask() {
    remoteVmTest.defineCacheServerRegions();
  }

  private void defineCacheServerRegions() {
    CacheVm[] cacheServers = null;
    if (admin == null) log().info("admin is null");
    try {
	cacheServers = admin.getCacheVms();
    } catch (AdminException ae) {
     throw new HydraRuntimeException("Could not get Cache Servers");
    }
    if (cacheServers.length == 0) {
	log().info("Admin found 0 configured cache servers");
    } 
    else {
	log().info("Admin found: " + cacheServers.length + " cacheServers");
    }

    for (int i=0; i < cacheServers.length; i++) {
      defineCacheRegions(cacheServers[i]);
    }
  }

    /* 
     * Create the regions in the CacheServer Cache
     */
    private void defineCacheRegions(CacheVm cacheServer) {

      boolean defineCacheLoader = AdminPrms.getDefineCacheLoaderRemote();
      log().info("getDefineCacheLoader returned: " + defineCacheLoader); 
      SystemMemberCache cache;
      try {
	cache = cacheServer.getCache();
      } catch (AdminException ae) {
	throw new TestException("Unable to get CacheServer cache", ae);
      }

      // Root and Master region use config values for
      // scope, mirror type, and keepalive only
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(CacheBB.getBB().getScopeAttribute());
      factory.setMirrorType(MirrorType.KEYS_VALUES);

      RegionAttributes regAttrs =
		factory.createRegionAttributes();

      // create root region
      SystemMemberRegion root;
      try {
        root = cache.createVMRegion(REGION_NAME, regAttrs);
      } catch (AdminException ae) {
	throw new TestException("Unable to create region: " + REGION_NAME);
      }

      // create sub region use same region attrbutes as root
      SystemMemberRegion master;
       try {
        master = root.createSubregion(MASTER_REGION_NAME, regAttrs);
      } catch (AdminException ae) {
	throw new TestException("Unable to create sub region: " + MASTER_REGION_NAME);
      }

      // set region attributes for sub region CACHED_REGION_NAME
      
      AttributesFactory factory2 = new AttributesFactory();
      factory2.setScope(CacheBB.getBB().getScopeAttribute());
      ObjectListener objEventListnr = new ObjectListener();
      factory2.setCacheListener(objEventListnr);
      factory2.setCacheWriter(objEventListnr);
      if (defineCacheLoader) {
	CacheLoader cacheLoader = new Loader();
	factory2.setCacheLoader(cacheLoader);
	factory2.setMirrorType(MirrorType.NONE);
      }
      else {
	factory2.setMirrorType(MirrorType.KEYS_VALUES);
      }

      SystemMemberRegion cachedRegion;
      try {
	cachedRegion = root.createSubregion(CACHED_REGION_NAME,
				            factory2.createRegionAttributes());
      } catch (AdminException ae) {
	throw new TestException("Unable to create sub region: " + CACHED_REGION_NAME);
      }

      log().info("Regions created... debug:");
      regionDebug(cacheServer);

    }

    private void regionDebug(SystemMember systemMember){
     SystemMemberCache cache;
     try {
       cache = systemMember.getCache();
     } catch (Exception e) {
       throw new TestException ("Could get System Member Cache for region debug",e);
     }
     cache.refresh();
     Set rootRegionNames = cache.getRootRegionNames();
     log().info("DEBUG Found " + rootRegionNames.size()+" root regions");
     for ( Iterator it = rootRegionNames.iterator(); it.hasNext(); ) {
       SystemMemberRegion region;
       try {
         region = cache.getRegion((String)it.next());
         Log.getLogWriter().info( "DEBUG Root region = " + region.getName());
         log().info("CacheLoader: " + region.getCacheLoader());
         log().info("CacheWriter: " + region.getCacheWriter());
         log().info("InitialCapacity: " + region.getInitialCapacity());
         log().info("Concurrency Level: " + region.getConcurrencyLevel());
         log().info("scope: " + region.getScope());
         log().info("DataPolicy: " + region.getDataPolicy().toString());

       } catch (AdminException ae) {
	   throw new TestException ("Failed to get root region: " + (String)it.next());
       }

     } 
    }  



}

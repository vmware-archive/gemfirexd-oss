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

package hydra;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;
import java.util.Map;
import java.util.Set;

/**
 * Provides version-dependent support for {@link CacheHelper}.
 */
public class CacheVersionHelper {

  /**
   * Version-dependent support for configuring a cache factory from the
   * the given cache configuration and distributed system.
   */
  protected static Cache configureAndCreateCache(String cacheConfig,
                   CacheDescription cd, DistributedSystem ds) {
    // configure the cache factory
    CacheFactory f = new CacheFactory();
    Log.getLogWriter().info("Configuring cache factory from config: "
                           + cacheConfig);
    cd.configure(f);

    // create the cache
    Log.getLogWriter().info("Creating cache");
    return f.create();
  }

  /**
   * Version-dependent support for generating a dummy disk store from
   * the given cache and disk store configuration.
   */
  protected static void generateDummyDiskStore(Cache dummyCache,
                                     String diskStoreConfig, String fn,
                                     Map XmlDiskStoreConfigs) {
    DiskStoreDescription dsd =
             DiskStoreHelper.getDiskStoreDescription(diskStoreConfig);
    DiskStoreFactory dummyFactory =
                     ((CacheCreation)dummyCache).createDiskStoreFactory();
    dsd.configure(dummyFactory);
    String diskStoreName = dsd.getName();
    DiskStore dummyDiskStore = null;
    try {
      dummyDiskStore = dummyFactory.create(diskStoreName);
    } catch (IllegalStateException e) {
      String msg = "A disk store named \"" + diskStoreName
                 + "\" already exists.";
      if (e.getMessage().equals(msg)) {
        if (!XmlDiskStoreConfigs.values().contains(diskStoreConfig)) {
          String s = "DiskStore with configuration " + diskStoreConfig
                   + " named " + diskStoreName
                   + " was already created without CacheHelper XML using"
                   + " an unknown, and possibly different, configuration";
          throw new HydraRuntimeException(s);
        } // else the configuration was done here, which is fine
      } else {
        throw e;
      }
    }
    if (Log.getLogWriter().infoEnabled()) {
      Log.getLogWriter().info("Added dummy disk store: "
         + DiskStoreHelper.diskStoreToString(dummyDiskStore));
    }

    // save the disk store config for future reference
    XmlDiskStoreConfigs.put(fn, diskStoreConfig);
  }

  /**
   * Version-dependent support for generating a dummy gateway sender from
   * the given cache and gateway sender configuration.
   */
  protected static void generateDummyGatewaySender(Cache dummyCache,
                                     String gatewaySenderConfig, String fn,
                                     Map XmlGatewaySenderConfigs) {
    String localds = DistributedSystemHelper.getDistributedSystemName();
    GatewaySenderDescription gsd =
           GatewaySenderHelper.getGatewaySenderDescription(gatewaySenderConfig);
    Set<String> remoteDistributedSystems = gsd.getRemoteDistributedSystems();
    if (remoteDistributedSystems.size() > 0) {
      GatewaySenderFactory dummyFactory =
                       ((CacheCreation)dummyCache).createGatewaySenderFactory();
      gsd.configure(dummyFactory);        
      for (String remoteds : remoteDistributedSystems) {
        String id = gsd.getName() + "_" + localds + "_to_" + remoteds;          
        int remoteid = DistributedSystemHelper.getDistributedSystemId(remoteds);
        GatewaySender dummyGatewaySender = null;
        try {
          dummyGatewaySender = dummyFactory.create(id, remoteid);
          GatewaySenderHelper.saveGatewaySenderId(gsd.getName(),
                                                  gsd.getParallel(), id);
        } catch (IllegalStateException e) {
          String msg = "A gateway sender named \"" + id
                     + "\" already exists.";
          if (e.getMessage().equals(msg)) {
            if (!XmlGatewaySenderConfigs.values().contains(gatewaySenderConfig)) {
              String s = "GatewaySender with configuration "
                       + gatewaySenderConfig + " named " + id
                       + " was already created without CacheHelper XML using"
                       + " an unknown, and possibly different, configuration";
              throw new HydraRuntimeException(s);
            } // else the configuration was done here, which is fine
          } else {
            throw e;
          }
        }
        if (Log.getLogWriter().infoEnabled()) {
          Log.getLogWriter().info("Added dummy gateway sender: "
             + GatewaySenderHelper.gatewaySenderToString(dummyGatewaySender));
        }
      }
      // save the gateway sender config for future reference
      XmlGatewaySenderConfigs.put(fn, gatewaySenderConfig);

    } else {
      Log.getLogWriter().info("No remote distributed systems found");
    }
  }
 
  /**
   * Version-dependent support for generating a dummy gateway receiver from
   * the given cache and gateway receiver configuration.
   */
  protected static void generateDummyGatewayReceiver(Cache dummyCache,
                                     String gatewayReceiverConfig, String fn,
                                     Map XmlGatewayReceiverConfigs) {
    GatewayReceiverDescription grd = GatewayReceiverHelper
                        .getGatewayReceiverDescription(gatewayReceiverConfig);
    int numInstances = grd.getNumInstances();
    Log.getLogWriter().info("Creating " + numInstances
       + " dummy gateway receivers for " + grd.getName());  
    GatewayReceiverFactory dummyFactory =
                     ((CacheCreation)dummyCache).createGatewayReceiverFactory();
    grd.configure(dummyFactory);        
    for (int i = 0; i < numInstances; i++) {
      GatewayReceiver dummyGatewayReceiver = dummyFactory.create();
      if (Log.getLogWriter().infoEnabled()) {
        Log.getLogWriter().info("Added dummy gateway receiver: "
           + GatewayReceiverHelper.gatewayReceiverToString(dummyGatewayReceiver));
      }
    }
    // save the gateway receiver config for future reference
    XmlGatewayReceiverConfigs.put(fn, gatewayReceiverConfig);
  }

  /**
   * Version-dependent support for generating a dummy async event queue from
   * the given cache and async event queue configuration.
   */
  protected static void generateDummyAsyncEventQueue(Cache dummyCache,
                                     String asyncEventQueueConfig, String fn,
                                     Map XmlAsyncEventQueueConfigs) {
    AsyncEventQueueDescription aeqd = AsyncEventQueueHelper
                        .getAsyncEventQueueDescription(asyncEventQueueConfig);
    Log.getLogWriter().info("Creating dummy async event queue for "
                           + aeqd.getName());  
    AsyncEventQueueFactory dummyFactory =
              ((CacheCreation)dummyCache).createAsyncEventQueueFactory();
    aeqd.configure(dummyFactory);        
    AsyncEventQueue dummyAsyncEventQueue =
      dummyFactory.create(aeqd.getName(), aeqd.getAsyncEventListenerInstance());
    if (Log.getLogWriter().infoEnabled()) {
      Log.getLogWriter().info("Added dummy async event queue: "
         + AsyncEventQueueHelper.asyncEventQueueToString(dummyAsyncEventQueue));
    }
    // save the async event queue config for future reference
    XmlAsyncEventQueueConfigs.put(fn, asyncEventQueueConfig);
  }

  /**
   * Version-dependent support for {@link CacheHelper#getCache}.
   *
   * Returns the cache if it exists and is open, or null if no cache exists.
   */
  protected static synchronized Cache getCache() {
    try {
      return CacheFactory.getAnyInstance();
    } 
    catch (CancelException e) {
      return null;
    }
  }

  /**
   * Version-dependent support for generating a dummy resource manager.
   */
  protected static void generateDummyResourceManager(CacheDescription cd,
                                                     CacheCreation dummyCache) {
    if (cd.getResourceManagerDescription() != null) {
      ResourceManagerCreation dummyResourceManager =
                              new ResourceManagerCreation();
      dummyCache.setResourceManagerCreation(dummyResourceManager);
    }
  }

  /**
   * Version-dependent support for generating dummy region attributes
   * from the the given cache and attributes factory.
   */
  protected static RegionAttributes getDummyRegionAttributes(
                   Cache dummyCache, RegionAttributes ratts) {
    return new RegionAttributesCreation((CacheCreation)dummyCache, ratts, false);
  }
}

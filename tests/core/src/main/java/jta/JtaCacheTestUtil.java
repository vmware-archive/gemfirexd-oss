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
package jta;

import java.util.*;

import com.gemstone.gemfire.cache.*;

import hydra.*;
import util.*;

/**
 * @author nandk
 *  
 */
public class JtaCacheTestUtil {

  private static final int PUT_OPERATION = 1;
  private static final int GET_OPERATION = 2;
  private static final int UPDATE_OPERATION = 3;

  /** 
   * Init task for bridgeServers working with JTA edge clients 
   */
  public synchronized static void HydraTask_initializeBridgeServer() {
     if (CacheHelper.getCache() == null) {
        Cache c = CacheHelper.createCache(ConfigPrms.getCacheConfig());

        // Install the TransactionListener, if configured
        TransactionListener txListener = JtaPrms.getTxListener();
        if (txListener != null) {
          c.getCacheTransactionManager().setListener(txListener);
          Log.getLogWriter().info("Installed TransactionListener " + txListener);
        }

        // Install the TransactionWriter, if configured
        TransactionWriter txWriter = JtaPrms.getTxWriter();
        if (txWriter != null) {
          ((CacheTransactionManager)c.getCacheTransactionManager()).setWriter(txWriter);
          Log.getLogWriter().info("Installed TransactionWriter " + txWriter);
        }

        RegionHelper.createRegion(ConfigPrms.getRegionConfig());
        createRootRegions();
        BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
     }
  }
  
  /**
   * @param whichPrm
   * @return
   */
  public static int getRandomOperation(Long whichPrm) {
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    if (operation.equals("put"))
      op = PUT_OPERATION;
    else if (operation.equals("update"))
      op = UPDATE_OPERATION;
    else if (operation.equals("get")) op = GET_OPERATION;
    return op;
  }

  /**
   * Returns a random region from the cache.
   * 
   * @param cache
   * @return Region
   */
  public static Region getRandomRegion(Cache cache) {
    return (Region) cache.rootRegions().iterator().next();
  }

  /**
   * Does the random put/get/update operation on region region.
   * 
   * @param region
   * @param total
   */
  public void doRandomOperation(Region region, int total) {
    int count = 0;
    while (count < total) {
      int whichOp = JtaCacheTestUtil.getRandomOperation(JtaPrms.entryOperations);
      switch (whichOp) {
        case PUT_OPERATION:
          doPutOperation(region);
          break;
        case GET_OPERATION:
          doGetOperation(region);
          break;
        case UPDATE_OPERATION:
          doUpdateOperation(region);
          break;
        default: {
          throw new TestException("Unknown operation " + whichOp);
        }
      }
      count++;
    }
  }

  /*
   * picks a random key from the list of keys available for the region. Updates
   * its value to a new one. @param region
   */
  private void doUpdateOperation(Region region) {
    Set aSet = region.keys();
    Iterator iter = aSet.iterator();
    if (!iter.hasNext()) {
      Log.getLogWriter().info("updateObject: No names in region");
      return;
    }
    Object name = iter.next();
    try {
      Object anObj = region.get(name);
      Object newObj = getRandomInteger();
      region.put(name, newObj);
      Log.getLogWriter().info("Updated region = " + region + "   key  " + name + "  old value " + anObj + "   new value " + newObj);
    } catch (Exception e) {
       throw new TestException("Exception in Update Operation" + TestHelper.getStackTrace(e));
    }
    JtaBB.incrementCounter("DoUpdateOperation", JtaBB.NUM_UPDATE);
  }

  private Integer getRandomInteger() {
    return new Integer((new Random()).nextInt(100));
  }

  /*
   * picks a randon key from the list of keys available for the region. Read its
   * value and print it. @param region
   */
  private static void doGetOperation(Region region) {
    Set aSet = region.keys();
    if (aSet.size() == 0) {
      Log.getLogWriter().info("doGetOperation: No names in region");
      return;
    }
    long maxNames = NameFactory.getPositiveNameCounter();
    if (maxNames <= 0) {
      Log.getLogWriter().info("doGetOperation: max positive name counter is " + maxNames);
      return;
    }
    Object name = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int) maxNames));
    Log.getLogWriter().info("doGetOperation: getting name " + name);
    try {
      Object anObj = region.get(name);
      Log.getLogWriter().info("doGetOperation: got value for name " + name + ": " + TestHelper.toString(anObj));
    } catch (CacheLoaderException e) {
       throw new TestException("Exception in doGetOperation " + TestHelper.getStackTrace(e));
    } catch (TimeoutException e) {
       throw new TestException("Exception in doGetOperation " + TestHelper.getStackTrace(e));
    }
    JtaBB.incrementCounter("DoGetOperation", JtaBB.NUM_GET);
  }

  /*
   * Puts a random key , value to the region. @param region
   */
  public void doPutOperation(Region region) {
    String name = NameFactory.getNextPositiveObjectName();
    Integer anObj = getRandomInteger();
    try {
      region.put(name, anObj);
    }
    catch (Exception e) {
      throw new TestException("Exception in Put Operation" + TestHelper.getStackTrace(e));
    }
    JtaBB.incrementCounter("DoPutOperation", JtaBB.NUM_CREATE);
  }

  public static void createRootRegions() {
    // The cache was created via CacheHelper + cachejta.xml (with a region name root)
    // in a synchronized init method.  So, only one thread in the VM will be 
    // executing this at any point in time.

    // get the attributes from the region created with the cache xml
    Cache cache = CacheHelper.getCache();
    int numRegions = cache.rootRegions().toArray().length;

    if (numRegions == 1) {
       // we only have the one region (root), create root (based on regionConfig)
       String regionConfig = ConfigPrms.getRegionConfig();
       Region aRegion = RegionHelper.createRegion(regionConfig);
       // edge clients register interest in ALL_KEYS
       if (aRegion.getAttributes().getPoolName() != null) {
         aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
         Log.getLogWriter().info("registered interest in ALL_KEYS for " + aRegion.getFullPath());
       }
      
       int noOfRootRgn = TestConfig.tab().intAt(JtaPrms.numberOfRandomRegions);
       for (int i = 0; i < noOfRootRgn; i++) {
         String regionName = "root" + (i + 1);
         aRegion = RegionHelper.createRegion(regionName, regionConfig);
         // edge clients register interest in ALL_KEYS
         if (aRegion.getAttributes().getPoolName() != null) {
           aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
           Log.getLogWriter().info("registered interest in ALL_KEYS for " + aRegion.getFullPath());
         }
       }
    }
  }
}

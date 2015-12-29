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
package dynamicReg; 

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.PoolHelper;
import hydra.ProcessMgr;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Set;

import tx.OpList;
import tx.Operation;
import util.NameFactory;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.util.BridgeWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;

/** Tests dynamic region creation on the server(s)
 *  in hierarchical cache  and peer2peer topologies.
 *
 * @author jfarris
 * @since 4.3
 */
public class DynamicRegionTest {

/* The singleton instance of DynamicRegionTest in this VM */
static protected DynamicRegionTest dynamicRegionTest;

/** 
 *  Hydra task to create a BridgeServer, creates the cache & regions to act as
 *  parents of dynamically created regions.
 */
public synchronized static void HydraTask_initBridgeServer() {

   // configure DynamicRegionFactory - required before cache is open
   File d = new File("DynamicRegionData" + ProcessMgr.getProcessId());
   d.mkdirs();
   DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(d, null));

   // create cache
   CacheHelper.createCache("bridge");

   // create regions to act as parent(s) of dynamic regions
   int numRoots = TestConfig.tab().intAt(DynamicRegionPrms.numRootRegions);
   int breadth = TestConfig.tab().intAt(DynamicRegionPrms.numSubRegions);
   int depth = TestConfig.tab().intAt(DynamicRegionPrms.regionDepth);

   for (int i=0; i<numRoots; i++) {
     String rootName = "root" + (i+1);
     Region rootRegion = RegionHelper.createRegion(rootName, "bridge");
     Log.getLogWriter().info("Created root region " + rootName);
     createSubRegions(rootRegion, breadth, depth, "Region");
   }

   // start the bridge server
   BridgeHelper.startBridgeServer("bridge");
}

/**
 *  Hydra task which establishes the edge clients for hierarchical cache:  
 *  configures for dynamic regions and creates regions to act as parent of dynamic region.
 */
public synchronized static void HydraTask_initialize() {
   if (dynamicRegionTest == null) {
      dynamicRegionTest = new DynamicRegionTest();
      dynamicRegionTest.initialize();
   }
}

/**
 * Initializes the edge clients for hierarchical cache and creates regions to 
 * use as parents of dynamic regions.
 */
protected void initialize() {

  if (CacheHelper.getCache() == null) {


    // configure for dynamic regions
    File d = new File("DynamicRegionData" + RemoteTestModule.MyPid);
    d.mkdirs();
    DistributedSystem ds = DistributedSystemHelper.connect();
    DynamicRegionFactory.Config config = ClientHelper.getDynamicRegionConfig(d, RegionHelper
        .getRegionDescription("edge"), true, true);
    DynamicRegionFactory.get().open(config);

    // create cache
    Cache myCache = CacheHelper.createCache("edge");

    // create root and subregions as potential parents of dynamic regions
    int numRoots = TestConfig.tab().intAt(DynamicRegionPrms.numRootRegions);
    int breadth = TestConfig.tab().intAt(DynamicRegionPrms.numSubRegions);
    int depth = TestConfig.tab().intAt(DynamicRegionPrms.regionDepth);

    for (int i=0; i<numRoots; i++) {
       String rootName = "root" + (i+1);
       Region rootRegion = RegionHelper.createRegion(rootName, "edge");
       Log.getLogWriter().info("Created root region " + rootName);

       //createEntries( rootRegion );
       createSubRegions(rootRegion, breadth, depth, "Region");
    }
  }
}

/** 
 * Hydra task which intializes for dynamic regions in P2P test
 */
public synchronized static void HydraTask_initializeP2P() {
   if (dynamicRegionTest == null) {
      dynamicRegionTest = new DynamicRegionTest();
      dynamicRegionTest.initializeP2P();
   }
}


/** 
 * Intializes for dynamic regions in P2P test and creates regions to
 * act as parents to dynamically created regions.
 */
protected void initializeP2P() {

  if (CacheHelper.getCache() == null) {


    // configure for dynamic regions
    File d = new File("DynamicRegionData" + ProcessMgr.getProcessId());
    d.mkdirs();
    DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(d, null));

    Cache myCache = CacheHelper.createCache("cache");

    // create root regions to act as parents to dynamic regions
    int numRoots = TestConfig.tab().intAt(DynamicRegionPrms.numRootRegions);
    int breadth = TestConfig.tab().intAt(DynamicRegionPrms.numSubRegions);
    int depth = TestConfig.tab().intAt(DynamicRegionPrms.regionDepth);

    for (int i=0; i<numRoots; i++) {
       String rootName = "root" + (i+1);
       Region rootRegion = RegionHelper.createRegion(rootName, "region");
       Log.getLogWriter().info("Created root region " + rootName);
       createSubRegions(rootRegion, breadth, depth, "Region");
    }

  }

}

  /**
   * Initializes client (in hierarchical cache topology) using cache.xml
   */
  public static void HydraTask_initHctClientCacheWithXml() {
    if (dynamicRegionTest == null) {
      dynamicRegionTest = new DynamicRegionTest();
      DynamicRegionTest.initClientCacheWithXml("edge", "edge");
    }
  }

  /**
   * Initializes client (in peer-to-peer topology) using cache.xml
   */
  public static void HydraTask_initClientCacheWithXml() {
    if (dynamicRegionTest == null) {
      dynamicRegionTest = new DynamicRegionTest();
      DynamicRegionTest.initClientCacheWithXml("cache", "region");
    }
  }

  /**
   * Initialize using XML based on the given cache and region configurations.
   */
  public static void initClientCacheWithXml(String cacheConfig,
                                            String regionConfig) {

    // connect to the distributed system with the xml file (not generated yet)
    String cacheXmlFile = "clientCache_" + RemoteTestModule.MyPid + ".xml";
    DistributedSystem ds = DistributedSystemHelper.connectWithXml(cacheXmlFile);

    // configure for dynamic regions, using the optional bridge writer
    File diskDir = new File("DynamicRegionData" + RemoteTestModule.MyPid);
    diskDir.mkdirs();
    
    DynamicRegionFactory.Config dynamicRegionConfig = null;
    String poolConfig = ConfigPrms.getPoolConfig();
    if (poolConfig != null) {
       String poolName = PoolHelper.getPoolDescription(ConfigPrms.getPoolConfig()).getName();
       dynamicRegionConfig =  new DynamicRegionFactory.Config(diskDir, poolName, true, true);
    } else {
       RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
       BridgeWriter writer = (BridgeWriter)rd.getCacheWriterInstance(true);
       dynamicRegionConfig = new DynamicRegionFactory.Config(diskDir, writer, true, true);
    }

    // generate the xml file
    CacheHelper.generateCacheXmlFile(cacheConfig, dynamicRegionConfig, regionConfig, null, null, poolConfig, cacheXmlFile);

    // create the cache and its region using the xml file
    CacheHelper.createCacheFromXml(cacheXmlFile);
 }

/**   
 *  Creates the subregion hierarchy of a root. 
 */
private static void createSubRegions(Region r, int numChildren, int levelsLeft, String parentName) {

   String currentName;
   for (int i=1; i<=numChildren; i++) {
      currentName = parentName + i;
      Region child = null;

      try {
        child = r.createSubregion(currentName, r.getAttributes());
        Log.getLogWriter().info("Created subregion " + TestHelper.regionToString(child, true));
      } catch (RegionExistsException e) {
        child = r.getSubregion(currentName);
        Log.getLogWriter().info("Got subregion " + TestHelper.regionToString(child, true));
      } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      if (levelsLeft > 1) {
         createSubRegions(child, numChildren, levelsLeft-1, currentName);
      }
   }
}

/**   
 *  Creates a dynamic region.  
 */

   private Region createDynamicRegion(String parentName, String drName) {
    
    Region dr = null;
    // dynamic region inherits attributes of parent
    try {
      dr = DynamicRegionFactory.get().createDynamicRegion(parentName, drName);
    } catch (CacheException ce) {
      throw new TestException(TestHelper.getStackTrace(ce));
    }
    Log.getLogWriter().info("Created dynamic region " + 
                            TestHelper.regionToString(dr, true));
    return dr;
}

/**
 * Hydra task which puts objects into the cache. 
 */
public synchronized static void HydraTask_putData() {

   dynamicRegionTest.putData();
}


/**
 * Puts object into a dynamically created region.   Each logical hydra client puts a configurable
 * number of entries each with a unique key.
 */
protected void putData() {

   int numEntries = TestConfig.tab().intAt(DynamicRegionPrms.maxEntries);

   // select random region, may be root
   Region aRegion = getRandomRegion(true);
   String drName = aRegion.getName() + "_DYNAMIC";
   Log.getLogWriter().info("### creating dynamic region using parent: " + aRegion.getFullPath());
   Log.getLogWriter().info("### dynamic region name will be: " + drName);

   Log.getLogWriter().info("### randomly selected region name: " + aRegion.getName());
   Log.getLogWriter().info("### randomly selected region path: " + aRegion.getFullPath());

   createDynamicRegion(aRegion.getFullPath(), drName);

   Cache myCache = CacheHelper.getCache();
   String drPath = aRegion.getFullPath() + "/" + drName;   
   Region dynRegion = myCache.getRegion(drPath);
   Log.getLogWriter().info("### creating dynamic region");
   Log.getLogWriter().info("Region is: " + dynRegion.getFullPath());

   OpList opList = new OpList();
  
   for (int i=0; i < numEntries; i++) {
     String name = NameFactory.getNextPositiveObjectName();
     Object anObj = new ValueHolder(name, new RandomValues());

     try {
         dynRegion.put(name, anObj);
         Operation op  = new Operation (dynRegion.getFullPath(), name, Operation.ENTRY_CREATE, null, anObj); 
         opList.add(op);
     } catch (Exception e) {
         throw new TestException(TestHelper.getStackTrace(e));
     }

   }

   DynamicRegionBB.putSingleOpList(opList);

}


/**
 * Selects random region (copied from event.EventTest. 
 */
protected Region getRandomRegion(boolean allowRootRegion) {
   // select a root region to work with
   Set rootRegions = CacheHelper.getCache().rootRegions();
   int randInt = TestConfig.tab().getRandGen().nextInt(0, rootRegions.size() - 1);
   Object[] regionList = rootRegions.toArray();
   Region rootRegion = (Region)regionList[randInt];

   Set subregionsSet = rootRegion.subregions(true);
   if (subregionsSet.size() == 0) {
     if (allowRootRegion) {
       return rootRegion;
     }
     else {
         return null;
     }
   }
   ArrayList aList = null;
   try {
     Object[] array = subregionsSet.toArray();
     aList = new ArrayList(array.length);
     for (int i=0; i<array.length; i++) {
       aList.add(array[i]);
     }
   } catch (NoSuchElementException e) {
      throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
   }
   if (allowRootRegion) {
      aList.add(rootRegion);
   }
   if (aList.size() == 0) { // this can happen because the subregionSet can change size after the toArray
      return null;
   }
   randInt = TestConfig.tab().getRandGen().nextInt(0, aList.size() - 1);
   Region aRegion = (Region)aList.get(randInt);
   if (aRegion == null) {
      throw new TestException("Bug 30171 detected: aRegion is null");
   }
   return aRegion;
}

/**
 * Hydra task which validates the contents of the cache.the keys and values in each region are what
 * the test expects.
 */
public synchronized static void HydraTask_validate() {
    if (dynamicRegionTest == null) {
      dynamicRegionTest = new DynamicRegionTest();
    }
    dynamicRegionTest.validate();
}

/**
 * Verifies the keys and values in each region are what the test expects.
 * Validation method copied from event tests.
 */

protected void validate() {

   Log.getLogWriter().info("validating...");
   Cache myCache = CacheHelper.getCache();
  
   OpList opList = DynamicRegionBB.getSingleOpList();

   Log.getLogWriter().info("In validate opList is: " + opList.toString());    

   for (int i = 0; i < opList.numOps() - 1; i++) {

     Operation op = opList.getOperation(i);
     Log.getLogWriter().info("verifying op: " + op.toString());
     String expectedRegionPath = op.getRegionName();
     Object expectedKey = op.getKey();
     Object expectedValue = op.getNewValue();

     try {
         Region r = myCache.getRegion(expectedRegionPath);
         if (r == null) {
           throw new TestException("Region: " + expectedRegionPath + " not found");
         }
         Object val = r.get(expectedKey);
         if (val == null) {
           throw new TestException("Region found but value not found for key " + expectedKey);
         }
         if (!val.equals(expectedValue)) {
           throw new TestException("Region found but value for key " + expectedKey + "is wrong value: " + val);
         }


     } catch (Exception e) {
       throw new TestException(TestHelper.getStackTrace(e));
     }
   }
}



}

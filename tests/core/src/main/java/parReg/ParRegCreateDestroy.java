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
package parReg; 

import hydra.BridgeHelper;
import hydra.BridgePrms;
import hydra.CacheHelper;
import hydra.DiskStoreHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import util.PRObserver;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.cache.persistence.ConflictingPersistentDataException;
import com.gemstone.gemfire.cache.persistence.RevokedPersistentDataException;
import com.gemstone.gemfire.cache.util.BridgeWriterException;
import com.gemstone.gemfire.distributed.PoolCancelledException;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import diskRecovery.RecoveryTest;
import diskRecovery.RecoveryTestVersionHelper;

public class ParRegCreateDestroy {

protected static ParRegCreateDestroy testInstance;
protected static List regionDescriptNames = new ArrayList();
protected static List bridgeRegionDescriptNames = new ArrayList();
protected static int destroyThreshold;
//private static int maxRegions;
protected static final int ENTRIES_TO_PUT = 50;
protected static Cache theCache;
protected static boolean isBridgeConfiguration = false;
protected static boolean isBridgeClient = true;
private static int selection = 0;


// ======================================================================
// Initialization

/** Creates and initializes the singleton instance of ParRegCreateDestroy in this VM.
 */
public synchronized static void HydraTask_initialize() {
   if (testInstance == null) {
      PRObserver.installObserverHook();
      PRObserver.initialize();
      testInstance = new ParRegCreateDestroy();
      testInstance.initialize();
   }
   Log.getLogWriter().info("isBridgeConfiguration: " + isBridgeConfiguration);
   Log.getLogWriter().info("isBridgeClient: " + isBridgeClient);
}

/** Creates and initializes the singleton instance of ParRegCreateDestroy in 
 *  a bridge server.
 */
public synchronized static void HydraTask_initializeBridgeServer() {
   if (testInstance == null) {
      testInstance = new ParRegCreateDestroy();
      testInstance.initialize();
      BridgeHelper.startBridgeServer("bridge");
   }
   isBridgeClient = false;
   Log.getLogWriter().info("isBridgeConfiguration: " + isBridgeConfiguration);
   Log.getLogWriter().info("isBridgeClient: " + isBridgeClient);
}

/**
 *  Initialize this test instance
 */
protected void initialize() {
   Vector bridgeNames = TestConfig.tab().vecAt(BridgePrms.names, null);
   isBridgeConfiguration = bridgeNames != null;
   Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
   for (int i = 0; i < regionNames.size(); i++) {
      String regionDescriptName = (String)(regionNames.get(i));
      if (regionDescriptName.startsWith("bridge")) { // this is a server region description   
         bridgeRegionDescriptNames.add(regionDescriptName);
      } else { // is either a bridge client or a peer
         regionDescriptNames.add(regionDescriptName);
      }
   }
   if (isBridgeConfiguration) {
      if (bridgeRegionDescriptNames.size() != regionDescriptNames.size()) {
         throw new TestException("Error in test configuration; need equal number of region descriptions for bridge servers and bridge clients");
      }
   }
   Log.getLogWriter().info("bridgeRegionDescriptNames is " + bridgeRegionDescriptNames);
   Log.getLogWriter().info("regionDescriptNames is " + regionDescriptNames);
//   maxRegions = regionDescriptNames.size();
   theCache = CacheHelper.createCache("cache1");
   destroyThreshold = (int)(regionDescriptNames.size() * 0.8);
   Log.getLogWriter().info("destroyThreshold is " + destroyThreshold);
   Log.getLogWriter().info("ENTRIES_TO_PUT is " + ENTRIES_TO_PUT);
}

// ======================================================================
// Hydra tasks

/** Task to create and destroy partitioned regions 
 */
public static void HydraTask_createDestroy() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   try {
     do {
       testInstance.doCreateDestroy();
     } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
   } finally {
     if (isBridgeConfiguration) {
       for (Iterator ri = theCache.rootRegions().iterator(); ri.hasNext(); ) {
         Region r = (Region) ri.next();
         ClientHelper.release(r);
       }
     }
   }
}
/** Task to create and destroy partitioned regions 
 */
public static void HydraTask_createDestroyWithPersistence() {
  int secondsToRun = TestConfig.tab().intAt(ParRegPrms.secondsToRun);
  ParRegTest.checkForLastIteration(secondsToRun);
  HydraTask_createDestroy();
  if (ParRegBB.getBB().getSharedCounters().read(ParRegBB.TimeToStop) > 1) {
    ParRegBB.getBB().getSharedCounters().increment(ParRegBB.Pausing); // signal this thread has paused
    throw new StopSchedulingTaskOnClientOrder("Stopping this thread from tasks");
  }
}

/** Task to create and destroy partitioned regions 
 */
public static void HydraTask_revokeWaitingMembers() {
  for (int i =1; i <= 100; i++) {
    RecoveryTestVersionHelper.forceRecovery(false);
  }
  long numThreadsStopped = ParRegBB.getBB().getSharedCounters().read(ParRegBB.Pausing);
  Log.getLogWriter().info("NumThreadsStopped counter is " + numThreadsStopped);
  if (numThreadsStopped >= TestHelper.getNumThreads()-1) {
    throw new StopSchedulingTaskOnClientOrder("Stopping admin thread");
  }
}
/**
 * Creates partitioned regions. Task to create partitioned regions
 */
public static void HydraTask_createRegions() {
  testInstance.doCreateRegionsOnly();
}

/**
 * Randomly create partitioned regions.
 */
protected void doCreateRegionsOnly() {
  if (isBridgeConfiguration && !isBridgeClient) { // this is a bridge server
    Log.getLogWriter().info("Inside bridge server");
    doCreateRegions(bridgeRegionDescriptNames);
  }
  else {
    if (isBridgeConfiguration) {
      doCreateRegions(regionDescriptNames);
    }
    else {
      long numOfAccessors = ParRegBB.getBB().getSharedCounters()
          .incrementAndRead(ParRegBB.numOfAccessors);
      if (ParRegBB.getBB().getSharedMap().get(
          "Vm Id: " + RemoteTestModule.getMyVmid()) == null) {
        ParRegBB.getBB().getSharedMap().put(
            "Vm Id: " + RemoteTestModule.getMyVmid(), "NotAnAccessor");
      }

      if ((numOfAccessors <= TestConfig.tab().longAt(
          ParRegPrms.numberOfAccessors, 0))
          || ParRegBB.getBB().getSharedMap().get(
              "Vm Id: " + RemoteTestModule.getMyVmid()).toString()
              .equalsIgnoreCase("Accessor")) {
        Iterator iterator = regionDescriptNames.iterator();
        ArrayList accessorRegionDescriptNames = new ArrayList();
        while (iterator.hasNext()) {
          String regionDescription = (String)iterator.next();
          if (regionDescription.startsWith("aRegion"))
            accessorRegionDescriptNames.add(regionDescription);
        }
        doCreateRegions(accessorRegionDescriptNames);

      }
      else {
        Iterator iterator = regionDescriptNames.iterator();
        ArrayList nonAccessorRegionDescriptNames = new ArrayList();
        while (iterator.hasNext()) {
          String regionDescription = (String)iterator.next();
          if (!regionDescription.startsWith("aRegion"))
            nonAccessorRegionDescriptNames.add(regionDescription);
        }
        doCreateRegions(nonAccessorRegionDescriptNames);
      }
    }
  }
}  

public void doCreateRegions(List regionDescriptNames) {
  String regionDescriptName;
  Region aRegion;
  for (int j = 0; j < regionDescriptNames.size(); j++) {
    regionDescriptName = (String)(regionDescriptNames.get(j));
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
    String regionName = RegionHelper.getRegionDescription(regionDescriptName)
        .getRegionName();
    RegionAttributes attributes = RegionHelper
        .getRegionAttributes(regionDescriptName);
    String diskStoreName = attributes.getDiskStoreName();
    if (diskStoreName != null) {
      DiskStoreHelper.createDiskStore(diskStoreName);
    }
    String poolName = attributes.getPoolName();
    if (poolName != null) {
      PoolHelper.createPool(poolName);
    }
    try {
      aRegion = theCache.createRegion(regionName, RegionHelper
          .getRegionAttributes(regionDescriptName));
      Log.getLogWriter().info(
          "Created partitioned region " + regionName
              + " with region descript name " + regionDescriptName);
    }
    catch (RegionExistsException e) {
      // region already exists; ok
      Log.getLogWriter().info(
          "Using existing partitioned region " + regionName);
      aRegion = e.getRegion();
      if (aRegion == null) {
        throw new TestException(
            "RegionExistsException.getRegion returned null");
      }
    }
  }

}

/**
 * 
 * @param e
 * @param aRegion
 * @return false on a problem
 */
private void checkPutException(RuntimeException e, Region aRegion) {
  if (e instanceof RegionDestroyedException) {
    Log.getLogWriter().info("Caught expected exception " + e + " while putting into " + 
        aRegion.getFullPath() + "; continuing test");
    return;
 }
  if (e instanceof CancelException) {
    // @todo grid change this to pool closed exception when we have it
    Log.getLogWriter().info("Caught expected exception " + e + " while putting into " +
        aRegion.getFullPath() + "; continuing test");
    return;
 }
  if (e instanceof PartitionedRegionStorageException) {
    String errStr = e.toString();
    if ((errStr.indexOf(PRHARedundancyProvider.INSUFFICIENT_STORES_MSG.toLocalizedString()) >= 0) ||
        (errStr.indexOf(PRHARedundancyProvider.TIMEOUT_MSG.toLocalizedString()) >= 0) ||
        (errStr.indexOf("putAll at server applied partial keys due to exception")) >= 0) {
       Log.getLogWriter().info("Caught expected exception " + e + " while putting into " + 
           aRegion.getFullPath() + "; continuing test");
       return;
    }
    throw e;
   }
  if (e instanceof ServerConnectivityException) {
    if (!isBridgeClient) {
      throw e;
    }
    
   if (e.getCause() instanceof RegionDestroyedException) {
     Log.getLogWriter().info("Caught expected exception " + e + " while putting into " + 
                             aRegion.getFullPath() + "; continuing test");
     return;
   } 
   if (e.getCause() instanceof PartitionedRegionStorageException) {
     String errStr = e.toString();
     if ((errStr.indexOf(PRHARedundancyProvider.INSUFFICIENT_STORES_MSG.toLocalizedString()) >= 0) ||
         (errStr.indexOf(PRHARedundancyProvider.TIMEOUT_MSG.toLocalizedString()) >= 0) || 
         (errStr.indexOf("putAll at server applied partial keys due to exception")) >= 0) {
       Log.getLogWriter().info("Caught expected exception " + e + " while putting into " + 
                               aRegion.getFullPath() + "; continuing test");
       return;
     }
   }
   throw e;
  }
  
  if (e instanceof CacheWriterException) {
    if (!isBridgeClient) {
      throw e;
    }
    String errStr = e.toString();
    if ((errStr.indexOf("Either the specified region, key or value was invalid. ") >= 0) ||
        (errStr.indexOf("The BridgeWriter has been closed") >= 0) ||
        (errStr.indexOf(PRHARedundancyProvider.TIMEOUT_MSG.toLocalizedString()) >= 0) ||
        (errStr.indexOf("was not found during put request") >= 0) || // Region named /region8 was not found during put request
        (errStr.indexOf("RegionDestroyedException") >= 0)) {
       Log.getLogWriter().info("Caught expected exception " + e + " while putting into " + 
           aRegion.getFullPath() + "; continuing test");
       return;
    }
    if (errStr.indexOf("Failed to put entry") >= 0) {
       throw new TestException("Bug 37120 detected: " + TestHelper.getStackTrace(e));
    }
    throw e;
 }
  
  if (e instanceof PartitionOfflineException) {
    Log.getLogWriter().info("Caught expected exception " + e + " while putting into " +
        aRegion.getFullPath() + "; continuting test");
    return;
  }
  throw e; 
}

/** Randomly create and destroy partitioned regions.
 */
protected void doCreateDestroy() {
   Set rootRegions = theCache.rootRegions();
   Region aRegion = null;

   // create a partitioned region
   // select a random region
   String regionDescriptName = null;
   if (isBridgeConfiguration && !isBridgeClient) { // this is a bridge server
     int randInt = TestConfig.tab().getRandGen().nextInt(0, bridgeRegionDescriptNames.size()-1);
     regionDescriptName = (String)(bridgeRegionDescriptNames.get(randInt));
   } else { // is bridge client or peer
     int randInt = TestConfig.tab().getRandGen().nextInt(0, regionDescriptNames.size()-1);
     regionDescriptName = (String)(regionDescriptNames.get(randInt));
   }
   System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
   String regionName = RegionHelper.getRegionDescription(regionDescriptName).getRegionName();
   //create the connection pool if this is a client cache.
   RegionAttributes attributes = RegionHelper.getRegionAttributes(regionDescriptName);
   if (attributes.getDiskStoreName() != null) {
     DiskStoreHelper.createDiskStore(attributes.getDiskStoreName());
   }
   String poolName = attributes.getPoolName();
   if(poolName != null) {
     PoolHelper.createPool(poolName);
   }
   try {
     aRegion = theCache.createRegion(regionName, attributes);
     Log.getLogWriter().info("Created partitioned region " + regionName + " with region descript name " + regionDescriptName);
   } catch (RegionExistsException e) {
     // region already exists; ok
     Log.getLogWriter().info("Using existing partitioned region " + regionName);
     aRegion = e.getRegion();
     if (aRegion == null) {
       throw new TestException("RegionExistsException.getRegion returned null");
     }
   } catch(BridgeWriterException bwe) {
     if(bwe.getCause() instanceof CancelException) {
       Log.getLogWriter().info("Caught expected exception " + bwe + " while creating the region " + 
           aRegion.getFullPath() + "; continuing test");
       // This is ok because the bridgewriter sharing in this test has a race condition in which we can get a closed bridgeWriter on our attributes
       return;
     } else if (isBridgeClient) {
         String errStr = bwe.toString();
         if (errStr.indexOf("The BridgeWriter has been closed") >= 0) {
            Log.getLogWriter().info("Caught expected exception " + bwe + " while creating region; continuing test");
            return;
         }
     } else {
       throw bwe;
     }
   } catch (ConflictingPersistentDataException e) {
     Log.getLogWriter().info("Caught expected exception " + e + " while creating region; continuing test");
     return;
   } catch (RevokedPersistentDataException e) {
     Log.getLogWriter().info("Caught expected exception " + e + " while creating region; continuing test");
     return;
   }

   // add entries to the partitioned region
   Log.getLogWriter().info("Putting " + ENTRIES_TO_PUT + " entries into " + aRegion.getFullPath());
   selection++;
   Map mapToPut = new HashMap();
   for (int i = 1; i <= ENTRIES_TO_PUT; i++) {
      mapToPut.put("" + System.currentTimeMillis(), new Integer(i));
   }
   try {
      aRegion.putAll(mapToPut);
   }
   catch (PoolCancelledException e) {
     Throwable cause = e;
     for (;;) {
       cause = cause.getCause();
       if (cause == null) {
         throw e; // ran out of options
       }
       if (cause instanceof com.gemstone.gemfire.cache.client.ServerOperationException) {
         continue;
       }
       if (!(cause instanceof RuntimeException)) {
         throw e;
       }
       checkPutException((RuntimeException)cause, aRegion);
       break; // success
     }
   }
   catch (RuntimeException e) {
     checkPutException(e, aRegion);
   }
 
   // destroy a random partitioned region
   rootRegions = theCache.rootRegions();
   if (rootRegions.size() >= destroyThreshold) {
      Iterator it = rootRegions.iterator();
      aRegion = (Region)it.next();
      if (isBridgeConfiguration) {
         // a BridgeClient can be cast to a BridgeWriter, but a
         // BridgeClient cannot be cast to a BridgeLoader
        ClientHelper.release(aRegion);
      }
      try {
         int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
         if (randInt <= 33) { // do a region destroy
            Log.getLogWriter().info("Destroying " + aRegion.getFullPath());
            aRegion.destroyRegion();
         } else if (randInt <= 66) { // do a local region destroy
            Log.getLogWriter().info("Locally destroying " + aRegion.getFullPath());
            aRegion.localDestroyRegion();
         } else { // do a close
            Log.getLogWriter().info("Closing " + aRegion.getFullPath());
            aRegion.close();
         }
      } catch (RegionDestroyedException e) {
         // some other thread already destroyed it
         Log.getLogWriter().info("Caught expected exception " + e.getClass().getName() + "; continuing test");
      } catch(ServerConnectivityException e) {
        if (isBridgeClient) {
          if(e.getCause() instanceof RegionDestroyedException) {
            Log.getLogWriter().info("Caught expected exception " + e + " while putting into " + 
                aRegion.getFullPath() + "; continuing test");
          } else {
            throw e;
          }
        } else {
          throw e;
        }
      } catch (CacheWriterException e) {
         String errStr = e.toString();
         if (errStr.indexOf("Either the specified region or key was invalid") >= 0) {
            throw new TestException("Bug 37039 detected: " + TestHelper.getStackTrace(e));
         } else if (errStr.indexOf("RegionDestroyedException") >= 0) {
            Log.getLogWriter().info("Caught expected exception " + e.getClass().getName() + "; continuing test");
         } else if (errStr.indexOf("was not found during destroy region request") >= 0) { // Region named /region6 was not found during destroy region request
            Log.getLogWriter().info("Caught expected exception " + e.getClass().getName() + "; continuing test");
         } else {
            throw e;
         }
      } finally {
         ClientHelper.release(aRegion);
      }
   }
} 

  public static void shutdownHook() {
    ParRegUtil.dumpAllPartitionedRegions();
  }

}

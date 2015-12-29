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
package rebalance; 

import hydra.*;
import util.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

import getInitialImage.*;
import parReg.*;

public class ColocatedRecoveryTest extends parReg.KnownKeysTest {

// ======================================================================== 
// overriding methods

public synchronized static void HydraTask_HA_dataStoreInitialize() {
  if (testInstance == null) {
     testInstance = new ColocatedRecoveryTest();
     ((ColocatedRecoveryTest)testInstance).initInstance(ConfigPrms.getRegionConfig());
     ParRegBB.getBB().getSharedMap().put(DataStoreVmStr + RemoteTestModule.getMyVmid(), new Integer(RemoteTestModule.getMyVmid()));
     if (isBridgeConfiguration) {
        BridgeHelper.startBridgeServer("bridge");
     }
  }
}

public synchronized static void HydraTask_HA_accessorInitialize() {
  if (testInstance == null) {
     testInstance = new ColocatedRecoveryTest();
     ((ColocatedRecoveryTest)testInstance).initInstance(ConfigPrms.getRegionConfig());
     if (isBridgeConfiguration) {
        Cache myCache = CacheHelper.getCache();
        Set<Region<?,?>> rootRegions = myCache.rootRegions();
        for (Region aRegion : rootRegions) {
           ParRegUtil.registerInterest(testInstance.aRegion); 
        }
     } 
  }
}

/**
 *  Initialize this test instance, supporting multiple regions 
 *  for use with co-location
 */
public void initInstance(String regionConfig) {
   // from InitImageTest (setup the keyIntervals)
   numNewKeys = TestConfig.tab().intAt(InitImagePrms.numNewKeys, -1);
   keyIntervals = (KeyIntervals)(InitImageBB.getBB().getSharedMap().get(InitImageBB.KEY_INTERVALS));
   Log.getLogWriter().info("initInstance, keyIntervals read from blackboard = " + keyIntervals.toString());
   int numDestroyed = keyIntervals.getNumKeys(KeyIntervals.DESTROY);
   int numKeyIntervals = keyIntervals.getNumKeys();
   totalNumKeys = numKeyIntervals + numNewKeys - numDestroyed;
   sc = InitImageBB.getBB().getSharedCounters();
   randomValues = new RandomValues();
   Log.getLogWriter().info("numKeyIntervals is " + numKeyIntervals);
   Log.getLogWriter().info("numNewKeys is " + numNewKeys);
   Log.getLogWriter().info("numDestroyed is " + numDestroyed);
   Log.getLogWriter().info("totalNumKeys is " + totalNumKeys);

   highAvailability = TestConfig.tab().booleanAt(ParRegPrms.highAvailability, false);

   Cache myCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());

   // this must be installed after Cache Creation and prior to Region creation
   InternalResourceManager.ResourceObserver ro = ParRegPrms.getResourceObserver();
   if (ro != null) {
      InternalResourceManager rm = InternalResourceManager.getInternalResourceManager(myCache);
      rm.setResourceObserver(ro);
   }

   AttributesFactory aFactory = RegionHelper.getAttributesFactory(regionConfig);
   RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
   String regionBase = rd.getRegionName();

   // override colocatedWith in the PartitionAttributes
   PartitionDescription pd = rd.getPartitionDescription();
   PartitionAttributesFactory prFactory = pd.getPartitionAttributesFactory();
   PartitionAttributes prAttrs = null;

   String colocatedWith = null;
   int numRegions = RebalancePrms.getNumRegions();
   for (int i = 0; i < numRegions; i++) {
      String regionName = regionBase + "_" + (i+1);
      if (i > 0) {
         colocatedWith = regionBase + "_" + i;
         prFactory.setColocatedWith(colocatedWith);
         prAttrs = prFactory.create();
         aFactory.setPartitionAttributes(prAttrs);
      }
      RegionHelper.createRegion(regionName, aFactory);
   }

   isBridgeConfiguration = (TestConfig.tab().stringAt(BridgePrms.names, null) != null);
}

/** Hydra task to initialize a region and load it according to hydra param settings.
 */
public static void HydraTask_loadRegions() {
   ((ColocatedRecoveryTest)testInstance).loadRegions();
}

protected void loadRegions() {
  final long LOG_INTERVAL_MILLIS = 10000;
  int numKeysToCreate = keyIntervals.getNumKeys();
  long lastLogTime = System.currentTimeMillis();
  long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, -1);
  long minTaskGranularityMS = -1;
  if (minTaskGranularitySec != -1)
    minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
  long startTime = System.currentTimeMillis();
  do {
    long shouldAddCount = sc.incrementAndRead(InitImageBB.SHOULD_ADD_COUNT);
    if (shouldAddCount > numKeysToCreate) {
      String aStr = "In loadRegion, for Region shouldAddCount is "
          + shouldAddCount + ", numOriginalKeysCreated is "
          + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED)
          + ", numKeysToCreate is " + numKeysToCreate;
      Log.getLogWriter().info(aStr);
      NameBB.getBB().printSharedCounters();
      throw new StopSchedulingTaskOnClientOrder(aStr);
    }

    Object key = NameFactory.getNextPositiveObjectName();
    Object value = getValueToAdd(key);
    try {   
       Set<Region<?,?>> rootRegions = CacheHelper.getCache().rootRegions();
       for (Region aRegion : rootRegions) {
          aRegion.put(key, value);
       }
       sc.increment(InitImageBB.NUM_ORIGINAL_KEYS_CREATED);
    } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (CacheWriterException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

    if (System.currentTimeMillis() - lastLogTime > LOG_INTERVAL_MILLIS) {
      Log.getLogWriter().info("Added " + sc.read(InitImageBB.NUM_ORIGINAL_KEYS_CREATED) + " out of " + numKeysToCreate);
      lastLogTime = System.currentTimeMillis();
    }
  } while ((minTaskGranularitySec == -1)
      || (System.currentTimeMillis() - startTime < minTaskGranularityMS));
}

}

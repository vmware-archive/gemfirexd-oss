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
/**
 * This contains snippets of code for evictors for inclusion in 
 * GemFire docs. The parts to be included in the docs are labeled by the 
 * string "DOCS". These examples are code snippets, rather than a complete 
 * programs. Code to create a cache and create the root region is assumed 
 * to be covered elsewhere in the docs.
 *
 * Only import statements needed by the examples are included. All other classes
 * used in this class are fully qualified. This is so we can help ensure the
 * snippets in the docs are useful, and include the necessary imports for them
 * to compile.
 *
 * This test will ensure that the capacity controller code snippets:
 *    -  compile
 *    -  execute without error
 * In addition, this test will add objects to the regions where the example
 * controllers have been installed as an added check.
 *=========================================================================
 */
package capCon;

// DOCS: begin import for docs snippet
import com.gemstone.gemfire.cache.*;
// DOCS: end import for docs snippet

import util.NameFactory;

public class DocExamples {

private static Region rootRegion = null;

private static final String REGION_NAME = "root";

// initialization methods
// ================================================================================
public synchronized static void HydraTask_initialize() {
   Cache myCache = util.CacheUtil.createCache();
   if (rootRegion == null) {
      util.RegionDefinition regDef = util.RegionDefinition.createRegionDefinition();
      rootRegion = regDef.createRootRegion(myCache, REGION_NAME, null, null, null);
      createExampleForLRUCapCon();
      createExampleForMemLRUCapCon();
   }
}

// task methods
//========================================================================
public static void HydraTask_exerciseControllers() {
   long timeToRunSec = hydra.TestConfig.tab().intAt(util.TestHelperPrms.minTaskGranularitySec);
   long timeToRunMS = timeToRunSec * util.TestHelper.SEC_MILLI_FACTOR;
   hydra.Log.getLogWriter().info("In HydraTask_exerciseControllers, running for " + timeToRunSec + " seconds");
   long startTime = System.currentTimeMillis();
   Region LRURegion = util.CacheUtil.getCache().getRegion(REGION_NAME).getSubregion("LRU_region");
   Region MemLRURegion = util.CacheUtil.getCache().getRegion(REGION_NAME).getSubregion("MemLRU_region");
   String clientName = System.getProperty("clientName");
   hydra.ClientDescription cd = hydra.TestConfig.getInstance().getClientDescription(clientName);  
   do {
      String unique = NameFactory.getNextPositiveObjectName(); 
      try {
         LRURegion.put(unique, unique); 
         byte[] value = new byte[100];
         MemLRURegion.put(unique, value);
      } catch (Exception e) {
         throw new util.TestException(util.TestHelper.getStackTrace(e));
      }
   } while (System.currentTimeMillis() - startTime < timeToRunMS);
}

// private methods
//========================================================================
private static void createExampleForLRUCapCon() {

   // DOCS: Begin docs snippet for entry evictor
   AttributesFactory factory = new AttributesFactory();
   EvictionAttributes evAttr = EvictionAttributes.createLRUEntryAttributes(100000);
   factory.setEvictionAttributes(evAttr);
   RegionAttributes regAttr = factory.createRegionAttributes();
   try {
      rootRegion.createSubregion("LRU_region", regAttr);
   } catch (CacheException e) {
      // handle CacheException here
   }
   // DOCS: End docs snippet for entry evictor

   Region aRegion = rootRegion.getSubregion("LRU_region");
   hydra.Log.getLogWriter().info("Created new region " + aRegion.getFullPath() + 
      " with region attributes " + regAttr);
}

private static void createExampleForMemLRUCapCon() {

   // DOCS: Begin docs snippet for memory evictor
   AttributesFactory factory = new AttributesFactory();
   EvictionAttributes evAttr = EvictionAttributes.createLRUMemoryAttributes(8);
   factory.setEvictionAttributes(evAttr);
   RegionAttributes regAttr = factory.createRegionAttributes();
   try {
      rootRegion.createSubregion("MemLRU_region", regAttr);
   } catch (CacheException e) {
      // handle CacheException here
   }
   // DOCS: End docs snippet for memory evictor

   Region aRegion = rootRegion.getSubregion("MemLRU_region");
   hydra.Log.getLogWriter().info("Created new region " + aRegion.getFullPath() + 
      " with region attributes " + regAttr);
}

}

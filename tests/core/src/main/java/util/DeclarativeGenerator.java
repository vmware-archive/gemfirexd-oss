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

import hydra.GemFirePrms;
import hydra.TestConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import memscale.OffHeapHelper;

import com.gemstone.gemfire.cache.*;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;

/** Class to generate xml for declarative caching */
public class DeclarativeGenerator {

/** Create a declarative caching xml file of the given name that defines the
 *  given cache.
 *
 *  @param xmlFileSpec The full directory path and file spec for the
 *         declarative xml file.
 *  @param aCache The cache to define in the declarative xml file 
 *         (including its regions)
 *  @param includeKeysValues True if the declarative xml file should define
 *         any keys/values in the regions being mimicked, false otherwise.
 *  @param useDTD - true if the generated xml should reference gemfire's
 *         dtd, false if it should reference gemfire's xsd Unless you 
 *         need a specific use case, calling this with a random boolean 
 *         is fine.
 */
public static void createDeclarativeXml(String xmlFileSpec, 
                                        Cache aCache,
                                        boolean includeKeysValues,
                                        boolean useDTD) {
   PrintWriter pw = null;
   try {
      pw = new PrintWriter(new FileWriter(new File(xmlFileSpec)));
   } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   CacheXmlGenerator.generate(aCache, pw, !useDTD, includeKeysValues);
}

/** Create a declarative caching xml file of the given name that defines 
 *  the given CacheDefinition and root RegionDefinitions, including
 *  any subregions of the root regions.
 *
 *  @param xmlFileSpec The full directory path and file spec for the
 *         declarative xml file.
 *  @param cacheDef The cache to define in the declarative xml file.
 *  @param rootRegion A RegionDefinition that represents a root region.
 *         If this region contain subregions, then they will be defined
 *         also, including any of their subregions, etc.
 *  @param useDTD - true if the generated xml should reference gemfire's
 *         dtd, false if it should reference gemfire's xsd Unless you 
 *         need a specific use case, calling this with a random boolean 
 *         is fine.
 */
public static void createDeclarativeXml(String xmlFileSpec, 
                                        CacheDefinition cacheDef, 
                                        RegionDefinition rootRegion,
                                        boolean useDTD) {
   ArrayList aList = new ArrayList();
   aList.add(rootRegion);
   createDeclarativeXml(xmlFileSpec, cacheDef, aList, useDTD);
}

/** Create a declarative caching xml file of the given name that defines 
 *  the given CacheDefinition and root RegionDefinitions, including
 *  any subregions of the root regions.
 *
 *  @param xmlFileSpec The full directory path and file spec for the
 *         declarative xml file.
 *  @param cacheDef The cache to define in the declarative xml file.
 *  @param rootRegions An ArrayList of RegionDefinitions that represent
 *         root regions of the cache to be defined in the declarative xml 
 *         file. If these regions contain subregions, then they will be 
 *         defined also, including any of their subregions, etc.
 *  @param useDTD - true if the generated xml should reference gemfire's
 *         dtd, false if it should reference gemfire's xsd Unless you 
 *         need a specific use case, calling this with a random boolean 
 *         is fine.
 */
public static void createDeclarativeXml(String xmlFileSpec, 
                                        CacheDefinition cacheDef, 
                                        ArrayList rootRegions,
                                        boolean useDTD) {
   // Create a fake cache instance
   CacheCreation fakeCache = createFakeCache(cacheDef);

   // create fake regions
   for (int i = 0; i < rootRegions.size(); i++) {
      // create the root region
      RegionDefinition regDef = (RegionDefinition)rootRegions.get(i);   
      RegionAttributes attr = createFakeRegionAttr(regDef, fakeCache);
      Region fakeRootRegion = null;
      String regionName = regDef.getRegionName();
      if (regionName == null)
         throw new TestException("Cannot create a cache xml file for " + regDef +
                   " because it does not contain \"regionName = <name>\"");
      try {
         fakeRootRegion = fakeCache.createVMRegion(regionName, attr);
      } catch (RegionExistsException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }

      // create any subregions
      ArrayList subregions = regDef.getSubregions();
      for (int j = 0; j < subregions.size(); j++) {
         RegionDefinition subRegDef = (RegionDefinition)subregions.get(j);
         createFakeSubregion(subRegDef, fakeRootRegion);
      }
   }

   createDeclarativeXml(xmlFileSpec, fakeCache, false, useDTD);
}

/** Create an instance of fake region attributes.  
 * 
 *  @param regDef The RegionDefinition to get the attributes from.
 *
 *  @returns A fake region attributes object.
 */
 private static RegionAttributes createFakeRegionAttr(RegionDefinition regDef, CacheCreation cc) {
   RegionAttributesCreation attr = new RegionAttributesCreation(cc);
   if (regDef.getScope() != null)
      attr.setScope(regDef.getScope());
   if (regDef.getMirroring() != null)
      attr.setMirrorType(regDef.getMirroring());
   setConcurrencyChecksEnabled(regDef, attr);  // specific to 7.0 and above (see VersionHelper)
   if (regDef.getCacheListeners() != null) {
      List listenerList = regDef.getCacheListeners();
      for (int i = 0; i < listenerList.size(); i++) {
         attr.addCacheListener((CacheListener)(TestHelper.createInstance((String)(listenerList.get(i)))));
      }
   }
   if (regDef.getEviction() != null)
      attr.setEvictionAttributes(regDef.getEvictionAttributes());
   if (regDef.getCacheLoader() != null)
      attr.setCacheLoader((CacheLoader)(TestHelper.createInstance(regDef.getCacheLoader())));
   if (regDef.getCacheWriter() != null)
      attr.setCacheWriter((CacheWriter)(TestHelper.createInstance(regDef.getCacheWriter())));
   if (regDef.getConcurrencyLevel() != null)
      attr.setConcurrencyLevel(regDef.getConcurrencyLevel().intValue());
   if (regDef.getEnableOffHeapMemory() != null) {
      if (regDef.getEnableOffHeapMemory().equalsIgnoreCase("ifOffHeapMemoryConfigured")) {
         String offHeapMemorySize = TestConfig.tab().stringAt(GemFirePrms.offHeapMemorySize, null);
         if (offHeapMemorySize != null) {
           if (offHeapMemorySize.charAt(0) != '0') {
             attr.setEnableOffHeapMemory(true);
           }
         }
      }
   }
   if (regDef.getKeyConstraint() != null)
      attr.setKeyConstraint(regDef.getKeyConstraint());
   if (regDef.getValueConstraint() != null)
      attr.setValueConstraint(regDef.getValueConstraint());
   if (regDef.getLoadFactor() != null)
      attr.setLoadFactor(regDef.getLoadFactor().floatValue());
   if (regDef.getStatisticsEnabled() != null)
      attr.setStatisticsEnabled(regDef.getStatisticsEnabled().booleanValue());
   if (regDef.getIndexMaintenanceSynchronous() != null)
      attr.setIndexMaintenanceSynchronous(regDef.getIndexMaintenanceSynchronous().booleanValue());
   if (regDef.getPersistBackup() != null)
      attr.setPersistBackup(regDef.getPersistBackup().booleanValue());
   if (regDef.getIsSynchronous() != null)
      attr.setDiskWriteAttributes(regDef.getDiskWriteAttributes());
   if (regDef.getNumDiskDirs() != null)
      attr.setDiskDirs(regDef.getDiskDirFileArr());
   if (regDef.getMulticastEnabled() != null)
       attr.setMulticastEnabled(regDef.getMulticastEnabled().booleanValue());
   if (regDef.getDataPolicy() != null)
       attr.setDataPolicy(regDef.getDataPolicy());
   if (regDef.getSubscriptionAttributes() != null)
       attr.setSubscriptionAttributes(regDef.getSubscriptionAttributes());

   ExpirationAttributes expAttr = regDef.getRegionTTL();
   if (expAttr != null)
      attr.setRegionTimeToLive(expAttr);
   expAttr = regDef.getRegionIdleTimeout();
   if (expAttr != null)
      attr.setRegionIdleTimeout(expAttr);

   expAttr = regDef.getEntryTTL();
   if (expAttr != null)
      attr.setEntryTimeToLive(expAttr);
   expAttr = regDef.getEntryIdleTimeout();
   if (expAttr != null)
      attr.setEntryIdleTimeout(expAttr);

   Boolean parReg = regDef.getPartitionedRegion();
   if (parReg != null) { 
      if (parReg.booleanValue()) {
         PartitionAttributesFactory parFac = new PartitionAttributesFactory();
//         if (regDef.getParRegCacheLoader() != null)
//            parFac.setCacheLoader((CacheLoader)(TestHelper.createInstance(regDef.getParRegCacheLoader())));
//         if (regDef.getParRegCacheWriter() != null)
//            parFac.setCacheWriter((CacheWriter)(TestHelper.createInstance(regDef.getParRegCacheWriter())));
         expAttr = regDef.getParRegEntryTTL();
//         if (expAttr != null)
//           parFac.setEntryTimeToLive(expAttr);
         expAttr = regDef.getParRegEntryIdleTimeout();
//         if (expAttr != null)
//            parFac.setEntryIdleTimeout(expAttr);
         if (regDef.getParRegLocalProperties() != null)
            parFac.setLocalProperties(regDef.getParRegLocalProperties());
         if (regDef.getParRegGlobalProperties() != null)
            parFac.setGlobalProperties(regDef.getParRegGlobalProperties());
         if (regDef.getParRegRedundantCopies() != null)
            parFac.setRedundantCopies(regDef.getParRegRedundantCopies().intValue());
         if (regDef.getParRegPartitionResolver() != null) {
            PartitionResolver instance = ((PartitionResolver)
               (TestHelper.createInstance(regDef.getParRegPartitionResolver())));
//            instance.init(regDef.getParRegPartitionResolverProperties());
            parFac.setPartitionResolver(instance);
         }
         if (regDef.getColocatedWith() != null) {
            parFac.setColocatedWith(regDef.getColocatedWith());
         }
         PartitionAttributes parRegAttr = parFac.create();
         attr.setPartitionAttributes(parRegAttr);
      }
   }
   return attr;
}

/** Create an instance of a fake cache, which is used by 
 *  com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator
 *
 *  @param cacheDef The description of the cache to create a fake
 *                  cache object for.
 *
 *  @returns A fake cache (one that doesn't really exist, but contains
 *           the appropriate cache settings).
 */
private static CacheCreation createFakeCache(CacheDefinition cacheDef) {
   CacheCreation fakeCache = new CacheCreation();
   Boolean aBool = cacheDef.getCopyOnRead();
   if (aBool != null)
      fakeCache.setCopyOnRead(aBool.booleanValue());
   Integer anInt = cacheDef.getLockTimeout();
   if (anInt != null)
      fakeCache.setLockTimeout(anInt.intValue());
   anInt = cacheDef.getSearchTimeout();
   if (anInt != null)
      fakeCache.setSearchTimeout(anInt.intValue());
   return fakeCache;
}

/** Create a fake subregion of the given fake parent region. Recursively
 *  create any subregions of the new subregion.
 *
 *  @param regDef The RegionDefinition for the subregion to create.
 *  @param fakeParent The fake region that is to be the parent of the new fake subregion.
 *
 *  @returns The new fake subregion
 */
private static Region createFakeSubregion(RegionDefinition regDef, Region fakeParent) {
   RegionAttributes attr = createFakeRegionAttr(regDef, (CacheCreation)fakeParent.getCache());
   Region fakeSubRegion = null;
   String regionName = regDef.getRegionName();
   if (regionName == null)
      throw new TestException("Cannot create a cache xml file for " + regDef +
                " because it does not contain \"regionName = <name>\"");
   try {
      fakeSubRegion = fakeParent.createSubregion(regDef.getRegionName(), attr);
   } catch (RegionExistsException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   ArrayList subregions = regDef.getSubregions();
   for (int i = 0; i < subregions.size(); i++) {
      RegionDefinition subRegDef = (RegionDefinition)subregions.get(i);
      createFakeSubregion(subRegDef, fakeSubRegion);
   } 
   return fakeSubRegion;
}

/** For GemFire7.0 and above, set concurrencyChecksEnabled (if defined)
 *
 *  @param regDef The RegionDefinition for the region to create.
 *  @param attr   The region attributes to update
 *
 * @returns The updated RegionAttributes
 */
private static RegionAttributes setConcurrencyChecksEnabled(RegionDefinition regDef, RegionAttributesCreation attr) {
   attr = DeclarativeGeneratorVersionHelper.setConcurrencyChecksEnabled(regDef, attr);
   return attr;
}

}

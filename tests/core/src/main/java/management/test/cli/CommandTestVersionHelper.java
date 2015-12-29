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

package management.test.cli;

import hydra.CacheHelper;
import hydra.DiskStoreHelper;
import hydra.JMXManagerBlackboard;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import management.util.ManagementUtil;
import util.TestHelper;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.ManagementService;

/**
 * @author lynn
 *
 */
public class CommandTestVersionHelper {

  /** Temporary until the product members can automatically become managers
   *
   */
  public static void HydraTask_becomeManager() {
    CommandTest.HydraTask_becomeManager();
  }

  public static void saveMemberMbeanInBlackboard() {
    ManagementUtil.saveMemberMbeanInBlackboard(); 
  }

  public static String getName(DistributedMember member) {
    return member.getName();
  }

  public static List<String> getGroups(DistributedMember member) {
    return member.getGroups();
  }
  
  /** Create region(s) in hydra.RegionPrms.names, using the hydra param
   *  PdxPrms.createProxyRegions to determine which regions specified in
   *  hdyra.RegionPrms.names to create.
   */
  public static void createRegions() {
    List<String> regionConfigNamesList = TestConfig.tab().vecAt(RegionPrms.names);
    for (String regionConfigName: regionConfigNamesList) {
      if (shouldCreateRegion(regionConfigName)) {
        Log.getLogWriter().info("Creating region with config name " + regionConfigName);
        Region aRegion = RegionHelper.createRegion(regionConfigName);
        Log.getLogWriter().info("Done creating region with config name " + regionConfigName + ", region name is " + aRegion.getFullPath());
      }
    }
    if (CommandPrms.getCreateSubregions()) {
      createSubregions();
    }
    Log.getLogWriter().info(TestHelper.regionHierarchyToString());
  }

  /** Determine if the given regionConfigName should be created considering the
   *  hydra param settings. 
   * @param regionConfigName The region config name to consider.
   * @return true if the region should be created, false otherwise.
   */
  protected static boolean shouldCreateRegion(String regionConfigName) {
    String configNameLC = regionConfigName.toLowerCase();
    boolean createProxyRegions = CommandPrms.getCreateProxyRegions();
    boolean createClientRegions = CommandPrms.getCreateClientRegions();
    boolean createPersistentRegions = CommandPrms.getCreatePersistentRegions();
    boolean isProxyRegion = ((configNameLC.indexOf("empty") >= 0) ||
                             (configNameLC.indexOf("accessor") >= 0));
    boolean isClientRegion = configNameLC.startsWith("client");
    boolean isPersistentRegion = configNameLC.indexOf("persist") >= 0;

    // now find conditions to turn shouldCreateRegionsTo false
    if (isPersistentRegion && !createPersistentRegions) {
      return false;
    }
    if (isClientRegion != createClientRegions) {
      return false;
    }
    if (isProxyRegion != createProxyRegions) {
      return false;
    }

    // now check for limiting regions to some members but not all
    try {
      RegionAttributes attr = RegionHelper.getRegionAttributes(regionConfigName);
      if (attr.getPartitionAttributes() != null) {
        String colocatedWithRegion = attr.getPartitionAttributes().getColocatedWith();
        if (colocatedWithRegion != null) {
          // if the region this region is colocated with exists then we need to create it
          return !(CacheHelper.getCache().getRegion(colocatedWithRegion) == null);
        }
      }
    } catch (IllegalStateException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Region specified in 'colocated-with' is not present") >= 0) { // colocated with region is not here so we can't create this region
        return false;
      } else {
        throw e;
      }
    }

    Vector configNamesVec = TestConfig.tab().vecAt(RegionPrms.names);
    int index = configNamesVec.indexOf(regionConfigName);
    String regionName = (String) TestConfig.tab().vecAt(RegionPrms.regionName).get(index);
    int numMembersToHostRegion = CommandPrms.getNumMembersToHostRegion();
    if (numMembersToHostRegion >= 0) { // we want to limit the number of members hosting a region
      SharedLock lock = CommandBB.getBB().getSharedLock();
      lock.lock();
      String key = regionName + "_counter";
      try {
        Integer counter = (Integer) CommandBB.getBB().getSharedMap().get(key);
        if (counter == null) {
          counter = 0;
        }
        counter = counter + 1;
        CommandBB.getBB().getSharedMap().put(key, counter);
        if (counter > numMembersToHostRegion) {
          Log.getLogWriter().info("Not creating " + regionName + " because this region is limited to existing in " + numMembersToHostRegion + " members");
          return false;
        }
      } finally {
        lock.unlock();
      }
    }

    return true;
  }
  
  /** Create one parent region, then recreate all existing regions as subregions of the parent,
   *  Then create a long subregion chain.
   */
  private static void createSubregions() {
    Cache theCache = CacheHelper.getCache();
    Set<Region<?, ?>> regionSet = theCache.rootRegions();
    boolean createProxyRegions = CommandPrms.getCreateProxyRegions();
    boolean createClientRegions = CommandPrms.getCreateClientRegions();
    boolean createPersistentRegions = CommandPrms.getCreatePersistentRegions();
    
    // create 2 parent regions, one that will have many subregions (fanoutParent)
    // and 1 that will create a long chain of subregions (chainParent)
    String regionConfigName = "replicate";
    if (createProxyRegions) { 
      regionConfigName = "emptyReplicate1";
    }
    if (createClientRegions) {
      regionConfigName = "clientReplicate";
    }
    RegionAttributes attr = RegionHelper.getRegionAttributes(RegionHelper.getAttributesFactory(regionConfigName));
    RegionFactory fac = theCache.createRegionFactory(attr);
    Region fanoutParent = fac.create("fanoutParent");
    Region chainParent = fac.create("chainParent");
    
    // create region chain
    String[] regionConfigNames = new String[] {"replicate", "replicateWithOverflow", "persistReplicate", "persistReplicateWithOverflow"};
    if (createProxyRegions) {
      regionConfigNames = new String[] {"emptyReplicate1", "emptyReplicate2", "persistEmptyReplicate1", "persistEmptyReplicate2"};
    } 
    if (createClientRegions) {
      regionConfigNames = new String[] {"clientReplicate", "clientReplicateWithOverflow", "clientPersistReplicate", "clientPersistReplicateWithOverflow"};
    }
    final int SUBREGION_CHAIN_LENGTH = 8;
    int count = 0;
    Region currentRegion = chainParent;
    for (int i= 1; i <= SUBREGION_CHAIN_LENGTH; i++) {
      String regionConfig = regionConfigNames[count % regionConfigNames.length];
      RegionAttributes regAttr = RegionHelper.getRegionAttributes(regionConfig);
      String diskStoreConfig = regAttr.getDiskStoreName();
      if (diskStoreConfig != null) {
        DiskStoreHelper.createDiskStore(diskStoreConfig);
      }
      fac = theCache.createRegionFactory(regAttr);
      currentRegion = fac.createSubregion(currentRegion, "region_" + ++count);
    }
    // add one more to the end of the chain
    String regionConfig = "persistPR";
    if (createProxyRegions) {
      regionConfig = "prAccessor1";
    }
    RegionAttributes regAttr = RegionHelper.getRegionAttributes(regionConfig);
    String diskStoreConfig = regAttr.getDiskStoreName();
    if (diskStoreConfig != null) {
      DiskStoreHelper.createDiskStore(diskStoreConfig);
    }
    fac = theCache.createRegionFactory(regAttr);
    fac.createSubregion(currentRegion, "region_" + ++count);
   
    // create fanout subregions
    for (Region existingRegion: regionSet) {
      fac = theCache.createRegionFactory(existingRegion.getAttributes());
      fac.createSubregion(fanoutParent, existingRegion.getName());
    }

  }

  public static void logManagerStatus() {
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {
      Log.getLogWriter().info("Cache does not exist in this jvm so cannot log manager status");
    } else {
      ManagementService service = ManagementService.getExistingManagementService(theCache);
      StringBuffer aStr = new StringBuffer();
      aStr.append("manager status\n");
      aStr.append("isManager: " + service.isManager() + "\n");
      Log.getLogWriter().info(aStr.toString());

      if (service.isManager()) {
        Object value = JMXManagerBlackboard.getInstance().getSharedMap().get(RemoteTestModule.getMyVmid());
        if (value != null) {
          Log.getLogWriter().info("This member is a manager and the jmx endpoint is " + value);
        }
      }
    }
  }

}

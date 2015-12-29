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
package diskRecovery;

import com.gemstone.gemfire.admin.SystemMembershipListener;
import com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver;

import java.util.Vector;

import util.TestException;
import hydra.BasePrms;
import hydra.HydraConfigException;

public class RecoveryPrms extends BasePrms {

  /**
   * (int) The number of root regions to create.
   */
  public static Long numRootRegions;
  public static int getNumRootRegions() {
    Long key = numRootRegions;
    int value = tasktab().intAt(key, tab().intAt(key, 1));
    return value;
  }

  /**
   * (int) The number of subregions each region should have.
   */
  public static Long numSubregions;
  public static int getNumSubregions() {
    Long key = numSubregions;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }

  /**
   * (int) The depth of the region hierarchy. Depth of 1 means create only root regions.
   */
  public static Long regionHierarchyDepth;
  public static int getRegionHierarchyDepth() {
    Long key = regionHierarchyDepth;
    int value = tasktab().intAt(key, tab().intAt(key, 1));
    return value;
  }

  /**
   * (Strings) A list of hydra region config names to use.
   */
  public static Long regionConfigNames;
  public static Vector getRegionConfigNames() {
    Long key = regionConfigNames;
    Vector value = tasktab().vecAt(key, tab().vecAt(key, null));
    if (value == null) {
      throw new TestException("diskRecovery.RecoveryPrms.regionConfigName was not specified");
    }
    return value;
  }
  
  /**
   * (boolean) If true, create regions concurrently, false otherwise.
   */
  public static Long concurrentRegionCreation;
  public static boolean getConcurrentRegionCreation() {
    Long key = concurrentRegionCreation;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /**
   * (boolean) true if the test should registerInteger in clients, false otherwise. 
   */
  public static Long registerInterest;
  public static boolean getRegisterInterest() {
    Long key = registerInterest;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /**
   * (boolean) true if the test start a bridge server, false otherwise. 
   */
  public static Long startBridgeServer;
  public static boolean getStartBridgeServer() {
    Long key = startBridgeServer;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /**
   * (boolean) The number of entries per region.
   */
  public static Long maxNumEntriesPerRegion;
  public static int getMaxNumEntriesPerRegion() {
    Long key = maxNumEntriesPerRegion;
    int value = tasktab().intAt(key, tab().intAt(key, 1000));
    return value;
  }

  /**
   * (int) For compaction test, number of keys to destroy and add at a time. 
   */
  public static Long chunkSize;
  public static int getChunkSize() {
    Long key = chunkSize;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /**
   * (String) String to indicate the strategy used for tests that verify the latest
   *              disk files are used for recovery. 
   */
  public static Long testStrategy;
  public static String getTestStrategy() {
    Long key = testStrategy;
    String value = tab().stringAt(key);
    return value;
  }

  /**
   * (String) String to indicate how to mark the vm in the blackboard
   */
  public static Long vmMarker;
  public static String getVmMarker() {
    Long key = vmMarker;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is > lowerThreshold and < upperThreshold.
   */
  public static Long operations;
  public static String getOperations() {
    Long key = operations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is >=  upperThreshold.
   */
  public static Long upperThresholdOperations;
  public static String getUpperThresholdOperations() {
    Long key = upperThresholdOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is >=  upperThreshold.
   */
  public static Long lowerThresholdOperations;
  public static String getLowerThresholdOperations() {
    Long key = upperThresholdOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in edge clients
   *           the region size is > lowerThresholdClient and < upperThresholdClient.
   */
  public static Long clientOperations;
  public static String getClientOperations() {
    Long key = clientOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in edge clients
   *           the region size is >=  upperThresholdClient.
   */
  public static Long upperThresholdClientOperations;
  public static String getUpperThresholdClientOperations() {
    Long key = upperThresholdClientOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Operations used for regions in servers or data hosts when
   *           the region size is >=  upperThreshold.
   */
  public static Long lowerThresholdClientOperations;
  public static String getLowerThresholdClientOperations() {
    Long key = upperThresholdClientOperations;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (int) The upper threshold that will trigger use of upperThresholdOperations
   *        in servers or data host vms.
   */
  public static Long upperThreshold;
  public static int getUpperThreshold() {
    Long key = upperThreshold;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The lower threshold that will trigger use of lowerThresholdOperations.
   *        in servers or data host vms.
   */
  public static Long lowerThreshold;
  public static int getLowerThreshold() {
    Long key = lowerThreshold;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The upper threshold that will trigger use of upperThresholdClientOperations
   *        in edge clients.
   */
  public static Long upperThresholdClient;
  public static int getUpperThresholdClient() {
    Long key = upperThresholdClient;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The lower threshold that will trigger use of lowerThresholdClientOperations.
   *        in edge clients.
   */
  public static Long lowerThresholdClient;
  public static int getLowerThresholdClient() {
    Long key = lowerThresholdClient;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The number of new keys to be included in the the map argument to putAll. 
   */
  public static Long numPutAllNewKeys;  
  public static int getNumPutAllNewKeys() {
    Long key = numPutAllNewKeys;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The number of new keys to be included in the the map argument to putAll. 
   */
  public static Long numPutAllExistingKeys;  
  public static int getNumPutAllExistingKeys() {
    Long key = numPutAllExistingKeys;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (int) The number of seconds to run a concurrent test.
   *           We cannot use hydra's taskTimeSec parameter
   *           because of a small window of opportunity for the test to hang due
   *           to the test's "concurrent round robin" type of strategy.
   */
  public static Long secondsToRun;  
  public static int getSecondsToRun() {
    Long key = secondsToRun;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /** (boolean) If true, then each thread will only write a unique set of keys.
   * 
   */
  public static Long useUniqueKeys;
  public static boolean getUseUniqueKeys() {
    Long key = useUniqueKeys;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (class name) Specifies the class name of a PersistenceObserver
   * 
   */
  public static Long persistenceObserver;
  public static PersistenceObserver getPersistenceObserver() {
    Long key = persistenceObserver;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    try {
      return (PersistenceObserver)instantiate(key, val);
    } catch (ClassCastException e) {
      throw new HydraConfigException("Illegal value for " + nameForKey(key) + ": " + val + " does not implement SystemMembershipListener", e );
    }
  }
  
  /** (boolean) If true use AdminDistributedSystem.shutDownAllMembers to stop vms. 
   * 
   */
  public static Long useShutDownAll;
  public static boolean getUseShutDownAll() {
    Long key = useShutDownAll;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (boolean) If true, use a 6.5 xml file to convert pre-6.5 disk files to
   *            6.5 (this explicitly names the DiskStores).
   *            If false, use a pre-6.5 xml file with DiskWriteAttributes
   *            to convert to 6.5 files (this automatically generates DiskStores).
   * 
   */
  public static Long convertWithNewVersionXml;
  public static boolean getConvertWithNewVersionXml() {
    Long key = convertWithNewVersionXml;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (boolean) If true use AdminDistributedSystem.shutDownAllMembers to stop vms. 
   * 
   */
  public static Long killDuringShutDownAll;
  public static boolean getKillDuringShutDownAll() {
    Long key = killDuringShutDownAll;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (boolean) If true, create regions using xml file, if false create
   *            regions programmatically.
   * 
   */
  public static Long createRegionsWithXml;
  public static boolean getCreateRegionsWithXml() {
    Long key = createRegionsWithXml;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (String) Directory containing a disk file converter script.
   */
  public static Long scriptLocation;
  public static String getScriptLocation() {
    Long key = scriptLocation;
    String value = tasktab().stringAt(key, tab().stringAt(key));
    return value;
  }

  /** (String) Get the name of a gateway hub config.
   */
  public static Long hubConfigName;
  public static String getHubConfigName() {
    Long key = hubConfigName;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Get the name of a gateway hub config.
   */
  public static Long gatewayConfigName;
  public static String getGatewayConfigName() {
    Long key = gatewayConfigName;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    return value;
  }

  /** (String) Class name of value to put into the region. Currently
   *  supported values are:
   *     util.ValueHolder (default)
   *     util.VHDataSerializable
   *     util.VHDataSerializableInstantiator
   */
  public static Long valueClassName;
  public static String getValueClassName() {
    Long key = valueClassName;
    String value = tasktab().stringAt(key, tab().stringAt(key, "util.ValueHolder"));
    return value;
  }

  /** (boolean) If true, then register the class VHDataSerializer, otherwise
   *            do not register this class.
   */
  public static Long registerSerializer;
  public static boolean getRegisterSerializer() {
    Long key = registerSerializer;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /** (int) The number of entries to load for each thread running the task.
   */
  public static Long numToLoad;  
  public static int getNumToLoad() {
    Long key = numToLoad;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }
   
  /** (int) The number of seconds to sleep before proceeding with a shutDownAll.
   *        This allows async ops to pile up before shutDownAll is called. 
   */
  public static Long sleepSecBeforeShutDownAll;  
  public static int getSleepSecBeforeShutDownAll() {
    Long key = sleepSecBeforeShutDownAll;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }

  /** (boolean) If true use colocated partitioned regions. 
   * 
   */
  public static Long useColocatedPRs;
  public static boolean getUseColocatedPRs() {
    Long key = useColocatedPRs;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }
  
  /** (int) The number of PRs to use in the test. 
   * 
   */
  public static Long maxPRs;
  public static int getMaxPRs() {
    int DEFAULT_PRS_USE_CASE = 18; // newEdge use case is 18 prs
    Long key = maxPRs;
    int value = tasktab().intAt(key, tab().intAt(key, DEFAULT_PRS_USE_CASE));
    return value;
  }
  
  /** (int) The number of replicate regions to use in the test. 
   * 
   */
  public static Long maxReplicates;
  public static int getMaxReplicates() {
    int DEFAULT_REPLICATES_USE_CASE = 10; // newEdge use case is 10
    Long key = maxReplicates;
    int value = tasktab().intAt(key, tab().intAt(key, DEFAULT_REPLICATES_USE_CASE));
    return value;
  }

  /**
   * (String) String to indicate the method to use for destroying entries.
   * Choices are destroy, clearRegion and expiration.
   *
   * With expiration the entryTimeToLive or entryIdleTimeout must be set to 10 seconds
   * with an action of destroy.
   */
  public static Long destroyMethod;
  public static String getDestroyMethod() {
    Long key = destroyMethod;
    String value = tab().stringAt(key);
    return value;
  }

  public static Long setIgnorePreallocate;
  public static boolean setIgnorePreallocate(){
    Long key = setIgnorePreallocate;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  //------------------------------------------------------------------------
  // Utility methods
  //------------------------------------------------------------------------

  private static Object instantiate(Long key, String classname) {
    if (classname == null) {
      return null;
    }
    try {
      Class cls = Class.forName(classname);
      return cls.newInstance();
    } catch (Exception e) {
      throw new HydraConfigException("Illegal value for " + nameForKey(key) + ": cannot instantiate " + classname, e);
    }
  }


  // ================================================================================
  static {
    BasePrms.setValues(RecoveryPrms.class);
  }

}

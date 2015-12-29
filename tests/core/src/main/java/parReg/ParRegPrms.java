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

import hydra.BasePrms;
import hydra.ClientVmMgr;
import hydra.TestConfig;
import util.*;

import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;


public class ParRegPrms extends BasePrms {

/** (boolean) If true then the test will designate ops by the vm type,
 *            for example a dataStore and accessor vm will use different
 *            sets of ops. If this is true, then ParRegTest will choose
 *            ops based on ParRegPrms.dataStoreOperations and 
 *            ParRegPrms.accessorOperations. If this is false, then the
 *            test chooses operations from ParRegPrms.entryOperations.
 */
public static Long designateOps;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do, as long as the global region
 *                      size is < regionSizeThreshold.
 */
public static Long entryOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do for a data store vm, as long as 
 *                      the global region size is < regionSizeThreshold.
 */
public static Long dataStoreOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do for an accessor vm, as long as 
 *                      the global region size is < regionSizeThreshold.
 */
public static Long accessorOperations;  

/** (int) The size of the partitioned region that will trigger the
 *        test to choose its operations from lowerThresholdOperations.
 */
public static Long lowerThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold.
 */
public static Long lowerThresholdOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do in a datastore vm when the region 
 *                      size falls below lowerThresold.
 */
public static Long lowerThresholdDataStoreOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do in an accessor vm when the region 
 *                      size falls below lowerThresold.
 */
public static Long lowerThresholdAccessorOperations;  

/** (int) The upper size of the partitioned region that will trigger the
 *        test to choose its operations from upperThresholdOperations.
 */
public static Long upperThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do in a datastore vm when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdDataStoreOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do in an accessor vm when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdAccessorOperations;  

/** (int) The global size of the partitioned region that will trigger the
 *        test to choose its operations from thresholdOperations.
 */
public static Long numOpsPerTask;  

/** (boolean) True if the test is doing high availability, false otherwise.
 */
public static Long highAvailability;  

/** (int) The number of seconds to run a test (currently used for concParRegHA)
 *        to terminate the test. We cannot use hydra's taskTimeSec parameter
 *        because of a small window of opportunity for the test to hang due
 *        to the test's "concurrent round robin" type of strategy.
 */
public static Long secondsToRun;  

/** (int) The number of VMs to stop at a time during an HA test.
 */
public static Long numVMsToStop;  

/** (int) The local max memory to use when reinitializing a stopped VM.
 */
public static Long localMaxMemory;  

/** (String) The possible stop modes for stopping a VM. Can be any of
 *           MEAN_EXIT, MEAN_KILL, NICE_EXIT, NICE_KILL.
 */
public static Long stopModes;  

/** (String) A String to use to choose vms to stop.
 */
public static Long stopVMsMatchStr;  

/** (String) A String to use to not choose vms to stop.
 */
public static Long stopVMsExcludeMatchStr;

/** (boolean) True if the test should use locking on PR operations
 *            to workaround data inconsistency problems (being addressed for
 *            5.1 release), false otherwise.
 *            lynn remove when primary buckets are implemented?
 */
public static Long lockOperations;  

/** (String) This is how a concParRegBridge opts to workaround bridge
 *           ordering problems (bug 35662).
 *           lynn - remove when bug 35662 is fixed.
 *           Can be either of: 
 *               registerInterest - the test will refresh the clients
 *                  with registerInterest to put them back in sync with
 *                  the bridge servers (before doing validation).
 *               uniqueKeys - each client thread works on a unique set
 *                  of keys to avoid ordering races
 */
public static Long bridgeOrderingWorkaround;  

/** (String) The number of new keys to be included in the the map argument to putAll. 
 *           This can be one of:
 *              - an int specifying a particular number
 *              - "useThreshold", which will fill the map with a random number of new 
 *                 keys such that a putAll will not exceed ParRegPrms.upperThreshold
 *                 or put more than ParRegPrms.numPutAllMaxNewKeys new keys
 *                 or put less than ParRegPrms-numPutAllMinNewKeys new keys.
 */
public static Long numPutAllNewKeys;  

/** (String) The maximum number of new keys to be included in the the map
 *           argument to putAll when using the "useThreshold" seting for
 *           ParRegPrms.numPutAllNewKeys.  Defaults to no set maximum.
 */
public static Long numPutAllMaxNewKeys;

/** (String) The minimum number of new keys to be included in the the map
 *           argument to putAll when using the "useThreshold" seting for
 *           ParRegPrms.numPutAllNewKeys.  Defaults to 1.
 */
public static Long numPutAllMinNewKeys;

/** (int) The number of existing keys (to be included in the the map argument to putAll. 
 *        If this many keys do not exist, then use as many as possible.
 */
public static Long numPutAllExistingKeys;  

/** (boolean) Limits the size of a putAll to 1 (either a new or existing key). This
 *            overrides all of the above settings if true. Defaults to false.
 */
public static Long limitPutAllToOne;  
public static boolean getLimitPutAllToOne() {
  Long key = limitPutAllToOne;
  boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
  return value;
}

  /**
   * (String)This is for the Function routing test for the PR tests. This
   * defines how the data for the routing function is passed on.
   * 
   */
  public static Long partitionResolverData;

  /**
   * (Boolean)The following boolean is to check whether the test is with
   * partition resolver.
   */
  public static Long isWithRoutingResolver;

  /**
   * (int)This is for the test to use the variable number of data stores for the
   * PR.
   * 
   */
  public static Long numberOfDataStore;
  
  /**
   * (int)Test configuration to find out the number of accessors required
   * 
   */
  public static Long numberOfAccessors;
  public static int getNumberOfAccessors() {
    Long key = numberOfAccessors;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }
  
  /**
   * (boolean)Test configuration to check whether this is an PR eviction test
   * 
   */
  public static Long isEvictionTest;

  /**
    * (String) Class name of the InternalResourceManager.ResourceObserver
    * to install on the cache.  Used by rebalance tests.  Defaults to null.
    * This provides a test hook callback when rebalance or recovery is
    * started/finished.
    */
  public static Long resourceObserver;
  public static InternalResourceManager.ResourceObserver getResourceObserver() {
    String className = tab().stringAt( ParRegPrms.resourceObserver, null );
    InternalResourceManager.ResourceObserver resourceObserver = null;

    if (className != null) {
       resourceObserver = (InternalResourceManager.ResourceObserver)TestHelper.createInstance( className );
    }
    return resourceObserver;
  }

  /**
   * (boolean) If true, recover from existing disk file, false otherwise.
   */
  public static Long recoverFromDisk;
  public static boolean getRecoverFromDisk() {
    Long key = recoverFromDisk;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /**
   * (boolean) If true, test uses shutDownAllMembers
   */
  public static Long useShutDownAllMembers;
  public static boolean getUseShutDownAllMembers() {
    Long key = useShutDownAllMembers;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /**
   * (int) The number of ops to do for each operation based on the 
   *       percentage of the region size.
   */
  public static Long opPercentage;
  public static int getOpPercentage() {
    Long key = opPercentage;
    int value = tasktab().intAt(key, tab().intAt(key));
    return value;
  }

  /**
   * (int) The number of bridge clients that should be empty.
   */
  public static Long numEmptyClients;
  public static int getNumEmptyClients() {
    Long key = numEmptyClients;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }
  
  /**
   * (int) The number of bridge clients that should be thin (meaning
   * they are configured with eviction to keep a small set of entries).
   */
  public static Long numThinClients;
  public static int getNumThinClients() {
    Long key = numThinClients;
    int value = tasktab().intAt(key, tab().intAt(key, 0));
    return value;
  }

  /**
   * (boolean) If true the test should run online backup.
   */
  public static Long doOnlineBackup;
  public static boolean getDoOnlineBackup() {
    Long key = doOnlineBackup;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }
  
  /**
   * (boolean) If true the test should run incremental backup.
   */
  public static Long doIncrementalBackup;
  public static boolean getDoIncrementalBackup() {
    Long key = doIncrementalBackup;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  /**
   * (boolean) If true create PR accessor.
   */
  public static Long createAccessor;
  public static boolean getCreateAccessor() {
    Long key = createAccessor;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }
  
  /**
   * (boolean) argument to PartitionManager.createPrimaryBucket(...).
   */
  public static Long destroyExistingRemote;
  public static boolean getDestroyExistingRemote() {
    Long key = destroyExistingRemote;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key));
    return value;
  }
  
  /**
   * (boolean) argument to PartitionManager.createPrimaryBucket(...).
   */
  public static Long destroyExistingLocal;
  public static boolean getDestroyExistingLocal() {
    Long key = destroyExistingLocal;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key));
    return value;
  }
  
  /**
   * (String) A query string.
   */
  public static Long query;
  public static String getQuery() {
    Long key = query;
    String value = tasktab().stringAt(key, tab().stringAt(key));
    return value;
  }
  
  /**
   * (String) An index string.
   */
  public static Long index;
  public static String getIndex() {
    Long key = index;
    String value = tasktab().stringAt(key, tab().stringAt(key));
    return value;
  }
  
  /**
   * (boolean) True if an instance of BaseValueHolder should test whether equals
   *           is being called for concurrent map operations. 
   */
  public static Long testMapOpsEquality;
  public static Boolean getTestMapOpsEquality() {
    Long key = testMapOpsEquality;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }
  
// ================================================================================
static {
   BasePrms.setValues(ParRegPrms.class);
}

}

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

package splitBrain;

import hydra.*;
import util.TestHelper;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.admin.*;
import java.util.*;

/**
 *
 * A class used to store keys for SplitBrain Tests
 *
 */

public class SplitBrainPrms extends BasePrms {

/** (boolean) True if the test should use locking.
 */
public static Long lockOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do, as long as the global region
 *                      size is < regionSizeThreshold.
 */
public static Long entryOperations;  

/** (int) The size of the partitioned region that will trigger the
 *        test to choose its operations from lowerThresholdOperations.
 */
public static Long lowerThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold.
 */
public static Long lowerThresholdOperations;  

/** (int) The upper size of the partitioned region that will trigger the
 *        test to choose its operations from upperThresholdOperations.
 */
public static Long upperThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdOperations;  

/** (int) The global size of the partitioned region that will trigger the
 *        test to choose its operations from thresholdOperations.
 */
public static Long numOpsPerTask;  

/** (String) Indicates the host (host1 or host2) NOT expected to survive a 
 *  NetworkPartition.  This is used when Shutdown and CacheClosedExceptions 
 *  are encountered to determine whether or not they should be accepted.
 *  Expected values are literally, host1 or host2.
 */
public static Long losingPartition;

/** (String) Indicates the HostPrms-name for the first remoteHost (remote
 *  from Master).  Used by SBUtil for network drop/restore.
 *  Defaults to 'host1'.
 */
public static Long hostDescription1;

/** (String) Indicates the HostPrms-name for the second remoteHost (remote
 *  from Master).  Used by SBUtil for network drop/restore.
 *  Defaults to 'host2'.
 */
public static Long hostDescription2;

/** (Vector) Indicates the HostPrms-names for all the remoteHosts (remote
 *  from Master).  Used by SBUtil for network drop/restore.
 *  Defaults to 'host2'.
 */
public static Long hostPairsForConnectionDropRestore;

/** (Boolean) Expect no datastore found exception on accessor nodes.
 * This is specific to certain scenarios where we lose datastores
 * due to network partitioning etc.
 */
public static Long expectNoDatastoreExceptionOnAccessor;


/** (boolean) Whether or not to execute the NetworkHelper (drop/restore)
 *  commands.  Defaults to true.
 *  This can be set to false in a local.conf to disable the NetworkHelper
 *  commands (to allow testing when multiple boxes are not available for
 *  exclusive use).
 */
public static Long enableNetworkHelper;

/** (String) Indicates the dropType (ONEWAY or TWOWAY) for network
 *  drop/restore commands.  Defaults to TWOWAY.
 *  5/20/08 - all tests now use dropType.TWOWAY only.
 */
public static Long dropType;

public static int getDropType() {
   Long key = dropType;
   String str = TestConfig.tasktab().stringAt(key, tab().stringAt(key, "twoWay"));
   if (str.equalsIgnoreCase("oneWay")) {
     return (SBUtil.ONEWAY);
   } else if (str.equalsIgnoreCase("twoWay")) {
     return (SBUtil.TWOWAY);
   } else {
     String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
     throw new HydraConfigException(s);
   }
}

/** (int) dropConnections will sleep for dropWaitTimeSec after dropping 
 *  the network connections.  This allows serial tests to 'settle' after  
 *  the network is dropped.  For example, serial PR tests which need to 
 *  re-establish redundancy after the drop.  (defaults to 0)
 */
public static Long dropWaitTimeSec;

public static int getDropWaitTimeSec() {
   Long key = dropWaitTimeSec;
   return (TestConfig.tasktab().intAt(key, tab().intAt(key, 0)));
}

/** (boolean) True if the test execute operations within a single transaction
 *  Defaults to false
 */
public static Long useTransactions;
public static boolean useTransactions() {
  Long key = useTransactions;
  return tasktab().booleanAt( key, tab().booleanAt( key, false ));
}

/** (int) In transaction tests, the percentage of transactions to perform commit
 *  (vs. rollback).  Default = 100%.
 */
public static Long commitPercentage;
public static int getCommitPercentage() {
  Long key = commitPercentage;
  return tasktab().intAt( key, tab().intAt( key, 100 ));
}

/** (int) The number of seconds to run before terminating the test. 
 *        We cannot use hydra's taskTimeSec parameter because of a small 
 *        window of opportunity for the test to hang due to the test's 
 *        "concurrent round robin" type of strategy.
 */
public static Long secondsToRun;  

/** (int) maximum number of keys to create while loading region
 *  See NetworkPartitionTest.loadRegion()
 */
public static Long maxKeys;

/** (boolean) whether or not the coordinator is on the losingSide of the 
 *  networkPartition (for NetworkPartition tests only).  Affects validation.
 *  Defaults to false
 */
public static Long coordinatorOnLosingSide;

/** (boolean) Indicates giiClients (for gii NetworkPartitionTests)
 *  Needed to determine if validation of regionSize required.
 *  Defaults to false
 */
public static Long isGiiClient;

/** (boolean) Indicates getFromOne (replicate) getInitialImage.  When
 *  this is interrupted by a FORCED_DISCONNECT, we clear the region (so
 *  we should verify that the region is indeed empty).
 *  Used by NetworkPartition-giiPref tests
 */
public static Long expectEmptyRegion;

/**
 * (String) DataPolicy for giiClients in gii NetworkPartition tests.
 */
public static Long giiDataPolicy;
public static DataPolicy getGiiDataPolicy() {
Long key = giiDataPolicy;

   String sDataPolicy = tasktab().stringAt(key, tab().stringAt(key, null));
   return (getDataPolicy(sDataPolicy, key));
}

/**
 * (String) How to cause an unhealthy vm. Can either be "sick" or "slow".
 */
public static Long unhealthiness;

/**
 * (String) Whether to combine a dead vm with unhealthinessSelection.
 */
public static Long playDead;

//------------------------------------------------------------------------
// Utility methods
//------------------------------------------------------------------------

/**
 * Returns the DataPolicy for the given string.
 * Allows null (if dataPolicy string is null).
 */
private static DataPolicy getDataPolicy(String str, Long key) {
  if (str == null) {
    return null;
  } 

  if (str.equalsIgnoreCase("empty")) {
    return DataPolicy.EMPTY;
  } else if (str.equalsIgnoreCase("normal")) {
    return DataPolicy.NORMAL;
  } else if (str.equalsIgnoreCase("partition")
          || str.equalsIgnoreCase("partitioned")) {
      return DataPolicy.PARTITION;
  } else if (str.equalsIgnoreCase("persistentReplicate")
          || str.equalsIgnoreCase("persistentReplicated")
          || str.equalsIgnoreCase("persistent_replicate")
          || str.equalsIgnoreCase("persistent_replicated")) {
    return DataPolicy.PERSISTENT_REPLICATE;
  } else if (str.equalsIgnoreCase("replicate")
          || str.equalsIgnoreCase("replicated")) {
    return DataPolicy.REPLICATE;
  } else {
    String s = BasePrms.nameForKey(key) + " has illegal value: " + str;
    throw new HydraConfigException(s);
  }
}

private static Object instantiate( Long key, String classname ) {
  if ( classname == null ) {
    return null;
  }
  try {
    Class cls = Class.forName( classname );
    return cls.newInstance();
  } catch( Exception e ) {
    throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": cannot instantiate " + classname, e );
  }
}

// ================================================================================
static {
   BasePrms.setValues(SplitBrainPrms.class);
}

}

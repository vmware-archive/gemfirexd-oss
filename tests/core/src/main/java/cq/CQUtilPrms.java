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

package cq;

import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

/**
 *
 *  A class used to store keys for cq.CQUtil configuration settings.
 *
 */

public class CQUtilPrms extends BasePrms {

  /** (boolean) Whether or not to execute CQ related operations
   *  Defaults to false
   */
  public static Long useCQ;

  /** (String) ClassName for CQListener
   */
  public static Long cqListener;

  /**
   * Returns the <code>CQListener</code> to be used in registering CQs via CQUtil
   *
   * @see CQListener
   */
  public static CqListener getCQListener() {
    String className = TestConfig.tasktab().stringAt(CQUtilPrms.cqListener, TestConfig.tab().stringAt(CQUtilPrms.cqListener, null)); 
    CqListener cqListener = null;
    if (className == null) {
       cqListener = new CQTestListener();
    } else {
       cqListener = (CqListener)TestHelper.createInstance((String)className);
    }
    return cqListener;
  }

  /** (boolean) Whether or not to log the results of operations initiated by
   *  doCQOperations().  Defaults to false
   */
  public static Long logCQOperations;

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do, as long as the region
 *                      size is < regionSizeThreshold.
 */
public static Long entryOperations;  

/** (Vector of Strings) A list of entry operations allowed on a server,
 *                      as long as the region size is < regionSizeThreshold.
 */
public static Long serverEntryOperations;  

/** (Vector of Strings) A list of entry operations allowed on a client,
 *                      as long as the region size is < regionSizeThreshold.
 */
public static Long clientEntryOperations;  

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
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold in a bridge server.
 */
public static Long lowerThresholdServerOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold in a bridge client.
 */
public static long lowerThresholdClientOperations;

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
 *                      test is allowed to do when the region exceeds
 *                      upperThresold in a bridge server.
 */
public static Long upperThresholdServerOperations;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region exceeds
 *                      upperThresold in a bridge client.
 */
public static Long upperThresholdClientOperations;  

/** (int) The global size of the partitioned region that will trigger the
 *        test to choose its operations from thresholdOperations.
 */
public static Long numOpsPerTask;  

/** (boolean) True if the test is doing high availability, false otherwise.
 */
public static Long highAvailability;  

/** (int) The number of seconds to run a test (currently used for concurrent tests)
 *        to terminate the test. We cannot use hydra's taskTimeSec parameter
 *        because of a small window of opportunity for the test to hang due
 *        to the test's "concurrent round robin" type of strategy.
 */
public static Long secondsToRun;  

/** (int) The number of VMs to stop at a time during an HA test.
 */
public static Long numVMsToStop;  

/** (String) The possible stop modes for stopping a VM. Can be any of
 *           MEAN_EXIT, MEAN_KILL, NICE_EXIT, NICE_KILL.
 */
public static Long stopModes;  

/** (String) The number of levels in a QueryObject, which implies the number of
 *           levels in a query.
 */
public static Long queryDepth;  

/**
 * (int) The number of bridge vms.
 */
public static Long numBridges;

/** 
 * (int) The number of feeder threads.
 */
public static Long feederThreads;

/** 
 * (int) The number of edge threads.
 */
public static Long edgeThreads;

/**
 * (int) The number of edge threads per vm
 */
public static Long edgeThreadsPerVM;

/**
 *  (boolean) edges perform put operation and verify results
 */
public static Long edgePutAndVerify;

/**
 * (boolean) use certain query strings to test primary index.
 */
public static Long testPrimaryIndex;

/**
 * (String) used for query purpose, only entry operations on this key will be a qualified cq event
 */
public static Long primary;

/**
 * (boolean) if true, bridges will create primary index on secId.
 */
public static Long createPrimaryIndex;

/**
 * (boolean) if true, bridges will create functional index on qty.
 */
public static Long createQtyIndex;

/**
 * (boolean) if true, bridges will create functional index on mktValue.
 */
public static Long createMktValueIndex;

/**
 * (boolean) if true, bridges will create index.
 */
public static Long createIndex;

/**
 * (int) used to determine the mininum size of putAllMap.
 */
public static Long minPutAllSize;

/**
 * (int) used to determine the maximum size of the putAllMap.
 */
public static Long maxPutAllSize;

/** 
 * (boolean) If true, then each thread uses a unique set of keys for its operations,
 *           false otherwise. This is used to workaround ordering problems so verification
 *           can be done.
 */
public static Long useUniqueKeys;

/** 
 * (String) The number of queries to execute per client VM in this test run.
 */
public static Long numQueriesPerClientVM;  

/**
 * (String) valueConstraint class name
 */
public static Long valueConstraint;

/** (boolean) Whether or not the CQEventTest initializeClientRegion method will
 *  also registerInterest (in all keys)
 */
public static Long registerInterest;

/** (boolean) Whether or not the ops client is in a feeder task (in same DS w/bridge)
 *  Used to determine if INVALIDATE events should be seen in client VMs
 */
public static Long clientInFeederVm;

/** (int) The number of new keys to add to the region.
 */
public static Long numNewKeys;  

/** (boolean) If true (the default), then the test is run with cqs executing, false
 *  otherwise. This is only used to isolate problems for debugging purposes.
 */
public static Long CQsOn;  

/** (boolean) True if the test should use delta objects.
 */
public static Long useDeltaObjects;  

/** (int) The number of keys in the region.
 */
public static Long numKeys;  

/** (boolean) Whether or not to require the exact CQEvent count (vs.
  *  a minimum value).  Used by validation method (for cq/hct tests) in CQUtil.
  */
  public static Long exactCounterValue;

  /** (boolean) If true, then the test is run by obtaining QueryService from Pool, false
   *  the QueryService will be obtained using Cache.
   */
  public static Long QueryServiceUsingPool;  

  /* Pool Name for the Query Service.
   * 
   */
  public static Long QueryServicePoolName;
  
  /* The class name of the object to use as a value.
   * 
   */
  public static Long  objectType;
  
  public static String getQueryServicePoolName() {
    return TestConfig.tasktab().stringAt(CQUtilPrms.QueryServicePoolName, TestConfig.tab().stringAt(CQUtilPrms.QueryServicePoolName)); 
  }
  
  public static String getRegionPoolName() {
    return TestConfig.tasktab().stringAt(RegionPrms.poolName, TestConfig.tab().stringAt(RegionPrms.poolName)); 
  }
  
  static {
      setValues( CQUtilPrms.class );
  }

  public static void main( String args[] ) {
      dumpKeys();
  }
  
  
}

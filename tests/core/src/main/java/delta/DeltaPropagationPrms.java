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

package delta;

import hydra.*;

/**
 * 
 * A class to store keys related to client queue HA.
 * 
 * @author aingle
 * @since 6.1
 */

public class DeltaPropagationPrms extends BasePrms {

  public static Long numPutThreads;

  public static Long numKeyRangePerThread;

  /**
   * (Vector of Strings) A list of the operations on a region entry that this
   * test is allowed to do. Can be one or more of:<br>
   * put - creates a new key/val in a region or updates it if key already
   * exists.<br>
   * invalidate - invalidate an entry in a region.<br>
   * destroy - destroy an entry in a region.<br>
   */
  public static Long entryOperations;

  /**
   *  Parameter used to create region name in the tests.
   */
  public static Long regionName;

  /**
   * Parameter used to specify number of regions in the tests.
   */
  public static Long numberOfRegions;

  /**
   * Parameter used to pick the region randomly to perform operations.
   */
  public static Long regionRange;

  /**
   * Parameter to get the minimum numbers of servers required alive in failover
   * tests. This is required while deciding whether a server should be killed or
   * not.
   */
  public static Long minServersRequiredAlive;

  public static Long maxClientsCanKill;

  /**
   * An integer corresponding to the entry-operation to be performed. This can
   * be from 1 to 7 corresponding to create, update, invalidate, destroy,
   * registerInterest,unregisterInterest and killing client respectively.
   */
  public static Long opCode;

  /**
   * Amount of time each {@link Feeder#feederTask} should last, in seconds.
   */
  public static Long feederTaskTimeSec;

  /**
   * Sets the client conflation
   */
  public static Long clientConflation;


  /**
   * Decides which implementation of Delta to use.
   */
  public static Long objectType;
  
  /**
   * Setting to simulate random failures.
   */
  public static Long enableFailure;

  public static String getClientConflation() {
    Long key = clientConflation;
    return tasktab().stringAt(key, tab().stringAt(key, "default"));
  }

  /** (boolean) If true, then the test is run by obtaining QueryService from Pool, false
   *  the QueryService will be obtained using Cache.
   */
  public static Long QueryServiceUsingPool;  

  /* Pool Name for the Query Service.
   * 
   */
  public static Long QueryServicePoolName;
  
  public static String getQueryServicePoolName() {
    return TestConfig.tasktab().stringAt(DeltaPropagationPrms.QueryServicePoolName, TestConfig.tab().stringAt(DeltaPropagationPrms.QueryServicePoolName)); 
  }
  
/** (boolean) True if the test is doing high availability, false otherwise.
 */
public static Long highAvailability;  

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

/** (String) The number of levels in a QueryObject, which implies the number of
 *           levels in a query.
 */
public static Long queryDepth;  

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

/** (int) The number of seconds to run a test (currently used for concurrent tests)
 *        to terminate the test. We cannot use hydra's taskTimeSec parameter
 *        because of a small window of opportunity for the test to hang due
 *        to the test's "concurrent round robin" type of strategy.
 */
public static Long secondsToRun;  

/** (String) Can be one of "zero" or "nonZero". "nonZero" will
 *  randomly choose between 1, 2 or 3.
 */
public static Long redundantCopies;  

/** (int) The number of round robin cycles to complete for a serial test.
 */
public static Long numberOfCycles;  

/** (boolean) Enable/disable cloning validation in DeltaObject, default is true.
 */
public static Long enableCloningValidation;  

/** (int) Used to specifiy a test case for bad delta testing.
 *        This is used only for debugging in a local.conf, and is not set 
 *        in the .confs.
 */
public static Long testCase;  

/** (int) The number of servers in the test.
 */
public static Long numServers;  

/** (String) The hydra.RegionPrms-name of the region to create.
 */
public static Long regionPrmsName;  
public static String getRegionPrmsName() {
   Long key = regionPrmsName;
   String val = tasktab().stringAt( key, tab().stringAt(key, null) );
   return val;
}

/** (int) The payload size (byte[] size) for eviction tests when updating
 *        a key.
 */
public static Long payloadSize;  
public static int getPayloadSize() {
   Long key = payloadSize;
   int val = tasktab().intAt( key, tab().intAt(key, -1) );
   return val;
}

/** (int) The pretend payload size used by the PretendSizer. 
 */
public static Long pretendSize;  
public static int getPretendSize() {
   Long key = pretendSize;
   int val = tasktab().intAt( key, tab().intAt(key, -1) );
   return val;
}

/** (String) The class name of the value to use (either util.ValueHolder
 *           or delta.DeltaValueHolder
 */
public static Long valueClass;  
public static String getValueClass() {
   Long key = valueClass;
   String val = tasktab().stringAt( key, tab().stringAt(key, "notSpecified"));
   return val;
}

  static {
    setValues(DeltaPropagationPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}

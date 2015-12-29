
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

package hct;

import hydra.*;
import util.*;
import com.gemstone.gemfire.cache.util.*;

/**
 * Hydra parameters for the hierarchical cache (cache server) tests.
 * It contains parameters for configuring both the cache server and
 * the the edge client VMs.
 *
 * @author Belinda Buenafe
 * @since 2.0.2
 */
public class HctPrms extends hydra.BasePrms {

  /** Name of region in edge and server caches */
   public static Long regionName;

  /** Which system component to kill */
   public static Long whatToKill;

  /** Wait period between kill and restart */
   public static Long restartWaitSec;

  /** Milliseconds between running kill task */
   public static Long killInterval;

  /** Do we alternate between killing two cache servers?  Or do we
   * just kill a random one? */
   public static Long toggleKill;

  /** (boolean) Do we allow a shutdown of the cacheServer in addition to kills?
   *  If true, killSomething will randomly choose (per invocation).
   *  Defaults to false
   */
  public static Long allowDisconnect;

  /** Wait period between gets in doGet task */
   public static Long getIntervalMs;

  /** Number of extra gets to execute on an entry after it iss loaded
   * in */
   public static Long hitsPerMiss;

  /** workLoad based tests (bloom-vm) limit the key space */
  public static Long maxKeys;
  
  /** Num times to run doGet in {@link HierCache#doGetBatch} task */
   public static Long getBatchSize;

  /** The cache loader used by the cache server.  Choices are:
   * <code>default</code>, <code>ccache</code>, <code>oracle</code>.
   * Any other value is treated as default */
   public static Long databaseLoader; 

  /** Wait period in DB loader */
   public static Long dbLoadTimeMs;

  /** Type of object returned by loader such as <code>bytearray</code>,
   * <code>string</code>, <code>xmlstring</code>, or the name of a
   * class with a zero-argument constructor. */
   public static Long objectType;

  /** For the useCase1Failover.conf, this is the average size of the entries
   *  in the region */
   public static Long averageSize;

  /** Whether to print debugging output */
   public static Long debug;

  /** (String) className of BridgeMembershipListener for edge clients */
   public static Long edgeMembershipListener;

  /** (String) className of BridgeMembershipListener for bridgeServer */
   public static Long serverMembershipListener;

  /** (String) className of UniversalMembershipListener for bridge & client */
   public static Long universalMembershipListener;

   public static Long storeNodeData;
   public static Long blackboardType;
   public static Long blackboardName;

  /**
   * (boolean(s))
   * Receive Values As Invalidates.
   */
  public static Long receiveValuesAsInvalidates;

  /**
   * Method to return an instance of BridgeMembershipListener as 
   * specified by the given Prm.
   *
   * @see HctPrms.edgeMembershipListener
   * @see HctPrms.serverMembershipListener
   */
  public static BridgeMembershipListener getMembershipListener( Long prm ) {
    String className = TestConfig.tab().stringAt( prm, null );
    BridgeMembershipListener bml = null;

    if (className != null) {
      bml = (BridgeMembershipListener)TestHelper.createInstance( className );
    }
    return bml;
  }

  /**
   * Method to return an instance of UniversalMembershipListener as 
   * specified by the given Prm.
   *
   * @see HctPrms.universalMembershipListener
   */
  public static UniversalMembershipListener getUniversalMembershipListener() {
    Long key = universalMembershipListener;
    String className = TestConfig.tab().stringAt( key, null );
    UniversalMembershipListener uListener = null;

    if (className != null) {
      uListener = (UniversalMembershipListener)TestHelper.createInstance( className );
    }
    return uListener;
  }

  /**
   *  Return a list of strings for VM args of cache servers
   */
  public static String serverVmArgs( int nServers, 
                                                  String memsize, 
                                                  boolean gcOptions ) {
    String gcStr = "";
    String argStr = "-Xmx" + memsize + " -Xms" + memsize;
    if (gcOptions)
       argStr = argStr + gcStr;
    return TestConfigFcns.duplicate(argStr, nServers, true);
  }

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do, as long as the global region
 *                      size is < regionSizeThreshold.
 */
public static Long entryOperations;  

/** (int) The size of the region that will trigger the
 *        test to choose its operations from lowerThresholdOperations.
 */
public static Long lowerThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region size falls below
 *                      lowerThresold.
 */
public static Long lowerThresholdOperations;  

/** (int) The upper size of the region that will trigger the
 *        test to choose its operations from upperThresholdOperations.
 */
public static Long upperThreshold;  

/** (Vector of Strings) A list of the operations on a region entry that this 
 *                      test is allowed to do when the region exceeds
 *                      upperThresold.
 */
public static Long upperThresholdOperations;  

/** (int) The number of operations to perform in a task.
 */
public static Long numOpsPerTask;  

/** (int) Millis to sleep before register interest call */
public static Long sleepBeforeRegisterInterest;

/** (int) The number of seconds to run a test (currently used for concParRegHA)
 *        to terminate the test. We cannot use hydra's taskTimeSec parameter
 *        because of a small window of opportunity for the test to hang due
 *        to the test's "concurrent round robin" type of strategy.
 */
public static Long secondsToRun;

/** (vector of strings) The result policy of a register interest. Can be any of
 *        keys, keysValues, none.
 */
public static Long resultPolicy;

/** (boolean) For some concurrent tests, specifies whether the test registers
 *  with an initial empty region or if it register with existing entries.
 */
public static Long registerWithEmptyRegion;

/** (boolean) For some concurrent tests, true specifies whether the test uses
 *  unique keys for each thread to avoid conflicts with random ops in other
 *  threads, or false if there are no limitations on the keys a thread can use.
 *  The test can do different validation if threads are using a unique set of keys.
 */
public static Long useOwnKeys;

   static {
       setValues( HctPrms.class );
   }


}

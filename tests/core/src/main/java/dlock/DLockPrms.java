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

package dlock;

import com.gemstone.gemfire.cache.*;

import hydra.*;

/**
*
* A class used to store keys for test configuration settings.
*
*/

public class DLockPrms extends BasePrms {

    public static Long blackboardName;
    public static Long blackboardType;

    public static Long iterations;

    /**
     *  (String(s))
     *  The datatypes to be used in a test run.
     */
    public static Long datatypes;
    /**
     *  (int(s))
     *  The datasizes to be used in a test run.
     */
    public static Long datasizes;
    public static Long dataSize;

    public static Long numLocks;
    public static Long numToLock;
    public static Long getLockFirst;

    public static Long scope;
    public static Scope getScope() {
      String val = TestConfig.getInstance().getParameters().stringAt( scope );
      if ( val.equalsIgnoreCase( "distributedAck" ) ) {
        return Scope.DISTRIBUTED_ACK;
      } else if ( val.equalsIgnoreCase( "global" ) ) {
        return Scope.GLOBAL;
      } else {
        throw new HydraConfigException( "Illegal value for DLockPrms.scope: " + val );
      }
    }

    public static Long sleep;
    public static Long sleepMs;
    public static Long sleepSec;

    /** (int) The lease time to use when creating a lock. */
    public static Long leaseTime;  

    /** (int) The number of times for a thread to get a lock (to test reentrant locks) */
    public static Long numTimesToEnterLock;  

    /** (int) The lease time setting on the cache (seconds) */
    public static Long cacheLeaseTime;  

    /** (int) The lock timeout setting on the cache (seconds) */
    public static Long cacheLockTimeout;  

    /** (int) The number of times to crash the client during lockHolderWork */
    public static Long numTimesToCrash;  

    /** (int) The number of dlock elders to disrupt */
    public static Long numEldersToDisrupt;  


    /** (boolean) If true, then the test will lock an entry in the region using
     *            region.getDistributedLock(key), otherwise it will get a lock
     *            using the DistributedLockService
     */
    public static Long useEntryLock;  

    /** (boolean) If true, then the test will crash the client which is the lock Grantor
     *            if the lock Grantor does  not hold a lock.
     */
    public static Long crashGrantor;

    /** (int) The percentage of time to crash the grantor */
    public static Long crashGrantorPercent;
 
    /** (int) The percentage of grantor crashes that will be a hard kill */
    public static Long crashViaKillPercent;

    /** (double) deviation multiplier to use in FairnessTest verify */
    public static Long fairnessDeviationMultiplier;


  // @todo darrel: is this needed on the trunk?
//     /** (boolean) If true, then the test will crash the client if the client is both the lock 
//      *            Grantor and the lock holder. 
//      */
//     public static Long crashGrantorWithLock;

    static {
        setValues( DLockPrms.class );
    }
    public static void main( String args[] ) {
        dumpKeys();
    }
}

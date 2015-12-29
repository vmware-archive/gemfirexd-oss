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

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import java.util.concurrent.locks.Lock;

import hydra.*;
import hydra.blackboard.*;

import util.*;

/**
 *
 *  Test Distributed LockingService Lock Grantor 
 *
 */

public class GrantorTest {

    private static Region TheRegion;
    public final static String ROOT_REGION_NAME = BasicDLockClient.ROOT_REGION_NAME;

  /**
   *  Task: TASK to become LockGrantor 
   */
  public static void becomeGrantorTask() { 

    GrantorTest testInstance  = new GrantorTest();
    testInstance.becomeGrantorTaskWork();
  }


  private void becomeGrantorTaskWork() {

    if (CacheUtil.getCache() == null) {
      BasicDLockClient.createCacheTask();
    }

    TheRegion = CacheUtil.getCache().getRegion(ROOT_REGION_NAME);
    if (TheRegion == null) {
      throw new TestException ("becomeGrantorTask failed to get region");
    }

    // let other threads get started before changing grantor
    int sleepMs = 2000;
    try {
      if ( sleepMs > 0 ) {
        Thread.sleep( sleepMs );
      }
    } catch( InterruptedException ignore ) {}

    if (((DistributedRegion)TheRegion).getLockService().isLockGrantor()) {
      log().info("becomeLockGrantor...I am already the lock grantor");
    }
    else {
      log().info("becomeLockGrantor...I am not already the LockGrantor, request becomeLockGrantor");
      //((DistributedRegion)TheRegion).getLockService().becomeLockGrantor();
      TheRegion.becomeLockGrantor();
    }

    if (((DistributedRegion)TheRegion).getLockService().isLockGrantor()) {
      log().info("becomeLockGrantor...I am the lock grantor");
    }
    else {
      log().info("becomeLockGrantor...I am not the LockGrantor");
    }
  }


 /**
  *  Task: TASK to become LockGrantor and then crash VM
  *        Another VM should be designated LockGrantor.
  *        the LockGrantor is not holding any locks
  */
  public static void crashGrantorTask() throws ClientVmNotFoundException {

    GrantorTest testInstance  = new GrantorTest();
    testInstance.crashGrantorTaskWork();
  }

  private void crashGrantorTaskWork() throws ClientVmNotFoundException {

    int crashGrantorPercent = tab().intAt( DLockPrms.crashGrantorPercent );
    int crashViaKillPercent = tab().intAt( DLockPrms.crashViaKillPercent );

    log().info("### Determining whether to crash grantor...");
    GsRandom randGen = new GsRandom();

    if ((randGen.nextInt(1, 100)) <= crashGrantorPercent) {
      becomeGrantorTaskWork();
      // may have mix of crashes via kill VM and disconnect 
      if ((randGen.nextInt(1, 100)) <= crashViaKillPercent) {
        ClientVmMgr.stopAsync( "Crashing grantor via kill",
          ClientVmMgr.MEAN_KILL,  // kill -TERM
          ClientVmMgr.NEVER       // never allow this VM to restart
        );
      }
      else {
	log().info("Crashing grantor via disconnect");
        DistributedConnectionMgr.disconnect();
        // clean up references to cache so this VM can reconnect 
        BasicDLockClient.closeCacheTask();
      }	  
    }
  }


 /**
   *  validateTask: CLOSETASK check to see if current member
   *  is the grantor. (If more than one thread per VM does this
   *  task, the same grantor will be counted more than once.)
   */

  public static void countGrantorsTask() {

    GrantorTest testInstance  = new GrantorTest();
    testInstance.countGrantors();
  }

  private void countGrantors() {

    if (CacheUtil.getCache() == null) {
      BasicDLockClient.createCacheTask();
    }

    TheRegion = CacheUtil.getCache().getRegion(ROOT_REGION_NAME);

    if (TheRegion == null) {
      throw new TestException ("countGrantorTask failed to get region");
    }

    log().info("counting lock grantors");
    if (((DistributedRegion)TheRegion).getLockService().isLockGrantor()) {
      log().info("is Lock Grant ... incrementing count");
      DLockBlackboard bb = DLockBlackboard.getInstance();
      SharedCounters sc = bb.getSharedCounters();
      sc.increment(DLockBlackboard.NumGrantors);
    }
  }

 
  /**
   *  validateTask: ENDTASK to validate results
   */
  public static void validateTask() {

    DLockBlackboard bb = DLockBlackboard.getInstance();
    SharedCounters sc = bb.getSharedCounters();
    int numGrantors = (int)sc.read(DLockBlackboard.NumGrantors);
    // Because of grantor selection is done lazily - there may not be a current grantor
    // There should be at most 1 lock grantor.
    if (numGrantors > 1) {
      String s = "ValidateTask failed.  Expected 1 Grantor, but found: " + numGrantors;	  
      throw new DLockTestException(s);
    }
  }

  public static void forceGrantorSelectionTask() {

    GrantorTest testInstance  = new GrantorTest();
    testInstance.forceGrantorSelectionTaskWork();
  }

  private void forceGrantorSelectionTaskWork() {

    if (CacheUtil.getCache() == null) {
      BasicDLockClient.createCacheTask();
    }

    TheRegion = CacheUtil.getCache().getRegion(ROOT_REGION_NAME);

    if (TheRegion == null) {
	throw new TestException ("forceGrantorSelectionTask failed to get Regoin");
    }

    // note lockGrantor is recovered lazily when needed so we need to do
    // something like request a lock here...
    Lock lock = TheRegion.getDistributedLock("forceGrantorSelectionTaskWork");
    lock.lock();
    lock.unlock();
  }


  protected static LogWriter log() {
    return Log.getLogWriter();
  }
  protected static ConfigHashtable tab() {
    return TestConfig.tab();
  }
}

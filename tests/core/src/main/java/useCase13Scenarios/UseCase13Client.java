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

package useCase13Scenarios;

//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.cache.*;
//import distcache.*;
import distcache.gemfire.*;
import hydra.*;
import hydra.blackboard.*;
import hydra.StopSchedulingOrder;
//import java.lang.reflect.*;
import java.util.*;
//import objects.*;
//import perffmwk.*;
import util.StopStartVMs;
import util.TestHelper;
import util.TestException;

/**
 *
 *  UseCase13Client based upon cacheperf.CachePerfClient
 *
 *  Configuration:  
 *  - Primary (distNoAck, keysValues, persist)
 *  - Secondary (distNoAck, keysValues, w/CacheListener)
 *
 *  Scenarios
 *  1. What happens to the secondary when the primary dies ...
 *    a.  while primary is updating 1000's of entries/second
 *    b.  while primary is in a steady state (100's / second)
 *    c.  primary is just coming up and recovering data from secondary
 *        (since it is a keysValue mirror)
 *
 *  2. What happens to the primary when the secondary dies ...
 *    a.  while primary is updating 1000's of entries/second
 *    b.  while primary is in a steady state (100's / second)
 *    c.  primary is just coming up and recovering data from secondary
 *        (since it is a keysValue mirror)
 *
 */

public class UseCase13Client extends cacheperf.CachePerfClient {

  //----------------------------------------------------------------------------
  //  Override methods
  //----------------------------------------------------------------------------

  /**
   *  TASK to putData (and record progress in the BB)
   */
  public static void putDataTask() {
    UseCase13Client c = new UseCase13Client();
    c.initialize( PUTS );
    c.putData();
  }
  private void putData() {
    super.putDataTask();

    // update progress for killer thread
    SharedCounters sc = UseCase13BB.getBB().getSharedCounters();
    sc.setIfLarger( UseCase13BB.numOperationsCompleted, this.count );
    Log.getLogWriter().info("putData: numOperationsCompleted = " + sc.read( UseCase13BB.numOperationsCompleted ));
  }

  /**
   *  TASK to unhook a vm from the cache.
   */
  public static void closeCacheTask() {
    UseCase13Client c = new UseCase13Client();
    c.initHydraThreadLocals();
    c.closeCache();
    c.updateHydraThreadLocals();
  }
  protected void closeCache() {
    // get the superclass to handle everything else
    super.closeCache();
  }

  //----------------------------------------------------------------------------
  //  Support for killing a VM
  //----------------------------------------------------------------------------

  public static void killDuringDiskRecovery() throws ClientVmNotFoundException {
    UseCase13Client c = new UseCase13Client();
    c.initHydraThreadLocals();
    c._killDuringDiskRecovery();
    c.updateHydraThreadLocals();
  }
  private void _killDuringDiskRecovery() throws ClientVmNotFoundException {

    // wait for DiskStoreStatistics(disk).recoveriesInProgress > 0
    log().info( "killDuringDiskRecovery: waiting for DiskStoreStatistics(disk).recoveriesInProgress");
    while (util.TestHelper.getStat_getRecoveriesInProgress("disk") == 0) {
       log().fine("monitorDiskRecovery: waiting for recoveriesInProgress > 0 ..." );
       MasterController.sleepForMs(100);
    }
    log().info("diskRecovery is IN PROGRESS");

    // the test only adds the primary or secondary to the list of potential targets (depending on the test)
    List<ClientVmInfo> targetVMs = StopStartVMs.getAllVMs();
    ClientVmInfo info = targetVMs.get(0);

    ClientVmMgr.stopAsync(
      "disk recovery is in progress",
      ClientVmMgr.MEAN_KILL,  // kill -TERM
      ClientVmMgr.NEVER,      // never allow this VM to restart
      info
    );
  }

  public static void killAfterMinOps() throws ClientVmNotFoundException {
    UseCase13Client c = new UseCase13Client();
    c.initHydraThreadLocals();
    c._killAfterMinOps();
    c.updateHydraThreadLocals();
  }
  private void _killAfterMinOps() throws ClientVmNotFoundException {
    Log.getLogWriter().info("In killAfterMinOps ...");

    // we want to make sure at least half the work is done before we
    // do the kill (trim + work/2).
    int minimumNumOpsBeforeKill = cacheperf.CachePerfPrms.getMaxKeys()*3;

    try {
       TestHelper.waitForCounter(UseCase13BB.getBB(), "UseCase13BB.numOperationsCompleted",
                                 UseCase13BB.numOperationsCompleted,  minimumNumOpsBeforeKill,
                                 false, 60000, 2000);
    } catch (TestException e) {
      return;  // batched
    }

    // the test only adds the primary or secondary to the list of potential targets (depending on the test)
    List<ClientVmInfo> targetVMs = StopStartVMs.getAllVMs();
    ClientVmInfo info = targetVMs.get(0);

    ClientVmMgr.stopAsync(
      "minimum number of ops (" + minimumNumOpsBeforeKill + ") have completed", 
      ClientVmMgr.MEAN_KILL,  // kill -TERM
      ClientVmMgr.NEVER,      // never allow this VM to restart
      info
    );

    // If we get to here, we must be killed the secondary ... tell hydra that we're done
    // (don't assign this task again)
    throw new StopSchedulingOrder("Killed " + info.toString());
  }
}

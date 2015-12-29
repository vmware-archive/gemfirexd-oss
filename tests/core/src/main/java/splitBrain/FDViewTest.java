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
import hydra.blackboard.*;
import util.*;

import tx.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;

import java.util.*;

/**
 * 
 * Extension of tx/ViewTest.java to allow forcedDisconnects (vs. kill) of
 * committor at different points of the tx lifecycle.
 *
 * @see tx.ViewTest
 *
 * @author Lynn Hughes-Godfrey
 * @since 5.5
 */
public class FDViewTest extends tx.ViewTest {

  /*
   *  Initialize info for test (random number generator, isSerialExecution,
   *  pid of this VM and isScopeLocal (derived from regionAttributes).
   */
  public static synchronized void HydraTask_initialize() {
    Log.getLogWriter().info("In HydraTask_initialize");
    if (viewTest == null) {
      viewTest = new FDViewTest();
      ((FDViewTest)viewTest).initialize();
    }
    // set to true when TxListener processed the afterCommit (for distIntegrity tests)
    TxBB.getBB().getSharedMap().put(TxBB.afterCommitProcessed, new Boolean(false));
  }

  public void initialize() {
     super.initialize();

     // Create our FD region (other VMs will put entries in this region
     // in order to start suspect processing on the FDTarget VM.
     Cache myCache = CacheUtil.getCache();
     String FDRegionName = TestConfig.tab().stringAt(TxPrms.excludeRegionName);
     Log.getLogWriter().info("Creating FDRegion " + FDRegionName);
     Region rootRegion = RegionHelper.createRegion(FDRegionName, ConfigPrms.getRegionConfig());
     Log.getLogWriter().info("Created FDRegion " + FDRegionName);
  }

    /**
     *  Override tx.ViewTest.HydraTask_executeTx().
     *  Allows Shutdown and CacheClosedException if the result of forcedDisconnect
     */
    public static void HydraTask_executeTx() {
        try {
           ViewTest.HydraTask_executeTx();
        } catch (DistributedSystemDisconnectedException sde) {
           checkForForcedDisconnect(sde);
        } 
        catch (CancelException cce) {
           checkForForcedDisconnect(cce);
        } catch (Exception e) {
           Log.getLogWriter().info("executeTx threw Exception " + e);
           throw new TestException("executeTx caught Exception " + TestHelper.getStackTrace(e));
        }
    }

  /**
   *  force a disconnect in the VM if we are the transactional thread and a 
   *  commit is in Progress (see TxBB.commitInProgress).
   *
   *  @see SBUtil.beSick
   *  @see SBUtil.playDead
   */
  public static void HydraTask_forceDisconnect() {
     Log.getLogWriter().info("In HydraTask_forceDisconnect() ...");
  
     synchronized(tx.ViewTest.killSyncObject) {
       try {
         tx.ViewTest.killSyncObject.wait();
       } catch (InterruptedException e) {
         Log.getLogWriter().info("forceDisconnect interrupted, did not forceDisconnect");
       } 
  
       // Once we're notified by the commitCallback, force the committor to
       // forcefully disconnect
       SBUtil.beSick();
       SBUtil.playDead();
  
       // Force a message distribution (so forcedDisconnect can be detected)
       Set rootRegions = CacheUtil.getCache().rootRegions();
       int randInt = TestConfig.tab().getRandGen().nextInt(0, rootRegions.size() - 1);
       Object[] regionList = rootRegions.toArray();
       Region fdRegion = (Region)regionList[randInt];

       try {
          fdRegion.put(fdRegion.getName(), new Long(System.currentTimeMillis()));
       } catch (Exception e) {
         String errStr = e.toString();
         boolean isCausedByForcedDisconnect = errStr.indexOf("com.gemstone.gemfire.ForcedDisconnectException") >= 0;
         if (!isCausedByForcedDisconnect) {
            throw new TestException("Unexpected Exception " + e + " thrown on put to fdRegion " + fdRegion.getName() + " " + TestHelper.getStackTrace(e));
          }
       }
       Log.getLogWriter().info("Done calling put on " + fdRegion.getName() + " to cause a forced disconnect");
     }
  }

  /**
   *  Task for validating VMs (waiting for distribution of the TX) to 
   *  add entries to an 'excluded' region (so original test not affected).
   *  This allows the validating VMs to start suspect processing on the
   *  FDTarget (the committor).
   * 
   *  Each VM creates/updates a single entry with the clientName as the key
   *  and the currentTimeMillis as the value.
   */
  public static void HydraTask_doFDRegionOps() {
     ((FDViewTest)viewTest).doFDRegionOps();
  }

  private void doFDRegionOps() {
     String FDRegionName = TestConfig.tab().stringAt(TxPrms.excludeRegionName);
     Region aRegion = CacheUtil.getCache().getRegion(FDRegionName);
     String key = RemoteTestModule.getMyClientName();
     
     long startTime = System.currentTimeMillis();

     do {
        aRegion.put(key, new Long(System.currentTimeMillis()));
        MasterController.sleepForMs(5000);
     } while (System.currentTimeMillis() - startTime < 60000);
  }

  /** 
   *  Helper method to scan throw ShutdownExceptions and CacheClosedExceptions
   *  for "Caused by" ForceDisconnectException.
   */
  static private boolean checkForForcedDisconnect(Exception e) {
      Log.getLogWriter().info("checkForForcedDisconnect processed Exception " + e);
      String errStr = e.toString();
      boolean causedByForcedDisconnect = errStr.indexOf("com.gemstone.gemfire.ForcedDisconnectException") >= 0;
      return (causedByForcedDisconnect);
   }
}

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
package vsphere.vijava;

import hct.BridgeNotifyBB;
import hydra.ClientVmInfo;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.blackboard.SharedMap;

import java.util.Collection;

import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.ExecuteCQ61;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.RegisterInterest61;
import com.gemstone.gemfire.internal.cache.vmotion.VMotionObserverAdapter;
import com.gemstone.gemfire.internal.cache.vmotion.VMotionObserverHolder;

import cq.CQUtilBB;

/**
 * This is the base class to migrate VMs
 */

public class VMotionTestBase {

  public static final String VMOTION_TRIGGERRED = "vMotionTriggerred";

  public static void setvMotionDuringCQRegistartion() {
    ExecuteCQ61.VMOTION_DURING_CQ_REGISTRATION_FLAG = true;

    VMotionObserverHolder.setInstance(new VMotionObserverAdapter() {
      public synchronized void vMotionBeforeCQRegistration() {

        try {
          SharedMap sm = CQUtilBB.getBB().getSharedMap();
          Boolean value = (Boolean)sm.get(VMOTION_TRIGGERRED);
          if (value == null || (value != null && !value)) {
            sm.put(VMOTION_TRIGGERRED, Boolean.TRUE);
            VIJavaUtil.HydraTask_migrateVM();

          }
        }
        catch (Exception e) {
          Log.getLogWriter().info("Exception while migrating VM", e);
        }
        finally {
          ExecuteCQ61.VMOTION_DURING_CQ_REGISTRATION_FLAG = false;
        }
      }
    });
  }

  public static void setvMotionDuringRegisterInterest() {
    RegisterInterest61.VMOTION_DURING_REGISTER_INTEREST_FLAG = true;

    VMotionObserverHolder.setInstance(new VMotionObserverAdapter() {
      public synchronized void vMotionBeforeRegisterInterest() {

        try {
          SharedMap sm = BridgeNotifyBB.getBB().getSharedMap();
          Boolean value = (Boolean)sm.get(VMOTION_TRIGGERRED);
          if (value == null || (value != null && !value)) {
            sm.put(VMOTION_TRIGGERRED, Boolean.TRUE);
            VIJavaUtil.HydraTask_migrateVM();
          }
        }
        catch (Exception e) {
          Log.getLogWriter().info("Exception while migrating VM", e);
        }
        finally {
          RegisterInterest61.VMOTION_DURING_REGISTER_INTEREST_FLAG = false;
        }
      }
    });
  }

  public static synchronized void updateBBWithServerVMNames() {
    int myVMid = RemoteTestModule.getMyVmid();
    ClientVmInfo myInfo = new ClientVmInfo(new Integer(myVMid),
        RemoteTestModule.getMyClientName(), null);
     Collection<CacheClientProxy> coll = CacheClientNotifier.getInstance()
        .getClientProxies();
     if (coll.size() > 0) {
      VMotionBB.getBB().getSharedMap().put(myInfo.getClientName(), myInfo);
      Log.getLogWriter().info("server vm name is ::" + myInfo.getClientName());
    }

  }
  
  /*public static void HydraTask_vMotionHAController() {
    try {
      SharedMap sm = VMotionBB.getBB().getSharedMap();
      String value = (String)sm.get(VIJavaUtil.VMOTION_FAILOVER);
      if (value != null && !(value.equalsIgnoreCase("RESUME"))) {
        sm.put(VIJavaUtil.VMOTION_FAILOVER, "RESUME");
        Thread.sleep(30 * 1000);
        Log.getLogWriter()
            .info(
                "Successfully completed the failover task during vMotion....................");
      }
      parReg.ParRegTest.HydraTask_HAController();
    }
    catch (InterruptedException ie) {
      Log.getLogWriter().info("Exception while StopStart Server :", ie);
    }
  }*/

  public static void setFailoverFlag() {
    VIJavaUtil.STOP_START_SERVER = true;
  }

  public static void setvMotionDuringGII() {
    InitialImageOperation.VMOTION_DURING_GII = true;

    VMotionObserverHolder.setInstance(new VMotionObserverAdapter() {
      public synchronized void vMotionDuringGII(Set recipients, LocalRegion region) {

        try {
          if (region.isUsedForMetaRegion() || region.isInternalRegion()) {
            return; // false alarm
          }
          SharedMap sm = BridgeNotifyBB.getBB().getSharedMap();
          Integer num = (Integer)sm.get(VMOTION_TRIGGERRED);
          num = (num == null) ? 0 : num;

          if (num == 0) { // Could be changed to trigger vMotion more than once.
            sm.put(VMOTION_TRIGGERRED, Integer.valueOf(++num));
            InternalDistributedMember idm = (InternalDistributedMember)(recipients.iterator().hasNext() ? recipients.iterator().next() : null);
            final String vmName = idm.getHost().substring(0, 14); // TODO: (ashetkar) complete hostname doesn't work. Investigate why not?

            Thread task = new Thread(new Runnable() {
              public void run() {
                try {
                  VIJavaUtil.migrateVMToTargetHost(vmName);
                } catch (Exception e) {
                  Log.getLogWriter().info("Exception while migrating VM", e);
                }
              }
            });
            task.start();

            Thread.sleep(60 * 1000); // sleep so that gii doesn't complete much earlier than vMotion
          }
        } catch (InterruptedException ie) {
          Log.getLogWriter().info(ie);
        } finally {
          InitialImageOperation.VMOTION_DURING_GII = false;
        }
      }
    });
  }

  public static void resetvMotionFlag() {
    ExecuteCQ61.VMOTION_DURING_CQ_REGISTRATION_FLAG = false;
    RegisterInterest61.VMOTION_DURING_REGISTER_INTEREST_FLAG = false;
    InitialImageOperation.VMOTION_DURING_GII = false;
    VMotionObserverHolder.setInstance(VMotionObserverHolder.NO_OBSERVER);
  }

  public static void resetFailoverFlag() {
    VIJavaUtil.STOP_START_SERVER = false;
  }
}

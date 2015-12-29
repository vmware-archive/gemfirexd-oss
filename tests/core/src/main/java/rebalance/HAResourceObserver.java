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
package rebalance;

import hydra.*;
import hydra.blackboard.*;
import util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.*;

public class HAResourceObserver extends InternalResourceManager.ResourceObserverAdapter {

private static String event = RebalancePrms.getTargetEvent();

   public void rebalancingOrRecoveryStarted(Region aRegion) {
      Log.getLogWriter().info("HAResourceObserver.rebalanceStarted() for Region " + aRegion.getName());

      // only option here is to kill the rebalancer during rebalance
      if (event.equalsIgnoreCase("rebalancingStarted")) {
         handleRebalancingStarted(aRegion);
      }
   }

   public void rebalancingOrRecoveryFinished(Region aRegion) {
      Log.getLogWriter().info("HAResourceObserver.rebalanceFinished() for Region " + aRegion.getName());
      RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.recoveryRegionCount);
      handleRebalancingFinished(aRegion);
   }

  
   public void movingBucket(Region aRegion, int bucketId, DistributedMember source, DistributedMember dest) {
      Log.getLogWriter().info("HAResourceObserver.movingBucket() for Region " + aRegion.getName());

      // It is possible that the wait thread could have missed the
      // notify for rebalancingStarted.  Handle here just in case.
      if (event.equalsIgnoreCase("rebalancingStarted")) {
         handleRebalancingStarted(aRegion);
      }

      if (event.equalsIgnoreCase("movingBucket")) {
         String sourceHost = source.getHost();
         int sourcePid = source.getProcessId();
         String destHost = dest.getHost();
         int destPid = dest.getProcessId();

         Log.getLogWriter().info("movingBucket: " + aRegion.getName() + ":" + bucketId + " source = " + source.toString() + " target = " + dest.toString());

         ClientVmInfo sourceVm = getClientVmInfo( sourceHost, sourcePid );
         ClientVmInfo destVm = getClientVmInfo( destHost, destPid );
         ClientVmInfo targetVm = null;

         // once we've taken down a VM, we may process additional
         // events related to that VM (this is exactly what we were 
         // trying to accomplish.  Continue without further action.
         if ((sourceVm == null) || (destVm == null)) {
            return;
         }

         Log.getLogWriter().info("movingBucket: " + " sourceVm = " + sourceVm.toString() + " targetVm = " + destVm.toString());

         // don't kill rebalancer
         int rebalancerVMid = RemoteTestModule.getMyVmid();
         if (sourceVm.getVmid() != rebalancerVMid) {
            targetVm = sourceVm;
         } else {
            targetVm = destVm;
         }
         RebalanceBB.getBB().getSharedMap().put(RebalanceBB.targetVmInfo, targetVm);
         Log.getLogWriter().info("movingBucket: targetVm = " + targetVm.toString());
         // let killing thread know we're ready
         synchronized(RebalanceTest.listenerSyncObject) {
            Log.getLogWriter().info("Notifying listenerSyncObject with movingBucket for Vm = " + targetVm.toString());
            RebalanceTest.listenerSyncObject.notify();
         }
      }
   }

   public void movingPrimary(Region aRegion, int bucketId, DistributedMember source, DistributedMember dest) {
      Log.getLogWriter().info("HAResourceObserver.movingPrimary() for Region " + aRegion.getName());

      // It is possible that the wait thread could have missed the
      // notify for rebalancingStarted.  Handle here just in case.
      if (event.equalsIgnoreCase("rebalancingStarted")) {
         handleRebalancingStarted(aRegion);
      }

      // cover previous targetEvents as well (in case they were missed)
      if ((event.equalsIgnoreCase("movingPrimary")) ||
          (event.equalsIgnoreCase("movingBucket"))) {
         String sourceHost = source.getHost();
         int sourcePid = source.getProcessId();
         String destHost = dest.getHost();
         int destPid = dest.getProcessId();

         ClientVmInfo sourceVm = getClientVmInfo( sourceHost, sourcePid );
         ClientVmInfo destVm = getClientVmInfo( destHost, destPid );
         ClientVmInfo targetVm = null;

         Log.getLogWriter().info("movingPrimary: " + aRegion.getName() + ":" + bucketId + " source = " + source.toString() + " target = " + dest.toString());

         // once we've taken down a VM, we may process additional
         // events related to that VM (this is exactly what we were 
         // trying to accomplish.  Continue without further action.
         if ((sourceVm == null) || (destVm == null)) {
            return;
         }

         // don't kill rebalancer
         int rebalancerVMid = RemoteTestModule.getMyVmid();
         if (sourceVm.getVmid() != rebalancerVMid) {
            targetVm = sourceVm;
         } else {
            targetVm = destVm;
         }
         RebalanceBB.getBB().getSharedMap().put(RebalanceBB.targetVmInfo, targetVm);
         Log.getLogWriter().info("movingPrimary: targetVm = " + targetVm.toString());
         // let killing thread know we're ready
         synchronized(RebalanceTest.listenerSyncObject) {
            Log.getLogWriter().info("Notifying listenerSyncObject with movingPrimary for Vm = " + targetVm.toString());
            RebalanceTest.listenerSyncObject.notify();
         }
      }
   }

  /*
   * Called for all targetEvents (rebalancingStarted, movingBucket, movingPrimary)
   * since the initial rebalancingStarted could be missed by the wait thread.
   */
   private void handleRebalancingStarted(Region aRegion) {

      long processedStart = RebalanceBB.getBB().getSharedCounters().read(RebalanceBB.processedStart);

      // if the listenerSyncObject waiter didn't process a start yet, do it now
      if (processedStart == 0) {
         // get the rebalance clientVmInfo (which is the VM where the ResourceObserver is installed)
         int myVMid = RemoteTestModule.getMyVmid();
         ClientVmInfo targetVm = new ClientVmInfo(new Integer(myVMid), RemoteTestModule.getMyClientName(), null);
         Log.getLogWriter().info("rebalancingStarted: " + aRegion.getName() + " rebalancing VM " + targetVm.toString());
         RebalanceBB.getBB().getSharedMap().put(RebalanceBB.targetVmInfo, targetVm);

         synchronized(RebalanceTest.listenerSyncObject) {
            Log.getLogWriter().info("Notifying listenerSyncObject with rebalancingStarted for Vm = " + targetVm.toString());
            RebalanceTest.listenerSyncObject.notify();
         }
      }
   }

  /*
   * Called for all targetEvents (rebalancingStarted, movingBucket, movingPrimary).
   * It's possible that we didn't the process the event we were looking for if
   * no moves were necessary for balance (movingBucket/Primary).
   * This will allow the test to continue.  In addition, we have an ENDTASK to 
   * flag test runs which NEVER see the target event.
   */
   private void handleRebalancingFinished(Region aRegion) {

      int myVMid = RemoteTestModule.getMyVmid();
      ClientVmInfo targetVm = new ClientVmInfo(new Integer(myVMid), RemoteTestModule.getMyClientName(), null);
      Log.getLogWriter().info("rebalancingStarted: " + aRegion.getName() + " rebalancing VM " + targetVm.toString());
      RebalanceBB.getBB().getSharedMap().put(RebalanceBB.targetVmInfo, targetVm);

      synchronized(RebalanceTest.listenerSyncObject) {
         Log.getLogWriter().info("Notifying listenerSyncObject with rebalancingFinished for Vm = " + targetVm.toString());
         RebalanceTest.listenerSyncObject.notify();
      }
   }

   /*
    * For a given host and PID (retrieved from a DistributedMemberId), return
    * the corresponding ClientVmInfo.  Returns null if the vmId cannot derived
    * (meaning that the vm in question is not active).
    */
   protected static ClientVmInfo getClientVmInfo(String host, int pid) {
 
      Integer vmid = null;
      try {
         vmid = RemoteTestModule.Master.getVmid(host, pid);
      } catch (java.rmi.RemoteException e) {
         throw new TestException("Could not convert dm host and pid to vmid, caught Exception " + e);
      }
      if (vmid == null) {
        Log.getLogWriter().info("HAResourceObserver cannot get vmId for host " + host + " and PID " + pid + ", returning null");
        return null;
      }
      return new ClientVmInfo(vmid.intValue());
   }
 
   public String toString() {
      return new String("HAResourceObserver");
   }
}


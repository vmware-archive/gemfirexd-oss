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
import parReg.*;
import util.*;
import java.util.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

public class ForcedDiscUtil {

/** Wait for forced disconnect conditions
 *     1) Cache.isClosed() should be true
 *     2) DistributedSystem.isConnected() should be false
 *     3) Notifier hooks for forced disconnect before and after should complete.
 *     4) An afterRegionDestroy with a forcedDisconnect operation should occur.
 *
 *  This relies on ControllerBB.incMapCounter(ControllerBB.NumForcedDiscEvents)
 *  being called when an afterRegionDestroyed event with a forced disconnect  
 *  Operation is detected.
 *  Also, splitBrain.MemberNotifyHook must be installed in the vm receiving
 *  the forced disconnect.
 */
public static void waitForForcedDiscConditions(DistributedSystem ds,
                                               Cache theCache) {
   waitForForcedDiscConditions(ds, theCache, 1);
}

public static void waitForForcedDiscConditions(DistributedSystem ds,
                                               Cache theCache,
                                               int numRegionsInVm) {
   Log.getLogWriter().info("Waiting for a forced disconnect to occur...");
   while (true) { // wait for conditions of a forced disconnect to occur
      boolean isConnected = ds.isConnected();
      boolean cacheClosed = theCache.isClosed();
      int forcedDiscEventCount = ControllerBB.getMapCounter(ControllerBB.NumForcedDiscEventsKey);
      boolean membershipFailureComplete = ControllerBB.isMembershipFailureComplete();
      Log.getLogWriter().info("Waiting for forced disconnect conditions," +
                              "\nis connected (looking for false): " + isConnected + 
                              "\nafterRegionDestroyedEvent count (looking for " + numRegionsInVm + "): " + forcedDiscEventCount + 
                              "\nmembership failure complete (looking for true): " + membershipFailureComplete + 
                              "\ncacheClosed (looking for true): " + cacheClosed);
      if ((forcedDiscEventCount == numRegionsInVm) && !isConnected && cacheClosed && membershipFailureComplete) {
         // conditions met for a forced disconnect
         Log.getLogWriter().info("Forced disconnect conditions have been met");
         break;
      } else if (forcedDiscEventCount > numRegionsInVm) { 
         String errStr = "Error occurred in vm_" + RemoteTestModule.getMyVmid() + 
                         "; Number of forced disconnect events for this vm is " + forcedDiscEventCount +
                         ", expected " + numRegionsInVm;
         Log.getLogWriter().info(errStr);
         ControllerBB.getBB().getSharedMap().put(ControllerBB.ErrorKey, errStr);
      } else {
         MasterController.sleepForMs(1500);
      }
   }      
   Log.getLogWriter().info("Done waiting for forced disconnect conditions...");
   long startTime = ControllerBB.getMembershipFailureStartTime();  
   long endTime = ControllerBB.getMembershipFailureCompletionTime();  
   long duration = endTime - startTime;
   String aStr = "It took " + duration + " ms to complete membership failure " +
      "(time between MembershipNotifyHook.beforeMembershipFailure and " +
      "MembershipNotifyHook.afterMembershipFailure.";
   Log.getLogWriter().info(aStr);
   if (duration > 60000) { // if it takes more than 1 minute throw an exception
      throw new TestException(aStr);
   }
}

}

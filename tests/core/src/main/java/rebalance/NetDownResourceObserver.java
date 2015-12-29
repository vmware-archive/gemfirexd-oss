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
import splitBrain.SBUtil;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.*;

public class NetDownResourceObserver extends InternalResourceManager.ResourceObserverAdapter {

   public void rebalancingOrRecoveryStarted(Region aRegion) {
      Log.getLogWriter().info("RebalanceResourceObserver.rebalanceStarted() for Region " + aRegion.getName());

      Thread workThread = new Thread(new Runnable() {
        public void run() {
           SBUtil.dropConnection();
        }
      });
      workThread.start();
   }

   public void rebalancingOrRecoveryFinished(Region aRegion) {
      Log.getLogWriter().info("RebalanceResourceObserver.rebalanceFinished() for Region " + aRegion.getName());
      RebalanceBB.getBB().getSharedCounters().increment(RebalanceBB.recoveryRegionCount);
   }

   public String toString() {
      return new String("NetDownResourceObserver");
   }

  public void recoveryConflated(PartitionedRegion region) {
    // TODO rebalancing implement
    
  }
}


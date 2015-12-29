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
package recovDelay;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserver;

import hydra.*;
import util.KeyIntervals;
import util.PRObserver;
import getInitialImage.*;

/** 
 *  Observer hook to notice when a recovery has started and stopped
 *  in the current vm.
 */
public class EventPRObserver extends PRObserver {
    
public static boolean recoveryInProgress = false;

//=========================================================================
// Implementation of interface methods

/** Log that rebalancing/recovery has started.
 *  Mark in the blackboard the earliest start time for recovery.
 *  Record in the blackboard that a recovery has started.
 *  Note that for the earliest start time to be accurate, initialize() must have been called prior to 
 *  expecting rebalancing/recovery.
 *
 *  @param region The region that is undergoing rebalancing/recovery.
 */
public void rebalancingOrRecoveryStarted(Region region) {
   recoveryInProgress = true;
   super.rebalancingOrRecoveryStarted(region);
}

/** Log that rebalancing/recovery has finished.
 *  Mark in the blackboard the latest finish time for recovery.
 *  Record in the blackboard that a recovery has finished.
 *  Note that for the latest finish time to be accurate, initialize() must have been called prior to 
 *  expecting rebalancing/recovery.
 *
 *  @param region The region that finished rebalancing/recovery.
 */
public void rebalancingOrRecoveryFinished(Region region) {
   recoveryInProgress = false;
   super.rebalancingOrRecoveryFinished(region);
}

/** Log that recovery was rejected because it was already in progress.
 *  Increment a counter that this vm rejected recovery.
 *
 */
public void recoveryConflated(PartitionedRegion region) {
   super.recoveryConflated(region);
}

/** Callback for moving a bucket
 */
public void movingBucket(Region region, 
                         int bucketId,
                         DistributedMember source, 
                         DistributedMember target) {
}
  
/** Callback for moving a primary
 */
public void movingPrimary(Region region, 
                          int bucketId,
                          DistributedMember source, 
                          DistributedMember target) {
}

//=========================================================================
// Initialization methods

/** Install a hook to determine when rebalancing/recovery starts/stops
 */
public static void installObserverHook() {
   Log.getLogWriter().info("Installing EventPRObserver");
   InternalResourceManager.setResourceObserver(new EventPRObserver());
}

}

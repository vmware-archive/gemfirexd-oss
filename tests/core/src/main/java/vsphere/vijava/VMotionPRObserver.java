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

import java.util.ArrayList;

import hydra.Log;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;

public class VMotionPRObserver extends ResourceObserverAdapter {

  private static int numTimesBucketMoved = 0; // This could be moved to blackboard.
  private static int numTimesVMotionTriggerred = 0; // This could be moved to blackboard.
  private static ArrayList<Thread> threads = new ArrayList<Thread>();

  public synchronized void movingBucket(Region region, int bucketId,
      DistributedMember source, DistributedMember target) {
    ++numTimesBucketMoved;
    if (numTimesBucketMoved == 1) {
      Log.getLogWriter().info("Triggerring vMotion from movePrimary()");
      final String vmName = target.getHost().substring(0, target.getHost().indexOf("."));
      // Complete hostname doesn't work.
      Thread task = new Thread(new Runnable() {
        public void run() {
          try {
            VIJavaUtil.migrateVMToTargetHost(vmName);
            ++numTimesVMotionTriggerred;
          } catch (Exception e) {
            Log.getLogWriter().warning("Error while migrating vm during PR rebalancing", e);
          }
        }
      });
      threads.add(task);
      task.start();
    }
  }

  public synchronized void movingPrimary(Region region, int bucketId,
      DistributedMember source, DistributedMember target) {
    ++numTimesBucketMoved;
    if (numTimesBucketMoved == 1) {
      Log.getLogWriter().info("Triggerring vMotion from movingPrimary()");
      final String vmName = target.getHost().substring(0, target.getHost().indexOf("."));
      // Complete hostname doesn't work.
      Thread task = new Thread(new Runnable() {
        public void run() {
          try {
            VIJavaUtil.migrateVMToTargetHost(vmName);
            ++numTimesVMotionTriggerred;
          } catch (Exception e) {
            Log.getLogWriter().warning("Error while migrating vm during PR rebalancing", e);
          }
        }
      });
      threads.add(task);
      task.start();
    }
  }

  public synchronized void rebalancingOrRecoveryFinished(Region region) {
    waitForAllVMotionThreadsToComplete();
    Log.getLogWriter().info("Done waiting for vMotion thread to join.");
  }
      
  public static void waitForAllVMotionThreadsToComplete() {
    try {
      for (Thread thread : threads) {
        if (thread.isAlive()) {
          thread.join(200 * 1000);
        }
      }
      threads.clear();
    } catch (InterruptedException ie) {
      Log.getLogWriter().warning(
          "Error while waiting for thread to complete vMotion", ie);
    }
  }

}

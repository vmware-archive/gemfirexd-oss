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

import hydra.Log;
import hydra.blackboard.SharedMap;
import parReg.ParRegBB;
import parReg.execute.RegionOperationsFunction;

import com.gemstone.gemfire.cache.execute.FunctionContext;

public class VMotionRegionOperationsFunction extends RegionOperationsFunction {

  public static final String VMOTION_TRIGGERRED = "vMotionTriggerred";

  public void execute(FunctionContext context) {
    
    Thread vMotionTask = new Thread(new Runnable() {
      public void run() {
        try {
          Log.getLogWriter().info("before vMotion");
          vsphere.vijava.VIJavaUtil.HydraTask_migrateVM();
        }
        catch (Exception e) {
          Log.getLogWriter().info("Exception while migrating VM", e);
        }
      }
    });

    SharedMap sm = ParRegBB.getBB().getSharedMap();
    final Boolean value = (Boolean)sm.get(VMOTION_TRIGGERRED);
    if (value == null || !value) {
      sm.put(VMOTION_TRIGGERRED, Boolean.TRUE);
      vMotionTask.start();

      for (int i = 0; i < 100; i++) {
        super.execute(context);
        try {
          Thread.sleep(300);
        } catch(InterruptedException ie) {
          Log.getLogWriter().info(
              "Exception while sleeping in execute()", ie);
        }
      }
    } else {
      super.execute(context);
    }

    try {
      if (vMotionTask.isAlive()) {
        vMotionTask.join(180 * 1000);
      }
    }
    catch (InterruptedException ie) {
      Log.getLogWriter().info(
          "Exception while waiting for vMotion thread to finish", ie);
    }
  }
}

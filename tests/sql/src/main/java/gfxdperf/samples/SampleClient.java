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
package gfxdperf.samples;

import gfxdperf.PerfClient;
import hydra.Log;

/**
 * Example of using the GFXD performance framework.
 */
public class SampleClient extends PerfClient {

  public static void sampleInitTask() {
    SampleClient client = new SampleClient();
    client.initialize();
    client.sampleInit();
  }

  private void sampleInit() {
    this.terminator.startBatch();
    while (!this.terminator.batchComplete()) {
      if (this.terminator.warmupComplete()) {
        // option to do stuff here before starting the performance run
        sync(); // in this case we choose to sync up
        this.terminator.startWork();
      }
      if (this.terminator.workComplete()) {
        // option to do stuff here before exiting the task
        this.terminator.reportTrimInterval(this, "sampleinit", 0);
        updateHydraThreadLocals(); // save state for next task
        this.terminator.terminateTask();
      }
      int key = getNextKey();
      sample(key);
    }
    updateHydraThreadLocals(); // save state for next batch
    this.terminator.terminateBatch();
  }

  private void sampleInit(int key) {
    Log.getLogWriter().info("init key=" + key);
    try {
      Thread.sleep(2500); // sleep for a bit
    } catch (InterruptedException e) {
      Log.getLogWriter().severe("Interrupted!");
      return;
    }
  }

  public static void sampleTask() {
    SampleClient client = new SampleClient();
    client.initialize();
    client.sample();
  }

  private void sample() {
    this.terminator.startBatch();
    while (!this.terminator.batchComplete()) {
      if (this.terminator.warmupComplete()) {
        // option to do stuff here before starting the performance run
        sync(); // in this case we choose to sync up
        this.terminator.startWork();
      }
      if (this.terminator.workComplete()) {
        // option to do stuff here before exiting the task
        this.terminator.reportTrimInterval(this, "sample", 0);
        updateHydraThreadLocals(); // save state for next task
        this.terminator.terminateTask();
      }
      int key = getNextKey();
      sample(key);
    }
    updateHydraThreadLocals(); // save state for next batch
    this.terminator.terminateBatch();
  }

  private void sample(int key) {
    Log.getLogWriter().info("key=" + key);
    try {
      Thread.sleep(2500); // sleep for a bit
    } catch (InterruptedException e) {
      Log.getLogWriter().severe("Interrupted!");
      return;
    }
  }
}

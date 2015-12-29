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
package gfxdperf.ycsb;

import com.gemstone.gemfire.internal.NanoTimer;

import gfxdperf.PerfClient;
import gfxdperf.ycsb.core.DB;
import gfxdperf.ycsb.core.Workload;

import hydra.HydraThreadLocal;

import java.util.Random;

/**
 * YCSB client for doing performance tests.
 */
public class YCSBClient extends PerfClient {

  private static HydraThreadLocal localdb = new HydraThreadLocal();
  private static HydraThreadLocal localworkload = new HydraThreadLocal();
  private static HydraThreadLocal localworkloadstate = new HydraThreadLocal();

  protected DB db; // initialized in subclasses
  protected Workload workload;
  protected Object workloadstate;

//------------------------------------------------------------------------------
// HydraThreadLocals

  public void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.db = (DB)localdb.get();
    this.workload = (Workload)localworkload.get();
    this.workloadstate = localworkloadstate.get();
  }

  public void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    localdb.set(this.db);
    localworkload.set(this.workload);
    localworkloadstate.set(this.workloadstate);
  }

//------------------------------------------------------------------------------
// Data loading

  protected void loadData() {
    String trimIntervalName = YCSBPrms.getTrimInterval();
    this.terminator.startBatch();
    while (!this.terminator.batchComplete()) {
      if (this.terminator.warmupComplete()) {
        // option to do stuff here before starting the performance run
        sync(); // in this case we choose to sync up
        this.terminator.startWork();
      }
      if (this.terminator.workComplete()) {
        // option to do stuff here before exiting the task
        long timestamp = completeTask(trimIntervalName);
        this.terminator.reportTrimInterval(this, trimIntervalName, timestamp);
        updateHydraThreadLocals(); // save state for next task
        this.terminator.terminateTask();
      }
      this.workload.doInsert(this.db, this.workloadstate);
    }
    updateHydraThreadLocals(); // save state for next batch
    this.terminator.terminateBatch();
  }

  /**
   * Does whatever is needed to complete the task using the given trim interval
   * name. Returns the timestamp when the task has completed, 0 if no extension
   * of the trim interval is needed.
   */
  protected long completeTask(String trimIntervalName) {
    throw new UnsupportedOperationException();
  }

//------------------------------------------------------------------------------
// Workloads

  protected void doWorkload() throws InterruptedException {
    String trimIntervalName = YCSBPrms.getTrimInterval();
    double throttledOpsPerSecond = YCSBPrms.getThrottledOpsPerSecond();
    boolean throttle = throttledOpsPerSecond > 0;
    int interval = (int)(1000.0/throttledOpsPerSecond);
    Random rng = new Random(this.ttgid);

    this.terminator.startBatch();

    if (throttle) { // scatter throttled threads a bit
      Thread.sleep(rng.nextInt(interval));
    }

    while (!this.terminator.batchComplete()) {
      if (this.terminator.warmupComplete()) {
        // option to do stuff here before starting the performance run
        sync(); // in this case we choose to sync up
        if (throttle) { // re-scatter throttled threads a bit
          Thread.sleep(rng.nextInt(interval));
        }
        this.terminator.startWork();
      }
      if (this.terminator.workComplete()) {
        // option to do stuff here before exiting the task
        long timestamp = completeTask(trimIntervalName);
        this.terminator.reportTrimInterval(this, trimIntervalName, timestamp);
        updateHydraThreadLocals(); // save state for next task
        this.terminator.terminateTask();
      }

      long start = NanoTimer.getTime();
      this.workload.doTransaction(this.db, this.workloadstate);
      long elapsed = NanoTimer.getTime() - start;

      if (throttle) { // sleep the remainder of the interval
        int remaining = interval - (int)(elapsed/1000000);
        if (remaining > 0) {
          Thread.sleep(remaining);
        }
      }
    }
    updateHydraThreadLocals(); // save state for next batch
    this.terminator.terminateBatch();
  }
}

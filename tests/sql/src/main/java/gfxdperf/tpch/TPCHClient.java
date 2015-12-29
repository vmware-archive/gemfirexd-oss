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
package gfxdperf.tpch;

import gfxdperf.PerfClient;
import gfxdperf.PerfTestException;
import hydra.HydraThreadLocal;

import com.gemstone.gemfire.internal.NanoTimer;

/**
 * TPCH client for doing performance tests.
 */
public class TPCHClient extends PerfClient {

  private static HydraThreadLocal localdb = new HydraThreadLocal();
  private static HydraThreadLocal localrng = new HydraThreadLocal();
  private static HydraThreadLocal localQueryPlanFrequency = new HydraThreadLocal();
  private static HydraThreadLocal localLastQueryPlanTime = new HydraThreadLocal();

  protected DB db; // initialized in subclasses
  protected int queryPlanFrequency = 0; // no query plans
  protected long lastQueryPlanTime = 0;

//------------------------------------------------------------------------------
// HydraThreadLocals

  public void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.db = (DB)localdb.get();
    this.queryPlanFrequency = (Integer)localQueryPlanFrequency.get();
    this.lastQueryPlanTime = (Long)localLastQueryPlanTime.get();
  }

  public void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    localdb.set(this.db);
    localQueryPlanFrequency.set(this.queryPlanFrequency);
    localLastQueryPlanTime.set(this.lastQueryPlanTime);
  }
  
//------------------------------------------------------------------------------
//Initialization
//------------------------------------------------------------------------------

 protected void initialize() {
   localQueryPlanFrequency.set(queryPlanFrequency);
   localLastQueryPlanTime.set(lastQueryPlanTime);
   super.initialize();
 }

//------------------------------------------------------------------------------
// Task completion

  /**
   * Does whatever is needed to complete the task using the given trim interval
   * name. Returns the timestamp when the task has completed, 0 if no extension
   * of the trim interval is needed.
   */
  protected long completeTask(String trimIntervalName) {
    throw new UnsupportedOperationException();
  }

//------------------------------------------------------------------------------
// Validation

  protected void validateQueries() throws DBException {
    this.db.validateQueries();
  }

  protected void validateQuery() throws DBException {
    this.db.validateQuery(TPCHPrms.getQueryNumber());
  }

//------------------------------------------------------------------------------
// Workloads

  protected void doWorkload() throws DBException, InterruptedException {
    String trimIntervalName = TPCHPrms.getTrimInterval();
    double throttledOpsPerSecond = TPCHPrms.getThrottledOpsPerSecond();
    boolean throttle = throttledOpsPerSecond > 0;
    int interval = (int)(1000.0/throttledOpsPerSecond);

    this.terminator.startBatch();

    if (throttle) { // scatter throttled threads a bit
      Thread.sleep(this.rng.nextInt(interval));
    }

    while (!this.terminator.batchComplete()) {
      if (this.terminator.warmupComplete()) {
        // option to do stuff here before starting the performance run
        sync(); // in this case we choose to sync up
        if (throttle) { // re-scatter throttled threads a bit
          Thread.sleep(this.rng.nextInt(interval));
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
      // allow for randomization with oneof or range
      if ((queryPlanFrequency != 0) && timeToExecuteQueryPlan()) {
        this.db.executeQueryPlan(TPCHPrms.getQueryNumber());
      } else {
        this.db.executeQuery(TPCHPrms.getQueryNumber());
      }

      if (throttle) { // sleep the remainder of the interval
        long elapsed = NanoTimer.getTime() - start;
        int remaining = interval - (int)(elapsed/1000000);
        if (remaining > 0) {
          Thread.sleep(remaining);
        }
      }
    }
    updateHydraThreadLocals(); // save state for next batch
    this.terminator.terminateBatch();
  }

  /** Return true if the next query should log a query plan, false otherwise
   * 
   */
  private boolean timeToExecuteQueryPlan() {
    if (queryPlanFrequency == 0) { // never run query plans
      return false;
    }
    if (queryPlanFrequency == -1) { // always run query plans
      return true;
    }
    if (queryPlanFrequency > 0) {
      long currentTime = System.currentTimeMillis();
      if (currentTime - lastQueryPlanTime >= queryPlanFrequency * 1000) {
        lastQueryPlanTime = currentTime;
        return true;
      } else {
        return false;
      }
    } else {
      throw new PerfTestException("Unknown value of queryPlanFrequency: " + queryPlanFrequency);
    }
  }
}

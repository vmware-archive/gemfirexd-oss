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
package gfxdperf.terminators;

import gfxdperf.PerfClient;
import hydra.Log;

/**
 * Terminator for time-based batch termination, and iteration-based warmup and
 * task termination. The iteration count is bumped by calling {@link
 * #batchComplete};
 * @see IterationTerminatorPrms
 */
public class IterationTerminator extends AbstractTerminator {

  int batchSeconds; // number of seconds in a batch
  long warmupIterations; // number of iterations to warm up
  long workIterations; // number of iterations to do work after warmup

  long batchStartTime; // time when this batch started
  long batchEndTime; // time when this batch will complete

  long warmupStartCount = -1; // count when warmup started
  long warmupEndCount; // count when warmup will complete

  long warmupStartTime = -1; // time when warmup started
  long warmupEndTime; // time when warmup will complete

  long workStartCount; // count when work after warmup started
  long workEndCount; // count when work after warmup will complete

  long workStartTime; // time when work after warmup started
  long workEndTime; // time when work after warmup will complete

  boolean warmedUp = false; // whether warmup has completed
  boolean reportedWarmup = false; // whether warmup has been reported

  long iterations = 0; // number of times batchComplete has been invoked

  public IterationTerminator() {
    this.batchSeconds = IterationTerminatorPrms.getBatchSeconds();
    this.warmupIterations = IterationTerminatorPrms.getWarmupIterations();
    this.workIterations = IterationTerminatorPrms.getWorkIterations();
  }

  /**
   * {@inheritDoc}
   * Resets the batch start and end times.
   */
  public void startBatch() {
    this.batchStartTime = System.currentTimeMillis();
    this.batchEndTime = this.batchStartTime + this.batchSeconds * 1000;
    Log.getLogWriter().info("IterationTerminator starting batch with startTime="
       + this.batchStartTime + " endTime=" + this.batchEndTime);
  }

  /**
   * {@inheritDoc}
   * Returns true when a client has run for {@link IterationTerminatorPrms
   * #batchSeconds} seconds. This method also bumps the iteration count.
   */
  public boolean batchComplete() {
    ++this.iterations;
    long now = System.currentTimeMillis();
    if (now > this.batchEndTime) {
      long elapsed = now - this.batchStartTime;
      Log.getLogWriter().info("IterationTerminator completed batch: "
         + this.batchSeconds + " seconds in " + elapsed + " ms");
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   * Returns true when a client has run for {@link IterationTerminatorPrms
   * #warmupIterations} iterations, but only the first time it is invoked,
   * then goes back to returning false.
   */
  public boolean warmupComplete() {
    if (this.reportedWarmup) {
      return false;
    } else if (this.warmupIterations == 0) {
      // skip warmup
      Log.getLogWriter().info("IterationTerminator skipping warmup");
      this.warmedUp = true;
      this.reportedWarmup = true;
      return true;
    } else if (this.warmupStartCount == -1) {
      // first time through so initialize
      long now = System.currentTimeMillis();
      this.warmupStartTime = now;
      this.warmupStartCount = 0;
      this.warmupEndCount = this.warmupStartCount + this.warmupIterations;
      Log.getLogWriter().info("IterationTerminator starting warmup: "
         + this.warmupIterations + " iterations (start=" + this.warmupStartCount
         + " end=" + this.warmupEndCount + ") at startTime="
         + this.warmupStartTime); 
      return false;
    } else {
      if (this.iterations == this.warmupIterations) {
        long now = System.currentTimeMillis();
        long elapsed = now - this.warmupStartTime;
        Log.getLogWriter().info("IterationTerminator completed warmup: "
           + this.warmupIterations + " iterations in " + elapsed + " ms");
        this.warmedUp = true;
        this.reportedWarmup = true;
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * {@inheritDoc}
   * Starts the count on the workload.
   */
  public void startWork() {
    long now = System.currentTimeMillis();
    this.workStartTime = now;
    this.workStartCount = this.iterations;
    this.workEndCount = this.workStartCount + this.workIterations;
    Log.getLogWriter().info("IterationTerminator starting work: "
       + this.workIterations + " iterations (start=" + this.workStartCount
       + " end=" + this.workEndCount + ") at startTime=" + this.workStartTime); 
  }

  /**
   * {@inheritDoc}
   * Returns true when a client has run for {@link IterationTerminatorPrms
   * #workIterations} seconds after warmup.
   */
  public boolean workComplete() {
    if (this.warmedUp) {
      if (this.iterations == this.workEndCount) {
        long now = System.currentTimeMillis();
        long elapsed = now - this.workStartTime;
        Log.getLogWriter().info("IterationTerminator completed work: "
           + this.workIterations + " iterations in " + elapsed + " ms");
        this.workEndTime = now;
        return true;
      }
    }
    return false;
  }

  @Override
  public void reportTrimInterval(PerfClient client, String trimIntervalName,
                                 long timestamp) {
    boolean extended = false;
    if (timestamp > 0) {
      this.workEndTime = timestamp;
      extended = true;
    }
    reportTrimInterval(client, trimIntervalName,
                       this.workStartTime, this.workEndTime, extended);
  }
}

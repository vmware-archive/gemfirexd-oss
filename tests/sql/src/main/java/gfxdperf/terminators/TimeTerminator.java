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
 * Terminator for time-based batch, warmup, and task termination.
 * @see TimeTerminatorPrms
 */
public class TimeTerminator extends AbstractTerminator {

  int batchSeconds; // number of seconds in a batch
  int warmupSeconds; // number of seconds to warm up
  int workSeconds; // number of seconds to do work after warmup

  long batchStartTime; // time when this batch started
  long batchEndTime; // time when this batch will complete

  long warmupStartTime = -1; // time when warmup started
  long warmupEndTime; // time when warmup will complete

  long workStartTime; // time when work after warmup started
  long workEndTime; // time when work after warmup will complete

  boolean warmedUp = false; // whether warmup has completed
  boolean reportedWarmup = false; // whether warmup has been reported

  public TimeTerminator() {
    this.batchSeconds = TimeTerminatorPrms.getBatchSeconds();
    this.warmupSeconds = TimeTerminatorPrms.getWarmupSeconds();
    this.workSeconds = TimeTerminatorPrms.getWorkSeconds();
  }

  /**
   * {@inheritDoc}
   * Resets the batch start and end times.
   */
  public void startBatch() {
    this.batchStartTime = System.currentTimeMillis();
    this.batchEndTime = this.batchStartTime + this.batchSeconds * 1000;
    Log.getLogWriter().info("TimeTerminator starting batch with start="
       + this.batchStartTime + " end=" + this.batchEndTime);
  }

  /**
   * {@inheritDoc}
   * Returns true when a client has run for {@link TimeTerminatorPrms
   * #batchSeconds} seconds.
   */
  public boolean batchComplete() {
    long now = System.currentTimeMillis();
    if (now > this.batchEndTime) {
      long elapsed = now - this.batchStartTime;
      Log.getLogWriter().info("TimeTerminator completed batch: "
         + this.batchSeconds + " seconds in " + elapsed + " ms");
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   * Returns true when a client has run for {@link TimeTerminatorPrms
   * #warmupSeconds} seconds, but only the first time it is invoked,
   * then goes back to returning false.
   */
  public boolean warmupComplete() {
    if (this.reportedWarmup) {
      return false;
    } else if (this.warmupSeconds == 0) {
      // skip warmup
      Log.getLogWriter().info("TimeTerminator skipping warmup");
      this.warmedUp = true;
      this.reportedWarmup = true;
      return true;
    } else if (this.warmupStartTime == -1) {
      // first time through so initialize
      long now = System.currentTimeMillis();
      this.warmupStartTime = now;
      this.warmupEndTime = now + this.warmupSeconds * 1000;
      Log.getLogWriter().info("TimeTerminator starting warmup: "
         + this.warmupSeconds + " seconds (start=" + this.warmupStartTime
         + " end=" +this.warmupEndTime + ")"); 
      return false;
    } else {
      long now = System.currentTimeMillis();
      if (now > this.warmupEndTime) {
        long elapsed = now - this.warmupStartTime;
        Log.getLogWriter().info("TimeTerminator completed warmup: "
           + this.warmupSeconds + " seconds in " + elapsed + " ms");
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
   * Starts the clock on the workload.
   */
  public void startWork() {
    this.workStartTime = System.currentTimeMillis();
    this.workEndTime = this.workStartTime + this.workSeconds * 1000;
    Log.getLogWriter().info("TimeTerminator starting work: " + this.workSeconds
       + " seconds (start=" + this.workStartTime + " end=" +this.workEndTime
       + ")"); 
  }

  /**
   * {@inheritDoc}
   * Returns true when a client has run for {@link TimeTerminatorPrms
   * #workSeconds} seconds after warmup.
   */
  public boolean workComplete() {
    if (this.warmedUp) {
      long now = System.currentTimeMillis();
      if (now > this.workEndTime) {
        long elapsed = now - this.workStartTime;
        Log.getLogWriter().info("TimeTerminator completed work: "
           + this.workSeconds + " seconds in " + elapsed + " ms");
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

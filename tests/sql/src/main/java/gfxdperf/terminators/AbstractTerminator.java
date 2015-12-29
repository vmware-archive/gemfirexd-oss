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

import com.gemstone.gemfire.distributed.DistributedSystem;
import gfxdperf.PerfClient;
import hydra.StopSchedulingTaskOnClientOrder;

public abstract class AbstractTerminator {

  /**
   * Initializes the terminator for the next batch of operations.
   */
  public abstract void startBatch();

  /**
   * Returns true when a batch for this task is complete, false otherwise.
   */
  public abstract boolean batchComplete();

  /**
   * Terminates the batch by releasing thread-owned sockets.
   */
  public void terminateBatch() {
    // is this still needed? for distributed system members only?
    DistributedSystem.releaseThreadsSockets();
  }

  /**
   * Returns true when warmup for this task is complete for THE FIRST TIME ONLY,
   * false otherwise.
   */
  public abstract boolean warmupComplete();

  /**
   * Initializes the terminator for running the workload.
   */
  public abstract void startWork();

  /**
   * Returns true when the workload is complete, false otherwise.
   */
  public abstract boolean workComplete();

  /**
   * Reports the start and end times for the client using the specified trim
   * interval name. If the name is null, logs the trim interval. Extends the
   * trim interval to the given timestamp, if it is non-zero.
   */
  public abstract void reportTrimInterval(PerfClient client,
                       String trimIntervalName, long timestamp);

  /**
   * Reports the start and end times for the client using the specified trim
   * interval name. If the name is null, logs the trim interval.
   */
  protected void reportTrimInterval(PerfClient client, String trimIntervalName,
                 long startTime, long endTime, boolean extended) {
    TrimReporter.reportTrimInterval(client, trimIntervalName,
                                    startTime, endTime, extended);
  }

  /**
   * Terminates the task by releasing thread-owned sockets and throwing a
   * {hydra.StopSchedulingTaskOnClientOrder}.
   */
  public void terminateTask() {
    // is this still needed? for distributed system members only?
    DistributedSystem.releaseThreadsSockets();
    throw new StopSchedulingTaskOnClientOrder();
  }
}

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

package cacheperf.poc.useCase3_2;

import hydra.*;

/**
 *  Contains static methods suitable for use as terminators.  All terminators
 *  should be kept brief for performance runs.
 *  <p>
 *  A <i>task</i> terminator is a public static boolean method that returns true
 *  when a client thread is considered to be "done" with a task.  It is
 *  executed with a configurable frequency until it returns true, at which time
 *  the client thread issues a {@link hydra.StopSchedulingTaskOnClientOrder} to
 *  stop work on the task and prevent being scheduled with the task in future.
 *  <p>
 *  A <i>batch</i> terminator is a public static void method that is executed
 *  by a client thread at the end of each batch of operations.
 *
 *  @see cacheperf.UseCase3Prms
 */

public class Terminators {

  private static long totalTaskTimeMs =
                      TestConfig.tab().intAt(Prms.totalTaskTimeSec) * 1000L;

  //----------------------------------------------------------------------------
  //  Batch terminators
  //----------------------------------------------------------------------------

  /**
   * Returns true when a client has done the operation for {@link
   * UseCase3Prms#batchSeconds} seconds.
   */
  public static boolean terminateOnBatchSeconds(UseCase3Client c) {
    if (c.batchSeconds == -1) {
      String s = BasePrms.nameForKey(UseCase3Prms.batchSeconds) + " is not set";
      throw new HydraConfigException(s);
    }
    return System.currentTimeMillis() >= c.batchStartTime + c.batchSeconds * 1000;
  }

  //----------------------------------------------------------------------------
  //  Task terminators
  //----------------------------------------------------------------------------

  /**
   * Returns true when a client has done the TASK workload for {@link
   * hydra.Prms#totalTaskTimeSec}.
   */
  public static boolean terminateOnTotalTaskTimeSec(UseCase3Client c) {
    if (c.workloadStartTime == -1) {
      String s = "UseCase3Client.workloadStartTime is not set";
      throw new HydraConfigException(s);
    }
    return System.currentTimeMillis() >= c.workloadStartTime + totalTaskTimeMs;
  }

  /**
   *  Returns true when a client has done {@link UseCase3Prms#maxKeys} of the
   *  operation.
   */
  public static boolean terminateOnMaxKey( UseCase3Client c ) {
    if ( c.currentKey >= c.maxKeys ) {
      Log.getLogWriter().info( "Completed " + c.currentKey + " keys, surpasses max key" );
      return true;
    }
    return false;
  }

  /**
   *  Returns true when the shared counter in {@link TaskSyncBlackboard.signal} is non-zero.
   */
  public static boolean terminateOnSignal( UseCase3Client c ) {
    return TaskSyncBlackboard.getInstance().getSharedCounters().read( TaskSyncBlackboard.signal ) != 0;
  }
}

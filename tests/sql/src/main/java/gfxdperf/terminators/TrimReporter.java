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
import perffmwk.PerfStatMgr;
import perffmwk.TrimInterval;

public class TrimReporter {

  /**
   * Reports the start and end times for the client using the specified trim
   * interval name. If the name is null, logs the trim interval.
   */
  public static void reportTrimInterval(PerfClient client,
        String trimIntervalName, long startTime, long endTime,
        boolean extended) {
    if (trimIntervalName != null) {
      Log.getLogWriter().info("TrimInterval " + trimIntervalName
         + " start=" + startTime + " end=" + endTime);
      TrimInterval trimInterval = new TrimInterval(trimIntervalName);
      trimInterval.setStart(startTime);
      trimInterval.setEnd(endTime);
      if (extended) {
        Log.getLogWriter().info("TrimInterval was extended to end=" + endTime);
        PerfStatMgr.getInstance().reportExtendedTrimInterval(trimInterval);
      } else {
        PerfStatMgr.getInstance().reportTrimInterval(trimInterval);
      }
    }
  }
}

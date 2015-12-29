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

import hydra.BasePrms;
import hydra.HydraConfigException;

/**
 * Parameters for TimeTerminator.
 */
public class TimeTerminatorPrms extends BasePrms {

  static {
    setValues(TimeTerminatorPrms.class);
  }

  /**
   * (int)
   * The number of seconds in each batch for the current task.
   */
  public static Long batchSeconds;
  public static int getBatchSeconds() {
    return getSeconds(batchSeconds);
  }

  /**
   * (int)
   * The number of seconds to warm up for the current task.
   */
  public static Long warmupSeconds;
  public static int getWarmupSeconds() {
    return getSeconds(warmupSeconds);
  }

  /**
   * (int)
   * The number of seconds to do work after warmup for the current task.
   */
  public static Long workSeconds;
  public static int getWorkSeconds() {
    return getSeconds(workSeconds);
  }

  private static int getSeconds(Long key) {
    int val = tasktab().intAt(key, tab().intAt(key, -1));
    if (val == -1) {
      String s = BasePrms.nameForKey(key) + " is not set";
      throw new HydraConfigException(s);
    }
    return val;
  }
}

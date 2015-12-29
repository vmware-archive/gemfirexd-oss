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

package perffmwk;

import hydra.BasePrms;
import hydra.HydraConfigException;
import java.util.Vector;

public class HistogramStatsPrms extends BasePrms {

  static {
    setValues(HistogramStatsPrms.class);
  }

  /**
   * (boolean)
   * Whether to enable {@link HistogramStats} for tasks that use them.
   * Defaults to false.  Histogram statistics can be enabled or disabled
   * on a per-task basis using task attributes.
   */
  public static Long enable;
  public static boolean enable() {
    Long key = enable;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to log warnings when the highest latencies occur, with the
   * value of the latency.  Defaults to false.
   */
  public static Long logHighLatencyWarnings;
  public static boolean logHighLatencyWarnings() {
    Long key = logHighLatencyWarnings;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (String)
   * Name of the statistics specification file.  Defaults to null.
   */
  public static Long statisticsSpecification;

  /**
   * (List of long)
   * List of monotonically increasing long time units, in nanoseconds, used
   * to configure the bins in an instance of {@link HistogramStats}.  This
   * is a required parameter when {@link #enable} is true.  The same set of
   * bin values is used for all statistics instances.
   * <p>
   * For example,
   * <code>
   *        perffmwk.HistogramStatsPrms-enable = true;
   *        perffmwk.HistogramStatsPrms-binVals = 500000 1000000;
   * <code>
   * would produce three bins, one to count all times under 500000 ns,
   * one for times between 500000 and 1000000 ns, and one for all times
   * over 1000000 ns.
   */
  public static Long binVals;
  public static long[] getBinVals() {
    Long key = binVals;
    Vector vals = tab().vecAt(key);
    Object[] svals = vals.toArray();
    long[] lvals = new long[svals.length];
    long last = 0;
    for (int i = 0; i < lvals.length; i++) {
      try {
        lvals[i] = Long.parseLong((String)svals[i]);
        if (lvals[i] <= last) {
          String s = nameForKey(key) + " are not monotonically increasing";
          throw new HydraConfigException(s);
        }
        last = lvals[i];
      } catch (NumberFormatException e) {
        String s = "Illegal value for " + nameForKey(key) + ": " + svals[i];
        throw new HydraConfigException(s, e);
      }
    }
    return lvals;
  }
}

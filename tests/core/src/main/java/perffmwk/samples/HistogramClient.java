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

package perffmwk.samples;

import com.gemstone.gemfire.internal.NanoTimer;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import java.util.Random;
import perffmwk.HistogramStats;

/**
 * Example using histogram statistics.
 */

public class HistogramClient {

  private static HydraThreadLocal localhistogram = new HydraThreadLocal();
  private static Random rng = new Random();

  /**
   * INITTASK to open the histogram.
   */
  public static void openHistogramTask() {
    HistogramClient c = new HistogramClient();
    c.openHistogram();
  }
  private void openHistogram() {
    HistogramStats histogram = HistogramStats.getInstance("sleeps");
    localhistogram.set(histogram);
    Log.getLogWriter().info("Opened sleep histogram");
  }

  /**
   * TASK to sleep random times and sort sleep time into buckets.
   */
  public static void sleepHistogramTask() {
    HistogramClient c = new HistogramClient();
    c.sleepHistogram();
  }
  private void sleepHistogram() {
    HistogramStats histogram = (HistogramStats)localhistogram.get();
    long start = NanoTimer.getTime();
    MasterController.sleepForMs(rng.nextInt(4000));
    long elapsed = NanoTimer.getTime() - start;
    histogram.incBin(elapsed);
  }

  /**
   * CLOSETASK to close the histogram.
   */
  public static void closeHistogramTask() {
    HistogramClient c = new HistogramClient();
    c.closeHistogram();
  }
  private void closeHistogram() {
    HistogramStats histogram = (HistogramStats)localhistogram.get();
    histogram.close();
    Log.getLogWriter().info("Closed sleep histogram");
  }
}

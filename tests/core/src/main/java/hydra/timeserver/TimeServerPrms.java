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
package hydra.timeserver;

import hydra.BasePrms;
import hydra.TestConfig;

/**
 * @author dsmith
 *
 */
public class TimeServerPrms extends BasePrms {

  /**
   * (long) Number of milliseconds of clock skew in System.currentTimeMillis() a
   * test will tolerate between multiple hosts on startup. Defaults to
   * {@link #DEFAULT_CLOCK_SKEW_THRESHOLD_MS}, which tolerates any amount of
   * skew. If the threshold is exceeded, hydra will attempt to resynchronize the
   * clocks via NTP. If this fails, hydra honors
   * {@link errorOnExceededClockSkewThreshold}.
   * 
   * @since 5.0
   * @author lises
   */
  public static Long clockSkewThresholdMs;
  public static final long DEFAULT_CLOCK_SKEW_THRESHOLD_MS = 0;
  public static long clockSkewThresholdMs()  {
    return tab().longAt(clockSkewThresholdMs, DEFAULT_CLOCK_SKEW_THRESHOLD_MS);
  }
  /** This method allows the hostagent to use its own sub-configuration */
  public static long clockSkewThresholdMs(TestConfig tc)  {
    return tc.getParameters().longAt(clockSkewThresholdMs, DEFAULT_CLOCK_SKEW_THRESHOLD_MS);
  }

  /**
   * (boolean)
   * Whether a test should fail if it exceeds {@link #clockSkewThresholdMs}.
   * Defaults to false, but note that, even when true, this has no effect if
   * the default threshold is used.
   *
   * @since 5.0
   * @author lises
   */
  public static Long errorOnExceededClockSkewThreshold;
  public static final boolean DEFAULT_ERROR_ON_EXCEEDED_CLOCK_SKEW_THRESHOLD = false;
  public static boolean errorOnExceededClockSkewThreshold()  {
    return tab().booleanAt(errorOnExceededClockSkewThreshold, DEFAULT_ERROR_ON_EXCEEDED_CLOCK_SKEW_THRESHOLD);
  }
  /** This method allows the hostagent to use its own sub-configuration */
  public static boolean errorOnExceededClockSkewThreshold(TestConfig tc)  {
    return tc.getParameters().booleanAt(errorOnExceededClockSkewThreshold, DEFAULT_ERROR_ON_EXCEEDED_CLOCK_SKEW_THRESHOLD);
  }
  
  /**
   * (long) Number of milliseconds to sleep in between updates to the clock skew
   * in System.nanoTime between the client VMs and the master controller.
   * Defaults to {@link #DEFAULT_CLOCK_SKEW_UPDATE_FREQUENCY_MS}, which that
   * the client will not calculate clock skew for System.nanoTime.
   * 
   * @since 5.0
   * @author lises
   */
  public static Long clockSkewUpdateFrequencyMs;
  public static final int DEFAULT_CLOCK_SKEW_UPDATE_FREQUENCY_MS = 0;
  public static int clockSkewUpdateFrequencyMs()  {
    return tab().intAt(clockSkewUpdateFrequencyMs, DEFAULT_CLOCK_SKEW_UPDATE_FREQUENCY_MS);
  }
  
  /**
   * (long)
   * Number of milliseconds of latency allowed in a clock skew update.
   * Responses from the master controller with a higher latency will be discarded.
   * Defaults to {@link #DEFAULT_CLOCK_SKEW_MAX_LATENCY_MS}
   *
   * @since 5.0
   * @author dsmith
   */
  public static Long clockSkewMaxLatencyMs;
  public static final int DEFAULT_CLOCK_SKEW_MAX_LATENCY_MS = 50;
  public static int clockSkewMaxLatencyMs()  {
    return tab().intAt(clockSkewMaxLatencyMs, DEFAULT_CLOCK_SKEW_MAX_LATENCY_MS);
  }
  
  /**
   * (int)
   * Number of clock skew samples to average for each update to the clock skew
   * Defaults to {@link #DEFAULT_CLOCK_SKEW_SAMPLES_TO_AVERAGE}
   *
   * @since 5.0
   * @author dsmith
   */
  public static Long clockSkewSamplesToAverage;
  public static final int DEFAULT_CLOCK_SKEW_SAMPLES_TO_AVERAGE = 10;
  public static int clockSkewSamplesToAverage()  {
    return tab().intAt(clockSkewSamplesToAverage, DEFAULT_CLOCK_SKEW_SAMPLES_TO_AVERAGE);
  }
  
  static {
    setValues( TimeServerPrms.class );
  }
  
  public static void main(String args[]) {
    dumpKeys();
  }
}

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

package wan.ml;

import hydra.*;

/**
 * Class <code>MLPrms</code> provides data parameters for the ML WAN test case
 */
public class MLPrms extends BasePrms {

  public static Long quotesDataFile;
  public static Long tradesDataFile;
  
  public static Long tradesPerSecond;
  public static Long tradesToProcess;
  
  public static Long quotesPerSecond;
  public static Long quotesToProcess;
  
  public static Long burstTradesPerSecond;
  public static Long burstSleepInterval;
  public static Long burstTime;

  /**
   * Returns the data file that provides the ML quote data
   */
  public static String getQuotesDataFile() {
    return tab().pathAt(quotesDataFile);
  }

  /**
   * Returns the data file that provides the ML trade data
   */
  public static String getTradesDataFile() {
    return tab().pathAt(tradesDataFile);
  }
  
  /**
   * Returns the number of trades per second to send
   */
  public static int getTradesPerSecond() {
    return tab().intAt(tradesPerSecond, 1);
  }

  /**
   * Returns the total number of trades to process
   */
  public static int getTradesToProcess() {
    return tab().intAt(tradesToProcess, 5000);
  }

  /**
   * Returns the number of quotes per second to send
   */
  public static int getQuotesPerSecond() {
    return tab().intAt(quotesPerSecond, 1);
  }

  /**
   * Returns the total number of quotes to process
   */
  public static int getQuotesToProcess() {
    return tab().intAt(quotesToProcess, 5000);
  }

  /**
   * Returns the number of burst trades per second to send
   */
  public static int getBurstTradesPerSecond() {
    return tab().intAt(burstTradesPerSecond, 1);
  }

  /**
   * Returns the burst sleep interval (how long to sleep between bursts)
   */
  public static int getBurstSleepInterval() {
    return tab().intAt(burstSleepInterval, 300);
  }

  /**
   * Returns the burst time
   */
  public static int getBurstTime() {
    return tab().intAt(burstTime, 3);
  }

  static {
    setValues(MLPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}

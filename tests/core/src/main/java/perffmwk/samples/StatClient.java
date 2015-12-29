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

import hydra.*;
import perffmwk.*;

/**
 *
 * Examples of using the statistics framework.
 *
 */

public class StatClient {

  //////////////////////////////////////////////////////////////////////////////
  ////  TASKS                                                               ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  INITTASK to create and register statistics.
   */
  public static void openStatisticsTask() {

    long start, t;

    start = System.currentTimeMillis();
    SampleVMStatistics.getInstance();
    t = System.currentTimeMillis() - start;
    System.out.println( "HEY: time to get vm stats, in ms: " + t );

    start = System.currentTimeMillis();
    SampleThreadStatistics.getInstance();
    t = System.currentTimeMillis() - start;
    System.out.println( "HEY: time to get thread stats, in ms: " + t );
  }
  /**
   *  TASK to modify per-thread statistics.
   */
  public static void workWithStatisticsTask() {
    StatClient c = new StatClient();
    c.workWithStatistics();
  }
  /**
   *  CLOSETASK to close statistics.
   */
  public static void closeStatisticsTask() {
    SampleThreadStatistics.getInstance().close();
    SampleVMStatistics.getInstance().close();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  SUPPORT METHODS                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  protected void workWithStatistics() {

    // warm up
    doSomething( TestConfig.tab().intAt( SamplePrms.warmupIterations ) );

    // do work, noting start and end of key timing period
    PerfStatMgr.getInstance().startTrim();
    doSomething( TestConfig.tab().intAt( SamplePrms.workIterations ) );
    PerfStatMgr.getInstance().endTrim();
  }
  protected void doSomething( int iterations ) {
    SampleThreadStatistics threadStats = SampleThreadStatistics.getInstance();
    for ( int i = 0; i < iterations; i++ ) {
      threadStats.startOperation();
      doSomethingHere();
      threadStats.endOperation();
    }
    SampleVMStatistics.getInstance().incLoops();
  }
  protected void doSomethingHere() {
    MasterController.sleepForMs( 10 );
  }
}

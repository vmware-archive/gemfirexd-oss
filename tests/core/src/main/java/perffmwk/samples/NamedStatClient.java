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

public class NamedStatClient {

  //////////////////////////////////////////////////////////////////////////////
  ////  TASKS                                                               ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  INITTASK to create and register statistics.
   */
  public static void openStatisticsTask() {
    SampleVMStatistics.getInstance( "vm-warmup", "warmup" );
    SampleVMStatistics.getInstance( "vm-work", "work" );
    SampleThreadStatistics.getInstance( "thread-warmup", "warmup" );
    SampleThreadStatistics.getInstance( "thread-work", "work" );
  }
  /**
   *  TASK to modify per-thread statistics.
   */
  public static void workWithStatisticsTask() {
    NamedStatClient c = new NamedStatClient();
    c.workWithStatistics();
  }
  /**
   *  CLOSETASK to close statistics.
   */
  public static void closeStatisticsTask() {
    SampleThreadStatistics.getInstance("vm-warmup").close();
    SampleThreadStatistics.getInstance("vm-work").close();
    SampleVMStatistics.getInstance( "thread-warmup" ).close();
    SampleVMStatistics.getInstance( "thread-work" ).close();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  SUPPORT METHODS                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  protected void workWithStatistics() {

    SampleVMStatistics vmStats;
    SampleThreadStatistics threadStats;

    // warm up
    vmStats = SampleVMStatistics.getInstance( "vm-warmup" );
    threadStats = SampleThreadStatistics.getInstance( "thread-warmup" );
    PerfStatMgr.getInstance().startTrim( "warmup" );
    doSomething( TestConfig.tab().intAt( SamplePrms.warmupIterations ), vmStats, threadStats );
    PerfStatMgr.getInstance().endTrim( "warmup" );

    // do work
    vmStats = SampleVMStatistics.getInstance( "vm-work" );
    threadStats = SampleThreadStatistics.getInstance( "thread-work" );
    PerfStatMgr.getInstance().startTrim( "work" );
    doSomething( TestConfig.tab().intAt( SamplePrms.workIterations ), vmStats, threadStats );
    PerfStatMgr.getInstance().endTrim( "work" );
  }
  protected void doSomething( int iterations, SampleVMStatistics vmStats, SampleThreadStatistics threadStats ) {
    for ( int i = 0; i < iterations; i++ ) {
      threadStats.startOperation();
      doSomethingHere();
      threadStats.endOperation();
    }
    vmStats.incLoops();
  }
  protected void doSomethingHere() {
    MasterController.sleepForMs( 10 );
  }
}

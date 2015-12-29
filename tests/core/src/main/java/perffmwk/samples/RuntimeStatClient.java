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

import com.gemstone.gemfire.*;
import hydra.*;
import perffmwk.*;
import java.util.*;

/**
 *
 * Examples of using the statistics framework.
 *
 */

public class RuntimeStatClient {

  //////////////////////////////////////////////////////////////////////////////
  ////  TASKS                                                               ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  INITTASK to create and register statistics.
   */
  public static void openStatisticsTask() {
    SampleThreadStatistics.getInstance();
  }
  /**
   *  TASK to modify per-thread statistics.
   */
  public static void workWithStatisticsTask() {
    RuntimeStatClient c = new RuntimeStatClient();
    c.workWithStatistics();
  }
  protected void workWithStatistics() {
    SampleThreadStatistics threadStats = SampleThreadStatistics.getInstance();
    doSomething( TestConfig.tab().intAt( SamplePrms.workIterations ), threadStats );
  }
  protected void doSomething( int iterations, SampleThreadStatistics threadStats ) {
    for ( int i = 0; i < iterations; i++ ) {
      threadStats.startOperation();
      doSomethingHere();
      threadStats.endOperation();
    }
  }
  private void doSomethingHere() {
    MasterController.sleepForMs( 10 );
  }
  /**
   *  CLOSETASK to read statistics using a statistics instance.
   */
  public static void readStatisticsUsingStatInstanceTask() {
    RuntimeStatClient c = new RuntimeStatClient();
    c.readStatisticsUsingStatInstance();
  }
  private void readStatisticsUsingStatInstance() {

    // read from instance
    SampleThreadStatistics statInst = SampleThreadStatistics.getInstance();
    Log.getLogWriter().info( "My thread did " + statInst.readOps() + " operations" );

    // read from archives via instance
    StatisticDescriptor sd = statInst.getStatisticDescriptor( SampleThreadStatistics.OPS );
    RuntimeStatSpec statSpec = new RuntimeStatSpec( statInst, sd );
    // change default spec a bit
    statSpec.setFilter( StatSpecTokens.FILTER_NONE );
    statSpec.setCombineType( StatSpecTokens.COMBINE_ACROSS_ARCHIVES );
    statSpec.setStddev( false );
    List psvs = PerfStatMgr.getInstance().readStatistics( statSpec );
    PerfStatValue psv = (PerfStatValue) psvs.get(0);
    int max = (int) psv.getMax();
    Log.getLogWriter().info( "All threads did " + max + " operations" );
  }
  /**
   *  CLOSETASK to close statistics.
   */
  public static void closeStatisticsTask() {
    SampleThreadStatistics.getInstance().close();
  }
  /**
   *  ENDTASK to read statistics using a statistics specification string.
   */
  public static void readStatisticsUsingSpecStringTask() {
    String spec = "* " // search all archives
                + "perffmwk.samples.SampleThreadStatistics "
                + "* " // match all instances
                + SampleThreadStatistics.OPS + " "
                + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List psvs = PerfStatMgr.getInstance().readStatistics( spec );
    PerfStatValue psv = (PerfStatValue) psvs.get(0);
    int max = (int) psv.getMax();
    if ( max < 1000 ) {
      throw new SampleStatException( "Test did not do enough operations: " + max );
    } else {
      Log.getLogWriter().info( "Did " + max + " operations" );
    }
  }
  /**
   *  TASK to get the average CPU.
   */
  public static void getMeanCpuActiveTask() {
    long startTime = System.currentTimeMillis();
    MasterController.sleepForMs( 30000 );
    long endTime = System.currentTimeMillis();
    Log.getLogWriter().info( "Mean CPU Active: " + getMeanCpuActive( startTime, endTime ) );
  }
  /**
   *  Returns the average CPU used on the local host during the specified time interval.
   */
  public static double getMeanCpuActive( long startTime, long endTime ) {
    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
    //String archive = cd.getGemFireDescription().getName() + "_" + RemoteTestModule.getMyPid();
    String archive = cd.getGemFireDescription().getName() + "*";

    TrimSpec trim = new TrimSpec( "runtime_MeanCpuActive" );
    trim.start( startTime );
    trim.end( endTime );
    Log.getLogWriter().info( "HEY: trim is " + trim );

    String spec = archive + " " // search the archive for this vm only
                + "LinuxSystemStats "
                + "* " // match all instances of the stat (there is only one)
                + "cpuActive " 
                + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.RAW + " "
                + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MEAN;
    Log.getLogWriter().info( "HEY: spec is " + spec );
    List psvs = PerfStatMgr.getInstance().readStatistics( spec, trim );
    Log.getLogWriter().info( "HEY: psvs are " + psvs );
    PerfStatValue psv = (PerfStatValue) psvs.get(0);
    double mean = psv.getMean();
    Log.getLogWriter().info( "HEY: mean cpu active is " + mean + " for trim " + trim );
    return mean;
  }
}

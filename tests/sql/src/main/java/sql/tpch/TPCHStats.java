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
package sql.tpch;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;

import perffmwk.PerformanceStatistics;
import sql.tpce.TPCEStats;

public class TPCHStats extends PerformanceStatistics {
  
 

  private static final int SCOPE = THREAD_SCOPE;
  protected static final String QUERY1 = "query1";
  protected static final String QUERY1_EXECUTION_TIME = "query1ExecutionTime";
  
  protected static final String QUERY2 = "query2";
  protected static final String QUERY2_EXECUTION_TIME = "query2ExecutionTime";
  
  protected static final String QUERY3 = "query3";
  protected static final String QUERY3_EXECUTION_TIME = "query3ExecutionTime";
  
  protected static final String QUERY4 = "query4";
  protected static final String QUERY4_EXECUTION_TIME = "query4ExecutionTime";
  
  protected static final String QUERY5 = "query5";
  protected static final String QUERY5_EXECUTION_TIME = "query5ExecutionTime";
  
  
  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>QueryPerfStats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {
        factory().createIntCounter
        ( 
          QUERY1,
          "Total query1 executions",
          "operations",
          largerIsBetter
         ),
         factory().createIntCounter
         ( 
           QUERY2,
           "Total query2 executions",
           "operations",
           largerIsBetter
          ), 
          factory().createIntCounter
          ( 
            QUERY3,
            "Total query3 executions",
            "operations",
            largerIsBetter
           ),
           factory().createIntCounter
           ( 
             QUERY4,
             "Total query4 executions",
             "operations",
             largerIsBetter
            ),
            factory().createIntCounter
            ( 
              QUERY5,
              "Total query5 executions",
              "operations",
              largerIsBetter
             ),
     factory().createLongCounter
        ( 
          QUERY1_EXECUTION_TIME,
          "Total time spent executing query1.",
          "nanoseconds",
          !largerIsBetter
         ),
      factory().createLongCounter
       ( 
         QUERY2_EXECUTION_TIME,
         "Total time spent executing query2.",
         "nanoseconds",
         !largerIsBetter
        ), 
      factory().createLongCounter
       ( 
         QUERY3_EXECUTION_TIME,
         "Total time spent executing query3.",
         "nanoseconds",
         !largerIsBetter
        ),
      factory().createLongCounter
       ( 
         QUERY4_EXECUTION_TIME,
         "Total time spent executing query4.",
         "nanoseconds",
         !largerIsBetter
        ),
      factory().createLongCounter
       ( 
         QUERY5_EXECUTION_TIME,
         "Total time spent executing query5.",
         "nanoseconds",
         !largerIsBetter
       ),
    };
  }
  
  
  public static TPCHStats getInstance() {
    return (TPCHStats) getInstance( TPCHStats.class, SCOPE );
  }
  
  public static TPCHStats getInstance(String name) {
    return (TPCHStats) getInstance(TPCHStats.class, SCOPE, name);
  }
  
  public static TPCHStats getInstance( String name, String trimspecName ) {
    return (TPCHStats) getInstance(TPCHStats.class, SCOPE, name, trimspecName );
  }
  
  /////////////////// Construction / initialization ////////////////

  public TPCHStats( Class<?> cls, StatisticsType type, int scope,
      String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Accessing stats ////////////////////////

  public int getQuery(int queryNum) {
    return statistics().getInt("query" + queryNum);
  }
  public long getQueryExecutionTime(int queryNum) {
    return statistics().getLong("query" + queryNum + "ExecutionTime");
  }
  
  /////////////////// Updating stats /////////////////////////
  
  /**
   * increase the count by 1
   */
  public void incQuery(int queryNum, int amount) {
    statistics().incInt("query" + queryNum, amount);
  }
  
  /**
   * increase the time by the supplied amount
   */
  public void incQueryExecutionTime(int queryNum, long amount) {
    statistics().incLong("query" + queryNum + "ExecutionTime", amount);
  }  
  
}

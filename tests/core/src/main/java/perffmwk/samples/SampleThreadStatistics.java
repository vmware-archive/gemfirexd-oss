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
import com.gemstone.gemfire.*;
//import java.util.*;
import perffmwk.*;

/**
 *
 *  An instance of a per-thread sample statistics type.  It allows more
 *  conveniently named methods to be used by clients.
 *
 */

public class SampleThreadStatistics extends PerformanceStatistics {

  private static final int SCOPE = THREAD_SCOPE;

  protected static final String OPS = "operations";
  protected static final String OP_TIME = "operationTime";

  private NanoTimer t = new NanoTimer();

  //// STATIC METHODS

  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[]
           {
             factory().createLongCounter
             (
               OPS,
               "Number of get operations per thread",
               "operations",
	       largerIsBetter
             ),
             factory().createLongCounter
             (
               OP_TIME,
               "Time per get operation per thread",
               "nanoseconds",
	       !largerIsBetter
             )
           };
  }
  public static SampleThreadStatistics getInstance() {
    return (SampleThreadStatistics) getInstance( SampleThreadStatistics.class, SCOPE );
  }
  public static SampleThreadStatistics getInstance( String name ) {
    return (SampleThreadStatistics) getInstance( SampleThreadStatistics.class, SCOPE, name );
  }
  public static SampleThreadStatistics getInstance( String name, String trimspecName ) {
    return (SampleThreadStatistics) getInstance( SampleThreadStatistics.class, SCOPE, name, trimspecName );
  }

  //// CONSTRUCTORS

  public SampleThreadStatistics( Class cls, StatisticsType type, int scope, String instanceName, String trimspecName ) {
    super( cls, type, scope, instanceName, trimspecName );
  }

  //// INSTANCE METHODS

  public long readOps() {
    return statistics().getLong( OPS );
  }
  public void incOps( long amount ) {
    statistics().incLong( OPS, amount );
  }
  public void incOpTime( long amount ) {
    statistics().incLong( OP_TIME, amount );
  }
  public void startOperation() {
    t.reset();
  }
  public void endOperation() {
    statistics().incLong( OPS, 1 );
    statistics().incLong( OP_TIME, t.reset() );
  }
}

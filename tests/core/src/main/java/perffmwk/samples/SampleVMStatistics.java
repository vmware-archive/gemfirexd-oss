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
 *  An instance of a per-VM sample statistics type.  It allows more
 *  conveniently named methods to be used by clients.
 *
 */

public class SampleVMStatistics extends PerformanceStatistics {

  private static final int SCOPE = VM_SCOPE;

  protected static final String LOOPS = "loops";

  private NanoTimer t = new NanoTimer();

  //// STATIC METHODS

  public static StatisticDescriptor[] getStatisticDescriptors() {
    return new StatisticDescriptor[]
           {              
             factory().createLongCounter
             ( 
               LOOPS,
               "Number of loops per VM",
               "operations"
             )
           };
  }
  public static SampleVMStatistics getInstance() {
    return (SampleVMStatistics) getInstance( SampleVMStatistics.class, SCOPE );
  }
  public static SampleVMStatistics getInstance( String name ) {
    return (SampleVMStatistics) getInstance( SampleVMStatistics.class, SCOPE, name );
  }
  public static SampleVMStatistics getInstance( String name, String trimspecName ) {
    return (SampleVMStatistics) getInstance( SampleVMStatistics.class, SCOPE, name, trimspecName );
  }

  //// CONSTRUCTORS

  public SampleVMStatistics( Class cls, StatisticsType type, int scope, String instanceName, String trimspecName ) {
    super( cls, type, scope, instanceName, trimspecName );
  }

  //// INSTANCE METHODS

  public void incLoops() {
    statistics().incLong( LOOPS, 1 );
  }
  public void incLoops( long amount ) {
    statistics().incLong( LOOPS, amount );
  }
}

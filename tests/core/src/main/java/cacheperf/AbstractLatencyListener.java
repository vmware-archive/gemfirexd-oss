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

package cacheperf;

import com.gemstone.gemfire.internal.NanoTimer;
import cacheperf.CachePerfStats;
import hydra.*;
import objects.ObjectHelper;
import util.*;

/**
 * Abstract superclass for cache listeners that record message latency
 * statistics on updates.  Adjusts for clock skew and drift between hosts.
 *
 * @author lises
 * @since 5.0
 */

public abstract class AbstractLatencyListener {

  public static final long LATENCY_SPIKE_THRESHOLD = 10000000;

  /** Used to report application-defined statistics */
  CachePerfStats statistics;

  /** The host where this listener resides */
  String localhost;

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  /**
   *  Creates a latency listener.
   */
  protected AbstractLatencyListener() {
    this.statistics = CachePerfStats.getInstance();
    this.localhost = HostHelper.getLocalHost();
  }

  //----------------------------------------------------------------------------
  // Support methods
  //----------------------------------------------------------------------------

  protected void recordLatency( Object obj ) {
    long now = NanoTimer.getTime();
    long then = ObjectHelper.getTimestamp(obj);
    long latency = now - then - RemoteTestModule.getClockSkew();
    synchronized( AbstractLatencyListener.class ) {
      if (latency > LATENCY_SPIKE_THRESHOLD) {
        this.statistics.incLatencySpikes( 1 );
      }
      if (latency < 0) {
        this.statistics.incNegativeLatencies(1);
      } else {
        this.statistics.incUpdateLatency(latency);
      }
    }
  }
  protected void doNonCacheWork() { 
    if ( CpuLoadPrms.doWorkTask() ) {
      RandomValues randVals = new RandomValues();
      String str = randVals.getRandom_String();
      //String str = "123;lj4ljk23ahksdnbfa;roiuwiohjrnasgkjasdf";
      int i = str.indexOf("str2");
      int int1 = randVals.getRandom_int();
      int int2 = randVals.getRandom_int();
      int int3 = int1 * int2;
    }
  }
}


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

package cacheperf.poc.useCase3_2;

import cacheperf.poc.useCase3_2.UseCase3Prms.RegionName;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.NanoTimer;
import hydra.*;
import objects.ObjectHelper;
import util.*;

/**
 * Abstract superclass for cache listeners that record message latency
 * statistics on updates.  Adjusts for clock skew and drift between hosts.
 */
public abstract class AbstractLatencyListener {

  public static final long LATENCY_SPIKE_THRESHOLD = 10000000;

  /** Used to report application-defined statistics */
  UseCase3Stats statistics;

  /** The host where this listener resides */
  String localhost;

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  /**
   *  Creates a latency listener.
   */
  protected AbstractLatencyListener() {
    this.statistics = UseCase3Stats.getInstance();
    this.localhost = HostHelper.getLocalHost();
  }

  //----------------------------------------------------------------------------
  // Support methods
  //----------------------------------------------------------------------------

  protected void recordLatency(Region region, Object obj) {
    long now = NanoTimer.getTime();
    long then = ObjectHelper.getTimestamp(obj);
    long latency = now - then - RemoteTestModule.getClockSkew();
    RegionName regionName = RegionName.toRegionName(region.getName());
    synchronized( AbstractLatencyListener.class ) {
      if (latency > LATENCY_SPIKE_THRESHOLD) {
        this.statistics.incLatencySpikes(1, regionName);
      }
      if (latency < 0) {
        this.statistics.incNegativeLatencies(1, regionName);
      } else {
        this.statistics.incUpdateLatency(latency, regionName);
      }
    }
  }
}

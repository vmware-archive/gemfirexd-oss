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

package cacheperf.poc.useCase14.rdb;

import cacheperf.AbstractLatencyListener;
import com.gemstone.gemfire.cache.*;
import hydra.*;
import objects.TimestampedObject;

/**
 * A cache listener that records message latency statistics on updates.
 */

public class RdbListener extends AbstractLatencyListener
                         implements CacheListener {

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  /**
   *  Creates the listener.
   */
  public RdbListener() {
    super();
  }

  //----------------------------------------------------------------------------
  // CacheListener API
  //----------------------------------------------------------------------------

  public void afterCreate( final EntryEvent event ) {
  }
  public void afterUpdate( final EntryEvent event ) {
    Log.getLogWriter().info("Got update for " + event.getKey()
                           + "=" + event.getNewValue());
    TimestampedObject value = (TimestampedObject)event.getNewValue();
    recordLatency(value);
  }
  public void afterInvalidate( EntryEvent event ) {
  }
  public void afterDestroy( EntryEvent event ) {
  }
  public void afterRegionCreate( RegionEvent event ) {
  }
  public void afterRegionInvalidate( RegionEvent event ) {
  }
  public void afterRegionDestroy( RegionEvent event ) {
  }
  public void afterRegionClear( RegionEvent event ) {
  }
  public void afterRegionLive( RegionEvent event ) {
  }
  public void close() {
  }
}

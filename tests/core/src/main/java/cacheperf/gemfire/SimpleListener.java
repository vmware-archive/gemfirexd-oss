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

package cacheperf.gemfire;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import hydra.Log;

/**
 *  A simple cache listener that optionally processes and logs events.
 */

public class SimpleListener extends CacheListenerAdapter {

  private static final String AFTER_CREATE = "afterCreate";
  private static final String AFTER_UPDATE = "afterUpdate";
  private static final String AFTER_INVALIDATE = "afterInvalidate";
  private static final String AFTER_DESTROY = "afterDestroy";
  private static final String AFTER_REGION_INVALIDATE = "afterRegionInvalidate";
  private static final String AFTER_REGION_DESTROY = "afterRegionDestroy";
  private static final String CLOSE = "close";

  private boolean processEvents;
  private boolean logEvents;

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  /**
   *  Creates a simple cache listener.
   */
  public SimpleListener() {
    this.processEvents = GemFireCachePrms.processListenerEvents();
    this.logEvents = GemFireCachePrms.logListenerEvents();
  }

  //----------------------------------------------------------------------------
  // CacheListener API
  //----------------------------------------------------------------------------

  public void afterCreate( EntryEvent event ) {
    if ( this.processEvents ) processEntryEvent( event, AFTER_CREATE );
  }
  public void afterUpdate( EntryEvent event ) {
    if ( this.processEvents ) processEntryEvent( event, AFTER_UPDATE );
  }
  public void afterInvalidate( EntryEvent event ) {
    if ( this.processEvents ) processEntryEvent( event, AFTER_INVALIDATE );
  }
  public void afterDestroy( EntryEvent event ) {
    if ( this.processEvents ) processEntryEvent( event, AFTER_DESTROY );
  }
  public void afterRegionInvalidate( RegionEvent event ) {
    if ( this.processEvents ) processRegionEvent( event, AFTER_REGION_INVALIDATE );
  }
  public void afterRegionDestroy( RegionEvent event ) {
    if ( this.processEvents ) processRegionEvent( event, AFTER_REGION_DESTROY );
  }
  public void close() {
    if ( this.processEvents ) processEvent( CLOSE );
  }

  //----------------------------------------------------------------------------
  // Event processing
  //----------------------------------------------------------------------------

  /**
   *  Processes an entry event.
   */
  private void processEntryEvent( EntryEvent event, String eventType ) {
    Object name = event.getKey();
    Object value = event.getNewValue();
    if ( this.logEvents ) {
      Log.getLogWriter().info( eventType + " " + name + "=" + value );
    }
  }
  /**
   *  Processes a region event.
   */
  private void processRegionEvent( RegionEvent event, String eventType ) {
    if ( this.logEvents ) {
      Log.getLogWriter().info( eventType );
    }
  }
  /**
   *  Processes other events.
   */
  private void processEvent( String eventType ) {
    if ( this.logEvents ) {
      Log.getLogWriter().info( eventType );
    }
  }
}

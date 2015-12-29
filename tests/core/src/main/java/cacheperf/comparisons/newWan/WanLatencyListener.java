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

package cacheperf.comparisons.newWan;

import hydra.GemFirePrms;
import hydra.TestConfig;
import cacheperf.AbstractLatencyListener;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;

/**
 *  A cache listener that records message wan latency statistics on updates.
 *  @author rdiyewar
 *  @since 7.0
 */

public class WanLatencyListener extends AbstractLatencyListener
                             implements CacheListener {

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  /**
   *  Creates a latency listener.
   */
  public WanLatencyListener() {
    super();
  }

  //----------------------------------------------------------------------------
  // CacheListener API
  //----------------------------------------------------------------------------

  public void afterCreate( EntryEvent event ) {
    if (!isWanRemoteEvent(event)) {
      //simply return local events
      return;
    }
    Object value = event.getNewValue();
    recordLatency(value);
  }
  public void afterUpdate( EntryEvent event ) {
    if (!isWanRemoteEvent(event)) {
      //simply return local events
      return;
    }
    Object value = event.getNewValue();
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
  private boolean isWanRemoteEvent(EntryEvent event){
    int dsid = TestConfig.getInstance().getGemFireDescription(
        System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY))
        .getDistributedSystemId();
    
    Object callback = ((EntryEventImpl)event).getRawCallbackArgument();
    if (callback == null || !(callback instanceof GatewaySenderEventCallbackArgument)){
      //not a gateway sender event
      return false;
    }
    
    GatewaySenderEventCallbackArgument senderCallback = (GatewaySenderEventCallbackArgument)callback;
    
    if(senderCallback.getOriginatingDSId() < 0){      
      //event is originated from edge client
      return false;
    }else if(senderCallback.getOriginatingDSId() == dsid){
      //event from same ds
      return false;
    } else {
      //check if event from remote ds, but routing from peer of same ds.
      String tName = Thread.currentThread().getName();
      if(!tName.contains("ServerConnection on port") && !tName.contains("PartitionedRegion Message Processor")){
        //event is not from sender
        return false;
      }
    }

    //all criteria passed
    return true;
  }
}

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
/**
 * 
 */
package cq;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import hydra.Log;

import hct.BridgeEventListener;

/**
 * @author eshu
 *
 */
public class EntryEventListener extends hct.BridgeEventListener implements CacheListener, Declarable{

//  ==============================================================================
//   implementation CacheListener methods
  
  /**
   * log the afterCreate event and increment EntryEventBB.NUM_CREATE
   */
  public void afterCreate(EntryEvent event) {
    EntryEventBB.getBB().getSharedCounters().increment(EntryEventBB.NUM_CREATE);
    Log.getLogWriter().info("eeListener got the Create event : key is " + event.getKey());
    super.afterCreate(event);
  }

  /**
   * log the afterDestroy event and increment EntryEventBB.NUM_DESTROY
   */
  public void afterDestroy(EntryEvent event) {
    EntryEventBB.getBB().getSharedCounters().increment(EntryEventBB.NUM_DESTROY);
    Log.getLogWriter().info("eeListener got the Destroy event : key is " + event.getKey());
    super.afterDestroy(event);
  } 

  /**
   * log the afterInvalidate event and increment EntryEventBB.NUM_INVALIDATE
   */
  public void afterInvalidate(EntryEvent event) {
    EntryEventBB.getBB().getSharedCounters().increment(EntryEventBB.NUM_INVALIDATE);
    Log.getLogWriter().info("eeListener got the Invalidate event : key is " + event.getKey());
    super.afterInvalidate(event);    
  }

  /**
   * Log the afterUpdate event and increment EntryEventBB.NUM_UPDATE
   */
  public void afterUpdate(EntryEvent event) {
    EntryEventBB.getBB().getSharedCounters().increment(EntryEventBB.NUM_UPDATE);
    super.afterUpdate(event);    
  }
  
  public void close() {
    EntryEventBB.getBB().getSharedCounters().increment(EntryEventBB.NUM_CLOSE);
    super.close();
  }

}

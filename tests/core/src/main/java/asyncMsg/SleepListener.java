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

package asyncMsg;

import com.gemstone.gemfire.cache.*;
import hydra.*;
import util.BaseValueHolder;

/**
 *  A cache listener with a configurable sleep time.
 */

public class SleepListener extends util.AbstractListener implements CacheListener {

  static int sleepMs = SleepListenerPrms.getSleepMs();

  //----------------------------------------------------------------------------
  // CacheListener API
  //----------------------------------------------------------------------------

  public void afterCreate( EntryEvent event ) {
    logCall("afterCreate", event);
    String key = (String)event.getKey();
    if (key.equals(AsyncMsgTest.SLEEP_KEY)) {
      Integer val = (Integer)event.getNewValue();
      sleepMs = val.intValue();
      Log.getLogWriter().info("SleepListener.afterCreate() - new sleepMs = " + sleepMs);
    }
    Log.getLogWriter().info("Sleeping for " + sleepMs);
    MasterController.sleepForMs( sleepMs );
    Log.getLogWriter().info("done Sleeping for " + sleepMs);
  }

  public void afterUpdate( EntryEvent event ) {
    logCall("afterUpdate", event);

    // only increment for the ValueHolder's used in the Operations OpList
    if (event.getNewValue() instanceof BaseValueHolder) {
      int numUpdates = AsyncMsgTest.incNumUpdatesThisRound();
      Log.getLogWriter().info("afterUpdate: after incrementing counter, numUpdatesThisRound = " + numUpdates);
    }
    
    String key = (String)event.getKey();
    if (key.equals(AsyncMsgTest.SLEEP_KEY)) {
      Integer val = (Integer)event.getNewValue();
      sleepMs = val.intValue();
      Log.getLogWriter().info("SleepListener.afterCreate() - new sleepMs = " + sleepMs);
    }
    Log.getLogWriter().info("Sleeping for " + sleepMs);
    MasterController.sleepForMs( sleepMs );
    Log.getLogWriter().info("done Sleeping for " + sleepMs);
  }

  public void afterInvalidate( EntryEvent event ) {
    logCall("afterInvalidate", event);
    Log.getLogWriter().info("Sleeping for " + sleepMs);
    MasterController.sleepForMs( sleepMs );
    Log.getLogWriter().info("done Sleeping for " + sleepMs);
  }

  public void afterDestroy( EntryEvent event ) {
    logCall("afterDestroy", event);
    Log.getLogWriter().info("Sleeping for " + sleepMs);
    MasterController.sleepForMs( sleepMs );
    Log.getLogWriter().info("done Sleeping for " + sleepMs);
  }

  public void afterRegionInvalidate( RegionEvent event ) {
    logCall("afterRegionInvalidate", event);
    Log.getLogWriter().info("Sleeping for " + sleepMs);
    MasterController.sleepForMs( sleepMs );
    Log.getLogWriter().info("done Sleeping for " + sleepMs);
  }

  public void afterRegionClear( RegionEvent event ) {
    logCall("afterRegionClear", event);
  }

  public void afterRegionCreate( RegionEvent event ) {
    logCall("afterRegionCreate", event);
  }

  public void afterRegionLive( RegionEvent event ) {
    logCall("afterRegionLive", event);
  }
  
  public void afterRegionDestroy( RegionEvent event ) {
    logCall("afterRegionDestroy", event);
    Log.getLogWriter().info("Sleeping for " + sleepMs);
    MasterController.sleepForMs( sleepMs );
    Log.getLogWriter().info("done Sleeping for " + sleepMs);
  }

  public void close() {
    logCall("close", null);
  }
}

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

import hydra.*;
import util.*;
import java.util.Vector;
import com.gemstone.gemfire.cache.*;

/**
 *  A cache listener that will change its speed depending on hydra parameters.
 */

public class BurstListener extends util.AbstractListener implements CacheListener {

static Vector sleepTimeMillis = null;
static long beginTime = 0;
static int vecIndex = 0;
static int durationMillis = 0;
static long lastInvocationTime = 0;
static boolean burstBehaviorEnabled = true;

static {
   sleepTimeMillis = TestConfig.tab().vecAt(BurstListenerPrms.sleepTimeMillis);
   durationMillis = (TestConfig.tab().intAt(BurstListenerPrms.durationSec)) * 1000;
}

/** Sleep for a time specified by the BurstListenerPrms.sleepTimeMillis
 *  and BurstListenerPrms.durationSec. 
 *
 *  BurstListenerPrms.sleepTimeMillis is a vector of sleep times in milliseconds.
 *  Each invocation of this listener will sleep for a particular time in this vector,
 *  and will continue to do so for each invocation until BurstListenerPrms.durationSec
 *  had elapsed. Then the next sleep time in BurstListenerPrms.sleepTimeMillis is
 *  used for the next BurstListenerPrms.durationSec seconds. In other words, this
 *  marches though the values in BurstListenerPrms.sleepTimeMillis, using each one
 *  for all listener invocations for a time period specified by BurstListenerPrms.durationSec.
 *  This can similuate bursts of speed in the consumer, followed by slowing of the consumer.
 */
private void sleep() {
   if (beginTime == 0) 
      beginTime = System.currentTimeMillis();
   if (System.currentTimeMillis() - beginTime > durationMillis) { 
      // time to change to a new sleep value by increment the vecIndex
      beginTime = System.currentTimeMillis();
      vecIndex = (vecIndex + 1) % sleepTimeMillis.size();
   }
   int sleepValue = (new Integer((String)(sleepTimeMillis.get(vecIndex)))).intValue();
   try {
      Log.getLogWriter().info("In BurstListener, sleeping for " + sleepValue + " ms");
      Thread.sleep(sleepValue);
   } catch (InterruptedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Return the last time this listener was invoked.
 */
public static long getLastInvocationTime() {
   return lastInvocationTime;
}

/** Set whether to enable the burst/slow down behavior of this listener.
 *  If aBool is false, then this listener only logs its events.
 */
public static void enableBurstBehavior(boolean aBool) {
   burstBehaviorEnabled = aBool;
}

//----------------------------------------------------------------------------
// CacheListener API
//----------------------------------------------------------------------------

  public void afterCreate( EntryEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterCreate", event);
    if (burstBehaviorEnabled) {
       sleep();
    }
  }

  public void afterUpdate( EntryEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterUpdate", event);
    if (burstBehaviorEnabled) {
       sleep();
    }
  }

  public void afterInvalidate( EntryEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterInvalidate", event);
    if (burstBehaviorEnabled) {
       sleep();
    }
  }

  public void afterDestroy( EntryEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterDestroy", event);
    if (burstBehaviorEnabled) {
       sleep();
    }
  }

  public void afterRegionInvalidate( RegionEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterRegionInvalidate", event);
    if (burstBehaviorEnabled) {
       sleep();
    }
  }

  public void afterRegionClear( RegionEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterRegionClear", event);
  }

  public void afterRegionCreate( RegionEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterRegionCreate", event);
  }

  public void afterRegionLive( RegionEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterRegionLive", event);
  }
  
  public void afterRegionDestroy( RegionEvent event ) {
    lastInvocationTime = System.currentTimeMillis();
    logCall("afterRegionDestroy", event);
    if (burstBehaviorEnabled) {
       sleep();
    }
  }

  public void close() {
    lastInvocationTime = System.currentTimeMillis();
    logCall("close", null);
    if (burstBehaviorEnabled) {
       sleep();
    }
  }
}

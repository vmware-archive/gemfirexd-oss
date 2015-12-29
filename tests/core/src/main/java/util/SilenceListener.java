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
package util; 

import hydra.Log;

import java.io.Serializable;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

public class SilenceListener extends util.AbstractListener implements CacheListener, Declarable, Serializable {

public void afterCreate(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterCreate", event);
}

public void afterDestroy(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterDestroy", event);
}

public void afterInvalidate(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterInvalidate", event);
}

public void afterRegionDestroy(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterRegionDestroy", event);
}

public void afterRegionInvalidate(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterRegionInvalidate", event);
}

public void afterUpdate(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterUpdate", event);
}

public void close() {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("close", null);
}

public void afterRegionClear(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterRegionClear", event);
}

public void afterRegionCreate(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
   logCall("afterRegionCreate", event);
}

public void afterRegionLive(RegionEvent event) {
  SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
  logCall("afterRegionLive", event);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

/** Return when no events have been invoked for the given number of seconds.
 *
 *  @param sleepMS The number of milliseonds to sleep between checks for
 *         silence.
 */
public static void waitForSilence(long desiredSilenceSec, long sleepMS) {
   Log.getLogWriter().info("Waiting for a period of silence for " + desiredSilenceSec + " seconds...");
   long desiredSilenceMS = desiredSilenceSec * 1000;

   long silenceStartTime = System.currentTimeMillis();
   long currentTime = System.currentTimeMillis();
   long lastEventTime = SilenceListenerBB.getBB().getSharedCounters().read(SilenceListenerBB.lastEventTime);

   while (currentTime - silenceStartTime < desiredSilenceMS) {
      try {
         Thread.sleep(sleepMS); 
      } catch (InterruptedException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      lastEventTime = SilenceListenerBB.getBB().getSharedCounters().read(SilenceListenerBB.lastEventTime);
      if (lastEventTime > silenceStartTime) {
         // restart the wait
         silenceStartTime = lastEventTime;
      }
      currentTime = System.currentTimeMillis();
   } 
   long duration = currentTime - silenceStartTime;
   Log.getLogWriter().info("Done waiting, clients have been silent for " + duration + " ms");
}

}

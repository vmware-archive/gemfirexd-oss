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

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

/** Listener class for a silence listener that does not log anything (it's invisible). This
 *  is used to be able to wait for silence in client-server tests, but not take the performance
 *  hit of logging every event; this makes ops run faster in tests that attempt to use a lot of
 *  keys and ops
 *
 * @author lynn
 *
 */
public class InvisibleSilenceListener extends util.SilenceListener implements CacheListener, Declarable {

public void afterCreate(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterDestroy(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterInvalidate(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterRegionDestroy(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterRegionInvalidate(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterUpdate(EntryEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void close() {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterRegionClear(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterRegionCreate(RegionEvent event) {
   SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void afterRegionLive(RegionEvent event) {
  SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
}

public void init(java.util.Properties prop) {
}

}

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
package parReg.eviction;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

public class ParRegCustomIdleTOInvalListener extends util.AbstractListener
    implements CacheListener {

  public void afterCreate(EntryEvent event) {
    logCall("afterCreate", event);
    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numAfterCreateEvents_CustomIdleTOInvalidate);
    if (event.getKey().toString().startsWith("Expire_")) {
      ParRegExpirationBB.getBB().getSharedCounters().increment(
          ParRegExpirationBB.numAfterCreateEvents_CustomExpiryIdleTOInvalidate);
    }
    else {
      ParRegExpirationBB
          .getBB()
          .getSharedCounters()
          .increment(
              ParRegExpirationBB.numAfterCreateEvents_CustomNoExpiryIdleTOInvalidate);
    }
  }

  public void afterDestroy(EntryEvent event) {
    logCall("afterDestroy", event);
  }

  public void afterInvalidate(EntryEvent event) {
    logCall("afterInvalidate", event);
    ParRegExpirationBB.getBB().getSharedCounters().increment(
        ParRegExpirationBB.numAfterInvalidateEvents_CustomIdleTOInvalidate);
  }

  public void afterRegionClear(RegionEvent event) {
    logCall("afterRegionClear", event);
  }

  public void afterRegionCreate(RegionEvent event) {
    logCall("afterRegionCreate", event);
  }

  public void afterRegionDestroy(RegionEvent event) {
    logCall("afterRegionDestroy", event);
  }

  public void afterRegionInvalidate(RegionEvent event) {
    logCall("afterRegionInvalidate", event);
  }

  public void afterRegionLive(RegionEvent event) {
    logCall("afterRegionLive", event);
  }

  public void afterUpdate(EntryEvent event) {
    logCall("afterUpdate", event);
  }

  public void close() {
    logCall("close", null);
  }

}

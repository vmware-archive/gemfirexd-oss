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

package com.pivotal.gemfirexd.internal.engine.ddl;

import java.sql.SQLException;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.pivotal.gemfirexd.callbacks.EventCallback;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.internal.engine.Misc;

/**
 * An implementation of GemFire {@link CacheListener} that wraps an GemFireXD
 * listener implementing the {@link EventCallback} interface.
 * 
 * @author kneeraj
 */
public final class GfxdCacheListener implements CacheListener<Object, Object> {

  /** name of this listener */
  private final String name;

  /** the {@link EventCallback} for this listener */
  private final EventCallback ecb;

  public GfxdCacheListener(String name, EventCallback ecb) {
    this.name = name;
    this.ecb = ecb;
  }

  public void afterCreate(EntryEvent<Object, Object> event) {
    GfxdCacheWriter.callEventCallbackOnEvent((EntryEventImpl)event, this.ecb,
        Type.AFTER_INSERT);
  }

  public void afterDestroy(EntryEvent<Object, Object> event) {
    GfxdCacheWriter.callEventCallbackOnEvent((EntryEventImpl)event, this.ecb,
        Type.AFTER_DELETE);
  }

  public void afterInvalidate(EntryEvent<Object, Object> event) {
  }

  public void afterRegionClear(RegionEvent<Object, Object> event) {
  }

  public void afterRegionCreate(RegionEvent<Object, Object> event) {
  }

  public void afterRegionDestroy(RegionEvent<Object, Object> event) {
  }

  public void afterRegionInvalidate(RegionEvent<Object, Object> event) {
  }

  public void afterRegionLive(RegionEvent<Object, Object> event) {
  }

  public void afterUpdate(EntryEvent<Object, Object> event) {
    final EntryEventImpl ev = (EntryEventImpl)event;
    if (ev.hasDelta()) {
      GfxdCacheWriter.callEventCallbackOnEvent(ev, this.ecb, Type.AFTER_UPDATE);
    }
    else {
      // GFE layer may sometimes change an insert into UPDATE event with posDup
      GfxdCacheWriter.callEventCallbackOnEvent(ev, this.ecb, Type.AFTER_INSERT);
    }
  }

  public void close() {
    try {
      this.ecb.close();
    } catch (SQLException e) {
      Misc.getCacheLogWriter().warning(
          "exception encountered while calling EventCallback.close", e);
    }
  }

  public String getName() {
    return this.name;
  }

  @Override
  public String toString() {
    // return the name of the underlying implementation (#44125)
    return String.valueOf(this.ecb);
  }
}

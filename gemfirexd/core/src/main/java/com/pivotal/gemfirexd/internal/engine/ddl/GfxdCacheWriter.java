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

import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventCallback;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdCallbackArgument;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;

/**
 * 
 * @author kneeraj
 * 
 */
public class GfxdCacheWriter implements CacheWriter<Object, Object> {

  private final EventCallback ecb;

  public GfxdCacheWriter(EventCallback ecb) {
    this.ecb = ecb;
  }

  public void beforeCreate(EntryEvent<Object, Object> event)
      throws CacheWriterException {
    callEventCallbackOnEvent((EntryEventImpl)event, this.ecb,
        Type.BEFORE_INSERT);
  }

  public void beforeDestroy(EntryEvent<Object, Object> event)
      throws CacheWriterException {
    callEventCallbackOnEvent((EntryEventImpl)event, this.ecb,
        Type.BEFORE_DELETE);
  }

  public void beforeRegionClear(RegionEvent<Object, Object> event)
      throws CacheWriterException {
  }

  public void beforeRegionDestroy(RegionEvent<Object, Object> event)
      throws CacheWriterException {
  }

  public void beforeUpdate(EntryEvent<Object, Object> event)
      throws CacheWriterException {
    final EntryEventImpl ev = (EntryEventImpl)event;
    if (ev.hasDelta()) {
      callEventCallbackOnEvent(ev, this.ecb, Type.BEFORE_UPDATE);
    }
    else {
      // GFE layer may sometimes change an insert into UPDATE event with posDup
      callEventCallbackOnEvent(ev, this.ecb, Type.BEFORE_INSERT);
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

  public static void callEventCallbackOnEvent(final EntryEventImpl event,
      EventCallback ecb, Event.Type type) throws CacheWriterException {
    // Asif: I suppose the callback arg now will always be of type
    // GfxdCallbackArgument
    GfxdCallbackArgument callbackArg = (GfxdCallbackArgument)event
        .getCallbackArgument();
    if (callbackArg != null && callbackArg.isSkipListeners()) {
      return;
    }
    EventImpl cbevent = new EventImpl(event, type);
    try {
      ecb.onEvent(cbevent);
    } catch (SQLException e) {
      throw GemFireXDRuntimeException.newRuntimeException(
          "GfxdCacheWriter#callEventCallbackOnEvent: unexpected exception", e);
    }
  }

  @Override
  public String toString() {
    // return the name of the underlying implementation (#44125)
    return String.valueOf(this.ecb);
  }
}

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

package com.pivotal.gemfirexd.internal.engine.ddl.wan.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;

public final class CacheLoadedDBSynchronizerMessage extends
    AbstractDBSynchronizerMessage {

  private boolean transactional;

  private static final short IS_TRANSACTIONAL = UNRESERVED_FLAGS_START;

  public CacheLoadedDBSynchronizerMessage() {  
  }

  public CacheLoadedDBSynchronizerMessage(LocalRegion rgn, Object key,
      Object value, boolean isTransactional) {
    super(rgn);
    this.transactional = isTransactional;
    EntryEventImpl event = this.getEntryEventImpl();
    // TODO OFFHEAP: for now disallow off-heap. If it turns out value can be off-heap then we need to track the lifetime of CacheLoadedDBSynchronizerMessage.
    event.disallowOffHeapValues();
    event.setKey(key);
    event.setNewValue(value);
    event.setCallbackArgument(GemFireXDUtils.wrapCallbackArgs(null, null,
        isTransactional, false, true, true, true, false, false));
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.transactional = (flags & IS_TRANSACTIONAL) != 0;
    try {
      // Read EventID
      EntryEventImpl event = this.getEntryEventImpl();
      if (event != null) {
        event.disallowOffHeapValues();
        event.setKey(DataSerializer.readObject(in));
        event.setNewValue(DataSerializer.readObject(in));
        event.setCallbackArgument(GemFireXDUtils.wrapCallbackArgs(null, null,
            this.transactional, false, true, true,true, false, false));
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    try {
      EntryEventImpl event = this.getEntryEventImpl();
      DataSerializer.writeObject(event.getKey(), out);
      DataSerializer.writeObject(event.getNewValueAsOffHeapDeserializedOrRaw(), out);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected final short computeCompressedShort(short flags) {
    flags = super.computeCompressedShort(flags);
    if (this.transactional) flags |= IS_TRANSACTIONAL;
    return flags;
  }

  @Override
  protected void appendFields(final StringBuilder sb) {
    super.appendFields(sb);
    EntryEventImpl event = getEntryEventImpl();
    if (event != null) {
      sb.append(";key=").append(event.getKey()).append(";value=")
          .append(event.getRawNewValue()).append(";transactional=")
          .append(this.transactional);
    }
  }

  @Override
  public byte getGfxdID() {
    return CACHE_LOADED_DB_SYNCH_MSG;
  }

  @Override
  EnumListenerEvent getListenerEvent() {
    return EnumListenerEvent.AFTER_CREATE;
  }

  @Override
  Operation getOperation() {
    return Operation.NET_LOAD_CREATE;
  }
  
  @Override
  boolean skipListeners() {
    return true;
  }
}

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
package com.pivotal.gemfirexd.internal.engine.ddl.wan;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.internal.cache.InternalDeltaEvent;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.internal.engine.ddl.AbstractEventImpl;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdCallbackArgument;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer.SerializableDelta;

/**
 * 
 * @author Asif
 * 
 */
public final class WBCLEventImpl extends AbstractEventImpl {

  private final AsyncEvent<Object, Object> event;

  public WBCLEventImpl(AsyncEvent<Object, Object> ev) {
    super(getOpType(ev), false);
    this.event = ev;
  }

  private static Event.Type getOpType(AsyncEvent<Object, Object> ev) {
    Operation op = ev.getOperation();
    InternalDeltaEvent event = (InternalDeltaEvent)ev;
    if (event.isGFXDCreate(false)) {
      return Event.Type.AFTER_INSERT;
    }
    else if (event.hasDelta()) {
      return Event.Type.AFTER_UPDATE;
    }
    else if (op.isDestroy()) {
      return Event.Type.AFTER_DELETE;
    }
    else {
      throw new UnsupportedOperationException("Operation type = " + op
          + " not supported yet");
    }
  }

  @Override
  public Object getNewValue() {
    Type eventType = this.getType();
    if (eventType == Type.AFTER_DELETE) {
      return null;
    } else {
      Object val = this.event.getDeserializedValue();
      if (val instanceof SerializableDelta) {
        return ((SerializableDelta) val).getChangedRow();
      }
      return val;
    }
  }

  @Override
  public Object getOldValue() {
    Type eventType = this.getType();
    if (eventType == Event.Type.AFTER_INSERT
        || eventType == Event.Type.AFTER_UPDATE) {
      return null;
    } else {
      return event.getDeserializedValue();
    }
  }

  @Override
  protected GemFireContainer getGemFireContainer() {
    return (GemFireContainer)this.event.getRegion().getUserAttribute();
  }

  public AsyncEvent<Object, Object> getAsyncEvent() {
    return this.event;
  }

  @Override
  public SerializableDelta getSerializableDelta() {
    assert this.getType() == Type.AFTER_UPDATE;
    return (SerializableDelta)this.event.getDeserializedValue();
  }

  public boolean isPossibleDuplicate() {
    return this.event.getPossibleDuplicate();
  }

  @Override
  public boolean isTransactional() {
    final Object cbArg = this.event.getCallbackArgument();
    return cbArg != null && cbArg instanceof GfxdCallbackArgument
        && ((GfxdCallbackArgument)cbArg).isTransactional();
  }

  @Override
  public Object extractKey() {
    return this.event.getKey();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof WBCLEventImpl) {
      return (((WBCLEventImpl)o).event).equals(this.event);
    }
    else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this.event.hashCode();
  }

  @Override
  public String toString() {
    return String.valueOf(this.event);
  }

  @Override
  public boolean isLoad() {
    return this.event.getOperation().isLoad();
  }

  @Override
  public boolean isExpiration() {
    return this.event.getOperation().isExpiration();
  }

  @Override
  public boolean isEviction() {
    return this.event.getOperation().equals(Operation.EVICT_DESTROY);
  }
}

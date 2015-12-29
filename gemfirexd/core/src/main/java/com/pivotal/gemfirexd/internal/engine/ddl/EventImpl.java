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

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer.SerializableDelta;
import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;
import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_OLD_VALUE;

/**
 * 
 * @author kneeraj
 * 
 */
public final class EventImpl extends AbstractEventImpl {

  private final EntryEventImpl entryEvent;

  public EventImpl(final EntryEventImpl event, final Event.Type type) {
    super(type, event.isOriginRemote());
    this.entryEvent = event;
  }

  @Unretained(ENTRY_EVENT_NEW_VALUE)
  @Override
  public Object getNewValue() {
    return this.entryEvent.getNewValueAsOffHeapDeserializedOrRaw();
  }

  @Unretained(ENTRY_EVENT_OLD_VALUE)
  @Override
  public Object getOldValue() {
    return this.entryEvent.getOldValueAsOffHeapDeserializedOrRaw();
  }

  @Override
  protected GemFireContainer getGemFireContainer() {
    return (GemFireContainer)this.entryEvent.getRegion().getUserAttribute();
  }

  @Override
  public SerializableDelta getSerializableDelta() {
    assert this.entryEvent.hasDelta();
    return (SerializableDelta)this.entryEvent.getDeltaNewValue();
  }

  public boolean isPossibleDuplicate() {
    return this.entryEvent.isPossibleDuplicate();
  }

  @Override
  public boolean isTransactional() {
    return this.entryEvent.getTXState() != null;
  }

  @Override
  public Object extractKey() {
    return this.entryEvent.getKey();
  }

  @Override
  public boolean isLoad() {
    return this.entryEvent.getOperation().isLoad();
  }

  @Override
  public boolean isExpiration() {
    return this.entryEvent.getOperation().isExpiration();
  }

  @Override
  public boolean isEviction() {
    return this.entryEvent.getOperation().equals(Operation.EVICT_DESTROY);
  }

  @Override
  public int hashCode() {
    return this.entryEvent.getEventId().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other instanceof EventImpl) {
      EventImpl otherEvent = (EventImpl)other;
      return this.entryEvent.getEventId().equals(
          otherEvent.entryEvent.getEventId());
    }
    else {
      return false;
    }
  }
}

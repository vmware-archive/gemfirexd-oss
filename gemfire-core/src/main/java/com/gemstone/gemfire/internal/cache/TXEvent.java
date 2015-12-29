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

package com.gemstone.gemfire.internal.cache;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.offheap.Releasable;

import java.util.*;

/** <p>The internal implementation of the {@link TransactionEvent} interface
 * 
 * @author Darrel Schneider
 *
 * @since 4.0
 * 
 */
public class TXEvent implements TransactionEvent, Releasable {
  private final TXState localTxState;
  private final TXId txId;
  private List events;
  private List createEvents = null;
  private List putEvents = null;
  private List invalidateEvents = null;
  private List destroyEvents = null;
  final private Cache cache;

  TXEvent(TXState localTxState, TXId txId, Cache cache) {
    this.localTxState = localTxState;
    this.txId = txId;
    this.cache = cache;
    this.events = null;
  }

  public TXId getTransactionId() {
    return this.txId;
  }

  public synchronized List getCreateEvents() {
    if (this.createEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isCreate()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.createEvents = Collections.EMPTY_LIST;
      } else {
        this.createEvents = Collections.unmodifiableList(result);
      }
    }
    return this.createEvents;
  }

  public synchronized List getPutEvents() {
    if (this.putEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isUpdate()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.putEvents = Collections.EMPTY_LIST;
      } else {
        this.putEvents = Collections.unmodifiableList(result);
      }
    }
    return this.putEvents;
  }

  public synchronized List getInvalidateEvents() {
    if (this.invalidateEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isInvalidate()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.invalidateEvents = Collections.EMPTY_LIST;
      } else {
        this.invalidateEvents = Collections.unmodifiableList(result);
      }
    }
    return this.invalidateEvents;
  }

  public synchronized List getDestroyEvents() {
    if (this.destroyEvents == null) {
      ArrayList result = new ArrayList();
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        CacheEvent ce = (CacheEvent)it.next();
        if (ce.getOperation().isDestroy()) {
          result.add(ce);
        }
      }
      if (result.isEmpty()) {
        this.destroyEvents = Collections.EMPTY_LIST;
      } else {
        this.destroyEvents = Collections.unmodifiableList(result);
      }
    }
    return this.destroyEvents;
  }

  public synchronized List getEvents() {
    if (this.events == null) {
      if (this.localTxState != null) {
        this.events = this.localTxState.getEvents();
        this.localTxState.cleanupEvents();
      }
      else {
        this.events = Collections.EMPTY_LIST;
      }
    }
    return this.events;
  }

  public final Cache getCache() {
    return this.cache;
  }
  
  @Override
  public synchronized void release() {
    if (this.events != null) {
      Iterator it = getEvents().iterator();
      while (it.hasNext()) {
        Object o = it.next();
        if (o instanceof EntryEventImpl) {
          ((EntryEventImpl) o).release();
        }
      }
    }
  }
}

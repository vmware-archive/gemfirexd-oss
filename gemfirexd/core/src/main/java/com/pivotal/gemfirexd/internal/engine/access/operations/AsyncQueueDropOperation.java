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

package com.pivotal.gemfirexd.internal.engine.access.operations;

import java.io.IOException;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * Encapsulates drop operation for async event queues. In conjunction with
 * {@link AsyncQueueCreateOperation} this helps implement atomicity for
 * AsyncQueue CREATE/DROP having proper undo in case CREATE fails on one or more
 * nodes.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class AsyncQueueDropOperation extends MemOperation {

  private final String id;

  public AsyncQueueDropOperation(String id) {
    super(null);
    this.id = id;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final GemFireCacheImpl cache = Misc.getGemFireCache();
    AsyncEventQueue asyncQueue = cache.getAsyncEventQueue(id);
    if (asyncQueue != null) {
      try {
        cache.removeAsyncEventQueue(asyncQueue);
        asyncQueue.destroy();
      } catch (Exception ex) {
        throw StandardException.newException(
            SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
            ex.toString());
      }
    }
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    throw new UnsupportedOperationException("AsyncQueueDropOperation: undo "
        + "unimplemented; require GFE queue rename support");
  }
}

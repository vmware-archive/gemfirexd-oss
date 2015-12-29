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

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * 
 * @author yjing
 * @author swale
 */
public final class GlobalHashIndexDeleteOperation extends MemIndexOperation {

  private boolean deleteSuccess;

  public GlobalHashIndexDeleteOperation(GemFireContainer container, Object key) {
    super(container, key, null);
  }

  @Override
  public void doMe(Transaction tran, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    this.deleteSuccess = doMe(null, null, this.memcontainer, this.key, false);
  }

  public static boolean doMe(GemFireTransaction tran,
      final TXStateInterface tx, GemFireContainer container, Object key, boolean doEvict)
      throws StandardException {
    // Neeraj: Switiching of the txstate from local TXSTate to proxy was
    // done to fix bug #42606. The issue was that unique constraint
    // violations were not been detected. This is because the actual put/insert
    // happens on regions to which the current TXstate set is fine but when
    // these inserts or/and delete cause global index regions to be updated for
    // which the corresponding TXRegionState might not be in the TXState.
    // Same comment is in GlobalHashIndexInsertOperation.
    TXManagerImpl txMgr = null;
    TXStateInterface proxy = null;
    try {
      if (tx != null) {
        proxy = tx.getProxy();
        if (proxy != tx) {
          if (GemFireXDUtils.TraceTran) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_TRAN,
                "GlobalHashIndexDeleteOperation#doMe: "
                    + " setting proxy transaction: " + proxy
                    + " in current thread replacing " + tx);
          }
          txMgr = tran.getTransactionManager();
          txMgr.masqueradeAs(proxy);
        }
      }
      if (tran != null && tran.needLogging()) {
        GlobalHashIndexDeleteOperation op = new GlobalHashIndexDeleteOperation(
            container, key);
        tran.logAndDo(op);
        return op.deleteSuccess;
      }
      if (GemFireXDUtils.TraceIndex) {
        GfxdIndexManager.traceIndex("GlobalHashIndexDeleteOp: deleting key=%s "
            + "from %s", key, container);
      }
      boolean success;
      try {
        container.delete(key, null, false, tran, proxy,
            GemFireTransaction.getLanguageConnectionContext(tran), doEvict);
        success = true;
      } catch (EntryNotFoundException ex) {
        // failed to delete
        success = false;
      }
      if (GemFireXDUtils.TraceIndex) {
        GfxdIndexManager.traceIndex("GlobalHashIndexDeleteOp: %s deleted "
            + "key=%s from %s", (success ? "successfully" : "unsuccessfully"),
            key, container);
      }
      return success;
    } finally {
      if (txMgr != null) {
        if (GemFireXDUtils.TraceTran) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_TRAN,
              "GlobalHashIndexDeleteOperation#doMe: " + " switching back to: "
                  + tx + " in current thread instead of: " + proxy);
        }
        txMgr.masqueradeAs(tx);
      }
    }
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    throw new UnsupportedOperationException("cannot undo global index "
        + "delete since PR#destroy does not provide old value");
  }

  @Override
  public final boolean shouldBeConflated() {
    return true;
  }
}

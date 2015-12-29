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

package com.pivotal.gemfirexd.internal.engine.sql.catalog;

import java.io.IOException;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.locks.LockMode;
import com.gemstone.gemfire.internal.cache.locks.ReentrantReadWriteWriteShareLock;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager;
import com.pivotal.gemfirexd.internal.engine.access.operations.MemOperation;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * @author kneeraj
 * 
 */
public class IRFLockAcquireOp extends MemOperation {

  private final ReentrantReadWriteWriteShareLock lock;

  private final Object owner;

  private LogWriter logger;

  public IRFLockAcquireOp(ReentrantReadWriteWriteShareLock irf2Lock,
      Object lockOwner) {
    super(null);
    this.lock = irf2Lock;
    this.owner = lockOwner;
    this.logger = Misc.getCacheLogWriter();
  }

  public void takeLock() {
    if (this.logger.fineEnabled() || GemFireXDUtils.TracePersistIndex) {
      GfxdIndexManager.traceIndex(
          "IRFLockAcquireOp::takeLock calling acquiring lock on=%s ", this.lock);
    }
    this.lock.attemptLock(LockMode.EX, -1, this.owner);
    if (this.logger.fineEnabled() || GemFireXDUtils.TracePersistIndex) {
      GfxdIndexManager.traceIndex(
          "IRFLockAcquireOp::takeLock acquired lock on=%s ", this.lock);
    }
  }

  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    if (this.logger.fineEnabled() || GemFireXDUtils.TracePersistIndex) {
      GfxdIndexManager.traceIndex(
          "IRFLockAcquireOp::doMe called releasing lock on=%s ", this.lock);
    }
    this.lock.releaseLock(LockMode.EX, false, this.owner);
    if (this.logger.fineEnabled() || GemFireXDUtils.TracePersistIndex) {
      GfxdIndexManager.traceIndex(
          "IRFLockAcquireOp::doMe called acquired lock on=%s ", this.lock);
    }
  }

  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    return null;
  }

  public boolean doAtCommitOrAbort() {
    return true;
  }
}

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
package com.pivotal.gemfirexd.internal.engine.store;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Page;
import com.pivotal.gemfirexd.internal.iapi.store.raw.RecordHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;

/**
 * Any object that implements this interface can be used as a locking policy for
 * rows of a {@link GemFireContainer}. This is used instead of
 * {@link LockingPolicy} primarily to avoid creating a wrapper
 * {@link RecordHandle} object and instead work on {@link ExecRow} directly.
 * 
 * @see LockingPolicy
 * @author Sumedh Wale
 * @since 6.0
 */
public interface GfxdRowLockingPolicy {

  /**
   * Called before a record is fetched. This will return false if no lock was
   * obtained which can happen if no lock is required e.g. since this is a
   * remote node, or the rows are for SYS entries not requiring any locks etc.
   * In case of timeout this will throw an appropriate exception, and in this it
   * differs from the contract of {@link LockingPolicy}.
   * 
   * @param t
   *          Transaction to associate lock with.
   * @param container
   *          Open Container used to get record. Will be used to row locks by
   *          the container they belong to.
   * @param record
   *          Record to lock.
   * @param waitForLock
   *          Should lock request wait until granted?
   * 
   * @exception StandardException
   *              Standard Derby error policy
   */
  public boolean lockRecordForRead(Transaction t, ContainerHandle container,
      ExecRow record, boolean waitForLock) throws StandardException;

  /**
   * Called before a record is inserted, updated or deleted. This will return
   * false if no lock was obtained which can happen if no lock is required e.g.
   * since this is a remote node, or the rows are for SYS entries not requiring
   * any locks etc. In case of timeout this will throw an appropriate exception,
   * and in this it differs from the contract of {@link LockingPolicy}.
   * 
   * @param t
   *          Transaction to associate lock with.
   * @param record
   *          Record to lock.
   * @param lockForInsert
   *          Lock is for an insert.
   * @param waitForLock
   *          Should lock request wait until granted?
   * 
   * @return true if the lock was obtained, false if it wasn't. False should
   *         only be returned if the waitForLock argument was set to "false,"
   *         and the lock was unavailable.
   * @exception StandardException
   *              Standard Derby error policy
   */
  public boolean lockRecordForWrite(Transaction t, ExecRow record,
      boolean lockForInsert, boolean waitForLock) throws StandardException;

  /**
   * Called after a record has been fetched.
   * 
   * @param t
   *          Transaction associated with the lock.
   * @param container
   *          Open Container used to get record.
   * @param record
   *          Record to unlock.
   * 
   * @exception StandardException
   *              Standard Derby error policy
   * @see Page
   */
  public void unlockRecordAfterRead(Transaction t, ContainerHandle container,
      ExecRow record) throws StandardException;
}

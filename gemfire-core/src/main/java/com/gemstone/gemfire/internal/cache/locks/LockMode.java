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

package com.gemstone.gemfire.internal.cache.locks;

/**
 * Locking mode to be used for a transactional operation or a distributed
 * operation in general. Combined with a {@link LockingPolicy} this will
 * determine the real locking semantics to be used for an operation e.g. shared
 * lock that will fail immediately if not available and be held till the end of
 * transaction.
 * 
 * @see LockingPolicy
 */
public enum LockMode {

  /**
   * Shared mode lock that allows multiple entities to have shared locks on the
   * object concurrently but no exclusive mode locks and exactly one
   * exclusive-shared mode lock.
   */
  SH {
    @Override
    public final boolean forWrite() {
      return false;
    }

    @Override
    public final int getLockModeArg() {
      return READ_SHARED_MODE;
    }
  },
  /**
   * Like {@link #SH} but also conflicts with {@link #EX_SH} locks at
   * READ_COMMITTED, and with {@link #EX} locks are REPEATABLE_READ. Typically
   * used to lock rows for reading in repeatable-read or foreign key checks in
   * master table.
   */
  READ_ONLY {
    @Override
    public final boolean forWrite() {
      return false;
    }

    @Override
    public final int getLockModeArg() {
      return READ_ONLY_MODE;
    }
  },
  /**
   * Exclusive-shared mode lock that allows other entities requesting shared
   * locks to go through but no other exclusive or exclusive-shared locks. If
   * reentrancy has been provided by an implementation then the same owner will
   * be able to lock the object in exclusive-shared mode again.
   */
  EX_SH {
    @Override
    public final boolean forWrite() {
      return true;
    }

    @Override
    public final int getLockModeArg() {
      return WRITE_SHARED_MODE;
    }
  },
  /**
   * Exclusive mode lock that will disallow any other entity access to this
   * object in any of the {@link LockMode}s. If reentrancy has been provided by
   * an implementation then the same owner will be able to lock the object in
   * exclusive mode again. This should be a short duration lock only because
   * most {@link LockingPolicy} implementations will go for infinite wait (even
   * fail-fast modes) when an entity is locked in this mode expecting the lock
   * duration is small.
   */
  EX {
    @Override
    public final boolean forWrite() {
      return true;
    }

    @Override
    public final int getLockModeArg() {
      return WRITE_EXCLUSIVE_MODE;
    }
  };

  // integer values corresponding to the LockModes passed to the lower
  // layer locking methods in ExclusiveSharedSynchronizer
  public static final int READ_SHARED_MODE = 0x1; // for SH
  public static final int READ_ONLY_MODE = 0x2; // for READ_ONLY
  public static final int WRITE_SHARED_MODE = 0x3; // for EX_SH
  public static final int WRITE_EXCLUSIVE_MODE = 0x4; // for EX

  /** to extract mode values above from an integer */
  public static final int MODE_MASK = 0x7;

  /**
   * Cache the array of all enumeration values for this enum.
   */
  private static final LockMode[] values = values();

  /**
   * Get a {@link LockMode} for given ordinal value ranging from 0 to (number of
   * enum values - 1).
   */
  public static final LockMode fromOrdinal(final int ordinal) {
    return values[ordinal];
  }

  /**
   * Get the number of enumeration values defined for this enum.
   */
  public static final int size() {
    return values.length;
  }

  /**
   * Return true if this is for a write operation.
   */
  public abstract boolean forWrite();

  /**
   * Get an integer corresponding to this LockMode passed to underlying locking
   * layers (one of {@link #READ_SHARED_MODE}, {@link #READ_ONLY_MODE},
   * {@link #WRITE_SHARED_MODE}, {@link #WRITE_EXCLUSIVE_MODE}).
   */
  public abstract int getLockModeArg();
}

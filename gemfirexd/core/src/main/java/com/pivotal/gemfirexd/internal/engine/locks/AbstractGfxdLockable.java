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

package com.pivotal.gemfirexd.internal.engine.locks;

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;

/**
 * Provides some common functionality for {@link GfxdLockable}s.
 * 
 * @author swale
 */
public abstract class AbstractGfxdLockable implements GfxdLockable {

  /** The {@link GfxdReadWriteLock} used for locking this object. */
  protected GfxdReadWriteLock rwLock;

  /** True if lock tracing is on for this object and false otherwise. */
  protected boolean traceLock;

  /** default constructor */
  protected AbstractGfxdLockable() {
    this.traceLock = GemFireXDUtils.TraceLock;
  }

  /**
   * @see GfxdLockable#getName()
   */
  public abstract Object getName();

  /**
   * @see GfxdLockable#getReadWriteLock()
   */
  public final GfxdReadWriteLock getReadWriteLock() {
    return this.rwLock;
  }

  /**
   * @see GfxdLockable#setReadWriteLock(GfxdReadWriteLock)
   */
  public final void setReadWriteLock(GfxdReadWriteLock rwLock) {
    this.rwLock = rwLock;
  }

  /**
   * @see GfxdLockable#traceLock()
   */
  public final boolean traceLock() {
    return this.traceLock;
  }

  /**
   * Classes extending this should return true when tracing for this particular
   * lock has been turned on and false otherwise. The public
   * {@link #traceLock()} method combines the result with that of the global
   * {@link GfxdLocalLockService#TraceOn} flag to enable/disable lock tracing
   * for this object.
   */
  protected abstract boolean traceThisLock();

  /**
   * This method should be invoked by the child classes at an appropriate time
   * (when {@link #traceThisLock()} will return a meaningful result) to enable
   * lock tracing for this object.
   */
  protected final void setTraceLock() {
    if (!this.traceLock) {
      this.traceLock = GemFireXDUtils.TraceLock || traceThisLock();
    }
  }

  /**
   * Child classes should give a meaningful implementation since this will go as
   * key of a HashMap.
   * 
   * @see Object#equals(Object)
   */
  @Override
  public abstract boolean equals(Object other);

  /**
   * Child classes should give a meaningful implementation since this will go as
   * key of a HashMap.
   * 
   * @see Object#hashCode()
   */
  @Override
  public abstract int hashCode();
}

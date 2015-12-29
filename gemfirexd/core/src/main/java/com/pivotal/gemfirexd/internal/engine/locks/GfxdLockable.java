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

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary;

/**
 * This class defines an object that can be locked efficiently by
 * {@link GfxdDRWLockService} using the {@link GfxdReadWriteLock} embedded
 * inside this object instead of having to look from a concurrent map.
 * Currently this is used for acquire and release of read locks on
 * {@link GemFireContainer} and {@link GfxdDataDictionary} to have maximum
 * efficiency for DML operations that take read locks on the respective
 * containers.
 * 
 * @author swale
 * @since 6.5
 */
public interface GfxdLockable {

  /**
   * Get the underlying name of the object that is used for mapping against the
   * {@link GfxdReadWriteLock}.
   */
  Object getName();

  /**
   * Set the {@link GfxdReadWriteLock} associated with this lockable. The
   * mechanism is that the {@link GfxdDRWLockService} will set the underlying
   * lock the first time and then will reuse the lock from the given
   * {@link GfxdLockable} without having to lookup the {@link GfxdReadWriteLock}
   * object from a map.
   */
  void setReadWriteLock(GfxdReadWriteLock rwLock);

  /**
   * Get the {@link GfxdReadWriteLock} object embedded inside this
   * {@link GfxdLockable} using the {@link #setReadWriteLock(GfxdReadWriteLock)}
   * method by {@link GfxdDRWLockService}.
   */
  GfxdReadWriteLock getReadWriteLock();

  /**
   * Return true to get a trace of lock/unlock for this object in logs. This is
   * in addition to the {@link GfxdConstants#TRACE_LOCK} flag that will trace all locks in
   * any case.
   */
  boolean traceLock();
}

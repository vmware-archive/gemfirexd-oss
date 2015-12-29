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

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.cache.delta.Delta;

/**
 * Interface to indicate whether an Event holds an internal {@link Delta}
 * object.
 * 
 * @author sjigyasu
 * @since 7.5
 */
public interface InternalDeltaEvent {

  /**
   * Return true if this event holds an internal {@link Delta} object.
   */
  public boolean hasDelta();

  /**
   * Return true if this event is really a create, which can happen in GemFireXD
   * if {@link Operation#isCreate()} is true or if {@link Operation#isUpdate()}
   * is true with {@link #hasDelta()} as false.
   * 
   * Currently this happens if there is a retry from GFXD thin client resulting
   * in a posdup where subsequent insert is put as an UPDATE operation so as to
   * not miss WAN queue (#47462).
   * 
   * @param updateAsCreateOnlyForPosDup
   *          if this is set to true, then treat UPDATE op type as a CREATE when
   *          value is not a delta ({@link #hasDelta()} is false) (i.e. return
   *          true) only when event is a possible duplicate, else if event is
   *          not a possible duplicate (e.g. for concurrent loader invocations)
   *          then don't treat an UPDATE as a CREATE even if value is a full row
   *          and not a delta
   * 
   *          If this is set to false, then always treat UPDATE op type as a
   *          CREATE whenever the value is not a delta ({@link #hasDelta()} is
   *          false)
   */
  public boolean isGFXDCreate(boolean updateAsCreateOnlyForPosDup);
}

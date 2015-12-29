
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

/*
 This file was based on the MemStore patch written by Knut Magne, published
 under the Derby issue DERBY-2798 and released under the same license,
 ASF, as described above. The MemStore patch was in turn based on Derby source
 files.
*/

package com.pivotal.gemfirexd.internal.engine.access.operations;

import java.io.IOException;

import com.gemstone.gemfire.cache.Region;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * DOCUMENT ME!
 * 
 * @author yjing
 */
public final class MemDeleteOperation extends MemOperation {

  /** Region key for the delete operation. */
  private final Object key;

  /** Routing object, if available, for the delete operation. */
  private final Object callbackArg;

  /** true if this operation is primary key based */
  private final boolean isPkBased;
  
  private final boolean isEvict;

  /**
   * The current value associated with the key, if any. This will be set only
   * after the {@link #doMe(Transaction, LogInstant, LimitObjectInput)} method
   * has been invoked for use by the
   * {@link #generateUndo(Transaction, LimitObjectInput)} method.
   */
  private Object currentValue;

  /**
   * Creates a new $class.name$ object.
   * 
   * @param container
   *          the {@link GemFireContainer} for the operation
   * @param regionKey
   *          the {@link Region} key to be destroyed
   * @param routingObject
   *          the routing object, if any, for the operation
   */
  public MemDeleteOperation(GemFireContainer container, Object regionKey,
      Object  callbackArg, boolean isPkBased, boolean isEvict) {
    super(container);
    this.key = regionKey;
    this.callbackArg = callbackArg;
    this.isPkBased = isPkBased;
    this.isEvict = isEvict;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    this.currentValue = this.memcontainer.delete(this.key, this.callbackArg,
        this.isPkBased, null, null, GemFireTransaction
            .getLanguageConnectionContext((GemFireTransaction)xact), this.isEvict);
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    // for PRs we cannot undo as PR#destroy does not return the old value
    if (this.memcontainer.getRegion().getDataPolicy().withPartitioning()) {
      throw new UnsupportedOperationException("cannot undo partitioned table "
          + "delete since PR#destroy does not provide old value");
    }
    return new MemInsertOperation(this.memcontainer, null, this.key,
        this.currentValue, false /* is cache loaded*/);
  }

  @Override
  public boolean shouldBeConflated() {
    return true;
  }

  @Override
  public Object getKeyToConflate() {
    return this.key;
  }

  public Object getOldValue() {
    return this.currentValue;
  }
}

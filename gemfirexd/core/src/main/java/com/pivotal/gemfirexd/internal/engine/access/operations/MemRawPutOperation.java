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

import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * Used only to undo a {@link MemUpdateOperation}.
 * 
 * @author swale
 */
public class MemRawPutOperation extends MemOperation {

  private final Object key;

  private final Object value;

  MemRawPutOperation(GemFireContainer container, Object key, Object value) {
    super(container);
    this.key = key;
    this.value = value;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    this.memcontainer.put(this.key, this.value, null);
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    throw new UnsupportedOperationException("unexpected call");
  }

  @Override
  public Object getKeyToConflate() {
    return this.key;
  }

  @Override
  public Object getValueToConflate() {
    return this.value;
  }
}

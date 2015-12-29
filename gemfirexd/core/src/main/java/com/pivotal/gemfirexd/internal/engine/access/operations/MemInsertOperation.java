
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
 * Changes for GemFireXD distributed data platform.
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

import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * MemInsertOperation
 * 
 * @author Eric Zoerner
 */
public final class MemInsertOperation extends MemOperation {

  /** The row to be inserted. */
  private final DataValueDescriptor[] row;

  /** The Region key for value to be inserted. */
  private Object regionKey;

  /**
   * The raw value to be inserted.
   */
  private final Object value;

  /** the routing object used in this insert operation */
  private Object routingObject;
  
  final private boolean isCacheLoaded ;

  /**
   * Creates a new {@link MemInsertOperation} object.
   * 
   * @param container
   *          the {@link GemFireContainer} for the operation
   * @param row
   *          the DVD array to be inserted
   * @param key
   *          the region key for the value to be inserted
   * @param value
   *          the raw value to be inserted
   */
  public MemInsertOperation(GemFireContainer container,
      DataValueDescriptor[] row, Object key, Object value, boolean isCacheLoaded) {
    super(container);
    this.row = row;
    this.regionKey = key;
    this.value = value;
    this.isCacheLoaded = isCacheLoaded;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final LanguageConnectionContext lcc = ((GemFireTransaction)xact)
        .getLanguageConnectionContext();
    Object oldContext = null;
    if (lcc != null) {
      oldContext = lcc.getContextObject();
    }
    try {
      if (this.row != null) {
        this.regionKey = this.memcontainer.insertRow(this.row, null, null, lcc, false /*isPutDML*/);
      }
      else {
        this.memcontainer.insert(this.regionKey, this.value, true, null, null,
            lcc, this.isCacheLoaded, false /*isPutDML*/, false /*wasPutDML*/);
      }
      if (lcc != null) {
        this.routingObject = lcc.getContextObject();
      }
    } finally {
      if (lcc != null) {
        lcc.setContextObject(oldContext);
      }
    }
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    if (this.regionKey != null) {
      // No need to wrap as it is undo operationso it should not result in
      // creation of GfxdTXEntryState etc.
      return new MemDeleteOperation(this.memcontainer, this.regionKey,
          this.routingObject, true, false);
    }
    return null;
  }

  @Override
  public Object getKeyToConflate() {
    return this.regionKey;
  }

  @Override
  public Object getValueToConflate() {
    return this.value;
  }

  public Object getRegionKey() {
    return this.regionKey;
  }

  @Override
  protected StringBuilder toStringBuilder(StringBuilder sb, String regionName) {
    return sb.append(this.getClass().getName()).append(" region=").append(
        regionName).append(" row={").append(RowUtil.toString(this.row)).append(
        '}');
  }
}

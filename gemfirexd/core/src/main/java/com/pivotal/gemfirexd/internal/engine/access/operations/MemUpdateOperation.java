
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
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.RegionEntry;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * DOCUMENT ME!
 */
public final class MemUpdateOperation extends MemOperation {

  /** RegionEntry for the update operation. */
  private final RegionEntry entry;

  /** Key for the update operation. */
  private final Object key;

  /** DOCUMENT ME! */
  private final DataValueDescriptor[] changedRow;

  /** DOCUMENT ME! */
  private final FormatableBitSet validColumns;

  private final int bucketId;

  private Object rawOldValue;
  
  private final Object callbackArg;

  /**
   * Creates a new $class.name$ object.
   * 
   * @param container
   *          DOCUMENT ME!
   * @param row
   *          DOCUMENT ME!
   * @param entry
   *          DOCUMENT ME!
   * @param validColumns
   *          DOCUMENT ME!
   * @param bucketId
   *          int identifying the bucket ID. This can be -1 if the region is not
   *          a partitioned region or if it is not overflowing to disk
   */
  public MemUpdateOperation(GemFireContainer container,
      DataValueDescriptor[] row, RegionEntry entry, Object key,
      FormatableBitSet validColumns, int bucketId, Object callbackArg) {
    super(container);
    this.entry = entry;
    this.key = key;
    this.changedRow = row;
    this.validColumns = validColumns;
    this.bucketId = bucketId;
    this.callbackArg = callbackArg;
    LogWriterI18n logger = Misc.getI18NLogWriter();
    if (logger.fineEnabled()) {
      logger.fine("MemUpdateOperation::Constructor: Entry=" + entry
          + ". bucket ID = " + bucketId + " for region ="
          + container.getRegion().getFullPath());
    }
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final LanguageConnectionContext lcc = ((GemFireTransaction)xact)
        .getLanguageConnectionContext();
    if (this.entry != null) {
      this.rawOldValue = this.memcontainer.replacePartialRow(this.entry,
          this.validColumns, this.changedRow, this.bucketId, null, null, lcc);
    }
    else {
      assert this.callbackArg != null;
      this.rawOldValue = this.memcontainer
          .replacePartialRow(this.key, this.validColumns, this.changedRow,
              this.callbackArg, null, null, lcc, null, false);
    }
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    return new MemRawPutOperation(this.memcontainer, this.key, this.rawOldValue);
  }

  @Override
  public Object getKeyToConflate() {
    return this.entry.getKey();
  }

  public Object getOldValue() {
    return this.rawOldValue;
  }
}

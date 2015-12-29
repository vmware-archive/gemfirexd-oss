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

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager;
import com.pivotal.gemfirexd.internal.engine.access.operations.MemOperation;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * This class enables us to delay the {@link ExtraTableInfo} refresh in DDL
 * execution till the commit time. Also helps to avoid refreshing during a DML
 * operation, since that should really take a DD read lock which is not
 * possible. See bugs #40707, #39808 which are due to inability to take DD read
 * lock during refresh.
 * 
 * @author swale
 */
public final class TableInfoRefresh extends MemOperation {

  final ExtraTableInfo tableInfo;

  private boolean skipRefresh;

  private ColumnDescriptor addColumn;

  private int dropColumn;

  private final boolean indexLocked;

  TableInfoRefresh(ExtraTableInfo tableInfo, boolean lockIndex,
      GemFireTransaction tran) {
    super(tableInfo.container);
    if (GemFireXDUtils.TraceConglom) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "Creating TableInfoRefresh object to refresh cached table info "
              + "in GemFireContainer at commit for TC " + tran);
    }
    this.tableInfo = tableInfo;
    this.skipRefresh = false;
    GfxdIndexManager indexManager;
    if (lockIndex && (indexManager = getIndexManager()) != null) {
      this.indexLocked = indexManager.lockForGII(true, tran);
    }
    else {
      this.indexLocked = false;
    }
  }

  public void scheduleAddColumn(ColumnDescriptor cd) {
    this.addColumn = cd;
  }

  private GfxdIndexManager getIndexManager() {
    LocalRegion r = this.tableInfo.container.getRegion();
    if (r != null) {
      return (GfxdIndexManager)r.getIndexUpdater();
    }
    else {
      return null;
    }
  }

  public void scheduleDropColumn(int columnPos) {
    this.dropColumn = columnPos;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final GemFireTransaction tc = (GemFireTransaction)xact;
    if (GemFireXDUtils.TraceConglom) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "Invoked TableInfoRefresh#doMe with skipRefresh=" + this.skipRefresh
              + " for TX " + tc);
    }
    if (!this.skipRefresh) {
      // check for add/drop column first
      if (this.addColumn != null || this.dropColumn > 0) {
        this.memcontainer.schemaVersionChange(this.tableInfo.dd,
            this.tableInfo.getTableDescriptor(),
            GemFireTransaction.getLanguageConnectionContext(tc));
        if (this.addColumn != null) {          
          this.memcontainer.addColumnAtCommit(this.addColumn);
        }
        else {
          this.memcontainer.dropColumnAtCommit(this.dropColumn);
        }
        this.memcontainer.getExtraTableInfo().refreshCachedInfo(null,
            this.memcontainer);        
      }
      
      this.tableInfo.refreshCachedInfo(null, this.memcontainer);
    }
    // release the index lock at the end if required
    if (this.indexLocked) {
      getIndexManager().unlockForGII(true, tc);
    }
  }

  @Override
  public boolean doAtCommitOrAbort() {
    return true;
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    this.skipRefresh = true;
    return null;
  }
}

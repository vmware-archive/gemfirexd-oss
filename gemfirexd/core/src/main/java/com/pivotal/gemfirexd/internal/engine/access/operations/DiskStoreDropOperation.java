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

import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author ymahajan
 * 
 */
public class DiskStoreDropOperation extends MemOperation {

  private String storeName;

  protected DiskStoreDropOperation(String storeName) {
    super(null);
    this.storeName = storeName;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final GemFireTransaction tc = (GemFireTransaction)xact;
    final LanguageConnectionContext lcc = tc.getLanguageConnectionContext();
    DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
    dd.startWriting(lcc);
    this.storeName = SharedUtils.SQLToUpperCase(storeName);
    DiskStoreImpl store = (DiskStoreImpl)Misc.getGemFireCache().findDiskStore(
        storeName);
    try {
      if (store != null) {
        store.destroy();
      }
    }
    catch (Exception e) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "DropDiskStore :: got Exception", e);
    }
    ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
    keyRow.setColumn(1, new SQLVarchar(storeName));
    TabInfoImpl ti = dd
        .getNonCoreTI(DataDictionaryImpl.SYSDISKSTORES_CATALOG_NUM);
    ti.deleteRow(tc, keyRow, 0);
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "DropDiskStore :: removed DiskStore " + storeName + " from SYS table");
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    throw new UnsupportedOperationException("DiskStoreDropOperation: undo "
        + "unimplemented; require GFE diskstore rename support");
  }
}

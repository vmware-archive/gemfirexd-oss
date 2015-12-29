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

import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreFactoryImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.services.uuid.UUIDFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdHDFSStoreDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
/**
 * 
 * @author jianxiachen
 *
 */

public class HDFSStoreCreateOperation extends MemOperation {

  private final HDFSStoreFactory hsf;

  private final String storeName;

  public HDFSStoreCreateOperation(HDFSStoreFactory hsf, String storeName) {
    super(null);
    this.hsf = hsf;
    this.storeName = storeName;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final GemFireTransaction tc = (GemFireTransaction)xact;
    final LanguageConnectionContext lcc = tc.getLanguageConnectionContext();
    DataDictionary dd = lcc.getDataDictionary();
    dd.startWriting(lcc);
    // Check to see if this hdfsstore already exists in the cache
    // TODO: HDFS: shouldn't this be checking for accessor node?
    // TODO: HDFS: server groups support in CREATE HDFSSTORE
    HDFSStoreImpl hsi = Misc.getGemFireCache().findHDFSStore(storeName);
    if (hsi != null)
    {
    	throw StandardException.newException(
			  SQLState.LANG_OBJECT_ALREADY_EXISTS, "HDFSSTORE", storeName);
    }
    
    try {
      UUIDFactory factory = dd.getUUIDFactory();
      UUID id = factory.recreateUUID(storeName);
      GfxdHDFSStoreDescriptor hsd = new GfxdHDFSStoreDescriptor(dd, id,
          ((HDFSStoreFactoryImpl)hsf).getConfigView());
      dd.addDescriptor(hsd, null, DataDictionary.SYSHDFSSTORES_CATALOG_NUM,
          false, tc);
      
      if (!ServerGroupUtils.isDataStore()) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
            "Skipping create hdfsstore for " + storeName + " on JVM of kind "
                + GemFireXDUtils.getMyVMKind());
        return;
      }
      
      HDFSStore hs = hsf.create(storeName);
      
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "CreateHDFSStoreNode: added hdfsstore configuration for "
            + storeName + " in SYS table");
    } catch (Throwable t) {
      throw StandardException.newException(SQLState.HDFS_ERROR, t, t.getMessage());
    }
    
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    return new HDFSStoreDropOperation(this.storeName);
  }

}

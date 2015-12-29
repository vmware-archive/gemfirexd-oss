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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.util.List;

import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author jianxiachen
 *
 */

public class DropHDFSStoreConstantAction extends DDLConstantAction {

  final String hdfsStoreName;

  final boolean onlyIfExists;

  DropHDFSStoreConstantAction(String hdfsStoreName, boolean onlyIfExists) {
    this.hdfsStoreName = hdfsStoreName;
    this.onlyIfExists = onlyIfExists;
  }

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP HDFSSTORE statements.
  @Override
  public final String getSchemaName() {
    // HDFS stores have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  @Override
  public final String getTableName() {
    return CreateHDFSStoreConstantAction.REGION_PREFIX_FOR_CONFLATION
        + hdfsStoreName;
  }

  @Override
  public final boolean isDropStatement() {
    return true;
  }

  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {
    int rowsDeleted = 0;

    List<GemFireContainer> containers = Misc.getMemStore().getAllContainers();
    for (GemFireContainer container : containers) {
      if (container.getRegion() != null && container.isApplicationTable()) {        
        String regionHDFSStoreName = container.getRegionAttributes()
            .getHDFSStoreName();
        if (regionHDFSStoreName != null
            && regionHDFSStoreName.equalsIgnoreCase(hdfsStoreName)) {
          throw StandardException.newException(
              SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, "DROP", "HDFSStore "
                  + hdfsStoreName, "table", container.getQualifiedTableName());
        }
      }
    }

    // OK, we're good to go - drop the object from the catalog first
    // and then drop the gemfire object second
    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
    TransactionController tc = lcc.getTransactionExecute();
    dd.startWriting(lcc);
    ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
    keyRow.setColumn(1, new SQLVarchar(hdfsStoreName));

    TabInfoImpl ti = dd
        .getNonCoreTI(DataDictionaryImpl.SYSHDFSSTORES_CATALOG_NUM);
    rowsDeleted = ti.deleteRow(tc, keyRow, 0);
    // If no row deleted from catalog, it's an error unless IF EXISTS specified
    if (rowsDeleted == 0) {
      if (onlyIfExists) {
        return;
      }
      else {
        // The HDFS store wasn't in the catalog in the first place
        // Throw object-not-found exception
        throw StandardException.newException(
            SQLState.LANG_OBJECT_DOES_NOT_EXIST, "DROP HDFSSTORE", hdfsStoreName);
      }
    }
    
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "DropHDFSStore:: removed HDFSStore " + hdfsStoreName+ " from SYS table");

    // If this node is not hosting data, nothing to do
    if (!ServerGroupUtils.isDataStore()) {
      return;
    }

    HDFSStoreImpl store = (HDFSStoreImpl) Misc.getGemFireCache().findHDFSStore(
        hdfsStoreName);
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "DropHDFSStore :: found HDFSStore " + store);
    if (store != null) {
      Misc.getGemFireCache().removeHDFSStore(store);
    }
  }

  @Override
  public String toString() {
    return constructToString("DROP HDFSSTORE ", hdfsStoreName);
  }

  @Override
  public boolean isCancellable() {
    return false;
  };
}

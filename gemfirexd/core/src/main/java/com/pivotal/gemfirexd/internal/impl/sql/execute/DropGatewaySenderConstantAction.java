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
import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
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

public class DropGatewaySenderConstantAction extends DDLConstantAction {

  final String id;

  final boolean onlyIfExists;

  DropGatewaySenderConstantAction(String id, boolean onlyIfExists) {
    this.id = id;
    this.onlyIfExists = onlyIfExists;
  }

  @Override
  public boolean isDropIfExists() {
    return onlyIfExists;
  }

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP GATEWAYSENDER statements.
  @Override
  public final String getSchemaName() {
    // gateways have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  @Override
  public final String getTableName() {
    return CreateGatewaySenderConstantAction.REGION_PREFIX_FOR_CONFLATION + id;
  }

  @Override
  public final boolean isDropStatement() {
    return true;
  }

  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {

    int rowsDeleted = 0;
    // If this node is not hosting data, return success, nothing to do
    if (!ServerGroupUtils.isDataStore()) {
      return;
    }

    // Check if GATEWAYSENDER is in use by a table
    List<GemFireContainer> containers = Misc.getMemStore().getAllContainers();
    for (GemFireContainer container : containers) {
      if (container.getRegion() != null && container.isApplicationTable()) {
        Set<String> senderIds = container.getRegionAttributes()
            .getGatewaySenderIds();
        if (senderIds != null && !senderIds.isEmpty() && senderIds.contains(id)) {
          throw StandardException
              .newException(SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT,
                  "DROP", "GatewaySender " + id, "table",
                  container.getQualifiedTableName());
        }
      }
    }

    // Drop GATEWAYSENDER from catalog
    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
    TransactionController tc = lcc.getTransactionExecute();
    dd.startWriting(lcc);
    ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
    TabInfoImpl ti = dd
        .getNonCoreTI(DataDictionaryImpl.GATEWAYSENDERS_CATALOG_NUM);

    keyRow.setColumn(1, new SQLVarchar(id));
    rowsDeleted = ti.deleteRow(tc, keyRow, 0);
    if (rowsDeleted == 0) {
      // The GATEWAYSENDER wasn't in the catalog in the first place
      // Throw object-not-found exception
      if (onlyIfExists) {
        return;
      }
      else {
        throw StandardException.newException(
            SQLState.LANG_OBJECT_DOES_NOT_EXIST, "DROP GATEWAYSENDER", id);
      }
    }
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "DropGatewaySender:: removed GatewaySender " + id + " from SYS table");

    // Finally, remove the GATEWAYSENDER cache object
    GatewaySender sender = Misc.getGemFireCache().getGatewaySender(id);
    if (sender != null) {
      try {
        Misc.getGemFireCache().removeGatewaySender(sender);
        sender.destroy();
      } catch (Exception ex) {
        throw StandardException.newException(
            SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
            ex.toString());
      }
    }
  }

  @Override
  public boolean isCancellable() {
    return false;
  }

  // OBJECT METHODS

  @Override
  public String toString() {
    return constructToString("DROP GATEWAYSENDER ", id);
  }
}

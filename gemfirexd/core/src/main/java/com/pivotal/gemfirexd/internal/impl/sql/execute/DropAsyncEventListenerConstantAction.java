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

import com.gemstone.gnu.trove.THashMap;
import com.gemstone.gnu.trove.TObjectObjectProcedure;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.operations.AsyncQueueDropOperation;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdAsyncEventListenerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public class DropAsyncEventListenerConstantAction extends DDLConstantAction {

  // TODO : if schema names are needed for asynceventlisteners, must handle
  // SchemaDescriptors here
  final String id; // The name of the asynceventlistener

  final boolean onlyIfExists;

  // Constructor for this constant action
  // Initialize all fields
  DropAsyncEventListenerConstantAction(String id, boolean onlyIfExists) {
    this.id = id;
    this.onlyIfExists = onlyIfExists;
  }

  // OBJECT METHODS
  @Override
  public String toString() {
    return "DROP ASYNCEVENTLISTENER " + id;
  }

  @Override
  public boolean isDropIfExists() {
    return onlyIfExists;
  }

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP ASYNCEVENTLISTENER statements.
  @Override
  public final String getSchemaName() {
    // Async Eventlisteners have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  @Override
  public final String getTableName() {
    return CreateAsyncEventListenerConstantAction.REGION_PREFIX_FOR_CONFLATION
        + id;
  }

  @Override
  public final boolean isDropStatement() {
    return true;
  }

  @Override
  public boolean isCancellable() {
    return false;
  }

  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {

    int rowsDeleted = 0;
    // If this node is not hosting data, nothing to do
    if (!ServerGroupUtils.isDataStore()) {
      return;
    }
    // Check if any asynceventlistener is being used by a table
    // Throw exception in this case, we cannot drop it
    List<GemFireContainer> containers = Misc.getMemStore().getAllContainers();
    for (GemFireContainer container : containers) {
      if (container.getRegion() != null && container.isApplicationTable()) {
        Set<String> senderIds = container.getRegionAttributes()
            .getAsyncEventQueueIds();
        if (senderIds != null && !senderIds.isEmpty() && senderIds.contains(id)) {
          throw StandardException.newException(
              SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT, "DROP",
              "AsyncEventListener " + id, "table",
              container.getQualifiedTableName());
        }
      }
    }

    // Drop it from the catalog

    final LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    final DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
    final TransactionController tc = lcc.getTransactionExecute();
    dd.startWriting(lcc);

    ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
    keyRow.setColumn(1, new SQLVarchar(id));

    TabInfoImpl ti = dd
        .getNonCoreTI(DataDictionaryImpl.ASYNCEVENTLISTENERS_CATALOG_NUM);
    rowsDeleted = ti.deleteRow(tc, keyRow, 0);
    // If no row deleted from catalog, it's an error unless IF EXISTS specified
    if (rowsDeleted == 0) {
      if (onlyIfExists) {
        return;
      }
      else {
        // The asynceventlistener wasn't in the catalog in the first place
        // Throw object-not-found exception
        throw StandardException.newException(
            SQLState.LANG_OBJECT_DOES_NOT_EXIST, "DROP ASYNCEVENTLISTENER", id);
      }
    }
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "DropAsyncEventListener:: removed AsyncEventListener " + id
            + " from SYS table");

    final THashMap uid_idmap = Misc.getMemStore().getUUID_IDMap();
    if (uid_idmap != null) {
      uid_idmap.forEachEntry(new TObjectObjectProcedure() {
        public boolean execute(Object a, Object b) {
          if (((String)b).equalsIgnoreCase(id)) {
            uid_idmap.remove(a);
            GfxdAsyncEventListenerDescriptor asyncD = new GfxdAsyncEventListenerDescriptor(id, (UUID)a);
            try {
              dd.getDependencyManager().clearDependencies(lcc, asyncD);
            } catch (StandardException e) {
              // can ignore exception here
              Misc.getCacheLogWriter().warning("problem while removing dependencies of: " + id, e);
            }
            return false;
          }
          return true;
        }
      });
    }
    // Finally, stop the cache object
    tc.logAndDo(new AsyncQueueDropOperation(id));
  }
}

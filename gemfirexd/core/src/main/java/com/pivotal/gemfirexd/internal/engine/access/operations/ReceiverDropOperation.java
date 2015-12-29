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

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.ddl.ServerGroupsTableAttribute;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdSysGatewayReceiverRowFactory;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author ymahajan
 * 
 */
public final class ReceiverDropOperation extends MemOperation {

  private final String id;
  private ServerGroupsTableAttribute serverGroups;
  private final boolean onlyIfExists;

  public ReceiverDropOperation(String id,
      ServerGroupsTableAttribute serverGroups, boolean onlyIfExists) {
    super(null);
    this.id = id;
    this.serverGroups = serverGroups;
    this.onlyIfExists = onlyIfExists;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final GemFireTransaction tc = (GemFireTransaction)xact;
    final LanguageConnectionContext lcc = tc.getLanguageConnectionContext();
    final DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
    dd.startWriting(lcc);

    // Drop GATEWAYRECEIVER from catalog
    ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
    TabInfoImpl ti = dd
        .getNonCoreTI(DataDictionaryImpl.GATEWAYRECEIVERS_CATALOG_NUM);

    keyRow.setColumn(1, new SQLVarchar(id));
    ExecRow row = ti.getRow(tc, keyRow, 0);
    if (row != null) {
      DataValueDescriptor groups = row
          .getColumn(GfxdSysGatewayReceiverRowFactory.SERVER_GROUPS);
      if (groups != null) {
        this.serverGroups = new ServerGroupsTableAttribute();
        this.serverGroups.addServerGroups(SharedUtils.toSortedSet(
            groups.getString(), false));
      }
    }
    int rowsDeleted = ti.deleteRow(tc, keyRow, 0);
    if (rowsDeleted == 0) {
      if (onlyIfExists) {
        return;
      }
      else {
        // We didn't find this gatewayreceiver
        // Throw object-not-found exception
        throw StandardException.newException(
            SQLState.LANG_OBJECT_DOES_NOT_EXIST, "DROP GATEWAYRECEIVER", id);
      }
    }
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "DropGatewayReceiver:: removed GatewayReceiver " + id
            + " from SYS table");

    try {
      // Stop the GATEWAYRECEIVER cache object
      GemFireCacheImpl cache = Misc.getGemFireCache();
      try {
        GatewayReceiver receiver = cache.getGatewayReceiver(id);
        if (receiver != null) {
          cache.removeGatewayReceiver(receiver);
          receiver.stop();
        }
      } catch (Exception ex) {
        if (onlyIfExists) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
              "DropGatewayReceiver:: got Exception", ex);
        }
        else {
          throw StandardException.newException(
              SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
              ex.toString());
        }
      }
    } catch (Exception e) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
          "DropGatewayReceiver:: got Exception", e);
    }
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    GatewayReceiver receiver;
    try {
      GemFireCacheImpl cache = Misc.getGemFireCache();
      receiver = cache.getGatewayReceiver(id);
    } catch (Exception e) {
      receiver = null;
    }
    if (receiver != null) {
      return new ReceiverCreateOperation(receiver, this.serverGroups);
    }
    else {
      return null;
    }
  }
}

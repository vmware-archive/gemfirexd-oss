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

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.operations.ReceiverDropOperation;
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

public class DropGatewayReceiverConstantAction extends DDLConstantAction {

  final private String id;

  final private Boolean onlyIfExists;

  DropGatewayReceiverConstantAction(String id, boolean onlyIfExists) {
    this.id = id;
    this.onlyIfExists = onlyIfExists;
  }

  @Override
  public String toString() {
    return "DROP GATEWAYRECEIVER " + id;
  }

  @Override
  public boolean isDropIfExists() {
    return onlyIfExists;
  }

  // Override the getSchemaName/getObjectName to enable
  // DDL conflation of CREATE and DROP GATEWAYRECEIVER statements.
  @Override
  public final String getSchemaName() {
    // Gatewayreceivers have no schema, so return 'SYS'
    return SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME;
  }

  @Override
  public final String getTableName() {
    return CreateGatewayReceiverConstantAction.REGION_PREFIX_FOR_CONFLATION
        + id;
  }

  @Override
  public final boolean isDropStatement() {
    return true;
  }

  @Override
  public void executeConstantAction(Activation activation)
      throws StandardException {

    // If this node is not hosting data, return success, nothing to do
    if (!ServerGroupUtils.isDataStore()) {
      return;
    }

    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    TransactionController tc = lcc.getTransactionExecute();
    ReceiverDropOperation dropOp = new ReceiverDropOperation(id, null,
        onlyIfExists);
    tc.logAndDo(dropOp);
  }

  @Override
  public boolean isCancellable() {
    return false;
  }
}

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
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.ddl.ServerGroupsTableAttribute;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.GfxdGatewayReceiverDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
/**
 * 
 * @author ymahajan
 *
 */
public final class ReceiverCreateOperation extends MemOperation {

  final GatewayReceiver receiver;

  final ServerGroupsTableAttribute serverGroups;

  public ReceiverCreateOperation(GatewayReceiver receiver,
      ServerGroupsTableAttribute serverGroups) {
    super(null);
    this.receiver = receiver;
    this.serverGroups = serverGroups;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    final GemFireTransaction tc = (GemFireTransaction)xact;
    final LanguageConnectionContext lcc = tc.getLanguageConnectionContext();
    final DataDictionary dd = lcc.getDataDictionary();
    try {
      this.receiver.start();
    }
    catch (IllegalStateException e)
    {
    	// No open port was found, throw StandardException
    	// And tell user which ports we tried. Do not drop the current connection!
    	throw StandardException.newException(SQLState.NOT_IMPLEMENTED, 
    	 "No open port was found between "+receiver.getStartPort() + " and " +receiver.getEndPort());   	
    }
    int runningPort = this.receiver.getPort();
    UUID uuid = dd.getUUIDFactory().recreateUUID(String.valueOf(runningPort));
    String servers = SharedUtils.toCSV(serverGroups.getServerGroupSet());
    GfxdGatewayReceiverDescriptor ghd = new GfxdGatewayReceiverDescriptor(dd,
        uuid, receiver.getId(), servers, receiver.getStartPort(),
        receiver.getEndPort(), runningPort, receiver.getSocketBufferSize(),
        receiver.getMaximumTimeBetweenPings(), receiver.getBindAddress(), receiver.getHost());
    dd.addDescriptor(ghd, null, DataDictionary.GATEWAYRECEIVERS_CATALOG_NUM,
        false, tc);
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
        "CreateGatewayReceiverNode:: inserted GatewayReceiver configuration for "
            + receiver.getId() + " in SYS table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    return new ReceiverDropOperation(this.receiver.getId(), this.serverGroups,
        true);
  }
}

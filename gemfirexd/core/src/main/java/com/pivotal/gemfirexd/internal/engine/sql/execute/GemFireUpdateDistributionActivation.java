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

package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdDistributedDeleteResultCollector;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdResultCollector;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DMLQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DeleteQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.UpdateQueryInfo;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;

/**
 * The Activation object which is used to distribute updates
 * 
 * @author Asif
 */
public final class GemFireUpdateDistributionActivation extends
    AbstractGemFireDistributionActivation {
	private final GemFireTransaction tran;
  // private final int execRowID;
  // private static AtomicInteger ai = new AtomicInteger(0);
  public GemFireUpdateDistributionActivation(ExecPreparedStatement st,
      LanguageConnectionContext _lcc, DMLQueryInfo qi) throws StandardException {
    super(st, _lcc, qi);
    this.tran = (GemFireTransaction) _lcc.getTransactionExecute();
  }

  @Override
  protected void executeWithResultSet(AbstractGemFireResultSet rs)
      throws StandardException {
    ((UpdateQueryInfo)this.qInfo).checkNotNullCriteria(this);
    super.executeWithResultSet(rs);
    // Distribute the bulk update to the DBSynchronizer senders.
    // First check if this table is attached with an DBSynchronizer.
    // If yes, find out the nodes on which the sender is running & then prepare
    // adjunct message
    LanguageConnectionContext lcc = this.getLanguageConnectionContext();
    LocalRegion rgn = this.qInfo.getRegion();
    // TODO: Suranjan : It can be optmized in the sense that
    // if the serial gatewaysender doesn't want dml string
    // then no need to send through this path.
    if (rgn.isSerialWanEnabled()) {
      distributeBulkOpToDBSynchronizer(this.qInfo.getRegion(),
          this.qInfo.isDynamic(),
          (GemFireTransaction)lcc.getTransactionExecute(),
          lcc.isSkipListeners());
    }
  }

  @Override
  protected AbstractGemFireResultSet createResultSet(int resultsetNum) throws StandardException {
    return new GemFireUpdateResultSet(this);
  }

  @Override
  protected boolean enableStreaming(LanguageConnectionContext lcc) {
    // don't require any kind of streaming support for updates
    return false;
  }

  @Override
  public void accept(ActivationStatisticsVisitor visitor) {
    visitor.visit(this);
  }
  
  @Override
  protected GfxdResultCollector<Object> getResultCollector(
      final boolean enableStreaming, final AbstractGemFireResultSet rs)
      throws StandardException {
    final GemFireContainer container = (GemFireContainer)
        ((UpdateQueryInfo)this.qInfo).getTargetRegion().getUserAttribute();
    return new GfxdDistributedDeleteResultCollector(container
        .getExtraTableInfo().getReferencedKeyColumns() != null, this.tran);
  }
}

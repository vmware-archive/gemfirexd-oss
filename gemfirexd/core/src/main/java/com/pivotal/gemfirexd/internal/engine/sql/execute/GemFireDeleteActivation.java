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

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.cache.PartitionedRegionDistributionException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DMLQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DynamicKey;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.reflect.GemFireActivationClass;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;

/**
 * Activation object used to execute update as region.put
 * @author Asif
 *
 */
public final class GemFireDeleteActivation extends AbstractGemFireActivation {

  private final GemFireContainer container;

  public GemFireDeleteActivation(ExecPreparedStatement st,
      LanguageConnectionContext _lcc, DMLQueryInfo qi, GemFireActivationClass gc)
      throws StandardException {
    super(st, _lcc, qi);
    this.container = (GemFireContainer)this.qInfo.getRegion()
        .getUserAttribute();
  }

  @Override
  protected AbstractGemFireResultSet createResultSet(int resultsetNum) throws StandardException {
    return new GemFireDeleteResultSet(this);
  }

  @Override
  protected void executeWithResultSet(AbstractGemFireResultSet rs)
      throws StandardException {
    final LanguageConnectionContext lcc = getLanguageConnectionContext();
    final GemFireTransaction tran = (GemFireTransaction)lcc
        .getTransactionExecute();
    final TXStateInterface tx = this.container.getActiveTXState(tran);
    final boolean isTransactional = tx != null;
    //Asif: Obtain the Primary Key. If it is dynamic  then we need to compute now
    //If it is static use it as it is
    Object pk = this.qInfo.getPrimaryKey();
    Object[] gfKeys = null;
    // TODO:Asif: We need to find out a cleaner
    // way to detect bulk get / bulk put conditions
    // TODO [sumedh] Making a clone for TX case since that key can go
    // into TXRegionState map. Can avoid clone if this node is not a
    // datastore.
    if (pk instanceof Object[]) {
      if (qInfo.isWhereClauseDynamic()) {
        Object[] pks = (Object[])pk;
        int len = pks.length;
        gfKeys = new Object[len];
        for (int i = 0; i < len; ++i) {
          if (pks[i] instanceof DynamicKey) {
            DynamicKey dk = (DynamicKey)pks[i];
            gfKeys[i] = dk.getEvaluatedPrimaryKey(this, this.container,
                isTransactional);
          }
        }
      }
      else {
        gfKeys = (Object[])pk;
      }
    }
    else {
      gfKeys = new Object[1];
      if (qInfo.isWhereClauseDynamic()) {
        DynamicKey dk = (DynamicKey)pk;
        pk = dk.getEvaluatedPrimaryKey(this, this.container, isTransactional);
      }
      gfKeys[0] = pk;
    }

    if (observer != null) {
      observer.beforeGemFireResultSetExecuteOnActivation(this);
    }
    //TODO:Neeraj : what happens if the delete is only partially successful ?
    int numRowsModified = 0;
    for (int i = 0; i < gfKeys.length; ++i) {
      try {
        this.container.pkBasedDelete(gfKeys[i], tran, tx, lcc);
        LocalRegion rgn;
        if (isTransactional
            && (rgn = this.container.getRegion()).isSerialWanEnabled()) {
          this.distributeBulkOpToDBSynchronizer(rgn, this.qInfo.isDynamic(),
              tran, this.lcc.isSkipListeners());
        }
        ++numRowsModified;
      } catch (PartitionedRegionDistributionException prde) {
        if (!(prde.getCause() instanceof EntryNotFoundException)) {
          // something bad happened
          throw prde;
        }
        // row did not exist remotely
      } catch (EntryNotFoundException enfe) {
        GemFireXDUtils.checkForInsufficientDataStore(this.container.getRegion());
      } catch (GemFireException gfeex) {
        throw Misc.processGemFireException(gfeex, gfeex, "execution of "
            + this.preStmt.getSource(), true);
      }
    }
    if (observer != null) {
      observer.afterGemFireResultSetExecuteOnActivation(this);
    }
    rs.setNumRowsModified(numRowsModified);
  }

  @Override
  public void accept(ActivationStatisticsVisitor visitor) {
    visitor.visit(this);
  } 
}

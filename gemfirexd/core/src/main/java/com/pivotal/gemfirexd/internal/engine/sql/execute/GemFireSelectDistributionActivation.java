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
import com.gemstone.gemfire.cache.DataPolicy;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DMLQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.InsertQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * 
 * The Activation object which is used to distribute Selects
 * 
 * @author Asif
 */
public final class GemFireSelectDistributionActivation extends
    AbstractGemFireDistributionActivation {

  private final ExecRow projExecRow;

  public GemFireSelectDistributionActivation(ExecPreparedStatement st,
      LanguageConnectionContext _lcc, DMLQueryInfo qi) throws StandardException {
    super(st, _lcc, qi);
    this.projExecRow = ((SelectQueryInfo)qInfo).getProjectionExecRow();
  }

  @Override
  protected void computeNodesForStaticRoutingKeys(
      final Set<Object> staticRoutingKeys) throws StandardException {
    if (this.qInfo.isSelectForUpdateQuery()) {
      // allow for replicated region routing for "SELECT FOR UPDATE" case
      final DataPolicy dataPolicy = this.qInfo.getRegion().getDataPolicy();
      if (dataPolicy.withPartitioning()) {
        this.qInfo.computeNodes(staticRoutingKeys, this, false);
      }
    }
    else {
      super.computeNodesForStaticRoutingKeys(staticRoutingKeys);
    }
  }

  @Override
  protected AbstractGemFireResultSet createResultSet(int resultsetNum) throws StandardException {
    if (qInfo.isInsertAsSubSelect()) {
      InsertQueryInfo.checkSupportedInsertSubSelect((SelectQueryInfo)qInfo);
      String targetTable = qInfo.getTargetTableName();
      return new GemFireUpdateResultSet(this, targetTable);
    }
    return new GemFireDistributedResultSet(this);
  }

  @Override
  public final ExecRow getProjectionExecRow() throws StandardException {
    return this.projExecRow;
  }

  @Override
  public final void resetProjectionExecRow() throws StandardException {
    DataValueDescriptor[] dvds = this.projExecRow.getRowArray();
    for (DataValueDescriptor dvd : dvds) {
      dvd.setToNull();
    }
  }

  @Override
  protected boolean enableStreaming(LanguageConnectionContext lcc) {
    return lcc.streamingEnabled() && !this.preStmt.isSubqueryPrepStatement()
        && !this.qInfo.isSelectForUpdateQuery()
        && !this.qInfo.isInsertAsSubSelect();
  }

  @Override
  public void accept(ActivationStatisticsVisitor visitor) {
    visitor.visit(this);
  }
}

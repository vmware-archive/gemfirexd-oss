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

import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DMLQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.reflect.GemFireActivationClass;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;


/**
 * Executes Select as region.get
 * @author soubhikc
 * @author Asif
 *
 */
public class GemFireSelectActivation extends AbstractGemFireActivation {

  private final SelectQueryInfo selectQI;
  private final ExecRow projExecRow;
  private final boolean forUpdate;

  public GemFireSelectActivation(ExecPreparedStatement st,
      LanguageConnectionContext _lcc, DMLQueryInfo qi,
      GemFireActivationClass gc, boolean forUpdate) throws StandardException {
    super(st, _lcc, qi);
    this.selectQI = (SelectQueryInfo)qi;
    this.projExecRow = this.selectQI.getProjectionExecRow();
    this.forUpdate = forUpdate;
  }

  @Override
  protected void executeWithResultSet(AbstractGemFireResultSet rs)
      throws StandardException {
    // nothing done here now!
    if (observer != null) {
      observer.beforeGemFireResultSetExecuteOnActivation(this);
      observer.afterGemFireResultSetExecuteOnActivation(this);
    }
  }

  @Override
  protected AbstractGemFireResultSet createResultSet(int resultSetNum) throws StandardException {
    final LanguageConnectionContext lcc = getLanguageConnectionContext();
    if (this.getHasQueryHDFS()) {
      // queryHDFS query hint
      return new GemFireResultSet(this, null, resultSetNum, -1, -1, -1, -1, -1,
          -1, 0, this.forUpdate, this.selectQI, lcc.getActiveStats(),
          true /* from GFE Activation */, this.getQueryHDFS());
    }
    else {
      // queryHDFS connection property
      return new GemFireResultSet(this, null, resultSetNum, -1, -1, -1, -1, -1,
          -1, 0, this.forUpdate, this.selectQI, lcc.getActiveStats(),
          true /* from GFE Activation */, lcc.getQueryHDFS());
    }
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
  public void accept(ActivationStatisticsVisitor visitor) {
    visitor.visit(this);
  }
}

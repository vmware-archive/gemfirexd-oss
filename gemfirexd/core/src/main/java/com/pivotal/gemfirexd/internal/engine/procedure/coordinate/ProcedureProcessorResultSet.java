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
package com.pivotal.gemfirexd.internal.engine.procedure.coordinate;

import java.util.List;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.ObjectArrayRow;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.impl.sql.execute.NoPutResultSetImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.PlanUtils;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

/**
 * This class resolves the interface mismatch between the procedure result processor and 
 * NoPutResultSet. 
 * @author yjing
 *
 */
public final class ProcedureProcessorResultSet extends NoPutResultSetImpl {

  ProcedureResultProcessor resultProcessor;
  int parentResultSetNumber;
  
  public ProcedureProcessorResultSet(Activation activation, 
                                     int resultSetNumber,                                    
                                     ProcedureResultProcessor resultProcessor) {
    super(activation, resultSetNumber, (double)0.0,
        (double)0.0);
    this.resultProcessor=resultProcessor;
    this.parentResultSetNumber=resultSetNumber;
   
    // GemStone changes BEGIN
    printResultSetHierarchy();
    // GemStone changes END
  }

  @Override
  public ExecRow getNextRowCore() throws StandardException {
    Object[] row = null;
    try {
      List<Object> rowAsList = this.resultProcessor
          .getNextResultRow(this.parentResultSetNumber);
      if (rowAsList != null) {
        row = rowAsList.toArray();
      }
    } catch (InterruptedException e) {
      throw new AssertionError("the thread is interrupted.");
    }
    if (row == null) {
      return null;
    }
    ExecRow execRow = new ObjectArrayRow(row, null);
    return execRow;
  }

  public void openCore() throws StandardException {
    this.isOpen = true;
  }

  public long getTimeSpent(int type, int timeType) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
  }
  
  @Override
  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_HASHTABLE);
    
    PlanUtils.xmlCloseTag(builder, context, this);
    
    return builder;
  }
  
  @Override
  public void printResultSetHierarchy() {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJ) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NON_COLLOCATED_JOIN,
            "ResultSet Created: " + this.getClass().getSimpleName()
                + " with resultSetNumber=" + resultSetNumber);
      }
    }
  }
}

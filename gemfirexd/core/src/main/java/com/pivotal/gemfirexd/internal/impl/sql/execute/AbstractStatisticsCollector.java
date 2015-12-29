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

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.OutgoingResultSetImpl;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDeleteResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireInsertResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireUpdateResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;

/**
 * Common class of any kind of statistics collection. We need three levels of
 * statistics collections either individually or simultaneously all or some.
 * 
 * so, we chain with another Collector of same kind.
 * 
 * @author soubhikc
 * 
 */
abstract public class AbstractStatisticsCollector implements
    ResultSetStatisticsVisitor {

  // mostly it will be null, if non-null, delegate call apart from self
  // processing.
  protected ResultSetStatisticsVisitor nextCollector;

  // the number of children of the current explained node
  protected int noChildren;
  
  protected String sender;

  AbstractStatisticsCollector(final ResultSetStatisticsVisitor nextCollector) {
    this.nextCollector = nextCollector;
  }

  /**
   * this method only stores the current number of children of the current
   * explained node. The child nodes then can re-use this information.
   */
  public void setNumberOfChildren(final int noChildren) {
    this.noChildren = noChildren;
  }
  
  public int getNumberOfChildren() {
    return noChildren;
  }
  
  public final ResultSetStatisticsVisitor getNextCollector() {
    return nextCollector;
  }

  public final void setNextCollector(final ResultSetStatisticsVisitor collector) {
    nextCollector = collector;
  }
  
  public ResultSetStatisticsVisitor getClone() {
     if(nextCollector != null)
       return nextCollector.getClone();
     
     return null;
  }
  
  public UUID getStatementUUID() {
    
     if(nextCollector != null) {
       return nextCollector.getStatementUUID();
     }
     
     return null;
  }

  final public void visit(final ResultSet rs, final int donotHonor) {
    throw new UnsupportedOperationException("NOT ALLOWED. Override child classes instead. ");
  }

  public void visit(final NoRowsResultSetImpl rs, final int donotHonor) {
    throw new UnsupportedOperationException("NOT ALLOWED. Override child classes instead. ");
  }
  
  public void visit(final BasicNoPutResultSetImpl rs, final int donotHonor) {
    throw new UnsupportedOperationException("NOT ALLOWED. Override child classes instead. ");
  }

  final public void visit(final DMLWriteResultSet rs, final int donotHonor) {
    throw new UnsupportedOperationException("NOT ALLOWED. Override child classes instead. ");
  }

  final public void visit(final DMLVTIResultSet rs, final int donotHonor) {
    throw new UnsupportedOperationException("NOT ALLOWED. Override child classes instead. ");
  }

  final public void visit(final AbstractGemFireResultSet rs, final int donotHonor) {
    throw new UnsupportedOperationException("NOT ALLOWED. Override child classes instead. ");
  }

  final public void visit(final ScanResultSet rs, final int donotHonor) {
    throw new UnsupportedOperationException("NOT ALLOWED. Override child classes instead. ");
  }

  // ==== begin ResultSet Handling ======
  public void visit(final CallStatementResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  abstract public void visit(DeleteResultSet rs, int overridable);

  //don't override this, instead implement (... ,int) version.
  final public void visit(final DeleteResultSet rs) {

    if (rs instanceof DeleteCascadeResultSet) {
      final DeleteCascadeResultSet deleteCascade = (DeleteCascadeResultSet)rs;
      this.visit(deleteCascade);
    }
    else {
      this.visit(rs, 1);
    }
  }

  final public void visit(final DeleteCascadeResultSet rs) {
      //default delegate to parent.
      visit(rs, 1);
  }

  public void visit(final DeleteVTIResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final InsertVTIResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final UpdateVTIResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final OutgoingResultSetImpl rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final GemFireDistributedResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final GemFireResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }
  
  @Override
  public void visit(InsertResultSet rs) {
    
  }

  @Override
  public void visit(GemFireInsertResultSet rs) {
    
  }


  public abstract void visit(GemFireUpdateResultSet rs, int overridable);

  final public void visit(final GemFireUpdateResultSet rs) {
    if (rs instanceof GemFireDeleteResultSet) {
      visit((GemFireDeleteResultSet)rs);
    }
    else {
      this.visit(rs, 1);
    }
  }

  final public void visit(final CurrentOfResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  final public void visit(final DependentResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final HashTableResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  final public void visit(final MaterializedResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final NormalizeResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final ScrollInsensitiveResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  final public void visit(final SetOpResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  public void visit(final SortResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }

  // TABLE SCAN BEGIN ====
  public abstract void visit(TableScanResultSet rs, int overridable);

  final public void visit(final TableScanResultSet rs) {
    if (rs instanceof BulkTableScanResultSet) {
      visit((BulkTableScanResultSet)rs);
    }
    else if (rs instanceof MultiProbeTableScanResultSet) {
      visit((MultiProbeTableScanResultSet)rs);
    }
    else {
      this.visit(rs, 1);
    }
  }

  public void visit(final BulkTableScanResultSet rs) {
    //default delegate to parent.
    visit(rs, 1);
  }

  public void visit(final MultiProbeTableScanResultSet rs) {
    //default delegate to parent.
    visit(rs, 1);
  }

  // TABLE SCAN END ====
  
  // SCALAR AGGREGATION BEGIN ====
  
  public abstract void visit(ScalarAggregateResultSet rs, int overridable);
  
  final public void visit(final ScalarAggregateResultSet rs) {
    if (rs instanceof ScalarAggregateResultSet) {
      visit((ScalarAggregateResultSet)rs, 1);
    }
    else if (rs instanceof DistinctScalarAggregateResultSet) {
      visit((DistinctScalarAggregateResultSet)rs, 2);
    }
    else {
      this.visit(rs, 1);
    }

  }

  public void visit(
      final DistinctScalarAggregateResultSet rs) {
    visit(rs, 2);
  }

  // SCALAR AGGREGATION END ====
  
  // JOIN RESULTSETS BEGIN ====
  
  public abstract void visit(final JoinResultSet rs, int overridable);
    
  final public void visit(final JoinResultSet rs) {
    
    if (rs instanceof NestedLoopJoinResultSet) {
      visit((NestedLoopJoinResultSet)rs);
    }
    else if (rs instanceof NestedLoopLeftOuterJoinResultSet) {
      visit((NestedLoopLeftOuterJoinResultSet)rs);
    }
    else if (rs instanceof HashJoinResultSet) {
      visit((HashJoinResultSet)rs);
    }
    else if (rs instanceof HashLeftOuterJoinResultSet) {
      visit((HashLeftOuterJoinResultSet)rs);
    }
    else if (rs instanceof MergeJoinResultSet) {
      visit((MergeJoinResultSet)rs);
    }
  }

  @Override
  public void visit(
      NestedLoopJoinResultSet rs) {
    visit(rs, 1);
  }
  
  @Override
  public void visit(
      final NestedLoopLeftOuterJoinResultSet rs) {
    visit(rs, 2);
  }

  @Override
  public void visit(
      final HashJoinResultSet rs) {
    visit(rs, 3);
  }

  @Override
  public void visit(
      final HashLeftOuterJoinResultSet rs) {
    visit(rs, 4);
  }

  @Override
  public void visit(
      final MergeJoinResultSet rs) {
    visit(rs, 5);
  }
  
  // JOIN RESULTSETS END ====
  
  // HASH SCAN VARIANTS BEGIN ====
  abstract void visit(final HashScanResultSet rs, int overridable);
  
  public void visit(final HashScanResultSet rs) {
       if(rs instanceof HashScanResultSet) {
         visit((HashScanResultSet)rs, 1);
       }
       else if(rs instanceof DistinctScanResultSet) {
         visit((DistinctScanResultSet)rs);
       }
  }
  
  public void visit(DistinctScanResultSet rs) {
    //default delegate to abstract method.
    visit(rs, 2);
  }

  // HASH SCAN VARIANTS END ====

  final public void visit(final VTIResultSet rs) {
    if (nextCollector != null)
      nextCollector.visit(rs);
  }
  // ==== end ResultSet Handling ======
}

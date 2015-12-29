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

import com.gemstone.gemfire.internal.cache.ObjectEqualsHashingStrategy;
import com.gemstone.gemfire.internal.cache.TXState;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gnu.trove.THashSet;
import com.pivotal.gemfirexd.internal.engine.access.heap.MemHeap;
import com.pivotal.gemfirexd.internal.engine.access.index.MemIndex;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapByteSource;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.TemporaryRowHolder;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

/**
 * This is an index scan for OR predicates that can potentially run across
 * multiple columns in a table. Repeated calls to this result set will fetch
 * rows from underlying scan controllers one by one taking care of eliminating
 * duplicate RowLocations.
 * 
 * @author swale
 * @since 7.0
 */
final class MultiColumnTableScanResultSet extends ScanResultSet implements
    CursorResultSet, Cloneable {

  protected final GeneratedMethod resultRowAllocator;

  protected final TableScanResultSet[] resultSets;

  /**
   * zero-based position of the <b>next</b> ResultSet to lookup w.r.t. the array
   * of ResultSets
   */
  protected int scanIndex;

  protected final THashSet rowLocations;

  protected final GemFireContainer container;

  protected final boolean addRegionAndKey;

  protected final boolean addKeyForSelectForUpdate;

  protected RowLocation currentRL;

  /**
   * Just save off the relevant probing state and pass everything else up to
   * TableScanResultSets in the constructor.
   * 
   * TODO: PERF: try to reuse TableScanResultSets by reopening with new
   * start/stop if they refer to the same conglomerate
   */
  MultiColumnTableScanResultSet(TableScanResultSet[] resultSets,
      StaticCompiledOpenConglomInfo scoci, Activation activation,
      GeneratedMethod resultRowAllocator, int resultSetNumber, int colRefItem,
      int lockMode, boolean tableLocked, int isolationLevel,
      double optimizerEstimatedRowCount, double optimizerEstimatedCost)
      throws StandardException {
    super(activation, resultSetNumber, resultRowAllocator, lockMode,
        tableLocked, isolationLevel, colRefItem, optimizerEstimatedRowCount,
        optimizerEstimatedCost);
    DataValueDescriptor conglom = scoci.getConglom();
    if (conglom instanceof MemHeap) {
      this.container = ((MemHeap)conglom).getGemFireContainer();
    }
    else {
      this.container = ((MemIndex)conglom).getBaseContainer();
    }
    this.resultRowAllocator = resultRowAllocator;

    this.addRegionAndKey = this.activation.isSpecialCaseOuterJoin();
    if (this.activation.getFunctionContext() != null) {
      this.addKeyForSelectForUpdate = this.activation
          .needKeysForSelectForUpdate();
    }
    else {
      this.addKeyForSelectForUpdate = false;
    }

    this.resultSets = resultSets;
    this.rowLocations = new THashSet(ObjectEqualsHashingStrategy.getInstance());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void openCore() throws StandardException {
    this.scanIndex = 0;
    this.isOpen = true;

    // initialize the compact row if required
    if (this.accessedCols != null
        && this.accessedCols.getLength() != this.accessedCols.getNumBitsSet()) {
      this.compactRow = getCompactRow(this.candidate, this.accessedCols, false,
          false, null, null, true);
    }
    else {
      this.compactRow = null;
    }
    // open the first scan
    this.resultSets[0].openCore();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reopenCore() throws StandardException {
    this.scanIndex = 0;
    // reopen the first scan while remaining will be reopened in
    // getNextRowCore() as required
    this.resultSets[0].reopenCore();
    //reopenCore(this.resultSets[0]);
  }

  /*
  protected void reopenCore(TableScanResultSet scan) throws StandardException {
    final SearchSpecification probeValue =
      this.probeValues[this.scanIndex];
    scan.startKeyGetter = probeValue.startKeyGetter;
    scan.startSearchOperator = probeValue.startSearchOperator;
    scan.stopKeyGetter = probeValue.stopKeyGetter;
    scan.stopSearchOperator = probeValue.stopSearchOperator;
    scan.sameStartStopPosition = probeValue.sameStartStopPosition;
    scan.qualifiers = probeValue.qualifiers;
    scan.oneRowScan = probeValue.oneRowScan;
    scan.reopenCore();
  }
  */

  /**
   * {@inheritDoc}
   */
  @Override
  public ExecRow getNextRowCore() throws StandardException {
    if (this.scanIndex >= 0) {
      // get the result from the current scan
      TableScanResultSet scan = this.resultSets[this.scanIndex];
      RowLocation rl;
      for (;;) {
        while (scan.getNextRowCore() != null) {
          // check if the result refers to a unique row
          if ((rl = scan.getRowLocation()) != null) {
            if (this.rowLocations.add(rl)) {
              // TODO: PERF: can avoid creating an ExecRow everytime by
              // filling candidate up with rl (RegionEntryUtils
              // has fillRow method for CERs but not others)
              this.currentRow = rl.getRowWithoutFaultIn(this.container);
              // can be null for a row that concurrently got deleted
              if (this.currentRow != null) {
                @Unretained Object byteSource = this.currentRow.getByteSource();                
                try {
                  if (this.compactRow != null) {
                    //ExecRow originalRow = this.currentRow;
                    // apply the accessedCols
                    for (int i = 1, pos = this.accessedCols.anySetBit();
                        pos != -1; i++, pos = this.accessedCols.anySetBit(pos)) {
                      this.compactRow.setColumn(i,
                          this.currentRow.getColumn(pos + 1));
                    }
                    this.currentRL = rl;
                    return (this.currentRow = this.compactRow);
                  }
                else {
                  this.currentRL = rl;
                  return this.currentRow;
                }
              }finally {
                if(this.container.isOffHeap()) {
                  if (byteSource instanceof OffHeapByteSource) {
                    scan.addByteSource((OffHeapByteSource)byteSource);
                  }
                  else {
                    scan.addByteSource(null);
                  }
                }
              }
              }
            }
          }
        }
        // open the next scan
        this.scanIndex++;
        if (this.scanIndex < this.resultSets.length) {
          scan = this.resultSets[this.scanIndex];
          if (scan.isOpen) {
            scan.reopenCore();
            // reopenCore(scan);
          }
          else {
            scan.openCore();
          }
        }
        else {
          this.scanIndex = -1;
          this.currentRL = null;
          return (this.currentRow = null);
        }
      }
    }
    else {
      this.currentRL = null;
      return (this.currentRow = null);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close(boolean cleanupOnError) throws StandardException {
    /* We'll let TableScanResultSet track the time it takes to close up,
     * so no timing here.
     */
    for (TableScanResultSet scan : this.resultSets) {
      scan.close(cleanupOnError);
    }
    this.scanIndex = -1;
   
    this.currentRow = null;
    this.currentRL = null;
    this.rowLocations.clear();
    super.close(cleanupOnError);
    this.isOpen = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  boolean canGetInstantaneousLocks() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateRowLocationPostRead() throws StandardException {
    if (this.scanIndex >= 0) {
      this.resultSets[this.scanIndex].updateRowLocationPostRead();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void filteredRowLocationPostRead(TXState localTXState)
      throws StandardException {
    if (this.scanIndex >= 0) {
      //Asif: Since the Table Scan Resultset will be having scan controller of type Index & not heap
      // the filtered row location method will not be able to release the offheap resource 
      //( as it would only be able to release the resource via IndexRowToBase, which is not present
      //in this case. So explicitcly calling a method on TableScanResultSet. Not clean but could
      // not think of other solution without major refactoring
      final TableScanResultSet rs = this.resultSets[this.scanIndex];
      rs.filteredRowLocationPostRead(localTXState);
      rs.releaseByteSource(0);    
    } 
  }

  @Override
  public void releasePreviousByteSource()  {
    if (this.scanIndex >= 0) {
      this.resultSets[this.scanIndex].releasePreviousByteSource();
    }    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getTimeSpent(int type, int timeType) {
    long totalTime = 0;
    for (TableScanResultSet scan : this.resultSets) {
      totalTime += scan.getTimeSpent(type, timeType);
    }
    return totalTime;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    // TODO: SW: add separate scan type to stats -- maybe also for MultiProbeSRS
    // visitor.setNumberOfChildren(this.resultSets.length);
    // visitor.visit(this);
    for (TableScanResultSet scan : this.resultSets) {
      scan.accept(visitor);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowLocation getRowLocation() throws StandardException {
    return this.currentRL;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExecRow getCurrentRow() throws StandardException {
    return this.currentRow;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  // methods for use by IndexRowToBaseRowRS
  @Override
  protected final TemporaryRowHolder getFutureForUpdateRows() {
    return this.resultSets[this.scanIndex].getFutureForUpdateRows();
  }

  @Override
  protected final TemporaryRowHolderResultSet getFutureRowResultSet() {
    return this.resultSets[this.scanIndex].getFutureRowResultSet();
  }

  @Override
  protected final void setFutureRowResultSet(
      final TemporaryRowHolderResultSet futureRowResultSet) {
    this.resultSets[this.scanIndex].setFutureRowResultSet(futureRowResultSet);
  }

  @Override
  protected final boolean sourceDrained() {
    return this.resultSets[this.scanIndex].sourceDrained();
  }

  @Override
  public RowLocation fetch(final RowLocation loc, ExecRow destRow,
      FormatableBitSet validColumns, boolean faultIn, GemFireContainer container)
      throws StandardException {
    return this.resultSets[this.scanIndex].fetch(loc, destRow, validColumns,
        faultIn, container);
  }

  public StringBuilder buildQueryPlan(StringBuilder builder, PlanUtils.Context context) {
    
    super.buildQueryPlan(builder, context);
    
    PlanUtils.xmlTermTag(builder, context, PlanUtils.OP_MULTITABLESCAN);
    
    for (TableScanResultSet scan : this.resultSets) {
      scan.buildQueryPlan(builder, context.pushContext());
    }
    
    PlanUtils.xmlCloseTag(builder, context, this);
    
    return builder;
  }
}

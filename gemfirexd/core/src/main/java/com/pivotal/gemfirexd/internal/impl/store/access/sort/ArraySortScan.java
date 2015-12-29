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

package com.pivotal.gemfirexd.internal.impl.store.access.sort;

import com.gemstone.gemfire.internal.util.ArraySortedCollectionWithOverflow;
import com.gemstone.gemfire.internal.util.Enumerator;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.SortObserver;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.ScanControllerRowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * A {@link ScanControllerRowSource} and ScanController for an
 * {@link ArraySorter} which just uses the underlying
 * {@link ArraySortedCollectionWithOverflow} iterator.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public final class ArraySortScan extends Scan implements
    ScanControllerRowSource {

  protected Enumerator sortIterator;
  protected SortObserver sortObserver;
  protected ExecRow currentRow;
  protected final TransactionManager tran;
  protected final boolean hold;

  protected ArraySortScan(ArraySorter sort, TransactionManager tran,
      boolean hold) {
    this.sortIterator = sort.enumerator();
    this.sortObserver = sort.sortObserver;
    this.tran = tran;
    this.hold = hold;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean next() throws StandardException {
    final Enumerator iter = this.sortIterator;
    if (iter != null) {
      try {
        ExecRow row;
        if ((row = (ExecRow)iter.nextElement()) != null) {
          this.currentRow = row;
          return true;
        }
        this.currentRow = null;
        return false;
      } catch (GemFireXDRuntimeException re) {
        if (re.getCause() instanceof StandardException) {
          throw (StandardException)re.getCause();
        }
        else {
          throw re;
        }
      }
    }
    else {
      throw StandardException.newException(SQLState.ALREADY_CLOSED, "SortScan");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final boolean fetchNext(DataValueDescriptor[] row)
      throws StandardException {
    if (next()) {
      fetch(row);
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean fetchNext(ExecRow destRow) throws StandardException {
    if (next()) {
      fetch(destRow);
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void fetch(DataValueDescriptor[] result)
      throws StandardException {
    final ExecRow row = this.currentRow;
    if (row != null) {
      // normally will not be invoked by sorter (the fetchRow version should be
      // invoked) else could optimize by providing a method to copy DVDs from
      // ExecRow directly into result without having to create new DVD objects
      System.arraycopy(row.getRowArray(), 0, result, 0, result.length);
    }
    else if (this.sortIterator != null) {
      throw StandardException.newException(SQLState.SORT_SCAN_NOT_POSITIONED);
    }
    else {
      throw StandardException.newException(SQLState.ALREADY_CLOSED, "SortScan");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fetch(ExecRow destRow) throws StandardException {
    final ExecRow row = this.currentRow;
    if (row != null) {
      final int nCols = destRow.nColumns();
      if (nCols == row.nColumns()) {
        destRow.setRowArray(row);
      }
      else {
        destRow.setColumns(nCols, row);
      }
      destRow.setAllRegionAndKeyInfo(row.getAllRegionAndKeyInfo());
    }
    else if (this.sortIterator != null) {
      throw StandardException.newException(SQLState.SORT_SCAN_NOT_POSITIONED);
    }
    else {
      throw StandardException.newException(SQLState.ALREADY_CLOSED, "SortScan");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExecRow fetchRow(ExecRow destRow) throws StandardException {
    final ExecRow row = this.currentRow;
    if (row != null) {
      return row;
      /*
      final int nCols = destRow.nColumns();
      if (nCols == row.nColumns()) {
        return row;
      }
      else {
        destRow.setColumns(nCols, row);
        destRow.setAllRegionAndKeyInfo(row.getAllRegionAndKeyInfo());
        return destRow;
      }
      */
    }
    else if (this.sortIterator != null) {
      throw StandardException.newException(SQLState.SORT_SCAN_NOT_POSITIONED);
    }
    else {
      throw StandardException.newException(SQLState.ALREADY_CLOSED, "SortScan");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ExecRow getNextRowFromRowSource() throws StandardException {
    final Enumerator iter = this.sortIterator;
    if (iter != null) {
      try {
        ExecRow row;
        if ((row = (ExecRow)iter.nextElement()) != null) {
          return row;
        }
        return null;
      } catch (GemFireXDRuntimeException re) {
        if (re.getCause() instanceof StandardException) {
          throw (StandardException)re.getCause();
        }
        else {
          throw re;
        }
      }
    }
    else {
      throw StandardException.newException(SQLState.ALREADY_CLOSED, "SortScan");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void fetchWithoutQualify(DataValueDescriptor[] result)
      throws StandardException {
    throw StandardException.newException(SQLState.SORT_IMPROPER_SCAN_METHOD);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void fetchWithoutQualify(ExecRow destRow) throws StandardException {
    throw StandardException.newException(SQLState.SORT_IMPROPER_SCAN_METHOD);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    this.sortIterator = null;
    this.sortObserver = null;
    this.currentRow = null;
    this.tran.closeMe(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean closeForEndTransaction(final boolean closeHeldScan)
      throws StandardException {
    if (closeHeldScan || !this.hold) {
      close();
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean needsRowLocation() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean needsToClone() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void rowLocation(RowLocation rl) throws StandardException {
    SanityManager.THROWASSERT("unexpected call to RowSource.rowLocation");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FormatableBitSet getValidColumns() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void closeRowSource() {
    close();
  }
}

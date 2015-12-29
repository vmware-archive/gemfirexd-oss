/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.IndexValueRow

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

import java.util.TreeSet;

import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.Storable;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
	Mapper of ValueRow into ExecIndexRow. 

 */
final class IndexValueRow implements ExecIndexRow {

	private ExecRow valueRow;

	IndexValueRow(final ExecRow valueRow) {
		 this.valueRow = valueRow;
	}

	/*
	 * class interface
	 */
	public String toString() {
		return valueRow.toString();
	}


	/**
		Get the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArray() {
		return valueRow.getRowArray();
	}

	/**	@see ExecRow#getRowArray */
	public void setRowArray(final DataValueDescriptor[] value) 
	{
		valueRow.setRowArray(value);
	}    
  
// GemStone changes BEGIN
  @Override
  public void setRowArray(final ExecRow otherRow) {
    valueRow.setRowArray(otherRow);
  }

  @Override
  public IndexValueRow getShallowClone() {
    return new IndexValueRow(this.valueRow.getShallowClone());
  }

  /**
   * Get the raw value of the row
   */
  @Override
  public Object getRawRowValue(final boolean doClone) {
    return this.valueRow.getRawRowValue(doClone);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setRowArrayClone(final ExecRow otherRow,
      final TreeSet<RegionAndKey> allKeys) {
    this.valueRow.setRowArrayClone(otherRow, allKeys);
  }

// GemStone changes END


	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone() 
	{
		return valueRow.getRowArrayClone();
	}

	// this is the actual current # of columns
	public int nColumns() {
		return valueRow.nColumns();
	}

	/*
	 * Row interface
	 */
	// position is 1-based
	public DataValueDescriptor	getColumn (final int position) throws StandardException {
		return valueRow.getColumn(position);
	}

	// position is 1-based.
	public void setColumn(final int position, final DataValueDescriptor col) {
		valueRow.setColumn(position, col);
	}

  // GemStone changes BEGIN
  /**
   * Set DataValueDescriptors in a Row.
   *
   * @param columns	which columns from values to set, or null if all the values
   *        should be set.
   * @param values a sparse array of the values to set
   */
  @Override
  public void setColumns(final FormatableBitSet columns,
                         final DataValueDescriptor[] values)
  throws StandardException {
    valueRow.setColumns(columns, values);
  }

  /**
   * Set values from a source row. 
   *
   * @param columns	which columns from values to set, or null if all the values
   *        should be set.
   * @param values the source row
   */
  @Override
  public void setColumns(final FormatableBitSet columns,
                         final ExecRow srcRow)
  throws StandardException {
    valueRow.setColumns(columns, srcRow);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCompactColumns(final FormatableBitSet columns,
      final ExecRow srcRow, final int[] baseColumnMap,
      final boolean copyColumns) throws StandardException {
    valueRow.setCompactColumns(columns, srcRow, baseColumnMap, copyColumns);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setColumns(final int[] columns, final boolean zeroBased,
      final ExecRow srcRow) throws StandardException {
    this.valueRow.setColumns(columns, zeroBased, srcRow);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setColumns(int nCols, ExecRow srcRow) throws StandardException {
    this.valueRow.setColumns(nCols, srcRow);
  }

  @Override
  public void addAllKeys(final TreeSet<RegionAndKey> allKeys) {
    this.valueRow.addAllKeys(allKeys);
  }

  @Override
  public void addRegionAndKey(final String regionName, final Object key,
      final boolean isRep) {
    this.valueRow.addRegionAndKey(regionName, key, isRep);
  }

  @Override
  public void addRegionAndKey(final RegionAndKey rak) {
    this.valueRow.addRegionAndKey(rak);
  }

  @Override
  public final void setAllRegionAndKeyInfo(final TreeSet<RegionAndKey> keys) {
    this.valueRow.setAllRegionAndKeyInfo(keys);
  }

  @Override
  public TreeSet<RegionAndKey> getAllRegionAndKeyInfo() {
    return this.valueRow.getAllRegionAndKeyInfo();
  }

  @Override
  public void clearAllRegionAndKeyInfo() {
    this.valueRow.clearAllRegionAndKeyInfo();
  }

// GemStone changes END
  
	// position is 1-based
	public IndexValueRow getClone() {
		return new IndexValueRow(valueRow.getClone());
	}
	/*
	public IndexValueRow getClone(boolean increaseUseCount) {
    return new IndexValueRow(valueRow.getClone(increaseUseCount));
  }*/

	public IndexValueRow getClone(final FormatableBitSet clonedCols) {
		return new IndexValueRow(valueRow.getClone(clonedCols));
	}

	public IndexValueRow getNewNullRow() {
		return new IndexValueRow(valueRow.getNewNullRow());
	}

    /**
     * Reset all columns in the row array to null values.
     */
    public void resetRowArray() {
        valueRow.resetRowArray();
    }

	// position is 1-based
	public DataValueDescriptor cloneColumn(final int columnPosition)
	{
		return valueRow.cloneColumn(columnPosition);
	}

	/*
	 * ExecIndexRow interface
	 */

	public void orderedNulls(final int columnPosition) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("Not expected to be called");
		}
	}

	public boolean areNullsOrdered(final int columnPosition) {
		if (SanityManager.DEBUG) {
			SanityManager.THROWASSERT("Not expected to be called");
		}

		return false;
	}

	/**
	 * Turn the ExecRow into an ExecIndexRow.
	 */
	public void execRowToExecIndexRow(final ExecRow valueRow)
	{
		this.valueRow = valueRow;
	}

	public void getNewObjectArray() 
	{
		valueRow.getNewObjectArray();
	}

  public long estimateRowSize() throws StandardException {
     return valueRow.estimateRowSize();
  }

  @Override
  public final int compare(final ExecRow row, final int colIdx,
      final boolean nullsOrderedLow) throws StandardException {
    assert row instanceof IndexValueRow;
    return valueRow.compare(((IndexValueRow)row).valueRow, colIdx,
        nullsOrderedLow);
  }

  @Override
  public final int compare(final ExecRow row, final int colIdx,
      final long thisOffsetWidth, final boolean nullsOrderedLow)
      throws StandardException {
    return compare(row, colIdx, nullsOrderedLow);
  }

  @Override
  public int computeHashCode(final int position, int hash) {
    return this.valueRow.computeHashCode(position, hash);
  }

  @Override
  public final long isNull(final int colIdx) throws StandardException {
    return this.valueRow.isNull(colIdx);
  }

  @Override
  public void setValue(int columnIndex, final DataValueDescriptor value)
      throws StandardException {
    this.valueRow.setValue(columnIndex, value);
  }

  @Override
  public void setValuesInto(final int[] srcColumns, boolean zeroBased,
      final ExecRow targetRow) throws StandardException {
    this.valueRow.setValuesInto(srcColumns, zeroBased, targetRow);
  }

  @Override
  public DataValueDescriptor getLastColumn() throws StandardException {
    return this.valueRow.getLastColumn();
  }

  @Override
  public Object getBaseByteSource() {
    return this.valueRow.getBaseByteSource();
  }

  @Override
  public Object getByteSource() {
    return this.valueRow.getByteSource();
  }

  @Override
  public void releaseByteSource() {
    this.valueRow.releaseByteSource();
  }

  @Override
  public byte[] getRowBytes(RowFormatter formatter) throws StandardException {
    return this.valueRow.getRowBytes(formatter);
  }

  @Override
  public byte[][] getRowByteArrays(RowFormatter formatter)
      throws StandardException {
    return this.valueRow.getRowByteArrays(formatter);
  }
}

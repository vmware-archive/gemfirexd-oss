/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow

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

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.cache.ClassSize;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

/**
	Basic implementation of ExecRow.

 */
public class ValueRow implements ExecRow
{
	///////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////

	private DataValueDescriptor[] column;
	private final int ncols;

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////


	/**
	  *	Make a value row with a designated number of column slots.
	  *
	  *	@param	ncols	number of columns to allocate
	  */
	public ValueRow(final int ncols)
	{
		 column = new DataValueDescriptor[ncols];
		 this.ncols = ncols;
	}


	///////////////////////////////////////////////////////////////////////
	//
	//	EXECROW INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	// this is the actual current # of columns
	public final int nColumns() {
		return ncols;
	}

	// get a new Object[] for the row
	public void getNewObjectArray()
	{
		column = new DataValueDescriptor[ncols];
	}

	/*
	 * Row interface
	 */
	// position is 1-based
	public final DataValueDescriptor	getColumn (final int position) {
		if (position <= column.length)
			return column[position-1];
		else
			return (DataValueDescriptor)null;
	}

	// position is 1-based.
	public final void setColumn(final int position, final DataValueDescriptor col) {
 
		if (position > column.length)
			realloc(position); // enough for this column
		column[position-1] = col;
	}
  
  // GemStone changes BEGIN
  /** Constructor to wrap a DVD[] */
  public ValueRow(final DataValueDescriptor[] row) {
    this.column = row;
    this.ncols = row.length;
  }

  /**
   * Set DataValueDescriptors in a Row.
   *
   * @param columns	which columns from values to set, or null if all the values
   *        should be set.
   * @param values a sparse array of the values to set
   */
  public final void setColumns(final FormatableBitSet columns,
                               final DataValueDescriptor[] values)
  throws StandardException {    
    for (int i = columns == null ? 0 : columns.anySetBit();
         i != -1 && i < values.length;
         i = columns == null ? i + 1 : columns.anySetBit(i)) {
      setColumn(i + 1, values[i]);
    }
  }

  /**
   * Set values from a source row. 
   *
   * @param columns	which columns from values to set, or null if all the values
   *        should be set.
   * @param srcRow the source row
   */
  @Override
  public final void setColumns(final FormatableBitSet columns,
                               final ExecRow srcRow)
  throws StandardException {
    if (columns == null) {
      for (int i = 0; i < this.ncols; i++) {
        // Keep the receiver's DataValueDescriptors
        // intact if possible
        if (this.column[i] != null) {
          this.column[i].setValue(srcRow.getColumn(i + 1));
        }
        else {
          this.column[i] = srcRow.getColumn(i + 1);
        }
      }
      return;
    }
    for (int i = columns.anySetBit(); i != -1 && i < this.ncols; i = columns
        .anySetBit(i)) {

      // Keep the receiver's DataValueDescriptors
      // intact if possible
      if (this.column[i] != null) {
        this.column[i].setValue(srcRow.getColumn(i + 1));
      }
      else {
        this.column[i] = srcRow.getColumn(i + 1);
      }
    }
  }

  /**
   * {@inhericDoc}
   */
  @Override
  public final void setCompactColumns(final FormatableBitSet columns,
      final ExecRow srcRow, final int[] baseColumnMap,
      final boolean copyColumns) throws StandardException {
    int nSrcCols = srcRow.nColumns();
    if (columns != null) {
      for (int i = columns.anySetBit(), pos = 0; i != -1 && i < nSrcCols
          && pos < this.ncols; i = columns.anySetBit(i), pos++) {

        if (copyColumns) {
          this.column[pos] = srcRow.getColumn(i + 1);
        }
        if (baseColumnMap != null) {
          baseColumnMap[pos] = i;
        }
      }
    }
    else if (copyColumns) {
      if (this.ncols < nSrcCols) {
        nSrcCols = this.ncols;
      }
      for (int i = 0; i < nSrcCols; i++) {
        this.column[i] = srcRow.getColumn(i + 1);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void setColumns(final int[] columns,
                               boolean zeroBased,
                               final ExecRow srcRow)
  throws StandardException {
    if (columns == null) {
      for (int i = 0; i < this.ncols; i++) {
        // Keep the receiver's DataValueDescriptors
        // intact if possible
        if (this.column[i] != null) {
          this.column[i].setValue(srcRow.getColumn(i + 1));
        }
        else {
          this.column[i] = srcRow.getColumn(i + 1);
        }
      }
      return;
    }
    for (int i = 0; i < columns.length; i++) {
      // Keep the receiver's DataValueDescriptors
      // intact if possible
      int col = columns[i];
      if (zeroBased) {
        col++;
      }
      this.column[i] = srcRow.getColumn(col);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void setColumns(int nCols, ExecRow srcRow)
      throws StandardException {
    for (int i = 0; i < nCols; i++) {
      // Keep the receiver's DataValueDescriptors
      // intact if possible
      if (this.column[i] != null) {
        this.column[i].setValue(srcRow.getColumn(i + 1));
      }
      else {
        this.column[i] = srcRow.getColumn(i + 1);
      }
    }
  }

// GemStone changes END

	/*
	** ExecRow interface
	*/

	// position is 1-based
	public ExecRow getClone() 
	{
		return getClone((FormatableBitSet) null);
	}
	
	public ExecRow getClone(boolean increaseUseCount) 
  {
    return getClone((FormatableBitSet) null);
  }

	public ExecRow getClone(final FormatableBitSet clonedCols)
	{
		final int numColumns = column.length;

		/* Get the right type of row */
		final ValueRow rowClone = cloneMe();

		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			// Copy those columns whose bit isn't set (and there is a FormatableBitSet)
			if (clonedCols != null && !(clonedCols.get(colCtr + 1)))
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, (DataValueDescriptor) column[colCtr]);
				continue;
			}

			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, column[colCtr].getClone());
			}
		}
		return rowClone;
	}

	// position is 1-based
	public ExecRow getNewNullRow()
	{
		final int numColumns = column.length;
		final ExecRow rowClone = cloneMe();


		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
				if (column[colCtr] instanceof RowLocation)
				{
					/*
					** The getClone() method for a RowLocation has the same
					** name as for DataValueDescriptor, but it's on a different
					** interface, so the cast must be different.
					**
					*/
					rowClone.setColumn(colCtr + 1, column[colCtr].getClone());
				}
				else
				{
					// otherwise, get a new null
					rowClone.setColumn(colCtr + 1,
						((DataValueDescriptor) (column[colCtr])).getNewNull());
				}
			}
		}
		return rowClone;
	}

	ValueRow cloneMe() {
		return new ValueRow(ncols);
	}

    /**
     * Reset all columns in the row array to null values.
     */
    public final void resetRowArray() {
        for (int i = 0; i < column.length; i++) {
            if (column[i] != null) {
                column[i] = column[i].recycle();
            }
        }
        // Gemstone changes BEGIN
        // even clear the set of keys
    if (this.setOfKeys != null) {
      this.setOfKeys.clear();
    }
        // Gemstone changes END
        
    }

	// position is 1-based
	public final DataValueDescriptor cloneColumn(final int columnPosition)
	{
		return column[columnPosition -1].getClone();
	}

	/*
	 * class interface
	 */
	public String toString() {
		// NOTE: This method is required for external functionality (the
		// consistency checker), so do not put it under SanityManager.DEBUG.
// GemStone changes BEGIN
	  // made efficient using StringBuilder and avoiding toString()
	  // for LOB columns
	  final StringBuilder sb = new StringBuilder();
	  sb.append("{ ");
	  DataValueDescriptor dvd;
	  for (int index = 0; index < this.column.length; index++) {
	    if (index != 0) {
	      sb.append(", ");
	    }
	    dvd = this.column[index];
	    if (dvd != null) {
	      if (RowFormatter.isLob(dvd.getTypeFormatId())) {
	        try {
	          sb.append('(').append(dvd.getTypeName()).append(";length=")
	              .append(dvd.getLength()).append(";hashCode=0x")
	              .append(Integer.toHexString(dvd.hashCode())).append(')');
	        } catch (StandardException se) {
	          throw GemFireXDRuntimeException.newRuntimeException(
	              "unexpected exception in ValueRow.toString", se);
	        }
	      }
	      else {
	        sb.append(dvd.toString());
	      }
	    }
	    else {
	      sb.append("(NULL)");
	    }
	  }
	  sb.append(" }");
	  return sb.toString();
	  /* (original code)
		String s = "{ ";
		for (int i = 0; i < column.length; i++)
		{
			if (column[i] == null)
				s += "null";
			else
				s += column[i].toString();
			if (i < (column.length - 1))
				s += ", ";
		}
		s += " }";
		return s;
	  */
	}


	/**
		Get the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public final DataValueDescriptor[] getRowArray() {
		return column;
	}

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public final DataValueDescriptor[] getRowArrayClone() 
	{
		final int numColumns = column.length;
		final DataValueDescriptor[] columnClones = new DataValueDescriptor[numColumns];

		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			if (column[colCtr] != null)
			{
				columnClones[colCtr] = column[colCtr].getClone();
			}
		}

		return columnClones;
	}

	/**
	 * Set the row array
	 *
	 * @see ExecRow#setRowArray
	 */
	public final void setRowArray(final DataValueDescriptor[] value)
	{
		column = value;
	}
  
// GemStone changes BEGIN
  @Override
  public final void setRowArray(final ExecRow otherRow) {
    setRowArray(otherRow.getRowArray());
  }

  @Override
  public final ExecRow getShallowClone() {
    final ExecRow clone = cloneMe();
    clone.setRowArray(this.column);
    return clone;
  }

  /**
   * Get the raw value of the row
   */
  @Override
  public final Object getRawRowValue(final boolean doClone) {
    if (doClone) {
      return getRowArrayClone();
    }
    return column;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void setRowArrayClone(final ExecRow otherRow,
      final TreeSet<RegionAndKey> allKeys) {
    this.column = otherRow.getRowArrayClone();
    this.setOfKeys = allKeys;
  }

  private TreeSet<RegionAndKey> setOfKeys;

  @Override
  public final void addRegionAndKey(final String regionName, final Object key,
      final boolean isRep) {
    if (this.setOfKeys == null) {
      this.setOfKeys = new TreeSet<RegionAndKey>();
    }
    this.setOfKeys.add(new RegionAndKey(regionName, key, isRep));
  }

  @Override
  public final void addRegionAndKey(final RegionAndKey rak) {
    if (rak != null) {
      if (this.setOfKeys == null) {
        this.setOfKeys = new TreeSet<RegionAndKey>();
      }
      this.setOfKeys.add(rak);
    }
  }

  @Override
  public final void addAllKeys(final TreeSet<RegionAndKey> allKeys) {
    if (allKeys == null) {
      return;
    }
    if (this.setOfKeys == null) {
      this.setOfKeys = new TreeSet<RegionAndKey>();
    }
    this.setOfKeys.addAll(allKeys);
  }

  @Override
  public final void setAllRegionAndKeyInfo(final TreeSet<RegionAndKey> keys) {
    this.setOfKeys = keys;
  }

  @Override
  public final TreeSet<RegionAndKey> getAllRegionAndKeyInfo() {
    return this.setOfKeys;
  }

  @Override
  public final void clearAllRegionAndKeyInfo() {
    this.setOfKeys = null;
  }

// GemStone changes END
		
	// Set the number of columns in the row to ncols, preserving
	// the existing contents.
	protected void realloc(final int ncols) {
		final DataValueDescriptor[] newcol = new DataValueDescriptor[ncols];

		System.arraycopy(column, 0, newcol, 0, column.length);
		column = newcol;
	}


  public long estimateRowSize() throws StandardException {
    long sz = estimateDVDArraySize(column);

    sz += ClassSize.refSize;
    if(setOfKeys != null) {
      try {
        Iterator<RegionAndKey> iterator = setOfKeys.iterator();
        while (iterator.hasNext()) {
          sz += ClassSize.refSize + iterator.next().estimateMemoryUsage();
        }
      } catch (ConcurrentModificationException ignore) {
      } catch (NoSuchElementException ignore) {
      }
    }
    return sz;
  }

  public static long estimateDVDArraySize(DataValueDescriptor[] dvdarr) {
    long sz = ClassSize.estimateArrayOverhead();
    for(int i = dvdarr.length - 1; i >= 0; i--) {
      sz += ClassSize.refSize;
      if (dvdarr[i] != null) {
        sz += dvdarr[i].estimateMemoryUsage();
      }
    }
    return sz;
  }

  @Override
  public final int compare(final ExecRow row, final int colIdx,
      final boolean nullsOrderedLow) throws StandardException {
    //assert ValueRow.class.isInstance(row);
    return column[colIdx - 1].compare(row.getColumn(colIdx),
        nullsOrderedLow);
  }

  @Override
  public final int compare(final ExecRow row, final int colIdx,
      final long thisOffsetWidth, final boolean nullsOrderedLow)
      throws StandardException {
    assert ValueRow.class.isInstance(row);
    return column[colIdx - 1].compare(row.getColumn(colIdx),
        nullsOrderedLow);
  }

  @Override
  public final int computeHashCode(final int position, int hash) {
    final DataValueDescriptor column = this.column[position - 1];
    if (!column.isNull()) {
      // passing a value <= strlen in maxWidth so that *CHAR is treated like
      // variable length column and will evaluate for exactly its length
      return column.computeHashCode(-1, hash);
    }
    else {
      return ResolverUtils.addByteToBucketHash((byte)0, hash,
          column.getTypeFormatId());
    }
  }

  @Override
  public final void setValue(int columnIndex, final DataValueDescriptor value)
      throws StandardException {
    final DataValueDescriptor[] column = this.column;
    final DataValueDescriptor dvd = column[columnIndex];
    if (dvd != null) {
      if (value != null && !value.isNull()) {
        dvd.setValue(value);
      }
      else {
        dvd.restoreToNull();
      }
    }
    else {
      column[columnIndex] = value;
    }
  }

  @Override
  public final void setValuesInto(final int[] srcColumns, boolean zeroBased,
      final ExecRow targetRow) throws StandardException {
    final DataValueDescriptor[] column = this.column;
    final int nsrcCols = srcColumns.length;
    final int ncols = column.length;
    for (int targetIndex = 0; targetIndex < nsrcCols; targetIndex++) {
      int colIndex = srcColumns[targetIndex];
      if (!zeroBased) {
        colIndex--;
      }
      if (colIndex >= 0) {
        if (colIndex < ncols) {
          targetRow.setValue(targetIndex, column[colIndex]);
        }
        else {
          targetRow.setValue(targetIndex, null);
        }
      }
    }
  }

  @Override
  public final DataValueDescriptor getLastColumn() {
    return this.column[this.ncols - 1];
  }

  @Override
  public final long isNull(final int logicalPosition) {
    return column[logicalPosition - 1].isNull()
        ? RowFormatter.OFFSET_AND_WIDTH_IS_NULL : 0;
  }

  @Override
  public final Object getBaseByteSource() {
    return null;
  }

  @Override
  public final Object getByteSource() {
    return null;
  }

  @Override
  public final void releaseByteSource() {
    //No Op
  }

  @Override
  public final byte[] getRowBytes(final RowFormatter formatter)
      throws StandardException {
    if (!formatter.hasLobs()) {
      return formatter.generateBytes(this.column);
    }
    else {
      throw new UnsupportedOperationException(
          "ValueRow does not support byte[] with LOBs");
    }
  }

  @Override
  public final byte[][] getRowByteArrays(final RowFormatter formatter)
      throws StandardException {
    if (formatter.hasLobs()) {
      return formatter.generateByteArrays(this.column);
    }
    else {
      throw new UnsupportedOperationException(
          "ValueRow does not support byte[][] without LOBs");
    }
  }
}

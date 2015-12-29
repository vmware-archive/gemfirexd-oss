/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow

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

package com.pivotal.gemfirexd.internal.iapi.sql.execute;

import java.util.TreeSet;

import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.Row;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * Execution sees this extension of Row that provides connectivity
 * to the Storage row interface and additional methods for manipulating
 * Rows in execution's ResultSets.
 *
 */
public interface ExecRow extends Row {

	/**
	 * Clone the Row and its contents.
	 *
	 *
	 * @return Row	A clone of the Row and its contents.
	 */
	ExecRow getClone();
	
	
	/**
   * Clone the Row and its contents.
   *
   *
   * @return Row  A clone of the Row and its contents.
   
  ExecRow getClone(boolean increaseUseCount);*/

	/**
	 * Clone the Row.  The cloned row will contain clones of the
	 * specified columns and the same object as the original row
	 * for the other columns.
	 *
	 * @param clonedCols	1-based FormatableBitSet representing the columns to clone.
	 *
	 * @return Row	A clone of the Row and its contents.
	 */
	ExecRow getClone(FormatableBitSet clonedCols);

	/**
	 * Get a new row with the same columns type as this one, containing nulls.
	 *
	 */
	ExecRow	getNewNullRow();

    /**
     * Reset all the <code>DataValueDescriptor</code>s in the row array to
     * (SQL) null values. This method may reuse (and therefore modify) the
     * objects currently contained in the row array.
     */
    void resetRowArray();

	/**
	 * Get a clone of a DataValueDescriptor from an ExecRow.
	 *
	 * @param columnPosition (1 based)
	 */
	DataValueDescriptor cloneColumn(int columnPosition);

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone();

	/**
		Return the array of objects that the store needs.
	*/
	public DataValueDescriptor[] getRowArray();

	/**
		Set the array of objects
	*/
	public void setRowArray(DataValueDescriptor[] rowArray);
  
	/**
		Get a new DataValueDescriptor[]
	 */
	public void getNewObjectArray();

// GemStone changes BEGIN
  /**
   * Set the array of objects from another ExecRow that has same format
   */
  public void setRowArray(ExecRow otherRow);

  /**
   * Set the given value at the given 0-based index. This is different from
   * {@link #setColumn(int, DataValueDescriptor)} in that this will set the
   * value in existing DVD, if present, instead of assigning by reference.
   * 
   * @param columnIndex
   *          0-based index of the column
   * @param value
   *          the column value to set in this row
   */
  public void setValue(int columnIndex, DataValueDescriptor value)
      throws StandardException;

  /**
   * An optimized version of <code>setColumn/setColumns</code> methods for
   * ValueRow that will allow a compact ExecRow to directly set the column value
   * into the ValueRow instead of having to create an intermediate DVD.
   */
  public void setValuesInto(int[] srcColumns, boolean zeroBased,
      ExecRow targetRow) throws StandardException;

  /**
   * Get a shallow clone of just the shell without cloning the underlying data.
   */
  public ExecRow getShallowClone();

  /**
   * Get the raw value of the row. clone it (e.g. for non-COW implementations)
   * whenever row object is getting re-used.
   * 
   * @param doClone
   *          false will return raw data in all implementations, true is
   *          honoured only by ValueRow to return a clone.
   */
  public Object getRawRowValue(boolean doClone);

  /**
   * Set the array of objects from row cloning its data, if required, and the
   * set of {@link RegionAndKey}s for special case outer join.
   */
  public void setRowArrayClone(ExecRow otherRow, TreeSet<RegionAndKey> allKeys);

  /**
   * Returns in bytes the estimated row size. For CompactRow variants it will
   * return the length of the byte array(s) and ValueRow variants will use
   * DVD.estimateRowSize() to estimate bytes used.
   * 
   * @return bytes used for the row
   * @throws StandardException 
   */
  public long estimateRowSize() throws StandardException;

  public void addRegionAndKey(String regionName, Object key,
      boolean isReplicated);
  public void addRegionAndKey(RegionAndKey rak);
  public void addAllKeys(TreeSet<RegionAndKey> allKeys);
  public void setAllRegionAndKeyInfo(final TreeSet<RegionAndKey> keys);
  public TreeSet<RegionAndKey> getAllRegionAndKeyInfo();
  public void clearAllRegionAndKeyInfo();

  /**
   * Returns whether particular column is null or not.
   * 
   * @param logicalPosition 1-based column position
   * 
   * @return returns column's begin offset & column width. must be compared with
   *         {@link RowFormatter#OFFSET_AND_WIDTH_IS_NULL} for null indication.
   * @throws StandardException
   */
  public long isNull(int logicalPosition) throws StandardException;

  /**
   * Get the last column in the row (used when last column is a RowLocation).
   */
  public DataValueDescriptor getLastColumn() throws StandardException;

  /**
   * compares the incoming row with this on a particular column. Caller must
   * ensure incoming <code><b>row</b></code> is of the same type as
   * <code><b>this</b></code>.
   * 
   * @param row
   *          variants of ExecRow.
   * @param colIdx
   *          column index to be compared. 1 based logical position in column
   *          descriptor list.
   * @param nullsOrderedLow
   * @return as per compareTo semantics.
   * @throws StandardException
   */
  public int compare(ExecRow row, int colIdx, boolean nullsOrderedLow)
      throws StandardException;

  /**
   * Override of {@link #compare(ExecRow, int, boolean)} with this rows begin
   * offset and column width. typically follow up call from isNull.
   * 
   * @param row
   *          variants of ExecRow.
   * @param colIdx
   *          1 based column index to cdl.
   * @return according to compareTo return values.
   * @throws StandardException
   */
  public int compare(ExecRow row, int colIdx, long thisOffsetWidth,
      boolean nullsOrderedLow) throws StandardException;

  /**
   * Compute the hash of a given column (1-based). Avoids deserializing the
   * column if this is a serialized row.
   * 
   * @param position
   *          The ordinal position of the column.
   * @param hash
   *          The hash value into which the hash of given column has to be
   *          accumulated
   * 
   * @exception StandardException
   *              Thrown on failure.
   * 
   * @return The new hash after accumulating the hash of given column into
   *         <code>hash</code> argument
   */
  public int computeHashCode(int position, int hash);

  /** the base raw object */
  public Object getBaseByteSource();

  /** the base raw first array of the object */
  @Unretained public Object getByteSource();

  @Released public void releaseByteSource();

  public byte[] getRowBytes(RowFormatter formatter) throws StandardException;
  public byte[][] getRowByteArrays(RowFormatter formatter)
      throws StandardException;
// GemStone changes END
}

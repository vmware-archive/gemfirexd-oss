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

package com.pivotal.gemfirexd.internal.engine.procedure;

import java.util.TreeSet;

import com.pivotal.gemfirexd.internal.engine.distributed.metadata.RegionAndKey;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.cache.ClassSize;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.UserType;

public class ObjectArrayRow implements ExecRow {

  Object[] row;

  DataTypeDescriptor[] dtds;

  public ObjectArrayRow(Object[] row, DataTypeDescriptor[] dtds) {
    this.row = row;
    this.dtds = dtds;
  }

  public DataValueDescriptor cloneColumn(int columnPosition) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public ExecRow getClone() {
    throw new UnsupportedOperationException("Unexpected invocation");
  }
  
  public ExecRow getClone(boolean increaseUseCount) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public ObjectArrayRow getShallowClone() {
    return new ObjectArrayRow(this.row, this.dtds);
  }

  public ExecRow getClone(FormatableBitSet clonedCols) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public ExecRow getNewNullRow() {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void getNewObjectArray() {
    throw new UnsupportedOperationException("Unexpected invocation");

  }

  public DataValueDescriptor[] getRowArray() {
    DataValueDescriptor[] values = new DataValueDescriptor[this.row.length];
    for (int index = 0; index < this.row.length; ++index) {
      values[index] = (new UserType(this.row[index]));
    }
    return values;
  }

  public DataValueDescriptor[] getRowArrayClone() {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void resetRowArray() {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void setRowArray(DataValueDescriptor[] rowArray) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void setRowArray(ExecRow otherRow) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public DataValueDescriptor getColumn(int position) throws StandardException {
    int index = position - 1;
    // DataTypeDescriptor dtd = this.dtds[index];
    // DataValueDescriptor dvd = dtd.getNull();
    // ((UserType)dvd).setValue(this.row[index]);
    // return dvd;
    return (new UserType(this.row[index]));
  }

  /**
   * Get the raw value of the row
   */
  public Object getRawRowValue(boolean doClone) {
    return this.row;
  }

  /**
   * {@inheritDoc}
   */
  public void setRowArrayClone(final ExecRow otherRow,
      final TreeSet<RegionAndKey> allKeys) {
    this.row = (Object[])otherRow.getRawRowValue(true);
  }

  public int nColumns() {
    return this.row.length;
  }

  public void setColumn(int position, DataValueDescriptor value) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void setColumns(FormatableBitSet columns, DataValueDescriptor[] values)
      throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void setColumns(FormatableBitSet columns, ExecRow srcRow)
      throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void setCompactColumns(FormatableBitSet columns, ExecRow srcRow,
      int[] baseColumnMap, boolean copyColumns) throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void setColumns(int[] columns, boolean zeroBased, ExecRow srcRow)
      throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  public void setColumns(int nCols, ExecRow srcRow) throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  /* sb: As type is unknown (user provided) leaving onto the best 
   * guess possible.
   * 
   * @see com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow#estimateRowSize()
   */
  public long estimateRowSize() {
    long sz = ClassSize.estimateArrayOverhead();
    for (int i = row.length - 1; i >= 0; i--) {
      sz += ClassSize.estimateBaseFromCatalog(row[i].getClass());
    }
    return sz;
  }

  public void addAllKeys(TreeSet<RegionAndKey> allKeys) {
  }

  public void addRegionAndKey(String regionName, Object key, boolean isRep) {
  }

  public void addRegionAndKey(RegionAndKey rak) {
  }

  public TreeSet<RegionAndKey> getAllRegionAndKeyInfo() {
    return null;
  }

  public void setAllRegionAndKeyInfo(TreeSet<RegionAndKey> keys) {
  }

  public void clearAllRegionAndKeyInfo() {
  }

  @Override
  public int compare(ExecRow row, int colIdx, boolean nullsOrderedLow) throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  @Override
  public int compare(final ExecRow row, final int colIdx,
      final long thisOffsetWidth, final boolean nullsOrderedLow)
      throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  @Override
  public int computeHashCode(int position, int hash) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  @Override
  public long isNull(int colIdx) throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  @Override
  public void setValue(int columnIndex, final DataValueDescriptor value)
      throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  @Override
  public void setValuesInto(final int[] srcColumns, boolean zeroBased,
      final ExecRow targetRow) throws StandardException {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  @Override
  public final DataValueDescriptor getLastColumn() throws StandardException {
    return new UserType(this.row[this.row.length - 1]);
  }

  @Override
  public Object getBaseByteSource() {
    return null;
  }

  @Override
  public Object getByteSource() {
    return null;
  }

  @Override
  public void releaseByteSource() {
    //No Op
  }

  @Override
  public byte[] getRowBytes(RowFormatter formatter) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }

  @Override
  public byte[][] getRowByteArrays(RowFormatter formatter) {
    throw new UnsupportedOperationException("Unexpected invocation");
  }
}

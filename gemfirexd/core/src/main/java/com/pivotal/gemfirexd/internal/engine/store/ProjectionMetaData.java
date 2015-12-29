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

package com.pivotal.gemfirexd.internal.engine.store;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.gemstone.gnu.trove.TIntIntHashMap;
import com.pivotal.gemfirexd.callbacks.TableMetaData;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;

/**
 * Encapsulates a full {@link ResultSetMetaData} and a set of projection columns
 * on it to provide the metadata of the projected columns.
 * 
 * @author swale
 * @since 7.5
 */
public final class ProjectionMetaData implements TableMetaData {

  private final TableMetaData metadata;

  //below, if used, should be made 0-based rather than 1-based as assumed
  //private final FormatableBitSet projectionSet;
  private final int[] projection;

  private TIntIntHashMap tableColumnToColumn;

  private final int numColumns;

  /*
  // cache the previous column to check against current and start from that
  // position to optimize for the common use-case of iterating all the columns
  private int prevColumn;
  private int prevColumnActual;

  public ProjectionMetaData(final TableMetaData fullMetaData,
      final FormatableBitSet projectionColumns) {
    this.metadata = fullMetaData;
    this.projectionSet = projectionColumns;
    this.projection = null;
    this.numColumns = projectionColumns.getNumBitsSet();
  }
  */

  public ProjectionMetaData(final TableMetaData fullMetaData,
      final int[] projectionColumns) {
    this.metadata = fullMetaData;
    //this.projectionSet = null;
    this.projection = projectionColumns;
    this.numColumns = projectionColumns.length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumnCount() throws SQLException {
    return this.numColumns;
  }

  /**
   * {@inheritDoc}
   */
  public final int getTableColumnPosition(int column) throws SQLException {
    if (column > 0 && column <= this.numColumns) {
      /*
      if (this.projectionSet != null) {
        if (column == (this.prevColumn + 1)) {
          this.prevColumn++;
          return (this.prevColumnActual = this.projectionSet
              .anySetBit(this.prevColumnActual));
        }
        else {
          this.prevColumn = column;
          int col = this.projectionSet.anySetBit();
          while (--column >= 1) {
            col = this.projectionSet.anySetBit(col);
          }
          return (this.prevColumnActual = col);
        }
      }
      else {
      */
      return this.projection[column - 1];
    }
    else {
      throw Util.generateCsSQLException(SQLState.COLUMN_NOT_FOUND,
          Integer.valueOf(column));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumnPosition(String columnName) throws SQLException {
    final int tablePosition = this.metadata.getColumnPosition(columnName);
    if (this.tableColumnToColumn != null) {
      return this.tableColumnToColumn.get(tablePosition);
    }
    int pos = 1;
    int retPos = 0;
    /*
    if (this.projectionSet != null) {
      for (int tablePos = this.projectionSet.anySetBit(); tablePos != -1;
          tablePos = this.projectionSet.anySetBit(tablePos), pos++) {
        if (tablePos == tablePosition) {
          return pos;
        }
      }
    }
    else {
    */
    this.tableColumnToColumn = new TIntIntHashMap(this.numColumns);
    for (int tablePos : this.projection) {
      if (tablePos == tablePosition) {
        retPos = pos;
      }
      this.tableColumnToColumn.put(tablePosition, pos);
      pos++;
    }
    if (retPos > 0) {
      return retPos;
    }
    throw Util.generateCsSQLException(SQLState.LANG_COLUMN_NOT_FOUND,
        columnName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getDeclaredColumnWidth(int column) throws SQLException {
    return this.metadata.getDeclaredColumnWidth(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getSchemaVersion() {
    return this.metadata.getSchemaVersion();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return this.metadata.isAutoIncrement(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return this.metadata.isCaseSensitive(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    return this.metadata.isSearchable(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    return this.metadata.isCurrency(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int isNullable(int column) throws SQLException {
    return this.metadata.isNullable(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSigned(int column) throws SQLException {
    return this.metadata.isSigned(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return this.metadata.getColumnDisplaySize(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    return this.metadata.getColumnLabel(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    return this.metadata.getColumnName(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    return this.metadata.getSchemaName(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    return this.metadata.getPrecision(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getScale(int column) throws SQLException {
    return this.metadata.getScale(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getTableName(int column) throws SQLException {
    return this.metadata.getTableName(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    return this.metadata.getCatalogName(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    return this.metadata.getColumnType(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return this.metadata.getColumnTypeName(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return this.metadata.isReadOnly(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    return this.metadata.isWritable(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return this.metadata.isDefinitelyWritable(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnClassName(int column) throws SQLException {
    return this.metadata.getColumnClassName(getTableColumnPosition(column));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.metadata.unwrap(iface);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.metadata.isWrapperFor(iface);
  }
}

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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;

/**
 * Base class for {@link ResultSet}s that encapsulate a set of one or more rows
 * that cannot be updated.
 * 
 * @author swale
 * @since 7.0
 */
public abstract class NonUpdatableRowsResultSet implements ResultSet,
    ResultWasNull {

  protected boolean wasNull;

  /**
   * {@inheritDoc}
   */
  public final void setWasNull() {
    this.wasNull = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final boolean wasNull() throws SQLException {
    return this.wasNull;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
      throws SQLException {
    return getBigDecimal(findColumn(columnLabel), scale);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAsciiStream(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getUnicodeStream(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException {
    return getObject(findColumn(columnLabel), map);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return getRef(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return getBlob(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return getClob(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return getArray(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return getDate(findColumn(columnLabel), cal);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getTime(findColumn(columnLabel), cal);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
      throws SQLException {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return getURL(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return getRowId(findColumn(columnLabel));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clearWarnings() throws SQLException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCursorName() throws SQLException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAfterLast() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isFirst() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLast() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void beforeFirst() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void afterLast() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean first() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean last() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean absolute(int row) throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean relative(int rows) throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean previous() throws SQLException {
    throw noCurrentRowException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getRow() throws SQLException {
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setFetchDirection(int direction) throws SQLException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getFetchDirection() throws SQLException {
    return FETCH_UNKNOWN;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getType() throws SQLException {
    return TYPE_FORWARD_ONLY;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getConcurrency() throws SQLException {
    return CONCUR_READ_ONLY;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void insertRow() throws SQLException {
    noUpdateOperationException("insertRow");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateRow() throws SQLException {
    noUpdateOperationException("updateRow");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteRow() throws SQLException {
    noUpdateOperationException("deleteRow");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void refreshRow() throws SQLException {
    noUpdateOperationException("refreshRow");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cancelRowUpdates() throws SQLException {
    noUpdateOperationException("cancelRowUpdates");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void moveToInsertRow() throws SQLException {
    noUpdateOperationException("moveToInsertRow");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void moveToCurrentRow() throws SQLException {
    noUpdateOperationException("moveToCurrentRow");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException {
    if (map != null && map.isEmpty()) {
      return getObject(columnIndex);
    }
    else {
      throw Util.notImplemented();
    }
  }

  public Array getArray(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public URL getURL(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public Ref getRef(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    throw Util.notImplemented();
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw Util.notImplemented();
  }

  public String getNString(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public String getNString(String columnLabel) throws SQLException {
    throw Util.notImplemented();
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw Util.notImplemented();
  }

  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw Util.notImplemented();
  }

  public int getHoldability() throws SQLException {
    return HOLD_CURSORS_OVER_COMMIT;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateNull(int columnIndex) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateNull(String columnLabel) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
      int length) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    noUpdateException(columnLabel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    noUpdateException(columnIndex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateNString(int columnIndex, String nString)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateNString(String columnLabel, String nString)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateNCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateBlob(String columnLabel, InputStream inputStream,
      long length) throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateBlob(int columnIndex, InputStream inputStream)
      throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    noUpdateException(columnLabel);
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    noUpdateException(columnIndex);
  }

  public void updateNClob(String columnLabel, Reader reader)
      throws SQLException {
    noUpdateException(columnLabel);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(this);
    } catch (ClassCastException cce) {
      throw Util.generateCsSQLException(SQLState.UNABLE_TO_UNWRAP, iface);
    }
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return type.cast(getObject(columnIndex));
  }

  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return type.cast(getObject(columnLabel));
  }

  protected final void noUpdateException(int columnIndex) throws SQLException {
    noUpdateOperationException("update of column "
        + getMetaData().getColumnName(columnIndex));
  }

  protected final void noUpdateException(String columnLabel)
      throws SQLException {
    noUpdateOperationException("update of column " + columnLabel);
  }

  protected final void noUpdateOperationException(String operation)
      throws SQLException {
    throw Util.generateCsSQLException(
        SQLState.UPDATABLE_RESULTSET_API_DISALLOWED, operation);
  }

  protected final StandardException noCurrentRow() {
    return StandardException.newException(SQLState.NO_CURRENT_ROW);
  }

  protected final SQLException noCurrentRowException() {
    return Util.generateCsSQLException(SQLState.NO_CURRENT_ROW);
  }

  protected final SQLException invalidColumnException(int columnIndex) {
    return Util.generateCsSQLException(SQLState.COLUMN_NOT_FOUND,
        Integer.valueOf(columnIndex));
  }

  protected final SQLException outOfRangeException(final String type,
      int columnIndex) {
    return Util.generateCsSQLException(
        SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, type, columnIndex);
  }

  protected final SQLException dataConversionException(String type,
      String targetType, int columnIndex) {
    return Util.generateCsSQLException(SQLState.LANG_DATA_TYPE_GET_MISMATCH,
        type, targetType, columnIndex);
  }

  protected final SQLException columnNotFoundException(String columnLabel) {
    return Util.generateCsSQLException(SQLState.LANG_COLUMN_NOT_FOUND,
        columnLabel);
  }
}

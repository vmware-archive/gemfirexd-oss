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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import com.pivotal.gemfirexd.callbacks.Event;

/**
 * Encapsulates a single column long value as a {@link ResultSet} for
 * {@link Event#getPrimaryKeysAsResultSet()}.
 * 
 * @author swale
 * @since 7.0
 */
public final class SingleColumnLongResultSet extends NonUpdatableRowsResultSet {

  private final long v;

  public SingleColumnLongResultSet(final long v) {
    this.v = v;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean next() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws SQLException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getString(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      return Long.toString(this.v);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      return this.v != 0;
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte getByte(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      if (this.v >= Byte.MIN_VALUE && this.v <= Byte.MAX_VALUE) {
        return (byte)this.v;
      }
      else {
        throw outOfRangeException("BIGINT", columnIndex);
      }
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public short getShort(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      if (this.v >= Short.MIN_VALUE && this.v <= Short.MAX_VALUE) {
        return (short)this.v;
      }
      else {
        throw outOfRangeException("BIGINT", columnIndex);
      }
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getInt(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      if (this.v >= Integer.MIN_VALUE && this.v <= Integer.MAX_VALUE) {
        return (int)this.v;
      }
      else {
        throw outOfRangeException("BIGINT", columnIndex);
      }
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLong(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      return this.v;
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getFloat(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      return this.v;
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double getDouble(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      return this.v;
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "BigDecimal", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "byte[]", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Date getDate(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Date", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Time getTime(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Time", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Timestamp", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "InputStream(ASCII)", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "InputStream(Unicode)",
          columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "InputStream(BINARY)", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "InputStream(CHAR)", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "BigDecimal", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Blob", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Clob", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Date", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Time", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    if (columnIndex == 1) {
      throw dataConversionException("long", "Timestamp", columnIndex);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getObject(int columnIndex) throws SQLException {
    if (columnIndex == 1) {
      return Long.valueOf(this.v);
    }
    else {
      throw invalidColumnException(columnIndex);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    throw columnNotFoundException(columnLabel);
  }
}

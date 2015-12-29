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
package com.pivotal.gemfirexd.internal.engine.hadoop.mapreduce;

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

import com.pivotal.gemfirexd.internal.engine.store.NonUpdatableRowsResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;

/**
 * A proxy for result set created by gemfirexd loner. The returned
 * {@link ResultSet} is already positioned at the returned single row with
 * {@link ResultSet#next()} always returning false, whose column values can be
 * retrieved using the ResultSet getter methods. Attempts to updated column
 * values will result in exceptions
 * 
 * @author ashvina
 */
public final class NoIterNoUpdateResultSetProxy extends NonUpdatableRowsResultSet {

  private final EmbedResultSet rs;

  public NoIterNoUpdateResultSetProxy(EmbedResultSet rs) {
    this.rs = rs;
  }

  @Override
  public boolean next() throws SQLException {
    // this result set instance is not navigable
    return false;
  }

  @Override
  public void close() throws SQLException {
    // close will be managed by input format. do nothing here
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return rs.getString(columnIndex);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return rs.getBoolean(columnIndex);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return rs.getByte(columnIndex);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return rs.getShort(columnIndex);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return rs.getInt(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return rs.getLong(columnIndex);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return rs.getFloat(columnIndex);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return rs.getDouble(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    return rs.getBigDecimal(columnIndex);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return rs.getBytes(columnIndex);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return rs.getDate(columnIndex);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return rs.getTime(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return rs.getTimestamp(columnIndex);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return rs.getAsciiStream(columnIndex);
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return rs.getUnicodeStream(columnIndex);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return rs.getBinaryStream(columnIndex);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return rs.getMetaData();
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return rs.getObject(columnIndex);
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return rs.findColumn(columnLabel);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return rs.getCharacterStream(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return rs.getBigDecimal(columnIndex);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return rs.getBlob(columnIndex);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return rs.getClob(columnIndex);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return rs.getDate(columnIndex, cal);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return rs.getTime(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    return rs.getTimestamp(columnIndex, cal);
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }
}

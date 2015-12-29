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

package com.pivotal.gemfirexd.thrift.internal;

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
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.thrift.BlobChunk;
import com.pivotal.gemfirexd.thrift.ClobChunk;
import com.pivotal.gemfirexd.thrift.GFXDException;
import com.pivotal.gemfirexd.thrift.GFXDType;
import com.pivotal.gemfirexd.thrift.Row;
import com.pivotal.gemfirexd.thrift.RowSet;
import com.pivotal.gemfirexd.thrift.gfxdConstants;
import com.pivotal.gemfirexd.thrift.common.ColumnValueConverter;
import com.pivotal.gemfirexd.thrift.common.Converters;
import com.pivotal.gemfirexd.thrift.common.LobService;
import com.pivotal.gemfirexd.thrift.common.ThriftExceptionUtil;

/**
 * Common base class to fetch various types of column values from a Row like
 * those in {@link ResultSet}.
 * 
 * @author swale
 * @since gfxd 1.1
 */
abstract class ClientFetchColumnValue implements LobService {

  protected final ClientService service;
  protected ClientFinalizer finalizer;
  protected boolean wasNull;

  protected ClientFetchColumnValue(ClientService service, byte entityId) {
    this.service = service;
    this.finalizer = entityId != gfxdConstants.INVALID_ID ? new ClientFinalizer(
        this, service, entityId) : null;
  }

  protected final HostConnection getLobSource(boolean throwOnFailure,
      String op) throws SQLException {
    final ClientFinalizer finalizer = this.finalizer;
    final HostConnection source;
    if (finalizer != null && (source = finalizer.source) != null) {
      return source;
    }
    else if (throwOnFailure) {
      throw (SQLException)service.newExceptionForNodeFailure(null, op,
          service.isolationLevel, null, false);
    }
    else {
      return null;
    }
  }

  protected final void setCurrentSource(byte entityId, int newId, RowSet rs) {
    ClientFinalizer finalizer = this.finalizer;
    if (newId != gfxdConstants.INVALID_ID) {
      final HostConnection currentSource = service.getCurrentHostConnection();
      HostConnection newSource = currentSource;
      if (rs != null && (currentSource.connId != rs.connId
          || currentSource.token != rs.token
          || currentSource.hostAddr != rs.source)) {
        newSource = new HostConnection(rs.source, rs.connId, rs.token);
      }
      if (finalizer == null) {
        // create a new finalizer
        this.finalizer = finalizer = new ClientFinalizer(this, this.service,
            entityId);
      }
      finalizer.updateReferentData(newId, newSource);
    }
    else if (finalizer != null) {
      // clear the finalizer
      finalizer.clearAll();
      this.finalizer = null;
    }
  }

  protected final void clearFinalizer() {
    final ClientFinalizer finalizer = this.finalizer;
    if (finalizer != null) {
      finalizer.clearAll();
      this.finalizer = null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Blob createBlob(BlobChunk firstChunk) throws SQLException {
    return new ClientBlob(firstChunk, service, getLobSource(true,
        "createBlob"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Clob createClob(ClobChunk firstChunk) throws SQLException {
    return new ClientClob(firstChunk, service, getLobSource(true,
        "createClob"));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final BlobChunk getBlobChunk(int lobId, long offset, int chunkSize,
      boolean freeLobAtEnd) throws SQLException {
    try {
      return service.getBlobChunk(getLobSource(true, "getBlobChunk"), lobId,
          offset, chunkSize, freeLobAtEnd);
    } catch (GFXDException gfxde) {
      throw ThriftExceptionUtil.newSQLException(gfxde);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final ClobChunk getClobChunk(int lobId, long offset, int chunkSize,
      boolean freeLobAtEnd) throws SQLException {
    try {
      return service.getClobChunk(getLobSource(true, "getClobChunk"), lobId,
          offset, chunkSize, freeLobAtEnd);
    } catch (GFXDException gfxde) {
      throw ThriftExceptionUtil.newSQLException(gfxde);
    }
  }

  protected void reset() {
    this.wasNull = false;
  }

  protected final String getString(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "String");
    String str = cvc.toString(row, columnIndex, this);
    this.wasNull = (str == null);
    return str;
  }

  protected final boolean getBoolean(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "boolean");
    boolean v = cvc.toBoolean(row, columnIndex);
    this.wasNull = cvc.isNull();
    return v;
  }

  protected final byte getByte(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "byte");
    byte v = cvc.toByte(row, columnIndex);
    this.wasNull = cvc.isNull();
    return v;
  }

  protected final short getShort(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "short");
    short v = cvc.toShort(row, columnIndex);
    this.wasNull = cvc.isNull();
    return v;
  }

  protected final int getInt(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "int");
    int v = cvc.toInteger(row, columnIndex);
    this.wasNull = cvc.isNull();
    return v;
  }

  protected final long getLong(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "long");
    long v = cvc.toLong(row, columnIndex);
    this.wasNull = cvc.isNull();
    return v;
  }

  protected final float getFloat(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "float");
    float v = cvc.toFloat(row, columnIndex);
    this.wasNull = cvc.isNull();
    return v;
  }

  protected final double getDouble(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "doube");
    double v = cvc.toDouble(row, columnIndex);
    this.wasNull = cvc.isNull();
    return v;
  }

  protected final BigDecimal getBigDecimal(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "BigDecimal");
    BigDecimal v = cvc.toBigDecimal(row, columnIndex);
    this.wasNull = (v == null);
    return v;
  }

  protected final BigDecimal getBigDecimal(final int columnIndex,
      final int scale, final GFXDType gfxdType, final Row row)
      throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "BigDecimal");
    BigDecimal v = cvc.toBigDecimal(row, columnIndex);
    if (v != null) {
      // rounding as per server side EmbedResultSet20
      v.setScale(scale, BigDecimal.ROUND_HALF_DOWN);
      this.wasNull = false;
      return v;
    }
    else {
      return null;
    }
  }

  protected final byte[] getBytes(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "byte[]");
    byte[] v = cvc.toBytes(row, columnIndex, this);
    this.wasNull = (v == null);
    return v;
  }

  protected final Date getDate(final int columnIndex, final Calendar cal,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "Date");
    Date v = cvc.toDate(row, columnIndex, cal);
    this.wasNull = (v == null);
    return v;
  }

  protected final Time getTime(final int columnIndex, final Calendar cal,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "Time");
    Time v = cvc.toTime(row, columnIndex, cal);
    this.wasNull = (v == null);
    return v;
  }

  protected final Timestamp getTimestamp(final int columnIndex,
      final Calendar cal, final GFXDType gfxdType, final Row row)
      throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "Timestamp");
    Timestamp v = cvc.toTimestamp(row, columnIndex, cal);
    this.wasNull = (v == null);
    return v;
  }

  protected final Object getObject(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "Object");
    Object v = cvc.toObject(row, columnIndex, this);
    this.wasNull = (v == null);
    return v;
  }

  protected final Blob getBlob(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "Blob");
    Blob v = cvc.toBlob(row, columnIndex, this);
    this.wasNull = (v == null);
    return v;
  }

  protected final InputStream getBinaryStream(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters
        .getConverter(gfxdType, "BinaryStream");
    InputStream v = cvc.toBinaryStream(row, columnIndex, this);
    this.wasNull = (v == null);
    return v;
  }

  protected final Clob getClob(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "Clob");
    Clob v = cvc.toClob(row, columnIndex, this);
    this.wasNull = (v == null);
    return v;
  }

  protected final Reader getCharacterStream(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType,
        "CharacterStream");
    Reader v = cvc.toCharacterStream(row, columnIndex, this);
    this.wasNull = (v == null);
    return v;
  }

  protected final InputStream getAsciiStream(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    ColumnValueConverter cvc = Converters.getConverter(gfxdType, "AsciiStream");
    InputStream v = cvc.toAsciiStream(row, columnIndex, this);
    this.wasNull = (v == null);
    return v;
  }

  protected Object getObject(final int columnIndex,
      final Map<String, Class<?>> map, final GFXDType gfxdType, final Row row)
      throws SQLException {
    if (map == null) {
      throw ThriftExceptionUtil.newSQLException(SQLState.INVALID_API_PARAMETER,
          null, map, "map", "FetchColumnValue.getObject(int,Map)");
    }
    if (map.isEmpty()) {
      // Map is empty call the normal getObject method.
      return getObject(columnIndex, gfxdType, row);
    }
    else {
      throw ThriftExceptionUtil
          .notImplemented("FetchColumnValue.getObject(int,Map)");
    }
  }

  protected Ref getRef(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    throw ThriftExceptionUtil.notImplemented("FetchColumnValue.getRef");
  }

  protected Array getArray(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    throw ThriftExceptionUtil.notImplemented("FetchColumnValue.getArray");
  }

  protected URL getURL(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    throw ThriftExceptionUtil.notImplemented("FetchColumnValue.getURL");
  }

  protected RowId getRowId(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    throw ThriftExceptionUtil.notImplemented("FetchColumnValue.getRowId");
  }

  protected NClob getNClob(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    throw ThriftExceptionUtil.notImplemented("FetchColumnValue.getNClob");
  }

  protected SQLXML getSQLXML(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    throw ThriftExceptionUtil.notImplemented("FetchColumnValue.getSQLXML");
  }

  protected String getNString(final int columnIndex, final GFXDType gfxdType,
      final Row row) throws SQLException {
    throw ThriftExceptionUtil.notImplemented("FetchColumnValue.getNString");
  }

  protected Reader getNCharacterStream(final int columnIndex,
      final GFXDType gfxdType, final Row row) throws SQLException {
    throw ThriftExceptionUtil
        .notImplemented("FetchColumnValue.getNCharacterStream");
  }

  protected <T> T getObject(final int columnIndex, final Class<T> type,
      final GFXDType gfxdType, final Row row) throws SQLException {
    throw ThriftExceptionUtil
        .notImplemented("FetchColumnValue.getObject(int,Class<T>)");
  }
}

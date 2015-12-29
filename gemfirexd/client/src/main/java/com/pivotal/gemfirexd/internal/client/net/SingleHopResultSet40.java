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
package com.pivotal.gemfirexd.internal.client.net;

import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import com.pivotal.gemfirexd.internal.client.am.PreparedStatement;
import com.pivotal.gemfirexd.internal.client.am.SQLExceptionFactory;
import com.pivotal.gemfirexd.internal.client.net.NetConnection.DSConnectionInfo;

/**
 * 
 * @author kneeraj
 *
 */
public class SingleHopResultSet40 extends SingleHopResultSet {

  public SingleHopResultSet40(java.sql.ResultSet nrs0, PreparedStatement[] psarray, DSConnectionInfo dsConnInfo) {
    super(nrs0, psarray, dsConnInfo);
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getRowId (int)");
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getRowId (String)");
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateRowId (int, RowId)");
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateRowId (String, RowId)");
  }

  public void updateNString(int columnIndex, String nString)
      throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateNString (int, String)");
  }

  public void updateNString(String columnLabel, String nString)
      throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateNString (String, String)");
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateNClob (int, NClob)");
  }

  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateNClob (String, NClob)");
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getRowId (int)");
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getNClob (String)");
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getSQLXML (int)");
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getSQLXML (String)");
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateSQLXML (int, SQLXML)");
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateSQLXML (String, SQLXML)");
  }

  public String getNString(int columnIndex) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getNString (int)");
  }

  public String getNString(String columnLabel) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getNString (String)");
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getNCharacterStream (int)");
  }

  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getNCharacterStream (String)");
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw SQLExceptionFactory
        .notImplemented("updateNCharacterStream (int, Reader, long)");
  }

  public void updateNCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    throw SQLExceptionFactory
        .notImplemented("updateNCharacterStream (String, Reader, long)");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw SQLExceptionFactory.notImplemented("updateNClob (int, Reader, long)");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw SQLExceptionFactory
        .notImplemented("updateNClob (String, Reader, int)");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.currentResultSet.unwrap(iface);
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.currentResultSet.isWrapperFor(iface);
  }

//  public Object getObject(int columnIndex, Map<String, Class<?>> map)
//      throws SQLException {
//    return this.currentResultSet.getObject(columnIndex, map);
//  }
//
//  public Object getObject(String columnLabel, Map<String, Class<?>> map)
//      throws SQLException {
//    return this.currentResultSet.getObject(columnLabel, map);
//  }

  // for compilation with JDK 7
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw SQLExceptionFactory.notImplemented("getObject(int,Class<T>)");
  }

  public <T> T getObject(String columnLabel, Class<T> type)
      throws SQLException {
    throw SQLExceptionFactory.notImplemented("getObject(String,Class<T>)");
  }

  public int getHoldability() throws SQLException {
    return this.currentResultSet.getHoldability();
  }

  public boolean isClosed() throws SQLException {
    return this.currentResultSet.isClosed();
  }
}

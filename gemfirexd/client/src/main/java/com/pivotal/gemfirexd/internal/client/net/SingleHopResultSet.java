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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.pivotal.gemfirexd.internal.client.am.PreparedStatement;
import com.pivotal.gemfirexd.internal.client.net.NetConnection.DSConnectionInfo;
import com.pivotal.gemfirexd.internal.shared.common.error.ExceptionSeverity;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author kneeraj
 *
 */
public abstract class SingleHopResultSet implements java.sql.ResultSet {
  
  private int currRSIndex;
  private PreparedStatement[] prepStmntArray;
  private int totalNumOfResults;
  protected java.sql.ResultSet currentResultSet;
  FinalizeSingleHopResultSet finalizer;

  // arg1 is corresponding the first result set
  public SingleHopResultSet(java.sql.ResultSet nrs0,
      PreparedStatement[] psarray, DSConnectionInfo dsConnInfo) {
    this.currRSIndex = 0;
    this.prepStmntArray = psarray;
    this.totalNumOfResults = psarray.length;
    this.currentResultSet = nrs0;
    if (psarray != null
        && psarray.length != 0
        && psarray[0].sqlMode_ == com.pivotal.gemfirexd.internal.client.am.Statement.isQuery__) {
      this.finalizer = new FinalizeSingleHopResultSet(this, dsConnInfo);
    }
  }
  
  public FinalizeSingleHopResultSet getFinalizer() {
    return this.finalizer;  
  }
  
  public PreparedStatement[] getPreparedStatements() {
    return this.prepStmntArray;  
  }

  private void returnConnection(String op) throws SQLException {
    final FinalizeSingleHopResultSet finalizer = this.finalizer;
    if (finalizer != null) {
      try {
        finalizer.doFinalize();
        this.finalizer = null;
      } catch (Exception e) {
        SQLException sqle = new SQLException(e.getMessage(),
            SQLState.JAVA_EXCEPTION, ExceptionSeverity.STATEMENT_SEVERITY);
        sqle.initCause(e);
        throw sqle;
      }
    }
  }

  public boolean next() throws SQLException {
    boolean gotException = false;
    try {
    for (;;) {
      final boolean ret = this.currentResultSet.next();
      if (ret) {
        return true;
      }
      if (this.currRSIndex == (this.totalNumOfResults - 1)) {
        // all ResultSets consumed; check if FORWARD_ONLY then return connection
        // to pool
        if (this.currentResultSet.getType() == TYPE_FORWARD_ONLY) {
          returnConnection("implicitClose");
        }
        return false;
      }

      this.currRSIndex++;
      this.currentResultSet = this.prepStmntArray[this.currRSIndex]
          .getSuperResultSet();
    }
    } catch (SQLException ex) {
      gotException = true;
      throw ex;
    } finally {
      if (gotException) {
        resetAndDoFinalize();
      }
    }
  }

  private void resetAndDoFinalize() {
    final FinalizeSingleHopResultSet finalizer = this.finalizer;
    if (finalizer != null) {
      finalizer.resetAndDoFinalize(); 
    }
  }

  public void close() throws SQLException {
    for (int i = 0; i < this.totalNumOfResults; i++) {
      this.prepStmntArray[i].getSuperResultSet().close();
    }
    this.currRSIndex = 0;
    this.totalNumOfResults = 0;
    this.prepStmntArray = null;
    returnConnection("close");
  }

  public boolean wasNull() throws SQLException {
    return this.currentResultSet.wasNull();
  }

  
  public String getString(int columnIndex) throws SQLException {
    return this.currentResultSet.getString(columnIndex);
  }

  
  public boolean getBoolean(int columnIndex) throws SQLException {
    return this.currentResultSet.getBoolean(columnIndex);
  }

  
  public byte getByte(int columnIndex) throws SQLException {
    return this.currentResultSet.getByte(columnIndex);
  }

  ///
  
  public short getShort(int columnIndex) throws SQLException {
    return this.currentResultSet.getShort(columnIndex);
  }

  
  public int getInt(int columnIndex) throws SQLException {
    return this.currentResultSet.getInt(columnIndex);
  }

  
  public long getLong(int columnIndex) throws SQLException {
    return this.currentResultSet.getLong(columnIndex);
  }

  
  public float getFloat(int columnIndex) throws SQLException {
    return this.currentResultSet.getFloat(columnIndex);
  }

  
  public double getDouble(int columnIndex) throws SQLException {
    return this.currentResultSet.getDouble(columnIndex);
  }

  
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    return this.currentResultSet.getBigDecimal(columnIndex);
  }

  
  public byte[] getBytes(int columnIndex) throws SQLException {
    return this.currentResultSet.getBytes(columnIndex);
  }

  
  public Date getDate(int columnIndex) throws SQLException {
    return this.currentResultSet.getDate(columnIndex);
  }

  
  public Time getTime(int columnIndex) throws SQLException {
    return this.currentResultSet.getTime(columnIndex);
  }

  
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return this.currentResultSet.getTimestamp(columnIndex);
  }

  
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return this.currentResultSet.getAsciiStream(columnIndex);
  }

  
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return this.currentResultSet.getUnicodeStream(columnIndex);
  }

  
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return this.currentResultSet.getBinaryStream(columnIndex);
  }

  
  public String getString(String columnLabel) throws SQLException {
    return this.currentResultSet.getString(columnLabel);
  }

  
  public boolean getBoolean(String columnLabel) throws SQLException {
    return this.currentResultSet.getBoolean(columnLabel);
  }

  
  public byte getByte(String columnLabel) throws SQLException {
    return this.currentResultSet.getByte(columnLabel);
  }

  
  public short getShort(String columnLabel) throws SQLException {
    return this.currentResultSet.getShort(columnLabel);
  }

  
  public int getInt(String columnLabel) throws SQLException {
    return this.currentResultSet.getInt(columnLabel);
  }

  
  public long getLong(String columnLabel) throws SQLException {
    return this.currentResultSet.getLong(columnLabel);
  }

  
  public float getFloat(String columnLabel) throws SQLException {
    return this.currentResultSet.getFloat(columnLabel);
  }

  
  public double getDouble(String columnLabel) throws SQLException {
    return this.currentResultSet.getDouble(columnLabel);
  }

  
  public BigDecimal getBigDecimal(String columnLabel, int scale)
      throws SQLException {
    return this.currentResultSet.getBigDecimal(columnLabel, scale);
  }

  
  public byte[] getBytes(String columnLabel) throws SQLException {
    return this.currentResultSet.getBytes(columnLabel);
  }

  
  public Date getDate(String columnLabel) throws SQLException {
    return this.currentResultSet.getDate(columnLabel);
  }

  
  public Time getTime(String columnLabel) throws SQLException {
    return this.currentResultSet.getTime(columnLabel);
  }

  
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return this.currentResultSet.getTimestamp(columnLabel);
  }

  
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return this.currentResultSet.getAsciiStream(columnLabel);
  }

  
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return this.currentResultSet.getUnicodeStream(columnLabel);
  }

  
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return this.currentResultSet.getBinaryStream(columnLabel);
  }

  
  public SQLWarning getWarnings() throws SQLException {
    SQLWarning allwarnings = null;
    for (int i=0; i<this.totalNumOfResults; i++) {
      SQLWarning warnings = this.prepStmntArray[i].getSuperResultSet().getWarnings();
      if (allwarnings != null) {
        SQLWarning warn = warnings.getNextWarning();
        while(warn != null) {
          allwarnings.setNextWarning(warn);
        }
      }
      else {
        allwarnings = warnings;
      }
    }
    return allwarnings;
  }

  
  public void clearWarnings() throws SQLException {
    for (int i=0; i<this.totalNumOfResults; i++) {
      this.prepStmntArray[i].getSuperResultSet().clearWarnings();
    }
  }

  
  public String getCursorName() throws SQLException {
    return this.currentResultSet.getCursorName();
  }

  
  public ResultSetMetaData getMetaData() throws SQLException {
    return this.currentResultSet.getMetaData();
  }

  
  public Object getObject(int columnIndex) throws SQLException {
    return this.currentResultSet.getObject(columnIndex);
  }

  
  public Object getObject(String columnLabel) throws SQLException {
    return this.currentResultSet.getObject(columnLabel);
  }

  
  public int findColumn(String columnLabel) throws SQLException {
    return this.currentResultSet.findColumn(columnLabel);
  }

  
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return this.currentResultSet.getCharacterStream(columnIndex);
  }

  
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return this.currentResultSet.getCharacterStream(columnLabel);
  }

  
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return this.currentResultSet.getBigDecimal(columnIndex);
  }

  
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return this.currentResultSet.getBigDecimal(columnLabel);
  }

  
  public boolean isBeforeFirst() throws SQLException {
    if (this.currRSIndex == 0) {
      return this.currentResultSet.isBeforeFirst();
    }
    return false;
  }

  
  public boolean isAfterLast() throws SQLException {
    if (this.currRSIndex == this.prepStmntArray.length - 1) {
      return this.currentResultSet.isAfterLast();
    }
    return false;
  }

  
  public boolean isFirst() throws SQLException {
    if (this.currRSIndex == 0) {
      return this.currentResultSet.isFirst();
    }
    return false;
  }

  
  public boolean isLast() throws SQLException {
    if (this.currRSIndex == this.prepStmntArray.length - 1) {
      return this.currentResultSet.isLast();
    }
    return false;
  }

  
  public void beforeFirst() throws SQLException {
    for(int i=0; i<this.totalNumOfResults; i++) {
      this.prepStmntArray[i].getSuperResultSet().beforeFirst();
    }
    this.currentResultSet = this.prepStmntArray[0].getSuperResultSet();
    this.currRSIndex = 0;
  }

  
  public void afterLast() throws SQLException {
    for(int i=0; i<this.prepStmntArray.length; i++) {
      this.prepStmntArray[i].getSuperResultSet().afterLast();
    }
    this.currentResultSet = this.prepStmntArray[this.totalNumOfResults - 1].getSuperResultSet();
    this.currRSIndex = this.totalNumOfResults - 1;
  }

  
  public boolean first() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public boolean last() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public int getRow() throws SQLException {
    return this.currentResultSet.getRow();
  }

  
  public boolean absolute(int row) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public boolean relative(int rows) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public boolean previous() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public void setFetchDirection(int direction) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  
  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  public int getType() throws SQLException {
    return this.currentResultSet.getType();
  }

  public int getConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  
  public boolean rowUpdated() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public boolean rowInserted() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public boolean rowDeleted() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  
  public void updateNull(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateByte(int columnIndex, byte x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateShort(int columnIndex, short x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateInt(int columnIndex, int x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateLong(int columnIndex, long x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateFloat(int columnIndex, float x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateDouble(int columnIndex, double x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateString(int columnIndex, String x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateDate(int columnIndex, Date x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateTime(int columnIndex, Time x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateObject(int columnIndex, Object x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateNull(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateByte(String columnLabel, byte x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateShort(String columnLabel, short x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateInt(String columnLabel, int x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateLong(String columnLabel, long x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateFloat(String columnLabel, float x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateDouble(String columnLabel, double x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBigDecimal(String columnLabel, BigDecimal x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateString(String columnLabel, String x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateDate(String columnLabel, Date x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateTime(String columnLabel, Time x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateTimestamp(String columnLabel, Timestamp x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateCharacterStream(String columnLabel, Reader reader,
      int length) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateObject(String columnLabel, Object x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void insertRow() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateRow() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void deleteRow() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void refreshRow() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void cancelRowUpdates() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void moveToInsertRow() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void moveToCurrentRow() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  // TODO: KN check the right implementation for this method
  public Statement getStatement() throws SQLException {
    return this.currentResultSet.getStatement();
  }

  
  public Ref getRef(int columnIndex) throws SQLException {
    return this.currentResultSet.getRef(columnIndex);
  }

  
  public Blob getBlob(int columnIndex) throws SQLException {
    return this.currentResultSet.getBlob(columnIndex);
  }

  
  public Clob getClob(int columnIndex) throws SQLException {
    return this.currentResultSet.getClob(columnIndex);
  }

  
  public Array getArray(int columnIndex) throws SQLException {
    return this.currentResultSet.getArray(columnIndex);
  }
  
  public Ref getRef(String columnLabel) throws SQLException {
    return this.currentResultSet.getRef(columnLabel);
  }

  
  public Blob getBlob(String columnLabel) throws SQLException {
    return this.currentResultSet.getBlob(columnLabel);
  }

  
  public Clob getClob(String columnLabel) throws SQLException {
    return this.currentResultSet.getClob(columnLabel);
  }

  
  public Array getArray(String columnLabel) throws SQLException {
    return this.currentResultSet.getArray(columnLabel);
  }

  
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return this.currentResultSet.getDate(columnIndex, cal);
  }

  
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return this.currentResultSet.getDate(columnLabel, cal);
  }

  
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return this.currentResultSet.getTime(columnIndex, cal);
  }

  
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return this.currentResultSet.getTime(columnLabel, cal);
  }

  
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    return this.currentResultSet.getTimestamp(columnIndex, cal);
  }

  
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
      throws SQLException {
    return this.currentResultSet.getTimestamp(columnLabel, cal);
  }

  
  public URL getURL(int columnIndex) throws SQLException {
    return this.currentResultSet.getURL(columnIndex);
  }

  
  public URL getURL(String columnLabel) throws SQLException {
    return this.currentResultSet.getURL(columnLabel);
  }

  
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateArray(int columnIndex, Array x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateArray(String columnLabel, Array x) throws SQLException {
    // TODO Auto-generated method stub
    
  }
  
//  public int getHoldability() throws SQLException {
//    return this.currentResultSet.getHoldability();
//  }
//
//  public boolean isClosed() throws SQLException {
//    return this.currentResultSet.isClosed();
//  }
//
//  
//  public void updateNString(int columnIndex, String nString)
//      throws SQLException {
//    // TODO Auto-generated method stub
//    
//  }
//
//  
//  public void updateNString(String columnLabel, String nString)
//      throws SQLException {
//    // TODO Auto-generated method stub
//    
//  }
//
//  
//  public String getNString(int columnIndex) throws SQLException {
//    return this.currentResultSet.getNString(columnIndex);
//  }
//
//  
//  public String getNString(String columnLabel) throws SQLException {
//    return this.currentResultSet.getNString(columnLabel);
//  }
//
//  
//  public Reader getNCharacterStream(int columnIndex) throws SQLException {
//    return this.currentResultSet.getNCharacterStream(columnIndex);
//  }
//
//  
//  public Reader getNCharacterStream(String columnLabel) throws SQLException {
//    return this.currentResultSet.getNCharacterStream(columnLabel);
//  }
//
//  
//  public void updateNCharacterStream(int columnIndex, Reader x, long length)
//      throws SQLException {
//    // TODO Auto-generated method stub
//    
//  }
//
//  
//  public void updateNCharacterStream(String columnLabel, Reader reader,
//      long length) throws SQLException {
//    // TODO Auto-generated method stub
//    
//  }

  public Object getObject(String columnName, java.util.Map map) throws SQLException {
    return this.currentResultSet.getObject(columnName, map);  
  }
  
  public Object getObject(int columnIndex, java.util.Map map) throws SQLException {
    return this.currentResultSet.getObject(columnIndex, map);  
  }
  
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBlob(String columnLabel, InputStream inputStream,
      long length) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBlob(int columnIndex, InputStream inputStream)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  
  public void updateNClob(String columnLabel, Reader reader)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }
}

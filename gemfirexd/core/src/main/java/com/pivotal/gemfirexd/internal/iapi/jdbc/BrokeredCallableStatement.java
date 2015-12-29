/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.jdbc.BrokeredCallableStatement

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

package com.pivotal.gemfirexd.internal.iapi.jdbc;

import java.sql.*;
import java.math.BigDecimal;

import java.util.Calendar;
import java.util.Map;

import java.io.InputStream;
import java.io.Reader;


/**
	JDBC 2 brokered CallableStatement
 */
public abstract class BrokeredCallableStatement extends BrokeredPreparedStatement
          implements CallableStatement
{

	public BrokeredCallableStatement(BrokeredStatementControl control, String sql) throws SQLException {
		super(control,sql);
	}

    public final void registerOutParameter(int parameterIndex,
                                     int sqlType)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType);
    }

    public final void registerOutParameter(int parameterIndex,
                                     int sqlType,
                                     int scale)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( parameterIndex, sqlType, scale);
    }

    public final boolean wasNull()
        throws SQLException
    {
        return getCallableStatement().wasNull();
    }

    public final String getString(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getString( parameterIndex);
    }

    public final boolean getBoolean(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBoolean( parameterIndex);
    }

    public final byte getByte(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getByte( parameterIndex);
    }

    public final short getShort(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getShort( parameterIndex);
    }

    public final int getInt(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getInt( parameterIndex);
    }

    public final long getLong(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getLong( parameterIndex);
    }

    public final float getFloat(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getFloat( parameterIndex);
    }

    public final double getDouble(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getDouble( parameterIndex);
    }

    /** @deprecated */
    public final BigDecimal getBigDecimal(int parameterIndex,
                                              int scale)
        throws SQLException
    {
        return getCallableStatement().getBigDecimal( parameterIndex, scale);
    }

    public final byte[] getBytes(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBytes( parameterIndex);
    }

    public final Date getDate(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getDate( parameterIndex);
    }

    public final Date getDate(int parameterIndex,
                        Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getDate( parameterIndex, cal);
    }

    public final Time getTime(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getTime( parameterIndex);
    }

    public final Timestamp getTimestamp(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getTimestamp( parameterIndex);
    }

    public final Object getObject(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getObject( parameterIndex);
    }

    public final BigDecimal getBigDecimal(int parameterIndex)
        throws SQLException
    {
        return getCallableStatement().getBigDecimal( parameterIndex);
    }

    public final Object getObject(int i,
                            Map map)
        throws SQLException
    {
        return getCallableStatement().getObject( i, map);
    }

    public final Ref getRef(int i)
        throws SQLException
    {
        return getCallableStatement().getRef( i);
    }

    public final Blob getBlob(int i)
        throws SQLException
    {
        return getCallableStatement().getBlob( i);
    }

    public final Clob getClob(int i)
        throws SQLException
    {
        return getCallableStatement().getClob( i);
    }

    public final Array getArray(int i)
        throws SQLException
    {
        return getCallableStatement().getArray( i);
    }

    public final Time getTime(int parameterIndex,
                        Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getTime( parameterIndex, cal);
    }

    public final Timestamp getTimestamp(int parameterIndex,
                                  Calendar cal)
        throws SQLException
    {
        return getCallableStatement().getTimestamp( parameterIndex, cal);
    }

    public final void registerOutParameter(int paramIndex,
                                     int sqlType,
                                     String typeName)
        throws SQLException
    {
        getCallableStatement().registerOutParameter( paramIndex, sqlType, typeName);
    }

	/*
	** Control methods
	*/

    /**
     * Access the underlying CallableStatement. This method
     * is package protected to restrict access to the underlying
     * object to the brokered objects. Allowing the application to
     * access the underlying object thtough a public method would
     * 
     */
    final CallableStatement getCallableStatement() throws SQLException {
		return control.getRealCallableStatement();
	}
	
    /**
     * Access the underlying PreparedStatement. This method
     * is package protected to restrict access to the underlying
     * object to the brokered objects. Allowing the application to
     * access the underlying object thtough a public method would
     * 
     */
    final PreparedStatement getPreparedStatement() throws SQLException {
		return getCallableStatement();
	}
	/**
		Create a duplicate CalableStatement to this, including state, from the passed in Connection.
	*/
	public CallableStatement createDuplicateStatement(Connection conn, CallableStatement oldStatement) throws SQLException {

		CallableStatement newStatement = conn.prepareCall(sql, resultSetType, resultSetConcurrency);

		setStatementState(oldStatement, newStatement);

		return newStatement;
	}
  

  // GemStone changes BEGIN
  // dummy methods so will compile in JDK 1.6
  public Reader getCharacterStream(int parameterIndex)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public Reader getCharacterStream(String parameterName)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public Reader getNCharacterStream(int parameterIndex)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public Reader getNCharacterStream(String parameterName)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public String getNString(int parameterIndex)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public String getNString(String parameterName)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public RowId getRowId(int parameterIndex) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public RowId getRowId(String parameterName) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setRowId(String parameterName, RowId x) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBlob(String parameterName, Blob x)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setClob(String parameterName, Clob x)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNString(String parameterName, String value)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNCharacterStream(String parameterName, Reader value)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNCharacterStream(String parameterName,Reader value,long length)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNClob(String parameterName, NClob value) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setClob(String parameterName, Reader reader)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setClob(String parameterName, Reader reader, long length)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBlob(String parameterName, InputStream inputStream)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBlob(String parameterName, InputStream inputStream, long length)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNClob(String parameterName, Reader reader)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNClob(String parameterName, Reader reader, long length)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public NClob getNClob(int i) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public NClob getNClob(String parameterName) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public SQLXML getSQLXML(int parameterIndex) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public SQLXML getSQLXML(String parametername) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setAsciiStream(int parameterIndex, InputStream x)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBinaryStream(int parameterIndex, InputStream x)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setCharacterStream(int parameterIndex, Reader reader)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setRowId(int parameterIndex, RowId x) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNString(int index, String value) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNCharacterStream(int parameterIndex, Reader value)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNCharacterStream(int index, Reader value, long length) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNClob(int index, NClob value) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setClob(int parameterIndex, Reader reader)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setClob(int parameterIndex, Reader reader, long length)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBlob(int parameterIndex, InputStream inputStream)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNClob(int parameterIndex, Reader reader)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setNClob(int parameterIndex, Reader reader, long length)
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public boolean isClosed() throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public <T> T unwrap(java.lang.Class<T> interfaces) 
  throws SQLException{
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }

  public boolean isPoolable() throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setPoolable(boolean poolable) throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setAsciiStream(int parameterIndex, InputStream x, long length)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBinaryStream(int parameterIndex, InputStream x, long length)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setCharacterStream(int parameterIndex, Reader x, long length)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setAsciiStream(String parameterName, InputStream x)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setAsciiStream(String parameterName, InputStream x, long length)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBinaryStream(String parameterName, InputStream x)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setBinaryStream(String parameterName, InputStream x, long length)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setCharacterStream(String parameterName, Reader x)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }
  
  public void setCharacterStream(String parameterName, Reader x, long length)
  throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.0");
  }

  // methods of jdbc 4.1 from jdk1.7
  public Object getObject(String parameterName, Map<String, Class<?>> map)
      throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.1");
  }

  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.1");
  }

  public <T> T getObject(String parameterName, Class<T> type)
      throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.1");
  }

  public void closeOnCompletion() throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.1");
  }

  public boolean isCloseOnCompletion() throws SQLException {
    throw new AssertionError("should have been overridden in JDBC 4.1");
  }
  // GemStone changes END  
}

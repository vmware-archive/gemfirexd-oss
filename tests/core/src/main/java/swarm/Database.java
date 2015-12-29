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
package swarm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;

public class Database {

	
	private static final String URL = "jdbc:postgresql://hydrogen.gemstone.com/gregp";
	private static final HashMap<String,Long> queryCount = new HashMap<String,Long>();
	private static final HashMap<String,Long> queryTime = new HashMap<String,Long>();
	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch(Exception e) {
			e.printStackTrace();
			throw new ExceptionInInitializerError(e.getMessage());
		}
	}
	
	static ThreadLocal<Connection> conns = new ThreadLocal<Connection>();
	
	public static Connection getConnection() throws SQLException {
	  Connection c = conns.get();
	  if(c==null) {
	    c = DriverManager.getConnection(URL,"gregp","Q6to2ZFd1j9y");
	    conns.set(c);
	  }
	  return c;
	}
	
	public static ResultSet executeQuery(String sql,boolean debug) throws SQLException {
		Statement st = getConnection().createStatement();
		if(debug) {
			System.out.println("[SWARMSQL]:"+sql);
		}
		long s = System.nanoTime();
		ResultSet rs = st.executeQuery(sql);
		long took = (System.nanoTime()-s)/1000000;
		if(took>50) {
		  System.out.println("XXXX"+took+"ms:"+sql);
		}
		synchronized(Database.class) {
		  Long l = queryCount.get(sql.substring(0,20));
		  if(l==null) {
		    l = 1l;
		  } else {
		    l = l+1l;
		  }
		  queryCount.put(sql.substring(0,20),l);
		  Long time = queryTime.get(sql.substring(0,20));
		  if(time == null) {
		    time = took;
		  } else {
		    time = time+took;
		  }
		  queryTime.put(sql.substring(0,20),time);
		  if(l%100==0) {
		    System.out.println("querycounts");
		    System.out.println("-------------------------");
		    Set<String> qs = queryCount.keySet();
		    for (String string : qs) {
          System.out.println(string+":"+queryCount.get(string)+"="+queryTime.get(string)+"ms");
        }
		    System.out.println("-------------------------");
		    
		  }
		}
		return rs;
	}
	
	 public static PreparedStatement prepareStatement(String sql) throws SQLException {
	   return prepareStatement(sql,true);
	 }
	 
	public static PreparedStatement prepareStatement(String sql,boolean debug) throws SQLException {
	  if(true || debug) {
      System.out.println("[SWARMPREPARGFXD]:"+sql);
    }
		PreparedStatement st = getConnection().prepareStatement(sql);
		return st;
	}
	
	
	public static ResultSet executeQuery(String sql) throws SQLException {
		return executeQuery(sql,true);
	}
	
	
	public static int executeUpdate(String sql,boolean debug) throws SQLException {
		Statement st = getConnection().createStatement();
		if(debug) {
			System.out.println("[SWARMSQL]:"+sql);
		}
		return st.executeUpdate(sql);
	}
	
	public static int executeUpdate(String sql) throws SQLException {
		return executeUpdate(sql,true);
	}
	
	
}
/*
class PreparedStatement implements java.sql.PreparedStatement {

  @Override
  public void addBatch() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void clearParameters() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean execute() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int executeUpdate() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setArray(int arg0, Array arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setAsciiStream(int arg0, InputStream arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setAsciiStream(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBinaryStream(int arg0, InputStream arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBinaryStream(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBlob(int arg0, Blob arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBlob(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBlob(int arg0, InputStream arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBoolean(int arg0, boolean arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setByte(int arg0, byte arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setBytes(int arg0, byte[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setCharacterStream(int arg0, Reader arg1, int arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setCharacterStream(int arg0, Reader arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setClob(int arg0, Clob arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setClob(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setDate(int arg0, Date arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setDouble(int arg0, double arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setFloat(int arg0, float arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setInt(int arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setLong(int arg0, long arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNCharacterStream(int arg0, Reader arg1, long arg2)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNClob(int arg0, NClob arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNClob(int arg0, Reader arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNString(int arg0, String arg1) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType,
      int scaleOrLength) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addBatch(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void cancel() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void clearBatch() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean execute(String arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String arg0, int[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String arg0, String[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int[] executeBatch() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet executeQuery(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int executeUpdate(String arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String arg0, int[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String arg0, String[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Connection getConnection() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxRows() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean getMoreResults(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setCursorName(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setEscapeProcessing(boolean arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setFetchDirection(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setFetchSize(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setMaxFieldSize(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setMaxRows(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setPoolable(boolean arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setQueryTimeout(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }
  
  
}
}
*/
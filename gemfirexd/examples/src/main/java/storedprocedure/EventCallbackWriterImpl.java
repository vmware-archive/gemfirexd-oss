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
package examples.storedprocedure;

import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventCallback;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * EventCallbackWriterImpl is an implementation of the EventCallback interface
 * that writes to backend database. It works with tables having a primary key.
 * 
 * EventCallbackWriterImpl has the following features:
 * <ul>
 * <li>It can be used for any JDBC data source (provided the driver is available
 * in the classpath of the server).</li>
 * <li>It can be used for any table, although a separate instance of the
 * EventCallbackWriterImpl is created for each table.</li>
 * <li>It will pool JDBC Connections, with a configurable minimum and maximum
 * number of connections.</li>
 * </ul>
 * 
 * 
 * The EventCallbackWriterImpl is configured with an initialization string
 * passed as the 4th parameter to the SYS.ATTACH_WRITER procedure. The
 * initialization string should contain a delimited set of parameters for the
 * EventCallbackWriterImpl.
 * 
 * The first character in the initialization string is used as the delimiter for
 * the rest of the parameters, so the string should start with a delimiter
 * character.
 * 
 * Accepted parameters are:
 * 
 * <ul>
 * <li>url (required) - the JDBC URL of the database to connect to</li>
 * <li>min-connections (optional, default is 1) - the minimum number of
 * connections to maintain in the connection pool</li>
 * <li>max-connections (optional, default is 1) - the maximum number of
 * connections to maintain in the connection pool</li>
 * <li>connection-timeout (optional, default is 10000) - the maximum amount of
 * time to wait, in milliseconds, for a connection to become available in the
 * connection pool</li>
 * </ul>
 * 
 * 
 * 
 * Note: For this implementation, it is required that the schema and table name
 * in GemFireXD match the schema and table name in the backend database. And the
 * column layout of the backend table should match the column layout of the
 * GemFireXD table.
 * 
 */

public class EventCallbackWriterImpl implements EventCallback {
  private static final int VENDOR_CODE_ARCHIVE_ERROR = 0;
  private static final int VENDOR_CODE_TIMEOUT = 1;
  private static final int VENDOR_CODE_NO_URL = 2;
  private static final int VENDOR_CODE_NO_UPDATE = 3;

  protected Properties props = new Properties();
  protected String url = "";
  protected int minConnections;
  protected int maxConnections;
  protected long connectionTimeout;
  private Integer connectionCount = 0;
  private final Logger log = Logger.getLogger("com.pivotal.gemfirexd");
  private final ExecutorService backgroundExecutor = Executors
      .newCachedThreadPool();
  private final Queue<ConnectionHolder[]> waitingConnections =
      new LinkedList<ConnectionHolder[]>();
  private final Queue<ConnectionHolder> availableConnections =
      new LinkedList<ConnectionHolder>();

  /** the maximum size of BLOBs that will be kept in memory */
  static final int MAX_MEM_BLOB_SIZE = 64 * 1024 * 1024;
  /** the maximum size of CLOBs that will be kept in memory */
  static final int MAX_MEM_CLOB_SIZE = (MAX_MEM_BLOB_SIZE >>> 1);

  /**
   * Flag to indicate whether the current {@link #driver} is JDBC4 compliant for
   * BLOB/CLOB related API. Value can be one of {@link #JDBC4DRIVER_UNKNOWN},
   * {@link #JDBC4DRIVER_FALSE} or {@link #JDBC4DRIVER_TRUE}.
   */
  private byte isJDBC4Driver = JDBC4DRIVER_UNKNOWN;

  /**
   * if it is unknown whether the driver is JDBC4 compliant for BLOB/CLOB
   * related API
   */
  static final byte JDBC4DRIVER_UNKNOWN = -1;
  /**
   * if it is known that the driver is not JDBC4 compliant for BLOB/CLOB related
   * API
   */
  static final byte JDBC4DRIVER_FALSE = 0;
  /**
   * if it is known that the driver is JDBC4 compliant for BLOB/CLOB related API
   */
  static final byte JDBC4DRIVER_TRUE = 1;

  /*
   * (non-Javadoc)
   * 
   * @see com.pivotal.gemfirexd.callbacks.EventCallback#close()
   */
  @Override
  public void close() throws SQLException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.pivotal.gemfirexd.callbacks.EventCallback#init(java.lang.String)
   */
  @Override
  public void init(String initStr) throws SQLException {
    log.entering("EventCallbackWriterImpl", "init()");
    loadParametersFromInitString(initStr);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * com.pivotal.gemfirexd.callbacks.EventCallback#onEvent(
   *   com.pivotal.gemfirexd.callbacks.Event)
   */
  @Override
  public void onEvent(Event event) throws SQLException {
    if (connectionCount < minConnections) {
      initConnection();
    }
    Event.Type evType = event.getType();
    String tableName = event.getTableName();
    ConnectionHolder[] holder = new ConnectionHolder[1];
    if (evType == Event.Type.BEFORE_UPDATE) {
      ResultSet updatedRow = event.getNewRowsAsResultSet();
      ResultSetMetaData updateMeta = updatedRow.getMetaData();
      ResultSet pkRow = event.getPrimaryKeysAsResultSet();
      ResultSetMetaData pkMeta = pkRow.getMetaData();

      String query = buildUpdateQuery(tableName, updateMeta, pkMeta);
      PreparedStatement pstmt = getPreparedStatement(query, holder);

      // Set updated col values in prepared statement
      int param;
      for (param = 1; param <= updateMeta.getColumnCount(); param++) {
        setColumnInPrepStatement(updateMeta.getColumnType(param), pstmt,
            updatedRow, param, param);
      }
      // Now set the Pk values
      setKeysInPrepStatement(pkRow, pkMeta, pstmt, param);

      executePreparedStatement(pstmt, holder[0]);
      log.info("Complete updating a row in the backend database.");
    } else if (evType == Event.Type.BEFORE_INSERT) {
      ResultSet newRow = event.getNewRowsAsResultSet();
      ResultSetMetaData tableMeta = event.getResultSetMetaData();

      String query = buildInsertQuery(tableName, tableMeta);
      PreparedStatement pstmt = getPreparedStatement(query, holder);

      // set insert column values in prepared statement
      for (int colIdx = 1; colIdx <= tableMeta.getColumnCount(); colIdx++) {
        setColumnInPrepStatement(tableMeta.getColumnType(colIdx), pstmt,
            newRow, colIdx, colIdx);
      }

      executePreparedStatement(pstmt, holder[0]);
      log.info("Complete inserting a row in the backend database.");
    } else if (evType == Event.Type.BEFORE_DELETE) {
      ResultSet pkRow = event.getPrimaryKeysAsResultSet();
      ResultSetMetaData pkMeta = pkRow.getMetaData();

      String query = buildDeleteQuery(tableName, pkMeta);
      PreparedStatement pstmt = getPreparedStatement(query, holder);

      // set the Pk values in prepared statement
      setKeysInPrepStatement(pkRow, pkMeta, pstmt, 1);

      executePreparedStatement(pstmt, holder[0]);
      log.info("Complete deleting a row in the backend database.");
    }
  }

  /**
   * For security purpose in the log file, the password is masked as string of
   * "x"
   * 
   * @param str
   *          the input string
   * @return replaced every character in the str with 'x'
   */
  private String maskString(String str) {
    if (str != null) {
      char[] masked = new char[str.length()];
      for (int i = 0; i < str.length(); i++) {
        masked[i] = 'x';
      }
      return String.copyValueOf(masked);
    }
    return "";
  }

  /**
   * Write the initialization parameters into log
   */
  private void logInitParameters() {
    if (log.isLoggable(Level.INFO)) {
      log.info("EventCallbackWriterImpl initialized.");
      for (Entry<Object, Object> entry : props.entrySet()) {
        if ("password".equals(entry.getKey()))
          log.info("   " + entry.getKey() + ": "
              + maskString((String) entry.getValue()));
        else
          log.info("   " + entry.getKey() + ": " + entry.getValue());
      }
    }
  }

  /**
   * Parse the initialization string and put key value pairs into property
   * 
   * @param initStr
   *          initialization string
   */
  private void parsePropertiesFromString(String initStr) {
    if (initStr.length() > 1) {
      String delimiter = initStr.substring(0, 1);
      String[] params = initStr.substring(1).split("\\" + delimiter);

      for (String parameter : params) {
        int equalsIndex = parameter.indexOf('=');
        if ((equalsIndex > 0) & (parameter.length() > equalsIndex + 1)) {
          String key = parameter.substring(0, equalsIndex).trim();
          String value = parameter.substring(equalsIndex + 1).trim();
          props.put(key, value);
        }
      }
    }
  }

  /**
   * Get the property with given key
   * 
   * @param key
   *          the key to be looked up for the value
   * @param defaultValue
   *          the default value for the key
   * @return the corresponding value of given key in the property
   */
  private String getProperty(String key, String defaultValue) {
    Object value = props.remove(key);
    if (value == null)
      return defaultValue;
    else
      return (String) value;
  }

  /**
   * Parse the initialization string, and initialize the member fields
   * 
   * @param initStr
   *          the initialization string
   * @throws SQLException 
   */
  private void loadParametersFromInitString(String initStr) throws SQLException {
    parsePropertiesFromString(initStr);
    logInitParameters();
    this.url = getProperty("url", "");
    if (url.isEmpty()) {
      throw new SQLException("Connection url not provided "
          + "for EventCallbackWriterImpl", "XJ028", VENDOR_CODE_NO_URL);
    }
    this.minConnections = Integer.parseInt(getProperty("min-connections", "1"));
    this.maxConnections = Integer.parseInt(getProperty("max-connections", "5"));
    this.connectionTimeout = Long.parseLong(getProperty("connection-timeout",
        "10000"));
  }

  /**
   * Get database connection
   * 
   * @return the connection object for the database connection
   * @throws SQLException
   */
  private Connection getDatabaseConnection() throws SQLException {
    return DriverManager.getConnection(url, props);
  }

  /**
   * Build the query string for updating the backend database
   * 
   * @param tableName
   *          the full qualified name of the table being updated
   * @param updateMeta
   *          meta-data for the rows being updated
   * @param pkMeta
   *          meta-data for the primary key of the table
   * 
   * @return SQL update query to the backend database
   * 
   * @throws SQLException
   *           on error
   */
  private String buildUpdateQuery(String tableName,
      ResultSetMetaData updateMeta, ResultSetMetaData pkMeta)
      throws SQLException {

    int numModifiedColumns = updateMeta.getColumnCount();
    if (numModifiedColumns == 0) {
      throw new SQLException("Nothing is updated.", "X0Y78",
          VENDOR_CODE_NO_UPDATE);
    }

    StringBuilder query = new StringBuilder();
    query.append("UPDATE ").append(tableName);
    query.append(" SET ");

    for (int i = 1; i < numModifiedColumns; i++) {
      query.append(updateMeta.getColumnName(i)).append("=?,");
    }
    query.append(updateMeta.getColumnName(numModifiedColumns)).append("=?");

    // add where clause "where pkName1=? and pkName2=? ... ;"

    query.append(" WHERE ");
    int numPkCols = pkMeta.getColumnCount();
    for (int i = 1; i < numPkCols; i++) {
      query.append(pkMeta.getColumnName(i));
      query.append("=? AND ");
    }
    query.append(pkMeta.getColumnName(numPkCols));
    query.append("=? ");

    return query.toString();
  }

  /**
   * Build the query string to insert a row to the backend database
   * 
   * @param tableName
   *          the full qualified name of the table being updated
   * @param tableMeta
   *          meta-data of the table being updated
   * 
   * @return SQL query string to insert a row to the back-end database
   * 
   * @throws SQLException
   *           on error
   */
  private String buildInsertQuery(String tableName, ResultSetMetaData tableMeta)
      throws SQLException {

    StringBuilder query = new StringBuilder().append("INSERT INTO ");

    query.append(tableName);
    query.append(" VALUES (");
    for (int i = 1; i <= tableMeta.getColumnCount(); i++) {
      query.append("?,");
    }
    query.setCharAt(query.length() - 1, ')');

    return query.toString();
  }

  /**
   * Build querying string to delete a row in the backend database
   * 
   * @param tableName
   *          fully qualified name of the table
   * @param pkMeta
   *          meta-data of the primary key columns of the table
   * 
   * @return SQL query string to delete a row in the backend database
   * 
   * @throws SQLException
   *           on error
   */
  private String buildDeleteQuery(String tableName, ResultSetMetaData pkMeta)
      throws SQLException {

    StringBuilder query = new StringBuilder().append("DELETE FROM ");

    query.append(tableName);
    query.append(" WHERE ");
    // use the primary key columns to fire the delete on backend DB
    final int numCols = pkMeta.getColumnCount();
    for (int col = 1; col < numCols; col++) {
      query.append(pkMeta.getColumnName(col));
      query.append("=? AND ");
    }
    query.append(pkMeta.getColumnName(numCols));
    query.append("=?");

    return query.toString();
  }

  /**
   * Initialize the backend database connection
   */
  private void initConnection() {
    for (int i = 0; i < minConnections; i++) {
      synchronized (connectionCount) {
        connectionCount = connectionCount + 1;
      }
      Runnable creator = new ConnectionCreator();
      this.backgroundExecutor.execute(creator);
    }
  }

  /**
   * Get a connection from the connection pool, then return a PreparedStatement
   * from the connection. If there is no connection available in the pool, the
   * thread will wait for available connection
   * 
   * @param query
   *          query string
   * @param holder
   *          to hold the returned connection
   * 
   * @return PreparedStatement from the connection retrieved from connection
   *         pool
   * 
   * @throws SQLException
   */
  private PreparedStatement getPreparedStatement(String query,
      ConnectionHolder[] holder) throws SQLException {
    synchronized (holder) {
      getPooledConnection(holder);
      if (holder[0] == null) {
        try {
          holder.wait(connectionTimeout);
        } catch (InterruptedException e) {
          log.warning("EventCallbackWriterImpl interrupted while waiting "
              + "for an available pooled statement.");
          log.warning(getStackTrace(e));
          Thread.currentThread().interrupt();
        }
      } // if
    } // synchronized
    if (holder[0] == null) {
      throw new SQLException(
          "Timeout waiting for pooled connection to archive database", "08001",
          VENDOR_CODE_TIMEOUT);
    }
    return holder[0].getPreparedStatement(query);
  }

  /**
   * Execute the given PreparedStatement
   * 
   * @param pstmt
   *          the PreparedStatement to be executed
   * @throws SQLException
   */
  private void executePreparedStatement(PreparedStatement pstmt,
      ConnectionHolder con) throws SQLException {
    try {
      log.info("Executing query " + pstmt.toString());
      pstmt.executeUpdate();
      log.info("Query succeeded");
      recyclePooledConnection(con);
    } catch (SQLException e) {
      releasePooledConnection(con);
      logGetRowError(e);
      throw new SQLException("Error executing query from archive database", e
          .getSQLState(), VENDOR_CODE_ARCHIVE_ERROR, e);
    } // catch
  }

  /**
   * Set column value at given index in a prepared statement. The implementation
   * tries using the matching underlying type to minimize any data type
   * conversions, and avoid creating wrapper Java objects (e.g. {@link Integer}
   * for primitive int).
   * 
   * @param javaSqlType
   *          the SQL type of the column as specified by JDBC {@link Types}
   *          class
   * @param ps
   *          the prepared statement where the column value has to be set
   * @param row
   *          the source row as a {@link ResultSet} from where the value has to
   *          be extracted
   * @param rowPosition
   *          the 1-based position of the column in the provided
   *          <code>row</code>
   * @param paramIndex
   *          the 1-based position of the column in the target prepared
   *          statement (provided <code>ps</code> argument)
   * 
   * @throws SQLException
   *           in case of an exception in setting parameters
   */
  public final void setColumnInPrepStatement(int javaSqlType,
      PreparedStatement ps, ResultSet row, int rowPosition, int paramIndex)
      throws SQLException {
    switch (javaSqlType) {
      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.NCHAR:
      case Types.NCLOB:
      case Types.NULL:
      case Types.NVARCHAR:
      case Types.OTHER:
      case Types.REF:
      case Types.ROWID:
      case Types.STRUCT:
        throw new UnsupportedOperationException("java.sql.Type = "
            + javaSqlType + " not supported");

      case Types.BIGINT:
        final long longVal = row.getLong(rowPosition);
        if (!row.wasNull()) {
          ps.setLong(paramIndex, longVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.BIT:
      case Types.BOOLEAN:
        final boolean boolVal = row.getBoolean(rowPosition);
        if (!row.wasNull()) {
          ps.setBoolean(paramIndex, boolVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.BLOB:
        final Blob blob = row.getBlob(rowPosition);
        final long blen;
        if (blob != null) {
          if ((blen = blob.length()) > MAX_MEM_BLOB_SIZE) {
            if (isJDBC4Driver == JDBC4DRIVER_TRUE) {
              // The Oracle driver does not work with ps.setBlob
              // where it expects an Oracle Blob type
              ps.setBinaryStream(paramIndex, blob.getBinaryStream());
              break;
            }
            else if (isJDBC4Driver == JDBC4DRIVER_UNKNOWN) {
              try {
                ps.setBinaryStream(paramIndex, blob.getBinaryStream(), blen);
                this.isJDBC4Driver = JDBC4DRIVER_TRUE;
                break;
              } catch (java.sql.SQLFeatureNotSupportedException e) {
              } catch (java.lang.AbstractMethodError e) {
              }
              this.isJDBC4Driver = JDBC4DRIVER_FALSE;
            }
          }
          // Yogesh: For small/medium Blobs and drivers which do not support
          // ps.setBinaryStream
          final byte[] bytes = blob.getBytes(1, (int)blen);
          ps.setBytes(paramIndex, bytes);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.CLOB:
        final Object clobVal = row.getObject(rowPosition);
        if (clobVal != null) {
          if (clobVal instanceof String) {
            // Yogesh: Ideally this should not be of type String
            ps.setString(paramIndex, (String)clobVal);
          }
          else {
            final Clob clob = (Clob)clobVal;
            final long len;
            if ((len = clob.length()) > MAX_MEM_CLOB_SIZE) {
              if (isJDBC4Driver == JDBC4DRIVER_TRUE) {
                // The Oracle driver does not work with ps.setClob
                // where it expects an Oracle Clob type
                ps.setCharacterStream(paramIndex, clob.getCharacterStream());
                break;
              }
              else if (isJDBC4Driver == JDBC4DRIVER_UNKNOWN) {
                try {
                  ps.setCharacterStream(paramIndex, clob.getCharacterStream(),
                      len);
                  this.isJDBC4Driver = JDBC4DRIVER_TRUE;
                  break;
                } catch (java.sql.SQLFeatureNotSupportedException e) {
                } catch (java.lang.AbstractMethodError e) {
                }
                this.isJDBC4Driver = JDBC4DRIVER_FALSE;
              }
            }
            // For small/medium Clobs and drivers which do not support
            // ps.setCharacterStream
            final String str = clob.getSubString(1, (int)len);
            ps.setString(paramIndex, str);
          }
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.DATE:
        final java.sql.Date dateVal = row.getDate(rowPosition);
        if (dateVal != null) {
          ps.setDate(paramIndex, dateVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.DECIMAL:
      case Types.NUMERIC:
        final BigDecimal decimalVal = row.getBigDecimal(rowPosition);
        if (decimalVal != null) {
          ps.setBigDecimal(paramIndex, decimalVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.DOUBLE:
        final double doubleVal = row.getDouble(rowPosition);
        if (!row.wasNull()) {
          ps.setDouble(paramIndex, doubleVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.FLOAT:
      case Types.REAL:
        final float floatVal = row.getFloat(rowPosition);
        if (!row.wasNull()) {
          ps.setFloat(paramIndex, floatVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.INTEGER:
      case Types.SMALLINT:
      case Types.TINYINT:
        final int intVal = row.getInt(rowPosition);
        if (!row.wasNull()) {
          ps.setInt(paramIndex, intVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.LONGNVARCHAR:
      case Types.LONGVARCHAR:
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.SQLXML:
        final String strVal = row.getString(rowPosition);
        if (strVal != null) {
          ps.setString(paramIndex, strVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.BINARY:
      case Types.LONGVARBINARY:
      case Types.VARBINARY:
        final byte[] bytesVal = row.getBytes(rowPosition);
        if (bytesVal != null) {
          ps.setBytes(paramIndex, bytesVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.TIME:
        final java.sql.Time timeVal = row.getTime(rowPosition);
        if (timeVal != null) {
          ps.setTime(paramIndex, timeVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      case Types.TIMESTAMP:
        final java.sql.Timestamp timestampVal = row.getTimestamp(rowPosition);
        if (timestampVal != null) {
          ps.setTimestamp(paramIndex, timestampVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        ps.setTimestamp(paramIndex, row.getTimestamp(rowPosition));
        break;
      case Types.JAVA_OBJECT:
        final Object objVal = row.getObject(rowPosition);
        if (objVal != null) {
          ps.setObject(paramIndex, objVal);
        }
        else {
          ps.setNull(paramIndex, javaSqlType);
        }
        break;
      default:
        throw new UnsupportedOperationException("java.sql.Type = "
            + javaSqlType + " not supported");
    }
  }

  /**
   * Set the key column values in {@link PreparedStatement} for a primary key
   * based update or delete operation.
   */
  private void setKeysInPrepStatement(ResultSet pkRow,
      ResultSetMetaData pkMeta, PreparedStatement ps, int startIndex)
      throws SQLException {
    final int numKeyCols = pkMeta.getColumnCount();
    for (int colIndex = 1; colIndex <= numKeyCols; colIndex++, startIndex++) {
      setColumnInPrepStatement(pkMeta.getColumnType(colIndex), ps, pkRow,
          colIndex, startIndex);
    }
  }

  /**
   * Get pooled connection
   * 
   * @param holder
   *          the holder to hold the selected pooled connection
   */
  private synchronized void getPooledConnection(ConnectionHolder[] holder) {
    holder[0] = this.availableConnections.poll();
    if (holder[0] == null) {
      this.waitingConnections.add(holder);
      synchronized (connectionCount) {
        if (connectionCount < maxConnections) {
          connectionCount = connectionCount + 1;
          Runnable creator = new ConnectionCreator();
          this.backgroundExecutor.execute(creator);
        }
      }
    }
  }

  /**
   * Return newly created or used connection to the pool, if there is no
   * database operation need the connection. Otherwise put the connection to
   * connection holder and notify the waiting thread.
   * 
   * @param con
   */
  private synchronized void returnPooledConnection(ConnectionHolder con) {
    ConnectionHolder[] holder = this.waitingConnections.poll();
    if (holder == null) {
      this.availableConnections.offer(con);
    } else {
      synchronized (holder) {
        holder[0] = con;
        holder.notify();
      }
    }
  }

  /**
   * Return used connection to the pool.
   * 
   * @param con
   *          the connection to be recycled
   */
  private void recyclePooledConnection(ConnectionHolder con) {
    ConnectionRecycler recycler = new ConnectionRecycler(con);
    this.backgroundExecutor.execute(recycler);
  }

  /**
   * Close the connection.
   * 
   * @param con
   *          the connection to be closed.
   */
  private void releasePooledConnection(ConnectionHolder con) {
    ConnectionReleaser releaser = new ConnectionReleaser(con);
    this.backgroundExecutor.execute(releaser);
  }

  private static String getStackTrace(Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    return sw.toString();
  }

  private static final class ConnectionHolder {
    private final Connection con;
    private final HashMap<String, PreparedStatement> pstmtMap;

    public ConnectionHolder(Connection c) {
      this.con = c;
      this.pstmtMap = new HashMap<String, PreparedStatement>();
    }

    public PreparedStatement getPreparedStatement(String query)
        throws SQLException {
      PreparedStatement pstmt = this.pstmtMap.get(query);
      if (pstmt != null) {
        return pstmt;
      }
      pstmt = this.con.prepareStatement(query);
      this.pstmtMap.put(query, pstmt);
      return pstmt;
    }

    public void close(Logger log) {
      for (PreparedStatement pstmt : this.pstmtMap.values()) {
        try {
          pstmt.close();
        } catch (SQLException e) {
          log.warning(getStackTrace(e));
        }
      }
      try {
        con.close();
      } catch (SQLException e) {
        log.warning(getStackTrace(e));
      }
    }
  }

  private class ConnectionCreator implements Runnable {
    public void run() {
      try {
        Connection con = getDatabaseConnection();
        log.info(" Successful connection to target database: " + url);
        recyclePooledConnection(new ConnectionHolder(con));
      } catch (SQLException e) {
        synchronized (connectionCount) {
          connectionCount = connectionCount - 1;
        }
        log.severe("Error connecting to target database");
        log.severe(getStackTrace(e));
      }
    }
  }

  private class ConnectionRecycler implements Runnable {
    private final ConnectionHolder con;

    public ConnectionRecycler(ConnectionHolder con) {
      this.con = con;
    }

    public void run() {
      try {
        returnPooledConnection(con);
      } catch (Exception e) {
        releasePooledConnection(con);
        log.warning(getStackTrace(e));
      }
    }
  }

  private class ConnectionReleaser implements Runnable {
    private final ConnectionHolder con;

    public ConnectionReleaser(ConnectionHolder con) {
      this.con = con;
    }

    public void run() {
      synchronized (connectionCount) {
        connectionCount = connectionCount - 1;
      }
      con.close(log);
    }
  }

  /**
   * Put the SQLException information in the log file
   * 
   * @param e
   *          the SQLException
   */
  private void logGetRowError(SQLException e) {
    log.severe("Error executing prepared statement in EventCallbackWriterImpl");
    log.severe(getStackTrace(e));
  }
}

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
package com.pivotal.gemfirexd.ddl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheFactory;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventCallback;

public class EventCallbackListener implements EventCallback {
  private static final int VENDOR_CODE_ARCHIVE_ERROR = 0;
  private static final int VENDOR_CODE_TIMEOUT = 1;
  protected Properties props = new Properties();
  protected String url = "";
  protected String primaryKeys = "";
  protected int minConnections;
  protected int maxConnections;
  protected long connectionTimeout;
  private Integer connectionCount = 0;
  private final LogWriter log = CacheFactory.getAnyInstance().getLogger();
  private final ExecutorService backgroundExecutor = Executors
      .newCachedThreadPool();
  private final Queue<Connection[]> waitingConnections = new LinkedList<Connection[]>();
  private final Queue<Connection> availableConnections = new LinkedList<Connection>();

  @Override
  public void close() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void init(String initStr) throws SQLException {
    // TODO Auto-generated method stub
    log.entering("EventCallbackListener", "init()");
    loadParametersFromInitString(initStr);
  }

  @Override
  public void onEvent(Event event) throws SQLException {
    // TODO Auto-generated method stub
    if ((event.getType() == Event.Type.AFTER_UPDATE)) {
      if (connectionCount < minConnections) {
        initConnection();
      }
      String query = buildUpdateQuery(event);
      PreparedStatement pstmt = getPreparedStatement(query);
      executePreparedStatement(pstmt);
      log.info("Complete updating a row in the backend database.");
    } else if ((event.getType() == Event.Type.AFTER_INSERT)) {
      if (connectionCount < minConnections) {
        initConnection();
      }
      String query = buildInsertQuery(event);
      PreparedStatement pstmt = getPreparedStatement(query);
      executePreparedStatement(pstmt);
      log.info("Complete inserting a row in the backend database.");
    } else if ((event.getType() == Event.Type.AFTER_DELETE)) {
      if (connectionCount < minConnections) {
        initConnection();
      }
      String query = buildDeleteQuery(event);
      PreparedStatement pstmt = getPreparedStatement(query);
      executePreparedStatement(pstmt);
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
    if (log.infoEnabled()) {
      log.info("EventCallbackListener initialized.");
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
   */
  private void loadParametersFromInitString(String initStr) {
    parsePropertiesFromString(initStr);
    logInitParameters();
    this.url = getProperty("url", "");
    this.primaryKeys = getProperty("primary-keys", "");
    this.minConnections = Integer.parseInt(getProperty("min-connections", "1"));
    this.maxConnections = Integer.parseInt(getProperty("max-connections", "1"));
    this.connectionTimeout = Long.parseLong(getProperty("connection-timeout",
        "3000"));
  }

  /**
   * Get database connection
   * 
   * @return the connection object for the database connection
   * @throws SQLException
   */
  private Connection getDatabaseConnection() throws SQLException {
    //return DriverManager.getConnection(url, props);
    return TestUtil.getConnection();
  }

  /**
   * Build the query string for updating the backend database
   * 
   * @param event
   *          the callback event
   * @return SQL update query to the backend database
   * @throws SQLException
   */
  private String buildUpdateQuery(Event event) throws SQLException {

    ResultSetMetaData meta = event.getResultSetMetaData();
    int[] modifiedCols = event.getModifiedColumns();
    List<Object> newRow = event.getNewRow();

    StringBuilder query = new StringBuilder();
    if (event.getModifiedColumns() == null) {
      throw new SQLException("Nothing is updated.");
    }

    // query.append("UPDATE " + meta.getSchemaName(1) + "." +
    // meta.getTableName(1));
    query.append("UPDATE " + meta.getSchemaName(1) + "." + meta.getTableName(1) + "_ONE");
    query.append(" SET ");

    for (int i = 0; i < modifiedCols.length; i++) {
      query.append(meta.getColumnName(modifiedCols[i]) + "=");
      int type = meta.getColumnType(modifiedCols[i]);
      Object value = newRow.get(modifiedCols[i] - 1);

      switch (type) {
      case Types.BIGINT:
      case Types.DECIMAL:
      case Types.NUMERIC:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.INTEGER:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.REAL:
      case Types.TIMESTAMP:
        query.append(value + ",");
        break;
      default:
        query.append("'" + value + "',");
      }
    }

    query.delete(query.length() - 1, query.length());

    // add where clause "where pkName1=pkValue1 and pkName2=pkValue2 ... ;"

    Object[] pkValue = event.getPrimaryKey();

    String[] keys = primaryKeys.split(",");
    if (keys.length > 0) {
      query.append(" WHERE ");
      for (int i = 0; i < keys.length; i++) {
        String keyName = keys[i];
        query.append(keyName).append("=");
        if (pkValue[i] instanceof String) {
          query.append("'" + pkValue[i] + "'");
        } else {
          query.append(pkValue[i]);
        }
        if (i < keys.length - 1) {
          query.append(" AND ");
        }
      }
    }

    return query.toString();
  }

  /**
   * Build the query string to insert a row to the backend database
   * 
   * @param event
   *          the callback event
   * @return SQL query string to insert a row to the back-end database
   * @throws SQLException
   */
  private String buildInsertQuery(Event event) throws SQLException {

    ResultSetMetaData meta = event.getResultSetMetaData();

    List<Object> newRow = event.getNewRow();

    StringBuilder query = new StringBuilder();

    // insert into table_name values (...); assume
    // Note: insert into table_name(col1, col2 ...) values (...) is not
    // supported here
    //query.append("INSERT INTO " + meta.getSchemaName(1) + "."
    //    + meta.getTableName(1) + " VALUES (");
    query.append("INSERT INTO " + meta.getSchemaName(1) + "." + meta.getTableName(1) + "_ONE VALUES (");

    for (int i = 1; i <= meta.getColumnCount(); i++) {

      int type = meta.getColumnType(i);

      Object value = newRow.get(i - 1);

      switch (type) {
      case Types.BIGINT:
      case Types.DECIMAL:
      case Types.NUMERIC:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.INTEGER:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.REAL:
      case Types.TIMESTAMP:
        query.append(value + ",");
        break;
      default:
        query.append("'" + value + "',");
      }
    }

    query.delete(query.length() - 1, query.length());

    query.append(");");

    return query.toString();

  }

  /**
   * Build querying string to delete a row in the backend database
   * 
   * @param event
   *          the callback event
   * @return SQL query string to delete a row in the backend database
   * @throws SQLException
   */
  private String buildDeleteQuery(Event event) throws SQLException {

    ResultSetMetaData meta = event.getResultSetMetaData();

    StringBuilder query = new StringBuilder();

    //query.append("DELETE FROM " + meta.getSchemaName(1) + "."
     //   + meta.getTableName(1));
    query.append("DELETE FROM " + meta.getSchemaName(1) + "." + meta.getTableName(1) + "_ONE");

    // add where clause: "where pkName1=pkValue1 and pkName2=pkValue2 ... ;"

    Object[] pkValue = event.getPrimaryKey();

    String[] keys = primaryKeys.split(",");
    if (keys.length > 0) {
      query.append(" WHERE ");
      for (int i = 0; i < keys.length; i++) {
        String keyName = keys[i];
        query.append(keyName).append("=");
        if (pkValue[i] instanceof String) {
          query.append("'" + pkValue[i] + "'");
        } else {
          query.append(pkValue[i]);
        }
        if (i < keys.length - 1) {
          query.append(" AND ");
        }
      }
    }

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
   * @return PreparedStatement from the connection retrieved from connection
   *         pool
   * @throws SQLException
   */
  private PreparedStatement getPreparedStatement(String query)
      throws SQLException {
    Connection[] holder = new Connection[1];
    synchronized (holder) {
      getPooledConnection(holder);
      if (holder[0] == null) {
        try {
          holder.wait(connectionTimeout);
        } catch (InterruptedException e) {
          log
              .warning("JDBCRowLoader interrupted while waiting for an available pooled statement.");
          log.warning(e);
          Thread.currentThread().interrupt();
        }
      } // if
    } // synchronized
    if (holder[0] == null) {
      throw new SQLException(
          "Timeout waiting for pooled connection to archive database", "08001",
          VENDOR_CODE_TIMEOUT);
    }
    PreparedStatement pstmt = holder[0].prepareStatement(query);
    return pstmt;
  }

  /**
   * Execute the given PreparedStatement
   * 
   * @param pstmt
   *          the PreparedStatement to be executed
   * @throws SQLException
   */
  private void executePreparedStatement(PreparedStatement pstmt)
      throws SQLException {
    try {
      log.info("Executing query " + pstmt.toString());
      pstmt.executeUpdate();
      log.info("Query succeeded");
      Connection con = pstmt.getConnection();
      recyclePooledConnection(con);
    } catch (SQLException e) {
      Connection con = pstmt.getConnection();
      releasePooledConnection(con);
      logGetRowError(e);
      throw new SQLException("Error executing query from archive database", e
          .getSQLState(), VENDOR_CODE_ARCHIVE_ERROR, e);
    } // catch
  }

  /**
   * Get pooled connection
   * 
   * @param holder
   *          the holder to hold the selected pooled connection
   */
  private synchronized void getPooledConnection(Connection[] holder) {
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
  private synchronized void returnPooledConnection(Connection con) {
    Connection[] holder = this.waitingConnections.poll();
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
  private void recyclePooledConnection(Connection con) {
    ConnectionRecycler recycler = new ConnectionRecycler(con);
    this.backgroundExecutor.execute(recycler);
  }

  /**
   * Close the connection.
   * 
   * @param con
   *          the connection to be closed.
   */
  private void releasePooledConnection(Connection con) {
    ConnectionReleaser releaser = new ConnectionReleaser(con);
    this.backgroundExecutor.execute(releaser);
  }

  private class ConnectionCreator implements Runnable {
    public void run() {
      if (url.isEmpty()) {
        log.error("Connection url not provided for JDBCRowLoader");
        return;
      }

      try {
        Connection con = getDatabaseConnection();
        log.info(" Successful connection to target database: " + url);
        recyclePooledConnection(con);
      } catch (SQLException e) {
        synchronized (connectionCount) {
          connectionCount = connectionCount - 1;
        }
        log.error("Error connecting to target database");
        log.error(e);
      }
    }
  }

  private class ConnectionRecycler implements Runnable {
    private Connection con;

    public ConnectionRecycler(Connection con) {
      this.con = con;
    }

    public void run() {
      try {
        returnPooledConnection(con);
      } catch (Exception e) {
        releasePooledConnection(con);
        log.warning(e);
      }
    }
  }

  private class ConnectionReleaser implements Runnable {
    private Connection con;

    public ConnectionReleaser(Connection con) {
      this.con = con;
    }

    public void run() {
      try {
        synchronized (connectionCount) {
          connectionCount = connectionCount - 1;
        }
        con.close();
      } catch (SQLException e) {
        log.warning(e);
      }
    }
  }

  /**
   * Put the SQLException information in the log file
   * 
   * @param e
   *          the SQLException
   */
  private void logGetRowError(SQLException e) {
    log.error("Error executing prepared statement in JDBCRowLoader");
    log.error(e);

  }


}

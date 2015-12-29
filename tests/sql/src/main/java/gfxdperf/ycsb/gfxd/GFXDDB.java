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
package gfxdperf.ycsb.gfxd;

import com.gemstone.gemfire.distributed.DistributedSystem;

import gfxdperf.PerfTestException;
import gfxdperf.ycsb.core.ByteIterator;
import gfxdperf.ycsb.core.DB;
import gfxdperf.ycsb.core.DBException;
import gfxdperf.ycsb.core.LongByteIterator;
import gfxdperf.ycsb.core.StringByteIterator;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms;
import gfxdperf.ycsb.core.workloads.CoreWorkloadStats;
import gfxdperf.ycsb.gfxd.GFXDPrms.ConnectionType;

import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.LonerHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * Client for measuring GFXD YCSB performance with GemFireXD.
 * <p> 
 * This class expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key.
 * Only one index on the primary key is needed.
 */
public class GFXDDB extends DB {

  public static final String PEER_DRIVER = "com.pivotal.gemfirexd.jdbc.EmbeddedDriver";
  public static final String THIN_DRIVER = "com.pivotal.gemfirexd.jdbc.ClientDriver";
  public static final String PROTOCOL = "jdbc:gemfirexd:";
  public static final boolean logDML = GFXDPrms.logDML();

  /** Prefix for each column in the table */
  public static String COLUMN_PREFIX = "field";

  /** Primary key column in the table */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  protected ConcurrentMap<StatementType, PreparedStatement> cachedStatements =
            new ConcurrentHashMap();
  protected Connection connection;
  private DistributedSystem distributedSystem;
  private CoreWorkloadStats statistics;
  protected long lastQueryPlanTime;

  /**
   * Imported from original GemFireXDClient
   */
  private static final String FIELD2 = COLUMN_PREFIX + 2;
  private static final String FIELD3 = COLUMN_PREFIX + 3;
  private boolean generateQueryData = false;

  /**
   * The statement type for the prepared statements.
   */
  private static class StatementType {
    
    enum Type {
      INSERT(1),
      DELETE(2),
      READ(3),
      UPDATE(4),
      SCAN(5),
      QUERY_WITH_FILTER(6),
      QUERY_WITH_AGGREGATE(7),
      QUERY_WITH_JOIN(8)
      ;
      int internalType;
      private Type(int type) {
        internalType = type;
      }
      
      int getHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + internalType;
        return result;
      }
    }
    
    Type type;
    int numFields;
    String tableName;
    String table2;
    
    StatementType(Type type, String tableName, int numFields) {
      this.type = type;
      this.tableName = tableName;
      this.numFields = numFields;
    }

    StatementType(Type type, String tableName, String table2) {
      this.type = type;
      this.tableName = tableName;
      this.table2 = table2;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + numFields;
      result = prime * result
          + ((tableName == null) ? 0 : tableName.hashCode());
      result = prime * result + ((type == null) ? 0 : type.getHashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      StatementType other = (StatementType)obj;
      if (type != other.type) {
        return false;
      }
      if (numFields != other.numFields) {
        return false;
      }
      if (tableName == null) {
        if (other.tableName != null)
          return false;
      } else if (!tableName.equals(other.tableName)) {
        return false;
      }
      return true;
    }
  }

  @Override
  public void init() throws DBException {
    ConnectionType type = GFXDPrms.getConnectionType();
    if (type == ConnectionType.thin) {
      this.connection = initConnection(type);
      this.distributedSystem = LonerHelper.connect(); // for statistics
    } else {
      FabricServerHelper.startFabricServer();
      this.connection = initConnection(type);
      this.distributedSystem = DistributedSystemHelper.getDistributedSystem();
    }
    this.statistics = CoreWorkloadStats.getInstance();
  }

  private Connection initConnection(ConnectionType type) throws DBException {
    try {
      return GFXDUtil.openConnection(type);
    } catch (SQLException e) {
      String s = "Unable to create " + type + " client connection";
      throw new DBException(s, e);
    }
  }

  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public void cleanup() throws DBException {
    Log.getLogWriter().info("Closing connection " + this.connection);
    if (this.connection == null) {
      Log.getLogWriter().info("Connection already closed");
    } else {
      try {
        this.connection.close();
        this.connection = null;
        Log.getLogWriter().info("Closed connection");
      } catch (SQLException e) {
        if (e.getSQLState().equalsIgnoreCase("X0Z01") && GFXDPrms.isHA()) {
          Log.getLogWriter().info("Connection already closed at server end");
        } else {
          throw new DBException("Problem closing connection", e);
        }
      }
    }
    if (this.statistics != null) {
      this.statistics.close();
      this.statistics = null;
    }
    ConnectionType type = GFXDPrms.getConnectionType();
    if (type == ConnectionType.peer) {
      FabricServerHelper.stopFabricServer();
    }
    if (this.distributedSystem != null) {
      this.distributedSystem.disconnect();
      this.distributedSystem = null;
    }
  }

//------------------------------------------------------------------------------
// DB

  @Override
  public int read(String tableName, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (logDML) {
      Log.getLogWriter().info("Reading key=" + key);
    }
    long start = this.statistics.startRead();
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, 1);
      PreparedStatement readStatement = this.cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type, key);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        resultSet = null;
        String s = "No results reading key=" + key + " from " + tableName;
        throw new PerfTestException(s);
      }
      if (fields == null) {
        // read all fields
        int fieldcount = CoreWorkloadPrms.getFieldCount();
        String value = resultSet.getString(PRIMARY_KEY);
        result.put(PRIMARY_KEY, new StringByteIterator(value));
        for (int i = 0; i < fieldcount; i++) {
          String field = "FIELD" + i;
          value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      } else {
        for (String field : fields) {
          String value = resultSet.getString(field);
          result.put(field, new StringByteIterator(value));
        }
      }
      resultSet.close();
      resultSet = null;
    } catch (SQLException e) {
      String s = "Error reading key=" + key + " from " + tableName;
      throw new PerfTestException(s, e);
    }
    this.statistics.endRead(start, 1);
    if (logDML) {
      Log.getLogWriter().info("Read key=" + key);
    }
    return 0;
  }
  
  @Override
  public int update(String tableName, String key, HashMap<String, ByteIterator> values) {
    if (logDML) {
      Log.getLogWriter().info("Updating key=" + key);
    }
    long start = this.statistics.startUpdate();
    try {
      int numFields = values.size();
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, numFields);
      PreparedStatement updateStatement = this.cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type, key, values.keySet());
      }
      int i = 1;
      for (String field : values.keySet()) {
        ByteIterator bytes = values.get(field);
        if (generateQueryData && (field.equals(FIELD2) || field.equals(FIELD3))) {
          updateStatement.setLong(i, ((LongByteIterator) bytes).toLong());
        } else {
          updateStatement.setString(i, bytes.toString());
        }
        ++i;
      }
      updateStatement.setString(i, key);
      int result = updateStatement.executeUpdate();
      if (result != 1) {
        String s = "Unexpected result updating key=" + key + " in " + tableName                  + ": " + result;
        throw new PerfTestException(s);
      }
    } catch (SQLException e) {
      String s = "Error updating key=" + key + " in " + tableName;
      throw new PerfTestException(s, e);
    }
    this.statistics.endUpdate(start, 1);
    if (logDML) {
      Log.getLogWriter().info("Updated key=" + key);
    }
    return 0;
  }

  @Override
  public int insert(String tableName, String key, HashMap<String, ByteIterator> values) {
    if (logDML) {
      Log.getLogWriter().info("Inserting key=" + key);
    }
    long start = this.statistics.startInsert();
    try {
      int numFields = values.size();
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, numFields);
      PreparedStatement insertStatement = this.cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type, key);
      }
      insertStatement.setString(1, key);
      for (int i = 0; i < numFields; i++) {
        ByteIterator bytes = values.get(COLUMN_PREFIX + i);
        if (generateQueryData && (i == 2 || i == 3)) {
          insertStatement.setLong(i + 2, ((LongByteIterator) bytes).toLong());
        } else {
          insertStatement.setString(i + 2, bytes.toString());
        }
      }
      int result = insertStatement.executeUpdate();
      if (result != 1) {
        String s = "Failed inserting key=" + key + " in " + tableName;
        throw new PerfTestException(s);
      }
    } catch (SQLException e) {
      String s = "Error inserting key=" + key + " in " + tableName;
      throw new PerfTestException(s, e);
    }
    this.statistics.endInsert(start, 1);
    if (logDML) {
      Log.getLogWriter().info("Inserted key=" + key);
    }
    return 0;
  }

  @Override
  public int delete(String tableName, String key) {
    if (logDML) {
      Log.getLogWriter().info("Deleting key=" + key);
    }
    long start = this.statistics.startDelete();
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, 1);
      PreparedStatement deleteStatement = this.cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type, key);
      }
      deleteStatement.setString(1, key);
      int result = deleteStatement.executeUpdate();
      if (result != 1) {
        String s = "Failed deleting key=" + key + " from " + tableName;
        throw new PerfTestException(s);
      }
    } catch (SQLException e) {
      String s = "Error deleting key=" + key + " from " + tableName;
      throw new PerfTestException(s, e);
    }
    this.statistics.endDelete(start, 1);
    if (logDML) {
      Log.getLogWriter().info("Deleted key=" + key);
    }
    return 0;
  }
  
  private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
  throws SQLException {
    StringBuilder insert;
    if (GFXDPrms.usePutDML()) {
      insert = new StringBuilder("PUT INTO ");
    } else {
      insert = new StringBuilder("INSERT INTO ");
    }
    insert.append(insertType.tableName);
    insert.append(" VALUES(?");
    for (int i = 0; i < insertType.numFields; i++) {
      insert.append(",?");
    }
    insert.append(");");
    PreparedStatement insertStatement = this.connection.prepareStatement(insert.toString());
    PreparedStatement stmt = this.cachedStatements.putIfAbsent(insertType, insertStatement);
    if (stmt == null) return insertStatement;
    else return stmt;
  }
  
  private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
  throws SQLException {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(readType.tableName);
    if (GFXDPrms.queryHDFS()) {
      read.append(" --GEMFIREXD-PROPERTIES queryHDFS=true \n");
    }
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ?;");
    PreparedStatement readStatement = this.connection.prepareStatement(read.toString());
    PreparedStatement stmt = this.cachedStatements.putIfAbsent(readType, readStatement);
    if (stmt == null) return readStatement;
    else return stmt;
  }
  
  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
  throws SQLException {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.tableName);
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?;");
    PreparedStatement deleteStatement = this.connection.prepareStatement(delete.toString());
    PreparedStatement stmt = this.cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (stmt == null) return deleteStatement;
    else return stmt;
  }
  
  private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key, Set<String> fields)
  throws SQLException {
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.tableName);
    update.append(" SET ");
    int i = 1;
    for (String field : fields) {
      update.append(field);
      update.append("=?");
      if (i < updateType.numFields) update.append(", ");
      ++i;
    }
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?;");
    PreparedStatement updateStatement = this.connection.prepareStatement(update.toString());
    PreparedStatement stmt = this.cachedStatements.putIfAbsent(updateType, updateStatement);
    if (stmt == null) return updateStatement;
    else return stmt;
  }
  
  private PreparedStatement createAndCacheQueryWithFilterStatement(StatementType query)
  throws SQLException {
    StringBuilder select = new StringBuilder("SELECT * FROM ");
    select.append(query.tableName);
    select.append(" u WHERE u.field0 = ? AND u.field2 > ? AND u.field2 < ?");
    select.append(" FETCH FIRST ? ROWS ONLY;");
    PreparedStatement queryStatement = this.connection.prepareStatement(select.toString());
    PreparedStatement stmt = this.cachedStatements.putIfAbsent(query, queryStatement);
    if (stmt == null) return queryStatement;
    else return stmt;
  }

  private PreparedStatement createAndCacheQueryWithAggregateStatement(StatementType query)
  throws SQLException {
    StringBuilder select = new StringBuilder("SELECT u.field1, sum(u.field3) FROM ");
    select.append(query.tableName);
    select.append(" u WHERE u.field0 = ? AND u.field2 > ? AND u.field2 < ?");
    select.append(" GROUP BY u.field1 ORDER BY u.field1");
    select.append(" FETCH FIRST ? ROWS ONLY;");
    PreparedStatement queryStatement = this.connection.prepareStatement(select.toString());
    PreparedStatement stmt = this.cachedStatements.putIfAbsent(query, queryStatement);
    if (stmt == null) return queryStatement;
    else return stmt;
  }
 
  private PreparedStatement createAndCacheQueryWithJoinStatement(StatementType query)
  throws SQLException {
    StringBuilder select = new StringBuilder("SELECT u.ycsb_key, v.ycsb_key, u.field1, v.field1 FROM ");
    select.append(query.tableName);
    select.append(" u JOIN ");
    select.append(query.table2);
    select.append(" v ON u.ycsb_key = v.field4");
    select.append(" WHERE u.field0 = ? AND u.field2 > ? AND u.field2 < ?");
    select.append(" ORDER BY u.field1, v.field0");
    select.append(" FETCH FIRST ? ROWS ONLY;");
    PreparedStatement queryStatement = this.connection.prepareStatement(select.toString());
    PreparedStatement stmt = this.cachedStatements.putIfAbsent(query, queryStatement);
    if (stmt == null) return queryStatement;
    else return stmt;
  }
}

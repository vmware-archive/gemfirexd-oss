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
package com.pivotal.gemfirexd.dataawareprocedure;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.internal.cache.TXId;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.ProcedureExecutionContextImpl;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

import io.snappydata.test.dunit.VM;

@SuppressWarnings("serial")
public class ProcedureTest2DUnit extends DistributedSQLTestBase {

  public ProcedureTest2DUnit(String name) {
    super(name);
  }

  private static String[] colocatedTableNames;
  private static String filter;
  private static String procedureName;
  private static String tableName;
  
  public static void setStaticValues(String[] ctns, String filt,
      String procName, String tabName) {
    getGlobalLogger().info(
        "setStaticValues called with ctns: " + ctns + ", filt: " + filt
            + ", procName: " + procName + ", tabName: " + tabName);
    colocatedTableNames = ctns;
    filter = filt;
    procedureName = procName;
    tableName = tabName;
  }

  private static void checkPECsAPIs(ProcedureExecutionContext pec) {
    String[] colnames = pec.getColocatedTableNames();
    
    getGlobalLogger().info(
        "static Values and corresponding from contexts are ctns: "
            + colocatedTableNames + ", from context: "
            + pec.getColocatedTableNames() + ", filt: " + filter
            + " from context: " + pec.getFilter() + ", procName: "
            + procedureName + " from context: " + pec.getProcedureName()
            + ", tabName: " + tableName + " from context: "
            + pec.getTableName());
    if (colocatedTableNames == null) {
      assertNull(colnames);
    }
    else {
      int len = colnames.length;
      assertEquals(len, colocatedTableNames.length);
      for(int i=0; i<len; i++) {
        String name = colnames[i];
        boolean found = false;
        for(int j=0; j<len; j++) {
          if(name.equalsIgnoreCase(colocatedTableNames[j])) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }
    }

    String filt = pec.getFilter();
    if (filt == null) {
      assertNull(filter);
    }
    else {
      assertTrue(filt.equalsIgnoreCase(filter));
    }
    
    String procName = pec.getProcedureName();
    if (procName == null) {
      assertNull(procedureName);
    }
    else {
      assertTrue(procName.equalsIgnoreCase(procedureName));
    }

    String tabName = pec.getTableName();
    if (tabName == null) {
      assertNull(tableName);
    }
    else {
      assertTrue(tabName.equalsIgnoreCase(tableName));
    }
  }

  public static void checkPECsAPIs_one(int number, ResultSet[] rs1,
      ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4,
      ProcedureExecutionContext pec) throws SQLException {
    assertTrue(pec.isPartitioned("DAP.T1_PARTITIONED"));
    assertFalse(pec.isPartitioned("DAP.T2_REPLICATE"));
    
    String userName = pec.getConnection().getMetaData().getUserName();
    getGlobalLogger().info("checkPECsAPIs_one username: " + userName);
    
    //assertTrue(pec.isPartitioned(userName+".t1_partitioned"));
    //assertFalse(pec.isPartitioned(userName+".t2_replicate"));
    checkPECsAPIs(pec);
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    /** get a new connection with default properties */
    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn.prepareCall(sql);   
    return cs;
  }

  public void testExecuteOnServerGroupProcedureCall() throws Exception {
    //startServerVMs(3, 0, "sg1");
    startServerVMs(1, 0, "SG2");
    //startServerVMs(1, 0, null);
    startClientVMs(1, 0, null);
    
    VM vm = this.serverVMs.get(0);
    vm.invoke(ProcedureTest2DUnit.class, "setStaticValues", new Object[] { null,
        null, "checkPECsAPIs_one", null });
    
    clientSQLExecute(1, "CREATE schema dap");
    
    clientSQLExecute(1, "CREATE table dap.t1_partitioned(field1 int not null primary key, field2 varchar(20) not null)");
    clientSQLExecute(1, "CREATE table dap.t2_replicate(field1 int not null primary key, field2 varchar(20) not null) replicate");
    
    clientSQLExecute(1, "CREATE table t1_partitioned(field1 int not null primary key, field2 varchar(20) not null)");
    clientSQLExecute(1, "CREATE table t2_replicate(field1 int not null primary key, field2 varchar(20) not null) replicate");
    
    clientSQLExecute(1, "CREATE PROCEDURE proc_num_one(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest2DUnit.class.getName() + ".checkPECsAPIs_one' "
        + "DYNAMIC RESULT SETS 4");
    CallableStatement cs = prepareCall(
        "CALL proc_num_one(?) ON SERVER GROUPS (sg2)");
    cs.setInt(1, 3);

    cs.execute();
  }

  private static final int onTable = 0;
  private static final int onAll = 1;
  private static final int onServerGroups = 2;
  private static final int noOnClause = 3;
  public static final int onTableNoWhereClause = 5;
  
  private static String getInvocationType(int invocationType) {
    switch(invocationType) {
      case onTable:
        return "onTable";
        
      case onAll:
        return "onAll";
        
      case onServerGroups:
        return "onServerGroups";
        
      case noOnClause:
        return "noOnClause";
        
      case onTableNoWhereClause:
        return "onTableNoWhereClause";
        
        default:
          return "unknown";
    }
  }
  
  public void testExecuteOnTableWithWhereClauseAndLocalAndGlobalEscape() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2");

    clientSQLExecute(1, 
        "insert into emp.partitiontesttable values (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')");
    
    clientSQLExecute(1, "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest2DUnit.class.getName() + ".select_proc' "
        + "DYNAMIC RESULT SETS 4");

    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn.prepareCall(
        "CALL MY_ESCAPE_SELECT(?) ON TABLE EMP.PARTITIONTESTTABLE where ID >= 20 and ID < 40");
    cs.setInt(1, onTable);
    cs.execute();
    
    ResultSet rs;
    int escapeSeq = 0;
    do {
      rs = cs.getResultSet();
      getLogWriter().info("result set no: "+escapeSeq);
      getLogWriter().info("*********************");
      int totalRows = 0;
      Set<String> rows = new HashSet<String>();
      while(rs.next()) {
        String row = rs.getInt(1)+", "+rs.getString(2);
        getLogWriter().info(row);
        totalRows++;
        rows.add(row);
      }
      getLogWriter().info("*********************");

      switch(escapeSeq) {
        case 0:
          assertEquals(2, totalRows);
          assertEquals(2, rows.size());
          break;
          
        case 1:
          assertEquals(2, totalRows);
          assertEquals(2, rows.size());
          break;
          
        case 2:
          assertEquals(2, totalRows);
          assertEquals(2, rows.size());
          break;
          
          default:
            break;
      }

      escapeSeq++;
    } while(cs.getMoreResults());
  }

  public void testExecuteOnTableWithNoWhereClauseAndLocalAndGlobalEscape() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2");

    clientSQLExecute(1, 
        "insert into emp.partitiontesttable values (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')");
    
    clientSQLExecute(1, "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest2DUnit.class.getName() + ".select_proc' "
        + "DYNAMIC RESULT SETS 4");

    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn.prepareCall(
        "CALL MY_ESCAPE_SELECT(?) ON TABLE EMP.PARTITIONTESTTABLE");
    cs.setInt(1, onTableNoWhereClause);
    cs.execute();
    
    ResultSet rs;
    int escapeSeq = 0;
    do {
      rs = cs.getResultSet();
      getLogWriter().info("result set no: "+escapeSeq);
      getLogWriter().info("*********************");
      int totalRows = 0;
      Set<String> rows = new HashSet<String>();
      while(rs.next()) {
        String row = rs.getInt(1)+", "+rs.getString(2);
        getLogWriter().info(row);
        totalRows++;
        rows.add(row);
      }
      getLogWriter().info("*********************");

      switch(escapeSeq) {
        case 0:
          assertEquals(12, totalRows);
          assertEquals(4, rows.size());
          break;
          
        case 1:
          assertEquals(4, totalRows);
          assertEquals(4, rows.size());
          break;
          
        case 2:
          assertEquals(12, totalRows);
          assertEquals(4, rows.size());
          break;
          
          default:
            break;
      }

      escapeSeq++;
    } while(cs.getMoreResults());
  }
  
  public static void select_proc(int invocationType, ResultSet[] rs1,
      ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4,
      ProcedureExecutionContext pec) throws SQLException {
    getGlobalLogger().info(
        "select_proc called with " + "invocation type: "
            + getInvocationType(invocationType) + " and pec: " + pec);
    ProcedureExecutionContextImpl cimpl = (ProcedureExecutionContextImpl)pec;
    Connection c = DriverManager.getConnection("jdbc:default:connection");
    getGlobalLogger().info("from pec tablename is: " + pec.getTableName());
    if (cimpl.getTableName() != null) {
      assertTrue(cimpl.getTableName().equalsIgnoreCase("emp.partitiontestTable"));
    }

    String filter = " where " + cimpl.getFilter();
    String sql = "select * from emp.partitiontestTable";
    String local_sql = "";
    String global_sql = "";
    String default_sql = "";

    switch (invocationType) {
      case onTable:
        filter = " where " + cimpl.getFilter();
        sql = sql + " " + filter;
        local_sql = "<LOCAL> " + sql;
        global_sql = "<GLOBAL> " + sql;
        default_sql = sql;
        break;

      case onAll:
      case onServerGroups:
      case noOnClause:
      case onTableNoWhereClause:
        local_sql = "<LOCAL> " + sql;
        global_sql = "<GLOBAL> " + sql;
        default_sql = sql;
        break;
        
      default:
        break;
    }
    getGlobalLogger().info(
        "select_proc def sql is: " + sql + " local sql is: " + local_sql
            + " and global sql is: " + global_sql);

    rs1[0] = c.createStatement().executeQuery(default_sql);
    rs2[0] = c.createStatement().executeQuery(local_sql);
    rs3[0] = c.createStatement().executeQuery(global_sql);
  }
  
  public void testExecuteOnAllAndLocalAndGlobalEscape() throws Exception {
    startVMs(1, 3);
    serverSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2");

    clientSQLExecute(1, "insert into emp.partitiontesttable values "
        + "(20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')");

    serverSQLExecute(1, "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest2DUnit.class.getName() + ".select_proc' "
        + "DYNAMIC RESULT SETS 4");

    Connection conn;
    CallableStatement cs;
    ResultSet rs;
    int escapeSeq;

    conn = TestUtil.getConnection();
    cs = conn.prepareCall("CALL MY_ESCAPE_SELECT(?) ON ALL");
    cs.setInt(1, onAll);
    cs.execute();

    escapeSeq = 0;
    do {
      rs = cs.getResultSet();
      getLogWriter().info("result set no: " + escapeSeq);
      getLogWriter().info("*********************");
      ResultSetMetaData rsmd = rs.getMetaData();
      getLogWriter().info("metadata: " + rsmd.getColumnCount());
      for (int col = 1; col <= rsmd.getColumnCount(); col++) {
        getLogWriter().info(
            "metadata: " + rsmd.getColumnName(col) + '['
                + rsmd.getColumnType(col) + ']');
      }
      int totalRows = 0;
      Set<String> rows = new HashSet<String>();
      while (rs.next()) {
        String row = rs.getInt(1) + ", " + rs.getString(2);
        getLogWriter().info(row);
        totalRows++;
        rows.add(row);
      }
      getLogWriter().info("*********************");

      switch (escapeSeq) {
        case 0:
          assertEquals(16, totalRows);
          assertEquals(4, rows.size());
          break;

        case 1:
          assertEquals(4, totalRows);
          assertEquals(4, rows.size());
          break;

        case 2:
          assertEquals(16, totalRows);
          assertEquals(4, rows.size());
          break;

        default:
          break;
      }

      escapeSeq++;
    } while (cs.getMoreResults());
    cs.close();
    conn.close();

    // also test with network client connection
    int clientPort = startNetworkServer(1, null, null);
    startNetworkServer(3, null, null);
    conn = TestUtil.getNetConnection(clientPort, null, null);
    cs = conn.prepareCall("CALL MY_ESCAPE_SELECT(?) ON ALL");
    cs.setInt(1, onAll);
    cs.execute();

    escapeSeq = 0;
    do {
      rs = cs.getResultSet();
      getLogWriter().info("result set no: " + escapeSeq);
      getLogWriter().info("*********************");
      ResultSetMetaData rsmd = rs.getMetaData();
      getLogWriter().info("metadata: " + rsmd.getColumnCount());
      for (int col = 1; col <= rsmd.getColumnCount(); col++) {
        getLogWriter().info(
            "metadata: " + rsmd.getColumnName(col) + '['
                + rsmd.getColumnType(col) + ']');
      }
      int totalRows = 0;
      Set<String> rows = new HashSet<String>();
      while (rs.next()) {
        String row = rs.getInt(1) + ", " + rs.getString(2);
        getLogWriter().info(row);
        totalRows++;
        rows.add(row);
      }
      getLogWriter().info("*********************");

      switch (escapeSeq) {
        case 0:
          assertEquals(16, totalRows);
          assertEquals(4, rows.size());
          break;

        case 1:
          assertEquals(4, totalRows);
          assertEquals(4, rows.size());
          break;

        case 2:
          assertEquals(16, totalRows);
          assertEquals(4, rows.size());
          break;

        default:
          break;
      }

      escapeSeq++;
    } while (cs.getMoreResults());
    cs.close();
    conn.close();
  }
  
  public void testExecuteOnServerGroupsAndLocalAndGlobalEscape() throws Exception {
    startServerVMs(2, 0, "SG2");
    startServerVMs(1, 0, null);
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create schema EMP default server groups (SG2)"); 
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2");

    clientSQLExecute(1, 
        "insert into emp.partitiontesttable values (20, 'r1'), (50, 'r2'), (30, 'r1'), (70, 'r3')");
    
    clientSQLExecute(1, "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest2DUnit.class.getName() + ".select_proc' "
        + "DYNAMIC RESULT SETS 4");

    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn.prepareCall(
        "CALL MY_ESCAPE_SELECT(?) ON server groups (sg2)");
    cs.setInt(1, onServerGroups);
    cs.execute();
    
    ResultSet rs;
    int escapeSeq = 0;
    do {
      rs = cs.getResultSet();
      getLogWriter().info("result set no: "+escapeSeq);
      getLogWriter().info("*********************");
      ResultSetMetaData rsmd = rs.getMetaData();
      getLogWriter().info("metadata: " + rsmd.getColumnCount());
      for (int col = 1; col <= rsmd.getColumnCount(); col++) {
        getLogWriter().info(
            "metadata: " + rsmd.getColumnName(col) + '['
                + rsmd.getColumnType(col) + ']');
      }
      int totalRows = 0;
      Set<String> rows = new HashSet<String>();
      while(rs.next()) {
        String row = rs.getInt(1)+", "+rs.getString(2);
        getLogWriter().info(row);
        totalRows++;
        rows.add(row);
      }
      getLogWriter().info("*********************");

      switch(escapeSeq) {
        case 0:
          assertEquals(8, totalRows);
          assertEquals(4, rows.size());
          break;
          
        case 1:
          assertEquals(4, totalRows);
          assertEquals(4, rows.size());
          break;
          
        case 2:
          assertEquals(8, totalRows);
          assertEquals(4, rows.size());
          break;
          
          default:
            break;
      }

      escapeSeq++;
    } while(cs.getMoreResults());
  }

  public void testExecuteAndLocalAndGlobalEscape() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2");

    clientSQLExecute(1,
        "insert into emp.partitiontesttable values (20, 'r1'), "
            + "(50, 'r2'), (30, 'r1'), (70, 'r3')");

    clientSQLExecute(1, "CREATE PROCEDURE MY_ESCAPE_SELECT(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest2DUnit.class.getName() + ".select_proc' "
        + "DYNAMIC RESULT SETS 4");

    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn.prepareCall("CALL MY_ESCAPE_SELECT(?)");
    cs.setInt(1, noOnClause);
    cs.execute();

    ResultSet rs;
    int escapeSeq = 0;
    do {
      rs = cs.getResultSet();
      getLogWriter().info("result set no: "+escapeSeq);
      getLogWriter().info("*********************");
      int totalRows = 0;
      Set<String> rows = new HashSet<String>();
      while(rs.next()) {
        String row = rs.getInt(1)+", "+rs.getString(2);
        getLogWriter().info(row);
        totalRows++;
        rows.add(row);
      }
      getLogWriter().info("*********************");

      switch(escapeSeq) {
        case 0:
          assertEquals(4, totalRows);
          assertEquals(4, rows.size());
          break;
          
        case 1:
          assertEquals(0, totalRows);
          assertEquals(0, rows.size());
          break;
          
        case 2:
          assertEquals(4, totalRows);
          assertEquals(4, rows.size());
          break;
          
          default:
            break;
      }

      escapeSeq++;
    } while(cs.getMoreResults());
  }

  public static void txn_proc(int number, ProcedureExecutionContext context)
      throws SQLException {
    Connection c = context.getConnection();
    GemFireTransaction gftxn = (GemFireTransaction)((EmbedConnection)c)
        .getLanguageConnectionContext().getTransactionExecute();
    TXId txid = gftxn.getActiveTXState().getTransactionId();
    getGlobalLogger().info("from proc txid is: " + txid);
  }

  public void testTxn() throws Exception {
    startVMs(1, 1);
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 59, "
            + "VALUES BETWEEN 60 and 80 ) redundancy 2");

    clientSQLExecute(1,
        "insert into emp.partitiontesttable values (20, 'r1'), (50, 'r2'), "
            + "(30, 'r1'), (70, 'r3')");

    clientSQLExecute(1, "CREATE PROCEDURE MY_TXN_PROC(number INT) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '"
        + ProcedureTest2DUnit.class.getName() + ".txn_proc'");

    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    TXId txid = TXManagerImpl.getCurrentTXId();
    getLogWriter().info("in controller txid is: " + txid);
    CallableStatement cs = conn.prepareCall("CALL MY_TXN_PROC(?) ON ALL");
    cs.setInt(1, 10);
    cs.execute();

    conn.commit();
  }
}

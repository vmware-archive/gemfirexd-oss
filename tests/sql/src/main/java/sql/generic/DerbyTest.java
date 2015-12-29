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
package sql.generic;

import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import sql.ClientDiscDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import util.TestException;
import util.TestHelper;

/**
 * 
 * @author Rahul Diyewar
 */

public class DerbyTest {
  
  Connection discConn=null; 
  SQLTestable sqlTest;
  HydraThreadLocal derbyConnection = new HydraThreadLocal();
  HydraThreadLocal resetDerbyConnection = new HydraThreadLocal();
  boolean testSecurity; 
      
  public DerbyTest(SQLTestable sqlTest){
    this.sqlTest = sqlTest;
    testSecurity = ((SQLGenericTest)sqlTest).getSQLOldTest().testSecurity;
  }
  
  public void createDiscDB() {
    if (discConn == null) {
      while (true) {
        try {
          discConn = getFirstDiscConnection();
          break;
        } catch (SQLException se) {
          Log.getLogWriter().info("Not able to connect to Derby server yet, Derby server may not be ready.");
          SQLHelper.printSQLException(se);
          int sleepMS = 10000;
          MasterController.sleepForMs(sleepMS); //sleep 10 sec to wait for Derby server to be ready.
        }
      }
    }
  }
  
  protected void createDiscSchemas() {
    Connection conn = getDiscConnection("superUser");
    Log.getLogWriter().info("creating schemas on disc.");
    createSchemas(conn);
    Log.getLogWriter().info("done creating schemas on disc.");
    closeDiscConnection(conn);
  }
  
  protected void createSchemas(Connection conn) {
    String[] schemas = SQLPrms.getSchemas();
    createSchemas(conn, schemas);
  }
  
  protected void createSchemas(Connection conn, String[] schemas){
    String ddlThread = "thr_" + SqlUtilityHelper.tid();
    try {
      Statement s = conn.createStatement();
      for (int i =0; i<schemas.length; i++) {
      if (testSecurity) {
        s.execute(schemas[i] + " AUTHORIZATION " + ddlThread); //owner of the schemas
      }
      else
          s.execute(schemas[i]);
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Y68")) {
        Log.getLogWriter().info("got schema existing exception if multiple threads" +
            " try to create schema, continuing tests");
      } else 
        SQLHelper.handleSQLException(se);
    }

    StringBuffer aStr = new StringBuffer("Created schemas \n");
    for (int i = 0; i < schemas.length; i++) {
      Object o = schemas[i];
      aStr.append(o.toString() + "\n");
    }
    Log.getLogWriter().info(aStr.toString());
  }
  
  protected void createDiscTables() {
    Connection conn = getDiscConnection();
    Log.getLogWriter().info("creating tables in derby db.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in derby db.");
    
    closeDiscConnection(conn);
  }
  
  protected void createTables(Connection conn) {
    // to get create table statements from config file
    String[] derbyTables = SQLPrms.getCreateTablesStatements(true);
    try {
      Statement s = conn.createStatement();
      for (int i = 0; i < derbyTables.length; i++) {
        Log.getLogWriter().info("derby : executing [" + derbyTables[i] + " ]");
        s.execute(derbyTables[i]);
        Log.getLogWriter().info("derby : executed [ " + derbyTables[i] + " ]");
      }
      s.close();
      commit(conn);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create derby tables\n"
          + TestHelper.getStackTrace(se));
    }
  }
  
  public Connection getFirstDiscConnection() throws SQLException {
    Connection conn = null;
    if (testSecurity)
      conn = ClientDiscDBManager.getAuthConnection(SqlUtilityHelper.tid());  //user, password requried
    else
      conn = ClientDiscDBManager.getConnection();
    return conn;
  }
  
  protected Connection getDiscConnection(String user) {
    Connection conn = null;
    try {
      //to determine which derby connection
      if (testSecurity)
        conn = ClientDiscDBManager.getAuthConnection(user);  //user, password requried
      else
        conn = ClientDiscDBManager.getConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get Derby Connection:\n " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  

  protected void closeDiscConnection(Connection conn) {
    closeDiscConnection(conn, false);
  }
  
  protected void closeDiscConnection(Connection conn, boolean end) {
    //close the connection at end of the test
    if (end) {
      try {
        conn.close();
        Log.getLogWriter().info("closing the connection");
      } catch (SQLException e) {
        SQLHelper.printSQLException(e);
        throw new TestException ("Not able to release the connection " + TestHelper.getStackTrace(e));
      }
    }
  }
  
  public void commit(Connection conn) {
    if (conn == null) return;    
    try {      
      Log.getLogWriter().info("committing the ops for derby");
      conn.commit();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se) 
          && se.getSQLState().equalsIgnoreCase("08003")) {
        Log.getLogWriter().info("detected current connection is lost, possibly due to reade time out");
        return; //add the case when connection is lost due to read timeout
      }
    }
  }
  
  protected Connection getDiscConnection() { 
    Connection conn = (Connection) derbyConnection.get();
    
    if (conn == null || (Boolean) resetDerbyConnection.get()) { 
      Log.getLogWriter().info("derbyConnection is not set yet");
      try {
        //to determine which derby connection
        if (testSecurity)
          conn = ClientDiscDBManager.getAuthConnection(SqlUtilityHelper.tid());  //user, password requried
        else
          conn = ClientDiscDBManager.getConnection();
      } catch (SQLException e) {
        SQLHelper.printSQLException(e);
        throw new TestException ("Not able to get Derby Connection:\n " + TestHelper.getStackTrace(e));
      }
      derbyConnection.set(conn);
      resetDerbyConnection.set(false);
    }
    return conn;
  }
}

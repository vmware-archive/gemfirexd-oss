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
/**
 * 
 */
package sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import util.*;
import hydra.*;
import hydra.gemfirexd.FabricSecurityPrms;

/**
 * @author eshu
 *
 */
public class GFEDBManager {
  private static String driver = "com.pivotal.gemfirexd.jdbc.EmbeddedDriver";
  private static String protocol = "jdbc:gemfirexd:";
  private static String dbName = "";
  private static boolean hasTx = TestConfig.tab().booleanAt(SQLPrms.hasTx, false);
  //private static boolean gfeExist = false; //no need for gfe 
  
  public enum Isolation {
    NONE, READ_COMMITTED, REPEATABLE_READ
  }

  //load appropriate driver
  static {
    //default is embedded driver
    driver = TestConfig.tab().stringAt(SQLPrms.driver, "com.pivotal.gemfirexd.jdbc.EmbeddedDriver");
    try {
      Class.forName(driver).newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new TestException ("Unable to load the JDBC driver " + driver + ":" 
          + TestHelper.getStackTrace(cnfe));
    } catch (InstantiationException ie) {
      throw new TestException ("Unable to instantiate the JDBC driver " + driver + ":" 
          + TestHelper.getStackTrace(ie));
    } catch (IllegalAccessException iae) {
      throw new TestException ("Not allowed to access the JDBC driver " + driver + ":" 
          + TestHelper.getStackTrace(iae));
    }
  }
  
  //to get Connection to the gfxd
  public static synchronized Connection getConnection() throws SQLException {
    //this is not being used for boot
    if (SQLTest.random.nextInt(100) == 1) 
      System.setProperty("gemfirexd.table-default-partitioned", "true");
    //Connection conn = DriverManager.getConnection(protocol + dbName + ";create=true");
    Connection conn = DriverManager.getConnection(protocol + dbName); //while create=true is not being ignored
    
    //if (SQLTest.random.nextBoolean()) conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    //after r50570 default set to Connection.TRANSACTION_READ_UNCOMMITTED
    
    conn.setAutoCommit(false); 
    //needed for derby comparison, so that exceptions during dml op can be 
    //verified separately to those during commit
    
    return conn;
  }
  
  //to get Connection to the gfxd
  public static synchronized Connection getTxConnection() throws SQLException {
    //this is not being used for boot
    if (SQLTest.random.nextInt(100) == 1) 
      System.setProperty("gemfirexd.table-default-partitioned", "true");
    Connection conn = DriverManager.getConnection(protocol + dbName);
    //Connection conn = DriverManager.getConnection(protocol + dbName); //while create=true is not being ignored
    
    if (SQLTest.random.nextInt(20) == 1) {
    	//test auto upgrade to read committed
    	conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    } else {
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
    
    conn.setAutoCommit(false);    
    return conn;
  }
  
  //to get Connection to the gfxd
  public static synchronized Connection getTxConnection(Properties info) throws SQLException {
    // not used for boot
    /*
    if (info != null) {
      info.setProperty("table-default-partitioned", "true");
    }
    else {
      System.setProperty("gemfirexd.table-default-partitioned", "true");
    }
    */
    Connection conn = DriverManager.getConnection(protocol + dbName, info);

    if (SQLTest.random.nextInt(20) == 1) {
    	//test auto upgrade to read committed
    	conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    } else {
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
    //conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    
    conn.setAutoCommit(false);    
    return conn;
  }
  
  //to get Connection to the gfxd
  public static synchronized Connection getRRTxConnection(Properties info) throws SQLException {
    // not used for boot
    /*
    if (info != null) {
      info.setProperty("table-default-partitioned", "true");
    }
    else {
      System.setProperty("gemfirexd.table-default-partitioned", "true");
    }
    */
    Connection conn = DriverManager.getConnection(protocol + dbName, info);
    
    conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    
    conn.setAutoCommit(false);    
    return conn;
  }
  
  //to get Connection to the gfxd and specify which server groups the dataStore belongs
  //should only be called by a dataStore
  //persist-dd and disable-streaming etc should be in boot property, 
  public static synchronized Connection getConnection(Properties info) throws SQLException {
    //Connection conn = DriverManager.getConnection(protocol + dbName + ";create=true");
    // should be handled in the boot properties
    /*
    if (info != null) {
      if (SQLTest.random.nextBoolean()) info.setProperty("disable-streaming", "false");
      info.setProperty("table-default-partitioned", "true");
    }
    else {
      System.setProperty("gemfirexd.table-default-partitioned", "true");
    }
    */
    if (info != null) {
      if (SQLTest.random.nextBoolean()) info.setProperty("disable-streaming", "false");
    }
    Connection conn = DriverManager.getConnection(protocol + dbName, info); //while create=true is not being ignored
    
    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static synchronized Connection getRestartConnection(Properties info) throws SQLException {
    try {
      Class.forName(driver).newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new TestException ("Unable to load the JDBC driver " + driver + ":" 
          + TestHelper.getStackTrace(cnfe));
    } catch (InstantiationException ie) {
      throw new TestException ("Unable to instantiate the JDBC driver " + driver + ":" 
          + TestHelper.getStackTrace(ie));
    } catch (IllegalAccessException iae) {
      throw new TestException ("Not allowed to access the JDBC driver " + driver + ":" 
          + TestHelper.getStackTrace(iae));
    }
    
    // default to partitioned tables for tests
    if (info != null) {
      if (SQLTest.random.nextBoolean()) info.setProperty("disable-streaming", "false");
      info.setProperty("table-default-partitioned", "true");
    }
    else {
      System.setProperty("gemfirexd.table-default-partitioned", "true");
    }
    Connection conn = DriverManager.getConnection(protocol + dbName, info); //while create=true is not being ignored
    
    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static String getDriver() {
    return driver;
  }
  
  public static String getUrl() {
    return protocol + dbName;
  }
  
  //shut down gfeDB
  public static void shutDownDB() {
    try {
      // the shutdown=true attribute shuts down 
      if (SQLTest.testSecurity) {
        shutDownDB("superUser", "superUser");
        return;
      } else {
        DriverManager.getConnection(protocol + ";shutdown=true");
      }
      Log.getLogWriter().info("gfxdbric has been shut down");
    }
    catch (SQLException se) {
      if (( (se.getErrorCode() == 50000)
           && ("XJ015".equals(se.getSQLState()) ))) {
        // we got the expected exception
      }
      else {
        // if the error code or SQLState is different, we have
        // an unexpected exception (shutdown failed)
        SQLHelper.handleSQLException(se);
      }
    }
  }
  
  public static void shutDownDB(String user, String passwd){ 
    try {
      // the shutdown=true attribute shuts down 
      Log.getLogWriter().info("shutting down as : ;user=" + user
          + ";passwprd=" + passwd);
      DriverManager.getConnection(protocol +";user=" + user
          + ";password=" + passwd + ";shutdown=true");   
      Log.getLogWriter().info("gfxdbric has been shut down");
    }
    catch (SQLException se) {
      if (( (se.getErrorCode() == 50000)
           && ("XJ015".equals(se.getSQLState()) ))) {
        // we got the expected exception
      }
      else {
        // if the error code or SQLState is different, we have
        // an unexpected exception (shutdown failed)
        SQLHelper.handleSQLException(se);
      }
    }

  }
  
  public static void closeConnection(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to release the connection " + TestHelper.getStackTrace(e));
    }
  }
  
  //to get Connection to the derby database on disc
  public static synchronized Connection getSuperUserConnection() throws SQLException {
    Connection conn = null;
    String user = TestConfig.tab().stringAt(FabricSecurityPrms.user, "superUser");
    String passwd = TestConfig.tab().stringAt(FabricSecurityPrms.password, "superUser");
    String superUser = ";user="+ user +";password="+passwd;
    //String superUser = ";user=\"" +user + "\";password="+passwd;
    Log.getLogWriter().info("get connection using this credential: " + superUser);
    //conn = DriverManager.getConnection(protocol + dbName + superUser);
    Properties p = new Properties();
    p.setProperty("user", user);
    p.setProperty("password", passwd);
    // default to partitioned tables for tests
    p.setProperty("table-default-partitioned", "true");
    conn = DriverManager.getConnection(protocol + dbName, p);
    conn.setAutoCommit(false);    
    return conn;
  }
  
  //get connection with authentication setting
  public static synchronized Connection getAuthConnection(int thrId) throws SQLException {
    Connection conn = null;
    String userAuth = ";user=thr_" + thrId + ";password=thr_" + thrId;
    Log.getLogWriter().info("current gfxd connection " +
        "use the following credential: " + userAuth);

    //Log.getLogWriter().info(protocol + dbName + userAuth );
    conn = DriverManager.getConnection(protocol + dbName +userAuth);

    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static synchronized Connection getInvalidAuthConnection(int thrId) throws SQLException {
    Connection conn = null;
    String userAuth = ";user=thr_" + thrId + ";password=thr_" + ++thrId;
    Log.getLogWriter().info("current gfxd connection " +
        "use the following credential: " + userAuth);

    //Log.getLogWriter().info(protocol + dbName + userAuth );
    conn = DriverManager.getConnection(protocol + dbName +userAuth);

    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static synchronized Connection getNoneAuthConnection(int thrId) throws SQLException {
    Log.getLogWriter().info("current gfxd connection " +
        "use no credential");
    return getConnection();
  }
  
  public static synchronized Connection getAuthConnection(String user) throws SQLException {
    Connection conn = null;
    String userAuth = ";user=" + user + ";password=" + user;
    Log.getLogWriter().info("current connection " +
        "use the following credential: " + userAuth);

    //Log.getLogWriter().info(protocol + dbName + userAuth );
    conn = DriverManager.getConnection(protocol + dbName +userAuth);

    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static synchronized Connection getAuthConnection(String user, String password) throws SQLException {
    Connection conn = null;
    String userAuth = ";user=" + user + ";password=" + password;
    Log.getLogWriter().info("current connection " +
        "use the following credential: " + userAuth);

    //Log.getLogWriter().info(protocol + dbName + userAuth );
    conn = DriverManager.getConnection(protocol + dbName +userAuth);

    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static Connection getReadCommittedConnection() throws SQLException {
    
    Connection conn = null;
    if (SQLTest.hasJSON) {
      Properties info = new Properties();
      info.setProperty("sync-commits", "true");
      conn = DriverManager.getConnection(protocol + dbName, info);
    }
    else {
      conn = DriverManager.getConnection(protocol + dbName);
    }
    if (SQLTest.random.nextInt(20) == 1) {
    	//test auto upgrade to read committed
    	conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    } else {
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
    conn.setAutoCommit(false); 
    return conn;
  }
  
  public static Connection getReadCommittedConnection(Properties info) throws SQLException {
    
    if (SQLTest.hasJSON ) {
      info.setProperty("sync-commits", "true");
    }
    Connection conn = DriverManager.getConnection(protocol + dbName, info);
    
    if (SQLTest.random.nextInt(20) == 1) {
        //test auto upgrade to read committed
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    } else {
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
      
    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static Connection getRepeatableReadConnection() throws SQLException {
    Connection conn = DriverManager.getConnection(protocol + dbName);
    
    conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    
    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static Connection getRepeatableReadConnection(Properties info) throws SQLException {
    Connection conn = DriverManager.getConnection(protocol + dbName, info);
    
    conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    
    conn.setAutoCommit(false);    
    return conn;
  }
  public static synchronized Connection getTxConnection(Isolation isolation) throws SQLException {
    // default to partitioned tables for tests
    //not used for boot
    //System.setProperty("gemfirexd.table-default-partitioned", "true");
    switch (isolation) {
    case NONE:
      Connection conn = getConnection();
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      return conn;
    case READ_COMMITTED:
      return getReadCommittedConnection();
    case REPEATABLE_READ:
      return getRepeatableReadConnection();
    default: 
      throw new TestException ("test issue -- unknow Isolation lever");
    }
  }
  
  public static synchronized Connection getTxConnection(Isolation isolation, Properties info) throws SQLException {
    // default to partitioned tables for tests
    //not used for boot
    //System.setProperty("gemfirexd.table-default-partitioned", "true");
    switch (isolation) {
    case NONE:
      Connection conn = getConnection(info);
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      return conn;
    case READ_COMMITTED:
      return getReadCommittedConnection(info);
    case REPEATABLE_READ:
      return getRepeatableReadConnection(info);
    default: 
      throw new TestException ("test issue -- unknow Isolation lever");
    }
  }
}

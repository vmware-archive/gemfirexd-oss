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

import java.sql.*;

import util.*;
import hydra.DerbyServerHelper;
import hydra.Log;
import hydra.MasterController;

/**
 * @author eshu
 *
 */
public class ClientDiscDBManager {
  private static String driver = "org.apache.derby.jdbc.ClientDriver";
  private static String protocol = "jdbc:derby:";
  private static String dbName = "test"; 
  private static boolean dbExist = false;
  private static String host;
  private static int port;
  private static String superUser =";user=superUser;password=superUser";
  
    
  //load driver
  static {
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
    getHostAndPort();
  }
  
  // to get the server port and hostname
  protected static void getHostAndPort() {
    if (DerbyServerHelper.getEndpoint() ==null) return;
    
    port = DerbyServerHelper.getEndpoint().getPort();    
    host = DerbyServerHelper.getEndpoint().getHost();
    
    Log.getLogWriter().info("hostname is " + host);
    Log.getLogWriter().info("port number is " + port);
    
    //add info to url
    protocol+="//"+host+":"+port+"/";
  }
  
  public static String getDriver() {
    return driver;
  }
  
  public static String getUrl() {
    return protocol + dbName;
  }
  
  public static String getDBName() {
    return dbName;
  }
  //to get Connection to the derby database on disc
  public static synchronized Connection getConnection() throws SQLException {
    boolean success = false;
    Connection conn = null;
    int maxRetries = 3;
    int count = 0;
    while (!success) {
      success = true;
      count++;
      try {
        DriverManager.setLoginTimeout(150);
        if (!dbExist) {
          //to create the databse if it is not yet created.
          Log.getLogWriter().info("creating disc database");
          conn = DriverManager.getConnection(protocol + dbName + ";create=true");
          dbExist = true;
          
        } else   conn = DriverManager.getConnection(protocol + dbName);
      } catch (SQLException se) {
        if (se.getSQLState().equals("08003")) {
          Log.getLogWriter().info("could not get derby connection, may retry again");
          success = false;
          MasterController.sleepForMs(1000);
          if (count > maxRetries) throw se;
        } else throw se;
      }
    }

    conn.setAutoCommit(false);    
    return conn;
  }
  
  
  //to get Connection to the derby database on disc
  public static synchronized Connection getSuperUserConnection() throws SQLException {
    Connection conn = null;
    if (!dbExist) {
      //to create the databse if it is not yet created.
      Log.getLogWriter().info("creating disc database as " + superUser);
        conn = DriverManager.getConnection(protocol + dbName + superUser + ";create=true");
        dbExist = true;
      
    } else   {
      Log.getLogWriter().info("get Connection as :" + superUser);
      conn = DriverManager.getConnection(protocol + dbName + superUser);
    }

    conn.setAutoCommit(false);    
    return conn;
  }
  
  //to get Connection to the derby database on disc
  public static synchronized Connection getAuthConnection(int thrId) throws SQLException {
    Connection conn = null;
    String userAuth = ";user=thr_" + thrId + ";password=thr_" + thrId;
    Log.getLogWriter().info("current derby connection " +
        "use the following credential: " + userAuth);

    //Log.getLogWriter().info(protocol + dbName + userAuth );
    conn = DriverManager.getConnection(protocol + dbName +userAuth);

    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static synchronized Connection getInvalidAuthConnection(int thrId) throws SQLException {
    Connection conn = null;
    String userAuth = ";user=thr_" + thrId + ";password=thr_" + ++thrId;
    Log.getLogWriter().info("current derby connection " +
        "use the following credential: " + userAuth);

    //Log.getLogWriter().info(protocol + dbName + userAuth );
    conn = DriverManager.getConnection(protocol + dbName +userAuth);

    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static synchronized Connection getNoneAuthConnection(int thrId) throws SQLException {
    Log.getLogWriter().info("current derby connection " +
        "use no credential");
    return getConnection();
  }
  
  //to get Connection to the derby database on disc
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
  
  public static void shutDownDB() {
    try {
      // the shutdown=true attribute shuts down Derby
      if (SQLTest.testSecurity) {
        DriverManager.getConnection("jdbc:derby:;user=superUser;password=superUser;shutdown=true");  
      //hardcoded for now
      } else {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      }
    }
    catch (SQLException se) {
      if (( (se.getErrorCode() == 50000)
           && ("XJ015".equals(se.getSQLState()) ))) {
        // we got the expected exception      
        Log.getLogWriter().info("derby db has been shut donw");
        dbExist = false;
      }
      else {
        // if the error code or SQLState is different, we have
        // an unexpected exception (shutdown failed)
        SQLHelper.printSQLException(se);
        throw new TestException ("Shutdown failed:" + TestHelper.getStackTrace(se));
      }
    }
  }
  
}

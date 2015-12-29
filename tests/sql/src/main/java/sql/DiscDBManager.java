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

/**
 * @author eshu
 *
 */
public class DiscDBManager {
  private static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
  private static String protocol = "jdbc:derby:";
  private static String dbName = "test"; 
  private static boolean dbExist = false;
  
  
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
    
    //setHostAndPort(); //to set host and port
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
    Connection conn = null;
    if (!dbExist) {
      //to create the databse if it is not yet created.
        conn = DriverManager.getConnection(protocol + dbName + ";create=true");
        dbExist = true;
      
    } else   conn = DriverManager.getConnection(protocol + dbName);

    conn.setAutoCommit(false);    
    return conn;
  }
  
  public static void shutDownDB() {
    try {
      // the shutdown=true attribute shuts down Derby
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
    }
    catch (SQLException se) {
      if (( (se.getErrorCode() == 50000)
           && ("XJ015".equals(se.getSQLState()) ))) {
        // we got the expected exception
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

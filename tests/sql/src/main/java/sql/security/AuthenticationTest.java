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
package sql.security;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import sql.SQLBB;
import sql.SQLHelper;

import hydra.*;

public class AuthenticationTest {
  static String derby_setProperty = 
    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(";
  static String derby_getProperty = 
    "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(";
  static String derby_defaultConnMode =
    "'derby.database.defaultConnectionMode'";
  static String derby_fullAccessUsers = "'derby.database.fullAccessUsers'";
  static String derby_readOnlyAccessUsers =
    "'derby.database.readOnlyAccessUsers'";
  static String derby_requireAuth = "'derby.connection.requireAuthentication'";
  static String derby_sqlAuthorization = "'derby.database.sqlAuthorization'";
  static String gfxd_setProperty = 
    "CALL SYSCS_UTIL.SET_DATABASE_PROPERTY(";
  static String gfxd_getProperty = 
    "VALUES SYSCS_UTIL.GET_DATABASE_PROPERTY(";
  static String gfxd_defaultConnMode =
    "'gemfirexd.authz-default-connection-mode'";
  static String gfxd_fullAccessUsers = "'gemfirexd.authz-full-access-users'";
  static String gfxd_readOnlyAccessUsers =
    "'gemfirexd.authz-read-only-access-users'";
  static String derbyDBPropertiesOnly = "'derby.database.propertiesOnly'";

  @SuppressWarnings("unchecked")
  public static void createUserAuthentication(Connection conn)
      throws SQLException {
    CallableStatement cs;
    String whichUser = SQLHelper.isDerbyConn(conn) ? "derby.user."
        : "gemfirexd.user.";
    String prefix = SQLHelper.isDerbyConn(conn) ? "SYSCS_" : "";
    Map<String, String> userPasswd = (Map<String, String>) SQLBB.getBB()
        .getSharedMap().get(SQLSecurityTest.userPasswdMap);
    for (Map.Entry<String, String> e : userPasswd.entrySet()) {
      String sql = "CALL SYSCS_UTIL." + prefix + "SET_DATABASE_PROPERTY"
      + "('" + whichUser + e.getKey() + "', '" + e.getValue() + "')";
      cs = conn.prepareCall(sql);
      Log.getLogWriter().info("executed: " + sql);
      cs.execute();
      conn.commit();
    }
    /*
     * cs = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
     * "( 'derby.database.defaultConnectionMode', 'readOnlyAccess')");
     * cs.execute(); conn.commit();
     * 
     * cs = conn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY" +
     * "( 'derby.database.fullAccessUsers', 'thr_" +
     * RemoteTestModule.getCurrentThread().getThreadId() +"')"); cs.execute();
     * conn.commit();
     * 
     * Statement stmt = conn.createStatement(); ResultSet rs =
     * stmt.executeQuery("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(" +
     * "'derby.database.fullAccessUsers')"); rs.next();
     * Log.getLogWriter().info("Value of fullAccessUsers is " +
     * rs.getString(1));
     */
  }

  public static void turnOnAuthorization(Connection dConn, Connection gConn) {
    String derby_sql = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY( "
      + "'derby.database.sqlAuthorization', 'true')";
    String gfxd_sql = "CALL SYSCS_UTIL.SET_DATABASE_PROPERTY( "
      + "'gemfirexd.sql-authorization', 'true')";
    ArrayList<SQLException> exList = new ArrayList<SQLException>();
    if (dConn != null) {
      turnOnDerbyAuthentication(dConn);
      turnOnDerbyAuthorization(dConn, derby_sql, exList);
      turnOnGfxdAuthorization(gConn, gfxd_sql, exList);
      SQLHelper.handleMissedSQLException(exList);
      } else
        turnOnGfxdAuthorization(gConn, gfxd_sql);
  }

  public static void turnOnDerbyAuthentication(Connection conn) {
    try {
      getDerbyAuthorizationInfo(conn);
      Statement stmt = conn.createStatement();
      Log.getLogWriter().info("turn on authentication statement in derby");
      stmt.executeUpdate(derby_setProperty + derby_requireAuth + ", 'true')");
      conn.commit();
      getDerbyAuthorizationInfo(conn);
    } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
    }
  }
  
  public static void turnOnDerbyAuthorization(Connection conn, String sql,
      ArrayList<SQLException> exList) {
    try {
      getDerbyAuthorizationInfo(conn);
      Log.getLogWriter().info("turn on authorization statement in derby");
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
      //setDerbyPropertiesOnly(conn);
      getDerbyAuthorizationInfo(conn);
    } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exList);
    }
  }
  
  public static void setDerbyPropertiesOnly(Connection conn) {
    try {
      Log.getLogWriter().info("turn on database only properties in derby");
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(derby_setProperty + derbyDBPropertiesOnly + ", 'true')");
      conn.commit();
      getDerbyAuthorizationInfo(conn);
    } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
    }
  }
  
  public static void getDerbyAuthorizationInfo(Connection conn) {
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt
      .executeQuery(derby_getProperty + derby_requireAuth + ")");
      rs.next();
      Log.getLogWriter()
        .info("Value of derby database properties requireAuth is " + rs.getString(1));
      rs.close();
      rs = stmt
      .executeQuery(derby_getProperty 
          + "'derby.database.sqlAuthorization')");
      rs.next();
      Log.getLogWriter()
        .info("Value of derby sqlAuthorization is " + rs.getString(1));
      rs.close();
      rs = stmt.executeQuery(derby_getProperty
          + derby_defaultConnMode + ")");
      rs.next();
      Log.getLogWriter().info(
          "Value of derby defaultConnectionMode is " + rs.getString(1));
      rs.close();
      conn.commit();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }

  public static void turnOnGfxdAuthorization(Connection conn, String sql,
      ArrayList<SQLException> exList) {
    Log.getLogWriter().info("turn on authorization statement in Gfxd");
    try {
      getGfxdAuthorizationInfo(conn);
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
      getGfxdAuthorizationInfo(conn);
    } catch (SQLException se) {
      SQLHelper.handleDerbySQLException(se, exList);
    }
  }
  
  public static void getGfxdAuthorizationInfo(Connection conn) {
    Properties sprops = System.getProperties(); 
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt
      .executeQuery(gfxd_getProperty
          + "'gemfirexd.sql-authorization')");
      rs.next();
      Log.getLogWriter()
        .info("Value of gfxd database properties sqlAuthorization is " + rs.getString(1));
      rs.close();
      Log.getLogWriter().info("Value of gemfirexd system properties sqlAuthorization is "
          + sprops.getProperty("gemfirexd.sql-authorization"));
      
      rs = stmt.executeQuery(gfxd_getProperty + gfxd_defaultConnMode +")");
      rs.next();
      Log.getLogWriter().info("Value of gemfirexd system properties defaultConnectionMode is "
          + sprops.getProperty("gemfirexd.authz-default-connection-mode"));
      Log.getLogWriter().info(
          "Value of gfxd defaultConnectionMode is " + rs.getString(1));
      rs.close();
      conn.commit();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }

  //will see how to turn on authorization, may need to reboot in gemfirexd
  //with the new system property
  public static void turnOnGfxdAuthorization(Connection conn, String sql) {
    Log.getLogWriter().info("turn on authorization statement in Gfxd");
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(sql);
      conn.commit();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void logFullAccessUsers(Connection conn) {
    boolean isDerbyConn = SQLHelper.isDerbyConn(conn);
    String getProperty = isDerbyConn? derby_getProperty : gfxd_getProperty;
    String fullAccessUsers = isDerbyConn? derby_fullAccessUsers : gfxd_fullAccessUsers;
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(getProperty + fullAccessUsers + ")");
      rs.next();
      Log.getLogWriter().info(
          fullAccessUsers + " are " + rs.getString(1));
      rs.close();
      conn.commit();
    }catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void logReadyOnlyAccessUsers(Connection conn) {
    boolean isDerbyConn = SQLHelper.isDerbyConn(conn);
    String getProperty = isDerbyConn? derby_getProperty : gfxd_getProperty;
    String readOnlyAccessUsers = isDerbyConn? derby_readOnlyAccessUsers : 
      gfxd_readOnlyAccessUsers;
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(getProperty + readOnlyAccessUsers +  ")");
      rs.next();
      Log.getLogWriter().info(
          readOnlyAccessUsers + " are " + rs.getString(1));
      rs.close();
      conn.commit();
    }catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void setUserFullAccessMode(Connection conn, String user) {
    try {
      boolean isDerbyConn = SQLHelper.isDerbyConn(conn);
      String setProperty = isDerbyConn? derby_setProperty : gfxd_setProperty;
      String fullAccessUsers = isDerbyConn? derby_fullAccessUsers : 
        gfxd_fullAccessUsers;
      Statement stmt = conn.createStatement();
      Log.getLogWriter().info(setProperty + fullAccessUsers + ", '" + user + "')");
      stmt.executeUpdate(setProperty + fullAccessUsers + ", '" + user + "')");
      conn.commit();
    }catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void setUserReadOnlyAccessMode(Connection conn, String user) {
    try {
      boolean isDerbyConn = SQLHelper.isDerbyConn(conn);
      String setProperty = isDerbyConn? derby_setProperty : gfxd_setProperty;
      String readOnlyAccessUsers = isDerbyConn? derby_readOnlyAccessUsers : 
        gfxd_readOnlyAccessUsers;
      Statement stmt = conn.createStatement();
      Log.getLogWriter().info(setProperty + readOnlyAccessUsers + ", '" + user + "')");
      stmt.executeUpdate(setProperty + readOnlyAccessUsers + ", '" + user + "')");
      conn.commit();
    }catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void setFullAccessMode(Connection dConn, Connection gConn, String user) {
  	if (dConn != null) setUserFullAccessMode(dConn, user);
    setUserFullAccessMode(gConn, user);
  }
  
  public static void setReadOnlyAccessMode(Connection dConn, Connection gConn, String user) {
  	if (dConn != null) setUserReadOnlyAccessMode(dConn, user);
    setUserReadOnlyAccessMode(gConn, user);
  }
  
  public static void logFullAccessUsers(Connection dConn, Connection gConn) {
    if (dConn != null) logFullAccessUsers(dConn);
    logFullAccessUsers(gConn);
  }
  
  public static void logReadyOnlyAccessUsers(Connection dConn, Connection gConn) {
    if (dConn != null) logReadyOnlyAccessUsers(dConn);
    logReadyOnlyAccessUsers(gConn);
  }
  
}

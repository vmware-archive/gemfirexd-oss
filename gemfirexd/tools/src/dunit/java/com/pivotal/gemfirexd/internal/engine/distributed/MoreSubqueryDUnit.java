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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

public class MoreSubqueryDUnit extends DistributedSQLTestBase
{
  protected static volatile boolean isQueryExecutedOnNode = false;
  @SuppressWarnings("unused")
  private static final int byrange = 0x01, bylist = 0x02, bycolumn = 0x04, ANDing = 0x08, ORing = 0x10, Contains = 0x20, OrderBy = 0x40;
  @SuppressWarnings("unused")
  private static final int noCheck = 0x01, fixLater = noCheck|0x02, fixImmediate = noCheck|0x04, wontFix = noCheck|0x08, fixDepend = noCheck|0x10, 
                           fixInvestigate = noCheck|0x20;
  private static volatile Connection derbyConn = null;

  public MoreSubqueryDUnit(String name) {
    super(name);
  }

  public void testBug45216() throws Exception {
    try {

      startVMs(1, 2);
      
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      
      clientSQLExecute(1,
          "create table app.customers (cid int primary key, tid int) replicate");

      clientSQLExecute(
          1,
          "create table app.networth (cid int primary key, tid int) partition by column (cid)");

      clientSQLExecute(1, "insert into app.customers values (11, 77)");
      clientSQLExecute(1, "insert into app.customers values (22, 77)");
      clientSQLExecute(1, "insert into app.customers values (33, 77)");
      clientSQLExecute(1, "insert into app.customers values (44, 77)");
      clientSQLExecute(1, "insert into app.customers values (55, 77)");

      clientSQLExecute(1, "insert into app.networth values (11, 78)");
      clientSQLExecute(1, "insert into app.networth values (22, 78)");
      clientSQLExecute(1, "insert into app.networth values (44, 78)");
      clientSQLExecute(1, "insert into app.networth values (55, 78)");

      executeOnDerby("create table app.customers (cid int primary key, tid int)",
          false, null);
      executeOnDerby("create table app.networth (cid int primary key, tid int)",
          false, null);

      executeOnDerby("insert into app.customers values (11, 77)", false, null);
      executeOnDerby("insert into app.customers values (22, 77)", false, null);
      executeOnDerby("insert into app.customers values (33, 77)", false, null);
      executeOnDerby("insert into app.customers values (44, 77)", false, null);
      executeOnDerby("insert into app.customers values (55, 77)", false, null);

      executeOnDerby("insert into app.networth values (11, 78)", false, null);
      executeOnDerby("insert into app.networth values (22, 78)", false, null);
      executeOnDerby("insert into app.networth values (44, 78)", false, null);
      executeOnDerby("insert into app.networth values (55, 78)", false, null);

      String subquery = "select cid from app.customers where cid not in "
          + "(select cid from app.networth)";

      ResultSet rsGfxd = executeQuery(subquery, false, null, false);
      ResultSet rsDerby = executeQuery(subquery, false, null, true);

      TestUtil.validateResults(rsDerby, rsGfxd, false);

    } finally {
      cleanup();
    }    
  }
  
  public static void executeOnDerby(String sql, boolean isPrepStmt,
      Object[] params) throws Exception
  {
    if (derbyConn == null) {
      String derbyDbUrl = "jdbc:derby:newDB;create=true;";
      if (TestUtil.currentUserName != null) {
        derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
            + TestUtil.currentUserPassword + ';');
      }
      
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      
    }
    if (isPrepStmt) {
      PreparedStatement ps = derbyConn.prepareStatement(sql);
      if (params != null) {
        int j = 1;
        for (Object param : params) {
          if (param == null) {
            ps.setNull(j, Types.JAVA_OBJECT);
          }
          else {
            ps.setObject(j, param);
          }
        }
      }
     ps.execute();
    }
    else {
      derbyConn.createStatement().execute(sql);
    }

  }
  
  private void cleanup() throws Exception
  {
    if (derbyConn != null) {
      Statement derbyStmt = derbyConn.createStatement();
      if (derbyStmt != null) {
        final String[] tables = new String[] { "networth", "portfolio"};
        for (String table : tables) {
          try {
            derbyStmt.execute("drop table " + table);
          }
          catch (SQLException ex) {
            // deliberately ignored
          }

          try {
            clientSQLExecute(1, "drop table " + table);
          }
          catch (Exception ex) {
            // deliberately ignored
          }
        }
      }

     /* try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      }
      catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }*/

    }

  }
  
  public static ResultSet executeQuery(String sql, boolean isPrepStmt,
      Object[] params, boolean isDerby) throws Exception
  {
    Connection derbyOrGfxd = null;
    ResultSet rs = null;
    if (isDerby) {
      if (derbyConn == null) {
        String derbyDbUrl = "jdbc:derby:newDB;create=true;";
        if (TestUtil.currentUserName != null) {
          derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
              + TestUtil.currentUserPassword + ';');
        }
       
          derbyConn = DriverManager.getConnection(derbyDbUrl);  
        
      }
      derbyOrGfxd = derbyConn;
    }
    else {
      derbyOrGfxd = TestUtil.getConnection();
    }

    if (isPrepStmt) {
      PreparedStatement ps = derbyOrGfxd.prepareStatement(sql);
      if (params != null) {
        int j = 1;
        for (Object param : params) {
          if (param == null) {
            ps.setNull(j, Types.JAVA_OBJECT);
          }
          else {
            ps.setObject(j, param);
          }
          ++j;
        }
      }
      rs = ps.executeQuery();
    }
    else {
      rs = derbyOrGfxd.createStatement().executeQuery(sql);
    }
    return rs;

  }

  public static int executeUpdate(String sql, boolean isPrepStmt,
      Object[] params, boolean isDerby) throws Exception
  {
    Connection derbyOrGfxd = null;
    int numUpdate= 0;
    if (isDerby) {
      if (derbyConn == null) {
        String derbyDbUrl = "jdbc:derby:newDB;create=true;";
        if (TestUtil.currentUserName != null) {
          derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
              + TestUtil.currentUserPassword + ';');
        }
       
          derbyConn = DriverManager.getConnection(derbyDbUrl);  
        
      }
      derbyOrGfxd = derbyConn;
    }
    else {
      derbyOrGfxd = TestUtil.getConnection();
    }

    if (isPrepStmt) {
      PreparedStatement ps = derbyOrGfxd.prepareStatement(sql);
      if (params != null) {
        int j = 1;
        for (Object param : params) {
          if (param == null) {
            ps.setNull(j, Types.JAVA_OBJECT);
          }
          else {
            ps.setObject(j, param);
          }
          ++j;
        }
      }
      numUpdate = ps.executeUpdate();
    }
    else {
      numUpdate = derbyOrGfxd.createStatement().executeUpdate(sql);
    }
    return numUpdate;

  }
  
  public void tearDown2() throws Exception
  {
    try {     
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
      derbyConn = null;
    }
    catch (SQLException sqle) {
      derbyConn = null;
      if (sqle.getMessage().indexOf("shutdown") == -1 && 
          sqle.getMessage().indexOf("Driver is not registered ") == -1) {
        sqle.printStackTrace();
        throw sqle;
      }
      derbyConn = null;
    }
    super.tearDown2();
  }
}

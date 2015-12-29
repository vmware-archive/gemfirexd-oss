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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

/**
 * 
 * @author Asif
 *
 */

public class StatementTest extends JdbcTestBase {
  private boolean callbackInvoked = false;
  private int index =0;

  public StatementTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(StatementTest.class));
  }

  /**
   * Validates that QueryInfo does not get created repeatedly if the
   * query string is unchanged, with same Statement object
   * @throws SQLException
   */
  public void testRegionGetMultipleTimesWithSameStmnt() throws SQLException {
    Connection conn = getConnection();
    createTableWithPrimaryKey(conn);

    String query = "Select * from orders where id =8";

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {          

            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              callbackInvoked = true;
               ++index;
             
            }

          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      conn = getConnection();
      final int numExecution = 5;
      Statement s = conn.createStatement();
      for (int i = 0; i < numExecution; ++i) {
        try {          
          s.executeQuery(query);
        }
        catch (SQLException e) {
          throw new SQLException(e.toString()
              + " Exception in executing query = " + query, e
              .getSQLState());
        }
      }
      assertTrue(this.callbackInvoked);
      assertEquals(1,index);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testBug42447() throws SQLException
  {
    Properties props = new Properties();
    props.put("host-data", "false");
    Connection conn = getConnection(props);
    Statement s = conn.createStatement();
    // We create a table...
    try {
      s.execute("create table orders"
          + "(id int PRIMARY KEY, cust_name varchar(200), vol int, "
          + "security_id varchar(10), num int, addr varchar(100)) replicate");
      fail("Test should fail because of insufficient data stores");
      /*
      s = conn.createStatement();

      String query = " update orders set num =4  where id =8";
      PreparedStatement ps = conn.prepareStatement(query);
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      try {
        int num = ps.executeUpdate();
        fail("Test should fail because of insufficient data stores");
      }catch(SQLException sqle) {
        assertEquals(sqle.getSQLState(), "X0Z08");
      }
      */
    } catch (SQLException sqle) {
      if (!"X0Z08".equals(sqle.getSQLState())) {
        throw sqle;
      }
    } finally {
      //s.execute("drop table orders");
    }
  }

  public void testBug40595() throws SQLException
  {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders"
        + "(id int PRIMARY KEY, cust_name varchar(200), vol int, "
        + "security_id varchar(10), num int, addr varchar(100))");
    s = conn.createStatement();
  
    String query = "Select * from orders where id =8";
    final boolean usedGfxdActivn[] = new boolean[] { false };
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void beforeGemFireResultSetExecuteOnActivation(
                AbstractGemFireActivation activation)
            {
              usedGfxdActivn[0] = true;
            }

          });

      PreparedStatement ps = conn.prepareStatement(query);
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
     
      try {
        ResultSet rs = ps.executeQuery();
        rs.next();
        rs.close();
        
      }
      catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e.getSQLState());
      }
     
      assertTrue(usedGfxdActivn[0]);
      usedGfxdActivn[0] = false;     
      s.execute("drop table orders");

      s.execute("create table orders"
          + "(id int PRIMARY KEY, cust_name varchar(200), vol int, "
          + "security_id varchar(10), num int, addr varchar(100))");
      try {
       ResultSet rs = ps.executeQuery();
       rs.next();
      }
      catch (SQLException e) {
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e);
      }
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Validates that QueryInfo does not get created repeatedly if the query
   * string is unchanged, but a different Statement object is used every time
   * 
   * @throws SQLException
   */
  public void testRegionGetMultipleTimesWithDiffStmnt() throws SQLException {
    Connection conn = getConnection();
    createTableWithPrimaryKey(conn);

    String query = "Select * from orders where id =8";

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {

            @Override
            public void beforeGemFireResultSetExecuteOnActivation(
                AbstractGemFireActivation activation) {
            }

            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              callbackInvoked = true;
               ++index;             
            }

          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
     
      final int numExecution = 5;
      
      for (int i = 0; i < numExecution; ++i) {
        try {
          conn = getConnection();
          Statement s = conn.createStatement();
          s.executeQuery(query);
        }
        catch (SQLException e) {
          throw new SQLException(e.toString()
              + " Exception in executing query = " + query, e
              .getSQLState());
        }
      }
      assertTrue(this.callbackInvoked);
      assertEquals(1,index);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void createTableWithPrimaryKey(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders"
        + "(id int PRIMARY KEY, cust_name varchar(200), vol int, "
        + "security_id varchar(10), num int, addr varchar(100))");
    s.close();
  }

  public void createTableWithCompositeKey(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders"
        + "(id int , cust_name varchar(200), vol int, "
        + "security_id varchar(10), num int, addr varchar(100),"
        + " Primary Key (id, cust_name))");
    s.close();

  }

  @Override
  public void tearDown() throws Exception {
    this.callbackInvoked = false;
    this.index = 0;
    super.tearDown();
  }
}

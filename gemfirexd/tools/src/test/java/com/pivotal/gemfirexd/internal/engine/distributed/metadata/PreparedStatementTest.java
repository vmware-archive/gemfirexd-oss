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
import java.sql.SQLException;
import java.sql.Statement;

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

public class PreparedStatementTest extends JdbcTestBase {
  private boolean callbackInvoked = false;
  private int index =0;

  public PreparedStatementTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(PreparedStatementTest.class));
  }

  /**
   * Validates that PreparedStatement execution reuses the original Activation
   * @throws SQLException
   */
  public void testRegionGetMultipleTimesWithPrepStmnt() throws SQLException {
    Connection conn = getConnection();
    createTableWithPrimaryKey(conn);

    String query = "Select * from orders where id =?";

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {            
            private AbstractGemFireActivation origAct = null;

            @Override
            public void beforeGemFireResultSetExecuteOnActivation(
                AbstractGemFireActivation activation) {
              if (index == 0) {
                origAct = activation;
              }
              else {
                assertEquals(activation, origAct);
              }
              ++index;
            }

            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              callbackInvoked = true;
             
            }

          });

      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      conn = getConnection();
      final int numExecution = 5;
      PreparedStatement ps = conn.prepareStatement(query);
      for (int i = 0; i < numExecution; ++i) {
        try {
          ps.setInt(1,i);
          ps.executeQuery();
        }
        catch (SQLException e) {
          throw new SQLException(e.toString()
              + " Exception in executing query = " + query, e
              .getSQLState());
        }
      }
      assertTrue(this.callbackInvoked);
      assertEquals(numExecution,index);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void createTableWithPrimaryKey(Connection conn) throws SQLException {
    // We create a table...
    PreparedStatement s = conn.prepareStatement("create table orders"
        + "(id int PRIMARY KEY, cust_name varchar(200), vol int, "
        + "security_id varchar(10), num int, addr varchar(100))");
    s.execute();
    // check for bug #40237 for non-null ParameterValueSet
    s = conn.prepareStatement("select * from orders where id=1");
    s.execute();
    assertEquals(0, s.getParameterMetaData().getParameterCount());
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

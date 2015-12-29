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

/**
 * 
 */

import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.DeleteQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDeleteActivation;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * 
 * @author Asif
 * 
 */
public class DeleteQueryInfoTest extends JdbcTestBase {
  private boolean callbackInvoked = false;

  public DeleteQueryInfoTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(DeleteQueryInfoTest.class));
  }

  public void testBasicDeleteOnPartitionedTable_Bug42863() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int primary key , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024))");
    s.executeUpdate("insert into TESTTABLE values (1, 'First', 'J 604')");
    String query = "Delete from TESTTABLE  where ID = 1 ";
    GemFireXDQueryObserver old = null;
    final boolean[] foundDeleteActivation = new boolean[] { false };
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof DeleteQueryInfo) {
                callbackInvoked = true;
              }

            }

            @Override
            public void afterGemFireActivationCreate(
                AbstractGemFireActivation ac) {
              foundDeleteActivation[0] = ac instanceof GemFireDeleteActivation;
            }

          });

      // Now insert some data
      this.callbackInvoked = false;

      Statement s2 = conn.createStatement();
      try {
        assertEquals(1, s2.executeUpdate(query));
        assertTrue(foundDeleteActivation[0]);
        ResultSet rs = s2.executeQuery("Select * from TESTTABLE where ID = 1");
        assertFalse(rs.next());

      } catch (SQLException e) {
        e.printStackTrace();
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e.getSQLState());
      }
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testBasicDeleteOnReplicateTable_Bug42863() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE (ID int primary key , "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024)) replicate");
    s.executeUpdate("insert into TESTTABLE values (1, 'First', 'J 604')");
    String query = "Delete from TESTTABLE  where ID = 1 ";
    GemFireXDQueryObserver old = null;
    final boolean[] foundDeleteActivation = new boolean[] { false };
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof DeleteQueryInfo) {
                callbackInvoked = true;
              }

            }

            @Override
            public void afterGemFireActivationCreate(
                AbstractGemFireActivation ac) {
              foundDeleteActivation[0] = ac instanceof GemFireDeleteActivation;
            }

          });

      // Now insert some data
      this.callbackInvoked = false;

      Statement s2 = conn.createStatement();
      try {
        assertEquals(1, s2.executeUpdate(query));
        assertTrue(foundDeleteActivation[0]);
        ResultSet rs = s2.executeQuery("Select * from TESTTABLE where ID = 1");
        assertFalse(rs.next());

      } catch (SQLException e) {
        e.printStackTrace();
        throw new SQLException(e.toString()
            + " Exception in executing query = " + query, e.getSQLState());
      }
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testPKDeleteOnReplicatedTableAccessorWithnoDataStore()
      throws Exception {
    Properties props = new Properties();
    props.put("host-data", "false");
    Connection conn = getConnection(props);
    Statement s = conn.createStatement();
    try {
      s.execute("create table TESTTABLE (ID int primary key , "
          + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024)) replicate");
      fail("Test should have failed for lack of data stores");
    } catch (SQLException sqle) {
      if (!"X0Z08".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    this.callbackInvoked = false;

    /*
    // Now insert some data
    String query = "Delete from TESTTABLE  where ID = 1 ";
    Statement s2 = conn.createStatement();
    try {
      s2.executeUpdate(query);
      fail("Test should have failed for lack of data stores");
    } catch (SQLException sqle) {
      assertEquals(sqle.getSQLState(), "X0Z08");
    }
    */
  }

  public void testBug43700() throws Exception {

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    final String subquery = "select cid from trade.portfolio where tid =? and sid >? and sid < ?";
    String dml = "delete from trade.networth n where tid = ? and n.cid IN "
        + "(" + subquery + ")";
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table trade.networth (cid int not null,  "
        + "  loanlimit int,  tid int, constraint netw_pk primary key (cid))"
        + "  partition by (tid) ");

    s
        .execute("create table trade.portfolio (cid int not null, sid int not null, "
            + "qty int not null, tid int, constraint portf_pk primary key (cid, sid))  "
            + "partition by list (tid) (VALUES (0, 1, 2, 3, 4, 5), VALUES (6, 7, 8, 9, 10, 11),"
            + " VALUES (12, 13, 14, 15, 16, 17))");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private final int index = 0;

            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {

              DeleteQueryInfoTest.this.callbackInvoked = true;
              assertTrue(qInfo instanceof DeleteQueryInfo);
              DeleteQueryInfo dqi = (DeleteQueryInfo)qInfo;
              assertFalse(dqi.hasCorrelatedSubQuery());
              assertEquals(dqi.getSubqueryInfoList().size(), 1);
              SubQueryInfo subQi = dqi.getSubqueryInfoList().iterator().next();
              assertEquals(subQi.getSubqueryString(), subquery);
              assertTrue(dqi.isRemoteGfxdSubActivationNeeded());
              assertFalse(dqi.isSubqueryFlatteningAllowed());

            }

          });

      conn.prepareStatement(dml);

      assertTrue(callbackInvoked);

    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
      SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    }

  }

  public void testBug42428ForDelete() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    GemFireXDQueryObserver old = null;
    final String subquery = "select ID2 from TESTTABLE2";
    String dml = "delete  from TESTTABLE1 where ID1 IN (" + subquery + ") ";
    try {

      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

      String table2 = "create table TESTTABLE2 (ID int not null,ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID) ) ";
      s.execute(table1 + " replicate");
      s.execute(table2);

      String insert = "Insert into  TESTTABLE1 values(1,'desc1_1','add1_1')";
      s.execute(insert);

      for (int i = 1; i < 3; ++i) {
        insert = "Insert into  TESTTABLE2 values(" + i + ",1,'desc2_"
            + "', 'add2_')";
        s.execute(insert);

      }
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private final int index = 0;

            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (!qInfo.isSelect()) {
                DeleteQueryInfoTest.this.callbackInvoked = true;
                assertTrue(qInfo instanceof DeleteQueryInfo);
                DeleteQueryInfo dqi = (DeleteQueryInfo)qInfo;
                assertFalse(dqi.hasCorrelatedSubQuery());
                assertEquals(dqi.getSubqueryInfoList().size(), 1);
                SubQueryInfo subQi = dqi.getSubqueryInfoList().iterator()
                    .next();
                assertEquals(subQi.getSubqueryString(), subquery);
                assertTrue(dqi.isRemoteGfxdSubActivationNeeded());
                assertFalse(dqi.isSubqueryFlatteningAllowed());
              }

            }

          });
      s.executeUpdate(dml);
      assertTrue(callbackInvoked);

    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
      SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    }

  }
  
  public void testBug43747() throws Exception {

    final String dml = "delete from networth n where tid = 6 and n.cid IN "
        + "(select cid from portfolio where tid =6 and sid >=2 and sid <=4 )";

    String table1 = "create table customers (cid int not null, cust_name varchar(100),"
        + "  tid int, primary key (cid))";

    String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
        + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

    String table3 = "create table portfolio (cid int not null, sid int not null,tid int,"
        + " constraint portf_pk primary key (cid, sid), constraint cust_fk foreign key (cid) references"
        + " customers (cid) on delete restrict, constraint sec_fk foreign key (sid) references"
        + " securities (sec_id) on delete restrict)";

    String table4 = "create table networth (cid int not null,   tid int,"
        + " constraint netw_pk primary key (cid), constraint cust_newt_fk foreign key (cid) references"
        + " customers (cid) on delete restrict) ";
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute(table1);
    s.execute(table2 + " replicate");
    s.execute(table3);
    s.execute(table4);
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private final int index = 0;

            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {

              DeleteQueryInfoTest.this.callbackInvoked = true;
              assertTrue(qInfo instanceof DeleteQueryInfo);
              DeleteQueryInfo dqi = (DeleteQueryInfo)qInfo;
              assertFalse(dqi.hasCorrelatedSubQuery());
              assertEquals(dqi.getSubqueryInfoList().size(), 0);
              
              assertFalse(dqi.isRemoteGfxdSubActivationNeeded());
              assertTrue(dqi.isSubqueryFlatteningAllowed());
              assertEquals(
                  ((GemFireContainer)dqi.getTargetRegion().getUserAttribute()).getTableName(),
                  "NETWORTH");

            }

          });
      s.executeUpdate(dml);
      assertTrue(callbackInvoked);

    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
      SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    }
  }

}

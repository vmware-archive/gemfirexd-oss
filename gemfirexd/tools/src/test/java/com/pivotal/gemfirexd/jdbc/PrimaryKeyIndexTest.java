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
package com.pivotal.gemfirexd.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SQLUtilities;

import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;

/**
 * junit tests for the Hash1Index
 * 
 * @todo investigate the problem that the result set is closed after
 *        accessing the run time statistics. 
 * @author yjing
 *
 */
public class PrimaryKeyIndexTest extends JdbcTestBase {

  public PrimaryKeyIndexTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(PrimaryKeyIndexTest.class));
  }

  
  public void testBug39489() throws SQLException {
    
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    
    s.execute("create table trade.customers (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid))");
    s.execute("create table trade.securities (sec_id int not null, symbol varchar(10) not null, price decimal (30, 25), exchange varchar(10) not null, tid int, constraint sec_pk primary key (sec_id), constraint sec_uq unique (symbol, exchange), constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))");
    s.execute("create table emp.employees (eid int not null constraint employees_pk primary key, emp_name varchar(100), since date, addr varchar(100), ssn varchar(9))");
    s.execute("create table trade.trades (tid int, cid int, eid int, tradedate date, primary Key (tid), foreign key (cid) references trade.customers (cid), constraint emp_fk foreign key (eid) references emp.employees (eid))");
    conn.commit();
    conn.prepareStatement("delete from trade.customers where (cust_name = ? or cid = ? ) and tid = ?");
  }
  
  /**
   * Test Primary index with DML.
   * 
   * @throws SQLException
   */

  public void testDMLOnPrimaryKeyIndex() throws SQLException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();

   // s.execute("create table t1 (c1 int primary key, c2 int NOT NULL, c3 char(20) NOT NULL, UNIQUE (c2,c3))");

    s.execute("create table t1 (c1 int not null, c2 int, c3 char(20), PRIMARY KEY (c1))");
    s.execute("insert into t1 (c1, c2, c3) values (10, 10, 'XXXX')");
    s.execute("insert into t1 (c1, c2, c3) values (20, 20, 'YYYY')");
    s.execute("insert into t1 (c1, c2, c3) values (30, 30, 'ZZZZ')");
    

    String[][] expectedRows = { { "10", "10", "XXXX" }, };

    // s.execute("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
    //test select 
    ResultSet rs = s.executeQuery("select * from t1");
   // JDBC.assertFullResultSet(rs, expectedRows);
    //
    // RuntimeStatisticsParser rsp=SQLUtilities.getRuntimeStatisticsParser(s);
    // RuntimeStatisticsParser
    // rsp=SQLUtilities.executeAndGetRuntimeStatistics(conn, "select * from t1
    // where t1.c2=10");
    // assertTrue("Using the Index to locate row!",rsp.usedIndexScan());
    
    //test delete
    //s.execute("Delete From t1 where t1.c1=10");
    
    //rs = s.executeQuery("select * from t1 where t1.c1=10");
   JDBC.assertDrainResults(rs, 3);
    
    //test update
   // s.execute("Update t1 set t1.c1=40 where t1.c1=10");
   // rs = s.executeQuery("select * from t1 where t1.c1=40");
    
    
   // String[][] expectedRows1 = { { "40", "10", "XXXX" }, };
   // JDBC.assertFullResultSet(rs, expectedRows1);
  }
  
  public void testDuplicatePrimaryKey() throws SQLException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();

    s.execute("create table t1 (c1 int primary key, c2 int, c3 char(20))");

    s.execute("insert into t1 (c1, c2, c3) values (10, 10, 'XXXX')");
    s.execute("insert into t1 (c1, c2, c3) values (20, 20, 'YYYY')");
    s.execute("insert into t1 (c1, c2, c3) values (30, 30, 'ZZZZ')");
    
    
    try {
      Monitor.getStream().println("<ExpectedException action=add>"
                                  + "com.gemstone.gemfire.cache.EntryExistsException"
                                  + "</ExpectedException>");
      Monitor.getStream().flush();

      s.execute("insert into t1 (c1, c2, c3) values (10, 10, 'XXXX')");
    }
    catch (Exception e) {
       return;
    }
    finally {
      Monitor.getStream().println("<ExpectedException action=remove>"
                                  + "com.gemstone.gemfire.cache.EntryExistsException"
                                  + "</ExpectedException>");
      Monitor.getStream().flush();
    }
    
    fail("Expected duplicate key exception");
     
  }

  /**
   * test the primary key index is used by select statement with
   * equal predicate, IN operator , and (...join).
   * 
   * @throws SQLException
   */
  public void testPrimaryKeyIndexWithTwoColumns() throws SQLException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();

    s.execute("create table t1 (c1 int, c2 int, c3 char(20), PRIMARY KEY (c1,c2))");

    s.execute("insert into t1 (c1, c2, c3) values (10, 10, 'YYYY')");
    s.execute("insert into t1 (c1, c2, c3) values (10, 20, 'AAAA')");
    s.execute("insert into t1 (c1, c2, c3) values (30, 10, 'AAAA')");
  //  s.execute("create index indx on t1 (c1)");
    
    PreparedStatement ps=conn.prepareStatement("select * from t1 where t1.c1=10 ");
    
    //String[][] expectedRows = { { "30", "10", "AAAA" }, };
    //s.execute("create index i1 on t1 (c2,c3)");

   // ps.setInt(1, 10);
   // ps.setInt(2, 20);
   // ps.setInt(3, 30);
    // s.execute("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
    ResultSet rs = ps.executeQuery();                 //executeQuery("select * from t1 where t1.c1 in (?,?,?) for update");
    
    
    //
    // RuntimeStatisticsParser rsp=SQLUtilities.getRuntimeStatisticsParser(s);
    // RuntimeStatisticsParser
    // rsp=SQLUtilities.executeAndGetRuntimeStatistics(conn, "select * from t1
    // where t1.c2=10");
    // assertTrue("Using the Index to locate row!",rsp.usedIndexScan());
  
    JDBC.assertDrainResults(rs, 2);
   // JDBC.assertFullResultSet(rs, expectedRows);

  }
  public void testHashPrimaryKeyIndexWithInPredicate() throws SQLException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    
    
    s.execute("create table t1 (c1 int primary key, c2 int, c3 char(20))");
    s.execute("create index i1 on t1 (c1)");
    PreparedStatement ps=
      conn.prepareStatement("insert into t1 (c1, c2, c3) values (?, 10, 'YYYY')");
    for(int i=1; i<100;i++) {
      ps.setInt(1, i);
      ps.execute();
    }
 
    //s.execute("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
    ResultSet rs = s.executeQuery("select * from t1 where t1.c1 in (10,20,30) ");
    
   // RuntimeStatisticsParser rsp=SQLUtilities.getRuntimeStatisticsParser(s);
    //  RuntimeStatisticsParser
     //   rsp=SQLUtilities.executeAndGetRuntimeStatistics(conn, 
     //       "select * from t1 where t1.c1=10");
     
    // assertTrue(rsp.toString(),false);
   //  assertTrue("Using the Index to locate row!",rsp.usedIndexScan());
     JDBC.assertDrainResults(rs, 3);
   //  throw new SQLException("the debug purpose!");

  }

  
  public void testPrimaryKeyWithInequality() throws SQLException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();

    s.execute("create table t1 (c1 int primary key, c2 int, c3 char(20))");

    s.execute("insert into t1 (c1, c2, c3) values (10, 10, 'YYYY')");
    s.execute("insert into t1 (c1, c2, c3) values (20, 10, 'AAAA')");
    s.execute("insert into t1 (c1, c2, c3) values (30, 10, 'AAAA')");
    s.execute("create index indx on t1 (c1)");
    
    //s.execute("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
    
    ResultSet rs=s.executeQuery("select * from t1 where t1.c1>=10 ");
    //RuntimeStatisticsParser rsp=SQLUtilities.getRuntimeStatisticsParser(s);
    
    //assertTrue(rsp.statistics,false);
    // RuntimeStatisticsParser
    // rsp=SQLUtilities.executeAndGetRuntimeStatistics(conn, "select * from t1
    // where t1.c2=10");
    // assertTrue("Using the Index to locate row!",rsp.usedIndexScan());
  
    JDBC.assertDrainResults(rs, 3);
  }
  
  public void testNullWithPrimaryKeySyntax() throws SQLException {
  
    // Test the JDBC standard 'NULL' keyword should not be allowed in conjunction with 
    // the PRIMARY KEY constraint, at either column or table level
    Connection conn = getConnection();
    Statement s = conn.createStatement();

    try {
      s.executeQuery(
          "create table nullpk1( col1 int null primary key)");
      fail("Should have failed with sqlstate 42831");
    } catch (SQLException sqe) {
      if (!sqe.getSQLState().equals("42831")) {
        throw sqe;
      }
    }
    try {
      s.executeQuery(
          "create table nullpk2( col1 int null, constraint pk1 primary key(col1))");
      fail("Should have failed with sqlstate 42831");
    } catch (SQLException sqe) {
      if (!sqe.getSQLState().equals("42831")) {
        throw sqe;
      }
    }
  }


}

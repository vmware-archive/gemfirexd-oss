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
package com.pivotal.gemfirexd.internal.engine.distributed.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.sql.GeneralizedStatement;
import com.pivotal.gemfirexd.internal.engine.sql.compile.SQLMatcherConstants;
import com.pivotal.gemfirexd.internal.engine.sql.execute.ConstantValueSetImpl;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;
import com.pivotal.gemfirexd.security.SecurityTestUtils;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class StatementCachingTest extends JdbcTestBase {
  
  private volatile boolean shouldWait = true;
  private AtomicIntegerArray stmtCompileState = null;
  private static final ThreadLocal<GemFireXDQueryObserver> tObserver =
    new ThreadLocal<GemFireXDQueryObserver>();

  private CountDownLatch countNumThreads = null;
  private boolean ignorePKViolation = false;

  public StatementCachingTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(StatementCachingTest.class));
    System.setProperty("gemfirexd.debug.true",
        GfxdConstants.TRACE_STATEMENT_MATCHING);
  }

  /**
   * It works now as GenericQualifier fixes negate
   * condition.
   * #42864
   */
  public void testLike() throws Exception {
    setupConnection();
    //Connection conn = getConnection();
    Connection conn = startNetserverAndGetLocalNetConnection();
    Statement st = conn.createStatement();

    st.execute("create table e.employee ( id int PRIMARY KEY, "
        + "fname varchar (50) NOT NULL, lname varchar (50) NULL, "
        + "email varchar(50) NULL)");

    PreparedStatement ps = conn
        .prepareStatement("insert into \"E\".employee values (?, ?, ?, ?)");
    int sumId = 0;
    for (int id = 1000; id <= 1500; ++id) {
      ps.setInt(1, id);
      ps.setString(2, "gfxd" + id);
      ps.setString(3, "gem" + id);
      ps.setString(4, "test" + id + "@pivotal.io");
      ps.addBatch();
      sumId += id;
    }
    ps.executeBatch();

    ResultSet rs = st
        .executeQuery("select sum(id) from e.employee where id >= 1000");
    assertTrue(rs.next());
    assertEquals(sumId, rs.getInt(1));
    rs.close();

    /*
    //ps = conn.prepareStatement("update \"E\".\"EMPLOYEE\" "
    //    + "set \"LNAME\"=? where \"ID\"=?");
    ps = conn.prepareStatement("update e.employee set lname=? where id=?");
    sumId = 0;
    for (int id = 1273; id <= 1380; ++id) {
      ps.setInt(2, id);
      ps.setString(1, "gem" + id);
      ps.addBatch();
      sumId += id;
    }
    ps.executeBatch();
    */

    Connection conn2 = jdbcConn;
    Statement st2 = conn2.createStatement();
    rs = st2
        .executeQuery("select sum(id) from e.employee where lname like 'gem%'");
    assertTrue(rs.next());
    assertEquals(sumId, rs.getInt(1));
    rs.close();

    rs = st
        .executeQuery("select sum(id) from e.employee where lname like 'gem%'");
    assertTrue(rs.next());
    assertEquals(sumId, rs.getInt(1));
    rs.close();

    stopNetServer();
  }

  // Uncomment after fixing #42304
  public void _testTriggerInvocation() throws SQLException {
    Connection conn = getConnection();
    Statement st = conn.createStatement();

    st.execute("create table x (x int, constraint ck check (x > 0))");

    st.execute("create trigger tokv1 NO CASCADE before insert on x "
        + "for each statement values 2");

    st.execute("insert into x values 1");

    st.execute("insert into x values (1) , (3), (4), (5) ");
  }

  public void testUserQueryRestore() {
    final String query1 = "Insert into TESTTABLE values(<?>,<?>,<?>,<?>)";
    final String expected1 = "Insert into TESTTABLE values(1,'desc1','Add1',1)";
    final int[] tokentypes1 = new int[] {
        SQLMatcherConstants.EXACT_NUMERIC,
        SQLMatcherConstants.STRING,
        SQLMatcherConstants.STRING,
        SQLMatcherConstants.EXACT_NUMERIC
    };
    
    final String[] tokenstrings1 = new String[] {
      "1",
      "desc1",
      "Add1",
      "1"
    };
    
    ConstantValueSetImpl cvs1 = new ConstantValueSetImpl(tokentypes1, tokenstrings1);
    
    final String userquery1 = GeneralizedStatement.recreateUserQuery(query1, cvs1);
    
    assertEquals(expected1, userquery1);

    final String expected = "update trade.customers set cust_name ='name1' , "
        + "addr ='address is name 1' where cid=83 and tid =5";
    final String query = "update trade.customers set cust_name =<?> , "
        + "addr =<?> where cid=<?> and tid =<?>";

    final int[] tokentypes = new int[] {
        SQLMatcherConstants.STRING,
        SQLMatcherConstants.STRING,
        SQLMatcherConstants.EXACT_NUMERIC,
        SQLMatcherConstants.EXACT_NUMERIC
    };
    
    final String[] tokenstrings = new String[] {
      "name1",
      "address is name 1",
      "83",
      "5"
    };
    
    ConstantValueSetImpl cvs = new ConstantValueSetImpl(tokentypes, tokenstrings );
    
    final String userquery = GeneralizedStatement.recreateUserQuery(query, cvs);
    
    assertEquals(expected, userquery);
  }
  
  @SuppressWarnings("serial")
  public void testBasicStatementReuse() throws SQLException {
    
    Connection conn = getConnection();
    
    conn.createStatement().execute(
        "create table flights (id int, val varchar(10))");

    Statement dmlSt = conn.createStatement();
    
    //build the cache with primary strings without observer.
    //honoring isGenericQueryCompiled = true 
    dmlSt.execute("delete from flights where id = 1");
    dmlSt.execute("insert into flights values (1, 'one')");
    dmlSt.executeQuery("select * from flights where id = 1");
    dmlSt.execute("update flights set id = 3, val = 'three' where 1=1 ");
    
    //set an observer to ensure now on parsing alone optionally happen
    //and no optimization.
    GemFireXDQueryObserver old = null;
    try {
      
      final boolean[] shouldParseQuery = new boolean[] {true};
      
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void afterQueryParsing(String query, StatementNode qt,
                LanguageConnectionContext lcc) {

              if (!shouldParseQuery[0]) {
                throw new AssertionError("parsing shouldn't have happened for "
                    + query);
              }
            }

            @Override
            public void afterOptimizedParsedTree(String query,
                StatementNode qt, LanguageConnectionContext lcc) {
              throw new AssertionError(
                  "optimization phase should NOT have been called for " + query);
            }

          });

      String query = "";

      //same statement should not parse/bind/optimize.
      shouldParseQuery[0] = false;
      ResultSet rs = dmlSt.executeQuery("select * from flights where id = 1");
      while(rs.next()) {
        getLogger().info("read data " + rs.getInt(1));
      }

      //similar statement should not parse/bind/optimize.
      shouldParseQuery[0] = false;
      rs = dmlSt.executeQuery("select * from flights where id = 3");
      while(rs.next()) {
        getLogger().info("read data " + rs.getInt(1));
      }
      
      //similar statement should can parse but no bind/optimization shall happen.
      shouldParseQuery[0] = true;
      query = "insert into flights values (2, 'two')";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'one row effected'", dmlSt
          .getUpdateCount() == 1);      

      //same exact statement should not be re-parsed
      shouldParseQuery[0] = false;
      query = "insert into flights values (1, 'one')";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'one row effected'", dmlSt
          .getUpdateCount() == 1);      
      

      shouldParseQuery[0] = true;
      query = "update flights set id = 4, val = 'four' where 1=1 ";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'three row effected' "
          + "but effected with " + dmlSt.getUpdateCount(),
          dmlSt.getUpdateCount() == 3);

      shouldParseQuery[0] = false;
      query = "update flights set id = 4, val = 'four' where 1=1 ";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'three row effected'",
          dmlSt.getUpdateCount() == 3);      

      
      shouldParseQuery[0] = false;
      query = "delete from flights where id = 1";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'zero row effected'",
          dmlSt.getUpdateCount() == 0);      

      
      shouldParseQuery[0] = true;
      query = "delete from flights where id = 4";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'zero row effected'",
          dmlSt.getUpdateCount() == 3);
      
      shouldParseQuery[0] = false;
      query = "delete from flights where id = 4";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'zero row effected'",
          dmlSt.getUpdateCount() == 0);      
    }
    finally {
      try { conn.createStatement().execute("drop table flights"); } 
      catch(Throwable t) {
        //ignore  
      }
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }

  }
  
  public void testBasicStatementReuse_Replicate() throws SQLException {
    Connection conn = getConnection();
    
    conn.createStatement().execute(
        "create table flights (id int, val varchar(10)) replicate");

    Statement dmlSt = conn.createStatement();
    
    //build the cache with primary strings without observer.
    //honoring isGenericQueryCompiled = true 
    dmlSt.execute("delete from flights where id = 1");
    dmlSt.execute("insert into flights values (1, 'one')");
    dmlSt.executeQuery("select * from flights where id = 1");
    dmlSt.execute("update flights set id = 3, val = 'three' where 1=1 ");
    
    //set an observer to ensure now on parsing alone optionally happen
    //and no optimization.
    GemFireXDQueryObserver old = null;
    try {
      
      final boolean[] shouldParseQuery = new boolean[] {true};
      
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void afterQueryParsing(String query, StatementNode qt,
                LanguageConnectionContext lcc) {

              if (!shouldParseQuery[0]) {
                throw new AssertionError("parsing shouldn't have happened for "
                    + query);
              }

            }

            @Override
            public void afterOptimizedParsedTree(String query,
                StatementNode qt, LanguageConnectionContext lcc) {
              throw new AssertionError(
                  "optimization phase should NOT have been called for " + query);
            }

          });
      
      String query = "";
      
      //similar statement should not parse/bind/optimize.
      shouldParseQuery[0] = false;
      ResultSet rs = dmlSt.executeQuery("select * from flights where id = 3");
      while(rs.next()) {
        getLogger().info("read data " + rs.getInt(1));
      }
      
      //similar statement should can parse but no bind/optimization shall happen.
      shouldParseQuery[0] = true;
      query = "insert into flights values (2, 'two')";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'one row effected'", dmlSt
          .getUpdateCount() == 1);      

      //same exact statement should not be re-parsed
      shouldParseQuery[0] = false;
      query = "insert into flights values (1, 'one')";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'one row effected'", dmlSt
          .getUpdateCount() == 1);      
      

      shouldParseQuery[0] = true;
      query = "update flights set id = 4, val = 'four' where 1=1 ";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'three row effected' "
          + "but effected with " + dmlSt.getUpdateCount(),
          dmlSt.getUpdateCount() == 3);

      shouldParseQuery[0] = false;
      query = "update flights set id = 4, val = 'four' where 1=1 ";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'three row effected'",
          dmlSt.getUpdateCount() == 3);      

      
      shouldParseQuery[0] = false;
      query = "delete from flights where id = 1";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'zero row effected'",
          dmlSt.getUpdateCount() == 0);      

      
      shouldParseQuery[0] = true;
      query = "delete from flights where id = 4";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'zero row effected'",
          dmlSt.getUpdateCount() == 3);
      
      shouldParseQuery[0] = false;
      query = "delete from flights where id = 4";
      dmlSt.execute(query);
      assertTrue(query + " should have resulted into 'zero row effected'",
          dmlSt.getUpdateCount() == 0);      
    }
    finally {
      try { conn.createStatement().execute("drop table flights"); } 
      catch(Throwable t) {
        //ignore  
      }
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  
  public void testQuotedTables() throws SQLException {
    Connection conn = getConnection();
    
    Statement stmt = conn.createStatement();
    
    stmt.executeUpdate("create table \"my table\" (x int)");
    stmt.executeUpdate("insert into \"my table\" values (1), (2), (3) ");
    ResultSet rs = stmt.executeQuery("select * from \"my table\"");
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertFalse(rs.next());
    
    rs = stmt.executeQuery("select * from \"my table\" order by x");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertFalse(rs.next());
  }
  
  public void testQuotedCursorsUpdate() throws SQLException {
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    
    stmt.executeUpdate("create table \"my table\" (x int)");
    stmt.executeUpdate("insert into \"my table\" values (1), (2), (3) ");
    
    stmt.close();
    
    stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
        ResultSet.CONCUR_UPDATABLE);
    stmt.setCursorName("\"\"my quoted cursor\"\" \"\"with quotes "
        + "in middle\"\"and last \"\"");
    ResultSet rs = stmt.executeQuery("select * from \"my table\"");
    rs.next();

    // remember which int was updated
    int updatedInt = rs.getInt(1);

    rs.updateInt(1, 4);
    rs.updateRow();
    rs.close();
    
    rs = stmt.executeQuery("select * from \"my table\" order by x");
    

    // in GemFireXD, queries are not guaranteed to return results
    // in the same order they were inserted, so changing this test
    // to not assume which x was updated
    List<Integer> expected = new ArrayList<Integer>(Arrays.asList(1, 2, 3));
    expected.remove((Integer)updatedInt);
    expected.add(4);
    
    for (int i=2; i<=4; i++) {
        assertTrue("there is a row", rs.next());
        assertTrue("row contains correct value",
                 expected.remove((Integer)rs.getInt(1)));
    }
    assertTrue("table correct size", expected.isEmpty());
    
    rs.close();
    stmt.close();        
  }
  
  public void testStatementPreparedStatementMixing() throws SQLException {

    Connection conn = getConnection();
    Statement st = conn.createStatement();

    conn.createStatement().execute(
        " create table course ( " + "course_id int, course_name varchar(20),"
            + "primary key(course_id)" + ") replicate ");

    conn.createStatement().execute(
        " create table student ( " + "st_id int, c_id int,"
            + "primary key(st_id, c_id)" + ") replicate ");

    st.execute("insert into student values(1, 1)");
    st.execute("insert into student values(1, 2)");
    st.execute("insert into student values(1, 3)");
    st.execute("insert into student values(2, 1)");
    st.execute("insert into student values(2, 4)");

    st.execute("insert into course values(1, 'ONE')");
    st.execute("insert into course values(2, 'TWO')");
    st.execute("insert into course values(3, 'THREE')");
    st.execute("insert into course values(4, 'FOUR')");
    st.execute("insert into course values(5, 'FIVE')");

    long[] res = new long[] { 3, 2, 1 };

    ResultSet st_st = st.executeQuery("select c_id from student "
        + "where st_id = 1 and c_id not in (select distinct course_id "
        + "from course) order by c_id desc NULLS first ");

    PreparedStatement c_ps = conn
        .prepareStatement("select * from course where course_id = ? ");

    while (st_st.next()) {
      int i = st_st.getInt(1);
      c_ps.setInt(1, i);
      ResultSet rc = c_ps.executeQuery();
      for (int r = 0; i < res.length; i++, rc.next()) {
        assertEquals(res[r], rc.getLong("course_id"));
      }
    }

  }

  public void testConcurrentInsertGenericStmtSeeking() throws SQLException,
      InterruptedException {
    shouldWait = true;
    Connection conn = getConnection();

    conn.createStatement().execute(
        "create table flights (id int primary key, val varchar(10))");

    Statement dmlSt = conn.createStatement();

    //prepare the initial plan (so that generic plan exists).
    //honoring isGenericQueryCompiled = true 
    dmlSt.execute("insert into flights values (1, 'one')");
    dmlSt.execute("delete from flights where id = 1");
    

    try {
      
      final Object sync = new Object();
      

      Thread[] parThds = new Thread[3];
      countNumThreads = new CountDownLatch(parThds.length);
      stmtCompileState = new AtomicIntegerArray(1);

      parThds[0] = getExecutionThread("insert into flights values (1, 'one')",
          false, false, null, 0);
      parThds[1] = getExecutionThread("insert into flights values (2, 'two')",
          sync);
      parThds[2] = getExecutionThread(
          "insert into flights values (3, 'three')", sync);

      for (Thread t : parThds) {
        t.start();
      }
      countNumThreads.await();

      synchronized (sync) {
        shouldWait = false;
        sync.notifyAll();
      }
      
      for(Thread t : parThds) {
        t.join();
      }
      
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      
      ResultSet rs = dmlSt.executeQuery("select count(*) from flights");
      assertTrue(rs.next());
      assertTrue(rs.getInt(1) == 3);
      assertFalse(rs.next());
      
    } finally {
      try { conn.createStatement().execute("drop table flights"); } 
      catch(Throwable t) {
        //ignore  
      }
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter());
    }
  }

  public void testConcurrentInsertStmtCompilation() throws SQLException,
      InterruptedException {
    shouldWait = true;
    Connection conn = getConnection();

    conn.createStatement().execute("create table flights (id int, "
        + "val varchar(20),  primary key (id, val) )");

    try {

      final Object sync = new Object();

      Thread[] parThds = new Thread[20];
      countNumThreads = new CountDownLatch(parThds.length);
      stmtCompileState = new AtomicIntegerArray(1);

      parThds[0] = getExecutionThread("insert into flights values (1, 'one')",
          sync);
      parThds[1] = getExecutionThread("insert into flights values (2, 'two')",
          sync);
      parThds[2] = getExecutionThread(
          "insert into flights values (3, 'three')", sync);
      parThds[3] = getExecutionThread("insert into flights values (4, 'four')",
          sync);
      parThds[4] = getExecutionThread("insert into flights values (5, 'five')",
          sync);
      parThds[5] = getExecutionThread("insert into flights values (6, 'six')",
          sync);
      parThds[6] = getExecutionThread(
          "insert into flights values (7, 'seven')", sync);
      parThds[7] = getExecutionThread(
          "insert into flights values (8, 'eight')", sync);
      parThds[8] = getExecutionThread("insert into flights values (9, 'nine')",
          sync);
      parThds[9] = getExecutionThread("insert into flights values (10, 'ten')",
          sync);

      parThds[10] = getExecutionThread(
          "insert into flights values (11, 'eleven')", sync);
      parThds[11] = getExecutionThread(
          "insert into flights values (12, 'twelve')", sync);
      parThds[12] = getExecutionThread(
          "insert into flights values (13, 'thirteen')", sync);
      parThds[13] = getExecutionThread(
          "insert into flights values (14, 'fourteen')", sync);
      parThds[14] = getExecutionThread(
          "insert into flights values (15, 'fiveteen')", sync);
      parThds[15] = getExecutionThread(
          "insert into flights values (16, 'sixteen')", sync);
      parThds[16] = getExecutionThread(
          "insert into flights values (17, 'seventeen')", sync);
      parThds[17] = getExecutionThread(
          "insert into flights values (18, 'eighteen')", sync);
      parThds[18] = getExecutionThread(
          "insert into flights values (19, 'nineteen')", sync);
      parThds[19] = getExecutionThread(
          "insert into flights values (20, 'twenty')", sync);

      for (Thread t : parThds) {
        t.start();
      }
      countNumThreads.await();

      synchronized (sync) {
        shouldWait = false;
        sync.notifyAll();
      }
      
      for(Thread t : parThds) {
        t.join();
      }
      
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      
      Statement dmlSt = conn.createStatement();
      ResultSet rs = dmlSt.executeQuery("select count(*) from flights");
      assertTrue(rs.next());
      assertTrue(rs.getInt(1) == 20);
      assertFalse(rs.next());
      
    } finally {
      try { conn.createStatement().execute("drop table flights"); } 
      catch(Throwable t) {
        //ignore  
      }
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter());
    }
  }

  public void testInsertStmtCompilationSomeThrowingError() throws SQLException,
      InterruptedException {
    shouldWait = true;
    ignorePKViolation = true;
    Connection conn = getConnection();

    conn.createStatement().execute("create table flights (id int, "
        + "val varchar(20),  primary key (id, val) )");

    try {

      final Object sync = new Object();

      Thread[] parThds = new Thread[20];
      countNumThreads = new CountDownLatch(parThds.length);
      stmtCompileState = new AtomicIntegerArray(1);
      
      try {
        conn.createStatement().executeQuery("insert into flights values (null, null)");
      } catch(SQLException e) {
        //ignore invalid insert
        if(!e.getSQLState().equals("23502")) {
          throw e;
        }
      }

      parThds[0] = getExecutionThread("insert into flights values (1, 'one')",
          sync);
      parThds[1] = getExecutionThread("insert into flights values (2, 'two')",
          sync);
      parThds[2] = getExecutionThread(
          "insert into flights values (3, 'three')", sync);
      parThds[3] = getExecutionThread("insert into flights values (4, 'four')",
          sync);
      parThds[4] = getExecutionThread("insert into flights values (5, 'five')",
          sync);
      parThds[5] = getExecutionThread("insert into flights values (6, 'six')",
          sync);
      parThds[6] = getExecutionThread(
          "insert into flights values (7, 'seven')", sync);
      parThds[7] = getExecutionThread(
          "insert into flights values (8, 'eight')", sync);
      parThds[8] = getExecutionThread("insert into flights values (9, 'nine')",
          sync);
      parThds[9] = getExecutionThread("insert into flights values (10, 'ten')",
          sync);

      // lets simulate SQLException unwinding ...
      parThds[10] = getExecutionThread("insert into flights values (1, 'one')",
          sync);
      parThds[11] = getExecutionThread("insert into flights values (2, 'two')",
          sync);
      parThds[12] = getExecutionThread(
          "insert into flights values (3, 'three')", sync);
      parThds[13] = getExecutionThread(
          "insert into flights values (4, 'four')", sync);
      parThds[14] = getExecutionThread(
          "insert into flights values (5, 'five')", sync);
      parThds[15] = getExecutionThread("insert into flights values (6, 'six')",
          sync);
      parThds[16] = getExecutionThread(
          "insert into flights values (7, 'seven')", sync);
      parThds[17] = getExecutionThread(
          "insert into flights values (8, 'eight')", sync);
      parThds[18] = getExecutionThread(
          "insert into flights values (9, 'nine')", sync);
      parThds[19] = getExecutionThread(
          "insert into flights values (10, 'ten')", sync);

      for (Thread t : parThds) {
        t.start();
      }
      countNumThreads.await();

      synchronized (sync) {
        shouldWait = false;
        sync.notifyAll();
      }
      
      for(Thread t : parThds) {
        t.join();
      }
      
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      
      Statement dmlSt = conn.createStatement();
      ResultSet rs = dmlSt.executeQuery("select count(*) from flights");
      assertTrue(rs.next());
      assertTrue(rs.getInt(1) == 10);
      assertFalse(rs.next());
      
    } finally {
      try { conn.createStatement().execute("drop table flights"); } 
      catch(Throwable t) {
        //ignore  
      }
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter());
        ignorePKViolation = false;
    }
  }

  public void testConcurrentSelectStmtCompilation() throws SQLException,
      InterruptedException {
    shouldWait = true;
    ignorePKViolation = false;
    Connection conn = getConnection();

    conn.createStatement().execute("create table flights (id int, "
        + "val varchar(20),  primary key (id, val) )");

    PreparedStatement ps = conn
        .prepareStatement("insert into flights values (?, ?)");

    final int totalRows = 1000;
    for (int i = totalRows; i > 0; i--) {
      ps.setInt(1, i);
      ps.setString(2, "DUMMY");
      ps.execute();
    }

    Statement dmlSt = conn.createStatement();
    ResultSet rs = dmlSt.executeQuery("select count(*) from flights");
    assertTrue(rs.next());
    assertTrue(rs.getInt(1) == totalRows);
    assertFalse(rs.next());

    try {

      final Object sync = new Object();

      Thread[] parThds = new Thread[40];
      countNumThreads = new CountDownLatch(parThds.length);
      stmtCompileState = new AtomicIntegerArray(1);
      
      final String baseQueries = "select * from flights where id > ";
      
      // 0-9
      for (int i = 0; i < 10; i++) {
        parThds[i] = getSelectExecutionThread(baseQueries + "990", sync);
      }

      // 10-19
      for (int i = 10; i < 20; i++) {
        parThds[i] = getSelectExecutionThread(baseQueries + "998", sync);
      }

      // 20-29
      for (int i = 20; i < 30; i++) {
        parThds[i] = getSelectExecutionThread(baseQueries + (totalRows / 100),
            sync);
      }

      // 30-39
      Random rand = PartitionedRegion.rand;
      for (int i = 30; i < 40; i++) {
        parThds[i] = getSelectExecutionThread(baseQueries
            + rand.nextInt(totalRows), sync);
      }

      for (Thread t : parThds) {
        t.start();
      }

      countNumThreads.await();
      
      synchronized (sync) {
        shouldWait = false;
        sync.notifyAll();
      }

      for (Thread t : parThds) {
        t.join();
      }
    }
    catch(Throwable t) {
      getLogger().error("ERROR: ", t);
      throw new RuntimeException(t);
    }
    finally {
      try { conn.createStatement().execute("drop table flights"); } 
      catch(Throwable t) {
        //ignore  
      }
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter());
        ignorePKViolation = false;
    }
  }

  public void testSchemaSwitching_Simple() throws SQLException {

    final String schemaName = "COMMON_SCHEMA";

    Connection conn = getConnection();

    Statement stmts = conn.createStatement();

    stmts.execute("create schema " + schemaName);

    String curSchema = SecurityTestUtils.currentSchema(conn.createStatement());

    stmts.execute("set schema " + schemaName);

    SecurityTestUtils.assertCurrentSchema(stmts, schemaName);

    stmts.execute("create table test_table (c1 int, c2 int, "
        + "primary key (c1) ) replicate");

    stmts.execute("create table test_table_pr (c1 int, c2 int, "
        + "primary key(c1) )");

    conn.close();

    conn = getConnection();
    stmts = conn.createStatement();

    curSchema = SecurityTestUtils.currentSchema(conn.createStatement());
    stmts.execute("set schema " + curSchema);

    SecurityTestUtils.assertCurrentSchema(conn.createStatement(), curSchema);

    ResultSet tabexist = stmts.executeQuery("select tablename from "
        + "sys.systables where tablename = 'test_table'");

    if (tabexist.next()) {
      String tab = tabexist.getString(1);
      if (tab != null) {
        assertEquals("test_table", tab);
      }
    }
    try {
      stmts.execute("drop table test_table");
    } catch (SQLException sqe_ignore) {
    }

    tabexist = stmts.executeQuery("select tablename from "
        + "sys.systables where tablename = 'test_table_pr'");

    if (tabexist.next()) {
      String tab = tabexist.getString(1);
      if (tab != null) {
        assertEquals("test_table_pr", tab);
      }
    }
    try {
      stmts.execute("drop table test_table_pr");
    } catch (SQLException sqe_ignore) {
    }

    stmts.execute("create table test_table (c1 int, c2 varchar(1), "
        + "primary key (c1) ) replicate");

    stmts.execute("create table test_table_pr (c1 int, c2 varchar(1), "
        + "primary key(c1) )");

    stmts.execute("set schema " + schemaName);

    SecurityTestUtils.assertCurrentSchema(stmts, schemaName);

    stmts.execute("insert into test_table values(1,1), (2,2)");

    stmts.execute("insert into test_table_pr values(1,1), (2,2)");

    ResultSet rs = stmts.executeQuery("select count(1) from test_table "
        + "where c2 = 2");

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));

    ResultSet rs1 = stmts.executeQuery("select count(1) from test_table "
        + "where c2 = 2");

    assertTrue(rs1.next());
    assertEquals(1, rs1.getInt(1));

    stmts.execute("set schema " + curSchema);

    SecurityTestUtils.assertCurrentSchema(stmts, curSchema);

    rs = stmts.executeQuery("select count(1) from test_table where c2 = '3'");

    assertTrue(rs.next());
    assertEquals(0, rs.getInt(1));

    rs1 = stmts.executeQuery("select count(1) from test_table where c2 = '3'");

    assertTrue(rs1.next());
    assertEquals(0, rs1.getInt(1));

    stmts.execute("set schema " + schemaName);

    SecurityTestUtils.assertCurrentSchema(stmts, schemaName);

    stmts.execute("update test_table set c2 = 3 ");

    stmts.execute("update test_table set c2 = 3 ");

    rs = stmts.executeQuery("select count(1) from test_table where c2 = 3");

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));

    rs1 = stmts.executeQuery("select count(1) from test_table where c2 = 3");

    assertTrue(rs1.next());
    assertEquals(2, rs1.getInt(1));
  }

  public void testGroupByHavingWithWhereClause() throws SQLException {
    Connection conn = getConnection();
    conn.createStatement().execute("create table flights (id int, "
        + "val varchar(20),  primary key (id, val) )");

    PreparedStatement ps = conn
        .prepareStatement("insert into flights values (?, ?)");

    final int totalRows = 10;
    for (int i = totalRows; i > 0; i--) {
      ps.setInt(1, i);
      ps.setString(2, "DUMMY" + (i % 2));
      ps.execute();
    }

    ResultSet rs = conn.createStatement().executeQuery("select val from "
        + "flights where id > 1 group by val having count(val) > 0");
    while (rs.next()) {
      System.out.println(rs.getObject(1));
    }
  }

  public void testMultipleSelectStmtCompilation() throws SQLException,
      InterruptedException {
    shouldWait = true;
    ignorePKViolation = false;
    Connection conn = getConnection();

    conn.createStatement().execute("create table flights (id int, "
        + "val varchar(20),  primary key (id, val) )");

    PreparedStatement ps = conn
        .prepareStatement("insert into flights values (?, ?)");

    final int totalRows = 1000;
    for (int i = totalRows; i > 0; i--) {
      ps.setInt(1, i);
      ps.setString(2, "DUMMY" + (i % 2));
      ps.execute();
    }

    Statement dmlSt = conn.createStatement();
    ResultSet rs = dmlSt.executeQuery("select count(*) from flights");
    assertTrue(rs.next());
    assertTrue(rs.getInt(1) == totalRows);
    assertFalse(rs.next());

    try {

      final Object sync = new Object();

      Thread[] parThds = new Thread[40];
      countNumThreads = new CountDownLatch(parThds.length);

      final String[] baseQueries = new String[] {
          "select * from flights where id > ",
          "select * from flights where id > ? order by 1",
          "select val from flights where id > ? group by val",
          "select * from flights where id > ? and "
              + "(case when val like 'DUMMY2' then 1 else 2 end) > 0 " };

      stmtCompileState = new AtomicIntegerArray(baseQueries.length);
      
      // 0-9
      for (int i = 0; i < 10; i++) {
        parThds[i] = getExecutionThread(baseQueries[0] + "990",
            true/*isSelect*/, true, sync, 0);
      }

      // 10-19
      for (int i = 10; i < 20; i++) {
        parThds[i] = getExecutionThread(baseQueries[1].replace("?", "998"),
            true/*isSelect*/, true, sync, 1);
      }

      // 20-29
      for (int i = 20; i < 30; i++) {
        parThds[i] = getExecutionThread(baseQueries[2].replace("?", Integer
            .toString((totalRows / 100))), true/*isSelect*/, true, sync, 2);
      }

      // 30-39
      Random rand = PartitionedRegion.rand;
      for (int i = 30; i < 40; i++) {
        parThds[i] = getExecutionThread(baseQueries[3].replace("?", Integer
            .toString(rand.nextInt(totalRows))), true/*isSelect*/, true, sync,
            3);
      }

      for (Thread t : parThds) {
        t.start();
      }

      countNumThreads.await();
      
      synchronized (sync) {
        shouldWait = false;
        sync.notifyAll();
      }

      for (Thread t : parThds) {
        t.join();
      }
    }
    catch(Throwable t) {
      getLogger().error("ERROR: ", t);
      throw new RuntimeException(t);
    }
    finally {
      try { conn.createStatement().execute("drop table flights"); } 
      catch(Throwable t) {
        //ignore  
      }
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter());
        ignorePKViolation = false;
    }
  }
  
  
  private Thread getExecutionThread(final String query, final Object waitOn) {
    return getExecutionThread(query, false /*isSelect*/, true, waitOn, 0);
  }

  private Thread getSelectExecutionThread(final String query,
      final Object waitOn) {
    return getExecutionThread(query, true /*isSelect*/, true, waitOn, 0);
  }
  
  @SuppressWarnings("serial")
  private Thread getExecutionThread(final String query, final boolean isSelect,
      final boolean shouldParsingOccur, final Object waitOn, final int queryNo) {
    
    
    return new Thread(new Runnable() {
      
      private final GemFireXDQueryObserver observer =
        new GemFireXDQueryObserverAdapter() {

        private final String prefix() {
          return Thread.currentThread().toString() + "@"
              + Integer.toHexString(System.identityHashCode(this)) + " ";
        }
        
        @Override
        public void afterQueryParsing(String query, StatementNode qt,
            LanguageConnectionContext lcc) {

          if(tObserver.get() != this) {
            return;
          }

          if (!shouldParsingOccur) {
            assertTrue("Won't wait, so wait object must be null ",
                waitOn == null);
            throw new AssertionError(prefix()
                + System.identityHashCode(shouldParsingOccur)
                + " parsing shouldn't have happened for " + query);
          }

          while (shouldWait) {
            synchronized (waitOn) {
              if (!shouldWait) break;
              try {
                waitOn.wait();
              } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException("Got Interrupted for " + query
                    + "... ", e);
              }
            }
          }
        }

        @Override
        public void afterOptimizedParsedTree(String query,
            StatementNode qt, LanguageConnectionContext lcc) {
          
          //ignore other observers in the list other than
          //current threads observer.
          if(tObserver.get() != this) {
            return;
          }

          assertTrue(stmtCompileState != null);
          if (stmtCompileState.getAndIncrement(queryNo) > 0) {
            throw new AssertionError(prefix() + "for query " + queryNo + " "
                + query + " optimization phase should NOT have been called. ");
          }

          Misc.getGemFireCache().getLogger().info(
              prefix() + query + " SUCCESS compilation");
        }
      };
      
      @Override
      public void run() {
        countNumThreads.countDown();
        Connection conn = null;

        try {
          conn = getConnection();
          // set an observer to ensure only parsing happens optionally and no
          // optimization occur.
          GemFireXDQueryObserverHolder.putInstance(observer);
          tObserver.set(observer);

          if (!isSelect) {
            conn.createStatement().execute(query);
          }
          else {
            ResultSet rs = conn.createStatement().executeQuery(query);
            ResultSetMetaData rsM = rs.getMetaData();
            final int colCnt = rsM.getColumnCount();

            while (rs.next()) {
              final StringBuilder row = new StringBuilder();
              row.append("\n");
              if (colCnt > 1)
                row.append(rs.getInt(1)).append(rs.getObject(2));
              else
                row.append(rs.getObject(1));

              //getLogger().info("Received " + row.toString());
            }
            rs.close();
            getLogger().info("Done for " + query);
          }
        } catch (SQLException e) {
          if (!ignorePKViolation || !e.getSQLState().equals("23505")) {
            fail("test failed because of SQLException=", e);
          }
        } finally {
          if(conn != null )
            try {
              conn.close();
            } catch (SQLException e) {
              getLogger().error("close failed because of SQLException=", e);
            }
          GemFireXDQueryObserverHolder.removeObserver(observer);
        }
      }
    });
  }
}

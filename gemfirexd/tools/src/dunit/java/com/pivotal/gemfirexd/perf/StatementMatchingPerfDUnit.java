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
package com.pivotal.gemfirexd.perf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

import com.pivotal.gemfirexd.ByteCompareTest;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.services.cache.CacheEntry;
import com.pivotal.gemfirexd.internal.impl.services.cache.ConcurrentCache;

/**
 * 
 * @author soubhikc
 *
 */
@SuppressWarnings("serial")
public final class StatementMatchingPerfDUnit extends DistributedSQLTestBase {
  private final static int numcolumns = 1;
  private final static int rowsperthrd = 1000;
  private final static int numthrd = 8;

  private String[][] psInsertValueSet;
  private String[][] stmtInsertSet;
  private String[][] stmtSelectSet;

  private final long[] psTiming = new long[numthrd];
  private final long[] stmtTiming = new long[numthrd];

  private boolean enableRuntimeStats = false; // Boolean.getBoolean("print-runtime-stats");

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (psInsertValueSet == null) {
      psInsertValueSet = new String[numthrd][rowsperthrd];
      stmtInsertSet = new String[numthrd][rowsperthrd];
      stmtSelectSet = new String[numthrd][rowsperthrd];
    }
    final String ins = new String(
        "insert into statement_optimization_checker values ('");
    // get p.k. strings.
    for (int t = 0; t < numthrd; t++) {
      for (int v = 0; v < rowsperthrd; v++) {
        String value = ByteCompareTest.getRandomAlphaNumericString(-1); //v%4);
        psInsertValueSet[t][v] = value;
        final StringBuilder sb = new StringBuilder(ins);
        sb.append(value).append("','");
        for (int i = numcolumns; i > 0; i--) {
          sb.append(value).append("','");
        }
        sb.append(value).append("')");
        stmtInsertSet[t][v] = sb.toString();
        stmtSelectSet[t][v] = "select * from statement_optimization_checker "
            + "where ID = '" + value + "'";
      }
    }
  }

  public StatementMatchingPerfDUnit(String name) {
    super(name);
  }

  public void testDummy() {
  }

  public void PERF_testStatementOptimizationUsingPuts() throws Exception {

    startVMs(1, 4);

    TestUtil.getLogger().info("creating table .... ");
    final StringBuilder sql = new StringBuilder("create table "
        + "statement_optimization_checker( ID varchar(100) primary key, ");

    for(int i = numcolumns; i > 0; i--) {
      sql.append("col").append(i);
      sql.append(" varchar(100),");
    }
    sql.append("colEnd varchar(100) ) partition by column (ID, colEnd) ");
    clientSQLExecute(1, sql.toString());

    profileInserts();
    
  }
  
  public void PERF_testStatementOptimizationUsingGets() throws Exception {

    startVMs(1, 4);

    TestUtil.getLogger().info("creating table .... ");
    final StringBuilder sql = new StringBuilder("create table "
        + "statement_optimization_checker( ID varchar(100) primary key, ");

    for(int i = numcolumns; i > 0; i--) {
      sql.append("col").append(i);
      sql.append(" varchar(100),");
    }
    sql.append("colEnd varchar(100) ) partition by column (ID, colEnd) ");
    clientSQLExecute(1, sql.toString());

    //temporarily disable runtime statistics
    boolean runTimeStatsState = enableRuntimeStats;
    enableRuntimeStats = false;
    
    //populate enough data
    for(int i = numthrd - 1; i >= 0; i--)
      insertDataUsingPrepStmt(i, true);
    
    enableRuntimeStats = runTimeStatsState;
    profileSelects();
  }
  
  private void profileInserts() throws SQLException, InterruptedException {
    
    final Statement stmt = TestUtil.getConnection().createStatement();
    
    //warmup
    TestUtil.getLogger().info("Warming up with preparedstatement inserts");
    for(int i = numthrd - 1; i >= 0; i--)
      insertDataUsingPrepStmt(i, false);
    stmt.execute("delete from statement_optimization_checker");
    
    TestUtil.getLogger().info("Warming up with statement inserts");
    for(int i = numthrd - 1; i > 0; i--)
      insertDataUsingStmt(i);
    stmt.execute("delete from statement_optimization_checker");
    TestUtil.getLogger().info("Warmup done");


    //Prep Stmt
    {
        Thread inserts[] = new Thread[numthrd];
        //create threads
        for(int inst = 0; inst < inserts.length; inst++) {
          inserts[inst] = createPreparedStatementInsertThreads(inst);
        }
        
        //START
        for(int inst = 0; inst < inserts.length; inst++) {
          inserts[inst].start();
        }
        
        //WAIT
        for(int inst = 0; inst < inserts.length; inst++) {
          if( inserts[inst].isAlive() ) {
            inserts[inst].join();
          }
        }
        TestUtil.getLogger().info("PreparedStatement activities done");
        
        ResultSet rs = stmt.executeQuery("select count(1) from statement_optimization_checker");
        assertTrue("one row is expected ", rs.next());
        
        TestUtil.getLogger().info("Got " + rs.getInt(1) + " number of rows ");
        assertTrue("expected " + (numthrd * rowsperthrd) + " rows ",
            rs.getInt(1) == (numthrd * rowsperthrd));

        rs.next();
        rs.close();
        stmt.execute("delete from statement_optimization_checker");
    }

    
    Thread.sleep(1000);

    TestUtil.getLogger().info("Resuming now with statements ...  ");
    //Stmts
    {
      Thread inserts[] = new Thread[numthrd];
      //create threads
      for(int inst = 0; inst < inserts.length; inst++) {
        inserts[inst] = createStatementInsertThreads(inst);
      }
      
      //START
      for(int inst = 0; inst < inserts.length; inst++) {
        inserts[inst].start();
      }
      
      //WAIT
      for(int inst = 0; inst < inserts.length; inst++) {
        if( inserts[inst].isAlive() ) {
          TestUtil.getLogger().info("About to wait for " + inserts[inst] + " to join.... ");
          inserts[inst].join();
        }
      }
      
      ResultSet rs = stmt.executeQuery("select count(1) from statement_optimization_checker");
      assertTrue("one row is expected ", rs.next());
      
      TestUtil.getLogger().info("Got " + rs.getInt(1) + " number of rows ");
      assertTrue("expected " + (numthrd * rowsperthrd) + " rows ",
          rs.getInt(1) == (numthrd * rowsperthrd));
      rs.next();
      rs.close();
    }
    
    
    long totalPSTiming = 0;
    for(long l : psTiming) {
      totalPSTiming += l;
    }
    
    long totalStmtTiming = 0;
    for(long l : stmtTiming) {
      totalStmtTiming += l;
    }

    TestUtil.getLogger().info(
        "INSERTs Avg PS Timing " + (totalPSTiming / numthrd) + " Avg Stmt timing "
            + (totalStmtTiming / numthrd) + " with Total Threads " + numthrd);    
  }
  
  private void profileSelects() throws SQLException, InterruptedException {
    
    //temporarily disable runtime statistics
    boolean runTimeStatsState = enableRuntimeStats;
    enableRuntimeStats = false;
    
    //warmup
    final Connection conn = TestUtil.getConnection();
    ConcurrentHashMap<Object, CacheEntry> cache = 
      ((ConcurrentCache) ((EmbedConnection)conn).getLanguageConnection().getLanguageConnectionFactory().getStatementCache()).getCache();
    TestUtil.getLogger().info("Statement Cache Size BEGIN : "  + cache.size());
    
    
    TestUtil.getLogger().info("Warming up with preparedstatement selects");
    final Statement stmt = conn.createStatement();
    for(int i = numthrd - 1; i >= 0; i--)
      selectDataUsingPrepStmt(i);

    enableRuntimeStats = runTimeStatsState;
    TestUtil.getLogger().info("Warming up with statement selects");
    for(int i = 1; i <= rowsperthrd; i++) {
      String str = stmtSelectSet[0][0];
      String value = psInsertValueSet[0][0];
      try {
          ResultSet rs = stmt.executeQuery(str);
          assertTrue("atleast 1 row is expected ", rs.next());
          assertTrue("Value expected " + value + " got " + rs.getString(1), rs.getString(1).equals(value));
          rs.close();
          if(i%1000 == 0) {
            TestUtil.getLogger().info("Selected " + i + " times for warm up");
          }
      }
      catch (SQLException e) {
        e.printStackTrace();
        TestUtil.getLogger().error("Error " + e);
        continue;
      }
    }

    TestUtil.getLogger().info("Warmup done");
    enableRuntimeStats = false;
    TestUtil.getLogger().info("Statement Cache Size After Warmup done : "  + cache.size());

    //Prep Stmt
    {
        Thread selects[] = new Thread[numthrd];
        //create threads
        for(int inst = 0; inst < selects.length; inst++) {
          selects[inst] = createPreparedStatementSelectThreads(inst);
        }
        
        //START
        for(int inst = 0; inst < selects.length; inst++) {
          selects[inst].start();
        }
        
        //WAIT
        for(int inst = 0; inst < selects.length; inst++) {
          if( selects[inst].isAlive() ) {
            selects[inst].join();
          }
        }
        TestUtil.getLogger().info("Select PreparedStatement activities done");
    }

    TestUtil.getLogger().info("Statement Cache Size After PreparedStmt done : "  + cache.size());
    Thread.sleep(1000);
    
    TestUtil.getLogger().info("sb: Waiting for 30 secs to attach profiler.... ");
    Thread.sleep(30000);

    TestUtil.getLogger().info("Select Statement activities begin");
    enableRuntimeStats = runTimeStatsState;
    //Stmts
    {
      Thread selects[] = new Thread[numthrd];
      //create threads
      for(int inst = 0; inst < selects.length; inst++) {
        selects[inst] = createStatementSelectThreads(inst);
      }
      
      //START
      for(int inst = 0; inst < selects.length; inst++) {
        selects[inst].start();
      }
      
      //WAIT
      for(int inst = 0; inst < selects.length; inst++) {
        if( selects[inst].isAlive() ) {
          selects[inst].join();
        }
      }
      TestUtil.getLogger().info("Select Statement activities done");
    }
    
    TestUtil.getLogger().info("Statement Cache Size After Statement done : "  + cache.size());
    
    long totalPSTiming = 0;
    for(long l : psTiming) {
      totalPSTiming += l;
    }
    
    long totalStmtTiming = 0;
    for(long l : stmtTiming) {
      totalStmtTiming += l;
    }

    TestUtil.getLogger().info(
        "SELECTs Avg PS Timing " + (totalPSTiming / numthrd) + " Avg Stmt timing "
            + (totalStmtTiming / numthrd) + " with Total Threads " + numthrd);    
  }
    
  
  private void insertDataUsingPrepStmt(final int tid, boolean useBatchMode) {
    try {
      final StringBuilder prepstmt = new StringBuilder("insert into "
          + "statement_optimization_checker values (?,");
      for(int i = numcolumns; i > 0; i--) {
        prepstmt.append("?,");
      }
      prepstmt.append("?)");
      
      final Connection conn = TestUtil.getConnection();
      final PreparedStatement pstmt = conn.prepareStatement("values SYSCS_UTIL.GET_RUNTIMESTATISTICS()");
      enableStatistics(conn);
      final PreparedStatement insps  = conn.prepareStatement(prepstmt.toString());
      
      long beginTime = System.currentTimeMillis();
      for(int i = 1; i <= rowsperthrd; i++) {
        String value = psInsertValueSet[tid][i-1];
        try {
            insps.setString(1, value);
            insps.setString(2, value);
            insps.setString(3, value);
            if(useBatchMode) {
              insps.addBatch();
              if(i%1000 == 0 || i%rowsperthrd == 0) {
                insps.executeBatch();
                insps.clearBatch();
                conn.commit();
              }
            }
            else {
              insps.executeUpdate();
            }
            
            if(i%1000 == 0 || i%rowsperthrd == 0)
              TestUtil.getLogger().info("Inserted uptil " +i);
            
            if (enableRuntimeStats) {
              String stat = getStats(pstmt);
              TestUtil.getLogger().info("ps_stat:" + prepstmt + "(" + value + ") ->>" + stat);
            }
        }
        catch (SQLException e) {
          continue;
        }
      }
      long timetaken = System.currentTimeMillis() - beginTime;
      TestUtil.getLogger().info(
          "PREPSTMT Thread " + Thread.currentThread() + " took " + timetaken
              + " ms ");
      psTiming[tid] = timetaken;
    }
    catch (SQLException e) {
      e.printStackTrace();
      TestUtil.getLogger().error("Error " + e);
      return;
    }
    finally {
      TestUtil.getLogger().info("Done " + Thread.currentThread().getName());
    }
  }
  
  private void insertDataUsingStmt(final int tid) {
    try {

      final Connection conn = TestUtil.getConnection();
      final PreparedStatement pstmt = conn.prepareStatement("values SYSCS_UTIL.GET_RUNTIMESTATISTICS()");
      final Statement stmt = conn.createStatement();
      
      enableStatistics(conn);
      long beginTime = System.currentTimeMillis();
      for(int i = 1; i <= rowsperthrd; i++) {
        String str = stmtInsertSet[tid][i-1];
        try {
          
            stmt.executeUpdate(str);

            if(i%1000 == 0) {
              TestUtil.getLogger().info("Inserted uptil " +i);
            }
            if (enableRuntimeStats) {
              String stat = getStats(pstmt);
              TestUtil.getLogger().info("stmt_stat:" + str + "->>" + stat);
            }
        }
        catch (SQLException e) {
          e.printStackTrace();
          TestUtil.getLogger().error("Error " + e);
          continue;
        }
      }
      long timetaken = System.currentTimeMillis() - beginTime;
      TestUtil.getLogger().info(
          "STMT Thread " + Thread.currentThread() + " took " + timetaken
              + " ms ");
      
      stmtTiming[tid] = timetaken;
    }
    catch (SQLException e) {
      e.printStackTrace();
      TestUtil.getLogger().error("Error " + e);
      return;
    }
    finally {
      TestUtil.getLogger().info("Done " + Thread.currentThread().getName());
    }
  }
  
  
  private Thread createPreparedStatementInsertThreads(final int tid) {
    
    Thread t = new Thread(new Runnable() {
      public void run() {
         insertDataUsingPrepStmt(tid, false);
      }
    }, "INSERT_PS_T"+tid);
    
    return t;
  }
  
  private Thread createStatementInsertThreads(final int tid) {
    
    Thread t = new Thread(new Runnable() {
      public void run() {
         insertDataUsingStmt(tid);
      }
    }, "INSERT_STMT_T"+tid);
    
    return t;
  }

  
  private void selectDataUsingPrepStmt(final int tid) {
    try {
      final String prepstmt = new String("select * from statement_optimization_checker where ID = ?");
      
      final Connection conn = TestUtil.getConnection();
      final PreparedStatement pstmt = conn.prepareStatement("values SYSCS_UTIL.GET_RUNTIMESTATISTICS()");
      enableStatistics(conn);
      final PreparedStatement insps  = conn.prepareStatement(prepstmt);
      
      long beginTime = System.currentTimeMillis();
      for(int i = 1; i <= rowsperthrd; i++) {
        String value = psInsertValueSet[tid][i-1];
        ResultSet rs = null;
        try {
            insps.setString(1, value);
            rs = insps.executeQuery();
            assertTrue("atleast 1 row is expected ", rs.next());
            assertEquals(value, rs.getString(1));
            rs.close();
            rs = null;
            if(i%1000 == 0) {
              TestUtil.getLogger().info("Selects uptil " +i);
            }
            if (enableRuntimeStats) {
              String stat = getStats(pstmt);
              TestUtil.getLogger().info(
                  "select_ps_stat:" + prepstmt + "->>" + stat);
            }
        }
        catch (SQLException e) {
          if(rs  != null) {
            rs.close();
          }
          e.printStackTrace();
          TestUtil.getLogger().error("Error " + e);
          continue;
        }
      }
      long timetaken = System.currentTimeMillis() - beginTime;
      TestUtil.getLogger().info(
          "PREPSTMT Thread " + Thread.currentThread() + " took " + timetaken
              + " ms ");
      psTiming[tid] = timetaken;
    }
    catch (Throwable e) {
      e.printStackTrace();
      TestUtil.getLogger().error("Error " + e);
      return;
    }
    finally {
      TestUtil.getLogger().info("Done " + Thread.currentThread().getName());
    }
  }
  
  private void selectDataUsingStmt(final int tid) {
    try {

      final Connection conn = TestUtil.getConnection();
      final PreparedStatement pstmt = conn.prepareStatement("values SYSCS_UTIL.GET_RUNTIMESTATISTICS()");
      final Statement stmt = conn.createStatement();
      
      enableStatistics(conn);
      long beginTime = System.currentTimeMillis();
      for(int i = 1; i <= rowsperthrd; i++) {
        String str = stmtSelectSet[tid][i-1];
        String value = psInsertValueSet[tid][i-1];
        try {
            ResultSet rs = stmt.executeQuery(str);
            assertTrue("atleast 1 row is expected ", rs.next());
            assertEquals(value, rs.getString(1));
            rs.close();
            if(i%1000 == 0) {
              TestUtil.getLogger().info("Selects uptil " +i);
            }
            if (enableRuntimeStats) {
              String stat = getStats(pstmt);
              TestUtil.getLogger().info("select_stmt_stat:" + str + "->>" + stat);
            }
        }
        catch (SQLException e) {
          e.printStackTrace();
          TestUtil.getLogger().error("Error " + e);
          continue;
        }
      }
      long timetaken = System.currentTimeMillis() - beginTime;
      TestUtil.getLogger().info(
          "STMT Thread " + Thread.currentThread() + " took " + timetaken
              + " ms ");
      
      stmtTiming[tid] = timetaken;
    }
    catch (Throwable e) {
      e.printStackTrace();
      TestUtil.getLogger().error("Error " + e);
      return;
    }
    finally {
      TestUtil.getLogger().info("Done " + Thread.currentThread().getName());
    }
  }
  
  private Thread createPreparedStatementSelectThreads(final int tid) {
    
    Thread t = new Thread(new Runnable() {
      public void run() {
        selectDataUsingPrepStmt(tid);
      }
    }, "SELECT_PS_T"+tid);
    
    return t;
  }
  
  private Thread createStatementSelectThreads(final int tid) {
    
    Thread t = new Thread(new Runnable() {
      public void run() {
        selectDataUsingStmt(tid);
      }
    }, "SELECT_STMT_T"+tid);
    
    return t;
  }
  
  private void enableStatistics(Connection conn) throws SQLException {
    if(!enableRuntimeStats) {
      return ;
    }
    conn.prepareCall("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)").execute();
    conn.prepareCall("call SYSCS_UTIL.SET_STATISTICS_TIMING(1)").execute();
  }
  
  private String getStats(PreparedStatement ps) throws SQLException {
    if(!enableRuntimeStats) {
      return "";
    }
    ResultSet rs = ps.executeQuery();
    rs.next();
    String rts = rs.getString(1);
    rs.close();
    return rts;
  }
}

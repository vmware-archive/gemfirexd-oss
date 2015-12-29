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
package sql.tpch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.pivotal.gemfirexd.Attribute;

import objects.query.QueryPrms;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.tpch.TPCHStats;
import sql.tpch.TPCHTest;
import util.TestException;
import util.TestHelper;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.ThinClientHelper;
import cacheperf.comparisons.gemfirexd.QueryPerfClient;
import cacheperf.comparisons.gemfirexd.QueryUtil;



public class TPCHTest extends QueryPerfClient {
  
  public static TPCHTest tpchTest;
  public TPCHStats tpchstats; // statistics
  public static boolean isClient = false;
  private static boolean reproduce49166OOME = false;
  protected static final String SCHEMA_NAME = "TPCHGFXD";
  public static boolean logDML = TestConfig.tab().booleanAt(sql.SQLPrms.logDML, false);
  protected static HydraThreadLocal localtpchstats = new HydraThreadLocal();
  
  private static String query1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, " +
                            "sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, " + 
                            "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, " + 
                            "avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, " + 
                            "avg(l_discount) as avg_disc,  count(*) as count_order from lineitem where " + 
                            "l_shipdate <= cast({fn timestampadd(SQL_TSI_DAY, -90, timestamp('1998-12-01 23:59:59'))} as DATE) " +
                            "group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";
  
  //private static String query1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, " +
  //                          "sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, " +
  //                          "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, " +
  //                          "avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, " +
  //                          "avg(l_discount) as avg_disc,  count(*) as count_order from temp_q1 " +
  //                          " group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";

  private static String query2 = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from   part , supplier, partsupp, nation, region " +
                            " where   p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS'   and " + 
                            " s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE'   and " + 
                            " ps_supplycost = ( select min(ps_supplycost) from  part, partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and " + 
                            " s_nationkey = n_nationkey and n_regionkey = r_regionkey  and r_name = 'EUROPE' ) order by s_acctbal desc, n_name, s_name, p_partkey";

  private static String query3 = "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from   customer, orders, lineitem where " + 
                            " c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey   and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' " + 
                            " group by   l_orderkey, o_orderdate, o_shippriority order by   revenue desc, o_orderdate";
  
  private static String query4 = "select o_orderpriority, count(*) as order_count from orders where " + 
                            " o_orderdate >= '1993-07-01'   and o_orderdate < cast({fn timestampadd(SQL_TSI_MONTH, 3, timestamp('1993-07-01 23:59:59'))} as DATE) and " + 
                            " exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate   ) group by   o_orderpriority order by   o_orderpriority";
  
  private static String query5 = "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from   customer, orders, lineitem, supplier, nation, region " + 
                            " where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey " +
                            " and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'ASIA'   and o_orderdate >= '1994-01-01' " + 
                            " and o_orderdate < cast({fn timestampadd(SQL_TSI_YEAR, 1, timestamp('1994-01-01 23:59:59'))} as DATE) group by   n_name order by   revenue desc";
  
 
  public static void HydraTask_initailizeClient() {
    isClient = true;
  }
  
  public static void HydraTask_initializeParameters() {

    if (tpchTest == null) {
      tpchTest = new TPCHTest();
    }
    tpchTest.initLocalVariables(-1);
    tpchTest.initLocalParameters();
  }
  
  /**
   *  TASK to register the tpc-h performance statistics object.
   */
  public static void openStatisticsTask() {
//    TPCHTest hTest = new TPCHTest();
    tpchTest.openStatistics();
  }

  private void openStatistics() {
    this.tpchstats = getTPCHStats();
    if (this.tpchstats == null) {
      this.tpchstats = TPCHStats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
    setTPCHStats(this.tpchstats);
  }
    
  protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.tpchstats != null) {
      RemoteTestModule.closeClockSkewStatistics();
      this.tpchstats.close();
    }
  }
  
  /**
   * Gets the per-thread TPCHStats wrapper instance.
   */
  protected TPCHStats getTPCHStats() {
    TPCHStats tpchstats = (TPCHStats)localtpchstats.get();
    return tpchstats;
  }
  
  /**
   * Sets the per-thread TPCHStats wrapper instance.
   */
  protected void setTPCHStats(TPCHStats tpchstats) {
    localtpchstats.set(tpchstats);
  }

  
  
  protected void initLocalParameters() {
    super.initLocalParameters();
    //TODO to add tpch parameters
    Log.getLogWriter().info("in HydraTask_initializeParameters, this.connection = " + this.connection);
  }
  
  public static void HydraTask_runSQLScript() throws SQLException {
    //tpchTest.initialize(-1);
    if (tpchTest == null) {
      tpchTest = new TPCHTest();
      tpchTest.queryAPI = QueryPrms.getAPI();
    }
    tpchTest.runSQLScript(true);
  }
  public static void HydraTask_runSQLScriptContinueOnError()throws SQLException {
    //tpchTest.initialize(-1);
    tpchTest.runSQLScript(false);
  }
  
  protected void runSQLScript(boolean failOnError) throws SQLException {
    Connection conn = null;

    switch (this.queryAPI) {
    //TODO, get connection to each db
      case QueryPrms.MYSQL:
      case QueryPrms.MYSQLC:
      case QueryPrms.ORACLE:
      case QueryPrms.GPDB:       
        break;
      case QueryPrms.GFXD:
        conn = getSqlfNonTxConnection();
        break;
      default:
        unsupported();
    }
    
    if (conn == null) throw new TestException ("Test issue, connection are not set for other db");
    runSQLScript(conn, failOnError);
  }
  
  protected void runSQLScript(Connection conn, boolean failOnError) throws SQLException {
    String sqlFilePath = SQLPrms.getSqlFilePath();
    //Log.getLogWriter().info("in runSQLScrip, this.connection = " + this.connection);
    //Log.getLogWriter().info("in runSQLScrip, this.queryAPI = " + this.queryAPI);
    
    Log.getLogWriter().info("running sql script " + sqlFilePath + " on gfxd");
    SQLHelper.runSQLScript(conn, sqlFilePath, failOnError);
  }
  
  public static void HydraTask_createBucketsTask()
  throws InterruptedException, SQLException {
    //tpchTest.initialize(-1);
    tpchTest.createBuckets();
  }
  
  protected void createBuckets() throws SQLException{
    if (this.queryAPI == QueryPrms.GFXD) {
      assignBucketsForTables();
    }
  }
  
  private void assignBucketsForTables() throws SQLException {
    Connection conn = getSqlfNonTxConnection();
    try {
      String sql = "call SYS.CREATE_ALL_BUCKETS( ? )";
      CallableStatement cs =
              conn.prepareCall(sql);
      List<String> tableNames = getPartitionedTables();
      for (String tableName : tableNames) {
        Log.getLogWriter().info("Creating buckets for table " + tableName + ": " + sql );
        cs.setString(1, tableName);
        cs.execute();
        Log.getLogWriter().info("Created buckets for table " + tableName + ": " + sql);
      }
      cs.close();
    } finally {
      conn.close();
    }
  }

  /**
   *  TASK to register the tpc-e performance statistics object.
   */
   /*public static void openStatisticsTask() {
    TPCHTest eTest = new TPCHTest();
    eTest.openStatistics();
  }

 private void openStatistics() {
    this.tpchstats = getTPCEStats();
    if (this.tpcestats == null) {
      this.tpcestats = TPCEStats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
    setTPCEStats(this.tpcestats);
  }*/
  
  /** 
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    TPCHTest hTest = new TPCHTest();
    hTest.initHydraThreadLocals();
    hTest.closeStatistics();
    hTest.updateHydraThreadLocals();
  }
  
  /*protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.tpcestats != null) {
      RemoteTestModule.closeClockSkewStatistics();
      this.tpcestats.close();
    }
  }*/
  
  /*
   * need to use new connection when run against gfxd for functional testing
   * may reuse 
   */
  protected Connection getConnection(int db) throws SQLException {
    Connection conn = null;
    switch (db) {
    case QueryPrms.GFXD:
      if (isClient) conn = QueryUtil.gfxdClientSetup(this);
      else conn = QueryUtil.gfxdEmbeddedSetup(this);
      break;
    default:
      throw new TestException("Test issue, other db should not use this method");
   
    }   
    
    return conn;
  }
  
  protected Connection getSqlfNonTxConnection() throws SQLException {
    Connection conn;
    if (isClient) {
      conn = QueryUtil.gfxdClientSetup(this);
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    } else {
      conn = QueryUtil.gfxdEmbeddedSetup(this);
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    }
    Log.getLogWriter().info("Using transaction isolation level: none");
    
    return conn;
  }
  
  public static void HydraTask_runImportTable() throws SQLException {
    if (tpchTest == null) {
      tpchTest = new TPCHTest();
    }
    tpchTest.runImportTable();
  }
  
  protected void runImportTable() throws SQLException {
    boolean skipConstraints = TestConfig.tab().booleanAt(SQLPrms.skipConstraints, true);
    if (skipConstraints) runImportTableSkipConstraints();
    else runImportTableWithConstraints();     
  }
  
  protected void runImportTableWithConstraints() throws SQLException {
    long start = System.currentTimeMillis();
    Log.getLogWriter().info("import table starts from " + start);
    Connection conn = getSqlfNonTxConnection();
    importTable(conn);
    
    long end = System.currentTimeMillis();
    Log.getLogWriter().info("import table finishes at " + end);
    
    long time = end - start;
  
    Log.getLogWriter().info("import_table takes " + time/1000 + " seconds");
    //TPCEBB.getBB().getSharedCounters().add(TPCEBB.importTableTime, time);
  }
  
  public static void HydraTask_runImportTableSkipConstraints() throws SQLException {
    tpchTest.runImportTableSkipConstraints();
  }
  
  protected void runImportTableSkipConstraints() throws SQLException {
    long start = System.currentTimeMillis();
    Log.getLogWriter().info("import table starts from " + start);
    
    Connection conn = getSkipConstraintsConnectionWithSchema();
    importTable(conn);
    
    long end = System.currentTimeMillis();
    Log.getLogWriter().info("import table finishes at " + end);
    
    long time = end - start;
    
    Log.getLogWriter().info("import_table takes " + time/1000 + " seconds");
    //TPCEBB.getBB().getSharedCounters().add(TPCEBB.importTableTime, time);
  }
  
  protected void importTable(Connection conn) throws SQLException { 
    StringBuilder sb = getSqlScript();  

    // here is our splitter ! We use ";" as a delimiter for each request  
    // then we are sure to have well formed statements  
    String[] inst = sb.toString().split(";");  

    Statement st = conn.createStatement();
    /*ResultSet set = st.executeQuery("select count(*) from PART");
    while (set.next()) {
      boolean exception = false;
      Exception ex = null;

      try {
        Log.getLogWriter().info("executing PART =" + set.getString(1));
      } catch (Exception e) {
          exception = true;
          ex =e;
          Log.getLogWriter().info("Encountered exception with 1" +e.toString());
      }
      if (exception) {

        throw new TestException("Exception encountered" + ex);
      }
    }*/

    for(int i = 0; i<inst.length; i++) {  
      // we ensure that there is no spaces before or after the request string  
      // in order to not execute empty statements 
      try {
        if(!inst[i].trim().equals("") && !inst[i].contains("exit")) {  
          log().info(">>"+inst[i]); 
          long start = System.currentTimeMillis();
          st.executeUpdate(inst[i]);  
          long end = System.currentTimeMillis();
          Log.getLogWriter().info("executing " + inst[i] + " takes " + 
             ( (end-start)/1000.0 < 30? (end-start)/1000.0 + " seconds."
                 :  (end-start)/60000.0 + " minutes."));
        }  
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
    } 
    /*set = st.executeQuery("select count(*) from PART");
    while (set.next()) {
      boolean exception = false;
      Exception ex = null;

      try {
        Log.getLogWriter().info("executing PART after=" + set.getString(1));
      } catch (Exception e) {
          exception = true;
          ex =e;
          Log.getLogWriter().info("Encountered exception with 1" +e.toString());
      }
      if (exception) {

        throw new TestException("Exception encountered" + ex);
      }
    }*/

  }
  
  public StringBuilder getSqlScript() {
    String jtests = System.getProperty( "JTESTS" );
    String sqlFilePath = SQLPrms.getSqlFilePath();
    String s = new String();  
    StringBuilder sb = new StringBuilder();  

    try  {  
      FileReader fr = new FileReader(new File(jtests+"/"+sqlFilePath));  
        // be sure to not have line starting with "--" or "/*" or any other non aplhabetical character  

      BufferedReader br = new BufferedReader(fr);  

      while((s = br.readLine()) != null)  {  
        //ignore comments starts with "--"
        int indexOfCommentSign = s.indexOf("--");  
        if(indexOfCommentSign != -1)  
        {  
            if(s.startsWith("--"))  
            {  
                s = new String("");  
            }  
            else   
                s = new String(s.substring(0, indexOfCommentSign-1));  
        } 
             
        //add to the statement
        sb.append(s);  
      }  
      br.close(); 
      return sb;
    } catch (Exception e) {
      throw new TestException("could not get sql script " + TestHelper.getStackTrace(e));
    }
  }
  
  
  protected Connection getSkipConstraintsConnectionWithSchema() throws SQLException {
    Connection conn;
    if (isClient) {
      Properties p = ThinClientHelper.getConnectionProperties();
      p.setProperty(Attribute.SKIP_CONSTRAINT_CHECKS, "true");
      Log.getLogWriter().info("setting " + Attribute.SKIP_CONSTRAINT_CHECKS + " to true" );
      conn = QueryUtil.gfxdClientSetup(this, p);
    } else {
      Properties p = new Properties();
      p.setProperty(Attribute.SKIP_CONSTRAINT_CHECKS, "true");
      Log.getLogWriter().info("setting " + Attribute.SKIP_CONSTRAINT_CHECKS + " to true" );
      conn = QueryUtil.gfxdEmbeddedSetup(this, p);
    }
    
    if (!reproduce49166OOME) {
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      Log.getLogWriter().info("Using transaction isolation level: none");
    }
    
    return getConnectionWithSchema(conn);
  }
  
  
  protected Connection getConnectionWithSchema(Connection conn) {
    String sql = "set schema " + SCHEMA_NAME;
    try {
      conn.createStatement().execute(sql);
      if (logDML) Log.getLogWriter().info(sql);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return conn;
  }
  
  protected Connection getConnectionWithSchema() {
    Connection conn = super.getConnection();
    return getConnectionWithSchema(conn);
  }
  
  /*Running TPCH Querys*/
  public static void HydraTask_runTPCHQueries() throws SQLException {
    tpchTest.initialize(QUERIES);
    tpchTest.runQueries();
  }
  
  protected void runQueries() throws SQLException {
    do {
      executeTaskTerminator(); // does not commit
      executeWarmupTerminator(); // does not commit
      executeQueries();      
      ++this.batchCount;
      ++this.count;
      ++this.keyCount; // not really needed
    } while (!executeBatchTerminator()); // does not commit

  }
  
  
  
  protected void executeQueries() throws SQLException {
   
    List<String> queryList = Arrays.asList(query1, query2, query3, query4, query5);
    
    int queryNo = 0;
    for (int i = 0; i < queryList.size(); i++) { //for ( String query : queryList ) {
      
      String query = queryList.get(i);  
      queryNo++;
      Log.getLogWriter().info("Query" + String.valueOf(queryNo) + " String:  " + query);
      
      ////long start = System.currentTimeMillis();
      ////Log.getLogWriter().info("Query" + String.valueOf(queryNo) + " execution starts from " + start);
      
      Connection conn = getSkipConstraintsConnectionWithSchema();
      executeQuery(conn, query, queryNo);
      
      ////long end = System.currentTimeMillis();
      ////tpchstats.incQuery(queryNo, 1);
      ////tpchstats.incQueryExecutionTime(queryNo, end - start);
      
      ////Log.getLogWriter().info("Query" + String.valueOf(queryNo) + " execution finishes at " + end);
      
      ////long time = end - start;
      ////Log.getLogWriter().info("Query" + String.valueOf(queryNo) + " execution time " + time/1000 + " seconds");
      
    }
    //TPCEBB.getBB().getSharedCounters().add(TPCEBB.importTableTime, time);
  }
  
  protected void executeQuery(Connection conn, String query, int queryNo) throws SQLException { 
    
    //Statement st = conn.createStatement();
    //ResultSet set = st.executeQuery(query);
    
    boolean exception = false;
    Exception ex = null;
    
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      
      long start = System.currentTimeMillis();
      
      ResultSet set = stmt.executeQuery(); // Execute queries
      
      long end = System.currentTimeMillis();
      tpchstats.incQuery(queryNo, 1);
      tpchstats.incQueryExecutionTime(queryNo, end - start);
      
      Log.getLogWriter().info("Query" + String.valueOf(queryNo) + " execution starts from " + start);
      Log.getLogWriter().info("Query" + String.valueOf(queryNo) + " execution finishes at " + end);
      
      long time = end - start;
      Log.getLogWriter().info("Query" + String.valueOf(queryNo) + " execution time " + time/1000 + " seconds");
      
      while (set.next()) {
        Log.getLogWriter().info("executing query" + String.valueOf(queryNo) + " = " + set.getString(1));
      }//end while
    } catch (Exception e) {
      exception = true;
      ex = e;
      Log.getLogWriter().info("Encountered exception while running query"+ String.valueOf(queryNo) + " :" + e.toString());
    }
    if (exception) {
      throw new TestException("Exception encountered" + ex);
    }
    
    /*PreparedStatement stmt = conn.prepareStatement(query);
    ResultSet set = stmt.executeQuery();
    
    while (set.next()) {
      boolean exception = false;
      Exception ex = null;
      
      try {
        Log.getLogWriter().info("executing query" + String.valueOf(queryNo) + " = " + set.getString(1));
      } catch (Exception e) {
        exception = true;
        ex = e;
        Log.getLogWriter().info("Encountered exception while running query"+ String.valueOf(queryNo) + " :" + e.toString());
      }//catch
      if (exception) {
        throw new TestException("Exception encountered" + ex);
      }
    }//while*/
    
  }
  
  
  
  
}


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
package sql.tpce;

//import hydra.GsRandom;
import hydra.gemfirexd.HDFSStoreDescription;
import hydra.gemfirexd.HDFSStoreHelper;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.gemfirexd.ThinClientHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.cache.query.Struct;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import objects.query.QueryPrms;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.hdfs.HDFSSqlTest;
import sql.sqlutil.ResultSetHelper;
import sql.tpce.tpcedef.*;
import sql.tpce.tpcedef.generator.CE;
import sql.tpce.tpcedef.generator.MEE;
import sql.tpce.tpcedef.input.MarketFeedTxnInput;
import sql.tpce.tpcedef.input.TradeOrderTxnInput;
import sql.tpce.tpcedef.input.TradeResultTxnInput;
import sql.tpce.tpcedef.output.MarketFeedTxnOutput;
import sql.tpce.tpcedef.output.TradeOrderTxnOutput;
import sql.tpce.tpcedef.output.TradeResultTxnOutput;
import sql.tpce.tpcetxn.TPCEMarketFeed;
import sql.tpce.tpcetxn.TPCETradeOrder;
import sql.tpce.tpcetxn.TPCETradeResult;
import util.TestException;
import util.TestHelper;
import cacheperf.comparisons.gemfirexd.QueryPerfClient;
import cacheperf.comparisons.gemfirexd.QueryPerfException;
import cacheperf.comparisons.gemfirexd.QueryUtil;



public class TPCETest extends QueryPerfClient {
  public static TPCETest tpceTest;
  public static boolean isClient = false;
  public static boolean isFunctionalTest = TestConfig.tab().booleanAt(sql.SQLPrms.isFunctionalTest, false);
  public static boolean tradeToMarketWithDefaultId = TestConfig.tab().booleanAt(sql.SQLPrms.tradeToMarketWithDefaultId, false);
  public static boolean logDML = TestConfig.tab().booleanAt(sql.SQLPrms.logDML, false);
  //public static GsRandom rand = TestConfig.getInstance().getParameters().getRandGen();
  public static final Random rand = new Random(SQLPrms.getRandSeed());
  
  public static final int TRADE_ORDER = 1;
  public static final int TRADE_RESULT = 2;
  public static final int MARKET_FEED = 3;

  //trim interval
  protected static final int TPCETXN = 18800000;
  protected static final int TRADEORDER = 1880001;
  protected static final int TRADERESULT = 1880002;
  
  protected static final String TPCETXN_NAME    = "tpcetxn";
  protected static final String TRADEORDER_NAME    = "tradeorder";
  protected static final String TRADERESULT_NAME    = "traderesult";
  
  protected static final String SCHEMA_NAME = "TPCEGFXD";
  protected static boolean reproduce48557 = true;
  
  protected static int initSellQty = 50;
  protected static int initBuyQty = 2598;
  protected static int acct_id = 9936;
  protected static String symbol = "ADPT";
  
  public static final String type_limit_buy = "TLB";
  public static final String type_limit_sell = "TLS";
  public static final String type_market_buy = "TMB";
  public static final String type_market_sell = "TMS";
  public static final String type_stop_loss = "TSL";
  public static final String status_submitted = "SBMT";
  public static final String status_completed = "CMPT";
  public static final String[] trade_types = {type_limit_buy, type_limit_sell, 
    type_market_buy, type_market_sell, type_stop_loss};
  
  private static int totalSellQtyInInit;
  private static final String CONFLICTEXCEPTION = "X0Z02";
  private static final String OFFHEAPCLAUSE = " offheap ";
  
  private static boolean isOffHeapTest = false;
  private static boolean isHDFSTest = false;
  
  private static String hdfsStoreName = "tpcehdfsStore";
  private static HydraThreadLocal queryHDFSConn = new HydraThreadLocal();
  
  private static boolean reproduce49166OOME = false;
  private static boolean isTicket49452Fixed = false;
  public static boolean useSyncCommit = false;
  
  
  //will be used in test for testing hdfs evict incoming combined with fetch offset conditions
  //but this needs to be synchronized for accessing to use the fect next
  //avoid this synchronization by not using default id and handled in test itself 
  //to be used in the functional testing instead of perf testing
  private static String createTradeToMarketTable = "CREATE TABLE trade_market " +
  		"(tm_id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY, " +
  		"tm_s_symb CHAR(15) NOT NULL, " +
  		"tm_t_qty INTEGER NOT NULL CHECK (tm_t_qty > 0), " +
  		"tm_t_bid_price DECIMAL(8,2) NOT NULL CHECK (tm_t_bid_price > 0), " +
  		"tm_tt_id CHAR(3) NOT NULL, " +
  		"tm_t_id BIGINT NOT NULL, " +
  		"tm_status SMALLINT NOT NULL CHECK (tm_status in (0, 1)), " +
  		"PRIMARY KEY (tm_id)) " +
  		"PARTITION BY PRIMARY KEY SERVER GROUPS(MarketGroup)";
  
  //test will handle which rows to be processed by which thread -- so that 
  //no threads will process the same trade send to market
  //tradeSentToMarket
  //tradeProcessedByMarket from TPCEBB will track the trades being processed etc.
  //tm_status 0 not processed, 1 processed
  private static String createTradeToMarketTableWithoutDefaultId = "CREATE TABLE trade_market " +
      "(tm_id BIGINT NOT NULL, " +
      "tm_s_symb CHAR(15) NOT NULL, " +
      "tm_t_qty INTEGER NOT NULL CHECK (tm_t_qty > 0), " +
      "tm_t_bid_price DECIMAL(8,2) NOT NULL CHECK (tm_t_bid_price > 0), " +
      "tm_tt_id CHAR(3) NOT NULL, " +
      "tm_t_id BIGINT NOT NULL, " +
      "tm_status SMALLINT NOT NULL CHECK (tm_status in (0, 1)), " +
      "PRIMARY KEY (tm_id)) " +
      "PARTITION BY PRIMARY KEY SERVER GROUPS(MarketGroup)";
  
  protected static HydraThreadLocal localtpcestats = new HydraThreadLocal();
  
  public TPCEStats tpcestats; // statistics
  
  public static void HydraTask_initailizeClient() {
    isClient = true;
  }
  

  
  public static void HydraTask_initializeParameters() {

    if (tpceTest == null) {
      tpceTest = new TPCETest();
    }
    tpceTest.initLocalVariables(-1);
    tpceTest.initLocalParameters();
    
  }
  
  protected void initLocalParameters() {
    super.initLocalParameters();
    //TODO to add tpce parameters
    Log.getLogWriter().info("in HydraTask_initializeParameters, this.connection = " + this.connection);
  }
  
  
  /**
   *  TASK to register the tpc-e performance statistics object.
   */
  public static void openStatisticsTask() {
    TPCETest eTest = new TPCETest();
    eTest.openStatistics();
  }

  private void openStatistics() {
    this.tpcestats = getTPCEStats();
    if (this.tpcestats == null) {
      this.tpcestats = TPCEStats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
    setTPCEStats(this.tpcestats);
  }
  
  /** 
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    TPCETest eTest = new TPCETest();
    eTest.initHydraThreadLocals();
    eTest.closeStatistics();
    eTest.updateHydraThreadLocals();
  }
  
  protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.tpcestats != null) {
      RemoteTestModule.closeClockSkewStatistics();
      this.tpcestats.close();
    }
  }
  
  /**
   * Gets the per-thread TPCEStats wrapper instance.
   */
  protected TPCEStats getTPCEStats() {
    TPCEStats tpcestats = (TPCEStats)localtpcestats.get();
    return tpcestats;
  }
  
  /**
   * Sets the per-thread TPCEStats wrapper instance.
   */
  protected void setTPCEStats(TPCEStats tpcestats) {
    localtpcestats.set(tpcestats);
  }
  
  public List<String> getPartitionedTables() throws SQLException {
    List<String> tableNames = new ArrayList<String>();
    final Connection conn = getGfxdNonTxConnection();
    try {
      PreparedStatement ps = conn.prepareStatement("SELECT tableschemaname, " +
      		"tablename, datapolicy FROM sys.systables WHERE tabletype = 'T' " +
      		"and tableschemaname != '" + GfxdConstants.PLAN_SCHEMA + "'");
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String schema = rs.getString("tableschemaname");
        String tablename = rs.getString("tablename");
        String datapolicy = rs.getString("datapolicy");
        if (datapolicy.contains("PARTITION")) {
          tableNames.add(schema + "." + tablename);
        }
      }
      rs.close();
      rs = null;
      ps.close();
    } finally {
      closeTmpConnection(conn);
    }
    return tableNames;
  }
  
  public static void HydraTask_createBucketsTask()
  throws InterruptedException, SQLException {
    //tpceTest.initialize(-1);
    tpceTest.createBuckets();
  }

  protected void createBuckets() throws SQLException{
    if (this.queryAPI == QueryPrms.GFXD) {
      assignBucketsForTables();
    }
  }


  private void assignBucketsForTables() throws SQLException {
    Connection conn = getGfxdNonTxConnection();
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
  
  protected Connection getGfxdNonTxConnection() throws SQLException {
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
  
  protected Connection getQueryHDFSConnectionWithSchema() throws SQLException {
    Connection conn;
    if (isClient) {
      Properties p = ThinClientHelper.getConnectionProperties();
      p.setProperty(Attribute.QUERY_HDFS, "true");
      Log.getLogWriter().info("setting " + Attribute.QUERY_HDFS + " to true" );
      conn = QueryUtil.gfxdClientSetup(this, p);
    } else {
      Properties p = new Properties();
      p.setProperty(Attribute.QUERY_HDFS, "true");
      Log.getLogWriter().info("setting " + Attribute.QUERY_HDFS + " to true" );
      conn = QueryUtil.gfxdEmbeddedSetup(this, p);
    }
    
    return getConnectionWithSchema(conn);
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
  
  public static void HydraTask_runSQLScript() throws SQLException {
    //tpceTest.initialize(-1);
    tpceTest.runSQLScript(true);
  }
  public static void HydraTask_runSQLScriptContinueOnError()throws SQLException {
    //tpceTest.initialize(-1);
    tpceTest.runSQLScript(false);
  }
  
  public static void HydraTask_runImportTable() throws SQLException {
    tpceTest.runImportTable();
  }
  
  protected void runImportTable() throws SQLException {
    boolean skipConstraints = TestConfig.tab().booleanAt(SQLPrms.skipConstraints, true);
    if (skipConstraints) runImportTableSkipConstraints();
    else runImportTableWithConstraints();
        
  }
  
  protected void runImportTableWithConstraints() throws SQLException {
    long start = System.currentTimeMillis();
    Log.getLogWriter().info("import table starts from " + start);
    Connection conn = getGfxdNonTxConnection();
    importTable(conn);
    
    long end = System.currentTimeMillis();
    Log.getLogWriter().info("import table finishes at " + end);
    
    long time = end - start;
    
    long seconds = time/1000;
    long minutes = seconds/60;
    Log.getLogWriter().info("import_table takes " + 
        (minutes >0 ? minutes + " minutes" : seconds + " seconds" ));
    TPCEBB.getBB().getSharedCounters().add(TPCEBB.importTableTime, time);
  }
  
  public static void HydraTask_runImportTableSkipConstraints() throws SQLException {
    tpceTest.runImportTableSkipConstraints();
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
    TPCEBB.getBB().getSharedCounters().add(TPCEBB.importTableTime, time);
  }
  
  protected void importTable(Connection conn) throws SQLException { 
    StringBuilder sb = getSqlScript();  

    // here is our splitter ! We use ";" as a delimiter for each request  
    // then we are sure to have well formed statements  
    String[] inst = sb.toString().split(";");  

    Statement st = conn.createStatement();  

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
  
  public static void HydraTask_createOffheapTables() throws SQLException {
    tpceTest.createOffheapTables();
  }
  
  protected void createOffheapTables() throws SQLException { 
    isOffHeapTest = true;
    createTables();
  }
  
  protected void createTables() throws SQLException { 
    if (isHDFSTest) {
      HDFSStoreDescription hdfsStoreDesc = HDFSStoreHelper.getHDFSStoreDescription(GfxdConfigPrms.getHDFSStoreConfig());
      hdfsStoreName = hdfsStoreDesc.getName();
    }
    
    StringBuilder sb = getSqlScript();  

    // here is our splitter ! We use ";" as a delimiter for each request  
    // then we are sure to have well formed statements  
    String[] inst = sb.toString().split(";");  

    Connection c = getGfxdNonTxConnection();  
    Statement st = c.createStatement();  

    for(int i = 0; i<inst.length; i++) {  
      // we ensure that there is no spaces before or after the request string  
      // in order to not execute empty statements 
      try {
        if(!inst[i].trim().equals("") && !inst[i].contains("exit")) {  
          if (isOffHeapTest) {
            if (inst[i].contains("CREATE TABLE")) inst[i]+=OFFHEAPCLAUSE;
          }

          if (isHDFSTest && isTicket49452Fixed) {
            if (inst[i].contains("CREATE TABLE trade ")) inst[i]+=getTradeHDFSClause();
            //if (inst[i].contains("CREATE TABLE settlement ")) inst[i]+=getSettlementHDFSClause();
            if (inst[i].contains("CREATE TABLE trade_history ")) inst[i]+=getTradeHistoryHDFSClause();
            if (inst[i].contains("CREATE TABLE holding_history ")) inst[i]+=getHoldingHistoryHDFSClause();
            if (inst[i].contains("CREATE TABLE cash_transaction ")) inst[i]+=getCashTransactionHDFSClause(); 
            //trade_request rows will be deleted, so do not evict to hdfs
            
          }
  
          log().info(">>"+inst[i]); 
          st.executeUpdate(inst[i]);                 
        }  
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
    }  

  }  
  
  private String getTradeHDFSClause() {
    /*work around #49001 -- will turn on casade in certain tests as it 
     * will affect a few txns which query the child tables of the trade 
     * like trade history, holding history etc
    return " EVICTION BY CRITERIA ( t_st_id = '" + status_completed
      + "') CASCADE EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";
      */
    return " EVICTION BY CRITERIA ( t_st_id = '" + status_completed
    + "') EVICTION FREQUENCY 30 SECONDS  HDFSSTORE (" + hdfsStoreName + ")";
  }
  
  private String getHoldingHistoryHDFSClause() {
    /*
    return " EVICTION BY CRITERIA ( hh_after_qty = 0 " +
    		") EVICTION FREQUENCY 10 SECONDS HDFSSTORE (" + hdfsStoreName + ")";
    */
    //TODO this impacts read in trade look up txn (yet to be implemented) if all rows are evicted
    //we may need to define the start trade date here and avoid hitting hdfs too much
    //once trade look up txn is implemented
    return " EVICTION BY CRITERIA ( hh_t_id > 0 or hh_h_t_id > 0) " +
        "EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";
  }
  
  private String getTradeHistoryHDFSClause() {
    /* cascade not supported yet see #49001, will bring up whether child needs to be 
     * set evict criteria with hdfs store as well, what if not set, will the  
     * child rows destroyed without hdfs backup?
    return " EVICTION BY CRITERIA ( th_st_id = 'NOTEXIST') EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";
    */
    /* subquery is not supported yet in eviction by criteria -- #49067
    return " EVICTION BY CRITERIA (th_t_id in (select t_id from trade where t_st_id = '" + status_completed
    + "' )) " + "EVICTION FREQUENCY 10 SECONDS HDFSSTORE (" + hdfsStoreName + ")";
    */
    //TODO this impacts read in trade look up txn (yet to be implemented) if all rows are evicted
    //we may need to define the start trade date here and avoid hitting hdfs too much
    //once trade look up txn is implemented
    
    return " EVICTION BY CRITERIA ( th_t_id > 0 ) " +
    "EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";

  }
  
  private String getCashTransactionHDFSClause() {
    
    /* cascade not supported yet
    return " EVICTION BY CRITERIA ( th_st_id = 'NOTEXIST') EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";
    */
    /*
    return " EVICTION BY CRITERIA ( DATE(ct_dts) < CURRENT_DATE - 7) EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";
    */
    /* misconfigured setting needs to be run after LAST_MODIFIED_DURATION is supported
    return " EVICTION BY CRITERIA (LAST_MODIFIED_DURATION > 300000) EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";
    */ 
    /*
    return " EVICTION BY CRITERIA (LAST_MODIFIED_DURATION > 300000) EVICTION FREQUENCY 30 SECONDS HDFSSTORE (" + hdfsStoreName + ")";
    */  
    
    return " EVICTION BY CRITERIA (DATE(ct_dts) < CURRENT_DATE OR HOUR(ct_dts) < HOUR(CURRENT_TIMESTAMP) - 1 ) " +
    		"EVICTION FREQUENCY 30 MINUTES HDFSSTORE (" + hdfsStoreName + ")";
  }
  
  public static void HydraTask_setHDFSFlag() throws SQLException {
    isHDFSTest = true;
    tpceTest.setHDFSConnection();
  }
  
  protected void setHDFSConnection() throws SQLException {
    queryHDFSConn.set(getQueryHDFSConnectionWithSchema());
    Log.getLogWriter().info("set the threadlocal for connection with query-HDFS to true");
  }
  
  
  
  public static void HydraTask_createHDFSTables() throws SQLException {
    isHDFSTest = true;
    tpceTest.createHDFSTables();
  }
  
  protected void createHDFSTables() throws SQLException {
    createTables();
  }

  public static void HydraTask_createHdfsStore() throws SQLException {    
    tpceTest.createHdfsStore();
  }
  
  protected void createHdfsStore() throws SQLException {
    Connection conn = getGfxdNonTxConnection();
    new HDFSSqlTest().createHdfsStore(conn);
    conn.close();
    
  }
  
  public static void HydraTask_createDiskStore() throws SQLException  {
    tpceTest.createDiskStores();
  }
  
  protected void createDiskStores() throws SQLException {
    Connection conn = getGfxdNonTxConnection();
    createDiskStores(conn);
    conn.close();
  }
  
  protected void createDiskStores(Connection conn) {
    String maxlogsize = " maxlogsize 2"; //work around run out of disk space on Windows run
    String createDiskStore = "create diskstore tpceHdfsDiskStore 'tpceHdfsDiskStore'" + maxlogsize ;
    try {
      conn.createStatement().execute(createDiskStore);
      Log.getLogWriter().info("successfuly exectued " + createDiskStore);

    }catch(SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }
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
        conn = getGfxdNonTxConnection();
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
  
  public static void HydraTask_runTradeOrderInInitForSell() {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TRADEORDER);
    tTest.runTradeOrderInInit(true);
  }
  
  public static void HydraTask_runTradeOrderInInitForBuy() {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TRADEORDER);
    tTest.runTradeOrderInInit(false);
  }
  
  protected void runTradeOrderInInit(Boolean isSell) {
    Connection conn = getConnectionWithSchema();
   
    try {
      runTradeOrderInInit(conn, isSell);
    } catch (SQLException se) {
      //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void runTradeOrderInInit(Connection conn, boolean isSell) throws SQLException {
    TradeOrderTxnInput toInput = (TradeOrderTxnInput) getTradeOrderTxnInputInInit(isSell);
       
    TPCETradeOrder to = new TPCETradeOrder();
    TradeOrderTxnOutput toOutput = (TradeOrderTxnOutput) to.runTxn(toInput, conn);
    if (toInput.getRollItBack() != 1 && (toInput.getTradeTypeId().equals(type_market_sell)
        ||toInput.getTradeTypeId().equals(type_market_buy))) {
      //only committed trade will be processed 
      TPCEBB.getBB().addTradeId(toOutput.getTradeId());      
      TPCEBB.getBB().getSharedCounters().increment(TPCEBB.TradeIdsInsertedInInitTask);
      Log.getLogWriter().info("TPCEBB adds " + toOutput.getTradeId());
      //Log.getLogWriter().info("toInput.getTradeTypeId() returns " + toInput.getTradeTypeId());
    }
    if (logDML) Log.getLogWriter().info("TradeOrderTxn output: " + toOutput.toString());
  }
  
  protected TPCETxnInput getTradeOrderTxnInputInInit(Boolean isSell) {
    TradeOrderTxnInput toInput = new TradeOrderTxnInput(); 
    toInput.setAcctId(acct_id);  //206LC5809NM565|Engelhardt|Yelena or 006WF1412RP228|Worton|Christopher
    if (rand.nextBoolean()) {
      toInput.setExecFirstName("Christopher");
      toInput.setExecLastName("Worton");
      toInput.setExecTaxId("006WF1412RP228");
    } else {
      toInput.setExecFirstName("Yelena");
      toInput.setExecLastName("Engelhardt");
      toInput.setExecTaxId("206LC5809NM565");
    }
     
    toInput.setSymbol(symbol);
    //toInput.setSymbol("COMS");
    toInput.setIssue("COMMON");
    
    int total = 10;
    if (rand.nextInt(total) == 1) {
      toInput.setRollItBack(1);
    } else {
      toInput.setRollItBack(0);
    }
    toInput.setIsLifo(1);
    if (isSell)
      toInput.setTradeQty(initSellQty);
    else
      toInput.setTradeQty(initBuyQty);
    
    if (rand.nextBoolean()) {
      if (isSell) {
        toInput.setTradeTypeId(type_market_sell);
      } else {
        toInput.setTradeTypeId(type_market_buy);
        toInput.setRollItBack(0);
      }
      toInput.setStSubmittedId(status_submitted);
    } else {
      if (isSell) {
        toInput.setTradeTypeId(type_limit_sell);
      } else {
        toInput.setTradeTypeId(type_limit_buy);
      }
      toInput.setRequestedPrice(new BigDecimal("25.97")); //last_trade 26.71
      toInput.setStPendingId("PNDG");
    }
    
    return toInput;
  }
  
  public static void HydraTask_runTradeResultInInit() {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TRADEORDER);
    tTest.runTradeResultInInit();
  }
  
  protected void runTradeResultInInit() {
    Connection conn = getConnectionWithSchema();
    //boolean singleThreadExe = false;
    boolean singleThreadExe = true;
    int maxRun = singleThreadExe? 80 : 2;
    try {
      for (int i=0; i<maxRun; i++)
        runTradeResultInInit(conn);
    } catch (SQLException se) {
      //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void runTradeResultInInit(Connection conn) throws SQLException {
    long trade_id = TPCEBB.getBB().getNextTradeId();
    if (trade_id == -1) {
      if (logDML) {
        Log.getLogWriter().info("no new trade_id available to process");
      } 
      return;
    } else {
      if (logDML) {
        Log.getLogWriter().info("processing trade_id: " + trade_id);
      }
    }
    TradeResultTxnInput trInput = (TradeResultTxnInput) getTradeResultTxnInputInInit(trade_id);
       
    TPCETradeResult tr = new TPCETradeResult();
    TradeResultTxnOutput trOutput = null;
    boolean retry = true;
    while (retry) {
      try {
        trOutput = (TradeResultTxnOutput) tr.runTxn(trInput, conn);
        retry = false;
      } catch (SQLException se) {
        if (se.getSQLState().equals(CONFLICTEXCEPTION)) {
          if (logDML) {
            Log.getLogWriter().info("Got conflict exception: " + TestHelper.getStackTrace(se));
          }
        } //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
        else if (se.getSQLState().equals("08003")) {
          if (logDML) {
            Log.getLogWriter().info("Got connection closed exception: " + TestHelper.getStackTrace(se));
                      
          }
          //reset the connection in threadLocal to work around the issue.  
          if (!reproduce48557) {
            this.connection = getConnection(QueryPrms.GFXD);
            setConnection(this.connection);
          }
        }
        
        else SQLHelper.handleSQLException(se);
      } 
    }
    if (logDML) Log.getLogWriter().info("TradeResultTxn output: " + trOutput.toString());
  }
  
  protected TPCETxnInput getTradeResultTxnInputInInit(long trade_id) throws SQLException {
    TradeResultTxnInput trInput = new TradeResultTxnInput(); 
    trInput.setTradeID(trade_id);
    //use trade price for now, may find out other way 
    Connection conn = getGfxdNonTxConnection();
    conn.createStatement().execute("set schema " + SCHEMA_NAME);
    
    String sql = "select t_bid_price from trade where t_id = " + trade_id;
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs.next()) {
      trInput.setTradePrice(rs.getBigDecimal("t_bid_price"));
    } else throw new TestException(sql + " does not get results for t_id = " + trade_id);

    return trInput;
  }
  
  protected static int intFor(String name) {
    if (name.equalsIgnoreCase(TRADEORDER_NAME)) {
      return TRADEORDER;
    } else if (name.equalsIgnoreCase(TRADERESULT_NAME)) {
      return TRADERESULT;
    } else if (name.equalsIgnoreCase(TPCETXN_NAME)) {
      return TPCETXN;
    } else {
      String s = "Unexpected trim interval: " + name;
      throw new QueryPerfException(s);
    }
  }

  protected String nameFor(int name) {
    switch (name) {
      case TRADEORDER:
        return TRADEORDER_NAME;
      case TRADERESULT:
        return TRADERESULT_NAME;
      case TPCETXN:
        return TPCETXN_NAME;
    }
    return super.nameFor(name);
  }
  
//------------------------------------------------------------------------------
//hydra thread locals
//------------------------------------------------------------------------------

  protected void initHydraThreadLocals() {
    Log.getLogWriter().info("TPCETest initHydraThreadLocals");
    super.initHydraThreadLocals();
    this.tpcestats = getTPCEStats();
    //Log.getLogWriter().info("in TPCETest initHydraThreadLocals, this.connection = " + this.connection);
    //Log.getLogWriter().info("end TPCETest initHydraThreadLocals");
  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    setTPCEStats(this.tpcestats);
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
  
  
 
  public static void HydraTask_alterTableSetTradeId() throws SQLException {
    //tpceTest.initialize(-1);
    tpceTest.alterTableSetTradeId();
  }
 
  protected void alterTableSetTradeId() throws SQLException{
    //currently generated default id with primary key setting works after import_table
    //The following only needs to be executed when importing data without primary key checking
    boolean performAlterTableRestart = rand.nextBoolean(); //TODO add condition if primary key was not set
    if (!performAlterTableRestart) {
      Log.getLogWriter().info("not use alter table with restart");
      return;
    }
    
    Connection conn = getGfxdNonTxConnection();
    conn.createStatement().execute("set schema " + SCHEMA_NAME);
    
    String getMaxTradeId = "select max(T_ID) from trade";
    PreparedStatement ps = conn.prepareStatement(getMaxTradeId);
    long maxT_ID;
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      maxT_ID = rs.getLong(1);
      Log.getLogWriter().info(getMaxTradeId + " returns " + maxT_ID);
    }
    else throw new TestException(getMaxTradeId + " failed to get result");
    
    if (rs.next()) {
      Log.getLogWriter().info(getMaxTradeId + " returns " + rs.getLong(1));
    } else {
      Log.getLogWriter().info(getMaxTradeId + " does not get any result ");
    }
    
    ps.close();
    
    String alterTableWithRestart = "alter table trade alter column id restart with " + ++maxT_ID;
    
    conn.createStatement().execute(alterTableWithRestart);
    Log.getLogWriter().info(alterTableWithRestart + " is executed");
    conn.close();
    
  }
  
  public static void HydraTask_verifyTradeResultAfterInit() throws SQLException {
    //tpceTest.initialize(-1);
    TPCETest tpceTest = new TPCETest();
    tpceTest.initLocalVariables(-1);
    tpceTest.verifyTradeResultAfterInit();
  }
 
  protected void verifyTradeResultAfterInit()throws SQLException {
    Connection conn = getGfxdNonTxConnection();
    conn.createStatement().execute("set schema " + SCHEMA_NAME);
    
    String selectHoldingSummaryTBL = "select HS_QTY from HOLDING_SUMMARY_TBL where HS_CA_ID = ? and HS_S_SYMB = ? ";
    PreparedStatement ps = conn.prepareStatement(selectHoldingSummaryTBL);
    ps.setLong(1, acct_id);
    ps.setString(2, symbol);
    
    int hs_qty;
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      hs_qty = rs.getInt("HS_QTY");
      if (logDML) { 
        Log.getLogWriter().info(selectHoldingSummaryTBL + " get HS_QTY: " + hs_qty
            + " for HS_CA_ID = " + acct_id + " and HS_S_SYMB = "  + symbol);
      }
    } else {
      throw new TestException(selectHoldingSummaryTBL +  " expected to get result after trade result txns " +
      		"for HS_CA_ID = " + acct_id + " and HS_S_SYMB = "  + symbol);
    }
    
    totalSellQtyInInit = -1 * initSellQty * (int) TPCEBB.getBB().getSharedCounters().read(TPCEBB.TradeIdsInsertedInInitTask);
    
    
    if (hs_qty != totalSellQtyInInit) {
      throw new TestException ("Total expected hs_qty after processing trade results is " + totalSellQtyInInit + 
          " but acutal hs_qty from holding_summary_table is " + hs_qty);
    }
    
    rs.close();
    
    String selectHolding = "select * from HOLDING where H_CA_ID = ? and H_S_SYMB = ? ";
    ps = conn.prepareStatement(selectHolding);
    ps.setLong(1, acct_id);
    ps.setString(2, symbol);
    
    rs = ps.executeQuery();
    if (logDML) Log.getLogWriter().info("holding table info: " + ResultSetHelper.
        listToString(ResultSetHelper.asList(rs, false)));
    
    rs.close();
    
  }
 
  public static void HydraTask_stopFabricServer() {
    FabricServerHelper.stopFabricServer();
  }
  
  public static void HydraTask_restartFabricServer() throws SQLException {
    long start = System.currentTimeMillis();
    Log.getLogWriter().info("recovery from persistent table starts from " + start);
    startFabricServerTask();
    
    long end = System.currentTimeMillis();
    Log.getLogWriter().info("recovery from persistent table finishes at " + end);
    
    long time = end - start;
    
    Log.getLogWriter().info("recovery takes " + time + " ms");
    TPCEBB.getBB().getSharedCounters().add(TPCEBB.totalServerRecoveryTime, time);
    
  }
  
  public static void HydraTask_reportRecoveryTestResults() {
    Log.getLogWriter().info("import table takes " + 
        TPCEBB.getBB().getSharedCounters().read(TPCEBB.importTableTime) + " ms");
    
    Log.getLogWriter().info("total aggregated recovery time for all data nodes is " +
        TPCEBB.getBB().getSharedCounters().read(TPCEBB.totalServerRecoveryTime)
        + " ms");
  }
  
  public static void HydraTask_runMarketFeedInInit() {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TRADEORDER);
    tTest.runMarketFeedInInit();
  }
  
  protected void runMarketFeedInInit() {
    Connection conn = getConnectionWithSchema();
   
    try {
      runMarketFeedInInit(conn);
    } catch (SQLException se) {
      //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void runMarketFeedInInit(Connection conn) throws SQLException {
    MarketFeedTxnInput mfInput = (MarketFeedTxnInput) getMarketFeedTxnInputInInit();
       
    TPCEMarketFeed mf = new TPCEMarketFeed();       
    MarketFeedTxnOutput mfOutput = (MarketFeedTxnOutput) mf.runTxn(mfInput, conn);
    
    
    if (logDML) Log.getLogWriter().info("MarketFeedTxn output: " + mfOutput.toString());
  }
  
  protected TPCETxnInput getMarketFeedTxnInputInInit() {
    MarketFeedTxnInput mfInput = new MarketFeedTxnInput(); 
    //TODO get valid data
    int num = 1;
    
    mfInput.setLimitBuy(type_limit_buy);
    mfInput.setLimitSell(type_limit_sell);
    mfInput.setStopLoss(type_stop_loss);
    mfInput.setStatusSubmitted(status_submitted);
    BigDecimal[] price_quotes = new BigDecimal[num];
    int[] trade_qty = new int[num];
    String[] symbols = new String[num];
    
    price_quotes[0] = new BigDecimal("25.97");
    trade_qty[0] = 500;
    symbols[0] = symbol;
    
    mfInput.setPriceQuotes(price_quotes);
    mfInput.setSymbol(symbols);
    mfInput.setTradeQty(trade_qty);
    
    return mfInput;
  }
  
  public static void HydraTask_runTradeOrder() {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TRADEORDER);
    CE ce = new CE();
    tTest.runTradeOrder(ce);
  }
  
  protected void runTradeOrder(CE ce) {
    Connection conn = getConnectionWithSchema();
   
    try {
      if (logDML) Log.getLogWriter().info("execute TradeOrderTxn");
      runTradeOrder(conn, ce);
    } catch (SQLException se) {
      //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void runTradeOrder(Connection conn, CE ce) throws SQLException {    
    TradeOrderTxnInput toInput = (TradeOrderTxnInput) ce.getTradeOrderTxn(); 
       
    TPCETradeOrder to = new TPCETradeOrder();       
    TradeOrderTxnOutput toOutput = null;
    
    long start = this.tpcestats.startTransaction(TRADE_ORDER);
    
    boolean retry = true;
    while (retry) {
      try {
        toOutput = (TradeOrderTxnOutput) to.runTxn(toInput, conn);
        retry = false;
      } catch (SQLException se) {
        if (se.getSQLState().equals(CONFLICTEXCEPTION)) {
          if (logDML) {
            Log.getLogWriter().info("Will retry after getting expected conflict exception: " 
                + TestHelper.getStackTrace(se));
          }
        } //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
             
        else SQLHelper.handleSQLException(se);
      } 
    }
     
    this.tpcestats.endTransaction(TRADE_ORDER, start);
    if (logDML) Log.getLogWriter().info("TradeOrderTxn output: " + toOutput.toString()); 
  }
  
  public static void HydraTask_runMarketFeed() throws InterruptedException {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TRADEORDER);
    MEE mee = new MEE();
    tTest.runMarketFeed(mee);
  }
  
  protected void runMarketFeed(MEE mee) throws InterruptedException {
    Connection conn = getConnectionWithSchema();

    try {
      if (logDML) Log.getLogWriter().info("execute MarketFeedTxn");
      runMarketFeed(conn, mee);
    } catch (SQLException se) {
      //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void runMarketFeed(Connection conn, MEE mee) 
  throws SQLException, InterruptedException {
    MarketFeedTxnInput mfInput = (MarketFeedTxnInput) getMarketFeedTxnInput(conn, mee);
       
    TPCEMarketFeed mf = new TPCEMarketFeed();
    
    long start = this.tpcestats.startTransaction(MARKET_FEED);
    MarketFeedTxnOutput mfOutput = (MarketFeedTxnOutput) mf.runTxn(mfInput, conn);
    
    this.tpcestats.endTransaction(MARKET_FEED, start);
    if (logDML) Log.getLogWriter().info("MarketFeedTxn output: " + mfOutput.toString());
  }
  
  protected TPCETxnInput getMarketFeedTxnInput(Connection conn, MEE mee)
  throws SQLException, InterruptedException {
    int minMfTxnInputsQueueSize = 10;
    if (mee.getMfTxnInputQueueSize() < minMfTxnInputsQueueSize)
      mee.processTradesSubmitted(conn);
    
    return mee.getMfTxnInput();  
  }
  
  public static void HydraTask_runTradeResult() throws InterruptedException {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TRADEORDER);
    MEE mee = new MEE();
    tTest.runTradeResult(mee);
  }
  
  protected void runTradeResult(MEE mee) throws InterruptedException {
    Connection conn = getConnectionWithSchema();
    
    try {
      if (logDML) Log.getLogWriter().info("execute TradeResultTxn");
      
      runTradeResult(conn, mee);
    } catch (SQLException se) {
      //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void runTradeResult(Connection conn, MEE mee) 
  throws SQLException, InterruptedException {
    TradeResultTxnInput trInput = (TradeResultTxnInput) getTradeResultTxnInput(conn, mee);
       
    TPCETradeResult tr = new TPCETradeResult();
    TradeResultTxnOutput trOutput = null;
    
    long start = this.tpcestats.startTransaction(TRADE_RESULT);
    boolean retry = true;
    while (retry) {
      try {
        trOutput = (TradeResultTxnOutput) tr.runTxn(trInput, conn);
        retry = false;
      } catch (SQLException se) {
        if (se.getSQLState().equals(CONFLICTEXCEPTION)) {
          if (logDML) {
            Log.getLogWriter().info("Will retry after getting expected conflict exception: " 
                + TestHelper.getStackTrace(se));
          }
        } //TODO, need to handle HA failure etc but in runTxn task, this is for testing each txn works
        else if (se.getSQLState().equals("08003")) {
          if (logDML) {
            Log.getLogWriter().info("Got connection closed exception: " + TestHelper.getStackTrace(se));
                      
          }
          //reset the connection in threadLocal to work around the issue.  
          if (!reproduce48557) {
            this.connection = getConnection(QueryPrms.GFXD);
            setConnection(this.connection);
          }
        }
        
        else SQLHelper.handleSQLException(se);
      } 
    }
    this.tpcestats.endTransaction(TRADE_RESULT, start);
    if (logDML) Log.getLogWriter().info("TradeResultTxn output: " + trOutput.toString());
  }
  
  protected TPCETxnInput getTradeResultTxnInput(Connection conn, MEE mee) 
  throws SQLException, InterruptedException {
    int minTrTxnInputsQueueSize = 100;
    if (mee.getTrTxnInputQueueSize() < minTrTxnInputsQueueSize)
      mee.processTradesSubmitted(conn);
    
    return mee.getTrTxnInput();  
  }
  
  
  public static void HydraTask_verifyTradeResultAfterBuyInit() throws SQLException {
    //tpceTest.initialize(-1);
    tpceTest.verifyTradeResultAfterBuyInit();
  }
 
  protected void verifyTradeResultAfterBuyInit()throws SQLException {
    Connection conn = getGfxdNonTxConnection();
    conn.createStatement().execute("set schema " + SCHEMA_NAME);
    
    String selectHoldingSummaryTBL = "select HS_QTY from HOLDING_SUMMARY_TBL where HS_CA_ID = ? and HS_S_SYMB = ? ";
    PreparedStatement ps = conn.prepareStatement(selectHoldingSummaryTBL);
    ps.setLong(1, acct_id);
    ps.setString(2, symbol);
    
    int hs_qty;
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      hs_qty = rs.getInt("HS_QTY");
      if (logDML) { 
        Log.getLogWriter().info(selectHoldingSummaryTBL + " get HS_QTY: " + hs_qty
            + " for HS_CA_ID = " + acct_id + " and HS_S_SYMB = "  + symbol);
      }
    } else {
      throw new TestException(selectHoldingSummaryTBL +  " expected to get result after trade result txns " +
          "for HS_CA_ID = " + acct_id + " and HS_S_SYMB = "  + symbol);
    }
    rs.close();
    
    int expectedHSQty = totalSellQtyInInit + initBuyQty;
      
    if (hs_qty != expectedHSQty) {
      throw new TestException ("Total expected hs_qty after processing trade results is " + expectedHSQty + 
          " but acutal hs_qty from holding_summary_table is " + hs_qty);
    }
    
    String selectHolding = "select * from HOLDING where H_CA_ID = ? and H_S_SYMB = ? ";
    ps = conn.prepareStatement(selectHolding);
    ps.setLong(1, acct_id);
    ps.setString(2, symbol);
    
    rs = ps.executeQuery();
    if (logDML) Log.getLogWriter().info("holding table info: " + ResultSetHelper.
        listToString(ResultSetHelper.asList(rs, false)));
    
    rs.close();
    
  }
  
  public static void HydraTask_createTradeToMEETable () throws SQLException {
    tpceTest.createTradeToMEETable();
  }
  
  
  protected void createTradeToMEETable() throws SQLException {
    Connection conn = getGfxdNonTxConnection();
    conn.createStatement().execute("set schema " + SCHEMA_NAME);
    
    String sql = tradeToMarketWithDefaultId ? createTradeToMarketTable : createTradeToMarketTableWithoutDefaultId;

    conn.createStatement().execute(sql);
    
    Log.getLogWriter().info(sql + " is created");
    conn.close();
    
  }
  
  public static void HydraTask_initCE(){
    
    tpceTest.initCE();
  }
  
  protected void initCE() {
    Log.getLogWriter().info("this connection is " + this.connection);
    Connection conn = getConnectionWithSchema();
    Log.getLogWriter().info("conn is " + conn);
    CE ce= new CE();
   
    try {
      ce.setCaInfo(conn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_runTPCETxns() throws InterruptedException, SQLException {
    TPCETest tTest = new TPCETest();
    tTest.initialize(TPCETXN); 
    tTest.runTPCETxns();
  }

  protected void runTPCETxns()
  throws InterruptedException, SQLException {
    //Log.getLogWriter().info("in runTPCETxns, this.queryAPI = " + this.queryAPI);
    //Log.getLogWriter().info("in runTPCETxns, this.connection = " + this.connection);
    CE ce = new CE();
    MEE mee = new MEE();
    int throttleMs = 0; // getThrottleMs(); //throttleMs tbd
    do {
      executeTaskTerminator(); // does not commit
      executeWarmupTerminator(); // does not commit
      enableQueryPlanGeneration();

      if (this.queryPlanGenerationEnabled) {
        // dump a query plan for each transaction type
        //TODO to add run with query plan
        
      } else {
        // shuffle and run the full deck
        int whichTxn = runWhichTxn();
        for (int i = 0; i < 5; i++) {
          //this.tpccstats.setTxCardInProgress(i);
          runTPCETransaction(whichTxn, ce, mee);
          if (throttleMs != 0) {
            Thread.sleep(throttleMs);
          }
        }
      }
      disableQueryPlanGeneration();
      updateLastTradeInCE(ce);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount; // not really needed
    } while (!executeBatchTerminator()); // does not commit
    
  }
  
  private int runWhichTxn() throws SQLException {
    long tradeSendToMarket = TPCEBB.getBB().getSharedCounters().read(TPCEBB.tradeSentToMarket);
    long tradeProcessed = TPCEBB.getBB().getSharedCounters().read(TPCEBB.tradeProcessedByMarket);
    
    //TODO, needs to be changed once other Txns are added.
    int minTradeToBeProcessed = 1000;
    if (tradeSendToMarket - tradeProcessed < minTradeToBeProcessed) {
      return TRADE_ORDER; 
    } else {      
      if (rand.nextInt(10) == 0) return MARKET_FEED;
      else return TRADE_RESULT;
    }
    
  }
  
  protected void runTPCETransaction(int whichTxn, CE ce, MEE mee) throws InterruptedException {
    switch (whichTxn) {
    case TRADE_ORDER:
      runTradeOrder(ce);
      break;
    case TRADE_RESULT:
      runTradeResult(mee);
      break;
    case MARKET_FEED:
      runMarketFeed(mee);
      break;
    default: throw new TestException ("should not happen with other txn");
    }
  }
  
  protected void updateLastTradeInCE(CE ce) throws SQLException {
    long currTime = System.currentTimeMillis();
    if (currTime - ce.getLastTradeUpdatedTime() > 30000) 
      ce.setLastTradeInfo(getConnectionWithSchema());
  }
  
  //need only one thr to check this
  public static void HydraTask_validateResults() throws SQLException {
    tpceTest.validateResults();
  }
  
  /* only limited tables are evicted with hdfs back up
   * currently - trade, trade_history, holding_history and cash_settlement with hdfs
   */
  private void validateResults() throws SQLException {
    Connection conn = getGfxdNonTxConnection();
    conn.createStatement().execute("set schema " + SCHEMA_NAME);
   
    validateCompletedOrders(conn);
    
    validateBrokerCommissions(conn);
    
    validateHoldingSummary(conn);
    
    conn.close();
  }
  
  private void validateCompletedOrders(Connection conn) throws SQLException {
    String sql = "select b_id, b_num_trades as num_trades from broker order by b_id";
    Log.getLogWriter().info("exectuing numOfTradesFromBroker qeury: " + sql);
    ResultSet bRs = conn.createStatement().executeQuery(sql);
    
    List<Struct> numOfTradesFromBroker = ResultSetHelper.asList(bRs, false);
    bRs.close();
    
    boolean useQueryHint = isHDFSTest && rand.nextBoolean();
    
    sql = "select b_id, count (*) as num_trades from broker, customer_account, trade where " +
    		"b_id = ca_b_id and ca_id = t_ca_id and t_st_id = '" + status_completed + 
    		"' group by b_id order by b_id";
    String sqlHdfsQueryHint = "select b_id, count (*) as num_trades from broker, customer_account, trade -- GEMFIREXD-PROPERTIES queryHDFS=true \n where " +
      "b_id = ca_b_id and ca_id = t_ca_id and t_st_id = '" + status_completed + 
      "' group by b_id order by b_id";
    
    
    Statement stmt = null;
    ResultSet joinRs = null;
    if (isHDFSTest && !useQueryHint) {
      Log.getLogWriter().info("using query-HDFS connection");
      Log.getLogWriter().info("exectuing numOfTradesFromJoin query: " + sql);
      stmt = ((Connection)queryHDFSConn.get()).createStatement();
      joinRs = stmt.executeQuery(sql);
    } else {
      stmt = conn.createStatement();
      if (useQueryHint) {
        Log.getLogWriter().info("exectuing numOfTradesFromJoin query using query hint: " + sqlHdfsQueryHint);
        joinRs = stmt.executeQuery(sqlHdfsQueryHint);
      }
      else {
        Log.getLogWriter().info("exectuing numOfTradesFromJoin query: " + sql);
        joinRs = stmt.executeQuery(sql);
      }      
    }

    List<Struct> numOfTradesFromJoin = ResultSetHelper.asList(joinRs, false);
    joinRs.close();
    
    Log.getLogWriter().info("number of trades for each broker is " 
        + ResultSetHelper.listToString(numOfTradesFromBroker));
    
    ResultSetHelper.compareSortedResultSets(numOfTradesFromBroker, numOfTradesFromJoin,
        "numOfTradesFromBroker", "numOfTradesFromJoin");

  }
  
  
  private void validateBrokerCommissions(Connection conn) throws SQLException {
    String sql = "select b_id, b_comm_total as commission from broker order by b_id";
    Log.getLogWriter().info("exectuing commissionFromBroker qeury: " + sql);
    ResultSet bRs = conn.createStatement().executeQuery(sql);
    
    List<Struct> commissionFromBroker = ResultSetHelper.asList(bRs, false);
    bRs.close();
    
    sql = "select b_id, sum (t_comm) as commission from broker, customer_account, trade where " +
        "b_id = ca_b_id and ca_id = t_ca_id and t_st_id = '" + status_completed + 
        "' group by b_id order by b_id";
    String sqlHdfsQueryHint = "select b_id, sum (t_comm) as commission from broker, customer_account, " +
    	  "trade -- GEMFIREXD-PROPERTIES queryHDFS=true \nwhere " +
    	  "b_id = ca_b_id and ca_id = t_ca_id and t_st_id = '" + status_completed + 
    	  "' group by b_id order by b_id";
    
    boolean useQueryHint = isHDFSTest && rand.nextBoolean();
    Statement stmt = null;
    ResultSet joinRs = null;
    if (isHDFSTest && !useQueryHint) {
      Log.getLogWriter().info("using query-HDFS connection");
      Log.getLogWriter().info("exectuing commissionFromJoin query: " + sql);
      stmt = ((Connection)queryHDFSConn.get()).createStatement();
      joinRs = stmt.executeQuery(sql);
    } else {
      stmt = conn.createStatement();
      if (useQueryHint) {
        Log.getLogWriter().info("exectuing commissionFromJoin query using query hint: " + sqlHdfsQueryHint);
        joinRs = stmt.executeQuery(sqlHdfsQueryHint);
      }
      else {
        Log.getLogWriter().info("exectuing commissionFromJoin query: " + sql);
        joinRs = stmt.executeQuery(sql);
      }      
    }
       
    List<Struct> commissionFromJoin = ResultSetHelper.asList(joinRs, false);
    joinRs.close();
    
    Log.getLogWriter().info("number of trades for each broker is " 
        + ResultSetHelper.listToString(commissionFromBroker));
    
    ResultSetHelper.compareSortedResultSets(commissionFromBroker, commissionFromJoin,
        "commissionFromBroker", "commissionFromJoin");

  }
  
  private void validateHoldingSummary(Connection conn) throws SQLException {
    boolean withQtyCondition = rand.nextBoolean();
    String sql = "select hs_ca_id, hs_s_symb, hs_qty from holding_summary_tbl " +
        (withQtyCondition ? " where hs_qty > 0 " : "") +
    		" order by hs_ca_id, hs_s_symb";
    Log.getLogWriter().info("exectuing holingSummaryTable qeury: " + sql);
    ResultSet bRs = conn.createStatement().executeQuery(sql);
    
    List<Struct> holingSummaryTable = ResultSetHelper.asList(bRs, false);
    bRs.close();
    
    sql = "select hs_ca_id, hs_s_symb, hs_qty from holding_summary " +
        (withQtyCondition ? "where hs_qty > 0" : "") +
    		" order by hs_ca_id, hs_s_symb";
    Log.getLogWriter().info("exectuing holingSummaryView query: " + sql);
    ResultSet viewRs = conn.createStatement().executeQuery(sql);
    
    List<Struct> holingSummaryView = ResultSetHelper.asList(viewRs, false);
    viewRs.close();
    
    //Log.getLogWriter().info("holding summary for each account per security is " 
    //    + ResultSetHelper.listToString(holingSummaryTable));
    
    ResultSetHelper.compareSortedResultSets(holingSummaryTable, holingSummaryView,
        "holingSummaryTable", "holingSummaryView");

  }
  
}

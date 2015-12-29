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

package sql.poc.useCase2.perf;


import hydra.*;
import hydra.gemfirexd.LonerHelper;
import hydra.HydraThreadLocal;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.*;

import com.gemstone.gemfire.cache.query.Struct;

import cacheperf.*;


import sql.GFEDBClientManager;
import sql.GFEDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

/**
 *
 *  Client used to measure cache performance.
 *
 */
public class QueryPerfClient extends cacheperf.CachePerfClient {

  //----------------------------------------------------------------------------
  //  Trim interval names
  //----------------------------------------------------------------------------

  protected static final int QUERIES     = 10031;
  protected static final int SELECTQUERIES = 10055;

  protected static final String QUERY_NAME = "queries"; 
  protected static final String SELECT_QUERY_NAME = "selectqueries";
  protected static boolean exeQuery1 = false; 
  protected static boolean exeQuery2 = false;
  protected static boolean exeselectQuery1 = false; 
  protected static boolean exeselectQuery2 = false;
  protected static boolean exeselectQuery3 = false; 
  protected static boolean exeselectQuery4 = false;
  protected static boolean exeselectQuery5 = false; 
  protected static boolean exeselectQuery6 = false;
  protected static HydraThreadLocal myConn = new HydraThreadLocal();
  static boolean[] queryDataSet = new boolean[1];
  static List<Struct>queryData = null;
  static boolean[] queryDataSetDOC_3 = new boolean[1];
  static List<Struct>queryDataDOC_3 = null;
  public static boolean useOldListAgg =  TestConfig.tab().booleanAt(SQLPrms.useOldListAgg, false);
  public static boolean useListAggNewImpl =  TestConfig.tab().booleanAt(SQLPrms.useListAggNewImpl, false);
  public static boolean useListAggPreparedStmt = TestConfig.tab().booleanAt(SQLPrms.useListAggPreparedStmt, false);
  static String listAggCols = "IDX_COL1";
  static String tableName = "CDSDBA.XML_IDX_1_1";      
  static String delimiter = ",";
  static String groupBy = "IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC";
  static String[] selects = {
    "select IDX_COL1, XML_DOC_ID_NBR from XML_IDX_20_1 where IDX_COL1>?  ORDER BY IDX_COL1 asc, XML_DOC_ID_NBR asc OFFSET 0 ROWS FETCH FIRST 26 ROWS ONLY",
    "select LAST_UPDATE_TMSTP, LAST_UPDATE_SYSTEM_NM,XML_DOC_ID_NBR,MSG_PAYLOAD1_IMG,MSG_PAYLOAD_QTY,MSG_MINOR_VERSION_NBR,PRESET_DICTIONARY_ID_NBR,CREATE_MINT_CD,MSG_PAYLOAD_SIZE_NBR,DELETED_FLG,STRUCTURE_ID_NBR,MSG_PURGE_DT,MSG_PAYLOAD2_IMG,OPT_LOCK_TOKEN_NBR,LAST_UPDATE_TMSTP,MSG_MAJOR_VERSION_NBR from XML_DOC_3 where XML_DOC_ID_NBR in (?) and STRUCTURE_ID_NBR in (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
    "select IDX_COL1, IDX_COL2, IDX_COL3, IDX_COL4, IDX_COL5, IDX_COL6, IDX_COL7, IDX_COL8, XML_DOC_ID_NBR from XML_IDX_65_1 where IDX_COL1=? and IDX_COL3<=? and IDX_COL4>=?  ORDER BY IDX_COL1 asc, IDX_COL2 asc, IDX_COL3 asc, IDX_COL4 asc, IDX_COL5 asc, IDX_COL6 asc, IDX_COL7 asc, IDX_COL8 asc, XML_DOC_ID_NBR asc OFFSET 0 ROWS FETCH FIRST 26 ROWS ONLY",
    "select IDX_COL1, IDX_COL2, IDX_COL3, XML_DOC_ID_NBR from XML_IDX_79_1 where IDX_COL1=? and IDX_COL2<=? and IDX_COL3>=?  ORDER BY IDX_COL1 asc, IDX_COL2 asc, IDX_COL3 asc, XML_DOC_ID_NBR asc OFFSET 0 ROWS FETCH FIRST 26 ROWS ONLY",
    "select LAST_UPDATE_SYSTEM_NM,XML_DOC_ID_NBR,MSG_PAYLOAD1_IMG,MSG_PAYLOAD_QTY,MSG_MINOR_VERSION_NBR,PRESET_DICTIONARY_ID_NBR,CREATE_MINT_CD,MSG_PAYLOAD_SIZE_NBR,DELETED_FLG,STRUCTURE_ID_NBR,MSG_PURGE_DT,MSG_PAYLOAD2_IMG,OPT_LOCK_TOKEN_NBR,LAST_UPDATE_TMSTP,MSG_MAJOR_VERSION_NBR from XML_DOC_3 where XML_DOC_ID_NBR in (?) and STRUCTURE_ID_NBR in (?)",
    "select IDX_COL1, IDX_COL2, IDX_COL3, IDX_COL4, IDX_COL5, XML_DOC_ID_NBR from XML_IDX_25_2 where IDX_COL1=? and IDX_COL2=? and IDX_COL3=?  ORDER BY IDX_COL1 asc, IDX_COL2 asc, IDX_COL3 asc, IDX_COL4 asc, IDX_COL5 asc, XML_DOC_ID_NBR asc OFFSET 0 ROWS FETCH FIRST 101 ROWS ONLY",
    "select LAST_UPDATE_SYSTEM_NM,XML_DOC_ID_NBR,MSG_PAYLOAD1_IMG,MSG_PAYLOAD_QTY,MSG_MINOR_VERSION_NBR,PRESET_DICTIONARY_ID_NBR,CREATE_MINT_CD,MSG_PAYLOAD_SIZE_NBR,DELETED_FLG,STRUCTURE_ID_NBR,MSG_PURGE_DT,MSG_PAYLOAD2_IMG,OPT_LOCK_TOKEN_NBR,LAST_UPDATE_TMSTP,MSG_MAJOR_VERSION_NBR from XML_DOC_3 where XML_DOC_ID_NBR in (?) and STRUCTURE_ID_NBR in (?)",
    "select IDX_COL1, IDX_COL2, IDX_COL3, IDX_COL4, IDX_COL5, XML_DOC_ID_NBR from XML_IDX_25_2 where IDX_COL1=? and IDX_COL2=? and IDX_COL3=?  ORDER BY IDX_COL1 asc, IDX_COL2 asc, IDX_COL3 asc, IDX_COL4 asc, IDX_COL5 asc, XML_DOC_ID_NBR asc OFFSET 0 ROWS FETCH FIRST 101 ROWS ONLY",
    "select LAST_UPDATE_SYSTEM_NM,XML_DOC_ID_NBR,MSG_PAYLOAD1_IMG,MSG_PAYLOAD_QTY,MSG_MINOR_VERSION_NBR,PRESET_DICTIONARY_ID_NBR,CREATE_MINT_CD,MSG_PAYLOAD_SIZE_NBR,DELETED_FLG,STRUCTURE_ID_NBR,MSG_PURGE_DT,MSG_PAYLOAD2_IMG,OPT_LOCK_TOKEN_NBR,LAST_UPDATE_TMSTP,MSG_MAJOR_VERSION_NBR from XML_DOC_3 where XML_DOC_ID_NBR in (?) and STRUCTURE_ID_NBR in (?)",
    "select IDX_COL1, IDX_COL2, IDX_COL3, IDX_COL4, IDX_COL5, XML_DOC_ID_NBR from XML_IDX_25_2 where IDX_COL1=? and IDX_COL2=? and IDX_COL3=?  ORDER BY IDX_COL1 asc, IDX_COL2 asc, IDX_COL3 asc, IDX_COL4 asc, IDX_COL5 asc, XML_DOC_ID_NBR asc OFFSET 0 ROWS FETCH FIRST 101 ROWS ONLY",
    "select LAST_UPDATE_SYSTEM_NM,XML_DOC_ID_NBR,MSG_PAYLOAD1_IMG,MSG_PAYLOAD_QTY,MSG_MINOR_VERSION_NBR,PRESET_DICTIONARY_ID_NBR,CREATE_MINT_CD,MSG_PAYLOAD_SIZE_NBR,DELETED_FLG,STRUCTURE_ID_NBR,MSG_PURGE_DT,MSG_PAYLOAD2_IMG,OPT_LOCK_TOKEN_NBR,LAST_UPDATE_TMSTP,MSG_MAJOR_VERSION_NBR from XML_DOC_3 where XML_DOC_ID_NBR in (?) and STRUCTURE_ID_NBR in (?)",
    "select LAST_UPDATE_SYSTEM_NM,XML_DOC_ID_NBR,MSG_PAYLOAD1_IMG,MSG_PAYLOAD_QTY,MSG_MINOR_VERSION_NBR,PRESET_DICTIONARY_ID_NBR,CREATE_MINT_CD,MSG_PAYLOAD_SIZE_NBR,DELETED_FLG,STRUCTURE_ID_NBR,MSG_PURGE_DT,MSG_PAYLOAD2_IMG,OPT_LOCK_TOKEN_NBR,LAST_UPDATE_TMSTP,MSG_MAJOR_VERSION_NBR from XML_DOC_3 where XML_DOC_ID_NBR in (?, ?, ?) and STRUCTURE_ID_NBR in (?)", 
  };
  //static String[] col3s = {"PP", "PA", "PBA", "PS"};
  static String[] col3s = {"PP"}; //only need one based on query to be executed
  static int XML_DOC_ID_NBR_base = 500;
  static int base = 1000;
  protected static HydraThreadLocal numOfOp = new HydraThreadLocal();
  static String IDX_25_COL4 = "1234567891234545667";
  static String IDX_65_COL3 = "1198775361795";
  static String IDX_65_COL4 = "4102380000000";
  static String IDX_79_COL2 = "1198775361795";
  static String IDX_79_COL3 = "4102380000000";
  public static int numOfWorkers = (int) TestConfig.tab().longAt(SQLPrms.numOfWorkers, 400);
  
  static int[] seedSTRUCTURE_ID_NBR = new int[5];
  static{  
    int n = 1;
    for (int i=0; i<5; i++) {
      n += i;
      seedSTRUCTURE_ID_NBR[i] = n;
    }
  }
  
  
  static byte[] insertOps = null;
  static {
    String jtests = System.getProperty( "JTESTS" );
    //String sqlInsertOpFilePath = jtests + "/sql/poc/useCase2/insertOps.sql";
    //insert into doc_3 is added in the test
    String sqlInsertOpFilePath = jtests + "/sql/poc/useCase2/insertOpsNoDOC_3.sql";
    File insertFile= new File(sqlInsertOpFilePath);
    try {
      insertOps = getBytesFromFile(insertFile);
    } catch (IOException e) {
      throw new TestException ("could not get bytes from file" + TestHelper.getStackTrace(e));
    }
  }

  
  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------
  /**
   *  queryTask()
   */
  
  public static void initQueryTask() {
    QueryPerfClient c = new QueryPerfClient();
    //c.initHydraThreadLocals();
    
    int exeQueryNum = QueryPerfPrms.exeQueryNum(); //which query to be executed
    switch (exeQueryNum) {
    case 1: 
      exeQuery1 = true;
      break;
    case 2:
      exeQuery2 = true;
      break;
    default:
      throw new HydraInternalException( "Can't execute this query." );
    }
  }
  
  public static void HydraTask_addData() {
    QueryPerfClient c = new QueryPerfClient();
    c.addData();
  }
  
  protected void addData() {
    Connection conn = (Connection) myConn.get();    
    addDataDOC_3(conn);
    addDataIDX_25(conn);
    doInsertIDX_20_Op(conn, 0);
    doInsertIDX_65_Op(conn, 0);
    doInsertIDX_79_Op(conn, 0);

  }  
  
  public static void HydraTask_generateQueryData() {
    QueryPerfClient c = new QueryPerfClient();
    c.generateQueryData();
  }
  
  protected void generateQueryData() {
    synchronized (queryDataSet) {
      if (!queryDataSet[0]) {
        if (queryData == null) {
          Connection conn = getConnection();
          String sql = "select IDX_COL1, IDX_COL2, IDX_COL3 from CDSDBA.XML_IDX_1_1 "; 
          try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
            queryData = ResultSetHelper.asList(rs, false);
          } catch (SQLException se) {
            SQLHelper.handleSQLException(se);
          }
        }
        else {
          throw new TestException ("query data is already set but queryDataSet flag is not set");
        }
        queryDataSet[0] = true;
      } else {
        log().info("queryData is set");
      }
    }
  }
  
  public static void HydraTask_generateQueryDataDOC_3() {
    QueryPerfClient c = new QueryPerfClient();
    c.generateQueryDataDOC_3();
  }
  
  protected void generateQueryDataDOC_3() {
    synchronized (queryDataSetDOC_3) {
      if (!queryDataSetDOC_3[0]) {
        if (queryDataDOC_3 == null) {
          Connection conn = getConnection();
          String sql = "select XML_DOC_ID_NBR, STRUCTURE_ID_NBR from CDSDBA.XML_DOC_3"; 
          try {
            log().info("setting queryDataDOC_3 ");
            ResultSet rs = conn.createStatement().executeQuery(sql);
            queryDataDOC_3 = ResultSetHelper.asList(rs, false);
          } catch (SQLException se) {
            SQLHelper.handleSQLException(se);
          }
        }
        else {
          throw new TestException ("query data is already set but queryDataSetDOC_3 flag is not set");
        }
        queryDataSetDOC_3[0] = true;
      } else {
        log().info("queryDataDOC_3 is set");
      }
    }
  }
  /*
  public static void HydraTask_initInsertInputStream() {
    QueryPerfClient c = new QueryPerfClient();
    c.initInsertInputStream();
  }
  
  protected void initInsertInputStream(){

  }
  */
  
  public static void queryTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initialize( QUERIES );
    c.executeQuery();
  }
  
  
  public static void selectQueryTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initialize( SELECTQUERIES );
    c.executeSelectQuery();
  } 
  
  private void executeQuery() {
    Connection conn = (Connection) myConn.get();
    if (log().fineEnabled()) log().fine("executing query");
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      
      if (exeQuery1)      query1(conn);
      else if (exeQuery2) query2(conn);

      
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;

    } while (!executeBatchTerminator());
    if (log().fineEnabled()) log().fine("finishing executing query");
  }
  
  private void executeSelectQuery() {
    Connection conn = (Connection) myConn.get();
    if (log().fineEnabled()) log().fine("executing select query");
    do {
      executeTaskTerminator();
      executeWarmupTerminator();
      
      selectQuery(conn);


      
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;

    } while (!executeBatchTerminator());
    if (log().fineEnabled()) log().fine("finishing executing query");
  }
  
  private void query1(Connection conn) {
    try {
      callXML_IDXProcedures(conn);   
    } catch(Exception e) {     // IllegalState or IllegalArgumentExceptions
      throw new CachePerfException( "Could not get query result", e );
    }
   
  }  
  
  private void query2(Connection conn) {   
    try {      
      callXML_DOCProcedures(conn);      
    } catch(Exception e) {     // IllegalState or IllegalArgumentExceptions
      throw new CachePerfException( "Could not get query result", e );
    }
   
    
  }
  
  //-- for perf tests cases
  protected void callXML_IDXProcedures(Connection conn) {
  /*  
    String[] sql = {
        "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC','IDX_COL1','CDSDBA.XML_IDX_1_1','IDX_COL1=''211Some'' and IDX_COL2=''809012'' and IDX_COL3=''NEWYORK''', ',') WITH RESULT PROCESSOR ListAggProcessor",
        "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC','IDX_COL1','CDSDBA.XML_IDX_1_1','IDX_COL1=''823OLD'' and IDX_COL2=''80938'' and IDX_COL3=''ORLANDO''', ',') WITH RESULT PROCESSOR ListAggProcessor",
        "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC','IDX_COL1','CDSDBA.XML_IDX_1_1','IDX_COL1=''600ELM'' and IDX_COL2=''80938'' and IDX_COL3=''PORTLAND''', ',') WITH RESULT PROCESSOR ListAggProcessor",
        "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC','IDX_COL1','CDSDBA.XML_IDX_1_1','IDX_COL1=''488OLD'' and IDX_COL2=''80938'' and IDX_COL3=''PORTLAND''', ',') WITH RESULT PROCESSOR ListAggProcessor",
        "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC','IDX_COL1','CDSDBA.XML_IDX_1_1','IDX_COL1=''013Some'' and IDX_COL2=''80938'' and IDX_COL3=''TRENTON''', ',') WITH RESULT PROCESSOR ListAggProcessor",
    };
    
    callOldListAggProcedure(conn, sql[SQLTest.random.nextInt(sql.length)]); //both impl in the test use the old procedure call, unless the new procedures api is called
   */ 
    
    Object[] fields = queryData.get(SQLTest.random.nextInt(queryData.size())).getFieldValues();
    String sql = null;
    
    if (!useListAggPreparedStmt) {
      if (useOldListAgg || useListAggNewImpl) {
        sql = "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC'," +
        		"'IDX_COL1','CDSDBA.XML_IDX_1_1'," +
        		"'IDX_COL1=''" + fields[0] + "'' " +
        		" and IDX_COL2=''" + fields[1] + "'' " +
        		" and IDX_COL3=''" + fields[2] + "'''," +
        		" ',') WITH RESULT PROCESSOR ListAggProcessor";
        callListAggProcedure(conn, sql);
      } else {
        sql = "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC'," +
        "'IDX_COL1','CDSDBA.XML_IDX_1_1'," +
        "'IDX_COL1=? and IDX_COL2=? and IDX_COL3=?'," +
        " '" + fields[0] +"," + fields[1] + "," + fields[2] + "'," +
        " ',') WITH RESULT PROCESSOR ListAggProcessor";
        callListAggProcedure(conn, sql);
      }
    } else {
      if (useOldListAgg || useListAggNewImpl) {
        sql = "{CALL CDSDBA.ListAgg(?,?,?,?,?) WITH RESULT PROCESSOR ListAggProcessor ON TABLE CDSDBA.XML_IDX_1_1 }";
        callPreparedOldListAggProcedure(conn, sql, fields);
      } else {
        sql = "{CALL CDSDBA.ListAgg(?,?,?,?,?,?) WITH RESULT PROCESSOR ListAggProcessor ON TABLE CDSDBA.XML_IDX_1_1 }";
        callPreparedListAggProcedure(conn, sql, fields);
      }
    }
   
  }
  
  protected void callPreparedOldListAggProcedure(Connection conn, String sql, Object[] fields) {
    try {
      CallableStatement stmt = conn.prepareCall(sql);
      /*   
      String listAggCols = "IDX_COL1";
      String tableName = "CDSDBA.XML_IDX_1_1";      
      String delimiter = ",";
      String groupBy = "IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC";
      */
      
      long start = this.querystats.startQuery();
      
      String whereClause = "IDX_COL1='" + fields[0] + "' and IDX_COL2='" +
        fields[1] + "' and IDX_COL3='" + fields[2] + "'";     
      stmt.setString(1, groupBy);      
      stmt.setString(2, listAggCols);      
      stmt.setString(3, tableName);      
      stmt.setString(4, whereClause);      
      stmt.setString(5, delimiter);      
      stmt.execute();

      if (log().fineEnabled()) log().fine(sql + " groupBy: " + groupBy + " listAggcols: " + listAggCols + 
          " tableName: " + tableName + " whereClause: " + whereClause + 
          " delimiter: " + delimiter 
          ) ;

      
      ResultSet rs = stmt.getResultSet();
      List<Struct> list = ResultSetHelper.asList(rs, false);
      this.querystats.endQuery( start );
      if (log().fineEnabled()) log().fine("Result set is " + ResultSetHelper.listToString(list) );

    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void callPreparedListAggProcedure(Connection conn, String sql, Object[] fields) {
    try {
      CallableStatement stmt = conn.prepareCall(sql);
      /*
      String listAggCols = "IDX_COL1";
      String tableName = "CDSDBA.XML_IDX_1_1";      
      String delimiter = ",";
      String groupBy = "IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC";
      */
      
      String whereClause = "IDX_COL1=? and IDX_COL2=? and IDX_COL3=?";   
      String whereParams = fields[0] + "," + fields[1] + "," + fields[2];
      
      long start = this.querystats.startQuery();
      
      stmt.setString(1, groupBy);      
      stmt.setString(2, listAggCols);      
      stmt.setString(3, tableName);      
      stmt.setString(4, whereClause);   
      stmt.setString(5, whereParams);
      stmt.setString(6, delimiter);  

      if (log().fineEnabled()) log().fine(sql + " groupBy: " + groupBy + " listAggcols: " + listAggCols + 
          " tableName: " + tableName + " whereClause: " + whereClause + " whereParams: " + whereParams +
          " delimiter: " + delimiter 
          ) ;

      stmt.execute();
      
      ResultSet rs = stmt.getResultSet();
      List<Struct> list = ResultSetHelper.asList(rs, false);
      this.querystats.endQuery( start );
      if (log().fineEnabled()) log().fine("Result set is " + ResultSetHelper.listToString(list) );

    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void callXML_DOCProcedures(Connection conn) {
    //TODO to be implemented if this is a needed test case
  }
  
  protected void callListAggProcedure(Connection cxn, String sql) {
    try {
      Statement stmt = cxn.createStatement();  
      long start = this.querystats.startQuery();
      //log().info(sql);
      stmt.execute(sql);
      ResultSet rs = stmt.getResultSet();
      if (log().fineEnabled()) log().fine(sql);
      ResultSetHelper.asList(rs, false);
      this.querystats.endQuery( start );
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected Connection getConnection() {
    Connection conn = null;
    try {
      if (SQLTest.isEdge) conn = GFEDBClientManager.getConnection();
      else conn = GFEDBManager.getConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }

  protected void closeConnection(Connection conn) {
    try {
      conn.close();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to release the connection " + TestHelper.getStackTrace(e));
    }
  }

  public static void HydraTask_setupLoner() {
    LonerHelper.connect();

    QueryPerfClient c = new QueryPerfClient();
    c.setupConnection(); 
  }

  public static void HydraTask_setupConnection() {
    QueryPerfClient c = new QueryPerfClient();
    c.setupConnection();
  }

  protected void setupConnection() {
    Connection conn = getConnection();
    myConn.set(conn);
  }

  /**
   *  TASK to register the query performance statistics object.
   */
  public static void openStatisticsTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.openStatistics();
  }
  
  private void openStatistics() {
    this.querystats = getQueryStats();
    if ( this.querystats == null ) {
      log().info( "Opening per-thread query performance statistics" );
      this.querystats = QueryPerfStats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
      log().info( "Opened per-thread query performance statistics" );
    }
    setQueryStats(this.querystats);
  }

  /**
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    QueryPerfClient c = new QueryPerfClient();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }
  
  protected void closeStatistics() {
    MasterController.sleepForMs( 2000 );
    if ( this.querystats != null ) {
      log().info( "Closing per-thread Query performance statistics" );
      this.querystats.close();
      log().info( "Closed per-thread Query performance statistics" );
    }
  }

  //----------------------------------------------------------------------------
  //  Hydra thread locals and their instance field counterparts
  //----------------------------------------------------------------------------

  public QueryPerfStats querystats;
  //public HashMap objectlist;

  private static HydraThreadLocal localquerystats = new HydraThreadLocal();
  //private static HydraThreadLocal localobjectlist = new HydraThreadLocal();

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();

    this.querystats = getQueryStats();
    //this.objectlist = getObjectList();

  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();

    setQueryStats( this.querystats );
    //setObjectList( this.objectlist );
  }

  /**
   *  Gets the per-thread QueryStats wrapper instance.
   */
  protected QueryPerfStats getQueryStats() {
    QueryPerfStats querystats = (QueryPerfStats) localquerystats.get();
    return querystats;
  }
  /**
   *  Sets the per-thread QueryStats wrapper instance.
   */
  protected void setQueryStats(QueryPerfStats querystats ) {
    localquerystats.set( querystats );
  }

  //----------------------------------------------------------------------------
  //  Overridden methods
  //----------------------------------------------------------------------------
  
  protected String nameFor( int name ) {
    switch (name) {
      case QUERIES:      return QUERY_NAME;
      case SELECTQUERIES: return SELECT_QUERY_NAME;
    }
    return super.nameFor(name);
  }
  
  private void selectQuery(Connection conn) {
    int num = (Integer) numOfOp.get();
    ++num;
    numOfOp.set(num);
    
    
    try {
      conn.createStatement().execute("set schema CDSDBA");
      performInsert(conn, num);
      performSelectQuery(conn, num);   
    } catch(Exception e) {     // IllegalState or IllegalArgumentExceptions
      throw new CachePerfException( "Could not get query result", e );
    }
   
  } 
  
  private void performInsert(Connection conn, int num) {
    /*
    ByteArrayInputStream sqlScriptInsertStream = new ByteArrayInputStream(insertOps);
    boolean logOutput = log().fineEnabled();
    SQLHelper.runSameSQLScript(conn, sqlScriptInsertStream, true, logOutput);    
    */   
    
    doInsertIDX_20_Op(conn, num);
    doInsertIDX_25_Op(conn, num);
    doInsertIDX_65_Op(conn, num);
    doInsertIDX_79_Op(conn, num);
    doInsertOpDOC_3(conn, num);
  }
  
  private void performSelectQuery(Connection conn, int num) {   
    int numberOfSelects = selects.length;

    try {
      for (int i=0; i< numberOfSelects; i++) {
               
        long start = this.querystats.startSelectQuery();
        selectQuery(conn, i, num);
        this.querystats.endSelectQuery( start );
      } 
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    
  }
  
  private void getSelect1Data(BigDecimal[] XML_DOC_ID_NBR, BigDecimal[] STRUCTURE_ID_NBR) {
    int index = SQLTest.random.nextInt(queryDataDOC_3.size()-17);
    XML_DOC_ID_NBR[0] = (BigDecimal) queryDataDOC_3.get(index).get("XML_DOC_ID_NBR");
    for (int i=0; i<17; i++) {
      STRUCTURE_ID_NBR[i] = (BigDecimal) queryDataDOC_3.get(index+i).get("STRUCTURE_ID_NBR");
    }
    for (int i=1; i<3; i++) {
      index = SQLTest.random.nextInt(queryDataDOC_3.size()-17);
      XML_DOC_ID_NBR[i] = (BigDecimal) queryDataDOC_3.get(index).get("XML_DOC_ID_NBR");     
    }
  }
  
  private void selectQuery(Connection conn, int whichSelect, int num) throws SQLException {
    String sql = selects[whichSelect];
    ResultSet rs = null;
    List<Struct>list = null;
    PreparedStatement ps = conn.prepareCall(sql);
    //Log.getLogWriter().info(sql);
    if (log().fineEnabled()) log().fine(sql);

    switch (whichSelect) {
    case 0:  
      ps.setString(1, getRandomIDX_COL1(num));
      rs = ps.executeQuery();
      list = ResultSetHelper.asList(rs, false);
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break;
    case 1: 
      rs = getSelectQueryResults(conn, sql);      
      list = ResultSetHelper.asList(rs, false);
      Log.getLogWriter().info("query result size is " + list.size());
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      //if (list.size() < 5) Log.getLogWriter().info("query results is " + ResultSetHelper.listToString(list));
      
      break;

    case 2:
      ps.setString(1, getRandomIDX_COL1(num));
      ps.setBigDecimal(2, getIDX65_COL3QueryBase());
      ps.setBigDecimal(3, getIDX65_COL4QueryBase());
      rs = ps.executeQuery();
      list = ResultSetHelper.asList(rs, false);
      //Log.getLogWriter().info("query results is " + ResultSetHelper.listToString(list));
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break;
      
    case 3:
      ps.setString(1, getRandomIDX_COL1(num));
      ps.setBigDecimal(2, getIDX79_COL2QueryBase());
      ps.setBigDecimal(3, getIDX79_COL3QueryBase());
      rs = ps.executeQuery();
      list = ResultSetHelper.asList(rs, false);
      //Log.getLogWriter().info("query results is " + ResultSetHelper.listToString(list));
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break;
      
    case 4:     
      rs = getSelectQueryResults_1(conn, sql);  
      list = ResultSetHelper.asList(rs, false);
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      //log().info("query results is " + ResultSetHelper.listToString(list));
      break;
      
    case 5:
      rs = getSelectQueryResults_5(conn, sql, num);
      list = ResultSetHelper.asList(rs, false);
      //Log.getLogWriter().info("query result size is " + list.size());
      //if (list.size() < 5) log().info("query results is " + ResultSetHelper.listToString(list));
      //Log.getLogWriter().info("query results is " + ResultSetHelper.listToString(list));      
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break;
    
    case 6:     
      rs = getSelectQueryResults_1(conn, sql);  
      list = ResultSetHelper.asList(rs, false);
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break; 
      
    case 7:
      rs = getSelectQueryResults_5(conn, sql, num);
      list = ResultSetHelper.asList(rs, false);
      //Log.getLogWriter().info("query results is " + ResultSetHelper.listToString(list));  
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break;
      
    case 8:     
      rs = getSelectQueryResults_1(conn, sql);  
      list = ResultSetHelper.asList(rs, false);
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break; 
      
    case 9:
      rs = getSelectQueryResults_5(conn, sql, num);
      list = ResultSetHelper.asList(rs, false);
      //Log.getLogWriter().info("query results is " + ResultSetHelper.listToString(list));   
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break;
      
    case 10:     
      rs = getSelectQueryResults_1(conn, sql);  
      list = ResultSetHelper.asList(rs, false);
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break; 
      
    case 11:   
      rs = getSelectQueryResults_11(conn, sql);  
      list = ResultSetHelper.asList(rs, false);
      if (log().fineEnabled()) log().fine("query results is " + ResultSetHelper.listToString(list));
      break; 
      
    default: 
      ;
    }
  }

  public static byte[] getBytesFromFile(File file) throws IOException {
    InputStream is = new FileInputStream(file);

    // Get the size of the file
    long length = file.length();
    if (length > Integer.MAX_VALUE)
      throw new TestException("input file is too long");

    byte[] bytes = new byte[(int)length];

    // Read in the bytes
    int offset = 0;
    int numRead = 0;
    while (offset < bytes.length
           && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
        offset += numRead;
    }

    // Ensure all the bytes have been read in
    if (offset < bytes.length) {
        throw new IOException("Could not completely read file "+file.getName());
    }

    // Close the input stream and return bytes
    is.close();
    return bytes;
  }
  
  private void doInsertIDX_25_Op(Connection conn, int num) {   
    int IDX_COL1 = getIDX_COL1(num);
    int diff = num * getMyTid();
    BigDecimal col4 = new BigDecimal(IDX_25_COL4).add(new BigDecimal(Integer.toString(diff)));
    String sql = "insert into CDSDBA.xml_idx_25_2 (IDX_COL1, IDX_COL2, IDX_COL3, IDX_COL4, IDX_COL5, XML_DOC_ID_NBR, CREATE_MINT_CD, LAST_UPDATE_SYSTEM_NM, LAST_UPDATE_TMSTP) " +
    		" values (?, 'FX', ?, ? , ?,  ?,  '1', 'APP4004', CURRENT_TIMESTAMP) ";
    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      int n = 1;
      for (int i = 0; i<10; i++) {
        n+=i;
        ps.setString(1, Integer.toString(IDX_COL1));
        ps.setString(2, col3s[SQLTest.random.nextInt(col3s.length)]);
        ps.setBigDecimal(3, col4);
        ps.setString(4, Integer.toString(n));
        ps.setBigDecimal(5, getRandomXML_DOC_ID_NBR(num));
        
        int row = ps.executeUpdate();
        //if (row != 1) throw new TestException("insert does not insert a row in a table without primary key");
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }    
  }
  
  private void doInsertIDX_20_Op(Connection conn, int num) {   
    int IDX_COL1 = getIDX_COL1(num);
    String sql = "insert into CDSDBA.xml_idx_20_1 (IDX_COL1, XML_DOC_ID_NBR, CREATE_MINT_CD, LAST_UPDATE_SYSTEM_NM, LAST_UPDATE_TMSTP) " +
        " values (?, ?, '1', 'APP4004', CURRENT_TIMESTAMP) ";
    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setString(1, Integer.toString(IDX_COL1));
      ps.setBigDecimal(2, getRandomXML_DOC_ID_NBR(num));
      
      int row = ps.executeUpdate();
      //if (row != 1) throw new TestException("insert does not insert a row in a table without primary key");
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
       
  }

  private int getIDX_COL1(int num) {
    return 100000001 + num * numOfWorkers + getMyTid();
  }

  private void doInsertIDX_65_Op(Connection conn, int num) {   
    String sql = "insert into CDSDBA.xml_idx_65_1 (IDX_COL1, IDX_COL2, IDX_COL3, IDX_COL4, IDX_COL5, IDX_COL6, IDX_COL7, IDX_COL8, XML_DOC_ID_NBR, CREATE_MINT_CD, LAST_UPDATE_SYSTEM_NM, LAST_UPDATE_TMSTP) " +
        " values (?, 'FX', ?, ? ,'FX', '13020', NULL, '001', ?, '1', 'APP4004', CURRENT_TIMESTAMP) ";
    BigDecimal XML_DOC_ID_NBR = new BigDecimal("1111111111111111111").add(new BigDecimal(Integer.toString(getMyTid() + numOfWorkers * num)));
    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setString(1, getRandomIDX_COL1(num));
      ps.setBigDecimal(2, getRandomIDX65_COL3());
      ps.setBigDecimal(3, getRandomIDX65_COL4());
      ps.setBigDecimal(4, XML_DOC_ID_NBR);
      
      int row = ps.executeUpdate();
      //if (row != 1) throw new TestException("insert does not insert a row in a table without primary key");
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    } 
  }
  
  private void doInsertIDX_79_Op(Connection conn, int num) {   
    int IDX_COL1 = getIDX_COL1(num);
    String sql = "insert into CDSDBA.xml_idx_79_1 (IDX_COL1, IDX_COL2, IDX_COL3, XML_DOC_ID_NBR, CREATE_MINT_CD, LAST_UPDATE_SYSTEM_NM, LAST_UPDATE_TMSTP)  " +
        " values (?, ?, ?, ?, '1', 'APP4004', CURRENT_TIMESTAMP) ";
    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setString(1, Integer.toString(IDX_COL1));
      ps.setBigDecimal(2, getRandomIDX65_COL3());
      ps.setBigDecimal(3, getRandomIDX65_COL4());
      ps.setBigDecimal(4, getRandomXML_DOC_ID_NBR(num));
      
      int row = ps.executeUpdate();
      
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
       
  }
 
  private void doInsertOpDOC_3(Connection conn, int num) {  
    BigDecimal XML_DOC_ID_NBR = new BigDecimal("1111111111111111111").add(new BigDecimal(Integer.toString(getMyTid() + numOfWorkers * num)));
    String sql = "  insert into CDSDBA.xml_doc_3 (XML_DOC_ID_NBR, STRUCTURE_ID_NBR, CREATE_MINT_CD, MSG_PAYLOAD_QTY, MSG_PAYLOAD1_IMG, MSG_PAYLOAD2_IMG, MSG_PAYLOAD_SIZE_NBR, MSG_PURGE_DT, DELETED_FLG, LAST_UPDATE_SYSTEM_NM, LAST_UPDATE_TMSTP, MSG_MAJOR_VERSION_NBR, MSG_MINOR_VERSION_NBR, OPT_LOCK_TOKEN_NBR, PRESET_DICTIONARY_ID_NBR)  " +
        " values (?, ?, '1', 1, CAST(X'12345678452984560289456029847609487234785012934857109348156034650234560897628900985760289207856027895602785608560786085602857602985760206106110476191087345601456105610478568347562686289765927868972691785634975604562056104762978679451308956205620437861508561034756028475180756917856190348756012876510871789546913485620720476107856479238579385923847934' AS BLOB), " +
        "NULL, 350, NULL, 'N', 'APP4004', CURRENT_TIMESTAMP, 0, 5, 1, 0) ";

    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      int n = 1;
      for (int i=0; i<5; i++) {
        n += i;
        ps.setBigDecimal(1, XML_DOC_ID_NBR);
        ps.setBigDecimal(2, new BigDecimal(Integer.toString(n)));
        
        int row = ps.executeUpdate();
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    
    
  }
  
  private void addDataDOC_3(Connection conn) { 
    int num = 1;
    numOfOp.set(num);
    BigDecimal XML_DOC_ID_NBR = new BigDecimal("1111111111111111111").add(new BigDecimal(Integer.toString(getMyTid())));
    String sql = "  insert into CDSDBA.xml_doc_3 (XML_DOC_ID_NBR, STRUCTURE_ID_NBR, CREATE_MINT_CD, MSG_PAYLOAD_QTY, MSG_PAYLOAD1_IMG, MSG_PAYLOAD2_IMG, MSG_PAYLOAD_SIZE_NBR, MSG_PURGE_DT, DELETED_FLG, LAST_UPDATE_SYSTEM_NM, LAST_UPDATE_TMSTP, MSG_MAJOR_VERSION_NBR, MSG_MINOR_VERSION_NBR, OPT_LOCK_TOKEN_NBR, PRESET_DICTIONARY_ID_NBR)  " +
        " values (?, ?, '1', 1, CAST(X'12345678452984560289456029847609487234785012934857109348156034650234560897628900985760289207856027895602785608560786085602857602985760206106110476191087345601456105610478568347562686289765927868972691785634975604562056104762978679451308956205620437861508561034756028475180756917856190348756012876510871789546913485620720476107856479238579385923847934' AS BLOB), " +
        "NULL, 350, NULL, 'N', 'APP4004', CURRENT_TIMESTAMP, 0, 5, 1, 0) ";

    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      for (int i = 0; i<100; i++) {
        ps.setBigDecimal(1, XML_DOC_ID_NBR);
        ps.setBigDecimal(2, new BigDecimal(Integer.toString(i)));
      
        int row = ps.executeUpdate();
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }    
    
  }
  
  private void addDataIDX_25(Connection conn) { 
    int num = 1;
    int IDX_COL1 = 100000001 + getMyTid();
    int diff = num * getMyTid();
    BigDecimal col4 = new BigDecimal(IDX_25_COL4).add(new BigDecimal(Integer.toString(diff)));
    int IDX_COL5 = SQLTest.random.nextInt(99) + 1;
    String sql = "insert into CDSDBA.xml_idx_25_2 (IDX_COL1, IDX_COL2, IDX_COL3, IDX_COL4, IDX_COL5, XML_DOC_ID_NBR, CREATE_MINT_CD, LAST_UPDATE_SYSTEM_NM, LAST_UPDATE_TMSTP) " +
        " values (?, 'FX', ?, ? , ?,  ?,  '1', 'APP4004', CURRENT_TIMESTAMP) ";
    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      for (int i=1; i<=50; i++) {
        ps.setString(1, Integer.toString(IDX_COL1));
        ps.setString(2, col3s[SQLTest.random.nextInt(col3s.length)]);
        ps.setBigDecimal(3, col4);
        ps.setString(4, Integer.toString(2*i - (SQLTest.random.nextBoolean()? 0: 1)));
        ps.setBigDecimal(5, getRandomXML_DOC_ID_NBR(num));
        
        int row = ps.executeUpdate();
        //if (row != 1) throw new TestException("insert does not insert a row in a table without primary key");
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }    
  }
  
  //return 1 to 99
  private int getSTRUCTURE_ID_NBR() {
    return SQLTest.random.nextInt(99) + 1;
  }

  private ResultSet getSelectQueryResults(Connection conn, String sql) throws SQLException {  
    PreparedStatement ps = conn.prepareStatement(sql);
    BigDecimal XML_DOC_ID_NBR[] = new BigDecimal[3];
    BigDecimal[] STRUCTURE_ID_NBR = new BigDecimal[17];
       
    if (SQLTest.random.nextBoolean()) {
      getSelect1Data(XML_DOC_ID_NBR, STRUCTURE_ID_NBR); 
      ps.setBigDecimal(1, XML_DOC_ID_NBR[0]);
      if (log().fineEnabled()) log().fine("XML_DOC_ID_NBR is " + XML_DOC_ID_NBR[0]);
      for (int i = 2; i<2+STRUCTURE_ID_NBR.length; i++) {
        ps.setBigDecimal(i, STRUCTURE_ID_NBR[i-2]);
        if (log().fineEnabled()) log().fine("STRUCTURE_ID_NBR is " + STRUCTURE_ID_NBR[i-2]);
      }
    } else {
      int diff = SQLTest.random.nextInt(XML_DOC_ID_NBR_base);
      BigDecimal _XML_DOC_ID_NBR = new BigDecimal("1111111111111111111").add(new BigDecimal(Integer.toString(diff)));
      ps.setBigDecimal(1, _XML_DOC_ID_NBR);
      for (int i = 2; i<2+STRUCTURE_ID_NBR.length; i++) {
        ps.setBigDecimal(i, new BigDecimal(Integer.toString(getSTRUCTURE_ID_NBR())));
      }
    } //use newly inserted data
    return ps.executeQuery();
  }
  
  private ResultSet getSelectQueryResults_1(Connection conn, String sql) throws SQLException {  
    PreparedStatement ps = conn.prepareStatement(sql);
    BigDecimal XML_DOC_ID_NBR[] = new BigDecimal[3];
    BigDecimal[] STRUCTURE_ID_NBR = new BigDecimal[17];
       
    if (SQLTest.random.nextBoolean()) {
      getSelect1Data(XML_DOC_ID_NBR, STRUCTURE_ID_NBR); 
      ps.setBigDecimal(1, XML_DOC_ID_NBR[0]);
      if (log().fineEnabled()) log().fine("XML_DOC_ID_NBR is " + XML_DOC_ID_NBR[0]);

      ps.setBigDecimal(2, STRUCTURE_ID_NBR[0]);
      if (log().fineEnabled()) log().fine("STRUCTURE_ID_NBR is " + STRUCTURE_ID_NBR[0]);

    } else {
      int diff = SQLTest.random.nextInt(XML_DOC_ID_NBR_base) * (SQLTest.random.nextBoolean()? 1: -1);
      BigDecimal _XML_DOC_ID_NBR = new BigDecimal("1111111111111111111").add(new BigDecimal(Integer.toString(diff)));
      ps.setBigDecimal(1, _XML_DOC_ID_NBR);
      if (log().fineEnabled()) log().fine("XML_DOC_ID_NBR is " + _XML_DOC_ID_NBR);
      ps.setBigDecimal(2, new BigDecimal(Integer.toString(
          seedSTRUCTURE_ID_NBR[SQLTest.random.nextInt(seedSTRUCTURE_ID_NBR.length)])));
    } //use newly inserted data
    return ps.executeQuery();
  }
  
  private ResultSet getSelectQueryResults_11(Connection conn, String sql) throws SQLException { 
    PreparedStatement ps = conn.prepareStatement(sql);
    BigDecimal XML_DOC_ID_NBR[] = new BigDecimal[3];
    BigDecimal[] STRUCTURE_ID_NBR = new BigDecimal[17];
    int whichOne = 0;
    
    if (SQLTest.random.nextBoolean()) {
      getSelect1Data(XML_DOC_ID_NBR, STRUCTURE_ID_NBR);  
      if (log().fineEnabled())
        log().fine("XML_DOC_ID_NBR are " + XML_DOC_ID_NBR[0] + "," + XML_DOC_ID_NBR[1]          
                     + "," + XML_DOC_ID_NBR[2]);
      whichOne = SQLTest.random.nextInt(STRUCTURE_ID_NBR.length);
      ps.setBigDecimal(1, XML_DOC_ID_NBR[0]);
      ps.setBigDecimal(2, XML_DOC_ID_NBR[1]);
      ps.setBigDecimal(3, XML_DOC_ID_NBR[2]);
      ps.setBigDecimal(4, STRUCTURE_ID_NBR[whichOne]);
      if (log().fineEnabled()) log().fine("STRUCTURE_ID_NBR is " + STRUCTURE_ID_NBR[whichOne]);
    } else {
      for (int i = 1; i<4; i++) { 
        int diff = SQLTest.random.nextInt(XML_DOC_ID_NBR_base) * (SQLTest.random.nextBoolean()? 1: -1);
        ps.setBigDecimal(i, new BigDecimal("1111111111111111111").add(new BigDecimal(Integer.toString(diff))));
      }
      ps.setBigDecimal(4, new BigDecimal(Integer.toString(getSTRUCTURE_ID_NBR())));
    } //use newly inserted data
    return ps.executeQuery();
  }
  
  private ResultSet getSelectQueryResults_5(Connection conn, String sql, int num) throws SQLException {  
    PreparedStatement ps = conn.prepareStatement(sql);
    if (SQLTest.random.nextInt(10) != 1) {
      ps.setString(1, getRandomIDX_COL1(1)); //using added data
    } else {
      ps.setString(1, getRandomIDX_COL1(num));
    }
    ps.setString(2, "FX");
    ps.setString(3, col3s[SQLTest.random.nextInt(col3s.length)]);

    return ps.executeQuery();
  }
  
  private int getMyTid() {
    return RemoteTestModule.getCurrentThread().getThreadId();
  }
  
  private BigDecimal getRandomXML_DOC_ID_NBR(int num) {
    int diff = SQLTest.random.nextInt(num == 0? numOfWorkers : num * numOfWorkers);
    return new BigDecimal("1111111111111111111").add(new BigDecimal(Integer.toString(diff)));
  }
  
  private String getRandomIDX_COL1(int num) {
    int IDX_COL1 = 100000001 + SQLTest.random.nextInt(num == 0? numOfWorkers : num * numOfWorkers);
    return Integer.toString(IDX_COL1);
  }
  
  private BigDecimal getRandomIDX65_COL3() {
    return new BigDecimal(IDX_65_COL3).add(new BigDecimal(Integer.toString(SQLTest.random.nextInt(base))));
  }
  
  private BigDecimal getRandomIDX65_COL4() {
    return new BigDecimal(IDX_65_COL4).add(new BigDecimal(Integer.toString(SQLTest.random.nextInt(base))));
  }
  
  private BigDecimal getIDX65_COL3QueryBase() {
    return new BigDecimal(IDX_65_COL3).add(new BigDecimal(Integer.toString(base/2)));
  }
  
  private BigDecimal getIDX65_COL4QueryBase() {
    return new BigDecimal(IDX_65_COL4).add(new BigDecimal(Integer.toString(base/2)));
  }
  
  private BigDecimal getRandomIDX79_COL2() {
    return new BigDecimal(IDX_79_COL2).add(new BigDecimal(Integer.toString(SQLTest.random.nextInt(base))));
  }
  
  private BigDecimal getRandomIDX79_COL3() {
    return new BigDecimal(IDX_79_COL3).add(new BigDecimal(Integer.toString(SQLTest.random.nextInt(base))));
  }
  
  private BigDecimal getIDX79_COL2QueryBase() {
    return new BigDecimal(IDX_79_COL2).add(new BigDecimal(Integer.toString(base/2)));
  }
  
  private BigDecimal getIDX79_COL3QueryBase() {
    return new BigDecimal(IDX_79_COL3).add(new BigDecimal(Integer.toString(base/2)));
  }

}

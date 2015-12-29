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
package sql.poc.useCase2;

import hydra.Log;
import hydra.TestConfig;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlBridge.SQLBridgeTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public class UseCase2Test extends SQLBridgeTest {
  protected static UseCase2Test useCase2Test;
  public static boolean loadLargeDataFile = true;
  public static boolean useOldListAgg =  TestConfig.tab().booleanAt(SQLPrms.useOldListAgg, false);
  public static boolean useListAggNewImpl =  TestConfig.tab().booleanAt(SQLPrms.useListAggNewImpl, true);
  static {
    if (useOldListAgg && useListAggNewImpl) 
      throw new TestException("could not use both oldListAgg and listAggNewImpl");
  }
  static List<Struct>tableIDX_1 = null;
  static boolean[] queryDataSet = new boolean[1];
  
  static String[] col1words = {
      "Some",
      "Old",
      "Gateway",
      "Cliff",
      "Highway",      
    };
  
  static String[] col3 = {
    "NEWYORK",
    "PORTLAND",
    "ORLANDO",
    "CHICAGO",
  };
  
  public static void HydraTask_runSQLScript() {
    if (useCase2Test == null) {
      useCase2Test = new UseCase2Test();
    }
    useCase2Test.runSQLScript(true);
  }
  
  public static void HydraTask_loadUseCase2Data() {
    useCase2Test.loadUseCase2Data();
  }
  
  protected void loadUseCase2Data() {
    Connection conn = getGFEConnection();
    String schema = "cdsdba";
    String table = "xml_doc_1";
    loadUseCase2DataXML_DOC(conn, schema, table);
    table = "xml_idx_1_1";
    loadUseCase2DataXML_IDX_1_1(conn, schema, table);
    commit(conn);
    closeGFEConnection(conn);    
  }
  
  protected void loadUseCase2DataXML_DOC(Connection conn, String schema, 
      String table) {
    String jtests = System.getProperty( "JTESTS" );
    
    if (loadLargeDataFile) {
      String filePath = jtests + "/sql/poc/useCase2/data/150XML_DOC_1.dat";
      Log.getLogWriter().info("filePath is " + filePath);
      loadUseCase2Data(conn, schema, table, filePath);
    } else {
      //no longer needed.
      /*
      String base = jtests + "/sql/poc/useCase2/data/parta" ;
      for (int i = 'a'; i<='o'; ++i) {
        String filePath = base + (char)i;
        Log.getLogWriter().info("filePath is " + filePath);
        loadUseCase2Data(conn, schema, table, filePath);
      }
      */
    }
  }
  
  public static void HydraTask_loadUseCase2Data_DOC_3() {
    //TODO get correct Hexadecimal data in the file
    useCase2Test.loadUseCase2Data_DOC_3();
  }
  
  protected void loadUseCase2Data_DOC_3() {
    Connection conn = getGFEConnection();
    String schema = "cdsdba";
    String table = "xml_doc_3";
    String dir = "/export/armenia2/users/eshu/"; 
    //temporarily use the file location here, until a better place is available
    String filePath =  dir + "XML_DOC_3.dat";
    loadUseCase2Data(conn, schema, table, filePath);
    commit(conn);
    closeGFEConnection(conn);    
  }
  
  protected void loadUseCase2DataXML_IDX_1_1(Connection conn, String schema, 
      String table) {
    String jtests = System.getProperty( "JTESTS" );
    
    String filePath = jtests + "/sql/poc/useCase2/data/XML_IDX_1_1.dat";
    Log.getLogWriter().info("filePath is " + filePath);
    loadUseCase2Data(conn, schema, table, filePath);
  }
  
  protected void loadUseCase2Data(Connection conn, String schema, 
      String table, String filePath) {
    String importTableEx = "CALL SYSCS_UTIL.IMPORT_TABLE_EX (?, ?, ?, '|', "
      + "null, null, 0, 0, 6, 0, ?, null)";
        
    String importOra = "sql.poc.useCase2.perf.ImportOraSp";
    Log.getLogWriter().info(importOra);
    Log.getLogWriter().info(importTableEx + " with table " + table);
    try {
      CallableStatement cs = conn.prepareCall(importTableEx);
      cs.setString(1, schema);
      cs.setString(2, table);
      cs.setString(3, filePath);
      cs.setString(4, importOra);
      cs.execute();
    } catch (SQLException se) {
      //se.printStackTrace();
       SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_installJar() {
    useCase2Test.installJar();
  }
  
  protected void installJar() {
    //to be implemented
    
  }
  
  public static void HydraTask_createProcessorAlias() {
    useCase2Test.createProcessorAlias();
  }
  
  protected void createProcessorAlias() {
    Connection gConn = getGFEConnection();
    try {
      //String sql = "create alias trade.customProcessor for 'sql.sqlDAP.CustomProcessor'";
      
      String sql = null;
      if (useOldListAgg) 
        sql = "CREATE ALIAS ListAggProcessor FOR 'sql.poc.useCase2.oldListAgg.LISTAGGPROCESSOR'";
      else if (useListAggNewImpl)
        sql = "CREATE ALIAS ListAggProcessor FOR 'sql.poc.useCase2.listAggNewImpl.LISTAGGPROCESSOR'";
      else
        sql = "CREATE ALIAS ListAggProcessor FOR 'sql.poc.useCase2.LISTAGGPROCESSOR'";
           
      gConn.createStatement().execute(sql);
      commit(gConn);
      Log.getLogWriter().info("executed the statement: " + sql );
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_createListAggProcedure() {
    if (useCase2Test == null) {
      useCase2Test = new UseCase2Test();
    }
    if (useOldListAgg) useCase2Test.createOldListAggProcedure(); 
    else if (useListAggNewImpl) useCase2Test.createListAggNewImplProcedure();
    else useCase2Test.createListAggProcedure();
  }
  
  protected void createListAggProcedure() {   
    //stmt.execute("DROP PROCEDURE ListAgg");
    // stmt.execute("DROP ALIAS  ListAggProcessor");
    //stmt.execute("DROP TABLE XML_IDX_1_1");
    /*
    stmt.execute("CREATE PROCEDURE CDSDBA.ListAgg(IN groupBy VARCHAR(256), "
        + "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), "
        + "IN whereClause VARCHAR(256), IN delimiter VARCHAR(10)) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA "
        + "DYNAMIC RESULT SETS 1 EXTERNAL NAME 'ListAggProcedure.ListAgg';");
        */
    //String aliasString = "CREATE ALIAS ListAggProcessor FOR '"
    //    + LISTAGGPROCESSOR.class.getName() + "'";
    //stmt.execute(aliasString);
    String sql = "CREATE PROCEDURE CDSDBA.ListAgg(IN groupBy VARCHAR(256), " +
    "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), " +
    "IN whereClause VARCHAR(256), IN whereParams VARCHAR(256), " +
    "IN delimiter VARCHAR(10)) LANGUAGE JAVA PARAMETER STYLE " +
    "JAVA READS SQL DATA DYNAMIC RESULT SETS 1 EXTERNAL NAME " +
    "'sql.poc.useCase2.ListAggProcedure.ListAgg'";
    createListAggProcedure(sql);   
  }
  
  protected void createOldListAggProcedure() {

    String sql = "CREATE PROCEDURE CDSDBA.ListAgg(IN groupBy VARCHAR(256), "
        + "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), "
        + "IN whereClause VARCHAR(256), IN delimiter VARCHAR(10)) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA "
        + "DYNAMIC RESULT SETS 1 EXTERNAL NAME " 
        +	"'sql.poc.useCase2.oldListAgg.ListAggProcedure.ListAgg'";

    createListAggProcedure(sql);   
  }
  
  protected void createListAggNewImplProcedure() {

    String sql = "CREATE PROCEDURE CDSDBA.ListAgg(IN groupBy VARCHAR(256), "
        + "IN ListAggCols VARCHAR(256), IN tableName VARCHAR(128), "
        + "IN whereClause VARCHAR(256), IN delimiter VARCHAR(10)) "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA READS SQL DATA "
        + "DYNAMIC RESULT SETS 1 EXTERNAL NAME " 
        + "'sql.poc.useCase2.listAggNewImpl.ListAggProcedure.ListAgg'";

    createListAggProcedure(sql);   
  }
  
  protected void createListAggProcedure(String sql) {
    try {
      Connection cxn = getGFEConnection();
      Statement stmt = cxn.createStatement();
      Log.getLogWriter().info(sql);
      stmt.execute(sql);
      commit(cxn);
      closeGFEConnection(cxn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_callProcedures() {
    if (useCase2Test == null) {
      useCase2Test = new UseCase2Test();
    }
    if (useOldListAgg || useListAggNewImpl) useCase2Test.callOldListAggProceduresAPI();
    else useCase2Test.callProcedures();
    //useCase2Test.callProcedureIDX();
  }
  
  protected void callProcedures() {
    String[] procedures = {
        "CALL CDSDBA.ListAgg('structure_id_nbr','create_mint_cd','CDSDBA.XML_DOC_1','MSG_PAYLOAD_QTY=5',NULL,',') WITH RESULT PROCESSOR ListAggProcessor ON TABLE CDSDBA.XML_DOC_1",
        "CALL CDSDBA.ListAgg('structure_id_nbr DESC','create_mint_cd','CDSDBA.XML_DOC_1','MSG_PAYLOAD_QTY=?','5',',') WITH RESULT PROCESSOR ListAggProcessor ON TABLE CDSDBA.XML_DOC_1",
        "CALL CDSDBA.ListAgg('structure_id_nbr','MSG_PAYLOAD_QTY','CDSDBA.XML_DOC_1','create_mint_cd=''x''',NULL,',') WITH RESULT PROCESSOR ListAggProcessor ON TABLE CDSDBA.XML_DOC_1",
        "CALL CDSDBA.ListAgg('structure_id_nbr,create_mint_cd','MSG_PAYLOAD_QTY','CDSDBA.XML_DOC_1','',NULL,',') WITH RESULT PROCESSOR ListAggProcessor ON TABLE CDSDBA.XML_DOC_1",          
    };
    
    int whichOne = random.nextInt(procedures.length);
    String sql = random.nextBoolean() ? procedures[whichOne]: "CALL CDSDBA.ListAgg('IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC','IDX_COL1','CDSDBA.XML_IDX_1_1','IDX_COL1=''211Some'' and IDX_COL2=''809012'' and IDX_COL3=''NEWYORK''', NULL, ',') WITH RESULT PROCESSOR ListAggProcessor";
    callProcedures(sql);
  }
  
  protected void callProcedures(String sql) {
    try {
      Connection cxn = getGFEConnection();
      Statement stmt = null;
      CallableStatement cs = null;
      boolean prepareCall = random.nextBoolean();
      if (!prepareCall)
        stmt = cxn.createStatement();
      else 
        cs = cxn.prepareCall(sql);
      
      for (int i=0; i<200; i++) {
        if (!prepareCall)
          stmt.execute(sql);
        else 
          cs.execute(); //repeat the same callablestatement
      }
      Log.getLogWriter().info(sql);
      commit(cxn);
      closeGFEConnection(cxn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }

  protected void callOldListAggProceduresAPI() {
    callOldListAggProcedures(random.nextBoolean());
  }
  
  protected void callOldListAggProcedures(boolean isParameterized) {
    String sql = null;
    Object[] fields = tableIDX_1.get(SQLTest.random.nextInt(tableIDX_1.size())).getFieldValues();
    String listAggCols = "IDX_COL1";
    String tableName = "CDSDBA.XML_IDX_1_1";      
    String delimiter = ",";
    String groupBy = "IDX_COL1 ASC,IDX_COL2 ASC,IDX_COL3 ASC,IDX_COL4 ASC,IDX_COL5 ASC,XML_DOC_ID_NBR ASC";
    String whereClause = null;
    if (isParameterized) 
      whereClause = "IDX_COL1='" + fields[0] + "' and IDX_COL2='" +
      fields[1] + "' and IDX_COL3='" + fields[2] + "'";  
    else 
      whereClause = "IDX_COL1=''" + fields[0] + "'' and IDX_COL2=''" +
      fields[1] + "'' and IDX_COL3=''" + fields[2] + "''"; 
    
    ResultSet rs = null;
    
    if (isParameterized)
      sql = "CALL CDSDBA.ListAgg(?,?,?,?,?) WITH RESULT PROCESSOR ListAggProcessor ON TABLE " + tableName;
    else
      sql = "CALL CDSDBA.ListAgg('" + groupBy + "','" + listAggCols + "','" +
        tableName + "','" + whereClause + "','" + delimiter + "') WITH RESULT PROCESSOR ListAggProcessor ON TABLE " + tableName;
    try {
      Connection cxn = getGFEConnection();
      CallableStatement stmt = null;
      if (isParameterized) {
        stmt= cxn.prepareCall(sql);     
      
        stmt.setString(1, groupBy);      
        stmt.setString(2, listAggCols);      
        stmt.setString(3, tableName);      
        stmt.setString(4, whereClause);      
        stmt.setString(5, delimiter);              
        Log.getLogWriter().info(sql + " with whereClause: " + whereClause);
        stmt.execute();
        rs = stmt.getResultSet();
        List<Struct> rsList = ResultSetHelper.asList(rs, false);
        Log.getLogWriter().info("Result set is " + ResultSetHelper.listToString(rsList));
        checkListAggResult(rsList, fields, whereClause);
      } else {
        stmt= cxn.prepareCall(sql);     
        Log.getLogWriter().info(sql);
        
        for (int i=0; i<200; i++) {
          stmt.execute();
          rs = stmt.getResultSet();
        }        
        List<Struct> rsList = ResultSetHelper.asList(rs, false);
        Log.getLogWriter().info("Result set is " + ResultSetHelper.listToString(rsList));
        checkListAggResult(rsList, fields, whereClause);
      }
  
      commit(cxn);
      closeGFEConnection(cxn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void checkListAggResult(List<Struct> rsList, Object[]fields, String whereClause) {
    for (Struct row: rsList) {
      Object[] rowFields = row.getFieldValues();
      for (int i=0; i<3; i++) {
        if (!fields[i].toString().equals(rowFields[i].toString()))
          throw new TestException ("ListAgg returns wrong results " +
              " whereClause is " + whereClause + " but resultset is " + 
              ResultSetHelper.listToString(rsList));
      }            
    }
  }
  
  public static void HydraTask_createIndex() {
    useCase2Test.createIndex();
  }
  
  
  protected void createIndex() {
    if (createIndex) {
      Connection cxn = getGFEConnection();
      createIndex(cxn);
      commit(cxn);
      closeGFEConnection(cxn);
    }
  }
  
  protected void createIndex(Connection conn) {
    String sql = "CREATE INDEX CDSDBA.IDX_1_1_COLIDX ON" +
        " CDSDBA.XML_IDX_1_1 (IDX_COL1, IDX_COL2, IDX_COL3)";
    try {
      Statement stmt = conn.createStatement();
      Log.getLogWriter().info(sql);
      stmt.executeUpdate(sql);
      SQLWarning warning = stmt.getWarnings();
      if (warning != null) {
        SQLHelper.printSQLWarning(warning);
      } 
    }catch (SQLException se) {
        SQLHelper.handleSQLException(se);
    }
    
  }
  
  public static void HydraTask_generateQueryData() {
    if (useCase2Test == null) {
      useCase2Test = new UseCase2Test();
    }
    useCase2Test.generateQueryData();
  }
  
  protected void generateQueryData() {
    synchronized (queryDataSet) {
      if (!queryDataSet[0]) {
        if (tableIDX_1 == null) {
          Connection conn = getGFEConnection();
          String sql = "select IDX_COL1, IDX_COL2, IDX_COL3 from CDSDBA.XML_IDX_1_1 "; 
          try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
            tableIDX_1 = ResultSetHelper.asList(rs, false);
          } catch (SQLException se) {
            SQLHelper.handleSQLException(se);
          }
        }
        else {
          throw new TestException ("query data is already set but queryDataSet flag is not set");
        }
        queryDataSet[0] = true;
      } else {
        Log.getLogWriter().info("queryData is set");
      }
    }
  }
  
}



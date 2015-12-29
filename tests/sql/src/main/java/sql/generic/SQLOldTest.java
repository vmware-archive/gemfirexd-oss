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
package sql.generic;

import hydra.Log;
import hydra.MasterController;
import hydra.TestConfig;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Vector;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.datagen.DataGeneratorBB;
import sql.datagen.DataGeneratorHelper;
import sql.generic.ddl.Executor;
import sql.generic.ddl.SchemaInfo;
import sql.generic.ddl.Functions.FunctionTest;
import sql.generic.ddl.alter.GenericAlterDDL;
import sql.generic.ddl.create.CreateTableTest;
import sql.generic.ddl.create.DDLStmtFactory;
import sql.generic.ddl.create.DDLStmtIF;
import sql.generic.ddl.procedures.GeneralProcedure;
import sql.generic.dmlstatements.GenericDML;
import sql.sqlutil.ResultSetHelper;
import sql.view.ViewPrms;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.LogWriter;

/**
 *  
 * @author Rahul Diyewar
 */

public class SQLOldTest extends SQLTest{
  public LogWriter log = Log.getLogWriter();
  public static final Random random = new Random(SQLPrms.getRandSeed());
  
  protected static DDLStmtFactory ddlFactory = new DDLStmtFactory();
  protected static int[] ddls = SQLGenericPrms.getDDLs();

  public String[] tableNames;
  public CreateTableTest createTable;
  public String mapper;
  public static final String FuncProcMap = "FuncProcMap";  

  public SQLOldTest(){
    tableNames = SQLPrms.getTableNames();
    createTable = new CreateTableTest(this);
    mapper = getMapperFileAbsolutePath();
  }
  
  @Override
  public void initialize() {
    super.initialize();
  }
  
  @Override
  public void initForServerGroup() {
    super.initForServerGroup();
  }

  @Override
  public String getSGForNode() {
    return super.getSGForNode();
  }
  
  @Override
  public void createDiskStores(){
    super.createDiskStores();
  }
  
  @Override
  public void createDBSynchronizer() {
    super.createDBSynchronizer();
  }
  
  @Override
  public void startDBSynchronizer() {
    super.startDBSynchronizer();
  }
  
  @Override
  public void putLastKeyDBSynchronizer() {
    super.putLastKeyDBSynchronizer();
  }
  
  @Override
  public void verifyResultSetsDBSynchronizer() {
    super.verifyResultSetsDBSynchronizer();
  }
  
  @Override
  public void shutDownAllFabricServers() {
    super.shutDownAllFabricServers();
  }
  
  @Override
  public Connection getHdfsQueryConnection() {
    return super.getHdfsQueryConnection();
  }
  
  @Override
  public void populateTables() {
    Connection dConn = hasDerbyServer ? getDiscConnection() : null;
    Connection gConn = getGFEConnection();
    populateTables(dConn, gConn);
    
    if (dConn !=null) closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public void doDMLOp(){
    if (networkPartitionDetectionEnabled) {
      doDMLOp_HandlePartition();
    } else {
      Connection dConn =null;
      if (hasDerbyServer) {
        dConn = getDiscConnection();
      }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
      Connection gConn = getHdfsQueryConnection();
      if(useGenericSQL){
        sqlGen.doDMLOp(dConn, gConn); 
      }else{
        doDMLOp(dConn, gConn);   
      }          
      if (dConn!=null) {
        closeDiscConnection(dConn);
        //Log.getLogWriter().info("closed the disc connection");
      }
      closeGFEConnection(gConn);
    }
    Log.getLogWriter().info("done dmlOp");
  }
  
  public void verifyResultSets(){
    super.verifyResultSets();
  }
  
  private String getMapperFileAbsolutePath() {
    String mPath = System.getProperty("JTESTS") + "/"
        + TestConfig.tab().stringAt(SQLGenericPrms.mapperFile, null);
    return mPath;
  }
  
  public void createGFESchemas() {
    ddlThread = getMyTid();
    Connection conn = getGFEConnection();  
    Log.getLogWriter().info("testServerGroupsInheritence is set to " + testServerGroupsInheritence);
    Log.getLogWriter().info("creating schemas in gfe.");
    if (!testServerGroupsInheritence) {
      String[] schemas = SQLPrms.getSchemas();
      createSchemas(conn, schemas);
      for(String s: schemas){
        StringTokenizer tokens = new StringTokenizer(s); 
        tokens.nextToken(); //create
        tokens.nextToken(); //schema
        String schemaName = tokens.nextToken();
        SchemaInfo schemaInfo = new SchemaInfo (schemaName);
        GenericBBHelper.putSchemaInfo(schemaInfo);
        Log.getLogWriter().info("Created schema info " + schemaInfo);
      }
    }
    else { 
      //with server group
      String[] schemas = SQLPrms.getGFESchemas(); 
      createSchemas(conn, schemas);
      for(String s: schemas){
         StringTokenizer tokens = new StringTokenizer(s); 
         tokens.nextToken(); //create
         tokens.nextToken(); //schema
         String schemaName = tokens.nextToken();
         SchemaInfo schemaInfo = new SchemaInfo (schemaName);
         
         if(s.indexOf("(") > 0){
           String serverGroup = s.substring(s.indexOf("(")+1, s.indexOf(")"));
           schemaInfo.setServerGroup(serverGroup);
         }
         GenericBBHelper.putSchemaInfo(schemaInfo);
         Log.getLogWriter().info("Created schema info " + schemaInfo);
       }
    }
    Log.getLogWriter().info("done creating schemas in gfe.");
    closeGFEConnection(conn);
  }
  
  public void createGFETables() {
    Connection conn = getGFEConnection();
    if (random.nextBoolean()){
      //drop table before creating it -- impact for ddl replay
      Log.getLogWriter().info("dropping tables in gfe.");
      createTable.dropTables(conn);
      Log.getLogWriter().info("done dropping tables in gfe");
    }    
    executeListener("CLOSE", "PRINT_CONN");    
    Log.getLogWriter().info("creating tables in gfe.");
    createTable.createGFETables(conn);
    Log.getLogWriter().info("done creating tables in gfe.");
    
    // this will init DataGenerator, create CSVs for populate and create rows for inserts    
    int[] rowCounts = SQLPrms.getInitialRowCountToPopulateTable();
    DataGeneratorHelper.initDataGenerator(mapper, tableNames, rowCounts, getGFEConnection());
    
    closeGFEConnection(conn);
  }
  
  public void populateTables(Connection dConn, Connection gConn){
    // populate 1000 rows for each table  
//    new DataGeneratorClient().parseMapper(mapper, datagenConn);
    
    //populate gfxd tables from CSVs
    int totalThreads = SqlUtilityHelper.totalTaskThreads();
    int ttgid = SqlUtilityHelper.ttgid();

    for(int i=0; i < tableNames.length ; i++){
//      if (i % totalThreads == ttgid){                // todo - need to handle fk dependencies,        
      if ((ttgid % totalThreads == totalThreads - 1) && SqlUtilityHelper.getRowsInTable(tableNames[i], gConn) <= 0  ) {       // thus make is single threaded and should not import if data is already in table        
        populateTables( tableNames[i], dConn, gConn);          
      }
    }
  }
  
  private void populateTables(final String fullTableName, final Connection dConn , final Connection gConn) {
    String[] names = fullTableName.trim().split("\\."); 
    String schemaName = names[0];
    String tableName = names[1];
    String csvFilePath = DataGeneratorBB.getCSVFileName(fullTableName);    

    importTablesToGfxd(gConn, schemaName, tableName,csvFilePath);
    
    if(hasDerbyServer && dConn != null){
      importTablesToDerby(dConn,schemaName,tableName,csvFilePath);
    }
  }
  
  
  protected void importTablesToGfxd(Connection conn, String schema, 
      String table , String csvPath ){
    
    Log.getLogWriter().info("Gfxd - Data Population Started for " + schema + "." + table + " using " + csvPath );
    String delimitor = ","; 
    String procedure = "sql.datagen.ImportOraDG";
    String importTable = "CALL SYSCS_UTIL.IMPORT_DATA_EX(?, ?, null,null, ?, ?, NULL, NULL, 0, 0, 10, 0, ?, null)";    
    try {
      CallableStatement cs = conn.prepareCall(importTable);
      cs.setString(1, schema.toUpperCase());
      cs.setString(2, table.toUpperCase());
      cs.setString(3, csvPath);
      cs.setString(4, delimitor);
      cs.setString(5, procedure);
      cs.execute();           
      } catch (SQLException se) {
       SQLHelper.handleSQLException(se);
    }
    
    Log.getLogWriter().info("Gfxd - Data Population Completed for " + schema + "." + table );
    
  }
  protected void importTablesToDerby(Connection conn, String schema, 
      String table , String csvPath) {
    
    Log.getLogWriter().info("Derby - Data Population Started for " + schema + "." + table + " using " + csvPath );
    String delimitor = ","; 
    String importTable = "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (?, ?, ?, ?, null, null, 0)";   
    try {
      CallableStatement cs = conn.prepareCall(importTable);
      cs.setString(1, schema.toUpperCase());
      cs.setString(2, table.toUpperCase());
      cs.setString(3, csvPath);
      cs.setString(4, delimitor);
      cs.execute();
    } catch (SQLException se) {
       SQLHelper.handleSQLException(se);
    }
    
    Log.getLogWriter().info("Derby - Data Population Completed for " + schema + "." + table );
  }
  
  public  void doDMLOp(Connection dConn, Connection gConn) {    
    try{     
    GenericDML dmlOps = new GenericDML(SQLGenericPrms.getAnyDMLGroup() , dConn , gConn);
    dmlOps.execute();
    } catch ( Exception e ){
      throw new TestException( TestHelper.getStackTrace(e));
    }
  }
  
  
  public void alterTable() {
    GenericAlterDDL genericAlterDDL;
    if(hasDerbyServer){
      genericAlterDDL = new GenericAlterDDL(SQLGenericPrms.getAnyAlterDDL(),getDiscConnection(),getGFEConnection());
    }
    else{
      genericAlterDDL = new GenericAlterDDL(SQLGenericPrms.getAnyAlterDDL(),getGFEConnection());
    } 
    genericAlterDDL.executeDDL();
  }
  
  public void createProcedures(boolean client) {
    Log.getLogWriter().info(
        "performing create procedure Op, myTid is " + getMyTid());
    Connection dConn = null, gConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    } // when not test uniqueKeys and not in serial execution, only connection
      // to gfe is provided.

    if (client == true)
      gConn = getGFXDClientConnection();
    else
      gConn = getGFEConnection();
    createProcedures(dConn, gConn);

    if (dConn != null) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  protected void createProcedures(Connection dConn, Connection gConn) {
    ArrayList<String> procedures = SQLGenericPrms.getProcedureNames();
    String prefix = "sql.generic.ddl.procedures.";
    String procedureName = "";
    for (String procName : procedures) {
      try {
        procedureName = prefix + procName.trim();
        Class proc = Class.forName(procedureName);
        GeneralProcedure procedure = (GeneralProcedure)proc.newInstance();
        procedure.setExecutor(new Executor(dConn, gConn));
        procedure.createProcedures();
      } catch (ClassNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalAccessException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (InstantiationException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }

  public void createFunctions() {
    boolean client=false;
    Log.getLogWriter().info("call procedures, myTid is " + getMyTid());
    Connection dConn = null, gConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }
    if (client == true)
      gConn = getGFXDClientConnection();
    else
      gConn = getGFEConnection();

    createFunctions(dConn, gConn);

    if (hasDerbyServer) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }

  protected void createFunctions(Connection dConn, Connection gConn) {
    ArrayList<String> functions = SQLGenericPrms.getFunctionNames();
    String prefix = "sql.generic.ddl.Functions.";
    for (String functionName : functions) {
      try {
        functionName = prefix + functionName.trim();
        Class function = Class.forName(functionName);
        FunctionTest currentFunction = (FunctionTest)function.newInstance();
        currentFunction.setExecutor(new Executor(dConn, gConn));
        currentFunction.createDDL();
      } catch (ClassNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalAccessException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (InstantiationException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }

  // get results set using CallableStatmenet
  public void callProcedures() {
    boolean client=false;
    Log.getLogWriter().info("call procedures, myTid is " + getMyTid());
    Connection dConn = null, gConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }
    if (client == true)
      gConn = getGFXDClientConnection();
    else
      gConn = getGFEConnection();

    callProcedures(dConn, gConn);

    if (hasDerbyServer) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }

  public void callProcedures(Connection dConn, Connection gConn) {
    String prefix = "sql.generic.ddl.procedures.";
    String procedureName = SQLGenericPrms.getAnyProcedure().trim();
    int loc = SQLGenericPrms.getProcedureNames().indexOf(procedureName);
    try {
      Class proc = Class.forName(prefix + procedureName);
      GeneralProcedure procedure = (GeneralProcedure)proc.newInstance();
      procedure.setExecutor(new Executor(dConn, gConn), loc);
      procedure.callProcedure();
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  protected void doDDLOp() {
    // perform the opeartions
    Connection dConn = null, gConn = null;
    if (hasDerbyServer) {
      dConn = getDiscConnection();
    }
    
     gConn = getGFEConnection();
    
    int ddl = ddls[random.nextInt(ddls.length)];
    DDLStmtIF ddlStmt = ddlFactory.createDDLStmt(ddl); // dmlStmt of a table

    ddlStmt.setExecutor(new Executor(dConn, gConn));

    ddlStmt.doDDLOp();

    commit(dConn);
    commit(gConn);

  }

  public void createIndex() {
    Connection gConn = getGFEConnection();
    createIndex(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }

  protected void createIndex(Connection gConn) {
    // createIndex will be independent
    DDLStmtIF ddlStmt = ddlFactory.createDDLStmt(DDLStmtFactory.INDEX); 
    ddlStmt.setExecutor(new Executor(null, gConn));
    ddlStmt.doDDLOp();
  }


  public void cycleStoreVms() {
    if (SQLGenericPrms.isHA()){
      super.cycleStoreVms();
    }else{
      int sleepMS = 10000;      
      MasterController.sleepForMs(sleepMS);
    }
  }
  

  
  public void createViews() {
    String viewDDLPath = ViewPrms.getViewDDLFilePath();
    if (hasDerbyServer) {
      Connection conn = getDiscConnection();
      Log.getLogWriter().info(
          "creating views on disc using sql script: " + viewDDLPath);
      SQLHelper.runDerbySQLScript(conn, viewDDLPath, true);
      Log.getLogWriter().info("done creating views on disc.");
      closeDiscConnection(conn);
    }

    // Connection conn = gfxdclient ? getGFXDClientTxConnection() :
    // getGFEConnection();

    Connection conn = getGFEConnection();
    Log.getLogWriter().info(
        "creating views in gfxd using sql script: " + viewDDLPath);
    SQLHelper.runSQLScript(conn, viewDDLPath, true);
    Log.getLogWriter().info("done creating views in gfxd.");
    closeGFEConnection(conn);
  }
  
  public void populateViewTables() {
    String viewDataPath = ViewPrms.getViewDataFilePath();
    if (hasDerbyServer) {
      Connection conn = getDiscConnection();
      Log.getLogWriter().info("Populating view base tables in disc using sql script: " + viewDataPath);
      SQLHelper.runDerbySQLScript(conn, viewDataPath, true);
      Log.getLogWriter().info("done populating view base tables in disc.");
      closeDiscConnection(conn);      
    }
    Connection conn = getGFEConnection();
    Log.getLogWriter().info("Populating view base tables in gfxd using sql script: " + viewDataPath);
    SQLHelper.runSQLScript(conn, viewDataPath, true);
    Log.getLogWriter().info("done populating view base tables in gfxd.");
    closeGFEConnection(conn);  
  }
  
  public void queryViews() {
    Vector<String> queryVec = ViewPrms.getQueryViewsStatements();
    String viewQuery = queryVec.get(random.nextInt(queryVec.size()));
    Connection dConn = getDiscConnection();
    Connection gConn = getGFEConnection();
    queryResultSets(dConn, gConn, viewQuery, false);
    closeDiscConnection(dConn); 
    closeGFEConnection(gConn);
  }
  
  public void verifyViews() {
    Vector<String> queryVec = ViewPrms.getQueryViewsStatements();
    Connection dConn = getDiscConnection();
    Connection gConn = getGFEConnection(); 
    for (int i = 0; i < queryVec.size(); i++) {
      String viewQuery = queryVec.get(i);      
      queryResultSets(dConn, gConn, viewQuery, true);
    }
    closeDiscConnection(dConn); 
    closeGFEConnection(gConn);
  }
  
  protected void queryResultSets(Connection dConn, Connection gConn, String queryStr, boolean verify) {
    try {
      Log.getLogWriter().info("Query view: " + queryStr);
      ResultSet dRS = dConn.createStatement().executeQuery(queryStr);
      ResultSet gRS = gConn.createStatement().executeQuery(queryStr);
      if (verify) {
        ResultSetHelper.compareResultSets(dRS, gRS);
      }
      commit(dConn);
      commit(gConn);
    } catch (SQLException se) {
      if(se.getSQLState().equals("0A000")){
        Log.getLogWriter().info("Ignoring " + queryStr + " as got " + se.getMessage());
      }else{
        SQLHelper.handleSQLException(se);  
      }
    }  
  }
}

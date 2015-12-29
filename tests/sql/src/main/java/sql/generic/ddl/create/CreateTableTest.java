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
package sql.generic.ddl.create;

import hydra.HydraVector;
import hydra.Log;
import hydra.TestConfig;
import hydra.gemfirexd.FabricServerHelper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.generic.GenericBBHelper;
import sql.generic.SQLGenericPrms;
import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.ConstraintInfoHolder;
import sql.generic.ddl.Executor;
import sql.generic.ddl.PKConstraint;
import sql.generic.ddl.SchemaInfo;
import sql.generic.ddl.TableInfo;
import sql.generic.ddl.TableInfoGenerator;
import sql.generic.ddl.UniqueConstraint;
import sql.wan.SQLWanBB;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.LogWriter;

public class CreateTableTest {
  protected Random random = SQLOldTest.random;
  private SQLOldTest sqlGen;
  LogWriter log;
  
  PartitionClauseGenerator partClause;
  List<TableInfo> tableInfoList = new ArrayList<TableInfo>();
  
  public CreateTableTest(SQLOldTest gen){
    this.sqlGen = gen;
    this.log = gen.log;
    partClause = new PartitionClauseGenerator(random, log);
  }
  
  /**
   * To create tables for gfe tables
   * @param conn 
   */
  public void createGFETables(Connection conn) {

    populateBBWithTableData();

    log.info("Starting create GFE Tables");
    getTableInfosFromBB();
    setTablePartitionClause();
    setServerGroupClause();
    // todo - gateway sender
    setAsyncEventListnerClause();
    setEvictionClause();
    setPersistentClause();
    setHdfsClause();
    setOffHeapClause();
    setEnableConcurrecyCheckClause();
    // expiration 
    setNumPrForRecovery();
    updateTableInfosToBB();

    Executor executor = new Executor(conn);
    int tableCount = tableInfoList.size();
    for (int i = 0; i < tableCount; i++) {
      TableInfo table = tableInfoList.get(i);
      sqlGen.executeListener("CLOSE", "PRINT_CONN");
      String tableDDL = table.getGfxdDDL();
      Log.getLogWriter().info("About to create table " + tableDDL);
      try {
        executor.execute(tableDDL);
      } catch (SQLException se) {
        if (SQLOldTest.multiThreadsCreateTables
            && se.getSQLState().equalsIgnoreCase("X0Y32"))
          Log.getLogWriter().info(
              "Got the expected exception, continuing tests");
        else if (se.getSQLState().equalsIgnoreCase("X0Y99")) {
          SQLHelper.printSQLException(se); // for expiration
          Log.getLogWriter().info(
              "Got the expected exception X0Y99, continuing tests");
        } else {
          SQLHelper.printSQLException(se);
          throw new TestException("Not able to create tables\n"
              + TestHelper.getStackTrace(se));
        }
      }
    }
    sqlGen.executeListener("CLOSE", "PRINT_CONN");
    sqlGen.commit(conn);
    sqlGen.executeListener("CLOSE", "PRINT_CONN");
  }

  public void populateBBWithTableData() {
    Log.getLogWriter().info("Populating BB with tables information");
    // create all table
    String[] createTablesStmt = SQLPrms.getCreateTablesStatements(true);
    Executor executor = new Executor(sqlGen.getGFEConnection());
    List<String> tableList = new ArrayList<String>();
    ConstraintInfoHolder constraintHolder = new ConstraintInfoHolder();
    try {
      for (String ddl : createTablesStmt) {
        executor.execute(ddl);
        Log.getLogWriter().info("executed : " + ddl);
        tableList.add(getFullyQualifiedTableName(ddl));
        TableInfoGenerator generator = new TableInfoGenerator(
            getFullyQualifiedTableName(ddl), executor, constraintHolder);
        generator.buildTableInfo().setDerbyDDL(ddl);
        generator.saveTableInfoToBB();
        GenericBBHelper.getTableInfo(getFullyQualifiedTableName(ddl)).setDerbyDDL(ddl);
      }

      while (!tableList.isEmpty()) {
        String command = "drop table " + tableList.get(tableList.size() - 1);
        executor.execute(command);
        Log.getLogWriter().info("executed : " + command);
        tableList.remove(tableList.size() - 1);
      }
    } catch (SQLException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  
  public String getFullyQualifiedTableName(String ddl){    
    return ddl.toUpperCase().split("\\(")[0].split("TABLE")[1].trim().split(" ")[0];
  }
  
  public void getTableInfosFromBB(){
    String[] tablesDDLs = SQLPrms.getCreateTablesStatements(true);
    for (int i =0; i<tablesDDLs.length; i++){
      String fullTableName = getFullyQualifiedTableName(tablesDDLs[i]);
      TableInfo t = GenericBBHelper.getTableInfo(fullTableName);      
      if(t == null){
        throw new TestException("No TableInfo found in BB for " + fullTableName);   
      }{
        tableInfoList.add(t);
      }
    }   
  }
  
  public void updateTableInfosToBB(){
    int numOfPRs = 0;
    for(TableInfo tbl: tableInfoList){
      numOfPRs += tbl.getNumPrForRecovery();
      log.info("numOfPR for " + tbl.getFullyQualifiedTableName() + " is " + tbl.getNumPrForRecovery() + " totalnumpr=" + numOfPRs);
      GenericBBHelper.putTableInfo(tbl);      
    }
    SQLBB.getBB().getSharedCounters().add(SQLBB.numOfPRs, numOfPRs);
  }
  
  protected void setTablePartitionClause() {
    /*
        {
          {
            PARTITION BY PRIMARY KEY | Partitioning by Column ( column-name [ , column-name ] * ) }
            |
            Partitioning by a Range of Values ( column-name )
            (
                VALUES BETWEEN value AND value
                    [ , VALUES BETWEEN value AND value ] *
            )
            |
            Partitioning by a List of Values ( column-name )
            (
                VALUES ( value [ , value ] * )
                    [ , VALUES ( value [ , value ] * ) ] *
            )
            |
            Partitioning by Expression
          }
          [ Colocating Related Rows ( table-name [ , table_name ] *  ) ]
        }
        [ REDUNDANCY Clause integer-constant ]
        [ BUCKETS Clause integer-constant ]
        [ RECOVERYDELAY Clause integer-constant ]
        [ MAXPARTSIZE Clause integer-constant ] 
     */
    log.info("Inside getTablePartitionClause");
    int tableCount = tableInfoList.size();
    Vector<String> gfeDDLExtension = TestConfig.tab().vecAt(SQLPrms.gfeDDLExtension, new HydraVector());
    boolean hasDDLExtension = gfeDDLExtension.size()>0 ? true : false;    
    if (hasDDLExtension && tableCount != gfeDDLExtension.size()){
      throw new TestException("Found " + tableCount + " tables but " + gfeDDLExtension.size() + " gfeDDLExtension"); 
    }
    
    for (int i =0; i < tableCount ; i++){
      String ddlExtension = gfeDDLExtension.elementAt(i);
      TableInfo tbl = tableInfoList.get(i);     
      
      String partition =  partClause.getPartitionedClause(tbl, ddlExtension);
      
      //redundancy
      String redundancyClause = "";
      if (SQLGenericPrms.hasRedundancy()) {
        if (!partition.toLowerCase().contains("replicate")) {
          Vector<String> redClauseVector = TestConfig.tab().vecAt(
              SQLPrms.redundancyClause, new HydraVector());

          if (i < redClauseVector.size()) {
            redundancyClause = redClauseVector.elementAt(i);
          } else {
            int r = SQLGenericPrms.redundancy();
            redundancyClause = " REDUNDANCY " + r;
          }
        }
      }
      
      // Currently following are read from conf in gfeDDLExtension parameter
      // todo - randomise the config based on some flag 
      // [ BUCKETS Clause integer-constant ]        
      // [ RECOVERYDELAY Clause integer-constant ] 
      // [ MAXPARTSIZE Clause integer-constant ]
       
      
      partition = partition + redundancyClause ;
      tbl.setPartitioningClause(partition);
    }
  }
  
  protected void setServerGroupClause(){    
    Vector<String> gfeDDLExtension = TestConfig.tab().vecAt(SQLPrms.gfeDDLExtension, new HydraVector());
    ArrayList<String[]>  groups = (ArrayList<String[]> ) SQLBB.getBB().getSharedMap().get("serverGroup_tables");
    int tableCount = tableInfoList.size();
    for (int i =0; i<tableCount; i++) {
      TableInfo tbl = tableInfoList.get(i);
      String ddlExtension = gfeDDLExtension.elementAt(i);
      String str[] = ddlExtension.split(":");      
      if (sqlGen.testServerGroups) {
        String serverGroup = "";
        if (str.length > 2){
          serverGroup = str[2];
          if (serverGroup.equals("random")){
            TableInfo parent = tbl.getColocatedParent();
            if(parent != null){
              // get server groups from parent
              serverGroup = parent.getServerGroups();
            }else if (SQLTest.random.nextBoolean()){
              //set random groups
              String[] group = groups.get(SQLTest.random.nextInt(groups.size()));
              serverGroup = group[SQLTest.random.nextInt(group.length)];
            }
            else{
            //use inheritence from schema
              serverGroup = "default"; 
            } 
            Log.getLogWriter().info("random server group is "+ serverGroup);
          } else {
            serverGroup = serverGroup.replace('.', ',');
            Log.getLogWriter().info("server group is "+ serverGroup);
          }
          
        }else{
          String[] group = groups.get(SQLTest.random.nextInt(groups.size()));
          serverGroup = group[SQLTest.random.nextInt(group.length)];
        }   
        
        tbl.setServerGroups(serverGroup);
      } 
    }
  }
  
  protected void setOffHeapClause(){    
    if (SQLOldTest.isOffheap && SQLOldTest.randomizeOffHeap) {
      throw new TestException("SqlPrms.isOffheap and SqlPrms.randomizeOffHeap are both set to true");
    }
    
    int tableCount = tableInfoList.size();
    if (SQLOldTest.isOffheap){
      Log.getLogWriter().info("enabling offheap." );
      for (int i =0; i<tableCount; i++) {
        TableInfo tbl = tableInfoList.get(i);
        if(!tbl.isOffHeap()) tbl.setOffHeap(true);
      }          
    }
    
    if (SQLOldTest.randomizeOffHeap) {
      Log.getLogWriter().info("Randomizing off-heap in some tables but not others");
      for (int i =0; i<tableCount; i++) {
        TableInfo tbl = tableInfoList.get(i);
        if(!tbl.isOffHeap()) {
          if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 50)
          tbl.setOffHeap(true);
        }
      }
    }
  }
  
  protected void setPersistentClause(){
    //[ PERSISTENT [ 'disk-store-name' ] [ ASYNCHRONOUS | SYNCHRONOUS ] ]
    
    int tableCount = tableInfoList.size();
    Vector gfePersistExtension = TestConfig.tab().vecAt(SQLPrms.gfePersistExtension, new HydraVector());
    boolean hasPersistClause = gfePersistExtension.size()>0 ? true : false;
    if (hasPersistClause && tableCount != gfePersistExtension.size()){
      throw new TestException("Found " + tableCount + " tables but " + gfePersistExtension.size() + " gfePersistExtension"); 
    }
    
    if(hasPersistClause){
      for (int i=0; i < tableCount; i++){
        TableInfo tbl = tableInfoList.get(i);
        tbl.setPersistClause((String)gfePersistExtension.elementAt(i));
      }  
    }  
  }
  
  protected void setGatewaySenderClause(){
    SQLWanBB wanBB = SQLWanBB.getBB();
    int myWanSite = FabricServerHelper.getDistributedSystemId();
    ArrayList<String> senderIDs = (ArrayList<String>)wanBB.getSharedMap().get(myWanSite+"_senderIDs");
    if (senderIDs == null) throw new TestException("senderIDs are not setting yet for creating tables");    
    int tableCount = tableInfoList.size();
    for (int i =0; i<tableCount; i++) {
      TableInfo tbl = tableInfoList.get(i);
      for (int j =0; j<senderIDs.size(); j++) {
        tbl.addGatewaySenderToList(senderIDs.get(j));
      }
    }
  }
  
  protected void setAsyncEventListnerClause() {
    int tableCount = tableInfoList.size();
    for (int i = 0; i < tableCount; i++) {
      TableInfo tbl = tableInfoList.get(i);
      if (SQLOldTest.hasAsyncDBSync) {
        String partition = tbl.getPartitioningClause();
        if (partition.contains("REPLICATE")) {
          String serverGroups = tbl.getServerGroups();

          if (SQLOldTest.testServerGroupsInheritence) {
            SchemaInfo schema = tbl.getSchemaInfo();
            serverGroups = schema.getServerGroup();
          }

          if (!serverGroups.contains(SQLOldTest.sgDBSync)) {
            serverGroups = SQLOldTest.sgDBSync + "," + serverGroups;
          }
          tbl.setServerGroups(serverGroups);
        } // handle replicate tables for sg

        if (SQLOldTest.hasAsyncEventListener) {
          String basicListener = "BasicAsyncListener";
          tbl.addAsyncEventListnerToList(basicListener);
        } else {
          tbl.addAsyncEventListnerToList(SQLOldTest.asyncDBSyncId);
        }
      }
    }
  }
  
  protected void setHdfsClause(){
    if (SQLOldTest.hasHdfs) { 
      Log.getLogWriter().info("creating hdfs extn...");
      String[] ddlString = SQLPrms.getCreateTablesStatements(true);
      String[] hdfsDDL = SQLPrms.getHdfsDDLExtn(ddlString);
      for (int i = 0; i < hdfsDDL.length; i++) {
        TableInfo tbl = tableInfoList.get(i);
        tbl.setHdfsClause(hdfsDDL[i]);
        Log.getLogWriter().info("hdfs extention: " + hdfsDDL[i]);
      }
    }
  }
  
  protected void setEnableConcurrecyCheckClause(){
    final String ENABLECONCURRENCYCHECKS = " ENABLE CONCURRENCY CHECKS ";
    int tableCount = tableInfoList.size();
    if (SQLOldTest.enableConcurrencyCheck) {
      for (int i =0; i<tableCount; i++) {
        TableInfo tbl = tableInfoList.get(i);
        tbl.setEnableConcurrencyCheck(true);
      }   
    }
  }
  
  protected void setEvictionClause() {
    int tableCount = tableInfoList.size();
    for (int i = 0; i < tableCount; i++) {
      TableInfo tbl = tableInfoList.get(i);
      String evictionClause = (SQLOldTest.useHeapPercentage 
          ? (SQLOldTest.alterTableDropColumn 
              ? sqlGen.getEvictionHeapPercentageOverflowForAlterTable() 
              : sqlGen.getEvictionHeapPercentageOverflow())
          : sqlGen.getEvictionOverflow());
      tbl.setEvictionClause(evictionClause);
    }    
  }
  
  protected void setNumPrForRecovery(){
    int tableCount = tableInfoList.size();
    for (int i = 0; i < tableCount; i++) {
      TableInfo tbl = tableInfoList.get(i);
      tbl.setNumPrForRecovery(calculateNumOfPRs(tbl));
    }    
  }
  
  protected int calculateNumOfPRs(TableInfo table){
    
    // 0 for replicated table
    String partition = table.getPartitioningClause().toLowerCase();
    boolean isReplicated = partition.contains("replicate");
    if(isReplicated){return 0;}
    
    int numPrs = 1; // for PR    
    // check if partitioned column is part of primary
    PKConstraint pk = table.getPrimaryKey();
    List<ColumnInfo> primaryCols = pk == null ? new ArrayList<ColumnInfo>() : pk.getColumns();
    List<ColumnInfo> partitionedCols = table.getPartitioningColumns();
    boolean isPartitionedPrimary = false;
    for (ColumnInfo col: partitionedCols){
      if (primaryCols.contains(col)){
        isPartitionedPrimary = true;
        break;
      }
    }
    
    // global hash index is created if partitioned column is not in primary 
    if(!isPartitionedPrimary) numPrs += 1;
    
    // for each unique index, one global hash index is created
    List<UniqueConstraint> uniqueList = table.getUniqueKeys();
    numPrs += uniqueList.size();
    
    // double it for hdfs
    String hdfsClause = table.getHdfsClause();
    if(!hdfsClause.equals("")){
      numPrs += numPrs;  
    }
    
    //double it for asyncEventListner
    List<String> aeqList = table.getAsyncEventListnerList(); //todo
    for (String aeq: aeqList){
      numPrs = numPrs * 2;
    }
    
    // double it for parallel sender
    List<String> senderList = new ArrayList<String>(); //todo
    for (String sender: senderList){
      boolean isparallel = false; //todo
      if(isparallel){
        numPrs = numPrs * 2;
      }
    }
    
    return numPrs;
  }
  
  public void dropTables(Connection conn) {
    String sql = null;
    ArrayList <String>tables = new ArrayList<String>();
    for(int i=sqlGen.tableNames.length-1; i>=0 ; i--){
      tables.add(sqlGen.tableNames[i]);
    }
    
    boolean testDropTableIfExists = SQLTest.random.nextBoolean();
    sql = "drop table " + ((testDropTableIfExists) ? " if exists " : "");
    
    try {
      Statement s = conn.createStatement();
      for (String table: tables) {        
        s.execute(sql + table);
        Log.getLogWriter().info("executed : " + sql + table);
      }
    } catch (SQLException se) {
      if (se.getSQLState().equalsIgnoreCase("42Y55") && !testDropTableIfExists) {
        Log.getLogWriter().info("Got expected table not exists exception, continuing tests");
      } else {
        SQLHelper.handleSQLException(se);
      }
    }
  }
}

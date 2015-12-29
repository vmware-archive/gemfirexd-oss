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
package sql.schemas;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.gemfirexd.GfxdHelperPrms;
import hydra.gemfirexd.NetworkServerHelper;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.GFEDBManager.Isolation;
import sql.backupAndRestore.BackupAndRestoreBB;
import sql.backupAndRestore.BackupRestoreTest;
import sql.sqlutil.ResultSetHelper;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;

public class SchemaTest extends SQLTest {
  protected static SchemaTest stest;
  protected static boolean mixOffheapTables = TestConfig.tab().booleanAt(SQLPrms.mixOffheapTables, false);
  protected static boolean isHDFSTest = TestConfig.tab().booleanAt(SQLPrms.hasHDFS, false);
  protected static String hdfsStoreName = "hdfsStore";
  protected static boolean hasPersistentTables = TestConfig.tab().booleanAt(GfxdHelperPrms.persistTables, false);
  public static final String PERSISTENTCLAUSE = " PERSISTENT " +
     (random.nextBoolean() ? "SYNCHRONOUS " : "ASYNCHRONOUS ");
  public static String REDUNDANCYCLAUSE = sql.SQLPrms.getRedundancyClause(0);
  public static long lastBackupTime = -1;
  public static boolean logDML = TestConfig.tab().booleanAt(sql.SQLPrms.logDML, false);
  public static String NUMOFPRSTABLENAME = "numofprs";
  public static String NUMOFPRSCOLUMNNAME = "number";
  public static String NUMOFPRSIDNAME = "pr_id";
  public static final int MILLSECPERMIN = 60 * 1000;
  
  protected static double initEvictionHeapPercentage = TestConfig.tab().doubleAt(SQLPrms.initEvictionHeapPercent, 10);
  protected boolean usePartitionBy = true;
  public static double criticalHeapPercentage = 50; //initial value
  public static double criticalOffHeapPercentage = 50; //initial value
  protected static long lastCriticalHeapUpdated = 0;
  protected static boolean increaseCriticalHeapPercent = false;
  
  public static synchronized void HydraTask_initialize() {
    if (stest == null) {
      stest = new SchemaTest();
      //if (sqlTest == null) sqlTest = new SQLTest();
      
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());

      stest.initialize();
    }
  }
  
  protected void initialize() {
    //TODO add necessary settings here
  }
  
  public synchronized static void HydraTask_startFabricServer() {
    stest.startFabricServer();
  }
  
  public synchronized static void HydraTask_stopFabricServer() {
    stest.stopFabricServer();
  } 
  
  public static synchronized void HydraTask_startNetworkServer() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    NetworkServerHelper.startNetworkServers(networkServerConfig);
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
  
  protected Connection getConnectionWithSchema(Connection conn, String schema, Isolation isolation) {    
    String sql = "set schema " + schema;
    try {
      conn.createStatement().execute(sql);
      Log.getLogWriter().info(sql);
      setIsolation(conn, isolation);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return conn;
  }
  
  protected Connection setIsolation(Connection conn, Isolation isolation) throws SQLException{
    switch (isolation) {
    case NONE:
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
      break;
    case READ_COMMITTED: 
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      conn.setAutoCommit(false); 
      break;
    case REPEATABLE_READ:
      conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      conn.setAutoCommit(false); 
      break;
    default: 
      throw new TestException ("test issue -- unknow Isolation lever");
    }

    return conn;
  }
 
  protected void runImportTable(Connection conn) throws SQLException {
    long start = System.currentTimeMillis();
    Log.getLogWriter().info("import table starts from " + start);
    
    importTable(conn);
    
    long end = System.currentTimeMillis();
    Log.getLogWriter().info("import table finishes at " + end);
    
    long time = end - start;
    
    Log.getLogWriter().info("import_table takes " + time/1000 + " seconds");
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
  
  public static void HydraTask_triggerBackup() {
    if (lastBackupTime == -1 || System.currentTimeMillis() - lastBackupTime  > 5*60*1000) {
      long start = System.currentTimeMillis();
      Log.getLogWriter().info("online backup starts from " + start);
      
      BackupRestoreTest.doBackup();
      
      long end = System.currentTimeMillis();
      Log.getLogWriter().info("online backup finishes at " + end);
      
      long time = end - start;   
      Log.getLogWriter().info("online backup takes " + time/1000 + " seconds");
      
      lastBackupTime = System.currentTimeMillis() ;
    }    
  }
  
  protected void alterTableAddTxId(Connection conn, String tablename) {
    try {
      String sql = "alter table " + tablename 
        + " add column txid bigint with default 0 not null"; 
      Log.getLogWriter().info(sql);
      conn.createStatement().execute(sql);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  public static void HydraTask_doVodafoneInsertOp() {
    if (stest == null) {
      stest = new SchemaTest();
    }
    stest.doVodafoneInsertOps();
  }
  
  private void doVodafoneInsertOps() {
    Connection conn = getGFEConnection();
   
    for (int i=0; i< 100; i++)
      doVodafoneInsertOp(conn);
    
    closeGFEConnection(conn);
  }
  
  private void doVodafoneInsertOp(Connection conn) {
    boolean useBatchInsert = false;
    boolean useTransaction = random.nextBoolean();
    
    if (useTransaction) {
      try {
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        conn.setAutoCommit(false);
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
    }
    
    String sql = "insert into rti.kpi_aaa values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    int size = 10;
    long[] id = new long[size];
    String[] subscriber_id = new String[size];
    String[] cell_id = new String[size];
    String[] tac = new String[size];
    int[] aggregation_interval = new int[size];
    Timestamp[] time_from = new Timestamp[size];
    long[] input_octets =  new long[size];
    long[] output_octets =  new long[size];
    long[] input_packets =  new long[size];
    long[] output_packets =  new long[size];
    int[] count_2g = new int[size];
    int[] count_3g = new int[size];
    int[] count_4g = new int[size];
    Timestamp[] time_new = new Timestamp[size];
    short[] worker_id = new short[size];
    
    getDateForInsert(id, subscriber_id, cell_id, tac, aggregation_interval,
        time_from, input_octets, output_octets, input_packets, output_packets,
        count_2g, count_3g, count_4g, time_new, worker_id, size);
    
    try {
      PreparedStatement stmt = conn.prepareStatement(sql);
      try {
        for (int i=0 ; i<size ; i++) { 

          stmt.setObject(1, id[i]);
          stmt.setString(2, subscriber_id[i]);
          stmt.setString(3, cell_id[i]);
          stmt.setString(4, tac[i]);
          stmt.setObject(5, time_from[i]);
          stmt.setObject(6, aggregation_interval[i]);
          stmt.setObject(7, input_octets [i]);
          stmt.setObject(8, output_octets[i]);
          stmt.setObject(9, input_packets[i]);
          stmt.setObject(10, output_packets[i]);
          stmt.setInt(11, count_2g[i]);
          stmt.setInt(12, count_3g[i]);
          stmt.setObject(13, count_4g[i]);
          stmt.setObject(14, time_new[i]);
          stmt.setObject(15, worker_id[i]);
          if (useBatchInsert) stmt.addBatch();
          else {
            int count = stmt.executeUpdate();
            Log.getLogWriter().info("inserted with aggregation_interval: " + aggregation_interval[i]);
            if (count != 1) throw new TestException("update count for insert is " + count);
          }
        }
        
        if (useBatchInsert) stmt.executeBatch();
        conn.commit();
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
      
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    
  }
  
  private void getDateForInsert(long[] id, String[] subscriber_id, String[] cell_id,
      String[] tac, int[] aggregation_interval, Timestamp[] time_from, long[] input_octets,
      long[] output_octets, long[] input_packets, long[] output_packets,
      int[] count_2g, int[] count_3g, int[] count_4g, Timestamp[] time_new, 
      short[] worker_id, int size) {
    long key = SQLBB.getBB().getSharedCounters().add(SQLBB.tradeCustomersPrimary, size);
    long counter;
    for (int i = 0 ; i <size ; i++) {
      counter = key - i;
      id[i]= counter;
      subscriber_id[i] = "name" + counter;   
      cell_id[i] = "cell" + counter;
      tac[i] = getRandVarChar(8);
      aggregation_interval[i] = random.nextInt();
      time_from[i] = new Timestamp(System.currentTimeMillis());
      input_octets[i] =  random.nextLong();
      output_octets[i] =  random.nextLong();
      input_packets[i] =  random.nextLong();
      output_packets[i] =  random.nextLong();
      count_2g[i] = random.nextInt();
      count_3g[i] = random.nextInt();
      count_4g[i] = random.nextInt();
      time_new[i] = new Timestamp(System.currentTimeMillis());
      worker_id[i] = 0;
    }
  }
  
  protected String getRandVarChar(int length) {
    int aVal = 'a';
    int symbolLength = random.nextInt(length) + 1;
    char[] charArray = new char[symbolLength];
    for (int j = 0; j<symbolLength; j++) {
      charArray[j] = (char) (random.nextInt(26) + aVal); //one of the char in a-z
    }
    String randChars = new String(charArray);
    return randChars;
  }
  
  protected int getNumOfPRs() {
    GemFireCacheImpl cache = (GemFireCacheImpl)Misc.getGemFireCache();
    Set<PartitionedRegion> prSet = cache.getPartitionedRegions();
    
    if (prSet == null) throw new TestException("test issue, no pr is set in the test");
    
    int numOfPRs = prSet.size();
    log().info("numOfPRs in cluster is " + numOfPRs);
    for (PartitionedRegion pr: prSet) {
      log().info("partitioned region name is " + pr.getName());
    }
    
    for (PartitionedRegion pr: prSet) {
      if (pr.getRedundantCopies() == 0 ) {
        log().info(pr.getRegion().getName() + " has redundancy set to 0");
        --numOfPRs;
        log().info("remove this pr from numOfPR calculation as there is no redundancy/recovery for this region");
        log().info("numOfPRs with redundancy in cluster now is " + numOfPRs);
      }
    }
    
    return numOfPRs;
  }
  
  //find the num of PRs in the cache from a member
  public static void HydraTask_findNumOfPRs() throws SQLException{
    if (stest == null) stest = new SchemaTest();
    
    stest.findNumOfPRs();  
  }
  
  protected void findNumOfPRs() throws SQLException{
    int numOfPRs = getNumOfPRs();
    
    String sql = "put into " + NUMOFPRSTABLENAME + " values (1, " + numOfPRs + ")";
    log().info(sql);
    Connection c = getGFEConnection();
    c.createStatement().executeUpdate(sql);
  }
  
  public static void HydraTask_createTableNumOfPRs() throws SQLException {
    if (stest == null) stest = new SchemaTest();
    
    stest.createTableNumOfPRs();  
  }

  protected void createTableNumOfPRs() throws SQLException{
    Connection c = getGFEConnection();
    String sql = "create table " + NUMOFPRSTABLENAME + "(" +
    NUMOFPRSIDNAME + " int, " +
    NUMOFPRSCOLUMNNAME + " int," +
    " PRIMARY KEY (" + NUMOFPRSIDNAME + ")) " +
    (isHATest ? "replicate " : "" ) +
    "persistent ";
    log().info(sql);
    c.createStatement().execute(sql); 
    c.commit();
  }
  
  public static void HydraTask_setNumOfPRs() throws SQLException{
    if (stest == null) stest = new SchemaTest();
    
    stest.setNumberOfPRs();  
  }
  
  protected void setNumberOfPRs() throws SQLException{
    int numOfPRs = 0;
    
    int originalNumOfPRs = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.numOfPRs);
    if (originalNumOfPRs != 0) throw new TestException("test issue, " +
    		"numOfPRs in SQLBB has been updated: " + originalNumOfPRs );
    
    String sql = "select " + NUMOFPRSCOLUMNNAME + " from " + NUMOFPRSTABLENAME ;
    log().info(sql);
    Connection c = getGFEConnection();
    ResultSet rs = c.createStatement().executeQuery(sql);
    
    if (rs.next()) {
      numOfPRs = rs.getInt(1); 
      log().info("numOfPRs from table " + NUMOFPRSTABLENAME + " is " + numOfPRs);
    } else {
      throw new TestException("Could not get result for query: " + sql);
    }
    
    if (rs.next()) throw new TestException("more than 1 row in the table " + 
        NUMOFPRSTABLENAME + " value: " + rs.getInt(1) );
    
    SQLBB.getBB().getSharedCounters().add(SQLBB.numOfPRs, numOfPRs);
    
    log().info("updated SQLBB.numOfPRs to " + numOfPRs );
  }
  
  public static void HydraTask_cycleStoreVms() {
    if (stest == null) stest = new SchemaTest();
    
    if (cycleVms) stest.cycleStoreVms();
  }
  
  public static void HydraTask_setupDsProperitesForThinClient() {
    Properties dsProp = DistributedSystemHelper.getDistributedSystem().getProperties();
    
    BackupAndRestoreBB.getBB().getSharedMap().put(BackupRestoreTest.DSPROP, dsProp);
  }
  
  protected double getCriticalHeapPercentage(Connection gConn, String sql, String serverGroup) {
    double heapPercent = 0;
    try {
      ResultSet rs = gConn.createStatement().executeQuery(sql);
      
      List<Struct> rsList = ResultSetHelper.asList(rs, false);
      
      if (rsList != null) {
        if (rsList.size() > 1) {
          String newsql = "select sys.get_critical_heap_percentage() as criticalHeapPercentage, id, servergroups as criticalHeapPercentage from sys.members where servergroups = '" +
          serverGroup + "'";
          log().info(newsql);
          ResultSet newrs = gConn.createStatement().executeQuery(newsql);
        
          List<Struct> newrsList = ResultSetHelper.asList(newrs, false);
          
          
          if (isHATest) {
            log().warning("hit #51290, continue testing: " + 
                sql + "\n" + ResultSetHelper.listToString(ResultSetHelper.asList(rs, false)));
          }
          else throw new TestException("more than 1 critical heap percentage are set in the same server group: " + 
              sql + "\n" + ResultSetHelper.listToString(rsList) + "\n" +
              newsql + "\n" + ResultSetHelper.listToString(newrsList)); 
        } else if (rsList.size() == 1) {
          float heapPercentF = (Float) rsList.get(0).get("criticalHeapPercentage".toUpperCase());
          heapPercent = Double.parseDouble(Float.toString(heapPercentF));
        }
      }
    } catch (SQLException se) {
      if (isHATest && se.getSQLState().equals(X0Z01)) {
        log().info("got expected node failure exception during select query, continue testing");
      } else if (isOfflineTest && 
          (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) { 
        log().info("Got expected Offline exception, continuing test");
      } else SQLHelper.handleSQLException(se);
    }
    return heapPercent;
  }
  
  protected void increaseCriticalHeapPercentage(Connection gConn, double heapPercent, 
      String serverGroup) {
    if (!setCriticalHeap) {
      Log.getLogWriter().info("No critical heap is set");
      return;
    }

    
    if (heapPercent > 0) log().info("critical heap percentage is " + heapPercent);
    else log().info("could not get ciritical heap percentage"); //HA, low memory etc
    
    long currentTime = System.currentTimeMillis();
    int waitMinute = 3;
    int waitTime = waitMinute * MILLSECPERMIN; //wait time to reset critical heap
    if (increaseCriticalHeapPercent && (lastCriticalHeapUpdated == 0 || 
        currentTime - lastCriticalHeapUpdated > waitTime)) {
      if (heapPercent > 0) {
        criticalHeapPercentage = heapPercent + 3;
      }
      else criticalHeapPercentage += 3;
      
      log().info("new critical percent to be set " + criticalHeapPercentage);
      if (criticalHeapPercentage <= 82) {
        setCriticalHeapPercentage(gConn, criticalHeapPercentage, serverGroup);
        if (criticalHeapPercentage<80) {
          increaseCriticalHeapPercent = false;        
        }     
        log().info("increaseCriticalHeapPercent is set to " + increaseCriticalHeapPercent);
        lastCriticalHeapUpdated = System.currentTimeMillis();
      }
      
    }
  }
}

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
package sql.hdfs;

import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.HadoopDescription;
import hydra.HadoopHelper;
import hydra.HadoopPrms;
import hydra.HostDescription;
import hydra.HydraRuntimeException;
import hydra.HydraTimeoutException;
import hydra.Log;
import hydra.Prms;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.gemfirexd.GfxdTestConfig;
import hydra.gemfirexd.HDFSStoreDescription;
import hydra.gemfirexd.NetworkServerHelper;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlutil.ResultSetHelper;
import sql.wan.SQLWanBB;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.CustomEvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.HoplogListenerForRegion;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogListener;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdEvictionCriteria;
import com.pivotal.gemfirexd.internal.engine.hadoop.mapreduce.DumpHDFSData;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;
import hydra.MasterController;

public class HDFSSqlTest extends SQLTest{

  public static final Random random = TestConfig.getInstance().getParameters().getRandGen();  
  private AtomicBoolean isHdfsStoreCreated = new AtomicBoolean(false);  
  private AtomicBoolean isEvictionObserverCreated = new AtomicBoolean(false); 
  public static boolean hasIdentityColumn = TestConfig.tab().booleanAt(SQLPrms.setIdentityColumn, false);
  
  //constants
  public static final String STORENAME = "STORENAME";
  public static final String EVICTION_CRITERIA = "EVICTIONCRITERIA";
  public static final String EVICT_INCOMING = "EVICTINCOMING";
  public static final String STARTTIME = "STARTTIME";
  public static final String FREQUENCY = "FREQUENCY";        
  public static final String CASCADE = "CASCADE";
  public static final String WRITEONLY = "WRITEONLY";

  public synchronized static void HydraTask_initCompactionListener() {
    HoplogListener compactionListener = new HoplogListener() {
      public void hoplogDeleted(String regionFolder, int bucketId, Hoplog... oplogs)
        throws IOException {
      }

      public void hoplogCreated(String regionFolder, int bucketId, Hoplog... oplogs)
        throws IOException {
      }

      public void compactionCompleted(String region, int bucket, boolean isMajor) {
        Log.getLogWriter().info("HoplogListener-compactionCompleted-region=" + region +
                                " bucket=" + bucket +
                                " isMajor=" + isMajor);
        if (isMajor) {
          long majorCompactionCnt = HDFSSqlBB.getBB().getSharedCounters().incrementAndRead(HDFSSqlBB.majorCompactionCnt);
          Log.getLogWriter().info("HoplogListener-compactionCompleted-majorCompactionCnt=" + majorCompactionCnt);
          //Statistics stats = hdfsSqlTest.getCompressionStat("/TRADE/SECURITIES");
          //Log.getLogWriter().info("HydraTask_waitForCompaction-majorCompactionsInProgress=" + stats.getLong("majorCompactionsInProgress"));
          //Log.getLogWriter().info("HydraTask_waitForCompaction-majorCompactions=" + stats.getLong("majorCompactions"));
        } else  {
          long minorCompactionCnt = HDFSSqlBB.getBB().getSharedCounters().incrementAndRead(HDFSSqlBB.minorCompactionCnt);
          Log.getLogWriter().info("HoplogListener-compactionCompleted-minorCompactionCnt=" + minorCompactionCnt);
          //Statistics stats = hdfsSqlTest.getCompressionStat("/TRADE/SECURITIES");
          //Log.getLogWriter().info("HoplogListener-minorCompactions=" + stats.getLong("minorCompactions"));
          //Log.getLogWriter().info("HoplogListener-minorCompactionsInProgress=" + stats.getLong("minorCompactionsInProgress"));
        }
      }
    };

    for (String table : tables) {
      Log.getLogWriter().info("HydraTask_initCompactionListener-HoplogListener-setting tables-table=" + table);
      String tableName = StringUtil.SQLToUpperCase(table);
      Log.getLogWriter().info("HydraTask_initCompactionListener-HoplogListener-tableName=" + tableName);
      Region<Object, Object> aRegion = Misc.getRegionForTable(tableName, true);
      HoplogListenerForRegion listenerManager = ((LocalRegion) aRegion).getHoplogListener();
      listenerManager.addListener(compactionListener);
    }
  }

  public static void HydraTask_waitForCompaction() {
    long minorCompactionCnt = HDFSSqlBB.getBB().getSharedCounters().read(HDFSSqlBB.minorCompactionCnt);
    long majorCompactionCnt = HDFSSqlBB.getBB().getSharedCounters().read(HDFSSqlBB.majorCompactionCnt);
    Log.getLogWriter().info("HydraTask_waitForCompaction-(before wait)-minorCompactionCnt=" + minorCompactionCnt +
                            " majorCompactionCnt=" + majorCompactionCnt);

    // Wait for HDFS Store Compaction to finish
    // Wait for Minor
    TestHelper.waitForCounter(HDFSSqlBB.getBB(),
      "HDFSSqlBB.minorCompactionCnt",
      HDFSSqlBB.minorCompactionCnt,
      HDFSTestPrms.getDesiredMinorCompactions(),
      false,
      -1);
    // Wait for Major
    TestHelper.waitForCounter(HDFSSqlBB.getBB(),
      "HDFSSqlBB.majorCompactionCnt",
      HDFSSqlBB.majorCompactionCnt,
      HDFSTestPrms.getDesiredMajorCompactions(),
      false,
      -1);


    // Signal for the test to stop when the compaction threshold is met
    minorCompactionCnt = HDFSSqlBB.getBB().getSharedCounters().read(HDFSSqlBB.minorCompactionCnt);
    majorCompactionCnt = HDFSSqlBB.getBB().getSharedCounters().read(HDFSSqlBB.majorCompactionCnt);
    if (minorCompactionCnt >= HDFSTestPrms.getDesiredMinorCompactions() && majorCompactionCnt >= HDFSTestPrms.getDesiredMajorCompactions()) {
      throw new StopSchedulingOrder("It's time to stop the test, Minor Compaction Count is " + minorCompactionCnt +
                                    " and Major Compaction Count is " + majorCompactionCnt);
    }
  }

  private Statistics getCompressionStat(String statFullName) {
    Log.getLogWriter().info("getCompressionStat statFullName=" + statFullName);

    StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    while (statFactory == null) {
      statFactory = DistributedSystemHelper.getDistributedSystem();
    }

    Statistics rtnStats = null;
    Statistics[] stats = statFactory.findStatisticsByTextId(statFullName);
    for (Statistics aStat : stats) {
      Log.getLogWriter().info("getCompressionStat aStat.getType().getName()=" + aStat.getType().getName());
      if (aStat.getType().getName().equals("HDFSRegionStatistics")) {
        rtnStats = aStat;
      }
    }

    return rtnStats;
  }

  public synchronized void createHdfsStore() {
    try {
      if (isHdfsStoreCreated.get() == false ||  isWanTest) {
        Connection conn = getGFEConnection();
        createHdfsStore(conn);
        closeGFEConnection(conn);
        isHdfsStoreCreated.set(true);
      }
    } catch (SQLException sqle) {
      Log.getLogWriter().info("HydraTask_createHDFSStoresFor49414-SQLException caught.");
      SQLHelper.handleSQLException(sqle);
    }
  }
  
  public void createHdfsStore(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
        List<String> hdfsStoreDDLs;
        if (useRandomConfHdfsStore) {
            hdfsStoreDDLs=getHDFSStoreRandomDDL();
        }
        else {
          hdfsStoreDDLs = hydra.gemfirexd.HDFSStoreHelper.getHDFSStoreDDL();
        }

    for (String hdfsStoreDDL : hdfsStoreDDLs) {
     
      if (isWanTest ){
        hydra.gemfirexd.HDFSStoreDescription hdfsStoreDesc = hydra.gemfirexd.HDFSStoreHelper.getHDFSStoreDescription(GfxdConfigPrms.getHDFSStoreConfig());
        String homeDir= hdfsStoreDesc.getHomeDir();
        hdfsStoreDDL = hdfsStoreDDL.replace(homeDir, homeDir +  getMyWanSite());
      }
      Log.getLogWriter().info("about to create HDFSSTORE :  " + hdfsStoreDDL);
      stmt.execute(hdfsStoreDDL);
      Log.getLogWriter().info("created HDFSSTORE :  " + hdfsStoreDDL);
    }
  }

  public static void HydraTask_createHDFSStoreWithWrongGFXDSecurity() {
    hdfsSqlTest.createHDFSStoreWithWrongGFXDSecurity();
  }
  private synchronized void createHDFSStoreWithWrongGFXDSecurity() {
    boolean correctExceptionWasThrown = false;
    Connection conn = getGFEConnection();
    try {
      createHdfsStore(conn);
    } catch (Exception e) {
      if (e.getCause() instanceof HDFSIOException) {
        correctExceptionWasThrown = true;
      } else {
        Throwable causedBy = e.getCause();
        while (causedBy != null) {
          if (causedBy instanceof HDFSIOException) {
            correctExceptionWasThrown = true;
            break;
          }
          causedBy = causedBy.getCause();
        }
      }
    }
    finally {
      closeGFEConnection(conn);
    }
    if (correctExceptionWasThrown) {
      Log.getLogWriter().info("The correct Exception, HDFSIOException was found.");
    } else {
      throw new TestException("ERROR! The wrong exception was thrown - this test was supposed to throw a HDFSIOException.");
    }
  }

   private List<String>  getHDFSStoreRandomDDL() {
    StringBuffer config = new StringBuffer();
    hydra.gemfirexd.HDFSStoreDescription hdfsStoreDesc = hydra.gemfirexd.HDFSStoreHelper.getHDFSStoreDescription(GfxdConfigPrms.getHDFSStoreConfig());

  
      int batchSize = random.nextInt(5) + 5;
      int batchTimeInterval = random.nextInt(5000) + 1000;
      int maxQueueMem = random.nextInt(50) + 100;
      boolean queuePersist =  random.nextBoolean();
      boolean disksynchronous = random.nextBoolean();
      boolean autoCompaction = HDFSTestPrms.isCompactionTest() ? true : random.nextBoolean();
      boolean autoMajorCompaction = HDFSTestPrms.isCompactionTest() ? true : random.nextBoolean();
      int maxInputFileSizeMb = random.nextInt(500) + 500;
      int minInputFileCount = random.nextInt(2) + 2;
      int maxInputFileCount = random.nextInt(10) + 10;
      int maxConcurrency = random.nextInt(20) + 1;
      int majorCompactionInterval = random.nextInt(300) + 200;
      int majorCompactionConcurrency = random.nextInt(10) + 1;
      int blockCacheSize = random.nextInt(5) + 5;
      int purgeInterval  = random.nextInt(10) + 500;
      String hdfsClientConfigFile = "'./file1'";     
      if (random.nextBoolean()) config.append(" MAXQUEUEMEMORY ").append(maxQueueMem);
      if (random.nextBoolean() ) config.append(" QUEUEPERSISTENT ").append(queuePersist);
      if (random.nextBoolean()) config.append(" DISKSYNCHRONOUS ").append(disksynchronous);
      if (random.nextBoolean()) {
        config.append(" MINORCOMPACT ").append(autoCompaction);
        if (autoCompaction){
          if (random.nextBoolean()) config.append(" BATCHSIZE ").append(batchSize);
          if (random.nextBoolean()) { config = HDFSSqlTestVersionHelper.appendBatchInterval(config, batchTimeInterval);}  
        }else{
          // increase batchsize and batchTimeInterval in case of minorcompact = 'false'
          config.append(" BATCHSIZE ").append(50);
          config = HDFSSqlTestVersionHelper.appendBatchInterval(config, 60000);
        }
      }else{
        if (random.nextBoolean()) config.append(" BATCHSIZE ").append(batchSize);
        if (random.nextBoolean())  { config = HDFSSqlTestVersionHelper.appendBatchInterval(config, batchTimeInterval);}
      }
      if (random.nextBoolean() ) config.append(" MAJORCOMPACT ").append(autoMajorCompaction);
      if (random.nextBoolean()) config.append(" MAXINPUTFILESIZE ").append(maxInputFileSizeMb);
      if (random.nextBoolean()) {
        config.append(" MININPUTFILECOUNT ").append(minInputFileCount);
        config.append(" MAXINPUTFILECOUNT ").append(maxInputFileCount);
      }
      if (random.nextBoolean()) config.append(" MINORCOMPACTIONTHREADS ").append(maxConcurrency);
      if (HDFSTestPrms.isCompactionTest()) // compaction should happen for compaction tests
        {config = HDFSSqlTestVersionHelper.appendMajorCompactionInterval(config, hdfsStoreDesc.getMajorCompactionInterval());}
      else if (random.nextBoolean())
      {config = HDFSSqlTestVersionHelper.appendMajorCompactionInterval(config, majorCompactionInterval);}
      if (random.nextBoolean()) config.append(" MAJORCOMPACTIONTHREADS ").append(majorCompactionConcurrency);
      if (random.nextBoolean() || hdfsMrJob) config.append(" MAXWRITEONLYFILESIZE ").append(1024);
      if (random.nextBoolean() || hdfsMrJob ) {config = HDFSSqlTestVersionHelper.appendWriteOnlyFileRolloverInterval(config, 1);}
      if (random.nextBoolean()) config.append(" BLOCKCACHESIZE ").append(blockCacheSize);
      if (random.nextBoolean()) config.append(" CLIENTCONFIGFILE ").append(hdfsClientConfigFile);
      if (random.nextBoolean()) {config = HDFSSqlTestVersionHelper.appendPurgeInterval(config, purgeInterval);}
    

    HadoopDescription hadoopDesc = HadoopHelper.getHadoopDescription(ConfigPrms.getHadoopConfig());
    List<String> storeDDL = new ArrayList<String>();
        // in case of multiple stores create same DDL with different names.
        Collection<HDFSStoreDescription> hsds = GfxdTestConfig.getInstance().getHDFSStoreDescriptions().values();
        for (HDFSStoreDescription hsd : hsds) {
         StringBuffer buff = new StringBuffer();    
        buff.append("CREATE HDFSSTORE ").append(hsd.getName())
            .append(" NAMENODE ").append("'" + hadoopDesc.getNameNodeURL() + "'")
            .append(" HOMEDIR ").append("'" + hsd.getHomeDir() + "'")
            .append(" DISKSTORENAME ").append(hsd.getDiskStoreDescription().getName())
             .append(config.toString());
         storeDDL.add(buff.toString());
        }
        return storeDDL;
  }

  public synchronized void setHDFSEvictionObserver(){
    if(isEvictionObserverCreated.get()) return;
    
    isEvictionObserverCreated.set(true);
    //Cache c = CacheFactory.getAnyInstance();        
    String[] tableNames = tables;
    for (String tableName : tableNames){     
      Region<?, ?>  r = Misc.getGemFireCache().getRegion("/" + tableName.toUpperCase().replace(".", "/"));
      if (r != null){
        CustomEvictionAttributes evictionAtt  = r.getAttributes().getCustomEvictionAttributes();        
        if(evictionAtt != null){
          GfxdEvictionCriteria criteria   = (GfxdEvictionCriteria)evictionAtt.getCriteria();
          criteria.setObserver(new EvictionObserver());
          Log.getLogWriter().info("EvictionObserver is configured for region " + r.getFullPath());  
        }else{
          Log.getLogWriter().info("EvictionObserver is not configured for region " + r.getFullPath() 
              + " as CustomEvictionAttributes=" + evictionAtt);
        }
      }      
    }    
  }
  
  public synchronized void dropHdfsStore() {
    Connection conn = getGFEConnection();
    dropHdfsStore(conn);
    closeGFEConnection(conn);
  }
  

  private void dropHdfsStore(Connection conn){
    try {
      String storeName = GfxdConfigPrms.getHDFSStoreConfig();
      String ifExists = random.nextBoolean() ? "IF EXISTS " : "";
      String dropStoreDDL = "DROP HDFSSTORE " + ifExists + storeName;
      
      Statement stmt = conn.createStatement();
      Log.getLogWriter().info("about to drop HDFSSTORE :  " + dropStoreDDL);
      stmt.execute(dropStoreDDL);    
      Log.getLogWriter().info("dropped HDFSSTORE :  " + dropStoreDDL);      
    }catch(SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }
  }
  
  public void verifyHdfsOperationData() {
    Connection gConn = getGFEConnection(); 
    verifyHdfsData(gConn, true);
    closeGFEConnection(gConn);
  }
  
  
  public void verifyHdfsNonOperationData() {
    Connection gConn = getGFEConnection(); 
    verifyHdfsData(gConn, false);
    closeGFEConnection(gConn);
  }
  

  @SuppressWarnings("unchecked")
  protected void verifyHdfsData(Connection gConn, boolean justOperationalData) {
    Map<String, String> hdfsExtnMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(SQLPrms.HDFS_EXTN_PARAMS);
    
    // update eviction start for tables having eviction frequency    

    // commented below alterTableSetEvictionStartTime check for:
    // 1) now it is not required as same is checked by checkTablesRowsEvicted() later
    // 2) HANG is reported with altering starttime because of difference in cache time and system time. Need to track this by separate bug and tests
    
    //alterTableSetEvictionStartTime(gConn, hdfsExtnMap); 
    
    // wait for eviction 
//    while (!checkForEvictionComplete(hdfsExtnMap)) {
    Log.getLogWriter().info("verifyHdfsData-Waiting for Eviction started.");
    while(!checkTablesRowsEvicted(gConn, hdfsExtnMap)){
      int waitForSecs=50;
      int sleepMs = waitForSecs * 1000;
      Log.getLogWriter().info("verifyHdfsData-Waiting for Eviction to finish, sleep for  " + waitForSecs + " seconds");
      hydra.MasterController.sleepForMs(sleepMs);
    }
    // wait more for few sec before validation 
    hydra.MasterController.sleepForMs(20 * 1000);
    Log.getLogWriter().info("verifyHdfsData-Waiting for Eviction finished.");
    
    boolean throwException = false;
    StringBuffer exceptionStr = new StringBuffer();
        
    for (String table : tables) {
      String hdfsSchemaTable = table.toUpperCase();       
      String operationalDataStr = justOperationalData ? "operation data" : "non-operation data";
      String hasHdfsStore = hdfsExtnMap.get(hdfsSchemaTable + HDFSSqlTest.STORENAME);
      
      if (hasHdfsStore == null || (!justOperationalData && hdfsExtnMap.get(hdfsSchemaTable + HDFSSqlTest.WRITEONLY) != null  &&  hdfsExtnMap.get(hdfsSchemaTable + HDFSSqlTest.EVICTION_CRITERIA) != null )) {
        Log.getLogWriter().info("verifyHdfsData-Skiping " + operationalDataStr + ( (!justOperationalData && hdfsExtnMap.get(hdfsSchemaTable + HDFSSqlTest.WRITEONLY)!= null  &&  hdfsExtnMap.get(hdfsSchemaTable + HDFSSqlTest.EVICTION_CRITERIA) != null) ? "validation as writeonly hdfs store with eviction is configured for table " : " validation as no hdfs store is configured for table " ) + hdfsSchemaTable);
      } else {
        Log.getLogWriter().info("verifyHdfsData-Started " + operationalDataStr + " validation for table " + hdfsSchemaTable);
        
        String[] dml = getQueryDml(hdfsSchemaTable, hdfsExtnMap, justOperationalData, false);
        String queryHdfsOpsData = dml[0];
        String queryOpsFullData = dml[1];

        try {
          compareResults(gConn, queryOpsFullData, queryHdfsOpsData, hdfsSchemaTable + "_fulldataset" , hdfsSchemaTable);
        } catch (TestException te) {
          Log.getLogWriter().info("verifyHdfsData-Do not throw Exception yet - until all tables are verified");
          throwException = true;
          exceptionStr.append(te.getMessage() + "\n");
        }
        
        //negative testing in case of eviction criteria to expect zero rows is result set
        String evictionCriteria = hdfsExtnMap.get(hdfsSchemaTable + HDFSSqlTest.EVICTION_CRITERIA);
        if(evictionCriteria != null ){
          Log.getLogWriter().info("verifyHdfsData using negative test query, " + operationalDataStr + " validation for table " + hdfsSchemaTable);
          dml = getQueryDml(hdfsSchemaTable, hdfsExtnMap, justOperationalData, true);
          queryHdfsOpsData = dml[0];
          queryOpsFullData = dml[1];

          try {            
            compareResults(gConn, queryOpsFullData, queryHdfsOpsData, hdfsSchemaTable + SQLTest.DUPLICATE_TABLE_SUFFIX , hdfsSchemaTable);
          } catch (TestException te) {
            Log.getLogWriter().info("verifyHdfsData-Do not throw Exception yet - until all tables are verified");
            throwException = true;
            exceptionStr.append(te.getMessage() + "\n");
          }
        }
      }
    }
    if (throwException) {
      throw new TestException("Verify results failed: " + exceptionStr);
    }
  }

  private boolean checkTablesRowsEvicted(Connection gconn, Map<String, String> hdfsExtnMap){     
    boolean evicted = true;
    for (String tableName : tables){ 
      String evictionCriteria = hdfsExtnMap.get(tableName.toUpperCase() + HDFSSqlTest.EVICTION_CRITERIA);
      if(evictionCriteria != null){
        ResultSet rs;
        try{
          Statement s= gconn.createStatement();    
          String selectstmt="SELECT COUNT(*) FROM  " + tableName + " -- GEMFIREXD-PROPERTIES queryHDFS=false \n where ( " + evictionCriteria + " ) ";
          Log.getLogWriter().info("Executing " + selectstmt);
          rs = s.executeQuery(selectstmt);
          if (rs.next()) {
            if (rs.getInt(1) != 0) evicted = false;
            Log.getLogWriter().info("Total rows waiting for eviction in table " + tableName + " are " + rs.getInt(1)) ;
          }else{
            throw new TestException ("Test issue - now expected to reach here.");
          }
        }catch (Exception e){
          throw new TestException ("Error  while executing displayTotalRowsinTable on  " + tableName + e.getStackTrace().toString()  + e.getMessage());
        } 
      }
    }
    return evicted; 
  }
  
  private String[] getQueryDml(String hdfsSchemaTable, Map<String, String> hdfsExtnMap, boolean justOperationalData, boolean negativeQuery) {
    String[] dmls = new String[2];
    String fullDataSchemaTable = hdfsSchemaTable + SQLTest.DUPLICATE_TABLE_SUFFIX.toUpperCase();
    String evictionCriteria = hdfsExtnMap.get(hdfsSchemaTable + HDFSSqlTest.EVICTION_CRITERIA);
    
    String selectQueryStringForIdentityColumnCheck = hasIdentityColumn ? (hdfsSchemaTable.equalsIgnoreCase("trade.securities") ? 
        "select sec_id, symbol, price, exchange, tid from " : (hdfsSchemaTable.equalsIgnoreCase("trade.customers") ? 
        "select cid, cust_name, since, addr, tid from " : "SELECT * FROM ")) : "SELECT * FROM ";
    
    if(evictionCriteria == null){
      // if no eviction criteria is set, all data is in hdfs and in memory
      if (justOperationalData) {
        // verify that expected data is retrieved when queryHDFS=false
        dmls[0] = selectQueryStringForIdentityColumnCheck + hdfsSchemaTable + " -- GEMFIREXD-PROPERTIES queryHDFS=false";
        dmls[1] = selectQueryStringForIdentityColumnCheck + fullDataSchemaTable;
      } else { // non-operational data        
        // verify that expected data is retrieved when queryHDFS=true 
        dmls[0] = selectQueryStringForIdentityColumnCheck + hdfsSchemaTable + " -- GEMFIREXD-PROPERTIES queryHDFS=true";
        dmls[1] = selectQueryStringForIdentityColumnCheck + fullDataSchemaTable;
      }  
    }else{
      // eviction criteria is set
      String reversedCriteria = getReversedSqlPredicate(evictionCriteria);
      if(!negativeQuery){
        if (justOperationalData) {
          // verify that correct entries from memory is retrieved  
          dmls[0] = selectQueryStringForIdentityColumnCheck + hdfsSchemaTable + " -- GEMFIREXD-PROPERTIES queryHDFS=false";
          dmls[1] = selectQueryStringForIdentityColumnCheck + fullDataSchemaTable + " WHERE ( " + reversedCriteria + " ) ";
        } else { // non-operational data        
          // verify that correct entries from HDFS is retrieved
          dmls[0] = selectQueryStringForIdentityColumnCheck + hdfsSchemaTable + " -- GEMFIREXD-PROPERTIES queryHDFS=true \n WHERE ( " + evictionCriteria + " ) ";
          dmls[1] = selectQueryStringForIdentityColumnCheck + fullDataSchemaTable + " WHERE ( " + evictionCriteria + " ) ";
        } 
      }else{
        // checks negative results        
        if (justOperationalData) {
          // verify that no unexpected entry is fetched from HDFS when queryHDFS=false
          dmls[0] = selectQueryStringForIdentityColumnCheck + hdfsSchemaTable + " -- GEMFIREXD-PROPERTIES queryHDFS=false \n WHERE ( " + evictionCriteria + " ) ";
          dmls[1] = selectQueryStringForIdentityColumnCheck + fullDataSchemaTable + " WHERE ( " + evictionCriteria + " ) AND ( " + reversedCriteria + " ) ";
        } else { // non-operational data
          // verify that no unexpected entry is fetched from memory when queryHDFS=true 
          dmls[0] = selectQueryStringForIdentityColumnCheck + hdfsSchemaTable + " -- GEMFIREXD-PROPERTIES queryHDFS=true \n WHERE ( " + reversedCriteria + " ) ";
          dmls[1] = selectQueryStringForIdentityColumnCheck + fullDataSchemaTable + " WHERE ( " + reversedCriteria + " ) ";
        }
      }
    }
    return dmls;
  }

protected void compareResults(Connection gConn, String fullQuery, String hdfsQuery, String table1 , String table2) throws TestException {
    List<Struct> fullList = getResultSet(gConn, fullQuery);
    List<Struct> hdfsList = getResultSet(gConn, hdfsQuery);
    
    if ( hdfsMrJob ) {
          compareMRResultSets(fullList, hdfsList, table1 , table2);
    }
    else{
      if ( fullList.size() == hdfsList.size()  && SQLTest.populateWithbatch ) {
          Log.getLogWriter().info("size of fulldata set is " + fullList.size()  + " size of hdfsList is " + hdfsList.size() );
      } else 
          ResultSetHelper.compareResultSets(fullList, hdfsList, table1 , table2);
    }
  }
 
  public void executeMR() {
    
    Map<String, String> hdfsExtnMap = (Map<String, String>)SQLBB.getBB().getSharedMap().get(SQLPrms.HDFS_EXTN_PARAMS);      
    String tables[] =SQLPrms.getTableNames();      

    // for 1.0 <=> 1.0.1 backward compatibility tests we cannot set majorCompactionInterval (RW tables) or
    // writeOnlyFileRolloverInterval (WriteOnly tables) due to non-portable DDL, so the mapreduce jobs must
    // wait for the default times for these attributes.  These are set in the sqlHdfsBackwardCompatibility "Mr" tests
    // via the HDFSTestPrms.mapReduceDelaySec (which defaults to 0)
    // In addition, because of BUG 50383 ... we have to wait 2 * writeOnlyFileRolloverSec (or we will see ~.shop.tmp 
    // files in the DirListing (from mapreduce) which aren't available to M/R.
    int mapReduceDelaySec = HDFSTestPrms.getMapReduceDelaySec();
    Log.getLogWriter().info("Waiting for mapReduceDelaySec (" + mapReduceDelaySec + ")");
    MasterController.sleepForMs(mapReduceDelaySec * 1000);
    Log.getLogWriter().info("Done waiting for mapReduceDelaySec (" + mapReduceDelaySec + ")");

    if ( hdfsExtnMap.get(tables[0].toUpperCase() + HDFSSqlTest.WRITEONLY) != null ) {        
      if (HDFSTestPrms.useRandomConfig()) {
        Log.getLogWriter().info("Wait 2 Mins for FileRollover..."); 
        try {             
          Thread.sleep(120000);             
        } catch (Exception e) {
          Log.getLogWriter().info("Exception Received while waiting for FileRollover"  + e.getMessage());
        }
      } else {
         HDFSSqlTestVersionHelper.waitForWriteOnlyFileRollover(getHdfsTables());
      }
    }
   
    DisplayHdfsFileStruct();
    String[] strArr = SQLPrms.getTableNames();
    for (String tableName : strArr) {
      try {
        executeMrJob(tableName.toUpperCase());
      } finally {
        // if running with auth.to.local mapping, remove kerberos ticket created for running secure hdfs command
        HadoopDescription hdd = HadoopHelper.getHadoopDescription(ConfigPrms.getHadoopConfig());
        String host = RemoteTestModule.getMyHost();
        if (hdd.getSecurityAuthentication().equals(HadoopPrms.KERBEROS)) {
          HadoopHelper.executeKdestroyCommand(host, 120);
        }
      }
    }
  }
  
  private boolean checkForEvictionComplete(Map<String, String> hdfsExtnMap){    
    SharedCounters counters = HDFSSqlBB.getBB().getSharedCounters();
    int expected = 0;
    int evictionCount = 0;
    StringBuffer buf = new StringBuffer();
    for (String table : tables){      
      String frequency  = hdfsExtnMap.get(table.toUpperCase() + HDFSSqlTest.FREQUENCY);
      if (frequency != null){
        expected++;
        int j = (int)counters.read(HDFSSqlBB.evictionCount);
        evictionCount +=j;
        buf.append("Eviction count for " + table + " is " + j);
      }
    }
    Log.getLogWriter().info("checkForEvictionComplete - expected=" + expected + " evictionCount=" + evictionCount + " \n" + buf.toString());
    return (expected == evictionCount);
  }
  
  private void alterTableSetEvictionStartTime(Connection gConn, Map<String, String>  hdfsExtnMap){
    //reset eviction counters
    HDFSSqlBB.getBB().getSharedCounters().zero(HDFSSqlBB.evictionCount);
   
    // update start time so that eviction starts for tables having EVICTION FREQUENCY 
    for (String table : tables){
      String frequency  = hdfsExtnMap.get(table.toUpperCase() + HDFSSqlTest.FREQUENCY);
      if (frequency != null){
        // read cache time as per #48780
        long timeMill = ((GemFireCacheImpl)Misc.getGemFireCache()).cacheTimeMillis();

        if (random.nextInt(100) < 10){
          timeMill -= 10 * 60 * 1000;
          Log.getLogWriter().info("Setting starttime to 10 mins earlier than current time");
        } else if (random.nextInt(100) < 10){
          timeMill += 30 * 1000;
          Log.getLogWriter().info("Setting starttime to 30 sec later than current time");
        }
        
        // Should use the GMT time as per #49562
        // Syntax:  START { D 'date-constant' | T 'time-constant' | TS 'timestamp-constant' } 
        // date-constant : yyyy-mm-dd
        // timestamp-constant: yyyy-mm-dd hh:mm:ss
        // time-constant : hh:mm:ss

        int w = random.nextInt(3);
        String format = (w==0) ? "yyyy-MM-dd HH:mm:ss" : ((w==1) ? "HH:mm:ss" : "yyyy-MM-dd" );
        String lit    = (w==0) ? "TS"                  : ((w==1) ? "T"        : "D" );
        
        SimpleDateFormat gmtFormat = new SimpleDateFormat(format);
        gmtFormat.setTimeZone(TimeZone.getTimeZone("GMT+00"));        
        String timeStr = lit + " '" + gmtFormat.format(new Date(timeMill)) + "'";    
        int freq = random.nextInt(10) + 1; 
        String sql = "ALTER TABLE " + table + " SET EVICTION FREQUENCY " + freq + " SECONDS START " + timeStr ;
        
        Log.getLogWriter().info("Time converted from '" + new Date(timeMill)  + "' to " + timeStr);
        
        try {
          Log.getLogWriter().info("executing alter table to set eviction start time " + sql );
          Statement statement = gConn.createStatement();
          statement.execute(sql);
          statement.close();          
          Log.getLogWriter().info("executed alter table to set eviction start time : " + sql);
        }catch (SQLException se) {
          SQLHelper.handleSQLException(se);
        }
      }
    }
  }
  
  private Timestamp getTimeStampfromGfxd(Connection gConn){
    String selectStmt = "select current_timestamp  from  SYS.SYSTABLES";
    Timestamp ts = new Timestamp(new Date().getTime());
    try{
    Statement stmt = gConn.createStatement();
    ResultSet rs = stmt.executeQuery(selectStmt);
    if (rs.next()){
      ts=rs.getTimestamp(1);
    }
    }catch (Exception e) {
      throw new TestException("Error in getTimeStampfromGfxd" + e.getMessage());
    }
    return ts;
  }
  public String getReversedSqlPredicate(String sqlPredicate){
    return "NOT ( " + sqlPredicate + ")" ;
  }
  
  public void verifyTotalRowsinTables() {
    String[] tableName = SQLPrms.getTableNames();
    Connection conn = getGFEConnection();
    Map<String, String> map = (Map<String, String>)SQLBB.getBB().getSharedMap().get(SQLPrms.HDFS_EXTN_PARAMS);
    try {
      Statement statement = conn.createStatement();
      for (int i = 0; i < tableName.length; i++) {
        String val = map.get(tableName[i].toUpperCase() + HDFSSqlTest.STORENAME);
        if (val != null) {
          Log.getLogWriter().info("VALIDATING TOTAL ROWS IN  " + tableName[i] );
          ResultSet rs = statement.executeQuery("SELECT COUNT(*)  FROM " + tableName[i] + " -- GEMFIREXD-PROPERTIES queryHDFS=true");
          if (rs.next() && rs.getInt(1) > 0)
            throw new TestException("selectTotalRowsFromTables failed for table  " 
                    + tableName[i] + ", expected 0 but found " + rs.getInt(1));
          else 
            Log.getLogWriter().info("total lines read from table " + tableName[i]  + " are " + rs.getInt(1));
            Log.getLogWriter().info("VALIDATING TOTAL ROWS ON  " + tableName[i]+ " COMPLETED. TOTAL ROWS FETCHED " + rs.getInt(1));
          rs.close();
        }
      }
      statement.close();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }

    closeGFEConnection(conn);
  }
    
   public void displayTotalRowsinTable(String tableName) {    
    Connection conn = getGFEConnection();    
    try {
      Statement statement = conn.createStatement();
      ResultSet rs = statement.executeQuery("SELECT COUNT(*)  FROM " + tableName );
          if (rs.next() )            
              Log.getLogWriter().info("total lines read from table " + tableName  + " are " + rs.getInt(1));
      rs.close();  
      statement.close();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }

    closeGFEConnection(conn);
  } 
      
   public void verifyHdfsDataUsingMR() {
    String[] strArr = SQLPrms.getTableNames();
    Boolean throwException = false;
    StringBuffer exceptionStr = new StringBuffer();

    for (String tableName : strArr) {
      try {
        compareMRwithDuplicateTables(tableName);
      } catch (TestException te) {
        Log
            .getLogWriter()
            .info(
                "verifyHdfsDataUsingMR-Do not throw Exception yet - until all tables are verified");
        throwException = true;
        exceptionStr.append(te.getMessage() + "\n");
      }

    }
    if (throwException) {
      throw new TestException("verifyHdfsDataUsingMR  Failed \n" + exceptionStr);
    }
  }

  // Sleeps for the configured HDFSStore.writeOnlyFileRolloverInterval (defaults to 3600 seconds) + 2 minutes
  // Ensure that maxResultWaitSec is greater than this!
  public static void HydraTask_waitForWriteOnlyFileRolloverInterval() {
    hydra.gemfirexd.HDFSStoreDescription hsd = hydra.gemfirexd.HDFSStoreHelper.getHDFSStoreDescription(GfxdConfigPrms.getHDFSStoreConfig());
    int rolloverInterval = hsd.getWriteOnlyFileRolloverInterval().intValue();  // executeMR adds 2 more minutes for rollover to complete
    Log.getLogWriter().info("Sleeping for " + rolloverInterval + " seconds to allow for writeOnlyFileRolloverInterval to expire");
    MasterController.sleepForMs(rolloverInterval * 1000);
    Log.getLogWriter().info("Done sleeping for " + rolloverInterval + " seconds to allow for writeOnlyFileRolloverInterval to expire");
  }

  // build up the url containing the locators to hand to mapreduce
  // resulting string (which is returned from this method) will look like this (if there are two locators)
  //   10.138.44.145:20222/;secondary-locators=10.138.44.144:25849
  protected String getLocatorEndpoints() {
    List<NetworkServerHelper.Endpoint> endpoints = NetworkServerHelper.getNetworkLocatorEndpoints();
    StringBuffer endpointString = new StringBuffer();
    endpointString.append(endpoints.get(0));
    for (int i = 1; i < endpoints.size(); i++) {
      if (i == 1) {
        endpointString.append("/;secondary-locators=");
      } else {
        endpointString.append(",");
      }
      endpointString.append(endpoints.get(i));
    }
    return endpointString.toString();
  }

  public void executeMrJob( String tableName) {
   
      String mapReduceClassName =   (String)( (Map)SQLBB.getBB().getSharedMap().get(SQLPrms.MAP_REDUCE_CLASSES)).get(tableName);

      String hadoopConfig = ConfigPrms.getHadoopConfig();
      HadoopDescription hdd = HadoopHelper.getHadoopDescription(hadoopConfig);
      String confDir = hdd.getResourceManagerDescription().getConfDir();
      hydra.gemfirexd.HDFSStoreDescription hsd =hydra.gemfirexd.HDFSStoreHelper.getHDFSStoreDescription(GfxdConfigPrms.getHDFSStoreConfig()); 
        
      //String homeDir = "/user/nthanvi/gemfirexd_data" ;//+ shsd.getHomeDir().toUpperCase();
        String homeDir=hsd.getHomeDir();

      String sep = File.separator;

      int vmid = RemoteTestModule.getMyVmid();
      String clientName = RemoteTestModule.getMyClientName();
      String host = RemoteTestModule.getMyHost();
      HostDescription hd = TestConfig.getInstance().getClientDescription( clientName ).getVmDescription().getHostDescription();

      Log.getLogWriter().info("all the basic parameters are configured by now" );
      
      String jtests = System.getProperty("JTESTS");
      String MRJarPath = jtests + sep + ".." + sep + "extraJars" + sep + "mapreduce.jar";
      String gemfirexdJarPath = jtests + sep + ".." + sep + ".." + sep + "product-gfxd" + sep + "lib" + sep + "gemfirexd.jar";
      String gemfirexdClientJarPath = jtests + sep + ".." + sep + ".." + sep + "product-gfxd" + sep + "lib" + sep + "gemfirexd-client.jar";
      String hbaseJarPath = jtests + sep + ".." + sep + ".." + sep + "product" + sep + "lib" + sep + "hbase-0.94.4-gemfire*.jar";

      // This url specifies the locators (which is a primary and a comma separated list of secondary locators
      // The resulting url should look like this:
      //   jdbc:gemfirexd://10.138.44.145:20222/;secondary-locators=10.138.44.144:25849
      String url = "jdbc:gemfirexd://" + getLocatorEndpoints();
      
      hbaseJarPath =  ProcessMgr.fgexec(new String[]{"sh","-c","ls -lrt " + hbaseJarPath + "|" + "awk '{print $9} ' "} , 100 );
      Log.getLogWriter().info("hbase jar file name is "  + hbaseJarPath);   
                  
      
      String cmd = "env CLASSPATH=" + System.getProperty( "java.class.path" ) + " ";
      cmd += "env HADOOP_CLASSPATH=" + System.getProperty( "java.class.path" ) + " ";
      cmd += hdd.getHadoopDist() + sep + "bin" + sep + "yarn ";
      cmd += "--config " + confDir + " ";
      cmd += "jar " + MRJarPath + " ";
      cmd += mapReduceClassName + " ";
      cmd += " -libjars " + " " + MRJarPath + "," + gemfirexdJarPath + "," + hbaseJarPath + "," + gemfirexdClientJarPath + " ";
      cmd += homeDir + " ";
      cmd += url + " ";
      
      String logfn = hd.getUserDir() + sep + "vm_" + vmid + "_" + clientName + "_" + host + "_" + mapReduceClassName + "_" + tableName + "_" + System.currentTimeMillis() + ".log";      
                                      
      if (hdd.isSecure()) {  // kinit is required for mapreduce ... even when running with auth.to.local mapping
        String userKinit = HadoopHelper.GFXD_SECURE_KEYTAB_FILE + " gfxd-secure@GEMSTONE.COM";
        HadoopHelper.executeKinitCommand(host, userKinit, 120);
      }
      int pid = ProcessMgr.bgexec(cmd + tableName, hd.getUserDir(), logfn);
                      
      try {
        RemoteTestModule.Master.recordHDFSPIDNoDumps(hd, pid, false);
      } catch (RemoteException e) {
        String s = "Failed to record PID: " + pid;
        throw new HydraRuntimeException(s, e);
      }
      int maxWaitSec = (int)TestConfig.tab().longAt( Prms.maxResultWaitSec );
      if (!ProcessMgr.waitForDeath(host, pid, maxWaitSec)) {
        String s = "Waited more than " + maxWaitSec + " seconds for MapReduce Job";
        throw new HydraTimeoutException(s);
      }
      try {
        RemoteTestModule.Master.removeHDFSPIDNoDumps(hd, pid, false);
      } catch (RemoteException e) {
        Log.getLogWriter().info("execMapReduceJob caught " + e + ": " + TestHelper.getStackTrace(e));
        String s = "Failed to remove PID: " + pid;
        throw new HydraRuntimeException(s, e);
      }

     
      Log.getLogWriter().info("Completed MapReduce job  on host " + host +
                                " using command: " + cmd +
                                ", see " + logfn + " for output");           
    
    } 
  
  public void DisplayHdfsFileStruct( ) {    
    String hadoopConfig = ConfigPrms.getHadoopConfig();
    HadoopDescription hdd = HadoopHelper.getHadoopDescription(hadoopConfig);
    hydra.gemfirexd.HDFSStoreDescription hsd = hydra.gemfirexd.HDFSStoreHelper.getHDFSStoreDescription(GfxdConfigPrms.getHDFSStoreConfig());
    String homeDir=hsd.getHomeDir();
    String sep = File.separator;
    int vmid = RemoteTestModule.getMyVmid();
    String clientName = RemoteTestModule.getMyClientName();
    String host = RemoteTestModule.getMyHost();
    HostDescription hd = TestConfig.getInstance().getClientDescription( clientName ).getVmDescription().getHostDescription();
    String dirLogfn = hd.getUserDir() + sep + "vm_" + vmid + "_" + clientName + "_" + host + "_" +  "DirStructure" + "_" + System.currentTimeMillis() + ".log";
    Log.getLogWriter().info("Hdfs data Structure before Running MR jobs is listed in File: " + homeDir + "/"+ dirLogfn);

    if (hdd.isSecure()) {  // always run kinit, required even when running with auth.to.local mapping
      String userKinit = HadoopHelper.GFXD_SECURE_KEYTAB_FILE + " gfxd-secure@GEMSTONE.COM";
      HadoopHelper.executeKinitCommand(host, userKinit, 120);
    }
    HadoopHelper.runHadoopCommand(hadoopConfig, "fs -ls -R " + homeDir + "/", dirLogfn);    

    // if running with auth.to.local mapping, remove kerberos ticket created for running secure hdfs command
    if (hdd.getSecurityAuthentication().equals(HadoopPrms.KERBEROS)) {
      HadoopHelper.executeKdestroyCommand(host, 120);
    }
  }

  // to do create a map and put in the bb
  public void setMapReduceClassName(){        
    HashMap<String, String> map = new HashMap<String, String>();
    if (HDFSTestPrms.useMapRedVersion1()) {
    map.put("TRADE.CUSTOMERS", "sql.hdfs.mapreduce.TradeCustomersHdfsDataVerifier");
    map.put("TRADE.BUYORDERS", "sql.hdfs.mapreduce.TradeBuyOrdersHdfsDataVerifier");
    map.put("TRADE.SELLORDERS","sql.hdfs.mapreduce.TradeSellOrdersHdfsDataVerifier");
    map.put("TRADE.PORTFOLIO", "sql.hdfs.mapreduce.TradePortfolioHdfsDataVerifier");
    map.put("TRADE.NETWORTH",  "sql.hdfs.mapreduce.TradeNetworthHdfsDataVerifier");
    map.put("TRADE.TRADES",    "sql.hdfs.mapreduce.TradeHdfsDataVerifier");
    map.put("TRADE.SECURITIES", "sql.hdfs.mapreduce.TradeSecurityHdfsDataVerifier");
    map.put("TRADE.TXHISTORY",  "sql.hdfs.mapreduce.TradeTxHistoryHdfsDataVerifier");
    } else {
    map.put("TRADE.CUSTOMERS", "sql.hdfs.mapreduce.TradeCustomersHdfsDataVerifierV2");
    map.put("TRADE.BUYORDERS", "sql.hdfs.mapreduce.TradeBuyOrdersHdfsDataVerifierV2");
    map.put("TRADE.SELLORDERS","sql.hdfs.mapreduce.TradeSellOrdersHdfsDataVerifierV2");
    map.put("TRADE.PORTFOLIO", "sql.hdfs.mapreduce.TradePortfolioHdfsDataVerifierV2");
    map.put("TRADE.NETWORTH",  "sql.hdfs.mapreduce.TradeNetworthHdfsDataVerifierV2");
    map.put("TRADE.TRADES",    "sql.hdfs.mapreduce.TradeHdfsDataVerifierV2");
    map.put("TRADE.SECURITIES", "sql.hdfs.mapreduce.TradeSecurityHdfsDataVerifierV2");
    map.put("TRADE.TXHISTORY",  "sql.hdfs.mapreduce.TradeTxHistoryHdfsDataVerifierV2");
    }
    SQLBB.getBB().getSharedMap().put(SQLPrms.MAP_REDUCE_CLASSES, map);
}
 
  
public void compareMRwithDuplicateTables(String tableName)
  {
      String queryFullData = "SELECT * FROM " + tableName + SQLTest.DUPLICATE_TABLE_SUFFIX  ;
      String queryMRData = "SELECT * FROM " + tableName + SQLTest.MR_TABLE_SUFFIX;
      compareResults(getGFEConnection(), queryMRData, queryFullData, tableName + SQLTest.MR_TABLE_SUFFIX , tableName + SQLTest.DUPLICATE_TABLE_SUFFIX);
  
      Map<String, String> hdfsExtnMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(SQLPrms.HDFS_EXTN_PARAMS);
      String evictionCriteria = hdfsExtnMap.get(tableName.toUpperCase() + HDFSSqlTest.EVICTION_CRITERIA);
      if ( evictionCriteria != null  &&  hdfsExtnMap.get(tableName.toUpperCase() + HDFSSqlTest.STORENAME  ) != null ) {
        //check that all the rows matching eviction criteria are no longer in sqlfire db
        queryFullData = "SELECT *  FROM " + tableName + " where " + evictionCriteria ;
        Log.getLogWriter().info (" verify that Data is evicted to Hdfs and no matching row found in operational data");
        Connection gConn = getGFEConnection(); 
        List<com.gemstone.gemfire.cache.query.Struct>   rs = getResultSet(gConn,queryFullData);
        
        if (rs.size() > 0 ) {
           throw new TestException("Following rows are not evicted from " +  tableName + "  \n" +  rs ) ;
        } else {
            Log.getLogWriter().info("Query returned " + rs.size()  + " rows. All the Data matching eviction Crriteria is evicted to Hdfs" );
          }
        Log.getLogWriter().info("comparing the sqlf operational data with fulldataset " );
         String querysqlfData = "SELECT *  FROM " + tableName + " where " + getReversedSqlPredicate(evictionCriteria)  ;
        queryFullData = "SELECT *  FROM " + tableName +  SQLTest.DUPLICATE_TABLE_SUFFIX  + " where " + getReversedSqlPredicate(evictionCriteria)  ;
        List<com.gemstone.gemfire.cache.query.Struct>   rs1  = getResultSet(gConn,querysqlfData);
        List<com.gemstone.gemfire.cache.query.Struct>   rs2 = getResultSet(gConn,queryFullData);
        ResultSetHelper.compareResultSets(rs2, rs1);
      }
 }
  
  public static void compareMRResultSets(List<Struct> MrResultSet, List<Struct> GFEResultSet,  String Mr, String Gfe) {
    
    Log.getLogWriter().info("size of resultSet from " + Mr + " is " + MrResultSet.size());
    Log.getLogWriter().info("size of resultSet from " + Gfe + " is " + GFEResultSet.size());
    
   // List<Struct> secondResultSetCopy = new ArrayList<Struct>(GFEResultSet);
    
    StringBuffer aStr = new StringBuffer();    
    for (int i=0; i<MrResultSet.size(); i++) {
      GFEResultSet.remove(MrResultSet.get(i));
    }
    
    if (GFEResultSet.size() > 0 ) {
      String errMsg= "The following " + GFEResultSet.size() + " records were missing in the " +  Mr + ":\n"  +  ResultSetHelper.listToString(GFEResultSet);
      Log.getLogWriter().info(errMsg + "\n Total records available in " + Mr + ": are \n " +  ResultSetHelper.listToString(MrResultSet)); 
      throw new TestException(errMsg);
     // throw new TestException(errMsg.toString());
    }
    else {
      Log.getLogWriter().info("Verified that all the latest  GFE data for table " + Gfe + " is available in HDFS" );
    }
    
  }
  
  public  void alterEvictionFrequency() {
    Map<String, String> m  = (Map<String, String>) SQLBB.getBB().getSharedMap().get(SQLPrms.HDFS_EXTN_PARAMS);
    alterTableSetEvictionStartTime(getGFEConnection(), m); 
  }
   public static void dumpHDFSTable(String wanId) {
    hydra.gemfirexd.HDFSStoreDescription hdfsStoreDesc = hydra.gemfirexd.HDFSStoreHelper
        .getHDFSStoreDescription(GfxdConfigPrms.getHDFSStoreConfig());
    HadoopDescription hadoopDesc = HadoopHelper.getHadoopDescription(ConfigPrms
        .getHadoopConfig());
    String[] params = new String[4];
    params[0] = hadoopDesc.getNameNodeURL();
    params[1] = hdfsStoreDesc.getHomeDir() + wanId;
    params[3] = ""; // "raw"?
    String[] tables = { "TRADE.CUSTOMERS", "TRADE.BUYORDERS",
        "TRADE.SELLORDERS", "TRADE.PORTFOLIO", "TRADE.NETWORTH",
        "TRADE.TRADES", "TRADE.SECURITIES", "TRADE.TXHISTORY",
        "TRADE.COMPANIES", "EMP.EMPLOYEES" };

    for (String table : tables) {
      params[2] = table;
      Log.getLogWriter().info(
          "Calling DumpHDFSData: " + params[0] + "; " + params[1] + "; "
              + params[2] + "; " + params[3]);
      try {
        DumpHDFSData.main(params);

      } catch (IOException e) {
        Log.getLogWriter().info(
            "dumpHDFSTable exeception caught " + e + ": "
                + TestHelper.getStackTrace(e));
        e.printStackTrace();
      } catch (InterruptedException e) {
        Log.getLogWriter().info(
            "dumpHDFSTable exeception caught " + e + ": "
                + TestHelper.getStackTrace(e));
        e.printStackTrace();
      } catch (SQLException e) {
        Log.getLogWriter().info(
            "dumpHDFSTable exeception caught " + e + ": "
                + TestHelper.getStackTrace(e));
        e.printStackTrace();
      }
    }
  }
}

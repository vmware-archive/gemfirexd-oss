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

import hydra.ClientVmInfo;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.GsRandom;
import hydra.HadoopDescription;
import hydra.HadoopHelper;
import hydra.HadoopPrms;
import hydra.HostDescription;
import hydra.HydraRuntimeException;
import hydra.HydraTimeoutException;
import hydra.Log;
import hydra.MasterController;
import hydra.Prms;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.gemfirexd.GfxdTestConfig;
import hydra.gemfirexd.HDFSStoreDescription;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import sql.hdfs.AlterHdfsStorePrms;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlutil.ResultSetHelper;
import sql.wan.SQLWanBB;
import util.PRObserver;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;

public class AlterHDFSTest extends HDFSSqlTest {

  public static AlterHDFSTest alterHdfsInstance;
  protected int secondsToRun = AlterHdfsStorePrms.getSecondsToRun();   // number of seconds to allow tasks (for concAlterHdfsStore)

  protected static List<AlterHdfsStoreEntry> alterHdfsStoreEntries = new ArrayList<AlterHdfsStoreEntry>();
  /** There are 5 groups of alter hdfsstore attributes
    *   1 - Default (all mutable attributes)
    *   2 - ReadWrite BucketOrganizer (compaction and hdfs file cleanup related) including: 
    *         purgeInterval, MajorCompactionInterval, MaxInputFileCount, MaxInputFileSize, MinInputFileCount
    *   3 - WriteOnly BucketOrgranizer (for flush operations) including:
    *         maxWriteOnlyFileSize, writeOnlyFileRolloverInterval
    *   4 - HDFS AEQ related including:
    *         batchSize, batchTimeInterval
    *   5 - HDFS Compaction Manager
    *         minorCompact, majorCompact, minorCompactionThreads, majorCompactionThreads
    */
  static {
    // DEFAULTS
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("PURGEINTERVAL", "PURGEINTERVALMINS", AlterType.DEFAULT, new Integer(720), null, new Integer(720), SQLTimeUnit.MINUTES));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAJORCOMPACTIONINTERVAL", "MAJORCOMPACTIONINTERVALMINS", AlterType.DEFAULT, new Integer(720), null, new Integer(720), SQLTimeUnit.MINUTES));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAXINPUTFILECOUNT", "MAXINPUTFILECOUNT", AlterType.DEFAULT, new Integer(10), null, new Integer(10), null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAXINPUTFILESIZE", "MAXINPUTFILESIZE", AlterType.DEFAULT, new Integer(512), null, new Integer(512), null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MININPUTFILECOUNT", "MININPUTFILECOUNT", AlterType.DEFAULT, new Integer(4), null, new Integer(4), null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAXWRITEONLYFILESIZE", "MAXWRITEONLYFILESIZE", AlterType.DEFAULT, new Integer(256), null, new Integer(256), null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("WRITEONLYFILEROLLOVERINTERVAL", "WRITEONLYFILEROLLOVERINTERVALSECS", AlterType.DEFAULT, new Integer(3600), null, new Integer(3600), SQLTimeUnit.SECONDS));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("BATCHSIZE", "BATCHSIZE", AlterType.DEFAULT, new Integer(32), null, new Integer(32), null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("BATCHTIMEINTERVAL", "BATCHTIMEINTERVALMILLIS", AlterType.DEFAULT, new Integer(60000), null, new Integer(60000), SQLTimeUnit.MILLISECONDS));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MINORCOMPACT", "MINORCOMPACT", AlterType.DEFAULT, Boolean.TRUE, null, Boolean.TRUE, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAJORCOMPACT", "MAJORCOMPACT", AlterType.DEFAULT, Boolean.TRUE, null, Boolean.TRUE, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MINORCOMPACTIONTHREADS", "MINORCOMPACTIONTHREADS", AlterType.DEFAULT, new Integer(10), null, new Integer(10), null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAJORCOMPACTIONTHREADS", "MAJORCOMPACTIONTHREADS", AlterType.DEFAULT, new Integer(2), null, new Integer(2), null));
    
    // RW BUCKET ORGANZER entries (for compaction and cleanup)
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("PURGEINTERVAL", "PURGEINTERVALMINS", AlterType.RW_BUCKET_ORGANIZER, new Integer(720), new ValueRange(5, 900), 3, SQLTimeUnit.MINUTES));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAJORCOMPACTIONINTERVAL", "MAJORCOMPACTIONINTERVALMINS", AlterType.RW_BUCKET_ORGANIZER, new Integer(720), new ValueRange(5, 900), 3, SQLTimeUnit.MINUTES));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAXINPUTFILECOUNT", "MAXINPUTFILECOUNT", AlterType.RW_BUCKET_ORGANIZER, new Integer(10), new ValueRange(10, 20), 20, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAXINPUTFILESIZE", "MAXINPUTFILESIZE", AlterType.RW_BUCKET_ORGANIZER, new Integer(512), new ValueRange(32, 1024), 32, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MININPUTFILECOUNT", "MININPUTFILECOUNT", AlterType.RW_BUCKET_ORGANIZER, new Integer(4), new ValueRange(1, 9), 3, null));

    // WO BUCKET ORGANIZER
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAXWRITEONLYFILESIZE", "MAXWRITEONLYFILESIZE", AlterType.WO_BUCKET_ORGANIZER, new Integer(256), new ValueRange(32, 1024), 32, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("WRITEONLYFILEROLLOVERINTERVAL", "WRITEONLYFILEROLLOVERINTERVALSECS", AlterType.WO_BUCKET_ORGANIZER, new Integer(3600), new ValueRange(60, 7200), 60, SQLTimeUnit.SECONDS));

    // HDFS AEQ
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("BATCHSIZE", "BATCHSIZE", AlterType.HDFS_AEQ, new Integer(32), new ValueRange(1, 1024), 256, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("BATCHTIMEINTERVAL", "BATCHTIMEINTERVALMILLIS", AlterType.HDFS_AEQ, new Integer(60000), new ValueRange(5, 600000), 120000, SQLTimeUnit.MILLISECONDS));

    // COMPACTION_MANAGER_DISABLE_MINOR
    // disabling minor compaction also requires raising values for batchSize, batchTimeInterval
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MINORCOMPACT", "MINORCOMPACT", AlterType.COMPACTION_MANAGER_DISABLE_MINOR, Boolean.TRUE, null, Boolean.FALSE, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("BATCHSIZE", "BATCHSIZE", AlterType.COMPACTION_MANAGER_DISABLE_MINOR, new Integer(32), new ValueRange(1, 1024), 512, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("BATCHTIMEINTERVAL", "BATCHTIMEINTERVALMILLIS", AlterType.COMPACTION_MANAGER_DISABLE_MINOR, new Integer(60000), new ValueRange(5, 600000), 180000, SQLTimeUnit.MILLISECONDS));

    // COMPACTION_MANAGER_DISABLE_MAJOR
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAJORCOMPACT", "MAJORCOMPACT", AlterType.COMPACTION_MANAGER_DISABLE_MAJOR, Boolean.TRUE, null, Boolean.FALSE, null));
    // COMPACTION_MANAGER  (modify minor/major compaction thread)
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MINORCOMPACTIONTHREADS", "MINORCOMPACTIONTHREADS", AlterType.COMPACTION_MANAGER, new Integer(10), new ValueRange(1,20), 20, null));
    alterHdfsStoreEntries.add(new AlterHdfsStoreEntry("MAJORCOMPACTIONTHREADS", "MAJORCOMPACTIONTHREADS", AlterType.COMPACTION_MANAGER, new Integer(2), new ValueRange(1,10), 5, null));
  }

  public static synchronized void HydraTask_alterHdfsStore() {
    if (alterHdfsInstance == null) {
      alterHdfsInstance = new AlterHDFSTest();
    } 
    alterHdfsInstance.alterHdfsStore();
  }

  protected void alterHdfsStore() {
    Connection conn = getGFEConnection();
    alterHdfsStore(conn);
    closeGFEConnection(conn);
  }

  private void alterHdfsStore(Connection conn) {
    alterHdfsStore(conn, AlterHdfsStorePrms.getAlterType());
  }

  private void alterHdfsStore(AlterType alterType) {
    Connection conn = getGFEConnection();
    alterHdfsStore(conn, alterType);
    closeGFEConnection(conn);
  }

  private void alterHdfsStore(Connection conn, AlterType alterType) {

   // generate ALTER HDFSSTORE DDL 
   StringBuffer attrs = new StringBuffer();
   for (AlterHdfsStoreEntry entry : alterHdfsStoreEntries) {
     if (entry.getAlterType().equals(alterType)) {
       //entry.setNewValue();
       attrs.append(" SET " + entry.getAttributeName() + " " + entry.getNewValue());
       if (entry.getTimeUnit() != null) {
         attrs.append(" " + entry.getTimeUnit());
       }
     }
   }

   // if configured, launch system procedure related to alterType
   // if these threads don't complete, then we have an unintended interaction
   List<String> hdfsTables = getHdfsTables();
   List<Thread> workerThreads = new ArrayList();
   boolean launchProcedure = AlterHdfsStorePrms.getLaunchProcedure();
   if (launchProcedure) {
     for (String table : hdfsTables) {
       switch (alterType) {
         case HDFS_AEQ:
           workerThreads.add(sqlTest.flushQueuesHDFS(table));
           break;
         case RW_BUCKET_ORGANIZER:
           workerThreads.add(sqlTest.forceCompactionHDFS(table));
           break;
         case WO_BUCKET_ORGANIZER:
           workerThreads.add(sqlTest.forceWriteOnlyFileRollover(table, -1));
           break;
         case COMPACTION_MANAGER:
           workerThreads.add(sqlTest.forceCompactionHDFS(table));
           break;
         default:
           // this is okay -- there aren't procedures that conflict with all groups of alter hdfs store
       }
     }
   }

   // execute ALTER HDFSSTORE DDL
   String storeName = GfxdConfigPrms.getHDFSStoreConfig();
   String alterHdfsStoreDDL = "ALTER HDFSSTORE " + storeName + attrs.toString();

   try {
     Statement stmt = conn.createStatement();
     Log.getLogWriter().info("about to alter HDFSSTORE :  " + alterHdfsStoreDDL);
     stmt.execute(alterHdfsStoreDDL);    
     Log.getLogWriter().info("altered HDFSSTORE :  " + alterHdfsStoreDDL);      
   } catch (SQLException sqle) {
     SQLHelper.handleSQLException(sqle);
   }

   // make sure procedure completes (if configured)
   if (launchProcedure) {
     // join flush threads (one per table)
     for (Thread wthread : workerThreads) {
       try {
         wthread.join(maxResultWaitSec-60);
       } catch (InterruptedException e) {
         throw new TestException("System Procedure to conflict with " + alterType + " caught " + e + " " + TestHelper.getStackTrace(e));
       }
     }
   }

    // validate SYS.SYSHDFSSTORES 
    String selectStmt = "select * from SYS.SYSHDFSSTORES where NAME='" + storeName.toUpperCase() + "'";
    Log.getLogWriter().info("query string = " + selectStmt);

    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(selectStmt);
      if (rs.next()) {
        Log.getLogWriter().info(ResultSetRowToString(rs));
        for (AlterHdfsStoreEntry entry : alterHdfsStoreEntries) {
          if (entry.getAlterType().equals(alterType)) {
            Log.getLogWriter().info("Processing entry " + entry.toString());
            String columnName = entry.getColumnName();
            Object expected = entry.getNewValue();
            Object found = rs.getObject(columnName);
            Log.getLogWriter().info("For " + columnName + " found = " + found);
            if (!expected.equals(found)) {
              Log.getLogWriter().info("Expected SYS.SYSHDFSSTORES(" + columnName + ") = " + expected + ", but found " + found);
              throw new TestException("Expected SYS.SYSHDFSSTORES(" + columnName + ") = " + expected + ", but found " + found);
            } else {
              Log.getLogWriter().info("SYS.SYSHDFSSTORES." + columnName + " returned expected value of " + found);
            }
          }
        }
      } else {
        throw new TestException("No results returned from " + selectStmt.toString());
      }
    } catch (SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }

    // restore defaults
    //restoreDefaults(conn);
  }

  private void restoreDefaults(Connection conn) {

   // generate ALTER HDFSSTORE DDL 
   StringBuffer attrs = new StringBuffer();
   for (AlterHdfsStoreEntry entry : alterHdfsStoreEntries) {
     if (entry.getAlterType().equals(AlterType.DEFAULT)) {
       attrs.append(" SET " + entry.getAttributeName() + " " + entry.getDefaultValue());
       if (entry.getTimeUnit() != null) {
         attrs.append(" " + entry.getTimeUnit());
       }
     }
   }

   // execute ALTER HDFSSTORE DDL
   String storeName = GfxdConfigPrms.getHDFSStoreConfig();
   String alterHdfsStoreDDL = "ALTER HDFSSTORE " + storeName + attrs.toString();

   try {
     Statement stmt = conn.createStatement();
     Log.getLogWriter().info("about to alter HDFSSTORE :  " + alterHdfsStoreDDL);
     stmt.execute(alterHdfsStoreDDL);    
     Log.getLogWriter().info("altered HDFSSTORE :  " + alterHdfsStoreDDL);      
   } catch (SQLException sqle) {
     SQLHelper.handleSQLException(sqle);
   }

    // validate SYS.SYSHDFSSTORES 
    String selectStmt = "select * from SYS.SYSHDFSSTORES where NAME='" + storeName.toUpperCase() + "'";
    Log.getLogWriter().info("query string = " + selectStmt);

    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(selectStmt);
      if (rs.next()) {
        Log.getLogWriter().info(ResultSetRowToString(rs));
        for (AlterHdfsStoreEntry entry : alterHdfsStoreEntries) {
          if (entry.getAlterType().equals(AlterType.DEFAULT)) {
            Log.getLogWriter().info("Processing entry " + entry.toString());
            String columnName = entry.getColumnName();
            Object expected = entry.getNewValue();
            Object found = rs.getObject(columnName);
            Log.getLogWriter().info("For " + columnName + " found = " + found);
            if (!expected.equals(found)) {
              Log.getLogWriter().info("Expected SYS.SYSHDFSSTORES(" + columnName + ") = " + expected + ", but found " + found);
              throw new TestException("Expected SYS.SYSHDFSSTORES(" + columnName + ") = " + expected + ", but found " + found);
            } else {
              Log.getLogWriter().info("SYS.SYSHDFSSTORES." + columnName + " returned expected value of " + found);
            }
          }
        }
      } else {
        throw new TestException("No results returned from " + selectStmt.toString());
      }
    } catch (SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }
  }

  private String ResultSetRowToString(ResultSet rs) {
    StringBuffer aStr = new StringBuffer();
    try {
      ResultSetMetaData rsmd = rs.getMetaData();
      int numColumns = rsmd.getColumnCount();
      aStr.append(rsmd.getTableName(1) + " has " + numColumns + " columns\n");
      for (int i = 1; i < numColumns; i++) {
        aStr.append("  " + rsmd.getColumnLabel(i) + " : " + rs.getObject(i) + "\n");
      }
    } catch (SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }
    return aStr.toString();
  }

  private String ResultSetToString(ResultSet rs) {
    StringBuffer aStr = new StringBuffer();
    try {
      int row = 1;
      while (rs.next()) {
        aStr.append("\nRow #" + row + " : ");
        aStr.append(ResultSetRowToString(rs));
        row++;
      }
    } catch (SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }
    return aStr.toString();
  }

  /** 
   * Methods for executing concurrent alterHdfsStore ops and verifying that 
   * the result is cumulative (all alterHdfsStore ops should be reflected in SYSHDFSSTORES).
   */
  static protected void logExecutionNumber() {
     long executionNumber = AlterHdfsStoreBB.getBB().getSharedCounters().incrementAndRead(AlterHdfsStoreBB.ExecutionNumber);
     Log.getLogWriter().info("Beginning task with execution number " + executionNumber);
  }

  public void checkForLastIteration() {
    checkForLastIteration(secondsToRun);
  }

  /** Check if we have run for the desired length of time. We cannot use
   *  hydra's taskTimeSec parameter because of a small window of opportunity
   *  for the test to hang due to the test's "concurrent round robin" type
   *  of strategy. Here we set a blackboard counter if time is up and this
   *  is the last concurrent round.
   */
  public static void checkForLastIteration(int numSeconds) {
    long taskStartTime = 0;
    final String bbKey = "TaskStartTime";
    Object anObj = AlterHdfsStoreBB.getBB().getSharedMap().get(bbKey);
    if (anObj == null) {
       taskStartTime = System.currentTimeMillis();
       AlterHdfsStoreBB.getBB().getSharedMap().put(bbKey, new Long(taskStartTime));
       Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
    } else {
       taskStartTime = ((Long)anObj).longValue();
    }
    if (System.currentTimeMillis() - taskStartTime >= numSeconds * 1000) {
       Log.getLogWriter().info("This is the last iteration of this task");
       AlterHdfsStoreBB.getBB().getSharedCounters().increment(AlterHdfsStoreBB.TimeToStop);
    } else {
       Log.getLogWriter().info("Running for " + numSeconds + " seconds; time remaining is " +
          (numSeconds - ((System.currentTimeMillis() - taskStartTime) / 1000)) + " seconds");
    }
  }

  public static synchronized void HydraTask_concAlterHdfsStore() {
    if (alterHdfsInstance == null) {
      alterHdfsInstance = new AlterHDFSTest();
    }     
    alterHdfsInstance.concAlterHdfsStore();
  }

  protected void concAlterHdfsStore() {
    Connection conn = getGFEConnection();
    concAlterHdfsStore(conn); 
    closeGFEConnection(conn);
  }

  protected void concAlterHdfsStore(Connection conn) {
    AlterType alterType;
    boolean leadMember = false;  // for HA tests, only one of the threads executing this task can recycle dataStores
    long readyToBegin = AlterHdfsStoreBB.getBB().getSharedCounters().incrementAndRead(AlterHdfsStoreBB.readyToBegin);

    if (readyToBegin == 1) {  // RW and WO share HDFS_AEQ attributes
      Log.getLogWriter().info("This thread is the alterHdfsStore leader");
      leadMember = true;
      logExecutionNumber();
      checkForLastIteration();
      alterType = AlterType.HDFS_AEQ;
    } else if (readyToBegin == 2) {  // use the appropriate (RW or WO) BUCKET_ORGANIZER attributes
      Map<String, String> hdfsExtnMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(SQLPrms.HDFS_EXTN_PARAMS);
      alterType = AlterType.RW_BUCKET_ORGANIZER;
      if ( hdfsExtnMap.get(tables[0].toUpperCase() + HDFSSqlTest.WRITEONLY) != null ) {
        alterType = AlterType.WO_BUCKET_ORGANIZER;
      } 
    } else {  // we will only have 3 VMs/validatorThreads in RW tests, so we can assign COMPACTION_MANAGER to the last thread
      alterType = AlterType.COMPACTION_MANAGER;
    }

    int numThreads = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
    TestHelper.waitForCounter(AlterHdfsStoreBB.getBB(),
                              "AlterHdfsStoreBB.readyToBegin",
                              AlterHdfsStoreBB.readyToBegin,
                              numThreads,
                              true,
                              -1,
                              5000);

    // if we've run long enough (secondsToRun), return and ask Master not to reschedule
    long counter = AlterHdfsStoreBB.getBB().getSharedCounters().read(AlterHdfsStoreBB.TimeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num executions is " + AlterHdfsStoreBB.getBB().getSharedCounters().read(AlterHdfsStoreBB.ExecutionNumber));
    }
    Log.getLogWriter().info("All " + numThreads + " threads at sync point (readyToBegin) alterHdfsStore");
    AlterHdfsStoreBB.getBB().getSharedCounters().zero(AlterHdfsStoreBB.finishedValidation);
    AlterHdfsStoreBB.clearAlteredAttributes();

    // for tests with HA, asynchronously recycle datastores while multiple members alterHdfsStore
    List <ClientVmInfo> vmList = null;
    if (leadMember && isHATest) {
      Log.getLogWriter().info("Asynchronously recycling datastore while multiple members alterHdfsStore attributes");
      int numToKill = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);
      Object[] tmpArr = StopStartVMs.getOtherVMs(numToKill, "store");
      vmList = (List<ClientVmInfo>)(tmpArr[0]);
      List<String> stopModeList = (List<String>)(tmpArr[1]);
      for (ClientVmInfo client : vmList) {
        PRObserver.initialize(client.getVmid());
      } //clear bb info for the vms to be stopped/started

      // asynchronously stop and start targeted datastore
      if (vmList.size() != 0) {
        StopStartVMs.stopStartAsync(vmList, stopModeList);
      }
    }
    
    alterHdfsStore(alterType);
    recordAlteredAttributes(alterType);

    AlterHdfsStoreBB.getBB().getSharedCounters().incrementAndRead(AlterHdfsStoreBB.completedOps);
    TestHelper.waitForCounter(AlterHdfsStoreBB.getBB(),
                              "AlterHdfsStoreBB.completedOps",
                              AlterHdfsStoreBB.completedOps,
                              numThreads,
                              true,
                              -1,
                              5000);
    Log.getLogWriter().info("All " + numThreads + " threads at completedOps sync point (after) alterHdfsStore");
    AlterHdfsStoreBB.getBB().getSharedCounters().zero(AlterHdfsStoreBB.readyToBegin);

    // wait for recycling/recovery to complete
    if (leadMember && isHATest) {
      int numOfPRs = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.numOfPRs);
      PRObserver.waitForRebalRecov(vmList, 1, numOfPRs, null, null, false);
    }

    validateAlteredAttributes(conn);

    AlterHdfsStoreBB.getBB().getSharedCounters().incrementAndRead(AlterHdfsStoreBB.finishedValidation);
    TestHelper.waitForCounter(AlterHdfsStoreBB.getBB(),
                              "AlterHdfsStoreBB.finishedValidation",
                              AlterHdfsStoreBB.finishedValidation,
                              numThreads,
                              true,
                              -1,
                              5000);
    Log.getLogWriter().info("All " + numThreads + " threads at completedOps sync point (after) alterHdfsStore");

    AlterHdfsStoreBB.getBB().getSharedCounters().zero(AlterHdfsStoreBB.completedOps);
 
  }

  /**
   * Write attributes changed to BB map (AlteredAttributes) for later use in validation
   */
  protected void recordAlteredAttributes(AlterType alterType) {
    for (AlterHdfsStoreEntry entry : alterHdfsStoreEntries) {
      if (entry.getAlterType().equals(alterType)) {
        AlterHdfsStoreBB.addAlteredAttribute(entry.getColumnName(), entry.getNewValue());
      }
    }
  }

  protected void validateAlteredAttributes(Connection conn) {
    Map attrMap = (Map)AlterHdfsStoreBB.getAlteredAttributes();

    String storeName = GfxdConfigPrms.getHDFSStoreConfig();
    String selectStmt = "select * from SYS.SYSHDFSSTORES where NAME='" + storeName.toUpperCase() + "'";
    Log.getLogWriter().info("query string = " + selectStmt);

    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(selectStmt);
      if (rs.next()) {
        Log.getLogWriter().info(ResultSetRowToString(rs));
        Iterator it = attrMap.keySet().iterator();
        while (it.hasNext()) {
          String columnName = (String)it.next();
          Object expected = attrMap.get(columnName);
          Object found = rs.getObject(columnName);
          Log.getLogWriter().info("For " + columnName + " found = " + found);
          if (!expected.equals(found)) {
            Log.getLogWriter().info("Expected SYS.SYSHDFSSTORES(" + columnName + ") = " + expected + ", but found " + found);
            throw new TestException("Expected SYS.SYSHDFSSTORES(" + columnName + ") = " + expected + ", but found " + found);
          } else {
            Log.getLogWriter().info("SYS.SYSHDFSSTORES." + columnName + " returned expected value of " + found);
          }
        }
      } else {
        throw new TestException("No results returned from " + selectStmt.toString());
      }
    } catch (SQLException sqle) {
      SQLHelper.handleSQLException(sqle);
    }
  }

  /**
   * Internal class depicting the range for values and a method for getting a randomized new value
   * within that range.
   */
  private static class ValueRange {
    Object low;  // low end of range
    Object high; // high end of range

    /** Constructor */
    public ValueRange(Object low, Object high) {
      this.low = low;
      this.high = high;
    }

    public Object getNewValue() {
      Object newValue;
      if (this.low instanceof Integer) {
        int min = ((Integer)low).intValue();
        int max = ((Integer)high).intValue();
        newValue = new Integer(((GsRandom)random).nextInt(min, max));
      } else {
        throw new TestException("Unexpected object type " + this.low.getClass().getName() + " encountered by " + this.getClass().getName());
      }
      return newValue;
    }
  
    public String toString() {
      return ("low = " + low + " high = " + high);
    }
  }
  
  /** 
   * Internal class for handling alter hdfs store settings and maintaining those values for validation against SYS.SYSHDFSSTORES
   */
  public static enum SQLTimeUnit { MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS; }
  public enum AlterType { 
    DEFAULT,
    RW_BUCKET_ORGANIZER, 
    WO_BUCKET_ORGANIZER, 
    HDFS_AEQ, 
    COMPACTION_MANAGER_DISABLE_MINOR,
    COMPACTION_MANAGER_DISABLE_MAJOR,
    COMPACTION_MANAGER;

    public static AlterType getRandom() {
      return values()[(int) (random.nextInt(values().length-1))];
    }
  }

  private static class AlterHdfsStoreEntry {
    String attributeName; // from HDFSStore
    String columnName;    // from SYS.SYSHDFSSTORES
    AlterType alterType;  // defines groupings of related attributes for use in alter hdfsstore
    ValueRange valueRange;     // allowable range for values
    Object defaultValue;  // Boolean or Integer default value
    Object newValue;      // Boolean or Integer value for alter hdfsstore (also used in validation)
    SQLTimeUnit timeUnit; 

    /** Constructor */
    public AlterHdfsStoreEntry(String attributeName, String columnName, AlterType alterType, Object defaultValue, ValueRange valueRange, Object newValue, SQLTimeUnit timeUnit) {
      this.attributeName = attributeName;
      this.columnName = columnName;
      this.alterType = alterType;
      this.defaultValue = defaultValue;
      this.valueRange = valueRange;
      this.newValue = newValue;
      this.timeUnit = timeUnit;
    }

    public String toString() {
      StringBuffer aStr = new StringBuffer();
      aStr.append("AlterHdfsStoreEntry: \n");
      aStr.append("   attributeName: " + this.attributeName + "\n");
      aStr.append("   columnName: " + this.columnName + "\n");
      aStr.append("   alterType: " + this.alterType + "\n");
      aStr.append("   defaultValue: " + this.defaultValue + "\n");
      //aStr.append("   valueRange: " + this.valueRange.toString() + "\n");
      aStr.append("   newValue: " + this.newValue + "\n");
      aStr.append("   timeUnit: " + this.timeUnit + "\n");
      return aStr.toString();
    }

    public String getAttributeName() {
      return this.attributeName;
    }

    public String getColumnName() {
      return this.columnName;
    }

    public AlterType getAlterType() {
      return this.alterType;
    }

    public Object getDefaultValue() {
      return this.defaultValue;
    }

    public Object getNewValue() {
      return this.newValue;
    }

    public SQLTimeUnit getTimeUnit() {
      return this.timeUnit;
    }

    /* set and return new value based on ValueRange */
    public Object setNewValue() {
      Object val;
      if (this.alterType.equals(AlterType.DEFAULT)) {
        val = this.defaultValue;
      } else if (this.defaultValue instanceof Boolean) {
        val = new Boolean(random.nextBoolean());
      } else {
        val = this.valueRange.getNewValue();
      }
      this.newValue = this.getNewValue();
      return this.newValue;
    }
  }
}

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
package com.pivotal.gemfirexd.dbsync;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.junit.JDBC;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.callbacks.AsyncEventHelper;

public class SerialDBSynchronizerDUnit extends DBSynchronizerBasicDUnit {
  public SerialDBSynchronizerDUnit(String name) {
    super(name);
  }
  
  protected Runnable createAsyncQueueConfigurationForBasicTests(String derbyUrl) {
    return getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.callbacks.DBSynchronizer",
        "org.apache.derby.jdbc.ClientDriver", derbyUrl, true,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "org.apache.derby.jdbc.ClientDriver," + derbyUrl, false);
  }
  
  public void _testPrimaryDBSynchronizerOnAccessorNodeWithReplicateTable() throws Exception {
    
  }
  
  
  /**
   * Verifies that error XML is generated in case underlying database
   * has different schema than that in GemFireXD.
   * The test creates a table with one extra column in GemFireXD than
   * in Derby, causing errors while applying inserts to Derby. 
   */
  public void DISABLED_BUG_50101_testBug48674() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    String derbyDbUrl = getDerbyURL(this.netPort);
    server = startNetworkServer();
    // createDerbyValidationArtefacts();

    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    derbyConn = DriverManager.getConnection(derbyDbUrl);
    // create schema
    derbyStmt = derbyConn.createStatement();
    final String schemaName = getCurrentDefaultSchemaName();
    ResultSet rs = derbyConn.getMetaData().getTables(null, schemaName,
        "TESTTABLE", null);
    boolean foundTesttable = false;
    while (rs.next()) {
      foundTesttable = rs.getString(3).equalsIgnoreCase("TESTTABLE")
          && rs.getString(2).equalsIgnoreCase(schemaName);
      if (foundTesttable) {
        // delete previous entries
        derbyStmt.executeUpdate("delete from TESTTABLE");
        break;
      }
    }
    if (!foundTesttable) {
      derbyStmt
          .execute("create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int ) ");
    }

    startVMs(0, 4, 0, "SG1", null);

    addExpectedException(new int[] {}, new int[] { 1, 2, 3, 4 },
        new Object[] { "org.apache.derby.client.am.SqlException", "java.sql.SQLIntegrityConstraintViolationException",
         "java.sql.SQLTransactionRollbackException", "java.sql.SQLSyntaxErrorException"});

    
    Runnable createWBCLConfig = getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.callbacks.DBSynchronizer",
        "org.apache.derby.jdbc.ClientDriver", derbyDbUrl, false,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "org.apache.derby.jdbc.ClientDriver," + derbyDbUrl, false);
    serverExecute(1, createWBCLConfig);

    serverSQLExecute(1, "create table TESTTABLE (ID int not null primary key, "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int, ID2 int)"
        + "PARTITION BY PRIMARY KEY AsyncEventListener (WBCL1) server groups (SG1)");

    //Runnable startWBCL = startAsyncEventListener("WBCL1");
    //serverExecute(1, startWBCL);

    // Do an insert in gfxd
    serverSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1, 1)");
    serverSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2, 2)");
    serverSQLExecute(1, "Insert into TESTTABLE values(3,'desc3','Add3',3, 3)");
    serverSQLExecute(1, "Insert into TESTTABLE values(4,'desc4','Add4',4, 4)");

    // wait for AsyncQueue flush
    serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 0, -1)");

    serverExecute(1, stopAsyncEventListener("WBCL1"));
    serverSQLExecute(1, "drop table TESTTABLE");
    serverExecute(1, dropAsyncEventListener("WBCL1"));
    
    // This is the path upto controller's directory
    String systemDir = this.getSysDiskDir();
    
    try {
      derbyStmt.execute("drop table TESTTABLE");
      derbyStmt.close();
      derbyConn.close();
      
    } catch (SQLException e) {
      // ignore
    }
    
    // We go one dir up
    String xmlLogLocation = systemDir.substring(0,systemDir.lastIndexOf(File.separator));
    String logRoot = xmlLogLocation + File.separator + AsyncEventHelper.EVENT_ERROR_LOG_FILE;
    String logEntries = xmlLogLocation + File.separator + AsyncEventHelper.EVENT_ERROR_LOG_ENTRIES_FILE;
    File logRootFile = new File(logRoot);
    getLogWriter().info("LOG ROOT FILE:" + logRoot);
    assertTrue(logRootFile.exists());
    getLogWriter().info("LOG ENTRIES FILE:" + logEntries);
    File logEntriesFile = new File(logEntries);
    assertTrue(logEntriesFile.exists());
    
    stopAllVMs();
  }
  
  
  
  /**
   * Verifies that error XML is generated in case of event failures
   * while applying to underlying database.
   * The test creates a table with unique column while leaving out the
   * constraint in GemFireXD, allowing events to be enqueued but failing
   * to be applied to Derby. 
   */
  public void DISABLED_BUG_50101_testDBSyncErrorXMLGeneration() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    String derbyDbUrl = getDerbyURL(this.netPort);
    server = startNetworkServer();
    // createDerbyValidationArtefacts();

    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    derbyConn = DriverManager.getConnection(derbyDbUrl);
    // create schema
    derbyStmt = derbyConn.createStatement();
    final String schemaName = getCurrentDefaultSchemaName();
    ResultSet rs = derbyConn.getMetaData().getTables(null, schemaName,
        "TESTTABLE", null);
    boolean foundTesttable = false;
    while (rs.next()) {
      foundTesttable = rs.getString(3).equalsIgnoreCase("TESTTABLE")
          && rs.getString(2).equalsIgnoreCase(schemaName);
      if (foundTesttable) {
        // delete previous entries
        derbyStmt.executeUpdate("delete from TESTTABLE");
        break;
      }
    }
    if (!foundTesttable) {
      derbyStmt
          .execute("create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int,"
              + "constraint unique_id1 UNIQUE(ID1)) ");
    }

    startVMs(0, 4, 0, "SG1", null);

    addExpectedException(new int[] {}, new int[] { 1, 2, 3, 4 },
        new Object[] { "org.apache.derby.client.am.SqlException", "java.sql.SQLIntegrityConstraintViolationException",
         "java.sql.SQLTransactionRollbackException"});

    
    Runnable createWBCLConfig = getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.callbacks.DBSynchronizer",
        "org.apache.derby.jdbc.ClientDriver", derbyDbUrl, true,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "org.apache.derby.jdbc.ClientDriver," + derbyDbUrl, false);
    serverExecute(1, createWBCLConfig);

    serverSQLExecute(1, "create table TESTTABLE (ID int not null primary key, "
        + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int)"
        + " AsyncEventListener (WBCL1) server groups (SG1)");

    Runnable startWBCL = startAsyncEventListener("WBCL1");
    serverExecute(1, startWBCL);

    // Do an insert in gfxd
    serverSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
    serverSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',1)");
    serverSQLExecute(1, "Insert into TESTTABLE values(3,'desc3','Add3',2)");
    serverSQLExecute(1, "Insert into TESTTABLE values(4,'desc4','Add4',2)");

    // wait for AsyncQueue flush
    serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 0, -1)");

    serverExecute(1, stopAsyncEventListener("WBCL1"));
    serverSQLExecute(1, "drop table TESTTABLE");
    serverExecute(1, dropAsyncEventListener("WBCL1"));
    
    // This is the path upto controller's directory
    String systemDir = this.getSysDiskDir();
    
    
    
    try {
      derbyStmt.execute("drop table TESTTABLE");
      derbyStmt.close();
      derbyConn.close();
    } catch (SQLException e) {
      // Ignore
    }
    
    // We go one dir up
    String xmlLogLocation = systemDir.substring(0,systemDir.lastIndexOf(File.separator));
    String logRoot = xmlLogLocation + File.separator + AsyncEventHelper.EVENT_ERROR_LOG_FILE;
    String logEntries = xmlLogLocation + File.separator + AsyncEventHelper.EVENT_ERROR_LOG_ENTRIES_FILE;
    File logRootFile = new File(logRoot);
    getLogWriter().info("LOG ROOT FILE:" + logRoot);
    assertTrue(logRootFile.exists());
    getLogWriter().info("LOG ENTRIES FILE:" + logEntries);
    File logEntriesFile = new File(logEntries);
    assertTrue(logEntriesFile.exists());
    
    stopAllVMs();
    //logRootFile.delete();
    //logEntriesFile.delete();
    
  }


  protected CallbackObserverAdapter getCallbackObserver() {
    CallbackObserverAdapter observerAdapter = new CallbackObserverAdapter() {
      private int numTimesStartCalled = 0;
      private int numTimesCloseCalled = 0;
      @Override
      public void asyncEventListenerStart() {
        getLogWriter().info("CallbackObserver::asyncEventListenerStart");
        ++numTimesStartCalled;
        // Must be called exactly once
        assertEquals(1, numTimesStartCalled);
      }
      @Override
      public void asyncEventListenerClose() {
        getLogWriter().info("CallbackObserver::asyncEventListenerClose");
        ++numTimesCloseCalled;
        // Must be called atleast once
        assertTrue(numTimesCloseCalled >= 1);
      }
    };
    return observerAdapter;
  }
  
  /**
   * Verifies that start and close methods on GemFireXD's
   * AsyncEventListener interface are invoked correctly. 
   */
  public void testBug49975() throws Exception {
    
    // Test manual start
    startVMs(0, 4, 0, "SG1", null);
    Runnable createWBCLConfig = getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestStartStopGatewayEventListener",
        "org.apache.derby.jdbc.ClientDriver", "", true,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "org.apache.derby.jdbc.ClientDriver," + "", false);
    
    CallbackObserverAdapter observerAdapter1 = getCallbackObserver();
    
    Object[] args = {observerAdapter1};
    invokeInEveryVM(CallbackObserverHolder.class, new String("setInstance"), args);
    
    serverExecute(1, createWBCLConfig);

    Runnable startWBCL = startAsyncEventListener("WBCL1");
    serverExecute(1, startWBCL);
    
    Runnable stopWBCL = stopAsyncEventListener("WBCL1");
    serverExecute(1, stopWBCL);
    
    serverExecute(1, dropAsyncEventListener("WBCL1"));
    
    stopAllVMs();
    
    // Test automatic start
    restartVMNums(new int[]{-1,-2,-3,-4}, 0, "SG1", null);
    
    Runnable createWBCLConfig2 = getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestStartStopGatewayEventListener",
        "org.apache.derby.jdbc.ClientDriver", "", false,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "org.apache.derby.jdbc.ClientDriver," + "", false);
    
    CallbackObserverAdapter observerAdapter2 = getCallbackObserver();
    
    Object[] args2 = {observerAdapter1};
    invokeInEveryVM(CallbackObserverHolder.class, new String("setInstance"), args2);
    
    serverExecute(1, createWBCLConfig);
    serverExecute(1, stopWBCL);
    serverExecute(1, dropAsyncEventListener("WBCL1"));
    stopAllVMs();
    
  }
  
  
  public void testBug51353() throws Exception {
    startVMs(0, 4, 0, "SG1", null);
    Runnable createListener = getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestAsyncEventListener",
        "", "", false,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "", false);

    serverExecute(1, createListener);
    
    Runnable stopListener = stopAsyncEventListener("WBCL1");
    serverExecute(1, stopListener);

    Runnable startListener = startAsyncEventListener("WBCL1");
    serverExecute(1, startListener);
    
    stopAllVMs();
    
    restartVMNums(new int[]{-1,-2,-3,-4}, 0, "SG1", null);

    serverExecute(1, dropAsyncEventListener("WBCL1"));
    stopAllVMs();
  }

  
  public void testDBSynchronizerWithTruncateTable_Replicate() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " REPLICATE AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = getExecutorForWBCLConfiguration("SG1", "WBCL1",
          "com.pivotal.gemfirexd.callbacks.DBSynchronizer",
          "org.apache.derby.jdbc.ClientDriver", derbyDbUrl, true,
          Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
          "org.apache.derby.jdbc.ClientDriver," + derbyDbUrl, false);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      
      // Do an insert in gfxd
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      clientSQLExecute(1, "Insert into TESTTABLE values(3,'desc3','Add3',3)");
      clientSQLExecute(1, "Insert into TESTTABLE values(4,'desc4','Add4',4)");
      
      //truncate deletes complete data from the table
      clientSQLExecute(1, "truncate table TESTTABLE");
      
      Connection conn = TestUtil.jdbcConn;
      Statement stmt = conn.createStatement();
      ResultSet gfxdRs = stmt.executeQuery("select count(*) from TESTTABLE");
      JDBC.assertSingleValueResultSet(gfxdRs, "0");
      
      //wait for AsyncQueue flush
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      //derby should have 4 rows because truncate table as DDL won't be propagated to derby
      ResultSet derbyRS = derbyStmt.executeQuery("select count(*) from TESTTABLE");
      JDBC.assertSingleValueResultSet(derbyRS, "4");

    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
}

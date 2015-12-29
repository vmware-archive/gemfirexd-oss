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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.derby.drda.NetworkServerControl;


public class DBSynchronizerBasicDUnit extends DBSynchronizerTestBase {
  
  public DBSynchronizerBasicDUnit(String name) {
    super(name);
  }
  
  protected Runnable createAsyncQueueConfigurationForBasicTests(String derbyUrl) {
    return getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.callbacks.DBSynchronizer",
        "org.apache.derby.jdbc.ClientDriver", derbyUrl, true,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "org.apache.derby.jdbc.ClientDriver," + derbyUrl, false);
  }
  
  public void testBasicInsert() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void testBasic_jsonCol() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL();
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      derbyStmt.execute("CREATE table t1(col1 int primary key, " +
      		"col2 varchar(100))");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "CREATE table t1(col1 int primary key, col2 json) persistent "
              + "partition by (col1) AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into t1 values(1,'{\n  \"f1\" : 1,\n  \"f2\" : true\n}')");
      clientSQLExecute(1, "Insert into t1 values(2,'{\n  \"f3\" : 1,\n  \"f4\" : true\n}')");
      clientSQLExecute(1, "Insert into t1 values(3,'{\n  \"f5\" : 1,\n  \"f6\" : true\n}')");
      clientSQLExecute(1, "Insert into t1 values(4,'{\n  \"f7\" : 1,\n  \"f8\" : true\n}')");
      clientSQLExecute(1, "Insert into t1 values(5, NULL)");
      clientSQLExecute(1, "delete from t1 where col1 = 2");
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from t1", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void testBasicUpdate() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      clientSQLExecute(1, "Update TESTTABLE set DESCRIPTION = 'desc1Mod' where DESCRIPTION = 'desc1'");
      clientSQLExecute(1, "Update TESTTABLE set DESCRIPTION = 'desc2Mod' where ID = 2");
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void testBasicDelete() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      clientSQLExecute(1, "delete from TESTTABLE where ID = 1");
      clientSQLExecute(1, "delete from TESTTABLE where ID = 2");
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void testBatchInsert() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do batch insert in sql fabric
      clientExecute(1, doBatchInsert());
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void testBatchUpdate() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      // Do batch update in sql fabric
      clientExecute(1, doBatchUpdate());
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void testBatchDelete() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null primary key , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      // Do batch delete in sql fabric
      clientExecute(1, doBatchDelete());
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void testInsertOnNonPKBasedTable() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  /**
   * Defect #48681
   */
  public void _testUpdateOnNonPKBasedTable() throws Exception {
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
          "create table TESTTABLE (ID int not null , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      // Do updates
      clientSQLExecute(1, "Update TESTTABLE set DESCRIPTION = 'desc1Mod' where ID = 1");
      clientSQLExecute(1, "Update TESTTABLE set DESCRIPTION = 'desc2Mod' where ID = 2");
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }
  
  public void _testDeleteOnNonPKBasedTable() throws Exception {
    Statement derbyStmt = null;
    Connection derbyConn = null;
    NetworkServerControl server = null;
    try {
      String derbyDbUrl = getDerbyURL(this.netPort);
      server = startNetworkServer();
      createDerbyValidationArtefacts();
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
      getLogWriter().info("Started derby network server");
      
      startClientVMs(1, 0, null);
      startServerVMs(2, -1, "SG1");
      getLogWriter().info("Started the accessor and datastore vms");
      
      clientSQLExecute(1,
          "create table TESTTABLE (ID int not null , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024), ID1 int )"
              + " AsyncEventListener (WBCL1) ");

      Runnable createWBCLConfig = createAsyncQueueConfigurationForBasicTests(derbyDbUrl);
      clientExecute(1, createWBCLConfig);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      getLogWriter().info("Created and started AsyncEventListener WBCL1");
      
      // Do an insert in sql fabric
      clientSQLExecute(1, "Insert into TESTTABLE values(1,'desc1','Add1',1)");
      clientSQLExecute(1, "Insert into TESTTABLE values(2,'desc2','Add2',2)");
      
      clientSQLExecute(1, "delete from TESTTABLE where ID = 1");
      clientSQLExecute(1, "delete from TESTTABLE where ID = 2");
      
      // check that queue is empty.
      serverSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('WBCL1', 1, 30)");
      
      validateResults(derbyStmt, "select * from testtable", this.netPort, true);
    }
    finally {
      derbyCleanup(derbyStmt, derbyConn, server);
    }
  }

}

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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;

import io.snappydata.test.dunit.SerializableRunnable;


@SuppressWarnings("serial")
public class MultipleInsertsLeveragingPutAllDUnit extends
    DistributedSQLTestBase {

  private final String goldenTextFile = TestUtil.getResourcesDir()
  + "/lib/checkQuery.xml";
  
  public MultipleInsertsLeveragingPutAllDUnit(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  public static class BatchInsertObserver extends GemFireXDQueryObserverAdapter {

    private int batchSize;
    private boolean posDup;
    @Override
    public void insertMultipleRowsBeingInvoked(int numElements) {
      this.batchSize = numElements;
    }

    public int getBatchSize() {
      return this.batchSize;
    }

    public void clear() {
      this.batchSize = 0;
    }
    
    @Override
    public void putAllCalledWithMapSize(int size) {
      this.batchSize = size;
    }
    
    public void afterGlobalIndexInsert(boolean posDup){
      if (!this.posDup){
        this.posDup = posDup ;
        throw new CacheClosedException();
      }
    }
  }

  public static class BulkGetObserver extends GemFireXDQueryObserverAdapter {
    private int batchSize;

    @Override
    public void getAllInvoked(int numElements) {
      this.batchSize = numElements;
    }

    public int getBatchSize() {
      return this.batchSize;
    }
  }

  private BatchInsertObserver bos;
  
  private void setUpBatchObserver() {
    bos = new BatchInsertObserver();
    GemFireXDQueryObserverHolder.setInstance(bos);
  }

  private void checkObservedNumber(Integer num) {
    assertEquals(num.intValue(), bos.getBatchSize());
  }

  public void testBug47444() throws Exception {
    startVMs(1, 1);
    serverExecute(1, new SerializableRunnable("") {
      @Override
      public void run() throws CacheException {
        GemFireXDQueryObserverHolder.setInstance(new BatchInsertObserver());
      }
    });
    
    serverSQLExecute(1, "create schema emp");
    serverSQLExecute(1, "create table emp.EMPLOYEE(lastname varchar(30) primary key, depId int) " +
                "partition by (depId) redundancy 0" + getSuffix());
    setUpBatchObserver();
    PreparedStatement pstmnt = TestUtil.getPreparedStatement("INSERT INTO emp.employee VALUES (?, ?)");
    pstmnt.setString(1, "Jones");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();
    
    int[] status = pstmnt.executeBatch();
    checkObservedNumber(1);
    assertEquals(status[0], 1);
    String jdbcSQL = "select * from emp.Employee";
    sqlExecuteVerify(null, new int[] {1}, jdbcSQL, goldenTextFile, "singleInsertBatch");
  }
  
  public void testStatementBatchInsert_NoPutAll() throws Exception {
    startVMs(1, 3);
    serverSQLExecute(1, "create schema emp");
    serverSQLExecute(1, "create table emp.EMPLOYEE(lastname varchar(30) primary key, depId int) " +
                "partition by (depId)"+ getSuffix());
    setUpBatchObserver();
    Statement stmnt = TestUtil.getStatement();
    stmnt.addBatch("insert into emp.employee VALUES ('Jones', 33)");
    stmnt.addBatch("insert into emp.employee VALUES ('Rafferty', 31)");
    stmnt.addBatch("insert into emp.employee VALUES ('Robinson', 34)");
    stmnt.addBatch("insert into emp.employee VALUES ('Steinberg', 33)");
    stmnt.addBatch("create table emp.dummy(id int not null)"+ getSuffix());
    stmnt.addBatch("insert into emp.dummy VALUES (1), (2), (3), (4)");
    stmnt.addBatch("insert into emp.employee VALUES ('Smith', 34)");
    stmnt.addBatch("insert into emp.employee VALUES ('John', null)");

    stmnt.executeBatch();
    checkObservedNumber(4);
    
    String jdbcSQL = "select * from emp.Employee";
    sqlExecuteVerify(null, new int[] {1}, jdbcSQL, goldenTextFile, "multInsert");
  }
  
  public void testPrepStatementBatchInsert() throws Exception {
    startVMs(1, 3);
    serverSQLExecute(1, "create schema emp");
    serverSQLExecute(1, "create table emp.EMPLOYEE(lastname varchar(30) primary key, depId int) " +
                "partition by (depId)"+ getSuffix());
    setUpBatchObserver();
    PreparedStatement pstmnt = TestUtil.getPreparedStatement("INSERT INTO emp.employee VALUES (?, ?)");
    pstmnt.setString(1, "Jones");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "Rafferty");
    pstmnt.setInt(2, 31);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "Robinson");
    pstmnt.setInt(2, 34);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "Steinberg");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "Smith");
    pstmnt.setInt(2, 34);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "John");
    pstmnt.setNull(2, Types.INTEGER);
    pstmnt.addBatch();
    
    int[] status = pstmnt.executeBatch();
    checkObservedNumber(6);
    for(int i=0; i<6; i++) {
      assertEquals(status[i], 1);
    }
    String jdbcSQL = "select * from emp.Employee";
    sqlExecuteVerify(null, new int[] {1}, jdbcSQL, goldenTextFile, "multInsert");
  }

  public void testMultipleInsertSameSQLStmnt() throws Exception {
    startVMs(1, 3);
    serverSQLExecute(1, "create schema emp");
    serverSQLExecute(1, "create table emp.EMPLOYEE(lastname varchar(30), depId int) " +
                "partition by (depId)"+ getSuffix());
    setUpBatchObserver();
    clientSQLExecute(1, "insert into emp.employee values " +
                "('Jones', 33), ('Rafferty', 31), " +
                "('Robinson', 34), ('Steinberg', 33), " +
                "('Smith', 34), ('John', null)");
    checkObservedNumber(6);
    String jdbcSQL = "select * from emp.Employee";
    sqlExecuteVerify(null, new int[] {1}, jdbcSQL, goldenTextFile, "multInsert");
  }

  public void testMultipleInsertSamePreptmnt() throws Exception {
    startVMs(1, 3);
    serverSQLExecute(1, "create schema emp");
    serverSQLExecute(1, "create table emp.EMPLOYEE(lastname varchar(30), depId int) " +
                "partition by (depId)"+ getSuffix());
    setUpBatchObserver();
    PreparedStatement pstmnt = TestUtil.getPreparedStatement("INSERT INTO emp.employee VALUES (?, ?)");
    pstmnt.setString(1, "Jones");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "Rafferty");
    pstmnt.setInt(2, 31);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "Robinson");
    pstmnt.setInt(2, 34);
    pstmnt.addBatch();
    
    int[] status = pstmnt.executeBatch();
    checkObservedNumber(3);
    for(int i=0; i<3; i++) {
      assertEquals(status[i], 1);
    }
    String jdbcSQL = "select * from emp.Employee";
    
    pstmnt.setString(1, "Steinberg");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "Smith");
    pstmnt.setInt(2, 34);
    pstmnt.addBatch();
    
    pstmnt.setString(1, "John");
    pstmnt.setNull(2, Types.INTEGER);
    pstmnt.addBatch();
    
    status = pstmnt.executeBatch();
    checkObservedNumber(3);
    for(int i=0; i<3; i++) {
      assertEquals(status[i], 1);
    }
    sqlExecuteVerify(null, new int[] {1}, jdbcSQL, goldenTextFile, "multInsert");
    
  }

  public void testPrepStatementBatchInsert_WithInvalidInsert() throws Exception {

    Properties props = new Properties();
    props.setProperty(Attribute.TX_SYNC_COMMITS, "true");

    startVMs(1, 3, 0, null, props);
    serverSQLExecute(1, "create schema emp");
    serverSQLExecute(1,
        "create table emp.EMPLOYEE(lastname varchar(30) primary key, depId int) "
            + "partition by (depId)"+ getSuffix());
    setUpBatchObserver();
    PreparedStatement pstmnt = TestUtil
        .getPreparedStatement("INSERT INTO emp.employee VALUES (?, ?)");
    pstmnt.setString(1, "Jones");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();

    pstmnt.setString(1, "Rafferty");
    pstmnt.setInt(2, 31);
    pstmnt.addBatch();

    pstmnt.setString(1, "Robinson");
    pstmnt.setInt(2, 34);
    pstmnt.addBatch();

    pstmnt.setString(1, "Rafferty");
    pstmnt.setInt(2, 351);
    pstmnt.addBatch();
    int[] status = null;
    try {
      status = pstmnt.executeBatch();
      fail("expected a constraint violation");
    } catch (SQLException sqle) {
      if (!"23505".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    final String jdbcSQL = "select * from emp.Employee";

    // delete anything that might have been added in the failure case
    clientSQLExecute(1, "delete from emp.Employee");

    pstmnt.setString(1, "Jones");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();

    pstmnt.setString(1, "Rafferty");
    pstmnt.setInt(2, 31);
    pstmnt.addBatch();

    pstmnt.setString(1, "Robinson");
    pstmnt.setInt(2, 34);
    pstmnt.addBatch();

    pstmnt.setString(1, "Steinberg");
    pstmnt.setInt(2, 33);
    pstmnt.addBatch();

    pstmnt.setString(1, "Smith");
    pstmnt.setInt(2, 34);
    pstmnt.addBatch();

    pstmnt.setString(1, "John");
    pstmnt.setNull(2, Types.INTEGER);
    pstmnt.addBatch();

    status = pstmnt.executeBatch();
    checkObservedNumber(6);
    bos.clear();
    for (int i = 0; i < 6; i++) {
      assertEquals(status[i], 1);
    }

    sqlExecuteVerify(null, new int[] { 1 }, jdbcSQL, goldenTextFile,
        "multInsert");

    // try the same with transaction in which case no delete will be required
    int[] isolationLevels = new int[] { Connection.TRANSACTION_READ_COMMITTED,
        Connection.TRANSACTION_REPEATABLE_READ };
    for (final int isolationLevel : isolationLevels) {
      clientSQLExecute(1, "delete from emp.Employee");

      TestUtil.jdbcConn.setTransactionIsolation(isolationLevel);
      pstmnt.setString(1, "Jones");
      pstmnt.setInt(2, 33);
      pstmnt.addBatch();

      pstmnt.setString(1, "Rafferty");
      pstmnt.setInt(2, 31);
      pstmnt.addBatch();

      pstmnt.setString(1, "Robinson");
      pstmnt.setInt(2, 34);
      pstmnt.addBatch();

      pstmnt.setString(1, "Rafferty");
      pstmnt.setInt(2, 351);
      pstmnt.addBatch();
      try {
        status = pstmnt.executeBatch();
        fail("expected a constraint violation");
      } catch (SQLException sqle) {
        if (!"23505".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }

      pstmnt.setString(1, "Jones");
      pstmnt.setInt(2, 33);
      pstmnt.addBatch();

      pstmnt.setString(1, "Rafferty");
      pstmnt.setInt(2, 31);
      pstmnt.addBatch();

      pstmnt.setString(1, "Robinson");
      pstmnt.setInt(2, 34);
      pstmnt.addBatch();

      pstmnt.setString(1, "Steinberg");
      pstmnt.setInt(2, 33);
      pstmnt.addBatch();

      pstmnt.setString(1, "Smith");
      pstmnt.setInt(2, 34);
      pstmnt.addBatch();

      pstmnt.setString(1, "John");
      pstmnt.setNull(2, Types.INTEGER);
      pstmnt.addBatch();

      status = pstmnt.executeBatch();
      checkObservedNumber(6);
      bos.clear();
      for (int i = 0; i < 6; i++) {
        assertEquals(status[i], 1);
      }

      TestUtil.jdbcConn.commit();
      // for READ_COMMITTED need to wait for any pending commit before reading
      // else may read partial commit data on remote server;
      // for REPEATABLE_READ no need to wait since commit atomicity is
      // guaranteed and commit already waits for phase1 to be complete
      // [sumedh] now using sync-commits on connection instead
      /*
      if (isolationLevel == Connection.TRANSACTION_READ_COMMITTED) {
        TXManagerImpl.waitForPendingCommitForTest();
      }
      */

      serverExecute(1, new SerializableRunnable() {
        @Override
        public void run() {
          try {
            Connection conn = TestUtil.jdbcConn;
            conn.setTransactionIsolation(isolationLevel);
            TestUtil.sqlExecuteVerifyText(jdbcSQL, goldenTextFile,
                "multInsert", true, false);
            conn.commit();
          } catch (Exception e) {
            getLogWriter().error("unexpected exception", e);
            fail("unexpected exception", e);
          }
        }
      });
    }
  }

  public void testPrepStatementBatchInsert_WithInvalidInsert_andThenReuseSamePrepStmnt() throws Exception {

  }

  public void testMultipleInsert() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    // Both PR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null)");
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data" + getSuffix());
    int actual = s.executeUpdate("insert into backup select * from account");
    assertEquals(4, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    
    // Both RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null) replicate"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate" + getSuffix());
    actual = s.executeUpdate("insert into backup select * from account");
    assertEquals(4, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    
    // RR PR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null) replicate"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account");
    assertEquals(4, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    
    // PR RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null)"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account");
    assertEquals(4, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
  }

  public void testMultipleInsertWithWhereClause() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    // Both PR
    Statement s = conn.createStatement();
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "partition by range(balance) (values between 100 and 350, values between 350 and 450)"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data"+ getSuffix());
    int actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // Both RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "replicate"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // RR PR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "replicate"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // PR RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "partition by range(balance) (values between 100 and 350, values between 350 and 450)"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
  }
  
  public void testMultipleInsert_prepStmnt() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    // BOTH PR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null)"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data"+ getSuffix());
    PreparedStatement ps = conn.prepareStatement("insert into backup select * from account where id > ?");
    ps.setInt(1, 0);
    int actual = ps.executeUpdate();
    assertEquals(4, actual);
    
    s.execute("create table backup2 as select * from account with no data"+ getSuffix());
    ps = conn.prepareStatement("insert into backup2 select * from account where id > ?");
    ps.setInt(1, 10);
    actual = ps.executeUpdate();
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // BOTH RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null) replicate"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate"+ getSuffix());
    ps = conn.prepareStatement("insert into backup select * from account where id > ?");
    ps.setInt(1, 0);
    actual = ps.executeUpdate();
    assertEquals(4, actual);
    
    s.execute("create table backup2 as select * from account with no data replicate"+ getSuffix());
    ps = conn.prepareStatement("insert into backup2 select * from account where id > ?");
    ps.setInt(1, 10);
    actual = ps.executeUpdate();
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // RR PR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null) replicate"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data"+ getSuffix());
    ps = conn.prepareStatement("insert into backup select * from account where id > ?");
    ps.setInt(1, 0);
    actual = ps.executeUpdate();
    assertEquals(4, actual);
    
    s.execute("create table backup2 as select * from account with no data"+ getSuffix());
    ps = conn.prepareStatement("insert into backup2 select * from account where id > ?");
    ps.setInt(1, 10);
    actual = ps.executeUpdate();
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // PR RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance2 int not null)"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate"+ getSuffix());
    ps = conn.prepareStatement("insert into backup select * from account where id > ?");
    ps.setInt(1, 0);
    actual = ps.executeUpdate();
    assertEquals(4, actual);
    
    s.execute("create table backup2 as select * from account with no data replicate"+ getSuffix());
    ps = conn.prepareStatement("insert into backup2 select * from account where id > ?");
    ps.setInt(1, 10);
    actual = ps.executeUpdate();
    assertEquals(0, actual);
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
  }
  
  // pbc - partition by column
  public void testMultipleInsertWithWhereClause_pbc() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    
    // Both PR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "partition by column(name) "+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data"+ getSuffix());
    int actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where name = 'name3'");
    assertEquals(1, actual);
    
    s.execute("select * from backup2");
    ResultSet rs = s.getResultSet();
    assertTrue(rs.next());
    
    assertEquals(300, rs.getInt(4));
    assertEquals(3, rs.getInt(1));
    assertEquals("name3", rs.getString(2));
    assertEquals("addr3", rs.getString(3));
    assertEquals(300, rs.getInt(4));
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // Both RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "replicate "+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where name = 'name3'");
    assertEquals(1, actual);
    
    s.execute("select * from backup2");
    rs = s.getResultSet();
    assertTrue(rs.next());
    
    assertEquals(300, rs.getInt(4));
    assertEquals(3, rs.getInt(1));
    assertEquals("name3", rs.getString(2));
    assertEquals("addr3", rs.getString(3));
    assertEquals(300, rs.getInt(4));
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // PR RR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "partition by column(name) "+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data replicate"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where name = 'name3'");
    assertEquals(1, actual);
    
    s.execute("select * from backup2");
    rs = s.getResultSet();
    assertTrue(rs.next());
    
    assertEquals(300, rs.getInt(4));
    assertEquals(3, rs.getInt(1));
    assertEquals("name3", rs.getString(2));
    assertEquals("addr3", rs.getString(3));
    assertEquals(300, rs.getInt(4));
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
    
    // RR PR
    s.execute("create table account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "replicate"+ getSuffix());
    s.execute("insert into account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    s.execute("create table backup as select * from account with no data"+ getSuffix());
    actual = s.executeUpdate("insert into backup select * from account where balance >= 100 and balance <= 300");
    assertEquals(3, actual);
    
    s.execute("create table backup2 as select * from account with no data"+ getSuffix());
    actual = s.executeUpdate("insert into backup2 select * from account where balance > 50");
    assertEquals(4, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where balance >= 50 and balance <= 85");
    assertEquals(0, actual);
    
    s.execute("truncate table backup2");
    actual = s.executeUpdate("insert into backup2 select * from account where name = 'name3'");
    assertEquals(1, actual);
    
    s.execute("select * from backup2");
    rs = s.getResultSet();
    assertTrue(rs.next());
    
    assertEquals(300, rs.getInt(4));
    assertEquals(3, rs.getInt(1));
    assertEquals("name3", rs.getString(2));
    assertEquals("addr3", rs.getString(3));
    assertEquals(300, rs.getInt(4));
    s.execute("drop table account");
    s.execute("drop table backup");
    s.execute("drop table backup2");
  }
  
  public void testInsertSubSelect() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    
    s.execute("create schema trade");
    
    s.execute("create table trade.account "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "partition by column(name) "+ getSuffix());
    s.execute("insert into trade.account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    
    s.execute("create table trade.account2 "
        + "(id int not null primary key, name varchar(20) not null, "
        + "addr varchar(20) not null, balance int not null) " +
                        "partition by column(name) "+ getSuffix());
    
    s.execute("insert into trade.account2 select * from trade.account where name = 'name3'");
    s.execute("select * from trade.account2");
    ResultSet rs = s.getResultSet();
    assertTrue(rs.next());
    
    assertEquals(300, rs.getInt(4));
    assertEquals(3, rs.getInt(1));
    assertEquals("name3", rs.getString(2));
    assertEquals("addr3", rs.getString(3));
    assertEquals(300, rs.getInt(4));
    s.execute("drop table trade.account");
    s.execute("drop table trade.account2");
    s.execute("drop schema trade RESTRICT");
  } 
  
  /* Test will fail as given query is not supported by gfxdilre
   */
  public void testInsertSubSelect_unsupported_case() throws Exception {
    startVMs(1, 4);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    
    s.execute("create schema trade");
    
    s.execute("create table trade.account "
        + "(id int not null primary key, name varchar(20), "
        + "addr varchar(20), balance int not null) " +
                        "partition by column(name) "+ getSuffix());
    s.execute("insert into trade.account values (1, 'name1', 'addr1', 100), (2, 'name2', 'addr2', 200),"
        + "(3, 'name3', 'addr3', 300), (4, 'name4', 'addr4', 400) ");
    
    s.execute("create table trade.account2 "
        + "(id int not null primary key, name varchar(20), "
        + "addr varchar(20), balance int not null) " +
                        "partition by column(name) "+ getSuffix());

    try {
      s.execute("insert into trade.account2 (id, balance) " +
      "select count(balance), max(balance) from trade.account group by id");
      fail("Test should fail: inserts as sub selects not supported for selects which needs aggregation");
    } catch (SQLException sqle) {
      assertEquals(sqle.getSQLState(), "0A000");
    }
    
    try {
      s.execute("insert into trade.account2 (id, balance) " +
          "select id, balance from trade.account where id > " +
      " (select max(id) from trade.account2)");
      fail("Test should fail: inserts as sub selects not supported for selects which needs aggregation");
    } catch (SQLException sqle) {
      assertEquals(sqle.getSQLState(), "0A000");
    }
    
    s.execute("drop table trade.account");
    s.execute("drop table trade.account2");
    s.execute("drop schema trade RESTRICT");
  } 

  public void testImportExport() throws Exception {
    startVMs(1, 4);
    Connection conn = TestUtil.getConnection();
    File outfile = new File("eximFile.flt");
    if (outfile.exists()) {
      outfile.delete();
    }

    Statement stmt = conn.createStatement();

    stmt.execute("create table t1(flight_id int not null, "
        + "segment_number int not null, aircraft varchar(20) not null, "
        + "CONSTRAINT FLIGHTS_PK PRIMARY KEY (FLIGHT_ID, SEGMENT_NUMBER))"+ getSuffix());

    stmt
        .execute("insert into t1 values (1, 1, 'kingfisher'), (1, 2, 'jet'), (2, 1, 'ai'), (3, 1, 'ial')");
    stmt
        .execute("CALL SYSCS_UTIL.EXPORT_TABLE(null, 't1', 'myfile.flt', null, null, null)");

    stmt.execute("create table imported_t1(flight_id int not null, "
        + "segment_number int not null, aircraft varchar(20) not null, "
        + "CONSTRAINT IMPORTED_FLIGHTS_PK PRIMARY KEY (FLIGHT_ID, SEGMENT_NUMBER))"+ getSuffix());
    stmt
        .execute("CALL SYSCS_UTIL.IMPORT_TABLE(null, 'imported_t1', 'myfile.flt', null, null, null, 0)");

    stmt.execute("select * from imported_t1");
    ResultSet rs = stmt.getResultSet();
    while(rs.next()) {
      System.out.println(rs.getInt(1)+", "+rs.getInt(2)+", "+rs.getString(3));
    }
    stmt.close();
    conn.close();
  }  
  
  
  public String getSuffix() throws Exception {   
    return  "  ";
   
  }
}

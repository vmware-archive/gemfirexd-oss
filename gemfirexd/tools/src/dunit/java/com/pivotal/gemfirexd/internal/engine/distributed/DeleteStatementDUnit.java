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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;

/**
 * Tests whether the statementID , connectionID etc are being passed correctly
 * or not
 * 
 * @author Kneeraj
 * @since 6.0
 * 
 */
public class DeleteStatementDUnit extends DistributedSQLTestBase {
  static volatile boolean isQueryExecutedOnNode = false;

  public DeleteStatementDUnit(String name) {
    super(name);
  }

  public void testBasicNodePruningWithPrimaryKey() throws Exception {
    try {
      // Start one client and three servers
      startServerVMs(3, 0, "SG1");
      startVMs(1, 0);
      String deleteQuery = "delete from EMP.TESTTABLE where ID > 0 AND ID < 3";
      // Create a schema with default server groups GemFire extension
      clientSQLExecute(1, "create schema EMP default server groups (SG1)");

      // Create the table and insert a row
      clientSQLExecute(
          1,
          "create table EMP.TESTTABLE (ID int primary key, "
              + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null ) PARTITION BY RANGE ( ID )"
              + " ( VALUES BETWEEN 0 and 3, VALUES BETWEEN  3 and 6 , VALUES BETWEEN 6 and  +Infinity )" + getSuffix());

      // Insert values 1 to 8
      for (int i = 0; i < 8; ++i) {
        clientSQLExecute(1, "insert into EMP.TESTTABLE values (" + (i + 1)
            + ", 'First" + (i + 1) + "', 'J 604" + (i + 1) + "')");
      }

      // Attach a GemFireXDQueryObserver in the server VM

      VM dataStore1 = this.serverVMs.get(0);
      VM dataStore2 = this.serverVMs.get(1);
      VM dataStore3 = this.serverVMs.get(2);
      SerializableRunnable setObserver = getGfxdQueryObserverIntializerForDataStore();
      dataStore1.invoke(setObserver);
      dataStore2.invoke(setObserver);
      dataStore3.invoke(setObserver);
      QueryInfo[] qi = new QueryInfo[1];
      NodesPruningHelper.setupObserverOnClient(qi);

      TestUtil.setupConnection();
      EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();

      int n = es.executeUpdate(deleteQuery);
      assertEquals(2, n);
      Object[][] routingInfo = new Object[][] { {
          new Integer(NodesPruningHelper.byrange), new SQLInteger(0),
          Boolean.FALSE, new SQLInteger(3), Boolean.FALSE } };

      Set expectedNodesToUpdate = NodesPruningHelper.getExpectedNodes(
          deleteQuery, qi[0], routingInfo, getLogWriter());
      Set allNodes = ((PartitionedRegion)qi[0].getRegion()).getRegionAdvisor()
          .adviseDataStore();

      SerializableRunnable validateNoQuerySend = getQueryNonExecutionValidator();

      SerializableRunnable validateQuerySend =  getQueryExecutionValidator();
      this.executeOnVMs(expectedNodesToUpdate, validateQuerySend);      
      allNodes.removeAll(expectedNodesToUpdate);
      this.executeOnVMs(allNodes, validateNoQuerySend);
      
      ResultSet rs = es.executeQuery("Select ID, ADDRESS from EMP.TESTTABLE" );
      int cnt = 0;
      while(rs.next()) {
        ++cnt;
      }
      assertEquals(6, cnt);
      
    }
    finally {
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        clientSQLExecute(1, "Drop table EMP.TESTTABLE ");
        clientSQLExecute(1, "Drop schema EMP restrict");
        //invokeInEveryVM(this.getClass(), "reset");         
    }
  }

  /**
   * Test to make sure an UPDATE on PK lookup with no existing row
   * updates zero rows
   */
  public void testUpdateMissPrimaryKey() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);
    String deleteQuery = "delete from EMP.TESTTABLE where ID = ?";
    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Create the table and insert a row
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int primary key, "
        + "DESCRIPTION varchar(1024) not null, ADDRESS "
        + "varchar(1024) not null ) PARTITION BY RANGE ( ID )"
        + " ( VALUES BETWEEN 0 and 3, VALUES BETWEEN  3 and 6 , "
        + "VALUES BETWEEN 6 and  +Infinity )" + getSuffix());

    try {
      // Insert values 1 to 8
      for (int i = 0; i < 8; ++i) {
        clientSQLExecute(1, "insert into EMP.TESTTABLE values (" + (i + 1)
                         + ", 'First" + (i + 1) + "', 'J 604" + (i + 1) + "')");
      }
      
      // Attach a GemFireXDQueryObserver in the server VM
      
      VM dataStore1 = this.serverVMs.get(0);
      VM dataStore2 = this.serverVMs.get(1);
      VM dataStore3 = this.serverVMs.get(2);
      SerializableRunnable setObserver = getGfxdQueryObserverIntializerForDataStore();
      dataStore1.invoke(setObserver);
      dataStore2.invoke(setObserver);
      dataStore3.invoke(setObserver);
      QueryInfo[] qi = new QueryInfo[1];
      NodesPruningHelper.setupObserverOnClient(qi);
      
      TestUtil.setupConnection();
      PreparedStatement ps = TestUtil.jdbcConn.prepareStatement(deleteQuery);
      
      // update some ID values, try to cover local as well as remote buckets
      // Expected ReplyException carrying an EntryNotFoundException, shows up
      // in DM Verbose logging
      addExpectedException(new int[] { 1 }, new int[] { 1, 2, 3 },
          EntryNotFoundException.class);

      try {
        for (int i = 42; i <= 420; i += 42) {
          ps.setInt(1, i);
          int n = ps.executeUpdate();
          assertEquals(0, n);
        }
      } finally {
        removeExpectedException(new int[] { 1 }, new int[] { 1, 2, 3 },
            EntryNotFoundException.class);
      }
    }
    finally {
      GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table EMP.TESTTABLE ");
      clientSQLExecute(1, "Drop schema EMP restrict");
      //invokeInEveryVM(this.getClass(), "reset");
    }
  }  

  /**
   * Test to make sure an delete on PK lookup with no existing row deletes zero
   * row for replicated table
   */
  public void testPKDeleteOnNonExistentRowForReplicatedTable_Bug42862() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);
    String deleteQuery = "delete from EMP.TESTTABLE where ID = ?";
    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Create the table and insert a row
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int primary key, "
        + "DESCRIPTION varchar(1024) not null, ADDRESS "
        + "varchar(1024) not null )  replicate"+ getSuffix());

    try {
      // Insert values 1 to 5
      for (int i = 0; i < 5; ++i) {
        clientSQLExecute(1, "insert into EMP.TESTTABLE values (" + (i + 1)
            + ", 'First" + (i + 1) + "', 'J 604" + (i + 1) + "')");
      }

      TestUtil.setupConnection();
      PreparedStatement ps = TestUtil.jdbcConn.prepareStatement(deleteQuery);
      ps.setInt(1, 100);
      int n = ps.executeUpdate();
      assertEquals(0, n);
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table EMP.TESTTABLE ");
      clientSQLExecute(1, "Drop schema EMP restrict");
      //invokeInEveryVM(this.getClass(), "reset");
    }
  }

  public void testBasicNodePruningWithParameter() throws Exception {
    // Start one client and three servers
    startVMs(1, 3);
    try {
      String deleteQuery = "delete from EMP.TESTTABLE where ID > ? AND ID < ?";
      // Create a schema
      clientSQLExecute(1, "create schema EMP");

      // Create the table and insert a row
      clientSQLExecute(
          1,
          "create table EMP.TESTTABLE (ID int primary key, "
              + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null ) PARTITION BY RANGE ( ID )"
              + " ( VALUES BETWEEN 0 and 3, VALUES BETWEEN  3 and 6 , VALUES BETWEEN 6 and  +Infinity )"+ getSuffix());

      // Insert values 1 to 8
      for (int i = 0; i < 8; ++i) {
        clientSQLExecute(1, "insert into EMP.TESTTABLE values (" + (i + 1)
            + ", 'First" + (i + 1) + "', 'J 604" + (i + 1) + "')");
      }

      // Attach a GemFireXDQueryObserver in the server VM

      VM dataStore1 = this.serverVMs.get(0);
      VM dataStore2 = this.serverVMs.get(1);
      VM dataStore3 = this.serverVMs.get(2);
      SerializableRunnable setObserver = getGfxdQueryObserverIntializerForDataStoreForPrepStmnt();
      dataStore1.invoke(setObserver);
      dataStore2.invoke(setObserver);
      dataStore3.invoke(setObserver);
      QueryInfo[] qi = new QueryInfo[1];
      Activation actArr[] = new Activation[1];
      NodesPruningHelper.setupObserverOnClient(qi,actArr);

      TestUtil.setupConnection();
      EmbedPreparedStatement es = (EmbedPreparedStatement)TestUtil.jdbcConn.prepareStatement(deleteQuery);
      es.setInt(1,0);
      es.setInt(2,3);
      int n = es.executeUpdate();
      assertEquals(2, n);
      Object[][] routingInfo = new Object[][] { {
          new Integer(NodesPruningHelper.byrange), new SQLInteger(0),
          Boolean.FALSE, new SQLInteger(3), Boolean.FALSE } };

      Set expectedNodesToUpdate = NodesPruningHelper.getExpectedNodes(
          deleteQuery, qi[0], routingInfo, getLogWriter());
      Set allNodes = ((PartitionedRegion)qi[0].getRegion()).getRegionAdvisor()
          .adviseDataStore();

      SerializableRunnable validateNoQuerySend = getQueryNonExecutionValidator();

      SerializableRunnable validateQuerySend =  getQueryExecutionValidator();
      this.executeOnVMs(expectedNodesToUpdate, validateQuerySend);      
      allNodes.removeAll(expectedNodesToUpdate);
      this.executeOnVMs(allNodes, validateNoQuerySend);
      
      EmbedStatement s = (EmbedStatement)TestUtil.jdbcConn.createStatement();
      ResultSet rs = s.executeQuery("Select ID, ADDRESS from EMP.TESTTABLE" );
      int cnt=0;
      while(rs.next()) {
        ++cnt;
      }
      assertEquals(6, cnt);
      
    }
    finally {
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        clientSQLExecute(1, "Drop table EMP.TESTTABLE ");
        clientSQLExecute(1, "Drop schema EMP restrict");
        //invokeInEveryVM(this.getClass(), "reset");         
    }
  }

  
  public String getSuffix() {
    return  "  ";
  }
  
  /*****************************************Helper ****************************/

  public static final void reset() {
    isQueryExecutedOnNode = false;
  }

  @Override
  protected void vmTearDown() throws Exception {
    super.vmTearDown();
    reset();
  }

  private void executeOnVMs(Set<DistributedMember> members, SerializableRunnable runnable) {
    Iterator itr = members.iterator();
    while (itr.hasNext()) {
      DistributedMember member = (DistributedMember)itr.next();
      VM vm = this.getHostVMForMember(member);
      vm.invoke(runnable);
    }
  }
  
  private  SerializableRunnable getGfxdQueryObserverIntializerForDataStore() {
    return new SerializableRunnable(
    "Set GemFireXDObserver") {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter() {
                @Override
                public void beforeQueryExecutionByStatementQueryExecutor(
                    GfxdConnectionWrapper wrapper, EmbedStatement stmt,
                    String query) {
                  isQueryExecutedOnNode = true;
                }

              });
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }
   
  private  SerializableRunnable getGfxdQueryObserverIntializerForDataStoreForPrepStmnt() {
    return new SerializableRunnable(
    "Set GemFireXDObserver") {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter() {
                @Override
                public void beforeQueryExecutionByPrepStatementQueryExecutor(
                    GfxdConnectionWrapper wrapper,
                    EmbedPreparedStatement pstmt, String query) {
                  isQueryExecutedOnNode = true;
                }

              });
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }
 
  private static SerializableRunnable getQueryExecutionValidator() {
    return new SerializableRunnable("validate Query execution") {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter());
          assertTrue(isQueryExecutedOnNode);
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }

  private static SerializableRunnable getQueryNonExecutionValidator() {
    return new SerializableRunnable("validate no query execution") {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter());
          assertFalse(isQueryExecutedOnNode);
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
  }

}

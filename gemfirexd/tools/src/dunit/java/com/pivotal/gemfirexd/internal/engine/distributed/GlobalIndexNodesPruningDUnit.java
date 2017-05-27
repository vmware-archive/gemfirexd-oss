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

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;

import io.snappydata.test.dunit.VM;

/**
 * Tests the nodes pruning behaviour of distributed queries in presence of
 * Global Index. If the where clause contains column on which global index is
 * available it can be used in nodes pruning.
 * 
 * @author Asif
 * @since 6.0
 * 
 */
@SuppressWarnings("serial")
public class GlobalIndexNodesPruningDUnit extends DistributedSQLTestBase {
  public GlobalIndexNodesPruningDUnit(String name) {
    super(name);
  }

  /**
   * Tests simple nodes pruning behavior by makking use of Global Index present
   * on the column of where clause
   * 
   */
  public void testSimpleNodesPruningUsingGlobalIndex() throws Exception {
    String query = "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2'";
    startVMs(1, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, "
        + "ADDRESS varchar(1024) not null, primary key (ID), unique(ADDRESS))");

    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'abc" + (i + 1) + "')");
    }
    try {
      // Create a prepared statement for the query & store the queryinfo so
      // obtained to get
      // region reference
      final SelectQueryInfo[] sqi = new SelectQueryInfo[1];
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqi);

      TestUtil.setupConnection();
      EmbedStatement stmnt = (EmbedStatement)TestUtil.jdbcConn
          .createStatement();

      Set<Integer> rslts = new HashSet<Integer>();
      for (int i = 2; i < 3; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = stmnt.executeQuery(query);
      for (int i = 2; i < 3; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      Object[][] routingInfo = new Object[][] { {
          new Integer(NodesPruningHelper.bycolumn), new SQLInteger(2) } };
      Set<DistributedMember> expectedPrunedNodes = NodesPruningHelper
          .getExpectedNodes(query, sqi[0], routingInfo, getLogWriter());
      Set<DistributedMember> allNodes = NodesPruningHelper.getExpectedNodes(
          query, sqi[0], new Object[][] { { new Integer(
              NodesPruningHelper.allnodes) } }, getLogWriter());
      allNodes.removeAll(expectedPrunedNodes);
      verifyQueryExecution(sqi[0], expectedPrunedNodes, allNodes, 1, 2);
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      isQueryExecutedOnNode = false;
      //invokeInEveryVM(DistributedSQLTestBase.class, "reset");
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }
  }

  /**
   * Tests simple nodes pruning behavior by makking use of Global Index present
   * on the column of where clause
   * 
   */
  public void testNodesPruningUsingExplicitGlobalIndex() throws Exception {
    String query = "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2'";
    startVMs(1, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, "
        + "ADDRESS varchar(1024) not null, primary key (ID))");
    clientSQLExecute(1,
        "CREATE GLOBAL HASH INDEX address_global ON TESTTABLE ( ADDRESS)");

    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'abc" + (i + 1) + "')");
    }
    try {
      // Create a prepared statement for the query & store the queryinfo so
      // obtained to get
      // region reference
      final SelectQueryInfo[] sqi = new SelectQueryInfo[1];
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqi);

      TestUtil.setupConnection();
      EmbedStatement stmnt = (EmbedStatement)TestUtil.jdbcConn
          .createStatement();

      Set<Integer> rslts = new HashSet<Integer>();
      for (int i = 2; i < 3; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = stmnt.executeQuery(query);
      for (int i = 2; i < 3; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      Object[][] routingInfo = new Object[][] { {
          new Integer(NodesPruningHelper.bycolumn), new SQLInteger(2) } };
      Set<DistributedMember> expectedPrunedNodes = NodesPruningHelper
          .getExpectedNodes(query, sqi[0], routingInfo, getLogWriter());
      Set<DistributedMember> allNodes = NodesPruningHelper.getExpectedNodes(
          query, sqi[0], new Object[][] { { new Integer(
              NodesPruningHelper.allnodes) } }, getLogWriter());
      allNodes.removeAll(expectedPrunedNodes);
      verifyQueryExecution(sqi[0], expectedPrunedNodes, allNodes, 1, 2);
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      isQueryExecutedOnNode = false;
      //invokeInEveryVM(DistributedSQLTestBase.class, "reset");
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }

  }

  /**
   * Tests simple nodes pruning behavior by makking use of Global Index present
   * on the column of where clause
   * 
   */
  public void testNodesPruningUsingExplicitGlobalIndexOnMultiColumnPartitioning_1()
      throws Exception {
    String query = "select ID, DESCRIPTION from TESTTABLE where "
        + "ADDRESS = 'abc2' and DESCRIPTION = 'First2'";
    startVMs(1, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, "
        + "ADDRESS varchar(1024) not null, primary key (ID))");
    clientSQLExecute(1,
        "CREATE  GLOBAL HASH  INDEX address_global ON TESTTABLE ( ADDRESS)");

    // Insert values 1 to 7
    for (int i = 0; i < 7; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'abc" + (i + 1) + "')");
    }
    try {
      // Create a prepared statement for the query & store the queryinfo so
      // obtained to get
      // region reference
      final SelectQueryInfo[] sqi = new SelectQueryInfo[1];
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqi);

      TestUtil.setupConnection();
      EmbedStatement stmnt = (EmbedStatement)TestUtil.jdbcConn
          .createStatement();

      Set<Integer> rslts = new HashSet<Integer>();
      for (int i = 2; i < 3; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = stmnt.executeQuery(query);
      for (int i = 2; i < 3; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      Object[][] routingInfo = new Object[][] { {
          new Integer(NodesPruningHelper.bycolumn), new SQLInteger(2) } };
      Set<DistributedMember> expectedPrunedNodes = NodesPruningHelper
          .getExpectedNodes(query, sqi[0], routingInfo, getLogWriter());
      Set<DistributedMember> allNodes = NodesPruningHelper.getExpectedNodes(
          query, sqi[0], new Object[][] { { new Integer(
              NodesPruningHelper.allnodes) } }, getLogWriter());
      allNodes.removeAll(expectedPrunedNodes);
      verifyQueryExecution(sqi[0], expectedPrunedNodes, allNodes, 1, 2);
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      isQueryExecutedOnNode = false;
      //invokeInEveryVM(DistributedSQLTestBase.class, "reset");
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }

  }

  /**
   * Tests simple nodes pruning behavior by making use of Global Index present
   * on the column of where clause. Tests if global index on compund key is used
   * and if one of the key of compund key is missing , then index is not used.
   * 
   */
  public void testNodesPruningUsingExplicitGlobalIndexOnMultiColumnPartitioning_2()
      throws Exception {

    String[] queries = {
        "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2' "
            + "and DESCRIPTION = 'First2'",
        "select ID, DESCRIPTION from TESTTABLE where ADDRESS = 'abc2'" };
    Object[][][] routingInfo = new Object[][][] {
        { { new Integer(NodesPruningHelper.bycolumn), new SQLInteger(2) } },
        { { new Integer(NodesPruningHelper.allnodes) } } };
    int[] numPrunedNodes = new int[] { 1, 3 };
    int[] numNoQueryNodes = new int[] { 2, 0 };
    startVMs(1, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, "
        + "ADDRESS varchar(1024) not null, primary key (ID))");
    clientSQLExecute(1,
        "CREATE GLOBAL HASH INDEX address_description_global ON TESTTABLE "
            + "(ADDRESS, DESCRIPTION)");
    try {
      // Insert values 1 to 7
      for (int i = 0; i < 7; ++i) {
        clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
            + ", 'First" + (i + 1) + "', 'abc" + (i + 1) + "')");
      }
      for (int j = 0; j < queries.length; ++j) {
        try {
          // Create a prepared statement for the query & store the queryinfo so
          // obtained to get
          // region reference
          final SelectQueryInfo[] sqi = new SelectQueryInfo[1];
          setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqi);

          TestUtil.setupConnection();
          EmbedStatement stmnt = (EmbedStatement)TestUtil.jdbcConn
              .createStatement();

          Set<Integer> rslts = new HashSet<Integer>();
          for (int i = 2; i < 3; ++i) {
            rslts.add(Integer.valueOf(i));
          }

          ResultSet rs = stmnt.executeQuery(queries[j]);
          for (int i = 2; i < 3; ++i) {
            assertTrue(rs.next());
            assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
          }
          assertTrue(rslts.isEmpty());
          assertFalse(rs.next());

          Set<DistributedMember> expectedPrunedNodes = NodesPruningHelper
              .getExpectedNodes(queries[j], sqi[0], routingInfo[j],
                  getLogWriter());
          Set<DistributedMember> allNodes = NodesPruningHelper
              .getExpectedNodes(queries[j], sqi[0],
                  new Object[][] { { new Integer(NodesPruningHelper.allnodes) } },
                  getLogWriter());
          allNodes.removeAll(expectedPrunedNodes);
          verifyQueryExecution(sqi[0], expectedPrunedNodes, allNodes,
              numPrunedNodes[j], numNoQueryNodes[j]);
        } finally {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter());
          isQueryExecutedOnNode = false;
          invokeInEveryVM(DistributedSQLTestBase.class, "reset");
        }
      }
    } finally {
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }
  }

  @Override
  protected void vmTearDown() throws Exception {
    super.vmTearDown();
    reset();
  }
}

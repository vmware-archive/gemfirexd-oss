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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;

import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;

/**
 * Tests whether the statementID , connectionID etc are being passed correctly
 * or not
 * 
 * Copy of com.pivotal.gemfirexd.internal.engine.distributed.EquiJoinQueryDUnit
 * and adapted for NCJ.
 * 
 * @author vivekb
 * @since 2.0
 * 
 */
@SuppressWarnings("serial")
public class NCJoinEquiJoinQueryDUnit extends DistributedSQLTestBase {

  public NCJoinEquiJoinQueryDUnit(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
      }
    });
    super.setUp();
  }

  @Override
  public void tearDown2() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
      }
    });
    super.tearDown2();
  }

  public String getOverflowSuffix() {
    return  " ";
  }

  /**
  * Tests basic equi join query having a single column partitioning 
  * 
  */ 
  public void testComputeNodesBehaviourWithSingleColumnAsPartitoningKey_1()
      throws Exception {
    // Create a table from client using partition by column 
    // Start one client and three servers
    startVMs(1, 3);

    clientSQLExecute(
        1,
        "create table TESTTABLE1 (ID1 int not null, "
            + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))"
            + "PARTITION BY COLUMN ( ID1 )"+getOverflowSuffix());
    clientSQLExecute(
        1,
        "create table TESTTABLE2 (ID2 int not null, "
            + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID2))"
            + "PARTITION BY COLUMN ( ID2 ) "+getOverflowSuffix());

    // Insert values 1 to 8
    for (int i = 0; i <= 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE1 values (" + (i + 1)
          + ", 'First1" + (i + 1) + "', 'J1 604')");
      clientSQLExecute(1, "insert into TESTTABLE2 values (" + (i + 1)
          + ", 'First2" + (i + 1) + "', 'J2 604')");
    }
    Object[][] queries = new Object[][] {
      //query 1
        {  "select ID1, DESCRIPTION2 from TESTTABLE1, TESTTABLE2 where ID1 = ID2 AND ID1 IN (7,9) ",
           //Conditions
          new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.allnodes),new SQLInteger(7)  },
             //each condition2
             { new Integer(NodesPruningHelper.allnodes),new SQLInteger(9), new Integer(NodesPruningHelper.ORing)  }            
           }
        }      
      
    };
      TestUtil.setupConnection();
      try {
        for( int i=0; i < queries.length; ++i) {
           final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
           final Activation[] actArr = new Activation[1];
           NodesPruningHelper.setupObserverOnClient(sqiArr, actArr);
           
           String queryString = (String)queries[i][0];
           EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();    

           String log="\nexecuting Query "+ (i+1) + " : \""+es.getSQLText() + "\"";
           
           
           getLogWriter().info(log);
           ResultSet rs = es.executeQuery(queryString);
           NodesPruningHelper.validateNodePruningForQuery(queryString, sqiArr[0], (Object[][])queries[i][1],this, actArr[0]);
           rs.close();
           getLogWriter().info("Query " + (i+1) + " : succeeded");
           es.close();
         }     
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table TESTTABLE2 ");
      clientSQLExecute(1, "Drop table TESTTABLE1 ");
    }

  }
  
  /**
  * Tests the nodes pruning logic when colocated queries are executed using partition by range.
  */ 
  public void testComputeNodesBehaviourWithSingleColumnAsPartitoningKey_2()
      throws Exception {
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
      }
    });
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
//  Create a table from client using partition by column 
    // Start one client and three servers
    startVMs(1, 3);

    clientSQLExecute(
        1,
        "create table TESTTABLE1 (ID1 int not null, "
            + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))"
            + "PARTITION BY RANGE ( ID1 )"
        + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 6, VALUES BETWEEN 6 and +infinity )"+getOverflowSuffix());
    clientSQLExecute(
        1,
        "create table TESTTABLE2 (ID2 int not null, "
            + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID2))"
            + "PARTITION BY RANGE ( ID2 )"
            + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 6, VALUES BETWEEN 6 and +infinity )"
            + getOverflowSuffix());

    // Insert values 1 to 8
    for (int i = 0; i <= 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE1 values (" + (i + 1)
          + ", 'First1" + (i + 1) + "', 'J1 604')");
      clientSQLExecute(1, "insert into TESTTABLE2 values (" + (i + 1)
          + ", 'First2" + (i + 1) + "', 'J2 604')");
    }
    Object[][] queries = new Object[][] {
      //query 1
        {  "select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 AND ID1 > 1 and ID1 < 6  ",
           //Conditions
          new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.allnodes),new SQLInteger(1), Boolean.FALSE, new SQLInteger(6),Boolean.FALSE  }
                         
           }
        }      
      
    };
      TestUtil.setupConnection();
      try {
        for( int i=0; i < queries.length; ++i) {
           final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
           NodesPruningHelper.setupObserverOnClient(sqiArr);
           
           String queryString = (String)queries[i][0];
           EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();    

           String log="\nexecuting Query "+ (i+1) + " : \""+es.getSQLText() + "\"";
           
           
           getLogWriter().info(log);
           ResultSet rs = es.executeQuery(queryString);
           int cnt =0;
           while(rs.next()) {
             ++cnt;
           }
           assertEquals(cnt,4);
           NodesPruningHelper.validateNodePruningForQuery(queryString, sqiArr[0], (Object[][])queries[i][1],this);
           
           getLogWriter().info("Query " + (i+1) + " : succeeded");
           rs.close();
         }     
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      //clientSQLExecute(1, "Drop table TESTTABLE1 ");
      clientSQLExecute(1, "Drop table TESTTABLE2 ");
      clientSQLExecute(1, "Drop table TESTTABLE1 ");
      invokeInEveryVM(new SerializableRunnable() {
        @Override
        public void run() {
          System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
        }
      });
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    }

  }
  
  /**
   * Tests the nodes pruning logic when colocated queries are executed using partition by range.
   * Projection attribute used is not *
   */ 
   public void testBug39862()
       throws Exception {
//   Create a table from client using partition by column 
     // Start one client and three servers
     invokeInEveryVM(new SerializableRunnable() {
       @Override
       public void run() {
         System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
       }
     });
     System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
     
     startVMs(1, 3);
     
     clientSQLExecute(
         1,
         "create table TESTTABLE1 (ID1 int not null, "
             + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))"
             + "PARTITION BY RANGE ( ID1 )"
         + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 6, VALUES BETWEEN 6 and +infinity )"+getOverflowSuffix());
     clientSQLExecute(
         1,
         "create table TESTTABLE2 (ID2 int not null, "
             + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID2))"
             + "PARTITION BY RANGE ( ID2 )"
             + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 6, VALUES BETWEEN 6 and +infinity )"
             + getOverflowSuffix());

     // Insert values 1 to 8
     for (int i = 0; i <= 8; ++i) {
       clientSQLExecute(1, "insert into TESTTABLE1 values (" + (i + 1)
           + ", 'First1" + (i + 1) + "', 'J1 604')");
       clientSQLExecute(1, "insert into TESTTABLE2 values (" + (i + 1)
           + ", 'First2" + (i + 1) + "', 'J2 604')");
     }
     Object[][] queries = new Object[][] {
       //query 1
         {  "select ID1, ID2 from TESTTABLE1, TESTTABLE2 where ID1 = ID2 AND ID1 > 1 and ID1 < 6  ",
            //Conditions
           new Object[][] {
             //each condition1
              { new Integer(NodesPruningHelper.allnodes),new SQLInteger(1), Boolean.FALSE, new SQLInteger(6),Boolean.FALSE  }
                          
            }
         }      
       
     };
     
       TestUtil.setupConnection();
       try {
         for( int i=0; i < queries.length; ++i) {
            final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
            NodesPruningHelper.setupObserverOnClient(sqiArr);
            
            String queryString = (String)queries[i][0];
            EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();    

            String log="\nexecuting Query "+ (i+1) + " : \""+ queryString + "\"";
            
            Set<Integer> rslts = new HashSet<Integer>();
            for (int j = 2; j < 6; ++j) {
              rslts.add(Integer.valueOf(j));
            }        
            getLogWriter().info(log);
            ResultSet rs = es.executeQuery(queryString);
            int cnt =0;
            while(rs.next()) {
              ++cnt;
              assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
            }
            assertTrue(rslts.isEmpty());
            assertFalse(rs.next());
            assertEquals(cnt,4);
            NodesPruningHelper.validateNodePruningForQuery(queryString, sqiArr[0], (Object[][])queries[i][1],this);
            
            getLogWriter().info("Query " + (i+1) + " : succeeded");
            rs.close();
          }     
     }
     finally {
       GemFireXDQueryObserverHolder
           .setInstance(new GemFireXDQueryObserverAdapter());
       //clientSQLExecute(1, "Drop table TESTTABLE1 ");
       clientSQLExecute(1, "Drop table TESTTABLE2 ");
       clientSQLExecute(1, "Drop table TESTTABLE1 ");
       try {
         System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
         invokeInEveryVM(new SerializableRunnable() {
           @Override
           public void run() {
             System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
           }
         });
       }
       catch(Throwable t) {
         throw new AssertionError("Couldn't clear STATEMENT OPTIMIZATION flag. " + t);
       }
     }

   }
   
   /**
    * Tests the nodes pruning logic when equijoin query involves a PR & replicated region
    */ 
    public void testEquijoinQueryForPRAndReplicatedRegion_1()
        throws Exception {
//    Create a table from client using partition by column 
      // Start one client a three servers
      startVMs(1, 3);

      VM dataStore1 = this.serverVMs.get(0);
      VM dataStore2 = this.serverVMs.get(1);
      VM dataStore3 = this.serverVMs.get(2);
      clientSQLExecute(
          1,
          "create table TESTTABLE1 (ID1 int not null, "
              + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))"
              + "PARTITION BY Column ( ID1 )" +getOverflowSuffix());
      clientSQLExecute(
          1,
          "create table TESTTABLE2 (ID2 int not null, "
              + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID2))"
              + " REPLICATE" +getOverflowSuffix());

      // Insert values 1 to 8
      for (int i = 0; i <= 8; ++i) {
        clientSQLExecute(1, "insert into TESTTABLE1 values (" + (i + 1)
            + ", 'First1" + (i + 1) + "', 'J1 604')");
        clientSQLExecute(1, "insert into TESTTABLE2 values (" + (i + 1)
            + ", 'First2" + (i + 1) + "', 'J2 604')");
      }
      Object[][] queries = new Object[][] {
        //query 1
          {  "select * from TESTTABLE1, TESTTABLE2 where  ID2 = 1 ",
             //Conditions
            new Object[][] {
              //each condition1
               { new Integer(NodesPruningHelper.allnodes)  }
                           
             }
          }      
        
      };
      TestUtil.setupConnection();
      try {
        for (int i = 0; i < queries.length; ++i) {
          final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
          setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqiArr);

          String queryString = (String)queries[i][0];
          EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();

          String log = "\nexecuting Query " + (i + 1) + " : \"" + queryString
              + "\"";

          getLogWriter().info(log);
          ResultSet rs = es.executeQuery(queryString);
          while (rs.next()) {
          }
          Set<DistributedMember> expectedPrunedNodes = NodesPruningHelper
              .getExpectedNodes(queryString, sqiArr[0],
                  (Object[][])queries[i][1], getLogWriter());
          Set<DistributedMember> allNodes = NodesPruningHelper.getExpectedNodes(
              queryString, sqiArr[0], new Object[][] { { new Integer(
                  NodesPruningHelper.allnodes) } }, getLogWriter());
          allNodes.removeAll(expectedPrunedNodes);
          verifyQueryExecution(sqiArr[0], expectedPrunedNodes, allNodes, 3, 0);
  
          getLogWriter().info("Query " + (i + 1) + " : succeeded");
          es.close();
        }
      }
      finally {
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        isQueryExecutedOnNode = false;
        //invokeInEveryVM(DistributedSQLTestBase.class, "reset");   
        clientSQLExecute(1, "Drop table TESTTABLE1 ");
        clientSQLExecute(1, "Drop table TESTTABLE2 ");
      }
    }

  /**
   * Tests for nodes pruning logic when equijoin query involves a PR &
   * replicated region with same or incompatible server groups.
   */
  /*
   * TODO: We are not sure to hadle join across different server groups now for NCJ. 
   * However this is a good test to evaluate such scenarios.
   * 
   * But this test, as expected, results into Colocation Matrix Assertion failure, 
   * and is thus disabled.
   * 
   * Will be enabled only when join across server-groups are supported.
   */
  public void _testEquijoinQueryForPRAndReplicatedRegionWithServerGroups()
      throws Exception {
    // Create a table from client using partition by column
    // Start one client three servers
    AsyncVM async1 = invokeStartServerVM(1, 0, "SG1", null);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG2", null);
    AsyncVM async3 = invokeStartServerVM(3, 0, "SG3", null);
    AsyncVM async4 = invokeStartServerVM(4, 0, null, null);
    startClientVMs(1, 0, null);
    joinVMs(true, async1, async2, async3, async4);

    clientSQLExecute(1, "create table EMP.PARTTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, "
        + "ADDRESS1 varchar(1024) not null, primary key (ID1))"
        + "PARTITION BY Column (ID1) SERVER GROUPS (SG1,SG2)");
    clientSQLExecute(1, "create table EMP.PARTTABLE2 (ID11 int not null, "
        + " DESCRIPTION11 varchar(1024) not null, "
        + "ADDRESS11 varchar(1024) not null, primary key (ID11))"
        + "PARTITION BY Column (ID11)" + getOverflowSuffix());
    clientSQLExecute(1, "create table EMP.PARTTABLE3 (ID12 int not null, "
        + " DESCRIPTION12 varchar(1024) not null, "
        + "ADDRESS12 varchar(1024) not null, primary key (ID12))"
        + "PARTITION BY Column (ID12) "
        + "SERVER GROUPS (SG1,SG2)" + getOverflowSuffix());
    clientSQLExecute(1, "create table EMP.TESTTABLE2 (ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, "
        + "ADDRESS2 varchar(1024) not null, primary key (ID2))"
        + " REPLICATE SERVER GROUPS (SG1,SG2, sg3)" + getOverflowSuffix());
    // also check for table in different server groups and matching one
    serverSQLExecute(1, "create table EMP.TESTTABLE3 (ID3 int not null, "
        + " DESCRIPTION3 varchar(1024) not null, "
        + "ADDRESS3 varchar(1024) not null, primary key (ID3))"
        + " REPLICATE SERVER GROUPS (SG1)" + getOverflowSuffix());
    serverSQLExecute(2, "create table EMP.TESTTABLE4 (ID4 int not null, "
        + " DESCRIPTION4 varchar(1024) not null, "
        + "ADDRESS4 varchar(1024) not null, primary key (ID4))"
        + " REPLICATE SERVER GROUPS (SG1, SG2)" + getOverflowSuffix());
    serverSQLExecute(1, "create table EMP.TESTTABLE5 (ID5 int not null, "
        + " DESCRIPTION5 varchar(1024) not null, "
        + "ADDRESS5 varchar(1024) not null, primary key (ID5)) REPLICATE"
        + getOverflowSuffix());

    // Insert values 1 to 10
    for (int i = 0; i <= 10; ++i) {
      clientSQLExecute(1, "insert into EMP.PARTTABLE1 values (" + (i + 1)
          + ", 'First1" + (i + 1) + "', 'J1 604')");
      clientSQLExecute(1, "insert into EMP.PARTTABLE2 values (" + (i + 1)
          + ", 'First2" + (i + 1) + "', 'J2 604')");
      serverSQLExecute(2, "insert into EMP.PARTTABLE3 values (" + (i + 1)
          + ", 'First3" + (i + 1) + "', 'J3 604')");
      clientSQLExecute(1, "insert into EMP.TESTTABLE2 values (" + (i + 1)
          + ", 'First2" + (i + 1) + "', 'J2 604')");
      serverSQLExecute(1, "insert into EMP.TESTTABLE3 values (" + (i + 1)
          + ", 'First3" + (i + 1) + "', 'J3 604')");
      serverSQLExecute(3, "insert into EMP.TESTTABLE4 values (" + (i + 1)
          + ", 'First4" + (i + 1) + "', 'J4 604')");
      serverSQLExecute(2, "insert into EMP.TESTTABLE5 values (" + (i + 1)
          + ", 'First5" + (i + 1) + "', 'J5 604')");
    }

    // check exception for queries involving different server groups
    try {
      clientSQLExecute(1, "select * from EMP.PARTTABLE1, EMP.TESTTABLE3 "
          + "where ID1 = 4 and ID3 = 1");
      fail("expected exception in join for tables in incompatible server groups");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    try {
      clientSQLExecute(1, "select * from EMP.PARTTABLE2, EMP.TESTTABLE3 "
          + "where ID11 = 4 and ID3 = 1");
      fail("expected exception in join for tables in incompatible server groups");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    try {
      clientSQLExecute(1, "select * from EMP.PARTTABLE1, EMP.PARTTABLE2 "
          + "where ID1 = 4 and ID11 = 1");
      fail("expected exception in join for tables in incompatible server groups");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    try {
      clientSQLExecute(1, "select * from EMP.PARTTABLE1, EMP.PARTTABLE2 "
          + "where ID1 = ID11");
      fail("expected exception in join for tables in incompatible server groups");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    try {
      clientSQLExecute(1, "select * from EMP.PARTTABLE2, EMP.TESTTABLE4 "
          + "where ID11 = 4 and ID4 = 1");
      fail("expected exception in join for tables in incompatible server groups");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    try {
      clientSQLExecute(1, "select * from EMP.TESTTABLE3, EMP.PARTTABLE2, "
          + "EMP.TESTTABLE4");
      fail("expected exception in join for tables in incompatible server groups");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    try {
      clientSQLExecute(1, "select * from EMP.PARTTABLE1, EMP.PARTTABLE3 "
          + "where ADDRESS1 = ADDRESS12");
      fail("expected exception in join for tables in improper join condition");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    try {
      clientSQLExecute(1, "select * from EMP.TESTTABLE5, EMP.PARTTABLE1, "
          + "EMP.PARTTABLE3");
      fail("expected exception in join for tables in improper join condition");
    } catch (SQLException ex) {
      // expected exception
      if (!"0A000".equals(ex.getSQLState())) {
        throw ex;
      }
    }

    // check success for queries involving same server groups
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select * from EMP.PARTTABLE1, EMP.TESTTABLE2 where ID2 = 1 "
            + "AND ID1 = 4", TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", "sgs_1");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select * from EMP.PARTTABLE1, EMP.TESTTABLE4 where ID4 = 1 "
            + "AND ID1 = 4", TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", "sgs_2");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select * from EMP.TESTTABLE2, EMP.TESTTABLE4 where ID2 = 1 "
            + "AND ID4 = 4", TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", "sgs_3");

    isQueryExecutedOnNode = false;
    invokeInEveryVM(DistributedSQLTestBase.class, "reset");
    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    VM dataStore4 = this.serverVMs.get(3);
    final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
    setupObservers(new VM[] { dataStore1, dataStore2, dataStore3, dataStore4 },
        sqiArr);

    try {
      // expect query to succeed when server groups of one table are a subset of
      // another
      clientSQLExecute(1, "select * from EMP.TESTTABLE2, EMP.TESTTABLE3",
          false, false, true);
      checkQueryExecution(true, dataStore1);
      // expect query to succeed when an replicated table has no server groups
      serverSQLExecute(3, "select * from EMP.PARTTABLE1, EMP.TESTTABLE5", true,
          false, true);
      checkQueryExecution(false, dataStore1, dataStore2);
      clientSQLExecute(1, "select * from EMP.TESTTABLE2, EMP.TESTTABLE5", true,
          false, true);
      checkQueryExecution(true, dataStore1, dataStore2, dataStore3);
      clientSQLExecute(1, "select * from EMP.TESTTABLE5", false, false, true);
      checkQueryExecution(true, dataStore1, dataStore2, dataStore3, dataStore4);
      serverSQLExecute(1, "select * from EMP.TESTTABLE5, EMP.PARTTABLE1, "
          + "EMP.PARTTABLE3 where ID1 = ID12", false, false, true);
      checkQueryExecution(false, dataStore1, dataStore2);
      // expect success when server groups of replicated tables are superset of those
      // of partitioned tables
      serverSQLExecute(2, "select * from EMP.TESTTABLE5, EMP.PARTTABLE1, "
          + "EMP.TESTTABLE2", true, false, true);
      checkQueryExecution(false, dataStore1, dataStore2);
      serverSQLExecute(3, "select * from EMP.TESTTABLE5, EMP.PARTTABLE1, "
          + "EMP.TESTTABLE2, EMP.PARTTABLE3 where ID1 = ID12", true, false,
          true);
      checkQueryExecution(false, dataStore1, dataStore2);
      clientSQLExecute(1, "select * from EMP.TESTTABLE2, EMP.PARTTABLE1, "
          + "EMP.TESTTABLE4, EMP.PARTTABLE3 where ID1 = ID12", true, false,
          true);
      checkQueryExecution(false, dataStore1, dataStore2);
      clientSQLExecute(1, "select * from EMP.TESTTABLE4, EMP.PARTTABLE3, "
          + "EMP.TESTTABLE2, EMP.PARTTABLE1, EMP.TESTTABLE5 where ID1 = ID12 "
          + "and ADDRESS1 = ADDRESS12", false, false, true);
      checkQueryExecution(false, dataStore1, dataStore2);
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      isQueryExecutedOnNode = false;
      //invokeInEveryVM(DistributedSQLTestBase.class, "reset");
    }
  }

  /**
   * test equi-join on multiple PR and replicated tables
   */
  public void testEquiJoinPRAndReplicatedMix_40307() throws Exception {
    startVMs(1, 3);

    clientSQLExecute(1, "create table EMP.PARTTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, "
        + "ADDRESS1 varchar(1024) not null, primary key (ID1))"
        + "PARTITION BY Column (ID1)"+getOverflowSuffix());
    clientSQLExecute(1, "create table EMP.PARTTABLE2 (ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, "
        + "ADDRESS2 varchar(1024) not null, primary key (ID2))"
        + "PARTITION BY Column (ID2) "+getOverflowSuffix());
    serverSQLExecute(1, "create table EMP.REPLTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, "
        + "ADDRESS1 varchar(1024) not null, primary key (ID1)) REPLICATE"+getOverflowSuffix());
    serverSQLExecute(2, "create table EMP.REPLTABLE2 (ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, "
        + "ADDRESS2 varchar(1024) not null, primary key (ID2)) REPLICATE"+getOverflowSuffix());

    // Insert values 1 to 4
    for (int i = 1; i <= 4; ++i) {
      clientSQLExecute(1, "insert into EMP.PARTTABLE1 values (" + i
          + ", 'First1" + i + "', 'J1 604')");
      clientSQLExecute(1, "insert into EMP.PARTTABLE2 values (" + i
          + ", 'First2" + i + "', 'J2 604')");
      serverSQLExecute(1, "insert into EMP.REPLTABLE1 values (" + i
          + ", 'RFirst1" + i + "', 'J1 604')");
      serverSQLExecute(3, "insert into EMP.REPLTABLE2 values (" + i
          + ", 'First2" + i + "', 'RJ2 604')");
    }

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select * from EMP.REPLTABLE1 r1, EMP.PARTTABLE1 p1, "
            + "EMP.PARTTABLE2 p2 where p1.ID1 = p2.ID2 and "
            + "r1.ADDRESS1 = p1.ADDRESS1 and r1.ID1 = p2.ID2", TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", "pr_repl_mix1");

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select * from EMP.REPLTABLE1 r1, EMP.PARTTABLE1 p1, "
        + "EMP.PARTTABLE2 p2, EMP.REPLTABLE2 r2 where p1.ID1 = p2.ID2 and "
        + "r1.ADDRESS1 = p1.ADDRESS1 and r1.ID1 = p2.ID2 and r1.ID1 = r2.ID2 "
        + "and r2.ID2 = 3", TestUtil.getResourcesDir() +
        "/lib/checkEquiJoinQuery.xml", "pr_repl_mix2");

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select * from EMP.REPLTABLE1 r1, EMP.REPLTABLE2 r2, "
        + "EMP.PARTTABLE1 p1, EMP.PARTTABLE2 p2 where p1.ID1 = p2.ID2 and "
        + "r1.ID1 = 2 and r2.ID2 = 4",
        TestUtil.getResourcesDir() + "/lib/checkEquiJoinQuery.xml",
        "pr_repl_mix3");

    clientSQLExecute(1, "drop table EMP.REPLTABLE1");
    clientSQLExecute(1, "drop table EMP.REPLTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE1");
  }

  public void testQueryConvertibleToJoin() throws Exception
  {
    startVMs(1, 3);

    clientSQLExecute(
        1,
        "create table TESTTABLE1 (ID1 int primary key, "
            + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null)"
            + "PARTITION BY Primary key"+getOverflowSuffix());
    clientSQLExecute(
        1,
        "create table TESTTABLE2 (ID2 int  primary key, "
            + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null,ID_FK int, foreign key (ID_FK) references TESTTABLE1(ID1))"+getOverflowSuffix());


    // Insert values 1 to 8
    for (int i = 0; i < 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE1 values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604' )");
    }
    for (int i = 0; i < 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE2 values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604', "+(i+1)+" )");
    }
    sqlExecuteVerify(new int[] { 1 }, null,
        "select ID2 from TESTTABLE2 where ADDRESS2 like '%604' AND ID_FK IN (select ID1 from TESTTABLE1 )",
        TestUtil.getResourcesDir() + "/lib/checkStatementQueryDUnit.xml", "testLikePredicateQuery_2",
        false, false);
  }

  /**
   * test NCJ on multiple PR and replicated tables
   * 
   * Especially for issue of Null pointer due to different resultset number in
   * result-columns
   */
  public void testPRAndReplicatedMix_1() throws Exception {
    startVMs(1, 3);
  
    clientSQLExecute(1, "create table EMP.PARTTABLE1 (IDP1 int not null, "
        + " DESCRIPTIONP1 varchar(1024) not null, "
        + "ADDRESSP1 varchar(1024) not null, primary key (IDP1))"
        + "PARTITION BY Column (IDP1)"+getOverflowSuffix());
    clientSQLExecute(1, "create table EMP.PARTTABLE2 (IDP2 int not null, "
        + " DESCRIPTIONP2 varchar(1024) not null, "
        + "ADDRESSP2 varchar(1024) not null, primary key (IDP2))"
        + "PARTITION BY Column (IDP2) "+getOverflowSuffix());
    serverSQLExecute(1, "create table EMP.REPLTABLE1 (IDR1 int not null, "
        + " DESCRIPTIONR1 varchar(1024) not null, "
        + "ADDRESSR1 varchar(1024) not null, primary key (IDR1)) REPLICATE"+getOverflowSuffix());
    serverSQLExecute(2, "create table EMP.REPLTABLE2 (IDR2 int not null, "
        + " DESCRIPTIONR2 varchar(1024) not null, "
        + "ADDRESSR2 varchar(1024) not null, primary key (IDR2)) REPLICATE"+getOverflowSuffix());
  
    // Insert values 1 to 4
    for (int i = 1; i <= 4; ++i) {
      clientSQLExecute(1, "insert into EMP.PARTTABLE1 values (" + i
          + ", 'First1" + i + "', 'J1 604')");
      clientSQLExecute(1, "insert into EMP.PARTTABLE2 values (" + i
          + ", 'First2" + i + "', 'J2 604')");
      serverSQLExecute(1, "insert into EMP.REPLTABLE1 values (" + i
          + ", 'RFirst1" + i + "', 'J1 604')");
      serverSQLExecute(3, "insert into EMP.REPLTABLE2 values (" + i
          + ", 'First2" + i + "', 'RJ2 604')");
    }
  
    {
      String query = "select * from EMP.REPLTABLE1 r1, EMP.PARTTABLE1 p1, "
      + "EMP.PARTTABLE2 p2, EMP.REPLTABLE2 r2 where "
      + "p1.IDP1 = p2.IDP2 and "
      + "r1.ADDRESSR1 = p1.ADDRESSP1 and "
      + "r1.IDR1 = p2.IDP2 and r1.IDR1 = r2.IDR2 "
      + "and p1.IDP1 = ?"
      ;

      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(3);
      Connection conn = TestUtil.getConnection();
      PreparedStatement st = conn.prepareStatement(query);
      st.setInt(1, 3);
      ResultSet rs = st.executeQuery();
      while(rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
    }
  
    clientSQLExecute(1, "drop table EMP.REPLTABLE1");
    clientSQLExecute(1, "drop table EMP.REPLTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE1");
  }

  /**
   * test NCJ on multiple PR and replicated tables
   * 
   * Especially for issue of Null pointer due to different resultset number in
   * result-columns
   */
  public void testPRAndReplicatedMix_2() throws Exception {
    startVMs(1, 3);
  
    clientSQLExecute(1, "create table EMP.PARTTABLE1 (IDP1 int not null, "
        + " DESCRIPTIONP1 varchar(1024) not null, "
        + "ADDRESSP1 varchar(1024) not null, primary key (ADDRESSP1))"
        + "PARTITION BY Column (IDP1)"+getOverflowSuffix());
    clientSQLExecute(1, "create table EMP.PARTTABLE2 (IDP2 int not null, "
        + " DESCRIPTIONP2 varchar(1024) not null, "
        + "ADDRESSP2 varchar(1024) not null, primary key (ADDRESSP2))"
        + "PARTITION BY Column (IDP2) "+getOverflowSuffix());
    serverSQLExecute(1, "create table EMP.REPLTABLE1 (IDR1 int not null, "
        + " DESCRIPTIONR1 varchar(1024) not null, "
        + "ADDRESSR1 varchar(1024) not null, primary key (ADDRESSR1)) REPLICATE"+getOverflowSuffix());
    serverSQLExecute(2, "create table EMP.REPLTABLE2 (IDR2 int not null, "
        + " DESCRIPTIONR2 varchar(1024) not null, "
        + "ADDRESSR2 varchar(1024) not null, primary key (ADDRESSR2)) REPLICATE"+getOverflowSuffix());
  
    // Insert values 1 to 4
    for (int i = 1; i <= 4; ++i) {
      clientSQLExecute(1, "insert into EMP.PARTTABLE1 values (" + i
          + ", 'First1" + i + "', 'J1 604" + i + "')");
      clientSQLExecute(1, "insert into EMP.PARTTABLE2 values (" + i
          + ", 'First2" + i + "', 'J2 604" + i + "')");
      serverSQLExecute(1, "insert into EMP.REPLTABLE1 values (" + i
          + ", 'RFirst1" + i + "', 'J1 604" + i + "')");
      serverSQLExecute(3, "insert into EMP.REPLTABLE2 values (" + i
          + ", 'First2" + i + "', 'RJ2 604" + i + "')");
    }
  
   
    {
      String query = "select * from EMP.REPLTABLE1 r1, EMP.PARTTABLE1 p1, "
      + "EMP.PARTTABLE2 p2, EMP.REPLTABLE2 r2 where "
      + "p1.IDP1 = p2.IDP2 and "
      + "r1.ADDRESSR1 = p1.ADDRESSP1 and "
      + "r1.IDR1 = p2.IDP2 and r1.IDR1 = r2.IDR2 "
      + "and p1.IDP1 = ?"
      ;

      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(3);
      Connection conn = TestUtil.getConnection();
      PreparedStatement st = conn.prepareStatement(query);
      st.setInt(1, 3);
      ResultSet rs = st.executeQuery();
      while(rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
    }
  
    clientSQLExecute(1, "drop table EMP.REPLTABLE1");
    clientSQLExecute(1, "drop table EMP.REPLTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE1");
  }

  /**
   * test NCJ on multiple PR and replicated tables
   * 
   * Especially for issue of Null pointer due to different resultset number in
   * result-columns. However this NPE comes at different place
   */
  public void testPRAndReplicatedMix_3() throws Exception {
    startVMs(1, 3);
  
    clientSQLExecute(1, "create table EMP.PARTTABLE1 (IDP1 int not null, "
        + " DESCRIPTIONP1 varchar(1024) not null, "
        + "ADDRESSP1 varchar(1024) not null, primary key (IDP1))"
        + "PARTITION BY Column (IDP1)"+getOverflowSuffix());
    clientSQLExecute(1, "create table EMP.PARTTABLE2 (IDP2 int not null, "
        + " DESCRIPTIONP2 varchar(1024) not null, "
        + "ADDRESSP2 varchar(1024) not null, primary key (IDP2))"
        + "PARTITION BY Column (IDP2) "+getOverflowSuffix());
    serverSQLExecute(1, "create table EMP.REPLTABLE1 (IDR1 int not null, "
        + " DESCRIPTIONR1 varchar(1024) not null, "
        + "ADDRESSR1 varchar(1024) not null, primary key (IDR1)) REPLICATE"+getOverflowSuffix());
    serverSQLExecute(2, "create table EMP.REPLTABLE2 (IDR2 int not null, "
        + " DESCRIPTIONR2 varchar(1024) not null, "
        + "ADDRESSR2 varchar(1024) not null, primary key (IDR2)) REPLICATE"+getOverflowSuffix());
  
    // Insert values 1 to 4
    for (int i = 1; i <= 4; ++i) {
      clientSQLExecute(1, "insert into EMP.PARTTABLE1 values (" + i
          + ", 'First1" + i + "', 'J1 604')");
      clientSQLExecute(1, "insert into EMP.PARTTABLE2 values (" + i
          + ", 'First2" + i + "', 'J2 604')");
      serverSQLExecute(1, "insert into EMP.REPLTABLE1 values (" + i
          + ", 'RFirst1" + i + "', 'J1 604')");
      serverSQLExecute(3, "insert into EMP.REPLTABLE2 values (" + i
          + ", 'First2" + i + "', 'RJ2 604')");
    }
  
    {
      String query = "select * from EMP.REPLTABLE1 r1, EMP.PARTTABLE1 p1, "
          + "EMP.PARTTABLE2 p2, EMP.REPLTABLE2 r2 where "
          + "p1.IDP1 = p2.IDP2 and " + "r1.ADDRESSR1 = p1.ADDRESSP1 and "
          + "r1.IDR1 = p2.IDP2 and r1.IDR1 = r2.IDR2 ";
  
      ArrayList<Integer> expected = new ArrayList<Integer>();
      for (int i = 1; i <= 4; ++i) {
        expected.add(i);
      }
      Connection conn = TestUtil.getConnection();
      PreparedStatement st = conn.prepareStatement(query);
      ResultSet rs = st.executeQuery();
      while(rs.next()) {
        Integer value = rs.getInt(1);
        assertTrue(expected.contains(value));
        expected.remove(value);
      }
      assertTrue(expected.isEmpty());
    }
    
    {
      String query = "select * from EMP.REPLTABLE1 r1, EMP.PARTTABLE1 p1, "
          + "EMP.PARTTABLE2 p2, EMP.REPLTABLE2 r2 where "
          + "p1.IDP1 = p2.IDP2 " + " and " + "r1.IDR1 = p2.IDP2 " + " and "
          + "r1.IDR1 = r2.IDR2 ";
  
      ArrayList<Integer> expected = new ArrayList<Integer>();
      for (int i = 1; i <= 4; ++i) {
        expected.add(i);
      }
      Connection conn = TestUtil.getConnection();
      PreparedStatement st = conn.prepareStatement(query);
      ResultSet rs = st.executeQuery();
      while(rs.next()) {
        Integer value = rs.getInt(1);
        assertTrue(expected.contains(value));
        expected.remove(value);
      }
      assertTrue(expected.isEmpty());
    }
  
    clientSQLExecute(1, "drop table EMP.REPLTABLE1");
    clientSQLExecute(1, "drop table EMP.REPLTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE1");
  }
  
  /**
   * test NCJ on multiple PR and replicated tables
   * 
   * Especially for issue of ClassCastException that is due to
   * BackingHashMapStore returning ArrayList sometimes (in case of duplicates).
   */
  public void testPRAndReplicatedMix_4() throws Exception {
    startVMs(1, 3);
  
    clientSQLExecute(1, "create table EMP.PARTTABLE1 (IDP1 int not null, "
        + " DESCRIPTIONP1 varchar(1024) not null, "
        + "ADDRESSP1 varchar(1024) not null, primary key (IDP1))"
        + "PARTITION BY Column (IDP1)"+getOverflowSuffix());
    clientSQLExecute(1, "create table EMP.PARTTABLE2 (IDP2 int not null, "
        + " DESCRIPTIONP2 varchar(1024) not null, "
        + "ADDRESSP2 varchar(1024) not null, primary key (IDP2))"
        + "PARTITION BY Column (IDP2) "+getOverflowSuffix());
    serverSQLExecute(1, "create table EMP.REPLTABLE1 (IDR1 int not null, "
        + " DESCRIPTIONR1 varchar(1024) not null, "
        + "ADDRESSR1 varchar(1024) not null, primary key (IDR1)) REPLICATE"+getOverflowSuffix());
    serverSQLExecute(2, "create table EMP.REPLTABLE2 (IDR2 int not null, "
        + " DESCRIPTIONR2 varchar(1024) not null, "
        + "ADDRESSR2 varchar(1024) not null, primary key (IDR2)) REPLICATE"+getOverflowSuffix());
  
    // Insert values 1 to 4
    for (int i = 1; i <= 4; ++i) {
      clientSQLExecute(1, "insert into EMP.PARTTABLE1 values (" + i
          + ", 'First1" + i + "', 'J1 604')");
      clientSQLExecute(1, "insert into EMP.PARTTABLE2 values (" + i
          + ", 'First2" + i + "', 'J2 604')");
      serverSQLExecute(1, "insert into EMP.REPLTABLE1 values (" + i
          + ", 'RFirst1" + i + "', 'J1 604')");
      serverSQLExecute(3, "insert into EMP.REPLTABLE2 values (" + i
          + ", 'First2" + i + "', 'RJ2 604')");
    }
  
    {
      String query = "select * from EMP.REPLTABLE1 r1, EMP.PARTTABLE1 p1, "
          + "EMP.PARTTABLE2 p2, EMP.REPLTABLE2 r2 where "
          + "p1.IDP1 = p2.IDP2 and " + "r1.ADDRESSR1 = p1.ADDRESSP1 and "
          + "r1.IDR1 = r2.IDR2 ";

      ArrayList<Integer> expected = new ArrayList<Integer>();
      for (int i = 1; i <= 4; ++i) {
        for (int j = 1; j <= 4; ++j) {
          expected.add(i);
        }
      }
      Connection conn = TestUtil.getConnection();
      PreparedStatement st = conn.prepareStatement(query);
      ResultSet rs = st.executeQuery();
      while(rs.next()) {
        Integer value = rs.getInt(1);
        assertTrue(expected.toString() + " " + value, expected.remove(value));
      }
      assertTrue(expected.isEmpty());
    }
  
    clientSQLExecute(1, "drop table EMP.REPLTABLE1");
    clientSQLExecute(1, "drop table EMP.REPLTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE1");
  }
}

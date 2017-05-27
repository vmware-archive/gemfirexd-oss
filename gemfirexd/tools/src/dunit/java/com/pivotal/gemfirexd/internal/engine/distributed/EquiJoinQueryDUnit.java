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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
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
 * @author Asif
 * @since 6.0
 * 
 */
@SuppressWarnings("serial")
public class EquiJoinQueryDUnit extends DistributedSQLTestBase {

  public EquiJoinQueryDUnit(String name) {
    super(name);
  }

  public void testColocation() throws Exception {
    // Create a table from client using partition by column
    // Start one client and three servers
    startVMs(1, 3);

    clientSQLExecute(
        1,
        " create table trade.customers " +
        "(cid int not null, cust_name varchar(100), since date, " +
        "addr varchar(100), tid int, primary key (cid))  partition by column (cid)");

    clientSQLExecute(
        1,
        " create table trade.portfolio " +
        "(cid int not null, sid int not null, qty int not null, " +
        "availQty int not null, subTotal decimal(30,20), tid int, " +
        "constraint portf_pk primary key (cid, sid), " +
        "constraint cust_fk foreign key (cid) references trade.customers (cid) on delete restrict, " +
        "constraint qty_ck check (qty>=0), constraint avail_ch check (availQty>=0 and availQty<=qty))  " +
        "partition by column (cid) colocate with (trade.customers)");

    clientSQLExecute(
        1,
        "create table trade.sellorders (oid int not null constraint orders_pk primary key, " +
        "cid int, sid int, qty int, ask decimal (30, 20), order_time timestamp, " +
        "status varchar(10) default 'open', tid int, " +
        "constraint portf_fk foreign key (cid, sid) references trade.portfolio (cid, sid) " +
        "on delete restrict, " +
        "constraint status_ch check (status in ('cancelled', 'open', 'filled')))  " +
        "partition by column (cid) colocate with (trade.portfolio)");
    clientSQLExecute(1, "select * from trade.customers c LEFT OUTER JOIN " +
    		"trade.portfolio f LEFT OUTER JOIN trade.sellorders so on f.cid = so.cid " +
    		"on c.cid= f.cid where f.tid = 0");
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
            + "PARTITION BY COLUMN ( ID2 ) Colocate with ( TESTTABLE1 )"+getOverflowSuffix());

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
             { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(7)  },
             //each condition2
             { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(9), new Integer(NodesPruningHelper.ORing)  }            
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
           es.executeQuery(queryString);
           NodesPruningHelper.validateNodePruningForQuery(queryString, sqiArr[0], (Object[][])queries[i][1],this, actArr[0]);
           
           getLogWriter().info("Query " + (i+1) + " : succeeded");
           es.close();
         }     
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      //clientSQLExecute(1, "Drop table TESTTABLE1 ");
      clientSQLExecute(1, "Drop table TESTTABLE2 ");
      clientSQLExecute(1, "Drop table TESTTABLE1 ");
    }

  }
  
  /**
   * Tests Exception string
   * 
   */ 
   public void testException_1()
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
             + "PARTITION BY COLUMN ( ID2 )  "+getOverflowSuffix());

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
              { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(7)  },
              //each condition2
              { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(9), new Integer(NodesPruningHelper.ORing)  }            
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
            es.executeQuery(queryString);    
            fail("The equijoin query without colocation should have failed");
         }
     } catch (SQLException sqle) {
       getLogWriter().info("Expected exception=" + sqle.getMessage(), sqle);
       assertTrue(sqle.getMessage().indexOf("not colocated with ") != -1);
     } finally {
       GemFireXDQueryObserverHolder
           .setInstance(new GemFireXDQueryObserverAdapter());
       clientSQLExecute(1, "Drop table TESTTABLE1 ");
       clientSQLExecute(1, "Drop table TESTTABLE2 ");
     }
   }

   /**
    * Tests Exception string
    * 
    */ 
    public void testException_2()
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
              + "PARTITION BY COLUMN ( ID2 ) Colocate with ( TESTTABLE1 )"+getOverflowSuffix());

      // Insert values 1 to 8
      for (int i = 0; i <= 8; ++i) {
        clientSQLExecute(1, "insert into TESTTABLE1 values (" + (i + 1)
            + ", 'First1" + (i + 1) + "', 'J1 604')");
        clientSQLExecute(1, "insert into TESTTABLE2 values (" + (i + 1)
            + ", 'First2" + (i + 1) + "', 'J2 604')");
      }
      Object[][] queries = new Object[][] {
        //query 1
          {  "select ID1, DESCRIPTION2 from TESTTABLE1, TESTTABLE2 where  ID1 IN (7,9) ",
             //Conditions
            new Object[][] {
              //each condition1
               { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(7)  },
               //each condition2
               { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(9), new Integer(NodesPruningHelper.ORing)  }            
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
             es.executeQuery(queryString);    
             fail("The equijoin query without colocation should have failed");
          }
      } catch (SQLException sqle) {
        getLogWriter().info("Expected exception=" + sqle.getMessage(), sqle);
        assertTrue(sqle.getMessage().indexOf(
          "The query cannot be executed as it does not have all the "
              + "required colocation equijoin conditions") != -1);
      } finally {
        GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter());
        //clientSQLExecute(1, "Drop table TESTTABLE1 ");
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
            + "Colocate with ( TESTTABLE1 )"+getOverflowSuffix());

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
             { new Integer(NodesPruningHelper.byrange),new SQLInteger(1), Boolean.FALSE, new SQLInteger(6),Boolean.FALSE  }
                         
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
             + "Colocate with ( TESTTABLE1 )"+getOverflowSuffix());

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
              { new Integer(NodesPruningHelper.byrange),new SQLInteger(1), Boolean.FALSE, new SQLInteger(6),Boolean.FALSE  }
                          
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
              + " REPLICATE"+getOverflowSuffix());

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
  public void testEquijoinQueryForPRAndReplicatedRegionWithServerGroups()
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
    // check for syntax error
    try {
      clientSQLExecute(1, "create table EMP.PARTTABLE3 (ID12 int not null, "
          + " DESCRIPTION12 varchar(1024) not null, "
          + "ADDRESS12 varchar(1024) not null, primary key (ID12))"
          + "PARTITION BY Column (ID12) SERVER GROUPS (SG1,SG2) "
          + "COLOCATE WITH (EMP.PARTTABLE1)" + getOverflowSuffix());
    } catch (SQLException ex) {
      if (!"42X01".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    clientSQLExecute(1, "create table EMP.PARTTABLE3 (ID12 int not null, "
        + " DESCRIPTION12 varchar(1024) not null, "
        + "ADDRESS12 varchar(1024) not null, primary key (ID12))"
        + "PARTITION BY Column (ID12) COLOCATE WITH (EMP.PARTTABLE1) "
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
        + "PARTITION BY Column (ID2) COLOCATE WITH (EMP.PARTTABLE1)"+getOverflowSuffix());
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
    addExpectedException(new int[] { 1 }, null,
        UnsupportedOperationException.class);
    try {
      clientSQLExecute(1, "drop table EMP.PARTTABLE1");
      fail("Expected exception in dropping the base table");
    } catch (SQLException ex) {
      // check for the expected exception
      if (!"X0Y98".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    removeExpectedException(new int[] { 1 }, null,
        UnsupportedOperationException.class);
    clientSQLExecute(1, "drop table EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table EMP.PARTTABLE1");
  }

  /**
   * Checks for query execution on exactly one node for queries involving only
   * replicated tables. If the "checkFirst" parameter is true then it will
   * expect the query to be executed on the first VM and on no other VMs --
   * useful for testing local execution on data store nodes.
   */
  private void checkReplicatedQueryExecution(boolean checkFirst, VM... vms) {
    boolean isExecuted = false;
    for (VM vm : vms) {
      Boolean isExecutedOnNode = (Boolean)vm.invoke(
          DistributedSQLTestBase.class, "getQueryStatus");
      // for local execution derby's activation plan is generated and
      // query observers are not fired; so just check that query is not
      // fired on any of the other nodes while result checking will ensure
      // the query was executed locally
      if (checkFirst && !isExecuted) {
        isExecutedOnNode = true;
      }
      assertFalse("Did not expect query to execute on this node: "
          + vm.toString(), checkFirst && isExecuted && isExecutedOnNode);
      if (isExecutedOnNode) {
        getLogWriter().info("Query executed on " + vm.toString());
        assertFalse("Did not expect query on replicated tables to execute "
            + "on this node", isExecuted);
        isExecuted = true;
        vm.invoke(DistributedSQLTestBase.class, "reset");
      }
    }
    assertTrue("Expected query to execute on exactly one node", isExecuted);
  }

  /**
   * Tests the result and that only one node should be used for equijoin query
   * involving two replicated tables.
   */
  public void testEquijoinQueryForReplicatedRegions_1() throws Exception {
    // Create a table from client using partition by column
    // Start one client a three servers
    startVMs(1, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(1, "create table TESTTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, "
        + "ADDRESS1 varchar(1024) not null, primary key (ID1)) REPLICATE"+getOverflowSuffix());
    clientSQLExecute(1, "create table TESTTABLE2 (ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, "
        + "ADDRESS2 varchar(1024) not null, primary key (ID2)) REPLICATE"+getOverflowSuffix());

    // Insert values 1 to 8
    for (int i = 0; i <= 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE1 values (" + (i + 1)
          + ", 'First1" + (i + 1) + "', 'J1 604')");
      clientSQLExecute(1, "insert into TESTTABLE2 values (" + (i + 1)
          + ", 'First2" + (i + 1) + "', 'J2 604')");
    }
    String[][] queries = new String[][] {
    // query 1
        { "select * from TESTTABLE1, TESTTABLE2 where ID2 = 1", "query1" },
        // query 2
        { "select * from TESTTABLE1, TESTTABLE2 where ID2 = ID1 AND ID1 = 2",
            "query2" },
        // query 3
        { "select * from TESTTABLE1, TESTTABLE2 where ID2 = 1 AND ID1 = 4",
            "query3" }, };

    try {
      for (int i = 0; i < queries.length; ++i) {
        final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
        setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 },
            sqiArr);

        String queryString = queries[i][0];
        String log = "\nexecuting Query " + (i + 1) + " : \"" + queryString
            + "\"";
        getLogWriter().info(log);

        // First execute from client node
        sqlExecuteVerify(new int[] { 1 }, null, queryString, TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", queries[i][1]);
        assertFalse("Did not expect query to be executed on this node",
            isQueryExecutedOnNode);
        checkReplicatedQueryExecution(false, dataStore1, dataStore2,
            dataStore3);

        // Next execute from each of the server nodes and check that query
        // gets executed locally

        // First server node
        sqlExecuteVerify(null, new int[] { 1 }, queryString, TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", queries[i][1]);
        assertFalse("Did not expect query to be executed on this node",
            isQueryExecutedOnNode);
        checkReplicatedQueryExecution(true, dataStore1, dataStore2,
            dataStore3);
        // Second server node
        sqlExecuteVerify(null, new int[] { 2 }, queryString, TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", queries[i][1]);
        assertFalse("Did not expect query to be executed on this node",
            isQueryExecutedOnNode);
        checkReplicatedQueryExecution(true, dataStore2, dataStore1,
            dataStore3);
        // Third server node
        sqlExecuteVerify(null, new int[] { 3 }, queryString, TestUtil.getResourcesDir()
            + "/lib/checkEquiJoinQuery.xml", queries[i][1]);
        assertFalse("Did not expect query to be executed on this node",
            isQueryExecutedOnNode);
        checkReplicatedQueryExecution(true, dataStore3, dataStore1,
            dataStore2);

        getLogWriter().info("Query " + (i + 1) + " : succeeded");
      }
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      isQueryExecutedOnNode = false;
      //invokeInEveryVM(DistributedSQLTestBase.class, "reset");
      clientSQLExecute(1, "Drop table TESTTABLE1");
      clientSQLExecute(1, "Drop table TESTTABLE2");
    }
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
        "select ID2 from TESTTABLE2 where ADDRESS2 like '%604' AND ID_FK IN (select ID1 from TESTTABLE1 )", TestUtil.getResourcesDir()
            + "/lib/checkStatementQueryDUnit.xml", "testLikePredicateQuery_2",
        false, false);
  }
  
  public void testSupportedCorrelatedQueries_1() throws Exception {    
    startVMs(1, 3);
    String derbyDbUrl = "jdbc:derby:newDB;create=true;";
    if (TestUtil.currentUserName != null) {
      derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
          + TestUtil.currentUserPassword + ';');
    }
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    Connection derbyConn = DriverManager.getConnection(derbyDbUrl);
    Statement derbyStmt = derbyConn.createStatement();
    cleanDerbyArtifacts(derbyStmt, new String[0],
        new String[0], new String[]{"orders","customers"}, null);
    String table1 = "create table Customers (cust_id int primary key , cust_name varchar(1024) not null )";
    String table2 = "create table Orders (oid int  primary key, "
      + "order_type varchar(1024) not null, order_amount int,  ordered_by int )";    
    clientSQLExecute(1, table1 + "partition by column ( cust_id)"+getOverflowSuffix());
    clientSQLExecute(1,table2 + "partition by column ( ordered_by ) colocate with ( Customers) "
        +getOverflowSuffix());
    derbyStmt.executeUpdate(table1);
    derbyStmt.executeUpdate(table2);

    Connection conn = TestUtil.getConnection();    
    PreparedStatement psInsertCust = conn.prepareStatement("insert into customers values(?,?)");
    PreparedStatement psInsertCustDerby = derbyConn.prepareStatement("insert into customers values(?,?)");
    PreparedStatement ps = null;
    for (int k = 0; k < 2; ++k) {
      ps = k == 0 ? psInsertCust : psInsertCustDerby;
      for (int i = 1; i < 101; ++i) {
        ps.setInt(1, i);
        ps.setString(2, "name_" + i);
        ps.executeUpdate();

      }
    }
    
    PreparedStatement psInsertOrdersGfxd= conn.prepareStatement("insert into Orders values(?,?,?,?)");
    PreparedStatement psInsertOrdersDerby= derbyConn.prepareStatement("insert into Orders values(?,?,?,?)");
    PreparedStatement psInsertOrders = null;
    for (int l = 0; l < 2; ++l) {
      psInsertOrders = l == 0 ? psInsertOrdersGfxd : psInsertOrdersDerby;
      int j = 1;
      for (int i = 1; i < 101; ++i) {
        for (int k = 1; k < 101; ++k) {
          psInsertOrders.setInt(1, j++);
          psInsertOrders.setString(2, "order_type_" + j);
          psInsertOrders.setInt(3, i * k * 10);
          psInsertOrders.setInt(4, i);
        }

      }
    }
    try {
      String query = "Select * from Customers where ( select sum(order_amount)"
          + " from orders where ordered_by = cust_id ) > 50";
      validateResults(derbyStmt, query, -1, false);
    } finally {
      clientSQLExecute(1, "drop table orders");
      clientSQLExecute(1, "drop table customers");      
      cleanDerbyArtifacts(derbyStmt, new String[0], new String[0],
          new String[] { "orders", "customers" }, derbyConn);
      try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      }
      catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }
    }  
  }
  
  public void testSupportedCorrelatedQueries_2() throws Exception
  {
    startVMs(1, 3);
    String derbyDbUrl = "jdbc:derby:newDB;create=true;";
    if (TestUtil.currentUserName != null) {
      derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
          + TestUtil.currentUserPassword + ';');
    }
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    Connection derbyConn = DriverManager.getConnection(derbyDbUrl);
    Statement derbyStmt = derbyConn.createStatement();
    cleanDerbyArtifacts(derbyStmt, new String[0], new String[0], new String[] {
        "orders", "customers" }, null);

    String table1 = "create table Customers (cust_id int primary key , "
        + "cust_name varchar(1024) not null, address varchar(1024) )";

    String table2 = "create table Orders (oid int  primary key, "
        + "order_type varchar(1024) not null, order_amount int,  ordered_by int, "
        + "orderer_name varchar(1024) )";

    String table3 = "create table Suborders (sub_oid int  primary key, "
        + "sub_order_type varchar(1024) not null, sub_order_amount int,  sub_ordered_by int, "
        + "sub_orderer_name varchar(1024) )";

    String table4 = "create table sub_suborders (sub_sub_oid int  primary key, "
        + "sub_sub_order_type varchar(1024) not null, sub_sub_order_amount int,  sub_sub_ordered_by int, "
        + "sub_sub_orderer_name varchar(1024) )";

    clientSQLExecute(1, table1 + "partition by column (cust_id, cust_name) "
        + getOverflowSuffix());
    clientSQLExecute(1, table2
        + "partition by column ( ordered_by,orderer_name) "
        + "colocate with  (customers) " + getOverflowSuffix());
    clientSQLExecute(1, table3
        + "partition by column ( sub_ordered_by,sub_orderer_name ) "
        + "colocate with (orders) " + getOverflowSuffix());
    clientSQLExecute(1, table4
        + "partition by column ( sub_sub_ordered_by,sub_sub_orderer_name ) "
        + "colocate with (customers)" + getOverflowSuffix());
    derbyStmt.executeUpdate(table1);
    derbyStmt.executeUpdate(table2);
    derbyStmt.executeUpdate(table3);
    derbyStmt.executeUpdate(table4);
    String query1 = "Select * from Customers where "
        + " ( select sum(order_amount) from orders where ordered_by = cust_id and "
        + "orderer_name = cust_name ) > 1000 and  "
        + "( select sum(sub_order_amount) from suborders where  sub_orderer_name = cust_name "
        + " and sub_ordered_by = cust_id  and 12 = "
        + "( select Sum(sub_sub_ordered_by) from sub_suborders where " +
      "sub_sub_ordered_by = sub_ordered_by  and sub_sub_orderer_name = cust_name)" + ")  < 500 ";
    String query2 = "Select * from Customers where "
        + " ( select sum(order_amount) from orders where ordered_by = cust_id and "
        + "orderer_name = cust_name  and "
        + " oid >  ( select sum(sub_order_amount) from suborders where "
        + " sub_orderer_name = orderer_name  and sub_ordered_by = cust_id  and "
        + "sub_oid  >  ( select sum(sub_sub_order_amount) from sub_suborders where "
        + " sub_sub_orderer_name = orderer_name  and sub_sub_ordered_by = sub_ordered_by ) )"
        + ") > 100";

    String queries[] = new String[] { query1, query2 };

    Connection conn = TestUtil.getConnection();
    PreparedStatement psInsertCust = conn
        .prepareStatement("insert into customers values(?,?,?)");
    PreparedStatement psInsertCustDerby = derbyConn
        .prepareStatement("insert into customers values(?,?,?)");
    PreparedStatement ps = null;
    for (int k = 0; k < 2; ++k) {
      ps = k == 0 ? psInsertCust : psInsertCustDerby;
      for (int i = 1; i < 101; ++i) {
        ps.setInt(1, i);
        ps.setString(2, "name_" + i);
        ps.setString(3, "address_" + i);
        ps.executeUpdate();

      }
    }

    PreparedStatement psInsertOrdersGfxd = conn
        .prepareStatement("insert into Orders values(?,?,?,?,?)");
    PreparedStatement psInsertOrdersDerby = derbyConn
        .prepareStatement("insert into Orders values(?,?,?,?,?)");
     
    for (int l = 0; l < 2; ++l) {
      PreparedStatement psInsertOrders = l == 0 ? psInsertOrdersGfxd : psInsertOrdersDerby;
      int j = 1;
      for (int i = 1; i < 101; ++i) {
        for (int k = 1; k < 101; ++k) {
          psInsertOrders.setInt(1, j++);
          psInsertOrders.setString(2, "order_type_" + j);
          psInsertOrders.setInt(3, i * k * 10);
          psInsertOrders.setInt(4, i);
          psInsertOrders.setString(5, "order_name_" + j);
        }

      }
    }

    PreparedStatement psInsertSubOrdersGfxd = conn
        .prepareStatement("insert into SubOrders values" + "(?,?,?,?,?)");
    PreparedStatement psInsertSubOrdersDerby = derbyConn
        .prepareStatement("insert into SubOrders " + "values(?,?,?,?,?)");
     
    for (int l = 0; l < 2; ++l) {
      PreparedStatement psInsertSubOrders = l == 0 ? psInsertSubOrdersGfxd : psInsertSubOrdersDerby;
      int j = 1;
      for (int i = 1; i < 101; ++i) {
        for (int k = 1; k < 101; ++k) {
          psInsertSubOrders.setInt(1, j++);
          psInsertSubOrders.setString(2, "sub_order_type_" + j);
          psInsertSubOrders.setInt(3, i * k * 10);
          psInsertSubOrders.setInt(4, i);
          psInsertSubOrders.setString(5, "sub_order_name_" + j);
        }

      }
    }
    PreparedStatement psInsertSubSubOrdersGfxd = conn
        .prepareStatement("insert into SubOrders values" + "(?,?,?,?,?)");
    PreparedStatement psInsertSubSubOrdersDerby = derbyConn
        .prepareStatement("insert into SubOrders " + "values(?,?,?,?,?)");
     
    for (int l = 0; l < 2; ++l) {
      PreparedStatement  psInsertSubSubOrders = l == 0 ? psInsertSubSubOrdersGfxd : psInsertSubSubOrdersDerby;
      int j = 1;
      for (int i = 1; i < 101; ++i) {
        for (int k = 1; k < 101; ++k) {
          psInsertSubSubOrders.setInt(1, j++);
          psInsertSubSubOrders.setString(2, "sub_order_type_" + j);
          psInsertSubSubOrders.setInt(3, i * k * 10);
          psInsertSubSubOrders.setInt(4, i);
          psInsertSubSubOrders.setString(5, "sub_order_name_" + j);
        }

      }
    }

    try {
      for (String query : queries) {
        validateResults(derbyStmt, query, -1, false);
      }
    } finally {
      clientSQLExecute(1, "drop table suborders");
      clientSQLExecute(1, "drop table sub_suborders");
      clientSQLExecute(1, "drop table orders");
      clientSQLExecute(1, "drop table customers");
      cleanDerbyArtifacts(derbyStmt, new String[0], new String[0],
          new String[] { "sub_suborders", "suborders", "orders", "customers" },
          derbyConn);
      try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      }
      catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }
    }
  }

  public void testUnsupportedCorrelatedQueries_1() throws Exception {
    startVMs(1, 1);

    String table1 = "create table Customers (cust_id int primary key, "
        + "cust_name varchar(1024) not null)";
    String table2 = "create table Orders (oid int  primary key, order_type "
        + "varchar(1024) not null, order_amount int, ordered_by int)";
    String table3 = "create table Customers2 (cust_id int primary key, "
        + "cust_name varchar(1024) not null, address varchar(1024))";
    String table4 = "create table Orders2 (oid int  primary key, order_type "
        + "varchar(1024) not null, order_amount int, ordered_by int, "
        + "orderer_name varchar(1024))";

    clientSQLExecute(1, table1 + "partition by column (cust_id)"
        + getOverflowSuffix());
    clientSQLExecute(1, table2 + "partition by column (ordered_by)"
        + getOverflowSuffix());
    clientSQLExecute(1, table3 + "partition by column (cust_id, cust_name)"
        + getOverflowSuffix());
    clientSQLExecute(1, table4 + "partition by column (ordered_by, "
        + "orderer_name) colocate with (Customers2) " + getOverflowSuffix());

    try {
      try {
        String query = "Select * from Customers where ( select sum("
            + "order_amount) from orders where ordered_by = cust_id ) > 1000";
        clientSQLExecute(1, query);
        fail("Query execution should have failed "
            + "as it is not having colocated tables");
      } catch (SQLFeatureNotSupportedException sqle) {
        if (sqle.toString().indexOf("not colocated with") == -1) {
          fail("Did not get the expected exception. Exception occured: " + sqle);
        }
      }
      try {
        String query = "Select * from customers2 where ( select sum("
            + "order_amount) from Orders2 where ordered_by = cust_id ) > 1000";
        clientSQLExecute(1, query);
        fail("Should have failed  to execute as tables are not colocated");
      } catch (SQLFeatureNotSupportedException sqle) {
        if (sqle.toString().indexOf(
            "The query cannot be executed as it does not have all the "
                + "required colocation equijoin conditions") == -1) {
          fail("Did not get the expected exception. Exception occured: " + sqle);
        }
      }
    } finally {
      clientSQLExecute(1, "drop table orders");
      clientSQLExecute(1, "drop table customers");
      clientSQLExecute(1, "drop table orders2");
      clientSQLExecute(1, "drop table customers2");
    }
  }

  public void test43206() throws Exception {
    startVMs(2, 3);

    Connection conn = TestUtil.getConnection();
    conn.createStatement().execute("create table pizza (i int) ");

    ResultSet rs = conn
        .createStatement()
        .executeQuery(
            "select m.ID, m.hostdata from sys.systables t, sys.members m where "
                + "t.tablename='PIZZA' and m.hostdata = 'true'");

    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertFalse(rs.next());

    rs = conn
        .createStatement()
        .executeQuery(
            "select m.ID, m.hostdata from sys.systables t, sys.members m where "
                + "t.tablename='PIZZA' and m.hostdata = 1");

    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertFalse(rs.next());

    rs = conn
        .createStatement()
        .executeQuery(
            "select m.ID, m.hostdata from sys.systables t, sys.members m where "
                + "t.tablename='PIZZA' and m.hostdata = 1");

    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertFalse(rs.next());
  }
  
  public String getOverflowSuffix() {
    return  " ";
  }

  @Override
  protected void vmTearDown() throws Exception {
    super.vmTearDown();
    reset();
  }
}

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireSelectActivation;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;

/**
 * Tests whether the statementID , connectionID etc are being passed correctly
 * or not
 * 
 * @author Asif
 * @since 6.0
 */
@SuppressWarnings("serial")
public class StatementQueryDUnit extends DistributedSQLTestBase {

  private volatile static long connID;

  private volatile static long stmtID;
  
  private volatile static boolean skipListeners = false;

  public StatementQueryDUnit(String name) {
    super(name);
  }

  private void checkOneResult(ResultSet rs) throws SQLException {
    assertTrue("Expected one result", rs.next());
    if (rs.next()) {
      fail("unexpected next element with ID: " + rs.getObject(1));
    }
  }

  public void testSerializationData() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Create the table and insert a row
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int , "
        + "DESCRIPTION varchar(1024) not null , primary key (ID))");
    clientSQLExecute(1, "insert into EMP.TESTTABLE values (1, 'First')");
    clientSQLExecute(1, "insert into EMP.TESTTABLE values (0, 'Zero')");
    // Attach a GemFireXDQueryObserver in the server VM

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    SerializableRunnable setObserver = new SerializableRunnable(
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
                  connID = wrapper.getIncomingConnectionId();
                  stmtID = stmt.getID();
                  try {
                    EmbedConnection conn = wrapper.getConnectionForSynchronization();
                    skipListeners = conn.getLanguageConnectionContext().isSkipListeners();
                  }catch(SQLException sqle) {
                    throw new GemFireXDRuntimeException(sqle);
                  }
                }
              });
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
    dataStore1.invoke(setObserver);
    dataStore2.invoke(setObserver);
    Properties props = new Properties();
    
    props.put(com.pivotal.gemfirexd.Attribute.SKIP_LISTENERS, "true");
    
    EmbedConnection mcon = (EmbedConnection)TestUtil.getConnection(props);
    EmbedStatement es = (EmbedStatement)mcon.createStatement();
    ResultSet rs = es.executeQuery("select ID, DESCRIPTION from EMP.TESTTABLE "
        + "where id > 0");
    checkOneResult(rs);
    final long clientConnID = mcon.getConnectionID();
    final long clientStmntID = es.getID();
    SerializableRunnable validate = new SerializableRunnable(
        "validate") {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter());
          assertEquals(clientConnID, connID);
          assertEquals(clientStmntID, stmtID);
          assertTrue(skipListeners);
        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
    dataStore1.invoke(validate);
    dataStore2.invoke(validate);

    clientSQLExecute(1, "Drop table EMP.TESTTABLE ");
    clientSQLExecute(1, "Drop schema EMP restrict");

  }

  public void testRegionGetConversionForCompositeKey() throws Exception {
    // Start one client and two servers
    startServerVMs(2, 0, "SG1");
    startClientVMs(1, 0, null);

    // Create a schema with default server groups GemFire extension
    clientSQLExecute(1, "create schema EMP default server groups (SG1)");

    // Create the table and insert a row
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int , "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024),"
        + " Primary Key (ID, DESCRIPTION))");
    clientSQLExecute(1, "insert into EMP.TESTTABLE values (1, 'First', 'asif')");
    // Attach a GemFireXDQueryObserver in the server VM

    final boolean[] callback = new boolean[] { false };
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void beforeGemFireResultSetExecuteOnActivation(
                AbstractGemFireActivation activation) {
              StatementQueryDUnit.getGlobalLogger().info(
                  "StatementQueryDunit::testRegionGetConversionForCompositeKey: "
                      + "The activation object is = " + activation);
              assertTrue(activation instanceof GemFireSelectActivation);
              callback[0] = true;
            }

          });

      TestUtil.setupConnection();
      EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();
      ResultSet rs = es
          .executeQuery("select ID, DESCRIPTION from EMP.TESTTABLE "
              + "where id = 1 AND DESCRIPTION = 'First'");
      assertTrue(rs.next());
      assertEquals(rs.getInt(1), 1);
      assertTrue(callback[0]);
      assertFalse(rs.next());

      clientSQLExecute(1, "Drop table EMP.TESTTABLE ");
      clientSQLExecute(1, "Drop schema EMP restrict");
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
    }

  }

  public void testBug39364() throws Exception {
    startVMs(1, 1);
    final String queryStr = "select ID, DESCRIPTION from EMP.TESTTABLE "
        + "where id IN (1,2,3) order by ID asc";
    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Create the table and insert a row
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int , "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024), " +
        	"primary key (ID))");

    // clientSQLExecute(1, "create index indx on EMP.TESTTABLE (ID)");
    clientSQLExecute(1,
        "insert into EMP.TESTTABLE values (1, 'First', 'J 604')");
    clientSQLExecute(1,
        "insert into EMP.TESTTABLE values (2, 'First', 'J 604')");
    clientSQLExecute(1,
        "insert into EMP.TESTTABLE values (3, 'First', 'J 604')");
    // Execute the query on server1.
    TestUtil.setupConnection();
    EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();

    ResultSet rs = es.executeQuery(queryStr);
    for (int i = 0; i < 3; ++i) {
      rs.next();
      assertEquals(rs.getInt(1), (i + 1));
    }
    assertFalse(rs.next());
  }

  public void testComputeNodesBehaviourForPartitionKeySameAsPrimaryKey()
      throws Exception {
    // Create a table from client using range partitioning enabled.
    // Check for PARTITION BY RANGE
    // Start one client a two servers
    String query = "select ID, DESCRIPTION from TESTTABLE where ID  > 6 AND ID =7 ";
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) "
        + "not null, primary key (ID)) PARTITION BY RANGE ( ID )"
        + " (VALUES BETWEEN -Infinity and 5, "
        + "VALUES BETWEEN 5 and +Infinity)");

    // Insert values 1 to 8
    for (int i = 0; i < 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    try {
      // Create a prepared statement for the query & store the queryinfo so
      // obtained to get
      // region reference
      final SelectQueryInfo[] sqi = new SelectQueryInfo[1];
      final Activation[] actArr = new Activation[1];

      NodesPruningHelper.setupObserverOnClient(sqi, actArr);
      TestUtil.setupConnection();
      EmbedStatement stmnt = (EmbedStatement)TestUtil.jdbcConn
          .createStatement();
      /*Set rslts = new HashSet();
       for (int i = 5; i < 10; ++i) {
       rslts.add(Integer.valueOf(i));
       }*/

      ResultSet rs = stmnt.executeQuery(query);
      checkOneResult(rs);
      Set<DistributedMember> members = new HashSet<DistributedMember>();
      Set<Object> routingKeys = new HashSet<Object>();
      routingKeys.add(ResolverUtils.TOK_ALL_NODES);
      sqi[0].computeNodes(routingKeys, actArr[0], false);
      members.addAll(NodesPruningHelper.convertRoutingKeysIntoMembersForPR(
          routingKeys, (PartitionedRegion)sqi[0].getRegion()));
      // Identyify all the nodes of the PR
      PartitionedRegion pr = (PartitionedRegion)sqi[0].getRegion();
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr
          .getPartitionResolver();
      Object[] routingObjects = spr.getRoutingObjectsForRange(new SQLInteger(5),
          true, null, true);
      Set<?> prunedNodes = pr.getMembersFromRoutingObjects(routingObjects);
      assertEquals(members.size(), prunedNodes.size());
      members.removeAll(prunedNodes);
      assertTrue(members.isEmpty());
    }
    finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }

  }

 /**
  * Tests the nodes pruning logic when partition by column is specified
  * with partitioning key same as primary key
  */ 
  public void testComputeNodesBehaviourWithSingleColumnAsPartitoningKey_1()
      throws Exception {
    // Create a table from client using partition by column 
    // Start one client a three servers
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) "
        + "not null, primary key (ID)) PARTITION BY COLUMN ( ID )");

    // Insert values 1 to 8
    for (int i = 0; i <= 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    Object[][] queries = new Object[][] {
      //query 1
        {  "select ID, DESCRIPTION from TESTTABLE where ID  > 6 AND ID = 7 ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bycolumn), new SQLInteger(7) },
             //each condition2
            // { new Integer(byrange), new Integer(6), Boolean.FALSE,
            //   null, null, new Integer(ANDing) }
           }
        },       
        //query 2
        {  "select ID, DESCRIPTION from TESTTABLE where ID  > 6 AND ID IN ( 7 )",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bycolumn), new SQLInteger(7) },
             //each condition2
            // { new Integer(byrange), new Integer(6), Boolean.FALSE,
            //   null, null, new Integer(ANDing) }
           }
        },       
        //query 3
        {  "select ID, DESCRIPTION from TESTTABLE where  ID IN ( 7 ,9)",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(7)  },
             //each condition2
             { new Integer(NodesPruningHelper.bycolumn), new SQLInteger(9),
               new Integer(NodesPruningHelper.ORing)  }            
           }
        },       
       //query 4
        {  "select ID, DESCRIPTION from TESTTABLE where  ID IN ( 7 ,8) OR ID = 6 ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bylist),new SQLInteger(7),
               new SQLInteger(8), new SQLInteger(6)  }
           }
        } ,      
 
       //query 5
        {  "select ID, DESCRIPTION from TESTTABLE where  ID > 7 AND  ID < 66 ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.allnodes)  }
           }
        } 
       //query 6
       /* {  "select ID, DESCRIPTION from TESTTABLE where  ID >= 7 AND  ID <= 7 ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(bycolumn),new Integer(7)  }
           }
        }*/      
    };
    try {
      for (int i = 0; i < queries.length; ++i) {
        final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
        final Activation[] actArr = new Activation[1];
        NodesPruningHelper.setupObserverOnClient(sqiArr, actArr);

        String queryString = (String)queries[i][0];
        EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();

        String log = "\nexecuting Query " + (i + 1) + " : \"" + es.getSQLText()
            + "\"";

        getLogWriter().info(log);
        ResultSet rs = es.executeQuery(queryString);
        NodesPruningHelper.validateNodePruningForQuery(queryString, sqiArr[0],
            (Object[][])queries[i][1], this, actArr[0]);
        rs.close();

        getLogWriter().info("Query " + (i + 1) + " : succeeded");
      }
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }
  }

 /**
  * Tests the nodes pruning logic when partition by column is specified
  * with partitioning key and is a subset of  primary key
  */ 
  public void testComputeNodesBehaviourWithSingleColumnAsPartitoningKey_2()
      throws Exception {
    // Create a table from client using partition by column
    // Start one client and three  servers
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) "
        + "not null, primary key (ID, DESCRIPTION)) PARTITION BY COLUMN (ID)");

    // Insert values 1 to 8
    for (int i = 0; i < 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    Object[][] queries = new Object[][] {
      //query 1
        {  "select ID, DESCRIPTION from TESTTABLE where ID  > 6 AND ID = 7 ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(7)  },
           }
        },       
        //query 2
        {  "select ID, DESCRIPTION from TESTTABLE where ID  > 6 AND ID IN ( 7 )",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(7)  },
           }
        },       
       //query 4
        {  "select ID, DESCRIPTION from TESTTABLE where  ID IN ( 7 ,8) OR ID = 6 ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bylist),new SQLInteger(7),
               new SQLInteger(8), new SQLInteger(6)  }
           }
        } ,      
 
       //query 5
        {  "select ID, DESCRIPTION from TESTTABLE where  ID > 7 AND  ID < 66 ",
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
        final Activation[] actArr = new Activation[1];
        NodesPruningHelper.setupObserverOnClient(sqiArr, actArr);

        String queryString = (String)queries[i][0];
        TestUtil.setupConnection();
        EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();

        String log = "\nexecuting Query " + (i + 1) + " : \"" + es.getSQLText()
            + "\"";

        getLogWriter().info(log);
        ResultSet rs = es.executeQuery(queryString);
        NodesPruningHelper.validateNodePruningForQuery(queryString, sqiArr[0],
            (Object[][])queries[i][1], this, actArr[0]);
        rs.close();

        getLogWriter().info("Query " + (i + 1) + " : succeeded");
      }
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }
  }

 /**
  * Tests the nodes pruning logic when partition by column is specified
  * with partitioning keys  being subset of  primary keys and there
  * exists more than one partitioning keys
  */ 
  public void testComputeNodesBehaviourWithMultipleColumnsAsPartitoningKey()
      throws Exception {
    // Create a table from client using partition by column
    // Start one client and three  servers
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) "
        + "not null, NAME varchar(1024) not null, primary key "
        + "(ID, DESCRIPTION,NAME)) PARTITION BY COLUMN (DESCRIPTION, NAME)");

    // Insert values 1 to 8
    for (int i = 0; i < 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604','Asif"+(i+1)+ "' )");
    }
    Object[][] queries = new Object[][] {
      //query 1
        { "select ID, DESCRIPTION from TESTTABLE where DESCRIPTION = 'First1'"
          + " AND NAME = 'Asif1' ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bycolumn), new String("First1"),
               new String("Asif1")  },
           }
        },       
        //query 2
        {  "select ID, DESCRIPTION from TESTTABLE where ID  > 6 AND ID IN ( 7 )",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bycolumn),new SQLInteger(7)  },
           }
        },       
       //query 4
        {  "select ID, DESCRIPTION from TESTTABLE where  ID IN ( 7 ,8) OR ID = 6 ",
           //Conditions
           new Object[][] {
            //each condition1
             { new Integer(NodesPruningHelper.bylist),new SQLInteger(7),
               new SQLInteger(8), new SQLInteger(6)  }
           }
        } ,      
 
       //query 5
        {  "select ID, DESCRIPTION from TESTTABLE where  ID > 7 AND  ID < 66 ",
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
        NodesPruningHelper.setupObserverOnClient(sqiArr);

        String queryString = (String)queries[i][0];
        TestUtil.setupConnection();
        EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();

        String log = "\nexecuting Query " + (i + 1) + " : \"" + es.getSQLText()
            + "\"";

        getLogWriter().info(log);
        ResultSet rs = es.executeQuery(queryString);
        rs.close();

        getLogWriter().info("Query " + (i + 1) + " : succeeded");
      }
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(1, "Drop table TESTTABLE ");
    }
  }

  public void testLikePredicateQuery_1() throws Exception
  {
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE (ID int primary key, "
        + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) "
        + "not null, NAME varchar(1024) not null) PARTITION BY Primary Key");

    // Insert values 1 to 8
    for (int i = 0; i < 8; ++i) {
      clientSQLExecute(1, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604','Asif" + (i + 1) + "' )");
    }
    sqlExecuteVerify(new int[] { 1 }, null,
        "select ID from TESTTABLE where ADDRESS like '%604'", TestUtil.getResourcesDir()
            + "/lib/checkStatementQueryDUnit.xml", "testLikePredicateQuery_1",
        false, false);

  }

  public void testLikePredicateQuery_2() throws Exception
  {
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE1 (ID1 int primary key, "
        + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) "
        + "not null) PARTITION BY Primary key");
    clientSQLExecute(1, "create table TESTTABLE2 (ID2 int  primary key, "
        + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) "
        + "not null, ID_FK int, foreign key (ID_FK) "
        + "references TESTTABLE1(ID1))");

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
        "select ID2 from TESTTABLE2 where ADDRESS2 like '%604'", TestUtil.getResourcesDir()
            + "/lib/checkStatementQueryDUnit.xml", "testLikePredicateQuery_2",
        false, false);
  }

  public void testBug40626() throws Exception {
    startVMs(1, 3);

    clientSQLExecute(1, "create table TESTTABLE (ID int primary key, "
        + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) "
        + "not null, NAME varchar(1024) not null) PARTITION BY Primary Key");

    //Check Statement Query
    sqlExecuteVerify(new int[] { 1 }, null,
        "select ID from TESTTABLE ", TestUtil.getResourcesDir()
            + "/lib/checkStatementQueryDUnit.xml", "testBug40626",
        false, false);
    //Check prepared statement query
    sqlExecuteVerify(new int[] { 1 }, null,
        "select ID from TESTTABLE ", TestUtil.getResourcesDir()
            + "/lib/checkStatementQueryDUnit.xml", "testBug40626",
        true, false);
  }
}

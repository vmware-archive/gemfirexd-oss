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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.query.QueryEvaluationHelper;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;

/**
 * Tests the pruning behaviour of distributed queries based on partition
 * resolver , gobal indexes etc
 * 
 * @author Asif
 * @author Shoubhik
 * @since 6.0
 * 
 */
public class PrepStatementNodesPruningDUnit extends DistributedSQLTestBase {
  protected static volatile boolean isQueryExecutedOnNode = false;
  @SuppressWarnings("unused")
  private static final int byrange = 0x01, bylist = 0x02, bycolumn = 0x04, ANDing = 0x08, ORing = 0x10, Contains = 0x20, OrderBy = 0x40;
  @SuppressWarnings("unused")
  private static final int noCheck = 0x01, fixLater = noCheck|0x02, fixImmediate = noCheck|0x04, wontFix = noCheck|0x08, fixDepend = noCheck|0x10, 
                           fixInvestigate = noCheck|0x20;

  public PrepStatementNodesPruningDUnit(String name) {
    super(name);
  }
  
  
  /**
   * Tests node pruning for a query with single condition with IN
   * operator for range partitioning. All the conditions are parameterized
   * 
   */
  public void testINClauseOnRangePartitioning()
      throws Exception {

    startVMs(2, 3);

    clientSQLExecute(2,
         "create table Account (" +
                 " id varchar(10) primary key, name varchar(100), type int )" +
                 " partition by range(id)" +
                 "   ( values between 'A'  and 'B'" +
                 "    ,values between 'C'  and 'D'" +
                 "    ,values between 'E'  and 'F'" +
                 "    ,values between 'G'  and 'Z'" +
                 "    ,values between '01' and '05'" +
                 "    ,values between '05' and '15'" +
                 "    ,values between '15' and '20'" +
                 "   )" );
    TestUtil.jdbcConn.commit();
    // Insert values 1 to 20
    for (int i = 1; i < 21; ++i) {
      // Add leading zero to keep ranges valid and nonoverlapping
      clientSQLExecute(2, "insert into Account values ('"+
                (i<10?"0":"")+
                "" + i + "', 'Account " + i + "'," + (i % 2) + " )");
    }

    Object queries[][][] = { 
        {  { "select id,name from Account where id in (?,?) and type >= 0" }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("01") }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("03") }
          ,{ new Integer(2), new Integer(1), new Integer(2)  } //expectedRows, prunedNodes, noExecQueryNodes
        }
     /*  ,{  { "select id,name from Account where id in (?,?) and id < ?" }
          ,{ new Integer(Types.VARCHAR), "1" }
          ,{ new Integer(Types.VARCHAR), "5" }
          ,{ new Integer(Types.VARCHAR), "17"}
          TODO expected 2 instead of 1 (DataQueryNode returns 1 row) 
          ,{ new Integer(1), new Integer(2), new Integer(1)  } expectedRows, prunedNodes, noExecQueryNodes
        }*/
       ,{  { "select id,name from Account where id in (?,?,?) and type >= 0" }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("01") }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("05") }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("17")}
          ,{ new Integer(3), new Integer(3), new Integer(0)  } //expectedRows, prunedNodes, noExecQueryNodes
        }
      };
  
      try {
        VM[] vms = new VM[] {this.serverVMs.get(0), this.serverVMs.get(1), this.serverVMs.get(2)};
        for (Object[][] querie : queries) {
          QueryEvaluationHelper.evaluateQueriesForListPartitionResolver(querie,vms,this);
          invokeInEveryVM(QueryEvaluationHelper.class, "reset");
        } 
      } finally {
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        clientSQLExecute(2, "Drop Table Account");
        //invokeInEveryVM(QueryEvaluationHelper.class, "reset");
        getLogWriter().info("Cleared all artifacts");
      }
  }
  
  /**
   * Tests node pruning for a query with single condition with IN
   * operator for range partitioning. All the conditions are parameterized
   * 
   */
  public void testINClauseOnListPartitioning()
      throws Exception {

    startVMs(2, 3);

    clientSQLExecute(2,
        "create table Account_list (" +
                " id varchar(10) primary key, name varchar(100), type int )" +
                " partition by list(id) (" +
                "   values ('00','01') " +
                "  ,values ('02','03') " +
                "  ,values ('04','05') " +
                "  ,values ('06','07','08','09') " +
                "  ,values ('10','11','12','13') " +
                "  ,values ('14','15') " +
                "  ,values ('16','17') " +
                "   )" );
    TestUtil.jdbcConn.commit();
    // Insert values 0 to 20
    for (int i = 0; i < 21; ++i) {
      clientSQLExecute(2, "insert into Account_list values ('" + (i<10?"0"+i:i) + "', 'Account " + i + "'," + (i % 2) + " )");
    }

    Object queries[][][] = { 
        {  { "select id,name from Account_list where id in (?,?,?) and type >= 0" }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("00") }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("01") }
          ,{ new Integer(Types.VARCHAR), new SQLVarchar("04") }
          ,{ new Integer(3), new Integer(2), new Integer(1)  } //expectedRows, prunedNodes, noExecQueryNodes
        }
      };
  
    VM[] vms = new VM[] {this.serverVMs.get(0), this.serverVMs.get(1), this.serverVMs.get(2)};
    try {
      for (Object[][] querie : queries) {
          QueryEvaluationHelper.evaluateQueriesForListPartitionResolver(querie,vms,this);
      } 
      
    } finally {
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(2, "Drop Table Account_list");
      //invokeInEveryVM(QueryEvaluationHelper.class, "reset");
      getLogWriter().info("Cleared all artifacts");
    }
  }
  
/**
   * Tests node pruning for a query with single condition with inequality
   * operator for range partitioning. All the conditions are parameterized
   * 
   */
  public void testSingleConditionWhereClauseWithRangePartitioning_1()
      throws Exception {

    // Create a table from client using range partitioning enabled.
    // Check for PARTITION BY RANGE
    // Start one client a two servers
    startVMs(2, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 8 )");
  
    try {
      // Insert values 1 to 7
      for (int i = 0; i < 8; ++i) {
        clientSQLExecute(2, "insert into TESTTABLE values (" + (i + 1)
            + ", 'First" + (i + 1) + "', 'J 604')");
      }
  
      
      final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];    
      setupObservers( new VM[] {dataStore1, dataStore2, dataStore3}, sqiArr,"");
  
      TestUtil.setupConnection();
      EmbedPreparedStatement eps = (EmbedPreparedStatement)TestUtil.jdbcConn
          .prepareStatement("select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ? ");    
      
      eps.setInt(1,1 );  
      eps.setInt(2,3 );
      Set rslts = new HashSet();
      for (int i = 1; i < 4; ++i) {
        rslts.add(Integer.valueOf(i));
      }
  
      ResultSet rs = eps.executeQuery();
  
      for ( int i = 1; i < 4 ;++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      //Identify all the nodes of the PR
      PartitionedRegion pr = (PartitionedRegion)sqiArr[0].getRegion();
      //Identify the nodes which should see the query based on partition resolver
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr.getPartitionResolver();
      Object[] routingObjects = spr.getRoutingObjectsForRange(new SQLInteger(1), true, new SQLInteger(3), true);
      getLogWriter().info("Number of routing keys retrieved ="+routingObjects.length + " value1 = "+ routingObjects[0]);
      verifyQueryExecution(sqiArr[0], routingObjects, 1, 2, eps.getSQLText());

    } finally {
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(2, "Drop table TESTTABLE ");   
      //invokeInEveryVM(this.getClass(), "reset");
    }
  }
  
  /**
   * Tests node pruning for a query with multiple  conditions with inequality
   * operator for range partitioning. 
   * 
   */
  public void testMultiConditionWhereClauseWithRangePartitioning()
      throws Exception {
    startVMs(2, 3);

    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 8 )");
    // Insert values 1 to 10
    for (int i = 1; i <= 10; ++i) {
      clientSQLExecute(2, "insert into TESTTABLE values (" + i
          + ", 'First" + i + "', 'J 604')");
    }

    Object queries[][][][] = { 
         { {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ?  AND ID <= ? AND ID >= ?" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(0), new Integer(0), new Integer(3) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            ,{ new Integer(ANDing) , new Integer(4), true, new Integer(7), true, 1 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes
           }
         }//1
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ?  AND ID <= ? AND ID >= ?" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(2), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(5), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            ,{ new Integer(ANDing) , new Integer(4), true, new Integer(7), true, 1 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes*/
           }
         }//2
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ?  AND ID <= 7 AND ID >= 4" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(0), new Integer(0), new Integer(3) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            ,{ new Integer(ANDing) , new Integer(4), true, new Integer(7), true, 1 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes
           }
         }//3
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ?  AND ID <= 7 AND ID >= 4" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(5) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(2), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(5), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
             //TODO soubhik last param should be 1. Due to bug its 2.
            ,{ new Integer(ANDing) , new Integer(4), true, new Integer(7), true, 1 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes
           }
         }//4
               /*Params and constants will flock together and ranges should be 1...3 and 4...7*/
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? AND ID <= 7 And ID <= ? AND ID >= 4" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(0), new Integer(0), new Integer(3) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            ,{ new Integer(ANDing) , new Integer(4), true, new Integer(7), true, 1 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes
           }
         }//5
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ?  AND ID <= ? AND ID >= ?" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
            ,{ new Integer(Types.INTEGER), new Integer(5) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(1), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(5), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            ,{ new Integer(ANDing) , new Integer(5), true, new Integer(7), true, 1 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes
           }
         } //6
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? AND ID <= ? And ID <= ? AND ID >= ?" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(0), new Integer(0), new Integer(3) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            ,{ new Integer(ANDing) , new Integer(4), true, null, false,1 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes
           }
         } //7
        ,{ {{"select ID, DESCRIPTION from TESTTABLE where ID  >= ? AND ID <= ? And ID <= ? AND ID >= ?" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(7) }
            ,{ new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(2) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(0), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
            { new Integer(byrange), new Integer(7), true, null, true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           ,{ new Integer(byrange), null, true, new Integer(1), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //8
        ,{ {{"select ID, DESCRIPTION from TESTTABLE where ID  >= ? AND ID <= ? And ID >= ? AND ID <= ? AND ID != 1" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(4), true, new Integer(4),  true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            ,{ new Integer(ORing)  , new Integer(1), true, new Integer(10), true, 3 } //ANDing/ORing, lowerBound, lboundinclusive, upperBound, uboundinclusive, prunedNodes
           }
         } //9
        ,{ {{ "select ID , ADDRESS as alias3 from TESTTABLE where ID  >= ? and ID <= ?" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(10) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(10), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(10), true, new Integer(3) } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //10
    };

      try {
        VM[] vms = new VM[] {this.serverVMs.get(0), this.serverVMs.get(1), this.serverVMs.get(2)};
        QueryEvaluationHelper.evaluate4DQueryArray(queries, vms, this);
      } finally {
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        clientSQLExecute(2, "Drop table TESTTABLE ");   
        //invokeInEveryVM(QueryEvaluationHelper.class, "reset");         
     }
  }
  
  /**
   * Tests node pruning for a query with multiple  conditions with inequality
   * operator for range partitioning having a mix of constants & parameters
   * @throws Exception
   */
  public void testMixedParametersAndConstantRangeCondition() throws Exception{

    // Create a table from client using range partitioning enabled.
    // Check for PARTITION BY RANGE
    // Start one client and two servers
    startVMs(2, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 8 )");

    try {
      // Insert values 1 to 9
      for (int i = 0; i < 9; ++i) {
        clientSQLExecute(2, "insert into TESTTABLE values (" + (i + 1)
            + ", 'First" + (i + 1) + "', 'J 604')");
      }
      // Create a prepared statement for the query & store the queryinfo so
      // obtained to get
      // region reference
      final SelectQueryInfo[] sqi = new SelectQueryInfo[1];    
      // set up a sql query observer in client VM
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qi, GenericPreparedStatement gps, LanguageConnectionContext lcc) {            
              sqi[0] = (SelectQueryInfo)qi;
            }
          });
  
      // set up a sql query observer in server VM to keep track of whether the
      // query got executed on the node or not
      SerializableRunnable setObserver = new SerializableRunnable(
          "Set GemFireXDObserver on DataStore Node") {
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
      dataStore1.invoke(setObserver);
      dataStore2.invoke(setObserver);
      dataStore3.invoke(setObserver);

      TestUtil.setupConnection();
      EmbedPreparedStatement eps = (EmbedPreparedStatement)TestUtil.jdbcConn
          .prepareStatement("select ID, DESCRIPTION from TESTTABLE "
              + "where ID  >= ? And ID <= ?  AND ID <= 7 AND ID >= 4");

      SerializableRunnable validateQueryExecution = new SerializableRunnable(
          "validate node has executed the query") {
        @Override
        public void run() throws CacheException {
          try {
            GemFireXDQueryObserverHolder
                .setInstance(new GemFireXDQueryObserverAdapter());
            assertTrue(isQueryExecutedOnNode);
            isQueryExecutedOnNode = false;
  
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }
        }
      };
      SerializableRunnable validateNoQueryExecution = new SerializableRunnable(
          "validate node has NOT executed the query") {
        @Override
        public void run() throws CacheException {
          try {
            GemFireXDQueryObserverHolder
                .setInstance(new GemFireXDQueryObserverAdapter());
            assertFalse(isQueryExecutedOnNode);
            isQueryExecutedOnNode = false;
  
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }
        }
      };
      eps.setInt(1,1 );  
      eps.setInt(2,3 );      
      
      ResultSet rs = eps.executeQuery();
      assertFalse(rs.next());   
      
      PartitionedRegion pr = (PartitionedRegion)sqi[0].getRegion();
      Set<InternalDistributedMember> nodesOfPr =  pr.getRegionAdvisor().adviseDataStore();
      //Identify the nodes which should see the query based on partition resolver
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr.getPartitionResolver();
      
      Object[] routingObjects1 = spr.getRoutingObjectsForRange(new SQLInteger(1), true, new SQLInteger(3), true);
      assertEquals(routingObjects1.length, 1);
      Object[] routingObjects2 = spr.getRoutingObjectsForRange(new SQLInteger(4), true, new SQLInteger(7), true);
  //    getLogWriter().info("Number of routing keys retrieved ="+routingObjects.length + " value1 = "+ routingObjects[0]);
     
      assertEquals(routingObjects2.length, 1);
      assertFalse(routingObjects1[0] == routingObjects2[0]);
      int i= 0;
      for( ; i < routingObjects1.length;++i) {     
        getLogWriter().info("Gfxd:: Range1: Routing key="+routingObjects1[i]);
      }
      
      for(int j = 0 ; i < routingObjects2.length;++j) {        
        getLogWriter().info("Gfxd:: Range2: Routing key="+routingObjects2[j]);
      }
      Set prunedNodes1 = pr.getMembersFromRoutingObjects(routingObjects1);
      Set prunedNodes2 = pr.getMembersFromRoutingObjects(routingObjects2);
      Set prunedNodes = new HashSet();
      prunedNodes.addAll(prunedNodes1);
      prunedNodes.retainAll(prunedNodes2);
      assertTrue(prunedNodes.isEmpty());     
      //Nodes not executing the query 
      nodesOfPr.removeAll(prunedNodes);
      //Does not include the client
      assertTrue( nodesOfPr.size()  > 0 );
      Iterator<InternalDistributedMember> itr = nodesOfPr.iterator();
      while( itr.hasNext()) {
        DistributedMember member = itr.next();
        VM nodeVM = this.getHostVMForMember(member);
         nodeVM.invoke(validateNoQueryExecution);
        assertNotNull(nodeVM);
      }      
      //Nodes executing the query
      
      itr = prunedNodes.iterator();
      while( itr.hasNext()) {
        DistributedMember member = itr.next();
        VM nodeVM = this.getHostVMForMember(member);
        assertNotNull(nodeVM);
        nodeVM.invoke(validateQueryExecution);
        
      }
      
    } finally {
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      //invokeInEveryVM(this.getClass(), "reset");
      clientSQLExecute(2, "Drop table TESTTABLE ");   
    }
  }
  
  /**
   * Tests node pruning for a query with multiple  conditions with equality operator
   * so that it can utilize the List partition resolver.
   * Both the conditions 
   * 
   * @throws Exception
   */
  public void testOrJunctionWithListResolver_1() throws Exception {

    // Create a table from client using range partitioning enabled.
    // Check for PARTITION BY RANGE
    // Start one client a two servers
    startVMs(2, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + " PARTITION BY LIST ( ID ) ( VALUES (1, 2,3,4,5), VALUES (6,7,8) ) ");

    // Insert values 1 to 9
    for (int i = 0; i < 9; ++i) {
      clientSQLExecute(2, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    try {
      final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqiArr,
          "");

      TestUtil.setupConnection();
      EmbedPreparedStatement eps = (EmbedPreparedStatement)TestUtil.jdbcConn
          .prepareStatement("select ID, DESCRIPTION from TESTTABLE where (ID  = ? OR ID = ?) AND ADDRESS = 'J 604'");

      eps.setInt(1, 1);
      eps.setInt(2, 2);

      Set rslts = new HashSet();
      for (int i = 1; i < 3; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = eps.executeQuery();

      for (int i = 1; i < 3; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      // Identyify all the nodes of the PR
      PartitionedRegion pr = (PartitionedRegion)sqiArr[0].getRegion();
      // Identify the nodes which should see the query based on partition
      // resolver
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr
          .getPartitionResolver();
      List<DataValueDescriptor> temp = new ArrayList<DataValueDescriptor>();
      temp.add(new SQLInteger(1));
      temp.add(new SQLInteger(2));
      DataValueDescriptor[] dvdarr = {new SQLInteger(1), new SQLInteger(2)};
      //Object[] routingObjects = spr.getRoutingObjectsForList((DataValueDescriptor[])temp.toArray());
      Object[] routingObjects = spr.getRoutingObjectsForList(dvdarr);
      assertEquals(routingObjects.length, 2);

      // Set routingkeysSet = new HashSet();

      for (int i = 0; i < routingObjects.length; ++i) {
       // DataValueDescriptor[] dvdArr = new DataValueDescriptor[1];
       // dvdArr[0] = new SQLInteger(((Integer)routingObjects[i]).intValue());
       // GemFireKey gfk = new GemFireKey(dvdArr);
       // routingObjects[i] = gfk;
        getLogWriter().info(
            "Gfxd::testOrJunctionWithListResolver : Routing key="
                + routingObjects[i]);
      }

      verifyQueryExecution(sqiArr[0], routingObjects, 1, 2,eps.getSQLText());
    }
    finally {
      //invokeInEveryVM(this.getClass(), "reset");
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(2, "Drop table TESTTABLE ");
    }
  }

  /**
   * Tests node pruning for a query with multiple  conditions with equality operator
   * so that it can utilize the List partition resolver.
   * Both the conditions 
   * 
   * @throws Exception
   */
  public void testOrJunctionWithListResolver_2() throws Exception {

    // Create a table from client using range partitioning enabled.
    // Check for PARTITION BY RANGE
    // Start one client a two servers
    startVMs(2, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + " PARTITION BY LIST ( ID ) ( VALUES (1, 2,3,4,5), VALUES (6,7,8) ) ");

    // Insert values 1 to 9
    for (int i = 0; i < 9; ++i) {
      clientSQLExecute(2, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
    final String query = "select ID, DESCRIPTION from TESTTABLE where (ID  = 1 OR ID = 2) AND ADDRESS = 'J 604'";
    try {
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqiArr,
          query);
      TestUtil.setupConnection();
      EmbedStatement es = (EmbedStatement)TestUtil.jdbcConn.createStatement();

      Set rslts = new HashSet();
      for (int i = 1; i < 3; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = es.executeQuery(query);

      for (int i = 1; i < 3; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      // Identyify all the nodes of the PR
      PartitionedRegion pr = (PartitionedRegion)sqiArr[0].getRegion();

      // Identify the nodes which should see the query based on partition
      // resolver
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr
          .getPartitionResolver();
      List<DataValueDescriptor> temp = new ArrayList<DataValueDescriptor>();
      temp.add(new SQLInteger(1));
      temp.add(new SQLInteger(2));
      DataValueDescriptor[] dvdarr = {new SQLInteger(1), new SQLInteger(2)};
      //Object[] routingObjects = spr.getRoutingObjectsForList((DataValueDescriptor[])temp.toArray());
      Object[] routingObjects = spr.getRoutingObjectsForList(dvdarr);
      assertEquals(routingObjects.length, 2);

      for (int i = 0; i < routingObjects.length; ++i) {
        //DataValueDescriptor[] dvdArr = new DataValueDescriptor[1];
       // dvdArr[0] = new SQLInteger(((Integer)routingObjects[i]).intValue());
       // GemFireKey gfk = new GemFireKey(dvdArr);
       // routingObjects[i] = gfk;
        getLogWriter().info(
            "Gfxd::testOrJunctionWithListResolver : Routing key="
                + routingObjects[i]);
      }

      verifyQueryExecution(sqiArr[0], routingObjects, 1, 2, es.getSQLText());
    }
    finally {

      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(2, "Drop table TESTTABLE ");
    }

  }
  
  /**
   * Tests node pruning for a query with an IN condition along with an
   * AND junction
   * so that it can utilize the List partition resolver.
   * Both the conditions 
   * 
   * @throws Exception
   */
  public void testInClauseWithAnANDCondition_1_Bug39553() throws Exception {

    startVMs(2, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + " PARTITION BY LIST ( ID ) ( VALUES (1, 2), VALUES (3,4,5), VALUES (6,7,8) ) ");

    // Insert values 1 to 9
    for (int i = 0; i < 9; ++i) {
      clientSQLExecute(2, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    try {
      final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqiArr,
          "");

      TestUtil.setupConnection();
      EmbedPreparedStatement eps = (EmbedPreparedStatement)TestUtil.jdbcConn
          .prepareStatement("select ID, DESCRIPTION from TESTTABLE where  (ID  IN   (?,?) OR ID = ?) AND DESCRIPTION = 'First1'") ;

      eps.setInt(1, 1);
      eps.setInt(2, 2);
      eps.setInt(3, 3);

      Set rslts = new HashSet();
      for (int i = 1; i < 2; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = eps.executeQuery();

      for (int i = 1; i < 2; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      // Identyify all the nodes of the PR
      PartitionedRegion pr = (PartitionedRegion)sqiArr[0].getRegion();
      // Identify the nodes which should see the query based on partition
      // resolver
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr
          .getPartitionResolver();
      List<DataValueDescriptor> temp = new ArrayList<DataValueDescriptor>();
      temp.add(new SQLInteger(1));
      temp.add(new SQLInteger(2));
      temp.add(new SQLInteger(3));
      DataValueDescriptor[] dvdarr = {new SQLInteger(1), new SQLInteger(2), new SQLInteger(3)};
      //Object[] routingObjects = spr.getRoutingObjectsForList((DataValueDescriptor[])temp.toArray());
      Object[] routingObjects = spr.getRoutingObjectsForList(dvdarr);
      assertEquals(routingObjects.length, 3);

      // Set routingkeysSet = new HashSet();
     
      for (int i = 0; i < routingObjects.length; ++i) {
      //  DataValueDescriptor[] dvdArr = new DataValueDescriptor[1];
      //  dvdArr[0] = new SQLInteger(((Integer)routingObjects[i]).intValue());
      //  GemFireKey gfk = new GemFireKey(dvdArr);
      //  routingObjects[i] = gfk;
        getLogWriter().info(
            "Gfxd::testOrJunctionWithListResolver : Routing key="
                + routingObjects[i]);
      }
       
      verifyQueryExecution(sqiArr[0], routingObjects, -1, -1, eps.getSQLText());
    }
    finally {
      //invokeInEveryVM(this.getClass(), "reset");
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(2, "Drop table TESTTABLE ");
    }
  }
  
  /**
   * Tests node pruning for a query with an IN condition along with an
   * AND junction
   * so that it can utilize the List partition resolver.
   * Both the conditions 
   * 
   * @throws Exception
   */
  public void testInClauseWithAnANDCondition_2_Bug39555() throws Exception {

    // Create a table from client using range partitioning enabled.
    // Check for PARTITION BY RANGE
    // Start one client a two servers
    startVMs(2, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 10 )");

    // Insert values 1 to 9
    for (int i = 0; i < 9; ++i) {
      clientSQLExecute(2, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    try {
      final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqiArr,
          "");

      TestUtil.setupConnection();
      EmbedPreparedStatement eps = (EmbedPreparedStatement)TestUtil.jdbcConn
          .prepareStatement("select id,description from TESTTABLE where ID in (?,?) and ID < ? AND ID >= ?") ;

      eps.setInt(1, 1);
      eps.setInt(2, 7);
      eps.setInt(3, 3);
      eps.setInt(4, 1);

      Set rslts = new HashSet();
      for (int i = 1; i < 2; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = eps.executeQuery();

      for (int i = 1; i < 2; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      PartitionedRegion pr = (PartitionedRegion)sqiArr[0].getRegion();
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr.getPartitionResolver();
      Object[] routingObjects = spr.getRoutingObjectsForRange(new SQLInteger(1), true, new SQLInteger(3), true);
      assertTrue(routingObjects.length == 1);
     
      // Set routingkeysSet = new HashSet();

      for (int i = 0; i < routingObjects.length; ++i) {
       // DataValueDescriptor[] dvdArr = new DataValueDescriptor[1];
       // dvdArr[0] = new SQLInteger(((Integer)routingObjects[i]).intValue());
       // GemFireKey gfk = new GemFireKey(dvdArr);
      //  routingObjects[i] = gfk;
        getLogWriter().info(
            "Gfxd::testOrJunctionWithListResolver : Routing key="
                + routingObjects[i]);
      }

      verifyQueryExecution(sqiArr[0], routingObjects, 1, 2, eps.getSQLText());
    }
    finally {
      //invokeInEveryVM(this.getClass(), "reset");
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(2, "Drop table TESTTABLE ");
    }
  }

  /**
   * Tests node pruning for a query with an IN condition along with an
   * AND junction
   * so that it can utilize the List partition resolver.
   * Both the conditions 
   * 
   * @throws Exception
   */
  public void testInClauseWithAnANDCondition_3_Bug39555() throws Exception {

    // Create a table from client using range partitioning enabled.
    // Check for PARTITION BY RANGE
    // Start one client a two servers
    startVMs(2, 3);

    VM dataStore1 = this.serverVMs.get(0);
    VM dataStore2 = this.serverVMs.get(1);
    VM dataStore3 = this.serverVMs.get(2);
    clientSQLExecute(2,
        "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 10 )");

    // Insert values 1 to 9
    for (int i = 0; i < 9; ++i) {
      clientSQLExecute(2, "insert into TESTTABLE values (" + (i + 1)
          + ", 'First" + (i + 1) + "', 'J 604')");
    }
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    try {
      final SelectQueryInfo[] sqiArr = new SelectQueryInfo[1];
      setupObservers(new VM[] { dataStore1, dataStore2, dataStore3 }, sqiArr,
          "");

      TestUtil.setupConnection();
      EmbedPreparedStatement eps = (EmbedPreparedStatement)TestUtil.jdbcConn
          .prepareStatement("select id,description from TESTTABLE where ID = ? and description = 'First1'") ;

      eps.setInt(1, 1);
      Set rslts = new HashSet();
      for (int i = 1; i < 2; ++i) {
        rslts.add(Integer.valueOf(i));
      }

      ResultSet rs = eps.executeQuery();

      for (int i = 1; i < 2; ++i) {
        assertTrue(rs.next());
        assertTrue(rslts.remove(Integer.valueOf(rs.getInt(1))));
      }
      assertTrue(rslts.isEmpty());
      assertFalse(rs.next());
      PartitionedRegion pr = (PartitionedRegion)sqiArr[0].getRegion();
      GfxdPartitionResolver spr = (GfxdPartitionResolver)pr.getPartitionResolver();
      Object[] routingObjects = spr.getRoutingObjectsForRange(new SQLInteger(1), true, new SQLInteger(3), true);
      assertTrue(routingObjects.length == 1);
     
      // Set routingkeysSet = new HashSet();

      for (int i = 0; i < routingObjects.length; ++i) {
       // DataValueDescriptor[] dvdArr = new DataValueDescriptor[1];
       // dvdArr[0] = new SQLInteger(((Integer)routingObjects[i]).intValue());
       // GemFireKey gfk = new GemFireKey(dvdArr);
      //  routingObjects[i] = gfk;
        getLogWriter().info(
            "Gfxd::testOrJunctionWithListResolver : Routing key="
                + routingObjects[i]);
      }

      verifyQueryExecution(sqiArr[0], routingObjects, 1, 2, eps.getSQLText());
    }
    finally {
      //invokeInEveryVM(this.getClass(), "reset");
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      clientSQLExecute(2, "Drop table TESTTABLE ");
    }
  }

  private void setupObservers(VM[] dataStores, final SelectQueryInfo[] sqi, String queryStr) {
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    //final SelectQueryInfo[] sqi = _qi;    
    // set up a sql query observer in client VM
    GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter() {
          @Override
          public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qi, GenericPreparedStatement gps, LanguageConnectionContext lcc) {            
            sqi[0] = (SelectQueryInfo)qi;
          }
        });

    // set up a sql query observer in server VM to keep track of whether the
    // query got executed on the node or not
    SerializableRunnable setObserver = new SerializableRunnable(
        "Set GemFireXDObserver on DataStore Node for query " + queryStr) {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter() {
                @Override
                public void beforeQueryExecutionByPrepStatementQueryExecutor(
                    GfxdConnectionWrapper wrapper, EmbedPreparedStatement pstmt, String query) {
                  getLogWriter().info("Observer::"
                      + "beforeQueryExecutionByPrepStatementQueryExecutor invoked");
                  isQueryExecutedOnNode = true;
                }
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
    
    for (VM dataStore : dataStores) {
      dataStore.invoke(setObserver);
    }
  }

  /**
   * 
   * @param sqi
   * @param routingObjects
   * @param noOfPrunedNodes
   * @param noOfNoExecQueryNodes  Query shouldn't get executed on exculding client nodes.
   */
  private void verifyQueryExecution(final SelectQueryInfo sqi, Object[] routingObjects, int noOfPrunedNodes, int noOfNoExecQueryNodes, String query) {
    PartitionedRegion pr = (PartitionedRegion)sqi.getRegion();
    Set prunedNodes = (routingObjects.length!=0?pr.getMembersFromRoutingObjects(routingObjects):new HashSet());
    getLogWriter().info("Number of members found after prunning ="+prunedNodes.size());
    verifyExecutionOnDMs(sqi, prunedNodes, noOfPrunedNodes, noOfNoExecQueryNodes,query);
  }

  private void verifyExecutionOnDMs(final SelectQueryInfo sqi, Set<DistributedMember> prunedNodes, int noOfPrunedNodes, int noOfNoExecQueryNodes, String query) {

    SerializableRunnable validateQueryExecution = new SerializableRunnable(
        "validate node has executed the query " + query) {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter());
          assertTrue(isQueryExecutedOnNode);
          isQueryExecutedOnNode = false;

        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
    SerializableRunnable validateNoQueryExecution = new SerializableRunnable(
        "validate node has NOT executed the query " + query) {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter());
          assertFalse(isQueryExecutedOnNode);
          isQueryExecutedOnNode = false;

        }
        catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
    PartitionedRegion pr = (PartitionedRegion)sqi.getRegion();
    Set<InternalDistributedMember> nodesOfPr =  pr.getRegionAdvisor().adviseDataStore();
    getLogWriter().info(" Total members = " + nodesOfPr.size());
    
    String logPrunedMembers = new String();
    logPrunedMembers = " Prunned member(s) " + prunedNodes.size() + "\n";
    for( DistributedMember dm : prunedNodes ) {
      if( dm != null) {
        logPrunedMembers += dm.toString() + " ";
      }
    }
    getLogWriter().info(logPrunedMembers);
    if( noOfPrunedNodes  > -1)
    assertEquals(noOfPrunedNodes, prunedNodes.size());
    
    nodesOfPr.removeAll(prunedNodes);   
    String logNonPrunedMembers = new String();
    logNonPrunedMembers = " Non-prunned member(s) " + nodesOfPr.size() + "\n";
    for(DistributedMember dm : nodesOfPr) {
      if( dm != null) {
        logNonPrunedMembers += dm.toString() + " ";
      }
    }
    getLogWriter().info(logNonPrunedMembers);
if(noOfNoExecQueryNodes > -1)
    assertEquals(noOfNoExecQueryNodes,nodesOfPr.size());
    
    //Nodes not executing the query 
    for( DistributedMember memberNoExec : nodesOfPr) {
      VM nodeVM = this.getHostVMForMember(memberNoExec);
      assertNotNull(nodeVM);
      getLogWriter().info("Checking non-execution on VM(pid) : " + nodeVM.getPid() );
      nodeVM.invoke(validateNoQueryExecution);
    }
    
    
    //Nodes executing the query
    for( DistributedMember memberExec : prunedNodes) {
      VM nodeVM = this.getHostVMForMember(memberExec);
      assertNotNull(nodeVM);
      getLogWriter().info("Checking execution on VM(pid) : " + nodeVM.getPid());
        nodeVM.invoke(validateQueryExecution);
    }
    
  }

  public static final void reset() {
    isQueryExecutedOnNode = false;
  }

  @Override
  protected void vmTearDown() throws Exception {
    super.vmTearDown();
    QueryEvaluationHelper.reset();
    reset();
  }
}

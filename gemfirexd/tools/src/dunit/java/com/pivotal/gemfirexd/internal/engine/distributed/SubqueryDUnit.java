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

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.ClientServerDUnit;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import io.snappydata.test.dunit.AsyncInvocation;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;
import org.apache.derby.drda.NetworkServerControl;

/**
 * 
 * @author Asif
 *
 */
public class SubqueryDUnit extends DistributedSQLTestBase
{
  protected static volatile boolean isQueryExecutedOnNode = false;
  @SuppressWarnings("unused")
  private static final int byrange = 0x01, bylist = 0x02, bycolumn = 0x04, ANDing = 0x08, ORing = 0x10, Contains = 0x20, OrderBy = 0x40;
  @SuppressWarnings("unused")
  private static final int noCheck = 0x01, fixLater = noCheck|0x02, fixImmediate = noCheck|0x04, wontFix = noCheck|0x08, fixDepend = noCheck|0x10, 
                           fixInvestigate = noCheck|0x20;
  private static volatile Connection derbyConn = null;

  public SubqueryDUnit(String name) {
    super(name);
  }

  public void testPR_PR_IN_NonCorrelatedSubqueryPKBased() throws Exception {
    try {

      final String subqueryStr = "Select SUM(ID2) from Testtable2  where description2 = ? and  address2 = ?";
      String query = "select ID1, DESCRIPTION1 from TESTTABLE1 where "
          + "description1 like ? and address1  like ?  and  ID1 IN ( " + subqueryStr
          + " ) ";
      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))";

      String table2 = "create table TESTTABLE2 (ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, "
          + "primary key (ID2)) ";

       Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2);
      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);

      for (int i = 35; i < 100; ++i) {
        String insert = "Insert into  TESTTABLE1 values(" + i + ",'desc1_" + i
            + "', 'add1_" + i + "')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);

      }
      for (int i = 1; i < 11; ++i) {
        String insert = "Insert into  TESTTABLE2 values(" + i + ",'desc2_"
            + "', 'add2_')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      Object[] params = new Object[] { "desc1%", "add1%", "desc2_", "add2_" };
      ResultSet rsGfxd = executeQuery(query, true, params, false);
      ResultSet rsDerby = executeQuery(query, true, params, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      cleanup();
    }

  }
  
  public void testDeletePR_PR_IN_NonCorrelatedSubqueryPKBased() throws Exception
  {
    try {

      final String subqueryStr = "Select SUM(ID2) from Testtable2  where description2 = ? and  address2 = ?";
      String query = "Delete from TESTTABLE1 where "
          + "description1 like ? and address1 like ?  and  ID1 IN ( " + subqueryStr
          + " ) ";
      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))";

      String table2 = "create table TESTTABLE2 (ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, "
          + "primary key (ID2)) ";

       Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2);
      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);

      for (int i = 35; i < 100; ++i) {
        String insert = "Insert into  TESTTABLE1 values(" + i + ",'desc1_" + i
            + "', 'add1_" + i + "')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);

      }
      for (int i = 1; i < 11; ++i) {
        String insert = "Insert into  TESTTABLE2 values(" + i + ",'desc2_"
            + "', 'add2_')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      Object[] params = new Object[] { "desc1%", "add1%", "desc2_", "add2_" };
      int numUpdGfxd = executeUpdate(query, true, params, false);
      int numUpdDerby = executeUpdate(query, true, params, true);
      assertTrue(numUpdDerby >0);
      assertEquals(numUpdDerby ,numUpdGfxd);
      String select = "select * from testtable1";
      ResultSet rsGfxd = executeQuery(select, false, null, false);
      ResultSet rsDerby = executeQuery(select, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      cleanup();
    }

  }
  
  public void testUpdatePR_PR_IN_NonCorrelatedSubqueryPKBased() throws Exception
  {
    try {

      final String subqueryStr = "Select SUM(ID2) from Testtable2  where description2 = ? and  address2 = ?";
      String query = "Update  TESTTABLE1 set description1 = 'no description' where "
          + "description1 like ? and address1 like ?  and  ID1 IN ( " + subqueryStr
          + " ) ";
      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null, primary key (ID1))";

      String table2 = "create table TESTTABLE2 (ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, "
          + "primary key (ID2)) ";

       Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2);
      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);

      for (int i = 35; i < 100; ++i) {
        String insert = "Insert into  TESTTABLE1 values(" + i + ",'desc1_" + i
            + "', 'add1_" + i + "')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);

      }
      for (int i = 1; i < 11; ++i) {
        String insert = "Insert into  TESTTABLE2 values(" + i + ",'desc2_"
            + "', 'add2_')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      Object[] params = new Object[] { "desc1%", "add1%", "desc2_", "add2_" };
      int numUpdGfxd = executeUpdate(query, true, params, false);
      int numUpdDerby = executeUpdate(query, true, params, true);
      assertTrue(numUpdDerby >0);
      assertEquals(numUpdDerby ,numUpdGfxd);
      String select = "select * from testtable1";
      ResultSet rsGfxd = executeQuery(select, false, null, false);
      ResultSet rsDerby = executeQuery(select, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      cleanup();
    }

  }
  
  public void testBug42428() throws Exception
  {
    try {      
      String query = "select  DESCRIPTION1 from TESTTABLE1 where "
          + "ID1 IN (select ID2 from TESTTABLE2) ";
      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

      String table2 = "create table TESTTABLE2 (ID int not null,ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID) ) ";

       Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1 + " replicate");
      clientSQLExecute(1, table2);
      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
       
      
     String insert = "Insert into  TESTTABLE1 values(1,'desc1_1','add1_1')";
     clientSQLExecute(1, insert);
     executeOnDerby(insert, false, null);

      
      for (int i = 1; i < 3; ++i) {
        insert = "Insert into  TESTTABLE2 values(" + i + ",1,'desc2_"
            + "', 'add2_')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      cleanup();
    }

  }
  
  public void testBug42428ForDelete() throws Exception
  {
    try {      
      String dml = "delete  from TESTTABLE1 where "
          + "ID1 IN (select ID2 from TESTTABLE2) ";
      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

      String table2 = "create table TESTTABLE2 (ID int not null,ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID) ) ";

       Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1 + " replicate");
      clientSQLExecute(1, table2);
      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
       
      
     String insert = "Insert into  TESTTABLE1 values(1,'desc1_1','add1_1')";
     clientSQLExecute(1, insert);
     executeOnDerby(insert, false, null);

      
      for (int i = 1; i < 3; ++i) {
        insert = "Insert into  TESTTABLE2 values(" + i + ",1,'desc2_"
            + "', 'add2_')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
     
      int numGfxdUpdates = executeUpdate(dml, false, null, false);
      int numDerbyUpdates= executeUpdate(dml, false, null, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from testtable1";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      cleanup();
    }

  }
  public void testBug42428ForUpdate() throws Exception
  {
    try {      
      String dml = "Update TESTTABLE1 set Description1 = 'no description' where "
          + "ID1 IN (select ID2 from TESTTABLE2) ";
      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

      String table2 = "create table TESTTABLE2 (ID int not null,ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID) ) ";

       Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1 + " replicate");
      clientSQLExecute(1, table2);
      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
       
      
     String insert = "Insert into  TESTTABLE1 values(1,'desc1_1','add1_1')";
     clientSQLExecute(1, insert);
     executeOnDerby(insert, false, null);

      
      for (int i = 1; i < 3; ++i) {
        insert = "Insert into  TESTTABLE2 values(" + i + ",1,'desc2_"
            + "', 'add2_')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
     
      int numGfxdUpdates = executeUpdate(dml, false, null, false);
      int numDerbyUpdates= executeUpdate(dml, false, null, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from testtable1";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      cleanup();
    }

  }
  public void BUG_testSubqueryWithOrderBy() throws Exception {
    startVMs(2, 3);
  
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
  
    conn.createStatement().execute(
        " create table course ( " + "course_id int, course_name varchar(20),"
            + "primary key(course_id, course_name)" + ") partition by column (course_id) ");
  
    conn.createStatement().execute(
        " create table student ( " + "st_id varchar(10), c_id int,"
            + "primary key(st_id, c_id)" + ") replicate ");

    st.execute("insert into student values('x', 1)");
    st.execute("insert into student values('x', 2)");
    st.execute("insert into student values('x', 3)");
    st.execute("insert into student values('a', 1)");
    st.execute("insert into student values('bb', 4)");
  
    st.execute("insert into course values(4, 'FOUR')");
    st.execute("insert into course values(5, 'FIVE')");
    st.execute("insert into course values(6, 'SIX')");

    @SuppressWarnings("serial")
    SerializableRunnable selects = new SerializableRunnable() {

      long[] res = new long[] { 3, 2, 1 };

      @Override
      public void run() {
        Connection conn;
        try {
          conn = TestUtil.getConnection();
          Statement st = conn.createStatement();
          ResultSet st_st = st
              .executeQuery("select c_id from student where st_id = 'x' and c_id not in (select distinct course_id from course) order by c_id desc NULLS first ");

          int r = -1;
          while (st_st.next()) {
            int received = st_st.getInt(1);
            assert r < res.length;
            getLogWriter().info("Received course id from student " + received);
            assertEquals(res[++r], received);
          }
          
          assertTrue(r != -1);
          
        } catch (SQLException e) {
          fail("Exception occured ", e);
        }
      }
    };
    
    ArrayList<AsyncInvocation> runlist1 = executeTaskAsync(new int[] {2} , new int[] {1,2,3} , selects);
    
    selects.run();

    joinAsyncInvocation(runlist1);
    
  }
  
  private void cleanup() throws Exception
  {
    if (derbyConn != null) {
      Statement derbyStmt = derbyConn.createStatement();
      if (derbyStmt != null) {
        final String[] tables = new String[] { "testtable", "testtable1",
            "testtable2", "testtable3" };
        for (String table : tables) {
          try {
            derbyStmt.execute("drop table " + table);
          }
          catch (SQLException ex) {
            // deliberately ignored
          }

          try {
            clientSQLExecute(1, "drop table " + table);
          }
          catch (Exception ex) {
            // deliberately ignored
          }
        }
      }

     /* try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      }
      catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }*/

    }

  }

  
  public static void executeOnDerby(String sql, boolean isPrepStmt,
      Object[] params) throws Exception
  {
    if (derbyConn == null) {
      String derbyDbUrl = "jdbc:derby:newDB;create=true;";
      if (TestUtil.currentUserName != null) {
        derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
            + TestUtil.currentUserPassword + ';');
      }
      
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      
    }
    if (isPrepStmt) {
      PreparedStatement ps = derbyConn.prepareStatement(sql);
      if (params != null) {
        int j = 1;
        for (Object param : params) {
          if (param == null) {
            ps.setNull(j, Types.JAVA_OBJECT);
          }
          else {
            ps.setObject(j, param);
          }
        }
      }
     ps.execute();
    }
    else {
      derbyConn.createStatement().execute(sql);
    }

  }

  public static ResultSet executeQuery(String sql, boolean isPrepStmt,
      Object[] params, boolean isDerby) throws Exception
  {
    Connection derbyOrGfxd = null;
    ResultSet rs = null;
    if (isDerby) {
      if (derbyConn == null) {
        String derbyDbUrl = "jdbc:derby:newDB;create=true;";
        if (TestUtil.currentUserName != null) {
          derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
              + TestUtil.currentUserPassword + ';');
        }
       
          derbyConn = DriverManager.getConnection(derbyDbUrl);  
        
      }
      derbyOrGfxd = derbyConn;
    }
    else {
      derbyOrGfxd = TestUtil.getConnection();
    }

    if (isPrepStmt) {
      PreparedStatement ps = derbyOrGfxd.prepareStatement(sql);
      if (params != null) {
        int j = 1;
        for (Object param : params) {
          if (param == null) {
            ps.setNull(j, Types.JAVA_OBJECT);
          }
          else {
            ps.setObject(j, param);
          }
          ++j;
        }
      }
      rs = ps.executeQuery();
    }
    else {
      rs = derbyOrGfxd.createStatement().executeQuery(sql);
    }
    return rs;

  }
  
  public static int executeUpdate(String sql, boolean isPrepStmt,
      Object[] params, boolean isDerby) throws Exception
  {
    Connection derbyOrGfxd = null;
    int numUpdate= 0;
    if (isDerby) {
      if (derbyConn == null) {
        String derbyDbUrl = "jdbc:derby:newDB;create=true;";
        if (TestUtil.currentUserName != null) {
          derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
              + TestUtil.currentUserPassword + ';');
        }
       
          derbyConn = DriverManager.getConnection(derbyDbUrl);  
        
      }
      derbyOrGfxd = derbyConn;
    }
    else {
      derbyOrGfxd = TestUtil.getConnection();
    }

    if (isPrepStmt) {
      PreparedStatement ps = derbyOrGfxd.prepareStatement(sql);
      if (params != null) {
        int j = 1;
        for (Object param : params) {
          if (param == null) {
            ps.setNull(j, Types.JAVA_OBJECT);
          }
          else {
            ps.setObject(j, param);
          }
          ++j;
        }
      }
      numUpdate = ps.executeUpdate();
    }
    else {
      numUpdate = derbyOrGfxd.createStatement().executeUpdate(sql);
    }
    return numUpdate;

  }
   
  @Override
  public void tearDown2() throws Exception
  {
    try {     
      DriverManager.getConnection("jdbc:derby:;shutdown=true");
      derbyConn = null;
    }
    catch (SQLException sqle) {
      derbyConn = null;
      if (sqle.getMessage().indexOf("shutdown") == -1 && 
          sqle.getMessage().indexOf("Driver is not registered ") == -1) {
        sqle.printStackTrace();
        throw sqle;
      }
      derbyConn = null;
    }
    super.tearDown2();
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

  private void verifyExecutionOnDMs(final SelectQueryInfo sqi,
      Set<DistributedMember> prunedNodes, int noOfPrunedNodes,
      int noOfNoExecQueryNodes, String query) {

    SerializableRunnable validateQueryExecution = new SerializableRunnable(
        "validate node has executed the query " + query) {
      @Override
      public void run() throws CacheException {
        try {
          GemFireXDQueryObserverHolder
              .setInstance(new GemFireXDQueryObserverAdapter());
          assertTrue(isQueryExecutedOnNode);
          isQueryExecutedOnNode = false;

        } catch (Exception e) {
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

        } catch (Exception e) {
          throw new CacheException(e) {
          };
        }
      }
    };
    PartitionedRegion pr = (PartitionedRegion)sqi.getRegion();
    Set<InternalDistributedMember> nodesOfPr = pr.getRegionAdvisor()
        .adviseDataStore();
    getLogWriter().info(" Total members = " + nodesOfPr.size());

    String logPrunedMembers = new String();
    logPrunedMembers = " Prunned member(s) " + prunedNodes.size() + "\n";
    for (DistributedMember dm : prunedNodes) {
      if (dm != null) {
        logPrunedMembers += dm.toString() + " ";
      }
    }
    getLogWriter().info(logPrunedMembers);
    if (noOfPrunedNodes > -1)
      assertEquals(noOfPrunedNodes, prunedNodes.size());

    nodesOfPr.removeAll(prunedNodes);
    String logNonPrunedMembers = new String();
    logNonPrunedMembers = " Non-prunned member(s) " + nodesOfPr.size() + "\n";
    for (DistributedMember dm : nodesOfPr) {
      if (dm != null) {
        logNonPrunedMembers += dm.toString() + " ";
      }
    }
    getLogWriter().info(logNonPrunedMembers);
    if (noOfNoExecQueryNodes > -1)
      assertEquals(noOfNoExecQueryNodes, nodesOfPr.size());

    // Nodes not executing the query
    for (DistributedMember memberNoExec : nodesOfPr) {
      VM nodeVM = this.getHostVMForMember(memberNoExec);
      assertNotNull(nodeVM);
      getLogWriter().info(
          "Checking non-execution on VM(pid) : " + nodeVM.getPid());
      nodeVM.invoke(validateNoQueryExecution);
    }

    // Nodes executing the query
    for (DistributedMember memberExec : prunedNodes) {
      VM nodeVM = this.getHostVMForMember(memberExec);
      assertNotNull(nodeVM);
      getLogWriter().info("Checking execution on VM(pid) : " + nodeVM.getPid());
      nodeVM.invoke(validateQueryExecution);
    }

  }
  
  public void testBug42416() throws Exception {

    startVMs(1, 2);
  
    Connection conn = TestUtil.getConnection();    
    
    Statement s = conn.createStatement();
   
   

    String table2 = "create table portfolio (cid int not null, sid int not null, " +
    		" constraint portf_pk primary key (cid, sid), "
        + "constraint cust_fk foreign key (cid) references customers (cid) on" +
        " delete restrict)"
        + " partition by range " +
          "(cid) ( VALUES BETWEEN 0 AND 5, VALUES BETWEEN 5 AND 10, " +
          "VALUES BETWEEN 10 AND 15) colocate with (customers)";

   
    String table3 = "create table customers (cid int not null, cust_name varchar(100), "
        + " tid int, primary key (cid)) partition by range " +
          "(cid) ( VALUES BETWEEN 0 AND 5, VALUES BETWEEN 5 AND 10, " +
          "VALUES BETWEEN 10 AND 15)";

    s.execute(table3);
    s.execute(table2);
    for(int i = 0 ; i <15;++i) {
      clientSQLExecute(1,
      "insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    clientSQLExecute(1, "insert into portfolio values (2,7)");
    clientSQLExecute(1, "insert into portfolio values (3,8)");
    clientSQLExecute(1, "insert into portfolio values (5,12)");
    clientSQLExecute(1, "insert into portfolio values (6,13)");
    clientSQLExecute(1, "insert into portfolio values (7,2)");
    clientSQLExecute(1, "insert into portfolio values (8,3)");
    clientSQLExecute(1, "insert into portfolio values (12,18)");
    clientSQLExecute(1, "insert into portfolio values (13,19)");
    GemFireXDQueryObserver old = null;
    String query = "select c.cid, c.tid from "
      + "customers c where c.tid IN (select sid from portfolio f)";
    
    
    /*String query = "select c.cid, c.tid from "
      + "customers c where c.tid IN (select sid from portfolio f where f.sid >= 0)";*/
    
    try {
      
      //Check prepared statement query
      sqlExecuteVerify(new int[] { 1 },null,
         query, TestUtil.getResourcesDir()
              + "/lib/checkSubquery.xml", "subquery_1",
          true, false);
      
    //Check Statement Query
      sqlExecuteVerify(new int[] { 1 }, null,
          query, TestUtil.getResourcesDir()
              + "/lib/checkSubquery.xml", "subquery_1",
          false, false);
   
    }
    finally {
      
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }  
  }
  
  public void testBug42416ForDelete() throws Exception {

    startVMs(1, 2);
  
    Connection conn = TestUtil.getConnection();    
    
    Statement s = conn.createStatement();
   
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    

    String table2 = "create table portfolio (cid int not null, sid int not null, " +
                " constraint portf_pk primary key (cid, sid), "
        + "constraint cust_fk foreign key (cid) references customers (cid) )"  ;

   
    String table3 = "create table customers (cid int not null, cust_name varchar(100), "
        + " tid int, primary key (cid))";

    executeOnDerby(table3, false, null);
    executeOnDerby(table2, false, null);


    
    s.execute(table3+ " partition by range " +
        "(cid) ( VALUES BETWEEN 0 AND 5, VALUES BETWEEN 5 AND 10, " +
        "VALUES BETWEEN 10 AND 15)");
    
    s.execute(table2 + " partition by range " +
        "(cid) ( VALUES BETWEEN 0 AND 5, VALUES BETWEEN 5 AND 10, " +
        "VALUES BETWEEN 10 AND 15) colocate with (customers)");
    
    for(int i = 0 ; i <15;++i) {
      String insert ="insert into customers values ("+i+","+"'name_"+i+"',"+i +")"; 
      clientSQLExecute(1,insert);
      executeOnDerby(insert, false, null);
    }
    clientSQLExecute(1, "insert into portfolio values (2,7)");
    executeOnDerby("insert into portfolio values (2,7)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (3,8)");
    executeOnDerby("insert into portfolio values (3,8)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (5,12)");
    executeOnDerby("insert into portfolio values (5,12)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (6,13)");
    executeOnDerby("insert into portfolio values (6,13)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (7,2)");
    executeOnDerby("insert into portfolio values (7,2)", false, null);
    
    
    clientSQLExecute(1, "insert into portfolio values (8,3)");
    executeOnDerby("insert into portfolio values (8,3)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (12,18)");
    executeOnDerby("insert into portfolio values (12,18)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (13,19)");
    executeOnDerby("insert into portfolio values (13,19)", false, null);
    
    GemFireXDQueryObserver old = null;
    String dml = "delete  from "
      + "portfolio p where p.sid IN (select tid from customers c)";
    
    
    try {
    
      int numGfxdUpdates = executeUpdate(dml, false, null, false);
      int numDerbyUpdates= executeUpdate(dml, false, null, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from portfolio";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table customers", false, null);
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  
  }
  
  public void testBug42416ForUpdate() throws Exception {

    startVMs(1, 2);
  
    Connection conn = TestUtil.getConnection();    
    
    Statement s = conn.createStatement();
   
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    

    String table2 = "create table portfolio (cid int not null, sid int not null, loan int not null, " +
                " constraint portf_pk primary key (cid, sid), "
        + "constraint cust_fk foreign key (cid) references customers (cid) )"  ;

   
    String table3 = "create table customers (cid int not null, cust_name varchar(100), "
        + " tid int, primary key (cid))";

    executeOnDerby(table3, false, null);
    executeOnDerby(table2, false, null);


    
    s.execute(table3+ " partition by range " +
        "(cid) ( VALUES BETWEEN 0 AND 5, VALUES BETWEEN 5 AND 10, " +
        "VALUES BETWEEN 10 AND 15)");
    
    s.execute(table2 + " partition by range " +
        "(cid) ( VALUES BETWEEN 0 AND 5, VALUES BETWEEN 5 AND 10, " +
        "VALUES BETWEEN 10 AND 15) colocate with (customers)");
    
    for(int i = 0 ; i <15;++i) {
      String insert ="insert into customers values ("+i+","+"'name_"+i+"',"+i +")"; 
      clientSQLExecute(1,insert);
      executeOnDerby(insert, false, null);
    }
    clientSQLExecute(1, "insert into portfolio values (2,7,100)");
    executeOnDerby("insert into portfolio values (2,7,100)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (3,8,150)");
    executeOnDerby("insert into portfolio values (3,8,150)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (5,12,200)");
    executeOnDerby("insert into portfolio values (5,12,200)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (6,13,250)");
    executeOnDerby("insert into portfolio values (6,13,250)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (7,2,300)");
    executeOnDerby("insert into portfolio values (7,2,300)", false, null);
    
    
    clientSQLExecute(1, "insert into portfolio values (8,3,350)");
    executeOnDerby("insert into portfolio values (8,3,350)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (12,18,400)");
    executeOnDerby("insert into portfolio values (12,18,400)", false, null);
    
    clientSQLExecute(1, "insert into portfolio values (13,19,450)");
    executeOnDerby("insert into portfolio values (13,19,450)", false, null);
    
    GemFireXDQueryObserver old = null;
    String dml = "Update  portfolio p set p.loan = p.loan +100 where p.sid IN (select tid from customers c)";
    
    
    try {
    
      int numGfxdUpdates = executeUpdate(dml, false, null, false);
      int numDerbyUpdates= executeUpdate(dml, false, null, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from portfolio";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table customers", false, null);
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  
  }
  public void testBug42426_1() throws Exception
  {
    NetworkServerControl server = null;
    try {

      final String query2 = "select sec_id, symbol, price from securities s where sec_id "
          + "IN (select sid from portfolio f where tid  > 0 "
          + " GROUP BY sid Having count(*) > 2)";
      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (5,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (9,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (5,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,4,16,56,8)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      ResultSet rsGfxd = executeQuery(query2, false, null, false);
      ResultSet rsDerby = executeQuery(query2, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException
        {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }
            derbyConn = DriverManager.getConnection(derbyDbUrl);

            ResultSet rsGfxd = executeQuery(query2, false, null, false);
            ResultSet rsDerby = executeQuery(query2, false, null, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };

      serverExecute(1, csr);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testBug42426_1ForDelete_1() throws Exception {
    NetworkServerControl server = null;
    try {

      
      final String dml = "delete from portfolio f where tid > 0 and sid "
        + "IN (select sec_id from securities  s where tid  > 0 "
        + " GROUP BY sec_id )";
      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (5,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (9,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (5,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,4,16,56,8)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }
            derbyConn = DriverManager.getConnection(derbyDbUrl);
            int numGfxdUpdates = executeUpdate(dml, false, null, false);
            int numDerbyUpdates = executeUpdate(dml, false, null, true);
            assertTrue(numDerbyUpdates > 0);
            assertEquals(numDerbyUpdates, numGfxdUpdates);
            String query = "select * from portfolio";
            ResultSet rsGfxd = executeQuery(query, false, null, false);
            ResultSet rsDerby = executeQuery(query, false, null, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
          } catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };

      serverExecute(1, csr);
    } finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testBug42426_1ForUpdate_1() throws Exception {
    NetworkServerControl server = null;
    try {

      
      final String dml = "Update portfolio f set f.qty = qty*100 where tid > 0 and sid "
        + "IN (select sec_id from securities  s where tid  > 0 "
        + " GROUP BY sec_id )";
      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (5,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (9,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (5,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,4,16,56,8)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }
            derbyConn = DriverManager.getConnection(derbyDbUrl);
            int numGfxdUpdates = executeUpdate(dml, false, null, false);
            int numDerbyUpdates = executeUpdate(dml, false, null, true);
            assertTrue(numDerbyUpdates > 0);
            assertEquals(numDerbyUpdates, numGfxdUpdates);
            String query = "select * from portfolio";
            ResultSet rsGfxd = executeQuery(query, false, null, false);
            ResultSet rsDerby = executeQuery(query, false, null, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
          } catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };

      serverExecute(1, csr);
    } finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testBug42426_1ForDelete_2() throws Exception
  {
    NetworkServerControl server = null;
    try {

      final String dml = "delete from portfolio f where tid > 0 and sid "
        + "IN (select sec_id from securities  s where tid  > 0 "
        + " GROUP BY sec_id )";
      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (5,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (9,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (5,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,4,16,56,8)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      int numGfxdUpdates = executeUpdate(dml, false, null, false);
      int numDerbyUpdates= executeUpdate(dml, false, null, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from portfolio";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testBug42426_1ForUpdate_2() throws Exception
  {
    NetworkServerControl server = null;
    try {

      final String dml = "Update portfolio f set f.qty = qty + 100 where f.tid > 0 and f.sid "
        + "IN (select sec_id from securities  s where tid  > 0 "
        + " GROUP BY sec_id )";
      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (5,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (9,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (5,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,4,16,56,8)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      int numGfxdUpdates = executeUpdate(dml, false, null, false);
      int numDerbyUpdates= executeUpdate(dml, false, null, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from portfolio";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testBug42426_2() throws Exception
  {
    NetworkServerControl server = null;
    try {

      final String query1 = " select sid from portfolio f where tid > 0 GROUP"
          + " BY sid Having count(*) > 2 ";

      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (5,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (6,3,13,52,5)");
      clientSQLExecute(1, "insert into portfolio values (7,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (8,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (9,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (5,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (6,3,13,52,5)", false, null);
      executeOnDerby("insert into portfolio values (7,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (8,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,4,16,56,8)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      ResultSet rsGfxd = executeQuery(query1, false, null, false);
      ResultSet rsDerby = executeQuery(query1, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException
        {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }         
             derbyConn = DriverManager.getConnection(derbyDbUrl);   
            ResultSet rsGfxd = executeQuery(query1, false, null, false);
            ResultSet rsDerby = executeQuery(query1, false, null, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
            TestUtil.getConnection().close();
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };
      serverExecute(1, csr);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }

  }
  
  public void testBug42659() throws Exception {

    NetworkServerControl server = null;
    try {

      final String query1 = " select * from securities trade where sec_id IN " +
      		"(select sid from portfolio where cid >?) ";

      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (5,3,13,52,5)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (9,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (5,3,13,52,5)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,2,11,51,3)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      ResultSet rsGfxd = executeQuery(query1, true, new Object[] {new Integer(5)}, false);
      ResultSet rsDerby = executeQuery(query1, true, new Object[] {new Integer(5)}, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }         
             derbyConn = DriverManager.getConnection(derbyDbUrl);   
            ResultSet rsGfxd = executeQuery(query1, true, new Object[] {new Integer(100)}, false);
            ResultSet rsDerby = executeQuery(query1, true, new Object[] {new Integer(100)}, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
            TestUtil.getConnection().close();
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };
      serverExecute(1, csr);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }  
  }
  
  
  public void testBug42697And42673() throws Exception {

    NetworkServerControl server = null;
    try {

      final String query1 = "select * from securities where sec_id IN" +
      	" (select sid from portfolio f where cid = " +
      	"     (select c.cid from customers c where tid > ? and c.cid = f.cid)" +
      	" ) ";

      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
          + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (5,3,13,52,5)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (9,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (5,3,13,52,5)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,2,11,51,3)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      ResultSet rsGfxd = executeQuery(query1, true, new Object[] {new Integer(0)}, false);
      ResultSet rsDerby = executeQuery(query1, true, new Object[] {new Integer(0)}, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }         
             derbyConn = DriverManager.getConnection(derbyDbUrl);   
            ResultSet rsGfxd = executeQuery(query1, true, new Object[] {new Integer(100)}, false);
            ResultSet rsDerby = executeQuery(query1, true, new Object[] {new Integer(100)}, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
            TestUtil.getConnection().close();
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };
      serverExecute(1, csr);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }  
  }
  
  public void testBug42697And42673ForDelete_1() throws Exception {

    NetworkServerControl server = null;
    try {

      final String dml = "delete  from securities where sec_id IN" +
        " (select sid from portfolio f where cid = " +
        "     (select c.cid from customers c where tid > ? and c.cid = f.cid)" +
        " ) ";

      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (5,3,13,52,5)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (9,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (5,3,13,52,5)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,2,11,51,3)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      int numGfxdUpdates = executeUpdate(dml, true, new Object[] {new Integer(0)}, false);
      int numDerbyUpdates= executeUpdate(dml, true, new Object[] {new Integer(0)}, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from securities";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }  
  }
  
  public void testBug42697And42673ForUpdate_1() throws Exception {

    NetworkServerControl server = null;
    try {

      final String dml = "Update  securities set symbol = 'no symbol' where sec_id IN" +
        " (select sid from portfolio f where cid = " +
        "     (select c.cid from customers c where tid > ? and c.cid = f.cid)" +
        " ) ";

      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (5,3,13,52,5)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (9,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (5,3,13,52,5)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,2,11,51,3)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      int numGfxdUpdates = executeUpdate(dml, true, new Object[] {new Integer(0)}, false);
      int numDerbyUpdates= executeUpdate(dml, true, new Object[] {new Integer(0)}, true);
      assertTrue(numDerbyUpdates >0);
      assertEquals(numDerbyUpdates,numGfxdUpdates);
      String query = "select * from securities";
      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }  
  }
  
  public void testBug42697And42673ForDelete_2() throws Exception {

    NetworkServerControl server = null;
    try {

      final String dml = "delete  from securities where sec_id IN" +
        " (select sid from portfolio f where cid = " +
        "     (select c.cid from customers c where tid > ? and c.cid = f.cid)" +
        " ) ";

      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (5,3,13,52,5)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (9,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (5,3,13,52,5)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,2,11,51,3)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }         
             derbyConn = DriverManager.getConnection(derbyDbUrl);  
             int numGfxdUpdates = executeUpdate(dml, true, new Object[] {new Integer(1)}, false);
             int numDerbyUpdates= executeUpdate(dml, true, new Object[] {new Integer(1)}, true);
             assertTrue(numDerbyUpdates >0);
             assertEquals(numDerbyUpdates,numGfxdUpdates);
             String query = "select * from securities";
            ResultSet rsGfxd = executeQuery(query, false, null, false);
            ResultSet rsDerby = executeQuery(query, false, null, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
            TestUtil.getConnection().close();
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };
      serverExecute(1, csr);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }  
  }
  
  public void testBug42697And42673ForUpdate_2() throws Exception {

    NetworkServerControl server = null;
    try {

      final String dml = "Update  securities set symbol = 'no symbol' where sec_id IN" +
        " (select sid from portfolio f where cid = " +
        "     (select c.cid from customers c where tid > ? and c.cid = f.cid)" +
        " ) ";

      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
          + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict )   ";

      String table4 = "create table sellorders (oid int not null constraint orders_pk primary key,"
          + " cid int, sid int, qty int, tid int, "
          + " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)"
          + " on delete restrict )";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);

      for (int i = 0; i < 15; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + i + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,10,50,2)");
      clientSQLExecute(1, "insert into portfolio values (3,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (4,3,12,52,4)");
      clientSQLExecute(1, "insert into portfolio values (5,3,13,52,5)");
      clientSQLExecute(1, "insert into portfolio values (6,3,14,70,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,15,55,7)");
      clientSQLExecute(1, "insert into portfolio values (8,4,16,56,8)");
      clientSQLExecute(1, "insert into portfolio values (9,2,11,51,3)");
      clientSQLExecute(1, "insert into portfolio values (10,4,16,56,8)");

      executeOnDerby("insert into portfolio values (2,2,10,50,2)", false, null);
      executeOnDerby("insert into portfolio values (3,2,11,51,3)", false, null);
      executeOnDerby("insert into portfolio values (4,3,12,52,4)", false, null);
      executeOnDerby("insert into portfolio values (5,3,13,52,5)", false, null);
      executeOnDerby("insert into portfolio values (6,3,14,70,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,15,55,7)", false, null);
      executeOnDerby("insert into portfolio values (8,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (10,4,16,56,8)", false, null);
      executeOnDerby("insert into portfolio values (9,2,11,51,3)", false, null);

      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }         
             derbyConn = DriverManager.getConnection(derbyDbUrl);  
             int numGfxdUpdates = executeUpdate(dml, true, new Object[] {new Integer(1)}, false);
             int numDerbyUpdates= executeUpdate(dml, true, new Object[] {new Integer(1)}, true);
             assertTrue(numDerbyUpdates >0);
             assertEquals(numDerbyUpdates,numGfxdUpdates);
             String query = "select * from securities";
            ResultSet rsGfxd = executeQuery(query, false, null, false);
            ResultSet rsDerby = executeQuery(query, false, null, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
            TestUtil.getConnection().close();
          }
          catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };
      serverExecute(1, csr);
    }
    finally {
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }  
  }
  public static void reset() {
    isQueryExecutedOnNode = false;
  }
  public void testBug43747() throws Exception {
    NetworkServerControl server = null;
    try {      
      final String dml = "delete from networth n where tid = 6 and n.cid IN " +
      "(select cid from portfolio where tid =6 and sid >=2 and sid <=4 )";
      startVMs(1, 3);
      String table1 = "create table customers (cid int not null, cust_name varchar(100),"
          + "  tid int, primary key (cid))";

      String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
          + "price int,  tid int, constraint sec_pk primary key (sec_id)  )   ";

      String table3 = "create table portfolio (cid int not null, sid int not null,tid int," +
      " constraint portf_pk primary key (cid, sid), constraint cust_fk foreign key (cid) references" +
      " customers (cid) on delete restrict, constraint sec_fk foreign key (sid) references" +
      " securities (sec_id) on delete restrict)" ;

      String table4 = "create table networth (cid int not null,   tid int," +
      	" constraint netw_pk primary key (cid), constraint cust_newt_fk foreign key (cid) references" +
      	" customers (cid) on delete restrict) ";
      String table5="create table sellorders (oid int not null constraint orders_pk primary key," +
      	" cid int, sid int, tid int, constraint portf_fk foreign key (cid, sid) references" +
      	" portfolio (cid, sid) on delete restrict)" ;
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1);
      clientSQLExecute(1, table2 + " replicate");
      clientSQLExecute(1, table3
          + " partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table4
          + "partition by column (cid) colocate with (customers)");
      clientSQLExecute(1, table5
          + "partition by column (cid) colocate with (customers)");

      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      executeOnDerby(table3, false, null);
      executeOnDerby(table4, false, null);
      executeOnDerby(table5, false, null);
      for (int i = 0; i < 45; ++i) {
        String insert = "insert into customers values (" + i + "," + "'name_"
            + i + "'," + 6 + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      
      for (int i = 0; i < 45; ++i) {
        String insert = "insert into networth values (" + i + "," + 6 + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      for (int i = 0; i < 40; ++i) {
        String insert = "insert into securities values (" + i + "," + "'sec_"
            + i + "'," + i * 10 + "," + 6 + ")";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      clientSQLExecute(1, "insert into portfolio values (2,2,6)");
      clientSQLExecute(1, "insert into portfolio values (3,2,6)");
      clientSQLExecute(1, "insert into portfolio values (4,2,6)");
      clientSQLExecute(1, "insert into portfolio values (5,3,6)");
      clientSQLExecute(1, "insert into portfolio values (6,3,6)");
      clientSQLExecute(1, "insert into portfolio values (7,4,6)");
      clientSQLExecute(1, "insert into portfolio values (8,4,6)");
      clientSQLExecute(1, "insert into portfolio values (9,4,6)");
      clientSQLExecute(1, "insert into portfolio values (10,4,6)");

      executeOnDerby("insert into portfolio values (2,2,6)", false, null);
      executeOnDerby("insert into portfolio values (3,2,6)", false, null);
      executeOnDerby("insert into portfolio values (4,2,6)", false, null);
      executeOnDerby("insert into portfolio values (5,3,6)", false, null);
      executeOnDerby("insert into portfolio values (6,3,6)", false, null);
      executeOnDerby("insert into portfolio values (7,4,6)", false, null);
      executeOnDerby("insert into portfolio values (8,4,6)", false, null);
      executeOnDerby("insert into portfolio values (10,4,6)", false, null);
      executeOnDerby("insert into portfolio values (9,4,6)", false, null);
      startServerVMs(1, 0, null);
      // start network server in controller VM
      final int derbyPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET);
      server = new NetworkServerControl(InetAddress.getByName("localhost"),
          derbyPort);
      server.start(null);
      // wait for server to initialize completely
      ClientServerDUnit.waitForDerbyInitialization(server);

      SerializableRunnable csr = new SerializableRunnable(
          "query executor") {
        final String uName = TestUtil.currentUserName;

        final String psswd = TestUtil.currentUserPassword;

        @Override
        public void run() throws CacheException {
          try {
            TestUtil.currentUserName = uName;
            TestUtil.currentUserPassword = psswd;
            String derbyDbUrl = "jdbc:derby://localhost:" + derbyPort
                + "/newDB;";
            if (TestUtil.currentUserName != null) {
              derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
                  + TestUtil.currentUserPassword + ';');
            }
            derbyConn = DriverManager.getConnection(derbyDbUrl);
            int numGfxdUpdates = executeUpdate(dml, false, null, false);
            int numDerbyUpdates = executeUpdate(dml, false, null, true);
            
            String query = "select * from networth";
            ResultSet rsGfxd = executeQuery(query, false, null, false);
            ResultSet rsDerby = executeQuery(query, false, null, true);
            TestUtil.validateResults(rsDerby, rsGfxd, false);
            assertTrue(numDerbyUpdates > 0);
            assertEquals(numDerbyUpdates, numGfxdUpdates);
            TestUtil.getConnection().close();

          } catch (Exception e) {
            throw new CacheException(e) {
            };
          }

        }
      };
      csr.run();
      //serverExecute(1, csr);
    } finally {
      clientSQLExecute(1, "drop table networth");
      clientSQLExecute(1, "drop table sellorders");
      clientSQLExecute(1, "drop table portfolio");
      clientSQLExecute(1, "drop table securities");
      clientSQLExecute(1, "drop table customers");
      executeOnDerby("drop table networth", false, null);
      executeOnDerby("drop table sellorders", false, null);
      executeOnDerby("drop table portfolio", false, null);
      executeOnDerby("drop table securities", false, null);
      executeOnDerby("drop table customers", false, null);
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  public void testBug42856() throws Exception {
    try {
      String query = "select ID1, DESCRIPTION1 from TESTTABLE1 where DESCRIPTION1 = "
          + "( select DESCRIPTION1 from TESTTABLE1 where DESCRIPTION1 > 'desc1_1' "
          + "intersect "
          + "select DESCRIPTION1 from TESTTABLE1 where DESCRIPTION1 < 'desc1_3')";

      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1 + "  partition by column(ID1)");
      executeOnDerby(table1, false, null);

      for (int i = 1; i < 4; ++i) {
        String insert = "Insert into  TESTTABLE1 values(" + i + ",'desc1_" + i
            + "','add1_" + i + "')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }

      // Derby should pass
      // This is derby jar which have not been corrupted by SqlFire
      ResultSet rsDerby = executeQuery(query, false, null, true);
      assertTrue(rsDerby.next());
      assertFalse(rsDerby.next());
      rsDerby.close();
      try {
        // SqlF in distribution should fail
        ResultSet rsGfxd = executeQuery(query, false, null, false);
        fail("Test should fail with feature not supported exception");
      } catch (SQLException sqle) {
        assertEquals("0A000", sqle.getSQLState());
      }
    } finally {
      cleanup();
    }

  }
  
  
  public void testStatementMatchingForSubquery() throws Exception
  {
    try {      
      
      startVMs(1, 2);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null , primary key (ID1))" ;

      String table2 = "create table TESTTABLE2 (ID int not null,ID2 int not null, "
          + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null, primary key (ID) )"; 

       Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      clientSQLExecute(1, table1 + " partition by primary key  ");
      clientSQLExecute(1, table2 + " partition by primary key  ");
      executeOnDerby(table1, false, null);
      executeOnDerby(table2, false, null);
      
      for (int i = 1; i < 20; ++i) {
        String insert = "Insert into  TESTTABLE1 values("+i +",'desc1_"+i +"','add1_"+i+"')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
            
      for (int i = 1; i < 20; ++i) {
       String insert = "Insert into  TESTTABLE2 values(" + i + ",1,'desc2_"
            + i +"', 'add2_"+i+"')";
        clientSQLExecute(1, insert);
        executeOnDerby(insert, false, null);
      }
      for(VM vm :serverVMs) {
        vm.invoke(SubqueryDUnit.class, "reset");
      }
      //Set up 
      SerializableRunnable setObserver = new SerializableRunnable(
          "Set GemFireXDObserver on DataStore Node for test= testStatementMatchingForSubquery") {
        private List<SelectQueryInfo> sqiList = new ArrayList<SelectQueryInfo>();
        private int numTimesInvoked = 0;
        @Override
        public void run() throws CacheException {
          try {
            GemFireXDQueryObserverHolder
                .setInstance(new GemFireXDQueryObserverAdapter() {                 
                  @Override
                  public void afterGemFireActivationCreate(AbstractGemFireActivation ac) {
                     try {
                      SelectQueryInfo sqi = (SelectQueryInfo)TestUtil.<AbstractGemFireActivation>getField(AbstractGemFireActivation.class, ac, "qInfo");
                      sqiList.add(sqi);
                    } catch (Exception e) {
                      throw new GemFireXDRuntimeException(e);
                    } 
                    ++ numTimesInvoked;
                    getLogWriter().info("Number of time GemFireActivation got created = "+numTimesInvoked);
                    getLogWriter().info("query string for activation = "+ac.getPreparedStatement().getSource());
                    isQueryExecutedOnNode = (sqiList.size() == 4);
                    if(isQueryExecutedOnNode) {
                      // All three executions should refer to same query info object for subquery, if the top query is undergoing 
                      //statement matching
                      for(int i =0 ; i < 3; ++i) {
                        assertEquals(sqiList.get(i), sqiList.get(i+1));
                      }
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
      
      for(VM vm :serverVMs) {
        vm.invoke(setObserver);
      }

      // first execution
      String query = "select  DESCRIPTION1 from TESTTABLE1 where "
          + "ID1 IN (select ID2 from TESTTABLE2 where ID2 > 5) and ID1 > 8";

      ResultSet rsGfxd = executeQuery(query, false, null, false);
      ResultSet rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);

      // second execution
      query = "select  DESCRIPTION1 from TESTTABLE1 where "
          + "ID1 IN (select ID2 from TESTTABLE2 where ID2 > 9) and ID1 > 13";

      rsGfxd = executeQuery(query, false, null, false);
      rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);

      // third execution
      query = "select  DESCRIPTION1 from TESTTABLE1 where "
          + "ID1 IN (select ID2 from TESTTABLE2 where ID2 > 2) and ID1 > 1";

      rsGfxd = executeQuery(query, false, null, false);
      rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);

      // fourth execution
      query = "select  DESCRIPTION1 from TESTTABLE1 where "
          + "ID1 IN (select ID2 from TESTTABLE2 where ID2 > 0) and ID1 > 0";

      rsGfxd = executeQuery(query, false, null, false);
      rsDerby = executeQuery(query, false, null, true);
      TestUtil.validateResults(rsDerby, rsGfxd, false);

      SerializableRunnable assertQueryExec = new SerializableRunnable(
          "Assert that queryExecutedOnNode is true") {        
        @Override
        public void run() throws CacheException {
           assertTrue(isQueryExecutedOnNode);  
        }
      };
      for(VM vm :serverVMs) {
        vm.invoke(assertQueryExec);
      }
      
      
    }
    finally {
      cleanup();
    }

  }
}

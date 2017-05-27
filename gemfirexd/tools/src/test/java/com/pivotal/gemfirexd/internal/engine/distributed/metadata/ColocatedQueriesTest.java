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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.index.OpenMemIndex;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.DerbySQLException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitor;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.compile.*;
import com.pivotal.gemfirexd.internal.impl.sql.execute.JoinResultSet;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.junit.Assert;

/**
 * Tests the SelectQueryInfo structure & the related colocation matrix to
 * idenitify if the query could be executed through scatter /gather or not. The
 * test check the correctness of colocation matrix in various types of where
 * clause where partitioning key is based on one or more columns
 * 
 * @author Asif
 * 
 */
@SuppressWarnings("serial")
public class ColocatedQueriesTest extends JdbcTestBase {

  private boolean callbackInvoked = false;

  //private final int index = 0;

  public ColocatedQueriesTest(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(ColocatedQueriesTest.class));
  }

  public void testNonColocatedTableQueriesNoThrowExceptionAsSingleNode() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by primary key");
    s
        .execute("create table TESTTABLE2 (ID2 int primary key, "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by primary key");
    s
        .execute("create table TESTTABLE3 (ID3 int primary key, "
            + "DESCRIPTION3 varchar(1024) , ADDRESS3 varchar(1024)) partition by primary key");
    String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3 where ID1 = ID2  and DESCRIPTION1 = 'asif' and  ADDRESS1 = 'asif' AND ID2 = ID3 ";
    try {
      s.executeQuery(query);
    }
    catch (GemFireXDRuntimeException sqre) {
      fail("Single VM queries should be executable even if non colocated ");
    }
  }

  /**
   * Tests the basic condition of colocation matrix where the only condition
   * present is equi join having only single column partitionin
   * 
   * @throws Exception
   */
  public void testSimpleColocationMatrixWithTwoTableJoinOnSingleColumnPartition_1()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by primary key");
    s
        .execute("create table TESTTABLE2 (ID2 int primary key, "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by primary key " +
            		"colocate With (TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 ";

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Tests the basic condition of colocation matrix where the only condition
   * present is equi join having only single column partitionin
   * 
   * @throws Exception
   */
  public void testSimpleColocationMatrixWithTwoTableJoinOnSingleColumnPartition_2()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by Column (ID1)");
    s
        .execute("create table TESTTABLE2 (ID2 int primary key, "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by Column (ID2) Colocate WITH ( TESTTABLE1)");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 ";

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Tests the basic condition of colocation matrix where the only condition
   * present is equi join having only single column partitionin
   * 
   * @throws Exception
   */
  public void testSimpleColocationMatrixWithTwoTableJoinOnSingleColumnPartition_3()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 int primary key , "
        + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) ");
    s.execute("create table TESTTABLE2 (ID2 int primary key, "
        + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) "
        + "partition by primary key colocate with ( TESTTABLE1 )");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 ";

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Tests the basic condition of colocation matrix where two columns are used
   * in partitioning so the colocation matrix is 2 * 2
   * 
   * @throws Exception
   */
  public void testSimpleColocationMatrixWithTwoTableJoinOnMultiColumnPartitioning_1()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by column ( ID1, DESCRIPTION1)");
    s
        .execute("create table TESTTABLE2 (ID2 int primary key, "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by column ( ID2, DESCRIPTION2) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 and DESCRIPTION1 = DESCRIPTION2";

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Tests the colocation matrix if the partitioning columns having equi join
   * conditions are in an OR clause
   * 
   * @throws Exception
   */
  public void testSimpleColocationMatrixWithTwoTableJoinOnMultiColumnPartitioning_2()
      throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by column ( ID1, DESCRIPTION1)");
    s
        .execute("create table TESTTABLE2 (ID2 int primary key, "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by column ( ID2, DESCRIPTION2) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNotNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 OR DESCRIPTION1 = DESCRIPTION2";

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Tests the colocation matrix if the partitioning columns having equi join
   * conditions are in a nested AND junction
   * 
   * @throws Exception
   */
  public void testSimpleColocationMatrixWithTwoTableJoinOnMultiColumnPartitioning_3()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by column ( ID1, DESCRIPTION1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024) primary key, "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by column ( ID2, DESCRIPTION2, ADDRESS2) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where (ID1 = DESCRIPTION2) AND ( DESCRIPTION1 = ADDRESS2 AND  ADDRESS1 = ID2) ";

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  /**
   * Tests the colocation matrix if the partitioning columns having equi join
   * conditions in OR junction but is satisfactory
   * 
   * @throws Exception
   */
  public void testSimpleColocationMatrixWithTwoTableJoinOnMultiColumnPartitioning_4()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) primary key , "
            + "DESCRIPTION1 varchar(1024) , ADDRESS1 varchar(1024)) partition by column ( ID1, DESCRIPTION1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024) primary key, "
            + "DESCRIPTION2 varchar(1024) , ADDRESS2 varchar(1024)) partition by column ( ID2, DESCRIPTION2, ADDRESS2) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where (ID1 = ID2 AND  ADDRESS1 = ADDRESS2 AND DESCRIPTION1 = DESCRIPTIOn2 ) OR ( ID1 = ADDRESS2  AND ADDRESS1 = ID2 AND DESCRIPTION1 = DESCRIPTION2)";

      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testSimpleColocationMatrixWithTwoTableJoinOnSingleColumnPartitioningWithNodesPruningCondition()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1)");
    s
        .execute("create table TESTTABLE2 (ID2 int  primary key, "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column (ID2) colocate  with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 AND   ID1 > 5";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");

      for (int i = 0; i < 10; ++i) {
        ps1.setInt(1, i);
        ps1.setString(2, "DESCRIPTION1" + i);
        ps1.setString(3, "ADDRESS1" + i);
        ps1.execute();
        ps2.setInt(1, i);
        ps2.setString(2, "DESCRIPTION2" + i);
        ps2.setString(3, "ADDRESS2" + i);
        ps2.execute();

      }
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testBug39860() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1)");
    s
        .execute("create table TESTTABLE2 (ID2 int  primary key, "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));                
                assertEquals(1,sqi.getParameterCount());
              }
            }
          });
      String query = "select i.ID1 from TESTTABLE1 i,TESTTABLE2 p where i.ID1 = p.ID2 and i.ID1 in (?)";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");

      for (int i = 0; i < 10; ++i) {
        ps1.setInt(1, i);
        ps1.setString(2, "DESCRIPTION1" + i);
        ps1.setString(3, "ADDRESS1" + i);
        ps1.execute();
        ps2.setInt(1, i);
        ps2.setString(2, "DESCRIPTION2" + i);
        ps2.setString(3, "ADDRESS2" + i);
        ps2.execute();

      }
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setInt(1, 5);
      ResultSet rs = ps.executeQuery();
      assertTrue(rs.next());
      assertEquals(5,rs.getInt(1));
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testSimpleColocationMatrixWithPruningConditionOfEquality()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1)");
    s
        .execute("create table TESTTABLE2 (ID2 int  primary key, "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2 where ID1 = ID2 AND   ID1 = 5";
      conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");

      /*
       * for(int i= 0 ; i < 10; ++i) { ps1.setInt(1, i); ps1.setString(2,
       * "DESCRIPTION1"+i); ps1.setString(3, "ADDRESS1"+i); ps1.execute();
       * ps2.setInt(1, i); ps2.setString(2, "DESCRIPTION2"+i); ps2.setString(3,
       * "ADDRESS2"+i); ps2.execute();
       *  }
       */
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  
  public void testMultiTableColocationMatrix_1() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 int  primary key, "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 int  primary key, "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE4 (ID4 int  primary key, "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4, ADDRESS4) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ID2 AND   ID2= ID3 AND ID2 = ID4 AND  ADDRESS1 = ADDRESS2 AND ADDRESS3 = ADDRESS4 AND ADDRESS1 = ADDRESS4";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");

      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testMultiTableColocationMatrix_2() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int  , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 int  , "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 int  , "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE4 (ID4 int  , "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4, ADDRESS4) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ID2 AND   ID2= ID3 AND ID2 = ID4 AND  ADDRESS1 = ADDRESS2 AND ADDRESS3 = ADDRESS4 AND ADDRESS1 = ADDRESS4";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");

      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testMultiTableColocationMatrix_3() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 int  primary key , "
        + "DESCRIPTION1 varchar(1024) not null, "
        + "ADDRESS1 varchar(1024) not null)");
    s.execute("create table TESTTABLE2 (ID2 int primary key , "
        + "DESCRIPTION2 varchar(1024) not null, "
        + "ADDRESS2 varchar(1024) not null) "
        + "partition by primary key colocate with ( TESTTABLE1)");
    s.execute("create table TESTTABLE3 (ID3 int primary key , "
        + "DESCRIPTION3 varchar(1024) not null, "
        + "ADDRESS3 varchar(1024) not null) "
        + "partition by column (ID3) colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE4 (ID4 int primary key , "
        + "DESCRIPTION4 varchar(1024) not null, "
        + "ADDRESS4 varchar(1024) not null) "
        + "partition by primary key colocate with ( TESTTABLE1)");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ID2 AND   ID2= ID3 AND ID2 = ID4 AND  ADDRESS1 = ADDRESS2 AND ADDRESS3 = ADDRESS4 AND ADDRESS1 = ADDRESS4";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");

      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  public void testMultiTableColocationMatrix_4() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 int  , "
        + "DESCRIPTION1 varchar(1024) not null , "
        + "ADDRESS1 varchar(1024) not null, primary key(ID1, ADDRESS1))");
    s.execute("create table TESTTABLE2 (ID2 int  , "
        + "DESCRIPTION2 varchar(1024) not null, "
        + "ADDRESS2 varchar(1024) not null, primary key (ID2, ADDRESS2))"
        + "partition by column (ID2, ADDRESS2) colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE3 (ID3 int  , "
        + "DESCRIPTION3 varchar(1024) not null, "
        + "ADDRESS3 varchar(1024) not null, primary key (ID3, ADDRESS3))"
        + "partition by primary key colocate with ( TESTTABLE1)");
    s.execute("create table TESTTABLE4 (ID4 int , "
        + "DESCRIPTION4 varchar(1024) not null, "
        + "ADDRESS4 varchar(1024) not null, primary key (ID4, ADDRESS4))"
        + "partition by primary key colocate with (TESTTABLE1)");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ID2 AND   ID2= ID3 AND ID2 = ID4 AND  ADDRESS1 = ADDRESS2 AND ADDRESS3 = ADDRESS4 AND ADDRESS1 = ADDRESS4";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");

      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testMultiTableColocationMatrixNotExecutable() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int  , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 int  , "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 int  , "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE4 (ID4 int  , "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4, ADDRESS4) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {      
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ID2 AND   ID2= ID3 AND ID2 = ID4 AND  ADDRESS1 = ADDRESS2 AND ADDRESS3 = ADDRESS4 ";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");

      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
      try {
        s.executeQuery(query);
        fail("The query should be unexecutable in multi vm scenario");
      } catch (SQLException snse) {
        assertEquals(snse.getSQLState(), "0A000");
        assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.10");
      }
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testComplexMultiTableColocationMatrix_Executable_1() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1,DESCRIPTION1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2,DESCRIPTION2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3,DESCRIPTION3) colocate with ( TESTTABLE1) ");
    
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
                        
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3 where ID1 = ADDRESS2 AND   ADDRESS1= ID2 AND   ID2 = DESCRIPTION3 AND ADDRESS2 = ID3 AND DESCRIPTION2 = ADDRESS3 AND DESCRIPTION1 = DESCRIPTION2 ";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
  
      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testComplexMultiTableColocationMatrix_NOT_Executable_1() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1,DESCRIPTION1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2,DESCRIPTION2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3,DESCRIPTION3) colocate with ( TESTTABLE1) ");
    
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNotNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
                        
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3 where ID1 = ADDRESS2 AND   ADDRESS1= ID2 AND   ID2 = DESCRIPTION3 AND ADDRESS2 = ID3 AND DESCRIPTION2 = ADDRESS3 AND DESCRIPTION2 = ADDRESS1 AND DESCRIPTION1 = ADDRESS2 ";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
  
      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testComplexMultiTableColocationMatrix_Executable_2() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1,DESCRIPTION1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2,DESCRIPTION2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3,DESCRIPTION3) colocate with( TESTTABLE1) ");
    
    s
    .execute("create table TESTTABLE4 (ID4 varchar(1024), "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4, ADDRESS4,DESCRIPTION4) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
                        
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ADDRESS2 AND   ADDRESS1= ID2 AND   ID2 = DESCRIPTION3 AND ADDRESS2 = ID3 AND ID3 = ADDRESS4 AND ADDRESS3 = ID4 AND DESCRIPTION1 = DESCRIPTION2 AND  DESCRIPTION3 = DESCRIPTION4 AND DESCRIPTION2 = ADDRESS3";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
  
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");
  
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testComplexMultiTableColocationMatrix_NOT_Executable_2() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1,DESCRIPTION1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2,DESCRIPTION2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3,DESCRIPTION3) colocate with ( TESTTABLE1) ");
    
    s
    .execute("create table TESTTABLE4 (ID4 varchar(1024), "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4, ADDRESS4,DESCRIPTION4) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
     
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ADDRESS2 AND   ADDRESS1= ID2 AND   ID2 = DESCRIPTION3 AND ADDRESS2 = ID3 AND ID3 = ADDRESS4 AND ADDRESS3 = ID4 AND DESCRIPTION1 = DESCRIPTION2 AND  DESCRIPTION3 = DESCRIPTION4 ";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
  
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");
  
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
      try {
        s.executeQuery(query);
        fail("This query should not be executable in multi VM scenario");
      } catch (SQLException snse) {
        assertEquals(snse.getSQLState(), "0A000");
        assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.10");
      }
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testComplexMultiTableColocationMatrix_Executable_3() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3) colocate with ( TESTTABLE1) ");
    
    s
    .execute("create table TESTTABLE4 (ID4 varchar(1024), "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4, ADDRESS4) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE5 (ID5 varchar(1024), "
        + "DESCRIPTION5 varchar(1024)not null, ADDRESS5 varchar(1024) not null) partition by column ( ID5, ADDRESS5) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
                        
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4,TESTTABLE5 where ID1 = ADDRESS3 AND   ADDRESS5= ID2 AND   ID3 = ADDRESS1 AND ADDRESS2 = ID5   AND ID3 = ID4   AND ID5 = ID4   AND ADDRESS5 = ADDRESS4 AND  ADDRESS3 = ADDRESS4";
           
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testComplexMultiTableColocationMatrix_NOT_Executable_3() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 varchar(1024) , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1, ADDRESS1)");
    s
        .execute("create table TESTTABLE2 (ID2 varchar(1024), "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2, ADDRESS2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3, ADDRESS3) colocate with ( TESTTABLE1) ");
    
    s
    .execute("create table TESTTABLE4 (ID4 varchar(1024), "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4, ADDRESS4) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE5 (ID5 varchar(1024), "
        + "DESCRIPTION5 varchar(1024)not null, ADDRESS5 varchar(1024) not null) partition by column ( ID5, ADDRESS5) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNotNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
                        
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4,TESTTABLE5 where ID1 = ADDRESS3 AND   ADDRESS5= ID2 AND   ID3 = ADDRESS1 AND ADDRESS2 = ID5   AND ID3 = ID4   AND ID5 = ID4   AND ADDRESS5 = ADDRESS4 AND  ADDRESS3 = ADDRESS2";
           
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testComplexMultiTableColocationMatrix_Executable_4_Bug40085() throws Exception {
    Connection conn = getConnection();    
    
    Statement s = conn.createStatement();
    s
        .execute("create table trade.customers (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid))");
    s
    .execute("create table trade.securities (sec_id int not null, symbol varchar(10) not null, exchange varchar(10) not null, tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    
    s
        .execute("create table trade.portfolio (cid int not null, sid int not null, qty int not null, availQty int not null,tid int, constraint portf_pk primary key (cid, sid), constraint cust_fk foreign key (cid) references trade.customers (cid) on delete restrict, constraint sec_fk foreign key (sid) references trade.securities (sec_id) )   partition by column (cid) colocate with (trade.customers)");
    
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
                        
              }
            }
          });
      String query = "select * from trade.customers c, trade.securities s, trade.portfolio f where c.cid = f.cid and sec_id = f.sid and f.tid = ?";
      conn.prepareStatement(query);    
      
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testBug40053() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table SECTORS (id int not null primary key, name varchar(20), market_cap double)");
    s.execute("create table INSTRUMENTS (id varchar(20) not null primary key, sector_id int) PARTITION BY PRIMARY KEY ");
    s.execute("create table POSITIONS (id int not null primary key, book_id int, instrument varchar(20) not null, amount int, synthetic int, owner varchar(20),  symbol varchar(5)) PARTITION BY COLUMN (instrument) COLOCATE WITH (INSTRUMENTS) ");
    s.execute("create table RISKS (id int not null primary key, position_id int not null, risk_value int)  PARTITION BY PRIMARY KEY");
    s.execute("create index position_instrument_idx on POSITIONS (instrument)");
    
    PreparedStatement pstmt1 = conn.prepareStatement("insert into  sectors values(?,?,?)");
    PreparedStatement pstmt2 = conn.prepareStatement("insert into  instruments values(?,?)");
    PreparedStatement pstmt3 = conn.prepareStatement("insert into  positions values(?,?,?,?,?,?,?)");
    PreparedStatement pstmt4 = conn.prepareStatement("insert into  risks values (?,?,?)");
    for(int i = 0 ;i < 10 ;++i) {
      pstmt1.setInt(1, i);
      pstmt1.setString(2, "sector"+i);
      pstmt1.setDouble(3, i);
      pstmt2.setString(1,"instrument"+i);
      pstmt2.setInt(2,i);
      pstmt3.setInt(1, i);
      pstmt3.setInt(2, i);
      pstmt3.setString(3, "instrument"+i);
      pstmt3.setInt(4, i);
      pstmt3.setInt(5, i);
      pstmt3.setString(6, "owner"+i);
      pstmt3.setString(7, "sym"+i%2);
      pstmt4.setInt(1, i);
      pstmt4.setInt(2, i);
      pstmt4.setInt(3, i);
      
      
      pstmt1.executeUpdate();
      pstmt2.executeUpdate();
      pstmt3.executeUpdate();
      pstmt4.executeUpdate();
    }
   /* GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps) {
              if (qInfo instanceof SelectQueryInfo) {
               
              }
            }
          });*/
      String query = "select * from Instruments i, Positions p where  i.id = p.instrument and i.id IN ('instrument1','instrument4','instrument5')";
       PreparedStatement ps = conn.prepareStatement(query);
      //ps.setInt(1, 5);
      ResultSet rs = ps.executeQuery();
      int num =0;
       while(rs.next()) {
         ++num;
       }
      assertEquals(3,num);
   }

  public void testBug40307() throws Exception {
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024), "
        + "DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024)"
        + " not null) partition by column (ID1)");
    s.execute("create table TESTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024)"
        + " not null) partition by column (ID2)"
        + " colocate with (TESTTABLE1) ");
    s.execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024)"
        + " not null) REPLICATE");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 r1, TESTTABLE1 p1, "
          + "TESTTABLE2 p2 where p1.ID1 = p2.ID2 and r1.ID3 = p2.ID2";
      String query2 = "select * from TESTTABLE3 r1, TESTTABLE1 p1, "
          + "TESTTABLE2 p2 where p1.ID1 = r1.ID3 and r1.ID3 = p2.ID2";
      String query3 = "select * from TESTTABLE3 r1, TESTTABLE1 p1, "
          + "TESTTABLE2 p2 where r1.ID3 = 'X3' and p1.ID1 = r1.ID3 "
          + "and 'X3' = p2.ID2";

      s.executeQuery(query1);
      s.executeQuery(query2);
      s.executeQuery(query3);
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    }
  }

  public void testDifferentColumnsOrConstantTying_EXECUTE()
      throws Exception {
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024)"
        + " not null) partition by column (ID1, ADDRESS1)");
    s.execute("create table TESTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null)"
        + " partition by column (ADDRESS2, ID2) colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024) not null, ADDRESS3 varchar(1024) not null)"
        + " partition by column (ADDRESS3, ID3) colocate with (TESTTABLE1)");
    s.execute("create table REPLTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024)"
        + " not null) REPLICATE");
    s.execute("create table PARTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024)"
        + " not null) partition by column (ID1)");
    s.execute("create table PARTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null)"
        + " partition by column (ADDRESS2) colocate with (PARTTABLE1)");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p1.ADDRESS1 and p1.ADDRESS1 = p2.ID2"
          + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
          + " and p1.ADDRESS1 = p2.ADDRESS2";
      String query2 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p3.ADDRESS3 and p3.ADDRESS3 = p2.ID2"
          + " and p1.ID1 = p3.ID3 and p1.ADDRESS1 = p2.DESCRIPTION2"
          + " and p2.ADDRESS2 = p2.ID2 and p3.ID3 = p2.DESCRIPTION2";
      String query3 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2 where p1.ID1 = 'X3' and 'X3' = p2.ID2"
        + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
        + " and p1.ADDRESS1 = p2.ADDRESS2";
      String query4 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2 where p1.ID1 IN ('X3') and p2.ID2 IN ('X3', 'X4')"
        + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
        + " and p1.ADDRESS1 = p2.ADDRESS2 and p2.ID2 IN ('X3')";
      String query5 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2 where p1.ID1 IN ('X3') and p2.ID2 IN ('X3', 'X3')"
        + " and p1.ID1 = p3.ID3 and p2.ADDRESS2 = p3.ADDRESS3"
        + " and p1.ADDRESS1 = p2.ADDRESS2";
      String query6 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2 where p1.ID1 = p1.DESCRIPTION1 and p1.ID1 = p3.ID3"
        + " and p1.DESCRIPTION1 = p2.ID2 and p2.ADDRESS2 = p3.ADDRESS3"
        + " and p1.ADDRESS1 = p2.ADDRESS2";
      String query7 = "select ID1, DESCRIPTION2 from REPLTABLE1, TESTTABLE2 "
        + "where ID2 = ADDRESS2";
      String query8 = "select p1.ID1, p2.DESCRIPTION2 from REPLTABLE1 r1, "
        + "PARTTABLE1 p1, PARTTABLE2 p2 where p1.ID1 = p2.ADDRESS2";
      String query9 = "select r1.ID1, p1.DESCRIPTION1 from REPLTABLE1 r1, "
        + "PARTTABLE1 p1 where p1.ID1 = 'X1'";
      String query10 = "select r1.ID1, p1.DESCRIPTION1 from REPLTABLE1 r1, "
        + "PARTTABLE1 p1 where p1.ID1 IN ('X1', 'X2', 'X3')";
      String query11 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 IN ('X7') and ADDRESS2 IN ('X7')";

      s.executeQuery(query1);
      s.executeQuery(query2);
      s.executeQuery(query3);
      s.executeQuery(query4);
      s.executeQuery(query5);
      s.executeQuery(query6);
      s.executeQuery(query7);
      s.executeQuery(query8);
      s.executeQuery(query9);
      s.executeQuery(query10);
      s.executeQuery(query11);
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    }
  }

  public void testNonPartitioningColumnOrConstantTying_NO_EXECUTE()
      throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) "
        + "not null) partition by column (ID1)");
    s.execute("create table TESTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) "
        + "not null) partition by column (ID2) colocate with (TESTTABLE1) ");
    s.execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024) not null, ADDRESS3 varchar(1024) "
        + "not null) partition by column (ID3) colocate with (TESTTABLE1)");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNotNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p3.ADDRESS3 and p3.ADDRESS3 = p2.ID2"
          + " and p1.ID1 = p3.ADDRESS3";
      String query2 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2 where p1.ID1 = p1.ADDRESS1 and p1.ADDRESS1 = p2.ID2"
        + " and p2.ADDRESS2 = p3.ID3";
      String query3 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2 where p1.ID1 = 'XP3' and 'XP33' = p2.ID2"
        + " and p1.ID1 = p3.ID3";

      s.executeQuery(query1);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query2);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query3);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testMultiColumnIncompleteJoin_NO_EXECUTE() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) "
        + "not null) partition by column (ID1, ADDRESS1)");
    s.execute("create table TESTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) not null)"
        + " partition by column (ADDRESS2, ID2) colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024) not null, ADDRESS3 varchar(1024) not null)"
        + " partition by column (ADDRESS3, ID3) colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE4 (ID4 varchar(1024), "
        + "DESCRIPTION4 varchar(1024) not null, ADDRESS4 varchar(1024) "
        + "not null) REPLICATE");
    s.execute("create table PARTTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) "
        + "not null, TID1 int, primary key (ID1)) PARTITION BY COLUMN (ID1)");
    s.execute("create table PARTTABLE2 (ID2 int not null, "
        + " DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) "
        + "not null, TID2 int, primary key (ID2)) PARTITION BY COLUMN (ID2) "
        + "colocate with (PARTTABLE1)");
    s.execute("create table REPLTABLE1 (ID1 int not null, "
        + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) "
        + "not null, TID1 int, primary key (ID1)) REPLICATE");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNotNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p2.ID2 and p1.ID1 = p3.ID3";
      String query2 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2 where p1.ID1 = p2.ID2 and p2.ID2 = p3.ID3"
          + " and p1.ID1 = p3.ID3";
      String query3 = "select * from TESTTABLE4 r4, TESTTABLE1 p1, "
        + "TESTTABLE2 p2 where r4.ID4 = 'X4' and p1.ID1 = r4.ID4 "
        + "and 'X3' = p2.ID2 and p1.ADDRESS1 = p2.ADDRESS2";
      String query4 = "select * from TESTTABLE1 p1, TESTTABLE2 p2 "
        + "where p1.ADDRESS1 = p2.ADDRESS2 and p1.ID1 IN ('X7', 'X9')";
      String query5 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
          + "where ID1 IN (7, 9)";
      String query6 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID2 IN (0)";
      String query7 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID2 = 0";
      String query8 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 = 0 or ID2 = 0";
      String query9 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 = 1 AND ID2 = 0";
      String query10 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 = TID1";
      String query11 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 = TID2";
      String query12 = "select p1.ID1, p2.DESCRIPTION2 from REPLTABLE1 r1, "
          + "PARTTABLE1 p1, PARTTABLE2 p2 where p1.ID1 = r1.ID1";
      String query13 = "select p1.ID1, p2.DESCRIPTION2 from REPLTABLE1 r1, "
        + "PARTTABLE1 p1, PARTTABLE2 p2 where p1.ID1 = 2";
      String query14 = "select p1.ID1, p2.DESCRIPTION2 from REPLTABLE1 r1, "
        + "PARTTABLE1 p1, PARTTABLE2 p2 where p1.ID1 in (2, 3, 6)";
      String query15 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 IN (7, 9) and ID2 IN (7, 9)";
      String query16 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 IN (7) and ID2 IN (9)";
      String query17 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where DESCRIPTION2 IN ('DESC7', 'DESC8')";
      String query18 = "select ID1, DESCRIPTION2 from PARTTABLE1, PARTTABLE2 "
        + "where ID1 IN (7) and TID2 IN (7)";

      s.executeQuery(query1);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query2);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query3);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query4);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query5);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query6);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query7);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query8);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query9);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query10);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query11);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query12);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query13);      
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query14);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query15);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query16);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query17);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query18);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testMultiTableColocationWithComplexTying() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) "
        + "not null) partition by column (ID1, ADDRESS1, DESCRIPTION1)");
    s.execute("create table TESTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) "
        + "not null) partition by column (ADDRESS2, ID2, DESCRIPTION2) "
        + "colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024) not null, ADDRESS3 varchar(1024) "
        + "not null) partition by column (ADDRESS3, DESCRIPTION3, ID3) "
        + "colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE4 (ID4 varchar(1024), "
        + "DESCRIPTION4 varchar(1024) not null, ADDRESS4 varchar(1024) "
        + "not null) partition by column (DESCRIPTION4, ADDRESS4, ID4) "
        + "colocate with (TESTTABLE2)");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2, TESTTABLE4 p4 where p1.ID1 = p2.ADDRESS2"
          + " and p1.ADDRESS1 = p2.DESCRIPTION2 and p1.DESCRIPTION1 = p2.ID2"
          + " and p3.DESCRIPTION3 = p2.ID2 and p2.DESCRIPTION2 = p3.ADDRESS3"
          + " and p4.ADDRESS4 = p3.DESCRIPTION3 and p4.ID4 = p3.ADDRESS3"
          + " and p3.ADDRESS3 = p4.DESCRIPTION4 and p2.ADDRESS2 = p3.ADDRESS3"
          + " and p3.ID3 = p2.ADDRESS2";
      String query2 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2, TESTTABLE4 p4 where p1.ID1 = p2.ADDRESS2"
          + " and p1.ADDRESS1 = p2.ID2 and p1.DESCRIPTION1 = p2.ID2"
          + " and p1.DESCRIPTION1 = p2.DESCRIPTION2 and p2.ID2 = p3.ADDRESS3"
          + " and p2.ADDRESS2 = p3.ID3 and p3.ADDRESS3 = p4.ADDRESS4"
          + " and p3.ID3 = p4.ID4 and p1.ADDRESS1 = p3.DESCRIPTION3"
          + " and p3.ADDRESS3 = p4.DESCRIPTION4";
      String query3 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2, TESTTABLE4 p4 where p1.ID1 = p2.ADDRESS2"
        + " and p1.ADDRESS1 = p2.ID2 and p1.DESCRIPTION1 = p2.ID2"
        + " and p1.DESCRIPTION1 = p2.DESCRIPTION2 and p2.ID2 = p3.ADDRESS3"
        + " and p2.ADDRESS2 = p3.ID3 and p3.ADDRESS3 = p4.ADDRESS4"
        + " and p3.ID3 = p4.ID4 and p1.ADDRESS1 = p3.DESCRIPTION3"
        + " and p1.DESCRIPTION1 = p3.ADDRESS3 and p2.ADDRESS2 = p4.ID4"
        + " and p3.ADDRESS3 = p4.DESCRIPTION4 and p1.ADDRESS1 = p4.DESCRIPTION4";

      s.executeQuery(query1);
      s.executeQuery(query2);
      s.executeQuery(query3);
      assertTrue(this.callbackInvoked);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testMultiTableColocationWithComplexTying_NO_EXECUTE()
      throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 varchar(1024) , "
        + "DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) "
        + "not null) partition by column (ID1, ADDRESS1, DESCRIPTION1)");
    s.execute("create table TESTTABLE2 (ID2 varchar(1024), "
        + "DESCRIPTION2 varchar(1024) not null, ADDRESS2 varchar(1024) "
        + "not null) partition by column (ADDRESS2, ID2, DESCRIPTION2) "
        + "colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE3 (ID3 varchar(1024), "
        + "DESCRIPTION3 varchar(1024) not null, ADDRESS3 varchar(1024) "
        + "not null) partition by column (ADDRESS3, DESCRIPTION3, ID3) "
        + "colocate with (TESTTABLE1)");
    s.execute("create table TESTTABLE4 (ID4 varchar(1024), "
        + "DESCRIPTION4 varchar(1024) not null, ADDRESS4 varchar(1024) "
        + "not null) partition by column (DESCRIPTION4, ADDRESS4, ID4) "
        + "colocate with (TESTTABLE2)");

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNotNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));

              }
            }
          });
      String query1 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2, TESTTABLE4 p4 where p1.ID1 = p2.ADDRESS2"
          + " and p1.ADDRESS1 = p2.DESCRIPTION2 and p1.DESCRIPTION1 = p2.ID2"
          + " and p3.DESCRIPTION3 = p2.ID2 and p2.DESCRIPTION2 = p3.ADDRESS3"
          + " and p4.ADDRESS4 = p3.DESCRIPTION3 and p4.ID4 = p3.ADDRESS3"
          + " and p3.ADDRESS3 = p4.DESCRIPTION4 and p2.ADDRESS2 = p3.ADDRESS3"
          + " and p3.ID3 = p4.ADDRESS4";
      String query2 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
          + " TESTTABLE2 p2, TESTTABLE4 p4 where p1.ID1 = p2.ADDRESS2"
          + " and p1.ADDRESS1 = p2.ID2 and p1.DESCRIPTION1 = p2.ID2"
          + " and p1.DESCRIPTION1 = p2.DESCRIPTION2 and p2.ID2 = p3.ADDRESS3"
          + " and p2.ADDRESS2 = p3.ID3 and p3.ADDRESS3 = p4.ADDRESS4"
          + " and p3.ID3 = p4.ID4 and p4.ID4 = p3.DESCRIPTION3"
          + " and p3.ADDRESS3 = p4.DESCRIPTION4";
      String query3 = "select * from TESTTABLE3 p3, TESTTABLE1 p1,"
        + " TESTTABLE2 p2, TESTTABLE4 p4 where p1.ID1 = p2.ADDRESS2"
        + " and p1.ADDRESS1 = p2.ID2 and p1.DESCRIPTION1 = p2.ID2"
        + " and p1.DESCRIPTION1 = p2.DESCRIPTION2 and p2.ID2 = p3.ADDRESS3"
        + " and p2.ADDRESS2 = p3.ID3 and p3.ADDRESS3 = p4.ADDRESS4"
        + " and p3.ID3 = p4.ID4 and p4.ID4 = p3.DESCRIPTION3"
        + " and p1.DESCRIPTION1 = p3.ADDRESS3 and p2.ADDRESS2 = p4.ID4"
        + " and p3.ADDRESS3 = p4.DESCRIPTION4 and p1.ADDRESS1 = p4.DESCRIPTION4";

      s.executeQuery(query1);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query2);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      s.executeQuery(query3);
      assertTrue(this.callbackInvoked);
      this.callbackInvoked = false;
      
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  public void testColocationDependencyOnMasterTable_Bug41574() throws Exception {    
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table TESTTABLE1 (ID1 int  primary key , "
        + "DESCRIPTION1 varchar(1024) not null, "
        + "ADDRESS1 varchar(1024) not null)");
    s.execute("create table TESTTABLE2 (ID2 int primary key , "
        + "DESCRIPTION2 varchar(1024) not null, "
        + "ADDRESS2 varchar(1024) not null) "
        + "partition by primary key colocate with ( TESTTABLE1)");
    s.execute("create table TESTTABLE3 (ID3 int primary key , "
        + "DESCRIPTION3 varchar(1024) not null, "
        + "ADDRESS3 varchar(1024) not null) "
        + "partition by column (ID3) colocate with (TESTTABLE2)");
    s.execute("create table TESTTABLE4 (ID4 int primary key , "
        + "DESCRIPTION4 varchar(1024) not null, "
        + "ADDRESS4 varchar(1024) not null) "
        + "partition by primary key colocate with ( TESTTABLE3)");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
                try {
                  assertNotNull(sqi.getRegion());
                  GfxdPartitionResolver spr =
                    (GfxdPartitionResolver)((PartitionedRegion)sqi.getRegion()).getPartitionResolver();
                  String masterTable = spr.getMasterTable(true);
                  assertEquals(masterTable.substring(masterTable.lastIndexOf('/')+1,masterTable.length()),"TESTTABLE1");
                  assertTrue(sqi.getTestFlagIgnoreSingleVMCriteria());                  
                  
                }catch(Exception e) {
                  fail(e.toString());
                }
              }
            }
            
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ID2 AND   ID2= ID3 AND ID2 = ID4 AND  ADDRESS1 = ADDRESS2 AND ADDRESS3 = ADDRESS4 AND ADDRESS1 = ADDRESS4";
      PreparedStatement ps1 = conn
          .prepareStatement("Insert into TESTTABLE1 values (?,?,?)");
      PreparedStatement ps2 = conn
          .prepareStatement("Insert into TESTTABLE2 values (?,?,?)");
      PreparedStatement ps3 = conn
      .prepareStatement("Insert into TESTTABLE3 values (?,?,?)");
      PreparedStatement ps4 = conn
      .prepareStatement("Insert into TESTTABLE4 values (?,?,?)");

      
        for(int i= 0 ; i < 10; ++i) { 
          ps1.setInt(1, i); 
          ps1.setString(2,"DESCRIPTION1"+i); 
          ps1.setString(3, "ADDRESS1"+i); 
          ps1.execute();
          ps2.setInt(1, i); 
          ps2.setString(2, "DESCRIPTION2"+i); 
          ps2.setString(3,"ADDRESS2"+i); 
          ps2.execute(); 
          ps3.setInt(1, i); 
          ps3.setString(2, "DESCRIPTION3"+i); 
          ps3.setString(3,"ADDRESS3"+i); 
          ps3.execute();
          ps4.setInt(1, i); 
          ps4.setString(2, "DESCRIPTION4"+i); 
          ps4.setString(3,"ADDRESS4"+i); 
          ps4.execute();
       }
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
      
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testMultiTableColocationMatrixWithRepeatedCondition()
      throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table TESTTABLE1 (ID1 int primary key , "
            + "DESCRIPTION1 varchar(1024) not null , ADDRESS1 varchar(1024) not null) partition by column ( ID1)");
    s
        .execute("create table TESTTABLE2 (ID2 int  primary key, "
            + "DESCRIPTION2 varchar(1024)not null, ADDRESS2 varchar(1024) not null) partition by column ( ID2) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE3 (ID3 int  primary key, "
        + "DESCRIPTION3 varchar(1024)not null, ADDRESS3 varchar(1024) not null) partition by column ( ID3) colocate with ( TESTTABLE1) ");
    s
    .execute("create table TESTTABLE4 (ID4 int  primary key, "
        + "DESCRIPTION4 varchar(1024)not null, ADDRESS4 varchar(1024) not null) partition by column ( ID4) colocate with ( TESTTABLE1) ");
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
                SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNotNull(sqi.getWhereClause()
                    .isEquiJoinColocationCriteriaFullfilled(null));
              }
            }
          });
      String query = "Select * from TESTTABLE1, TESTTABLE2, TESTTABLE3, TESTTABLE4 where ID1 = ID2 AND   ID3= ID4 AND ID2 = ID1 AND  ID4 = ID3";
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }
  
  public void testSupportedColocatedSubqueries_1() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table Customers (cust_id int primary key , "
            + "cust_name varchar(1024) not null ) partition by column ( cust_id)");
    s
        .execute("create table Orders (oid int  primary key, "
            + "order_type varchar(1024) not null, order_amount int,  ordered_by int ) " +
            		"partition by column ( ordered_by ) colocate with ( Customers) ");
    
    /*PreparedStatement psInsertCust = conn.prepareStatement("insert into customers values(?,?)");
    for(int i=1; i < 101; ++i) {
      psInsertCust.setInt(1,i);
      psInsertCust.setString(2,"name_"+i);
      psInsertCust.executeUpdate();  
    }
    
    PreparedStatement psInsertOrders= conn.prepareStatement("insert into Orders values(?,?,?,?)");
    int j = 1;
    for(int i=1; i < 101; ++i) {
      for(int k = 1; k < 101;++k) {
        psInsertOrders.setInt(1,j++);
        psInsertOrders.setString(2,"order_type_"+j);
        psInsertOrders.setInt(3,i*k*10 );
        psInsertOrders.setInt(4,i );        
      }
      
    }*/
    
    
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void subQueryInfoObjectFromOptmizedParsedTree(List<SubQueryInfo>  qInfoList, 
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
               assertEquals(qInfoList.size(),1);               
                SelectQueryInfo sqi = qInfoList.get(0);
                ColocatedQueriesTest.this.callbackInvoked = true;
                assertNull(sqi.getWhereClause().isEquiJoinColocationCriteriaFullfilled(null));              
            }
          });
      //Select all the customers whose total order value is more than 1000
      String query = "Select * from Customers where ( select sum(order_amount) from orders where " +
      		"ordered_by = cust_id ) > 1000";
       
      s.executeQuery(query);
      assertTrue(this.callbackInvoked);
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }  
  }
  public void testSupportedColocatedSubqueries_2() throws Exception
  {
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table Customers (cust_id int primary key , "
        + "cust_name varchar(1024) not null, address varchar(1024) )";
    String table2 = "create table Orders (oid int  primary key, "
        + "order_type varchar(1024) not null, order_amount int,  ordered_by int, "
        + "orderer_name varchar(1024) )";
    s.execute(table1 + "partition by column (cust_id, cust_name)");
    s.execute(table2 + "partition by column ( ordered_by,orderer_name ) colocate with ( customers )");
    GemFireXDQueryObserver old = null;
    try {
      /*PreparedStatement psInsertCust = conn
          .prepareStatement("insert into customers values(?,?,?)");
      PreparedStatement ps = null;

      ps = psInsertCust;
      for (int i = 1; i < 101; ++i) {
        ps.setInt(1, i);
        ps.setString(2, "name_" + i);
        ps.setString(3, "address_" + i);
        ps.executeUpdate();

      }

      PreparedStatement psInsertOrdersGfxd = conn
          .prepareStatement("insert into Orders " + "values(?,?,?,?,?)");
      PreparedStatement psInsertOrders = null;

      psInsertOrders = psInsertOrdersGfxd;
      int j = 1;
      for (int i = 1; i < 101; ++i) {
        for (int k = 1; k < 101; ++k) {
          psInsertOrders.setInt(1, j++);
          psInsertOrders.setString(2, "order_type_" + j);
          psInsertOrders.setInt(3, i * k * 10);
          psInsertOrders.setInt(4, i);
          psInsertOrders.setString(5, "name_" + i);
        }

      }*/

      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void subQueryInfoObjectFromOptmizedParsedTree(
                List<SubQueryInfo> qInfoList, GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
              assertEquals(qInfoList.size(), 1);
              SelectQueryInfo sqi = qInfoList.get(0);
              ColocatedQueriesTest.this.callbackInvoked = true;
              assertNull(sqi.getWhereClause()
                  .isEquiJoinColocationCriteriaFullfilled(null));

            }
          });

      String query = "Select * from Customers where ( select sum(order_amount) from orders where "
          + "ordered_by = cust_id and orderer_name = cust_name ) > 1000";     
       s.executeQuery(query);
       assertTrue(this.callbackInvoked);
    }
    finally {
      s.execute("drop table orders");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  }
  public void testSupportedCorrelatedSubqueries_3() throws Exception
  {    
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table Customers (cust_id int primary key , "
        + "cust_name varchar(1024) not null, address varchar(1024) )";
    String table2 = "create table Orders (oid int  primary key, "
        + "order_type varchar(1024) not null, order_amount int,  ordered_by int, "
        + "orderer_name varchar(1024) )";
    String table3 = "create table Suborders (sub_oid int  primary key, "
      + "sub_order_type varchar(1024) not null, sub_order_amount int,  sub_ordered_by int, "
      + "sub_orderer_name varchar(1024) )";
    s.execute(table1 + "partition by column (cust_id, cust_name)");
    s.execute(table2 + "partition by column ( ordered_by,orderer_name) colocate with  (customers) ");
    s.execute(table3 + "partition by column ( sub_ordered_by,sub_orderer_name ) colocate with (orders)");
    
    String query1 = "Select * from Customers where ( select sum(order_amount) from orders where "
      + "ordered_by = cust_id and orderer_name = cust_name ) > 1000 and " +
      "( select sum(sub_order_amount) from suborders where sub_ordered_by = cust_id " +
      "and sub_orderer_name = cust_name ) < 500 "; 
    
    String query2 = "Select * from Customers where ( select  sum(order_amount) from orders where "
      + "ordered_by = cust_id and orderer_name = cust_name and " +
      "( select sum(sub_order_amount) from suborders where sub_ordered_by = ordered_by and " +
      		"sub_orderer_name = orderer_name ) < 500  ) > 1000 ";
    String queries[] = new String[] {query1,query2};
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void subQueryInfoObjectFromOptmizedParsedTree(
                List<SubQueryInfo> qInfoList, GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
              assertEquals(qInfoList.size(), 2);
              for(SubQueryInfo sqi:qInfoList) {
                assertNull(sqi.getWhereClause() .isEquiJoinColocationCriteriaFullfilled(null));
              }
              ColocatedQueriesTest.this.callbackInvoked = true;            

            }
          });

       for(String query : queries) { 
         s.executeQuery(query);
         assertTrue(this.callbackInvoked);
         this.callbackInvoked = false;
       }
       
    }
    finally {
      s.execute("drop table suborders");
      s.execute("drop table orders");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  }
  
  public void testSupportedCorrelatedSubqueries_4() throws Exception
  {    
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
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
    s.execute(table1 + "partition by column (cust_id, cust_name)");
    s.execute(table2 + "partition by column ( ordered_by,orderer_name) colocate with  (customers) ");
    s.execute(table3 + "partition by column ( sub_ordered_by,sub_orderer_name ) colocate with (orders)");
    s.execute(table4 + "partition by column ( sub_sub_ordered_by,sub_sub_orderer_name ) colocate with " +
    		"(customers)");
    String query1 = "Select * from Customers where " 
    	+	" ( select sum(order_amount) from orders where ordered_by = cust_id and " +
    		"orderer_name = cust_name ) > 1000 and  " +
      "( select sum(sub_order_amount) from suborders where  sub_orderer_name = cust_name " +
      " and sub_ordered_by = cust_id  and 12 = " +
      "( select Sum(sub_sub_ordered_by) from sub_suborders where sub_sub_ordered_by = sub_ordered_by " 
      + " and sub_sub_orderer_name = cust_name)" + 
      ")  < 500 ";
    String query2 = "Select * from Customers where " 
      +       " ( select sum(order_amount) from orders where ordered_by = cust_id and " +
              "orderer_name = cust_name  and " +
              " oid >  ( select sum(sub_order_amount) from suborders where " +
              " sub_orderer_name = orderer_name  and sub_ordered_by = cust_id  and " +
              "sub_oid  >  ( select sum(sub_sub_order_amount) from sub_suborders where " +
              " sub_sub_orderer_name = orderer_name  and sub_sub_ordered_by = sub_ordered_by ) )" +
              ") > 100" ;
    
    String queries[] = new String[] {query1,query2};
    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void subQueryInfoObjectFromOptmizedParsedTree(
                List<SubQueryInfo> qInfoList, GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
              assertEquals(qInfoList.size(), 3);
              for(SubQueryInfo sqi:qInfoList) {
                assertNull(sqi.getWhereClause() .isEquiJoinColocationCriteriaFullfilled(null));
              }
              ColocatedQueriesTest.this.callbackInvoked = true;            

            }
          });

       for(String query : queries) { 
         s.executeQuery(query);
         assertTrue(this.callbackInvoked);
         this.callbackInvoked = false;
       }
       
    }
    finally {
      s.execute("drop table sub_suborders");
      s.execute("drop table suborders");
      s.execute("drop table orders");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  }
  public void testUnsupportedColocatedSubqueries_1() throws Exception
  {   
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table Customers (cust_id int primary key , "
        + "cust_name varchar(1024) not null, address varchar(1024) )";
    String table2 = "create table Orders (oid int  primary key, "
        + "order_type varchar(1024) not null, order_amount int,  ordered_by int, "
        + "orderer_name varchar(1024) )";
    s.execute(table1 + "partition by column (cust_id)");
    s.execute(table2 + "partition by column ( ordered_by ) ");
    GemFireXDQueryObserver old = null;
    try {       
      String query = "Select * from Customers where ( select sum(order_amount) from orders where "
          + "ordered_by = cust_id ) > 1000";
      try {
        s.executeQuery(query);
        fail("Should have got an exception");
      } catch (SQLException snse) {
        assertEquals(snse.getSQLState(), "0A000");
        assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.9");
      }
    }
    finally {
      s.execute("drop table orders");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  }
  
  public void testBug41374SupportedSubquery() throws Exception
  {   
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table TESTTABLE1 (ID1 int not null, DESCRIPTION1 varchar(1024) not null," +
    " ADDRESS1 varchar(1024) not null, primary key (ID1)) PARTITION BY COLUMN ( ID1 )";
    String table2 = "create table TESTTABLE2 (ID2 int not null, DESCRIPTION2 varchar(1024) not null, " +
    "ADDRESS2 varchar(1024) not null, primary key (ID2)) partition by column( ID2) colocate with (TESTTABLE1)";
    s.execute(table1);
    s.execute(table2);
    GemFireXDQueryObserver old = null;
    try {       
      String query = "select ID1, DESCRIPTION1 from TESTTABLE1 where ID1 IN (" +
      		" Select ID2 from Testtable2 where description2 = description1 )";       
        s.executeQuery(query);  
       
    }
    finally {
      s.execute("drop table testtable2");
      s.execute("drop table testtable1");
     
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  }
  
  public void testUnsupportedColocatedSubqueries_2() throws Exception
  {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table Customers (cust_id int primary key , "
        + "cust_name varchar(1024) not null, address varchar(1024) )";
    String table2 = "create table Orders (oid int  primary key, "
        + "order_type varchar(1024) not null, order_amount int,  ordered_by int, "
        + "orderer_name varchar(1024) )";
    s.execute(table1 + "partition by column (cust_id, cust_name)");
    s.execute(table2 + "partition by column ( ordered_by,orderer_name ) ");
    
    try {
      String query = "Select * from Customers where ( select sum(order_amount) from orders where "
          + "ordered_by = cust_id ) > 1000";
      s.executeQuery(query);
      fail("Should have got an Exception");
    } catch (SQLException snse) {
      assertEquals(snse.getSQLState(), "0A000");
      assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.9");

      //assertTrue(this.callbackInvoked);
    }
    finally {
      s.execute("drop table orders");
      s.execute("drop table customers");    
    }
  }
  
  /**
   * Unsupported because outer table is replicated while inner is a PR
   * @throws Exception
   */
  public void testUnsupportedColocatedSubqueries_3() throws Exception
  {

    String query = " select sec_id, symbol, price, tid from securities s where" +
    " price >(select (Avg(availQty/qty)) from portfolio f where sec_id = f.sid and f.tid =? and " +
    "qty <> 0) and tid =?";

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table customers (cid int not null, cust_name varchar(100), " +
                " addr varchar(100), tid int, primary key (cid))"); 

    s.execute("create table securities (sec_id int not null, symbol varchar(10) not null," +
                " exchange varchar(10) not null, price decimal(30,20),tid int, constraint sec_pk primary key (sec_id), " +
                "constraint sec_uq unique (symbol, exchange), " +
                "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))" +
                "  replicate");
        
    
    s.execute("create table portfolio (cid int not null, sid int not null," +
        " qty int not null, availQty int not null, tid int, constraint portf_pk primary key (cid, sid)," +
    " constraint cust_fk foreign key (cid) references customers (cid) on delete restrict, " +
    "constraint sec_fk foreign key (sid) references securities (sec_id), constraint qty_ck check (qty>=0), " +
    "constraint avail_ch check (availQty>=0 and availQty<=qty)) " +
    " partition by column (cid) colocate with (customers)");
    
    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, status varchar(10) default 'open', tid int," +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid) on " +
                "delete restrict, constraint status_ch check (status in ('cancelled', 'open', 'filled'))) " +
                " partition by column (cid) colocate with (customers)");

    try {      
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setInt(1, 10);
      ps.setInt(2, 10);
      ResultSet rs = ps.executeQuery();
      rs.close();
      s.executeQuery(query);
      fail("Should have got an Exception");
    } catch (SQLException snse) {
      assertEquals(snse.getSQLState(), "0A000");
      assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.14");

      //assertTrue(this.callbackInvoked);
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");
    }

  
  }

  public void testBug42413() throws Exception {    
      
     SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table securities (sec_id int not null, symbol varchar(10) not null," +
    		" tid int, constraint sec_pk primary key (sec_id))  replicate";
    String table2 ="create table portfolio (cid int not null, sid int not null, qty int not null," +
    		" tid int, constraint portf_pk primary key (cid, sid), constraint sec_fk foreign key (sid)" +
    		" references securities (sec_id)) partition by column (cid) ";
    s.execute(table1 );
    s.execute(table2);
    
    try {    
      String query = " select sec_id, tid from securities s where " +
      	"tid >(select (Avg(qty)) from portfolio f where sec_id = f.sid and qty <> 0)";
      conn.prepareStatement(query);      
      fail("Should have got an Exception");
    } catch (SQLException snse) {
      assertEquals(snse.getSQLState(), "0A000");
      assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.14");

      //assertTrue(this.callbackInvoked);
    }
    finally {
      s.execute("drop table portfolio");
      s.execute("drop table securities"); 

    }  
       
       
  }

  public void testBug42416() throws Exception
  {

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table securities (sec_id int not null, "
        + "symbol varchar(10) not null, price decimal (30, 20), tid int, constraint sec_pk primary key (sec_id))"
        + "  replicate";

    String table2 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
        + "tid int, constraint portf_pk primary key (cid, sid), "
        + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict)"
        + " partition by column (cid) colocate with (customers)";

    String table3 = "create table customers (cid int not null, cust_name varchar(100), addr varchar(100),"
        + " tid int, primary key (cid))";

    s.execute(table3);
    s.execute(table2);
    s.execute(table1);

    GemFireXDQueryObserver old = null;
    String query = "select sec_id, symbol, s.tid, cid, cust_name, c.tid from securities s, "
        + "customers c where c.cid = (select f.cid from portfolio f where c.cid = f.cid"
        + " group by f.cid having count(*) >2) and sec_id in "
        + "(select sid from portfolio f where qty > 399)";
    try {
      conn.prepareStatement(query);

      // fail("Should have got an Exception");
    }
    finally {
      s.execute("drop table securities");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }
  }
  
  public void testBug42416_1() throws Exception
  {

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
  

    String table2 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
        + "tid int, constraint portf_pk primary key (cid, sid), "
        + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict)"
        + " partition by column (cid) colocate with (customers)";

    String table3 = "create table customers (cid int not null, cust_name varchar(100), addr varchar(100),"
        + " tid int, primary key (cid))";

    s.execute(table3);
    s.execute(table2);

    GemFireXDQueryObserver old = null;

    String query = "select cust_name, c.tid from "
        + "customers c where c.tid IN (select sid from portfolio f where f.sid > 10)";
        
    try {
      conn.prepareStatement(query);

      // fail("Should have got an Exception");
    }
    finally {
    
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }

  }
  
  public void testBug42416_2() throws Exception
  {

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
  

    String table2 = "create table portfolio (cid int not null, sid int not null, constraint portf_pk primary key (cid, sid), "
        + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict)"
        + " partition by column (cid) colocate with (customers)";

    String table3 = "create table customers (cid int not null, cust_name varchar(100),"
        + " tid int, primary key (cid))";

    s.execute(table3);
    s.execute(table2);
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    s.executeUpdate( "insert into portfolio values (2,7)");
    s.executeUpdate("insert into portfolio values (3,8)");
    s.executeUpdate( "insert into portfolio values (5,12)");
    s.executeUpdate( "insert into portfolio values (6,13)");
    s.executeUpdate( "insert into portfolio values (7,2)");
    s.executeUpdate( "insert into portfolio values (8,3)");
    s.executeUpdate( "insert into portfolio values (12,18)");
    s.executeUpdate( "insert into portfolio values (13,19)");
    GemFireXDQueryObserver old = null;
    String query = "select cust_name, c.tid from "
        + "customers c where c.tid IN (select sid from portfolio f where f.sid > 0)";
        
    try {
      ResultSet rs = s.executeQuery(query);
      Set<Integer> expectedCust = new HashSet<Integer>();
      expectedCust.add(2);
      expectedCust.add(3);
      expectedCust.add(7);
      expectedCust.add(8);
      expectedCust.add(12);
      expectedCust.add(13);
      while(rs.next()) {
        String custName = rs.getString(1);
        int tid = rs.getInt(2);
        assertTrue(expectedCust.remove(tid));
        assertEquals("name_"+tid,custName);
        
      }
      assertTrue(expectedCust.isEmpty());

      // fail("Should have got an Exception");
    }
    finally {
    
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }

    }

  }
  
  public void testBug42402() throws Exception
  {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    String table2 = "create table portfolio (cid int not null, sid int not null, qty int not null,"
        + "tid int, constraint portf_pk primary key (cid, sid), "
        + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict)"
        + " partition by column (cid) ";

    String table3 = "create table customers (cid int not null, cust_name varchar(100), addr varchar(100),"
        + " tid int, primary key (cid)) partition by column (tid)";

    s.execute(table3);
    s.execute(table2);
    try {
      
      String query = "select * from customers where "
          + "cid IN (select cid from portfolio f where tid =? and sid >?  )";

      conn.prepareStatement(query);
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table portfolio");
      s.execute("drop table customers");    
    }
  }
  
  public void testBug42574_1() throws Exception
  {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    s
        .execute("create table customers (cid int not null, cust_name varchar(100), since date,"
            + " addr varchar(100), tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "exchange varchar(10) not null,price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");

    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");
    

    String query = "select sec_id, symbol, price, tid from securities s where price "
        + " >(select (Avg(availQty/qty)) from portfolio f where sec_id = f.sid "
        + " and qty <> 0 and (select cid from customers c where c.cid = f.cid) > ?)";

    try {

      conn.prepareStatement(query);
      fail("Should have got an Exception");
    } catch (SQLException snse) {
      assertEquals(snse.getSQLState(), "0A000");
      assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.14");
    }
    finally {
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");     
    
    }
  }
  
  public void testBug42574_2() throws Exception
  {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    s
        .execute("create table customers (cid int not null, cust_name varchar(100), since date,"
            + " addr varchar(100), tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "exchange varchar(10) not null,price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");

    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    String query = "select * from customers where tid=? and cid IN (select avg(cid) from " +
    		"portfolio where tid =? and sid >? group by sid )";

    try {
      conn.prepareStatement(query);
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  }
  
  public void testBug42421() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100), since date,"
            + " addr varchar(100), tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "exchange varchar(10) not null,price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
    		" cid int, sid int, qty int, tid int, " +
    		" constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
    		" on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
    String query = "select f.cid, f.sid, f.tid, s.oid, s.tid from portfolio f," +
    		" sellorders s where (f.cid = s.cid and f.tid=s.tid) and " +
    		"f.cid IN (select cid from customers c where tid = ? and since > ?)";

    try {
      conn.prepareStatement(query);
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  }
  
  public void testBug42588_1() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
   
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
     for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,53,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,54,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
              }
            }
          });
     s.executeQuery("select sec_id, symbol, price, tid from securities s where price >" +
                "(select Sum(availQty/qty)/1 from portfolio f where sec_id = f.sid and f.tid =4 and" +
                " qty <> 0) and tid =4");
    
     fail("Should have got an Exception");
    } catch (SQLException snse) {
      assertEquals(snse.getSQLState(), "0A000");
      assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.14");

      //assertTrue(this.callbackInvoked);
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }  
  }
  public void testBug42588_2() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);   
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
     for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,53,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,54,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {
              }
            }
          });
     ResultSet rs= s.executeQuery("select Sum(availQty/qty)/1 from portfolio f where  f.tid =4 and" +
                " qty <> 0 ");
    
     while(rs.next()) {
       
     } 
       //assertTrue(rs.next());
       rs.close();
       
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }  
  }
  public void testBug42589() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
    String query = "select sec_id, symbol, price, tid from securities s where price >" +
                "(select Avg(availQty/qty) from portfolio f where sec_id = f.sid and f.tid =4 and" +
                " qty <> 0) and tid =4";
   
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,53,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,54,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
     s.executeQuery(query);
     fail("Should have got an Exception");
    } catch (SQLException snse) {
      assertEquals(snse.getSQLState(), "0A000");
      assertEquals(((DerbySQLException)snse).getMessageId(), "0A000.S.14");

      //assertTrue(this.callbackInvoked);
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  
  }
  public void testIncorrectQueryInfoHandlingFromSubqueryNode() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
   
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
    String query = "select * from sellorders s where exists " +
    		"(select * from portfolio f where f.cid = s.cid and f.sid < ? and tid =? and " +
    		"f.cid IN (select cid from customers c where tid <? ))";
   
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    
    
    s.executeUpdate( "insert into sellorders values (1,2,7,10,2)");
    s.executeUpdate("insert into sellorders values (2,3,8,11,3)");
    s.executeUpdate( "insert into sellorders values (3,5,12,12,4)");
    
    
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
     PreparedStatement ps = conn.prepareStatement(query);
     ps.setInt(1, 1);
     ps.setInt(2, 1);
     ps.setInt(3, 1);
     ResultSet rs= ps.executeQuery();
    
     while(rs.next()) {
       
     } 
       //assertTrue(rs.next());
       rs.close();
       
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  
  }
  
  public void testBug42697And42673() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
  
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
    String query1 = "select * from securities where sec_id IN" +
    " (select sid from portfolio f where cid = " +
    "     (select c.cid from customers c where c.tid > ? and c.cid = f.cid)" +
    " ) ";
    
    String query2 = "select * from securities where sec_id IN" +
    " (select sid from portfolio f where cid = " +
    "     (select c.cid from customers c where c.tid > 1 and c.cid = f.cid)" +
    " ) ";
   
    
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    
    
    s.executeUpdate( "insert into sellorders values (1,2,7,10,2)");
    s.executeUpdate("insert into sellorders values (2,3,8,11,3)");
    s.executeUpdate( "insert into sellorders values (3,5,12,12,4)");   
    
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
     
      PreparedStatement ps = conn.prepareStatement(query1);
      ps.setInt(1, 0);
      ResultSet rs = ps.executeQuery();    
     while(rs.next()) {       
     } 
      rs= ((EmbedStatement)s).executeQuery(query2);
     while(rs.next()) {       
     } 
     
     conn.close();
     
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s =TestUtil.getConnection().createStatement();
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  
  }
  
   
  

  
  public void testBug42659() throws Exception {
    Statement derbyStmt = null;
    try {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    System.getProperties().put(Optimizer.MAX_MEMORY_PER_TABLE, "0");
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    String table1 = "create table customers (cid int not null, cust_name varchar(100),"
      + "  tid int, primary key (cid))";
    String table2 = "create table securities (sec_id int not null, symbol varchar(10) not null, "
      + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  ";
    String table3 ="create table portfolio (cid int not null, sid int not null, qty int not null,"
      + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
      + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
      + " constraint sec_fk foreign key (sid) references securities (sec_id) )   ";
    String table4 = "create table sellorders (oid int not null constraint orders_pk primary key," +
    " cid int, sid int, qty int, tid int, " +
    " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
    " on delete restrict )";
    s .execute(table1);
    s.execute( table2+ " replicate");
    s .execute(table3   + " partition by column (cid) colocate with (customers)");

    s.execute( table4+"   partition by column (cid) colocate with (customers)"); 
    
    String query = "select * from securities trade where sec_id IN " +
                "(select sid from portfolio where cid >?)"; 
   
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
  
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    
    
    s.executeUpdate( "insert into sellorders values (1,2,7,10,2)");
    s.executeUpdate("insert into sellorders values (2,3,8,11,3)");
    s.executeUpdate( "insert into sellorders values (3,5,12,12,4)");
    
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    Connection derbyConn ;    
      String derbyDbUrl = "jdbc:derby:newDB;create=true;";
      if (TestUtil.currentUserName != null) {
        derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
            + TestUtil.currentUserPassword + ';');
      }      
    derbyConn = DriverManager.getConnection(derbyDbUrl);
    derbyStmt = derbyConn.createStatement();
    derbyStmt.execute(table1);
    derbyStmt.execute(table2);
    derbyStmt.execute(table3);
    derbyStmt.execute(table4);
    for(int i = 0 ; i <15;++i) {
      derbyStmt.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      derbyStmt.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    } 
    derbyStmt.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    derbyStmt.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    derbyStmt.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    derbyStmt.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
    derbyStmt.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
    derbyStmt.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    derbyStmt.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    derbyStmt.executeUpdate( "insert into portfolio values (13,19,17,57,9)");   
    
    derbyStmt.executeUpdate( "insert into sellorders values (1,2,7,10,2)");
    derbyStmt.executeUpdate("insert into sellorders values (2,3,8,11,3)");
    derbyStmt.executeUpdate( "insert into sellorders values (3,5,12,12,4)");
    
    
    
    
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
     //ResultSet rs= ((EmbedStatement)s).executeQueryByPassQueryInfo(query, null,false, true);
    // ResultSet rs= ((EmbedStatement)s).executeQuery(query);
     // PreparedStatement ps = ((EmbedConnection)conn).prepareStatementByPassQueryInfo(-2, query,false,false);
      PreparedStatement ps = ((EmbedConnection)conn).prepareStatement(query);
      ps.setInt(1, 0);
      PreparedStatement derbyPrep = derbyConn.prepareStatement(query);
      derbyPrep.setInt(1,0);
      validateResults(derbyPrep, ps, query, false);
      conn.close();

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s =TestUtil.getConnection().createStatement();
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");   
      
    }
    }finally {
      if( derbyStmt != null) {
        derbyStmt.execute("drop table sellorders");
        derbyStmt.execute("drop table portfolio");
        derbyStmt.execute("drop table customers");
        derbyStmt.execute("drop table securities");
      }
      System.clearProperty(Optimizer.MAX_MEMORY_PER_TABLE);
    }
  
  }
  
  
  public void _testSomeBug() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
  
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
    /*String query = "select sec_id, symbol, s.tid, cid, cust_name, c.tid from securities s, " +
                "customers c where c.cid = (select f.cid from portfolio f where c.cid = f.cid " +
                "group by f.cid having count(*) >0) and sec_id in" +
                " (select sid from portfolio f where availQty > ?) ";*/
    String query = "select * from securities trade where sec_id IN " +
                "(select sid from portfolio where cid >?)"; 
   
    /*String query = "select * from securities where sec_id IN" +
    " (select sid from portfolio f where cid = " +
    "     (select c.cid from customers c where c.tid > 0 and c.cid = f.cid)" +
    " ) ";*/
    
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    
    
    s.executeUpdate( "insert into sellorders values (1,2,7,10,2)");
    s.executeUpdate("insert into sellorders values (2,3,8,11,3)");
    s.executeUpdate( "insert into sellorders values (3,5,12,12,4)");   
    
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
     //ResultSet rs= ((EmbedStatement)s).executeQueryByPassQueryInfo(query, null,false, true);
    // ResultSet rs= ((EmbedStatement)s).executeQuery(query);
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setInt(1, 0);
      ResultSet rs = ps.executeQuery();
     while(rs.next()) {
       //System.out.println("asif");
     } 
       //assertTrue(rs.next());
       conn.close();
       
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s =TestUtil.getConnection().createStatement();
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  
  }
  
  public void testComboCorrelatedAndNonCorrelatedSubquery() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
  
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
    String query = "  select sec_id, symbol, s.tid, cid, cust_name, c.tid from securities s," +
    		" customers c where c.cid = (select f.cid from portfolio f where" +
    		" c.cid = f.cid and f.tid  > 1 group by f.cid having count(*) >2) and " +
    		"sec_id in (select sid from portfolio f where availQty > 1)";
   
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 0 ; i <40;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    
    
    s.executeUpdate( "insert into sellorders values (1,2,7,10,2)");
    s.executeUpdate("insert into sellorders values (2,3,8,11,3)");
    s.executeUpdate( "insert into sellorders values (3,5,12,12,4)");   
    
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
     ResultSet rs= s.executeQuery(query);
    
     while(rs.next()) {
       //System.out.println("asif");
     } 
       //assertTrue(rs.next());
       conn.close();
       
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s =TestUtil.getConnection().createStatement();
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  
  }
  
  public void testBug42777() throws Exception {

    String query = " select * from customers c where tid > ? and c.cid IN (select cid from "
        + "portfolio where tid > ? and sid >? )";

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid)) partition by list (tid) (VALUES (0, 1, 3, 4, 5), "
            + "VALUES (6, 10, 11), VALUES (12, 15, 17))");

    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by range (sid) ( VALUES BETWEEN 0 AND 299, VALUES BETWEEN 409 AND 1102,"
            + " VALUES BETWEEN 1193 AND 1251, VALUES BETWEEN 1291 AND 1677, VALUES BETWEEN 1678 AND 10000) ");
    for (int i = 0; i < 15; ++i) {
      s.executeUpdate("insert into customers values (" + i + "," + "'name_" + i
          + "'," + i + ")");
    }
    for (int i = 0; i < 40; ++i) {
      s.executeUpdate("insert into securities values (" + i + "," + "'sec_" + i
          + "'," + i * 10 + "," + i + ")");
    }
    s.executeUpdate("insert into portfolio values (2,7,10,50,2)");
    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
    s.executeUpdate("insert into portfolio values (5,12,12,52,4)");
    s.executeUpdate("insert into portfolio values (6,13,13,52,5)");
    s.executeUpdate("insert into portfolio values (7,2,14,70,6)");
    s.executeUpdate("insert into portfolio values (8,3,15,55,7)");
    s.executeUpdate("insert into portfolio values (12,18,16,56,8)");
    s.executeUpdate("insert into portfolio values (13,19,17,57,9)");

    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setInt(1, 1);
      ps.setInt(2, 1);
      ps.setInt(3, 1);
      ResultSet rs = ps.executeQuery();

      while (rs.next()) {
        //System.out.println("asif");
      }
      // assertTrue(rs.next());
      rs.close();

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");
    }

  }
  
  public void testBug42699() throws Exception {

    String query = " select * from customers c where EXISTS (select * from portfolio f " +
   "where c.cid = f.cid and tid =?) and NOT EXISTS (select * from sellorders s where c.cid = s.cid" +
   " and status IN ('open','filled'))";

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table customers (cid int not null, cust_name varchar(100), " +
                " addr varchar(100), tid int, primary key (cid))"); 

    s.execute("create table securities (sec_id int not null, symbol varchar(10) not null," +
                " exchange varchar(10) not null, tid int, constraint sec_pk primary key (sec_id), " +
                "constraint sec_uq unique (symbol, exchange), " +
                "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))" +
                "  replicate");
        
    
    s.execute("create table portfolio (cid int not null, sid int not null," +
        " qty int not null, availQty int not null, tid int, constraint portf_pk primary key (cid, sid)," +
    " constraint cust_fk foreign key (cid) references customers (cid) on delete restrict, " +
    "constraint sec_fk foreign key (sid) references securities (sec_id), constraint qty_ck check (qty>=0), " +
    "constraint avail_ch check (availQty>=0 and availQty<=qty)) " +
    " partition by column (cid) colocate with (customers)");
    
    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, status varchar(10) default 'open', tid int," +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid) on " +
                "delete restrict, constraint status_ch check (status in ('cancelled', 'open', 'filled'))) " +
                " partition by column (cid) colocate with (customers)");

    
     PreparedStatement psCustIn = conn.prepareStatement("insert into customers values (?,?,?,?)");
     PreparedStatement psSecIn = conn.prepareStatement("insert into securities values (?,?,?,?)");
     PreparedStatement psPortIn = conn.prepareStatement("insert into portfolio values (?,?,?,?,?)");
     PreparedStatement psSellIn = conn.prepareStatement("insert into sellorders values (?,?,?,?,?,?)");
     for(int i = 1 ; i<3;++i) {
       psCustIn.setInt(1,i);
       psCustIn.setString(2,"name_"+i);
       psCustIn.setString(3,"addr_"+i);
       psCustIn.setInt(4,10);
       psCustIn.executeUpdate();
     }
     
     for(int i = 1 ; i<101;++i) {
       psSecIn.setInt(1,i);
       psSecIn.setString(2,"rzk"+i);
       psSecIn.setString(3,"fse");
       psSecIn.setInt(4,10);
       psSecIn.executeUpdate();
     }
     
     for(int i = 1 ; i<101;++i) {
       psPortIn.setInt(1,1);
       psPortIn.setInt(2,i);
       psPortIn.setInt(3,100);
       psPortIn.setInt(4,100);
       psPortIn.setInt(5,10);       
       psPortIn.executeUpdate();
     }
     
     for(int i = 1 ; i<2;++i) {
       psSellIn.setInt(1,i);
       psSellIn.setInt(2,i);
       psSellIn.setInt(3,i);
       psSellIn.setInt(4,100);
       psSellIn.setString(5,"open");
       psSellIn.setInt(6,10);       
       psSellIn.executeUpdate();
     }     

    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public double overrideDerbyOptimizerIndexUsageCostForHash1IndexScan(
                OpenMemIndex memIndex, double optimzerEvalutatedCost) {
              return optimzerEvalutatedCost;
            }

            @Override
            public double overrideDerbyOptimizerCostForMemHeapScan(
                GemFireContainer gfContainer, double optimzerEvalutatedCost) {
              return optimzerEvalutatedCost;
            }

            @Override
            public double overrideDerbyOptimizerIndexUsageCostForSortedIndexScan(
                OpenMemIndex memIndex, double optimzerEvalutatedCost) {
                return 1;
            }
          });
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setInt(1, 10);
      
      ResultSet rs = ps.executeQuery();
      int count =0;
      while(rs.next()) {
        ++count;
      }

      assertEquals(0,count);
      // assertTrue(rs.next());
      rs.close();

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");
    }

  }
  
  
  public void testBug46969_42273() throws Exception {
    final String subqueryInFocus = "select f.cid from trade.portfolio f " +
        "where c.cid = f.cid group by f.cid having count(*) >3";
   String query = "select sec_id, symbol, s.tid, cid, cust_name, c.tid from trade.securities s, " +
    	"trade.customers c where c.cid = ("+subqueryInFocus +") and sec_id in" +
    	" (select sid from trade.portfolio f where availQty > 399 and availQty < 517) " +
    	"and s.symbol > 'k' and s.symbol <'s'";
    
    //String query = "select sec_id, symbol, s.tid, cid, cust_name, c.tid from trade.securities s, " +
      //  "trade.customers c ";

    
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING,"true");
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table trade.customers (cid int not null, cust_name varchar(100), " +
                " addr varchar(100), tid int, primary key (cid))"); 

    s.execute("create table trade.securities (sec_id int not null, symbol varchar(10) not null," +
                " exchange varchar(10) not null, tid int, constraint sec_pk primary key (sec_id), " +
                "constraint sec_uq unique (symbol, exchange), " +
                "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))" +
                "  replicate");
        
    
    s.execute("create table trade.portfolio (cid int not null, sid int not null," +
        " qty int not null, availQty int not null, tid int, constraint portf_pk primary key (cid, sid)," +
    " constraint cust_fk foreign key (cid) references trade.customers (cid) on delete restrict, " +
    "constraint sec_fk foreign key (sid) references trade.securities (sec_id), constraint qty_ck check (qty>=0), " +
    "constraint avail_ch check (availQty>=0 and availQty<=qty)) " +
    " partition by column (cid) colocate with (trade.customers)");
    
    s.execute("create table trade.sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, status varchar(10) default 'open', tid int," +
                " constraint portf_fk foreign key (cid, sid) references trade.portfolio (cid, sid) on " +
                "delete restrict, constraint status_ch check (status in ('cancelled', 'open', 'filled'))) " +
                " partition by column (cid) colocate with (trade.customers)");

    
     PreparedStatement psCustIn = conn.prepareStatement("insert into trade.customers values (?,?,?,?)");
     PreparedStatement psSecIn = conn.prepareStatement("insert into trade.securities values (?,?,?,?)");
     PreparedStatement psPortIn = conn.prepareStatement("insert into trade.portfolio values (?,?,?,?,?)");
     PreparedStatement psSellIn = conn.prepareStatement("insert into trade.sellorders values (?,?,?,?,?,?)");
     for(int i = 1 ; i<25;++i) {
       psCustIn.setInt(1,i);
       psCustIn.setString(2,"name_"+i);
       psCustIn.setString(3,"addr_"+i);
       psCustIn.setInt(4,10);
       psCustIn.executeUpdate();
     }
     
     for(int i = 1 ; i<101;++i) {
       psSecIn.setInt(1,i);
       psSecIn.setString(2,"rzk"+i);
       psSecIn.setString(3,"fse");
       psSecIn.setInt(4,10);
       psSecIn.executeUpdate();
     }
     
     for(int i = 1 ; i<25;++i) {
       for(int j = 1 ; j < 101; ++j) {
        psPortIn.setInt(1,i);
       psPortIn.setInt(2,j);
       psPortIn.setInt(3,1000);
       psPortIn.setInt(4,400);
       psPortIn.setInt(5,10);       
       psPortIn.executeUpdate();
       }
     }
     
     for(int i = 1 ; i<2;++i) {
       psSellIn.setInt(1,i);
       psSellIn.setInt(2,i);
       psSellIn.setInt(3,i);
       psSellIn.setInt(4,100);
       psSellIn.setString(5,"open");
       psSellIn.setInt(6,10);       
       psSellIn.executeUpdate();
     }     

    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void afterOptimizedParsedTree(String query, StatementNode qt,
                LanguageConnectionContext lcc) {
              callbackInvoked = true;
              //Get the PRN on top of the Customer table resultset.
              //That PRN's restriction list should contain the subquery predicate
              final boolean[] asserted = new boolean[] {false};
              Visitor visitor = new VisitorAdaptor() {
                private boolean stop = false;
                @Override
                public Visitable visit(Visitable node) throws StandardException {
                  if(node instanceof ProjectRestrictNode) {
                    ResultSetNode child = ((ProjectRestrictNode)node).getChildResult();
                    boolean foundPRN = false;
                    if(child instanceof FromBaseTable) {
                      if(((FromBaseTable)child).getOrigTableName().equals("TRADE", "CUSTOMERS")) {
                        foundPRN = true;
                      }
                    }else if(child instanceof IndexToBaseRowNode) {
                      IndexToBaseRowNode ibrn  = (IndexToBaseRowNode)child;
                      FromBaseTable fbt = ibrn.getSource();
                      TableName tn = fbt.getActualTableName();
                      if(tn.getSchemaName().toLowerCase().equals("trade") && tn.getTableName().toLowerCase().equals("customers")) {
                        foundPRN = true;
                      }
                    }
                    if(foundPRN) {
                      this.stop = true;
                      asserted[0] = true;
                      final boolean[] assertedInner = new boolean[] {false};
                      Visitor subqueryChecker = new VisitorAdaptor() {
                        private boolean stop = false;
                        
                        @Override
                        public Visitable visit(Visitable node) throws StandardException {
                          if(node instanceof SubqueryNode) {
                            assertedInner[0] = true;
                            stop = true;
                            SubqueryNode sqn = (SubqueryNode)node;
                            if(!sqn.getSubqueryString().equals(subqueryInFocus)) {
                              Assert.fail("Subquery string found at PRN is  = " + sqn.getSubqueryString() + "; expected is " + subqueryInFocus);
                            }else {
                              //all ok. disable the adapter
                              GemFireXDQueryObserverHolder.clearInstance();
                            }
                          }
                          return node;
                        }
                        @Override
                        public boolean stopTraversal() {
                          return stop;
                        }
                      };
                      ((ProjectRestrictNode)node).restrictionList.accept(subqueryChecker);
                      assertTrue(assertedInner[0]);
                      
                    }
                  }
                  return node;
                }

                /**
                 * {@inheritDoc}
                 */
                @Override
                public boolean stopTraversal() {
                  return stop;
                }
              };
              try {
                qt.accept(visitor);
                assertTrue(asserted[0]);
              } catch (StandardException e) {
                 throw new GemFireXDRuntimeException(e);
              }
            }
          });
     
      ResultSet rs = s.executeQuery(query);
      
      
      int count =0;
      while(rs.next()) {
        ++count;
      }
      assertTrue(count > 0);    
      assertTrue(this.callbackInvoked);
      /*
      rs.close();
      */

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table trade.sellorders");
      s.execute("drop table trade.portfolio");
      s.execute("drop table trade.customers");
      s.execute("drop table trade.securities");
    }

  }
  public void testCastNodeNehaviourWithDistinctAggregate() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);    

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + "partition by column (cid) colocate with (customers)");

    s.execute("create table sellorders (oid int not null constraint orders_pk primary key," +
                " cid int, sid int, qty int, tid int, " +
                " constraint portf_fk foreign key (cid, sid) references portfolio (cid, sid)" +
                " on delete restrict )   partition by column (cid) colocate with (customers)"); 
    
    //String query = "select AVG(cid) *2 from customers";
    String query1 = "Select avg( distinct sec_id) as avg_distinct_price from securities" +
    		" where tid = ? Or tid = ? or tid = ?";
    String query2 = "Select avg( distinct cid) as avg_distinct_price from customers where" +
    		" tid =? or tid = ? or tid = ?" ;
   
    for(int i = 0 ; i <15;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 1 ; i <8;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
//    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
//    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
//    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
//    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
//    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
//    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
//    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
//    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
      PreparedStatement ps = conn.prepareStatement(query1);
      ps.setInt(1, 1);
      ps.setInt(2, 5);
      ps.setInt(3, 7);
      ResultSet rs= ((EmbedPreparedStatement)ps).executeQuery();      
      assertTrue(rs.next());
      assertEquals(rs.getObject(1),new Long(4));
      assertFalse(rs.next());
      rs.close();    
      
      ps = conn.prepareStatement(query2);
      ps.setInt(1, 1);
      ps.setInt(2, 5);
      ps.setInt(3, 7);           
      rs= ((EmbedPreparedStatement)ps).executeQuery();      
      assertTrue(rs.next());
      assertEquals(rs.getObject(1),new Long(4));
      assertFalse(rs.next());
      rs.close();    
      
//      ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(true);
//      PreparedStatement ps1 =      ((EmbedConnection)conn).prepareStatementByPassQueryInfo(-1, query, false, true);
//      ps1.setInt(1, 0);
//      ps1.setInt(2, 0);
//      rs= ((EmbedPreparedStatement)ps1).executeQuery();    
//      assertTrue(rs.next());      
//      assertEquals(rs.getObject(1),new Integer(4));
//      assertFalse(rs.next());
//      rs.close();   
      
      
       
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      s.execute("drop table sellorders");
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  }

  public void testBug42613() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);    

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
            + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + " replicate");

    
    //String query = "select AVG(cid) *2 from customers";
    String query1 = "select * from customers where tid > ? and cid IN " +
    		"(select avg(cid) from portfolio where tid > ? and sid < ? group by sid )";
  //  String query2 = "select avg(cid) from portfolio where tid > ? and sid < ? group by sid " ;
    
    
    ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(true);
    
    for(int i = 1 ; i <101;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 1 ; i <101;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    for(int i = 10 ; i <100;++i) {
      int sid = i / 10 ;
      s.executeUpdate("insert into portfolio values ("+i+","+ sid+","+i*10 +"," + i*100+","+i+")");
    }
//    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
//    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
//    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
//    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
//    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
//    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
//    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
//    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
      ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(true);
      PreparedStatement ps = ((EmbedConnection)conn).prepareStatementByPassQueryInfo(-1, query1, false, true,
          false, null, 0, 0); //conn.prepareStatement(query1);
      ps.setInt(1, 0);
      ps.setInt(2, 0);
      ps.setInt(3, 6);
      ResultSet rs= ((EmbedPreparedStatement)ps).executeQuery();      
      //  ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(true);
//        PreparedStatement ps1 =      ((EmbedConnection)conn).prepareStatementByPassQueryInfo(-1, query2, false, true);
//        ps1.setInt(1, 0);
//       // ps1.setInt(2, 0);
//        ps1.setInt(2, 6);
//        ResultSet rs= ((EmbedPreparedStatement)ps1).executeQuery();
        Set expected = new HashSet();
        for (int i = 1; i < 6; ++i) {
          expected.add(i*10+4);
        }
        while(rs.next()) {
          expected.remove(rs.getInt(1));
        }
        rs.close();
        assertTrue(expected.isEmpty());
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
    
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  }
  
  public void testViewColumnElimination() throws SQLException {
    
    final Connection conn = getConnection();
    final Statement stmt = conn.createStatement();

    stmt.execute("create table trade.customers (cid int not null, "
        + "cust_name varchar(100), since date, addr varchar(100), tid int, "
        + "primary key (cid))  partition by range (cid) "
        + "( VALUES BETWEEN 0 AND 999, VALUES BETWEEN 1000 AND 1999, "
        + "VALUES BETWEEN 2000 AND 2999, VALUES BETWEEN 3000 AND 3999, "
        + "VALUES BETWEEN 4000 AND 100000)");
    stmt.execute("create table trade.networth (cid int not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), "
        + "loanlimit int, availloan decimal (30, 20),  tid int, "
        + "constraint netw_pk primary key (cid), constraint cust_newt_fk "
        + "foreign key (cid) references trade.customers (cid) on delete "
        + "restrict, constraint cash_ch check (cash>=0), constraint sec_ch check "
        + "(securities >=0), constraint availloan_ck check (loanlimit>=availloan "
        + "and availloan >=0))  partition by range (cid) "
        + "( VALUES BETWEEN 0 AND 999, VALUES BETWEEN 1000 AND 1999, "
        + "VALUES BETWEEN 2000 AND 2999, VALUES BETWEEN 3000 AND 3999, "
        + "VALUES BETWEEN 4000 AND 100000) colocate with (trade.customers)");
    stmt.execute("create table emp.employees (eid int not null constraint "
        + "employees_pk primary key, emp_name varchar(100), since date, "
        + "addr varchar(100), ssn varchar(9))  replicate");
    stmt.execute("create table trade.trades (tid int, cid int, eid int, "
        + "tradedate date, primary Key (tid), foreign key (cid) "
        + "references trade.customers (cid), constraint emp_fk "
        + "foreign key (eid) references emp.employees (eid))  "
        + "partition by range (cid) ( VALUES BETWEEN 0 AND 999, "
        + "VALUES BETWEEN 1000 AND 1999, VALUES BETWEEN 2000 AND 2999, "
        + "VALUES BETWEEN 3000 AND 3999, VALUES BETWEEN 4000 AND 100000) "
        + "colocate with (trade.customers)");
    // view in #44619
    stmt.execute("create view trade.cust_count_since2010_vw (since_date, "
        + "cust_count) as select since, count(*) from trade.customers "
        + "group by since having since > '2010-01-01'");
    stmt.execute("create view trade.cust_tradeCount_with5KNetworth_vw "
        + "(cid, name, cash, trade_count) as select t1.cid, t1.cust_name, "
        + "t2.cash, count(t3.tradedate) from trade.customers t1, "
        + "trade.networth t2, trade.trades t3 where t1.cid = t2.cid and "
        + "t2.cid = t3.cid group by t1.cid, t1.cust_name, t2.cash "
        + "having t2.cash > 5000");


    // insert some data
    for (int id = 1; id <= 20; id++) {
      int year = (id / 2) + 2;
      stmt.execute("insert into trade.customers (cid, cust_name, since) "
          + "values (" + id + ", 'n_" + id + "', '20"
          + (year < 10 ? ("0" + year) : year) + "-01-01')");
      stmt.execute("insert into trade.networth (cid, cash, loanlimit) values ("
          + id + ", " + (((id % 4) + 1) * 2000) + ", " + (id * 1000) + ")");
      stmt.execute("insert into emp.employees (eid, emp_name) values (" + id
          + ", 'n_" + id + "')");
      stmt.execute("insert into trade.trades (tid, cid, eid, tradedate) "
          + "values (" + (id * 2) + ", " + id + ", " + id + ", '2010-01-01')");
    }

    // for #44619
    try {
      ResultSet rs = stmt
          .executeQuery("select * from trade.cust_count_since2010_vw "
              + "where cust_count = 2");
      fail("Test should fail with feature not supported exception");
    } catch (SQLException sqle) {
      assertEquals("0A000", sqle.getSQLState());
    }
  }
  
  public void testHarnessTestBugOnSubqueryStatementMatching() throws Exception {
    //System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING,"true");
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    String derbyDbUrl= null;
    Connection derbyConn = null;;  
    Statement derbyStmt = null;
    Statement s = null;
    try {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    System.getProperties().put(Optimizer.MAX_MEMORY_PER_TABLE, "0");
    Connection conn = TestUtil.getConnection();
    s = conn.createStatement();
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      
       derbyDbUrl = "jdbc:derby:newDB;create=true;";
      if (TestUtil.currentUserName != null) {
        derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
            + TestUtil.currentUserPassword + ';');
      }      
    derbyConn = DriverManager.getConnection(derbyDbUrl);
    derbyStmt = derbyConn.createStatement();
    
    
    executeOnDerbyAndSqlFire("create table outer1 (c1 int, c2 int, c3 int)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create table outer2 (c1 int, c2 int, c3 int)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create table noidx (c1 int)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create table idx1 (c1 int primary key)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create unique index idx1_1 on idx1(c1)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create table idx2 (c1 int, c2 int primary key)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create unique index idx2_1 on idx2(c1, c2)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create table nonunique_idx1 (c1 int)",derbyStmt,s);
    executeOnDerbyAndSqlFire("create index nonunique_idx1_1 on nonunique_idx1(c1)",derbyStmt,s);
    
    executeOnDerbyAndSqlFire("insert into outer1 values (1, 2, 3)",derbyStmt,s);
    executeOnDerbyAndSqlFire("insert into outer1 values (4, 5, 6)",derbyStmt,s);
    executeOnDerbyAndSqlFire("insert into outer2 values (1, 2, 3)",derbyStmt,s);
    executeOnDerbyAndSqlFire("insert into outer2 values (4, 5, 6)",derbyStmt,s);
    executeOnDerbyAndSqlFire("insert into noidx values 1, 1",derbyStmt,s);
    executeOnDerbyAndSqlFire("insert into idx1 values 1, 2",derbyStmt,s);
    executeOnDerbyAndSqlFire("insert into idx2 values (1, 1), (1, 2)",derbyStmt,s);
    executeOnDerbyAndSqlFire("insert into nonunique_idx1 values 1, 1",derbyStmt,s);
    
     
    //queryOnDerbyAndSqlFire("select * from outer1 o where c1 <= (select c1 from idx1 i group by c1)", derbyStmt, s);
    //queryOnDerbyAndSqlFire("select * from outer1 o where c1 + 0 = 1 or c1 in (select c1 from idx1 i where i.c1 = 0)", derbyStmt, s);
  
    
    queryOnDerbyAndSqlFire("select * from outer1 o where o.c1 in (select c1 from idx1)", derbyStmt, s);    
  
    queryOnDerbyAndSqlFire("select * from outer1 o where o.c1 = ANY (select c1 from idx1)", derbyStmt, s);
    queryOnDerbyAndSqlFire("select * from outer1 o where exists (select * from idx1 where idx1.c1 = 1 + 0)", derbyStmt, s);
    
    
    derbyConn.close();
    conn.close();  
        
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
    
        
       
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
     
    }
    }finally {
      s =TestUtil.getConnection().createStatement();
      Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        
      derbyDbUrl = "jdbc:derby:newDB;create=true;";
        if (TestUtil.currentUserName != null) {
          derbyDbUrl += ("user=" + TestUtil.currentUserName + ";password="
              + TestUtil.currentUserPassword + ';');
        }      
      derbyConn = DriverManager.getConnection(derbyDbUrl);
      derbyStmt = derbyConn.createStatement();
         
      executeOnDerbyAndSqlFire("drop table outer1", derbyStmt, s);
      executeOnDerbyAndSqlFire("drop table outer2", derbyStmt, s);
      executeOnDerbyAndSqlFire("drop table noidx", derbyStmt, s);
      executeOnDerbyAndSqlFire("drop table idx1", derbyStmt, s);
      executeOnDerbyAndSqlFire("drop table idx2", derbyStmt, s); 
      derbyConn.commit();
      System.clearProperty(Optimizer.MAX_MEMORY_PER_TABLE);
    }
  
  }
  
  
  
  private void executeOnDerbyAndSqlFire(String stmt, Statement derby, Statement gemfirexd) throws SQLException {
    try {
      derby.execute(stmt);
    }catch(Exception e) {
      
    }
    try {
    gemfirexd.execute(stmt);
    }catch(Exception e) {
      
    }
  }

  private void queryOnDerbyAndSqlFire(String query, Statement derby,
      Statement gemfirexd) throws Exception {
    validateResults(derby, gemfirexd, query, false);
  }

  public void testUseCase3SampleQuery() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    //System.setProperty("gemfirexd.optimizer.trace", "true");
    SanityManager.DEBUG_SET("PrintRCList");
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    props.setProperty("table-default-partitioned", "false");
    props.setProperty("server-groups", "dbsync");
    props.setProperty("persist-dd", "true");
    Connection conn = getConnection(props);

    // create the schema
    String useCase3Script = TestUtil.getResourcesDir()
        + "/lib/useCase3Data/schema.sql";
    GemFireXDUtils.executeSQLScripts(conn, new String[] { useCase3Script }, false,
        getLogger(), null, null, false);

    String useCase3DataScript = TestUtil.getResourcesDir()
    + "/lib/useCase3Data/importAll.sql";

    GemFireXDUtils.executeSQLScripts(conn, new String[] { useCase3DataScript }, false,
        getLogger(), "<path_prefix>", TestUtil.getResourcesDir(), false);

    // now the complex views on views + tables query
    final HashMap<String, String> equijoinCols = new HashMap<String, String>();
    GemFireXDQueryObserverAdapter observer = new GemFireXDQueryObserverAdapter() {
      
      IdentityHashMap i = new IdentityHashMap();
      int nesting = 0;
      
      @Override
      public void onGetNextRowCore(com.pivotal.gemfirexd.internal.iapi.sql.ResultSet resultSet) {
        
        if(resultSet.isDistributedResultSet()) {
          return;
        }
        
        if(resultSet instanceof JoinResultSet) {
          JoinResultSet jrs = ((JoinResultSet)resultSet);
          int rsNum = jrs.resultSetNumber();
          Activation act = resultSet.getActivation();
          assertTrue(act != null);
          ExecRow r = act.getCurrentRow(rsNum);
          if (r != null) {
            String message = jrs.resultSetNumber + "  " + jrs + " numColumns: " + r.nColumns() + " leftColumns ("
                + (jrs.leftResultSetColumnNames != null ? jrs.leftResultSetColumnNames.length : "0") + ") : "
                + Arrays.toString(jrs.leftResultSetColumnNames)
                + " rightColumns (" + ( jrs.rightResultSetColumnNames != null ? jrs.rightResultSetColumnNames.length : 0)
                + ") : " + Arrays.toString(jrs.rightResultSetColumnNames);
            if(!i.containsKey(jrs)) {
              //System.out.println(message);
              getLogger().info(message);
              i.put(jrs, message);
            }
            // need to fix the following resultSetNumbers where columns getting selected are still high.
//             assertTrue(message,
//             r.nColumns() < 100 || (jrs.resultSetNumber == 20 && r.nColumns()
//             == 1148) || (jrs.resultSetNumber == 21 && r.nColumns() == 575) ||
//             (jrs.resultSetNumber == 30 && r.nColumns() == 725) ||
//             (jrs.resultSetNumber == 31 && r.nColumns() == 547) );
            assertTrue(message,
                    r.nColumns() < 100 || (jrs.resultSetNumber == 21 && r.nColumns()
                    == 1148) || (jrs.resultSetNumber == 22 && r.nColumns() == 575) ||
                    (jrs.resultSetNumber == 31 && r.nColumns() == 725) ||
                    (jrs.resultSetNumber == 32 && r.nColumns() == 547) );
          }
        }
        
      }


      @Override
      public void updatingColocationCriteria(ComparisonQueryInfo cqi) {
        if (cqi.leftOperand instanceof ColumnQueryInfo
            && cqi.rightOperand instanceof ColumnQueryInfo) {
          // keep FACT1 on right since it will be repeated
          ColumnQueryInfo leftCQI = (ColumnQueryInfo)cqi.leftOperand;
          ColumnQueryInfo rightCQI = (ColumnQueryInfo)cqi.rightOperand;
          if ("TX_PL_USER_POSN_MAP".equals(leftCQI.getTableName())) {
            leftCQI = rightCQI;
            rightCQI = (ColumnQueryInfo)cqi.leftOperand;
          }
          equijoinCols.put(
              leftCQI.getTableName() + '.' + leftCQI.getActualColumnName(),
              rightCQI.getTableName() + '.' + rightCQI.getActualColumnName());
        }
      }
    };
    /*
    SanityManager.TRACE_ON("DumpOptimizedTree");    
    SanityManager.TRACE_ON("PrintRCList");    
    SanityManager.TRACE_ON("DumpClassFile");
    */

    GemFireXDQueryObserverHolder.setInstance(observer);
    String viewQuery = "SELECT VD_INSTRUMENT_SCD.CUSIP_SRC_CD a7a0a0,"
        + "TL_SOURCE_SYSTEM.SRC_SYS_CD a1a1a9a4,"
        + "VD_TRADER_SCD.FIRM_ACCT_ID a1a4a2a9,"
        + "VD_TRADER_SCD.CCY_ID a6a9a9,"
        + "FACT1.pl_posn_id a6a9a5,"
        + "VD_POSN_EXTENDED_KEY.posn_ext_key_id a1a5a5a7,"
        + "VD_TRADER_SCD.FIRM_ACCT_MNEM_CD a6a8a9,"
        + "VD_TRADER_SCD.CCY_CD a6a9a4,"
        + "VD_INSTRUMENT_SCD.CUSIP_CD a6a9a6,"
        + "VD_INSTRUMENT_SCD.CUSIP_DESC a7a0a1,"
        + "VD_TRADER_SCD.LE_MNEM_CD a6a7a3,"
        + "(NVL(FACT2.PTD_DIV_INCM_AMT,"
        + " 0.0)+nvl(VF_PL_ADJUSTMENT.PTD_DIV_INCM_AMT,"
        + " 0.0) ) a2a6a5,"
        + " FACT2.DEAL_STATUS,"
        + " ((CASE WHEN MONTH('2012-05-07') = 1 THEN 0 ELSE"
        + " NVL(FACT3.YTD_DIV_INCM_AMT, 0) END) + (NVL(FACT2.PTD_DIV_INCM_AMT,"
        + " 0.0) ) + (NVL(VF_PL_ADJUSTMENT.PTD_DIV_INCM_AMT,"
        + " 0.0) ) ) a6a1a8"
        + " FROM VD_TRADER_SCD,"
        + "VD_POSN_EXTENDED_KEY,"
        + "TX_PL_USER_POSN_MAP FACT1"
        + " LEFT OUTER JOIN VF_PL_POSITION_PTD FACT2 on FACT1.TM_ID = FACT2.TM_ID"
        + " AND FACT1.PL_POSN_ID = FACT2.PL_POSN_ID AND"
        + " FACT1.TM_ID = 20120330 left outer join VF_PL_POSITION_QTD_YTD FACT3"
        + " on FACT3.tm_id = 20120229 AND"
        + " FACT3.PL_POSN_ID = FACT1.PL_POSN_ID left outer join"
        + " VF_PL_ADJUSTMENT on FACT1.tm_id=VF_PL_ADJUSTMENT.tm_id AND"
        + " FACT1.PL_POSN_ID=VF_PL_ADJUSTMENT.PL_POSN_ID,"
        + "TL_SOURCE_SYSTEM,"
        + "VD_INSTRUMENT_SCD WHERE FACT1.tm_id = 20120330  AND"
        + " FACT1.FIRM_ACCT_ID = VD_TRADER_SCD.FIRM_ACCT_ID AND"
        + " FACT1.CCY_ID = VD_TRADER_SCD.CCY_ID AND"
        + " FACT1.SRC_SYS_ID = TL_SOURCE_SYSTEM.SRC_SYS_ID AND"
        + " FACT1.POSN_EXT_KEY_ID = VD_POSN_EXTENDED_KEY.POSN_EXT_KEY_ID AND"
        + " FACT1.INSM_ID = VD_INSTRUMENT_SCD.INSM_ID  AND"
        + " (VD_TRADER_SCD.SUB_MICRO_CD in ('34GI', '34G4', '34E2', '61AG')"
        + " OR VD_TRADER_SCD.FIRM_ACCT_MNEM_CD in "
        + "('LCFT') OR VD_TRADER_SCD.STGY_CD in ('65G', '63K', '60G', 'L20',"
        + " '60N', '60J')) AND"
        + " (VD_TRADER_SCD.FIRM_ACCT_MNEM_CD not in ('FZAR'))";
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(viewQuery);
    int rows = 0;
    while(rs.next()) {
      rows++;
    }
    assertEquals(11, rows);
    assertFalse(rs.next());

    GemFireXDQueryObserverHolder.clearInstance();
    // check columns against expected ones for equijoin colocation criteria
    assertEquals("TX_PL_USER_POSN_MAP.TM_ID",
        equijoinCols.remove("TF_PL_POSITION_PTD.TM_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.PL_POSN_ID",
        equijoinCols.remove("TF_PL_POSITION_PTD.PL_POSN_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.PL_POSN_ID",
        equijoinCols.remove("TF_PL_POSITION_YTD.PL_POSN_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.TM_ID",
        equijoinCols.remove("TF_PL_ADJ_REPORT.TM_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.PL_POSN_ID",
        equijoinCols.remove("TF_PL_ADJ_REPORT.PL_POSN_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.FIRM_ACCT_ID",
        equijoinCols.remove("TD_TRADER_SCD.FIRM_ACCT_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.CCY_ID",
        equijoinCols.remove("TD_TRADER_SCD.CCY_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.SRC_SYS_ID",
        equijoinCols.remove("TL_SOURCE_SYSTEM.SRC_SYS_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.POSN_EXT_KEY_ID",
        equijoinCols.remove("TD_POSN_EXTENDED_KEY.POSN_EXT_KEY_ID"));
    assertEquals("TX_PL_USER_POSN_MAP.INSM_ID",
        equijoinCols.remove("TD_INSTRUMENT_SCD.INSM_ID"));

    assertEquals(equijoinCols.toString(), 0, equijoinCols.size());

    String useCase3ScriptDrop = TestUtil.getResourcesDir()
          + "/lib/useCase3Data/schemaDrop.sql";
    
    GemFireXDUtils.executeSQLScripts(conn, new String[] { useCase3ScriptDrop }, false,
            getLogger(), null, null, false);
  }

  /** check the hongkong query that was causing NPE in left-outer join */
  public void testUseCase3SampleQuery2() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    Properties props = new Properties();
    props.setProperty("log-level", "config");
    props.setProperty("table-default-partitioned", "false");
    Connection conn = getConnection(props);

    // create the schema
    String useCase3Script = TestUtil.getResourcesDir()
        + "/lib/useCase3Schema.sql";
    GemFireXDUtils.executeSQLScripts(conn, new String[] { useCase3Script }, false,
        getLogger(), null, null, false);

    // now the complex views on views + tables query
    final HashMap<String, String> equijoinCols = new HashMap<String, String>();
    GemFireXDQueryObserverAdapter observer = new GemFireXDQueryObserverAdapter() {

      @Override
      public void updatingColocationCriteria(ComparisonQueryInfo cqi) {
        if (cqi.leftOperand instanceof ColumnQueryInfo
            && cqi.rightOperand instanceof ColumnQueryInfo) {
          // keep FACT1 on right since it will be repeated
          ColumnQueryInfo leftCQI = (ColumnQueryInfo)cqi.leftOperand;
          ColumnQueryInfo rightCQI = (ColumnQueryInfo)cqi.rightOperand;
          if ("TF_PL_POSITION_PTD".equals(leftCQI.getTableName())) {
            leftCQI = rightCQI;
            rightCQI = (ColumnQueryInfo)cqi.leftOperand;
          }
          equijoinCols.put(
              leftCQI.getTableName() + '.' + leftCQI.getActualColumnName(),
              rightCQI.getTableName() + '.' + rightCQI.getActualColumnName());
        }
      }
    };
    GemFireXDQueryObserverHolder.setInstance(observer);
    BufferedReader scriptReader = new BufferedReader(new FileReader(
        TestUtil.getResourcesDir()
            + "/lib/useCase3-HongKong_All_06Apr2012_HS-Extract-T_H.sql"));
    StringBuilder qry = new StringBuilder();
    String qryLine;
    while ((qryLine = scriptReader.readLine()) != null) {
      qry.append(qryLine).append('\n');
    }
    scriptReader.close();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(qry.toString());
    // no data so no results; only want to check the query though
    assertFalse(rs.next());

    GemFireXDQueryObserverHolder.clearInstance();
    // check columns against expected ones for equijoin colocation criteria
    assertEquals("TF_PL_POSITION_PTD.TM_ID",
        equijoinCols.remove("TF_PL_ADJ_REPORT.TM_ID"));
    assertEquals("TF_PL_POSITION_PTD.PL_POSN_ID",
        equijoinCols.remove("TF_PL_ADJ_REPORT.PL_POSN_ID"));
    assertEquals("TF_PL_POSITION_PTD.FIRM_ACCT_ID",
        equijoinCols.remove("TD_TRADER_SCD.FIRM_ACCT_ID"));
    assertEquals("TF_PL_POSITION_PTD.CCY_ID",
        equijoinCols.remove("TD_TRADER_SCD.CCY_ID"));
    assertEquals("TF_PL_POSITION_PTD.SRC_SYS_ID",
        equijoinCols.remove("TL_SOURCE_SYSTEM.SRC_SYS_ID"));
    assertEquals("TF_PL_POSITION_PTD.POSN_EXT_KEY_ID",
        equijoinCols.remove("TD_POSN_EXTENDED_KEY.POSN_EXT_KEY_ID"));
    assertEquals("TF_PL_POSITION_PTD.INSM_ID",
        equijoinCols.remove("TD_INSTRUMENT_SCD.INSM_ID"));

    assertEquals(equijoinCols.toString(), 0, equijoinCols.size());
  }
  
  public void testBug() throws Exception {
    System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
   SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);    

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    //s.execute("create table business(businesskey int, name varchar(50), changedate int, primary key ( businesskey))");
    s.execute("create table business(businesskey int, name varchar(50), changedate int)");
 //   s.execute("create table nameelement(parentkey int, parentelt varchar(50), seqnum int) partition by column( parentkey) colocate with (business)");
    s.execute("create table nameelement(parentkey int, parentelt varchar(50), seqnum int) ");
    s
        .execute("create table categorybag(cbparentkey int, cbparentelt varchar(50),  " +
        		"krtModelKey varchar(50), keyvalue varchar(50))");

    
    //String query = "select AVG(cid) *2 from customers";
    String query1 = "select businesskey, name, changedate  from business as biz " +
    	"left outer join nameelement as nameElt " +
    	" on (businesskey = parentkey and parentelt = 'businessEntity')   where" +
    	" (nameElt.seqnum = 1) and businesskey in (select cbparentkey  from " +
    	"categorybag    where (cbparentelt = 'businessEntity') and" +
    	"  (krtModelKey = 'UUID:CD153257-086A-4237-B336-6BDCBDCC6634' and" +
    	" keyvalue = '40.00.00.00.00'))  order by name asc , biz.changedate asc";
  //  String query2 = "select avg(cid) from portfolio where tid > ? and sid < ? group by sid " ;
    s.executeQuery(query1);
       
  }
  
  public void testSubqueryPredicatePush_42273() throws Exception {
    //System.clearProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING);
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);    

     Connection conn = TestUtil.getConnection();
     Statement s = conn.createStatement();
    s.execute("  CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY,   " +
    		"R_NAME       CHAR(25) NOT NULL,  R_COMMENT    VARCHAR(152)) partition by column(R_REGIONKEY) ");
   
    s.execute(" CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL PRIMARY KEY,   " +
    		"N_NAME CHAR(25) NOT NULL,  N_REGIONKEY  INTEGER NOT NULL    " +
    		"REFERENCES REGION(R_REGIONKEY),  N_COMMENT    VARCHAR(152))   partition by column (N_REGIONKEY ) colocate with ( REGION)");

    s.execute("CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
    		" P_NAME   VARCHAR(55) NOT NULL,  P_MFGR        CHAR(25) NOT NULL, " +
    		" P_BRAND       CHAR(10) NOT NULL,  P_TYPE        VARCHAR(25) NOT NULL," +
    		"  P_SIZE        INTEGER NOT NULL,   P_CONTAINER   CHAR(10) NOT NULL,  " +
    		"P_RETAILPRICE DECIMAL(15,2) NOT NULL,    P_COMMENT     VARCHAR(23) NOT NULL )  " +
    		" REPLICATE");

    s.execute("CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL PRIMARY KEY,  " +
    		"S_NAME        CHAR(25) NOT NULL,   S_ADDRESS     VARCHAR(40) NOT NULL,   " +
    		" S_NATIONKEY   INTEGER NOT NULL    REFERENCES NATION(N_NATIONKEY),  " +
    		"  S_PHONE       CHAR(15) NOT NULL, S_ACCTBAL     DECIMAL(15,2) NOT NULL, " +
    		" S_COMMENT     VARCHAR(101) NOT NULL)  REPLICATE");

    s.execute("CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL  REFERENCES PART(P_PARTKEY), " +
    		"PS_SUPPKEY     INTEGER NOT NULL  REFERENCES SUPPLIER(S_SUPPKEY),  " +
    		" PS_AVAILQTY    INTEGER NOT NULL,  PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL," +
    		"   PS_COMMENT     VARCHAR(199) NOT NULL,    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)) REPLICATE" );
    		
    String query = "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, " +
    		"supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and" +
    		" p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and " +
    		"n_regionkey = r_regionkey and r_name = 'EUROPE' and " +
    		"ps_supplycost = ( select min(ps_supplycost) from partsupp, supplier, nation, region where " +
    		" p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and " +
    		"n_regionkey = r_regionkey and r_name = 'EUROPE') order by s_acctbal desc, n_name, " +
    		"s_name, p_partkey";
    query = query.replaceAll("\u00a0", " ");
  
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void afterOptimizedParsedTree(String query, StatementNode qt,
                LanguageConnectionContext lcc) {
              callbackInvoked = true;
              GemFireXDQueryObserverHolder.clearInstance();
              //Get the PRN on top of the Customer table resultset.
              //That PRN's restriction list should contain the subquery predicate
              final boolean[] asserted = new boolean[] {false};
              Visitor visitor = new VisitorAdaptor() {
                private boolean stop = false;
                @Override
                public Visitable visit(Visitable node) throws StandardException {
                  if(node instanceof ProjectRestrictNode) {
                    ResultSetNode child = ((ProjectRestrictNode)node).getChildResult();
                    boolean foundPRN = false;
                    if(child instanceof FromBaseTable) {
                      if(((FromBaseTable)child).getOrigTableName().equals("APP", "PARTSUPP")) {
                        foundPRN = true;
                      }
                    }else if(child instanceof IndexToBaseRowNode) {
                      IndexToBaseRowNode ibrn  = (IndexToBaseRowNode)child;
                      FromBaseTable fbt = ibrn.getSource();
                      if(fbt.getTableName().equals("APP", "PARTSUPP")) {
                        foundPRN = true;
                      }
                    }
                    if(foundPRN) {
                      this.stop = true;
                      asserted[0] = true;
                      
                      Visitor subqueryChecker = new VisitorAdaptor() {
                        private boolean stop = false;
                        
                        @Override
                        public Visitable visit(Visitable node) throws StandardException {
                          if(node instanceof SubqueryNode) {
                            
                            stop = true;
                            SubqueryNode sqn = (SubqueryNode)node;
                            Assert.fail("Subquery string found at PRN is  = " + sqn.getSubqueryString() + "; expected is none");
                            
                          }
                          return node;
                        }
                        @Override
                        public boolean stopTraversal() {
                          return stop;
                        }
                      };
                      ((ProjectRestrictNode)node).restrictionList.accept(subqueryChecker);
                     
                      
                    }
                  }
                  return node;
                }

                /**
                 * {@inheritDoc}
                 */
                @Override
                public boolean stopTraversal() {
                  return stop;
                }
              };
              try {
                qt.accept(visitor);
                assertTrue(asserted[0]);
              } catch (StandardException e) {
                 throw new GemFireXDRuntimeException(e);
              }
            }
          });
     
    ResultSet rs = s.executeQuery(query);
  }
  public void testBehaviour() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);    

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          //  + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references securities (sec_id) )   "
            + " replicate");

    s.execute("create index portfolio_cid on Portfolio (cid)");
    s.execute("create index security_price on Securities (price)");
    //String query = "select AVG(cid) *2 from customers";
    String query1 = "select * from customers c, portfolio pf, securities s where  " +
    		"pf.cid > c.cid and  s.price > pf.qty ";
  //  String query2 = "select avg(cid) from portfolio where tid > ? and sid < ? group by sid " ;
    
    
    ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(true);
    
    for(int i = 1 ; i <101;++i) {
      s.executeUpdate("insert into customers values ("+i+","+"'name_"+i+"',"+i +")");
    }
    for(int i = 1 ; i <101;++i) {
      s.executeUpdate("insert into securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+i+")");
    }
    for(int i = 10 ; i <100;++i) {
      int sid = i / 10 ;
      s.executeUpdate("insert into portfolio values ("+i+","+ sid+","+i*10 +"," + i*100+","+i+")");
    }
//    s.executeUpdate( "insert into portfolio values (2,7,10,50,2)");
//    s.executeUpdate("insert into portfolio values (3,8,11,51,3)");
//    s.executeUpdate( "insert into portfolio values (5,12,12,52,4)");
//    s.executeUpdate( "insert into portfolio values (6,13,13,52,5)");
//    s.executeUpdate( "insert into portfolio values (7,2,14,70,6)");
//    s.executeUpdate( "insert into portfolio values (8,3,15,55,7)");
//    s.executeUpdate( "insert into portfolio values (12,18,16,56,8)");
//    s.executeUpdate( "insert into portfolio values (13,19,17,57,9)");
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              if (qInfo instanceof SelectQueryInfo) {

              }
            }
          });
           ResultSet rs= s.executeQuery(query1);      
      //  ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(true);
//        PreparedStatement ps1 =      ((EmbedConnection)conn).prepareStatementByPassQueryInfo(-1, query2, false, true);
//        ps1.setInt(1, 0);
//       // ps1.setInt(2, 0);
//        ps1.setInt(2, 6);
//        ResultSet rs= ((EmbedPreparedStatement)ps1).executeQuery();
        Set expected = new HashSet();
        for (int i = 1; i < 6; ++i) {
          expected.add(i*10+4);
        }
        while(rs.next()) {
          expected.remove(rs.getInt(1));
        }
        rs.close();
        assertTrue(expected.isEmpty());
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
    
      s.execute("drop table portfolio");
      s.execute("drop table customers");
      s.execute("drop table securities");    
    }
  }
  
  public void testBug47114_2() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    s.execute("create schema trade");
    s.execute("create table trade.customers (cid int not null, cust_name varchar(100),"
        + "  tid int, primary key (cid))");
    s.execute("create table trade.securities (sec_id int not null, symbol varchar(10) not null, "
        + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s.execute("create table trade.portfolio (cid int not null, sid int not null, qty int not null,"
        + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
        // +
        // "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
        + " constraint sec_fk foreign key (sid) references trade.securities (sec_id) )   "
        + " partition by column (cid) colocate with ( trade.customers)");

    s.execute("create index portfolio_cid on trade.Portfolio (cid)");
    s.execute("create index security_price on trade.Securities (price)");

    String query1 = "select sec_id, symbol, s.tid, cid, cust_name, c.tid from trade.securities s, trade.customers c "
        + "where c.cid = (select f.cid from trade.portfolio f where c.cid = f.cid and f.tid = 1 group by f.cid "
        + "having count(*) >2) and sec_id in (select sid from trade.portfolio f where availQty > 0 and availQty < 1000000) ";

    ((EmbedConnection)conn).getLanguageConnectionContext()
        .setIsConnectionForRemote(true);

    for (int i = 1; i < 10; ++i) {
      s.executeUpdate("insert into trade.customers values (" + i + ","
          + "'name_" + i + "'," + 1 + ")");
    }
    for (int i = 1; i < 10; ++i) {
      s.executeUpdate("insert into trade.securities values (" + i + ","
          + "'sec_" + i + "'," + i * 10 + "," + 1 + ")");
    }
    for (int i = 1; i < 10; ++i) {
      for (int j = 1; j < 5; ++j) {
        s.executeUpdate("insert into trade.portfolio values (" + i + "," + j
            + "," + i * 10 + "," + i * 100 + "," + 1 + ")");
      }
    }
    this.callbackInvoked = false;
    try {
      GemFireXDQueryObserverHolder.setInstance(new GemFireXDQueryObserverAdapter() {
        private int numInvocation = 0;

        @Override
        public void afterOptimizedParsedTree(String query, StatementNode qt,
            LanguageConnectionContext lcc) {
          ++numInvocation;
          if (numInvocation == 3) {
            callbackInvoked = true;
            CursorNode cn = (CursorNode)qt;
            ScrollInsensitiveResultSetNode sirn = (ScrollInsensitiveResultSetNode)cn
                .getResultSetNode();
            ProjectRestrictNode topPRN = (ProjectRestrictNode)sirn
                .getChildResult();
            // There should be no restriction present here.
            assertTrue(topPRN.restriction == null);
            assertTrue(topPRN.restrictionList == null
                || topPRN.restrictionList.size() == 0);
            JoinNode jn = (JoinNode)topPRN.getChildResult();
            ProjectRestrictNode leftRS = (ProjectRestrictNode)jn
                .getLeftResultSet();
            
            ProjectRestrictNode rightRS = (ProjectRestrictNode)jn
                .getRightResultSet();
            // The left resultset should have fbt as security and restriction
            // list contain subquery
            FromBaseTable rightFBT = (FromBaseTable)rightRS.getChildResult();
            assertEquals(rightFBT.getBaseTableName(), "SECURITIES");
            PredicateList pl = rightRS.restrictionList;
            Predicate pred = (Predicate)pl.elementAt(0);
            SubqueryNode sn = (SubqueryNode)((IsNullNode)(pred.getAndNode().leftOperand))
                .getOperand();
            assertEquals(
                "select sid from trade.portfolio f where availQty > <?> and availQty < <?>",
                sn.getSubqueryString());

            
            // The left resultset should have fbt as security and restriction
            // list contain subquery
            IndexToBaseRowNode leftIN = (IndexToBaseRowNode)leftRS.getChildResult();
            FromBaseTable leftFBT = leftIN.getSource();
            assertEquals(leftFBT.getBaseTableName(), "CUSTOMERS");
            pl = leftRS.restrictionList;
            pred = (Predicate)pl.elementAt(0);
            sn = (SubqueryNode)((BinaryRelationalOperatorNode)(pred
                .getAndNode().leftOperand)).rightOperand;
            assertEquals(
                "select f.cid from trade.portfolio f where c.cid = f.cid and f.tid = <?> group by f.cid "
                    + "having count(*) ><?>", sn.getSubqueryString());
            GemFireXDQueryObserverHolder.clearInstance();
          }

        }
      });
      ResultSet rs = s.executeQuery(query1);
      assertTrue(callbackInvoked);
      int numRows = 0;
      while (rs.next()) {
        ++numRows;
      }
      rs.close();
      assertTrue(numRows > 0);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    // fail("Should have got an Exception");
    finally {
      ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(false);
      s.execute("drop table trade.portfolio");
      s.execute("drop table trade.customers");
      s.execute("drop table trade.securities");
    }
  }
  
  
  public void testBug47114_1() throws Exception {
   // SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);    

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema trade");
    s
        .execute("create table trade.customers (cid int not null, cust_name varchar(100),"
            + "  tid int, primary key (cid))");
    s
        .execute("create table trade.securities (sec_id int not null, symbol varchar(10) not null, "
            + "price int,  tid int, constraint sec_pk primary key (sec_id)  )  replicate");
    s
        .execute("create table trade.portfolio (cid int not null, sid int not null, qty int not null,"
            + " availQty int not null,tid int, constraint portf_pk primary key (cid, sid), "
          //  + "constraint cust_fk foreign key (cid) references customers (cid) on delete restrict,"
            + " constraint sec_fk foreign key (sid) references trade.securities (sec_id) )   "
            + " partition by column (cid) colocate with ( trade.customers)");

    s.execute("create index portfolio_cid on trade.Portfolio (cid)");
    s.execute("create index security_price on trade.Securities (price)");
   
    String query1 = "select f.cid from trade.portfolio f where f.cid  > 3 and f.tid = 1 group by f.cid " +
                "having count(*) > 2 " ;
    
    
    ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(true);
    
    for(int i = 1 ; i <10;++i) {
      s.executeUpdate("insert into trade.customers values ("+i+","+"'name_"+i+"',"+1 +")");
    }
    for(int i = 1 ; i <10;++i) {
      s.executeUpdate("insert into trade.securities values ("+i+","+"'sec_"+i+"',"+i*10 +","+1+")");
    }
    for(int i = 1 ; i <10;++i) {
      for(int j = 1; j < 5; ++j) {
        s.executeUpdate("insert into trade.portfolio values ("+i+","+ j+","+i*10 +"," + i*100+","+1+")");
      }
    }
    this.callbackInvoked = false;
    try {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
           
            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo,
                GenericPreparedStatement gps, LanguageConnectionContext lcc) {
              SelectQueryInfo sqi = (SelectQueryInfo)qInfo;
              GroupByQueryInfo gb = sqi.getGroupByQI();
              assertTrue(gb.isInSortedOrder());
              callbackInvoked = true;
            }
            
          });
           ResultSet rs= s.executeQuery(query1);      
         assertTrue(this.callbackInvoked);
         this.callbackInvoked = false;
     
        rs.close();
        GemFireXDQueryObserverHolder.clearInstance();
    }catch(Exception e) {
      e.printStackTrace();
      throw e;
    }
    finally {
      ((EmbedConnection)conn).getLanguageConnectionContext().setIsConnectionForRemote(false);
      s.execute("drop table trade.portfolio");
      s.execute("drop table trade.customers");
      s.execute("drop table trade.securities");    
    }
  }
  
  public void testBug46851() throws Exception {
    String query = "select nation, o_year,sum(amount) as sum_profit " +
    "from ( select n_name as nation, year(o_orderdate) as o_year, " +
    "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, " +
    "supplier " +
    " \r\n--GEMFIREXD-PROPERTIES joinStrategy = HASH \r\n" +
    " , lineitem, partsupp, orders, nation " +
   //"supplier , lineitem, partsupp, orders, nation " +
    "where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and " +
    "p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%green%' ) " +
    "as profit group by nation, o_year order by nation, o_year desc";
    System.setProperty(GfxdConstants.GFXD_DISABLE_STATEMENT_MATCHING, "true");
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);    

     Connection conn = TestUtil.getConnection();
     Statement s = conn.createStatement();
    s.execute("  CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY,   " +
                "R_NAME       CHAR(25) NOT NULL,  R_COMMENT    VARCHAR(152)) partition by column(R_REGIONKEY) ");
   
    s.execute(" CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL PRIMARY KEY,   " +
                "N_NAME CHAR(25) NOT NULL,  N_REGIONKEY  INTEGER NOT NULL    " +
                "REFERENCES REGION(R_REGIONKEY),  N_COMMENT    VARCHAR(152))   partition by column (N_REGIONKEY ) colocate with ( REGION)");

    s.execute("CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
                " P_NAME   VARCHAR(55) NOT NULL,  P_MFGR        CHAR(25) NOT NULL, " +
                " P_BRAND       CHAR(10) NOT NULL,  P_TYPE        VARCHAR(25) NOT NULL," +
                "  P_SIZE        INTEGER NOT NULL,   P_CONTAINER   CHAR(10) NOT NULL,  " +
                "P_RETAILPRICE DECIMAL(15,2) NOT NULL,    P_COMMENT     VARCHAR(23) NOT NULL )  " +
                " REPLICATE");

    s.execute("CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL PRIMARY KEY,  " +
                "S_NAME        CHAR(25) NOT NULL,   S_ADDRESS     VARCHAR(40) NOT NULL,   " +
                " S_NATIONKEY   INTEGER NOT NULL    REFERENCES NATION(N_NATIONKEY),  " +
                "  S_PHONE       CHAR(15) NOT NULL, S_ACCTBAL     DECIMAL(15,2) NOT NULL, " +
                " S_COMMENT     VARCHAR(101) NOT NULL)  REPLICATE");

    s.execute("CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL  REFERENCES PART(P_PARTKEY), " +
                "PS_SUPPKEY     INTEGER NOT NULL  REFERENCES SUPPLIER(S_SUPPKEY),  " +
                " PS_AVAILQTY    INTEGER NOT NULL,  PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL," +
                "   PS_COMMENT     VARCHAR(199) NOT NULL,    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)) REPLICATE" );

    s.execute(" CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY, C_NAME    VARCHAR(25) NOT NULL," +
        " C_ADDRESS     VARCHAR(40) NOT NULL, C_NATIONKEY   INTEGER NOT NULL REFERENCES NATION(N_NATIONKEY)," +
        " C_PHONE       CHAR(15) NOT NULL,  C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "   C_MKTSEGMENT  CHAR(10) NOT NULL,   C_COMMENT     VARCHAR(117) NOT NULL)      REPLICATE" );
 
    
    s.execute("    CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL PRIMARY KEY, " +
        "O_CUSTKEY INTEGER NOT NULL   REFERENCES CUSTOMER(C_CUSTKEY), O_ORDERSTATUS    CHAR(1) NOT NULL," +
        " O_TOTALPRICE     DECIMAL(15,2) NOT NULL,  O_ORDERDATE      DATE NOT NULL," +
        " O_ORDERPRIORITY  CHAR(15) NOT NULL, O_CLERK  CHAR(15) NOT NULL, O_SHIPPRIORITY   INTEGER NOT NULL," +
        " O_COMMENT        VARCHAR(79) NOT NULL)  PARTITION BY PRIMARY KEY" );
    
     
    s.execute(" CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL   REFERENCES ORDERS(O_ORDERKEY)," +
                " L_PARTKEY     INTEGER NOT NULL  REFERENCES PART(P_PARTKEY),  L_SUPPKEY     INTEGER NOT NULL" +
                "   REFERENCES SUPPLIER(S_SUPPKEY), L_LINENUMBER  INTEGER NOT NULL,L_QUANTITY DECIMAL(15,2) NOT NULL," +
                " L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,   L_DISCOUNT    DECIMAL(15,2) NOT NULL," +
                " L_TAX  DECIMAL(15,2) NOT NULL, L_RETURNFLAG  CHAR(1) NOT NULL, L_LINESTATUS  CHAR(1) NOT NULL," +
                " L_SHIPDATE    DATE NOT NULL, L_COMMITDATE  DATE NOT NULL,  L_RECEIPTDATE DATE NOT NULL," +
                " L_SHIPINSTRUCT CHAR(25) NOT NULL, L_SHIPMODE     CHAR(10) NOT NULL, L_COMMENT VARCHAR(44) NOT NULL," +
                " PRIMARY KEY (L_ORDERKEY, L_LINENUMBER))   PARTITION BY COLUMN(L_ORDERKEY) COLOCATE WITH (ORDERS)" );

                
    ResultSet rs = s.executeQuery(query);
  }

  @Override
  public void setUp() throws Exception {
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    this.callbackInvoked = false;
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(false);
    super.tearDown();
  }

  public void testBug49116() throws Exception {
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      st.execute("create schema trade");

      // Create table
      st.execute("CREATE TABLE trade.usertable " + "("
          + "  YCSB_KEY VARCHAR(128) PRIMARY KEY," + "  FIELD0 VARCHAR(128),"
          + "  FIELD1 VARCHAR(128)" + ")" + " PARTITION BY COLUMN (field1)");
    }

    { // insert
      String[] securities = { "IBM", "MOT", "INTC", "TEK", "AMD", "CSCO",
          "DELL", "HP", "SMALL1", "SMALL2" };
      Connection conn = TestUtil.getConnection();
      PreparedStatement psInsert = conn
          .prepareStatement("insert into trade.usertable values (?, ?, ?)");
      for (int i = 0; i < 8; i++) {
        psInsert.setString(1, securities[i % 9]);
        psInsert.setString(2, securities[i % 9]);
        psInsert.setString(3, securities[i % 9]);
        psInsert.executeUpdate();
      }
    }

    {
      Connection conn = TestUtil.getConnection();
      String query = " SELECT u.ycsb_key, v.ycsb_key, u.field1"
          + " FROM trade.usertable u "
          + " INNER JOIN trade.usertable v ON u.field1 = v.field1"
          + " ORDER BY u.field1";
      PreparedStatement st = conn.prepareStatement(query);
      ResultSet r = st.executeQuery();
      int count = 0;
      while (r.next()) {
        count++;
      }
      assertEquals(8, count);
      r.close();
    }

    try {
      Connection conn = TestUtil.getConnection();
      String query = " SELECT u.ycsb_key, v.ycsb_key, u.field0"
          + " FROM trade.usertable u "
          + " INNER JOIN trade.usertable v ON u.field0 = v.field0"
          + " ORDER BY u.field0";
      PreparedStatement st = conn.prepareStatement(query);
      ResultSet r = st.executeQuery();
      int count = 0;
      while (r.next()) {
        count++;
      }
      assertEquals(8, count);
      r.close();
      fail("Test should fail with feature not supported exception");
    } catch (SQLException sqle) {
      assertEquals("0A000", sqle.getSQLState());
    }

    try {
      Connection conn = TestUtil.getConnection();
      String query = " SELECT u.ycsb_key, v.ycsb_key, u.field1"
          + " FROM trade.usertable u "
          + " INNER JOIN trade.usertable v ON u.field1 = v.field0"
          + " ORDER BY u.field1";
      PreparedStatement st = conn.prepareStatement(query);
      ResultSet r = st.executeQuery();
      int count = 0;
      while (r.next()) {
        count++;
      }
      assertEquals(8, count);
      r.close();
      fail("Test should fail with feature not supported exception");
    } catch (SQLException sqle) {
      assertEquals("0A000", sqle.getSQLState());
    }
    
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      st.execute("drop table trade.usertable");
      st.execute("drop schema trade restrict");
    }
  }
}

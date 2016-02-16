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
import java.sql.Statement;
import java.util.HashSet;

import com.gemstone.gemfire.cache.CacheException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Non Collocated Join Functional Test.
 * 
 * Test NC Join between two Non Collocated tables
 * 
 * @author vivekb
 */
@SuppressWarnings("serial")
public class NCJoinPredicatesDUnit extends DistributedSQLTestBase {

  public NCJoinPredicatesDUnit(String name) {
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

  public void test_Or_predicate() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      st.execute("create schema EMP");
      st.execute("create table EMP.PARTTABLE1 (IDP1 int not null, "
          + " DESCRIPTIONP1 varchar(1024) not null, "
          + "ADDRESSP1 varchar(1024) not null, primary key (IDP1))"
          + "PARTITION BY Column (IDP1)");
      st.execute("create table EMP.PARTTABLE2 (IDP2 int not null, "
          + " DESCRIPTIONP2 varchar(1024) not null, "
          + "ADDRESSP2 varchar(1024) not null, primary key (IDP2))"
          + "PARTITION BY Column (IDP2) ");
      st.execute("create table EMP.REPLTABLE1 (IDR1 int not null, "
          + " DESCRIPTIONR1 varchar(1024) not null, "
          + "ADDRESSR1 varchar(1024) not null, primary key (IDR1)) REPLICATE");
      st.execute("create table EMP.REPLTABLE2 (IDR2 int not null, "
          + " DESCRIPTIONR2 varchar(1024) not null, "
          + "ADDRESSR2 varchar(1024) not null, primary key (IDR2)) REPLICATE");
      

      for (int j = 1; j <= 9; ++j) {
      for (int i = 1; i <= 9; ++i) {
        st.execute("insert into EMP.PARTTABLE1 values (" + i + "" + j
            + ", 'First1" + i + "', 'J1')");
        st.execute("insert into EMP.PARTTABLE2 values (" + i + "" + j
            + ", 'First2" + i + "', 'J2')");
        st.execute("insert into EMP.REPLTABLE1 values (" + i + "" + j
            + ", 'First1" + i + "', 'J1')");
        st.execute("insert into EMP.REPLTABLE2 values (" + i + "" + j
            + ", 'First2" + i + "', 'J2')");
      }
      }
    }
    
    {
      HashSet<String> expected = new HashSet<String>();
      {
        String query = "select * from EMP.REPLTABLE1 r1 "
            + " ,EMP.REPLTABLE2 r2 where " + "r1.IDR1 = r2.IDR2 "
            + " and (r1.IDR1 = 22" + " or r1.IDR1=33) ";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          expected.add(rs.getString(2));
        }
        assertFalse(expected.isEmpty());
        s1.close();
      }

      {
        String query = "select * from EMP.PARTTABLE1 r1 "
            + " ,EMP.PARTTABLE2 r2 where " + "r1.IDP1 = r2.IDP2 "
            + " and (r1.IDP1 = 22" + " or r1.IDP1=33) ";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          assertTrue(expected.remove(rs.getString(2)));
        }
        assertTrue(expected.isEmpty());
        s1.close();
      }
    }
    
    {
      HashSet<String> expected = new HashSet<String>();
      {
        String query = "select * from EMP.REPLTABLE1 r1 "
            + " ,EMP.REPLTABLE2 r2 where " + "r1.IDR1 = r2.IDR2 "
            + " and (r1.IDR1 = 22" + " or r2.IDR2=33) ";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          expected.add(rs.getString(2));
        }
        assertFalse(expected.isEmpty());
        s1.close();
      }
      
      {
        String query = "select * from EMP.PARTTABLE1 r1 "
            + " ,EMP.PARTTABLE2 r2 where " + "r1.IDP1 = r2.IDP2 "
            + " and (r1.IDP1 = 22" + " or r2.IDP2=33) ";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          assertTrue(expected.remove(rs.getString(2)));
        }
        assertTrue(expected.isEmpty());
        s1.close();
      }
    }
    
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      {
        String query = "select * from EMP.REPLTABLE1 r1 "
            + " ,EMP.REPLTABLE2 r2 where " + "r1.IDR1 = r2.IDR2 "
            + " and (r1.IDR1 = 22" + " or r1.IDR1 > 22) " + " and (r2.IDR2 = 33"
            + " or r2.IDR2 > 33) ";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          expected.add(rs.getInt(1));
        }
        assertFalse(expected.isEmpty());
        s1.close();
      }
      
      {
        String query = "select * from EMP.PARTTABLE1 r1 "
            + " ,EMP.PARTTABLE2 r2 where " + "r1.IDP1 = r2.IDP2 "
            + " and (r1.IDP1 = 22" + " or r1.IDP1 > 22) " + " and (r2.IDP2 = 33"
            + " or r2.IDP2 > 33) ";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          assertTrue(expected.remove(rs.getInt(1)));
        }
        assertTrue(expected.isEmpty());
        s1.close();
      }
    }
    
    clientSQLExecute(1, "drop table if exists EMP.PARTTABLE1");
    clientSQLExecute(1, "drop table if exists EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table if exists EMP.REPLTABLE1");
    clientSQLExecute(1, "drop table if exists EMP.REPLTABLE2");
    clientSQLExecute(1, "drop schema EMP restrict");
  }

  public void test_In_predicate() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      st.execute("create schema EMP");
      st.execute("create table EMP.PARTTABLE1 (IDP1 int not null, "
          + " DESCRIPTIONP1 varchar(1024) not null, "
          + "ADDRESSP1 varchar(1024) not null, primary key (IDP1))"
          + "PARTITION BY Column (IDP1)");
      st.execute("create table EMP.PARTTABLE2 (IDP2 int not null, "
          + " DESCRIPTIONP2 varchar(1024) not null, "
          + "ADDRESSP2 varchar(1024) not null, primary key (IDP2))"
          + "PARTITION BY Column (IDP2) ");
      st.execute("create table EMP.REPLTABLE1 (IDR1 int not null, "
          + " DESCRIPTIONR1 varchar(1024) not null, "
          + "ADDRESSR1 varchar(1024) not null, primary key (IDR1)) REPLICATE");
      st.execute("create table EMP.REPLTABLE2 (IDR2 int not null, "
          + " DESCRIPTIONR2 varchar(1024) not null, "
          + "ADDRESSR2 varchar(1024) not null, primary key (IDR2)) REPLICATE");

      for (int j = 1; j <= 9; ++j) {
        for (int i = 1; i <= 9; ++i) {
          st.execute("insert into EMP.PARTTABLE1 values (" + i + "" + j
              + ", 'First1" + i + "', 'J1')");
          st.execute("insert into EMP.PARTTABLE2 values (" + i + "" + j
              + ", 'First2" + i + "', 'J2')");
          st.execute("insert into EMP.REPLTABLE1 values (" + i + "" + j
              + ", 'First1" + i + "', 'J1')");
          st.execute("insert into EMP.REPLTABLE2 values (" + i + "" + j
              + ", 'First2" + i + "', 'J2')");
        }
      }
    }

    {
      serverExecute(1, ncjPullResultSetOpenCoreObserverSet);
      HashSet<Integer> expected = new HashSet<Integer>();
      {
        String query = "select * from EMP.REPLTABLE1 r1 "
            + " ,EMP.REPLTABLE2 r2 where " + "r1.IDR1 = r2.IDR2 "
            + " and r1.IDR1 in (11, 22)" + " and r2.IDR2 in (22, 33, 44, 55, 66, 77, 88, 99)";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          expected.add(rs.getInt(1));
        }
        assertFalse(expected.isEmpty());
        s1.close();
      }

      {
        String query = "select * from EMP.PARTTABLE1 r1 "
            + " ,EMP.PARTTABLE2 r2 where " + "r1.IDP1 = r2.IDP2 "
            + " and r1.IDP1 in (11, 22)" + " and r2.IDP2 in (22, 33, 44, 55, 66, 77, 88, 99)";

        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          assertTrue(expected.remove(rs.getInt(1)));
        }
        assertTrue(expected.isEmpty());
        s1.close();
      }
      serverExecute(1, ncjPullResultSetOpenCoreObserverReset);
    }

    {
      clientSQLExecute(1, "drop table if exists EMP.PARTTABLE1");
      clientSQLExecute(1, "drop table if exists EMP.PARTTABLE2");
      clientSQLExecute(1, "drop table if exists EMP.REPLTABLE1");
      clientSQLExecute(1, "drop table if exists EMP.REPLTABLE2");
      clientSQLExecute(1, "drop schema EMP restrict");
    }
  }
  
  final GemFireXDQueryObserver ncjPullResultSetOpenCoreObserver = new GemFireXDQueryObserverAdapter() {
    @Override
    public void ncjPullResultSetVerifyVarInList(boolean value) {
      SanityManager.ASSERT(!value, "Expected no Variable Length InList");
    }
  };

  SerializableRunnable ncjPullResultSetOpenCoreObserverSet = new SerializableRunnable(
      "Set ncjPullResultSetOpenCoreObserver") {
    @Override
    public void run() throws CacheException {
      GemFireXDQueryObserverHolder
          .setInstance(ncjPullResultSetOpenCoreObserver);
    }
  };

  SerializableRunnable ncjPullResultSetOpenCoreObserverReset = new SerializableRunnable(
      "Reset ncjPullResultSetOpenCoreObserver") {
    @Override
    public void run() throws CacheException {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
          });
    }
  };

  public void test_Or_Join_predicate() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
  
      st.execute("create schema EMP");
      st.execute("create table EMP.PARTTABLE1 (IDP1 int not null, "
          + " DESCRIPTIONP1 varchar(1024) not null, "
          + "ADDRESSP1 varchar(1024) not null, primary key (IDP1))"
          + "PARTITION BY Column (IDP1)");
      st.execute("create table EMP.PARTTABLE2 (IDP2 int not null, "
          + " DESCRIPTIONP2 varchar(1024) not null, "
          + "ADDRESSP2 varchar(1024) not null, primary key (IDP2))"
          + "PARTITION BY Column (IDP2) ");
      st.execute("create table EMP.REPLTABLE1 (IDR1 int not null, "
          + " DESCRIPTIONR1 varchar(1024) not null, "
          + "ADDRESSR1 varchar(1024) not null, primary key (IDR1)) REPLICATE");
      st.execute("create table EMP.REPLTABLE2 (IDR2 int not null, "
          + " DESCRIPTIONR2 varchar(1024) not null, "
          + "ADDRESSR2 varchar(1024) not null, primary key (IDR2)) REPLICATE");
      
  
      for (int j = 1; j <= 9; ++j) {
      for (int i = 1; i <= 9; ++i) {
        st.execute("insert into EMP.PARTTABLE1 values (" + i + "" + j
            + ", 'First1" + i + "" + j + "', 'J1')");
        st.execute("insert into EMP.PARTTABLE2 values (" + i + "" + j
            + ", 'First2" + i + "" + j + "', 'J2')");
        st.execute("insert into EMP.REPLTABLE1 values (" + i + "" + j
            + ", 'First1" + i + "" + j + "', 'J1')");
        st.execute("insert into EMP.REPLTABLE2 values (" + i + "" + j
            + ", 'First2" + i + "" + j + "', 'J2')");
      }
      }
    }
        
    {
      HashSet<String> expected = new HashSet<String>();
      {
        String query = "select * from EMP.REPLTABLE1 r1 "
            + " ,EMP.REPLTABLE2 r2 where " + "r1.IDR1 = r2.IDR2 "
            + " and r1.IDR1 = 22" + " or r1.IDR1=33 ";
  
        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          expected.add(rs.getString(2) + " " + rs.getString(5));
        }
        assertFalse(expected.isEmpty());
        s1.close();
      }
  
      {
        String query = "select * from EMP.PARTTABLE1 r1 "
            + " ,EMP.PARTTABLE2 r2 where " + "r1.IDP1 = r2.IDP2 "
            + " and r1.IDP1 = 22" + " or r1.IDP1=33 ";
  
        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          assertTrue(expected.remove(rs.getString(2) + " " + rs.getString(5)));
        }
        assertTrue(expected.isEmpty());
        s1.close();
      }
    }
    
    {
      HashSet<String> expected = new HashSet<String>();
      {
        String query = "select * from EMP.REPLTABLE1 r1 "
            + " ,EMP.REPLTABLE2 r2 where " + "r1.IDR1 = r2.IDR2 "
            + " and r1.IDR1 = 22" + " or r2.IDR2=33 ";
  
        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          expected.add(rs.getString(2) + " " + rs.getString(5));
        }
        assertFalse(expected.isEmpty());
        s1.close();
      }
      
      {
        String query = "select * from EMP.PARTTABLE1 r1 "
            + " ,EMP.PARTTABLE2 r2 where " + "r1.IDP1 = r2.IDP2 "
            + " and r1.IDP1 = 22" + " or r2.IDP2=33 ";
  
        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          assertTrue(expected.remove(rs.getString(2) + " " + rs.getString(5)));
        }
        assertTrue(expected.isEmpty());
        s1.close();
      }
    }
    
    {
      HashSet<String> expected = new HashSet<String>();
      {
        String query = "select * from EMP.REPLTABLE1 r1 "
            + " ,EMP.REPLTABLE2 r2 where " + "r1.IDR1 = r2.IDR2 "
            + " and r1.IDR1 = 22" + " or r1.IDR1 > 22 " + " and r2.IDR2 = 33"
            + " or r2.IDR2 > 33 ";
  
        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          expected.add(rs.getString(2) + " " + rs.getString(5));
        }
        assertFalse(expected.isEmpty());
        s1.close();
      }
      
      {
        String query = "select * from EMP.PARTTABLE1 r1 "
            + " ,EMP.PARTTABLE2 r2 where " + "r1.IDP1 = r2.IDP2 "
            + " and r1.IDP1 = 22" + " or r1.IDP1 > 22 " + " and r2.IDP2 = 33"
            + " or r2.IDP2 > 33 ";
  
        Connection conn = TestUtil.getConnection();
        PreparedStatement s1 = conn.prepareStatement(query);
        ResultSet rs = s1.executeQuery();
        while (rs.next()) {
          assertTrue(expected.remove(rs.getString(2) + " " + rs.getString(5)));
        }
        assertTrue(expected.isEmpty());
        s1.close();
      }
    }
    
    clientSQLExecute(1, "drop table if exists EMP.PARTTABLE1");
    clientSQLExecute(1, "drop table if exists EMP.PARTTABLE2");
    clientSQLExecute(1, "drop table if exists EMP.REPLTABLE1");
    clientSQLExecute(1, "drop table if exists EMP.REPLTABLE2");
    clientSQLExecute(1, "drop schema EMP restrict");
  }
}  


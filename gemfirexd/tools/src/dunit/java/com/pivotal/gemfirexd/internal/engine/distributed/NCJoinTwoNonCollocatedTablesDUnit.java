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
import java.util.Properties;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Non Collocated Join Functional Test.
 * 
 * Test NC Join between two Non Collocated tables
 * 
 * @author vivekb
 */
@SuppressWarnings("serial")
public class NCJoinTwoNonCollocatedTablesDUnit extends DistributedSQLTestBase {

  public NCJoinTwoNonCollocatedTablesDUnit(String name) {
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

  public void testPR_PK_COL() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (String s : securities) {
      expected.add(s);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " ";

      Connection conn = TestUtil.getConnection();
      Statement s1 = conn.createStatement();
      ResultSet rs = s1.executeQuery(query);
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_PK_COL_RED() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key redundancy 2");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid) redundancy 2");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (String s : securities) {
      expected.add(s);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " ";

      Connection conn = TestUtil.getConnection();
      Statement s1 = conn.createStatement();
      ResultSet rs = s1.executeQuery(query);
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }

  public void testPR_PK_COL_QualLeft() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 6; i < 10; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ?" + " ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 10);
      ResultSet rs = s1.executeQuery();
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_PK_COL_QualBoth() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_PK_COL_GroupByAVG() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expectedVID = new HashSet<String>();
    expectedVID.add("DAM");
    expectedVID.add("DPM");

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(4);
    expectedID.add(6);

    {
      String query = "Select B.VID, AVG (B.ID) as aid from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? " + " GROUP BY B.VID";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(2)));
        assertTrue(expectedVID.remove(rs.getString(1)));
      }
      assertTrue(expectedID.isEmpty());
      assertTrue(expectedVID.isEmpty());
    }
  }
  
  public void testPR_PK_COL_GroupBySUM() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expectedVID = new HashSet<String>();
    expectedVID.add("AM");
    expectedVID.add("PM");

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(4);
    expectedID.add(6);

    {
      String query = "Select A.VID, AVG (A.ID) as aid from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? " + " GROUP BY A.VID";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(2)));
        assertTrue(expectedVID.remove(rs.getString(1)));
      }
      assertTrue(expectedID.isEmpty());
      assertTrue(expectedVID.isEmpty());
    }
  }

  public void testPR_PK_COL_Like_In() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where B.VID like ? " + "and A.SID IN (?, ?)";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setString(1, "DA%");
      s1.setInt(2, 4);
      s1.setInt(3, 6);
      ResultSet rs = s1.executeQuery();
      for(int i=0; i < 2; i++) {
        assertTrue(rs.next());
        assertEquals("AM", rs.getString(2));
      }
      assertFalse(rs.next());
    }
    
    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(0);
    expectedID.add(1);
    expectedID.add(2);
    expectedID.add(3);
    expectedID.add(4);
    
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where B.VID like ? " + "and A.VID IN (?, ?)";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setString(1, "DA%");
      s1.setString(2, "AM");
      s1.setString(3, "PM");
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(1)));
      }
      assertTrue(expectedID.isEmpty());
    }
    
    expectedID.clear();
    expectedID.add(0);
    expectedID.add(1);
    expectedID.add(2);
    expectedID.add(3);
    expectedID.add(4);
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.VID like ? " + "and B.VID IN (?, ?)";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setString(1, "A%");
      s1.setString(2, "DAM");
      s1.setString(3, "DPM");
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(1)));
      }
      assertTrue(expectedID.isEmpty());
    }
  }
  
  public void testPR_PK_COL_Equality() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();

    {
      expected.add(securities[8]);
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID = ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }

    {
      expected.add(securities[8]);
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.ID = ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 8);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_PK_PK() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (String s : securities) {
      expected.add(s);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " ";

      Connection conn = TestUtil.getConnection();
      Statement s1 = conn.createStatement();
      ResultSet rs = s1.executeQuery(query);
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_PK_PK_NoProj() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    {
      String query = "Select * from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " ";

      Connection conn = TestUtil.getConnection();
      Statement s1 = conn.createStatement();
      ResultSet rs = s1.executeQuery(query);
      for (int i = 0; i < 10; i++) {
        assertTrue(rs.next());
      }
      assertFalse(rs.next());
    }
  }


  public void testPR_PK_PK_QualLeft() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 6; i < 10; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ?" + " ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 10);
      ResultSet rs = s1.executeQuery();
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_PK_PK_QualBoth() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_PK_PK_GroupByAVG() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expectedVID = new HashSet<String>();
    expectedVID.add("DAM");
    expectedVID.add("DPM");

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(4);
    expectedID.add(6);

    {
      String query = "Select B.VID, AVG (B.ID) as aid from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? " + " GROUP BY B.VID";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(2)));
        assertTrue(expectedVID.remove(rs.getString(1)));
      }
      assertTrue(expectedID.isEmpty());
      assertTrue(expectedVID.isEmpty());
    }
  }
  
  public void testPR_PK_PK_GroupBySUM() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expectedVID = new HashSet<String>();
    expectedVID.add("AM");
    expectedVID.add("PM");

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(4);
    expectedID.add(6);

    {
      String query = "Select A.VID, AVG (A.ID) as aid from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? " + " GROUP BY A.VID";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(2)));
        assertTrue(expectedVID.remove(rs.getString(1)));
      }
      assertTrue(expectedID.isEmpty());
      assertTrue(expectedVID.isEmpty());
    }
  }

  public void testPR_PK_PK_Like_In() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(0);
    expectedID.add(1);
    expectedID.add(2);
    expectedID.add(3);
    expectedID.add(4);
    
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where B.VID like ? " + "and A.VID IN (?, ?)";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setString(1, "DA%");
      s1.setString(2, "AM");
      s1.setString(3, "PM");
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(1)));
      }
      assertTrue(expectedID.isEmpty());
    }
    
    expectedID.clear();
    expectedID.add(0);
    expectedID.add(1);
    expectedID.add(2);
    expectedID.add(3);
    expectedID.add(4);
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.VID like ? " + "and B.VID IN (?, ?)";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setString(1, "A%");
      s1.setString(2, "DAM");
      s1.setString(3, "DPM");
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(1)));
      }
      assertTrue(expectedID.isEmpty());
    }
  }
  
  public void testPR_PK_PK_Equality() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();

    {
      expected.add(securities[8]);
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID = ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
    
    {
      expected.add(securities[8]);
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tpk A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.ID = ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 8);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_GI_COL() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (String s : securities) {
      expected.add(s);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " ";

      Connection conn = TestUtil.getConnection();
      Statement s1 = conn.createStatement();
      ResultSet rs = s1.executeQuery(query);
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }

  public void testPR_GI_COL_QualLeft() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 6; i < 10; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ?" + " ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 10);
      ResultSet rs = s1.executeQuery();
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_GI_COL_QualBoth() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_GI_COL_QualBoth_incomingNode() throws Exception {
    // Start one client and three servers
    startServerVMs(1, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
    
    startServerVMs(2, 0, "SG1");
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_GI_COL_ThinClient() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Properties props = new Properties();
      props.setProperty("log-level", getLogLevel());
      int clientPort = startNetworkServer(1, null, null);
      Connection conn = TestUtil.getNetConnection(clientPort, null, props);
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }

  public void testPR_GI_PK() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (String s : securities) {
      expected.add(s);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " ";

      Connection conn = TestUtil.getConnection();
      Statement s1 = conn.createStatement();
      ResultSet rs = s1.executeQuery(query);
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }

  public void testPR_GI_PK_QualLeft() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 6; i < 10; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ?" + " ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 10);
      ResultSet rs = s1.executeQuery();
      while(rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_GI_PK_QualBoth() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
    }
  }
    
  public void testPR_GI_PK_QualBoth_LessCols() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    {
      String query = "Select A.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(1)));
      }
      assertTrue(expected.isEmpty());
    }
  }
  
  public void testPR_GI_PK_GroupByAVG() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expectedVID = new HashSet<String>();
    expectedVID.add("DAM");
    expectedVID.add("DPM");

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(4);
    expectedID.add(6);

    {
      String query = "Select B.VID, AVG (B.ID) as aid from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? " + " GROUP BY B.VID";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(2)));
        assertTrue(expectedVID.remove(rs.getString(1)));
      }
      assertTrue(expectedID.isEmpty());
      assertTrue(expectedVID.isEmpty());
    }
  }
  
  public void testPR_GI_PK_GroupBySUM() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<String> expectedVID = new HashSet<String>();
    expectedVID.add("AM");
    expectedVID.add("PM");

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(4);
    expectedID.add(6);

    {
      String query = "Select A.VID, AVG (A.ID) as aid from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? " + " GROUP BY A.VID";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(2)));
        assertTrue(expectedVID.remove(rs.getString(1)));
      }
      assertTrue(expectedID.isEmpty());
      assertTrue(expectedVID.isEmpty());
    }
  }

  public void testPR_GI_PK_Like_In() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");

    for (int i = 0; i < 5; i++) {
      String s = "AM";
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    for (int i = 5; i < 10; i++) {
      String s = "PM";
      clientSQLExecute(1, "Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      clientSQLExecute(1, "Insert into tdriver values(" + i + ",'" + s + "',"
          + 2 * i + ")");
    }

    HashSet<Integer> expectedID = new HashSet<Integer>();
    expectedID.add(0);
    expectedID.add(1);
    expectedID.add(2);
    expectedID.add(3);
    expectedID.add(4);
    
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where B.VID like ? " + "and A.VID IN (?, ?)";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setString(1, "DA%");
      s1.setString(2, "AM");
      s1.setString(3, "PM");
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(1)));
      }
      assertTrue(expectedID.isEmpty());
    }
    
    expectedID.clear();
    expectedID.add(0);
    expectedID.add(1);
    expectedID.add(2);
    expectedID.add(3);
    expectedID.add(4);
    {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.VID like ? " + "and B.VID IN (?, ?)";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setString(1, "A%");
      s1.setString(2, "DAM");
      s1.setString(3, "DPM");
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expectedID.remove(rs.getInt(1)));
      }
      assertTrue(expectedID.isEmpty());
    }
  }
  
  /*
   * Error ArrayIndexOutOfBoundsException
   * 
   * @see HashTableResultSet 
   * line columns[q.getColumnId()].compare
   */
  public void testBug_ColMismatch() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1, "create table trade.tpk ( pid int primary key, "
        + "pvid varchar(10), psid int not null) partition by primary key");
    clientSQLExecute(1, "create table trade.tcol ( cid int not null, "
        + "cvid varchar(10), csid int primary key) partition by primary key");
    
    int baseId = 50;
    {
      String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
          "HP", "SMALL1", "SMALL2" };
      
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      
      for (int i = 0; i < 10; i++) {
        st.execute("Insert into trade.tpk values(" + i + ",'"
            + securities[i % 10] + "'," + baseId * i + ")");
      }
      for (int i = 0; i < 10; i++) {
        st.execute("Insert into trade.tcol values(" + i + ",'"
            + securities[i % 10] + "'," + baseId * i + ")");
      }
    }
        
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 0; i < 10; i++) {
        expected.add(baseId * i);
      }
      
      String query = "Select A.PSID, B.CSID from "
          + " trade.tpk A inner join trade.tcol B " 
          + "on A.PSID = B.CSID";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      st.execute("drop table trade.tcol");
      st.execute("drop table trade.tpk");
      st.execute("drop schema trade restrict");
    }
  }
}  


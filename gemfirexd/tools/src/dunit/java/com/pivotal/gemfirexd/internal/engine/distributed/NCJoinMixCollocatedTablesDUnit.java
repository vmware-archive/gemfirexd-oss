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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Non Collocated Join Functional Test.
 * 
 * Will test One Non Collocated table with other Collocated tables and
 * Replicated tables
 * 
 * @author vivekb
 */
@SuppressWarnings("serial")
public class NCJoinMixCollocatedTablesDUnit extends DistributedSQLTestBase {

  public NCJoinMixCollocatedTablesDUnit(String name) {
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

  public void testMixTables_MultiColJoin() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create table tpk ( id int not null, "
        + "vid varchar(10) primary key, sid int not null) partition by primary key");
    clientSQLExecute(1, "create table tcol ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id)");
    clientSQLExecute(1, "create table tcol2 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    clientSQLExecute(1, "create table tcol3 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    clientSQLExecute(1, "create table trep ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      final String sec = securities[i % 10];
      clientSQLExecute(1, "Insert into tpk values(" + i + ","
          + (sec != null ? "'" + sec +"'" : "null") + "," + baseId * i + ")");
    }
    securities[0] = null;
    for (int i = 0; i < 10; i++) {
      final String sec = securities[i % 10];
      clientSQLExecute(1, "Insert into trep values(" + i + ","
          + (sec != null ? "'" + sec +"'" : "null") + "," + baseId * i + ")");
    }
    securities[1] = null;
    for (int i = 0; i < 10; i++) {
      final String sec = securities[i % 10];
      clientSQLExecute(1, "Insert into tcol values(" + i + ","
          + (sec != null ? "'" + sec +"'" : "null") + "," + baseId * i + ")");
    }
    securities[2] = null;
    for (int i = 0; i < 10; i++) {
      final String sec = securities[i % 10];
      clientSQLExecute(1, "Insert into tcol2 values(" + i + ","
          + (sec != null ? "'" + sec +"'" : "null") + "," + baseId * i + ")");
    }
    securities[3] = null;
    for (int i = 0; i < 10; i++) {
      final String sec = securities[i % 10];
      clientSQLExecute(1, "Insert into tcol3 values(" + i + ","
          + (sec != null ? "'" + sec +"'" : "null") + "," + baseId * i + ")");
    }

    {
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID, B.ID, B.VID, C.ID, C.VID, D.ID, D.VID, E.ID, E.VID from "
          + " tpk A inner join tcol B " +
          "on A.VID = B.VID and A.ID = B.ID"
          + " inner join tcol2 C " +
          "on B.ID = C.ID and B.VID = C.VID"
          + " inner join tcol3 D " +
          "on C.ID = D.ID and D.VID = C.VID"
          + " inner join trep E " +
          "on E.ID = D.ID and E.VID = D.VID"
          + " where A.SID < ? " + "and B.SID != ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    {
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      expected.add("IBM");
      expected.add("INTC");
      expected.add("MOT");
      expected.add("TEK");
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol B " +
          "on A.ID = B.ID"
          + " inner join tcol2 C " +
          "on B.SID = C.SID "
          + " inner join tcol3 D " +
          "on C.SID = D.SID "
          + " inner join trep E " +
          "on E.SID = D.SID "
          + " where A.SID < ? " + "and B.SID != ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        final String val = rs.getString(2);
        //getLogWriter().info("received val = " + val);
        assertTrue(expected.remove(val));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table if exists tcol3");
    clientSQLExecute(1, "drop table if exists tcol2");
    clientSQLExecute(1, "drop table if exists tcol");
  }

  public void testMixTables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int not null) partition by primary key");
    clientSQLExecute(1, "create table tgi ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id)");
    clientSQLExecute(1, "create table tcol ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key");
    clientSQLExecute(1, "create table tcol2 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key colocate with (tcol)");
    clientSQLExecute(1, "create table tcol3 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key colocate with (tcol)");
    clientSQLExecute(1, "create table trep ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tgi values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into trep values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol2 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol3 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }

    {
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol B " +
          "on A.ID = B.ID"
          + " inner join tcol2 C " +
          "on B.SID = C.SID "
          + " inner join tcol3 D " +
          "on C.SID = D.SID "
          + " inner join trep E " +
          "on E.SID = D.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    {
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tgi A inner join tcol B " +
          "on A.SID = B.SID"
          + " inner join tcol2 C " +
          "on B.SID = C.SID "
          + " inner join tcol3 D " +
          "on C.SID = D.SID "
          + " inner join trep E " +
          "on E.SID = D.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table if exists tcol3");
    clientSQLExecute(1, "drop table if exists tcol2");
    clientSQLExecute(1, "drop table if exists tcol");
  }

  public void testReplicatedTables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create table tncl1 ( id int primary key, "
        + "vid varchar(10), sid int not null) partition by primary key");
    clientSQLExecute(1, "create table tncl2 ( id int primary key, "
        + "vid varchar(10), sid int not null) partition by column(sid)");
    clientSQLExecute(1, "create table trep1 ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");
    clientSQLExecute(1, "create table trep2 ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");
    
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tncl1 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tncl2 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into trep1 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into trep2 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }

    { // 2 NC with 2 Reps
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tncl1 A inner join tncl2 B " +
          "on A.ID = B.ID"
          + " inner join trep1 C " +
          "on B.SID = C.SID "
          + " inner join trep2 D " +
          "on C.SID = D.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { // 2 NC with 1 Rep
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tncl1 A inner join tncl2 B " +
          "on A.ID = B.ID"
          + " inner join trep1 C " +
          "on B.SID = C.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { // One NC with 2 Reps
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tncl1 A inner join trep2 B " +
          "on A.ID = B.ID"
          + " inner join trep1 C " +
          "on B.SID = C.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
  }

  public void testCollocatedTables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int not null) partition by primary key");
    clientSQLExecute(1, "create table tcol ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key");
    clientSQLExecute(1, "create table tcol2 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key colocate with (tcol)");
    clientSQLExecute(1, "create table tcol3 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key colocate with (tcol)");


    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol2 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol3 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }

    {// 1 NC with 2 Cols
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol B " +
          "on A.ID = B.ID"
          + " inner join tcol2 C " +
          "on B.SID = C.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { // Cols without Master
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol3 B " +
          "on A.ID = B.ID"
          + " inner join tcol2 C " +
          "on B.SID = C.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { // Cols intra-join
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol B " +
          "on A.ID = B.ID"
          + " inner join tcol2 C " +
          "on B.SID = C.SID "
          + " inner join tcol3 D "
          + "on D.ID = C.ID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    } 
    
    clientSQLExecute(1, "drop table if exists tcol3");
    clientSQLExecute(1, "drop table if exists tcol2");
    clientSQLExecute(1, "drop table if exists tcol");
  }

  public void testSelfJoin() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int not null) partition by primary key");
    clientSQLExecute(1, "create table tpk2 ( id int primary key, "
        + "vid varchar(10), sid int not null) partition by primary key");
    clientSQLExecute(1, "create table tcol ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key");
    clientSQLExecute(1, "create table tcol2 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key colocate with (tcol)");
    clientSQLExecute(1, "create table tcol3 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by primary key colocate with (tcol)");
    clientSQLExecute(1, "create table trep ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tpk values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tpk2 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into trep values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol2 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }
    for (int i = 0; i < 10; i++) {
      clientSQLExecute(1, "Insert into tcol3 values(" + i + ",'"
          + securities[i % 10] + "'," + baseId * i + ")");
    }

    { // Self Join
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from " 
          + " tpk A inner join tpk B "
          + "on A.ID = B.ID" 
          + " where A.SID < ? " 
          + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    { // NCs as Self Join -2
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tpk2 B " 
          + "on A.ID = B.ID"
          + " inner join tpk2 C " 
          + "on B.SID = C.SID "
          + " where A.SID < ? "
          + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { // Cols As Self Join
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol B " +
          "on A.ID = B.ID"
          + " inner join tcol C " +
          "on B.SID = C.SID "
          + " inner join tcol3 D " +
          "on C.SID = D.SID "
          + " inner join trep E " +
          "on E.SID = D.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { // Cols As Self Join - 2
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol B " +
          "on A.ID = B.ID"
          + " inner join tcol C " +
          "on B.SID = C.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { // Reps As Self Join
      HashSet<String> expected = new HashSet<String>();
      for (int i = 5; i < 9; i++) {
        expected.add(securities[i]);
      }
      String query = "Select A.ID, A.VID from "
          + " tpk A inner join tcol B " +
          "on A.ID = B.ID"
          + " inner join trep C " +
          "on B.SID = C.SID "
          + " inner join tcol3 D " +
          "on C.SID = D.SID "
          + " inner join trep E " +
          "on E.SID = D.SID "
          + " where A.SID < ? " + "and B.SID > ? ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, baseId * 9);
      s1.setInt(2, baseId * 4);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table if exists tcol3");
    clientSQLExecute(1, "drop table if exists tcol2");
    clientSQLExecute(1, "drop table if exists tcol");
  }

  public void testMix_3NC_1REP_2COL_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1, "create table trade.ORDERS (OID int primary key, OSID int not null, "
    + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1, "create table trade.DRIVER (DID int primary key, DSID int not null, "
        + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1, "create table trade.TRIPLI (TID int not null, TSID int primary key, "
        + "TVID varchar(10)) partition by primary key");
    clientSQLExecute(1, "create table trade.rep ( rid int not null, rsid int primary key, "
        + "rvid varchar(10)) replicate");
    clientSQLExecute(1, "create table trade.col ( cid int not null, csid int primary key, "
        + "cvid varchar(10)) partition by column(csid) colocate with (trade.DRIVER)");
    clientSQLExecute(1, "create table trade.dcol ( dcid int not null, dcsid int primary key, "
        + "dcvid varchar(10)) partition by column(dcsid) colocate with (trade.DRIVER)");
    
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    for (int i = 1; i < 61; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + ","
          + i + ",'" + securities[i % 10] + "'" + ")");
    }
    for (int i = 11; i < 71; i++) {
      clientSQLExecute(1, "Insert into trade.DRIVER values(" + i + ","
          + i + ",'" + "2" + securities[i % 10] + "'" + ")");
    }
    for (int i = 21; i < 81; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + ","
          + i + ",'" + "3" + securities[i % 10] + "'" + ")");
    }
    for (int i = 31; i < 91; i++) {
      clientSQLExecute(1, "Insert into trade.rep values(" + i + ","
          + i + ",'" + "4" + securities[i % 10] + "'" + ")");
    }
    for (int i = 41; i < 101; i++) {
      clientSQLExecute(1, "Insert into trade.col values(" + i + ","
          + i + ",'" + "5" + securities[i % 10] + "'" + ")");
    }
    for (int i = 51; i < 111; i++) {
      clientSQLExecute(1, "Insert into trade.dcol values(" + i + ","
          + i + ",'" + "6" + securities[i % 10] + "'" + ")");
    }
  
    /*
     * Test of more four remote tables
     * 
     *  TODO: see if we are going to allow same
     */
    try {
      HashSet<String> expected2 = new HashSet<String>();
      HashSet<String> expected8 = new HashSet<String>();
      for (int i = 0; i < 10; i++) {
        expected2.add(securities[i]);
        expected8.add("4" + securities[i]);
      }
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID, D.RID, D.RVID, E.CID, E.CVID, F.DCID, F.DCVID from "
          + " trade.ORDERS A "
          + " inner join trade.DRIVER B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " inner join trade.REP D " 
          + "on C.TID = D.RID "
          + " inner join trade.COL E " 
          + "on D.RSID = E.CSID "
          + " inner join trade.DCOL F " 
          + "on E.CSID = F.DCSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected2.remove(rs.getString(2)));
        assertTrue(expected8.remove(rs.getString(8)));
      }
      assertTrue(expected2.isEmpty());
      assertTrue(expected8.isEmpty());
      s1.close();
      //fail("Test should fail with feature not supported exception");
    } catch (SQLException sqle) {
      throw sqle;
      //assertEquals("XJ001", sqle.getSQLState());
    }

    {
      List<String> expected2 = new ArrayList<String>();
      List<String> expected8 = new ArrayList<String>();
      for (int i = 0; i < 20; i++) {
        expected2.add(securities[i%10]);
        expected8.add("4" + securities[i%10]);
      }
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID, D.RID, D.RVID, E.CID, E.CVID from "
          + " trade.ORDERS A "
          + " inner join trade.DRIVER B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " inner join trade.REP D " 
          + "on C.TID = D.RID "
          + " inner join trade.COL E " 
          + "on D.RSID = E.CSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        final String val2 = rs.getString(2);
        final String val8 = rs.getString(8);
        // getLogWriter().info("got2 = " + val2);
        assertTrue(expected2.toString() + " " + val2, expected2.remove(val2));
        assertTrue(expected8.toString() + " " + val8, expected8.remove(val8));
      }
      assertTrue(expected2.isEmpty());
      assertTrue(expected8.isEmpty());
      s1.close();
    }
        
    clientSQLExecute(1, "drop table trade.REP");
    clientSQLExecute(1, "drop table trade.col");
    clientSQLExecute(1, "drop table trade.dcol");
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DRIVER");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void testSelfJoin_threeTables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      st.execute("create schema EMP");
      st.execute("create table EMP.REPTABLE1 (IDP1 int not null, CIDP1 int not null,"
          + " DESCRIPTIONP1 varchar(1024) not null, "
          + "ADDRESSP1 varchar(1024) not null, primary key (DESCRIPTIONP1))"
          + " REPLICATE " + " ");
      st.execute("create table EMP.REPTABLE3 (IDP3 int not null, CIDP3 int not null, "
          + " DESCRIPTIONP3 varchar(1024) not null, "
          + "ADDRESSP3 varchar(1024) not null, primary key (DESCRIPTIONP3))"
          + " REPLICATE " + " ");

      for (int i = 1; i <= 4; ++i) {
        st.execute("insert into EMP.REPTABLE1 values (" + i + "," + (2 * i)
            + ", 'First1" + i + "', 'J1')");
        st.execute("insert into EMP.REPTABLE3 values (" + i + "," + (4 * i)
            + ", 'First3" + i + "', 'J3')");
      }
    }
    
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      st.execute("create table EMP.PARTTABLE1 (IDP1 int not null, CIDP1 int not null,"
          + " DESCRIPTIONP1 varchar(1024) not null, "
          + "ADDRESSP1 varchar(1024) not null, primary key (DESCRIPTIONP1))"
          + "PARTITION BY Column (ADDRESSP1)");
      st.execute("create table EMP.PARTTABLE3 (IDP3 int not null, CIDP3 int not null, "
          + " DESCRIPTIONP3 varchar(1024) not null, "
          + "ADDRESSP3 varchar(1024) not null, primary key (DESCRIPTIONP3))"
          + "PARTITION BY Column (ADDRESSP3)");

      for (int i = 1; i <= 4; ++i) {
        st.execute("insert into EMP.PARTTABLE1 values (" + i + "," + (2 * i)
            + ", 'First1" + i + "', 'J1')");
        st.execute("insert into EMP.PARTTABLE3 values (" + i + "," + (4 * i)
            + ", 'First3" + i + "', 'J3')");
      }
    }
  
    HashSet<String> expected = new HashSet<String>();
    {
      String query = "select * from EMP.REPTABLE1 p1, EMP.REPTABLE1 p2, EMP.REPTABLE3 p3 "
          + " where "
          + " p3.CIDP3 = p1.CIDP1 "
          + " and p1.CIDP1 = p2.CIDP1 "
          + " and p1.IDP1 = p2.IDP1 "
          + " and p1.ADDRESSP1 = 'J1'"
          ;


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
      String query = "select * from EMP.PARTTABLE1 p1, EMP.PARTTABLE1 p2, EMP.PARTTABLE3 p3 "
          + " where "
          + " p3.CIDP3 = p1.CIDP1 "
          + " and p1.CIDP1 = p2.CIDP1 "
          + " and p1.IDP1 = p2.IDP1 "
          + " and p1.ADDRESSP1 = 'J1'"
          ;

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    clientSQLExecute(1, "drop table if exists EMP.PARTTABLE1");
    clientSQLExecute(1, "drop table if exists EMP.PARTTABLE3");
    clientSQLExecute(1, "drop table if exists EMP.REPTABLE1");
    clientSQLExecute(1, "drop table if exists EMP.REPTABLE3");
    clientSQLExecute(1, "drop schema EMP restrict");
  }
}  


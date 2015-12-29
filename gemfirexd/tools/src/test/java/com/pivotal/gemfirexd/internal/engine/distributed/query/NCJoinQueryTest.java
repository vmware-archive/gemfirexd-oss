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
package com.pivotal.gemfirexd.internal.engine.distributed.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.internal.AvailablePort;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

/**
 * Non Collocated Join Functional Test.
 * 
 * @author vivekb
 */
public class NCJoinQueryTest extends JdbcTestBase {

  public NCJoinQueryTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(NCJoinQueryTest.class));
  }
  
  @Override
  protected void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    super.setUp();
  }

  @Override
  protected void tearDown() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    super.tearDown();
  }
  
  /*
   * Distributed scenario: mcast-port is Non-zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testSingleDistributedNode_PRoth_PRoth() throws SQLException {
    System.setProperty("gemfirexd.optimizer.trace", "true");
    System.setProperty("gemfirexd.debug.true", "TraceNCJ");
    Properties props = new Properties();
    String available_port = String.valueOf(AvailablePort
        .getRandomAvailablePort(AvailablePort.JGROUPS));
    props.setProperty("mcast-port", available_port);
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    st.execute("create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      st.execute("Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      st.execute("Insert into tdriver values(" + i + ",'" + s + "'," + 2 * i
          + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 4; i < 8; i++) {
      expected.add(securities[i]);
    }

    try {
      String query = "Select A.ID, A.VID, B.ID, B.VID from "
          + " tglobalindex A inner join tdriver B "
          + " on A.ID = B.ID "
          + " where A.SID > ? " + "and B.SID < ? ";

      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    } finally {
      System.out.println("<IgnoreGrepLogsStart/>");
      System.out.println(((EmbedConnection)conn).getLanguageConnection().getOptimizerTraceOutput());
      System.out.println("<IgnoreGrepLogsStop/>");
    }

    conn.commit();
  }
  
  /*
   * Distributed scenario: mcast-port is Non-zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testSingleDistributedNode_PRpk_PRpk() throws SQLException {
    Properties props = new Properties();
    String available_port = String.valueOf(AvailablePort
        .getRandomAvailablePort(AvailablePort.JGROUPS));
    props.setProperty("mcast-port", available_port);
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    st.execute("create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      st.execute("Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      st.execute("Insert into tdriver values(" + i + ",'" + s + "'," + 2 * i
          + ")");
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

      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    conn.commit();
  }
  
  /*
   * Distributed scenario: mcast-port is Non-zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testSingleDistributedNode_PRpk_REPpk() throws SQLException {
    Properties props = new Properties();
    String available_port = String.valueOf(AvailablePort
        .getRandomAvailablePort(AvailablePort.JGROUPS));
    props.setProperty("mcast-port", available_port);
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    st.execute("create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) replicate");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      st.execute("Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      st.execute("Insert into tdriver values(" + i + ",'" + s + "'," + 2 * i
          + ")");
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

      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    conn.commit();
  }
  
  /*
   * Distributed scenario: mcast-port is Non-zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testSingleDistributedNode_REPpk_REPpk() throws SQLException {
    Properties props = new Properties();
    String available_port = String.valueOf(AvailablePort
        .getRandomAvailablePort(AvailablePort.JGROUPS));
    props.setProperty("mcast-port", available_port);
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) replicate");
    st.execute("create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) replicate");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      st.execute("Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      st.execute("Insert into tdriver values(" + i + ",'" + s + "'," + 2 * i
          + ")");
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

      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    conn.commit();
  }
  
  
  /*
   * Distributed scenario: mcast-port is Non-zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testSingleDistributedNode_PRpk_COLoth_COLoth_COLoth_REP() throws SQLException {
    System.setProperty("gemfirexd.optimizer.trace", "true");
    //System.setProperty("gemfirexd.debug.true", "TraceNCJ");
    Properties props = new Properties();
    String available_port = String.valueOf(AvailablePort
        .getRandomAvailablePort(AvailablePort.JGROUPS));
    props.setProperty("mcast-port", available_port);
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tpk ( id int not null, "
        + "vid varchar(10) primary key, sid int not null) partition by primary key");
    st.execute("create table tcol ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id)");
    st.execute("create table tcol2 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    st.execute("create table tcol3 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    st.execute("create table trep ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tpk values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[0] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into trep values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[1] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tcol values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[2] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tcol2 values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[3] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tcol3 values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 5; i < 9; i++) {
      expected.add(securities[i]);
    }

    {
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

    conn.commit();
  }

  /*
   * Distributed scenario: mcast-port is Zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testLonerVmNode_PRoth_PRoth() throws SQLException {
    Properties props = new Properties();
    props.setProperty("mcast-port", String.valueOf(0));
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tglobalindex ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    st.execute("create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      st.execute("Insert into tglobalindex values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      st.execute("Insert into tdriver values(" + i + ",'" + s + "'," + 2 * i
          + ")");
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

      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    conn.commit();
  }
  
  /*
   * Distributed scenario: mcast-port is Zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testLonerVmNode_PRpk_PRpk() throws SQLException {
    Properties props = new Properties();
    props.setProperty("mcast-port", String.valueOf(0));
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    st.execute("create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 0; i < 10; i++) {
      String s = securities[i % 10];
      st.execute("Insert into tpk values(" + i + ",'" + s + "'," + 2
          * i + ")");
      s = "D" + s;
      st.execute("Insert into tdriver values(" + i + ",'" + s + "'," + 2 * i
          + ")");
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

      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 6);
      s1.setInt(2, 16);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getString(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    conn.commit();
  }
  
  /*
   * Distributed scenario: mcast-port is Non-zero
   * TestUtil has set partition attribute to Partitioned
   */
  public void testLonerVmNode_PRpk_COLoth_COLoth_COLoth_REP() throws SQLException {
    Properties props = new Properties();
    props.setProperty("mcast-port", String.valueOf(0));
    Connection conn = getConnection(props);
    Statement st = conn.createStatement();
    st.execute("create table tpk ( id int not null, "
        + "vid varchar(10) primary key, sid int not null) partition by primary key");
    st.execute("create table tcol ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id)");
    st.execute("create table tcol2 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    st.execute("create table tcol3 ( id int not null, "
        + "vid varchar(10), sid int primary key) partition by column(id) colocate with (tcol)");
    st.execute("create table trep ( id int primary key, "
        + "vid varchar(10), sid int not null) replicate");
    conn.commit();

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    int baseId = 50;
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tpk values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[0] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into trep values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[1] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tcol values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[2] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tcol2 values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }
    securities[3] = "NULL";
    for (int i = 0; i < 10; i++) {
      st.execute("Insert into tcol3 values(" + i + ",'" + securities[i % 10]
          + "'," + baseId * i + ")");
    }

    HashSet<String> expected = new HashSet<String>();
    for (int i = 5; i < 9; i++) {
      expected.add(securities[i]);
    }

    {
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

    conn.commit();
  }
  
  public void testMix_3NC_1REP_2COL_Tables() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", String.valueOf(0));
    
    {
      Connection conn = getConnection(props);
      Statement st = conn.createStatement();
      st.execute("create schema trade");

      // Create table
      st.execute("create table trade.ORDERS (OID int primary key, OSID int not null, "
          + "OVID varchar(10)) partition by primary key");
      st.execute("create table trade.DRIVER (DID int primary key, DSID int not null, "
          + "DVID varchar(10)) partition by column (DSID)");
      st.execute("create table trade.TRIPLI (TID int not null, TSID int primary key, "
          + "TVID varchar(10)) partition by primary key");
      st.execute("create table trade.rep ( rid int not null, rsid int primary key, "
          + "rvid varchar(10)) replicate");
      st.execute("create table trade.col ( cid int not null, csid int primary key, "
          + "cvid varchar(10)) partition by column(csid) colocate with (trade.DRIVER)");
      st.execute("create table trade.dcol ( dcid int not null, dcsid int primary key, "
          + "dcvid varchar(10)) partition by column(dcsid) colocate with (trade.DRIVER)");
    }
    
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
    {
      Connection conn = getConnection(props);
      { // insert -1
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.orders values (?, ?, ?)");
        for (int i = 1; i < 61; i++) {
          psInsert.setInt(1, i);
          psInsert.setInt(2, i);
          psInsert.setString(3, securities[i % 10]);
          psInsert.executeUpdate();
        }
      }

      { // insert -2
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.DRIVER values (?, ?, ?)");
        for (int i = 11; i < 71; i++) {
          psInsert.setInt(1, i);
          psInsert.setInt(2, i);
          psInsert.setString(3, "2" + securities[i % 10]);
          psInsert.executeUpdate();
        }
      }

      { // insert -3
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.TRIPLI values (?, ?, ?)");
        for (int i = 21; i < 81; i++) {
          psInsert.setInt(1, i);
          psInsert.setInt(2, i);
          psInsert.setString(3, "3" + securities[i % 10]);
          psInsert.executeUpdate();
        }
      }

      { // insert -4
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.rep values (?, ?, ?)");
        for (int i = 31; i < 91; i++) {
          psInsert.setInt(1, i);
          psInsert.setInt(2, i);
          psInsert.setString(3, "4" + securities[i % 10]);
          psInsert.executeUpdate();
        }
      }

      { // insert -5
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.col values (?, ?, ?)");
        for (int i = 41; i < 101; i++) {
          psInsert.setInt(1, i);
          psInsert.setInt(2, i);
          psInsert.setString(3, "5" + securities[i % 10]);
          psInsert.executeUpdate();
        }
      }

      { // insert -6
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.dcol values (?, ?, ?)");
        for (int i = 51; i < 111; i++) {
          psInsert.setInt(1, i);
          psInsert.setInt(2, i);
          psInsert.setString(3, "6" + securities[i % 10]);
          psInsert.executeUpdate();
        }
      }
    }
  
    {
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
    }
        
    {
      Connection conn = TestUtil.getConnection(props);
      Statement st = conn.createStatement();
      st.execute("drop table trade.REP");
      st.execute("drop table trade.col");
      st.execute("drop table trade.dcol");
      st.execute("drop table trade.TRIPLI");
      st.execute("drop table trade.DRIVER");
      st.execute("drop table trade.orders");
      st.execute("drop schema trade restrict");
    }
  }

  public void test_Predicate_ThreeTables() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", String.valueOf(0));
    
    {
      Connection conn = getConnection(props);
      Statement st = conn.createStatement();
      st.execute("create schema trade");

      // Create table
      st.execute("create table trade.ORDERS (OID int not null, OSID int primary key, "
          + "OVID varchar(10)) partition by primary key");
      st.execute("create table trade.DUPLI (DID int not null, DSID int primary key, "
          + "DVID varchar(10)) partition by column (DSID)");
      st.execute("create table trade.TRIPLI (TID int not null, TSID int primary key, "
          + "TVID varchar(10)) partition by primary key");

      String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO",
          "DELL", "HP", "SMALL1", "SMALL2" };

      for (int i = 1; i < 31; i++) {
        st.execute("Insert into trade.ORDERS values(" + i + "," + i + ",'"
            + securities[i % 10] + "'" + ")");
      }
      for (int i = 11; i < 41; i++) {
        st.execute("Insert into trade.DUPLI values(" + i + "," + i + ",'"
            + securities[i % 10] + "'" + ")");
      }
      for (int i = 21; i < 51; i++) {
        st.execute("Insert into trade.TRIPLI values(" + i + "," + i + ",'"
            + securities[i % 10] + "'" + ")");
      }
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(23);
      expected.add(24);
      expected.add(26);
      expected.add(27);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " where A.OID > ? and B.DID <> ? and C.TID < ?"
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 22);
      s1.setInt(2, 25);
      s1.setInt(3, 28);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(23);
      expected.add(24);
      expected.add(26);
      expected.add(27);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          + " where A.OID > ? and B.DID <> ? and C.TID < ?"
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      s1.setInt(1, 22);
      s1.setInt(2, 25);
      s1.setInt(3, 28);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    {
      Connection conn = getConnection(props);
      Statement st = conn.createStatement();
      st.execute("drop table trade.TRIPLI");
      st.execute("drop table trade.DUPLI");
      st.execute("drop table trade.orders");
      st.execute("drop schema trade restrict");
    }
  }
}

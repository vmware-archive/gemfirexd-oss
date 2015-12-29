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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.internal.AvailablePort;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.client.am.SingleHopPreparedStatement;
import com.pivotal.gemfirexd.internal.client.net.NetConnection;
import com.pivotal.gemfirexd.internal.client.net.NetConnection.DSConnectionInfo;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.impl.sql.execute.CreateAliasConstantAction;
import com.pivotal.gemfirexd.internal.shared.common.BoundedLinkedQueue;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;
import com.pivotal.gemfirexd.jdbc.SingleHopTest;
import com.pivotal.gemfirexd.jdbc.SingleHopTest.RoutingKeysObserver;

public class SingleHop2Test extends JdbcTestBase {

  public SingleHop2Test(String name) {
    super(name);
  }

  public void testPoolReplenishment() throws Exception {
    // Bug #51846
    if (isTransactional) {
      return;
    }

    Properties props1 = new Properties();
    int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props1.put("mcast-port", String.valueOf(mport));
    setupConnection(props1);
    Connection conn = TestUtil.getConnection(props1);
    Properties props = new Properties();
    props.setProperty("single-hop-enabled", "true");
    props.setProperty("single-hop-max-connections", "5");
    int netPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startNetServer(netPort, null);
    final Connection connShop = getNetConnection(netPort, null, props);
    Statement s = conn.createStatement();
    s.execute("create table employee (id int not null primary key, "
        + "name varchar(20) not null, age int not null, dept varchar(20) not null) partition by column (age)");
    // so that the bucket gets created
    s.execute("insert into employee values(2, 'two', 10, 'new')");
    NetConnection nconn = (NetConnection)connShop;
    DSConnectionInfo dsconninfo = nconn.getDSConnectionInfo();
    HashMap connMap = dsconninfo.getServerToConnMap();
    assertEquals(0, connMap.size());
    // calling the below in method so that the rs gets gc'ed
    createPrepAndExec_shop(nconn);
    assertEquals(1, connMap.size());
    Iterator itr = connMap.entrySet().iterator();
    BoundedLinkedQueue bq = null;
    while (itr.hasNext()) {
      Map.Entry ent = (Entry)itr.next();
      bq = (BoundedLinkedQueue)ent.getValue();
      break;
    }
    assertNotNull(bq);
    int sz = bq.size();
    System.gc();
    while (sz != 1) {
      Thread.sleep(100);
      sz = bq.size();
    }

    PreparedStatement ps = connShop
        .prepareStatement("select * from employee where age = ?");
    ps.setInt(1, 10);
    ResultSet rs = ps.executeQuery();
    int cnt = 0;
    while (rs.next()) {
      cnt++;
      sz = bq.size();
      assertEquals(0, sz);
    }
    assertEquals(1, cnt);
    sz = bq.size();
    assertEquals(1, sz);
    ps.setInt(1, 10);
    rs = ps.executeQuery();
    cnt = 0;
    while (rs.next()) {
      cnt++;
      sz = bq.size();
      break;
    }
    sz = bq.size();
    assertEquals(0, sz);
    rs.close();
    sz = bq.size();
    assertEquals(1, sz);
  }

  private void createPrepAndExec_shop(Connection conn) throws SQLException {
    PreparedStatement ps = conn.prepareStatement("select * from employee where age = ?");
    ps.setInt(1, 10);
    ResultSet rs = ps.executeQuery();
    int cnt = 0;
    while(rs.next()) {
      cnt++;
    }
    assertEquals(1, cnt);
  }
  
  public void testNPEInErrorMsgFetch() throws Exception {
    // Bug #51846
    if (isTransactional) {
      return;
    }
    
    Properties props1 = new Properties();
    int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props1.put("mcast-port", String.valueOf(mport));
    setupConnection(props1);
    Connection conn = TestUtil.getConnection(props1);
    Properties props = new Properties();
    props.setProperty("single-hop-enabled", "true");
    props.setProperty("single-hop-max-connections", "5");
    int netPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    startNetServer(netPort, null);
    final Connection connShop = getNetConnection(netPort, null, props);
    final Connection connShop2 = getNetConnection(netPort, null, props);

    Statement s = conn.createStatement();
    s.execute("create table employee (id int not null primary key, "
        + "name varchar(20) not null, age int not null, dept varchar(20) not null) partition by column (age)");
    // so that the bucket gets created
    s.execute("insert into employee values(2, 'two', 10, 'new')");

    final Exception[] exc1= new Exception[1];
    Runnable exceptionThrower1 = new Runnable() {
      @Override
      public void run() {
        try {
          int loop = -1;
          PreparedStatement ps = connShop
              .prepareStatement("insert into employee values(?, ?, ?, ?)");
          while (loop++ < 1000) {
            ps.setInt(1, 2);
            ps.setString(2, "one");
            ps.setInt(3, 10);
            ps.setString(4, "dept1");
            ((SingleHopPreparedStatement)ps).createNewSetForTesting();
            try {
              ps.executeUpdate();
              exc1[0] = new RuntimeException("execution1 should have got exception");
            } catch (SQLException sle) {
              if (!"23505".equalsIgnoreCase(sle.getSQLState())) {
                exc1[0] = new RuntimeException("execution2 unexpected sqlstate: " + sle.getSQLState());
                break;
              }
            }
          }
        } catch (Exception ex) {
          exc1[0] = ex;
        }
      }
    };

    final Exception[] exc2= new Exception[1];
    Runnable exceptionThrower2 = new Runnable() {
      @Override
      public void run() {
        try {
          int loop = -1;
          PreparedStatement ps = connShop2
              .prepareStatement("insert into employee values(?, ?, ?, ?)");
          while (loop++ < 1000) {
            ps.setInt(1, 2);
            ps.setString(2, "one");
            ps.setInt(3, 10);
            ps.setString(4, "dept1");
            ((SingleHopPreparedStatement)ps).createNewSetForTesting();
            try {
              ps.executeUpdate();
              exc2[0] = new RuntimeException("execution2 should have got exception");
            } catch (SQLException sle) {
              if (!"23505".equalsIgnoreCase(sle.getSQLState())) {
                exc2[0] = new RuntimeException("execution2 unexpected sqlstate: " + sle.getSQLState());
                break;
              }
              Thread.sleep(10);
            }
          }
        } catch (Exception ex) {
          exc2[0] = ex;
        }
      }
    };

    Thread t1 = new Thread(exceptionThrower1);
    Thread t2 = new Thread(exceptionThrower2);
    t1.start();
    Thread.sleep(10);
    t2.start();
    t1.join();
    t2.join();
    if (exc1[0] != null) {
      fail("got exception exc1 = ", exc1[0]);
    }
    if (exc2[0] != null) {
      fail("got exception exc2 = ", exc2[0]);
    }
  }
  
  public void testInserts() throws Exception {
	if(isTransactional){
	  return;
	}
    Properties props1 = new Properties();
    int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props1.put("mcast-port", String.valueOf(mport));
    setupConnection(props1);
    Connection conn = TestUtil.getConnection(props1);
    Properties props = new Properties();
    props.setProperty("single-hop-enabled", "true");
    props.setProperty("single-hop-max-connections", "5");
    Connection connShop = startNetserverAndGetLocalNetConnection(props);
    
    RoutingKeysObserver observer =  new RoutingKeysObserver();
    GemFireXDQueryObserverHolder.setInstance(observer);
    Statement s = conn.createStatement();
    s.execute("create table employee (id int not null primary key, "
        + "name varchar(20) not null, age int not null, dept varchar(20) not null) partition by column (age)");
    // so that the bucket gets created
    s.execute("insert into employee values(2, 'two', 10, 'new')");
    PreparedStatement ps = connShop.prepareStatement("insert into employee values(?, ?, ?, ?)");
    ps.setInt(1, 1);
    ps.setString(2, "one");
    ps.setInt(3, 10);
    ps.setString(4, "dept1");
    ((SingleHopPreparedStatement)ps).createNewSetForTesting();
    int cnt = ps.executeUpdate();
    assertEquals(1, cnt);
    
    Set shopset = ((SingleHopPreparedStatement)ps).getRoutingObjectSet();
    Set rkeys = observer.getRoutingKeys();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    shopset.clear();
    rkeys.clear();
    
    PreparedStatement ps1 = connShop.prepareStatement("insert into employee (id, name, age, dept) values (?, ?, ?, ?)");
    ps1.setInt(1, 3);
    ps1.setString(2, "three");
    ps1.setInt(3, 10);
    ps1.setString(4, "dept2");
    ((SingleHopPreparedStatement)ps1).createNewSetForTesting();
    cnt = ps1.executeUpdate();
    assertEquals(1, cnt);
    
    shopset = ((SingleHopPreparedStatement)ps1).getRoutingObjectSet();
    rkeys = observer.getRoutingKeys();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    shopset.clear();
    rkeys.clear();

    PreparedStatement ps2 = connShop.prepareStatement("insert into employee values(4, 'four', 10, 'dept')");
    ((SingleHopPreparedStatement)ps2).createNewSetForTesting();
    cnt = ps2.executeUpdate();
    assertEquals(1, cnt);
    
    shopset = ((SingleHopPreparedStatement)ps2).getRoutingObjectSet();
    rkeys = observer.getRoutingKeys();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    shopset.clear();
    rkeys.clear();
  }

  public void testAggregates() throws Exception {
   if(isTransactional){
	  return;
	}
    Properties props1 = new Properties();
    int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props1.put("mcast-port", String.valueOf(mport));
    setupConnection(props1);
    Connection conn = TestUtil.getConnection(props1);
    Properties props = new Properties();
    props.setProperty("single-hop-enabled", "true");
    props.setProperty("single-hop-max-connections", "5");
    Connection connShop = startNetserverAndGetLocalNetConnection(props);
  
    RoutingKeysObserver observer =  new RoutingKeysObserver();
    GemFireXDQueryObserverHolder.setInstance(observer);
    Statement s = conn.createStatement();
    s.execute("create table employee (id int not null primary key, "
        + "name varchar(20) not null, age int not null, dept varchar(20) not null) partition by column (age)");
  
    s.execute("insert into employee values(1, 'name1', 10, 'it1'), (2, 'name2', 20, 'it2')," +
                " (3, 'name3', 20, 'it3'), (4, 'name4', 20, 'it3')");
    
    // check max and min
    String dml = "select max(id) from employee where age = ?";
    PreparedStatement psimple = conn.prepareStatement(dml);
    psimple.setInt(1, 20);
    ResultSet rs = psimple.executeQuery();
    Set rkeys = observer.getRoutingKeys();
    PreparedStatement pshop = connShop.prepareStatement(dml);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 20);
    pshop.executeQuery();
    Set shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    rkeys.clear();
    shopset.clear();
    assertTrue(rs.next());
    assertEquals(((Integer)rs.getObject(1)).intValue(), 4);
    assertFalse(rs.next());
    
    // check group by
    dml = "select id, name, age, dept from employee where age = ? order by id desc";
    psimple = conn.prepareStatement(dml);
    psimple.setInt(1, 20);
    rs = psimple.executeQuery();
    rkeys = observer.getRoutingKeys();
    pshop = connShop.prepareStatement(dml);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 20);
    rs = pshop.executeQuery();
    shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    rkeys.clear();
    shopset.clear();
    
    dml = "select count(id) from employee where age = ? group by age";
    psimple = conn.prepareStatement(dml);
    psimple.setInt(1, 20);
    rs = psimple.executeQuery();
    rkeys = observer.getRoutingKeys();
    pshop = connShop.prepareStatement(dml);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 20);
    rs = pshop.executeQuery();
    shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    rkeys.clear();
    shopset.clear();
    assertTrue(rs.next());
    assertEquals(((Integer)rs.getObject(1)).intValue(), 3);
    
    // multiple column partitioning
    //"SELECT SUM(ol_amount) AS ol_total FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
    
    s.execute("create table orders(ol_amount int not null, ol_o_id int not null, " +
    		"ol_d_id int not null, ol_w_id int not null) " +
    		"partition by column(ol_o_id, ol_d_id, ol_w_id)");
    String insertDML = "insert into orders values(1000, 1, 1, 1), (2000, 2, 2, 2), (5000, 1, 1, 1), (6000, 2, 2, 2), (10000, 1, 1, 1)";
    s.execute(insertDML);
    String sumQry = "SELECT SUM(ol_amount) AS ol_total FROM orders WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?";
    psimple = conn.prepareStatement(sumQry);
    psimple.setInt(1, 1);
    psimple.setInt(2, 1);
    psimple.setInt(3, 1);
    rs = psimple.executeQuery();
    rkeys = observer.getRoutingKeys();
    pshop = connShop.prepareStatement(sumQry);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 1);
    pshop.setInt(2, 1);
    pshop.setInt(3, 1);
    rs = pshop.executeQuery();
    shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    rkeys.clear();
    shopset.clear();
    assertTrue(rs.next());
    assertEquals(((Integer)rs.getObject(1)).intValue(), 16000);
    assertFalse(rs.next());
  }
  
  public void testWhereClause_column() throws Exception {
	if(isTransactional){
	  return;
	}  
    Properties props1 = new Properties();
    int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props1.put("mcast-port", String.valueOf(mport));
    setupConnection(props1);
    Connection conn = TestUtil.getConnection(props1);
    Properties props = new Properties();
    props.setProperty("single-hop-enabled", "true");
    props.setProperty("single-hop-max-connections", "5");
    Connection connShop = startNetserverAndGetLocalNetConnection(props);
  
    RoutingKeysObserver observer =  new RoutingKeysObserver();
    GemFireXDQueryObserverHolder.setInstance(observer);
    Statement s = conn.createStatement();
    s.execute("create table employee (id int not null primary key, "
        + "name varchar(20) not null, age int not null, dept varchar(20) not null) partition by column (age)");
  
    s.execute("insert into employee values(1, 'name1', 10, 'it1'), (2, 'name2', 20, 'it2')," +
    		" (3, 'name3', 30, 'it3'), (4, 'name4', 40, 'it4')");
    
    String dml = "select * from employee where age = ? or age = 25";
    PreparedStatement psimple = conn.prepareStatement(dml);
    psimple.setInt(1, 10);
    ResultSet rs = psimple.executeQuery();
    Set rkeys = observer.getRoutingKeys();
    PreparedStatement pshop = connShop.prepareStatement(dml);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 10);
    pshop.executeQuery();
    Set shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    rkeys.clear();
    shopset.clear();
    // --------------------------- in list -------------
    dml = "select * from employee where age in (10, ?, 30)";
    psimple = conn.prepareStatement(dml);
    psimple.setInt(1, 20);
    psimple.executeQuery();
    rkeys = observer.getRoutingKeys();
    pshop = connShop.prepareStatement(dml);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 20);
    pshop.executeQuery();
    shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    
    rkeys.clear();
    shopset.clear();
    // --------------------------- in list anded with a random and condition -------------
    dml = "select * from employee where age in (10, ?, 30) and name = ?";
    psimple = conn.prepareStatement(dml);
    psimple.setInt(1, 20);
    psimple.setString(2, "name1");
    psimple.executeQuery();
    rkeys = observer.getRoutingKeys();
    pshop = connShop.prepareStatement(dml);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 20);
    pshop.setString(2, "name1");
    pshop.executeQuery();
    shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);  
    rkeys.clear();
    shopset.clear();
  }
  
  // range to be dealt later
  public void _testWhereClause_rangeCondition() throws Exception {
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    s.execute("create table employee (id int not null primary key, "
        + "name varchar(20) not null, age int not null, dept varchar(20) not null) partition by range (age) " +
        		"(values between 0 and 10, values between 10 and 20, values between 20 and 30)");
  
    s.execute("insert into employee values(1, 'name1', 10, 'it1'), (2, 'name2', 20, 'it2')," +
                " (3, 'name3', 30, 'it3'), (4, 'name4', 40, 'it4')");
    
    PreparedStatement ps = conn.prepareStatement("select * from employee where age >= ? and age < ?");
    ps.setInt(1, 10);
    ps.setInt(2, 30);
    ps.executeQuery();
  }
  
  
  public void testWhereClause_multipleColPartitioning() throws Exception {
	if(isTransactional){
	  return;
	}  
    Properties props1 = new Properties();
    int mport = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props1.put("mcast-port", String.valueOf(mport));
    setupConnection(props1);
    Connection conn = TestUtil.getConnection(props1);
    Properties props = new Properties();
    props.setProperty("single-hop-enabled", "true");
    props.setProperty("single-hop-max-connections", "5");
    Connection connShop = startNetserverAndGetLocalNetConnection(props);
  
    RoutingKeysObserver observer =  new RoutingKeysObserver();
    GemFireXDQueryObserverHolder.setInstance(observer);
    Statement s = conn.createStatement();
    s.execute("create table employee (id int not null primary key, "
        + "name varchar(20) not null, age int not null, dept varchar(20) not null) partition by column (age, name)");
  
    s.execute("insert into employee values(1, 'name1', 10, 'it1'), (2, 'name2', 20, 'it2')," +
                " (3, 'name3', 30, 'it3'), (4, 'name4', 40, 'it4')");
    
    String dml = "select * from employee where age = ? and name = ?";
    PreparedStatement psimple = conn.prepareStatement(dml);
    psimple.setInt(1, 20);
    psimple.setString(2, "name2");
    ResultSet rs = psimple.executeQuery();
    Set rkeys = observer.getRoutingKeys();
    PreparedStatement pshop = connShop.prepareStatement(dml);
    ((SingleHopPreparedStatement)pshop).createNewSetForTesting();
    pshop.setInt(1, 20);
    pshop.setString(2, "name2");
    pshop.executeQuery();
    Set shopset = ((SingleHopPreparedStatement)pshop).getRoutingObjectSet();
    SingleHopTest.compareSetEqulas(rkeys, shopset, true);
    rkeys.clear();
    shopset.clear();
  }
}

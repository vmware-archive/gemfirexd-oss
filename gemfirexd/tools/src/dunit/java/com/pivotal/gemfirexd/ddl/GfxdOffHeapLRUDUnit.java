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
package com.pivotal.gemfirexd.ddl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import com.gemstone.gemfire.internal.cache.control.OffHeapMemoryMonitor;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;

import io.snappydata.test.dunit.VM;

@SuppressWarnings("serial")
public class GfxdOffHeapLRUDUnit extends DistributedSQLTestBase {
  
  public GfxdOffHeapLRUDUnit(String name) {
    super(name);
  }
  
  //Disable this test, because sys.set_critical_offheap_percentage_sg() will persist to the locators.
  //The result is that after the test is run, later the other non-offheap dunit tests will also run 
  //sys.set_critical_offheap_percentage_sg() during DDL replay and fail to boot up GemFireXD.
  //However, if the test is run individually, it works fine.
  public void _testPRLRUOffHeapPercDestroy() throws Exception {
    Properties extra = new Properties();
    extra.put("gemfire.off-heap-memory-size","500m");
    startVMs(1, 1, 0, null, extra);
    
    clientSQLExecute(1, "create table trade.bigcustomers (cid int not null, cust_addr clob) offheap " +
                "EVICTION BY LRUHEAPPERCENT EVICTACTION destroy");
    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn.prepareCall("call sys.set_critical_offheap_percentage_sg(?, ?)");
    cs.setInt(1, 90);
    cs.setNull(2, Types.VARCHAR);
    cs.execute();
    cs = conn.prepareCall("call sys.set_eviction_offheap_percentage_sg(?, ?)");
    cs.setInt(1, 25);
    cs.setNull(2, Types.VARCHAR);
    cs.execute();
    float evictionOffHeapPercentage = Misc.getGemFireCache().getResourceManager().getEvictionOffHeapPercentage();
    assertEquals(Float.valueOf(25), evictionOffHeapPercentage);
    VM servervm = this.serverVMs.get(0);
    servervm.invoke(GfxdOffHeapLRUDUnit.class, "assertOffHeapPercentage", new Object[] {Float.valueOf(evictionOffHeapPercentage)});
    PreparedStatement ps = conn.prepareStatement("insert into trade.bigcustomers values(?, ?)");
    
    insertNBigElements2(200, ps, 0);
    Statement s = conn.createStatement();
    
    Thread.sleep(10000);
    s.execute("select count(*) from trade.bigcustomers");
    ResultSet rs = s.getResultSet();
    int cnt = 0;
    if (rs.next()) {
      cnt = rs.getInt(1);
    }
    TestUtil.getLogger().info("cnt: "+cnt);
    assertTrue(cnt < 200);
  }
  
  public void testPRLRUOffHeapPercDestroy_1() throws Exception {
    
    // The test is valid only for transaction isolation level NONE. 
    if (isTransactional) {
      return;
    }
    
    Properties extra = new Properties();
    extra.put("gemfire.off-heap-memory-size", "500m");
    startVMs(1, 1, 0, null, extra);
    
    clientSQLExecute(
        1,
        " create table trade.bigcustomers (cid int not null, cust_name varchar(2000), cust_addr varchar(2000), cust_addr2 varchar(2000))"
            + " offheap EVICTION BY LRUHEAPPERCENT EVICTACTION destroy");
    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn
        .prepareCall("call sys.set_eviction_offheap_percentage(?)");
    cs.setInt(1, 25);
    cs.execute();
    float evictionOffHeapPercentage = Misc.getGemFireCache().getResourceManager()
        .getEvictionOffHeapPercentage();
    assertEquals(Float.valueOf(25), evictionOffHeapPercentage);
    TestUtil.getLogger().info(
        "evictionOffHeapPercentage: " + evictionOffHeapPercentage);
    
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.bigcustomers values(?, ?, ?, ?)");
    insertNBigElements(1000, ps, 0);
    Statement s = conn.createStatement();
    VM servervm = this.serverVMs.get(0);
    servervm.invoke(GfxdOffHeapLRUDUnit.class, "raiseFakeOffHeapEvictorOnEvent");
    insertNBigElements(1000, ps, 1000);
    s.execute("select count(*) from trade.bigcustomers");
    ResultSet rs = s.getResultSet();
    int cnt = 0;
    if (rs.next()) {
      cnt = rs.getInt(1);
    }
    TestUtil.getLogger().info("cnt: " + cnt);
    assertTrue(cnt < 2000);
  }
  
  public void testPRLRUOffHeapPercOverflow() throws Exception {
    Properties extra = new Properties();
    extra.put("gemfire.off-heap-memory-size", "500m");
    startVMs(1, 1, 0, null, extra);

    clientSQLExecute(
        1,
        " create table trade.bigcustomers (cid int not null, cust_name varchar(2000), cust_addr varchar(2000), cust_addr2 varchar(2000)) "
            + " offheap EVICTION BY LRUHEAPPERCENT EVICTACTION overflow synchronous");
    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn
        .prepareCall("call sys.set_eviction_offheap_percentage(?)");
    cs.setInt(1, 25);
    cs.execute();
    float evictionOffHeapPercentage = Misc.getGemFireCache()
        .getResourceManager().getEvictionOffHeapPercentage();
    assertEquals(Float.valueOf(25), evictionOffHeapPercentage);
    TestUtil.getLogger().info(
        "evictionOffHeapPercentage: " + evictionOffHeapPercentage);
    
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.bigcustomers values(?, ?, ?, ?)");
    insertNBigElements(1000, ps, 0);
     VM servervm = this.serverVMs.get(0);
     servervm.invoke(GfxdOffHeapLRUDUnit.class, "raiseFakeOffHeapEvictorOnEvent");
    Statement s = conn.createStatement();
    insertNBigElements(1000, ps, 1000);

    s.execute("select count(*) from trade.bigcustomers");
    ResultSet rs = s.getResultSet();
    int cnt = 0;
    if (rs.next()) {
      cnt = rs.getInt(1);
    }
    TestUtil.getLogger().info("cnt: " + cnt);
    assertEquals("expected 2000 elements but found " + cnt, 2000, cnt);
  }
  
  public void testReplicatedRegionLRUOffHeapPercOverflow() throws Exception {
    Properties extra = new Properties();
    extra.put("gemfire.off-heap-memory-size", "500m");
    startVMs(1, 1, 0, null, extra);
    
    clientSQLExecute(1, " create diskstore teststore 'temp' ");
    clientSQLExecute(
        1,
        " create table trade.bigcustomers (cid int not null, cust_name varchar(2000), cust_addr varchar(2000), cust_addr2 varchar(2000)) "
            + " replicate offheap EVICTION BY LRUHEAPPERCENT EVICTACTION overflow synchronous 'teststore' ");
    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn
        .prepareCall("call sys.set_eviction_offheap_percentage(?)");
    cs.setInt(1, 25);
    cs.execute();
    float evictionOffHeapPercentage = Misc.getGemFireCache()
        .getResourceManager().getEvictionOffHeapPercentage();
    assertEquals(Float.valueOf(25), evictionOffHeapPercentage);
    TestUtil.getLogger().info(
        "evictionOffHeapPercentage: " + evictionOffHeapPercentage);
    
    PreparedStatement ps = conn
        .prepareStatement("insert into trade.bigcustomers values(?, ?, ?, ?)");
    insertNBigElements(1000, ps, 0);
    VM servervm = this.serverVMs.get(0);
    servervm.invoke(GfxdOffHeapLRUDUnit.class, "raiseFakeOffHeapEvictorOnEvent");
    Statement s = conn.createStatement();
    insertNBigElements(1000, ps, 1000);

    s.execute("select count(*) from trade.bigcustomers");
    ResultSet rs = s.getResultSet();
    int cnt = 0;
    if (rs.next()) {
      cnt = rs.getInt(1);
    }
    TestUtil.getLogger().info("cnt: " + cnt);
    assertEquals("expected 2000 elements but found " + cnt, 2000, cnt);
  }
  
  private void insertNBigElements(int n, PreparedStatement ps, int baseCnt) throws SQLException {
    String dummyPrefix = "";
    for(int i=0; i<100; i++) {
      dummyPrefix += "jjjjjjjjjj";
    }
    for (int i = baseCnt; i < n+baseCnt; i++) {
      String name = dummyPrefix + "_name"+i;
      String addr = dummyPrefix + "_addr"+(i+baseCnt);
      String addr2 = dummyPrefix + "_addr2"+(i+baseCnt);
      ps.setInt(1, i+baseCnt);
      ps.setString(2, name);
      ps.setString(3, addr);
      ps.setString(4, addr2);
      ps.executeUpdate();
    }
  }
  
  private void insertNBigElements2(int n, PreparedStatement ps, int baseCnt) throws SQLException {
    //String dummyPrefix = "";
    StringBuilder sb = new StringBuilder(1000000);
    for(int i=0; i<100000; i++) {
      sb.append("jjjjjjjjjj");
    }
    for (int i = baseCnt; i < n+baseCnt; i++) {
      String addr = sb.toString() + "_addr"+(i+baseCnt);
      ps.setInt(1, i+baseCnt);
      ps.setString(2, addr);
      ps.executeUpdate();
    }
  }
  
  public static void assertOffHeapPercentage(Float perc) {
    float evictionOffHeapPercentage = Misc.getGemFireCache().getResourceManager().getEvictionOffHeapPercentage();
    assertEquals(perc.floatValue(), evictionOffHeapPercentage);
  }
  
  public static void raiseFakeOffHeapEvictorOnEvent() {
    Misc.getGemFireCache().getOffHeapEvictor().testAbortAfterLoopCount = 1;
    
    Misc.getGemFireCache().getResourceManager().setEvictionOffHeapPercentage(85);
    OffHeapMemoryMonitor ohmm = Misc.getGemFireCache().getResourceManager().getOffHeapMonitor();
    ohmm.stopMonitoring();

    ohmm.updateStateAndSendEvent(471859199);
  }
}

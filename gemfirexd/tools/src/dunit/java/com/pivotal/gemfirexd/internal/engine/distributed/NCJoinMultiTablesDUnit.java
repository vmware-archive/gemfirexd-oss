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
import java.util.HashSet;
import com.gemstone.gemfire.cache.CacheException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Non Collocated Join Functional Test.
 * 
 * Test NC Join between more than two Non Collocated tables
 * 
 * @author vivekb
 */
@SuppressWarnings("serial")
public class NCJoinMultiTablesDUnit extends DistributedSQLTestBase {

  public NCJoinMultiTablesDUnit(String name) {
    super(name);
    // TODO Auto-generated constructor stub
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

  public void test_Three_PK_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int primary key, OSID int not null, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int not null, TSID int primary key, "
            + "TVID varchar(10)) partition by primary key");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      
      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 1, 0);

      // Second call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 1, 0);
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);

      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 1, 0);

      // Second call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 1, 0);
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Three_GI_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int primary key, OSID int not null, "
            + "OVID varchar(10)) partition by column (OSID)");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int not null, TSID int primary key, "
            + "TVID varchar(10)) partition by column (TID)");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      
      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 2, 0);
  
      // Second call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 2, 0);
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
  
      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 2, 0);
  
      // Second call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 2, 2, 0);
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }
  
  public void test_Three_LI_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int not null, OSID int primary key, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1, "create INDEX trade.t1index on trade.ORDERS(OID)");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int primary key, TSID int not null, "
            + "TVID varchar(10)) partition by  primary key");
    clientSQLExecute(1, "create INDEX trade.t2index on trade.TRIPLI(TSID)");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      
      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 1, 1, 1);
  
      // Second call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 1, 1, 1);
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
  
      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 1, 1, 1);
  
      // Second call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      execute_and_verify_ThreeTables(s1, expected, 3, 1, 1, 1);
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Predicate_Three_PK_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int primary key, OSID int not null, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int not null, TSID int primary key, "
            + "TVID varchar(10)) partition by primary key");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 11; i < 41; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 21; i < 51; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + "where B.DID > ? and TID < ?"
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      
      // First call
      s1.setInt(1, 23);
      s1.setInt(2, 27);
      expected.clear();
      expected.add(24);
      expected.add(25);
      expected.add(26);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Second call
      s1.setInt(1, 24);
      s1.setInt(2, 28);
      expected.clear();
      expected.add(27);
      expected.add(25);
      expected.add(26);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Third call
      s1.setInt(1, 23);
      s1.setInt(2, 27);
      expected.clear();
      expected.add(24);
      expected.add(25);
      expected.add(26);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // close
      s1.close();
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          + "where B.DID > ? and TID < ?"
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      
      // First call
      s1.setInt(1, 23);
      s1.setInt(2, 27);
      expected.clear();
      expected.add(24);
      expected.add(25);
      expected.add(26);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Second call
      s1.setInt(1, 24);
      s1.setInt(2, 28);
      expected.clear();
      expected.add(27);
      expected.add(25);
      expected.add(26);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Third call
      s1.setInt(1, 23);
      s1.setInt(2, 27);
      expected.clear();
      expected.add(24);
      expected.add(25);
      expected.add(26);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // close
      s1.close();
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  static int[] remoteCallbackInvokeCount = {0, 0, 0, 0};//0,1,2,3

  final GemFireXDQueryObserver ncjPullResultSetOpenCoreObserver = new GemFireXDQueryObserverAdapter() {
    @Override
    public void ncjPullResultSetOpenCoreInvoked() {
      remoteCallbackInvokeCount[0]++;
    }
    
    @Override
    public void getAllInvoked(int numElements) {
      remoteCallbackInvokeCount[1]++;
    }
    
    @Override
    public void getAllGlobalIndexInvoked(int numElements) {
      remoteCallbackInvokeCount[2]++;
    }
    
    @Override
    public void getAllLocalIndexExecuted() {
      remoteCallbackInvokeCount[3]++;
    }
  };
  
  class NcjPullResultsetTestSerializableRunnable extends
      SerializableRunnable {
    private int[] verifyCount = {0, 0, 0, 0};//0,1,2,3

    public SerializableRunnable setVerifyCount(int param0, int param1,
        int param2, int param3) {
      this.verifyCount[0] = param0;
      this.verifyCount[1] = param1;
      this.verifyCount[2] = param2;
      this.verifyCount[3] = param3;
      return this;
    }

    public NcjPullResultsetTestSerializableRunnable(String name) {
      super(name);
    }

    @Override
    public void run() throws CacheException {
      StringBuilder sb = new StringBuilder();
      if (remoteCallbackInvokeCount[0] != this.verifyCount[0]) {
        sb.append("remoteCallbackInvokeCount[0]=").append(remoteCallbackInvokeCount[0])
            .append(" ,verifyCount[0]=").append(verifyCount[0]).append(";");
      }
      
      if (remoteCallbackInvokeCount[1] != this.verifyCount[1]) {
        sb.append("remoteCallbackInvokeCount[1]=").append(remoteCallbackInvokeCount[1])
        .append(" ,verifyCount[1]=").append(verifyCount[1]).append(";");
      }
      
      if (remoteCallbackInvokeCount[2] != this.verifyCount[2]) {
        sb.append("remoteCallbackInvokeCount[2]=").append(remoteCallbackInvokeCount[2])
        .append(" ,verifyCount[2]=").append(verifyCount[2]).append(";");
      }
      
      if (remoteCallbackInvokeCount[3] != this.verifyCount[3]) {
        sb.append("remoteCallbackInvokeCount[3]=").append(remoteCallbackInvokeCount[3])
        .append(" ,verifyCount[3]=").append(verifyCount[3]).append(";");
      }
      
      if (sb.length() > 0) {
        fail(sb.toString());
      }
    }
  }
  
  NcjPullResultsetTestSerializableRunnable ncjPullResultSetOpenCoreObserverVerify = new NcjPullResultsetTestSerializableRunnable(
      "Verify ncjPullResultSetOpenCoreObserver");

  SerializableRunnable ncjPullResultSetOpenCoreObserverSet = new SerializableRunnable(
      "Set ncjPullResultSetOpenCoreObserver") {
    @Override
    public void run() throws CacheException {
      remoteCallbackInvokeCount[0] = 0;
      remoteCallbackInvokeCount[1] = 0;
      remoteCallbackInvokeCount[2] = 0;
      remoteCallbackInvokeCount[3] = 0;
      GemFireXDQueryObserverHolder
          .setInstance(ncjPullResultSetOpenCoreObserver);
    }
  };
  
  SerializableRunnable ncjPullResultSetOpenCoreObserverReset = new SerializableRunnable(
      "Reset ncjPullResultSetOpenCoreObserver") {
    @Override
    public void run() throws CacheException {
      remoteCallbackInvokeCount[0] = 0;
      remoteCallbackInvokeCount[1] = 0;
      remoteCallbackInvokeCount[2] = 0;
      remoteCallbackInvokeCount[3] = 0;
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
          });
    }
  };
  
  private void execute_and_verify_ThreeTables(PreparedStatement prepSt,
      HashSet<Integer> expected, int param0, int param1, int param2, int param3)
      throws Exception {
    serverExecute(1, ncjPullResultSetOpenCoreObserverSet);

    ResultSet rs = prepSt.executeQuery();
    while (rs.next()) {
      assertTrue(expected.remove(rs.getInt(1)));
    }
    assertTrue(expected.isEmpty());
    rs.close();

    serverExecute(1, ncjPullResultSetOpenCoreObserverVerify.setVerifyCount(
        param0, param1, param2, param3));
    serverExecute(1, ncjPullResultSetOpenCoreObserverReset);
  }

  public void test_Four_PK_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int primary key, OSID int not null, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int not null, TSID int primary key, "
            + "TVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.FORTI (FID int not null, FSID int primary key, "
            + "FVID varchar(10)) partition by primary key");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 9; i < 19; i++) {
      clientSQLExecute(1, "Insert into trade.FORTI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(9);
      expected.add(10);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID, D.FID, D.FVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          + " inner join trade.FORTI D " 
          + "on C.TSID = D.FSID "
          ;
  
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
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(9);
      expected.add(10);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID, D.FID, D.FVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " inner join trade.FORTI D " 
          + "on C.TSID = D.FSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table trade.FORTI");
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Three_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int not null, OSID int primary key, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int not null, DSID int primary key, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int primary key, TSID int not null, "
            + "TVID varchar(10)) partition by primary key");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          ;
  
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
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Predicate_ThreeTables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int not null, OSID int primary key, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int not null, DSID int primary key, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int primary key, TSID int not null, "
            + "TVID varchar(10)) partition by primary key");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 11; i < 41; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 21; i < 51; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
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
      
      // First Call
      s1.setInt(1, 22);
      s1.setInt(2, 25);
      s1.setInt(3, 28);
      expected.clear();
      expected.add(23);
      expected.add(24);
      expected.add(26);
      expected.add(27);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Second Call
      s1.setInt(1, 21);
      s1.setInt(2, 24);
      s1.setInt(3, 28);
      expected.clear();
      expected.add(22);
      expected.add(23);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Third Call
      s1.setInt(1, 22);
      s1.setInt(2, 25);
      s1.setInt(3, 28);
      expected.clear();
      expected.add(23);
      expected.add(24);
      expected.add(26);
      expected.add(27);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Close
      s1.close();
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
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
      
      // First Call
      s1.setInt(1, 22);
      s1.setInt(2, 25);
      s1.setInt(3, 28);
      expected.clear();
      expected.add(23);
      expected.add(24);
      expected.add(26);
      expected.add(27);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Second Call
      s1.setInt(1, 21);
      s1.setInt(2, 24);
      s1.setInt(3, 28);
      expected.clear();
      expected.add(22);
      expected.add(23);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Third Call
      s1.setInt(1, 22);
      s1.setInt(2, 25);
      s1.setInt(3, 28);
      expected.clear();
      expected.add(23);
      expected.add(24);
      expected.add(26);
      expected.add(27);
      execute_and_verify_ThreeTables(s1, expected, 3, 0, 0, 0);
      
      // Close
      s1.close();
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Four_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int not null, OSID int primary key, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int not null, DSID int primary key, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int primary key, TSID int not null, "
            + "TVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.FORTI (FID int primary key, FSID int not null, "
            + "FVID varchar(10)) partition by primary key");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 9; i < 19; i++) {
      clientSQLExecute(1, "Insert into trade.FORTI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(9);
      expected.add(10);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID, D.FID, D.FVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          + " inner join trade.FORTI D " 
          + "on C.TSID = D.FSID "
          ;
  
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
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(9);
      expected.add(10);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID, D.FID, D.FVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " inner join trade.FORTI D " 
          + "on C.TSID = D.FSID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table trade.FORTI");
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Three_Tables_MultiColumn() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int primary key, OSID int not null, "
         + "OVID int not null) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
             + "DVID int not null) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int not null, TSID int primary key, "
             + "TVID int not null) partition by primary key");
       
    {
      Connection conn = TestUtil.getConnection();
      { // insert -1
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.orders values (?, ?, ?)");
        for (int i = 1; i < 31; i++) {
          final int vId = i + 5000;
          final int sId;
          if (i % 2 == 0) {
            sId = i + 100;
          }
          else {
            sId = i + 1000;
          }
          psInsert.setInt(1, i);
          psInsert.setInt(2, sId);
          psInsert.setInt(3, vId);
          psInsert.executeUpdate();
        }
      }

      { // insert -2
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.DUPLI values (?, ?, ?)");
        for (int i = 11; i < 41; i++) {
          final int vId;
          final int sId;
          if (i % 2 == 0) {
            sId = i + 100;
            if (i > 25) {
              vId = i + 600;
            }
            else {
              vId = i + 6000;
            }
          }
          else {
            sId = i + 2000;
            vId = i + 7000;
          }
          psInsert.setInt(1, i);
          psInsert.setInt(2, sId);
          psInsert.setInt(3, vId);
          psInsert.executeUpdate();
        }
      }

      { // insert -3
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.TRIPLI values (?, ?, ?)");
        for (int i = 21; i < 51; i++) {
          final int vId;
          final int sId;
          if (i % 2 == 0) {
            sId = i + 100;
            if (i > 25) {
              vId = i + 600;
            }
            else {
              vId = i + 8000;
            }
          }
          else {
            sId = i + 3000;
            vId = i + 9000;
          }
          psInsert.setInt(1, i);
          psInsert.setInt(2, sId);
          psInsert.setInt(3, vId);
          psInsert.executeUpdate();
        }
      }
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(22);
      expected.add(24);
      expected.add(26);
      expected.add(28);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + "and "
          + "A.OSID = B.DSID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    // Test for redundant predicate
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(22);
      expected.add(24);
      expected.add(26);
      expected.add(28);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + "and "
          + "A.OSID = B.DSID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          + "and "
          + "B.DSID = C.TSID "
          ;
  
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
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(26);
      expected.add(28);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + "and "
          + "A.OSID = B.DSID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          + "and "
          + "B.DVID = C.TVID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Three_Tables_MultiColumn_VarChar() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int primary key, OSID int not null, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int not null, TSID int primary key, "
            + "TVID varchar(10)) partition by primary key");
       
    {
      String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO",
          "DELL", "HP", "SMALL1", "SMALL2" };
      Connection conn = TestUtil.getConnection();
      { // insert -1
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.orders values (?, ?, ?)");
        for (int i = 1; i < 31; i++) {
          final String vId = "1" + securities[i % 10];
          final int sId;
          if (i % 2 == 0) {
            sId = i + 100;
          }
          else {
            sId = i + 1000;
          }
          psInsert.setInt(1, i);
          psInsert.setInt(2, sId);
          psInsert.setString(3,  vId);
          psInsert.executeUpdate();
        }
      }
  
      { // insert -2
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.DUPLI values (?, ?, ?)");
        for (int i = 11; i < 41; i++) {
          String vId = securities[i % 10];
          final int sId;
          if (i % 2 == 0) {
            sId = i + 100;
            if (i < 25) {
              vId = "2" + vId;
            }
          }
          else {
            sId = i + 2000;
            vId = "3" + vId;
          }
          psInsert.setInt(1, i);
          psInsert.setInt(2, sId);
          psInsert.setString(3,  vId);
          psInsert.executeUpdate();
        }
      }
  
      { // insert -3
        PreparedStatement psInsert = conn
            .prepareStatement("insert into trade.TRIPLI values (?, ?, ?)");
        for (int i = 21; i < 51; i++) {
          String vId = securities[i % 10];
          final int sId;
          if (i % 2 == 0) {
            sId = i + 100;
            if (i < 25) {
              vId = "4" + vId;
            }
          }
          else {
            sId = i + 3000;
            vId = "5" + vId;
          }
          psInsert.setInt(1, i);
          psInsert.setInt(2, sId);
          psInsert.setString(3,  vId);
          psInsert.executeUpdate();
        }
      }
    }
    
    { 
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(22);
      expected.add(24);
      expected.add(26);
      expected.add(28);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + "and "
          + "A.OSID = B.DSID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          ;
  
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
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(26);
      expected.add(28);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + "and "
          + "A.OSID = B.DSID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          + "and "
          + "B.DVID = C.TVID "
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  public void test_Three_Tables_OR_predicates() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int not null, OSID int primary key, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int not null, DSID int primary key, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int primary key, TSID int not null, "
            + "TVID varchar(10)) partition by primary key");
       
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 11; i < 41; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
    }
    for (int i = 21; i < 51; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i + ",'"
          + securities[i % 10] + "'" + ")");
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
          + " where A.OID > ? AND B.DID <> ? AND C.TID < ?"
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      expected.add(28);
      expected.add(29);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " where A.OID > ? or B.DID <> ? AND C.TID < ?" // AND has higher priority than OR
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      expected.add(28);
      expected.add(29);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " where A.OID > ? or (B.DID <> ? AND C.TID < ?)"
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DSID = C.TSID "
          + " where (A.OID > ? or B.DID <> ?) AND C.TID < ?"
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
          + " where A.OID > ? AND B.DID <> ? AND C.TID < ?" 
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      expected.add(28);
      expected.add(29);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          + " where A.OID > ? or B.DID <> ? AND C.TID < ?" // AND has higher priority than OR
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      expected.add(28);
      expected.add(29);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          + " where A.OID > ? or (B.DID <> ? AND C.TID < ?)"
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TSID "
          + " where (A.OID > ? or B.DID <> ?) AND C.TID < ?"
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
          + "on B.DID = C.TID "
          + " where A.OID > ? AND B.DID <> ? AND C.TID < ?"
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      expected.add(28);
      expected.add(29);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          + " where A.OID > ? or B.DID <> ? AND C.TID < ?" // AND has higher priority than OR
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      expected.add(28);
      expected.add(29);
      expected.add(30);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          + " where A.OID > ? or (B.DID <> ? AND C.TID < ?)"
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
      expected.add(21);
      expected.add(22);
      expected.add(23);
      expected.add(24);
      expected.add(25);
      expected.add(26);
      expected.add(27);
      String query = 
          "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A "
          + " inner join trade.DUPLI B " 
          + "on A.OID = B.DID "
          + " inner join trade.TRIPLI C " 
          + "on B.DID = C.TID "
          + " where (A.OID > ? or B.DID <> ?) AND C.TID < ?"
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
    
    clientSQLExecute(1, "drop table trade.TRIPLI");
    clientSQLExecute(1, "drop table trade.DUPLI");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop schema trade restrict");
  }
  

  /**
   * Test rootId for level 2 distribution queries for join on non primary key
   * columns
   */
  public void testRootIdForNCJLevel2Queries_Three_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int not null, OSID int primary key, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int not null, DSID int primary key, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int primary key, TSID int not null, "
            + "TVID varchar(10)) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i
          + ",'" + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i
          + ",'" + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i
          + ",'" + securities[i % 10] + "'" + ")");
    }

    serverExecute(1, ncjRootIDObserverServerSet);
    serverExecute(2, ncjRootIDObserverServerSet);
    serverExecute(3, ncjRootIDObserverServerSet);
    clientExecute(1, ncjRootIDObserverClientSet);
    
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      String query = "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A " + " inner join trade.DUPLI B "
          + "on A.OID = B.DID " + " inner join trade.TRIPLI C "
          + "on B.DID = C.TSID ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    serverExecute(1, ncjRootIDObserverVerify.setVerifyCount(NCJoinMultiTablesDUnit.ncjRootID));
    serverExecute(2, ncjRootIDObserverVerify.setVerifyCount(NCJoinMultiTablesDUnit.ncjRootID));
    serverExecute(3, ncjRootIDObserverVerify.setVerifyCount(NCJoinMultiTablesDUnit.ncjRootID));
    
    serverExecute(1, ncjRootIDObserverReSet);
    serverExecute(2, ncjRootIDObserverReSet);
    serverExecute(3, ncjRootIDObserverReSet);
    clientExecute(1, ncjRootIDObserverReSet);
    
    
    // Go over everything again
    serverExecute(1, ncjRootIDObserverServerSet);
    serverExecute(2, ncjRootIDObserverServerSet);
    serverExecute(3, ncjRootIDObserverServerSet);
    clientExecute(1, ncjRootIDObserverClientSet);
    
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);
      String query = "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A " + " inner join trade.DUPLI B "
          + "on A.OID = B.DID " + " inner join trade.TRIPLI C "
          + "on B.DID = C.TSID ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    serverExecute(1, ncjRootIDObserverVerify.setVerifyCount(NCJoinMultiTablesDUnit.ncjRootID));
    serverExecute(2, ncjRootIDObserverVerify.setVerifyCount(NCJoinMultiTablesDUnit.ncjRootID));
    serverExecute(3, ncjRootIDObserverVerify.setVerifyCount(NCJoinMultiTablesDUnit.ncjRootID));
    
    serverExecute(1, ncjRootIDObserverReSet);
    serverExecute(2, ncjRootIDObserverReSet);
    serverExecute(3, ncjRootIDObserverReSet);
    clientExecute(1, ncjRootIDObserverReSet);
  }

  /**
   * Test rootId for level 2 distribution queries for join on primary key
   * columns
   */
  public void testRootIdForNCJLevel2Queries_Three_PK_Tables() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.ORDERS (OID int primary key, OSID int not null, "
            + "OVID varchar(10)) partition by primary key");
    clientSQLExecute(1,
        "create table trade.DUPLI (DID int primary key, DSID int not null, "
            + "DVID varchar(10)) partition by column (DSID)");
    clientSQLExecute(1,
        "create table trade.TRIPLI (TID int not null, TSID int primary key, "
            + "TVID varchar(10)) partition by primary key");

    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 1; i < 11; i++) {
      clientSQLExecute(1, "Insert into trade.ORDERS values(" + i + "," + i
          + ",'" + securities[i % 10] + "'" + ")");
    }
    for (int i = 4; i < 14; i++) {
      clientSQLExecute(1, "Insert into trade.DUPLI values(" + i + "," + i
          + ",'" + securities[i % 10] + "'" + ")");
    }
    for (int i = 7; i < 17; i++) {
      clientSQLExecute(1, "Insert into trade.TRIPLI values(" + i + "," + i
          + ",'" + securities[i % 10] + "'" + ")");
    }

    serverExecute(1, ncjRootIDObserverServerSet);
    serverExecute(2, ncjRootIDObserverServerSet);
    serverExecute(3, ncjRootIDObserverServerSet);
    clientExecute(1, ncjRootIDObserverClientSet);

    {
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A " + " inner join trade.DUPLI B "
          + "on A.OID = B.DID " + " inner join trade.TRIPLI C "
          + "on B.DID = C.TSID ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);

      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);

      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      rs.close();
    }
    // Should not have a level 2 query
    serverExecute(1, ncjRootIDObserverVerify.setVerifyCount(0));
    serverExecute(2, ncjRootIDObserverVerify.setVerifyCount(0));
    serverExecute(3, ncjRootIDObserverVerify.setVerifyCount(0));
    
    serverExecute(1, ncjRootIDObserverReSet);
    serverExecute(2, ncjRootIDObserverReSet);
    serverExecute(3, ncjRootIDObserverReSet);
    clientExecute(1, ncjRootIDObserverReSet);
    
    
    // Redo everything
    serverExecute(1, ncjRootIDObserverServerSet);
    serverExecute(2, ncjRootIDObserverServerSet);
    serverExecute(3, ncjRootIDObserverServerSet);
    clientExecute(1, ncjRootIDObserverClientSet);

    {
      HashSet<Integer> expected = new HashSet<Integer>();
      String query = "Select A.OID, A.OVID, B.DID, B.DVID, C.TID, C.TVID from "
          + " trade.ORDERS A " + " inner join trade.DUPLI B "
          + "on A.OID = B.DID " + " inner join trade.TRIPLI C "
          + "on B.DID = C.TSID ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);

      // First call
      expected.clear();
      expected.add(7);
      expected.add(8);
      expected.add(9);
      expected.add(10);

      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(1)));
      }
      assertTrue(expected.isEmpty());
      rs.close();
    }
    // Should not have a level 2 query
    serverExecute(1, ncjRootIDObserverVerify.setVerifyCount(0));
    serverExecute(2, ncjRootIDObserverVerify.setVerifyCount(0));
    serverExecute(3, ncjRootIDObserverVerify.setVerifyCount(0));
    
    serverExecute(1, ncjRootIDObserverReSet);
    serverExecute(2, ncjRootIDObserverReSet);
    serverExecute(3, ncjRootIDObserverReSet);
    clientExecute(1, ncjRootIDObserverReSet);
  }

  /**
   * Runnable to set the Observer for root-ids, on server
   */
  SerializableRunnable ncjRootIDObserverServerSet = new SerializableRunnable(
      "Set ncjRootIDServerObserverServerSet") {
    @Override
    public void run() throws CacheException {
      NCJoinMultiTablesDUnit.ncjRootID = 0;
      GemFireXDQueryObserverHolder.setInstance(ncjRootIDServerObserver);
    }
  };

  /**
   * Runnable to set the Observer for root-ids, on client
   */
  SerializableRunnable ncjRootIDObserverClientSet = new SerializableRunnable(
      "Set ncjRootIDServerObserverClientSet") {
    @Override
    public void run() throws CacheException {
      NCJoinMultiTablesDUnit.ncjRootID = 0;
      GemFireXDQueryObserverHolder.setInstance(ncjRootIDClientObserver);
    }
  };

  /**
   * Runnable to reset the Observer for root-id
   */
  SerializableRunnable ncjRootIDObserverReSet = new SerializableRunnable(
      "Set ncjRootIDServerObserverReset") {
    @Override
    public void run() throws CacheException {
      NCJoinMultiTablesDUnit.ncjRootID = 0;
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
          });
    }
  };

  static long ncjRootID = 0l;

  /**
   * Observer implementation class for root-id observer on Client
   */
  final GemFireXDQueryObserver ncjRootIDClientObserver = new GemFireXDQueryObserverAdapter() {
    @Override
    public void getStatementIDs(long stmtID, long rootID, int stmtLevel) {
      NCJoinMultiTablesDUnit.ncjRootID = stmtID;
    }
  };

  /**
   * Observer implementation class for root-id observer on Server
   * 
   * can also use beforeQueryExecutionByPrepStatementQueryExecutor
   */
  final GemFireXDQueryObserver ncjRootIDServerObserver = new GemFireXDQueryObserverAdapter() {

    @Override
    public void getStatementIDs(long stmtID, long rootID, int stmtLevel) {
      if (stmtLevel == 2) {
        NCJoinMultiTablesDUnit.ncjRootID = rootID;
      }
    }
  };

  /**
   * Runnable to verify the data stored by Observer
   */
  class NcjRootIDSerializableRunnable extends SerializableRunnable {

    public NcjRootIDSerializableRunnable(String name) {
      super(name);
    }

    private long verifyStatementID = 0l;

    public SerializableRunnable setVerifyCount(long ncjRootID) {
      this.verifyStatementID = ncjRootID;
      return this;
    }

    @Override
    public void run() throws CacheException {
      StringBuilder sb = new StringBuilder();
      if (NCJoinMultiTablesDUnit.ncjRootID != this.verifyStatementID) {
        sb.append("ncjPullResultSet rootID for level 2 statements should be set correctly; ");
        sb.append("givenRootID=").append(verifyStatementID)
            .append(" ,gotRootID=").append(NCJoinMultiTablesDUnit.ncjRootID)
            .append(";");
      }

      if (sb.length() > 0) {
        fail(sb.toString());
      }
    }
  }

  NcjRootIDSerializableRunnable ncjRootIDObserverVerify = new NcjRootIDSerializableRunnable(
      "Verify ncjRootIDObserver");
}  


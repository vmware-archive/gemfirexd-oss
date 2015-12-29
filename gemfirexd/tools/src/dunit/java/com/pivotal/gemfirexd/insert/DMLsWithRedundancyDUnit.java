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
package com.pivotal.gemfirexd.insert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

/**
 * Test class for dmls with redundant copies.
 * 
 * @author rdubey
 * 
 */
@SuppressWarnings("serial")
public class DMLsWithRedundancyDUnit extends DistributedSQLTestBase {
  
  public DMLsWithRedundancyDUnit(String name) {
    super(name);
  }
  
  
  /**
   * Test for #40349. Deletes on secondary should not throw any
   * exception.
   * @throws Exception on test failure.
   */
  public void testDeletesWithIndexes() throws Exception {
    startVMs(1, 2);

    clientSQLExecute(1, "create schema trade");

    clientSQLExecute(1, "create table trade.customers (cid int not null, " +
    		"cust_name varchar(100), since date, addr varchar(100), tid int, " +
    		"primary key (cid))   partition by range (cid) ( VALUES BETWEEN 0 AND 999, " +
    		"VALUES BETWEEN 1000 AND 1102, VALUES BETWEEN 1103 AND 1250, " +
    		"VALUES BETWEEN 1251 AND 1677, VALUES BETWEEN 1678 AND 1700) " +
    		"REDUNDANCY 1");
    
    java.sql.Date since = new java.sql.Date(System.currentTimeMillis());
    Connection conn = TestUtil.jdbcConn;
    PreparedStatement psInsertCust = conn.prepareStatement("insert into " +
    		"trade.customers values (?,?,?,?,?)");
    // Insert 0-499.
    for (int i = 0 ; i < 500; i++) {
      psInsertCust.setInt(1, i);
      psInsertCust.setString(2, "XXXX"+i);
      since = new java.sql.Date(System.currentTimeMillis());
      psInsertCust.setDate(3, since);
      psInsertCust.setString(4, "XXXX"+i);
      psInsertCust.setInt(5,i);
      psInsertCust.executeUpdate();
    }
    
    // Create index.
    clientSQLExecute(1,"create  index index1_8 on trade.customers ( SINCE   )");
    
    java.sql.Statement s = conn.createStatement();
    ResultSet rs = s.executeQuery("select * from trade.customers");
    int i = 0;
    while (i < 500) {
      assertTrue("ResultSet should have more rows"+i, rs.next());
      i++;
    }
    assertFalse("ResultSet should only have "+i+" rows but has more ", 
        rs.next());
    
    // delete 0-249
    for (int k= 0 ; k < 250; k++) {
      clientSQLExecute(1, "delete from trade.customers where cid = "+k);
    }
    
    rs = s.executeQuery("select * from trade.customers");
    
    // Only 250 rows left.
    i = 0;
    while (i < 250) {
      assertTrue("ResultSet should have more rows"+i, rs.next());
      i++;
    }
    assertFalse("ResultSet should only have "+i+" rows but it has more ",
        rs.next());
    
    // delete every row.
    clientSQLExecute(1, "delete from trade.customers where cid > 0");
    // this should not retrun any rows.
    rs = s.executeQuery("select * from trade.customers");
    
    assertFalse("ResultSet should be empty",rs.next());
  }
  
  /**
   * Test for 40349. Updates on secondary should not throw any assertion.
   */
  public void testUpdatesWithIndexes() throws Exception {

    startVMs(1, 2);

    clientSQLExecute(1, "create schema trade");

    clientSQLExecute(1, "create table trade.customers (cid int not null, " +
        "cust_name varchar(100), since date, addr varchar(100), tid int, " +
        "primary key (cid))   partition by range (cid) ( VALUES BETWEEN 0 AND 999, " +
        "VALUES BETWEEN 1000 AND 1102, VALUES BETWEEN 1103 AND 1250, " +
        "VALUES BETWEEN 1251 AND 1677, VALUES BETWEEN 1678 AND 1700) " +
        "REDUNDANCY 1");
    
    java.sql.Date since = new java.sql.Date(System.currentTimeMillis());
    Connection conn = TestUtil.jdbcConn;
    PreparedStatement psInsertCust = conn.prepareStatement("insert into " +
        "trade.customers values (?,?,?,?,?)");
    // Insert 0-499.
    for (int i = 0 ; i < 500; i++) {
      psInsertCust.setInt(1, i);
      psInsertCust.setString(2, "XXXX"+i);
      since = new java.sql.Date(System.currentTimeMillis());
      psInsertCust.setDate(3, since);
      psInsertCust.setString(4, "XXXX"+i);
      psInsertCust.setInt(5,i);
      assertEquals("Should insert one row",1,psInsertCust.executeUpdate());
    }
    
    // Create index.
    clientSQLExecute(1,"create  index index_tid on trade.customers ( TID )");
    
    // update every rows tid.
    PreparedStatement psUpdate = conn.prepareStatement("update " +
    		"trade.customers set tid  = tid+500 where tid = ?");
    
    for (int i = 0 ; i < 500; i++) {
      psUpdate.setInt(1, i);
      assertEquals("Should update one row",1, psUpdate.executeUpdate());
    }

    java.sql.Statement s = conn.createStatement();
    // the where predicate should force picking up of index, so we expect
    // an ordered result
    ResultSet rs = s.executeQuery("select tid from trade.customers "
        + "where tid >= 0");
    int i = 0;
    while (i < 500) {
      assertTrue("ResultSet should have more rows"+i, rs.next());
      assertEquals("Unexpected result",(i+500),rs.getInt(1));
      i++;
    }
    assertFalse("ResultSet should have only"+i+" rows", rs.next());
    
  }
}

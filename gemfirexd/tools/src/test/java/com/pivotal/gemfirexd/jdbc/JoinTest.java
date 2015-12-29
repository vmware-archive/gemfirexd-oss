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
package com.pivotal.gemfirexd.jdbc;

import java.util.*;
import java.sql.*;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class JoinTest extends JdbcTestBase {
  
  public JoinTest(String name) {
    super(name);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(JoinTest.class));
  }
  
  
  public void testJoin() throws Exception {
    // reduce logs
    reduceLogLevelForTest("config");

    int numCust = 200;
    int numOrder = 100;
    int interval = 50;
    int numQueries = 1;
    int expectedRows = interval * numQueries;
    int startLoc = 0;
    Connection conn = getConnection();
    createTables(conn);
    loadSampleData(conn, numCust, numOrder);
        
    int countFromTable = selectFromTables(conn, interval, numQueries);
    
    System.out.println("retrieved " + countFromTable + " rows");
    /*
    assertEquals("Row count from table did not match", expectedRows, countFromTable);
     */
    dropTables(conn);
  }
  
  
  
  public int selectFromTables(Connection conn, int retrievalSize, int numQueries)
  throws SQLException {
    Random rand = new Random(System.currentTimeMillis());
    int count = 0;
    ResultSet rs;
    int startLoc = rand.nextInt(5000);
    String queryString = "Select * from orders o, customers c " +
                         "where o.cust_id = c.id AND o.vol > ? and o.vol < ?";
    PreparedStatement q = conn.prepareStatement("Select * from orders o, customers c where vol > ? and vol < ?");
    
    for (int i = startLoc; i < startLoc + numQueries; i++) {
      count += selectFromTable(conn, q, i, retrievalSize);
    }
    return count;
  }
  
  
  private int selectFromTable(Connection conn,
                              PreparedStatement q,
                              int from,
                              int retrievalSize)
  throws SQLException {
    ResultSet rs;
    int count = 0;
    q.setInt(1, from);
    q.setInt(2, from + retrievalSize + 1);
    rs = q.executeQuery();
    while (rs.next()) {
      count ++;
    }
    /*
    assertEquals("Row count for single query did not match;", retrievalSize, count);
     */
    return count;
  }
  
    
  public void clearTables(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    s.execute("delete from orders, customers");
    s.close();
  }
  
  public void dropTables(Connection conn) throws SQLException { 
    Statement s = conn.createStatement();
    try {
      s.execute("drop table orders; drop table customers");
    } 
    catch (SQLException sqle) {
    }
    s.close();
  }
  
  public void createTables(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders" +
              "(id int not null , vol int, " +
              "security_id varchar(10), num int, cust_id int)");
    s.execute("create table customers " +
              "(id int not null , name varchar(200), addr varchar(100))");
    s.close();
  }
  
  /*
  public void createTableWithPrimaryKey(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders" +
              "(id int PRIMARY KEY, cust_name varchar(200), vol int, " +
              "security_id varchar(10), num int, addr varchar(100))");
    s.close();
    
  }
  
  public void createIndex(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    // create an index on 'volume'
    s.execute("create index volumeIdx on orders(vol)");
    s.close();
  }   
   */
  
  public void loadSampleData(Connection conn, int numOfCustomers, int numOrdersPerCustomer)
  throws SQLException {
    System.out.println("Loading data (" + numOfCustomers * numOrdersPerCustomer + " rows)...");
    String[] securities = { "IBM",
      "INTC",
      "MOT",
      "TEK",
      "AMD",
      "CSCO",
      "DELL",
      "HP",
      "SMALL1",
    "SMALL2" };
    
    Random rand = new Random(System.currentTimeMillis());
    PreparedStatement orderInsert =
      conn.prepareStatement("insert into orders values (?, ?, ?, ?, ?)");
    PreparedStatement custInsert =
      conn.prepareStatement("insert into customers values (?, ?, ?)");
    int volNum = 0;
    for (int i = 0; i < numOfCustomers; i++) {
      String custName = "CustomerWithaLongName" + i;
      String addr = "123 Wisteria Lane, Anywhere, USA";
      custInsert.setInt(1, i);
      custInsert.setString(2, custName);
      custInsert.setString(3, addr);
      custInsert.executeUpdate();
      for (int j = 0; j < numOrdersPerCustomer; j++) {
        orderInsert.setInt(1, j);
        orderInsert.setInt(2, volNum++); // max volume is 500
        orderInsert.setString(3, securities[rand.nextInt(10)]);
        orderInsert.setInt(4, rand.nextInt(100));
        orderInsert.setInt(5, i);
        orderInsert.executeUpdate();
      }
    }
  }
}

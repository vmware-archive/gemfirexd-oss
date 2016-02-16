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
 * Test NC Join on North Wind Traders Schema
 * 
 * @see http://gemfirexd.docs.pivotal.io/latest/userguide/index.html#data_management/adapting-existing-schema.html
 * 
 * @author vivekb
 */
@SuppressWarnings("serial")
public class NCJoinOnNorthWindTradersSchemaDUnit extends DistributedSQLTestBase {

  public NCJoinOnNorthWindTradersSchemaDUnit(String name) {
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

  protected void createSchema1() throws Exception {
    clientSQLExecute(1, "create schema trade");
    clientSQLExecute(1,
        "create table trade.Customers (customerID int primary key "
            + " ,companyName varchar(10)" + " ,cityName varchar(10)"
            + " ,postalCode int not null" + ") partition by primary key");
    clientSQLExecute(1,
        "create table trade.Employees (employeeID int primary key "
            + " ,firstName varchar(10)" + " ,lastName varchar(10)"
            + " ,postalCode int not null" + ") partition by primary key");
    clientSQLExecute(1,
        "create table trade.Products (productID int primary key "
            + " ,productName varchar(10)" + " ,productLabel varchar(10)"
            + " ,unitPrice int not null" + ") partition by primary key");
    clientSQLExecute(1, "create table trade.Orders (orderID int primary key "
        + " ,shipName varchar(10)" + " ,countryName varchar(10)"
        + " ,shipPostalCode int not null" + " ,customerID int not null"
        + " ,employeeID int not null"
        + " ,FOREIGN KEY (customerID) REFERENCES trade.Customers(customerID)"
        + " ,FOREIGN KEY (employeeID) REFERENCES trade.Employees(employeeID)"
        + ") partition by column(shipPostalCode)");
    clientSQLExecute(1, "create table trade.OrderDetails (orderID int "
        + " ,unitPrice int not null" + " ,quantity int" + " ,productID int not null"
        + " ,FOREIGN KEY (orderID) REFERENCES trade.Orders(orderID)"
        + " ,FOREIGN KEY (productID) REFERENCES trade.Products(productID)"
        + ") partition by column(unitPrice)");
  }

  protected void populateSchema1() throws Exception {
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.Customers values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + ")");
    }
    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.Employees values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + ")");
    }
    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.Products values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + ")");
    }
    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.Orders values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + "," + i + "," + i + ")");
    }
    for (int i = 1; i < 31; i++) {
      clientSQLExecute(1, "Insert into trade.OrderDetails values(" + i + ","
          + i + "," + i + "," + i + ")");
    }
  }

  protected void populateSchema2() throws Exception {
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };
  
    for (int i = 0; i < 30; i++) {
      clientSQLExecute(1, "Insert into trade.Customers values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + ")");
    }
    for (int i = 0; i < 30; i++) {
      clientSQLExecute(1, "Insert into trade.Employees values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + ")");
    }
    for (int i = 0; i < 30; i++) {
      clientSQLExecute(1, "Insert into trade.Products values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + ")");
    }
    for (int i = 0; i < 60; i++) {
      clientSQLExecute(1, "Insert into trade.Orders values(" + i + ",'"
          + securities[i % 10] + "'" + ",'" + securities[i % 10] + "'" + ","
          + i + "," + (i % 30) + "," + (i % 30) + ")");
    }
    for (int i = 0; i < 60; i++) {
      clientSQLExecute(1, "Insert into trade.OrderDetails values(" + i + ","
          + i + "," + i + "," + (i % 30) + ")");
    }
  }

  protected void dropSchema1() throws Exception {
    clientSQLExecute(1, "drop table trade.orderdetails");
    clientSQLExecute(1, "drop table trade.orders");
    clientSQLExecute(1, "drop table trade.products");
    clientSQLExecute(1, "drop table trade.employees");
    clientSQLExecute(1, "drop table trade.customers");
    clientSQLExecute(1, "drop schema trade restrict");
  }

  /*
   * Aggregates with 2 tables
   * Predicate with 4 tables
   */
  public void test_aggregate1() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    createSchema1();
    populateSchema1();

    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 1; i < 31; i++) {
        expected.add(i);
      }
      String query = "Select A.orderID, A.customerID, B.customerID, B.postalCode from "
          + " trade.ORDERS A "
          + " inner join trade.Customers B "
          + "on A.customerID = B.customerID ";

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
      for (int i = 1; i < 31; i++) {
        expected.add(i);
      }
      String query = "Select B.customerID, AVG(A.quantity) as Sales "
          + "from trade.ORDERDETAILS A join trade.ORDERS B on A.orderID = B.orderID "
          + "group by B.customerID ";

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
      List<Integer> expected = new ArrayList<Integer>();
      for (int i = 1; i < 31; i++) {
        expected.add(i);
      }
      String query = "Select B.customerID, AVG(A.quantity) as Sales "
          + "from trade.ORDERDETAILS A join trade.ORDERS B on A.orderID = B.orderID "
          + "group by B.customerID " + "order by Sales ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(rs.getInt(1) == expected.remove((int)0));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 1; i < 11; i++) {
        expected.add(i);
      }
      String query = "Select B.customerID, AVG(A.quantity) as Sales "
          + "from trade.ORDERDETAILS A join trade.ORDERS B on A.orderID = B.orderID "
          + "group by B.customerID " + "order by Sales "
          + "fetch first 10 rows only ";

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
      for (int i = 1; i < 31; i++) {
        expected.add(i*i);
      }
      String query = "Select B.customerID, AVG(A.unitPrice * A.quantity) as Sales "
          + "from trade.ORDERDETAILS A join trade.ORDERS B on A.orderID = B.orderID "
          + "group by B.customerID ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 1; i < 11; i++) {
        expected.add(i * i);
      }
      String query = "Select B.customerID, AVG(A.unitPrice * A.quantity) as Sales "
          + "from trade.ORDERDETAILS A join trade.ORDERS B on A.orderID = B.orderID "
          + "group by B.customerID "
          + "order by Sales "
          + "fetch first 10 rows only ";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
    
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(8);

      String query = "Select A.companyName, C.unitPrice, C.quantity, D.productName "
          + "from trade.CUSTOMERS A "
          + "inner join trade.ORDERS B on A.customerID = B.customerID "
          + "inner join trade.ORDERDETAILS C on B.orderID = C.orderID "
          + "inner join trade.PRODUCTS D on C.productID = D.productID "
          + "where A.postalCode=8";

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    dropSchema1();
  }

  /*
   * Aggregate with 4 tables
   */
  public void test_aggregate2() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    createSchema1();
    populateSchema1();

    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 1; i < 31; i++) {
        expected.add(i);
      }
      
      String query = "Select A.customerID, AVG(C.unitPrice * C.quantity) as Sales "
          + "from trade.CUSTOMERS A "
          + "inner join trade.ORDERS B on A.customerID = B.customerID "
          + "inner join trade.ORDERDETAILS C on B.orderID = C.orderID "
          + "inner join trade.PRODUCTS D on C.productID = D.productID "
          + "group by A.customerID "
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
      List<Integer> expected = new ArrayList<Integer>();
      for (int i = 1; i < 31; i++) {
        expected.add(i);
      }

      String query = "Select A.customerID, AVG(C.unitPrice * C.quantity) as Sales "
          + "from trade.CUSTOMERS A "
          + "inner join trade.ORDERS B on A.customerID = B.customerID "
          + "inner join trade.ORDERDETAILS C on B.orderID = C.orderID "
          + "inner join trade.PRODUCTS D on C.productID = D.productID "
          + "group by A.customerID "
          + "order by Sales ";
          ;

      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(rs.getInt(1) == expected.remove((int)0));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }

    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 1; i < 11; i++) {
        expected.add(i);
      }
      
      String query = "Select A.customerID, AVG(C.unitPrice * C.quantity) as Sales "
          + "from trade.CUSTOMERS A "
          + "inner join trade.ORDERS B on A.customerID = B.customerID "
          + "inner join trade.ORDERDETAILS C on B.orderID = C.orderID "
          + "inner join trade.PRODUCTS D on C.productID = D.productID "
          + "group by A.customerID "
          + "order by Sales "
          + "fetch first 10 rows only "
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

    dropSchema1();
  }

  /*
   * Aggregate with 4 tables, with different data set
   */
  public void test_aggregate3() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);
    createSchema1();
    populateSchema2();
  
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 0; i < 30; i++) {
        expected.add(i);
      }
      
      String query = "Select A.customerID, AVG(C.unitPrice * C.quantity) as Sales "
          + "from trade.CUSTOMERS A "
          + "inner join trade.ORDERS B on A.customerID = B.customerID "
          + "inner join trade.ORDERDETAILS C on B.orderID = C.orderID "
          + "inner join trade.PRODUCTS D on C.productID = D.productID "
          + "group by A.customerID "
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
      for (int i = 0; i < 30; i++) {
        int plusi = i + 30;
        int sum1 = i*i;
        int sum2 = plusi * plusi;
        int avg = (sum1 + sum2) / 2;
        expected.add(avg);
      }
  
      String query = "Select A.customerID, AVG(C.unitPrice * C.quantity) as Sales "
          + "from trade.CUSTOMERS A "
          + "inner join trade.ORDERS B on A.customerID = B.customerID "
          + "inner join trade.ORDERDETAILS C on B.orderID = C.orderID "
          + "inner join trade.PRODUCTS D on C.productID = D.productID "
          + "group by A.customerID "
          + "order by Sales ";
          ;
  
      Connection conn = TestUtil.getConnection();
      PreparedStatement s1 = conn.prepareStatement(query);
      ResultSet rs = s1.executeQuery();
      while (rs.next()) {
        assertTrue(expected.remove(rs.getInt(2)));
      }
      assertTrue(expected.isEmpty());
      s1.close();
    }
  
    {
      HashSet<Integer> expected = new HashSet<Integer>();
      for (int i = 0; i < 10; i++) {
        expected.add(i);
      }
      
      String query = "Select A.customerID, AVG(C.unitPrice * C.quantity) as Sales "
          + "from trade.CUSTOMERS A "
          + "inner join trade.ORDERS B on A.customerID = B.customerID "
          + "inner join trade.ORDERDETAILS C on B.orderID = C.orderID "
          + "inner join trade.PRODUCTS D on C.productID = D.productID "
          + "group by A.customerID "
          + "order by Sales "
          + "fetch first 10 rows only "
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
  
    dropSchema1();
  }
}

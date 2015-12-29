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
package sql;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author eshu
 *
 */
public class ProcedureTest {
  protected static int maxNumOfTries = 1;
  //static boolean reproduce40504 = true;
  protected static Connection getDefaultConnection() {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:default:connection");
    } catch (SQLException se) {
      //TODO, add Asif's work around for ticket #41642
      //and use jdbc:default:gemfirexd:connection
      SQLHelper.handleSQLException(se);
    }
    return conn;
  }
   
  //let the calling procedure handling the exception instead for this procedure call
  public static void selectCustomers(int tid, ResultSet[] rs) throws SQLException {
    Connection conn = getDefaultConnection();
    PreparedStatement ps1 = conn.prepareStatement("select * from trade.customers where tid= ?" );
    ps1.setInt(1, tid);
    rs[0] = ps1.executeQuery();    
    conn.close();
  }
  
  
  public static void queryCustomers(int tid, String inputJson, String outputJson[])
      throws SQLException {
    Connection conn = getDefaultConnection();
    String customerList = ",", buyorderList = ",";

    Log.getLogWriter().info(
        "comparing customer.buyorder_json and buyorder table  where TID:" + tid
            + " and STATUS:'open' ");
    PreparedStatement ps1 = conn
        .prepareStatement("select json_evalPath (buyorder_json,'$..buyorder[?(@.status == open)].oid')  as listOfCustomerWithOpenStatus  , cid from trade.customers where tid= "
            + tid
            + " and json_evalPath (buyorder_json,'$..buyorder[?(@.status == open)]') is not null ");

    ResultSet rs = ps1.executeQuery();
    while (rs.next()) {
      customerList = "," + rs.getString(1) + ",";
      buyorderList = ",";
      int cid = rs.getInt(2);

      PreparedStatement ps2 = conn
          .prepareStatement("select oid  as listOfCustomerWithOpenStatus from trade.buyorders where tid= "
              + tid + " and status = 'open' and cid = " + cid);
      ResultSet rs2 = ps2.executeQuery();
      while (rs2.next()) {
        String currOid = rs2.getString(1);
        if (customerList.contains("," + currOid + ",")) {
          customerList = customerList.replace("," + currOid + ",", ",");
        }
        else {
          buyorderList = buyorderList + currOid + ",";
        }
      }

      if (!customerList.equals(buyorderList)) {
        throw new SQLException(
            "Data mismatch found between customer.buyorder_json  and buyorder. Oid's missing from customer.buyorder_json:"
                + buyorderList
                + " Unexpected Oid's in customer.buyorder_json:"
                + customerList);
      }
    }

    if (inputJson != null) {
      PreparedStatement ps3 = conn
          .prepareStatement("select  networth_json  from trade.customers where cast(networth_json as varchar(500)) = '"
              + inputJson + "'");
      ResultSet rs3 = ps3.executeQuery();
      if (rs3.next()) {
        outputJson[0] = rs3.getString(1);
        if (!outputJson[0].equalsIgnoreCase(inputJson)) {
          throw new SQLException(
              "queryCustomers - Inout Parameter Mismatch. Input param is: "
                  + inputJson + " outputParam is " + outputJson[0]);
        }
      }
    }

    conn.close();
  }
  
  
    //let the calling procedure handling the exception instead for this procedure call
    public static void longRunningProcedure(int tid, ResultSet[] rs) throws SQLException {
      Connection conn = getDefaultConnection();
      PreparedStatement ps1 = conn.prepareStatement("select * from trade.customers where tid= ?" );
      ps1.setInt(1, tid);
    
      rs[0] = ps1.executeQuery();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    
      conn.close();
    }
  
  public static void addInterest(int tid) throws SQLException {
    Connection conn = getDefaultConnection();
    //PreparedStatement ps1 = conn.prepareStatement("update trade.networth set cash=cash*1.01 where cash>=10000 and cash < 1000000 and tid= ?" );
    PreparedStatement ps1 = conn.prepareStatement("update trade.networth set cash=trade.multiply(cash) where cash>=10000 and cash < 1000000 and tid= ?" );
    PreparedStatement ps2 = conn.prepareStatement("update trade.networth set cash=cash*1.02 where cash<10000 and cash >=1000 and tid= ?" );
    PreparedStatement ps3 = conn.prepareStatement("update trade.networth set cash=cash*1.03 where cash<1000 and tid= ?" );
    if (tid % 11 == 1) {
      ps1.setInt(1, tid);
      ps2.setInt(1, tid);
      ps3.setInt(1, tid);
      
      ps1.execute();
      //int num = ps2.executeUpdate();
      ps3.execute();
      
      //System.out.println("ps2 warning is " + ps2.getWarnings() + " ps2 updated " + num + " of rows.");
           
    } else {
      ps1.setInt(1, tid);
      ps2.setInt(1, tid);
      ps3.setInt(1, tid);

      ps1.execute();
      int num = ps2.executeUpdate();
      ps3.execute();

      System.out.println("ps2 warning is " + ps2.getWarnings() + " ps2 updated " + num + " of rows.");
    }
    conn.close();
  }
 
  //all record will multiply same amount
  public static void addInterest2() throws SQLException {
    Connection conn = getDefaultConnection();
    PreparedStatement ps1 = conn.prepareStatement("update trade.networth set cash=trade.multiply(cash)" );   
    ps1.execute();
    conn.close();
  }
  
  
  //one input param, one output param, one inout param, and two resultSets
  //need to retry during HA tests
  public static void testInOutParam(int tid, BigDecimal[] maxCash, int[] inOut, ResultSet[] rs, ResultSet[] rs2) throws SQLException {
    boolean[] success = new boolean[1];
    testInOutParam(tid, maxCash, inOut, rs, rs2, success);
    while (!success[0]) testInOutParam(tid, maxCash, inOut, rs, rs2, success);    
  }
  
  //one input param, one output param, one inout param, and two resultSets
  //retrying during HA tests
  private static void testInOutParam(int tid, BigDecimal[] maxCash, int[] inOut, 
      ResultSet[] rs, ResultSet[] rs2, boolean[] success) throws SQLException {
    Connection conn = getDefaultConnection();
    success[0] = true;
    try {
      PreparedStatement ps1 = conn.prepareStatement("select * from trade.networth where tid= ? order by cash desc" );
      ps1.setInt(1, tid);
      rs[0] = ps1.executeQuery();  
      
      PreparedStatement ps2 = conn.prepareStatement("select * from trade.portfolio where tid= ?" );
      ps2.setInt(1, tid);
      rs2[0] = ps2.executeQuery();
      
      PreparedStatement ps3 = conn.prepareStatement("select max(cash + securities-(loanlimit-availloan)) " +
          " as net from trade.networth where tid = ?");
      ps3.setInt(1, tid);
      ResultSet rs3 = ps3.executeQuery();
        
      if (rs3.next()) {
        maxCash[0] = rs3.getBigDecimal("NET");      
      }
          
      inOut[0]++;
      System.out.println("inOut is " + inOut[0]);
    } catch (SQLException se){
      if (se.getSQLState().equals("X0Z01")) {
        System.out.println("remote node is down during the query, need to retry");
        success[0] = false;
      } else throw se;
    }
    
    //conn.close();
  }
    
}

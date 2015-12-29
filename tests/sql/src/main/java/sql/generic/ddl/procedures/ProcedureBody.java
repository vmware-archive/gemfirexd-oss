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
package sql.generic.ddl.procedures;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import sql.SQLHelper;

public class ProcedureBody {

  public static Connection getDefaultConnection() {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:default:connection");
      // Log.getLogWriter().info("Connection in - getDefaultConnection " +
      // conn.getMetaData().getDriverName());
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return conn;
  }

  // let the calling procedure handling the exception instead for this procedure
  // call
  public static void selectCustomers(int tid, ResultSet[] rs)
      throws SQLException {
    // Log.getLogWriter().info("I am inside the selectCustomers" );
    Connection conn = getDefaultConnection();
    PreparedStatement ps2 = conn
        .prepareStatement("select count(*) from trade.customers where tid= ?");
    ps2.setInt(1, tid);
    ResultSet rsCount = ps2.executeQuery();
    if (rsCount.next())
      System.out.println("total rown in db " + rsCount.getInt(1));

    PreparedStatement ps1 = conn
        .prepareStatement("select * from trade.customers where tid= ?");
    ps1.setInt(1, tid);
    rs[0] = ps1.executeQuery();
    if (rs[0] != null)
      System.out.println("resultset in the method " + rs[0]);
    System.out.println("getting metdata" + rs[0].getMetaData());
    conn.close();
  }

  public static void addInterest(int tid) throws SQLException {
    Connection conn = getDefaultConnection();
    // PreparedStatement ps1 =
    // conn.prepareStatement("update trade.networth set cash=cash*1.01 where cash>=10000 and cash < 1000000 and tid= ?"
    // );
    PreparedStatement ps1 = conn
        .prepareStatement("update trade.networth set cash=trade.multiply(cash) where cash>=10000 and cash < 1000000 and tid= ?");
    PreparedStatement ps2 = conn
        .prepareStatement("update trade.networth set cash=cash*1.02 where cash<10000 and cash >=1000 and tid= ?");
    PreparedStatement ps3 = conn
        .prepareStatement("update trade.networth set cash=cash*1.03 where cash<1000 and tid= ?");
    if (tid % 11 == 1) {
      ps1.setInt(1, tid);
      ps2.setInt(1, tid);
      ps3.setInt(1, tid);

      ps1.execute();
      // int num = ps2.executeUpdate();
      ps3.execute();

      // System.out.println("ps2 warning is " + ps2.getWarnings() +
      // " ps2 updated " + num + " of rows.");

    }
    else {
      ps1.setInt(1, tid);
      ps2.setInt(1, tid);
      ps3.setInt(1, tid);

      ps1.execute();
      int num = ps2.executeUpdate();
      ps3.execute();

      System.out.println("ps2 warning is " + ps2.getWarnings()
          + " ps2 updated " + num + " of rows.");
    }
    conn.close();
  }

  // one input param, one output param, one inout param, and two resultSets
  // need to retry during HA tests
  public static void testInOutParam(int tid, BigDecimal[] maxCash, int[] inOut,
      ResultSet[] rs, ResultSet[] rs2) throws SQLException {
    boolean[] success = new boolean[1];
    testInOutParam(tid, maxCash, inOut, rs, rs2, success);
    while (!success[0])
      testInOutParam(tid, maxCash, inOut, rs, rs2, success);
  }

  // one input param, one output param, one inout param, and two resultSets
  // retrying during HA tests
  private static void testInOutParam(int tid, BigDecimal[] maxCash,
      int[] inOut, ResultSet[] rs, ResultSet[] rs2, boolean[] success)
      throws SQLException {
    Connection conn = getDefaultConnection();
    success[0] = true;
    try {
      PreparedStatement ps1 = conn
          .prepareStatement("select * from trade.networth where tid= ? order by cash desc");
      ps1.setInt(1, tid);
      rs[0] = ps1.executeQuery();

      PreparedStatement ps2 = conn
          .prepareStatement("select * from trade.portfolio where tid= ?");
      ps2.setInt(1, tid);
      rs2[0] = ps2.executeQuery();

      PreparedStatement ps3 = conn
          .prepareStatement("select max(cash + securities-(loanlimit-availloan)) "
              + " as net from trade.networth where tid = ?");
      ps3.setInt(1, tid);
      ResultSet rs3 = ps3.executeQuery();

      if (rs3.next()) {
        maxCash[0] = rs3.getBigDecimal("NET");
      }

      inOut[0]++;
      System.out.println("inOut is " + inOut[0]);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01")) {
        System.out
            .println("remote node is down during the query, need to retry");
        success[0] = false;
      }
      else
        throw se;
    }

    // conn.close();
  }
}

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
package sql.sqlTx.txTrigger;

import hydra.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import sql.SQLHelper;
import util.TestException;
import util.TestHelper;

public class TxTriggerProcedureTest {
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
  
  protected static void closeConnection(Connection conn) throws SQLException {    
    conn.close();
  }
  
  static String insertsql = "insert into trade.monitor values " +
  "(?, ?, ?, ?, ?, ? )" ;
  
  static String updatgfxd = "update trade.monitor set updateCount = updateCount + 1 " +
  "where tname = ? and pk1 = ?";
  
  static String deletgfxd = "update trade.monitor set deleteCount = deleteCount + 1 " +
  "where tname = ? and pk1 = ? ";
  
  public static final int SELL = -100;
  public static final int BUY = -200;
  
  public static void insertSingleKeyTable(String tableName, int pk1) throws SQLException {
    Connection conn = getDefaultConnection();
   
    PreparedStatement ps = conn.prepareStatement(insertsql);
  
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    ps.setInt(3, -1);
    ps.setInt(4, 1);
    ps.setInt(5, 0);
    ps.setInt(6, 0);
    Log.getLogWriter().info("insert into trade.monitor values("
        + tableName + ", " + pk1 + ", -1, 1, 0, 0 )");
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }

    closeConnection(conn);
  }
  
  public static void updateSingleKeyTable(String tableName, int pk1) throws SQLException {
    Connection conn = getDefaultConnection();

    PreparedStatement ps = conn.prepareStatement(updatgfxd);
  
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    Log.getLogWriter().info(updatgfxd + " for " + tableName + " and pk1 " + pk1 );
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }

    closeConnection(conn);
  }
  
  public static void deleteSingleKeyTable(String tableName, int pk1) throws SQLException {
    Connection conn = getDefaultConnection();

    PreparedStatement ps = conn.prepareStatement(deletgfxd);
  
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    Log.getLogWriter().info(deletgfxd + " for " + tableName + " and pk1 " + pk1 );
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }

    closeConnection(conn);
  }
  
  public static void insertPortfolio(String tableName, int pk1, int pk2) 
  throws SQLException {
    Connection conn = getDefaultConnection();
    String sql = "select * from trade.monitor where tname = '" + tableName
    		+ "' and pk1 = " + pk1 + " and pk2 = " + pk2;
    Log.getLogWriter().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs.next()) {
      validateDelete(rs);
      reInsertedTable(conn, tableName, pk1, pk2);
      //portfolio key could be deleted and insert again
    } else {
      newlyInsertedTable(conn, tableName, pk1, pk2);
    }

    closeConnection(conn);
  }
  
//for portfolio and txhistory
  private static void reInsertedTable(Connection conn, String tableName,
      int pk1, int pk2) throws SQLException {
    String reinsertsql = "update trade.monitor set insertCount = insertCount + 1 " +
    "where tname = ? and pk1 = ? and pk2 = ?";
    PreparedStatement ps = conn.prepareStatement(reinsertsql);
    
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    ps.setInt(3, pk2);
    Log.getLogWriter().info(reinsertsql + " for " + tableName + " and pk1 " + pk1 
        + " and pk2 " + pk2);
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }
    
  }
  
  //for portfolio and txhistory
  private static void newlyInsertedTable(Connection conn,
      String tableName, int pk1, int pk2) throws SQLException {
    PreparedStatement ps = conn.prepareStatement(insertsql);
    
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    ps.setInt(3, pk2);
    ps.setInt(4, 1);
    ps.setInt(5, 0);
    ps.setInt(6, 0);
    Log.getLogWriter().info("insert into trade.monitor values("
        + tableName + ", " + pk1 + ", " + pk2 + ", 1, 0, 0 )");
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }
  }
  
  public static void updatePortfolio(String tableName, int pk1, int pk2) throws SQLException {
    Connection conn = getDefaultConnection();
    String updatgfxdPortfolio = "update trade.monitor set updateCount = updateCount + 1 " +
    "where tname = ? and pk1 = ? and pk2 = ?";

    PreparedStatement ps = conn.prepareStatement(updatgfxdPortfolio);
  
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    ps.setInt(3, pk2);
    Log.getLogWriter().info(updatgfxdPortfolio + " for " + tableName + " and pk1 " + pk1 
        + " and pk2 " + pk2);
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }

    closeConnection(conn);
  }
  
  public static void deletePortfolio(String tableName, int pk1, int pk2)
  throws SQLException {
    Connection conn = getDefaultConnection();
    
    String deletgfxdPortfolio = "update trade.monitor set deleteCount = deleteCount + 1 " +
    "where tname = ? and pk1 = ? and pk2 = ?";
    PreparedStatement ps = conn.prepareStatement(deletgfxdPortfolio);
  
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    ps.setInt(3, pk2);
    Log.getLogWriter().info(deletgfxdPortfolio + " for " + tableName + " and pk1 " + pk1 
        + " and pk2 " + pk2);
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }

    closeConnection(conn);
  } 
  
  public static void insertTxhistory(String tableName, int pk1, String type) 
  throws SQLException {
    Connection conn = getDefaultConnection();
    int pk2 = type.equalsIgnoreCase("sell")? SELL : BUY;
    String sql = "select * from trade.monitor where tname = '" + tableName
        + "' and pk1 = " + pk1 + " and pk2 = " + pk2;
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs.next()) {
      reInsertedTable(conn, tableName, pk1, pk2);
      //portfolio key could be deleted and insert again
    } else {
      newlyInsertedTable(conn, tableName, pk1, pk2);
    }

    closeConnection(conn);
  }
  
  public static void updateTxhistory(String tableName, int pk1, String type) throws SQLException {
    Connection conn = getDefaultConnection();
    int pk2 = type.equalsIgnoreCase("sell")? SELL : BUY;
    String updatgfxdTxhistory = "update trade.monitor set updateCount = updateCount + 1 " +
    "where tname = ? and pk1 = ? and pk2 = ?";

    PreparedStatement ps = conn.prepareStatement(updatgfxdTxhistory);
  
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    ps.setInt(3, pk2);
    Log.getLogWriter().info(updatgfxdTxhistory + " for " + tableName + " and pk1 " + pk1 
        + " and pk2 " + pk2);
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }

    closeConnection(conn);
  }
  
  public static void deleteTxhistory(String tableName, int pk1, String type)
  throws SQLException {
    Connection conn = getDefaultConnection();
    int pk2 = type.equalsIgnoreCase("sell")? SELL : BUY;
    
    String deletgfxdTxhistory = "update trade.monitor set deleteCount = deleteCount + 1 " +
    "where tname = ? and pk1 = ? and pk2 = ?";
    PreparedStatement ps = conn.prepareStatement(deletgfxdTxhistory);
  
    ps.setString(1, tableName);
    ps.setInt(2, pk1);
    ps.setInt(3, pk2);
    Log.getLogWriter().info(deletgfxdTxhistory + " for " + tableName + " and pk1 " + pk1 
        + " and pk2 " + pk2);
    
    try {
      ps.execute();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        throw new TestException("Got unexpected conflict exception in trigger" 
            + TestHelper.getStackTrace(se));
      } else throw se;
    }

    closeConnection(conn);
  } 
  
  public static void insertNetworth(String tableName, int pk1) 
  throws SQLException {
    Connection conn = getDefaultConnection();
    int pk2 = -1;
    String sql = "select * from trade.monitor where tname = '" + tableName
        + "' and pk1 = " + pk1 ;
    Log.getLogWriter().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs.next()) {
      validateDelete(rs);
      reInsertedTable(conn, tableName, pk1, pk2);
      //could reinsert any networth, but should be caught by constraint check
    } else {
      newlyInsertedTable(conn, tableName, pk1, pk2);
    }

    closeConnection(conn);
  }
  
  protected static void validateDelete(ResultSet rs) throws SQLException {    
    int deleteCount = rs.getInt("DELETECOUNT");
    int insertCount = rs.getInt("INSERTCOUNT");
    Log.getLogWriter().info("deleteCount is " + deleteCount + 
        " and insertCount is " + insertCount);
    if (deleteCount != insertCount || deleteCount < 1) {
      throw new TestException ("the originial inserted row has not been deleted" +
      		" and a new insert passes constraint check in after trigger. The row" +
      		" in question is " + rs.getString("TNAME") + " and pk1 " + rs.getInt("PK1") +
      		" and pk2 " + rs.getInt("PK2") + " the deletecCount is " + deleteCount + 
      		" but insertCount is " + insertCount);
    }
  }
  
}

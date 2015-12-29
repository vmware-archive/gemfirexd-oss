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
package sql.sqlDAP;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import sql.SQLHelper;

public class DerbyDAPTest {
  private static String selectBuyOrderByTidListSql = "select * " +
  "from trade.buyorders where oid <? and tid= ?";
  private static String selectPortfolioByCidRangeSql = "select * " +
  "from trade.portfolio where cid >? and cid <? and sid<? and tid= ?";
  private static String selectPortfolioByCidRangeSql2 = "select cid, sid, subTotal " +
  "from trade.portfolio where cid >? and cid <? and tid= ?";
  private static String updatePortfolioByCidRangeSql = "update trade.portfolio " +
  "set subTotal=? where cid> ? and cid < ? and tid=?";
  private static String updateSellordersSGSql = "update trade.sellorders set order_time=? " +
  "where tid = ?";
  
  protected static Connection getDerbyDefaultConnection() {
    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:default:connection");
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return conn;
  }
  
  public static void selectDerbyBuyordersByTidList(int oid, int tid, ResultSet[] rs) throws SQLException {
    Connection conn = getDerbyDefaultConnection();
    PreparedStatement ps1 = conn.prepareStatement(selectBuyOrderByTidListSql);
    ps1.setInt(1, oid);
    ps1.setInt(2, tid);
  
    rs[0] = ps1.executeQuery();    
  
    conn.close();
  }
  
  public static void getListOfDerbyBuyordersByTidList(int oid, int tid, ResultSet[] rs)
      throws SQLException {
    Connection conn = getDerbyDefaultConnection();
    PreparedStatement ps1 = conn.prepareStatement(selectBuyOrderByTidListSql);
    ps1.setInt(1, oid);
    ps1.setInt(2, tid);
    
    rs[0] = ps1.executeQuery();    
    
    conn.close();
  }
  
  public static void selectDerbyPortfolioByCidRange(int cid1, int cid2, int sid, int tid, 
      int[] data, ResultSet[] rs, ResultSet[] rs2, ResultSet[] rs3, ResultSet[] rs4) 
      throws SQLException {
    Connection conn = getDerbyDefaultConnection();
    PreparedStatement ps1 = conn.prepareStatement(selectPortfolioByCidRangeSql);
    ps1.setInt(1, cid1);
    ps1.setInt(2, cid2);
    ps1.setInt(3, sid);
    ps1.setInt(4, tid);

    rs[0] = ps1.executeQuery();    
    
    PreparedStatement ps2 = conn.prepareStatement(selectPortfolioByCidRangeSql2);
    ps2.setInt(1, cid1);
    ps2.setInt(2, cid2);
    ps2.setInt(3, tid);

    rs2[0] = ps2.executeQuery();    
    
    data[0] = 1;
    
    conn.close();
  }
  
  public static void updateDerbyPortfolioByCidRange(int cid1, int cid2, 
      BigDecimal subTotal, int tid) throws SQLException {
    Connection conn = getDerbyDefaultConnection();
    PreparedStatement ps = conn.prepareStatement(updatePortfolioByCidRangeSql);
    ps.setBigDecimal(1, subTotal);
    ps.setInt(2, cid1);
    ps.setInt(3, cid2);
    ps.setInt(4, tid);
    ps.executeUpdate();
    
    conn.close();

  }
  
  public static void updateDerbySellordersSG(Timestamp orderTime, int tid) 
      throws SQLException{
    Connection conn = getDerbyDefaultConnection();
    PreparedStatement ps = conn.prepareStatement(updateSellordersSGSql);
    ps.setTimestamp(1, orderTime);
    ps.setInt(2, tid);
    ps.executeUpdate();
    
    conn.close();

  }
  
  public static void updateDerbyPortfolioWrongSG(BigDecimal subTotal, int qty) {
    //nothing to do in derby
  }
  
  public static void customDerbyProc(int cid1, int cid2, int tid) {
    //To be coded
  }

}

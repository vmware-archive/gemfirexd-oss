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

//import hydra.Log;

import java.sql.*;
import java.util.Calendar;
import java.math.BigDecimal;

/**
 * @author eshu
 *
 */
public class FunctionTest {
  protected static int maxNumOfTries = 1;
  protected static Connection getDefaultConnection() throws SQLException {
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
  
  protected static Connection getDerbyConnection() throws SQLException {
    return ClientDiscDBManager.getConnection();
  }

  public static ResultSet readPortfolioDerby(int tid) throws SQLException {
    Connection conn = getDerbyConnection();
    PreparedStatement ps1 = conn.prepareStatement("select * from trade.portfolio where tid = ?" );
    ps1.setInt(1, tid);
    return ps1.executeQuery();    
  }
  
  public static ResultSet readPortfolioGfxd(int tid) throws SQLException {
    Connection conn = getGfxdConnection();
    PreparedStatement ps1 = conn.prepareStatement("select * from trade.portfolio where tid = ?" );
    ps1.setInt(1, tid);
    return ps1.executeQuery();    
  }
  
  protected static Connection getGfxdConnection() throws SQLException {
    return GFEDBManager.getConnection();
  }
  
  public static BigDecimal multiply(BigDecimal cash)  {
    return cash.multiply(new BigDecimal(Double.toString(1.01)));
  }
  
  public static BigDecimal multiply2(BigDecimal cash)  {
    return cash.multiply(new BigDecimal(Double.toString(1.001)));
  }
    
  public static int getMaxCid(int tid) throws SQLException {
    boolean[] success = new boolean[1];
    int cid =0;
    cid = getMaxCid(tid, success);
    while (!success[0]) {
      cid = getMaxCid(tid, success);
    }
    return cid;
  }
  
  //retrying for HA test when node went down
  private static int getMaxCid(int tid, boolean[] success) throws SQLException {
    int cid = 0;
    Connection conn = getDefaultConnection();
    PreparedStatement ps1 = null;
    success[0] = true;
    try {  
      ps1 = conn.prepareStatement("select max(cid) as lastcid from trade.networth where tid= ? " );
      ps1.setInt(1, tid);
      ResultSet rs = ps1.executeQuery();
      if (rs.next()) {
        cid = rs.getInt("LASTCID");
      }
      rs.close();
      conn.close();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01")) {
        System.out.println("remote node is down during the query, need to retry");
        success[0] = false;
      } else throw se;
    }
    return cid;
  }
  
  public static int month(Date since) {
    Calendar c = Calendar.getInstance();
    c.setTime(since);
    return c.get(Calendar.MONTH);    
  }
}

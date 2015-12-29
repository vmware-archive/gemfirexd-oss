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
package sql.loader;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import sql.ClientDiscDBManager;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import util.TestException;

import com.pivotal.gemfirexd.callbacks.RowLoader;

public class BuyOrdersLoader implements RowLoader {

  private Connection dConn;
  private Object[] params = new Object[1];
  private boolean testSecurity = SQLTest.testSecurity;

  public BuyOrdersLoader() {
    if (!sql.SQLTest.hasDerbyServer) return; //do not init derby connection
    int retries = 5;
    while (true) {
      try {
        if (!testSecurity)
          this.dConn = ClientDiscDBManager.getConnection();
        else 
          this.dConn = ClientDiscDBManager.getSuperUserConnection();
        break;
      } catch (SQLException ex) {
        if (!"08003".equals(ex.getSQLState()) || retries-- <= 0) {
          throw new TestException("failed while getting a connection", ex);
        }
        // wait and retry to get connection again
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public static BuyOrdersLoader createRowLoader() {
    return new BuyOrdersLoader();
  }

  public Object getRow(String schemaName, String tableName, Object[] primarykey) throws SQLException{
    if (params != null) {
      //Log.getLogWriter().info("params in getRow is" + params);
      if ( params.length> 0 && params[0] != null) { 
        if (((String)params[0]).contains("createRandomRow")) { 
          return getCreatedRow(primarykey);
        }
      }
    }
  	Log.getLogWriter().info("in BuyOrderLoader getValuForColumns");
    String select = "select * from trade.buyorders where oid = ?";
    Object[] row = null;
    // Log.getLogWriter().info("primarykey length is " + primarykey.length);
    //no need after fix of #40699
    // DataValueDescriptor dvd[] = ((GemFireKey)primarykey[0]).getKey();

    try {
      Log.getLogWriter().info("primary key is " + primarykey[0]);
      PreparedStatement ps = this.dConn.prepareStatement(select);
      ps.setInt(1, (Integer)primarykey[0]); // non composite primary key

      ResultSet rs = ps.executeQuery();
      return rs;
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return row;
  }
  
  private Object getCreatedRow(Object[] primaryKey){
    /*
     * trade.orders table fields
     *   int oid;
     *   int cid; //ref customers
     *   int sid; //ref securities sec_id
     *   int qty;
     *   BigDecimal bid;
     *   long ordertime;
     *   String stauts;
     *   int tid; //for update or delete unique records to the thread
     */
  	int initMaxQty = 2000; 
  	int maxTids = 20;
  	Object[] row = new Object[8];
  	row[0] = primaryKey[0];
  	row[1] = sql.SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary))+1;
  	row[2] = sql.SQLTest.random.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary))+1;
  	row[3] = sql.SQLTest.random.nextInt(initMaxQty);
  	row[4] = new BigDecimal (Double.toString((sql.SQLTest.random.nextInt(10000)+1) * .01));
  	row[5] = new Timestamp(System.currentTimeMillis());
  	row[6] = "open";
  	row[7] = sql.SQLTest.random.nextInt(maxTids);
  	Log.getLogWriter().info("loader getCreatedrow creates the following row," +
  			" oid " + row[0] + " cid " + row[1] + " sid " + row[2] + " qty " + row[3] + 
  			" bid " + row[4] + " ordertime " + row[5] + " status " + row[6] + " tid " + row[7]);
  	
  	return row;
  }

  public void init(String initStr) throws SQLException {
    params[0] = initStr;
  }
}

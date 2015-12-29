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
package sql.subqueryStatements;

import hydra.Log;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLBB;
import sql.SQLHelper;
import sql.dmlStatements.AbstractDMLStmt;
import sql.joinStatements.SecuritiesPortfolioJoinStmt;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class SecuritiesPortfolioSubqueryStmt extends
		SecuritiesPortfolioJoinStmt implements SubqueryStmtIF {
  /*
   * trade.securities table fields
   *   private int sec_id;
   *   String symbol;
   *   String exchange;
   *   BigDecimal price;
   *   int tid; //for update or delete unique records to the thread
   *
   * trade.Portfolio table fields
   *   int cid;
   *   int sid;
   *   int qty;
   *   int availQty;
   *   BigDecimal subTotal;
   *   int tid; //for update or delete unique records to the thread
   */
  

  protected static String[] uniqSelect = 
  {
  	"select * from trade.securities where sec_id IN " +
			"(select sid from trade.portfolio where tid = ? and cid >?) ",
		"select sec_id, symbol, price, tid from trade.securities s where price >" +
	  	"(select (Avg(subTotal/qty)) from trade.portfolio f where " +
  	  "sec_id = f.sid and f.tid =? and qty <> 0) and tid =? ",
  	"select sec_id, symbol, price, exchange from trade.securities s where sec_id IN " +
	  	"(select sid from trade.portfolio f where " +
  	  "tid =? GROUP BY sid Having count(*) > 2) ",
    "select cid,  sid, qty from trade.portfolio f where " +
   		"(select price from trade.securities s where sec_id = f.sid " +
   		"and cid >? and f.tid = ? ) <? and tid = ?",
     "select cid,  sid, availQty, tid, subTotal from trade.portfolio f where " +
   		"(select symbol from trade.securities s where sec_id = f.sid " +
   		"and f.tid = ? ) like ? and cid >? and tid =?",
    };

  protected static String[] nonUniqSelect = {
  	"select * from trade.securities trade where sec_id IN " +
			"(select sid from trade.portfolio where cid >?)",
		"select sec_id, symbol, price, tid from trade.securities s where price >" +
	  	"(select (Avg(subTotal/qty)) from trade.portfolio f where " +
  	  "sec_id = f.sid and qty <> 0) ",
  	"select sec_id, symbol, price, exchange from trade.securities s where sec_id IN " +
  		"(select sid from trade.portfolio f " +
  		"GROUP BY sid Having count(*) > 2) ",
  	"select cid,  sid, qty from trade.portfolio f where " +
 			"(select price from trade.securities s where sec_id = f.sid " +
 			"and cid >?) <?",
 		"select cid,  sid, availQty, tid, subTotal from trade.portfolio f where " +
 			"(select symbol from trade.securities s where sec_id = f.sid " +
 			") like ? and cid >? ",
 		};
  
  protected static boolean isTableSecuritiesReplicated = TestConfig.tab().
    booleanAt(sql.SQLPrms.isTableSecuritiesReplicated, false);

	@Override
	public void subquery(Connection dConn, Connection gConn) {  
    int whichQuery;    
    if (dConn!=null) {
      if (testUniqueKeys) { //test uniqKeys
        whichQuery = rand.nextInt(uniqSelect.length); 
        getUniqQuery(dConn, gConn, whichQuery); 
      } else {
      	whichQuery = rand.nextInt(nonUniqSelect.length);
      	getNonUniqQuery (dConn, gConn, whichQuery);
      }
    }
    else {
      whichQuery = rand.nextInt(nonUniqSelect.length);
      getNonUniqQuery (gConn, whichQuery);   
    }  
	}
	
  @Override
  public void subqueryDelete(Connection dConn, Connection gConn) {
    //TODO to be implemented
  }
	
	protected void getUniqQuery(Connection dConn, Connection gConn, int whichQuery) { 
    ResultSet derbyRS = null;
    ResultSet gfxdRS = null;
    int tid = getMyTid();
    int cid = AbstractDMLStmt.getCid();
    BigDecimal price = getPrice();
    StringBuilder symbol = new StringBuilder();
    
    switch (whichQuery) {
    case 0:
    	derbyRS = getUniqQuery0(dConn, whichQuery, cid, tid);  // to reproduce #42428
    	gfxdRS = getUniqQuery0 (gConn, whichQuery, cid, tid);
    	break;
    case 1: 
    	derbyRS = getUniqQuery1(dConn, whichQuery, tid);  //to reproduce #42413
    	gfxdRS = getUniqQuery1 (gConn, whichQuery, tid);
    	break;
    case 2: //same as uniqQuery1, reuse the code
    	derbyRS = getUniqQuery2(dConn, whichQuery, tid);  //to reproduce #42414
    	gfxdRS = getUniqQuery2 (gConn, whichQuery, tid);
    	break;
    case 3: 
    	derbyRS = getUniqQuery3(dConn, whichQuery, cid, price, tid);
    	gfxdRS = getUniqQuery3 (gConn, whichQuery, cid, price, tid);
    	break;
    case 4: 
    	getDataForQuery4(symbol);
    	derbyRS = getUniqQuery4(dConn, whichQuery, cid, symbol.toString(), tid);
    	gfxdRS = getUniqQuery4 (gConn, whichQuery, cid, symbol.toString(), tid);
    	break;
    default:
    	throw new TestException ("invalid uinque quey selection");
    }
       
    if (derbyRS == null) {
      Log.getLogWriter().info("Could not get query results from derby.");
      return;
    }
    List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
    if (derbyList == null) {
      Log.getLogWriter().info("Not able to convert derby resultSet to a list");
      return;
    }
    
    if (gfxdRS==null && !isHATest) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    List<Struct> gfxdList = ResultSetHelper.asList(gfxdRS, false);
    ResultSetHelper.compareResultSets(derbyList, gfxdList);   
	}
	
  private ResultSet getUniqQuery0(Connection conn, int whichQuery,  
      int cid, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery0(conn, whichQuery, cid, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery0(conn, whichQuery, cid, tid, success);
    } //retry
    return rs;
  }
  
  //  	"select * from trade.securities trade where sec_id IN " +
	// "(select sid from trade.portfolio where tid = ? and cid >?) ",
  private ResultSet getUniqQuery0(Connection conn, int whichQuery,  
      int cid, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- tid: "+ tid + " cid: " + cid);
      stmt.setInt(1, tid);
      stmt.setInt(2, cid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getUniqQuery1(Connection conn, int whichQuery,  
      int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery1(conn, whichQuery, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery1(conn, whichQuery, tid, success);
    } //retry
    return rs;
  }
  
  @SuppressWarnings("unchecked")
  private ResultSet getUniqQuery1(Connection conn, int whichQuery,  
      int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- tid: "+ tid);
      stmt.setInt(1, tid);
      stmt.setInt(2, tid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else if (se.getSQLState().equals("0A000") && se.getMessage().contains("Correlated" +
      		" query with outer query on replicated table and inner on partitioned table is unsupported")) {
        ArrayList<String> securitiesPartition = (ArrayList<String>)SQLBB.getBB().
          getSharedMap().get("securitiesPartition");
        if ((securitiesPartition != null && securitiesPartition.size() == 0) || isTableSecuritiesReplicated) {
          Log.getLogWriter().info("Correlated query with outer query on replicated table and inner on " +
          		"partitioned table is unsupported");
          success[0]= false;
        } else {
          throw new TestException ("get unexpected 0A000 as outer table is not replicate " +
          		"table " + TestHelper.getStackTrace(se));
        }
      }
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getUniqQuery2(Connection conn, int whichQuery,  
      int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery2(conn, whichQuery, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery2(conn, whichQuery, tid, success);
    } //retry
    return rs;
  }
  
  private ResultSet getUniqQuery2(Connection conn, int whichQuery,  
      int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- tid: "+ tid);
      stmt.setInt(1, tid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private void getDataForQuery4(StringBuilder symbol) {
  	int length = 1;
  	symbol.append('_' + getRandVarChar(length) + '%');
  }
  
  private ResultSet getUniqQuery3(Connection conn, int whichQuery,  
      int cid, BigDecimal price, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery3(conn, whichQuery, cid, price, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery3(conn, whichQuery, cid, price, tid, success);
    } //retry
    return rs;
  }
  
  //"select cid,  sid, qty from trade.portfolio f where " +
	//"(select price from trade.securities s where sec_id = f.sid " +
	//"and cid >? and f.tid = ? ) <?",
  private ResultSet getUniqQuery3(Connection conn, int whichQuery,  
      int cid, BigDecimal price, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query price " + price + " and tid: "+ tid
      		+ " and cid: " + cid);
      stmt.setInt(1, cid);
      stmt.setBigDecimal(3, price);
      stmt.setInt(2, tid);
      stmt.setInt(4, tid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else if (se.getSQLState().equalsIgnoreCase("0A000") && whichQuery == 3) success[0] = true;
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }  

  
  //  "select cid,  sid, availQty, tid, subTotal from trade.portfolio f where " +
	//	"(select symbol from trade.securities s where sec_id = f.sid " +
 	//	"and f.tid = ? ) like ? and cid >? ",
  private ResultSet getUniqQuery4(Connection conn, int whichQuery,  
      int cid, String symbol, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery4(conn, whichQuery, cid, symbol, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery4(conn, whichQuery, cid, symbol, tid, success);
    } //retry
    return rs;
  }
  
  private ResultSet getUniqQuery4(Connection conn, int whichQuery,  
      int cid, String symbol, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query symbol " + symbol + " and tid: "+ tid
      		+ " and cid: " + cid);
      stmt.setInt(1, tid);
      stmt.setString(2, symbol);
      stmt.setInt(3, cid);
      stmt.setInt(4, tid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }  
  
	protected void getNonUniqQuery(Connection dConn, Connection gConn, int whichQuery) { 
    ResultSet derbyRS = null;
    ResultSet gfxdRS = null;
    int cid = AbstractDMLStmt.getCid();
    BigDecimal price = getPrice();
    StringBuilder symbol = new StringBuilder();
    
    switch (whichQuery) {
    case 0:
    	derbyRS = getNonUniqQuery0(dConn, whichQuery, cid);    // to reproduce #42428
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery, cid);
    	break;
    case 1: 
    	derbyRS = getNonUniqQuery1(dConn, whichQuery);		//to reproduce #42413
    	gfxdRS = getNonUniqQuery1 (gConn, whichQuery);
    	break;
    case 2: //same as Query1, reuse the code
    	derbyRS = getNonUniqQuery1(dConn, whichQuery);   //to reproduce #42414
    	gfxdRS = getNonUniqQuery1 (gConn, whichQuery);
    	break;
    case 3: 
    	derbyRS = getNonUniqQuery3(dConn, whichQuery, cid, price);
    	gfxdRS = getNonUniqQuery3 (gConn, whichQuery, cid, price);
    	break;
    case 4: 
    	getDataForQuery4(symbol);
    	derbyRS = getNonUniqQuery4(dConn, whichQuery, cid, symbol.toString());
    	gfxdRS = getNonUniqQuery4 (gConn, whichQuery, cid, symbol.toString());
    	break;
    default:
    	throw new TestException ("invalid non uinque quey selection");
    }
       
    if (derbyRS == null) {
      Log.getLogWriter().info("Could not get query results from derby.");
      return;
    }
    List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
    if (derbyList == null) {
      Log.getLogWriter().info("Not able to convert derby resultSet to a list");
      return;
    }
    
    if (gfxdRS==null && !isHATest) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    List<Struct> gfxdList = ResultSetHelper.asList(gfxdRS, false);
    ResultSetHelper.compareResultSets(derbyList, gfxdList);   
	}
	
  private ResultSet getNonUniqQuery0(Connection conn, int whichQuery,  
      int cid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery0(conn, whichQuery, cid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery0(conn, whichQuery, cid, success);
    } //retry
    return rs;
  }
  
  private ResultSet getNonUniqQuery0(Connection conn, int whichQuery,  
      int cid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- cid: " + cid);
      stmt.setInt(1, cid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getNonUniqQuery1(Connection conn, int whichQuery) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery1(conn, whichQuery, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery1(conn, whichQuery, success);
    } //retry
    return rs;
  }
  
  @SuppressWarnings("unchecked")
  private ResultSet getNonUniqQuery1(Connection conn, int whichQuery,  
      boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- none");
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else if (se.getSQLState().equals("0A000") && se.getMessage().contains("Correlated" +
        " query with outer query on replicated table and inner on partitioned table is unsupported")) {
        ArrayList<String> securitiesPartition = (ArrayList<String>)SQLBB.getBB().
          getSharedMap().get("securitiesPartition");
        if ((securitiesPartition != null && securitiesPartition.size() == 0) || isTableSecuritiesReplicated) {
          Log.getLogWriter().info("Correlated query with outer query on replicated table and inner on " +
              "partitioned table is unsupported");
          success[0]= false;
        } else {
          throw new TestException ("get unexpected 0A000 as outer table is not replicate " +
              "table " + TestHelper.getStackTrace(se));
        }
      }
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getNonUniqQuery3(Connection conn, int whichQuery,  
      int cid, BigDecimal price) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery3(conn, whichQuery, cid, price, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery3(conn, whichQuery, cid, price, success);
    } //retry
    return rs;
  }

  private ResultSet getNonUniqQuery3(Connection conn, int whichQuery,  
      int cid, BigDecimal price, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query price " + price 
      		+ " and cid: " + cid);
      stmt.setInt(1, cid);
      stmt.setBigDecimal(2, price);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }  

  private ResultSet getNonUniqQuery4(Connection conn, int whichQuery,  
      int cid, String symbol) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery4(conn, whichQuery, cid, symbol, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery4(conn, whichQuery, cid, symbol, success);
    } //retry
    return rs;
  }
  
  //"select cid,  sid, availQty, tid, subTotal from trade.portfolio f where " +
	//	"(select symbol from trade.securities s where sec_id = f.sid " +
	//		") like ? and cid >? ",
  private ResultSet getNonUniqQuery4(Connection conn, int whichQuery,  
      int cid, String symbol, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query symbol " + symbol
      		+ " and cid: " + cid);
      stmt.setString(1, symbol);
      stmt.setInt(2, cid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else if (se.getSQLState().equalsIgnoreCase("0A000") && whichQuery == 3) success[0] = true;
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }

	protected void getNonUniqQuery(Connection gConn, int whichQuery) { 
    ResultSet gfxdRS;
    int cid = AbstractDMLStmt.getCid();
    BigDecimal price = getPrice();
    StringBuilder symbol = new StringBuilder();
    
    switch (whichQuery) {
    case 0:
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery, cid);
    	break;
    case 1: 
    	gfxdRS = getNonUniqQuery1 (gConn, whichQuery);
    	break;
    case 2: //same as Query1, reuse the code
    	gfxdRS = getNonUniqQuery1 (gConn, whichQuery);
    	break;
    case 3: 
    	gfxdRS = getNonUniqQuery3 (gConn, whichQuery, cid, price);
    	break;
    case 4: 
    	getDataForQuery4(symbol);
    	gfxdRS = getNonUniqQuery4 (gConn, whichQuery, cid, symbol.toString());
    	break;
    default:
    	throw new TestException ("invalid non uinque quey selection");
    }
          
    if (gfxdRS==null && !isHATest) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    ResultSetHelper.asList(gfxdRS, false);
	}
}

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLHelper;
import sql.joinStatements.CustomersNetworthJoinStmt;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public class CustomersNetworthSubqueryStmt extends CustomersNetworthJoinStmt
		implements SubqueryStmtIF {

  protected static String[] uniqSelect = 
  { "select * from trade.customers c where c.tid = ? and c.cid " +
  		"IN (select cid from trade.networth n where tid =? and " +
  		"(cash + securities - (loanLimit - availloan)) > 5000 )",
  	"select cid, cust_name, since from trade.customers c where " +
  	  "(select (cash + securities) from trade.networth n where " +
  	  "c.cid = n.cid and n.tid =?) > 5000 ",
    "select cid, cash, securities from trade.networth n where " +
  	  "(select (cust_name) from trade.customers c where " +
  	  "c.cid = n.cid and c.tid =?) like ? ",
    "select cid, cash, securities from trade.networth n where " +
  	  "(select (cust_name) from trade.customers c where " +
  	  "c.cid = n.cid and c.tid = n.tid and c.tid =?) like ? ",  	  
  		
  };

  protected static String[] nonUniqSelect = 
  {
  	"select * from trade.customers c where c.cid " +
			"IN (select cid from trade.networth n where " +
			"(cash + securities - (loanLimit - availloan)) > 5000 )",
	  "select cid, cust_name, since from trade.customers c where " +
  	  "(select (cash + securities) from trade.networth n where " +
  	  "c.cid = n.cid) > 5000 ",
    "select cid, cash, securities from trade.networth n where " +
  	  "(select (cust_name) from trade.customers c where " +
  	  "c.cid = n.cid) like ? ",
    "select cid, cash, securities from trade.networth n where " +
  	  "(select (cust_name) from trade.customers c where " +
  	  "c.cid = n.cid and c.tid = n.tid) like ? ",	
		
  };
  
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
    ResultSet derbyRS;
    ResultSet gfxdRS;
    int tid = getMyTid();
    StringBuilder custName = new StringBuilder();
    
    switch (whichQuery) {
    case 0:
    	derbyRS = getUniqQuery0(dConn, whichQuery, tid);
    	gfxdRS = getUniqQuery0 (gConn, whichQuery, tid);
    	break;
    case 1: //same as uniqQuery0, reuse the code
    	derbyRS = getUniqQuery1(dConn, whichQuery, tid);
    	gfxdRS = getUniqQuery1 (gConn, whichQuery, tid);
    	break;
    case 2: 
    	getDataForQuery2(custName);
    	derbyRS = getUniqQuery2(dConn, whichQuery, custName.toString(), tid);
    	gfxdRS = getUniqQuery2 (gConn, whichQuery, custName.toString(), tid);
    	break;
    case 3: 
    	getDataForQuery2(custName);
    	derbyRS = getUniqQuery2(dConn, whichQuery, custName.toString(), tid);
    	gfxdRS = getUniqQuery2 (gConn, whichQuery, custName.toString(), tid);
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
    
    if (gfxdRS==null) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    List<Struct> gfxdList = ResultSetHelper.asList(gfxdRS, false);
    ResultSetHelper.compareResultSets(derbyList, gfxdList);   
	}
	
	// only tid needs to be passed in the query,	
  private ResultSet getUniqQuery0(Connection conn, int whichQuery,  
      int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery0(conn, whichQuery, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery0(conn, whichQuery, tid, success);
    } //retry
    return rs;
  }
  
  private ResultSet getUniqQuery0(Connection conn, int whichQuery,  
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
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
	// only tid needs to be passed in the query,	
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
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private void getDataForQuery2(StringBuilder custName) {
  	int length = 2;
  	custName.append('%' + getRandVarChar(length) + '%');
  }
  
  private ResultSet getUniqQuery2(Connection conn, int whichQuery,  
      String custName, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery2(conn, whichQuery, custName, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery2(conn, whichQuery, custName, tid, success);
    } //retry
    return rs;
  }
  
  private ResultSet getUniqQuery2(Connection conn, int whichQuery,  
      String custName, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query cust_name " + custName + " and tid: "+ tid);
      stmt.setInt(1, tid);
      stmt.setString(2, custName);
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
  
  
	protected void getNonUniqQuery(Connection dConn, Connection gConn, int whichQuery) { 
    ResultSet derbyRS;
    ResultSet gfxdRS;
    StringBuilder custName = new StringBuilder();
    
    switch (whichQuery) {
    case 0:
    	derbyRS = getNonUniqQuery0(dConn, whichQuery);
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery);
    	break;
    case 1: //same as uniqQuery0, reuse the code
    	derbyRS = getNonUniqQuery0(dConn, whichQuery);
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery);
    	break;
    case 2: 
    	getDataForQuery2(custName);
    	derbyRS = getNonUniqQuery2(dConn, whichQuery, custName.toString());
    	gfxdRS = getNonUniqQuery2 (gConn, whichQuery, custName.toString());
    	break;
    case 3: 
    	getDataForQuery2(custName);
    	derbyRS = getNonUniqQuery2(dConn, whichQuery, custName.toString());
    	gfxdRS = getNonUniqQuery2 (gConn, whichQuery, custName.toString());
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
    
    if (gfxdRS==null) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    List<Struct> gfxdList = ResultSetHelper.asList(gfxdRS, false);
    ResultSetHelper.compareResultSets(derbyList, gfxdList);   
	}
	
	// only tid needs to be passed in the query,	
  private ResultSet getNonUniqQuery0(Connection conn, int whichQuery) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery0(conn, whichQuery, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery0(conn, whichQuery, success);
    } //retry
    return rs;
  }
  
  private ResultSet getNonUniqQuery0(Connection conn, int whichQuery,  
      boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("no bound data used in query");
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getNonUniqQuery2(Connection conn, int whichQuery,  
      String custName) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery2(conn, whichQuery, custName, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
      }
      count++;
      rs = getNonUniqQuery2(conn, whichQuery, custName, success);
    } //retry
    return rs;
  }
  
  private ResultSet getNonUniqQuery2(Connection conn, int whichQuery,  
      String custName, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query cust_name " + custName);
      stmt.setString(1, custName);
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
    StringBuilder custName = new StringBuilder();
    
    switch (whichQuery) {
    case 0:
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery);
    	break;
    case 1: //same as uniqQuery0, reuse the code
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery);
    	break;
    case 2: 
    	getDataForQuery2(custName);
    	gfxdRS = getNonUniqQuery2 (gConn, whichQuery, custName.toString());
    	break;
    case 3: 
    	getDataForQuery2(custName);
    	gfxdRS = getNonUniqQuery2 (gConn, whichQuery, custName.toString());
    	break;
    default:
    	throw new TestException ("invalid uinque quey selection");
    }
          
    if (gfxdRS==null && !isHATest) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    ResultSetHelper.asList(gfxdRS, false);
	}

}

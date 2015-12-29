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
import hydra.MasterController;
import hydra.blackboard.SharedMap;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import sql.dmlStatements.AbstractDMLStmt;
import sql.joinStatements.CustPortfSoJoinStmt;
import sql.security.SQLSecurityTest;
import sql.sqlTx.SQLDistTxTest;
import sql.sqlutil.ResultSetHelper;
import sql.wan.SQLWanBB;
import util.TestException;
import util.TestHelper;

public class CustPortfSoSubqueryStmt extends CustPortfSoJoinStmt implements
		SubqueryStmtIF {
  /*
   *    * trade.customers table fields
   *   private int cid;
   *   String cust_name;
   *   Date since;
   *   String addr;
   *   int tid; //for update or delete unique records
   *
   * trade.Portfolio table fields
   *   int cid;
   *   int sid;
   *   int qty;
   *   int availQty;
   *   BigDecimal subTotal;
   *   int tid; //for update or delete unique records to the thread
   *   
   * trade.orders table fields
   *   int oid;
   *   int cid; //ref customers
   *   int sid; //ref securities sec_id
   *   int qty;
   *   BigDecimal ask;
   *   Timestamp order_time;
   *   String stauts;
   *   int tid; //for update or delete unique records to the thread
   */
	
  protected static String[] uniqSelect = 
  {
  	"select * from trade.customers c where exists (select * from " +
  	  "trade.portfolio f where c.cid = f.cid and qty > 297) and tid =?",
    "select * from trade.customers c where EXISTS (select * from " +
  	  "trade.portfolio f where c.cid = f.cid and tid =?) and NOT EXISTS " +
  	  "(select * from trade.sellorders s where c.cid = s.cid and status IN " +
  	  "('open','filled'))",
  	"select * from trade.portfolio f , " +
  	 	"trade.sellorders s where f.cid = s.cid and f.tid=s.tid and ask > 49.9 " +
  	 	"and s.cid IN (select cid from trade.customers where cust_name LIKE 'name1%' and tid = ?)", 
  	"select f.cid, f.sid, f.tid, s.oid, s.tid from trade.portfolio f, trade.sellorders s " +
  		"where (f.cid = s.cid and f.tid=s.tid) and f.cid IN (select cid from " +
  		"trade.customers c where tid = ? and since > ?)",
  	"select * from trade.sellorders s where exists (select * from " +
  	  "trade.portfolio f where f.cid = s.cid and f.sid < ? and tid =? and f.cid IN " +
  	  "(select cid from trade.customers c where since <? ))",
  	"select c.cid from trade.customers c where exists (select * from trade.portfolio f " +
  	  "where f.cid = c.cid and tid =? and f.qty >927) UNION " +
  	  "select s.cid from trade.sellorders s where (select sum(qty) from trade.portfolio f " +
  	  "where f.cid = s.cid GROUP BY f.cid) > 1277 and tid =? "

    };

  protected static String[] nonUniqSelect = {
	  "select * from trade.customers c where exists (select * from " +
		  "trade.portfolio f where c.cid = f.cid and qty > 297) ",
	  "select * from trade.customers c where EXISTS (select * from " +
		  "trade.portfolio f where c.cid = f.cid) and NOT EXISTS " +
		  "(select * from trade.sellorders s where c.cid = s.cid and status IN " +
		  "('open','filled'))",
		"select s.cid, s.sid, s.qty, s.tid as soTID, ask from trade.portfolio f , " +
  	 	"trade.sellorders s where f.cid = s.cid and f.tid=s.tid and ask > 49.9 " +
  	 	"and s.cid IN (select cid from trade.customers where cust_name LIKE 'name1%')", 
		"select f.cid, f.sid, f.tid, s.oid, s.tid from trade.portfolio f, trade.sellorders s " +
			"where (f.cid = s.cid and f.tid=s.tid) and f.cid IN (select cid from " +
			"trade.customers c where since > ?)",
		"select * from trade.sellorders s where exists (select * from " +
		  "trade.portfolio f where f.cid = s.cid and f.sid < ? and f.cid IN " +
		  "(select cid from trade.customers c where since <? ))",
		"select c.cid from trade.customers c where exists (select * from trade.portfolio f " +
		  "where f.cid = c.cid and f.qty >927) UNION " +
		  "select s.cid from trade.sellorders s where (select sum(qty) from trade.portfolio f " +
		  "where f.cid = s.cid GROUP BY f.cid) > 1231 "
 		};
  
  protected static String[] uniqDelete = 
  {
    "delete from trade.sellorders s where exists (select * from " +
      "trade.portfolio f where f.cid = s.cid and f.sid < ? and tid =? and f.cid IN " +
      "(select cid from trade.customers c where since <? ))",
  };
  
  protected static String[] uniqUpdate = 
  {
    "update trade.sellorders s set qty= qty+?  where tid = ? and s.cid " +
    "IN (select cid from trade.portfolio where sid >? and sid < ? )",
  };
  
  protected static String[] nonUniqUpdate = {
    "update trade.sellorders s set qty= qty+?  where s.cid " +
      "IN (select cid from trade.portfolio where sid >? and sid < ? )",

  };

  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  protected static ArrayList<String> partitionKeys = null;
  static SharedMap partitionMap = SQLBB.getBB().getSharedMap();
  static SharedMap wanPartitionMap = SQLWanBB.getBB().getSharedMap();
  boolean isWanTest = AbstractDMLStmt.isWanTest;
  boolean isTicket42422Fixed = false; //currently test will expect 0A000, once it is supported in gfxd, test code needs to be modified
  
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
    int whichDelete;    
    if (dConn!=null) {
      if (testUniqueKeys) { //test uniqKeys
        whichDelete = rand.nextInt(uniqDelete.length); 
        deleteUniqQuery(dConn, gConn, whichDelete); 
      } else {
        whichDelete = rand.nextInt(uniqDelete.length); //use others tid instead 
        deleteNonUniqQuery (dConn, gConn, whichDelete);
      }
    }
    else {
      whichDelete = rand.nextInt(uniqDelete.length);  //use others tid instead 
      deleteNonUniqQuery (gConn, whichDelete);   
    }  
  }
	
	protected void getUniqQuery(Connection dConn, Connection gConn, int whichQuery) { 
    ResultSet derbyRS = null;
    ResultSet gfxdRS = null;
    int tid = getMyTid();
    int sid = getSid();
    Date since = getSince();
    
    switch (whichQuery) {
    case 0:
    	derbyRS = getUniqQuery0(dConn, whichQuery, tid);
    	gfxdRS = getUniqQuery0(gConn, whichQuery, tid);
    	break;
    case 1: 
    	derbyRS = getUniqQuery0(dConn, whichQuery, tid);
    	gfxdRS = getUniqQuery0(gConn, whichQuery, tid);
    	break;
    case 2: 
    	derbyRS = getUniqQuery0(dConn, whichQuery, tid);  //to reproduce #42426
    	gfxdRS = getUniqQuery0 (gConn, whichQuery, tid);
    	break;
    case 3: 
    	derbyRS = getUniqQuery3(dConn, whichQuery, since, tid);
    	gfxdRS = getUniqQuery3 (gConn, whichQuery, since, tid);
    	break;
    case 4: 
    	derbyRS = getUniqQuery4(dConn, whichQuery, sid, since, tid);  //to reproduce #42421
    	gfxdRS = getUniqQuery4 (gConn, whichQuery, sid, since, tid);
    	break;
    case 5: 
    	derbyRS = getUniqQuery5(dConn, whichQuery, tid);  //to reproduce #42422
    	gfxdRS = getUniqQuery5(gConn, whichQuery, tid);
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
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getUniqQuery3(Connection conn, int whichQuery,  
      Date since, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery3(conn, whichQuery, since, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery3(conn, whichQuery, since, tid, success);
    } //retry
    return rs;
  }
  
  // "select f.cid, f.sid, f.tid, s.oid s.tid from trade.portfolio f, trade.sellorders s " +
	// "where (f.cid = s.cid and f.tid=s.tid) and f.cid IN (select cid from " +
	// "trade.customers c where tid = ? and since > ?)",
  private ResultSet getUniqQuery3(Connection conn, int whichQuery,  
      Date since, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- tid: "+ tid + " since: " + since);
      stmt.setInt(1, tid);
      stmt.setDate(2, since);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getUniqQuery4(Connection conn, int whichQuery,  
      int sid, Date since, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery4(conn, whichQuery, sid, since, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery4(conn, whichQuery, sid, since, tid, success);
    } //retry
    return rs;
  }
  
  //" "select * from trade.sellorders s where exists (select null from " +
  //"trade.portfolio f where f.cid = s.cid and f.sid < ? and tid =? and f.cid IN " +
  // "(select cid from trade.customers c where since <? ))",
  private ResultSet getUniqQuery4(Connection conn, int whichQuery,  
      int sid, Date since, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query tid: "+ tid
      		+ " and sid: " + sid + " since: " + since);
      stmt.setInt(1, sid);
      stmt.setInt(2, tid);
      stmt.setDate(3, since);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }  
  
  private ResultSet getUniqQuery5(Connection conn, int whichQuery,  
      int tid) {
    if (!isTicket42422Fixed && SQLHelper.isDerbyConn(conn)) return null;
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery5(conn, whichQuery, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
      }
      count++;
      rs = getUniqQuery5(conn, whichQuery, tid, success);
    } //retry
    return rs;
  }
  
  //"select c.cid from trade.customers c where exists (select * from trade.portfolio f " +
  //"f.cid = c.cid and tid =? and f.qty >927) UNION " +
  //"select s.cid from trade.sellorders s where (select sum(qty) from f " +
  //"where f.cid = s.cid GROUP BY f.cid) > 577 and tid =? )"
  private ResultSet getUniqQuery5(Connection conn, int whichQuery,  
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
    	if (!isTicket42422Fixed && se.getSQLState().equalsIgnoreCase("0A000")) {
    	  Log.getLogWriter().info("get unsupported exception for union query, continue testing");
    	  success[0] = true;
    	  return null; //only suitable for cases when derby DB is available.
    	}
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
	protected void getNonUniqQuery(Connection dConn, Connection gConn, int whichQuery) { 
    ResultSet derbyRS = null;
    ResultSet gfxdRS = null;
    int sid = getSid();
    Date since = getSince();
    
    switch (whichQuery) {
    case 0:
    	derbyRS = getNonUniqQuery0(dConn, whichQuery);
    	gfxdRS = getNonUniqQuery0(gConn, whichQuery);
    	break;
    case 1: 
    	derbyRS = getNonUniqQuery0(dConn, whichQuery);
    	gfxdRS = getNonUniqQuery0(gConn, whichQuery);
    	break;
    case 2: 
    	derbyRS = getNonUniqQuery0(dConn, whichQuery);  //to reproduce #42426
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery);
    	break;
    case 3: 
    	derbyRS = getNonUniqQuery3(dConn, whichQuery, since);
    	gfxdRS = getNonUniqQuery3 (gConn, whichQuery, since);
    	break;
    case 4: 
    	derbyRS = getNonUniqQuery4(dConn, whichQuery, sid, since); //to reproduce #42421
    	gfxdRS = getNonUniqQuery4 (gConn, whichQuery, sid, since);
    	break;
    case 5: 
    	//derbyRS = getNonUniqQuery0(dConn, whichQuery);  //to reproduce #42422
    	//gfxdRS = getNonUniqQuery0(gConn, whichQuery);
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
  
  private ResultSet getNonUniqQuery0(Connection conn, int whichQuery, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- none ");
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  private ResultSet getNonUniqQuery3(Connection conn, int whichQuery,  
      Date since) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery3(conn, whichQuery, since, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery3(conn, whichQuery, since, success);
    } //retry
    return rs;
  }
  
  //"select f.cid, f.sid, f.tid, s.oid s.tid from trade.portfolio f, trade.sellorders s " +
	//"where (f.cid = s.cid and f.tid=s.tid) and f.cid IN (select cid from " +
	//"trade.customers c where since > ?)",
  private ResultSet getNonUniqQuery3(Connection conn, int whichQuery,  
      Date since, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- since: " + since);
      stmt.setDate(1, since);
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
      int sid, Date since) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery4(conn, whichQuery, sid, since, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery4(conn, whichQuery, sid, since, success);
    } //retry
    return rs;
  }
  
  //"select c.cid from trade.customers c where exists (select * from trade.portfolio f " +
  //"f.cid = c.cid and f.qty >927) UNION " +
  //"select s.cid from trade.sellorders s where (select sum(qty) from f " +
  //"where f.cid = s.cid GROUP BY f.cid) > 1231 "
  private ResultSet getNonUniqQuery4(Connection conn, int whichQuery,  
      int sid, Date since, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query sid: " + sid + 
      		" since: " + since);
      stmt.setInt(1, sid);
      stmt.setDate(2, since);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else SQLHelper.handleSQLException(se);
    }
    return rs;
  } 
  
	protected void getNonUniqQuery(Connection gConn, int whichQuery) { 
    ResultSet gfxdRS = null;
    int sid = getSid();
    Date since = getSince();
    
    switch (whichQuery) {
    case 0:
    	gfxdRS = getNonUniqQuery0(gConn, whichQuery);
    	break;
    case 1: 
    	gfxdRS = getNonUniqQuery0(gConn, whichQuery);
    	break;
    case 2: 
    	gfxdRS = getNonUniqQuery0 (gConn, whichQuery);
    	break;
    case 3: 
    	gfxdRS = getNonUniqQuery3 (gConn, whichQuery, since);
    	break;
    case 4: 
    	gfxdRS = getNonUniqQuery4 (gConn, whichQuery, sid, since);
    	break;
    case 5: 
    	gfxdRS = getNonUniqQuery0(gConn, whichQuery);
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
	
  protected void deleteUniqQuery(Connection dConn, Connection gConn, int whichDelete) { 
    int tid = getMyTid();
    int sid = getSid();
    Date since = getSince();
    int retrySleepMs =100;
    List<SQLException> exList= new ArrayList<SQLException>();
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, since, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exList.clear();
        success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, since, exList); //retry
      }
      deleteFromGfxdTable(gConn, whichDelete, tid, sid, since, exList);
      SQLHelper.handleMissedSQLException(exList);
    } 
  }	
  
  private boolean deleteFromDerbyTable(Connection conn, int whichDelete,  
      int tid, int sid, Date since, List<SQLException> exList) {
    PreparedStatement stmt = getStmt(conn, uniqDelete[whichDelete]); 
    if (stmt == null) return false;
    int count = -1;
    Log.getLogWriter().info("delete from derby, myTid is " + tid);
    try {
      verifyRowCount.put(tid+"_delete", 0);
      count = deleteFromTable(stmt, whichDelete, tid, sid, since);
      verifyRowCount.put(tid+"_delete", count);
      Log.getLogWriter().info("Derby deletes " + count + " rows");
      
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  private void deleteFromGfxdTable(Connection conn, int whichDelete,  
      int tid, int sid, Date since, List<SQLException> exList) {
    PreparedStatement stmt = getStmt(conn, uniqDelete[whichDelete]); 
    if (SQLTest.testSecurity && stmt == null) {
      SQLHelper.handleGFGFXDException((SQLException)
          SQLSecurityTest.prepareStmtException.get(), exList);
      SQLSecurityTest.prepareStmtException.set(null);
      return;
    } //work around #43244
    int count = -1;
    Log.getLogWriter().info("delete from gemfirexd, myTid is " + tid);
    try {
      count = deleteFromTable(stmt, whichDelete, tid, sid, since);
      if (!isHATest && count != (verifyRowCount.get(tid+"_delete")).intValue()){
        throw new TestException("Gfxd delete has different row count from that of derby " +
                "derby deleted " + (verifyRowCount.get(tid+"_delete")).intValue() +
                " but gfxd deleted " + count);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  private int deleteFromTable(PreparedStatement stmt,  int whichDelete, 
      int tid, int sid, Date since) throws SQLException {
    Log.getLogWriter().info("delete statement is " + uniqDelete[whichDelete]);
    int rowCount = 0;
    switch (whichDelete) {
    case 0:  
      Log.getLogWriter().info("deleting record where sid is " + sid + 
          " and tid is " +tid +" and since is " + since);
      stmt.setInt(1, sid);
      stmt.setInt(2, tid);
      stmt.setDate(3, since);
      rowCount = stmt.executeUpdate();
      break;
    default:
      throw new TestException("incorrect delete statement, should not happen");
    }  
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }  
  
  protected void deleteNonUniqQuery(Connection dConn, Connection gConn, int whichDelete) { 
    int tid = getOtherTid();
    int sid = getSid();
    Date since = getSince();
    int retrySleepMs =100;
    List<SQLException> exList= new ArrayList<SQLException>();
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, since, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exList.clear();
        success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, since, exList); //retry
      }
      deleteFromGfxdTable(gConn, whichDelete, tid, sid, since, exList);
      SQLHelper.handleMissedSQLException(exList);
    } 
  } 
  
  protected int getOtherTid() {
    int myTid = getMyTid();
    int offset = rand.nextInt(3);
    return rand.nextBoolean() ? myTid+offset : myTid-offset;
  }
  
  protected void deleteNonUniqQuery(Connection gConn, int whichDelete) { 
    int tid = getOtherTid();
    int sid = getSid();
    Date since = getSince();

    deleteFromGfxdTable(gConn, whichDelete, tid, sid, since);
  }
  
  private void deleteFromGfxdTable(Connection conn, int whichDelete,  
      int tid, int sid, Date since) {
    PreparedStatement stmt = getStmt(conn, uniqDelete[whichDelete]); 
    if (SQLTest.testSecurity && stmt == null) {
      if (SQLSecurityTest.prepareStmtException.get() != null) {
        SQLSecurityTest.prepareStmtException.set(null);
        return;
      } else ; //need to find out why stmt is not obtained
    } //work around #43244
    
    try {
      deleteFromTable(stmt, whichDelete, tid, sid, since);
    } catch (SQLException se) {
      if (se.getSQLState().equals("23503"))
        Log.getLogWriter().info("detected the foreign key constraint violation, continuing test");
      else if ((se.getSQLState().equals("42500") || se.getSQLState().equals("42502"))
          && SQLTest.testSecurity) {
        Log.getLogWriter().info("Got the expected exception for authorization," +
           " continuing tests");
      } else
        SQLHelper.handleSQLException(se); //handle the exception
    }

  }
  
  protected PreparedStatement getStmt(Connection conn, String sql) {
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement(sql);
    } catch (SQLException se) {
      if (SQLTest.testSecurity && (se.getSQLState().equals("42500")
          || se.getSQLState().equals("42502"))) {
        Log.getLogWriter().info("sql is " + sql);
        SQLHelper.printSQLException(se);
        SQLSecurityTest.prepareStmtException.set(se);
      } else if (!SQLHelper.checkDerbyException(conn, se)) { //handles the read time out
        if (SQLTest.hasTx) {
          SQLDistTxTest.rollbackGfxdTx.set(true); 
          Log.getLogWriter().info("force gfxd to rollback operations as well");
          //force gfxd rollback as one operation could not be performed in derby
          //such as read time out 
        }
        else
          ; //possibly gets read timeout in derby -- return null stmt
      } 
      else
        SQLHelper.handleSQLException(se);  
    }  
    return stmt;
  }  
  
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate){
    if (partitionKeys == null) setPartitionKeys();

    return getCorrectStmt(conn, whichUpdate, partitionKeys);
  }
  
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys){
    PreparedStatement stmt = null;
    switch (whichUpdate) {
    case 0: 
      if (partitionKeys.contains("qty")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, uniqUpdate[whichUpdate]);
        else ;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, uniqUpdate[whichUpdate]);
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
   
    return stmt;
  }
  
  protected PreparedStatement getUnsupportedStmt(Connection conn, String sql) {
    try {
      conn.prepareStatement(sql);
    } catch (SQLException se) {
      if (SQLTest.testSecurity && (se.getSQLState().equals("42500")
          || se.getSQLState().equals("42502"))) {
        SQLHelper.printSQLException(se);
        SQLSecurityTest.prepareStmtException.set(se);
      } else if (se.getSQLState().equals("0A000")) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("Got the expected Exception, continuing test");
        return null;
      } else {
        throw new TestException("Not the expected  Feature not implemented \n" + TestHelper.getStackTrace(se));
      }
    }  
    throw new TestException("Did not get the expected  'Feature not implemented' exception\n" );
  }  
  @SuppressWarnings("unchecked")
  protected void setPartitionKeys() {
    if (!isWanTest) {
      partitionKeys= (ArrayList<String>)partitionMap.get("sellordersPartition");
    }
    else {
      int myWanSite = getMyWanSite();
      partitionKeys = (ArrayList<String>)wanPartitionMap.
        get(myWanSite+"_sellordersPartition");
    }
    Log.getLogWriter().info("partition keys are " + partitionKeys);
  }
  
  protected int getMyWanSite() {
    if (isWanTest) {
      return sql.wan.WanTest.myWanSite;
    } else return -1;
  }
  
  protected void updateUniqQuery(Connection dConn, Connection gConn, int whichUpdate) { 
    int qty = rand.nextInt(30);
    int tid = getMyTid();
    int sid = getSid();
    int sid2 = sid + 10;
    int retrySleepMs =100;
    List<SQLException> exList= new ArrayList<SQLException>();
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = updateFromDerbyTable(dConn, whichUpdate, qty, tid, sid, sid2, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exList.clear();
        success = updateFromDerbyTable(dConn, whichUpdate, qty, tid, sid, sid2, exList); //retry
      }
      updateFromGfxdTable(gConn, whichUpdate, qty, tid, sid, sid2, exList);
      SQLHelper.handleMissedSQLException(exList);
    } 
  } 
  
  private boolean updateFromDerbyTable(Connection conn, int whichUpdate,  
      int qty, int tid, int sid, int sid2, List<SQLException> exList) {
    PreparedStatement stmt = null; 
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate);
    else stmt = getStmt(conn, uniqUpdate[whichUpdate]); //use only this after bug#39913 is fixed
    
    if (stmt == null) return false;
    int count = -1;
    Log.getLogWriter().info("update from derby, myTid is " + tid);
    try {
      verifyRowCount.put(tid+"_update", 0);
      count = updateFromTable(stmt, whichUpdate, qty, tid, sid, sid2);
      verifyRowCount.put(tid+"_update", count);
      Log.getLogWriter().info("Derby updates " + count + " rows");
      
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  private void updateFromGfxdTable(Connection conn, int whichUpdate,  
      int qty, int tid, int sid, int sid2, List<SQLException> exList) {
    PreparedStatement stmt;
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate);
    else stmt = getStmt(conn, uniqUpdate[whichUpdate]); //use only this after bug#39913 is fixed
    
    if (SQLTest.testSecurity && stmt == null) {
      SQLHelper.handleGFGFXDException((SQLException)
          SQLSecurityTest.prepareStmtException.get(), exList);
      SQLSecurityTest.prepareStmtException.set(null);
      return;
    } //work around #43244
    int count = -1;
    Log.getLogWriter().info("update from gemfirexd, myTid is " + tid);
    try {
      count = updateFromTable(stmt, whichUpdate, qty, tid, sid, sid2);
      if (!isHATest && count != (verifyRowCount.get(tid+"_update")).intValue()){
        throw new TestException("Gfxd update has different row count from that of derby " +
                "derby updated " + (verifyRowCount.get(tid+"_update")).intValue() +
                " but gfxd updated " + count);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  private int updateFromTable(PreparedStatement stmt,  int whichUpdate, int qty,
      int tid, int sid, int sid2) throws SQLException {
    Log.getLogWriter().info("update statement is " + uniqUpdate[whichUpdate]);
    int rowCount = 0;
    switch (whichUpdate) {
    case 0:  
      Log.getLogWriter().info("updating record where sid between " + sid + 
          " and " + sid2 + " and tid is " +tid);
      stmt.setInt(1, qty);
      stmt.setInt(2,tid);
      stmt.setInt(3, sid);
      stmt.setInt(4, sid2);
      rowCount = stmt.executeUpdate();
      break;
    default:
      throw new TestException("incorrect update statement, should not happen");
    }  
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }  
  
  protected void updateNonUniqQuery(Connection dConn, Connection gConn, int whichUpdate) { 
    int qty = rand.nextInt(30);
    int tid = getOtherTid();
    int sid = getSid();
    int sid2 = sid + 10;
    int retrySleepMs =100;
    List<SQLException> exList= new ArrayList<SQLException>();
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = updateFromDerbyTable(dConn, whichUpdate, qty, tid, sid, sid2, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exList.clear();
        success = updateFromDerbyTable(dConn, whichUpdate, qty, tid, sid, sid2, exList); //retry
      }
      updateFromGfxdTable(gConn, whichUpdate, qty, tid, sid, sid2, exList);
      SQLHelper.handleMissedSQLException(exList);
    } 
  } 
  
  protected void updateNonUniqQuery(Connection gConn, int whichUpdate) { 
    int qty = rand.nextInt(30);
    int tid = getOtherTid();
    int sid = getSid();
    int sid2 = sid + 10;

    updateFromGfxdTable(gConn, whichUpdate, qty, tid, sid, sid2);
  }
  
  private void updateFromGfxdTable(Connection conn, int whichUpdate, int qty, 
      int tid, int sid, int sid2) {
    PreparedStatement stmt; 
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate);
    else stmt = getStmt(conn, uniqUpdate[whichUpdate]); //use only this after bug#39913 is fixed
    
    if (SQLTest.testSecurity && stmt == null) {
      if (SQLSecurityTest.prepareStmtException.get() != null) {
        SQLSecurityTest.prepareStmtException.set(null);
        return;
      } else ; //need to find out why stmt is not obtained
    } //work around #43244
    
    try {
      updateFromTable(stmt, whichUpdate, qty, tid, sid, sid2);
    } catch (SQLException se) { 
      if ((se.getSQLState().equals("42500") || se.getSQLState().equals("42502"))
          && SQLTest.testSecurity) {
        Log.getLogWriter().info("Got the expected exception for authorization," +
           " continuing tests");
      } else
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }

}

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
package sql.subquery;

import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.AbstractDMLStmt;
import sql.security.SQLSecurityTest;
import sql.sqlTx.SQLDistTxTest;
import sql.sqlutil.ResultSetHelper;
import sql.wan.SQLWanBB;
import util.TestException;
import util.TestHelper;

public class Subquery {
  protected static String[] uniqSelect = {
  	"select * from trade.customers c where tid = ? and c.cid " +
  		"IN (select cid from trade.portfolio where tid =? and sid >? )",
    "select * from trade.customers where tid=? and cid " +
  		"IN (select avg(cid) from trade.portfolio where tid =? and sid >? group by sid )",
  };
  protected static String[] nonUniqSelect = {
  	"select * from trade.customers c where c.cid " +
  			"IN (select cid from trade.portfolio where sid >?)",
  };
  
  protected static String[] uniqDelete = {
    "delete from trade.networth n where tid = ? and n.cid " +
      "IN (select cid from trade.portfolio where tid =? and sid >? and sid < ? )",

  };
  
  protected static String[] uniqUpdate = {
    "update trade.sellorders s set qty= qty+?  where tid = ? and s.cid " +
      "IN (select cid from trade.portfolio where sid >? and sid < ? )",

  };
  
  protected static ArrayList<String> partitionKeys = null;
  static SharedMap partitionMap = SQLBB.getBB().getSharedMap();
  static SharedMap wanPartitionMap = SQLWanBB.getBB().getSharedMap();
  boolean isWanTest = AbstractDMLStmt.isWanTest;
  public static final Random rand = SQLTest.random;
  protected static boolean testUniqueKeys = TestConfig.tab().
  	booleanAt(SQLPrms.testUniqueKeys, true);
  protected static boolean isHATest = SQLTest.isHATest;
  protected static int maxNumOfTries =2;
  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  
  /**
   * randomly select a subquery and compare query results if results from 
   * derby is available.
   * @param dConn connection to derby 
   * @param gConn connection to gfxdabirc
   */
	public void query(Connection dConn, Connection gConn) {
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
  
  public void subqueryUpdate(Connection dConn, Connection gConn) {
    int whichUpdate;    
    if (dConn!=null) {
      if (testUniqueKeys) { //test uniqKeys
        whichUpdate = rand.nextInt(uniqUpdate.length); 
        updateUniqQuery(dConn, gConn, whichUpdate); 
      } else {
        whichUpdate = rand.nextInt(uniqUpdate.length); //use others tid instead 
        updateNonUniqQuery (dConn, gConn, whichUpdate);
      }
    }
    else {
      whichUpdate = rand.nextInt(uniqUpdate.length);  //use others tid instead 
      updateNonUniqQuery (gConn, whichUpdate);   
    }  
  } 
		
  protected int getMyTid() {
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    return myTid;
  }
  
	protected void getUniqQuery(Connection dConn, Connection gConn, int whichQuery) { 
    ResultSet derbyRS;
    ResultSet gfeRS;
    int tid = getMyTid();
    int[] sid = new int[1];
    switch (whichQuery) {
    case 0: //fall through
    case 1:
    	getDataForQuery0(sid);
    	Log.getLogWriter().info("getting result from derby");
    	derbyRS = getUniqQuery0(dConn, whichQuery, sid[0], tid);
    	Log.getLogWriter().info("getting result from gfxd");
    	gfeRS = getUniqQuery0 (gConn, whichQuery, sid[0], tid);
    	break;
    default:
    	throw new TestException ("invalid uinque quey selection");
    }
       
    if (derbyRS == null) {
      Log.getLogWriter().info("Could not get query results from derby.");
      return;
    }
    Log.getLogWriter().info("converting derby result to list of Struct");
    List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
    if (derbyList == null) {
      Log.getLogWriter().info("Not able to convert derby resultSet to a list");
      return;
    }
                	
    Log.getLogWriter().info("converting gfxd result to list of Struct");
    List<Struct> gfeList = ResultSetHelper.asList(gfeRS, false);
    if (gfeList == null) {
      if (isHATest) {
        Log.getLogWriter().info("could not get results after a few retries");
        return;
      } else {
        throw new TestException("non HA test and gemfirexd query result is " + gfeList);
      }
    }    
    ResultSetHelper.compareResultSets(derbyList, gfeList);   
	}
	
	private void getDataForQuery0(int[] sid) {
		sid[0] = getSid();
	}
	
	// "select * from trade.customers c where c.cid " +
	// "IN (select f.cid from trade.portfolio f where f.tid =? and f.sid >?)",	
  private ResultSet getUniqQuery0(Connection conn, int whichQuery, int sid, 
      int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery0(conn, whichQuery, sid, tid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery0(conn, whichQuery, sid, tid, success);
    } //retry
    return rs;
  }
  
  private ResultSet getUniqQuery0(Connection conn, int whichQuery, int sid, 
      int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- sid: " + sid + ", tid: "+ tid);
      stmt.setInt(1, tid);
      stmt.setInt(2, sid);
      stmt.setInt(3, tid);
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
    ResultSet derbyRS;
    ResultSet gfeRS;
    int[] sid = new int[1];
    switch (whichQuery) {
    case 0: //fall through
    case 1:
    	getDataForQuery0(sid);
    	derbyRS = getNonUniqQuery0(dConn, whichQuery, sid[0]);
    	gfeRS = getNonUniqQuery0 (gConn, whichQuery, sid[0]);
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
    
    if (gfeRS==null && !isHATest) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    List<Struct> gfeList = ResultSetHelper.asList(gfeRS, false);
    ResultSetHelper.compareResultSets(derbyList, gfeList);   
	}
	
  private ResultSet getNonUniqQuery0(Connection conn, int whichQuery, 
      int sid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery0(conn, whichQuery, sid, success);
    int count=0;
    while (!success[0]) {
      if (SQLHelper.isDerbyConn(conn) || count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the resultSet " +
        		"abort this operation");
        return rs;
    }
      count++;
      rs = getNonUniqQuery0(conn, whichQuery, sid, success);
    } //retry
    return rs;
  }
	
  private ResultSet getNonUniqQuery0(Connection conn, int whichQuery, int sid, 
      boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      Log.getLogWriter().info("which query is -- " + nonUniqSelect[whichQuery]);
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      Log.getLogWriter().info("data used in query -- sid: "+ sid);
      stmt.setInt(1, sid);
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
    ResultSet gfeRS;
    int[] sid = new int[1];
    switch (whichQuery) {
    case 0: //fall through
    case 1:
    	getDataForQuery0(sid);
    	gfeRS = getNonUniqQuery0 (gConn, whichQuery, sid[0]);
    	break;
    default:
    	throw new TestException ("invalid non uinque quey selection");
    }
    
    if (gfeRS==null && !isHATest) {
      Log.getLogWriter().info("could not get results after a few retries");
      return;
    }                	
    ResultSetHelper.asList(gfeRS, false); 
	}
	
	//non unique case
  protected int getSid() {
    return rand.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary))+1;
  }
  
  protected void deleteUniqQuery(Connection dConn, Connection gConn, int whichDelete) { 
    int tid = getMyTid();
    int sid = getSid();
    int sid2 = sid + 10;
    int retrySleepMs =100;
    List<SQLException> exList= new ArrayList<SQLException>();
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, sid2, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exList.clear();
        success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, sid2, exList); //retry
      }
      deleteFromGfxdTable(gConn, whichDelete, tid, sid, sid2, exList);
      SQLHelper.handleMissedSQLException(exList);
    } 
  } 
  
  private boolean deleteFromDerbyTable(Connection conn, int whichDelete,  
      int tid, int sid, int sid2, List<SQLException> exList) {
    PreparedStatement stmt = getStmt(conn, uniqDelete[whichDelete]); 
    if (stmt == null) return false;
    int count = -1;
    Log.getLogWriter().info("delete from derby, myTid is " + tid);
    try {
      verifyRowCount.put(tid+"_delete", 0);
      count = deleteFromTable(stmt, whichDelete, tid, sid, sid2);
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
      int tid, int sid, int sid2, List<SQLException> exList) {
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
      count = deleteFromTable(stmt, whichDelete, tid, sid, sid2);
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
      int tid, int sid, int sid2) throws SQLException {
    Log.getLogWriter().info("delete statement is " + uniqDelete[whichDelete]);
    int rowCount = 0;
    switch (whichDelete) {
    case 0:  
      Log.getLogWriter().info("deleting record where sid between " + sid + 
          " and " + sid2 + " and tid is " +tid);
      stmt.setInt(1,tid);
      stmt.setInt(2, tid);
      stmt.setInt(3, sid);
      stmt.setInt(4, sid2);
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
    int sid2 = sid + 10;
    int retrySleepMs =100;
    List<SQLException> exList= new ArrayList<SQLException>();
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, sid2, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exList.clear();
        success = deleteFromDerbyTable(dConn, whichDelete, tid, sid, sid2, exList); //retry
      }
      deleteFromGfxdTable(gConn, whichDelete, tid, sid, sid2, exList);
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
    int sid2 = sid + 10;

    deleteFromGfxdTable(gConn, whichDelete, tid, sid, sid2);
  }
  
  private void deleteFromGfxdTable(Connection conn, int whichDelete,  
      int tid, int sid, int sid2) {
    PreparedStatement stmt = getStmt(conn, uniqDelete[whichDelete]); 
    if (SQLTest.testSecurity && stmt == null) {
      if (SQLSecurityTest.prepareStmtException.get() != null) {
        SQLSecurityTest.prepareStmtException.set(null);
        return;
      } else ; //need to find out why stmt is not obtained
    } //work around #43244
    
    try {
      deleteFromTable(stmt, whichDelete, tid, sid, sid2);
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
    
    if (stmt == null) return; //update on partitioned key 
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

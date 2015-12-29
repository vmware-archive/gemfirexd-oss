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
package sql.dmlStatements;

import hydra.Log;
import hydra.MasterController;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.rowset.serial.SerialClob;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import sql.security.SQLSecurityTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class TradeCustomerProfileDMLStmt extends AbstractDMLStmt {
  /* 
   * cid int 
   * rep int, 
   * profile clob, 
   * tid int,
   */
  protected static String insert = "insert into trade.customerprofile values (?,?,?,?)";
  protected static String put = "put into trade.customerprofile values (?,?,?,?)";
  protected static String[] update = {
    //uniq
    "update trade.customerprofile set rep = ? where cid=? and tid = ? and profile IS NULL", 
    "update trade.customerprofile set profile = ? , rep = ? where cid IN (?,?) and tid =?", 
    "update trade.customerprofile set profile = ?  where rep = ? and tid =? ",
    //non uniq
    "update trade.customerprofile set rep = ? where cid=? and profile IS NULL", 
    "update trade.customerprofile set profile = ? , rep = ? where cid IN (?,?) ", 
    "update trade.customerprofile set profile = ?  where rep = ? ",
  };
  protected static String[] select = {
    //uniq
    //"select * from trade.customerprofile where tid = ?",
    "select cid from trade.customerprofile where tid = ?",
    //"select cid, profile from trade.customerprofile where tid=? and rep >?",
    "select cid, rep from trade.customerprofile where tid=? and rep >?",
    //"select rep, profile from trade.customerprofile where (cid =? or rep =?) and profile IS NOT NULL and tid = ?",
    "select cid, rep from trade.customerprofile where (cid =? or rep =?) and profile IS NOT NULL and tid = ?",
    "select min(cid) from trade.customerprofile where cid >? and rep >? and tid = ? group by rep having count(*) > 2",
    //non uniq
    "select * from trade.customerprofile ",
    "select cid, profile from trade.customerprofile where rep >?",
    "select rep, profile from trade.customerprofile where (cid =? or rep =?) and profile IS NOT NULL ",
    "select min(cid) from trade.customerprofile where cid >? and rep > ? group by rep having count(*) > 2",
  };
  protected static String[] delete = {
    //uniq
    "delete from trade.customerprofile where rep = ? and profile is NOT NULL and tid = ?",
    "delete from trade.customerprofile where cid IN (?,?) and tid=?",
    //non uniq
    "delete from trade.customerprofile where rep = ? and profile is NOT NULL ",
    "delete from trade.customerprofile where cid IN (?,?) ",
  }; //used in concTest without verification 
  protected static int maxNumOfTries = 2;
  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  protected static ArrayList<String> partitionKeys = null;

  @Override
  public void delete(Connection dConn, Connection gConn) {
    int[] reps = getReps(gConn, 1);
    int[] cid = getCustProfCid(gConn, 1);
    int[] cid2 = getCustProfCid(gConn, 1);   
    List<SQLException> exList = new ArrayList<SQLException>(); //exception list to be compared.
    int numOfNonUniqDelete = delete.length/2;  //how many delete statement is for non unique keys    
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    
    if (setCriticalHeap) resetCanceledFlag();
    
    if (dConn!=null && !useWriterForWriteThrough) {
      //for verification both connections are needed
      boolean success = deleteFromDerbyTable(dConn, cid[0], cid2[0], reps[0], whichDelete, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the op in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++;    
        exList.clear();
        success = deleteFromDerbyTable(dConn, cid[0], cid2[0], reps[0], whichDelete, exList);
      }
      deleteFromGFXDTable(gConn, cid[0], cid2[0], reps[0], whichDelete, exList);
      SQLHelper.handleMissedSQLException(exList);      
    }else {
      //no verification
      deleteFromGFXDTable(gConn, cid[0], cid2[0], reps[0], whichDelete);
    }
  }

  @Override
  public void insert(Connection dConn, Connection gConn, int initSize) {
    //initSize is not used in this method
    //The actual size will be determined by a sub query on customers
    ArrayList<Integer> cid = new ArrayList<Integer>();
    getCustProfileCidsForInsert(gConn, cid);
    int size = cid.size();
    Log.getLogWriter().info("inserting to customerprofile " + size + " rows");
    int[] reps = getReps(gConn, size);
    Clob[] profile = getClob(size);
    List<SQLException> exList = new ArrayList<SQLException>();
    boolean success = false;

    if (setCriticalHeap) resetCanceledFlag();
    
    if (dConn != null && !useWriterForWriteThrough) {
      try {
        success = insertToDerbyTable(dConn, cid, reps, profile, size, exList);  //insert to derby table 
        int count = 0;
        while (!success) {
          if (count>=maxNumOfTries) {
            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
            return;
          }
          MasterController.sleepForMs(rand.nextInt(retrySleepMs));
          exList .clear();
          success = insertToDerbyTable(dConn, cid, reps, profile, size, exList);  //retry insert to derby table                  
          count++;
        }
        insertToGFXDTable(gConn, cid, reps, profile, size, exList);    //insert to gfxd table 
        
        commit(dConn); //to commit and avoid rollback the successful operations
        commit(gConn);
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        throw new TestException (" insert to trade.customerprofile fails\n" + TestHelper.getStackTrace(se));
      }  //for verification

    }
    else {
      try {
        insertToGFXDTable(gConn, cid, reps, profile, size);    //insert to gfxd table 
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        throw new TestException ("gemfirexd - insert to trade.customerprofile  fails\n" + TestHelper.getStackTrace(se));
      }
    } 
  }

  @Override
  public void query(Connection dConn, Connection gConn) {
    int numOfNonUniq = select.length/2; //how many query statement is for non unique keys, 
    int whichQuery = getWhichOne(numOfNonUniq, select.length); //randomly select one query sql based on test uniq or not

    int cid = rand.nextInt((int) SQLBB.getBB().getSharedCounters().
        read(SQLBB.tradeCustomersPrimary));
    int[] rep = getReps(gConn, 1);
   
    if (dConn!=null) {
      ResultSet discRS = getQuery(dConn, whichQuery, cid, rep[0]);
      if (discRS == null) {
        Log.getLogWriter().info("could not get the derby result set after retry, abort this query");
        return;
      }
      ResultSet gfxdRS = getQuery (gConn, whichQuery, cid, rep[0]);   

      if (gfxdRS == null) {
        if (isHATest) {
          Log.getLogWriter().info("Testing HA and did not get GFXD result set");
          return;
        }
        else     
          throw new TestException("Not able to get gfxd result set after retry");
      }
      
      boolean success = ResultSetHelper.compareResultSets(discRS, gfxdRS);  
      SQLHelper.closeResultSet(gfxdRS, gConn);
      if (!success) {
        Log.getLogWriter().info("Not able to compare results due to derby server error");
      } //not able to compare results due to derby server error
    }// we can verify resultSet
    else {
      ResultSet gfxdRS = getQuery (gConn, whichQuery, cid, rep[0]);   
      if (gfxdRS != null)
        ResultSetHelper.asList(gfxdRS, false);  
      else if (isHATest)
        Log.getLogWriter().info("could not get gfxd query results after retry due to HA");
      else
        throw new TestException ("gfxd query returns null and not a HA test");   
      SQLHelper.closeResultSet(gfxdRS, gConn);
    }
    
  }

  @Override
  public void update(Connection dConn, Connection gConn, int size) {
    int[] cid = new int[size];
    int[] cid2 = new int[size];
    int[] reps = new int[size];
    Clob[] profile = new Clob[size];   
    int[]  whichUpdate = new int[size];
    List<SQLException>  exceptionList = new ArrayList<SQLException>();
    
    
    getDataForUpdate(gConn, cid, cid2, reps, profile, whichUpdate, size);
    
    if (setCriticalHeap) resetCanceledFlag();
    
    if (dConn != null && !useWriterForWriteThrough) {           
      int count =0;
      boolean success = updateDerbyTable(dConn, cid, cid2, reps, profile, 
          whichUpdate, size, exceptionList);  //insert to derby table  
      while (!success) {
        if (count>=maxNumOfTries) {
            Log.getLogWriter().info("Could not get the lock to finish update in derby, abort this operation");
            return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        exceptionList .clear();
        success = updateDerbyTable(dConn, cid, cid2, reps, profile, 
            whichUpdate, size, exceptionList);  //insert to derby table
        count++;
      } //retry until this update will be executed.
      updateGFXDTable(gConn, cid, cid2, reps, profile, 
          whichUpdate, size, exceptionList); 
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else {
      updateGFXDTable(gConn, cid, cid2, reps, profile, 
          whichUpdate, size);
    } //no verification for gfxd only or user writer case

  }
  
  protected void getCustProfileCidsForInsert(Connection gConn, ArrayList<Integer> cid) {
    String sql = "select cid from trade.customers where tid = " 
        + getMyTid() +" and cid NOT IN " + 
        "(select cid from trade.customerprofile where tid = " + getMyTid() + ")";
    List<Struct> results= null;
    ResultSet rs = null;
    try {
      rs = gConn.createStatement().executeQuery(sql);
      results = ResultSetHelper.asList(rs, false);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(gConn, se)) 
        Log.getLogWriter().info("could not get resultset due to derby locking issue"); //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(gConn, se)) 
        Log.getLogWriter().info("could not get resultset due to HA"); 
      else
        SQLHelper.handleSQLException(se);
    } finally {
      SQLHelper.closeResultSet(rs, gConn);
    }
    if (results == null) return;
    for (Struct r: results) {
      cid.add((Integer)r.get("CID"));
    }   
    
    
  }
  
  protected int[] getReps(Connection gConn, int size) {
    String sql = "select eid from emp.employees where tid =" + getMyTid() 
        + " and deptno IN (select deptid from emp.department d " +
          "where d.deptname IN ('sales', 'marketing', 'purchasing'))";
    int[] reps = new int[size];
    List<Struct> results = null;
    int sleepMs = 100;

    ResultSet rs = getResults(gConn, sql);
    results = ResultSetHelper.asList(rs, false);
    SQLHelper.closeResultSet(rs, gConn);
    
    while (results ==null || results.size() ==0) {
      String newsql = "select eid from emp.employees where tid =" + getMyTid();
      rs = getResults(gConn, newsql);
      results = ResultSetHelper.asList(rs, false);
      SQLHelper.closeResultSet(rs, gConn);
      Log.getLogWriter().info("does not get results set, will retry again");
      MasterController.sleepForMs(sleepMs);
    }
        
    for (int i=0; i<size; i++) {
      reps[i] = ((Integer)(results.get(rand.nextInt(results.size()))).get("EID"));
    }    

    return reps;
  }
  
  protected ResultSet getResults(Connection gConn, String sql) {
    ResultSet rs = null;
    try {
      rs = gConn.createStatement().executeQuery(sql);
    } catch (SQLException se) {
      if (se.getSQLState().equals("0A000") && se.getMessage().contains("non-colocated tables")) {
        Log.getLogWriter().info("subquery requires colocated tables as well");
      } else if (se.getSQLState().equals("X0Z01") && isHATest) { // handles HA issue for #41471
        Log.getLogWriter().warning("GFXD_NODE_SHUTDOWN happened and need to retry the query");
      } else SQLHelper.handleSQLException(se);
    }
    return rs;
  }

  protected boolean insertToDerbyTable(Connection conn, List<Integer> cid, 
      int[] reps, Clob[] profile, int size, 
      List<SQLException> exceptions) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    if (stmt == null) return false;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) { 
      try {
        count = insertToTable(stmt, cid.get(i), reps[i], profile[i], tid); 
        verifyRowCount.put(tid+"_insert"+i, new Integer(count));
        
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry
        else SQLHelper.handleDerbySQLException(se, exceptions);      
      }
    }
    return true;
  }

  protected void insertToGFXDTable(Connection conn, List<Integer> cid, 
      int[] reps, Clob[] profile, int size, 
      List<SQLException> exceptions) throws SQLException {
    insertToGFXDTable(conn, cid, reps, profile, size, exceptions, false);
  }
  protected void insertToGFXDTable(Connection conn, List<Integer> cid, 
      int[] reps, Clob[] profile, int size, 
      List<SQLException> exceptions, boolean isPut) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(isPut ? put : insert);
    if (SQLTest.testSecurity && stmt == null) {
    	SQLHelper.handleGFGFXDException((SQLException)
    			SQLSecurityTest.prepareStmtException.get(), exceptions);
    	SQLSecurityTest.prepareStmtException.set(null);
    	return;
    } //work around #43244
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      //see a gfxd connection was able to continue performing ops in a txn
      //even after the connection got node failure exception, 
      //to avoid derby rollback all ops in a txn, will work around this by abort
      //any other ops in gfxd once node failure exception occurs.
      if (!SQLTest.hasTx && SQLTest.setTx && isHATest && getNodeFailureFlag()) {
        Log.getLogWriter().info("abort other ops in the txn in gfxd");
        return;        
      }
      try {
        count = insertToTable(stmt, cid.get(i), reps[i], profile[i], tid, isPut); 
        if (!isHATest && count != ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue()) {
          throw new TestException("Gfxd insert has different row count from that of derby " +
            "derby inserted " + ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue() +
            " but gfxd inserted " + count);
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptions);  
      }
    }
  }
  
//for gemfirexd
  protected void insertToGFXDTable(Connection conn, List<Integer> cid, 
      int[] reps, Clob[] profile, int size) throws SQLException {
    insertToGFXDTable(conn, cid, reps, profile, size, false);
  }
  //for gemfirexd
  protected void insertToGFXDTable(Connection conn, List<Integer> cid, 
      int[] reps, Clob[] profile, int size, boolean isPut) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(isPut ? put : insert);
    int tid = getMyTid();
    
    
    for (int i=0 ; i<size ; i++) {
      try {
        insertToTable(stmt, cid.get(i), reps[i], profile[i], tid, isPut); 
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);      
      }
    }
  }

  //insert the record into database
  protected int insertToTable(PreparedStatement stmt, int cid, 
      int rep, Clob profile, int tid) throws SQLException {  
    return insertToTable(stmt, cid, rep, profile, tid, false);
  }
  //insert the record into database
  protected int insertToTable(PreparedStatement stmt, int cid, 
      int rep, Clob profile, int tid, boolean isPut) throws SQLException {  
    String clob = null;
    clob = getStringFromClob(profile);

    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    Log.getLogWriter().info(database + (isPut ? "putting" : "inserting") + " into trade.customerprofile with CID:" 
        + cid +  ",REP:" + rep + ",PROFILE:" + clob + ",TID:" + tid);    

    stmt.setInt(1, cid);
    stmt.setInt(2, rep);
    if (profile == null)
      stmt.setNull(3, Types.CLOB);
    else
      stmt.setClob(3, profile);
    stmt.setInt(4, tid);      
    int rowCount = stmt.executeUpdate();
    Log.getLogWriter().info(database + (isPut ? "put " : "inserted ") + rowCount + " rows in trade.customerprofile CID:" 
        + cid +  ",REP:" + rep + ",PROFILE:" + clob + ",TID:" + tid);   
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }  
  
  public ResultSet getQuery(Connection conn, int whichQuery, int cid,
      int rep) {
    int tid = getMyTid();
    return getQuery(conn, whichQuery, cid, rep, tid);
  }
  
  //retries when lock could not obtained
  public static ResultSet getQuery(Connection conn, int whichQuery, int cid, 
      int rep, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = getQuery(conn, whichQuery, cid, rep, tid, success);
    int count = 0;
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        if (SQLHelper.isDerbyConn(conn))
          Log.getLogWriter().info("Could not get the lock to finisht the op in derby, " +
              "abort this operation");
        return null; 
      };
      count++;        
      MasterController.sleepForMs(rand.nextInt(retrySleepMs));
      rs = getQuery(conn, whichQuery, cid, rep, tid, success);
    }
    return rs;
  }
  
  public static ResultSet getQuery(Connection conn, int whichQuery, int cid,  
      int rep, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;

    try {
      String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";  
      String query = " QUERY: " + select[whichQuery];
      
      stmt = conn.prepareStatement(select[whichQuery]);      
      switch (whichQuery){
      case 0:
        // "select * from trade.customerprofile where tid = ?",
        Log.getLogWriter().info(database + "querying trade.customerprofile with TID:"+ tid + query);
        stmt.setInt(1, tid);
        break;
      case 1: 
        //"select cid, profile from trade.customerprofile where tid=? and rep >?",
        Log.getLogWriter().info(database + "querying trade.customerprofile with REP:" + rep + ",TID:"+ tid + query);
        stmt.setInt(1, tid);
        stmt.setInt(2, rep); 
        break;
      case 2:
        //"select rep, profile from trade.customerprofile where (cid =? or rep =?) 
        //and profile IS NOT NULL and tid = ?",  
        Log.getLogWriter().info(database + "querying trade.customerprofile with CID:" + cid + 
            ",REP:" + rep + ",TID:"+ tid + query);
        stmt.setInt(1, cid);
        stmt.setInt(2, rep); 
        stmt.setInt(3, tid);
        break;
      case 3:
        //"select min(cid) from trade.customerprofile where cid >? and rep >? tid = ?",
        Log.getLogWriter().info(database + "querying trade.customerprofile with CID:" + cid +
            ",REP:" + rep + ",TID:"+ tid + query);        
        stmt.setInt(1, cid); 
        stmt.setInt(2, rep);
        stmt.setInt(3, tid);
        break;
      case 4:
        //"select * from trade.customerprofile ",
        Log.getLogWriter().info(database + "querying trade.customerprofile with no data"  + query);
        break;
      case 5: 
        // "select cid, profile from trade.customerprofile where rep >?",
        Log.getLogWriter().info(database + "querying trade.customerprofile with REP:" + rep   + query);
        stmt.setInt(1, rep); 
        break;
      case 6:
        //""select rep, profile from trade.customerprofile where (cid =? or rep =?)
        // and profile IS NOT NULL ",
        Log.getLogWriter().info(database + "querying trade.customerprofile with CID:" + cid + 
            " rep: " + rep  + query);
        stmt.setInt(1, cid);
        stmt.setInt(2, rep); 
        break;
      case 7:
        //"select min(cid) from trade.customerprofile where cid >? and rep > ?"
        Log.getLogWriter().info(database + "querying trade.customerprofile with -- cid: " + cid + 
            ",REP:" + rep + query);
        stmt.setInt(1, cid);
        stmt.setInt(2, rep); 
        break;   
      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else      SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  protected void getDataForUpdate(Connection conn, int[] cid, int[ ] cid2, int[] reps,
      Clob[] profile, int[] whichUpdate, int size){
    int numOfNonUniqUpdate = update.length/2;
    reps = getReps(conn, size);
    profile = getClob(size);
    cid = getCustProfCid(conn, size); 
    cid2 = getCustProfCid(conn, size);
    for (int i=0; i<size; i++) {
      whichUpdate[i] = getWhichOne(numOfNonUniqUpdate, update.length);
    }
  }
  
  //could return an int[] with each cid as 0 
  protected int[] getCustProfCid(Connection conn, int size) {
    String sql = "select cid from trade.customerprofile where tid = " + getMyTid();
    List<Struct> list =null;
    ResultSet rs = null;
    int[] cids = new int[size];
    try {
      rs = conn.createStatement().executeQuery(sql);
      list = ResultSetHelper.asList(rs, SQLHelper.isDerbyConn(conn));      
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) 
        Log.getLogWriter().info("could not get resultset due to derby locking issue"); //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) 
        Log.getLogWriter().info("could not get resultset due to HA"); 
      else SQLHelper.handleSQLException(se);
    } finally {
      SQLHelper.closeResultSet(rs, conn);
    }
    if (list != null && list.size() >0) {
      for (@SuppressWarnings("unused") int cid: cids){
        cid = (Integer)(list.get(rand.nextInt(list.size()))).get("CID");
      }      
    } 
    return cids;    
  }
  
  protected boolean updateDerbyTable(Connection conn, int[] cid, int[ ] cid2,
      int[] reps, Clob[] profile, int[] whichUpdate, int size, 
      List<SQLException> exceptions) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i]);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed      
      
      if (stmt == null) {
      	try {
      		conn.prepareStatement(update[whichUpdate[i]]);
      	} catch (SQLException se) {
      		if (se.getSQLState().equals("08006") || se.getSQLState().equals("08003"))
      			return false;
      	} 
      	//this test of connection is lost is necessary as stmt is null
      	//could be caused by not allowing update on partitioned column. 
      	//the test of connection lost could be removed after #39913 is fixed
      	//just return false if stmt is null
      }
      
      try {
        if (stmt!=null) {
          verifyRowCount.put(tid+"_update"+i, 0);
          count = updateTable(stmt, cid[i], cid2[i], reps[i], 
              profile[i], tid, whichUpdate[i]);
          verifyRowCount.put(tid+"_update"+i, new Integer(count));
          
        }
      } catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
          Log.getLogWriter().info("detected the deadlock, will try it again");
          return false;
        } else
            SQLHelper.handleDerbySQLException(se, exceptions);
      }    
    }  
    return true;
  }
  
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate){
    if (partitionKeys == null) setPartitionKeys();

    return getCorrectStmt(conn, whichUpdate, partitionKeys);
  }
  
  @SuppressWarnings("unchecked")
  protected void setPartitionKeys() {
    if (!isWanTest) {
      //partitionKeys= (ArrayList<String>)partitionMap.get("employeesPartition");
      partitionKeys= new ArrayList<String>(); //TODO to remove after setting PartitionClause
    }
    else {
      int myWanSite = getMyWanSite();
      partitionKeys = (ArrayList<String>)wanPartitionMap.
        get(myWanSite+"_employeesPartition");
    }
    Log.getLogWriter().info("partition keys are " + partitionKeys);
  }
  
  //used to parse the partitionKey and test unsupported update on partitionKey, 
  //no need after bug #39913 is fixed
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys){
    PreparedStatement stmt = null;
    switch (whichUpdate) {
    case 0: 
      // "update trade.customerprofile set rep = ? where cid=? and tid = ? and profile IS NULL", 
      if (partitionKeys.contains("rep")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
      // "update trade.customerprofile set profile = ? , rep = ? where cid IN (?,?) and tid =?",  
      if (partitionKeys.contains("profile")|| partitionKeys.contains("rep")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: 
      //"update trade.customerprofile set profile = ?  where rep = ? and tid =? ",
      if (partitionKeys.contains("profile")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: 
      //"update trade.customerprofile set rep = ? where cid=? and profile IS NULL",
      if (partitionKeys.contains("rep")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: 
      // "update trade.customerprofile set profile = ? , rep = ? where cid IN (?,?) ", 
      if (partitionKeys.contains("profile")|| partitionKeys.contains("rep")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 5: 
      // "update trade.customerprofile set profile = ?  where rep = ? ", 
      if (partitionKeys.contains("profile")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
   
    return stmt;
  }
  
  protected int updateTable(PreparedStatement stmt, int cid, int cid2, 
      int rep, Clob profile, int tid, int whichUpdate) throws SQLException {    
    int rowCount = 0;
    String clob = null;
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    String query = " QUERY: " + update[whichUpdate];
    
    switch (whichUpdate) {
    case 0: 
      // "update trade.customerprofile set rep = ? where cid=? and tid = ? and profile IS NULL", 
      Log.getLogWriter().info(database + "updating trade.customerprofile with REP:" + rep + 
          " where CID:" + cid + ",TID:" + tid + query);
      stmt.setInt(1, rep);
      stmt.setInt(2, cid);
      stmt.setInt(3, tid); 
      rowCount = stmt.executeUpdate();   
      
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.customerprofile REP:" + rep + 
          " where CID:" + cid + ",TID:" + tid + query);
      break;
    case 1: 
      // "update trade.customerprofile set profile = ? , rep = ? where cid IN (?,?) and tid =?",
      if (profile != null) {
        if (profile.length() == 0) clob = "empty profile";
        else {
          BufferedReader reader = (BufferedReader) profile.getCharacterStream();
          clob = ResultSetHelper.convertCharArrayToString(reader, (int)profile.length()); 
          try {
            reader.close();
          } catch (IOException e) {
            throw new TestException("could not close the BufferedReader" + 
                TestHelper.getStackTrace(e));
          }
        }            
      }
      Log.getLogWriter().info(database + "updating trade.customerprofile with PROFILE:" + clob + 
          ",REP:" + rep + " where 1_CID:" + cid + ",2_CID:" + cid2 + ",TID:"+ tid +  query);      
      stmt.setClob(1, profile);
      stmt.setInt(2, rep);
      stmt.setInt(3, cid);
      stmt.setInt(4, cid2);
      stmt.setInt(5, tid);
      rowCount = stmt.executeUpdate();   
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.customerprofile with PROFILE:" + clob + 
          ",REP:" + rep + " where 1_CID:" + cid + ",2_CID:" + cid2 + ",TID:"+ tid +  query); 
      break;
    case 2: 
      //"update trade.customerprofile set profile = ?  where rep = ? and tid =? ",
      if (profile != null) {
        if (profile.length() == 0) clob = "empty profile";
        else {
          BufferedReader reader = (BufferedReader) profile.getCharacterStream();
          clob = ResultSetHelper.convertCharArrayToString(reader, (int)profile.length());
          try {
            reader.close();
          } catch (IOException e) {
            throw new TestException("could not close the BufferedReader" + 
                TestHelper.getStackTrace(e));
          }
        }            
      }
      Log.getLogWriter().info(database + "updating trade.customerprofile with PROFILE:" + clob + 
          " where REP:" + rep + ",TID:"+ tid +  query); 
      stmt.setClob(1, profile);
      stmt.setInt(2, rep);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.customerprofile with PROFILE:" + clob + 
          " where REP:" + rep + ",TID:"+ tid +  query); 
      break;
    case 3: 
      //"update trade.customerprofile set rep = ? where cid=? and profile IS NULL",
      Log.getLogWriter().info(database + "updating trade.customerprofile with REP:" + rep + 
          " where CID:" + cid  + query);
      stmt.setInt(1, rep);
      stmt.setInt(2, cid);
      rowCount = stmt.executeUpdate();    
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.customerprofile with REP:" + rep + 
          " where CID:" + cid  + query);
      break;
    case 4: 
      // "update trade.customerprofile set profile = ? , rep = ? where cid IN (?,?) ", 
      if (profile != null) {
        if (profile.length() == 0) clob = "empty profile";
        else {
          BufferedReader reader = (BufferedReader) profile.getCharacterStream();
          clob = ResultSetHelper.convertCharArrayToString(reader, (int)profile.length()); 
          try {
            reader.close();
          } catch (IOException e) {
            throw new TestException("could not close the BufferedReader" + 
                TestHelper.getStackTrace(e));
          }
        }            
      }
      Log.getLogWriter().info(database + "updating trade.customerprofile with PROFILE:" + clob + 
          ",REP:" + rep + " where 1_CID:" + cid + ",2_CID:" + cid2 +  query);
      stmt.setClob(1, profile);
      stmt.setInt(2, rep);
      stmt.setInt(3, cid);
      stmt.setInt(4, cid2);
      rowCount = stmt.executeUpdate(); 
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.customerprofile with PROFILE:" + clob + 
          ",REP:" + rep + " where 1_CID:" + cid + ",2_CID:" + cid2 +  query);
      break;
    case 5: 
      // "update trade.customerprofile set profile = ?  where rep = ? ", 
      if (profile != null) {
        if (profile.length() == 0) clob = "empty profile";
        else {
          BufferedReader reader = (BufferedReader) profile.getCharacterStream();
          clob = ResultSetHelper.convertCharArrayToString(reader, (int)profile.length()); 
          try {
            reader.close();
          } catch (IOException e) {
            throw new TestException("could not close the BufferedReader" + 
                TestHelper.getStackTrace(e));
          }
        }            
      }
      Log.getLogWriter().info(database + "updating trade.customerprofile with PROFILE:" + clob + 
          " where REP:" + rep  +  query);
      stmt.setClob(1, profile);
      stmt.setInt(2, rep);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.customerprofile with PROFILE:" + clob + 
          " where REP:" + rep  +  query);
    default:
     throw new TestException ("Wrong update sql string here");
    }
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
  
  protected void updateGFXDTable(Connection conn, int[] cid, int[ ] cid2,
      int[] reps, Clob[] profile, int[] whichUpdate, int size, 
      List<SQLException> exceptions) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {   
      if (SQLTest.testPartitionBy) { //will be removed after bug #39913 is fixed
        stmt = getCorrectStmt(conn, whichUpdate[i]);
      } 
      else {
        stmt = getStmt(conn, update[whichUpdate[i]]);
      }
      
      if (SQLTest.testSecurity && stmt == null) {
      	if (SQLSecurityTest.prepareStmtException.get() != null) {
      	  SQLHelper.handleGFGFXDException((SQLException)
      			SQLSecurityTest.prepareStmtException.get(), exceptions);
      	  SQLSecurityTest.prepareStmtException.set(null);
      	  return;
      	}
      } //work around #43244
                
      try {
        if (stmt!=null) {
          count = updateTable(stmt, cid[i], cid2[i], reps[i],  
              profile[i], tid, whichUpdate[i]);
          if (!isHATest && count != ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue()){
            throw new TestException("Gfxd update has different row count from that of derby " +
                    "derby updated " + ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue() +
                    " but gfxd updated " + count);
          }
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptions);  
      }    
    }  
  }
  
  protected void updateGFXDTable(Connection conn, int[] cid, int[ ] cid2,
      int[] reps, Clob[] profile, int[] whichUpdate, int size) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      
   
      if (SQLTest.testPartitionBy) { //will be removed after bug #39913 is fixed
        stmt = getCorrectStmt(conn, whichUpdate[i]);
      } 
      else {
        stmt = getStmt(conn, update[whichUpdate[i]]);
      }
                
      try {
        if (stmt!=null) {
          count = updateTable(stmt, cid[i], cid2[i], reps[i],  
              profile[i], tid, whichUpdate[i]);
        }
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);  
      }    
    }  
  }
  
  protected boolean deleteFromDerbyTable(Connection dConn, int cid, int cid2, 
      int rep, int whichDelete, List<SQLException> exList) {
    int tid = getMyTid();
    int count = -1;
  
    PreparedStatement stmt = getStmt(dConn, delete[whichDelete]);
    if (stmt == null) return false;
    try {      
      verifyRowCount.put(tid+"_delete", 0); //to avoid NPE in varifying stage if gemfirexd does not get the same exception
      count = deleteFromTable(stmt, cid, cid2, rep, tid, whichDelete); 
      verifyRowCount.put(tid+"_delete", new Integer(count));
      
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dConn, se))
        return false;
      else {
      SQLHelper.handleDerbySQLException(se, exList); //handle the exception
      }
    }
    return true;
  }
  
  protected void deleteFromGFXDTable(Connection gConn, int cid, int cid2, 
      int rep, int whichDelete, List<SQLException> exList) {
    int tid = getMyTid();
    int count = -1;
    
    PreparedStatement stmt = getStmt(gConn, delete[whichDelete]);
    if (SQLTest.testSecurity && stmt == null) {
    	SQLHelper.handleGFGFXDException((SQLException)
    			SQLSecurityTest.prepareStmtException.get(), exList);
    	SQLSecurityTest.prepareStmtException.set(null);
    	return;
    } //work around #43244
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    try {
      count = deleteFromTable(stmt, cid, cid2, rep, tid, whichDelete); 
      if (count != ((Integer)verifyRowCount.get(tid+"_delete")).intValue() && !isHATest){
        throw new TestException("Gfxd delete (customers) has different row count from that of derby " +
                "derby deleted " + ((Integer)verifyRowCount.get(tid+"_delete")).intValue() +
                " but gfxd deleted " + count);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  //w/o verification, no exception list available
  protected void deleteFromGFXDTable(Connection gConn, int cid, int cid2, 
      int rep, int whichDelete) {
    int tid = getMyTid();
   
    PreparedStatement stmt = getStmt(gConn, delete[whichDelete]);
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    try {
      deleteFromTable(stmt, cid, cid2, rep, tid, whichDelete); 
    } catch (SQLException se) {
      if (se.getSQLState().equals("23503"))
        Log.getLogWriter().info("detected the foreign key constraint violation, continuing test");
      else
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  
  //delete from table based on statement
  protected int deleteFromTable(PreparedStatement stmt, int cid, int cid2, 
      int rep, int tid, int whichDelete) throws SQLException {
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    String query = " QUERY: " + delete[whichDelete];
    int rowCount =0;
    
    switch (whichDelete) {
    case 0:  
      // "delete from trade.customerprofile where rep = ? and profile is NOT NULL and tid = ?",  
      Log.getLogWriter().info(database + "deleting  trade.customerprofile with REP:" + rep + 
          ",TID:" + tid + query);
      stmt.setInt(1, rep);
      stmt.setInt(2,tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.customerprofile REP:" + rep + 
          ",TID:" + tid + query);
      break;
    case 1:
      // "delete from trade.customerprofile where cid IN (?,?) and tid=?",
      Log.getLogWriter().info(database + "deleting trade.customerprofile with CID:" + cid  +
          "," + cid2 +",TID:" + tid + query); 
      stmt.setInt(1, cid);
      stmt.setInt(2, cid2);
      stmt.setInt(3, tid);
      rowCount=stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.customerprofile with CID:" + cid  +
          "," + cid2 +",TID:" + tid + query); 
      break;
    case 2:
      // "delete from trade.customerprofile where rep = ? and profile is NOT NULL ",
      Log.getLogWriter().info(database + "deleting trade.customerprofile with REP:" + rep + query);
      stmt.setInt(1, rep);
      rowCount=stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.customerprofile REP:" + rep + query);
      break;
    case 3:
      // "delete from trade.customerprofile where cid IN (?,?) ",
      Log.getLogWriter().info(database + "deleting trade.customerprofile with 1_CID:"  + cid  +
          ",2_CID:" + cid2 + query); 
      stmt.setInt(1, cid);
      stmt.setInt(2, cid2);
      rowCount= stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount +" rows in trade.customerprofile  1_CID:"  + cid  +
          ",2_CID:" + cid2 + query); 
      break;
    default:
      throw new TestException("incorrect delete statement, should not happen");
    }
   //  rowCount = stmt.executeUpdate();
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
  

  @Override
  public void put(Connection dConn, Connection gConn, int initSize) {
    //initSize is not used in this method
    //The actual size will be determined by a sub query on customers
    ArrayList<Integer> cid = new ArrayList<Integer>();
    getCustProfileCidsForInsert(gConn, cid);
    int size = cid.size();
    
    int[] reps = getReps(gConn, size);
    Clob[] profile = getClob(size);
    List<SQLException> exList = new ArrayList<SQLException>();
    boolean success = false;

    if (dConn != null && !useWriterForWriteThrough) {
      try {
        success = insertToDerbyTable(dConn, cid, reps, profile, size, exList);  //insert to derby table 
        int count = 0;
        while (!success) {
          if (count>=maxNumOfTries) {
            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
            return;
          }
          MasterController.sleepForMs(rand.nextInt(retrySleepMs));
          exList .clear();
          success = insertToDerbyTable(dConn, cid, reps, profile, size, exList);  //retry insert to derby table                  
          count++;
        }
        insertToGFXDTable(gConn, cid, reps, profile, size, exList, true);    //insert to gfxd table 
        
        if (SQLTest.setTx) {
          commit(gConn);
          commit(dConn);
        } else {         
          commit(dConn); //to commit and avoid rollback the successful operations
          commit(gConn);
        }
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        throw new TestException ("put to trade.customerprofile fails\n" + TestHelper.getStackTrace(se));
      }  //for verification

    }
    else {
      try {
        insertToGFXDTable(gConn, cid, reps, profile, size, true);    //insert to gfxd table 
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        throw new TestException ("put to trade.customerprofile in gfxd fails\n" + TestHelper.getStackTrace(se));
      }
    } 
  }


}

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import sql.SQLHelper;
import sql.SQLTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public class EmpDepartmentDMLStmt extends AbstractDMLStmt {
  /*
   * table fields for emp.department 
   * deptid int 
   * deptname varchar(100)
   * tid int
   */
  
  protected static String insert = "insert into emp.department values (?,?,?)";
  protected static String put = "put into emp.department values (?,?,?)";
  protected static String[] update = { 

  }; 
  protected static String[] select = {
    "select * from emp.department",
    //"select * from emp.department where deptname > ? ",
    "select * from emp.department where deptname = ? "
  };
  protected static String[] delete = {                          
  };

  protected static String[] names = {                                        
    "accounting", "finance", "admin", "HR", "sales", "marketing", "executive", "purchasing", "legal", "IT"                                         
  };
  protected static int maxNumOfTries = 1;
  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  protected static ArrayList<String> partitionKeys = null;

  @Override
  public void delete(Connection dConn, Connection gConn) {
    //no delete for this table
  }

  @Override
  public void insert(Connection dConn, Connection gConn, int size) {
    //no insert except the populate table

  }

  @Override
  public void query(Connection dConn, Connection gConn) {
    int whichQuery = rand.nextInt(select.length);
    String name = names[rand.nextInt(names.length)];
    ResultSet discRS = null;
    ResultSet gfeRS = null;
    
    if (dConn!=null) {
      discRS = getQuery(dConn, whichQuery, name);
      if (discRS == null) {
        Log.getLogWriter().info("could not get the derby result set after retry, abort this query");
        return;
      }
      gfeRS = getQuery (gConn, whichQuery, name); 
      if (gfeRS == null) {
        if (isHATest) {
          Log.getLogWriter().info("Testing HA and did not get GFXD result set after retry");
          return;
        }
        else     
          throw new TestException("Not able to get GFXD result set and not a HA test");
      }
      
      boolean success = ResultSetHelper.compareResultSets(discRS, gfeRS); 
      if (!success) {
        Log.getLogWriter().info("Not able to compare results in emp.department table");
      } //not able to compare results      
    }// we can verify resultSet
    else {
      gfeRS = getQuery (gConn,  whichQuery, name);  
      if (gfeRS != null)
        ResultSetHelper.asList(gfeRS, false);  
      else if (isHATest)
        Log.getLogWriter().info("could not get gfxd query results after retry due to HA");
      else
        throw new TestException ("gfxd query returns null and not a HA test");  
    }
  }

  @Override
  public void update(Connection dConn, Connection gConn, int size) {
    // no update for this table

  }
  
  //populate the table
  public void populate (Connection dConn, Connection gConn) {
    int size = names.length;
    populate (dConn, gConn, size);
  }
  
  //populate the table
  public void populate (Connection dConn, Connection gConn, int size) {
    insertDepartment(dConn, gConn, size);
  }
  
  private void insertDepartment(Connection dConn, Connection gConn, int size) {
    int[] did = new int[size];
    List<SQLException> exceptionList = new ArrayList<SQLException>();    
    getDataForInsert(did); //get the data
    if (dConn != null) {
      if (SQLTest.setTx) {
        if (SQLTest.ddlThread != -1 && getMyTid() == SQLTest.ddlThread) {
          for (int i=0 ; i<did.length ; i++) {
            insertToDerbyTable(dConn, did[i], names[i], exceptionList);  //insert to derby table  
          }
          
          insertToGFETable(gConn, did); //only one thread needed to populate table once
          
          commit(gConn);
          commit(dConn);
          
          if (exceptionList.size() != 0) throw new TestException("insert into department table in derby failed");
          
        }
        
        return;
      } else {
        for (int i=0 ; i<did.length ; i++) {
          boolean success = insertToDerbyTable(dConn, did[i], names[i], exceptionList);  //insert to derby table  
          if (!success) {
              continue;
          } //if not getting the lock, continue for next as some other thread will insert the key
          insertToGFETable(gConn, did[i], names[i], exceptionList); 
          SQLHelper.handleMissedSQLException(exceptionList);
        }
      }

      commit(gConn);
      commit(dConn);
    }
    else {
      if (SQLTest.ddlThread != -1) {
        if (getMyTid() == SQLTest.ddlThread) 
          insertToGFETable(gConn, did); //only one thread needed to populate table once
      }        
    } //no verification
  }
  
  //insert into Derby
  protected boolean insertToDerbyTable(Connection conn, int did, String name, 
      List<SQLException> exceptions)  {
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    int count = -1;
    
    try {
      verifyRowCount.put(tid+"_insert", 0);
      count = insertToTable(stmt, did, name, tid);
      verifyRowCount.put(tid+"_insert", new Integer(count));
      
    }  catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exceptions);
    }    

    return true;
  }
  
  //insert into gemfirexd/gfe
  protected void insertToGFETable(Connection conn, int did, String name, List<SQLException> exceptions)  {
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    int count = -1;
    
    try {
      count = insertToTable(stmt, did, name, tid);
      if (count != ((Integer)verifyRowCount.get(tid+"_insert")).intValue()) {
        Log.getLogWriter().info("Gfxd insert has different row count from that of derby " +
          "derby inserted " + ((Integer)verifyRowCount.get(tid+"_insert")).intValue() +
          " but gfxd inserted " + count);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exceptions);
    }
  }
  
  //insert into gemfirexd/gfe w/o verification
  protected void insertToGFETable(Connection conn, int[] did)  {
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    
    for (int i=0 ; i<did.length ; i++) {
      try {
        insertToTable(stmt, did[i], names[i], tid);
      } catch (SQLException se) {
        if (( /*temp comment out (se.getErrorCode() == -1) &&  */("23505".equals(se.getSQLState()) ))) {
          Log.getLogWriter().info("Got the expected exception due to primary key constraint check");
        }
        else 
          SQLHelper.handleSQLException(se); //handle the exception
      }
    }    
  }
  
  //insert a record into the table  
  protected int insertToTable(PreparedStatement stmt, int did, String name, int tid, boolean isPut) throws SQLException {
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";
    Log.getLogWriter().info(database + (isPut ? "putting " : "inserting" ) +" into emp.department with DEPTID:" + did +
        ",DEPTNAME:"+ name + ",TID:" + tid);
    stmt.setInt(1, did);
    stmt.setString(2, name);   
    stmt.setInt(3, tid);
    int rowCount = stmt.executeUpdate();
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    Log.getLogWriter().info(database + (isPut ? "put " : "inserted" ) + rowCount + " rows in emp.department DEPTID:" + did +
        ",DEPTNAME:"+ name + ",TID:" + tid);
    return rowCount;    
  }

  protected int insertToTable(PreparedStatement stmt, int did, String name, int tid) throws SQLException {
    return insertToTable(stmt, did, name, tid, false);
  }

  private void getDataForInsert(int[] did) {    
    int size = names.length;    
    for (int i = 0 ; i <size ; ) {
      did[i] = ++i;
    }
  }
  
  public static ResultSet getQuery(Connection conn, int whichQuery, String name) {
    boolean[] success = new boolean[1];
    ResultSet rs = getQuery(conn, whichQuery, name, success);
    int count = 0;
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
        return null; 
      }
      count++;   
      MasterController.sleepForMs(rand.nextInt(retrySleepMs));    
      rs = getQuery(conn, whichQuery, name, success);
    } //retry 
    return rs;
  } 
  
  public static ResultSet getQuery(Connection conn, int whichQuery,
      String name, boolean[] success)  {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    
    
    Log.getLogWriter().info("which query is -- " + select[whichQuery]);   
    try {
      stmt = conn.prepareStatement(select[whichQuery]);      
      String database =  SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";
      String query = " QUERY: " + select[whichQuery];
      switch (whichQuery){
      case 0:
        Log.getLogWriter().info(database + "querying emp.department with no data " + query);
        break;
      case 1: //fall through
      case 2:        
        Log.getLogWriter().info(database + "querying emp.department with DEPTNAME:" + name + query);
        stmt.setString(1, name);
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

  @Override
  public void put(Connection dConn, Connection gConn, int size) {
    // TODO Auto-generated method stub
    
  }

}

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
package sql.dmlStatements.json;

import hydra.Log;
import hydra.MasterController;

import java.math.BigDecimal;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;



import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import sql.security.SQLSecurityTest;
import sql.sqlTx.SQLDistTxTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;
import sql.dmlStatements.AbstractDMLStmt;
/**
 * @author eshu
 *
 */
public class TradeNetworthDMLStmtJson extends AbstractDMLStmt {
  /*
   * trade.NetWorth table fields
   *   int cid;
   *   BigDecimal cash;
   *   BigDecimal securities;
   *   int loanLimit;
   *   BigDecimal availLoan;
   *   int tid; //for update or delete unique records to the thread
   */
  
  protected static String insert = "insert into trade.networth (cid , cash , securities , loanlimit , availloan ,  tid )   values (?,?,?,?,?,?)";
  protected static String put = "put into trade.networth (cid , cash , securities , loanlimit , availloan ,  tid )  values (?,?,?,?,?,?)";
  protected static String[] update = { 
    //uniqs
    "update trade.networth set availloan=availloan-? where cid = ? and tid= ?", 
    "update trade.networth set securities=? where cid = ?  and tid= ?",
    "update trade.networth set cash=cash-? where cid = ? and tid= ?",
    "update trade.networth set loanLimit=? where  tid= ? and securities > ?",  //bulk update
    //no uniqs
    "update trade.networth set availloan=availloan-? where cid = ? ", 
    "update trade.networth set securities=? where cid = ?  ",
    "update trade.networth set cash=cash-? where cid = ? ",
    "update trade.networth set loanLimit=? where securities > ? "
  }; 
  protected static String[] select = {
    //uniqs
    "select * from trade.networth where tid = ?",
    "select cid, (cash + securities - (loanLimit - availloan))as networth from trade.networth where tid = ? order by networth desc, cid",
    "select cid, loanlimit, availloan from trade.networth where (loanlimit >? and loanlimit-availloan <= ?) and tid=? ",
    "select cid, cash, securities from trade.networth where (cash<? or securities >=?) and tid =?",
    "select * from trade.networth where (cash > loanLimit - availloan) and tid =?",
    "select cash, securities, loanlimit, cid, availloan from trade.networth where (cash <=securities or cash > loanLimit - availloan) and tid =?",
    "select securities, cash, availloan, loanlimit, cid, availloan from trade.networth where (availloan >=securities or cash > loanLimit - availloan) and tid = ?",
    //non uniq
    "select * from trade.networth",
    "select cid, (cash + securities - (loanLimit - availloan)) as networth from trade.networth",
    "select cid, loanlimit, availloan from trade.networth where (loanlimit >? and loanlimit-availloan <= ?)",
    "select cid, cash, securities from trade.networth where (cash<? or securities >=?) ",
    "select * from trade.networth where (cash > loanLimit - availloan)",
    "select cash, securities, loanlimit, cid, availloan from trade.networth where (cash <=securities or cash > loanLimit - availloan)",
    "select securities, cash, availloan, loanlimit, cid, availloan from trade.networth where (availloan >=securities or cash > loanLimit - availloan)"
  };
  protected static String[] delete = {
    //uniq key
    "delete from trade.networth where cid = ?", //required to get the cid insert by this thread, which will be converted to a region.destroy
    //no uniq
    "delete from trade.networth where cid=?"
  }; //used in concTest without verification

  
  protected static String insertJSON = "insert into trade.networth (cid , cash , securities , loanlimit , availloan ,  tid , json_details )   values (?,?,?,?,?,?,?)";
  protected static String putJSON = "put into trade.networth (cid , cash , securities , loanlimit , availloan ,  tid ,json_details )  values (?,?,?,?,?,?,?)";
  
  protected enum Operation {INSERT,UPDATE,DELETE};
   
  
  protected static String[] selectJSON = {
    //uniqs
    "select cast(json_evalPath(json_details, '$..networth.cid')  as integer) as cid ,  cash , securities ,  "  +
    " cast(json_evalPath(json_details, '$..networth.loanlimit')  as integer) as loanlimit, availloan  , " +
    " cast(json_evalPath(json_details, '$..networth.tid')  as integer) as tid from trade.networth where tid = ?",
    "select cid, (cash + securities - ( cast(json_evalPath(json_details, '$..networth.loanlimit')  as integer)  - availloan  )) as networth from trade.networth where tid = ? order by networth desc, cid",
    "select cid, loanlimit, availloan from trade.networth where (loanlimit >? and cast(json_evalPath(json_details, '$..networth[0].loanlimit')  as integer) -availloan <= ?) and tid=? ",
    "select cast ( json_evalPath(trade.customers.networth_json ,'$..networth[-1:].cid') as Integer)  as cid , cash, securities from trade.networth , trade.customers  where trade.customers.cid = trade.networth.cid and (cash<? or securities >=?)  and cast(json_evalPath(trade.networth.json_details, '$..networth.tid')  as integer)  =?",
    "select cast(json_evalPath(trade.customers.networth_json , '$..networth[0].cid') as integer)  as cid, cash,securities,loanlimit, availloan , trade.networth.tid as tid from trade.networth , trade.customers where trade.networth.cid = trade.customers.cid and (cash > cast(json_evalPath(trade.networth.json_details, '$..networth.loanlimit')  as integer)  - availloan ) and trade.networth.tid =?",
    "select cash, securities, loanlimit, cid, availloan from trade.networth where (cash <=securities or cash  > ( loanLimit - availloan) ) and tid =?",
    "select securities, cash, availloan, loanlimit, cid, availloan from trade.networth where (availloan >=securities or cash > loanLimit - availloan) and tid = ?",
    //non uniq
    "select cast(json_evalPath(json_details, '$..networth.cid')  as integer) as cid ,  cash , securities ,  "  +
    " cast(json_evalPath(json_details, '$..networth.loanlimit')  as integer) as loanlimit, availloan  , " +
    " cast(json_evalPath(json_details, '$..networth.tid')  as integer) as tid from trade.networth",
    "select cid, (cash + securities - ( cast(json_evalPath(json_details, '$..networth.loanlimit')  as integer)  - availloan  )) as networth from trade.networth  order by networth desc, cid",
    "select cid, loanlimit, availloan from trade.networth where (loanlimit >? and cast(json_evalPath(json_details, '$..networth[0].loanlimit')  as integer) -availloan <= ?)  ",
    "select cast ( json_evalPath(json_details ,'$..networth[-1:].cid') as Integer)  as cid , cash, securities from trade.networth where (cash<? or securities >=?)",
    "select cast(json_evalPath(json_details , '$..networth[0].cid') as integer)  as cid, cash,securities,loanlimit, availloan , trade.networth.tid as tid from trade.networth where (cash > loanLimit - availloan)",
    "select cash, securities, loanlimit, cid, availloan from trade.networth where (cash <=securities or cash  > ( loanLimit - availloan) )",
    "select securities, cash, availloan, loanlimit, cid, availloan from trade.networth where (availloan >=securities or cash > loanLimit - availloan) "
  };
  
  
  protected static String[] updateJSON = { 
    //uniqs
    "update trade.networth set availloan=availloan-? , json_details = ? where json_evalPath(json_details, '$..networth[?(@.cid == -- && @.tid == -- )]') is not null", 
    "update trade.networth set securities=? , json_details = ? where json_evalPath(json_details, '$..networth[?(@.cid == -- && @.tid == -- )]') is not null",
    "update trade.networth set cash=cash-? , json_details = ? where json_evalPath(json_details, '$..networth[?(@.cid == -- && @.tid == -- )]') is not null",
    "update trade.networth set loanLimit=? , json_details = ? where tid = ?and  securities > ? ",  //bulk update
    //no uniqs
    "update trade.networth set availloan=availloan-?  , json_details = ? where cast(json_evalPath(json_details, '$..networth.cid')  as integer)  = ? ", 
    "update trade.networth set securities=? ,  json_details = ? where cast(json_evalPath(json_details, '$..networth.cid')  as integer)  = ?  ",
    "update trade.networth set cash=cash-? , json_details = ? where cid = ? ",
    "update trade.networth set loanLimit=? , json_details = ? where securities > ? "
  }; 
  
  
  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  protected static int maxNumOfTries = 2;  
  protected static int[] loanLimits = {1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, Integer.MAX_VALUE};
  protected static int numOfNonUniqUpdate = 4;  //how many update statement is for non unique keys
  protected static int numOfNonUniq = 7; //how many select statement is for non unique keys, non uniq query must be at the end
  protected static ArrayList<String> partitionKeys = null;
  protected static boolean hasTx = sql.SQLTest.hasTx;
  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#insert(java.sql.Connection, java.sql.Connection, int)
   */
  @Override
  public void insert(Connection dConn, Connection gConn, int size) {
    // the table should be populated/inserted when a new customer is added (trade.customers insert)
  }

  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#update(java.sql.Connection, java.sql.Connection, int)
   */
  
  //*** put ***/
  public void put(Connection dConn, Connection gConn, int size) {
    
  }
  @Override
  public void update(Connection dConn, Connection gConn, int size) {    
    int[] whichUpdate = new int[size];
    int[] cid = new int[size];
    BigDecimal[] availLoanDelta = new BigDecimal[size];
    BigDecimal[] sec = new BigDecimal[size];
    BigDecimal[] cashDelta = new BigDecimal[size];
    int[] newLoanLimit = new int[size];
    List<SQLException> exceptionList = new ArrayList<SQLException>();   
    
    
    if (dConn != null) {  
      if (rand.nextInt(numGettingDataFromDerby) == 1) 
        getDataForUpdate(dConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size); //get the data
      else 
        getDataForUpdate(gConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size); //get the data
      boolean success = updateDerbyTable(dConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size, exceptionList);  //insert to derby table  
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the update op in derby, will abort this operation in derby");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++;        
        exceptionList .clear();
        success = updateDerbyTable(dConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size, exceptionList);  //insert to derby table  
      } //retry until this update will be executed.
      updateGFETable(gConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size, exceptionList); 
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else {
      getDataForUpdate(gConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size); //get the data
      updateGFETable(gConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, whichUpdate, size);
    } //no verification

  }

  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#delete(java.sql.Connection, java.sql.Connection)
   */
  @Override
  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = delete.length/2;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    int cid = (dConn !=null && rand.nextInt(numGettingDataFromDerby) == 1)
      ? getCid(dConn) : getCid(gConn); 
    //for first delete (testUniqKey), getCid(dConn) will return the cid inserted by this 
    //thread or 0 which means delete will have no effect (no cid = 0 is being inserted)
    
    List<SQLException> exceptionList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, whichDelete, cid, exceptionList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the delete op in derby, will abort this operation in derby");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exceptionList.clear();
        success = deleteFromDerbyTable(dConn, whichDelete, cid, exceptionList); //retry
      }
      deleteFromGFETable(gConn, whichDelete, cid, exceptionList);
      SQLHelper.handleMissedSQLException(exceptionList);
    } 
    else {
      deleteFromGFETable(gConn, whichDelete, cid); //w/o verification
    }


  }

  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#query(java.sql.Connection, java.sql.Connection)
   */
  @Override
  public void query(Connection dConn, Connection gConn) {
    int whichQuery = getWhichOne(numOfNonUniq, select.length); //randomly select one query sql based on test uniq or not
    int cash = 100000;
    int sec = 100000;
    int tid = getMyTid();
    int loanLimit = loanLimits[rand.nextInt(loanLimits.length)];
    BigDecimal loanAmount = new BigDecimal (Integer.toString(rand.nextInt(loanLimit)));
    BigDecimal queryCash = new BigDecimal (Integer.toString(rand.nextInt(cash)));
    BigDecimal querySec= new BigDecimal (Integer.toString(rand.nextInt(sec)));
    
    ResultSet discRS = null;
    ResultSet gfeRS = null;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    
    if (dConn!=null) {
      try {
        discRS = query(dConn, whichQuery, queryCash, querySec, loanLimit, loanAmount, tid);
        if (discRS == null) {
          Log.getLogWriter().info("could not get the derby result set after retry, abort this query");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) 
            ; //do nothing, expect gfxd fail with the same reason due to alter table
          else return;
        }
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exceptionList);
      }
      try {
        gfeRS = query (gConn,  whichQuery, queryCash, querySec, loanLimit, loanAmount, tid);
        if (gfeRS == null) {
          if (isHATest) {
            Log.getLogWriter().info("Testing HA and did not get GFXD result set after retry");
            return;
          } else if (setCriticalHeap) {
            Log.getLogWriter().info("got XCL54 and does not get query result");
            return; //prepare stmt may fail due to XCL54 now
          }
          else     
            throw new TestException("Not able to get gfe result set after retry");
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptionList);
      }
      SQLHelper.handleMissedSQLException(exceptionList);
      if (discRS == null || gfeRS == null) return;
      
      boolean success = false;
      if (whichQuery == 1)   {        
        success = ResultSetHelper.compareSortedResultSets(discRS, gfeRS);  //order by case       
      }
      else {
        success =ResultSetHelper.compareResultSets(discRS, gfeRS); //no order by cases    
      }
      if (!success) {
        Log.getLogWriter().info("Not able to compare results due to derby server error");
      } //not able to compare results due to derby server error  
    }// we can verify resultSet
    else {
      try {
        gfeRS = query (gConn, whichQuery, queryCash, querySec, loanLimit, loanAmount, tid);
      } catch (SQLException se) {
        if (se.getSQLState().equals("42502") && SQLTest.testSecurity) {
          Log.getLogWriter().info("Got expected no SELECT permission, continuing test");
          return;
        } else if (alterTableDropColumn && se.getSQLState().equals("42X04")) {
          Log.getLogWriter().info("Got expected column not found exception, continuing test");
          return;
        } else SQLHelper.handleSQLException(se);
      }
      if (gfeRS != null)
        ResultSetHelper.asList(gfeRS, false);  
      else if (isHATest)
        Log.getLogWriter().info("could not get gfxd query results after retry due to HA");
      else if (setCriticalHeap)
        Log.getLogWriter().info("could not get gfxd query results after retry due to XCL54");
      else
        throw new TestException ("gfxd query returns null and not a HA test");    
    }
  }
  
  protected void getDataForInsert(BigDecimal[] cash, 
      int [] loanLimit, BigDecimal[] availLoan, int size) {
    int minStart = 1000;
    int maxStart = 100000;

    int numLimits = loanLimits.length;
    int start = rand.nextInt(maxStart - minStart + 1) + minStart;
    
    for (int i=0; i<size; i++) {
      cash[i]= new BigDecimal(Integer.toString(rand.nextInt(start)));
      /*
      if (i==1 || i == 2 || i ==3 || i==4) {
        cash[i] = new BigDecimal(Integer.toString(maxStart));        
      } //to have some records have the same max value to be sorted
      */
      loanLimit[i] = loanLimits[rand.nextInt(numLimits)];
      availLoan[i] =  new BigDecimal(Integer.toString(loanLimit[i]));
    }
    
  }

  //actually insert should be called by insert thread for customers table
  public void insert(Connection dConn, Connection gConn, int size, int[] cid) {
    BigDecimal[] cash = new BigDecimal[size];
    int[] loanLimit = new int[size];
    BigDecimal[] availLoan = new BigDecimal[size];
    getDataForInsert(cash, loanLimit, availLoan, size);
    BigDecimal securities = new BigDecimal(Integer.toString(0));

    List<SQLException> exList = new ArrayList<SQLException>();    

    if (dConn!=null) {
      boolean success = insertToDerbyTable(dConn, cid, cash, securities, loanLimit, availLoan, size, exList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the insert op in derby, will abort this operation in derby");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++;   
        exList .clear(); //clear the exceptionList and retry
        success = insertToDerbyTable(dConn, cid, cash, securities, loanLimit, availLoan, size, exList);
      } 
      try {
        insertToGFETable(gConn, cid, cash, securities, loanLimit, availLoan, size, exList);
      } catch (TestException te) {
        if (te.getMessage().contains("Execute SQL statement failed with: 23505")
            && isHATest && SQLTest.isEdge) {
          //checkTicket49605(dConn, gConn, "networth");
          try {
            checkTicket49605(dConn, gConn, "customers", cid[0], -1, null, null);
          } catch (TestException e) {
            Log.getLogWriter().info("insert failed due to #49605 ", e);
            //deleteRow(dConn, gConn, "buyorders", cid[0], -1, null, null);
            Log.getLogWriter().info("retry this using put to work around #49605");
            insertToGFETable(gConn, cid, cash, securities, loanLimit, availLoan, size, exList, true);
            
            checkTicket49605(dConn, gConn, "customers", cid[0], -1, null, null);
          }
        } else throw te;
      }
      SQLHelper.handleMissedSQLException(exList);      
    }    
    else {
      insertToGFETable(gConn, cid, cash, securities, loanLimit, availLoan, size);
    } //no verification
  }
  
  public void insertTx(Connection dConn, Connection gConn, int size, int[] cid) {
    //TODO need to be implemented
    Log.getLogWriter().info("Needs to be implemented");
  }
  
  //insert into Derby
  protected boolean insertToDerbyTable(Connection conn, int[] cid, BigDecimal[] cash, 
      BigDecimal securities, int[] loanLimit, BigDecimal[] availLoan, int size, 
      List<SQLException> exceptions)  {
    PreparedStatement stmt = getStmt(conn, insert);
    if (stmt == null) return false;
    int tid = getMyTid();
    int count = -1;
    
    for (int i = 0; i < size; i++) {
      try {
      verifyRowCount.put(tid+"_insert"+i, 0);
        count = insertToTable(stmt, cid[i], cash[i], securities, loanLimit[i], availLoan[i], tid);
        verifyRowCount.put(tid+"_insert"+i, new Integer(count));        
      }  catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se))     return false;
        else    SQLHelper.handleDerbySQLException(se, exceptions);
      }   
    }
    return true;
  }
  
  //insert into GFE
  protected void insertToGFETable (Connection conn, int[] cid, BigDecimal[] cash, 
      BigDecimal securities, int[] loanLimit, BigDecimal[] availLoan, int size, 
      List<SQLException> exceptions)  {
    PreparedStatement stmt;
    if (SQLTest.hasJSON)
      stmt = getStmt(conn, insertJSON);
    else 
     stmt = getStmt(conn, insert);
    if (SQLTest.testSecurity && stmt == null) {
    	SQLHelper.handleGFGFXDException((SQLException)
    			SQLSecurityTest.prepareStmtException.get(), exceptions);
    	SQLSecurityTest.prepareStmtException.set(null);
    	return;
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    int count = -1;
    for (int i = 0; i < size; i++) {
      try {
        count = insertToTable(stmt, cid[i], cash[i], securities, loanLimit[i], availLoan[i], tid);
        if (count != verifyRowCount.get(tid+"_insert"+i).intValue()) {
          Log.getLogWriter().info("Gfxd insert has different row count from that of derby " +
            "derby inserted " + verifyRowCount.get(tid+"_insert"+i).intValue() +
            " but gfxd inserted " + count);
        }
      }  catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptions);
      }    
    }
  }
  
  //insert into GFE
  protected void insertToGFETable (Connection conn, int[] cid, BigDecimal[] cash, 
      BigDecimal securities, int[] loanLimit, BigDecimal[] availLoan, int size, 
      List<SQLException> exceptions, boolean isPut)  {
    PreparedStatement stmt;
    if (SQLTest.hasJSON)
       stmt = getStmt(conn, isPut ? putJSON  : insertJSON);
    else
       stmt = getStmt(conn, isPut ? put  : insert);
    if (SQLTest.testSecurity && stmt == null) {
      SQLHelper.handleGFGFXDException((SQLException)
          SQLSecurityTest.prepareStmtException.get(), exceptions);
      SQLSecurityTest.prepareStmtException.set(null);
      return;
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    int count = -1;
    for (int i = 0; i < size; i++) {
      try {
        count = insertToTable(stmt, cid[i], cash[i], securities, loanLimit[i], availLoan[i], tid, isPut);
        if (count != verifyRowCount.get(tid+"_insert"+i).intValue()) {
          Log.getLogWriter().info("Gfxd insert has different row count from that of derby " +
            "derby inserted " + verifyRowCount.get(tid+"_insert"+i).intValue() +
            " but gfxd inserted " + count);
        }
      }  catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptions);
      }    
    }
  }
  
  //insert into GFE w/o verification
  protected void insertToGFETable (Connection conn, int[] cid, BigDecimal[] cash, 
      BigDecimal securities, int[] loanLimit, BigDecimal[] availLoan, int size)  {
    
    PreparedStatement stmt;
    if (SQLTest.hasJSON )
      stmt= getStmt(conn, insertJSON);
    else 
      stmt= getStmt(conn, insert);
    
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    
    for (int i = 0; i < size; i++) {
      try {
        insertToTable(stmt, cid[i], cash[i], securities, loanLimit[i], availLoan[i], tid);
      }  catch (SQLException se) {
        if (se.getSQLState().equals("23503"))
          Log.getLogWriter().info("detected foreign key constraint violation during insert, continuing test");
        else if (se.getSQLState().equals("42500") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
             " continuing tests");
        } else if (alterTableDropColumn && se.getSQLState().equals("42802")) {
          Log.getLogWriter().info("Got expected column not found exception in insert, continuing test");
        } else if (se.getSQLState().equals("23505")
            && isHATest && SQLTest.isEdge) {
          Log.getLogWriter().info("detected pk constraint violation during insert -- relaxing due to #43571, continuing test");
        } else if (alterTableDropColumn && se.getSQLState().equals("42X14")) {
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else
          SQLHelper.handleSQLException(se);
      }  
    }
  }

  //insert a record into the table
  protected int insertToTable(PreparedStatement stmt, int cid, BigDecimal cash,
      BigDecimal securities, int loanLimit, BigDecimal availLoan, int tid)
      throws SQLException {
    return insertToTable(stmt, cid, cash, securities, loanLimit, availLoan, tid, false);
  }
  //insert a record into the table
  protected int insertToTable(PreparedStatement stmt, int cid, BigDecimal cash,
      BigDecimal securities, int loanLimit, BigDecimal availLoan, int tid, boolean isPut)
      throws SQLException {
    String jsonLog="";
    String jsontext = "";
    if (SQLTest.hasJSON  & !SQLHelper.isDerbyConn(stmt.getConnection())) {
      jsontext = getJSON(cid, cash, securities, loanLimit, availLoan, tid);
      jsonLog = ", JSON_DETAILS: " + jsontext;
    }
    String txid =  SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " "; 
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - " + txid + " " ;
    Log.getLogWriter().info( database + (isPut ? "putting" : "inserting") + " into trade.networth with CID:"
        + cid + ",CASH:" + cash + ":SECURITIES," + securities
        + ",LOANLIMIT:" + loanLimit + ",AVAILLOAN:" + availLoan
        + ",TID:" + tid + jsonLog);
    stmt.setInt(1, cid);
    stmt.setBigDecimal(2, cash);
    stmt.setBigDecimal(3, securities);  //insert is 0, will be updated by security through trigger
    stmt.setInt(4, loanLimit); 
    stmt.setBigDecimal(5, availLoan);   //availLoan is the same as loanLimit during insert
    stmt.setInt(6, tid);
    
    if (SQLTest.hasJSON & ! SQLHelper.isDerbyConn(stmt.getConnection())) {
      stmt.setObject(7, jsontext);
    }
    int rowCount = stmt.executeUpdate();
    //update the same row in the parent table customer as  well in case of json
    if (!SQLTest.hasTx && !setCriticalHeap)
    updateJSONToCustomer(stmt.getConnection() ,  cid , getJSONObject(cid, cash, securities, loanLimit, availLoan, tid));
    Log.getLogWriter().info(database + (isPut ? "put" : "inserted ") + rowCount + " rows in trade.networth CID:"
        + cid + ",CASH:" + cash + ":SECURITIES," + securities
        + ",LOANLIMIT:" + loanLimit + ",AVAILLOAN:" + availLoan
        + ",TID:" + tid +  jsonLog);
    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning   
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    
    if ( database.contains("gemfirexd") && isPut) {
      if (! SQLTest.ticket49794fixed) {
        //manually update fulldataset table for above entry.
          String deleteStatement = "DELETE FROM TRADE.NETWORTH_FULLDATASET  WHERE  cid = "  + cid ;
          String insertStatement = " INSERT INTO TRADE.NETWORTH_FULLDATASET  VALUES ( " + cid + " ,  " +   cash  + " ,  " + securities + "," +  loanLimit  + " ,  " +  availLoan + "," +   tid +  ")";
          Log.getLogWriter().info(" Trigger behaviour is not defined for putDML hence deleting  the  row  from TRADE.NETWORTH_FULLDATASET with data CID:" +  cid );
          stmt.getConnection().createStatement().execute(deleteStatement);
          Log.getLogWriter().info(" Trigger behaviour is not defined for putDML hence inserting  the  row  into  TRADE.NETWORTH_FULLDATASET with data CID:" +  cid +  ",CASH:" + cash  + ",SECURITIES:" +  securities + " ,LOANLIMIT:" + loanLimit + " ,AVAILLOAN:" + availLoan + ",TID:" + tid );
          stmt.getConnection().createStatement().execute(insertStatement);
        }
         Log.getLogWriter().info( database + (isPut ? "putting" : "inserting") + " into trade.networth with CID:"
          + cid + ",CASH:" + cash + ":SECURITIES," + securities
          + ",LOANLIMIT:" + loanLimit + ",AVAILLOAN:" + availLoan
          + ",TID:" + tid);
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + (isPut ? "put" : "inserted ") + rowCount + " rows in trade.networth CID:"
            + cid + ",CASH:" + cash + ":SECURITIES," + securities
            + ",LOANLIMIT:" + loanLimit + ",AVAILLOAN:" + availLoan
            + ",TID:" + tid);
        warning = stmt.getWarnings(); //test to see there is a warning   
        if (warning != null) {
          SQLHelper.printSQLWarning(warning);
        } 
    }    
    return rowCount;
  }
  

  
  /*** update related methods ****/ 
  protected void getDataForUpdate(Connection conn, int[] cid, BigDecimal[] availLoanDelta, 
      BigDecimal[] sec, BigDecimal[] cashDelta, int[] newLoanLimit, int[] whichUpdate, int size) {
    int maxAvailLimitDelta = 10000;
    int maxCashDelta = 10000;
    int maxSec = 1000000;
    for (int i = 0; i<size; i++) {
      if (testUniqueKeys || testWanUniqueness) {
        cid[i] =  getCid(conn);  //to get the cid and could be updated by this thread for uniq
        //cid could be 0 and the update will fail as the record for cid is 0 (in customer and networth) does not exist
      } else {
        cid[i] = rand.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary))+1;
      } //non uniq
      availLoanDelta[i] = new BigDecimal(Double.toString(rand.nextInt(maxAvailLimitDelta * 100 +1)*0.01));
      sec[i] = new BigDecimal(Double.toString(rand.nextInt(maxSec * 100 +1)*0.01)); //will be updated in portfolio in trigger test
      cashDelta[i] =  new BigDecimal(Double.toString(rand.nextInt(maxCashDelta * 100 +1)*0.01));
      newLoanLimit[i] = loanLimits[rand.nextInt(loanLimits.length)];
      whichUpdate[i] = getWhichOne(numOfNonUniqUpdate, update.length);
      if ((SQLTest.testSecurity && whichUpdate[i] == 3 && !hasTx) //avoid bulk operation in security test due to #43244
          || (setCriticalHeap && whichUpdate[i] == 3 ) //avoid #39605 for bulk operation due to XCL54 
      ) whichUpdate[i]--; 
    }
  }
  
  //update records in Derby 
  protected boolean updateDerbyTable(Connection conn, int[] cid, BigDecimal[] availLoanDelta, 
      BigDecimal[] sec, BigDecimal[] cashDelta, int[] newLoanLimit, int[] whichUpdate, 
      int size, List<SQLException> exList){
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      if (isHATest && (whichUpdate[i] == 0 || whichUpdate[i] == 2)) {
        continue;
      } //avoid x=x+1 update in HA test for now
      boolean[] unsupported = new boolean[1];
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], unsupported);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed

      if (stmt == null) {
        if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true)
          return true; //do the same in gfxd to get alter table exception
        else if (unsupported[0]) return true; //do the same in gfxd to get unsupported exception
        else return false;
        /*
        try {
          conn.prepareStatement(update[whichUpdate[i]]);
        } catch (SQLException se) {
          if (se.getSQLState().equals("08006") || se.getSQLState().equals("08003"))
            return false;
        } 
        */
        //this test of connection is lost is necessary as stmt is null
        //could be caused by not allowing update on partitioned column. 
        //the test of connection lost could be removed after #39913 is fixed
        //just return false if stmt is null
      }
      
      try {
        if (stmt!=null) {
          verifyRowCount.put(tid+"_update"+i, 0);
          count = updateTable(stmt, cid[i], availLoanDelta[i], sec[i], cashDelta[i], newLoanLimit[i], tid, whichUpdate[i] );        
          verifyRowCount.put(tid+"_update"+i, new Integer(count));
          
        }
      } catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
          Log.getLogWriter().info("detected the deadlock, will try it again");
          return false;
        } else
            SQLHelper.handleDerbySQLException(se, exList);
      }    
    } 
    return true;
  }
  
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate, boolean[] unsupported){
    if (partitionKeys == null) setPartitionKeys();

    return getCorrectStmt(conn, whichUpdate, partitionKeys, unsupported);
  }
  
  @SuppressWarnings("unchecked")
  protected void setPartitionKeys() {
    if (!isWanTest) {
      partitionKeys= (ArrayList<String>)partitionMap.get("networthPartition");
    }
    else {
      int myWanSite = getMyWanSite();
      partitionKeys = (ArrayList<String>)wanPartitionMap.
        get(myWanSite+"_networthPartition");
    }
    Log.getLogWriter().info("partition keys are " + partitionKeys);
  }
  
  //used to parse the partitionKey and test unsupported update on partitionKey, no need after bug #39913 is fixed
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys, boolean[] unsupported){
    PreparedStatement stmt = null;
    String[] update = TradeNetworthDMLStmtJson.update;    
    
    if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(conn) ){
        update = TradeNetworthDMLStmtJson.updateJSON;
    }
    
    switch (whichUpdate) {
    case 0: 
      //"update trade.networth set availloan=availloan-? where cid = ? and tid= ?", 
      if (partitionKeys.contains("availloan")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
      //"update trade.networth set securities=? where cid = ?  and tid= ?",
      if (partitionKeys.contains("securities")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: 
      //"update trade.networth set cash=cash-? where cid = ? and tid= ?",
      if (partitionKeys.contains("cash")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: 
      //"update trade.networth set loanLimit=? where  tid= ? and securities > ?",  //batch operation 
      if (partitionKeys.contains("loanLimit")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: 
      //"update trade.networth set availloan=availloan-? where cid = ? ", 
      if (partitionKeys.contains("availloan")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 5: 
      // "update trade.networth set securities=? where cid = ?  ",
      if (partitionKeys.contains("securities")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 6: 
      // "update trade.networth set cash=cash-? where cid = ? ", 
      if (partitionKeys.contains("cash")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 7: 
      //"update trade.networth set loanLimit=? where securities > ? " 
      if (partitionKeys.contains("loanLimit")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
   
    return stmt;
  }
  
  //update records in gemfirexd  
  protected void updateGFETable(Connection conn, int[] cid, BigDecimal[] availLoanDelta, 
      BigDecimal[] sec, BigDecimal[] cashDelta, int[] newLoanLimit, int[] whichUpdate, 
      int size, List<SQLException> exList){
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -2;
    
    for (int i=0 ; i<size ; i++) {
      if (isHATest && (whichUpdate[i] == 0 || whichUpdate[i] == 2)) {
      continue;
      } //avoid x=x+1 update in HA test for now
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], null);
      else   stmt = getStmt(conn, (SQLTest.hasJSON ? updateJSON[whichUpdate[i]]: update[whichUpdate[i]])); //use only this after bug#39913 is fixed

      if (SQLTest.testSecurity && stmt == null) {
      	if (SQLSecurityTest.prepareStmtException.get() != null) {
      	  SQLHelper.handleGFGFXDException((SQLException)
      			SQLSecurityTest.prepareStmtException.get(), exList);
      	  SQLSecurityTest.prepareStmtException.set(null);
      	  return;
      	}
      } //work around #43244
      if (setCriticalHeap && stmt == null) {
        return; //prepare stmt may fail due to XCL54 now
      }
      
      try {
        if (stmt!=null) {
          count = updateTable(stmt, cid[i],availLoanDelta[i], sec[i], cashDelta[i], newLoanLimit[i], tid, whichUpdate[i] );
          if (count != verifyRowCount.get(tid+"_update"+i).intValue()){
            Log.getLogWriter().info("Gfxd update has different row count from that of derby " +
                    "derby updated " + verifyRowCount.get(tid+"_update"+i).intValue() +
                    " but gfxd updated " + count);
          }
        }
      }  catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exList);
      }    
    }   
  }
  
  //update records in gemfirexd, no verification
  protected void updateGFETable(Connection conn, int[] cid, BigDecimal[] availLoanDelta, 
      BigDecimal[] sec, BigDecimal[] cashDelta, int[] newLoanLimit, int[] whichUpdate, int size){
    PreparedStatement stmt = null;
    int tid = getMyTid();
    
    for (int i=0 ; i<size ; i++) {
      if (isHATest && whichUpdate[i] % 2 == 0) {
        continue;
      } //avoid x=x+1 update in HA test for now
      
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], null);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed
      
      try {
        if (stmt !=null)
          updateTable(stmt, cid[i],availLoanDelta[i], sec[i], cashDelta[i], newLoanLimit[i], tid, whichUpdate[i] );
      }  catch (SQLException se) {
        if (se.getSQLState().equals("23513"))  
          Log.getLogWriter().info("detected the constraint check violation, continuing test");
        else if (se.getSQLState().equals("42502") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
             " continuing tests");
        } else if (alterTableDropColumn && (se.getSQLState().equals("42X14")|| se.getSQLState().equals("42X04"))) {
          //42X04 is possible when column in where clause is dropped
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else
          SQLHelper.handleSQLException(se);
      }    
    }   
  }
  
  protected int updateTable(PreparedStatement stmt, int cid, BigDecimal availLoanDelta, BigDecimal sec,
      BigDecimal cashDelta, int newLoanLimit, int tid, int whichUpdate) throws SQLException {
    int rowCount =0;
    String txid = SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " ";
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - " + txid + " "; ;
    String query = " QUERY: " + update[whichUpdate];
    
    String jsonString ="";
    String jsonLog ="";
    JSONObject json = new JSONObject();
    HashMap<String,Object> map = new HashMap<String, Object>();
    
    if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
      map = (HashMap<String,Object>) getCurrentRowOfUpdate(stmt.getConnection() , cid);
      query = " QUERY: " + updateJSON[whichUpdate];      
    } 
       
    int i =1;
    switch (whichUpdate) { 
    case 0: 
      //"update trade.networth set availLoan=availLoan-? where cid = ? and tid= ?",

        if (!SQLHelper.isDerbyConn(stmt.getConnection())) {
          json = getJSONObject(cid, (BigDecimal)map.get("cash"),
              (BigDecimal)map.get("securities"), (Integer)map.get("loanlimit"),
              ((BigDecimal)map.get("availloan")).subtract(availLoanDelta), (Integer)map.get("tid"));
          jsonString = json.toJSONString();
          jsonLog = ",JSON_DETAILS: " + jsonString;
          String jsonQuery = updateJSON[whichUpdate].replaceFirst("--", Integer
              .toString(cid));
          jsonQuery = jsonQuery.replaceFirst("--", Integer.toString(tid));
          stmt = stmt.getConnection().prepareStatement(jsonQuery);
        }

        Log.getLogWriter().info(
            database + "updating trade.networth with AVAILLOAN:(availLoan-"
                + availLoanDelta + ")  " + jsonLog + " where CID:" + cid
                + ",TID:" + tid + query);

        stmt.setBigDecimal(i++, availLoanDelta);
        if (!SQLHelper.isDerbyConn(stmt.getConnection())) {
          stmt.setString(i++, jsonString);          
        }
        else {
          stmt.setInt(i++, cid);
          stmt.setInt(i++, tid);
        }

        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(
            database + "updated " + rowCount
                + " rows in trade.networth  AVAILLOAN:(availLoan-"
                + availLoanDelta + ") " + jsonLog + " where CID:" + cid
                + ",TID:" + tid + query);

        break;
    case 1: 
      //"update trade.networth set securities=? where cid = ?  and tid= ?",

        if (!SQLHelper.isDerbyConn(stmt.getConnection())) {
          json = getJSONObject(cid, (BigDecimal)map.get("cash"), sec,
              (Integer)map.get("loanlimit"), (BigDecimal)map.get("availloan"),
              (Integer)map.get("tid"));
          jsonString = json.toJSONString();
          jsonLog = ",JSON_DETAILS: " + jsonString;
          String jsonQuery = updateJSON[whichUpdate].replaceFirst("--", Integer
              .toString(cid));
          jsonQuery = jsonQuery.replaceFirst("--", Integer.toString(tid));
          stmt = stmt.getConnection().prepareStatement(jsonQuery);
        }

        Log.getLogWriter().info(
            database + "updating trade.networth with SECURITIES:" + sec
                + jsonLog + " where CID:" + cid + ",TID:" + tid + query);
        stmt.setBigDecimal(i++, sec);
        if (!SQLHelper.isDerbyConn(stmt.getConnection())) {
          stmt.setString(i++, jsonString);
        }
        else {
          stmt.setInt(i++, cid);
          stmt.setInt(i++, tid);
        }
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(
            database + "updated " + rowCount
                + " rows in trade.networth SECURITIES:" + sec + jsonLog
                + " where CID:" + cid + ",TID:" + tid + query);
        break;
    case 2: 
      //"update trade.networth set cash=cash-? where cid = ? and tid= ?",
      
      if (SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
          json = getJSONObject(cid, ((BigDecimal)map.get("cash"))
              .subtract(cashDelta), (BigDecimal)map.get("securities"),
              (Integer)map.get("loanlimit"), (BigDecimal)map.get("availloan"),
              (Integer)map.get("tid"));
          jsonString = json.toJSONString();
          jsonLog = ",JSON_DETAILS: " + jsonString;
          String jsonQuery = updateJSON[whichUpdate].replaceFirst("--", Integer
              .toString(cid));
          jsonQuery = jsonQuery.replaceFirst("--", Integer.toString(tid));
          stmt = stmt.getConnection().prepareStatement(jsonQuery);
        }

        Log.getLogWriter().info(
            database + "updating trade.networth with CASH:(cash-" + cashDelta
                + ") " + jsonLog + " where CID:" + cid + ",TID:" + tid + query);
        stmt.setBigDecimal(i++, cashDelta);
        if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection())) {
          stmt.setObject(i++, jsonString);
        }
        else {
          stmt.setInt(i++, cid);
          stmt.setInt(i++, tid);
        }
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(
            database + "updated " + rowCount
                + " rows in trade.networth CASH:(cash-" + cashDelta + ") "
                + jsonLog + " where CID:" + cid + ",TID:" + tid + query);
        break; 
    case 3: 
      //"update trade.networth set loanLimit=? where securities > ? and tid= ?",  //bulk operation
      
      if (SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
          json = getJSONObject(cid, (BigDecimal)map.get("cash"),
              (BigDecimal)map.get("securities"), newLoanLimit, (BigDecimal)map
                  .get("availloan"), (Integer)map.get("tid"));
          jsonString = json.toJSONString();
          jsonLog = ",JSON_DETAILS: " + jsonString;
         }

        if (!hasTx || SQLTest.hasJSON)
          Log
              .getLogWriter()
              .info(
                  database
                      + "updating trade.networth multiple rows -- will not execute due to limitation "
                      + "when exception is thrown due to constraint check, some rows are updated already"
                      + query);
        else {
          Log.getLogWriter().info(
              database + "updating trade.networth with LOANLIMIT:"
                  + newLoanLimit + jsonLog + " where SECURITIES:" + sec
                  + ",TID:" + tid + query);
          stmt.setInt(i++, newLoanLimit);
          if (!SQLHelper.isDerbyConn(stmt.getConnection())) {
            stmt.setObject(i++, jsonString);
          }
            stmt.setInt(i++, tid);
            stmt.setBigDecimal(i++, sec);
          

          rowCount = stmt.executeUpdate();
          Log.getLogWriter().info(
              database + "updated " + rowCount
                  + " rows in trade.networth LOANLIMIT:" + newLoanLimit
                  + jsonLog + " where SECURITIES:" + sec + ",TID:" + tid
                  + query);

        } // only test for transaction case for rollback all operations
        break;

      case 4: 
      //"update trade.networth set availLoan=availLoan-? where cid = ? ", 
        
        if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
          json = getJSONObject(cid, (BigDecimal ) map.get("cash"), (BigDecimal ) map.get("securities"), (Integer) map.get("loanlimit"), ((BigDecimal) map.get("availloan")).subtract(availLoanDelta), (Integer)map.get("tid"));
          jsonString=json.toJSONString();
          jsonLog = ",JSON_DETAILS: " + jsonString;
          }
        
      Log.getLogWriter().info(database + "updating trade.networth with AVAILLOAN:(availLoan- " + availLoanDelta 
          + ") " + jsonLog + " where CID:" + cid + query);
      stmt.setBigDecimal(i++, availLoanDelta);
      if (SQLTest.hasJSON && ! SQLHelper.isDerbyConn(stmt.getConnection())) stmt.setString(i++, jsonString);
      stmt.setInt(i++, cid);      
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.networth AVAILLOAN:(availLoan- " + availLoanDelta 
          + ") " + jsonLog + " where CID:" + cid + query);
      break;
    case 5: 
      //"update trade.networth set securities=? where cid = ? ",
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSONObject(cid, (BigDecimal ) map.get("cash"), sec, (Integer) map.get("loanlimit"), (BigDecimal) map.get("availloan"), (Integer)map.get("tid"));
        jsonString=json.toJSONString();
        jsonLog = ",JSON_DETAILS: " + jsonString;
        }
      
      Log.getLogWriter().info(database +  "updating trade.networth with SECURITIES = " +sec +  jsonLog + " where CID:" + cid + query );
      stmt.setBigDecimal(i++, sec);
      if (SQLTest.hasJSON && ! SQLHelper.isDerbyConn(stmt.getConnection())) stmt.setString(i++, jsonString);
      stmt.setInt(i++, cid);  //TODO comment out this line to reproduce #42008
      
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.networth SECURITIES = " +sec   + jsonLog + " where CID:" + cid + query );
      break;

    case 6: 
      //"update trade.networth set cash=cash-? where cid = ?",
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSONObject(cid, ((BigDecimal ) map.get("cash")).subtract(cashDelta), (BigDecimal ) map.get("securities"), (Integer) map.get("loanlimit"), (BigDecimal) map.get("availloan"), (Integer)map.get("tid"));
        jsonString=json.toJSONString();
        jsonLog = ",JSON_DETAILS: " + jsonString;
        }
      
      Log.getLogWriter().info(database +  "updating trade.networth with CASH:(cash- " + cashDelta 
          + ") "  + jsonLog + " where CID:" + cid + query );          
      stmt.setBigDecimal(i++, cashDelta);
      if (SQLTest.hasJSON && ! SQLHelper.isDerbyConn(stmt.getConnection())) stmt.setString(i++, jsonString);
      stmt.setInt(i++, cid);
      
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.networth CASH:(cash- " + cashDelta 
          + ") " + jsonLog + " where CID:" + cid + query );
      break; 

    case 7: //update name, since
      //"update trade.networth set loanLimit=? where securities > ? ",  //batch operation
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSONObject(cid, (BigDecimal ) map.get("cash"), (BigDecimal ) map.get("securities"), newLoanLimit, (BigDecimal) map.get("availloan"), (Integer)map.get("tid"));
        jsonString=json.toJSONString();
        jsonLog = ",JSON_DETAILS: " + jsonString;
        }
      
      if (!SQLTest.hasAsyncDBSync && !isSerial && !SQLTest.hasJSON) {
        Log.getLogWriter().info(database +  "updating trade.networth  LOANLIMIT:" + newLoanLimit  + jsonLog + " where SECURITIES:" 
            + sec + query);
        stmt.setInt(i++, newLoanLimit);
        if (SQLTest.hasJSON && ! SQLHelper.isDerbyConn(stmt.getConnection())) stmt.setString(i++, jsonString);
        stmt.setBigDecimal(i++, sec);
        
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.networth LOANLIMIT:" + newLoanLimit  + jsonLog + " where SECURITIES:" 
            + sec + query);
      } else {
        Log.getLogWriter().info(database +  "update multiple rows in trade.networth -- will not execute due to limitation: " +
        "when exception is thrown due to constraint check, some rows are updated already"+ query );
      }
      break; 
    default:
     throw new TestException ("Wrong update sql string here");
    }
    
    
    //pdate customer table
    if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection())&& !SQLTest.hasTx)
      updateJSONToCustomer(stmt.getConnection(), cid, json);
      
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;    
  }
 
  /*** query related methods ***/
  //retries when lock could not obtained
  public static ResultSet getQuery(Connection conn, int whichQuery, BigDecimal cash, BigDecimal sec, 
      int loanLimit, BigDecimal loanAmount, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    try {
      rs =getQuery(conn, whichQuery, cash, sec, loanLimit, loanAmount, tid, success);
      int count = 0;
      while (!success[0]) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
          return null; 
        }
        count++;   
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        rs = getQuery(conn, whichQuery, cash, sec, loanLimit, loanAmount, tid, success);
      } //retry
    } catch (SQLException se) {
      if (!SQLHelper.isAlterTableException(conn, se)) SQLHelper.handleSQLException(se);
      //allow alter table related exceptions.
    }
    return rs;
  }
  
  protected static ResultSet query(Connection conn, int whichQuery, BigDecimal cash, BigDecimal sec, 
      int loanLimit, BigDecimal loanAmount, int tid) throws SQLException {
    boolean[] success = new boolean[1];
    ResultSet rs = getQuery(conn, whichQuery, cash, sec, loanLimit, loanAmount, tid, success);
    int count = 0;
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
        return null; 
      }
      count++;   
      MasterController.sleepForMs(rand.nextInt(retrySleepMs));
      rs = getQuery(conn, whichQuery, cash, sec, loanLimit, loanAmount, tid, success);
    } //retry
    return rs;
  }
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, BigDecimal cash, BigDecimal sec, 
      int loanLimit, BigDecimal loanAmount, int tid, boolean[] success) throws SQLException {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - " ;
    
    if ( (SQLTest.hasHdfs || SQLTest.hasTx || setCriticalHeap) && (whichQuery == 3 || whichQuery ==4) ) {
      whichQuery =5;
    }
    
    String query = " QUERY: " + select[whichQuery]; 
    if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(conn) ){
      query = " QUERY: " + selectJSON[whichQuery]; 
    }
         
    try {
      
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(conn) ){
           stmt = conn.prepareStatement(selectJSON[whichQuery]);
      } else 
      stmt = conn.prepareStatement(select[whichQuery]);
      /*
      stmt = getStmt(conn, select[whichQuery]); 
      if (setCriticalHeap && stmt == null) {
        return null; //prepare stmt may fail due to XCL54 now
      }
      */
      
      switch (whichQuery){
      case 0:
        //"select * from trade.networth where tid = ?",
        Log.getLogWriter().info(database + "querying trade.networth with TID:" + tid + query );     
        stmt.setInt(1, tid);
        break;
      case 1: 
        //"select cid, (cash + securities - (loanLimit - availloan))as networth from trade.networth where tid = ? ",
        Log.getLogWriter().info(database + "querying trade.networth with TID:" + tid + query  );     
        stmt.setInt(1, tid);
        break;
      case 2:
        //"select cid, loanlimit, availloan from trade.networth where (loanlimit >? and loanlimit-availloan <= ?) and tid=? ",
        Log.getLogWriter().info(database + "querying trade.networth with LOANLIMIT:" + loanLimit + ",LOANAMOUNT:" 
            + loanAmount + ",TID:" + tid  + query );     
        stmt.setInt(1, loanLimit);
        stmt.setBigDecimal(2, loanAmount); 
        stmt.setInt(3, tid);
        break;
      case 3:
        //"select cid, cash, securities from trade.networth where (cash<? or securities >=?) and tid =?",        
        if ( !SQLHelper.isDerbyConn(conn)) {
          String jsonQuery = selectJSON[whichQuery];
          Log.getLogWriter().info(database + "querying trade.networth with CASH:" +cash + ",SECURITIES:" + sec 
              + ",TID:" + tid + jsonQuery  ); 
          stmt.setBigDecimal(1, cash);
          stmt.setBigDecimal(2, sec);
          stmt.setInt(3, tid);
        } else {
          Log.getLogWriter().info(database + "querying trade.networth with CASH:" +cash + ",SECURITIES:" + sec 
              + ",TID:" + tid + query  ); 
        stmt.setBigDecimal(1, cash);
        stmt.setBigDecimal(2, sec);
        stmt.setInt(3, tid);
        }
        break;
      case 4:
        //"select * from trade.networth where (cash > loanLimit - availloan) and tid =?",
        Log.getLogWriter().info(database + "querying trade.networth with TID:" + tid  + query );     
        stmt.setInt(1, tid);
        break;
      case 5:     
       // "select cash, securities, loanlimit, cid, availloan from trade.networth where (cash <=securities or cash > loanLimit - availloan) and tid =?",      
        Log.getLogWriter().info(database + "querying trade.networth with TID:" + tid  + query ); 
        stmt.setInt(1, tid);
        break;
      case 6:
        //"select securities, cash, availloan, loanlimit, cid, availloan from trade.networth where (availloan >=securities or cash > loanLimit - availloan) and tid = ?",
        Log.getLogWriter().info(database + "querying trade.networth with TID:" + tid  + query ); 
        stmt.setInt(1, tid);
        break;
      case 7:
        Log.getLogWriter().info(database + "querying: " + query); 
        //"select * from trade.networth 
        break;
      case 8: 
        Log.getLogWriter().info(database + "querying: " + query); 
        //"select cid, (cash + securities - (loanLimit - availloan))as networth from trade.networth 
        break;
      case 9:
        //"select cid, loanlimit, availloan from trade.networth where (loanlimit >? and loanlimit-availloan <= ?) 
        Log.getLogWriter().info(database + "querying trade.networth with LOANLIMIT:" + loanLimit + ",LOANAMOUNT:" + loanAmount + query );     
        stmt.setInt(1, loanLimit);
        stmt.setBigDecimal(2, loanAmount); 
        break;
      case 10:
        //"select cid, cash, securities from trade.networth where (cash<? or securities >=?) 
        Log.getLogWriter().info(database + "querying trade.networth with CASH:" +cash + ",SECURITIES:" + sec + query );     
        stmt.setBigDecimal(1, cash);
        stmt.setBigDecimal(2, sec);
        break;
      case 11:
        Log.getLogWriter().info(database + "querying: " + query); 
        //"select * from trade.networth where (cash > loanLimit - availloan) 
        break;
      case 12:     
        Log.getLogWriter().info(database + "querying: " + query); 
       // "select cash, securities, loanlimit, cid, availloan from trade.networth where (cash <=securities or cash > loanLimit - availloan)       
        break;
      case 13:
        Log.getLogWriter().info(database + "querying: " + query); 
        //"select securities, cash, availloan, loanlimit, cid, availloan from trade.networth where (availloan >=securities or cash > loanLimit - availloan) 
        break;
      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else throw se;
    }
    return rs;
  }
  
  //add the exception to expceptionList to be compared with gfe
  protected boolean deleteFromDerbyTable(Connection dConn, int whichDelete, 
      int cid, List<SQLException> exList){
    PreparedStatement stmt = getStmt(dConn, delete[whichDelete]); 
    if (stmt == null) return false;
    int tid = getMyTid();
    int count=-1;
    
    try {
      verifyRowCount.put(tid+"_delete", 0);
      count = deleteFromTable(stmt, cid, tid, whichDelete);
      verifyRowCount.put(tid+"_delete", new Integer(count));
      

    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dConn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  //compare whether the exceptions got are same as those from derby
  protected void deleteFromGFETable(Connection gConn, int whichDelete, int cid, 
      List<SQLException> exList){
    PreparedStatement stmt = getStmt(gConn, delete[whichDelete]); 
    if (SQLTest.testSecurity && stmt == null) {
    	SQLHelper.handleGFGFXDException((SQLException)
    			SQLSecurityTest.prepareStmtException.get(), exList);
    	SQLSecurityTest.prepareStmtException.set(null);
    	return;
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    int count=-1;
    
    try {
      count = deleteFromTable(stmt, cid, tid, whichDelete);
      if (count != verifyRowCount.get(tid+"_delete").intValue()){
        Log.getLogWriter().info("Gfxd delete (networth) has different row count from that of derby " +
                "derby deleted " + (verifyRowCount.get(tid+"_delete")).intValue() +
                " but gfxd deleted " + count);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  //no verification
  protected void deleteFromGFETable(Connection gConn, int whichDelete, int cid){
    PreparedStatement stmt = getStmt(gConn, delete[whichDelete]); 
    if (SQLTest.testSecurity && stmt == null) {
    	if (SQLSecurityTest.prepareStmtException.get() != null) {
    	  SQLSecurityTest.prepareStmtException.set(null);
    	  return;
    	} else ; //need to find out why stmt is not obtained
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    
    try {
      deleteFromTable(stmt, cid, tid, whichDelete);
    } catch (SQLException se) {
      if ((se.getSQLState().equals("42500") || se.getSQLState().equals("42502"))
          && testSecurity) {
        Log.getLogWriter().info("Got the expected exception for authorization," +
           " continuing tests");
      } else if (alterTableDropColumn && se.getSQLState().equals("42X14")) {
        Log.getLogWriter().info("Got expected column not found exception in delete, continuing test");
      } else 
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  //delete from table based on whichDelete
  protected int deleteFromTable(PreparedStatement stmt, int cid, int tid,
      int whichDelete) throws SQLException {
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - " ;
    String query = " QUERY: " + delete[whichDelete];
       
    switch (whichDelete) {
    case 0:   
      //"delete from trade.customers where cid = ?", 
      Log.getLogWriter().info(database +  "deleting trade.networth with CID:" + cid + query );        
      stmt.setInt(1, cid); //the cid to be deleted is inserted by this thread
      break;
    case 1:
      //"delete from trade.customers where cid = ?", 
      Log.getLogWriter().info(database +  "deleting trade.networth with CID:" + cid + query );  
      stmt.setInt(1, cid);
      break;
    default:
      throw new TestException("incorrect delete statement, should not happen");
    }  
    int rowCount = stmt.executeUpdate();
    Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.networth with CID:" + cid + query );
    
    if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) && !SQLTest.hasTx && !setCriticalHeap)
               updateJSONToCustomer(stmt.getConnection(), cid, null);
    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
  
  private void updateJSONToCustomer(Connection conn , int cid , JSONObject json ){    
    
    if ( !SQLTest.hasJSON  || SQLHelper.isDerbyConn(conn))
      return;
  
    String jsonString = null ;
    try{
      
      if ( json != null ) {
        jsonString = json.toJSONString();
      }
             
   Log.getLogWriter().info("updating trade.customers with NETWORTH_JSON:" +  jsonString + " where CID:" + cid );
   
   String stmt = "update trade.customers set networth_json = ? where cid = " + cid;
   
   PreparedStatement ps = conn.prepareStatement(stmt);
   
   ps.setObject(1, jsonString);
   ps.execute();
   
    }catch (SQLException se ){      
      throw new TestException ( "Adding " + jsonString + " to trade.customers" + "Exception: "  + TestHelper.getStackTrace(se));
    }
    
  }
   
private String getJSON(int cid, BigDecimal cash,
     BigDecimal securities, int loanLimit, BigDecimal availLoan, int tid){
     return getJSONObject(cid, cash, securities, loanLimit, availLoan, tid).toJSONString();
 } 

 @SuppressWarnings("unchecked")
 private JSONObject getJSONObject(int cid, BigDecimal cash,
     BigDecimal securities, int loanLimit, BigDecimal availLoan, int tid){
    
   JSONArray json = new JSONArray();
   JSONObject arrayObj = new JSONObject();
   arrayObj.put("cid", cid);
   arrayObj.put("cash", cash);
   arrayObj.put("securities", securities);
   arrayObj.put("loanlimit", loanLimit);
   arrayObj.put("availloan", availLoan);
   arrayObj.put("tid", tid);
   
   json.add(arrayObj);
   
   JSONObject jsonObj = new JSONObject();
   jsonObj.put("networth", json);
   return jsonObj;
 }
 private  Map<String, Object> getCurrentRowOfUpdate(Connection conn , int cid){
    
    HashMap<String,Object>  map = new HashMap<String, Object>();
    
    String query = "select cash, securities, loanlimit,availloan,tid from trade.networth where cid = " + cid;
    try{
    ResultSet rs = conn.createStatement().executeQuery(query);
    
    if ( rs.next() ) {
      map.put("cid" , cid);
      map.put("cash", rs.getBigDecimal(1));
      map.put("securities", rs.getBigDecimal(2));
      map.put("loanlimit", rs.getInt(3));
      map.put("availloan", rs.getBigDecimal(4));
      map.put("tid", rs.getInt(5));
    }  else {
      map.put("cid" , cid);
      map.put("cash", new BigDecimal(0));
      map.put("securities", new BigDecimal(0));
      map.put("loanlimit", 0);
      map.put("availloan", new BigDecimal(0));
      map.put("tid", 0);
    }
    
    }catch (SQLException se){
      throw new TestException(TestHelper.getStackTrace(se));
    }
    
    return map;
  }
 
 
}

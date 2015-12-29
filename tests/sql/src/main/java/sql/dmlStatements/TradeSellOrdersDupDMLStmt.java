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
package sql.dmlStatements;

import hydra.Log;
import hydra.MasterController;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.security.SQLSecurityTest;
import sql.sqlutil.ResultSetHelper;
import sql.sqlutil.GFXDStructImpl;
import util.TestException;

/**
 * @author eshu
 *
 */
public class TradeSellOrdersDupDMLStmt extends AbstractDMLStmt {
  /*
   * trade.sellorders table fields
   *   int oid;
   *   int cid; //ref customers
   *   int sid; //ref securities sec_id
   *   int qty;
   *   BigDecimal ask;
   *   Timestamp order_time;
   *   String stauts;
   *   int tid; //for update or delete unique records to the thread
   */
  
  protected static String[] update = { 
    //uniq
    "update trade.sellordersdup set status = 'filled'  where sid = ? and ask<? and status = 'open' and tid = ? ",  //for trigger test it could be a batch update
    "update trade.sellordersdup set status = 'cancelled' where order_time >? and status = 'open' and tid =? ",  //batch operation
    "update trade.sellordersdup set ask = ? where cid = ? and sid= ? and status = 'open' and qty >? and tid =? ",
    "update trade.sellordersdup set ask = ? , qty=? where cid = ? and sid= ? and ask <? and status = 'open' and tid =? ",
    //no uniq
    "update trade.sellordersdup set status = 'filled'  where sid = ? and ask<? and status = 'open' ",  //for trigger test it could be a batch update
    "update trade.sellordersdup set status = 'cancelled' where order_time <? and status = 'open'  ",  //batch operation
    "update trade.sellordersdup set ask = ? where cid = ? and sid= ? and status = 'open' and qty >? ",
    "update trade.sellordersdup set ask = ? , qty=? where cid = ? and sid= ? and ask <? and status = 'open'  ",                                        
  }; 
  protected static String[] select = {
    //uniqkey queries
    "select * from trade.sellordersdup where status = 'open' and tid = ?",
    "select cid, ask, sid from trade.sellordersdup where cid >? and tid = ?",
    "select oid, cid, ask from trade.sellordersdup where (ask<? or ask >=?) and tid =?",
    "select oid, sid, ask, status, cid from trade.sellordersdup  where status =? and cid =? and tid =?",
    "select oid, sid, ask, cid, status from trade.sellordersdup  " +
    "where ((status =? and ask <?) OR (status =? and ask >?))  and cid IN (?, ?, ?, ?, ?) and tid =?", 
    "select oid, sid, ask, cid, order_time, status from trade.sellordersdup  " +
    "where (((order_time >? and ask <?) and (status =? and ask >?))  OR cid IN (?, ?, ?, ?, ?)) and tid =?", // avoid #39766
    "select oid, cid, order_time, sid from trade.sellordersdup where (order_Time < ?) and tid =? ",
    "select * from  trade.sellordersdup where oid IN (?, ?, ?, ?, ?) and tid =?",
    //non uniqkey queries
    "select * from trade.sellordersdup",
    "select cid, ask, sid from trade.sellordersdup where cid >? ",
    "select oid, sid from trade.sellordersdup where (ask<? or ask >=?) ",
    "select oid, sid, ask, status, cid from trade.sellordersdup  where status =? and cid =? ",
    "select oid, sid, ask, cid, status from trade.sellordersdup  " +
    "where ((status =? and ask <?) OR (status =? and ask >?))  and cid IN (?, ?, ?, ?, ?) ",
    "select oid, sid, ask, cid, order_time, status from trade.sellordersdup  " +
    "where ((order_time >? and ask <?) and (status =? and ask >?))  OR cid IN (?, ?, ?, ?, ?)",
    "select oid, cid, order_time, sid from trade.sellordersdup where (order_time < ?)",
    "select * from  trade.sellordersdup where oid IN (?, ?, ?, ?, ?)"
  };
  protected static String[] delete = {
    //uniqkey                            
    "delete from trade.sellordersdup where cid=? and sid=? and tid=?", //could be batch delete, but no foreign key reference, this should work
    "delete from trade.sellordersdup where oid=? and tid=?",
    "delete from trade.sellordersdup where status IN ('cancelled', 'filled') and tid=?",
    "delete from trade.sellordersdup where tid=?",
    //non uniqkey
    "delete from trade.sellordersdup where cid=? and sid=?",
    "delete from trade.sellordersdup where oid=?",
    "delete from trade.sellordersdup where status IN ('cancelled', 'filled')",
    "delete from trade.sellordersdup"
  };

  protected static String[] statuses = {                                       
    "cancelled", "open", "filled"                                       
  };
  
  protected static String[] selectinto = {
    "insert into trade.sellordersdup (select * from trade.sellorders where tid = ? and oid > ?) ",
    "insert into trade.sellordersdup select * from trade.sellorders where tid = ? and " +
    "oid > (select max(oid) from trade.sellordersdup where tid = ? ) " ,
  };

  protected static String[] putinto = {
    "put into trade.sellordersdup (select * from trade.sellorders where tid = ? and oid > ?) ",
    "put into trade.sellordersdup select * from trade.sellorders where tid = ? and " +
    "oid > (select max(oid) from trade.sellordersdup where tid = ? ) " ,
  };
  
  protected static int maxNumOfTries = 1;
  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  protected static boolean useDefaultValue = TestConfig.tab().booleanAt(SQLPrms.useDefaultValue, false);
  protected static ArrayList<String> partitionKeys = null;
  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#insert(java.sql.Connection, java.sql.Connection, int)
   */
  @Override
  public void insert(Connection dConn, Connection gConn, int size) {    
    insert(dConn, gConn, false);
  }

  @Override
  public void put(Connection dConn, Connection gConn, int size) {    
    insert(dConn, gConn, true);
  }


  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#update(java.sql.Connection, java.sql.Connection, int)
   */
  @Override
  public void update(Connection dConn, Connection gConn, int size) {
    int[] sid = new int[size];
    BigDecimal[] ask = new BigDecimal[size];
    Timestamp[] orderTime = new Timestamp[size];
    int[] cid = new int[size];
    int[] qty = new int[size];
    BigDecimal[] ask2 = new BigDecimal[size];
    
    int[]  whichUpdate = new int[size];
    List<SQLException> exceptionList = new ArrayList<SQLException>();

    if (dConn != null) {     
      //get the data
      if (rand.nextInt(numGettingDataFromDerby) == 1) {
        if (!getDataForUpdate(dConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size)) {
          Log.getLogWriter().info("not able to get the data for update, abort this operation"); 
          return;
        }
      } else getDataForUpdate(gConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size);
      
      if (setCriticalHeap) resetCanceledFlag();
      
      boolean success = updateDerbyTable(dConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size, exceptionList);  //insert to derby table  
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exceptionList .clear();
        success = updateDerbyTable(dConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size, exceptionList);  //insert to derby table  
      } //retry only once.
      updateGFETable(gConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size, exceptionList); 
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else {
      if (getDataForUpdate(gConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size)) //get the da
        updateGFETable(gConn, cid, sid, qty, orderTime, ask, ask2, whichUpdate, size);
    } //no verification

  }

  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#delete(java.sql.Connection, java.sql.Connection)
   */
  @Override
  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = delete.length/2;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    if (whichDelete == 3 && whichDelete != getMyTid()) whichDelete--; 
    int size = 1; //how many delete to be completed in this delete operation
    int[] cid = new int[size]; //only delete one record
    int[] sid = new int[size];
    int[] oid = new int[size];
    for (int i=0; i<size; i++) {
      oid[i] = rand.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSellOrdersPrimary)); //random instead of uniq
    }
    List<SQLException> exceptionList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    int availSize;
    if (dConn!=null && rand.nextInt(numGettingDataFromDerby) == 1) 
        availSize = getDataForDelete(dConn, cid ,sid);
    else availSize = getDataForDelete(gConn, cid ,sid);
    
    if(availSize == 0) return; //did not get the results
    
    if (setCriticalHeap) resetCanceledFlag();
    
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, whichDelete, cid, sid, oid, exceptionList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exceptionList.clear();
        success = deleteFromDerbyTable(dConn, whichDelete, cid, sid, oid, exceptionList); //retry
      }
      deleteFromGFETable(gConn, whichDelete, cid, sid, oid, exceptionList);
      SQLHelper.handleMissedSQLException(exceptionList);
    } 
    else {
      deleteFromGFETable(gConn, whichDelete, cid, sid, oid); //w/o verification
    }
  }

  /* (non-Javadoc)
   * @see sql.dmlStatements.AbstractDMLStmt#query(java.sql.Connection, java.sql.Connection)
   */
  @Override
  public void query(Connection dConn, Connection gConn) {
    int numOfNonUniq = select.length/2; //how many query statement is for non unique keys, non uniq query must be at the end
    int whichQuery = getWhichOne(numOfNonUniq, select.length); //randomly select one query sql based on test uniq or not
    
    String[] status = new String[2];
    BigDecimal[] ask = new BigDecimal[2];
    int[] cid = new int[5];  //test In for 5 
    int[] oid = new int[5];
    int tid = getMyTid();
    Timestamp orderTime = getRandTime();
    getStatus(status);
    getAsk(ask);
    if (dConn!=null) getCids(dConn, cid);
    else getCids(gConn, cid);
    getOids(oid);
    
    ResultSet discRS = null;
    ResultSet gfeRS = null;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    
    if (dConn!=null) {
      try {
        discRS = query(dConn, whichQuery, status, ask, cid, oid, orderTime, tid);
        if (discRS == null) {
          Log.getLogWriter().info("could not get the derby result set after retry, abort this query");
          return;
        }
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exceptionList);
      }
      try {
        gfeRS = query (gConn,  whichQuery, status, ask, cid, oid, orderTime, tid); 
        if (gfeRS == null) {
          if (isHATest) {
            Log.getLogWriter().info("Testing HA and did not get GFXD result set after retry");
            return;
          }
          else     
            throw new TestException("Not able to get gfe result set after retry");
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptionList);
      }
      SQLHelper.handleMissedSQLException(exceptionList);
      if (discRS == null || gfeRS == null) return;
      
      boolean success = ResultSetHelper.compareResultSets(discRS, gfeRS); 
      if (!success) {
        Log.getLogWriter().info("Not able to compare results");
      } //not able to compare results due to derby server error     
    }// we can verify resultSet
    else {
      try {
        gfeRS = query (gConn, whichQuery, status, ask, cid, oid, orderTime, tid);  
      } catch (SQLException se) {
        if (se.getSQLState().equals("42502") && SQLTest.testSecurity) {
          Log.getLogWriter().info("Got expected no SELECT permission, continuing test");
          return;
        } else SQLHelper.handleSQLException(se);
      }
      if (gfeRS != null)
        ResultSetHelper.asList(gfeRS, false); 
    }
    
    SQLHelper.closeResultSet(gfeRS, gConn);
  }
  
  //populate the table
  @Override
  public void populate (Connection dConn, Connection gConn) {
  //the table should be populated when populating customers table, 
    int initSize = rand.nextInt(10)+1; //howmany time a customer could insert sell orders
    int numCid;
    for (int i=0; i<initSize; i++) {
      numCid = rand.nextInt(30) + 1; 
      for (int j=0; j<numCid; j++) {
        if (setCriticalHeap) resetCanceledFlag();
        insert(dConn, gConn, 1);
        if (dConn != null) commit(dConn);
        commit(gConn);
      }
    }
  }
  
  //*** insert/Put ***/
  private void insert(Connection dConn, Connection gConn, boolean isPut) {
    List<SQLException> exceptionList = new ArrayList<SQLException>();
    int[] maxOid = new int[1];
    int whichOne = rand.nextInt(selectinto.length);
    boolean getData = getDataForInsert(gConn, maxOid);
    if (!getData) {
      Log.getLogWriter().info("does not get data, abort this op");
      return;
    }
    
    if (setCriticalHeap) resetCanceledFlag();
    
    if (dConn != null) {
      boolean success = insertToDerbyTable(dConn, maxOid[0], whichOne, exceptionList);  //insert to derby table  
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finish the operation in derby, abort this operation");
          return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exceptionList .clear(); //clear the exceptionList and retry
        success = insertToDerbyTable(dConn, maxOid[0], whichOne, exceptionList); 
      }
      insertToGFETable(gConn, maxOid[0], whichOne, exceptionList, isPut); 
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else {
      insertToGFETable(gConn, maxOid[0], whichOne, true);
    } //no verification
  }
  
  
  //populate arrays for insert
  protected boolean getDataForInsert(Connection conn, int[] maxOid) {
    String sql = "select max (oid) as maxoid from trade.sellordersdup where tid = " + getMyTid();
    ResultSet rs = null;
    try {
      rs = conn.createStatement().executeQuery(sql);
      if (rs.next()) {
        maxOid[0] = rs.getInt("MAXOID"); 
        Log.getLogWriter().info("max oid by this thread is " + maxOid[0]);
      }
      else maxOid[0] = 0;
    } catch (SQLException se) {
      if (!SQLHelper.checkGFXDException(conn, se)) return false; //need retry or no op
      else SQLHelper.handleSQLException(se);
    } finally {
      SQLHelper.closeResultSet(rs, conn);
    }
    return true;
  }

  //populate int[] of oids
  protected void getOids(int[] oid, int size) {
    int key = (int) SQLBB.getBB().getSharedCounters().add(SQLBB.tradeSellOrdersPrimary, size);
    for (int i=0; i<size; i++) {
      oid[i]= (key-size+1)+i;
    }
  }  
  
  //get appropriate sid to be used for insert, and only the sid inserted by this tid will be chosen
  //from portfolio table (fk constraint) or 0 (if sid is 0, this record would not be inserted as it 
  //violates the fk constraint -- there is no cid = 0 in the securities table and thus no cid =0 in 
  //the portfolio table
  protected int getSid(Connection regConn, int cid) {
    Connection conn = getAuthConn(regConn);
    int sid =0; 
    int max = 5; 
    int n = rand.nextInt(max);  //randomly choose a number from 0 to max-1
    if (testUniqueKeys) {
      ResultSet rs =null;
      try {
        //String s = "select sid from trade.portfolio where cid = " + cid + " and tid = " +getMyTid() ; //tid is not needed in where clause
        //rs = dConn.createStatement().executeQuery(s);
        int whichQuery =0;
        rs = TradePortfolioDMLStmt.getQuery(conn, whichQuery, null, null, 0, 0, 0, cid, getMyTid());
        if (rs == null) return 0; //return 0 as sid
        int temp=0;
        while (rs.next()) {
          if (temp == 0) {
            sid = rs.getInt("SID");
          } //default is to select the first sid if there is one
          if (n==temp) {
            sid = rs.getInt("SID");
            break;
          }//it may return a random sid in the portfolio for this customer (cid)
          temp++;          
        }
        rs.close();
      } catch (SQLException se) {
        if (!SQLHelper.checkGFXDException(conn, se)) return sid; //need retry or no op
        else SQLHelper.handleSQLException(se); //throws TestException.
      }      
    } //dConn will not be null
    else
      sid = getSid(); //randomly
    
    //Log.getLogWriter().info("in getSid and sid is " + sid);
    return sid;  //could return sid =0 
  }
  
  //insert into Derby
  protected boolean insertToDerbyTable(Connection conn, int maxOid, int whichOne,
      List<SQLException> exceptions)  {    
    int tid = getMyTid();
    int count = -1;
    
    
    whichOne = 0; //work around #44791
    
    Log.getLogWriter().info(selectinto[whichOne]);
    PreparedStatement stmt = getStmt(conn, selectinto[whichOne]);
    if (stmt == null) return false;
    try {
      verifyRowCount.put(tid+"_insert", 0);
      count = selectIntoTable(stmt, maxOid, whichOne, tid, false);
      verifyRowCount.put(tid+"_insert", new Integer(count));
      
    }  catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exceptions);
    }    

    return true;
  }
  
  //insert into gemfirexd
  protected void insertToGFETable(Connection conn, int maxOid, int whichOne, 
      List<SQLException> exceptions, boolean isPut) {
 
    int tid = getMyTid();
    int count = -1;
    
    
    whichOne = 0; //work around #44791
    
    PreparedStatement stmt = getStmt(conn, isPut ? putinto[whichOne] : selectinto[whichOne]);
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
    
    try {
      count = selectIntoTable(stmt, maxOid, whichOne, tid, isPut);
      if (count != (verifyRowCount.get(tid+"_insert")).intValue()) {
        String str = "Gfxd select into has different row count from that of derby " +
          "derby inserted " + (verifyRowCount.get(tid+"_insert")).intValue() +
          " but gfxd inserted " + count;
        if (failAtUpdateCount && !isHATest) throw new TestException (str);
        else Log.getLogWriter().warning(str);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exceptions);
    }

  }
  
  //insert into gemfirexd/gfe w/o verification
  protected void insertToGFETable(Connection conn, int maxOid, int whichOne, boolean isPut)  {
    if (rand.nextInt(100) == 1) {
      int secondOne = 1;
      try {
        conn.prepareStatement(isPut ? putinto[secondOne] : selectinto[secondOne]);
      } catch (SQLException se) {
        if (se.getSQLState().equals("0A000"))
          Log.getLogWriter().info("get expected unsupported exception for " 
              + selectinto[secondOne]);
        else
          throw new TestException("does not get unsupported exception for " 
              + selectinto[secondOne]);
      }
    } //only test this exception a few times
    
    whichOne = 0; //work around #44791
    
    PreparedStatement stmt = getStmt(conn, isPut ? putinto[whichOne] : selectinto[whichOne]);
    int tid = getMyTid();
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    try {
      selectIntoTable(stmt, maxOid, whichOne, tid, isPut);
    } catch (SQLException se) {
      if (se.getSQLState().equals("23503"))  
        Log.getLogWriter().info("detected the foreign key constraint violation, continuing test");
      else if (se.getSQLState().equals("42500") && testSecurity) 
        Log.getLogWriter().info("Got the expected exception for authorization," +
           " continuing tests");
      else
        SQLHelper.handleSQLException(se); //handle the exception
    }
   
  }
  
  //insert a record into the table
  protected int selectIntoTable(PreparedStatement stmt, int maxOid, int whichOne, int tid, boolean isPut) 
    throws SQLException {
    Log.getLogWriter().info((isPut ? putinto[whichOne] : selectinto[whichOne]) + " maxOid is " + maxOid + " tid is " + tid);
    stmt.setInt(1, tid);
    if (whichOne == 0) stmt.setInt(2, maxOid);
    else if (whichOne == 1) stmt.setInt(2, tid);
    int rowCount = stmt.executeUpdate();
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
  
  //populate the status array
  protected void getStatus(String[] status) {
    for (int i=0; i<status.length; i++) {
      status[i]= statuses[rand.nextInt(statuses.length)];
    }
  }
  
  //populate the ask price array
  protected void getAsk(BigDecimal[] ask) {
    for (int i=0; i<ask.length; i++) {
      ask[i]= getPrice();
    }
  }
  
  //for query IN (?,?,?,?,?) primary keys for get all
  protected void getOids(int[] oid) {
    int startOid =  rand.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSellOrdersPrimary) + 1);
    for (int i=0; i<oid.length; i++) {
      oid[i]= startOid++;
    }
  }
  
  //return a Date used in query
  protected Timestamp getRandTime() {
    return new Timestamp(System.currentTimeMillis()-rand.nextInt(3*60*1000)); //randomly choose in the last 3 minutes
  }
  
  protected boolean getDataForUpdate(Connection regConn, int[] cid, int[] sid, int[] qty, 
      Timestamp[] orderTime, BigDecimal[] ask, BigDecimal[] ask2, int[] whichUpdate, int size) {
    Connection conn = getAuthConn(regConn);
    int numOfNonUniq = 4;
    if (!getDataFromResult(conn, cid, sid)) return false; //did not get data
    for (int i=0; i<size; i++) {
      qty[i] = getQty();
      orderTime[i] = getRandTime();
      ask[i] = getPrice();
      ask2[i] = getPrice();
      whichUpdate[i] = getWhichUpdate(numOfNonUniq, update.length);
    }
    return true;
  }
  
  protected int getWhichUpdate(int numOfNonUniq, int total) {
    return getWhichOne(numOfNonUniq, total);
  }
  
  protected boolean getDataFromResult(Connection conn, int[] cid, int[] sid) {
    int tid = getMyTid();  //for test unique key case
    if (!testUniqueKeys) {
      if (tid == 0)  ; //do nothing
      else tid = rand.nextInt(tid);  //data inserted from any tid
    }     
      
    return getDataFromResult(conn, cid, sid, tid);
  }
  
  protected boolean getDataFromResult(Connection conn, int[] cid, int[] sid, int tid) {
    int defaultCid =0; 
    int defaultSid =0; 
    int size = cid.length;
    int num = 20; //used to randomly return the cid, sid pair
    int n = rand.nextInt(num);
    //Log.getLogWriter().info("n is " + n);
    ResultSet rs =null;

    try {  
      int whichQuery = 0;
      rs = getQuery(conn, whichQuery, null, null, null, null, null, tid); 
      if (rs == null) {
          return false; //already did retry in getQuery
      }
      
      int i=0;
      int temp = 0;
      while (rs.next() && i<size) {
        if (temp ==0 ) {
          cid[i] = rs.getInt("CID");
          sid[i] = rs.getInt("SID");
        } else if (n<=temp) {
          cid[i] = rs.getInt("CID");
          sid[i] = rs.getInt("SID");
          i++;
        }
        temp++;             
      } //update cids with result set
      rs.close();
      while (i<size) {
        cid[i] = defaultCid;  //cid is 0
        sid[i] = defaultSid; //sid is 0
        i++;
      } //continues to update cids with cid=0
    } catch (SQLException se) {
      if (!SQLHelper.checkGFXDException(conn, se)) return false; //need retry or no op
      else SQLHelper.handleSQLException(se); //throws TestException.
    }
    return true;
  }
  

  
  protected boolean updateDerbyTable (Connection conn, int[] cid, int[] sid, int[] qty, 
      Timestamp[] orderTime, BigDecimal[] ask, BigDecimal[] ask2, int[] whichUpdate, 
      int size, List<SQLException> exList) {
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
        if (stmt !=null) {
          verifyRowCount.put(tid+"_update"+i, 0);
          count = updateTable(stmt, cid[i], sid[i], qty[i], orderTime[i], ask[i], ask2[i], tid, whichUpdate[i] );
          verifyRowCount.put(tid+"_update"+i, new Integer(count));
          
        } 
      } catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
          return false;
        } else
            SQLHelper.handleDerbySQLException(se, exList);
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
      partitionKeys= (ArrayList<String>)partitionMap.get("sellordersPartition");
    }
    else {
      int myWanSite = getMyWanSite();
      partitionKeys = (ArrayList<String>)wanPartitionMap.
        get(myWanSite+"_sellordersPartition");
    }
    Log.getLogWriter().info("partition keys are " + partitionKeys);
  }
  
  //used to parse the partitionKey and test unsupported update on partitionKey, no need after bug #39913 is fixed
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys){
    PreparedStatement stmt = null;
    switch (whichUpdate) {
    case 0: 
      // "update trade.sellordersdup set status = 'filled'  where sid = ? and ask<? and status = 'open' and tid = ? ", 
      if (partitionKeys.contains("status")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else ;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
      //"update trade.sellordersdup set status = 'cancelled' where order_time >? and status = 'open' and tid =? ",
      if (partitionKeys.contains("status")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else ;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: 
      //"update trade.sellordersdup set ask = ? where cid = ? and sid= ? and status = 'open' and qty >? and tid =? ",
      if (partitionKeys.contains("ask")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else ;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: 
      //"update trade.sellordersdup set ask = ? , qty=? where cid = ? and sid= ? and ask <? and status = 'open' and tid =? ", 
      if (partitionKeys.contains("ask") || partitionKeys.contains("qty")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else ;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: 
      //"update trade.sellordersdup set status = 'filled'  where sid = ? and ask<? and status = 'open' ", 
      if (partitionKeys.contains("status")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 5: 
      // "update trade.sellordersdup set status = 'cancelled' where order_time <? and status = 'open'  ", 
      if (partitionKeys.contains("status")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 6: 
      //  "update trade.sellordersdup set ask = ? where cid = ? and sid= ? and status = 'open' and qty >? ", 
      if (partitionKeys.contains("ask")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 7: 
      //"update trade.sellordersdup set ask = ? , qty=? where cid = ? and sid= ? and ask <? and status = 'open'  ",
      if (partitionKeys.contains("ask") || partitionKeys.contains("qty")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else ;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
   
    return stmt;
  }
  
  //check expected exceptions
  protected void updateGFETable (Connection conn, int[] cid, int[] sid, int[] qty, 
      Timestamp[] orderTime, BigDecimal[] ask, BigDecimal[] ask2, int[] whichUpdate, 
      int size, List<SQLException> exList) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i]);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed
      
      if (SQLTest.testSecurity && stmt == null) {
      	if (SQLSecurityTest.prepareStmtException.get() != null) {
      	  SQLHelper.handleGFGFXDException((SQLException)
      			SQLSecurityTest.prepareStmtException.get(), exList);
      	  SQLSecurityTest.prepareStmtException.set(null);
      	  return;
      	}
      } //work around #43244
      
      try {
        if (stmt!=null) {
          count = updateTable(stmt, cid[i], sid[i], qty[i], orderTime[i], ask[i], ask2[i], tid, whichUpdate[i] );
          if (count != (verifyRowCount.get(tid+"_update"+i)).intValue()){
            String str = "Gfxd update has different row count from that of derby " +
                    "derby updated " + (verifyRowCount.get(tid+"_update"+i)).intValue() +
                    " but gfxd updated " + count;
            if (failAtUpdateCount && !isHATest) throw new TestException (str);
            else Log.getLogWriter().warning(str);
          }
        }
      } catch (SQLException se) {
         SQLHelper.handleGFGFXDException(se, exList);
      }    
    }  
  }
  
  //w/o verification
  protected void updateGFETable (Connection conn, int[] cid, int[] sid, int[] qty, 
      Timestamp[] orderTime, BigDecimal[] ask, BigDecimal[] ask2, int[] whichUpdate, int size) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    
    for (int i=0 ; i<size ; i++) {
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i]);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed

      try {
        if (stmt!=null)
          updateTable(stmt, cid[i], sid[i], qty[i], orderTime[i], ask[i], ask2[i], tid, whichUpdate[i] );
      } catch (SQLException se) {
        if (se.getSQLState().equals("42502") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
          " continuing tests");
        } else SQLHelper.handleSQLException(se);
      }    
    }  
  }
  
  protected int updateTable (PreparedStatement stmt, int cid, int sid, int qty, 
      Timestamp orderTime, BigDecimal ask, BigDecimal ask2, int tid, int whichUpdate) throws SQLException {
    int rowCount = 0; 
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - " ;
    String query = " QUERY: " + update[whichUpdate];
                                       
    switch (whichUpdate) {
    case 0: 
      //"update trade.sellordersdup set status = 'filled'  where sid = ? and ask>? and status = 'open' and tid = ? ",  //for trigger test it could be a batch update
      Log.getLogWriter().info(database + "updating trade.sellordersdup  with SID:" + sid  + ",ASK:" + ask +
          " where ,STATUS:open,TID:" + tid + query);
      stmt.setInt(1, sid);
      stmt.setBigDecimal(2, ask);      
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.sellordersdup SID:" + sid  + ",ASK:" + ask +
          " where ,STATUS:open,TID:" + tid + ",STATUS:" + "open" + query);
      break;
    case 1: 
      //"update trade.sellordersdup set status = 'cancelled' where order_time <? and status = 'open' and tid =? ",  //batch operation
      Log.getLogWriter().info(database +  "updating sellorders with STATUS:cancelled  where  ORDERTIME:" + orderTime +  ",STATUS:" + "open"  + ",TID: " + tid + query);
      stmt.setTimestamp(1, orderTime);
      stmt.setInt(2, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database +  "updated " + rowCount + " rows in trade.sellordersdup  with STATUS:cancelled  where ORDERTIME:" + orderTime +  ",STATUS:" + "open"  + ",TID: " + tid + query);
      break;
    case 2: 
      //"update trade.sellordersdup set ask = ? where cid = ? and sid= ? and status = 'open' and qty >? and tid =? ",
      Log.getLogWriter().info(database + "updating tarade.sellordersdup  with ASK:" + ask + " where CID:" + cid 
          + ",STATUS:open,SID:" + sid + ",QTY:" + qty + ",TID:" + tid + query);
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
      stmt.setInt(4, qty);
      stmt.setInt(5, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database +  "updated " + rowCount + " rows in trade.sellordersdup with ASK:" + ask + " where CID:" + cid 
          + ",STATUS:open,SID:" + sid + ",QTY:" + qty + ",TID:" + tid + query);
      break;
    case 3: 
      //"update trade.sellordersdup set ask = ? , qty=? where cid = ? and sid= ? and ask <? and status = 'open' and tid =? ",
      Log.getLogWriter().info(database + "updating trade.sellordersdup  with ASK:" + ask + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",ASK:" + ask + ",STATUS:open,TID:" + tid + query);
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, qty);
      stmt.setInt(3, cid);
      stmt.setInt(4, sid);
      stmt.setBigDecimal(5, ask2);
      stmt.setInt(6, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database +  "updated " + rowCount + " rows in trade.sellordersdup  with ASK:" + ask + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",ASK:" + ask + ",STATUS:open,TID:" + tid + query);
      break;
    case 4:
      //"update trade.sellordersdup set status = 'filled'  where sid = ? and ask>? and status = 'open' ",  //for trigger test it could be a batch update
      Log.getLogWriter().info(database + "updating trade.sellordersdup  with STATUS:filled where SID:" + sid  + " ASK: " + ask + query);
      stmt.setInt(1, sid);
      stmt.setBigDecimal(2, ask);      
      rowCount=stmt.executeUpdate();
      Log.getLogWriter().info(database +  "updated " + rowCount + " rows in trade.sellordersdup  with STATUS:filled where SID:" + sid  + " ASK: " + ask + query);
      break;
    case 5: 
      //"update trade.sellordersdup set status = 'cancelled' where order_time <? and status = 'open'  ",  //batch operation
      Log.getLogWriter().info(database + "updating trade.sellordersdup  with STATUS:filled where ORDERTIME:" + orderTime +",STATUS:open" + query);
      stmt.setTimestamp(1, orderTime);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database +  "updated " + rowCount + " rows in trade.sellordersdup  with STATUS:filled where ORDERTIME:" + orderTime +",STATUS:open" + query);
      break;
    case 6: 
      //"update trade.sellordersdup set ask = ? where cid = ? and sid= ? and status = 'open' and qty >? ",
      Log.getLogWriter().info(database + "updating trade.sellordersdup with ASK:" + ask + " where CID:" + cid 
          + ", sid: " + sid + ", qty: " + qty + query);
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
      stmt.setInt(4, qty);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database +  "updated " + rowCount + " rows in trade.sellordersdup with ASK:" + ask + " where CID:" + cid 
          + ", sid: " + sid + ", qty: " + qty + query);
      break;
    case 7: 
      //"update trade.sellordersdup set ask = ? and qty=? where cid = ? and sid= ? and ask <? and status = 'open'  ",  
      Log.getLogWriter().info(database + "updating trade.sellordersdup with ASK: " + ask + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",ASK:" + ask2 + ",STATUS:open" + query);
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, qty);
      stmt.setInt(3, cid);
      stmt.setInt(4, sid);
      stmt.setBigDecimal(5, ask2);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database +  "updated " + rowCount + " rows in trade.sellordersdup  with ASK: " + ask + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",ASK:" + ask2 + ",STATUS:open" + query);
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
  
  //will retry
  public static ResultSet getQuery(Connection conn, int whichQuery, String[] status, 
      BigDecimal[] ask, int[] cid, int[] oid, Timestamp orderTime, int tid) {
    boolean[] success = new boolean[1];
    int count = 0;
    ResultSet rs = null;
    try {
      rs = getQuery(conn, whichQuery, status, ask, cid, oid, orderTime, tid, success);
      while (!success[0]) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
          return null; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++;   
        MasterController.sleepForMs(100 * rand.nextInt(10)); //sleep from 0 - 900 ms
        rs = getQuery(conn, whichQuery, status, ask, cid, oid, orderTime, tid, success);
      } //retry 
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  protected static ResultSet query(Connection conn, int whichQuery, String[] status, 
      BigDecimal[] ask, int[] cid, int[] oid, Timestamp orderTime, int tid) 
      throws SQLException {
    boolean[] success = new boolean[1];
    int count = 0;
    ResultSet rs = getQuery(conn, whichQuery, status, ask, cid, oid, orderTime, tid, success);
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
        return null; 
      }
      MasterController.sleepForMs(rand.nextInt(retrySleepMs));
      count++;   
      MasterController.sleepForMs(100 * rand.nextInt(10)); //sleep from 0 - 900 ms
      rs = getQuery(conn, whichQuery, status, ask, cid, oid, orderTime, tid, success);
    } //retry 
    return rs;
  }
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, String[] status, 
      BigDecimal[] ask, int[] cid, int[] oid, Timestamp orderTime, int tid, 
      boolean[] success) throws SQLException {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    
    String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - " ;
    String query = " QUERY: " + select[whichQuery];
       
    try {
      stmt = conn.prepareStatement(select[whichQuery]);      
      switch (whichQuery){
      case 0:
        //"select * from trade.sellordersdup where tid = ?",
        Log.getLogWriter().info( database + "querying trade.sellordersdup with TID:" +tid + query);
        stmt.setInt(1, tid);
        break;
      case 1: 
        //"select cid, ask, sid from trade.sellordersdup where cid >? and tid = ?",
        Log.getLogWriter().info( database + "querying trade.sellordersdup with CID:" + cid[0] + ",TID: " + tid  + query);   
        stmt.setInt(1, cid[0]);
        stmt.setInt(2, tid); 
        break;
      case 2:
        //"select oid, sid from trade.sellordersdup where (ask<? or ask >=?) and tid =?",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with 1_ASK:" + ask[0] 
         + ",TID:" + tid + ",2_ASK:" + ask[1] + ",TID:" +tid + query);   
        stmt.setBigDecimal(1, ask[0]);
        stmt.setBigDecimal(2, ask[1]);
        stmt.setInt(3, tid);
        break;
      case 3:
        // "select oid, sid, ask from trade.sellordersdup  where status =? and cid =? and tid =?",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with STATUS:" + status[0] + ",CID:" + cid[0] 
         + ",TID:" +tid + query);   
        stmt.setString(1, status[0]);
        stmt.setInt(2, cid[0]);
        stmt.setInt(3, tid);
        break;
      case 4:
        //    "select oid, sid, ask from trade.sellordersdup  " +
        // "where ((status =? and ask <?) OR (status =? and ask >?))  and cid IN {?, ?, ?, ?, ?} and tid =?", 
        Log.getLogWriter().info(database + "querying trade.sellordersdup with 1_STATUS:" + status[0] + ",1_ASK:" 
            + ask[0] + ",2_STATUS:" + status[1] + ",2_ASK:"  + ask[1] + ",1_CID:" +
            cid[0] + ",2_CID:" + cid[1] + "3_CID:" + cid[2] + ",4_CID:" + cid[3] + "5_CID:" + cid[4]  + " ,TID:" +tid + query);
        stmt.setString(1, status[0]);
        stmt.setBigDecimal(2, ask[0]);
        stmt.setString(3, status[1]);
        stmt.setBigDecimal(4, ask[1]);
        stmt.setInt(5, cid[0]);
        stmt.setInt(6, cid[1]);
        stmt.setInt(7, cid[2]);
        stmt.setInt(8, cid[3]);
        stmt.setInt(9, cid[4]);
        stmt.setInt(10, tid);
        break;
      case 5:
       // "select oid, sid, ask from trade.sellordersdup  " +
       // "where ((order_time >? and ask <?) and (status =? and ask >?))  OR cid IN (?, ?, ?, ?, ?) and tid =?",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with ORDERTIME:" + orderTime + ",1_ASK:" 
            + ask[0] + ",STATUS:" + status[1] + "2_ASK:"  + ask[1] + ",1_CID:" +
            cid[0] + ",2_CID: " + cid[1] + "3_CID:" + cid[2] + ",4_CID:" + cid[3] + ",5_CID:" + cid[4]  + ",TID:" +tid + query);
        stmt.setTimestamp(1, orderTime);
        stmt.setBigDecimal(2, ask[0]);
        stmt.setString(3, status[1]);
        stmt.setBigDecimal(4, ask[1]);
        stmt.setInt(5, cid[0]);
        stmt.setInt(6, cid[1]);
        stmt.setInt(7, cid[2]);
        stmt.setInt(8, cid[3]);
        stmt.setInt(9, cid[4]);
        stmt.setInt(10, tid);
        break;
      case 6: 
        //"select oid, cid, order_time, sid from trade.sellordersdup where (order_time > ?) and tid =? ",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with ORDERTIME:" + orderTime + ",TID:" +tid + query);   
        stmt.setTimestamp(1, orderTime); 
        stmt.setInt(2, tid);
        break;
      case 7:
        //"select * from trade.sellordersdup where oid IN (?, ?, ?, ?, ?)  and tid =?  ",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with 1_OID:"+
            oid[0] + ",2_OID:" + oid[1] + ",3_OID:" + oid[2] + ",4_OID:" + oid[3] + ",5_OID:" + oid[4] + query);
        for (int i=1; i<6; i++)
          stmt.setInt(i, oid[i-1]); 
        
        stmt.setInt(6, tid);
        break;
      case 8:
        //"select * from trade.sellordersdup",
        break;
      case 9: 
        //"select cid, ask, sid from trade.sellordersdup where cid >? ",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with CID:" + cid[0] + query); 
        stmt.setInt(1, cid[0]);
        break;
      case 10:
        //"select oid, sid from trade.sellordersdup where (ask<? or ask >=?) ",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with 1_ASK:" + ask[0] 
         + ",2_ASK:" + ask[1] + query);     
        stmt.setBigDecimal(1, ask[0]);
        stmt.setBigDecimal(2, ask[1]);
        break;
      case 11:
        // "select oid, sid, ask from trade.sellordersdup  where status =? and cid =? ",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with STATUS:" + status[0] + ",CID:" + cid[0] + query);   
        stmt.setString(1, status[0]);
        stmt.setInt(2, cid[0]);
        break;
      case 12:
        //    "select oid, sid, ask from trade.sellordersdup  " +
        // "where ((status =? and ask <?) OR (status =? and ask >?))  and cid IN {?, ?, ?, ?, ?}", 
        Log.getLogWriter().info(database + "querying trade.sellordersdup with 1_STATUS:" + status[0] + ",1_ASK:" 
            + ask[0] + ":2_STATUS:" + status[1] + ",2_ASK:"  + ask[1] + ",1_CID:" +
            cid[0] + ",2_CID:" + cid[1] + ",3_CID:" + cid[2] + ",4_CID:" + cid[3] + ",5_CID:" + cid[4] + query);
        stmt.setString(1, status[0]);
        stmt.setBigDecimal(2, ask[0]);
        stmt.setString(3, status[1]);
        stmt.setBigDecimal(4, ask[1]);
        stmt.setInt(5, cid[0]);
        stmt.setInt(6, cid[1]);
        stmt.setInt(7, cid[2]);
        stmt.setInt(8, cid[3]);
        stmt.setInt(9, cid[4]);
        break;
      case 13:
        // "select oid, sid, ask from trade.sellordersdup  " +
        // "where ((order_time >? and ask <?) and (status =? and ask >?))  OR cid IN (?, ?, ?, ?, ?)",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with ORDERTIME:" + orderTime + ",1_ASK:" 
            + ask[0] + ",STATUS:" + status[1] + ",2_ASK:"  + ask[1] + ",1_CID:" +
            cid[0] + ",2_CID:" + cid[1] + ",3_CID:" + cid[2] + ",4_CID:" + cid[3] + ",5_CID:" + cid[4] + query );
         stmt.setTimestamp(1, orderTime);
         stmt.setBigDecimal(2, ask[0]);
         stmt.setString(3, status[1]);
         stmt.setBigDecimal(4, ask[1]);
         stmt.setInt(5, cid[0]);
         stmt.setInt(6, cid[1]);
         stmt.setInt(7, cid[2]);
         stmt.setInt(8, cid[3]);
         stmt.setInt(9, cid[4]);
         break;
       case 14: 
        //"select oid, cid, order_time, sid from trade.sellordersdup where (order_time > ?) ",
        Log.getLogWriter().info(database + "querying trade.sellordersdup with ORDERTIME:" + orderTime + query);   
        stmt.setTimestamp(1, orderTime); 
        break;
       case 15: 
         //""select * from trade.sellordersdup where oid IN (?, ?, ?, ?, ?) "
         Log.getLogWriter().info(database + "querying trade.sellordersdup with 1_OID:" +
             oid[0] + ",2_OID:" + oid[1] + ",3_OID:" + oid[2] + ",4_OID:" + oid[3] + ",5_OID:" + oid[4] + query);
         for (int i=1; i<6; i++)
           stmt.setInt(i, oid[i-1]); 
         break;
      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire lokc or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else throw se;
    }
    return rs;
  } 
  
  //*** for delete ***//
  protected int getDataForDelete(Connection conn, int[] cid, int[] sid) {
    int tid = getMyTid();  //for test unique key case
    if (!testUniqueKeys) {
      if (tid == 0)  ; //do nothing
      else tid = rand.nextInt(tid);  //data inserted from any tid
    }     
      
    return getDataForDelete(conn, cid, sid, tid);    
  }
  
  protected int getDataForDelete(Connection regConn, int[] cid, int[] sid, int tid) {
    Connection conn = getAuthConn(regConn);
    int availSize = cid.length;
    int firstQuery = 0; //first query is select *
    ResultSet rs = null;
    int[] offset = new int[1];      

    rs = getQuery(conn, firstQuery, null, null, null, null, null, tid);
      //get the resultSet of sellordersdup inserted by this thread
   
    if (rs==null && SQLHelper.isDerbyConn(conn)) {
      Log.getLogWriter().info("Not able to get data for delete");
      return 0;
    } //query may terminated earlier to get sids if not able to obtain lock
    
    ArrayList<Struct> result = (ArrayList<Struct>) ResultSetHelper.asList(rs, SQLHelper.isDerbyConn(conn));    
    SQLHelper.closeResultSet(rs, conn);
    
    if (result == null) {
      Log.getLogWriter().info("did not get result set");
      return 0;
    } else {
      availSize = getAvailSize(result, cid.length, offset);
    }
   
    for (int i =0 ; i<availSize; i++) {
      cid[i] = ((Integer)((GFXDStructImpl)result.get(i+offset[0])).get("CID")).intValue();
      sid[i] = ((Integer)((GFXDStructImpl)result.get(i+offset[0])).get("SID")).intValue();   
    }
   
    return availSize;
  }
  
  //add the exception to expceptionList to be compared with gfe
  protected boolean deleteFromDerbyTable(Connection dConn, int whichDelete, 
      int[]cid, int []sid, int[] oid, List<SQLException> exList){    
    int tid = getMyTid();
    int count = -1;
    
    try {
      PreparedStatement stmt = dConn.prepareStatement(delete[whichDelete]); 
      if (stmt == null) return false;
      for (int i=0; i<cid.length; i++) {
      verifyRowCount.put(tid+"_delete_"+i, 0);
        count = deleteFromTable(stmt, cid[i], sid[i], oid[i], tid, whichDelete);
        verifyRowCount.put(tid+"_delete_"+i, new Integer(count));
        
      } 
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dConn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  //compare whether the exceptions got are same as those from derby
  protected void deleteFromGFETable(Connection gConn, int whichDelete, int[]cid,
      int []sid, int[] oid, List<SQLException> exList){
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
    
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete from gemfirexd, myTid is " + tid);
    try {
      for (int i=0; i<cid.length; i++) {
        count = deleteFromTable(stmt, cid[i], sid[i], oid[i], tid, whichDelete);
        if (count != (verifyRowCount.get(tid+"_delete_"+i)).intValue()){
          String str = "Gfxd delete (sellordersdup) has different row count from that of derby " +
                  "derby deleted " + (verifyRowCount.get(tid+"_delete_"+i)).intValue() +
                  " but gfxd deleted " + count;
          if (failAtUpdateCount && !isHATest) throw new TestException (str);
          else Log.getLogWriter().warning(str);
        }
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  //no verification
  protected void deleteFromGFETable(Connection gConn, int whichDelete, int[] cid,
      int[] sid, int[] oid){
    PreparedStatement stmt = getStmt(gConn, delete[whichDelete]);     
    if (SQLTest.testSecurity && stmt == null) {
    	if (SQLSecurityTest.prepareStmtException.get() != null) {
    	  SQLSecurityTest.prepareStmtException.set(null);
    	  return;
    	} else ; //need to find out why stmt is not obtained
    } //work around #43244
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    Log.getLogWriter().info("data will be used in the delete: none" );
    int tid = getMyTid();
    Log.getLogWriter().info("delete from gemfirexd, myTid is " + tid);
    try {
      for (int i=0; i<cid.length; i++) {
        deleteFromTable(stmt, cid[i], sid[i], oid[i], tid, whichDelete);
      }
    } catch (SQLException se) {
      if ((se.getSQLState().equals("42500") || se.getSQLState().equals("42502"))
          && testSecurity) {
        Log.getLogWriter().info("Got the expected exception for authorization," +
           " continuing tests");
      } else SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  //delete from table based on whichDelete
  protected int deleteFromTable(PreparedStatement stmt, int cid, int sid, 
      int oid, int tid, int whichDelete) throws SQLException {
    Log.getLogWriter().info("delete statement is " + delete[whichDelete]);
    int rowCount = 0;
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - " ;
    String query = " QUERY: " + delete[whichDelete];
    
    switch (whichDelete) {
    case 0:   
      //"delete from trade.sellordersdup where cid=? and sid=? and tid=?", //could be batch delete, but no foreign key reference, this should work    
      Log.getLogWriter().info(database + "deleting trade.sellordersdup with CID:" + cid + ",SID:" + sid 
          + ",TID:" + tid  + query);   
      stmt.setInt(1, cid);
      stmt.setInt(2, sid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 1:
      //"delete from trade.sellordersdup where oid=? and tid=?",
      Log.getLogWriter().info(database + "deleting trade.sellordersdup with OID:" + oid 
          + ",TID:" + tid + query);   
      stmt.setInt(1, oid);
      stmt.setInt(2, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 2:   
      //"delete from trade.sellordersdup where status IN ('cancelled', 'filled') and tid=?",
      Log.getLogWriter().info(database + "deleting trade.sellordersdup with TID:" + tid + query);  
      stmt.setInt(1, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 3:   
      //"delete from trade.sellordersdup where tid=?",
      Log.getLogWriter().info(database + "deleting trade.sellordersdup with TID:" + tid + query );  
      stmt.setInt(1, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 4:   
      //"delete from trade.sellordersdup where cid=? and sid=?",
      Log.getLogWriter().info(database + "deleting trade.sellordersdup with CID:" + cid + ",SID:" + sid + query );  
      stmt.setInt(1, cid);
      stmt.setInt(2, sid);
      rowCount = stmt.executeUpdate();
      break;
    case 5:
      //"delete from trade.sellordersdup where oid=?",
      Log.getLogWriter().info(database + "deleting trade.sellordersdup with OID:" + oid + query);  
      stmt.setInt(1, oid);
      rowCount = stmt.executeUpdate();
      break;
    case 6:   
      //"delete from trade.sellordersdup where status IN ('cancelled', 'filled')",
      Log.getLogWriter().info(database + "deleting trade.sellordersdup with no data" +query  );  
      rowCount = stmt.executeUpdate();
      break;
    case 7:   
      //"delete from trade.sellordersdup",
      Log.getLogWriter().info(database +  "deleting trade.sellordersdup with no data" + query);  
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

}

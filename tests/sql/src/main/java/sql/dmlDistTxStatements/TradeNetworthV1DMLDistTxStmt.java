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
package sql.dmlDistTxStatements;

import hydra.Log;
import hydra.MasterController;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.TradeNetworthDMLStmt;
import sql.sqlTx.ForeignKeyLocked;
import sql.sqlTx.SQLDistTxTest;
import sql.sqlTx.SQLTxBB;
import sql.sqlTx.SQLTxDeleteHoldKeysBlockingChildBB;
import sql.sqlTx.SQLTxHoldForeignKeysBB;
import sql.sqlTx.SQLTxHoldKeysBlockingChildBB;
import sql.sqlTx.SQLTxHoldNewForeignKeysBB;
import sql.sqlutil.DMLDistTxStmtsFactory;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public class TradeNetworthV1DMLDistTxStmt extends TradeNetworthDMLStmt
implements DMLDistTxStmtIF {
  /*
   * trade.NetWorthv1 table fields
   *   long cid;
   *   BigDecimal cash;
   *   BigDecimal securities;
   *   int loanLimit;
   *   BigDecimal availLoan;
   *   int tid; //for update or delete unique records to the thread
   *   int round;
   *   long c_cid; //added to show how to track update on foreign key field (reference customers table)
   */
  
  protected static String insert = "insert into trade.networthv1 values (?,?,?,?,?,?,?,?)";
  
  protected static String[] update = { 
    "update trade.networthv1 set c_cid=?,  round = ? where cid = ? ", 
    "update trade.networthv1 set availloan=availloan-?, round = ?  where cid in (?, ?)",
    "update trade.networthv1 set cash=cash-?, round = ?  where cid in (select min(cid) from trade.networthv1 where (securities>? and cash > ? and cid >=?)and round < ?) ",
    "update trade.networthv1 set loanLimit=?, round = ?  where  tid= ?  and round < ? and securities > ? and securities < ?",  //bulk update
    "update trade.networthv1 set availloan=availloan+? ,  round = ? where c_cid = ? and round < ?", 
    "update trade.networthv1 set c_cid = (select max(c.cid) from trade.customersv1 c where " +
    "(c.since >= ? or c.cid < ? ) and c.round <?), round = ? where cid = ?  ",
  }; 
  
  protected static String[] delete = {
    "delete from trade.networthv1 where cid = ?", 
    "delete from trade.networthv1 where cid <=? and cid> ? and (cash>? or availloan <?) and round <?"
  }; 

  protected static String[] select = {
    "select * from trade.networthv1 where cid <? and cid >= ? ",
    "select a.cid, (a.cash + a.securities - (a.loanLimit - a.availloan)) as networthv1 from (select cid, cash, securities, loanLimit, availloan from trade.networthv1 where securities <? and cash >?) as a where a.cid<? ",
    "select cid, loanlimit, availloan from trade.networthv1 where loanlimit >? and loanlimit-availloan <= ?",
    "select cid, cash, securities, round from trade.networthv1 where (cash<? or securities >=?) ",
    "select * from trade.networthv1 where cash > loanLimit - availloan and c_cid < ?",
    "select cash, securities, loanlimit, cid, availloan, c_cid from trade.networthv1 where (c_cid<? or cash > loanLimit - availloan)",
    "select securities, cash, availloan, loanlimit, cid, availloan, round from trade.networthv1 where (availloan >=securities or cash > loanLimit - availloan)"
  };
  protected static final String RANGE = "500";
  protected static final int CIDRANGE = 10;
  
  protected static boolean reproduce50649 = TestConfig.tab().booleanAt(SQLPrms.toReproduce50649, true);
  protected static boolean reproduce50607  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50607, false);
  protected static boolean reproduce50608  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50608, true);
  protected static boolean reproduce50609  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50609, true);
  protected static boolean reproduce50610  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50610, true);
  protected static boolean reproduce50658  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50658, true);
  protected static boolean reproduce50710  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50710, true);
  public boolean insertGfxd(Connection gConn, boolean withDerby) {
    return true;
    //actual insert is being called with cid when insert into customer table
  }
  
  public boolean insertGfxd(Connection gConn, boolean withDerby, long cid){
    if (!withDerby) {
      return insertGfxdOnly(gConn, cid);
    }
    int size = 1;
    BigDecimal[] cash = new BigDecimal[size];
    int[] loanLimit = new int[size];
    BigDecimal[] availLoan = new BigDecimal[size];
    int[] updateCount = new int[size];
    getDataForInsert(cash, loanLimit, availLoan, size);
    BigDecimal securities = new BigDecimal(Integer.toString(0));
    SQLException gfxdse = null;
    int txId = (Integer)SQLDistTxTest.curTxId.get();
    
    HashMap<String, Integer> modifiedKeysByOp = new HashMap<String, Integer>();
    HashMap<String, Integer> holdKeysBlockingChildByOp = new HashMap<String, Integer>(); 
    HashMap<String, Integer> holdNewParentKeysByOp = new HashMap<String, Integer>();
    
    modifiedKeysByOp.put(getTableName()+"_"+cid, txId);
    //holdKeysBlockingChildByOp.put(getTableName()+"_"+cid, (Integer)SQLDistTxTest.curTxId.get()); //networth does not have child
    
    //parent table is customersv1
    holdNewParentKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+cid, txId);
    
    try {
      insertToGfxdTable(gConn, cid, cash, securities, loanLimit, availLoan, updateCount, size);
      
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equalsIgnoreCase("X0Z02") ) { 
        if (!batchingWithSecondaryData) {
          verifyConflictNewTables(modifiedKeysByOp, 
              holdKeysBlockingChildByOp,  
              null, 
              null, 
              holdNewParentKeysByOp, se, true);
        } else {
          verifyConflictNewTablesWithBatching(modifiedKeysByOp, 
              holdKeysBlockingChildByOp,  
              null, 
              null, 
              holdNewParentKeysByOp, se, true);
        }
        return false;
      } else if (gfxdtxHANotReady && isHATest &&
        SQLHelper.gotTXNodeFailureException(se) ) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got node failure exception during Tx with HA support, continue testing");
        return false;
      } else {
        gfxdse = se; //added the test case for duplicate cid 
      }
    }
    
    if (!batchingWithSecondaryData) {
      verifyConflictNewTables(modifiedKeysByOp, 
          holdKeysBlockingChildByOp, 
          null, 
          null, 
          holdNewParentKeysByOp, gfxdse, false);
    } else verifyConflictNewTablesWithBatching(modifiedKeysByOp, 
        holdKeysBlockingChildByOp, 
        null, 
        null, 
        holdNewParentKeysByOp, gfxdse, false);
    
    //add this operation for derby
    addInsertToDerbyTx(cid, cash, loanLimit, availLoan, securities, updateCount, gfxdse);
    
    return true;
  }
  
  protected void insertToGfxdTable (Connection conn, long cid, BigDecimal[] cash, 
      BigDecimal securities, int[] loanLimit, BigDecimal[] availLoan, int[] updateCount,
      int size) throws SQLException {
    PreparedStatement stmt = null;    
    stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    
    for (int i = 0; i < size; i++) {
      updateCount[i] = insertToTable(stmt, cid, cash[i], securities, loanLimit[i], availLoan[i], tid);      
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void addInsertToDerbyTx(long cid, BigDecimal[] cash, int[] loanLimit,
      BigDecimal[] availLoan, BigDecimal securities, int[] updateCount, SQLException gfxdse){
    Object[] data = new Object[9];      
    data[0] = DMLDistTxStmtsFactory.TRADE_NETWORTH;
    data[1] = "insert";
    data[2] = cid;
    data[3] = cash;
    data[4] = loanLimit;
    data[5] = availLoan;
    data[6] = securities;
    data[7] = updateCount;
    data[8] = gfxdse;
    
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  protected boolean insertGfxdOnly(Connection gConn, long cid){
    /* need new implementation
    try {
      insert(null, gConn, 1, new long[]{cid}); 
    } catch (TestException te) {
      if (te.getMessage().contains("X0Z02") ) {
        Log.getLogWriter().info("got expected conflict exception, continuing test");
        return false;
      } else if (te.getMessage().contains("23505")) { //only try in this case
        Log.getLogWriter().info("detected duplicate primary key constraint violation during insert, continuing test");
        return false;
      } else if (gfxdtxHANotReady && isHATest && 
          SQLHelper.gotTXNodeFailureTestException(te)) {
        Log.getLogWriter().info ("got expected node failure exception, continuing test");
        return false;
      } else throw te;
    }
    return true;
    */
    //TODO to be implemented 
    Log.getLogWriter().info("needs implementation");
    return true;
  }
  
  //actually insert should be called by insert thread for customers table
  public void insert(Connection dConn, Connection gConn, int size, long[] cid) {
    BigDecimal[] cash = new BigDecimal[size];
    int[] loanLimit = new int[size];
    BigDecimal[] availLoan = new BigDecimal[size];
    getDataForInsert(cash, loanLimit, availLoan, size);
    BigDecimal securities = new BigDecimal(Integer.toString(0));

    if (dConn!=null) {
      throw new TestException("should not be executed here");
    }    
    else {
      insertToGFETable(gConn, cid, cash, securities, loanLimit, availLoan, size);
    } //no verification
  }
  
  protected void insertToGFETable (Connection conn, long[] cid, BigDecimal[] cash, 
      BigDecimal securities, int[] loanLimit, BigDecimal[] availLoan, int size)  {
    PreparedStatement stmt = getStmt(conn, insert);
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
  
  protected int insertToTable(PreparedStatement stmt, long cid, BigDecimal cash,
      BigDecimal securities, int loanLimit, BigDecimal availLoan, int tid)
      throws SQLException {
    return insertToTable(stmt, cid, cash, securities, loanLimit, availLoan, tid, false);
  }
  
  //insert a record into the table
  protected int insertToTable(PreparedStatement stmt, long cid, BigDecimal cash,
      BigDecimal securities, int loanLimit, BigDecimal availLoan, int tid, boolean isPut)
      throws SQLException {
    String txid =  SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " "; 
    
    int round = (Integer) SQLDistTxTest.iteration.get();
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - " + txid + " " ;
    Log.getLogWriter().info( database + (isPut ? "putting" : "inserting") + " into trade.networthv1 with CID:"
        + cid + ",CASH:" + cash + ":SECURITIES," + securities
        + ",LOANLIMIT:" + loanLimit + ",AVAILLOAN:" + availLoan
        + ",TID:" + tid + ",ROUND:" + round+ ",C_CID:" + cid);
    stmt.setLong(1, cid);
    stmt.setBigDecimal(2, cash);
    stmt.setBigDecimal(3, securities);  //insert is 0, will be updated by security through trigger
    stmt.setInt(4, loanLimit); 
    stmt.setBigDecimal(5, availLoan);   //availLoan is the same as loanLimit during insert
    stmt.setInt(6, tid);
    stmt.setInt(7, round);
    stmt.setLong(8, cid);
    int rowCount = stmt.executeUpdate();
    Log.getLogWriter().info(database + (isPut ? "put" : "inserted ") + rowCount + " rows in trade.networthv1 with CID:"
        + cid + ",CASH:" + cash + ":SECURITIES," + securities
        + ",LOANLIMIT:" + loanLimit + ",AVAILLOAN:" + availLoan
        + ",TID:" + tid + ",ROUND:" + round+ ",C_CID:" + cid);
    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning   
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    /* tables needs to be changed in hdfs 
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
    */  
    return rowCount;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void insertDerby(Connection dConn, int index) {
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[8];
    
    try {
      //insertToDerbyTable(dConn, cid, cash, securities, loanLimit, availLoan, size);
      insertToDerbyTable(dConn, (Long)data[2], (BigDecimal[])data[3], 
          (BigDecimal)data[6], (int[])data[4], (BigDecimal [])data[5], 
          (int[])data[7], 1); 
    } catch (SQLException derbyse) {
      SQLHelper.compareExceptions(derbyse, gfxdse); //add the duplicate primary key case
      return;
    }
    if (gfxdse != null) {
      SQLHelper.handleMissedSQLException(gfxdse);
    }
  }
  
  protected void insertToDerbyTable(Connection conn, long cid, BigDecimal[] cash, 
      BigDecimal securities, int[] loanLimit, BigDecimal[] availLoan, 
      int[] updateCount, int size) 
      throws SQLException{
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    for (int i = 0; i < size; i++) {
      count = insertToTable(stmt, cid, cash[i], securities, loanLimit[i], availLoan[i], tid);
      if (count != updateCount[i]) {
       String str = "derby insert has different row count from that of gfxd " +
          "gfxd inserted " + updateCount[i] +
          " row, but derby inserted " + count + " row";
        if (failAtUpdateCount) throw new TestException (str);
        else Log.getLogWriter().info(str);
      }
    }
  }
  
  public boolean updateGfxd(Connection gConn, boolean withDerby){
    if (!withDerby) {
      return updateGfxdOnly(gConn);
    }
    
    int size =1;
    int[] whichUpdate = new int[size];
    long[] cid = new long[size];
    long[] c_cid = new long[size];
    BigDecimal[] availLoanDelta = new BigDecimal[size];
    BigDecimal[] sec = new BigDecimal[size];
    BigDecimal[] cashDelta = new BigDecimal[size];
    BigDecimal[] cash = new BigDecimal[size];
    Date[] since = new Date[size];
    int[] newLoanLimit = new int[size];
    int[] updateCount = new int[size];
    SQLException gfxdse = null;
        
    
    //Connection nonTxConn = (Connection)SQLDistTxTest.gfxdNoneTxConn.get();
    //work around #50552 by using derby Connection instead
    //Connection nonTxConn = (Connection) SQLTest.derbyConnection.get();
    Connection nonTxConn = rand.nextBoolean()? 
        (Connection)SQLDistTxTest.gfxdNoneTxConn.get():
          (Connection) SQLTest.derbyConnection.get();
        
    getDataForUpdate(nonTxConn, cid, c_cid,
        availLoanDelta, sec, cashDelta, newLoanLimit, since, cash, whichUpdate, size);
    
    if (!reproduce50607 &&  whichUpdate[0]==5) whichUpdate[0] = 2;
    if ((!reproduce50608 || !reproduce50609) &&  
        (whichUpdate[0]==1 || whichUpdate[0] == 0 || whichUpdate[0] == 4)) whichUpdate[0] = 2;
    if (!reproduce50610 &&  whichUpdate[0]==3) whichUpdate[0] = 2;
    
    //if (reproduce50607) whichUpdate[0] = 5;
    //if (reproduce50609 || reproduce50608) whichUpdate[0] = 1; //or whichUpdate[0] = 0;
    //if (reproduce50610) whichUpdate[0] = 3;
    
    //TODO to comment out the above once any ticket is fixed and set the default to true.
    
    if (SQLTest.testPartitionBy) {
      PreparedStatement stmt = getCorrectTxStmt(gConn, whichUpdate[0]);
      if (stmt == null) {
        if ((Boolean) SQLDistTxTest.updateOnPartitionCol.get()) {
          SQLDistTxTest.updateOnPartitionCol.set(false); 
          return true; //update on partitioned column
        } 
      } 
    } //to avoid a false key to be obtained due to no op by updating on partition col
    
    HashMap<String, Integer> modifiedKeysByOp = new HashMap<String, Integer>(); //pk
    //no child table, so no blocking keys
    HashMap<String, Integer> holdForeignKeysByOp = new HashMap<String, Integer>(); 
    HashMap<String, Integer> holdNewForeignKeysByOp = new HashMap<String, Integer>(); 
    
    /*
    "update trade.networthv1 set c_cid=?,  round = ? where cid = ? ", 
    "update trade.networthv1 set availloan=availloan-?, round = ?  where cid in (?, ?)",
    "update trade.networthv1 set cash=cash-?, round = ?  where cid in (select min(cid) where (securities>? and cash > ? and cid >=?)and round < ?) ",
    "update trade.networthv1 set loanLimit=?, round = ?  where  tid= ?  and round < ? and securities > ? and securities < ?",  //bulk update
    "update trade.networthv1 set availloan=availloan+? ,  round = ? where c_cid = ? and round < ?", 
    "update trade.networthv1 set c_cid = (select max(c.cid) from trade.customersv1 c where " +
    "(c.since >= ? or c.cid < ?) and c.round <?), round = ? where cid = ?  ",
     */
    
    boolean success = false;
    try {
      success = getKeysForUpdate(nonTxConn, modifiedKeysByOp, holdForeignKeysByOp, holdNewForeignKeysByOp,
          whichUpdate[0], cid[0], sec[0], cash[0], c_cid[0], since[0]);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equals("X0Z01") && isHATest) { // handles HA issue for #41471
        Log.getLogWriter().warning("Not able to process the keys for this op due to HA, this update op does not proceed");
        return true; //not able to process the keys due to HA, it is a no op
      } else SQLHelper.handleSQLException(se);
    } 
    
    if (!success) {
      Log.getLogWriter().info("not able to get the locking keys, treat as a no op");
      return true;
    }
    
    try {
      updateGfxdTable(gConn, cid, c_cid, availLoanDelta, sec, cashDelta, 
        newLoanLimit, since, cash, whichUpdate, updateCount, size);
      
      if (isHATest && (Boolean) SQLDistTxTest.failedToGetStmtNodeFailure.get()) {
        SQLDistTxTest.failedToGetStmtNodeFailure.set(false); //reset flag
        return false; //due to node failure, assume txn rolled back        
      } 
      
      //will not update on partition column in txn unless is specifically set
      if ((Boolean) SQLDistTxTest.updateOnPartitionCol.get()) {
        SQLDistTxTest.updateOnPartitionCol.set(false); //reset flag
        return true; 
        //assume 0A000 exception does not cause txn to rollback when allowUpdateOnPartitionColumn set to true.
        //
        //when allowUpdateOnPartitionColumn set to  false, the get stmt for update is not executed
        //needs to return true here.
      }
      
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equalsIgnoreCase("X0Z02") ) { 
        if (!batchingWithSecondaryData) {
          verifyConflictNewTables(modifiedKeysByOp,  
            null, 
            null, 
            holdForeignKeysByOp, 
            holdNewForeignKeysByOp, se, true);
        } else {
          verifyConflictNewTablesWithBatching(modifiedKeysByOp,  
              null, 
              null, 
              holdForeignKeysByOp, 
              holdNewForeignKeysByOp, se, true);
        }
        return false;
      } else if (gfxdtxHANotReady && isHATest &&
        SQLHelper.gotTXNodeFailureException(se) ) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got node failure exception during Tx with HA support, continue testing");
        return false;
      } else {
        //SQLHelper.handleSQLException(se);
        gfxdse = se;
      }
    }
    
    if (!batchingWithSecondaryData) {
      verifyConflictNewTables(modifiedKeysByOp,  
          null,  
          null, 
          holdForeignKeysByOp, 
          holdNewForeignKeysByOp, gfxdse, false);
    } else {
      verifyConflictNewTablesWithBatching(modifiedKeysByOp,  
          null,  
          null, 
          holdForeignKeysByOp, 
          holdNewForeignKeysByOp, gfxdse, false);
    }
    
    //add this operation for derby
    addUpdateToDerbyTx(cid, c_cid, availLoanDelta, sec, cashDelta, 
        newLoanLimit, since, cash, whichUpdate, updateCount, gfxdse);
    
    return true;

  }
  
  protected void getDataForUpdate(Connection conn, long[] cid, long[] c_cid,
      BigDecimal[] availLoanDelta, BigDecimal[] sec, BigDecimal[] cashDelta, 
      int[] newLoanLimit, Date[] since, BigDecimal[] cash, int[] whichUpdate, int size) {
    int maxAvailLimitDelta = 10000;
    int maxCashDelta = 10000;
    int maxSec = 1000000;
    for (int i = 0; i<size; i++) {
      cid[i] = getNewTypeCidFromQuery(conn, getTableName(), (rand.nextBoolean()? getMyTid() : 
        rand.nextInt(SQLDistTxTest.numOfWorkers)));
      c_cid[i] = getNewTypeCidFromQuery(conn, TradeCustomersV1DMLDistTxStmt.getTableName(), (rand.nextBoolean()? getMyTid() : 
        rand.nextInt(SQLDistTxTest.numOfWorkers)));
      availLoanDelta[i] = getBigDecimal(Double.toString(rand.nextInt(maxAvailLimitDelta * 100 +1)*0.01));
      sec[i] = getBigDecimal(Double.toString(rand.nextInt(maxSec * 100 +1)*0.01)); //will be updated in portfolio in trigger test
      cashDelta[i] =  getBigDecimal(Double.toString((rand.nextInt(maxCashDelta) +1)*0.1));
      newLoanLimit[i] = loanLimits[rand.nextInt(loanLimits.length)];
      since[i] = getSince();
      cash[i] = getCash();
      whichUpdate[i] = rand.nextInt(update.length);
    }
  }
  
  protected BigDecimal getCash() {
    int maxCash = 10000;
    int minCash = 1000;
    return getBigDecimal(Integer.toString(rand.nextInt(maxCash-minCash) + minCash));
  }
  
  protected BigDecimal getBigDecimal(String s) {
    String p = s.substring(0, s.indexOf(".") + 2); //work around #42669 --to xxxxx.x type
    return new BigDecimal(p);
  }
  
  protected boolean getKeysForUpdate(Connection conn, HashMap<String, Integer > keys, 
      HashMap<String, Integer> holdForeignKeysByOp, HashMap<String, Integer> holdNewForeignKeysByOp,
      int whichUpdate, long cid, BigDecimal sec, BigDecimal cash,
      long c_cid, Date since) throws SQLException {

    int tid = getMyTid();
    int txId = (Integer)SQLDistTxTest.curTxId.get();
    int round = (Integer)SQLDistTxTest.iteration.get();
    String database ="gemfirexd - TXID:" + txId + " ";
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
      //"update trade.networthv1 set c_cid=?,  round = ? where cid = ? ",      
      sql = "select cid, c_cid from trade.networthv1 where cid ="+cid ;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+c_cid + " fk to be held  to block parent dml op");
        holdNewForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+c_cid, txId);
      }  
      break;
    case 1: 
      //"update trade.networthv1 set availloan=availloan-?, round = ?  where cid in (?, ?)",   
      sql = "select cid, c_cid from trade.networthv1 where cid in (" + cid + ", " + (cid-tid) + ")";
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery( sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
        //no new c_cid
      } 
      break;
    case 2: 
      // "update trade.networthv1 set cash=cash-?, round = ?  where cid in (select min(cid) where (securities>? and cash > ? and cid >=?)and round < ?) ",
      sql = "select cid, c_cid from trade.networthv1 where cid in (select min(cid) " +
      		"from trade.networthv1 where (securities>" + sec + " and cash > " + cash + 
      		" and cid >= " + cid + ") and round < " + round + ")";
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
        //no new fk held
      }       
      break; 
    case 3:
      //"update trade.networthv1 set loanLimit=?, round = ?  where  tid= ?  and round < ? and securities > ? and securities < ?",  //bulk update 
      sql = "select cid, c_cid from trade.networthv1 where tid ="+tid + " and round < " + round
        + " and securities > " + sec + " and securities < " + sec.add(new BigDecimal(RANGE));
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
        //no new fk held
      }  
      break;
    case 4:
      //"update trade.networthv1 set availloan=availloan+? ,  round = ? where c_cid = ? and round < ?", 
      sql = "select cid, c_cid from trade.networthv1 where c_cid = "+ c_cid + " and round <" +  round;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
        //no new fk held
      }  
      break;
    case 5:
      //"update trade.networthv1 set c_cid = (select max(c.cid) from trade.customersv1 c where " +
      //"(c.since >= ? or c.cid < ?) and c.round <?), round = ? where cid = ?  ",    
      sql = "select max(cid) from trade.customersv1 where (since >= '" + since
         + "' or cid < " + cid + ") and round <" + round;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      long newc_cid = 0;
      while (rs.next()) {
        newc_cid = rs.getLong(1);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+newc_cid + " fk to be held to block parent dml op");
        holdNewForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+newc_cid, txId);
      }  
      if (newc_cid == 0) {
        Log.getLogWriter().info(sql + " does not gets valid result");
        return false;
      }
      sql = "select cid, c_cid from trade.networthv1 where cid = "+ cid;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      long availCid = 0;
      while (rs.next()) {
        availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
      } 
      break;

    default:
     throw new TestException ("Wrong update statement here");
    }  
    if (rs!=null) rs.close();
    return true;
  }
  
  protected void updateGfxdTable(Connection conn, long[] cid, long[] c_cid, 
      BigDecimal[] availLoanDelta, BigDecimal[] sec, BigDecimal[] cashDelta, 
      int[] newLoanLimit, Date[] since, BigDecimal[] cash, int[] whichUpdate, 
      int[] updateCount, int size) throws SQLException {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    for (int i=0 ; i<size ; i++) {
      /* should work with tx
      if (isHATest && (whichUpdate[i] == 0 || whichUpdate[i] == 2)) {
      continue;
      } //avoid x=x+1 update in HA test for now
      */
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectTxStmt(conn, whichUpdate[i]);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed

      if (stmt!=null) {
        updateCount[i] = updateTable(stmt, cid[i], c_cid[i], availLoanDelta[i], sec[i], 
            cashDelta[i], newLoanLimit[i], since[i], cash[i], tid, whichUpdate[i] );
        
      }
    }   
  }
  
  protected PreparedStatement getCorrectTxStmt(Connection conn, int whichUpdate){
    if (partitionKeys == null) setPartitionKeys();

    return getCorrectTxStmt(conn, whichUpdate, partitionKeys);
  }
  
  //used to parse the partitionKey and test unsupported update on partitionKey, no need after bug #39913 is fixed
  protected PreparedStatement getCorrectTxStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys){
    PreparedStatement stmt = null;
    Log.getLogWriter().info("getting stmt for :" + update[whichUpdate]);
    switch (whichUpdate) {
    //round is added column, could not be used as partition column
    case 0: 
      // "update trade.networthv1 set c_cid=?,  round = ? where cid = ? ",  
      //c_cid and round are newly added columns, not partitioned fields
      stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
      // "update trade.networthv1 set availloan=availloan-?, round = ?  where cid in (?, ?)",
      if (partitionKeys.contains("availloan")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: 
      //"update trade.networthv1 set cash=cash-?, round = ?  where cid in (select min(cid) from networthv1 where (securities>? and cash > ? )and round < ?) ",
      
      if (partitionKeys.contains("cash")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: 
      //"update trade.networthv1 set loanLimit=?, round = ?  where  tid= ?  and round < ? and securities > ? and securities < ?",  //bulk update
      if (partitionKeys.contains("loanLimit")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: //update since
      //"update trade.networthv1 set availloan=availloan+? ,  round = ? where c_cid = ? and round < ?", 
      if (partitionKeys.contains("availloan")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 5: 
      //"update trade.networthv1 set c_cid = (select max(c.cid) from trade.customersv1 c where " +
      //"(c.since >= ? or c.cid < ?) and c.round <?), round = ? where cid = ?  ",
      
      //c_cid and round are newly added columns, not partitioned fields
      stmt = getStmt(conn, update[whichUpdate]);
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
   
    return stmt;
  }
  
  protected int updateTable(PreparedStatement stmt, long cid, long c_cid, 
      BigDecimal availLoanDelta, BigDecimal sec, BigDecimal cashDelta, int newLoanLimit, 
      Date since, BigDecimal cash, int tid, int whichUpdate) throws SQLException {    
    int rowCount = 0;
    int round = (Integer) SQLDistTxTest.iteration.get();
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    String query = " QUERY: " + update[whichUpdate];
    
    switch (whichUpdate) {
    case 0: 
      //"update trade.networthv1 set c_cid=?,  round = ? where cid = ? ", 
      Log.getLogWriter().info(database + "updating trade.networthv1 with C_CID:" + c_cid +
          ",ROUND:" + round +" where CID:" + cid + query);
      stmt.setLong(1, c_cid);
      stmt.setInt(2, round);  //the cid got was inserted by this thread before
      stmt.setLong(3, cid);
      rowCount = stmt.executeUpdate();  
      Log.getLogWriter().info(database + "updated " + rowCount + " in trade.networthv1 with C_CID:" + c_cid +
          ",ROUND:" + round +" where CID:" + cid + query);
      break;
    case 1: 
      //"update trade.networthv1 set availloan=availloan-?, round = ?  where cid in (?, ?)",
      Log.getLogWriter().info(database + "updating trade.networthv1 with AVAILLOANDELTA:" +
          availLoanDelta + ",ROUND:" + round +
          " where CID IN(" + cid + "," + (cid-tid) + ")" + query); 
      stmt.setBigDecimal(1, availLoanDelta);
      stmt.setInt(2, round);
      stmt.setLong(3, cid);
      stmt.setLong(4, cid - tid);
      rowCount = stmt.executeUpdate();   //may or may not be successful, depends on the cid and tid
      Log.getLogWriter().info(database + "updated " + rowCount + " in trade.networthv1 with AVAILLOANDELTA:" +
          availLoanDelta + ",ROUND:" + round +
          " where CID IN(" + cid + "," + (cid-tid) + ")" + query); 
      break;
    case 2: 
      //"update trade.networthv1 set cash=cash-?, round = ?  where cid in (select min(cid) where (securities>? and cash > ? and cid >=? )and round < ?) ",
        Log.getLogWriter().info(database + "updating trade.networthv1 with CASHDELTA:" + cashDelta + 
            ",ROUND:" + round + " where SECURITIES:" + sec + ",CASH:" + cash + ",CID:" + cid +
            ",ROUND:" + round + query);
        stmt.setBigDecimal(1, cashDelta);
        stmt.setInt(2, round);
        stmt.setBigDecimal(3, sec);
        stmt.setBigDecimal(4, cash);
        stmt.setLong(5, cid);
        stmt.setInt(6, round);
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " in " +
        		"trade.networthv1 with CASHDELTA:" + cashDelta + 
            ",ROUND:" + round + " where SECURITIES:" + sec + ",CASH:" + cash + ",CID:" + cid +
            ",ROUND:" + round + query);
      break;
    case 3: 
      //"update trade.networthv1 set loanLimit=?, round = ?  where  tid= ?  and round < ? and securities > ? and securities < ?",  //bulk update
        Log.getLogWriter().info(database + "updating trade.networthv1 with LOANLIMIT:" + newLoanLimit + 
            ",ROUND:" + round + " where TID:" + tid + ",ROUND:" + round + ",SECURITIES:" + sec +
            " AND SECURITIES:" + sec.add(new BigDecimal(RANGE)) + query); 
        stmt.setInt(1, newLoanLimit);
        stmt.setInt(2, round);
        stmt.setInt(3, tid);
        stmt.setInt(4, round);
        stmt.setBigDecimal(5, sec);
        stmt.setBigDecimal(6, sec.add(new BigDecimal(RANGE)));
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " in trade.networthv1 with LOANLIMIT:" + newLoanLimit + 
            ",ROUND:" + round + " where TID:" + tid + ",ROUND:" + round + ",SECURITIES:" + sec +
            " AND SECURITIES:" + sec.add(new BigDecimal(RANGE)) + query); 
      break;
    case 4: 
      //"update trade.networthv1 set availloan=availloan+? , round = ? where c_cid = ? and round < ?", 
        Log.getLogWriter().info(database + "updating trade.networthv1 with AVAILLOANDELTA:" +
          availLoanDelta + ",ROUND:" + round +
          " where C_CID:" + c_cid + ",ROUND:" + round + query); 
        stmt.setBigDecimal(1, availLoanDelta);
        stmt.setInt(2, round);
        stmt.setLong(3, c_cid);
        stmt.setInt(4, round);
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " in trade.networthv1 with AVAILLOANDELTA:" +
          availLoanDelta + ",ROUND:" + round +
          " where C_CID:" + c_cid + ",ROUND:" + round + query);  
      break;
    case 5: 
      //"update trade.networthv1 set c_cid = (select max(c.cid) from trade.customersv1 c where " +
      //"(c.since >= ? or c.cid < ?) and c.round <?), round = ? where cid = ?  ",
        Log.getLogWriter().info(database + "updating trade.networthv1 with C.SINCE:" + since + 
            ",C.CID:" + cid+ ",C.ROUND:" + round + ",ROUND:" + round +" where CID:" +cid +
            query); 
        stmt.setDate(1, since);
        stmt.setLong(2, cid);
        stmt.setInt(3, round);
        stmt.setInt(4, round);
        stmt.setLong(5, cid);
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " in trade.networthv1 with C.SINCE:" + since + 
            ",C.CID:" + cid+ ",C.ROUND:" + round + ",ROUND:" + round +" where CID:" +cid +
            query);
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
  
  @SuppressWarnings("unchecked")
  protected void addUpdateToDerbyTx(long[] cid, long[] c_cid, BigDecimal[] availLoanDelta, 
      BigDecimal[] sec, BigDecimal[] cashDelta, int[] newLoanLimit, Date[] since,
      BigDecimal[] cash, int[] whichUpdate, int[] updateCount, SQLException gfxdse){
    Object[] data = new Object[13];      
    data[0] = DMLDistTxStmtsFactory.TRADE_NETWORTH;
    data[1] = "update";
    data[2] = cid;
    data[3] = c_cid;
    data[4] = availLoanDelta;
    data[5] = sec;
    data[6] = cashDelta;
    data[7] = newLoanLimit;
    data[8] = since;
    data[9] = cash;
    data[10] = whichUpdate;
    data[11] = updateCount;
    data[12] = gfxdse;
     
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  @SuppressWarnings("unchecked")
  public void updateDerby(Connection dConn, int index){
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[12];
    
    try {
      //updateDerbyTableConnection conn, int[] cid, BigDecimal[] availLoanDelta, 
      //BigDecimal[] sec, BigDecimal[] cashDelta, int[] newLoanLimit, int[] whichUpdate, 
      //int[] updateCount, int size)
      updateDerbyTable(dConn, (long[])data[2], (long[])data[3], (BigDecimal[])data[4],
          (BigDecimal[])data[5], (BigDecimal[])data[6], (int[])data[7], (Date[])data[8], 
          (BigDecimal[])data[9], (int[])data[10], (int[])data[11], 1); 
    }  catch (SQLException derbyse) {
       SQLHelper.compareExceptions(derbyse, gfxdse);
       return;
    }
    if (gfxdse != null) {
      SQLHelper.handleMissedSQLException(gfxdse);
    }
  }
  
  protected void updateDerbyTable(Connection conn, long[] cid, long[] c_cid, 
      BigDecimal[] availLoanDelta, BigDecimal[] sec, BigDecimal[] cashDelta, 
      int[] newLoanLimit, Date[] since, BigDecimal[] cash, int[] whichUpdate, 
      int[] updateCount, int size) throws SQLException {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {    
      if (SQLTest.testPartitionBy)    stmt = getCorrectTxStmt(conn, whichUpdate[i]);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed

      if (stmt!=null) {
        count = updateTable(stmt, cid[i], c_cid[i], availLoanDelta[i], sec[i], 
            cashDelta[i], newLoanLimit[i], since[i], cash[i], tid, whichUpdate[i] );        
        if (count != updateCount[i]){
          String str= "Derby update has different row count from that of gfxd, " +
                  "gfxd updated " + updateCount[i] +
                  " rows, but derby updated " + count + " rows";
          if (failAtUpdateCount) throw new TestException (str);
          else Log.getLogWriter().info(str);  
        }
      }
    }
  }
  
  protected boolean updateGfxdOnly(Connection gConn){
    //TODO to be implemented 
    Log.getLogWriter().info("needs implementation");
    return true;
  }

  public boolean deleteGfxd(Connection gConn, boolean withDerby){
    if (!withDerby) {
      return deleteGfxdOnly(gConn);
    }
    
    int whichDelete = rand.nextInt(delete.length);
    if (!reproduce50649 && whichDelete ==0) whichDelete++;
    if (!reproduce50658 && whichDelete ==1) whichDelete--;
    
    if (!reproduce50658 && !reproduce50649) {
      Log.getLogWriter().info("could not execute delete stmt due to bugs");
      return true;
    }
    
    long[] cid = new long[1];
    int[] updateCount = new int[1];
    int tid = getMyTid();
    BigDecimal[] cash = new BigDecimal[1];
    BigDecimal[] availloan = new BigDecimal[1];
    SQLException gfxdse = null;
    cash[0] = getCash();
    availloan[0] = getCash();
    
    Connection nonTxConn = rand.nextBoolean()? 
        (Connection)SQLDistTxTest.gfxdNoneTxConn.get():
          (Connection) SQLTest.derbyConnection.get();
    //Connection nonTxConn = (Connection)SQLDistTxTest.gfxdNoneTxConn.get();
    //work around #50552 by using derby Connection instead
    //Connection nonTxConn = (Connection) SQLTest.derbyConnection.get();
    
    cid[0] = getNewTypeCidFromQuery(nonTxConn, getTableName(), (rand.nextBoolean()? getMyTid() : 
      rand.nextInt(SQLDistTxTest.numOfWorkers)));

    HashMap<String, Integer> modifiedKeysByOp = new HashMap<String, Integer>(); //pk
    //no child table, so no blocking keys
    HashMap<String, Integer> holdForeignKeysByOp = new HashMap<String, Integer>(); 
    HashMap<String, Integer> holdNewForeignKeysByOp = new HashMap<String, Integer>(); 
    
    try {
      getKeysForDelete(nonTxConn, modifiedKeysByOp, holdForeignKeysByOp,
          whichDelete, cid[0], cash[0], availloan[0], tid);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equals("X0Z01") && isHATest) { // handles HA issue for #41471
        Log.getLogWriter().warning("Not able to process the keys for this op due to HA, this update op does not proceed");
        return true; //not able to process the keys due to HA, it is a no op
      } else SQLHelper.handleSQLException(se);
    } 
    
    try {
      deleteFromGfxdTable(gConn, cid, cash, availloan, whichDelete, updateCount);
      
      if (isHATest && (Boolean) SQLDistTxTest.failedToGetStmtNodeFailure.get()) {
        SQLDistTxTest.failedToGetStmtNodeFailure.set(false); //reset flag
        return false; //due to node failure, assume txn rolled back        
      } 
      
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equalsIgnoreCase("X0Z02") ) { 
        if (!batchingWithSecondaryData) {
          verifyConflictNewTables(modifiedKeysByOp,  
              null, 
              null, 
              holdForeignKeysByOp, 
              holdNewForeignKeysByOp, se, true);
        } else {
          verifyConflictNewTablesWithBatching(modifiedKeysByOp,  
              null, 
              null, 
              holdForeignKeysByOp, 
              holdNewForeignKeysByOp, se, true);
        }
        return false;
      } else if (gfxdtxHANotReady && isHATest &&
        SQLHelper.gotTXNodeFailureException(se) ) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got node failure exception during Tx with HA support, continue testing");
        return false;
      } else {
        //SQLHelper.handleSQLException(se);
        gfxdse = se;
      }
    }
    
    if (!batchingWithSecondaryData) {
      verifyConflictNewTables(modifiedKeysByOp, 
          null,  
          null,
          holdForeignKeysByOp, 
          holdNewForeignKeysByOp, gfxdse, false);
    } else {
      verifyConflictNewTablesWithBatching(modifiedKeysByOp, 
          null,  
          null,
          holdForeignKeysByOp, 
          holdNewForeignKeysByOp, gfxdse, false);
    }
    
    //add this operation for derby
    addDeleteToDerbyTx(cid[0], cash[0], availloan[0], whichDelete, updateCount[0], gfxdse);
    
    return true;
  }

  @SuppressWarnings("unchecked")
  protected void addDeleteToDerbyTx(long cid, BigDecimal cash, BigDecimal availloan, 
      int whichDelete, int updateCount, SQLException gfxdse){
    Object[] data = new Object[8];      
    data[0] = DMLDistTxStmtsFactory.TRADE_NETWORTH;
    data[1] = "delete";
    data[2] = cid;
    data[3] = cash;
    data[4] = availloan;
    data[5] = whichDelete;
    data[6] = updateCount;
    data[7] = gfxdse;
    
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  protected void getKeysForDelete(Connection conn, HashMap<String, Integer > keys, 
      HashMap<String, Integer > holdForeignKeysByOp, int whichDelete, long cid, 
      BigDecimal cash, BigDecimal availloan, int tid) throws SQLException {
    int txId = (Integer)SQLDistTxTest.curTxId.get();
    int round = (Integer)SQLDistTxTest.iteration.get();
    String database ="gemfirexd - TXID:" + txId + " ";
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
      //"delete from trade.networthv1 where cid = ?", 
      sql = "select cid, c_cid from trade.networthv1 where cid ="+cid ;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
      }  
      break;
    case 1: 
      //"delete from trade.networthv1 where cid <=? and cid> ? and (cash>? or availloan <?) and round <?"
      sql = "select cid, c_cid from trade.networthv1 where cid <=" + cid + " and cid > " 
      + (cid-CIDRANGE) + " and (cash >" + cash + " or availloan<" + availloan + ") and round<" + round;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery( sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        long existingC_Cid = rs.getLong(2);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+ "_" + availCid, txId);
        Log.getLogWriter().info(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid + " fk to be held to block parent delete");
        holdForeignKeysByOp.put(TradeCustomersV1DMLDistTxStmt.getTableName()+"_"+existingC_Cid, txId);
      } 
      break;
    default:
     throw new TestException ("Wrong delete statement here");
    }  
  }
  
  protected void deleteFromGfxdTable(Connection gConn, long[] cid, BigDecimal[] cash, 
      BigDecimal[] availloan, int whichDelete, int[] updateCount) throws SQLException{
    PreparedStatement stmt = null;
    if (SQLTest.isEdge && !isTicket48176Fixed && isHATest) stmt = getStmtThrowException(gConn, delete[whichDelete]);
    else stmt = gConn.prepareStatement(delete[whichDelete]); 
    
    updateCount[0] = deleteFromTable(stmt, cid[0], cash[0], availloan[0], whichDelete);
    
  }
  
  protected int deleteFromTable(PreparedStatement stmt, long cid, BigDecimal cash, 
      BigDecimal availloan, int whichDelete) throws SQLException {
    
    int txId = (Integer) SQLDistTxTest.curTxId.get();
    int round = (Integer) SQLDistTxTest.iteration.get();
    String database =  SQLHelper.isDerbyConn(stmt.getConnection()) ? "Derby - " : "gemfirexd - TXID:" + txId + " " ;
    String query = " QUERY: " + delete[whichDelete];
    
    int rowCount=0;
    switch (whichDelete) {
    case 0:   
      //"delete from trade.networthv1 where cid = ?", 
      Log.getLogWriter().info(database + "deleteing from trade.networthv1 " +
          "with CID:" +cid + query);
      stmt.setLong(1, cid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows from " +
          "trade.networthv1 with CID:" +cid + query);
      break;
    case 1:
      //"delete from trade.networthv1 where cid <=? and cid> ? and (cash>? or availloan <?) and round <?"
      Log.getLogWriter().info(database + "deleteing from trade.networthv1 with" +
          " CID:" + cid +",CID:" + (cid-CIDRANGE) + ",CASH:" + cash +
          ",AVAILLOAN:" + availloan + ",ROUND:" + round + query);
      stmt.setLong(1, cid);
      stmt.setLong(2, cid-CIDRANGE);
      stmt.setBigDecimal(3, cash);
      stmt.setBigDecimal(4, availloan);
      stmt.setInt(5, round);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows from " +
          "trade.networthv1 with" +
          " CID:" + cid +",CID:" + (cid-CIDRANGE) + ",CASH:" + cash +
          ",AVAILLOAN:" + availloan + ",ROUND:" + round + query);
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
  
  
  protected boolean deleteGfxdOnly(Connection gConn){
    /*
    try {
      delete(null, gConn);
    } catch (TestException te) {
      if (te.getMessage().contains("X0Z02") ) {
        Log.getLogWriter().info("got expected conflict exception, continuing test");
        return false;
      } else if (gfxdtxHANotReady && isHATest && 
          SQLHelper.gotTXNodeFailureTestException(te)) {
          Log.getLogWriter().info ("got expected node failure exception, continuing test");
          return false;
      } else throw te;
    }
    return true;
    */
    //TODO to be implemented 
    Log.getLogWriter().info("needs implementation");
    return true;
  }
  
  public static String getTableName() {
    return "networthv1";
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void deleteDerby(Connection dConn, int index) {
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[7];
    
    try {
      deleteFromDerbyTable(dConn, (Long)data[2], (BigDecimal)data[3], 
          (BigDecimal)data[4],(Integer)data[5], (Integer) data[6]); 
    }  catch (SQLException derbyse) {
       SQLHelper.compareExceptions(derbyse, gfxdse);
       return;
    }
    
    if (gfxdse != null) {
      SQLHelper.handleMissedSQLException(gfxdse);
    }
  }
  
  protected void deleteFromDerbyTable(Connection dConn, long cid, BigDecimal cash, 
      BigDecimal availloan, int whichDelete, int updateCount) throws SQLException{
    PreparedStatement stmt = getStmt(dConn, delete[whichDelete]);
    int tid = getMyTid();
    Log.getLogWriter().info("Delete in derby, myTid is " + tid);
    
    int count = deleteFromTable(stmt, cid, cash, availloan, whichDelete);
    
    if (count != updateCount) {
      String str = "derby delete has different row count from that of gfxd " +
      "gfxd deleted " +  updateCount + " rows, but derby deleted " + count + " rows in "
      + getTableName();
      if (failAtUpdateCount) throw new TestException (str);
      else Log.getLogWriter().info(str);      
    }
  }
  
  
  @SuppressWarnings("unchecked")
  @Override
  public void queryDerby(Connection dConn, int index) {
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[data.length-1];
    List<Struct> gfxdList = (List<Struct>) data[data.length-2];
    ResultSet derbyRS = null;
    
    try {
      //query(dConn, whichQuery, queryCash, querySec, loanLimit, loanAmount, tid); 
      derbyRS = query(dConn, (Integer)data[2], (BigDecimal)data[3], (BigDecimal)data[4],
         (Integer)data[5], (BigDecimal)data[6], (Integer)data[7], (Long)data[8], 
         (String)data[9]); 
    }  catch (SQLException derbyse) {
      SQLHelper.compareExceptions(derbyse, gfxdse);
    }
    
    ResultSetHelper.compareResultSets(ResultSetHelper.asList(derbyRS, true), gfxdList);
    
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public boolean queryGfxd(Connection gConn, boolean withDerby) {
    if (!withDerby) {
      return queryGfxdOnly(gConn);
    }
    
    int whichQuery = rand.nextInt(select.length); //randomly select one query sql based on test uniq or not
    int cash = 100000;
    int sec = 100000;
    int tid = getMyTid();
    int loanLimit = loanLimits[rand.nextInt(loanLimits.length)];
    BigDecimal loanAmount = new BigDecimal (Integer.toString(rand.nextInt(loanLimit)));
    BigDecimal queryCash = new BigDecimal (Integer.toString(rand.nextInt(cash)));
    BigDecimal querySec= new BigDecimal (Integer.toString(rand.nextInt(sec)));
    
    //Connection nonTxConn = (Connection)SQLDistTxTest.gfxdNoneTxConn.get();
    //work around #50552 by using derby Connection instead
    //Connection nonTxConn = (Connection) SQLTest.derbyConnection.get();
    Connection nonTxConn = rand.nextBoolean()? 
        (Connection)SQLDistTxTest.gfxdNoneTxConn.get():
          (Connection) SQLTest.derbyConnection.get();
    long cid =getNewTypeCidFromQuery(nonTxConn, getTableName(), (rand.nextBoolean()? getMyTid() : 
      rand.nextInt(SQLDistTxTest.numOfWorkers)));
    
    ResultSet gfxdRS = null;
    SQLException gfxdse = null;
    
    if (!reproduce50710) return true; 
    
    HashMap<String, Integer> modifiedKeysByTx = (HashMap<String, Integer>)
    SQLDistTxTest.curTxModifiedKeys.get();
    String additionalWhereClause = getUpdatedPksByTx(modifiedKeysByTx);
    
    try {
      gfxdRS = query (gConn,  whichQuery, queryCash, querySec, loanLimit, loanAmount, tid, cid, additionalWhereClause);   
      if (gfxdRS == null) {
        /* has specific node failure exception for txn being thrown by 
         * select query, this is handled in catch block
        if (isHATest) {
          Log.getLogWriter().info("Testing HA and did not get GFXD result set");
          return true;
        }
        else  
        */ 
          throw new TestException("Not able to get gfxd result set");
      }
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equalsIgnoreCase("X0Z02") ) { 
        if (batchingWithSecondaryData) {
          verifyConflictNewTablesWithBatching(null, 
              null, 
              null,
              null, 
              null, se, true);
          return false;
        } else {
          throw new TestException("Select query in Read Committed isolation gets conflict exception without batching");
        }
      } else if (isHATest &&
          SQLHelper.gotTXNodeFailureException(se) ) {
          SQLHelper.printSQLException(se);
          Log.getLogWriter().info("got node failure exception during Tx without HA support, continue testing");
          return false;
      } else SQLHelper.handleSQLException(se);
      
      gfxdse = se;
    } 
    
    List<Struct> gfxdList = ResultSetHelper.asList(gfxdRS, false);
    /*
    if (gfxdList == null && isHATest && (Boolean) SQLDistTxTest.convertTxnRSGotNodeFailure.get()) {
      Log.getLogWriter().info("Testing HA and did not get GFXD result set due to node failure");
      SQLDistTxTest.convertTxnRSGotNodeFailure.set(false); //reset flag
      return false; //do not compare query results as gemfirexd does not get any due to node failure
    }
    */
    if (gfxdList == null && isHATest) {
      Log.getLogWriter().info("Testing HA and did not get GFXD result set due to node failure");
      return true; //see #50764
      //return false; //assume txn failure occur and txn rolled back by product, otherwise return true here 
    }

    addQueryToDerbyTx(whichQuery, queryCash, querySec, loanLimit, loanAmount, 
         tid, cid, additionalWhereClause, gfxdList, gfxdse);
      //only the first thread to commit the tx in this round could verify results
      //this is handled in the SQLDistTxTest doDMLOp
    return true;
  }
  
  protected static String getUpdatedPksByTx(HashMap<String, Integer> modifiedKeysByTx) {
    StringBuffer str = new StringBuffer();
    StringBuffer keyStr = new StringBuffer();
    
    for (String key: modifiedKeysByTx.keySet()) {
      //Log.getLogWriter().info(key);
      if (key.startsWith(getTableName())) {
        String aKey = key.substring(getTableName().length()+1, key.length());
        keyStr.append(" " + aKey +",");
      }
    }
    
    if (keyStr.length()>0) {
      str.append(" and cid not in(");
      str.append(keyStr);
      str.replace(str.lastIndexOf(","), str.length(), ")");
    }
    
    //Log.getLogWriter().info(str.toString());
    
    return str.toString();
  }
  
  protected static ResultSet query (Connection conn, int whichQuery, BigDecimal cash, BigDecimal sec, 
      int loanLimit, BigDecimal loanAmount, int tid, long cid, String additionalWhereClause) throws SQLException {
    ResultSet rs = getQuery(conn, whichQuery, cash, sec, loanLimit, loanAmount, tid, cid, additionalWhereClause);
    return rs;
  }
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, BigDecimal cash, BigDecimal sec, 
      int loanLimit, BigDecimal loanAmount, int tid, long cid, String additionalWhereClause) throws SQLException{
    PreparedStatement stmt;
    ResultSet rs = null;
    String transactionId = (Integer)SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " ";
    String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - " + transactionId + " " ;
    String selectsql = select[whichQuery];
    if (queryOpTimeNewTables) {
      if (selectsql.startsWith("select a.cid")) {
        String whereClause = additionalWhereClause.replace("cid", "a.cid");
        selectsql += whereClause;
        String fetchClause = " order by a.cid desc OFFSET 12 ROWS FETCH NEXT 5 ROWS ONLY";
        selectsql += fetchClause;
      }
      else selectsql += additionalWhereClause;
    }
    
    String query = " QUERY: " + selectsql;
    Log.getLogWriter().info(query);
    
    try {  
      stmt = conn.prepareStatement(selectsql);
      
      switch (whichQuery){
      case 0:
        //"select * from trade.networthv1 where cid <? and cid >= ? ",
        Log.getLogWriter().info(database + "querying trade.networthv1 with CID:" + cid + ",CID:" + (cid-CIDRANGE) + query );     
        stmt.setLong(1, cid);
        stmt.setLong(2, cid - CIDRANGE);
        break;
      case 1:
        //"select cid, (cash + securities - (loanLimit - availloan)) as networthv1 from (select cid, cash, securities, loanLimit, availloan from trade.networthv1 where securities <? and cash >?) where cid<?",
        Log.getLogWriter().info(database + "querying trade.networthv1 with SECURITIES:" + sec + ",CASH:" + cash + ",CID:"+cid + query );     
        stmt.setBigDecimal(1, sec);
        stmt.setBigDecimal(2, cash);
        stmt.setLong(3, cid);        
        break;
      case 2:
        //"select cid, loanlimit, availloan from trade.networthv1 where loanlimit >? and loanlimit-availloan <= ?",
        
        Log.getLogWriter().info(database + "querying trade.networthv1 with LOANLIMIT:" + loanLimit + ",LOANAMOUNT:" + loanAmount + query );     
        stmt.setInt(1, loanLimit);
        stmt.setBigDecimal(2, loanAmount); 
        break;
      case 3:
        //"select cid, cash, securities, round from trade.networthv1 where (cash<? or securities >=?) ",
        Log.getLogWriter().info(database + "querying trade.networthv1 with CASH:" +cash + ",SECURITIES:" + sec 
            + ",TID:" + tid + query  );     
        stmt.setBigDecimal(1, cash);
        stmt.setBigDecimal(2, sec);
        break;
      case 4:
        //"select * from trade.networthv1 where cash > loanLimit - availloan and c_cid < ?",
        Log.getLogWriter().info(database + "querying trade.networthv1 with C_CID:" + cid  + query );     
        stmt.setLong(1, cid);
        break;
      case 5:     
       // "select cash, securities, loanlimit, cid, availloan, c_cid from trade.networthv1 where (c_cid<? or cash > loanLimit - availloan)",
        Log.getLogWriter().info(database + "querying trade.networthv1 C_CID:" + cid + query ); 
        stmt.setLong(1, cid);
        break;
      case 6:     
        // "select securities, cash, availloan, loanlimit, cid, availloan from trade.networthv1 where (availloan >=securities or cash > loanLimit - availloan)"
         Log.getLogWriter().info(database + "querying trade.networthv1 no predicate" + query ); 
         break;
      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) return null;      
      else throw se;
    }
    return rs;
  }
  
  @SuppressWarnings("unchecked")
  protected void addQueryToDerbyTx(int whichQuery, BigDecimal queryCash, 
      BigDecimal querySec, int loanLimit, BigDecimal loanAmount, int tid, long cid,
      String additionalWhereClause, List<Struct> gfxdList, SQLException gfxdse){
    Object[] data = new Object[12];      
    data[0] = DMLDistTxStmtsFactory.TRADE_NETWORTH;
    data[1] = "query";
    data[2] = whichQuery;
    data[3] = queryCash;
    data[4] = querySec;
    data[5] = loanLimit;
    data[6] = loanAmount;
    data[7] = tid;
    data[8] = cid;
    data[9] = additionalWhereClause;
    data[10] = gfxdList;
    data[11] = gfxdse;
    
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  protected boolean queryGfxdOnly(Connection gConn){
    //TODO to be implemented
    return true;
  }
}

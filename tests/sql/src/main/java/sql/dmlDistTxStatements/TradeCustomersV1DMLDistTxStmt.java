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
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.TradeCustomersDMLStmt;
import sql.sqlTx.ForeignKeyLocked;
import sql.sqlTx.SQLDistTxTest;
import sql.sqlTx.SQLTxBB;
import sql.sqlTx.SQLTxDeleteHoldKeysBlockingChildBB;
import sql.sqlTx.SQLTxHoldForeignKeysBB;
import sql.sqlTx.SQLTxHoldKeysBlockingChildBB;
import sql.sqlutil.DMLDistTxStmtsFactory;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class TradeCustomersV1DMLDistTxStmt extends TradeCustomersDMLStmt 
implements DMLDistTxStmtIF {
  /*
   * trade.customers table fields
   *   private int cid; //auto-generated field in the test
   *   String cust_name;
   *   Date since;
   *   String addr;
   *   int tid; //for update or delete unique records
   *   long genId;
   *   int round; //for update or delete rows inserted in previous round to avoid phantom read in RC and RR
   */
  
  /*
   * trade.customersv1 table fields
   *   private long cid; //auto-generated field in the test
   *   String cust_name;
   *   Date since;
   *   String addr;
   *   int tid; //for update or delete unique records
   *   int round; //for update or delete rows inserted in previous round to avoid phantom read in RC and RR
   */
  public static boolean reproduce50676  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50676, false);
  protected static boolean addGenIdCol = SQLDistTxTest.addGenIdColInCustomersv1; //whether table add an additional genId column
  protected static String insertGenId = rand.nextBoolean()? 
      "insert into trade.customersv1 (cid, cust_name, since, addr, tid, genId, round) values (?,?,?,?,?, default, ?)":
      "insert into trade.customersv1 (cid, cust_name, since, addr, tid, round) values (?,?,?,?,?,?)";
  
  protected static String insertDerbyGenid = 
      "insert into trade.customersv1 (cid, cust_name, since, addr, tid, genId, round) values (?,?,?,?,?,?,?)";
  
  
  protected static String insert = rand.nextBoolean()? 
      "insert into trade.customersv1 (cid, cust_name, since, addr, tid, round) values (default,?,?,?,?,?)":
      "insert into trade.customersv1 (cust_name, since, addr, tid, round) values (?,?,?,?,?)";
  
  protected static String insertDerby = 
      "insert into trade.customersv1 (cid, cust_name, since, addr, tid, round) values (?,?,?,?,?,?)";
  
  //every update will update the round in the table
  //every update will update the rows with round < current round so that
  //no phantom read occurs (newly inserted/updated row qualifies the predicate)
  //the one without round in the where clause are the rows exist before
  //the current round. (select query using non txn connection would not get any new rows)
  //and any updates from other txns on the same cid will get conflict exception
  protected static String[] update = {
    "update trade.customersv1 set tid = ? , round = ? where cid=? ", 
    "update trade.customersv1 set addr = ?, round = ? where (cust_name=? or since = ? ) and round < ?  ", 
    "update trade.customersv1 set cust_name = ? , addr = ? , round = ? where cid<=? and cid>? and tid <=? and round <?",
    "update trade.customersv1 set tid = ? , round = ? where since>? and round < ? and cid in (select cid " +
        "from trade.customersv1 where cust_name in (?, ?) or addr = ?)",
    "update trade.customersv1 set since =? , round = ? where cid in (?, ?, ?) ",
  };
  
  protected static String[] delete = {
    //avoid bulk delete in parent table to avoid 23503 and cause whole txn to rollback
    "delete from trade.customersv1 where cid=?",
    "delete from trade.customersv1 where cid in (? , ?) ",
  }; 
  
  protected static String[] select = {
    "select * from trade.customersv1 where tid = ? and round <? and since> ? and cid>?",
    "select " + (!reproduce50676 ? "* " : "cid, since, cust_name, round ") + "from trade.customersv1 where cid =?",
    "select " + (!reproduce50676 ? "* " : "cid, since, addr, cust_name, round ") + "from trade.customersv1 where (cid =? or cid = ?)",
    "select " + (!reproduce50676 ? "* " : "cid, addr, since, cust_name, round ") + "from trade.customersv1 where (cid in (?, ?) or cid = ?)",
    "select cust_name, count(cid) as num from trade.customersv1 where cid> ? and round <? group by cust_name order by num, cust_name ",
    "select " + (!reproduce50676 ? "* " : "cid, since, cust_name, round, tid ") + "from trade.customersv1 where cust_name like ? and round < ?" +
    " union " +
    "select " + (!reproduce50676 ? "* " : "cid, since, cust_name, round, tid ") + "from trade.customersv1 where tid =? and cid<? and round <?" ,
    "select " + (!reproduce50676 ? "* " : "cid, since, cust_name, round ") + "from trade.customersv1 where (substr(cust_name, 5, length(cust_name)-4) = ? or " +
    "substr(addr, 16, length(addr)-15) = ?) and round <?",
  };
  
  protected static final int RANGE = 10;
  protected static boolean hasSecondary = SQLDistTxTest.hasSecondaryTables;
  protected static boolean reproduce50010 = TestConfig.tab().booleanAt(SQLPrms.toReproduce50010, false);
  protected static boolean holdParentKeysByUpdate = false;
  protected static boolean reproduce50546  = TestConfig.tab().booleanAt(SQLPrms.toReproduce50546, false);
  
  public boolean deleteGfxd(Connection gConn, boolean withDerby){
    if (!withDerby) {
      return deleteGfxdOnly(gConn);
    }
    
    //avoid too many 23503 exceptions to interference the testing of txn
    int chance = 5 * SQLDistTxTest.numOfWorkers;
    if (rand.nextInt(chance) != 1) return true;
    
    int whichDelete = rand.nextInt(delete.length);
    long[] cid = new long[1];
    int[] updateCount = new int[1];
    int tid = getMyTid();
    Connection nonTxConn = (Connection)SQLDistTxTest.gfxdNoneTxConn.get();
    SQLException gfxdse = null;
    cid[0] = getNewTypeCidFromQuery(nonTxConn, getTableName(), (rand.nextBoolean()? getMyTid() : 
      rand.nextInt(SQLDistTxTest.numOfWorkers)));
    
    HashMap<String, Integer> modifiedKeysByOp = new HashMap<String, Integer>(); //pk
    HashMap<String, Integer> holdDeleteKeysBlockingChildByOp = new HashMap<String, Integer>(); //pk 
    
    try {
      getKeysForDelete(nonTxConn, modifiedKeysByOp, holdDeleteKeysBlockingChildByOp, whichDelete, cid[0], tid); 
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z01") && isHATest) { // handles HA issue for #41471
        Log.getLogWriter().warning("Not able to process the keys for this op due to HA, this insert op does not proceed");
        return true; //not able to process the keys due to HA, it is a no op
      } else SQLHelper.handleSQLException(se);
    }
    
    try {
      deleteFromGfxdTable(gConn, cid, tid, whichDelete, updateCount);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equalsIgnoreCase("X0Z02") ) { 
        if (!batchingWithSecondaryData) {
          verifyConflictNewTables(modifiedKeysByOp, 
              null, 
              holdDeleteKeysBlockingChildByOp,
              null, 
              null, se, true);
        }
        else verifyConflictNewTablesWithBatching(modifiedKeysByOp, 
            null, 
            holdDeleteKeysBlockingChildByOp,
            null, 
            null, se, true);
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
          holdDeleteKeysBlockingChildByOp, 
          null,
          null, gfxdse, false);
    }
    else verifyConflictNewTablesWithBatching(modifiedKeysByOp,  
        null, 
        holdDeleteKeysBlockingChildByOp, 
        null,
        null, gfxdse, false);
    
    //add this operation for derby
    addDeleteToDerbyTx(cid[0], tid, whichDelete, updateCount[0], gfxdse);
    
    /* handled in verify conflict now
    hydra.blackboard.SharedMap modifiedKeysByOtherTx = SQLTxBB.getBB().getSharedMap(); 
    hydra.blackboard.SharedMap holdDeleteBlockingKeysByOtherTx = SQLTxDeleteHoldKeysBlockingChildBB.getBB().getSharedMap(); 
    
    modifiedKeysByTx.putAll(modifiedKeysByOp);
    modifiedKeysByOtherTx.putAll(modifiedKeysByOp);
    
    holdDeleteKeysByTx.putAll(holdDeleteKeysBlockingChildByOp); //parent key in the foreign key detection
    holdDeleteBlockingKeysByOtherTx.putAll(holdDeleteKeysBlockingChildByOp);
    
    SQLDistTxTest.curTxModifiedKeys.set(modifiedKeysByTx);
    SQLDistTxTest.curTxDeleteHoldKeys.set(holdDeleteKeysByTx); //used for txn_batching using thin client
    */
    
    return true;

  }

  protected void getKeysForDelete(Connection conn, HashMap<String, Integer > keys, 
      HashMap<String, Integer > holdDeleteKeysBlockingChild, 
      int whichDelete, long cid, int tid) throws SQLException {
    int txId = (Integer)SQLDistTxTest.curTxId.get();
    String database ="gemfirexd - TXID:" + txId + " ";

    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
      //"delete from trade.customersv1 where cid=?",
      sql = "select cid from trade.customersv1 where cid ="+cid ;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+"_"+availCid, txId);
        holdDeleteKeysBlockingChild.put(getTableName()+"_"+availCid, txId);
      }  
      break;
    case 1: 
      //"delete from trade.customersv1 where cid in (? , ?) ", 
      sql = "select cid from trade.customersv1 where cid in (" + cid + ", " + (cid-tid) + ")";
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery( sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+"_"+availCid, txId);
        holdDeleteKeysBlockingChild.put(getTableName()+"_"+availCid, txId);
      } 
      rs.close();
      break;
    default:
     throw new TestException ("Wrong delete statement here");
    }  
  }

  protected void deleteFromGfxdTable(Connection gConn, long[] cid, int tid, 
      int whichDelete, int[] updateCount) throws SQLException{
    PreparedStatement stmt = null;
    if (SQLTest.isEdge && !isTicket48176Fixed && isHATest) stmt = getStmtThrowException(gConn, delete[whichDelete]);
    else stmt = gConn.prepareStatement(delete[whichDelete]); 
    
    updateCount[0] = deleteFromTable(stmt, cid[0], tid, whichDelete);
    
  }
  
  protected int deleteFromTable(PreparedStatement stmt, long cid, int tid, 
      int whichDelete) throws SQLException {
    
    int txId = (Integer) SQLDistTxTest.curTxId.get();
    String database =  SQLHelper.isDerbyConn(stmt.getConnection()) ? "Derby - " : "gemfirexd - TXID:" + txId + " " ;
    String query = " QUERY: " + delete[whichDelete];
    
    int rowCount=0;
    switch (whichDelete) {
    case 0:   
      //"delete from trade.customersv1 where cid=?",
      Log.getLogWriter().info(database + "deleting from trade.customersv1 " +
      		"with CID:" +cid + query);
      stmt.setLong(1, cid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows from " +
      		"trade.customersv1 with CID:" +cid + query);
      break;
    case 1:
      //"delete from trade.customersv1 where cid in (? , ?) ", 
      Log.getLogWriter().info(database + "deleting from trade.customersv1 with" +
      		" CID:" + cid +",CID:" + (cid-tid) + query);
      stmt.setLong(1, cid);
      stmt.setLong(2, cid-tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows from " +
      		"trade.customersv1 with CID:" + cid +",CID:" + (cid-tid) + query);
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
  
  @SuppressWarnings("unchecked")
  protected void addDeleteToDerbyTx(long cid, int tid, int whichDelete, 
      int updateCount, SQLException gfxdse){
    Object[] data = new Object[7];      
    data[0] = DMLDistTxStmtsFactory.TRADE_CUSTOMERS;
    data[1] = "delete";
    data[2] = cid;
    data[3] = tid;
    data[4] = whichDelete;
    data[5] = updateCount;
    data[6] = gfxdse;
    
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  public boolean updateGfxd(Connection gConn, boolean withDerby){
    if (!withDerby) {
      return updateGfxdOnly(gConn);
    }
    
    int size =1;
    long[] cid = new long[size];
    long[] cid2 = new long[size];
    String[] cust_name = new String[size];
    String[] cust_name2 = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size]; 
    
    int[] whichUpdate = new int[size];
    int[] updateCount = new int[size];
    SQLException gfxdse = null;

    if (holdParentKeysByUpdate || (!ticket42672fixed && isCustomersPartitionedOnPKOrReplicate())) {
      //does not have global index 
      //depends on how 42672 is fixed, we need to revisit this 
      //(possibly no conflict for all updates conflict with insert of child)      
      holdParentKeysByUpdate = true;
    }
    
    getDataForUpdate((Connection)SQLDistTxTest.gfxdNoneTxConn.get(), cid, 
        cid2, cust_name, cust_name2, since, addr, whichUpdate, size);
    
    if (!reproduce50546 &&  whichUpdate[0]==3) whichUpdate[0] = 4;
    
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
    HashMap<String, Integer> holdNonDeleteKeysBlockingChildByOp = new HashMap<String, Integer>(); //pk 
    HashMap<String, Integer> holdParentKeysByOp = new HashMap<String, Integer>(); //no fk in customers
    
    try {
      getKeysForUpdate(modifiedKeysByOp, holdNonDeleteKeysBlockingChildByOp, holdParentKeysByOp,
          whichUpdate[0], cid[0], cid2[0], since[0], cust_name[0], cust_name2[0], addr[0]);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      Log.getLogWriter().warning("not able to get the keys, abort this update op");
      return true;
    }
    
    try {
      updateGfxdTable(gConn, cid, cid2, cust_name, cust_name2, since, addr, whichUpdate, updateCount, size);

      //handles get stmt failure conditions -- node failure or unsupported update on partition field
      if (isHATest && (Boolean) SQLDistTxTest.failedToGetStmtNodeFailure.get()) {
        SQLDistTxTest.failedToGetStmtNodeFailure.set(false); //reset flag
        return false; //due to node failure, assume txn rolled back        
      } 
      if ((Boolean) SQLDistTxTest.updateOnPartitionCol.get()) {
        SQLDistTxTest.updateOnPartitionCol.set(false); //reset flag
        return true; //assume 0A000 exception does not cause txn to rollback
      }
  
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equalsIgnoreCase("X0Z02") ) { 
        if (!batchingWithSecondaryData) {
          verifyConflictNewTables(modifiedKeysByOp,  
              holdNonDeleteKeysBlockingChildByOp,  
              null, 
              holdParentKeysByOp, 
              null, se, true);
        }
        else {
          verifyConflictNewTablesWithBatching(modifiedKeysByOp,  
              holdNonDeleteKeysBlockingChildByOp,  
              null, 
              holdParentKeysByOp, 
              null, se, true);
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
          holdNonDeleteKeysBlockingChildByOp, 
          null, 
          holdParentKeysByOp,  
          null, gfxdse, false);
    } else verifyConflictNewTablesWithBatching(modifiedKeysByOp, 
        holdNonDeleteKeysBlockingChildByOp, 
        null, 
        holdParentKeysByOp,  
        null, gfxdse, false);

    //add this operation for derby
    addUpdateToDerbyTx(cid, cid2, cust_name, cust_name2, since, addr, whichUpdate, updateCount, gfxdse);

    return true;

  }
  
  protected void getDataForUpdate(Connection conn, long[ ] cid, long[] cid2, String[] cust_name, 
      String[] cust_name2, Date[] since, String[] addr, int[] whichUpdate, int size){
    getExistingCidFromCustomers((Connection)SQLDistTxTest.gfxdNoneTxConn.get(), cid); //get random cid
    getExistingCidFromCustomers((Connection)SQLDistTxTest.gfxdNoneTxConn.get(), cid2); //get random cid
    
    int maxCid = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary);
    for (int i=0; i<size; i++) {
      cust_name[i] = "name" + rand.nextInt(maxCid);
      cust_name2[i] = "name" + rand.nextInt(maxCid);
      addr[i] = "address is " + cust_name[i];    
      since[i] =  getSince(); 
      whichUpdate[i] = rand.nextInt(update.length); //randomly select one update sql
    }//random data for update
  }
  
  //could get 0 as cid, used in tx
  protected void getExistingCidFromCustomers(Connection conn, long[] cid) { 
    List<Struct> list = null;
    String sql = "select cid from trade.customersv1";
    try {
      ResultSet rs = conn.createStatement().executeQuery(sql);
      list = ResultSetHelper.asList(rs, SQLHelper.isDerbyConn(conn));
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    if (list == null || list.size() == 0) {
      cid[0] = 0;
      Log.getLogWriter().info("could not get valid cid, using 0 instead");
    }
    else cid[0] =  (Long)(list.get(rand.nextInt(list.size()))).get("CID");

  }
  
  protected void getKeysForUpdate(HashMap<String, Integer > keys, 
      HashMap<String, Integer > holdKeysBlockingChild, HashMap<String, Integer > holdParentKeys,
      int whichUpdate, long cid, long cid2, Date since, String custName, String custName2,
      String addr) throws SQLException {
    int tid = getMyTid();
    int txId = (Integer)SQLDistTxTest.curTxId.get();
    int round = (Integer)SQLDistTxTest.iteration.get();
    String database ="gemfirexd - TXID:" + txId + " ";
    Connection conn = (Connection)SQLDistTxTest.gfxdNoneTxConn.get();
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
      //"update trade.customersv1 set tid = ? , round = ? where cid=? ", 
      sql = "select cid from trade.customersv1 where cid ="+cid ;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+"_"+availCid, txId);
        if (holdParentKeysByUpdate) holdKeysBlockingChild.put(getTableName()+"_"+availCid, txId);
        //no holdParentKyes for updating customers table
      }  
      break;
    case 1: 
      //"update trade.customersv1 set addr = ?, round = ? where (cust_name=? or since = ? ) and round < ?  ", 
      sql = "select cid from trade.customersv1 where (cust_name='"+custName +"' or since='" + since
        + "') and round < " + round;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery( sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+"_"+availCid, txId);
        if (holdParentKeysByUpdate) holdKeysBlockingChild.put(getTableName()+"_"+availCid, txId);
      } 
      rs.close();
      break;
    case 2: 
      // "update trade.customersv1 set cust_name = ? , addr = ? , round = ? where cid<=? and cid>? and tid <=? and round <?",
      sql = "select cid from trade.customersv1 where cid <="+cid + " and cid >"+ (cid - RANGE)
        +" and tid<=" + tid + " and round < " + round;
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+"_"+availCid, txId);
        if (holdParentKeysByUpdate) holdKeysBlockingChild.put(getTableName()+"_"+availCid, txId);
      }       
      break; 
    case 3:
      //"update trade.customersv1 set tid = ? , round = ? where since>? and round < ? and cid in (select cid " +
      //"from trade.customersv1 where cust_name in (?, ?) or addr = ?)",
      sql = "select cid from trade.customersv1 where since>'"+since + "' and round < " + round
        + " and cid in (select cid from trade.customersv1 where cust_name in('" + custName + "', '" + 
        custName2 + "') or addr = '" + addr + "')";
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+"_"+availCid, txId);
        if (holdParentKeysByUpdate) holdKeysBlockingChild.put(getTableName()+"_"+availCid, txId);
      }  
      break;
    case 4:
      //"update trade.customersv1 set since =? , round = ? where cid in (?, ?, ?) ",
      sql = "select cid from trade.customersv1 where cid in ("+cid + ", " +  (cid - RANGE) + 
      		", " + cid2 + ")";
      Log.getLogWriter().info("executing stmt:" + sql);
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
        long availCid = rs.getLong(1);
        Log.getLogWriter().info(database + getTableName()+ "_" + availCid + " exists for update");
        keys.put(getTableName()+"_"+availCid, txId);
        if (holdParentKeysByUpdate) holdKeysBlockingChild.put(getTableName()+"_"+availCid, txId);
      }  
      break;
    default:
     throw new TestException ("Wrong update statement here");
    }  
  }
  
  protected void updateGfxdTable(Connection conn, long[] cid, long[] cid2, 
      String[] cust_name, String[] cust_name2, Date[] since, String[] addr, 
      int[] whichUpdate, int[] updateCount, int size) throws SQLException {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    for (int i=0 ; i<size ; i++) {;
      if (SQLTest.testPartitionBy) { //will be removed after bug #39913 is fixed
        stmt = getCorrectTxStmt(conn, whichUpdate[i]);
      } 
      else {
        stmt = getStmt(conn, update[whichUpdate[i]]);
      }

      //stmt could be null if update on partition column, not expect txn rollback
      //stmt could be null if node failure condition hit.
      if (stmt!=null) {
        updateCount[i] = updateTable(stmt, cid[i], cid2[i], cust_name[i], cust_name2[i], 
            since[i], addr[i], tid, whichUpdate[i]);      
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
      // "update trade.customersv1 set tid = ? , round = ? where cid=? ", 
      if (partitionKeys.contains("tid")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
      // "update trade.customersv1 set addr = ?, round = ? where (cust_name=? or since = ? ) and round < ?  ", 
      if (partitionKeys.contains("addr")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: //update name, addr
      //"update trade.customersv1 set cust_name = ? , addr = ? , round = ? where cid<? and cid>? and tid <=? and round <?",
      if (partitionKeys.contains("cust_name") || partitionKeys.contains("addr")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: //update name, since
      //"update trade.customersv1 set tid = ? , round = ? where since>? and round < ? and cid in (select cid " +
      //"from trade.customersv1 where cust_name in (?, ?) or addr = ?)",
      if (partitionKeys.contains("tid")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: //update since
      //"update trade.customersv1 set since =? , round = ? where cid in (?, ?, ?) ",
      if (partitionKeys.contains("since")) {
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
  
  protected int updateTable(PreparedStatement stmt, long cid, long cid2, String cust_name,
      String cust_name2, Date since, String addr, int tid, int whichUpdate) throws SQLException {    
    int rowCount = 0;
    int round = (Integer) SQLDistTxTest.iteration.get();
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    String query = " QUERY: " + update[whichUpdate];
    
    switch (whichUpdate) {
    case 0: 
      //"update trade.customersv1 set tid = ? , round = ? where cid=? ", 
      Log.getLogWriter().info(database + "updating trade.customersv1 with TID:" + tid +
          ",ROUND:" + round +" where CID:" + cid + query);
      stmt.setInt(1, tid);
      stmt.setInt(2, round);  //the cid got was inserted by this thread before
      stmt.setLong(3, cid);
      rowCount = stmt.executeUpdate();  
      Log.getLogWriter().info(database + "updated " + rowCount + " in trade.customersv1 with TID:" + tid +
          ",ROUND:" + round +" where CID:" + cid + query);
      break;
    case 1: 
      //"update trade.customersv1 set addr = ?, round = ? where (cust_name=? or since = ? ) and round < ?  ", 
      Log.getLogWriter().info(database + "updating trade.customersv1 with ADDR:" + addr + ",ROUND:" + round +
          " where CUST_NAME" + cust_name + ",SINCE:" + since + ",ROUND:" + round + query); //use update count to see if update successful of not
      stmt.setString(1, addr);
      stmt.setInt(2, round);
      stmt.setString(3, cust_name);
      stmt.setDate(4, since);
      stmt.setInt(5, round);
      rowCount = stmt.executeUpdate();   //may or may not be successful, depends on the cid and tid
      Log.getLogWriter().info(database + "updated " + rowCount + " in trade.customersv1 with ADDR:" + addr + 
          " where CUST_NAME" + cust_name + ",SINCE:" + since + ",ROUND:" + round + query);
      break;
    case 2: //update name, addr
      //"update trade.customersv1 set cust_name = ? , addr = ? , round = ? where cid<? and cid>? and tid <=? and round <?",
        Log.getLogWriter().info(database + "updating trade.customersv1 with CUSTNAME:" + cust_name + 
            ",ADDR:" + addr +  ",ROUND:" + round + " where CID:" + cid + ",CID:" + (cid-RANGE) + 
            ",TID:" + tid + ",ROUND:" + round + query); //use update count to see if update successful of not
        stmt.setString(1, cust_name);
        stmt.setString(2, addr);
        stmt.setInt(3, round);
        stmt.setLong(4, cid);
        stmt.setLong(5, cid-RANGE);
        stmt.setInt(6, tid);
        stmt.setInt(7, round);
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " in  trade.customersv1 with CUSTNAME:" + cust_name + 
            ",ADDR:" + addr +  ",ROUND:" + round + " where CID:" + cid + ",CID:" + (cid-RANGE) + 
            ",TID:" + tid + ",ROUND:" + round + query);
      break;
    case 3: //update name, since
      //"update trade.customersv1 set tid = ? , round = ? where since>? and round < ? and cid in (select cid " +
      //"from trade.customersv1 where cust_name in (?, ?) or addr = ?)",
        Log.getLogWriter().info(database + "updating trade.customersv1 with TID:" + tid + 
            ", round:" + round + " where SINCE:" + since + ",ROUND:" + round + ",CUST_NAME in (" + cust_name + "," + 
            cust_name2 + ") " + " OR ADDR:" + addr + query); //use update count to see if update successful of not
        stmt.setInt(1, tid);
        stmt.setInt(2, round);
        stmt.setDate(3, since);
        stmt.setInt(4, round);
        stmt.setString(5, cust_name);
        stmt.setString(6, cust_name2);
        stmt.setString(7, addr);
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " in trade.customersv1 with TID:" + tid + 
            ", round:" + round + " where SINCE:" + since + ",ROUND:" + round + ",CUST_NAME in (" + cust_name + "," + 
            cust_name2 + ") " + " OR ADDR:" + addr + query);
      break;
    case 4: //update since
      //"update trade.customersv1 set since =? , round = ? where cid in (?, ?, ?) ", 
        Log.getLogWriter().info(database + "updating trade.customersv1 with SINCE:" + since + 
            ",ROUND:" + round + " where CID in (" + cid + "," + (cid-RANGE) + "," + cid2 + ") " +
            query); //use update count to see if update successful of not
        stmt.setDate(1, since);
        stmt.setInt(2, round);
        stmt.setLong(3, cid);
        stmt.setLong(4, cid-RANGE);
        stmt.setLong(5, cid2);
        rowCount = stmt.executeUpdate();
        Log.getLogWriter().info(database + "updated " + rowCount + " in  trade.customersv1 with SINCE:" + since + 
            ",ROUND:" + round + " where CID in (" + cid + "," + (cid-RANGE) + "," + cid2 + ") " +
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
  protected void addUpdateToDerbyTx(long[] cid, long[] cid2, String[] cust_name, 
      String[] cust_name2, Date[] since, String[] addr, int[] whichUpdate, 
      int[] updateCount, SQLException gfxdse){
    Object[] data = new Object[11];      
    data[0] = DMLDistTxStmtsFactory.TRADE_CUSTOMERS;
    data[1] = "update";
    data[2] = cid;
    data[3] = cid2;
    data[4] = cust_name;
    data[5] = cust_name2;
    data[6] = since;
    data[7] = addr;
    data[8] = whichUpdate;
    data[9] = updateCount;
    data[10] = gfxdse;
    
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  @SuppressWarnings("unchecked")
  public void updateDerby(Connection dConn, int index){
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[10];
    
    try {
      //(Connection conn, long[] cid, long[] cid2, 
      //String[] cust_name, String[] cust_name2, Date[] since, String[] addr, 
      //int[] whichUpdate, int[] updateCount, int size)
      updateDerbyTable(dConn, (long[])data[2], (long[])data[3], (String[])data[4], 
          (String[])data[5], (Date[])data[6], (String[])data[7], (int[])data[8], 
          (int[])data[9], 1); 
    }  catch (SQLException derbyse) {
       SQLHelper.compareExceptions(derbyse, gfxdse);
       return;
    }
    
    if (gfxdse != null) {
      SQLHelper.handleMissedSQLException(gfxdse);
    }
  }
  
  protected void updateDerbyTable(Connection conn, long[] cid, long[] cid2, 
      String[] cust_name, String[] cust_name2, Date[] since, String[] addr, 
      int[] whichUpdate, int[] updateCount, int size) throws SQLException {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      if (SQLTest.testPartitionBy)    stmt = getCorrectTxStmt(conn, whichUpdate[i]);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed
      

      if (stmt!=null) {
        count = updateTable(stmt, cid[i], cid2[i], cust_name[i], cust_name2[i], 
            since[i], addr[i], tid, whichUpdate[i]);
        
        if (count != updateCount[i]){         
          String str = "Derby update has different row count from that of gfxd " +
                  "gfxd updated " + updateCount[i] +
                  " rows, but derby updated " + count + "rows";
          if (failAtUpdateCount) throw new TestException (str);
          else Log.getLogWriter().info(str);
          
        } else 
          Log.getLogWriter().info("Derby updates " + count + " rows");
      }
    } 
  }
  
  @Override
  public boolean insertGfxd(Connection gConn, boolean withDerby){
    if (!withDerby) {
     return insertGfxdOnly(gConn);
    }

    int size = 1;
    long[] generatedCid = new long[size];
    int[] cid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];
    SQLException gfxdse = null;
    int[] updateCount = new int[size];
    getDataForInsert(cid, cust_name,since,addr, size); //get the data
    
    HashMap<String, Integer> modifiedKeysByOp = new HashMap<String, Integer>(); //pk
    HashMap<String, Integer> holdNonDeleteKeysBlockingChildByOp = new HashMap<String, Integer>(); //pk, used to blocking portfolio, buyorders inserts etc
    HashMap<String, Integer> holdParentKeysByOp = new HashMap<String, Integer>(); //no fk in customers
    
    /* no longer used in new tables txn testing
    if (addGenIdCol) {
      for (int i=0; i<size; i++) {
        modifiedKeysByOp.put(getTableName()+"_"+cid[i], (Integer)SQLDistTxTest.curTxId.get());
        //add original cid into the modified keys as the table key
      }
    }
    */
    
    try {
      insertToGfxdTable(gConn, cid, cust_name,since, addr, generatedCid, updateCount, size);
      if (!addGenIdCol) {
        for (int i=0; i<size; i++) {
          modifiedKeysByOp.put(getTableName()+"_"+generatedCid[i], (Integer)SQLDistTxTest.curTxId.get());
          //add generated id into the modified keys
          holdNonDeleteKeysBlockingChildByOp.put(getTableName()+"_"+generatedCid[i], (Integer)SQLDistTxTest.curTxId.get());
          //add the blocking keys
        }
      }
    
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equalsIgnoreCase("X0Z02") ) { 
        if (!batchingWithSecondaryData) { 
          verifyConflictNewTables(modifiedKeysByOp, 
              holdNonDeleteKeysBlockingChildByOp, 
              null, 
              holdParentKeysByOp, 
              null, se, true);
          throw new TestException("Does not expect any conflict exception here" +
          		" for inserting into table with generated alway as identity and " +
          		"no batching" + TestHelper.getStackTrace(se));
        } else {
          verifyConflictNewTablesWithBatching(modifiedKeysByOp, 
              holdNonDeleteKeysBlockingChildByOp, 
              null, 
              holdParentKeysByOp, 
              null, se, true);
        }
        return false;
      } else if (gfxdtxHANotReady && isHATest &&
        SQLHelper.gotTXNodeFailureException(se) ) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got node failure exception during Tx with HA support, continue testing");
        return false;
      } else {
        gfxdse = se;
      }
    } 
    
    if (!batchingWithSecondaryData) {
      verifyConflictNewTables(modifiedKeysByOp, 
          holdNonDeleteKeysBlockingChildByOp, 
          null,
          holdParentKeysByOp, 
          null, gfxdse, false);
    } else {
      verifyConflictNewTablesWithBatching(modifiedKeysByOp, 
          holdNonDeleteKeysBlockingChildByOp, 
          null,
          holdParentKeysByOp, 
          null, gfxdse, false);
    }
    
    SQLDistTxTest.cidInserted.set(generatedCid[0]);
    
    //add this operation for derby
    addInsertToDerbyTx(cid, cust_name, since, addr, generatedCid, updateCount, gfxdse);
    
    return true;
  }
  
  protected void getDataForInsert(int[]cid, String[] cust_name,
      Date[] since, String[] addr, int size ) {
    int key = (int) SQLBB.getBB().getSharedCounters().add(SQLBB.tradeCustomersPrimary, size);
    int counter;
    for (int i = 0 ; i <size ; i++) {
      counter = key - i;
      cid[i]= counter;
      cust_name[i] = "name" + counter;   
      addr[i] = "address is " + cust_name[i];
      since[i] = getSince();
    }    
  } 
  
  @SuppressWarnings("unchecked")
  //cid is bigint now for generated id
  protected void addInsertToDerbyTx(int[] cid, String[] cust_name, 
      Date[] since, String[] addr, long[] generatedCid, int[] updateCount, SQLException gfxdse){
    Object[] data = new Object[9];      
    data[0] = DMLDistTxStmtsFactory.TRADE_CUSTOMERS;
    data[1] = "insert";
    data[2] = cid;
    data[3] = cust_name;
    data[4] = since;
    data[5] = addr;
    data[6] = generatedCid;
    data[7] = updateCount;
    data[8] = gfxdse;
    
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  protected void insertToGfxdTable(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, long[] generatedCid, int[] count, int size) throws SQLException {
    PreparedStatement stmt = null;
    if (addGenIdCol) stmt =  conn.prepareStatement(insertGenId, Statement.RETURN_GENERATED_KEYS);
    else stmt =  conn.prepareStatement(insert, Statement.RETURN_GENERATED_KEYS);
    int tid = getMyTid();

    if (size != 1) throw new TestException("Should only insert 1 row in the test");
    for (int i=0 ; i<size ; i++) {
      count[i] = insertToTable(stmt, cid[i], cust_name[i],since[i], addr[i], generatedCid, tid); 
      Log.getLogWriter().info("gemfirexd -  inserts " + count[i] + " rows");
    }
  }

  protected int insertToTable(PreparedStatement stmt, int cid, String cust_name, Date since, String addr, 
      long[] generatedCid, int tid) throws SQLException {   
    return insertToTable(stmt, cid, cust_name, since, addr, generatedCid, tid, false);
  }
  
  //insert the record into database
  protected int insertToTable(PreparedStatement stmt, int cid, String cust_name,
      Date since, String addr, long[] generatedCid, int tid, boolean isPut) throws SQLException {   
    String transactionId = SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " ";
    boolean isDerbyConn = SQLHelper.isDerbyConn(stmt.getConnection());
    String database = isDerbyConn ? "Derby - " : "gemfirexd - " + transactionId + " " ;  
       
    int round = (Integer) SQLDistTxTest.iteration.get();
    
    int rowCount = -1;
    if (!isDerbyConn) {
      if (addGenIdCol) {
        //cid is still used to insert to customersv1 table with new genId column
        Log.getLogWriter().info(database + (isPut ? "putting" : "inserting") + " into trade." + getTableName() + " with " 
              + "CID:" + cid  
              + ",CUSTNAME:" +  cust_name
              + ",SINCE:" + since 
              + ",ADDR:" + addr 
              + ",TID:" + tid
              + ",GENERATEDCID:to be generated" 
              + ",ROUND:" + round);
        stmt.setInt(1, cid);
        stmt.setString(2, cust_name);
        stmt.setDate(3, since);
        stmt.setString(4, addr);       
        stmt.setInt(5, tid);
        stmt.setInt(6, round); //which round inserted
        rowCount = stmt.executeUpdate();
        
        long gCid = 0;
        ResultSet rs = stmt.getGeneratedKeys();
        if (rs.next()) gCid = rs.getLong(1);
        
        Log.getLogWriter().info(database + (isPut ? "put " : "inserted ") + rowCount + " rows into trade." + getTableName() + " with "  
            + "CID:" + cid 
            + ",CUSTNAME:" + cust_name 
            + ",SINCE:" + since 
            + ",ADDR:" + addr 
            + ",TID:" + tid
            + "GENERATED CID:" + gCid
            + ",ROUND:" + round);
        
        generatedCid[0] = gCid; //derby to use the generated cid
      } else {
        //cid is not used to insert to customersv1 table as cid is auto generated
        Log.getLogWriter().info(database + (isPut ? "putting" : "inserting") + " into trade." + getTableName() + " with "
            + "CID:to be generated"   
            + ",CUSTNAME:" +  cust_name
            + ",SINCE:" + since 
            + ",ADDR:" + addr 
            + ",TID:" + tid
            + ",ROUND:" + round);
        stmt.setString(1, cust_name);
        stmt.setDate(2, since);
        stmt.setString(3, addr);       
        stmt.setInt(4, tid);
        stmt.setInt(5, round); //which round inserted
        rowCount = stmt.executeUpdate();
        
        long gCid = 0;
        ResultSet rs = stmt.getGeneratedKeys();
        if (rs.next()) gCid = rs.getLong(1);
        
        Log.getLogWriter().info(database + (isPut ? "put " : "inserted ") + rowCount + " rows into trade." + getTableName() + " with " 
            + "CID:" + gCid 
            + ",CUSTNAME:" + cust_name 
            + ",SINCE:" + since 
            + ",ADDR:" + addr 
            + ",TID:" + tid
            + ",ROUND:" + round);
        
        generatedCid[0] = gCid; //derby to use the generated cid
      }
      
      SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
      if (warning != null) {
        SQLHelper.printSQLWarning(warning);
      } 
    } else {
      if (addGenIdCol) {
        Log.getLogWriter().info(database + (isPut ? "putting" : "inserting") + " into trade." + getTableName() + " with "
            + "CID:" + cid 
            + ",CUSTNAME:" + cust_name
            + ",SINCE:" + since 
            + ",ADDR:" + addr 
            + ",TID:" + tid
            + ",GENID:"+ generatedCid[0]
            + ",ROUND:" + round);
        stmt.setInt(1, cid);
        stmt.setString(2, cust_name);
        stmt.setDate(3, since);
        stmt.setString(4, addr);       
        stmt.setInt(5, tid); 
        stmt.setLong(6, generatedCid[0]);
        stmt.setInt(7, round);   
        rowCount = stmt.executeUpdate();
      
        Log.getLogWriter().info(database + (isPut ? "put " : "inserted ") + rowCount + " rows into trade." + getTableName() + " with "
          + "CID:" + cid 
          + ",CUSTNAME:" + cust_name 
          + ",SINCE:" + since 
          + ",ADDR:" + addr 
          + ",TID:" + tid
          + ",GENID:"+ generatedCid[0]
          + ",ROUND:" + round);
      } else {
        Log.getLogWriter().info(database + (isPut ? "putting" : "inserting") + " into trade." + getTableName() + " with " 
            + "CID:" + generatedCid[0] 
            + ",CUSTNAME:" + cust_name
            + ",SINCE:" + since 
            + ",ADDR:" + addr 
            + ",TID:" + tid
            + ",ROUND:" + round);
        stmt.setLong(1, generatedCid[0]);
        stmt.setString(2, cust_name);
        stmt.setDate(3, since);
        stmt.setString(4, addr);       
        stmt.setInt(5, tid);      
        stmt.setInt(6, round);   
        rowCount = stmt.executeUpdate();
      
        Log.getLogWriter().info(database + (isPut ? "put " : "inserted ") + rowCount + " rows into trade." + getTableName() + " with "
          + "CID:" + generatedCid[0] 
          + ",CUSTNAME:" + cust_name 
          + ",SINCE:" + since 
          + ",ADDR:" + addr 
          + ",TID:" + tid
          + ",ROUND:" + round);
      }
    }
    
    
    /*
    // hdfs related changes to be made in full data set table as well, before the following
    // insert could be executed.  
     * String driverName = stmt.getConnection().getMetaData().getDriverName();
    if ( driverName.toLowerCase().contains("gemfirexd") && isPut) {
      if (! SQLTest.ticket49794fixed) {
        //manually update fulldataset table for above entry.
          insertToCustomersFulldataset(stmt.getConnection() , cid, cust_name, since, addr, tid);
        }

      Log.getLogWriter().info(database + (isPut ? "putting" : "inserting") + " into trade.customers with " 
          + "CID:" + cid 
                + ",CUSTNAME:" + ((cid > 100 && cid < 120 && !generateDefaultId)   ? "null" : cust_name) 
          + ",SINCE:" + since 
          + ",ADDR:" + addr 
          + ",TID:" + tid);
          
     rowCount = stmt.executeUpdate();
     
     
     Log.getLogWriter().info(database + (isPut ? "put " : "inserted ") + rowCount + " rows into trade.customers " 
         + "CID:" + cid 
               + ",CUSTNAME:" + ((cid > 100 && cid < 120 && !generateDefaultId)   ? "null" : cust_name) 
         + ",SINCE:" + since 
         + ",ADDR:" + addr 
         + ",TID:" + tid);
     
     warning = stmt.getWarnings(); //test to see there is a warning   
     if (warning != null) {
       SQLHelper.printSQLWarning(warning);
     } 
    }   
    */   
    return rowCount;
  } 
  
  protected boolean insertGfxdOnly(Connection gConn){
    /* needs new implementation as filed data type has been changed.
    try {
      insert(null, gConn, 1, false); //use the one without inserting to networth
    } catch (TestException te) {
      if (te.getMessage().contains("X0Z02")) {
        if (nobatching || RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString().
            equalsIgnoreCase("INITTASK")) //populate table should be using sync-commit now
          throw new TestException ("Without batching, we should not get conflict " +
              "exception " + TestHelper.getStackTrace(te) );
        else {
          Log.getLogWriter().info("got expected conflict exception");
          //please note use batch insert, we may get conflict
          return false;
        }
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
  
  @SuppressWarnings("unchecked")
  @Override
  public void insertDerby(Connection dConn, int index) {
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[8];

    try {
      //insertToDerbyTable(dConn, cid, cust_name,since, addr, genId, count, size);
      insertToDerbyTable(dConn, (int[])data[2], (String[])data[3], (Date[])data[4], 
          (String[])data[5], (long[])data[6], (int[])data[7], ((int[])data[2]).length);

    } catch (SQLException derbyse) {
      if (derbyse.getSQLState().equals("38000") ||
          derbyse.getSQLState().equals("XJ208")) {
        if (!SQLTest.isEdge) {
          //if gfxd using peer driver, it may not be wrapped to 38000
          //or XJ208 for batch update exception -- added to check batch insert failed on 23505
          derbyse = derbyse.getNextException();
          if (derbyse == null) {
            throw new TestException ("derby batch update exception does not have nested exception");
          }   
        } else {
          if (gfxdse.getSQLState().equals("XJ208")) {
            gfxdse = gfxdse.getNextException();
            derbyse = derbyse.getNextException();
          } else {
            if (reproduce50010) {
              Log.getLogWriter().info("derby excetpion:");
              SQLHelper.printSQLException(derbyse);
              Log.getLogWriter().info("gfxd excetpion:");
              SQLHelper.printSQLException(gfxdse);
              throw new TestException("gfxd does not get correct batch update exception");
            } else {
              Log.getLogWriter().info("ignore ticket 50010 for now");
              derbyse = derbyse.getNextException();
            }
          }
        }
      }
      SQLHelper.compareExceptions(derbyse, gfxdse);
      return;
    }
    
    if (gfxdse != null) {
      SQLHelper.handleMissedSQLException(gfxdse);
    }
  }
  
  protected void insertToDerbyTable(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, long[] genId, int[] updateCount, int size) throws SQLException {
    PreparedStatement stmt = null;
    if (addGenIdCol) stmt = conn.prepareStatement(insertDerbyGenid);
    else stmt = conn.prepareStatement(insertDerby);
    int tid = getMyTid();
    
    int count = 0;
    for (int i=0 ; i<size ; i++) { 
      count = insertToTable(stmt, cid[i], cust_name[i],since[i], addr[i], genId, tid); 
      if (count != updateCount[i]) {
        String str = "derby insert has different row count from that of gfxd " +
          "gfxd inserted " + updateCount[i] +
          " row, but derby inserted " + count + " row";
        if (failAtUpdateCount) throw new TestException (str);
        else Log.getLogWriter().info(str);
      }
    }

  }
  
  public static String getTableName() {
    return "customersv1";
  }

  protected boolean updateGfxdOnly(Connection gConn){
    /*
    try {
      update(null, gConn, 1);
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
    */

    //TODO to be implemented 
    Log.getLogWriter().info("needs implementation");
    return true;
  }
  
  protected boolean deleteGfxdOnly(Connection gConn){
    //TO DO to be implemented
    return true;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void deleteDerby(Connection dConn, int index){
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[6];
    
    try {
      deleteFromDerbyTable(dConn, (Long)data[2],  
          (Integer)data[3], (Integer)data[4], (Integer) data[5]); 
    }  catch (SQLException derbyse) {
       SQLHelper.compareExceptions(derbyse, gfxdse);
       return;
    }
    if (gfxdse != null) {
      SQLHelper.handleMissedSQLException(gfxdse);
    }
  }
  
  protected void deleteFromDerbyTable(Connection dConn, long cid, int tid, 
      int whichDelete, int updateCount) throws SQLException{
    PreparedStatement stmt = getStmt(dConn, delete[whichDelete]);
    Log.getLogWriter().info("Delete in derby, myTid is " + tid);
    
    int count = deleteFromTable(stmt, cid, tid, whichDelete);
    
    if (count != updateCount) {
      String str = "derby delete has different row count from that of gfxd " +
        "gfxd deleted " +  updateCount + " rows, but derby deleted " + count + " rows in "
        + getTableName();
      if (failAtUpdateCount) throw new TestException (str);
      else Log.getLogWriter().info(str);
    }
  }
  
  protected boolean queryGfxdOnly(Connection gConn){
    //TODO to be implemented
    /*
    try {
      query(null, gConn);
    } catch (TestException te) {
      if (te.getMessage().contains("X0Z02") && batchingWithSecondaryData) {
        Log.getLogWriter().info("got expected conflict exception, continuing test");
        return false;
      } else if (isHATest && 
          SQLHelper.gotTXNodeFailureTestException(te)) {
        Log.getLogWriter().info ("got expected node failure exception, continuing test");
        return false;
      } else throw te;
    }
    */
    return true;
  }
  
  public boolean queryGfxd(Connection gConn, boolean withDerby){
    if (!withDerby) {
      return queryGfxdOnly(gConn);
    }
    
    int whichQuery = rand.nextInt(select.length); //randomly select one query sql
    int tid = getMyTid();
    Connection nonTxConn = (Connection)SQLDistTxTest.gfxdNoneTxConn.get();
    long cid = getNewTypeCidFromQuery(nonTxConn, getTableName(), (rand.nextBoolean()? getMyTid() : 
      rand.nextInt(SQLDistTxTest.numOfWorkers)));
    Date since = getSince();
    ResultSet gfxdRS = null;
    SQLException gfxdse = null;
    String additionalWhereClause = getUpdatedPksByTx();
    
    try {
      gfxdRS = query (gConn, whichQuery, cid, since, tid, additionalWhereClause);   
      if (gfxdRS == null) {
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
        //select query gets X0Z01 when node failure occurs. 
      } else SQLHelper.handleSQLException(se);
          
      gfxdse = se;
    }
    
    //convert gfxd result set
    List<Struct> gfxdList = ResultSetHelper.asList(gfxdRS, false);

    if (gfxdList == null && isHATest) {
      Log.getLogWriter().info("Testing HA and did not get GFXD result set due to node failure");
      return true; //see #50764
      //return false; //assume txn failure occur and txn rolled back by product, otherwise return true here 
    }
    
    addQueryToDerbyTx(whichQuery, cid, since, additionalWhereClause, gfxdList, gfxdse);

    return true;
  }  
  
  @SuppressWarnings("unchecked")
  protected void addQueryToDerbyTx(int whichQuery, long cid, 
      Date since, String additionalWhereClause, List<Struct> gfxdList, 
      SQLException gfxdse){
    Object[] data = new Object[8];      
    data[0] = DMLDistTxStmtsFactory.TRADE_CUSTOMERS;
    data[1] = "query";
    data[2] = whichQuery;
    data[3] = cid;
    data[4] = since;
    data[5] = additionalWhereClause;
    data[6] = gfxdList;
    data[7] = gfxdse;
    
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    if (derbyOps == null) derbyOps = new ArrayList<Object[]>();
    derbyOps.add(data);
    SQLDistTxTest.derbyOps.set(derbyOps);
  }
  
  protected static ResultSet query (Connection conn, int whichQuery, long cid, 
      Date since, int tid, String additionalWhereClause) throws SQLException {
    ResultSet rs = getQuery(conn, whichQuery, cid, since, tid, additionalWhereClause);
    return rs;
  }
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, long cid, Date since, 
      int tid, String additionalWhereClause) throws SQLException{
    PreparedStatement stmt;
    ResultSet rs = null;
    String transactionId = (Integer)SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " ";
    String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - " + transactionId + " " ;
    int round = (Integer)SQLDistTxTest.iteration.get();
    String selectsql = select[whichQuery];
    if (queryOpTimeNewTables) {
      if (selectsql.contains("group by")) {
        selectsql = selectsql.substring(0, selectsql.indexOf("group")) + additionalWhereClause
        + selectsql.substring(selectsql.indexOf("group"), selectsql.length());
      } else if (selectsql.contains("union")) {
        selectsql = selectsql.substring(0, selectsql.indexOf("union")) + additionalWhereClause
        + selectsql.substring(selectsql.indexOf("union"), selectsql.length()) + additionalWhereClause;
      } else
        selectsql += additionalWhereClause;
    }
    
    String query = " QUERY: " + selectsql;
    Log.getLogWriter().info(query);
    
    try {  
      stmt = conn.prepareStatement(selectsql);
      
      switch (whichQuery){
      case 0:
        //"select * from trade.customersv1 where tid = ? and round <? and since> ? and cid>?",
        Log.getLogWriter().info(database + "querying trade.customersv1 with TID:"+ tid + 
            ",ROUND:" + round + ",SINCE:" + since + ",CID:" + cid + query);
        stmt.setInt(1, tid);
        stmt.setInt(2, round);
        stmt.setDate(3, since);
        stmt.setLong(4, cid);
        break;
      case 1: 
        //"select cid, since, cust_name, round from trade.customersv1 where cid =?", 
        Log.getLogWriter().info(database + "querying trade.customersv1 with CID:" + cid + query);
        stmt.setLong(1, cid);
        break;
      case 2:
        //"select cid, since, addr, cust_name, round from trade.customersv1 where cid =? or cid = ?",  
        Log.getLogWriter().info(database + "querying trade.customersv1 with CID:" + cid + ",CID: "+ (cid-RANGE) + query);
        stmt.setLong(1, cid);
        stmt.setLong(2, cid-RANGE); 
        break;
      case 3:
        //"select cid, addr, since, cust_name, round from trade.customersv1 where cid in (?, ?) or cid = ?",
        Log.getLogWriter().info(database + "querying trade.customersv1 with CID:" + cid + ",CID"+ (cid-RANGE) + ",CID:"+ (cid - 2 *RANGE) + query);
        stmt.setLong(1, cid); 
        stmt.setLong(2, cid - RANGE); 
        stmt.setLong(3, cid - 2*RANGE);
        break;
      case 4:
        //"select cust_name, count(cid) as num from trade.customersv1 where cid> ? and round <? group by cust_name order by num, cust_name ",
        
        Log.getLogWriter().info(database + "querying trade.customersv1 with CID:"+ cid + ",ROUND:" + round + query);
        stmt.setLong(1, cid);
        stmt.setInt(2, round);
        break;
      case 5:
        // "select cid, since, cust_name, round from trade.customersv1 where cust_name like ? and round <?" +
        //"union " +
        //"select cid, since, cust_name, round from trade.customersv1 where tid =? and cid<? and round <?" ,        
        String custName = getUnionCustNameWildcard(cid);
        Log.getLogWriter().info(database + "querying trade.customersv1 with " +
            "CUSTNAME:" + custName + ",ROUND:" + round + ",TID:" + tid +
            ",CID:" + cid + ",ROUND:" + round + query);
        stmt.setInt(2, round);
        stmt.setString(1, custName);
        stmt.setInt(3, tid);
        stmt.setLong(4, cid);
        stmt.setInt(5, round); 
        break;        
      case 6:
        //"select cid, since, cust_name from trade.customersv1 where (substr(cust_name, 5, length(cust_name)-4) = ? or " +
        //"substr(addr, 16, length(addr)-15) = ?) and round <?",
        Log.getLogWriter().info(database + "querying trade.customersv1 with substr(cust_name, 5, length(cust_name)-4) = '" + cid + 
            "' or substr(addr, 16, length(addr)-15) = '" + (cid - 10) + "',ROUND" + round + query);
        stmt.setString(1, ""+cid);
        stmt.setString(2, ""+(cid-10)); 
        stmt.setInt(3, round);
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
  protected static String getUpdatedPksByTx() {
    StringBuffer str = new StringBuffer();
    StringBuffer keyStr = new StringBuffer();
    HashMap<String, Integer> modifiedKeysByTx = (HashMap<String, Integer>)
    SQLDistTxTest.curTxModifiedKeys.get();
    
    for (String key: modifiedKeysByTx.keySet()) {
      Log.getLogWriter().info(key);
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
    
    Log.getLogWriter().info(str.toString());
    
    return str.toString();
  }
  
  protected static String getUnionCustNameWildcard(long i) {
    String str = i>=10? Long.toString(i).substring(0,2) : Long.toString(i);
    return "%me%" + str  + "%";
  }
  
  @SuppressWarnings("unchecked")
  public void queryDerby(Connection dConn, int index){
    ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDistTxTest.derbyOps.get();
    Object[] data = derbyOps.get(index);
    SQLException gfxdse = (SQLException) data[7];
    List<Struct> gfxdList = (List<Struct>) data[6];
    ResultSet derbyRS = null;
    
    try {
      //query (dConn, whichQuery, cid, since, getMyTid()); 
      derbyRS = query(dConn, (Integer)data[2], (Long)data[3], (Date)data[4], 
          getMyTid(), (String) data[5]); 
      if (derbyRS == null) throw new TestException ("Test issue, does not " +
      		"expect there is no result from derby");
    }  catch (SQLException derbyse) {
      SQLHelper.compareExceptions(derbyse, gfxdse);
    }
    
    ResultSetHelper.compareResultSets(ResultSetHelper.asList(derbyRS, true), gfxdList);
  }
  
}

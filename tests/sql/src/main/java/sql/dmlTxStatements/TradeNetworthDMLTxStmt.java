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
package sql.dmlTxStatements;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import sql.SQLBB;
import sql.SQLHelper;
import sql.dmlStatements.TradeNetworthDMLStmt;
import sql.sqlTx.SQLDerbyTxBB;
import sql.sqlutil.DMLTxStmtsFactory;
import util.TestException;
import util.TestHelper;

public class TradeNetworthDMLTxStmt extends TradeNetworthDMLStmt implements
		DMLTxStmtIF {
	
	String[] updateByCidRange = {
		  "update trade.networth set availloan=availloan-? where cid = ? ", 
		  "update trade.networth set securities=? where cid = ?  ",
		  "update trade.networth set cash=cash-? where cid = ? ",
		  //update multi rows in one statement
		  "update trade.networth set loanLimit=? where cid>? and cid < ? ", 
	};
	
	int updateByCidRangeSingleRowStmt = 3;
	
	String[] updateByTidList = {
		  "update trade.networth set availloan=availloan-? where cid = ? and tid= ?", 
		  "update trade.networth set securities=? where cid = ?  and tid= ?",
		  "update trade.networth set cash=cash-? where cid = ? and tid= ?",
		  //update multi rows in one statement
		  "update trade.networth set loanLimit=? where securities > ? and tid = ?", 
	};
	
	int updateByTidListSingleRowStmt = 3;
	
	String[] deleteByCidRange = {
	    "delete from trade.networth where cid=?", 
	    //delete multi rows in one statement
	    "delete from trade.networth where cid > ? and cid < ? " //multiple records
	};
	
	int deleteByCidRangeSingleRowStmt = 1;
	
	String[] deleteByTidList = {
			"delete from trade.networth where cid=? and tid=? ", 
		  //delete multi rows in one statement
			"delete from trade.networth where (cid > ? and cid < ? ) and tid = ?",
	};
	
	int deleteByTidListSingleRowStmt = 1;
	
	@SuppressWarnings("unchecked")
	public void deleteTx(Connection dConn, Connection gConn, 
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int whichDelete = getWhichTxDelete();
    int txId = modifiedKeys.get(thisTxId);
   
    int tid = getTxColocatedTid();  //colocated tid;
    int cid = getTxColocatedCid(gConn, tid, txId);  //colocated Cid  
    int cid2 = cid + cidRangeForTxOp - 1;
    boolean opFailed = false;
    
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeDelete(gConn, keys, whichDelete, cid, cid2, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
        //TODO check keys are colocated with other dml operations in the tx
    		deleteToGFXDTableCidRangeTx(gConn, cid, cid2, whichDelete);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListDelete(gConn, keys, whichDelete, cid, cid2, tid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		deleteToGFXDTableTidListTx(gConn, cid, cid2, whichDelete, tid);
    	}
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      //for partitionByCidRange cases
    	//only expected exception within the tx is colocate conflict exception
      //dConn != null -- otherwise in gfxd only testing, a select query could return the key,
      // but before actually perform the operation within the tx context, 
      // the row could be deleted by another thread
    	if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn != null
    			&& byCidRange) { 
    		if (!sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid)) {
    			Log.getLogWriter().info("got expected non colocated exception, continuing testing");
    			return; //this is like a no op in the tx.
    		} else {
    			throw new TestException("got unexpected non colocated exception, keys are colocated"
    					+ TestHelper.getStackTrace(se));
    		}
    	}  else if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn == null
    			&& byCidRange) {
    		if (sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid)) {
    			Log.getLogWriter().warning("should not get non colocated exception here");
    		}
    		return; //this is like a no op in the tx.
    	} else {
    		SQLHelper.handleSQLException(se);
    	}
    }
    
    //check if it should throw non colocated conflict exception
    //dConn != null -- otherwise in gfxd only testing, a select query could return the key,
    // but before actually perform the operation within the tx context, 
    // the row could be deleted by another thread
    if (modifiedKeys.size() > 1 && byCidRange && dConn != null
    		&& !sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid)) {
    	Set<String> modkeys = modifiedKeys.keySet();
    	StringBuffer str = new StringBuffer();
    	str.append("The inserted cid is " + cid);
    	str.append(", it is not colocated with following cid in this tx: \n");
    	for (String key: modkeys) {
    		if (!key.equalsIgnoreCase(thisTxId)) {
    			str.append(key +",\t");
    		}
    	}
    	throw new TestException ("Did not get the expected non colocated exception\n" + str.toString());
    }  //range buckets are same as data nodes, each bucket should be on one data node if it is balanced.
    
    if (!opFailed) {
    	sExList.add(null); //to compare two exception list
    	modifiedKeys.putAll(keys);
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = null;
    	if (byCidRange) {
	    	data = new Object[5];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_NETWORTH;
	    	data[1] = deleteToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid;
	    	data[3] = cid2;
	    	data[4] = whichDelete;
    	} else if (byTidList) {
	    	data = new Object[6];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_NETWORTH;
	    	data[1] = deleteToDerbyTableTidListTx; //which operation
	    	data[2] = cid;
	    	data[3] = cid2;
	    	data[4] = whichDelete;
	    	data[5] = tid;    		
    	}
    	    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
      SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }

	}

	public void insertTx(Connection dConn, Connection gConn, int size) {
		// actual inert being called in customer insert using insertTX(dConn, gConn, size, cid);		
	}

	public void populateTx(Connection dConn, Connection gConn) {
		//populate are handled in customers table insert 
	}

	public void queryTx(Connection dConn, Connection gConn) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	public void updateTx(Connection dConn, Connection gConn,
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
		int size = 1;
		int[] ignore = new int[size];
    int[] cid = new int[size];
    BigDecimal[] availLoanDelta = new BigDecimal[size];
    BigDecimal[] sec = new BigDecimal[size];
    BigDecimal[] cashDelta = new BigDecimal[size];
    int[] newLoanLimit = new int[size];
    getDataForUpdate(gConn, cid, availLoanDelta, sec, cashDelta, newLoanLimit, ignore, size); //get the data
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int whichUpdate = getWhichTxUpdate();
    int tid = -1;
    int txId = modifiedKeys.get(thisTxId);
    boolean opFailed = false;
  
    if (byTidList) {
    	tid = sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    	cid[0] = getCidByList(gConn, tid); //use this cid instead
    }
    if (byCidRange) {
    	Object txIdCid = SQLBB.getBB().getSharedMap().get(cid_txId + txId);
    	cid[0] = sql.sqlutil.GFXDTxHelper.getColocatedCid(getTableName(), (Integer)txIdCid);
    }
    int cid2 = cid[0] + cidRangeForTxOp;
    
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeUpdate(gConn, keys, whichUpdate, cid[0], cid2, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		updateToGFXDTableCidRangeTx(gConn,  cid[0], cid2, availLoanDelta[0],  sec[0],
    	      cashDelta[0], newLoanLimit[0], whichUpdate);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListUpdate(gConn, keys, whichUpdate, cid[0], sec[0], tid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableTidListTx(gConn, cid[0], availLoanDelta[0], sec[0],
            cashDelta[0], newLoanLimit[0], tid, whichUpdate);
    	}
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      //for partitionByCidRange cases
    	//only expected exception within the tx is colocate conflict exception
      //dConn != null -- otherwise in gfxd only testing, a select query could return the key,
      // but before actually perform the operation within the tx context, 
      // the row could be deleted by another thread
    	if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn != null
    			&& byCidRange) { 
    		if (!sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid[0])) {
    			Log.getLogWriter().info("got expected non colocated exception, continuing testing");
    			return; //this is like a no op in the tx.
    		} else {
    			throw new TestException("got unexpected non colocated exception, keys are colocated"
    					+ TestHelper.getStackTrace(se));
    		}
    	} else if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn == null
    			&& byCidRange) {
    		if (sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid[0])) {
    			Log.getLogWriter().warning("should not get non colocated exception here");
    		}
    		return; //this is like a no op in the tx.
    	} else if (se.getSQLState().equalsIgnoreCase("23513")) {
      	opFailed = true;
      	sExList.add(se);
    	}else {
    		SQLHelper.handleSQLException(se);
    	}
    }
    if (!opFailed) {
    	sExList.add(null); //to compare two exception list
    	modifiedKeys.putAll(keys);
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = null;
    	if (byCidRange) {
	    	data = new Object[9];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_NETWORTH;
	    	data[1] = updateToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid[0];
	    	data[3] = cid2;
	    	data[4] = availLoanDelta[0];
	    	data[5] = sec[0];
	    	data[6] = cashDelta[0];
	    	data[7] = newLoanLimit[0];
	    	data[8] = whichUpdate;
    	} else if (byTidList) {
	    	data = new Object[9];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_NETWORTH;
	    	data[1] = updateToDerbyTableTidListTx; //which operation
	    	data[2] = cid[0];
	    	data[3] = availLoanDelta[0];
	    	data[4] = sec[0];
	    	data[5] = cashDelta[0];
	    	data[6] = newLoanLimit[0];
	    	data[7] = tid;
	    	data[8] = whichUpdate;    		
    	}
    			
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
        SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }	
	}
	
	protected void updateToGFXDTableCidRangeTx(Connection conn, int cid, int cid2, BigDecimal availLoanDelta, 
			BigDecimal sec, BigDecimal cashDelta, int newLoanLimit, int whichUpdate
			) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update networth table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    count = updateToTableCidRangeTx(stmt, cid, cid2, availLoanDelta, sec,
        cashDelta, newLoanLimit, whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableCidRangeTx(Connection conn, int cid, int cid2, 
			BigDecimal availLoanDelta, BigDecimal sec, BigDecimal cashDelta, 
			int newLoanLimit, int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update networth table in derby, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    try {
    	count = updateToTableCidRangeTx(stmt, cid, cid2, availLoanDelta, sec,
          cashDelta, newLoanLimit, whichUpdate);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	protected void updateToGFXDTableTidListTx(Connection conn, int cid, BigDecimal availLoanDelta, 
			BigDecimal sec, BigDecimal cashDelta, int newLoanLimit, int tid, int whichUpdate
			) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update networth table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    count = updateToTableTidListTx(stmt, cid, availLoanDelta, sec,
        cashDelta, newLoanLimit, tid, whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableTidListTx(Connection conn, int cid, 
			BigDecimal availLoanDelta, BigDecimal sec, BigDecimal cashDelta, 
			int newLoanLimit, int tid, int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update networth table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    try {
    	count = updateToTableTidListTx(stmt, cid, availLoanDelta, sec,
          cashDelta, newLoanLimit, tid, whichUpdate);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	protected int updateToTableCidRangeTx(PreparedStatement stmt, int cid, int cid2, BigDecimal availLoanDelta, BigDecimal sec,
      BigDecimal cashDelta, int newLoanLimit, int whichUpdate) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		// "update trade.networth set availloan=availloan-? where cid = ? ", 
      Log.getLogWriter().info("updating networth table availLoan to (availLoan- " + availLoanDelta 
          + ") for cid: " + cid );
      stmt.setBigDecimal(1, availLoanDelta);
      stmt.setInt(2, cid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"update trade.networth set securities=? where cid = ?  ",
      Log.getLogWriter().info("updating networth table securities to " +sec + " for cid: " + cid ); 
     stmt.setBigDecimal(1, sec);
     stmt.setInt(2, cid);
     rowCount =  stmt.executeUpdate();
     break;
    case 2: 
      //  "update trade.networth set cash=cash-? where cid = ? ",
      Log.getLogWriter().info("updating networth table cash to (cash- " + cashDelta 
          + ") for cid: " + cid);
      stmt.setBigDecimal(1, cashDelta);
      stmt.setInt(2, cid);
      rowCount =  stmt.executeUpdate();
      break; 
    case 3: 
		  //"update trade.networth set loanLimit=? where cid>? and cid < ? ", 
      Log.getLogWriter().info("updating networth table newLoanLimit with " + newLoanLimit + 
      		", for cid: " + cid + " to " + cid2);
      stmt.setInt(1, newLoanLimit);
      stmt.setInt(2, cid);  
      stmt.setInt(3, cid2);
      rowCount = stmt.executeUpdate();
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

	protected int updateToTableTidListTx(PreparedStatement stmt, int cid, BigDecimal availLoanDelta, BigDecimal sec,
      BigDecimal cashDelta, int newLoanLimit, int tid, int whichUpdate) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
      //"update trade.networth set availLoan=availLoan-? where cid = ? and tid= ?",
      Log.getLogWriter().info("updating networth table availLoan to (availLoan- " + availLoanDelta 
          + ") for cid: " + cid + " tid: " + tid);
      stmt.setBigDecimal(1, availLoanDelta);
      stmt.setInt(2, cid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"update trade.networth set securities=? where cid = ?  and tid= ?",
      Log.getLogWriter().info("updating networth table securities = " +sec + " for cid: " + cid  +
           " tid: " + tid); 
      stmt.setBigDecimal(1, sec);
      stmt.setInt(2, cid);
      stmt.setInt(3, tid);
      rowCount =  stmt.executeUpdate();
      break;
    case 2: 
      //"update trade.networth set cash=cash-? where cid = ? and tid= ?",
      Log.getLogWriter().info("updating networth table cash to (cash- " + cashDelta 
          + ") for cid: " + cid + " tid: " + tid);
      stmt.setBigDecimal(1, cashDelta);
      stmt.setInt(2, cid);
      stmt.setInt(3, tid);
      rowCount =  stmt.executeUpdate();
      break; 
    case 3: 
      //"update trade.networth set loanLimit=? where securities > ? and tid = ?",  //batch operation   
      Log.getLogWriter().info("updating networth table newLoanLimit with " + newLoanLimit + " for securities is more than " 
          + sec +  " and tid: " + tid);
      stmt.setInt(1, newLoanLimit);
      stmt.setBigDecimal(2, sec);
      stmt.setInt(3, tid);      
      rowCount = stmt.executeUpdate();
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
	
	
	//return correct update stmt based on whether stmt only update on single row
	protected int getWhichTxUpdate() {
		if (byCidRange && !singleRowTx) 
			return rand.nextInt(updateByCidRange.length);
		else if (byCidRange && singleRowTx) 
			return rand.nextInt(updateByCidRangeSingleRowStmt);
		else if (byTidList && !singleRowTx)
			return rand.nextInt(updateByTidList.length);
		else if (byTidList && singleRowTx)
			return rand.nextInt(updateByTidListSingleRowStmt);
		else 
			return -1;
	}
	
	//return correct delete stmt based on whether stmt only update on single row
	protected int getWhichTxDelete() {
		if (byCidRange && !singleRowTx) 
			return rand.nextInt(deleteByCidRange.length);
		else if (byCidRange && singleRowTx) 
			return rand.nextInt(deleteByCidRangeSingleRowStmt);
		else if (byTidList && !singleRowTx)
			return rand.nextInt(deleteByTidList.length);
		else if (byTidList && singleRowTx)
			return rand.nextInt(deleteByTidListSingleRowStmt);
		else 
			return -1;
	}
	
	protected void getKeysForCidRangeDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int cid2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //"delete from trade.networth where cid=?", 
    	sql = "select cid from trade.networth where cid="+cid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;
    case 1: 
	    //"delete from trade.networth where cid > ? and cid < ? " //multiple records
      sql = "select cid from trade.networth where cid>"+cid 
      	+" and cid<" + cid2;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availCid = rs.getInt(1);
        Log.getLogWriter().info("cid: " + availCid + " exists for update");
      	keys.put(getTableName()+"_"+availCid, txId);
      }   
      rs.close();
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void getKeysForTidListDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int cid2, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //"delete from trade.networth where cid=? and tid=? ", 
    	sql = "select cid from trade.networth where cid="+cid + " and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;
    case 1: 
			//"delete from trade.networth where (cid > ? and cid < ? ) and tid = ?", 
      sql = "select cid from trade.networth where (cid>" + cid 
      	+" and cid<" + cid2 + ") and tid = " + tid;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availCid = rs.getInt(1);
        Log.getLogWriter().info("cid: " + availCid + " exists for update");
      	keys.put(getTableName()+"_"+availCid, txId);
      }  
      rs.close();
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected boolean deleteToDerbyTableCidRangeTx(Connection conn, int cid, int cid2, 
			 int whichDelete) throws SQLException {
   PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
   int tid = getMyTid();
   int count = -1;
   Log.getLogWriter().info("delete networth table in derby, myTid is " + tid);
   Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
   try {
   	count = deleteToTableCidRangeTx(stmt, cid, cid2, whichDelete);
   } catch (SQLException se) {
     if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
       Log.getLogWriter().info("detected the lock issue, will try it again");
       return false;
     } else throw se;
   }
   Log.getLogWriter().info("derby deleted " + count + " rows");
   return true;
	}
	
	protected void deleteToGFXDTableCidRangeTx(Connection conn, int cid, int cid2, 
			 int whichDelete) throws SQLException {
	  //for gemfirexd checking
   PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
   int tid = getMyTid();
   int count = -1;
   Log.getLogWriter().info("delete networth table in gemfirexd, myTid is " + tid);
   Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
   count = deleteToTableCidRangeTx(stmt, cid, cid2, whichDelete);
   Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
	}
	
	protected int deleteToTableCidRangeTx(PreparedStatement stmt, int cid, int cid2, 
			int whichDelete) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.networth where cid=?", 
      Log.getLogWriter().info("delete from networth for cid: " + cid);
      stmt.setInt(1, cid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
	    //"delete from trade.networth where cid > ? and cid < ? " //multiple records
      Log.getLogWriter().info("delete from networth for cid> " + cid  + " and cid< " + cid2);
      stmt.setInt(1, cid);
      stmt.setInt(2, cid2); 
      rowCount = stmt.executeUpdate();       
      break;
    default:
     throw new TestException ("Wrong delete sql string here");
    }
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;		
		
	}
	
	protected void deleteToGFXDTableTidListTx(Connection conn, int cid, int cid2, 
			int whichDelete, int tid) throws SQLException {
	  //for gemfirexd checking
   PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
   int count = -1;    
   Log.getLogWriter().info("delete networth table in gemfirexd, myTid is " + getMyTid());
   Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
   count = deleteToTableTidListTx(stmt, cid, cid2, whichDelete, tid);
   Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
		
	}
	
	protected boolean deleteToDerbyTableTidListTx(Connection conn, int cid,  
			int cid2, int whichDelete, int tid) throws SQLException {
   PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
   int count = -1;
   Log.getLogWriter().info("delete networth table in derby, myTid is " + getMyTid());
   Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
   try {
   	count = deleteToTableTidListTx(stmt, cid, cid2,  whichDelete, tid);
   } catch (SQLException se) {
     if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
       Log.getLogWriter().info("detected the lock issue, will try it again");
       return false;
     } else throw se;
   }
   Log.getLogWriter().info("derby deleted " + count + " rows");
   return true;
	}
	
	protected int deleteToTableTidListTx(PreparedStatement stmt, int cid, int cid2,
			int whichDelete, int tid) throws SQLException {
   int rowCount = 0;
   switch (whichDelete) {
   case 0: 
			//"delete from trade.networth where cid=? and tid=? ",  
     Log.getLogWriter().info("delete from networth for cid: " + cid + " and tid: " + tid);
     stmt.setInt(1, cid);
     stmt.setInt(2, tid);
     rowCount = stmt.executeUpdate();    
     break;
   case 1: 
			//"delete from trade.networth where (cid > ? and cid < ? ) and tid = ?",
     Log.getLogWriter().info("delete from networth for cid>" + cid  + " and " +
     		"cid< " + cid2 + " and tid: " + tid);
     stmt.setInt(1, cid);
     stmt.setInt(2, cid2);
     stmt.setInt(3, tid);
     rowCount = stmt.executeUpdate();       
     break;
   default:
    throw new TestException ("Wrong delete sql string here");
   }
   SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
   if (warning != null) {
     SQLHelper.printSQLWarning(warning);
   } 
   return rowCount;		
	}
	
	
  //find  a cid which was put by the tid
  protected int getCidByList(Connection conn, int tid) {
  	String sql = "select cid from trade.networth where tid = ?";
  	try {
  		PreparedStatement ps = conn.prepareStatement(sql);
  		ps.setInt(1, tid);
  		ResultSet rs = ps.executeQuery();
  		if (rs.next()) {
  			return rs.getInt(1);
  		}
  		rs.close();
  	} catch (SQLException se) {
  		if (!SQLHelper.checkGFXDException(conn, se)) return 0; //return 0
  		SQLHelper.handleSQLException(se);
  	}
  	return 0;
  }
	
	protected void getKeysForCidRangeUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, int cid2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		// "update trade.networth set availloan=availloan-? where cid = ? ", 		  
    case 1: 
      //"update trade.networth set securities=? where cid = ?  ",
    case 2:
		  //"update trade.networth set cash=cash-? where cid = ? ",
    	sql = "select cid from trade.networth where cid="+cid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;
    case 3: 
      //"update trade.networth set loanLimit=? where cid>? and cid < ? ", 
      sql = "select cid from trade.networth where cid>"+cid +" and cid<" +cid2;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availCid = rs.getInt(1);
        Log.getLogWriter().info("cid: " + availCid + " exists for update");
      	keys.put(getTableName()+"_"+availCid, txId);
      }          
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void getKeysForTidListUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, BigDecimal securities, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		// "update trade.networth set availloan=availloan-? where cid = ? and tid = ?", 		  
    case 1: 
      //"update trade.networth set securities=? where cid = ? and tid = ? ",
    case 2:
		  //"update trade.networth set cash=cash-? where cid = ? and tid = ? ",
    	sql = "select cid from trade.networth where cid="+cid + " and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;
    case 3: 
      //"update trade.networth set loanLimit=? where securities > ? and tid = ? ", 
      sql = "select cid from trade.networth where securities>"+ securities +" and tid = "+tid;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availCid = rs.getInt(1);
        Log.getLogWriter().info("cid: " + availCid + " exists for update");
      	keys.put(getTableName()+"_"+availCid, txId);
      }          
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
  //actual insert should be called by insert thread for customers table
  public void insert(Connection dConn, Connection gConn, int size, int[] cid) {
    BigDecimal[] cash = new BigDecimal[size];
    int[] loanLimit = new int[size];
    BigDecimal[] availLoan = new BigDecimal[size];
    getDataForInsert(cash, loanLimit, availLoan, size);
    BigDecimal securities = new BigDecimal(Integer.toString(0));
    SQLException derbySe = null;
    SQLException gfxdSe = null;
    
    if (dConn != null) {
	    for (int i=0; i<cid.length; i++) {
	    	//insert to each table
	    	try {
	    		insertToGFXDTable(gConn, cid[i], cash[i], securities, 
	    				loanLimit[i], availLoan[i]);
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		gfxdSe = se;
	    	}
	    	try {
	    		boolean success = insertToDerbyTable(dConn, cid[i], cash[i], 
	    				securities, loanLimit[i], availLoan[i]); 
	        int count = 0;
	        while (!success) {
	          if (count>=maxNumOfTries) {
	            Log.getLogWriter().info("Could not get the lock to finish " +
	            		"insert into derby, abort this operation");
	            gConn.rollback();
	            dConn.rollback();
	            break;
	          }
	          success =  insertToDerbyTable(dConn, cid[i], cash[i], 
	          		securities, loanLimit[i], availLoan[i]);  //retry insert to derby table         
	          
	          count++;
	        }
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		derbySe = se;
	    	}
		    SQLHelper.compareExceptions(derbySe, gfxdSe);
		    derbySe = null;
		    gfxdSe = null;

		    //commit to each table
		    try {
		    	gConn.commit();
		    	Log.getLogWriter().info("gfxd committed the operation");
		    	dConn.commit();
		    	Log.getLogWriter().info("derby committed the operation");
		    } catch (SQLException se) {
		    	SQLHelper.handleSQLException(se);
		    	//not expect any exception during commit time
		    	//gfxd should not have commitConflict as each thread operate on different keys
		    	//gfxd should not have non colocated exception as it commit per insert
		    }
	    }
    }
    else {
	    for (int i=0; i<cid.length; i++) {
	    	try {
	    		insertToGFXDTable(gConn, cid[i], cash[i], securities, 
	    				loanLimit[i], availLoan[i]);
	    	} catch (SQLException se) {
	    		//commit each time, should not got commit conflict exception here
	    		Log.getLogWriter().info("inserting to networth using tx failed");
	    		SQLHelper.handleSQLException(se);
	    	}
	    	
	    	try {
	    		gConn.commit();
	    	} catch (SQLException se) {
	    		SQLHelper.handleSQLException(se);
	    	}
	    }
    } //no verification
  }
  
  protected void insertToGFXDTable (Connection conn, int cid, BigDecimal cash, 
      BigDecimal securities, int loanLimit, BigDecimal availLoan) throws SQLException {
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    count = insertToTable(stmt, cid, cash, securities, loanLimit, availLoan, tid);
    Log.getLogWriter().info("gfxd inserts " + count + " record");
  }
  
  protected boolean insertToDerbyTable (Connection conn, int cid, BigDecimal cash, 
      BigDecimal securities, int loanLimit, BigDecimal availLoan) throws SQLException {
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    try {
      count = insertToTable(stmt, cid, cash, securities, loanLimit, availLoan, tid);
      Log.getLogWriter().info("derby inserts " + count + " record");
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) return false;
      else throw se;
    }    
    return true;
  }
  
  //the insert will be handled in customer insert
  public void insertTx(Connection dConn, Connection gConn, 
  		HashMap<String, Integer> modifiedKeys,
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
  	//no op
  }
  
  
  //used in one of the tx
  @SuppressWarnings("unchecked")
  public void insertTx(Connection dConn, Connection gConn, 
  		HashMap<String, Integer> modifiedKeys,
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList, int cid) {
  	int size = 1;
    BigDecimal[] cash = new BigDecimal[size];
    int[] loanLimit = new int[size];
    BigDecimal[] availLoan = new BigDecimal[size];
    getDataForInsert(cash, loanLimit, availLoan, size);
    BigDecimal securities = new BigDecimal(Integer.toString(0));
    
    try {
    	insertToGFXDTable(gConn, cid, cash[0], securities, loanLimit[0], availLoan[0]);
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
    	sExList.add(se);
    }
    int txId = modifiedKeys.get(thisTxId);
    
    sExList.add(null); //to compare two exception list
    modifiedKeys.put(getTableName()+"_"+cid, txId); 
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = new Object[7];    	
    	data[0] = DMLTxStmtsFactory.TRADE_NETWORTH;
    	data[1] = insertToDerbyTable; //which operation
    	data[2] = cid;
    	data[3] = cash[0];
    	data[4] = securities;
    	data[5] = loanLimit[0];
    	data[6] = availLoan[0];
    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data);
    	SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }
  }
  
  public static String getTableName() {
  	return "networth";
  }
  
  public String getMyTableName() {
  	return getTableName();
  }
  
  protected int getTxColocatedTid() {
  	int tid = -1;
    if (byTidList) {
    	tid = (int)sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    } 
    return tid;
  }
  
  protected int getTxColocatedCid(Connection gConn, int tid, int txId) {
  	int cid = -1;
    if (byTidList) {
    	cid = getCidByList(gConn, tid);
    } else if (byCidRange) {
    	Object txIdCid = SQLBB.getBB().getSharedMap().get(cid_txId + txId);
    	cid = sql.sqlutil.GFXDTxHelper.getColocatedCid(getTableName(), (Integer)txIdCid);
    }
    return cid;
  }
  
  public void performDerbyTxOps(Connection dConn, Object[]data, ArrayList<SQLException> dExList) {
  	boolean success = true;
  	try {
	  	switch ((Integer) data[1]) {
	  	case insertToDerbyTable: 
	  		success = insertToDerbyTable(dConn, (Integer)data[2], (BigDecimal)data[3], 
	  				(BigDecimal) data[4], (Integer) data[5], (BigDecimal) data[6]);
	  			//insertToDerbyTable (Connection conn, int cid, BigDecimal cash, 
	      	//BigDecimal securities, int loanLimit, BigDecimal availLoan)
	  		break;
	  	case updateToDerbyTableCidRangeTx: 
	  		success = updateToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(BigDecimal) data[4], (BigDecimal) data[5], (BigDecimal)data[6], 
	  				(Integer)data[7], (Integer)data[8]);
	  		//updateToDerbyTableCidRangeTx(Connection conn, int cid, int cid2, 
				//BigDecimal availLoanDelta, BigDecimal sec, BigDecimal cashDelta, 
				//int newLoanLimit, int whichUpdate)
	  		break;
	  	case updateToDerbyTableTidListTx: 
	  		success = updateToDerbyTableTidListTx(dConn, (Integer)data[2], (BigDecimal)data[3], 
	  				(BigDecimal) data[4], (BigDecimal) data[5], (Integer)data[6], (Integer)data[7], (Integer)data[8]);
	  		//(Connection conn, int cid, 
				//BigDecimal availLoanDelta, BigDecimal sec, BigDecimal cashDelta, 
				//int newLoanLimit, int tid, int whichUpdate)
	  		break;
	  	case deleteToDerbyTableCidRangeTx: 
	  		success = deleteToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4]);
	  		//(Connection conn, int cid, int cid2,  int whichDelete)
	  		break;
	  	case deleteToDerbyTableTidListTx: 
	  		success = deleteToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (Integer) data[5]);
	  		//(Connection conn, int cid,  
				//int cid2, int whichDelete, int tid)
	  		break;
	  	default:
	  		throw new TestException("wrong derby tx ops");  	
	  	}
  	} catch (SQLException se) {
  		SQLHelper.printSQLException(se);
  		dExList.add(se);
  		return;
  	}
  	if (!success) {
  		throw new TestException ("Only one thread performing derby ops, should not fail");
  	} else {
  		dExList.add(null);
  	}
  }
  
  public static final int insertToDerbyTable = 1;
  public static final int updateToDerbyTableCidRangeTx = 2;
  public static final int updateToDerbyTableTidListTx = 3;
  public static final int deleteToDerbyTableCidRangeTx = 4;
  public static final int deleteToDerbyTableTidListTx = 5;  
}

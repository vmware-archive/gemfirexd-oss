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
import hydra.MasterController;
import hydra.TestConfig;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Set;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.TradeCustomersDMLStmt;
import sql.dmlStatements.TradeNetworthDMLStmt;
import sql.sqlTx.SQLDerbyTxBB;
import sql.sqlutil.DMLTxStmtsFactory;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

public class TradeCustomersDMLTxStmt extends TradeCustomersDMLStmt implements
		DMLTxStmtIF {

	String[] updateByCidRange = {
	    "update trade.customers set cust_name = ? , addr = ? where cid=? ", 
	    "update trade.customers set cust_name = ?,  since= ? where cid=? ",
	    //update multiple records
	    "update trade.customers set addr=? where cid>? and cid <?  " //multiple records
	};
	
	int updateByCidRangeSingleRowStmt = 2;
	
	String[] updateByTidList = {
	    "update trade.customers set addr = ? where cid=? and tid=? ", 
	    "update trade.customers set cust_name = ?, since= ? where cid=? and tid=?",
	    //multiple records
	    "update trade.customers set addr=? where since <? and tid=? ", //multiple records
	    "update trade.customers set since=? where cid>? and cid<? and tid=? " //multiple records
	};
	
	int updateByTidListSingleRowStmt = 2;
	
	String[] deleteByCidRange = {
	    "delete from trade.customers where cid=?", 
	    //delete multi rows in one statement
	    "delete from trade.customers where  cid > ? and cid < ? " //multiple records
	};
	
	int deleteByCidRangeSingleRowStmt = 1;
	
	String[] deleteByTidList = {
			"delete from trade.customers where cid=? and tid=? ", 
			//delete multi rows in one statement
			"delete from trade.customers where (cust_name > ? or cid < ? ) and tid = ?",
	};
	
	int deleteByTidListSingleRowStmt = 1;
	
	@SuppressWarnings("unchecked")
	public void deleteTx(Connection dConn, Connection gConn, 
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
		int custNameLength = 20;
    String cust_name = getRandVarChar(custNameLength);
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int whichDelete = getWhichTxDelete();
    int txId = modifiedKeys.get(thisTxId);
   
    int tid = getTxColocatedTid();  //colocated tid;
    int cid = getTxColocatedCid(gConn, tid, txId);  //colocated Cid  
    int cid2 = cid + cidRangeForTxOp;
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
        	getKeysForTidListDelete(gConn, keys, whichDelete, cid, cust_name, tid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		deleteToGFXDTableTidListTx(gConn, cid, cust_name, whichDelete, tid);
    	}
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      //dConn != null -- otherwise in gfxd only testing, a select query could return the key,
      // but before actually perform the operation within the tx context, 
      // the row could be deleted by another thread
    	if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn != null
    			&& byCidRange) { 
	  		if (!sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid)) {
	  			Log.getLogWriter().info("got expected non colocated exception, continuing testing");
	  			return; //this is like a no op in the tx.
	  		} else {
	  			throw new TestException("got unexpected non colocated exception, keys are colocated\n"
	  					+ TestHelper.getStackTrace(se));
	  		}
	  	} else if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn == null
	  			&& byCidRange) {
    		if (sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid)) {
    			Log.getLogWriter().warning("should not get non colocated exception here");
    		}
  			return; //this is like a no op in the tx.
    	}	else {
	    	opFailed = true; 
	    	//TODO possible 23503 foreign key violation constraint, this check at execution time
	    	//which actually should be checked at commit time, need to modify test once product
	    	//change occurs #41872
	    	sExList.add(se);	
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
    }    
    
    if (!opFailed) {
    	sExList.add(null); //to compare two exception list
    	modifiedKeys.putAll(keys);
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = null;
    	if (byCidRange) {
	    	data = new Object[5];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_CUSTOMERS;
	    	data[1] = deleteToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid;
	    	data[3] = cid2;
	    	data[4] = whichDelete;
    	} else if (byTidList) {
	    	data = new Object[6];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_CUSTOMERS;
	    	data[1] = deleteToDerbyTableTidListTx; //which operation
	    	data[2] = cid;
	    	data[3] = cust_name;
	    	data[4] = whichDelete;
	    	data[5] = tid;    		
    	}
    	    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
        SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }
    
	}

	public void queryTx(Connection dConn, Connection gConn) {
		// TODO Auto-generated method stub

	}
	
	@SuppressWarnings("unchecked")
	public void updateTx(Connection dConn, Connection gConn,
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
		int custNameLength = 20;
		int addrLength = 40;
    String cust_name = getRandVarChar(custNameLength);
    Date since =  getSince();
    String addr = getRandVarChar(addrLength);
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int whichUpdate = getWhichTxUpdate();
    int txId = modifiedKeys.get(thisTxId);
  
    int tid = getTxColocatedTid();  //colocated tid;
    int cid = getTxColocatedCid(gConn, tid, txId);  //colocated Cid
    int cid2 = cid + cidRangeForTxOp;
    
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeUpdate(gConn, keys, whichUpdate, cid, cid2, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		updateToGFXDTableCidRangeTx(gConn, cid, cid2, cust_name, since, addr, whichUpdate);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListUpdate(gConn, keys, whichUpdate, cid, cid2, since, tid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableTidListTx(gConn, cid, cid2, cust_name, since, addr, whichUpdate, tid);
    	}
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      //for partitionByCidRange cases
    	//check colocation violation
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
    	} else if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn == null
    			&& byCidRange) {
    		if (sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid)) {
    			Log.getLogWriter().warning("should not get non colocated exception here");
    		}
  			return; //this is like a no op in the tx.
    	}	else {
    		SQLHelper.handleSQLException(se);  //should not have any other sql exception during operation
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
    }
    
    //range buckets are same as data nodes, each bucket should be on one data node if it is balanced.
    sExList.add(null); //to compare two exception list
    modifiedKeys.putAll(keys);
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = null;
    	if (byCidRange) {
	    	data = new Object[8];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_CUSTOMERS;
	    	data[1] = updateToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid;
	    	data[3] = cid2;
	    	data[4] = cust_name;
	    	data[5] = since;
	    	data[6] = addr;
	    	data[7] = whichUpdate;
    	} else if (byTidList) {
	    	data = new Object[9];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_CUSTOMERS;
	    	data[1] = updateToDerbyTableTidListTx; //which operation
	    	data[2] = cid;
	    	data[3] = cid2;
	    	data[4] = cust_name;
	    	data[5] = since;
	    	data[6] = addr;
	    	data[7] = whichUpdate;
	    	data[8] = tid;    		
    	}
    	    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
        SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }	
	}
	
	protected void updateToGFXDTableCidRangeTx(Connection conn, int cid, int cid2, 
			String cust_name, Date since, String addr, int whichUpdate 
			) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update customers table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    count = updateToTableCidRangeTx(stmt, cid, cid2, cust_name, since, addr, whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableCidRangeTx(Connection conn, int cid, int cid2, 
			String cust_name, Date since, String addr, int whichUpdate) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update customers table in derby, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    try {
    	count = updateToTableCidRangeTx(stmt, cid, cid2, cust_name, since, addr, whichUpdate);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	protected void getKeysForCidRangeUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, int cid2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.customers set cust_name = ? , addr = ? where cid=? ",
    case 1: 
      // "update trade.customers set cust_name = ? since= ? where cid=? ",
    	sql = "select cid from trade.customers where cid="+cid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;
    case 2: 
      // "update trade.customers set addr=? where cid>? and cid <?  "
      sql = "select cid from trade.customers where cid>"+cid 
      	+" and cid<" + cid2;
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
			int whichUpdate, int cid, int cid2, Date since, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
	    //"update trade.customers set addr = ? where cid=? and tid=? ", 
    case 1: 
      // "update trade.customers set cust_name = ? since= ? where cid=? and tid=?",
    	sql = "select cid from trade.customers where cid="+cid +" and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      } 
      rs.close();
      break;
    case 2: 
      // "update trade.customers set addr=? where since <? and tid=? ", //multiple records
      sql = "select cid from trade.customers where since<'"+since 
      	+"' and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availCid = rs.getInt(1);
        Log.getLogWriter().info("cid: " + availCid + " exists for update");
      	keys.put(getTableName()+"_"+availCid, txId);
      }       
      break; 
    case 3:
    	//"update trade.customers set since=? where cid>? and cid<? and tid=? " //multiple records
      sql = "select cid from trade.customers where cid>"+cid 
	    	+" and cid<" + cid2 +" and tid=" +tid;
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
	
	protected void getKeysForCidRangeDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int cid2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //"delete from trade.customers where cid=?", 
    	sql = "select cid from trade.customers where cid="+cid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;
    case 1: 
	    //"delete from trade.customers where  cid > ? and cid < ? " //multiple records
      sql = "select cid from trade.customers where cid>"+cid 
      	+" and cid<" + cid2;
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
	
	protected void getKeysForTidListDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, String cust_name, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //"delete from trade.customers where cid=? and tid=? ", 
    	sql = "select cid from trade.customers where cid="+cid + " and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;
    case 1: 
			//"delete from trade.customers where (cust_name > ? or cid < ? ) and tid = ?",
      sql = "select cid from trade.customers where (cust_name >'"+cust_name 
      	+"' and cid<" + cid + ") and tid = " + tid;
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
	
	protected void updateToGFXDTableTidListTx(Connection conn, int cid, int cid2, 
			String cust_name, Date since, String addr, int whichUpdate, int tid
			) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;    
    Log.getLogWriter().info("update customers table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    count = updateToTableTidListTx(stmt, cid, cid2, cust_name, since, addr, whichUpdate, tid);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableTidListTx(Connection conn, int cid, int cid2, 
			String cust_name, Date since, String addr, int whichUpdate, int tid
			) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update customers table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    try {
      count = updateToTableTidListTx(stmt, cid, cid2, cust_name, since, addr, whichUpdate, tid);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows"); 
    return true;
	}
	
	protected int updateToTableCidRangeTx(PreparedStatement stmt, int cid, int cid2, 
			String cust_name, Date since, String addr, int whichUpdate) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		// "update trade.customers set cust_name = ? , addr = ? where cid=? ",
      Log.getLogWriter().info("updating cust_name to " + cust_name +
      		", addr to " + addr + ", for cid: " + cid);
      stmt.setString(1, cust_name);
      stmt.setString(2, addr);  
      stmt.setInt(3, cid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
      // "update trade.customers set cust_name = ? since= ? where cid=? ",
      Log.getLogWriter().info("updating cust_name to " + cust_name +
      		", since to " + since + ", for cid: " + cid);
      stmt.setString(1, cust_name);
      stmt.setDate(2, since);  
      stmt.setInt(3, cid);
      rowCount = stmt.executeUpdate();              
      break;
    case 2: 
      // "update trade.customers set addr=? where cid>? and cid <?  "
      Log.getLogWriter().info("updating addr to " + addr +
      		", for cid: " + cid + " to " + cid2);
      stmt.setString(1, addr);
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
	
	protected int updateToTableTidListTx(PreparedStatement stmt, int cid, int cid2, 
			String cust_name, Date since, String addr, int whichUpdate, int tid) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.customers set addr = ? where cid=? and tid=? ", 	    
      Log.getLogWriter().info("updating addr to " + addr + ", for cid: " + cid 
      		+ " and tid: " + tid);
      stmt.setString(1, addr);
      stmt.setInt(2, cid);  
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
      // "update trade.customers set cust_name = ? since= ? where cid=? and tid=?",
      Log.getLogWriter().info("updating cust_name to " + cust_name
      		+ ", since to " + since
      		+ ", for cid: " + cid 
      		+ " and tid: " + tid);
      stmt.setString(1, cust_name);
      stmt.setDate(2, since);
      stmt.setInt(3, cid);  
      stmt.setInt(4, tid);
      rowCount = stmt.executeUpdate();               
      break;
    case 2: 
      // "update trade.customers set addr=? where since <? and tid=? ", 
      Log.getLogWriter().info("updating addr to " + addr
      		+ ", for since< " + since 
      		+ " and tid: " + tid);
      stmt.setString(1, addr);
      stmt.setDate(2, since);
      stmt.setInt(3, tid);  
      rowCount = stmt.executeUpdate();       
      break;
    case 3: 
    	//"update trade.customers set since=? where cid>? and cid<? and tid=? " 
      Log.getLogWriter().info("updating since to " + since
      		+ ", for cid> " + cid  + " and cid< " + cid2
      		+ " and tid: " + tid);
      stmt.setDate(1, since);
      stmt.setInt(2, cid);
      stmt.setInt(3, cid2);
      stmt.setInt(4, tid);  
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
	
	protected void deleteToGFXDTableCidRangeTx(Connection conn, int cid, int cid2, 
			 int whichDelete) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete customers table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
    count = deleteToTableCidRangeTx(stmt, cid, cid2, whichDelete);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
	}
	
	protected int deleteToTableCidRangeTx(PreparedStatement stmt, int cid, int cid2, 
			int whichDelete) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
  		// "delete from trade.customers where cid=?", 
      Log.getLogWriter().info("delete from customers for cid: " + cid);
      stmt.setInt(1, cid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
	    //"delete from trade.customers where  cid > ? and cid < ? " //multiple records
      Log.getLogWriter().info("delete customers for cid> " + cid  + " and cid< " + cid2);
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
	
	protected boolean deleteToDerbyTableCidRangeTx(Connection conn, int cid, int cid2, 
			 int whichDelete) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete customers table in derby, myTid is " + tid);
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
	
	protected void deleteToGFXDTableTidListTx(Connection conn, int cid, String cust_name, 
			int whichDelete, int tid) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;    
    Log.getLogWriter().info("delete customers table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    count = deleteToTableTidListTx(stmt, cid, cust_name, whichDelete, tid);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
		
	}
	
	protected boolean deleteToDerbyTableTidListTx(Connection conn, int cid,  
			String cust_name, int whichDelete, int tid) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;
    Log.getLogWriter().info("delete customers table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    try {
    	count = deleteToTableTidListTx(stmt, cid, cust_name,  whichDelete, tid);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby deleted " + count + " rows");
    return true;
	}
	
	protected int deleteToTableTidListTx(PreparedStatement stmt, int cid, String cust_name,
			int whichDelete, int tid) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
			//"delete from trade.customers where cid=? and tid=? ", 
      Log.getLogWriter().info("delete from customers for cid: " + cid + " and tid: " + tid);
      stmt.setInt(1, cid);
      stmt.setInt(2, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
			//"delete from trade.customers where (cust_name > ? or cid < ? ) and tid = ?",
      Log.getLogWriter().info("delete from customers for cust_name>'" + cust_name  + "'and " +
      		"cid< " + cid + " and tid: " + tid);
      stmt.setString(1, cust_name);
      stmt.setInt(2, cid);
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
	
	//insert in tx does not expect exceptions
	public void populate (Connection dConn, Connection gConn, int initSize) {
		insert(dConn, gConn, initSize);
	}
	
  /**
   * The method got specific key from BB and insert a new record
   */
  public void insert(Connection dConn, Connection gConn, int size) {
    int[] cid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];
    boolean success = false;
    getDataForInsert(cid, cust_name,since,addr, size); //get the data
    SQLException derbySe = null;
    SQLException gfxdSe = null;
    if (dConn != null) {	    	    	
	    for (int i=0; i<cid.length; i++) {
	    	//insert to each table
	    	try {
	    		insertToGFXDTable(gConn, cid[i], cust_name[i], since[i], addr[i]); 
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		gfxdSe = se;
	    	}
	    	try {
	    		success = insertToDerbyTable(dConn, cid[i], cust_name[i], since[i], addr[i]); 
	        int count = 0;
	        while (!success) {
	          if (count>=maxNumOfTries) {
	            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
	            gConn.rollback();
	            dConn.rollback();
	            break;
	          }
	          success = insertToDerbyTable(dConn, cid[i], cust_name[i], since[i], addr[i]);  //retry insert to derby table         
	          
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
	    		insertToGFXDTable(gConn, cid[i], cust_name[i], since[i], addr[i]); 
	    		gConn.commit();
	    		Log.getLogWriter().info("gfxd committed the operation");
	    	} catch (SQLException se) {
	    		//commit each insert, so no commit conflict exception should be thrown here
	    		//no constraint check in the table, so commit should not fail
	    		Log.getLogWriter().info("in inserting a single record in customers and op failed");
	    		SQLHelper.handleSQLException(se);	    		
	    	}
      }
    } 
    
    //after bug fixed for existing tests, we may change this to apply for all tests 
    if (hasNetworth) {
      //also insert into networth table
      Log.getLogWriter().info("inserting into networth table");
      TradeNetworthDMLTxStmt networthTx = new TradeNetworthDMLTxStmt();
      networthTx.insert(dConn, gConn, size, cid);
    }    
  }
  
  //for derby insert
  protected boolean insertToDerbyTable(Connection conn, int cid, String cust_name,
      Date since, String addr) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    try {
      count = insertToTable(stmt, cid, cust_name,since, addr, tid); 
      Log.getLogWriter().info("derby inserts " + count + " record");
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry   
      else throw se;
    }
    return true;
  }
  
  //for gemfirexd checking
  protected void insertToGFXDTable(Connection conn, int cid, String cust_name,
      Date since, String addr) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    count = insertToTable(stmt, cid, cust_name,since, addr, tid); 
    Log.getLogWriter().info("gfxd inserts " + count + " record");
  }
	
  //populate the table
  public void populateTx (Connection dConn, Connection gConn, int initSize) {
  	/*
  	if (rand.nextBoolean()) populate(dConn, gConn, initSize);
  	else insertTx(dConn, gConn, initSize);
  	*/
  	//use this for now to avoid colocation error, as each insert commit right away
  	populate(dConn, gConn, initSize);
  }
  
  //insert to both db and commit
  public void insertTx(Connection dConn, Connection gConn, int size) {
    int[] cid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];
    getDataForInsert(cid, cust_name,since,addr, size); //get the data

    insertCustTx(dConn, gConn, cid, cust_name, since, addr);
  }
  
  //used in one of the tx
  @SuppressWarnings("unchecked")
  public void insertTx(Connection dConn, Connection gConn, 
  		HashMap<String, Integer> modifiedKeys,
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
  	int size = 1;
    int[] cid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];
    getDataForInsert(cid, cust_name,since,addr, size); //get the data
    int txId = modifiedKeys.get(thisTxId);
    
    try {
    	insertToGFXDTable(gConn, cid[0], cust_name[0], since[0], addr[0]);
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
    	}	else {
    		SQLHelper.handleSQLException(se);
    	}
    }  
    
    //to access following, it means no SQLException has been thrown 
    //after inserting the record
    
    
    //this is first operation, all other operations need to be colocate with this cid now
    //modifiedKeys has one entry for txId already
    if (modifiedKeys.size() == 1) {
    	SQLBB.getBB().getSharedMap().put("cid_txId_"+txId, cid[0]);
    }
    

    //check if it should throw non colocated conflict exception
    //dConn != null -- otherwise in gfxd only testing, a select query could return the key,
    // but before actually perform the operation within the tx context, 
    // the row could be deleted by another thread
    else if (modifiedKeys.size() > 1 && byCidRange && dConn != null
    		&& !sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid[0])) {
    	Set<String> keys = modifiedKeys.keySet();
    	StringBuffer str = new StringBuffer();
    	str.append("The inserted cid is " + cid[0]);
    	str.append(", it is not colocated with following cid in this tx: \n");
    	for (String key: keys) {
    		if (!key.equalsIgnoreCase(thisTxId)) {
    			str.append(key +",\t");
    		}
    	}
    	throw new TestException ("Did not get the expected non colocated exception\n" + str.toString());
    }
    //range buckets are same as data nodes, each bucket should be on one data node if it is balanced.

    
    //put the keys that were modified in this operation
    sExList.add(null); //to compare two exception list
    modifiedKeys.put(getTableName()+"_"+cid[0], txId);
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = new Object[6];    	
    	data[0] = DMLTxStmtsFactory.TRADE_CUSTOMERS;
    	data[1] = insertToDerbyTable; //which operation
    	data[2] = cid[0];
    	data[3] = cust_name[0];
    	data[4] = since[0];
    	data[5] = addr[0];
    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data);
    	SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }    
    
    //insert to networth table as well
    TradeNetworthDMLTxStmt networthTx = new  TradeNetworthDMLTxStmt();
    networthTx.insertTx(dConn, gConn, modifiedKeys, dExList, sExList, cid[0]);
  }

  private void insertCustTx(Connection dConn, Connection gConn, int[] cid, String[] cust_name, 
  		Date[] since, String[] addr) {
    List<SQLException> derbyExList = new ArrayList<SQLException>();
    List<SQLException> gfxdExList = new ArrayList<SQLException>();
    boolean success = false;
  	int size = cid.length;
    //boolean executeBatch = rand.nextBoolean();
    boolean executeBatch = false;    

    if (dConn != null) {
    	try {
    	  //insert to gfe table 
	      if (executeBatch)
	    	  insertBatchToGFETableTx(gConn, cid, cust_name,since, addr, size, gfxdExList);
	    	else
	        insertToGFETableTx(gConn, cid, cust_name,since, addr, size, gfxdExList);    
	      
	      //insert to derby table
	    	if (executeBatch)
	    	  success = insertBatchToDerbyTableTx(dConn, cid, cust_name,since, addr, size, derbyExList);
	    	else
	        success = insertToDerbyTableTx(dConn, cid, cust_name,since, addr, size, derbyExList);  //insert to derby table 
        int count = 0;
        while (!success) {
          if (count>=maxNumOfTries) {
            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
            dConn.rollback();
            gConn.rollback();           
            return;
          }
          derbyExList.clear();
          if (executeBatch)
          	success = insertBatchToDerbyTable(dConn, cid, cust_name,since, addr, size, derbyExList);
          else
            success = insertToDerbyTable(dConn, cid, cust_name,since, addr, size, derbyExList);  //retry insert to derby table         
          
          count++;
        }
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        throw new TestException ("insert to trade.customers fails\n" + TestHelper.getStackTrace(se));
      }  //for verification
      
      //handle two exceptionList, so that only additional derby SQLExceptions are left
      //if gfxd got conflict commit exception, it should have corresponding SQLException such as primary key existing exception
      ArrayList<SQLException>gfxdExListCopy = new ArrayList<SQLException>(gfxdExList);
      SQLHelper.removeSameExceptions(gfxdExListCopy, derbyExList);
      SQLHelper.removeSameExceptions(derbyExList, gfxdExList);
      
      //print out the additional gfxd and derby exceptions
      if (!gfxdExListCopy.isEmpty()) {
      	Log.getLogWriter().warning("gfxd has following additional exceptions");
      	for (SQLException gfxd: gfxdExListCopy)
      		SQLHelper.printSQLException(gfxd);
      }
                
	    //serialize commit for commit. only one tranaction will succeed if there is a conflict
      boolean expectFailure = false;
      
      getLock(); //get the lock      
	    try {
	    	Log.getLogWriter().info("committing gfxd inserts");
	    	gConn.commit(); //to commit and avoid rollback the successful operations
	    } catch (SQLException se) {
	    	SQLHelper.printSQLException(se);
	    	if (se.getSQLState().equalsIgnoreCase("X0Z02")) {
	    		expectFailure = true;
	    		if (derbyExList.isEmpty()) 
	    			throw new TestException("Got commit conflict exception, but derby does not have any exception");
	    		else {
	    			//if (gConn != null) Log.getLogWriter().warning("gfxd connection is avail");
	    			SQLException derbySe = (SQLException) derbyExList.get(0);
	    			if (derbySe.getSQLState().equalsIgnoreCase("23505")) {
	    				try {	    					
	    					dConn.rollback();
	    				} catch (SQLException dse) {
	    		    	SQLHelper.handleSQLException(dse);
	    		    }
	    				derbyExList.remove(0);
	    			} else {
	    				SQLHelper.printSQLException(derbySe);
	    				throw new TestException("Got commit conflict exception, but derby does not get the expected exception");
	    			}
	    		}
	    	}
	    }
	    if (!expectFailure){
		    try {
		    	Log.getLogWriter().info("committing derby inserts");
		    	dConn.commit();
		    } catch (SQLException se) {
		    	SQLHelper.handleSQLException(se);
		    }
	    }
	    
	    //release the lock
	    releaseLock();

	    if (!derbyExList.isEmpty()) {
	    	Log.getLogWriter().warning("derby has following additional exceptions");
	    	for (SQLException derby: derbyExList) 
	    		SQLHelper.printSQLException(derby);
	    }
    }
    else {
      try {
        insertToGFETableTx(gConn, cid, cust_name,since, addr, size);    //insert to gfe table 
        gConn.commit();
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        throw new TestException ("insert to gfe trade.customers fails\n" + TestHelper.getStackTrace(se));
      }
    } //no verification  
    
    //after bug fixed for existing tests, we may change this to apply for all tests 
    if (hasNetworth) {
      //also insert into networth table
      Log.getLogWriter().info("inserting into networth table");
      TradeNetworthDMLStmt networth = new TradeNetworthDMLStmt();
      networth.insertTx(dConn, gConn, size, cid);
      /* should be handled in the insertTx in Networth table
      try {
        if (dConn!=null)        dConn.commit(); //to commit and avoid rollback the successful operations
        gConn.commit();
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
      */
    } 
  }
  
  //for derby checking
  protected boolean insertToDerbyTableTx(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, int size, List<SQLException> exceptions) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    for (int i=0 ; i<size ; i++) { 
      try {
        count = insertToTable(stmt, cid[i], cust_name[i],since[i], addr[i], tid); 
        if (count != ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue()) {
          Log.getLogWriter().info("Derby insert has different row count from that of gfxd, " +
            "gfxd inserted " + ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue() +
            " but derby inserted " + count);            
        }
      } catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry
        else {
        	SQLHelper.handleDerbyTxSQLException(se, exceptions);      
        }
      }
    }
    return true;
  }
  
  protected boolean insertBatchToDerbyTableTx(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, int size, List<SQLException> exceptions) throws SQLException {
  	//TODO need to implement it
  	return true;
  }
  
  //for gemfirexd checking
  protected void insertToGFETableTx(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, int size, List<SQLException> exceptions) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    for (int i=0 ; i<size ; i++) {
      try {
      	verifyRowCount.put(tid+"_insert"+i, 0);
        count = insertToTable(stmt, cid[i], cust_name[i],since[i], addr[i], tid); 
        //Log.getLogWriter().info("count is " + count);
        verifyRowCount.put(tid+"_insert"+i, new Integer(count));
        Log.getLogWriter().info("gfxd inserts " + verifyRowCount.get(tid+"_insert"+i) + " rows");
      } catch (SQLException se) {
        SQLHelper.handleGFETxSQLException(se, exceptions);      
      }
    }
  }  
  
  protected void insertBatchToGFETableTx(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, int size, List exceptions) throws SQLException{
  	//TODO need to be implemented
  	Log.getLogWriter().info("Needs to be implemented");
  }
  
  //for gemfirexd
  protected void insertToGFETableTx(Connection conn, int[] cid, String[] cust_name,
      Date[] since, String[] addr, int size) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    for (int i=0 ; i<size ; i++) {
      try {
        insertToTable(stmt, cid[i], cust_name[i],since[i], addr[i], tid); 
      } catch (SQLException se) {
      	//TODO to consider expected exceptions here
        SQLHelper.handleSQLException(se);      
      }
    }
  }
  
  public void populateTx(Connection dConn, Connection gConn){
    int size = TestConfig.tab().intAt(SQLPrms.initCustomersSizePerThread, 100);
    populateTx (dConn, gConn, size); //populate customers and networth
  }
  
  //To test only one thread succeeds
  public void insertCustomersTxConflict(Connection dConn, Connection gConn, int size) {  
  	size = 3;
    int[] cid = new int[size];
    String[] cust_name = new String[size];
    Date[] since = new Date[size];
    String[] addr = new String[size];
    List<SQLException> derbyExList = new ArrayList<SQLException>();
    List<SQLException> gfxdExList = new ArrayList<SQLException>();
    boolean success = false;
    getDataForInsert(cid, cust_name,since,addr, size); //get the data

    //add conflict in Tx run
    if (getMyTid()%3 ==0) {
    	cid[0] = 701;
    	cust_name[0] = "newName1";
    	cid[1] = 702;
    	cust_name[1] = "newName1";
    	cid[2] = 703;
    	cust_name[2] = "newName1";
    } 
    
    //add conflict in Tx run
    if (getMyTid()%3 ==1) {
    	cid[0] = 9999;
    	cust_name[0] = "newName1";
    	cid[1] = 9998;
    	cust_name[1] = "newName1";
    	cid[2] = 9997;
    	cust_name[2] = "newName1";
    } 
    
    insertCustTx(dConn, gConn, cid, cust_name, since, addr);        
  }
  
  //this could only be executed once in a test.
  public void updateCustomersTxConflict(Connection dConn, Connection gConn, int size) {
  	if (dConn == null) return; 
  	boolean firstSql = rand.nextBoolean();
  	String updateSql = (firstSql) ? 
  			"update trade.customers set cust_name =?  where cid=1" :
  			"update trade.customers set addr =?  where cid=1";
  	int maxVarCharLength = 10;
  	String field = getRandVarChar(maxVarCharLength)+"update"; 
  	
  	PreparedStatement dStmt = null;
  	PreparedStatement gStmt = null;
  	ArrayList partitionKeys = (ArrayList)partitionMap.get("customersPartition");
  	Log.getLogWriter().info("update statement is " + updateSql);
  	
  	if (dConn != null) {
      if (firstSql && 	//test does not partitioned on addr in partition clause
      		SQLTest.testPartitionBy && (partitionKeys.contains("cust_name")))  dStmt = null;  
      else dStmt = getStmt(dConn, updateSql); 
  	}
  	if (firstSql && 	//test does not partitioned on addr in partition clause
    		SQLTest.testPartitionBy && (partitionKeys.contains("cust_name"))) 
  				gStmt = getUnsupportedStmt(gConn, updateSql);
  	else gStmt = getStmt(gConn, updateSql);
  	
  	if (gStmt == null) return; //could not perform update
  	int count = 0;
  	try {
  		gStmt.setString(1, field);
  		count = gStmt.executeUpdate();	  			  	
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	}
  	Log.getLogWriter().info("wait for 5 seconds in updateCustTxInitTask, " +
			"so that only one update operation will succeed");
  	MasterController.sleepForMs(5000);
  	
  	try {
  		dStmt.setString(1, field);
  		count = dStmt.executeUpdate();	  			  	
		} catch (SQLException se) {
			if (!SQLHelper.checkDerbyException(dConn, se)) { //handles the lock timeout
        Log.getLogWriter().info("lock timeout, rollback the operations");
        try {
        	dConn.rollback();
        	gConn.rollback();
        	return;
        } catch (SQLException e) {
    			SQLHelper.handleSQLException(e);
    		}
  	  } 
		}
		
		boolean expectFailure=false;
		
		getLock();
		try {
			gConn.commit();
		} catch (SQLException se) {
			SQLHelper.printSQLException(se);  
			if (se.getSQLState().equalsIgnoreCase("X0Z02")) {
				expectFailure = true;
			}
  	}
		
		try {
			if (expectFailure){
				 dConn.rollback();
				 if (SQLBB.getBB().getSharedMap().get("custUpdateTxInitTask") != null) ;
				 else {
					 Log.getLogWriter().warning("update failed, but no successful update yet, wait for 1 sec");					 
					 MasterController.sleepForMs(1000);
					 if (SQLBB.getBB().getSharedMap().get("custUpdateTxInitTask") == null) 
						 throw new TestException ("after wait for 1 sec, " +
						 		"update failed, but there is no successful update");
				 }
			}
			else {
				dConn.commit();
				if (SQLBB.getBB().getSharedMap().get("custUpdateTxInitTask") == null) 
					SQLBB.getBB().getSharedMap().put("custUpdateTxInitTask", true);
				
				else
					throw new TestException("in custUpdateTxInitTask, " +
							"more than one update on same record are successfuly"); 
							
			}
		} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	}
			
		releaseLock();
  	
  }
  
//this could only be executed once in a test.
  public void updateCustomersTxScatter(Connection dConn, Connection gConn) {
  	String name=null;
  	String name2= null;
  	String updateScatter1 =  "update trade.customers set cust_name = 'newName1' " +
  			" where cust_name='"; 
  	String updateScatter2 =  "update trade.customers set cust_name = 'newName2' " +
		" where cust_name='"; 
  	
  	String updateScatter3 =  "update trade.customers set cust_name = 'newName3' " +
		" where cust_name='newName1'"; 
  	String select = "select * from trade.customers";
  	String selectName =  "select cust_name from trade.customers";
  	try { 		
  		ResultSet rsName = gConn.createStatement().executeQuery(selectName);
  		if (rsName.next()) {
  			name=rsName.getString(1);
  			Log.getLogWriter().info("name is " + name);
  		}
  		if (rsName.next()) {
  			name2=rsName.getString(1);
  			Log.getLogWriter().info("name2 is " + name2);
  		}
  		  		
  		gConn.createStatement().executeUpdate(updateScatter1 + name + "'");
  		Log.getLogWriter().info("execute update statement: " + updateScatter1 + name + "'");
  		
  		ResultSet rs = gConn.createStatement().executeQuery(select);
  		String result = sql.sqlutil.ResultSetHelper.listToString(ResultSetHelper.asList(rs, false));
  		Log.getLogWriter().info("results are : " + result);

  		gConn.createStatement().executeUpdate(updateScatter2 + name2 + "'");
  		Log.getLogWriter().info("execute update statement: " + updateScatter2 + name2 + "'");
  		
  		ResultSet rs2 = gConn.createStatement().executeQuery(select);
  		String result2 = sql.sqlutil.ResultSetHelper.listToString(ResultSetHelper.asList(rs2, false));
  		Log.getLogWriter().info("results after second update within tx are : " + result2);
  		
  		gConn.createStatement().executeUpdate(updateScatter3);
  		Log.getLogWriter().info("execute update statement: " + updateScatter3);
  		
  		ResultSet rs3 = gConn.createStatement().executeQuery(select);
  		String result3 = sql.sqlutil.ResultSetHelper.listToString(ResultSetHelper.asList(rs3, false));
  		Log.getLogWriter().info("results after third update within tx are : " + result3);
  		 		
  		gConn.commit(); 		
  		
  		ResultSet rs4 = gConn.createStatement().executeQuery(select);
  		String result4 = sql.sqlutil.ResultSetHelper.listToString(ResultSetHelper.asList(rs4, false));
  		Log.getLogWriter().info("results after commit are : " + result4);
  		
  	} catch (SQLException se) {
  		//this operation should fail once the tx for bulk update work is done
  		//TODO need to catch the colocated exception
  		SQLHelper.handleSQLException(se);
  	}
  }
  
  public static String getTableName() {
  	return "customers";
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
  
  //find  a cid which was put by this tid
  protected int getCidByList(Connection conn, int tid) {
  	String sql = "select * from trade.customers where tid = ?";
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
  	return 0; //will not execute
  }
  
  public void performDerbyTxOps(Connection dConn, Object[]data, ArrayList<SQLException> dExList) {
  	boolean success = true;
  	try {
	  	switch ((Integer) data[1]) {
	  	case insertToDerbyTable: 
	  		success = insertToDerbyTable(dConn, (Integer)data[2], (String)data[3], 
	  				(Date) data[4], (String) data[5]);
	  			//insertToDerbyTable(dConn, cid[0], cust_name[0], since[0], addr[0]);
	  		break;
	  	case updateToDerbyTableCidRangeTx: 
	  		success = updateToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(String) data[4], (Date) data[5], (String)data[6], (Integer)data[7]);
	  		//updateToDerbyTableCidRangeTx(Connection conn, int cid, int cid2, 
				//String cust_name, Date since, String addr, int whichUpdate)
	  		break;
	  	case updateToDerbyTableTidListTx: 
	  		success = updateToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(String) data[4], (Date) data[5], (String)data[6], (Integer)data[7], (Integer)data[8]);
	  		//updateToDerbyTableTidListTx(Connection conn, int cid, int cid2, 
				//String cust_name, Date since, String addr, int whichUpdate, int tid)
	  		break;
	  	case deleteToDerbyTableCidRangeTx: 
	  		success = deleteToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4]);
	  		//(Connection conn, int cid, int cid2,  int whichDelete)
	  		break;
	  	case deleteToDerbyTableTidListTx: 
	  		success = deleteToDerbyTableTidListTx(dConn, (Integer)data[2], (String)data[3], 
	  				(Integer) data[4], (Integer) data[5]);
	  		//(Connection conn, int cid,  
				//String cust_name, int whichDelete, int tid)
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

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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import sql.SQLBB;
import sql.SQLHelper;
import sql.dmlStatements.TradeBuyOrdersDMLStmt;
import sql.sqlTx.SQLDerbyTxBB;
import sql.sqlutil.DMLTxStmtsFactory;
import sql.sqlutil.ResultSetHelper;
import com.gemstone.gemfire.cache.query.Struct;
import sql.sqlutil.GFXDTxHelper;
import util.TestException;
import util.TestHelper;

public class TradeBuyOrdersDMLTxStmt extends TradeBuyOrdersDMLStmt implements
		DMLTxStmtIF {
  /*
   * trade.orders table fields
   *   int oid;
   *   int cid; //ref customers
   *   int sid; //ref securities sec_id
   *   int qty;
   *   BigDecimal bid;
   *   long ordertime;
   *   String stauts;
   *   int tid; //for update or delete unique records to the thread
   */
  
	String[] updateByCidRange = {	
	    "update trade.buyorders set bid = ? where cid = ? and oid= ? and status = 'open' ",
	    "update trade.buyorders set sid = ? , qty=? where cid = ? and oid= ? and bid <? and status = 'open'  ",
	    //multi rows
	    "update trade.buyorders set status = 'filled'  where cid>? and cid<? and sid = ? and bid<? and status = 'open' ",  
	    "update trade.buyorders set status = 'cancelled' where cid>? and cid<? and ordertime <? and status = 'open'  ", 
	};
	
	int updateByCidRangeSingleRowStmt = 2;
	
	String[] updateByTidList = {
	    "update trade.buyorders set bid = ? where oid= ? and status = 'open' and tid=? ",
	    "update trade.buyorders set cid = ? , qty=? where oid= ? and bid <? and status = 'open' and tid =? ",
	    //multi rows
	    "update trade.buyorders set status = 'filled'  where sid = ? and bid<? and status = 'open' and (tid =? or tid=? )",  
	    "update trade.buyorders set status = 'cancelled' where ordertime <? and status = 'open' and tid =? ", 
	};
	
	int updateByTidListSingleRowStmt = 2;
	
	String[] deleteByCidRange = {
	    "delete from trade.buyorders where cid=? and oid=? and bid <?",
	    //multiple rows
	    "delete from trade.buyorders where cid>? and cid<? and status IN ('cancelled', 'filled')",    
	};
	
	int deleteByCidRangeSingleRowStmt = 1;
	
	String[] deleteByTidList = {
	    "delete from trade.buyorders where oid=? and tid=? and bid <?",
	    //multiple rows
	    "delete from trade.buyorders where oid>? and oid<? and status IN ('cancelled', 'filled') and tid=? ", 
	};
	
	int deleteByTidListSingleRowStmt = 1;  
   
	@SuppressWarnings("unchecked")
	public void deleteTx(Connection dConn, Connection gConn,
			HashMap<String, Integer> modifiedKeys, ArrayList<SQLException> dExList,
			ArrayList<SQLException> sExList) {
		int size =1;
		int[] cid = new int[size];
		int[] tid = new int[size];
		HashMap<String, Integer> keys = new HashMap<String, Integer>();
		int txId = modifiedKeys.get(thisTxId);
    int whichDelete = getWhichTxDelete();
    BigDecimal bid = new BigDecimal (Double.toString((rand.nextInt(10000)+1) * .01));
    
		getCidAndTid(gConn, txId, cid, tid);
		int cid2 = cid[0] + cidRangeForTxOp;
    int oid=0;
    if (byTidList) {
    	oid = getOidByTid(gConn, tid[0]);
    }
    else if (byCidRange) {
    	oid = getOidByCid(gConn, cid[0]);
    }
		int oid1 = oid;
		int oid2 = oid1 + 20;

    boolean opFailed = false;
    
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeDelete(gConn, keys, whichDelete, cid[0], cid2, oid, bid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		deleteToGFXDTableCidRangeTx(gConn, cid[0], cid2, oid, bid, whichDelete);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListDelete(gConn, keys, whichDelete, oid1, oid2, oid, bid, tid[0], txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		deleteToGFXDTableTidListTx(gConn, oid1, oid2, oid, bid, whichDelete, tid[0]);
    	}
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      //dConn != null -- otherwise in gfxd only testing, a select query could return the key,
      // but before actually perform the operation within the tx context, 
      // the row could be deleted by another thread
    	if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn != null
    			&& byCidRange) { 
	  		if (!sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid[0])) {
	  			Log.getLogWriter().info("got expected non colocated exception, continuing testing");
	  			return; //this is like a no op in the tx.
	  		} else {
	  			throw new TestException("got unexpected non colocated exception, keys are colocated\n"
	  					+ TestHelper.getStackTrace(se));
	  		}
    	} else {
    		opFailed = true;
    		sExList.add(se);	//could got fk reference check violation
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
	    	data = new Object[7];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_BUYORDERS;
	    	data[1] = deleteToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid[0];
	    	data[3] = cid2;
	    	data[4] = oid;
	    	data[5] = bid;
	    	data[6] = whichDelete;
    	} else if (byTidList) {
	    	data = new Object[8];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_BUYORDERS;
	    	data[1] = deleteToDerbyTableTidListTx; //which operation
	    	data[2] = oid1;
	    	data[3] = oid2;
	    	data[4] = oid;
	    	data[5] = bid;
	    	data[6] = whichDelete;
	    	data[7] = tid[0];    		
    	}
    	    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
      SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }
	}

	protected void getKeysForCidRangeDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int cid2, int oid, BigDecimal bid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.buyorders where cid=? and oid=? and bid <?",
    	sql = "select oid from trade.buyorders where cid="+cid + " and oid=" + oid 
    		+ " and bid <" + bid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("oid: " + oid +" exists for update");
      	keys.put(getTableName()+"_"+oid, txId);
      }    
      rs.close();
      break;	    	    
    case 1: 
      //"delete from trade.buyorders where cid>? and cid<? and status IN ('cancelled', 'filled')", 
      sql = "select oid from trade.buyorders where cid>"+cid 
    		+" and cid<" +cid2 + " and status IN ('cancelled', 'filled')";
	    rs = conn.createStatement().executeQuery(sql); 
	    while (rs.next()) {
	    	int availOid = rs.getInt("OID");
	      Log.getLogWriter().info("oid: " + availOid + " exists for update");
	    	keys.put(getTableName()+"_"+availOid, txId);
	    }
	    rs.close();
	    break;
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}		
	
	protected void getKeysForTidListDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int oid1, int oid2, int oid, BigDecimal bid, int tid, 
			int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.buyorders where oid=? and tid=? and bid <?",	   
    	sql = "select oid from trade.buyorders where oid = " + oid 
    		+ " and tid=" + tid + " and bid<" + bid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("oid: " + oid +" exists for update");
      	keys.put(getTableName()+"_"+oid, txId);
      }    
      rs.close();
      break;	    	    
    case 1: 
      // "delete from trade.buyorders where oid>? and oid<? and status IN ('cancelled', 'filled') and tid=? ", 
      sql = "select oid from trade.buyorders where oid>"+oid1 
    		+" and oid<" +oid2 + " and status IN ('cancelled', 'filled') and tid=" + tid ;
	    rs = conn.createStatement().executeQuery(sql); 
	    while (rs.next()) {
	    	int availOid = rs.getInt("OID");
	      Log.getLogWriter().info("oid: " + availOid + " exists for update");
	    	keys.put(getTableName()+"_"+availOid, txId);
	    }
	    rs.close();
	    break;
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}	
	
	protected void deleteToGFXDTableCidRangeTx(Connection conn,  int cid, int cid2, 
			int oid, BigDecimal bid, int whichDelete) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete buyorders table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
    count = deleteToTableCidRangeTx(stmt, cid, cid2, oid, bid, whichDelete);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
	}
	
	protected boolean deleteToDerbyTableCidRangeTx(Connection conn,  int cid, int cid2, 
			int oid, BigDecimal bid, int whichDelete) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete buyorders table in derby, myTid is " + tid);
    Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
    try {
    	count = deleteToTableCidRangeTx(stmt, cid, cid2, oid, bid, whichDelete);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby deleted " + count + " rows");
    return true;
	}
	
	protected int deleteToTableCidRangeTx(PreparedStatement stmt, int cid, int cid2, 
			int oid, BigDecimal bid, int whichDelete) throws SQLException { 
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.buyorders where cid=? and oid=? and bid <?",
      Log.getLogWriter().info("deleting from buyorders table for cid: " + cid 
      		+ " and oid: " + oid + " and bid<" + bid);
      stmt.setInt(1, cid);
      stmt.setInt(2, oid);
      stmt.setBigDecimal(3, bid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      // "delete from trade.buyorders where cid>? and cid<? and status IN ('cancelled', 'filled')",  
      Log.getLogWriter().info("deleting from buyorders table for cid> "+ cid + " to cid< " + cid2 + 
      		" and status IN ('cancelled', 'filled')");
      stmt.setInt(1, cid);
      stmt.setInt(2, cid2);
      rowCount =  stmt.executeUpdate();
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
	
	protected void deleteToGFXDTableTidListTx(Connection conn, int oid1, int oid2, 
			int oid, BigDecimal bid, int whichDelete, int tid) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;    
    Log.getLogWriter().info("delete buyorders table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    count = deleteToTableTidListTx(stmt, oid1, oid2, oid, bid, whichDelete, tid);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 		
	}
	
	protected boolean deleteToDerbyTableTidListTx(Connection conn, int oid1,  
			int oid2, int oid, BigDecimal bid, int whichDelete, int tid) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;
    Log.getLogWriter().info("delete buyorders table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    try {
    	count = deleteToTableTidListTx(stmt, oid1, oid2, oid, bid, whichDelete, tid);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby deleted " + count + " rows");
    return true;
	}
	
	protected int deleteToTableTidListTx(PreparedStatement stmt, int oid1, int oid2,
			int oid, BigDecimal bid, int whichDelete, int tid) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
			//	    "delete from trade.buyorders where oid=? and tid=? and bid <?",
      Log.getLogWriter().info("deleting from buyorders for oid: " + oid 
      		+ " and tid: " + tid + " and bid<" + bid);
      stmt.setInt(1, oid);
      stmt.setInt(2, tid);
      stmt.setBigDecimal(3, bid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
			//"delete from trade.buyorders where oid>? and oid<? and status IN ('cancelled', 'filled') and tid=? ", 
      Log.getLogWriter().info("deleting from buyorders for oid > " + oid1 
      		+ " oid < " + oid2 + " and status IN ('cancelled', 'filled') and tid: " + tid);
      stmt.setInt(1, oid1);
      stmt.setInt(2, oid2);
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
	
	public String getMyTableName() {
		return getTableName();
	}
	
  public static String getTableName() {
  	return "buyorders";
  }

  @SuppressWarnings("unchecked")
	public void insertTx(Connection dConn, Connection gConn,
			HashMap<String, Integer> modifiedKeys, ArrayList<SQLException> dExList,
			ArrayList<SQLException> sExList) {
		int size = 1;
    int[] oid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    int[] cid = new int[size];
    int[] tid = new int[size];
    String status = "open";
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] bid = new BigDecimal[size];
    
    int txId = modifiedKeys.get(thisTxId);
    getCidAndTid(gConn, txId, cid, tid);
    getDataForInsert(gConn, oid, cid, sid, qty, time, bid, size); //get the data   
    boolean opFailed = false;
    
    try {
    	insertToGFXDTable(gConn, oid[0], cid[0], sid[0], qty[0], status, time[0], bid [0]);
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
    	if (se.getSQLState().equalsIgnoreCase("X0Z04")) { 
    			throw new TestException("got unexpected non colocated exception, keys are colocated"
    					+ TestHelper.getStackTrace(se));
    	}
    	else {
    		opFailed = true;
    		sExList.add(se);  //primary key violation only - foreign key check at commit time
    	}
    } 

    if (!opFailed) {
    	sExList.add(null); //to compare two exception list
    	modifiedKeys.put(getTableName()+"_"+oid[0], txId);
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = new Object[9];    	
    	data[0] = DMLTxStmtsFactory.TRADE_BUYORDERS;
    	data[1] = insertToDerbyTable; //which operation
    	data[2] = oid[0];
    	data[3] = cid[0];
    	data[4] = sid[0];
    	data[5] = qty[0];
    	data[6] = status;
    	data[7] = time[0];
    	data[8] = bid[0];
    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data);
    	SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    } 
	}
	
  protected void insertToGFXDTable(Connection conn, int oid, int cid, int sid, int qty,
      String status, Timestamp time, BigDecimal bid) throws SQLException{
	  PreparedStatement stmt = getStmt(conn, insert);
	  int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    count = insertToTable(stmt, oid, cid, sid, qty, status, time, bid, tid);
    Log.getLogWriter().info("gfxd inserts " + count + " record");
  }
  
  //for derby insert
  protected boolean insertToDerbyTable(Connection conn, int oid, int cid, int sid, int qty,
  String status, Timestamp time, BigDecimal bid) throws SQLException{
  	PreparedStatement stmt = getStmt(conn, insert);
  	int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    try {
    	count = insertToTable(stmt, oid, cid, sid, qty, status, time, bid, tid);
      Log.getLogWriter().info("derby inserts " + count + " record");
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry   
      else throw se;
    }
    return true;
  }
	
	protected void getCidAndTid(Connection gConn, int txId, int[] cid, int[] tid) {
    if (byTidList) {
    	tid[0] = sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    	cid[0] = getCid(); //partition by list, any cid will be OK
    }
    if (byCidRange) {
    	Object txIdCid = SQLBB.getBB().getSharedMap().get(cid_txId + txId);
    	cid[0] = sql.sqlutil.GFXDTxHelper.getColocatedCid(getTableName(), (Integer)txIdCid);
    	tid[0] = getMyTid();
    } 
	}

	public void populateTx(Connection dConn, Connection gConn) {
		int size = 20;
    populateTx (dConn, gConn, size); 
	}
	
  //populate the table
  public void populateTx (Connection dConn, Connection gConn, int initSize) {
  	insert(dConn, gConn, initSize);
  }
  
  public void insert(Connection dConn, Connection gConn, int size) {
    for (int i =0; i<size; i++) {
    	int randSize = rand.nextInt(size) +1;
    	insertRows (dConn, gConn, randSize, getCid());
    }   
  }
  
  public void insertRows(Connection dConn, Connection gConn, int size, int cid) {
    int[] oid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    String status = "open";
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] bid = new BigDecimal[size];

    boolean success = false;
    getDataForInsert(gConn, oid, cid, sid, qty, time, bid, size); //get the data
    SQLException derbySe = null;
    SQLException gfxdSe = null;
    
    
    if (dConn != null) {	
    	try {
    		insertBatchToGFXDTable(gConn, oid, cid, sid, qty, status, time, bid);
    	} catch (SQLException se) {
    		SQLHelper.printSQLException(se);
    		gfxdSe = se; 
    	}	 	   	
    	try {
    		success = insertBatchToDerbyTable(dConn, oid, cid, sid, qty, status, time, bid); 
        int count = 0;
        while (!success) {
          if (count>=maxNumOfTries) {
            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
            throw new TestException("Could not get the lock to finish insert into derby");
          }
          success = insertBatchToDerbyTable(dConn, oid, cid, sid, qty, status, time, bid);  //retry insert to derby table         
          
          count++;
        }     
    	} catch (SQLException se) {
    		SQLHelper.printSQLException(se);
    		derbySe = se;
    	}
	    //commit to each table
	    try {
		    try {
		    	Log.getLogWriter().info("gfxd committing the operation");
		    	gConn.commit();
		    	Log.getLogWriter().info("gfxd committed the operation");
		    } catch (SQLException se) {
		    	SQLHelper.printSQLException(se);
		    	if (se.getSQLState().equals("X0Z05")) {
		    	  //fk check violation is possible here due to random sid
		    		//currently product throw in doubt transaction, which should be changed after #41898 is fixed
				    if (derbySe !=null  && SQLHelper.isSameRootSQLException(derbySe.getSQLState(), se)) {
				      if (derbySe.getSQLState().equals("23503")) {
				    		Log.getLogWriter().warning("got foreign key check violation and masked as" +
		    				" in doubt transaction exception -- which is bug# 41898 ");
				    		gConn.rollback(); //may not nessary as commit fail should make the tx fail
				    		dConn.rollback();
				    		return; //this operation failed
				      }
				    } 
		    	} else {
		    		SQLHelper.handleSQLException(se);
		    	}
		    	//gfxd should not have non colocated exception as the batch are by same cid or same tid
		    } 			    
	    }	catch (SQLException se) {
	    	SQLHelper.handleSQLException(se); //for dConn.rollback()
	    }
    	    
    	try {
    		Log.getLogWriter().info("derby committing the operation");
	    	dConn.commit();
	    	Log.getLogWriter().info("derby committed the operation");        
    	} catch (SQLException se) {
    		SQLHelper.printSQLException(se);
    		derbySe = se;
    	}
    	
    	SQLHelper.compareExceptions(derbySe, gfxdSe);
    	
    }	    
    else {

    	try {
    		insertBatchToGFXDTable(gConn, oid, cid, sid, qty, status, time, bid);  
    	} catch (SQLException se) {
    		//should not get commit non colocated exception here
    		//partition by cid range, insert are on same cid
    		//partition by list, insert are on same tid
    		if (se.getSQLState().equals("23505")) {
	    		Log.getLogWriter().info("got expected gfxd duplicate primary key exception, continue testing");
	    		//key may be inserted already
	    	  // primary key check should not be at commit time
	    	} else if (se.getSQLState().equalsIgnoreCase("XJ208")) {
	    		Log.getLogWriter().info("got expected gfxd batch update exception due to fk constraint," +
	    				" continue testing");
	    	} else {
	    		SQLHelper.handleSQLException(se);
	    	}  		
    	}
    	try {
    		gConn.commit();
    		Log.getLogWriter().info("gfxd committed the operation");
    	} catch (SQLException se) {
    		//should not get commit non colocated exception here
    		//partition by cid range, insert are on same cid
    		//partition by list, insert are on same tid
    		if (se.getSQLState().equals("X0Z05")) {    			
    			if (SQLHelper.isSameRootSQLException("23503", se))
		    		Log.getLogWriter().warning("got foreign key check violation and masked as" +
    				" in doubt transaction exception -- which is bug# 41898 ");
	    	} else {
	    		SQLHelper.handleSQLException(se);
	    	}  		
    	}
    } 
  }
  
  protected void getDataForInsert(Connection gConn, int[] oid, int cid, int[] sid, int[] qty, Timestamp[] time, 
      BigDecimal[] bid, int size) {
    getOids(oid, size); //populate oids  
    for (int i=0; i<size; i++) {
      qty[i] = getQty(initMaxQty/2);
      time[i] = new Timestamp (System.currentTimeMillis());
      bid[i] =   new BigDecimal (Double.toString((rand.nextInt(10000)+1) * .01));  //random ask price      
      sid[i] = getSid();
    } 
  }

  //not expect insert to fail here
  protected void insertBatchToGFXDTable(Connection conn, int[] oid, int cid, int[] sid, int []qty,
      String status, Timestamp[] time, BigDecimal[] bid) throws SQLException{
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int counts[] = null;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    int size = oid.length;
    for (int i=0; i<size; i++) {
      Log.getLogWriter().info("inserting into table trade.buyorders oid is " + oid[i] +
          " cid is "+ cid + " sid is " + sid[i] + " qty is " + qty[i] + " status is " + status +
          " time is "+ time[i] + " bid is " + bid[i] + " tid is " + tid);
      stmt.setInt(1, oid[i]);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid[i]);
      stmt.setInt(4, qty[i]);
      stmt.setBigDecimal(5, bid[i]);
      stmt.setTimestamp(6, time[i]);
      stmt.setString(7, status);       
      stmt.setInt(8, tid);
      stmt.addBatch();
    }
    
    counts = stmt.executeBatch();   
    
    Log.getLogWriter().info("gfxd inserts " + counts.length + " record");
  }
  
  //not expect insert to fail here
  protected boolean insertBatchToDerbyTable(Connection conn, int[] oid, int cid, int[] sid, int []qty,
      String status, Timestamp[] time, BigDecimal[] bid) throws SQLException{
  	try {
	    PreparedStatement stmt = conn.prepareStatement(insert);
	    int tid = getMyTid();
	    int counts[] = null;
	    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
	    int size = oid.length;
	    for (int i=0; i<size; i++) {
	      Log.getLogWriter().info("inserting into table trade.buyorders oid is " + oid[i] +
	          " cid is "+ cid + " sid is " + sid[i] + " qty is " + qty[i] + " status is " + status +
	          " time is "+ time[i] + " bid is " + bid[i] + " tid is " + tid);
	      stmt.setInt(1, oid[i]);
	      stmt.setInt(2, cid);
	      stmt.setInt(3, sid[i]);
	      stmt.setInt(4, qty[i]);
	      stmt.setBigDecimal(5, bid[i]);
	      stmt.setTimestamp(6, time[i]);
	      stmt.setString(7, status);       
	      stmt.setInt(8, tid);
	      stmt.addBatch();
	    }
	    
	    counts = stmt.executeBatch();   
	    
	    Log.getLogWriter().info("gfxd inserts " + counts.length + " record");
  	} catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry   
      else throw se;
    }
    return true;
  }
  
	public void queryTx(Connection dConn, Connection gConn) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	public void updateTx(Connection dConn, Connection gConn,
			HashMap<String, Integer> modifiedKeys, ArrayList<SQLException> dExList,
			ArrayList<SQLException> sExList) {
    int txId = modifiedKeys.get(thisTxId);    
    int[] cids = new int[1];
    int[] tids = new int[1];
    
    getCidAndTid(gConn, txId, cids, tids);
    int cid = cids[0];
    int tid = tids[0];
    BigDecimal bid = getPrice();
    Timestamp orderTime = getRandTime();
    int qty = getQty();
    int sid = getSid();
    int whichUpdate = getWhichTxUpdate();
    int tid2 = 0;
    if (byTidList) tid2=sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    int oid=0;
    if (byTidList) {
    	oid = getOidByTid(gConn, tid);
    }
    else if (byCidRange) {
    	oid = getOidByCid(gConn, cid);
    }
       
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int smallCid = 0;
    int largeCid = 0;
    
    if (byCidRange) {
    	smallCid= GFXDTxHelper.getRangeEnds(getTableName(), cid)[0];
    	largeCid = GFXDTxHelper.getRangeEnds(getTableName(), cid)[1];
    }

    boolean opFailed = false;   
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeUpdate(gConn, keys, whichUpdate, cid, sid, oid, bid, orderTime,
        			smallCid, largeCid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		updateToGFXDTableCidRangeTx(gConn, cid, sid, oid, bid, qty, orderTime, 
    				smallCid, largeCid, whichUpdate);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListUpdate(gConn, keys, whichUpdate, cid, sid, oid, bid,
        			orderTime, tid, tid2, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableTidListTx(gConn, cid, sid, oid, bid, qty, orderTime,
    				whichUpdate, tid, tid2);
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
    	} else if (se.getSQLState().equalsIgnoreCase("X0Z04") && dConn == null
    			&& byCidRange) {
    		if (sql.sqlutil.GFXDTxHelper.isColocatedCid(getTableName(), txId, cid)) {
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
	    	data = new Object[11];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_BUYORDERS;
	    	data[1] = updateToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = oid;
	    	data[5] = bid;
	    	data[6] = qty;
	    	data[7] = orderTime;
	    	data[8] = smallCid;
	    	data[9] = largeCid;
	    	data[10] = whichUpdate;
    	} else if (byTidList) {
	    	data = new Object[11];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_BUYORDERS;
	    	data[1] = updateToDerbyTableTidListTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = oid;
	    	data[5] = bid;
	    	data[6] = qty;
	    	data[7] = orderTime;
	    	data[8] = whichUpdate; 
	    	data[9] = tid;
	    	data[10] = tid2;
    	}
    			
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
      SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }	

	}
	
	protected void getKeysForCidRangeUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, int sid, int oid, BigDecimal bid, Timestamp orderTime, int smallCid,
			int largeCid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.buyorders set bid = ? where cid = ? and oid= ? and status = 'open' ",
    	sql = "select oid from trade.buyorders where cid="+cid + " and oid=" +oid 
    	  + " and status='open'";
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("oid: " + oid +" exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;	    	    
    case 1: 
      //"update trade.buyorders set sid = ? , qty=? where cid = ? and oid= ? and bid <? and status = 'open'  ",
    	sql = "select oid from trade.buyorders where cid="+cid + " and oid=" +oid 
  	  + " and bid<" + bid + " and status='open'";
	    rs = conn.createStatement().executeQuery(sql); 
	    if (rs.next()) {
	      Log.getLogWriter().info("oid: " + oid +" exists for update");
	    	keys.put(getTableName()+"_"+rs.getInt(1), txId);
	    }    
	    rs.close(); 
	    break;
    case 2:
      //"update trade.buyorders set status = 'filled'  where cid>? and cid<? and sid = ? and bid<? and status = 'open' ", 	
      sql = "select oid from trade.buyorders where cid>"+smallCid 
    		+" and cid<" +largeCid + " and sid = " +sid + " and bid<" + bid + " and status='open'";
	    rs = conn.createStatement().executeQuery(sql); 
	    while (rs.next()) {
	    	int availOid = rs.getInt("OID");
	      Log.getLogWriter().info("oid: " + availOid + " exists for update");
	    	keys.put(getTableName()+"_"+availOid, txId);
	    }
	    rs.close();
	    break;
	  case 3: 
    // "update trade.buyorders set status = 'cancelled' where cid>? and cid<? and ordertime <? and status = 'open'  ",   
    	sql = "select oid from trade.buyorders where cid>? and cid<? and ordertime< ? " +
    			"and status='open'";
    	PreparedStatement ps = conn.prepareStatement(sql);
    	ps.setInt(1, smallCid);
    	ps.setInt(2, largeCid);
    	ps.setTimestamp(3, orderTime);
	    rs = ps.executeQuery(); 
	    while (rs.next()) {
	    	int availOid = rs.getInt("OID");
	      Log.getLogWriter().info("oid: " + availOid + " exists for update");
	    	keys.put(getTableName()+"_"+availOid, txId);
	    }   
	    rs.close();
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}	
	
	protected void getKeysForTidListUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, int sid, int oid, BigDecimal bid, Timestamp orderTime,
			int tid, int tid2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.buyorders set bid = ? where oid= ? and status = 'open' and tid=? ",	 
    	sql = "select oid from trade.buyorders where oid=" + oid 
  	  	+ " and status='open' and tid=" + tid;
	    rs = conn.createStatement().executeQuery(sql); 
	    if (rs.next()) {
	      Log.getLogWriter().info("oid: " + oid +" exists for update");
	    	keys.put(getTableName()+"_"+rs.getInt(1), txId);
	    }    
	    rs.close();
	    break;
    case 1: 
      // "update trade.buyorders set cid = ? , qty=? where oid= ? and bid <? and status = 'open' and tid =? ",
    	sql = "select oid from trade.buyorders where oid=" +oid 
  	  	+ " and bid<" + bid + " and status='open' and tid=" + tid;
	    rs = conn.createStatement().executeQuery(sql); 
	    if (rs.next()) {
	      Log.getLogWriter().info("oid: " + oid +" exists for update");
	    	keys.put(getTableName()+"_"+rs.getInt(1), txId);
	    }    
	    rs.close(); 
	    break;
    case 2: 
      //"update trade.buyorders set status = 'filled'  where sid = ? and bid<? and status = 'open' and (tid =? or tid=? )",
      sql = "select oid from trade.buyorders where sid="+sid + " and bid<" + bid  
      	+ " and status='open' and (tid=" + tid +" or tid=" + tid2 + ")";
	    rs = conn.createStatement().executeQuery(sql); 
	    while (rs.next()) {
	    	int availOid = rs.getInt("OID");
	      Log.getLogWriter().info("oid: " + availOid + " exists for update");
	    	keys.put(getTableName()+"_"+availOid, txId);
	    }
	    rs.close();
	    break;
    case 3:
    	//"update trade.buyorders set status = 'cancelled' where ordertime <? and status = 'open' and tid =? ",
    	sql = "select oid from trade.buyorders where ordertime< ? and status='open' and tid =? ";
    	PreparedStatement ps = conn.prepareStatement(sql);
    	ps.setTimestamp(1, orderTime);
    	ps.setInt(2, tid);
	    rs = ps.executeQuery(); 
	    while (rs.next()) {
	    	int availOid = rs.getInt("OID");
	      Log.getLogWriter().info("oid: " + availOid + " exists for update");
	    	keys.put(getTableName()+"_"+availOid, txId);
	    }   
	    rs.close();
	    break;
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void updateToGFXDTableCidRangeTx(Connection conn,  int cid, int sid, 
			int oid, BigDecimal bid, int qty, Timestamp orderTime, int smallCid, int largeCid, 
			int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update buyorders table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    count = updateToTableCidRangeTx(stmt, cid, sid, oid, bid, qty, orderTime, 
    		smallCid, largeCid, whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableCidRangeTx(Connection conn,  int cid, int sid, 
			int oid, BigDecimal bid, int qty, Timestamp orderTime, int smallCid, int largeCid, 
			int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update buyorders table in derby, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    try {
    	count = updateToTableCidRangeTx(stmt, cid, sid, oid, bid, qty, orderTime, 
    			smallCid, largeCid, whichUpdate);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	protected int updateToTableCidRangeTx(PreparedStatement stmt, int cid, int sid, 
			int oid, BigDecimal bid, int qty, Timestamp orderTime, int smallCid, int largeCid,
			int whichUpdate) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.buyorders set bid = ? where cid = ? and oid= ? and status = 'open' ",        
      Log.getLogWriter().info("updating buyorders table bid to " + bid +" for cid: "
      		+ cid + " and oid: " + oid + " and status = 'open'");
      stmt.setBigDecimal(1, bid);
      stmt.setInt(2, cid);
      stmt.setInt(3, oid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"update trade.buyorders set sid = ? , qty=? where cid = ? and oid= ? and bid <? and status = 'open'  ",
      Log.getLogWriter().info("updating buyorders table sid to " + sid +
      		" and  qty to " + qty + " for cid: "+ cid + " and oid: " + oid + 
      		" and bid < " + bid + " and status = 'open'");
      stmt.setInt(1, sid);
      stmt.setInt(2, qty);
      stmt.setInt(3, cid);
      stmt.setInt(4, oid);
      stmt.setBigDecimal(5, bid);
     rowCount =  stmt.executeUpdate();
     break;
    case 2: 
      // "update trade.buyorders set status = 'filled'  where cid>? and cid<? and sid = ? and bid<? and status = 'open' ",  
      Log.getLogWriter().info("updating buyorders table status to 'filled' " + 
      		"for cid: " + smallCid + " to " + largeCid + " and sid: " + sid +
      		" and ask < " + bid + " and status = 'open'");
      stmt.setInt(1, smallCid);
      stmt.setInt(2, largeCid);
      stmt.setInt(3, sid);
      stmt.setBigDecimal(4, bid);
      rowCount = stmt.executeUpdate();
      break; 
    case 3: 
		  // "update trade.buyorders set status = 'cancelled' where cid>? and cid<? and ordertime <? and status = 'open'  ",
      Log.getLogWriter().info("updating buyorders table status to 'cancelled' " + 
      		"for cid: " + smallCid + " to " + largeCid +
      		" and ordertime < '" + orderTime + "' and status = 'open'");
      stmt.setInt(1, smallCid);
      stmt.setInt(2, largeCid);
      stmt.setTimestamp(3, orderTime);
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
	
	protected void updateToGFXDTableTidListTx(Connection conn, int cid, int sid,  
			int oid, BigDecimal bid, int qty, Timestamp orderTime, int whichUpdate, 
			int tid, int tid2) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update buyorders table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    count = updateToTableTidListTx(stmt, cid, sid, oid, bid, qty, orderTime, 
    		whichUpdate, tid, tid2);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableTidListTx(Connection conn, int cid, int sid,  
			int oid, BigDecimal bid, int qty, Timestamp orderTime, int whichUpdate, 
			int tid, int tid2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update buyorder table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    try {
    	count = updateToTableTidListTx(stmt, cid, sid, oid, bid, qty, 
    			orderTime, whichUpdate, tid, tid2);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	protected int updateToTableTidListTx(PreparedStatement stmt, int cid,  
			int sid, int oid, BigDecimal bid, int qty, Timestamp orderTime, 
			int whichUpdate, int tid, int tid2) throws SQLException {
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.buyorders set bid = ? where oid= ? and status = 'open' and tid=? ", 	    
      Log.getLogWriter().info("updating buyorders table bid to " + bid 
      		+" for  oid: " + oid + " and status = 'open' and tid=" + tid);
      stmt.setBigDecimal(1, bid);
      stmt.setInt(2, oid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      // "update trade.buyorders set cid = ? , qty=? where oid= ? and bid <? and status = 'open' and tid =? ",
      Log.getLogWriter().info("updating buyorders table cid to " + cid +
      		" and  qty to " + qty + " for oid: " + oid + 
      		" and bid < " + bid + " and status = 'open' and tid=" + tid);
      stmt.setInt(1, cid);
      stmt.setInt(2, qty);
      stmt.setInt(3, oid);
      stmt.setBigDecimal(4, bid);
      stmt.setInt(5, tid);
      rowCount =  stmt.executeUpdate();            
      break;
    case 2: 
	    // "update trade.buyorders set status = 'filled'  where sid = ? and bid<? and status = 'open' and (tid =? or tid=? )",  
      Log.getLogWriter().info("updating buyorders table status to 'filled' " + 
      		"for sid: " + sid +	" and bid < " + bid + " and status = 'open' " +
      		"and (tid=" + tid + " or tid=" + tid2 + ")" );
      stmt.setInt(1, sid);
      stmt.setBigDecimal(2, bid);
      stmt.setInt(3, tid);
      stmt.setInt(4, tid2);
      rowCount = stmt.executeUpdate();
      break;
    case 3:
    	//"update trade.buyorders set status = 'cancelled' where ordertime <? and status = 'open' and tid =? ",       
      Log.getLogWriter().info("updating buyorders table status to 'cancelled' " + 
      		"for ordertime < '" + orderTime + "' and status = 'open' and tid=" +tid);
      stmt.setTimestamp(1, orderTime);
      stmt.setInt(2, tid);
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
	
	protected int getOidByCid(Connection gConn, int cid) {
  	ResultSet rs = null;
  	List<Struct> result = null;
		try {
      String sql="select oid from trade.buyorders where status = 'open' and cid= " + cid;
      rs = gConn.createStatement().executeQuery(sql);   
      result = ResultSetHelper.asList(rs, false);
      return getOid(result);
  	} catch (SQLException se) {
    	SQLHelper.handleSQLException(se); //throws TestException.
    } 
  	return 0;
	}
	
	protected int getOidByTid(Connection gConn, int tid) {
  	ResultSet rs = null;
  	List<Struct> result = null;
		try {
      String sql="select oid from trade.buyorders where status = 'open' and tid= " + tid;
      rs = gConn.createStatement().executeQuery(sql);   
      result = ResultSetHelper.asList(rs, false);
      return getOid(result);
  	} catch (SQLException se) {
    	SQLHelper.handleSQLException(se); //throws TestException.
    } 
  	return 0;
	}
	
	protected int getOid(List<Struct> result) {
		if (result == null || result.size() == 0) return 0;
		int whichOne = rand.nextInt(result.size());
		return (Integer)result.get(whichOne).getFieldValues()[0];		
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
	
	public void performDerbyTxOps(Connection dConn, Object[] data,
			ArrayList<SQLException> dExList) {
  	boolean success = true;
  	try {
	  	switch ((Integer) data[1]) {
	  	case insertToDerbyTable: 
	  		success = insertToDerbyTable(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (Integer) data[5], (String) data[6], (Timestamp) data[7], 
	  				(BigDecimal) data[8]);
	  			//(Connection conn, int oid, int cid, int sid, int qty,
	  	  	//String status, Timestamp time, BigDecimal bid)
	  		break;
	  	case updateToDerbyTableCidRangeTx: 
	  		success = updateToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (BigDecimal) data[5], (Integer) data[6], (Timestamp) data[7],
	  				(Integer)data[8], (Integer)data[9], (Integer)data[10]);
	  		//(Connection conn,  int cid, int sid, 
				//int oid, BigDecimal bid, int qty, Timestamp orderTime, int smallCid, int largeCid, 
				//int whichUpdate) 
	  		break;
	  	case updateToDerbyTableTidListTx: 
	  		success = updateToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (BigDecimal) data[5], (Integer) data[6], (Timestamp)data[7], 
	  				(Integer)data[8], (Integer) data[9], (Integer) data[10]);
	  		//(PreparedStatement stmt, int cid,  
				//int sid, int oid, BigDecimal bid, int qty, Timestamp orderTime, 
				//int whichUpdate, int tid, int tid2)
	  		break;
	  	case deleteToDerbyTableCidRangeTx: 
	  		success = deleteToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (BigDecimal) data[5], (Integer) data[6]);
	  		//(Connection conn,  int cid, int cid2, int oid, BigDecimal bid, int whichDelete)
	  		break;
	  	case deleteToDerbyTableTidListTx: 
	  		success = deleteToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (BigDecimal) data[5], (Integer) data[6], (Integer) data[7]);
	  		//deleteToDerbyTableTidListTx(Connection conn, int oid1,  
				//int oid2, int oid, BigDecimal bid, int whichDelete, int tid)
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

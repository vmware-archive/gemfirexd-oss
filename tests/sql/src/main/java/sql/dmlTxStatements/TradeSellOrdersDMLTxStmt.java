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
import java.util.List;
import java.util.HashMap;

import sql.SQLBB;
import sql.SQLHelper;
import sql.dmlStatements.TradeSellOrdersDMLStmt;
import sql.sqlTx.SQLDerbyTxBB;
import sql.sqlutil.DMLTxStmtsFactory;
import sql.sqlutil.ResultSetHelper;
import sql.sqlutil.GFXDTxHelper;
import com.gemstone.gemfire.cache.query.Struct;
import util.TestException;
import util.TestHelper;

public class TradeSellOrdersDMLTxStmt extends TradeSellOrdersDMLStmt implements
		DMLTxStmtIF {

	String[] updateByCidRange = {	
	    "update trade.sellorders set ask = ? where cid = ? and oid= ? and status = 'open' ",
	    "update trade.sellorders set ask = ? , qty=? where cid = ? and oid= ? and ask <? and status = 'open'  ",
	    //multi rows
	    "update trade.sellorders set status = 'filled'  where cid>? and cid<? and sid = ? and ask<? and status = 'open' ",  
	    "update trade.sellorders set status = 'cancelled' where cid>? and cid<? and order_time <? and status = 'open'  ", 
	};
	
	int updateByCidRangeSingleRowStmt = 2;
	
	String[] updateByTidList = {
	    "update trade.sellorders set ask = ? where oid= ? and status = 'open' and tid=? ",
	    "update trade.sellorders set ask = ? , qty=? where oid= ? and ask <? and status = 'open' and tid =? ",
	    //multi rows
	    "update trade.sellorders set status = 'filled'  where sid = ? and ask<? and status = 'open' and (tid =? or tid=? )",  
	    "update trade.sellorders set status = 'cancelled' where order_time <? and status = 'open' and tid =? ", 
	};
	
	int updateByTidListSingleRowStmt = 2;
	
	String[] deleteByCidRange = {
	    "delete from trade.sellorders where cid=? and oid=?",
	    //multiple rows
	    "delete from trade.sellorders where cid>? and cid<? and status IN ('cancelled', 'filled')",    
	};
	
	int deleteByCidRangeSingleRowStmt = 1;
	
	String[] deleteByTidList = {
	    "delete from trade.sellorders where oid=? and tid=? ",
	    //multiple rows
	    "delete from trade.sellorders where oid>? and oid<? and status IN ('cancelled', 'filled') and tid=? ", 
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
        	getKeysForCidRangeDelete(gConn, keys, whichDelete, cid[0], cid2, oid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		deleteToGFXDTableCidRangeTx(gConn, cid[0], cid2, oid, whichDelete);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListDelete(gConn, keys, whichDelete, oid1, oid2, oid, tid[0], txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		deleteToGFXDTableTidListTx(gConn, oid1, oid2, oid, whichDelete, tid[0]);
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
	    	data = new Object[6];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_SELLORDERS;
	    	data[1] = deleteToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid[0];
	    	data[3] = cid2;
	    	data[4] = oid;
	    	data[5] = whichDelete;
    	} else if (byTidList) {
	    	data = new Object[7];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_SELLORDERS;
	    	data[1] = deleteToDerbyTableTidListTx; //which operation
	    	data[2] = oid1;
	    	data[3] = oid2;
	    	data[4] = oid;
	    	data[5] = whichDelete;
	    	data[6] = tid[0];    		
    	}
    	    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
      SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }
	}
	
	protected void getKeysForCidRangeDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int cid2, int oid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.sellorders where cid=? and oid=?",    
    	sql = "select oid from trade.sellorders where cid="+cid + " and oid=" + oid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("oid: " + oid +" exists for update");
      	keys.put(getTableName()+"_"+oid, txId);
      }    
      rs.close();
      break;	    	    
    case 1: 
      //"delete from trade.sellorders where cid>? and cid<? and status IN ('cancelled', 'filled')", 
      sql = "select oid from trade.sellorders where cid>"+cid 
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
			int whichDelete, int oid1, int oid2, int oid, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.sellorders where oid=? and tid=? ",
    	sql = "select oid from trade.sellorders where oid=" + oid 
    		+ " and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("oid: " + oid +" exists for update");
      	keys.put(getTableName()+"_"+oid, txId);
      }    
      rs.close();
      break;	    	    
    case 1: 
      //"delete from trade.sellorders where oid>? and oid<? and status IN ('cancelled', 'filled') and tid=? ",  
      sql = "select oid from trade.sellorders where oid>"+oid1 
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
			int oid, int whichDelete) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete sellorders table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
    count = deleteToTableCidRangeTx(stmt, cid, cid2, oid, whichDelete);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
	}
	
	protected boolean deleteToDerbyTableCidRangeTx(Connection conn,  int cid, int cid2, 
			int oid, int whichDelete) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete sellorders table in derby, myTid is " + tid);
    Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
    try {
    	count = deleteToTableCidRangeTx(stmt, cid, cid2, oid, whichDelete);
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
			int oid, int whichDelete) throws SQLException { 
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.sellorders where cid=? and oid=?",     
      Log.getLogWriter().info("deleting from sellorders table for cid: " + cid + " and oid: " + oid );
      stmt.setInt(1, cid);
      stmt.setInt(2, oid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"delete from trade.sellorders where cid>? and cid<? and status IN ('cancelled', 'filled')", 
      Log.getLogWriter().info("deleting from sellorders table for cid> "+ cid + " to cid< " + cid2 + 
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
	
	protected void deleteToGFXDTableTidListTx(Connection conn, int oid1,  
			int oid2, int oid, int whichDelete, int tid) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;    
    Log.getLogWriter().info("delete sellorders table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    count = deleteToTableTidListTx(stmt, oid1, oid2, oid, whichDelete, tid);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 		
	}
	
	protected boolean deleteToDerbyTableTidListTx(Connection conn, int oid1,  
			int oid2, int oid, int whichDelete, int tid) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;
    Log.getLogWriter().info("delete sellorders table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    try {
    	count = deleteToTableTidListTx(stmt, oid1, oid2, oid, whichDelete, tid);
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
			int oid, int whichDelete, int tid) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
			//"delete from trade.sellorders where oid=? and tid=? ",
      Log.getLogWriter().info("deleting from sellorders for oid: " + oid 
      		+ " and tid: " + tid);
      stmt.setInt(1, oid);
      stmt.setInt(2, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
			//"delete from trade.sellorders where oid>? and oid<? and status IN ('cancelled', 'filled') and tid=? ",  
      Log.getLogWriter().info("deleting from sellorders for oid > " + oid1 
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
  	return "sellorders";
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
    BigDecimal[] ask = new BigDecimal[size];
    
    int txId = modifiedKeys.get(thisTxId);
    getCidAndTid(gConn, txId, cid, tid);
    getDataForInsert(gConn, oid, cid, sid, qty, time, ask, size); //get the data    
    boolean opFailed = false;
    
    try {
    	insertToGFXDTable(gConn, oid[0], cid[0], sid[0], qty[0], status, time[0], ask[0]);
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
    	data[0] = DMLTxStmtsFactory.TRADE_SELLORDERS;
    	data[1] = insertToDerbyTable; //which operation
    	data[2] = oid[0];
    	data[3] = cid[0];
    	data[4] = sid[0];
    	data[5] = qty[0];
    	data[6] = status;
    	data[7] = time[0];
    	data[8] = ask[0];
    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data);
    	SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    } 
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
		int size = 35;
    populateTx (dConn, gConn, size); 
	}
	
  //populate the table
  public void populateTx (Connection dConn, Connection gConn, int initSize) {
  	insert(dConn, gConn, initSize);
  }
  
  public void insert(Connection dConn, Connection gConn, int size) {
    for (int i =0; i<size; i++) {
    	int maxSize = 10;
    	int randSize = rand.nextInt(maxSize) +1;
    	insertRows (dConn, gConn, randSize, getCid());
    }   
  }
  
  public void insertRows(Connection dConn, Connection gConn, int size, int cid) {
    int[] oid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    String status = "open";
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] ask = new BigDecimal[size];

    boolean success = false;
    getDataForInsert(gConn, oid, cid, sid, qty, time, ask, size); //get the data
    SQLException derbySe = null;
    SQLException gfxdSe = null;
    if (dConn != null) {	    	    	
	    for (int i=0; i<size; i++) {
	    	//insert to each table
	    	try {
	    		insertToGFXDTable(gConn, oid[i], cid, sid[i], qty[i], status, time[i], ask[i]); 
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		gfxdSe = se; 
	    	}
	    	try {
	    		success = insertToDerbyTable(dConn, oid[i], cid, sid[i], qty[i], status, time[i], ask[i]); 
	        int count = 0;
	        while (!success) {
	          if (count>=maxNumOfTries) {
	            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
	            gConn.rollback();
	            dConn.rollback();
	            break;
	          }
	          success = insertToDerbyTable(dConn, oid[i], cid, sid[i], qty[i], status, time[i], ask[i]);  //retry insert to derby table         
	          
	          count++;
	        }
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		derbySe = se;
	    	}	    	
		   		    	    
		    //commit to each table
		    try {
			    try {
			    	gConn.commit();
			    	Log.getLogWriter().info("gfxd committed the operation");
			    	dConn.commit();
			    	Log.getLogWriter().info("derby committed the operation");
			    } catch (SQLException se) {
			    	SQLHelper.printSQLException(se);
			    	if (se.getSQLState().equalsIgnoreCase("X0Z02")) {
			    		Log.getLogWriter().info("get commit conflict exception, roll back the operation");
			    		dConn.rollback(); //commit conflict exception in gfxd, rollback the op in derby
					    derbySe = null;
					    gfxdSe = null;
			    		continue;
			    	} else if (se.getSQLState().equals("X0Z05")) {
			    	  //fk check violation is possible here
			    		//currently product throw in doubt transaction, which should be changed after #41898 is fixed
					    if (derbySe !=null  && SQLHelper.isSameRootSQLException(derbySe.getSQLState(), se)) {
					      if (derbySe.getSQLState().equals("23503")) {
					    		Log.getLogWriter().warning("got foreign key check violation and masked as" +
			    				" in doubt transaction exception -- which is bug# 41898 ");
					      }
						    derbySe = null;
						    gfxdSe = null;
						    continue;
					    } 
			    	} else {
			    		SQLHelper.handleSQLException(se);
			    	}
			    	//gfxd should not have non colocated exception as it commit per insert
			    } 			    
		    }	catch (SQLException se) {
		    	SQLHelper.handleSQLException(se); //for dConn.rollback()
		    }

		    SQLHelper.compareExceptions(derbySe, gfxdSe);
		    derbySe = null;
		    gfxdSe = null;
	    }
	    
	    try {
	    	dConn.commit();
	    } catch (SQLException se) {
	    	SQLHelper.handleSQLException(se); 
	    }
    }	    
    else {
    	for (int i=0; i<size; i++) {
	    	try {
	        insertToGFXDTable(gConn, oid[i], cid, sid[i], qty[i], status, time[i], ask[i]); 
	    	} catch (SQLException se) {
	    		//should not get commit non colocated exception here
	    		//partition by cid range, insert are on same cid
	    		//partition by list, insert are on same tid
	    		if (se.getSQLState().equals("23505")) {
		    		Log.getLogWriter().info("got expected gfxd duplicate primary key exception, continue testing");
		    		//key may be inserted already
		    	} // primary key check should not be at commit time
		    	else {
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
		    	if (se.getSQLState().equalsIgnoreCase("X0Z02")) {
		    		Log.getLogWriter().info("got expected gfxd commit conflict exception, continue testing"); 
		    		//commit conflict exception in gfxd
		    	}  else if (se.getSQLState().equals("X0Z05")) {    			
			    	if (SQLHelper.isSameRootSQLException("23505", se))
			    		Log.getLogWriter().warning("got duplicate primary key exception and masked as" +
			    				" in doubt transaction exception -- primary key should be checked at operation time ");
			    	else if (SQLHelper.isSameRootSQLException("23503", se))
			    		Log.getLogWriter().warning("got foreign key check violation and masked as" +
	    				" in doubt transaction exception -- which is bug# 41898 ");
		    	} else {
		    		SQLHelper.handleSQLException(se);
		    	}  		
	    	}
    	}
    } 
  }
  
  protected void insertToGFXDTable(Connection conn, int oid, int cid, int sid, int qty,
      String status, Timestamp time, BigDecimal ask) throws SQLException{
	  PreparedStatement stmt = useDefaultValue? getStmt(conn, insertWithDefaultValue) : getStmt(conn, insert);
	  int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    count = insertToTable(stmt, oid, cid, sid, qty, status, time, ask, tid);
    Log.getLogWriter().info("gfxd inserts " + count + " record");
  }
  
  //for derby insert
  protected boolean insertToDerbyTable(Connection conn, int oid, int cid, int sid, int qty,
  String status, Timestamp time, BigDecimal ask) throws SQLException{
  	PreparedStatement stmt = useDefaultValue? getStmt(conn, insertWithDefaultValue) : getStmt(conn, insert);
  	int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    try {
    	count = insertToTable(stmt, oid, cid, sid, qty, status, time, ask, tid);
      Log.getLogWriter().info("derby inserts " + count + " record");
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry   
      else throw se;
    }
    return true;
  }
  
  //populate arrays for insert
  protected void getDataForInsert(Connection gConn, int[] oid, int cid, int[] sid, int[] qty, Timestamp[] time, 
      BigDecimal[] ask, int size) {
    getOids(oid, size); //populate oids  
    for (int i=0; i<size; i++) {
      qty[i] = getQty(initMaxQty/2);
      time[i] = new Timestamp (System.currentTimeMillis());
      ask[i] =   new BigDecimal (Double.toString((rand.nextInt(10000)+1) * .01));  //random ask price      
    } 
    try {
    	getSids(gConn, cid, sid);
    } catch (SQLException se) {
    	SQLHelper.handleSQLException(se);
    }

    if (size >0)
      sid[0] = getSid();  //replace the first sid to be a random number, due to foreign key constraint (protfolion), this record may not be inserted
  }
  
  protected void getSids(Connection gConn, int cid, int[] sid) throws SQLException{
  	ResultSet rs = null;
    int i=0;
    int size = sid.length;
  	try {
      String sql="select sid from trade.portfolio where cid= " + cid;
      rs = gConn.createStatement().executeQuery(sql);   
      while (rs!=null && rs.next() && i<size) {
        sid[i] = rs.getInt(1);
        i++;
      } 
  	}catch (SQLException se) {
    	SQLHelper.handleSQLException(se); //throws TestException.
    } finally {
    	if (rs != null) rs.close();
    }
    while (i<size) {
    	sid[i] = getSid();
    	i++;
    }
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
    BigDecimal ask = getPrice();
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
        	getKeysForCidRangeUpdate(gConn, keys, whichUpdate, cid, sid, oid, ask, orderTime,
        			smallCid, largeCid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		updateToGFXDTableCidRangeTx(gConn, cid, sid, oid, ask, qty, orderTime, 
    				smallCid, largeCid, whichUpdate);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListUpdate(gConn, keys, whichUpdate, cid, sid, oid, ask,
        			orderTime, tid, tid2, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableTidListTx(gConn, cid, sid, oid, ask, qty, orderTime,
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
	    	data[0] = DMLTxStmtsFactory.TRADE_SELLORDERS;
	    	data[1] = updateToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = oid;
	    	data[5] = ask;
	    	data[6] = qty;
	    	data[7] = orderTime;
	    	data[8] = smallCid;
	    	data[9] = largeCid;
	    	data[10] = whichUpdate;
    	} else if (byTidList) {
	    	data = new Object[11];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_SELLORDERS;
	    	data[1] = updateToDerbyTableTidListTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = oid;
	    	data[5] = ask;
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
			int whichUpdate, int cid, int sid, int oid, BigDecimal ask, Timestamp orderTime, int smallCid,
			int largeCid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.sellorders set ask = ? where cid = ? and oid= ? and status = 'open' ",
    	sql = "select oid from trade.sellorders where cid="+cid + " and oid=" +oid 
    	  + " and status='open'";
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("oid: " + oid +" exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }    
      rs.close();
      break;	    	    
    case 1: 
      //"update trade.sellorders set ask = ? , qty=? where cid = ? and oid= ? and ask <? and status = 'open'  ",,
    	sql = "select oid from trade.sellorders where cid="+cid + " and oid=" +oid 
  	  + " and ask<" + ask + " and status='open'";
	    rs = conn.createStatement().executeQuery(sql); 
	    if (rs.next()) {
	      Log.getLogWriter().info("oid: " + oid +" exists for update");
	    	keys.put(getTableName()+"_"+rs.getInt(1), txId);
	    }    
	    rs.close(); 
	    break;
    case 2:
      //"update trade.sellorders set status = 'filled'  where cid>? and cid<? and sid = ? and ask<? and status = 'open' ", 	
      sql = "select oid from trade.sellorders where cid>"+smallCid 
    		+" and cid<" +largeCid + " and sid = " +sid + " and ask<" + ask + " and status='open'";
	    rs = conn.createStatement().executeQuery(sql); 
	    while (rs.next()) {
	    	int availOid = rs.getInt("OID");
	      Log.getLogWriter().info("oid: " + availOid + " exists for update");
	    	keys.put(getTableName()+"_"+availOid, txId);
	    }
	    rs.close();
	    break;
	  case 3: 
    //"update trade.sellorders set status = 'cancelled' where cid>? and cid<? and order_time <? and status = 'open'  ",  
     /* sql = "select oid from trade.sellorders where cid>"+smallCid 
  			+" and cid<" +largeCid + " and order_time<CAST('" + orderTime + "' as TIMESTAMP) and status='open'";
  			to work around #42011*/
    	sql = "select oid from trade.sellorders where cid>? and cid<? and order_time< ? " +
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
	
	protected void updateToGFXDTableCidRangeTx(Connection conn,  int cid, int sid, 
			int oid, BigDecimal ask, int qty, Timestamp orderTime, int smallCid, int largeCid, 
			int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update sellorders table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    count = updateToTableCidRangeTx(stmt, cid, sid, oid, ask, qty, orderTime, 
    		smallCid, largeCid, whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableCidRangeTx(Connection conn,  int cid, int sid, 
			int oid, BigDecimal ask, int qty, Timestamp orderTime, int smallCid, int largeCid, 
			int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update sellorders table in derby, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    try {
    	count = updateToTableCidRangeTx(stmt, cid, sid, oid, ask, qty, orderTime, 
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
			int oid, BigDecimal ask, int qty, Timestamp orderTime, int smallCid, int largeCid,
			int whichUpdate) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.sellorders set ask = ? where cid = ? and oid= ? and status = 'open' ",        
      Log.getLogWriter().info("updating sellorders table ask to " + ask +" for cid: "
      		+ cid + " and oid: " + oid + " and status = 'open'");
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, cid);
      stmt.setInt(3, oid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"update trade.sellorders set ask = ? , qty=? where cid = ? and oid= ? and ask <? and status = 'open'  ",
      Log.getLogWriter().info("updating sellorders table ask to " + ask +
      		" and  qty to " + qty + " for cid: "+ cid + " and oid: " + oid + 
      		" and ask < " + ask + " and status = 'open'");
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, qty);
      stmt.setInt(3, cid);
      stmt.setInt(4, oid);
      stmt.setBigDecimal(5, ask);
     rowCount =  stmt.executeUpdate();
     break;
    case 2: 
      //  "update trade.sellorders set status = 'filled'  where cid>? and cid<? and sid = ? and ask<? and status = 'open' ",
      Log.getLogWriter().info("updating sellorders table status to 'filled' " + 
      		"for cid: " + smallCid + " to " + largeCid + " and sid: " + sid +
      		" and ask < " + ask + " and status = 'open'");
      stmt.setInt(1, smallCid);
      stmt.setInt(2, largeCid);
      stmt.setInt(3, sid);
      stmt.setBigDecimal(4, ask);
      rowCount = stmt.executeUpdate();
      break; 
    case 3: 
		  // "update trade.sellorders set status = 'cancelled' where cid>? and cid<? and order_time <? and status = 'open'  ", 
      Log.getLogWriter().info("updating sellorders table status to 'cancelled' " + 
      		"for cid: " + smallCid + " to " + largeCid +
      		" and order_time < '" + orderTime + "' and status = 'open'");
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
	
	protected void getKeysForTidListUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, int sid, int oid, BigDecimal ask, Timestamp orderTime,
			int tid, int tid2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.sellorders set ask = ? where oid= ? and status = 'open' and tid=? ",	 
    	sql = "select oid from trade.sellorders where oid=" + oid 
  	  	+ " and status='open' and tid=" + tid;
	    rs = conn.createStatement().executeQuery(sql); 
	    if (rs.next()) {
	      Log.getLogWriter().info("oid: " + oid +" exists for update");
	    	keys.put(getTableName()+"_"+rs.getInt(1), txId);
	    }    
	    rs.close();
	    break;
    case 1: 
      // "update trade.sellorders set ask = ? , qty=? where oid= ? and ask <? and status = 'open' and tid =? ",
    	sql = "select oid from trade.sellorders where oid=" +oid 
  	  	+ " and ask<" + ask + " and status='open' and tid=" + tid;
	    rs = conn.createStatement().executeQuery(sql); 
	    if (rs.next()) {
	      Log.getLogWriter().info("oid: " + oid +" exists for update");
	    	keys.put(getTableName()+"_"+rs.getInt(1), txId);
	    }    
	    rs.close(); 
	    break;
    case 2: 
      //"update trade.sellorders set status = 'filled'  where sid = ? and ask<? and status = 'open' and (tid =? or tid=? )", 
      sql = "select oid from trade.sellorders where sid="+sid + " and ask<" + ask  
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
    	//"update trade.sellorders set status = 'cancelled' where order_time <? and status = 'open' and tid =? ",
      /*sql = "select oid from trade.sellorders where order_time< CAST('" + orderTime 
      	+ "' as TIMESTAMP) and status='open' and tid =" +tid;
      	*/
    	sql = "select oid from trade.sellorders where order_time< ? and status='open' and tid =? ";
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
	
	protected void updateToGFXDTableTidListTx(Connection conn, int cid, int sid,  
			int oid, BigDecimal ask, int qty, Timestamp orderTime, int whichUpdate, 
			int tid, int tid2) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update sellorders table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    count = updateToTableTidListTx(stmt, cid, sid, oid, ask, qty, orderTime, 
    		whichUpdate, tid, tid2);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableTidListTx(Connection conn, int cid, int sid,  
			int oid, BigDecimal ask, int qty, Timestamp orderTime, int whichUpdate, 
			int tid, int tid2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update sellorder table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    try {
    	count = updateToTableTidListTx(stmt, cid, sid, oid, ask, qty, 
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
			int sid, int oid, BigDecimal ask, int qty, Timestamp orderTime, 
			int whichUpdate, int tid, int tid2) throws SQLException {
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.sellorders set ask = ? where oid= ? and status = 'open' and tid=? ",	    
      Log.getLogWriter().info("updating sellorders table ask to " + ask 
      		+" for  oid: " + oid + " and status = 'open' and tid=" + tid);
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, oid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"update trade.sellorders set ask = ? , qty=? where oid= ? and ask <? and status = 'open' and tid =? ",
      Log.getLogWriter().info("updating sellorders table ask to " + ask +
      		" and  qty to " + qty + " for oid: " + oid + 
      		" and ask < " + ask + " and status = 'open' and tid=" + tid);
      stmt.setBigDecimal(1, ask);
      stmt.setInt(2, qty);
      stmt.setInt(3, oid);
      stmt.setBigDecimal(4, ask);
      stmt.setInt(5, tid);
      rowCount =  stmt.executeUpdate();            
      break;
    case 2: 
	    //  "update trade.sellorders set status = 'filled'  where sid = ? and ask<? and status = 'open' and (tid =? or tid=? )",  
      Log.getLogWriter().info("updating sellorders table status to 'filled' " + 
      		"for sid: " + sid +	" and ask < " + ask + " and status = 'open' " +
      		"and (tid=" + tid + " or tid=" + tid2 + ")" );
      stmt.setInt(1, sid);
      stmt.setBigDecimal(2, ask);
      stmt.setInt(3, tid);
      stmt.setInt(4, tid2);
      rowCount = stmt.executeUpdate();
      break;
    case 3:
    	// "update trade.sellorders set status = 'cancelled' where order_time <? and status = 'open' and tid =? ",        
      Log.getLogWriter().info("updating sellorders table status to 'cancelled' " + 
      		"for order_time < '" + orderTime + "' and status = 'open' and tid=" +tid);
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
      String sql="select oid from trade.sellorders where status = 'open' and cid= " + cid;
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
      String sql="select oid from trade.sellorders where status = 'open' and tid= " + tid;
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
	  	  	//String status, Timestamp time, BigDecimal ask)
	  		break;
	  	case updateToDerbyTableCidRangeTx: 
	  		success = updateToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (BigDecimal) data[5], (Integer) data[6], (Timestamp) data[7],
	  				(Integer)data[8], (Integer)data[9], (Integer)data[10]);
	  		//(Connection conn,  int cid, int sid, 
				//int oid, BigDecimal ask, int qty, Timestamp orderTime, int smallCid, int largeCid, 
				//int whichUpdate) 
	  		break;
	  	case updateToDerbyTableTidListTx: 
	  		success = updateToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (BigDecimal) data[5], (Integer) data[6], (Timestamp)data[7], 
	  				(Integer)data[8], (Integer) data[9], (Integer) data[10]);
	  		//(PreparedStatement stmt, int cid,  
				//int sid, int oid, BigDecimal ask, int qty, Timestamp orderTime, 
				//int whichUpdate, int tid, int tid2)
	  		break;
	  	case deleteToDerbyTableCidRangeTx: 
	  		success = deleteToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (Integer) data[5]);
	  		//Connection conn,  int cid, int cid2, int oid, int whichDelete)
	  		break;
	  	case deleteToDerbyTableTidListTx: 
	  		success = deleteToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (Integer) data[5], (Integer) data[6]);
	  		//(Connection conn, int oid1, int oid2, int oid, int whichDelete, int tid)
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

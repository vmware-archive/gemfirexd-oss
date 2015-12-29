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
import sql.dmlStatements.TradeTxHistoryDMLStmt;
import sql.sqlTx.SQLDerbyTxBB;
import sql.sqlutil.DMLTxStmtsFactory;
import sql.sqlutil.ResultSetHelper;
import com.gemstone.gemfire.cache.query.Struct;
import sql.sqlutil.GFXDTxHelper;
import util.TestException;
import util.TestHelper;

public class TradeTxHistoryDMLTxStmt extends TradeTxHistoryDMLStmt implements
		DMLTxStmtIF {
	
	public static final int TXHISTORYINSERT = -100;
	public static final int TXHISTORYUPDATE = -101;
	String[] updateByCidRange = {	
	    "update trade.txhistory set oid = ? where cid = ? and oid= ? ",
	    "update trade.txhistory set price = ? , qty=? where cid = ? and oid= ? and price <? ",
	    "update trade.txhistory set sid = ?  where cid>? and cid<? and sid = ? and price <?",  
	    "update trade.txhistory set price = ? where cid>? and cid<? and sid = ?  ", 
	};
	
	int updateByCidRangeSingleRowStmt = 0;
	
	String[] updateByTidList = {
	    "update trade.txhistory set oid = ? where oid= ? and tid=? ",
	    "update trade.txhistory set price = ? , qty=? where oid= ? and price <? and tid =? ",
	    "update trade.txhistory set sid = ?  where sid = ? and price<? and (tid =? or tid=? )",  
	    "update trade.txhistory set price = ? where sid <? and tid =? ", 
	};
	
	int updateByTidListSingleRowStmt = 0;
	
	String[] deleteByCidRange = {
	    "delete from trade.txhistory where cid=? and oid=?",
	    "delete from trade.txhistory where cid>? and cid<? ",    
	};
	
	int deleteByCidRangeSingleRowStmt = 0;
	
	String[] deleteByTidList = {
	    "delete from trade.txhistory where oid=? and tid=? ",
	    "delete from trade.txhistory where oid>? and oid<? and tid=? ", 
	};
	
	int deleteByTidListSingleRowStmt = 0;
	
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
    if (whichDelete <0) {
    	Log.getLogWriter().info("Does not have single row operation in no primary key table");
    	return;
    }
    
		getCidAndTid(gConn, txId, cid, tid);
		int cid2 = cid[0] + cidRangeForTxOp;
		int oid = 0;
    if (byTidList) {
    	oid = getOidByTid(gConn, tid[0]);
    }
    else if (byCidRange) {
    	oid = getOidByCid(gConn, cid[0]);
    }
    int oid1 = oid;
		int oid2 = oid + 20;

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
	    	data[0] = DMLTxStmtsFactory.TRADE_TXHISTORY;
	    	data[1] = deleteToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid[0];
	    	data[3] = cid2;
	    	data[4] = oid;
	    	data[5] = whichDelete;
    	} else if (byTidList) {
	    	data = new Object[7];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_TXHISTORY;
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
	
	protected void deleteToGFXDTableCidRangeTx(Connection conn,  int cid, int cid2, 
			int oid, int whichDelete) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("delete txhistory table in gemfirexd, myTid is " + tid);
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
    Log.getLogWriter().info("delete txhistory table in derby, myTid is " + tid);
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
  		//"delete from trade.txhistory where cid=? and oid=?",	       
      Log.getLogWriter().info("deleting from txhistory table for cid: " + cid + " and oid: " + oid );
      stmt.setInt(1, cid);
      stmt.setInt(2, oid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      // "delete from trade.txhistory where cid>? and cid<? ", 
      Log.getLogWriter().info("deleting from txhistory table for cid> "+ cid + " to cid< " + cid2);
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
    Log.getLogWriter().info("delete txhistory table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    count = deleteToTableTidListTx(stmt, oid1, oid2, oid, whichDelete, tid);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 		
	}
	
	protected boolean deleteToDerbyTableTidListTx(Connection conn, int oid1,  
			int oid2, int oid, int whichDelete, int tid) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;
    Log.getLogWriter().info("delete txhistory table in derby, myTid is " + getMyTid());
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
			//"delete from trade.txhistory where oid=? and tid=? ",
      Log.getLogWriter().info("deleting from txhistory for oid: " + oid 
      		+ " and tid: " + tid);
      stmt.setInt(1, oid);
      stmt.setInt(2, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
			//"delete from trade.txhistory where oid>? and oid<? and tid=? ", 
      Log.getLogWriter().info("deleting from txhistory for oid > " + oid1 
      		+ " oid < " + oid2 + " and tid: " + tid);
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
	
	protected void getKeysForCidRangeDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int cid2, int oid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.txhistory where cid=? and oid=?",     
    	sql = "select * from trade.txhistory where cid="+cid
    	  + " and oid=" + oid;
      rs = conn.createStatement().executeQuery(sql); 
      break;	    	    
    case 1: 
      //"delete from trade.txhistory where cid>? and cid<? ", 
      sql = "select * from trade.txhistory where cid>"+cid 
    		+" and cid<" +cid2;
      rs = conn.createStatement().executeQuery(sql); 
	    break;
    default:
     throw new TestException ("Wrong update statement here");
    }	
    processKeys(rs, keys);
	}	
	
	protected void getKeysForTidListDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int oid1, int oid2, int oid, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
  		//"delete from trade.txhistory where oid=? and tid=? ",	    
    	sql = "select * from trade.txhistory where oid=" + oid 
    		+ " and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      break;	    	    
    case 1: 
      //"delete from trade.txhistory where oid>? and oid<? and tid=? ", 
      sql = "select * from trade.txhistory where oid>"+oid1 
    		+" and oid<" +oid2 + " and tid=" + tid ;
	    rs = conn.createStatement().executeQuery(sql); 
	    break;
    default:
     throw new TestException ("Wrong update statement here");
    }	
    processKeys(rs, keys);
	}	


	public String getMyTableName() {
		return getTableName();
	}
	
  public static String getTableName() {
  	return "txhistory";
  }
  
  @SuppressWarnings("unchecked")
	public void insertTx(Connection dConn, Connection gConn,
			HashMap<String, Integer> modifiedKeys, ArrayList<SQLException> dExList,
			ArrayList<SQLException> sExList) {
		if (usingTrigger) {
			Log.getLogWriter().info("insert will be done in trigger"); 
			//need to figure out how to detect commit conflict using trigger
			return;
		}
		int size = 1;
    int[] cid = new int[size];
    int[] oid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    String[] type = new String[size];
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] price = new BigDecimal[size];
    int[] tid = new int[size];
    
    int txId = modifiedKeys.get(thisTxId);
    getCidAndTid(gConn, txId, cid, tid);
    getDataForInsert(gConn, oid, cid, sid, qty, type, time, price, size); //get the data    
    boolean opFailed = false;
    
    try {
    	insertToGFXDTable(gConn, oid[0], cid[0], sid[0], qty[0], type[0], time[0], price[0]);
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
    	String key = getTableName()+"_"+oid[0]+"_"+cid[0]+"_"+sid[0]+"_"+qty[0]+"_"+price[0]+
    		"_"+time[0]+"_"+type[0]+"_"+getMyTid();
    	if (modifiedKeys.get(key) == null) {
    		modifiedKeys.put(key, TXHISTORYINSERT);
    	}
    	//insert into table w/o pk won't cause commit conflict
    	//but this will have impact with update statement due to #41911
    	//most likely this insert will be committed out to avoid #41911, otherwise need to 
    	//compare the row/key was an update or an insert to determine if commit conflict should 
    	//be raised -- all insert no conflict, otherwise should throw commit conflict exception
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = new Object[9];    	
    	data[0] = DMLTxStmtsFactory.TRADE_TXHISTORY;
    	data[1] = insertToDerbyTable; //which operation
    	data[2] = oid[0];
    	data[3] = cid[0];
    	data[4] = sid[0];
    	data[5] = qty[0];
    	data[6] = type[0];
    	data[7] = time[0];
    	data[8] = price[0];
    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data);
    	SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    } 

	}
  
  protected boolean getDataForInsert(Connection conn, int[] oid, int[] cid, int[] sid, int[] qty, String[] type, 
      Timestamp[] time, BigDecimal[] price, int size) {
    boolean[] success = new boolean[1];
    ResultSet bors = getFromBO(conn, cid[0]);
    if (!success[0]) return false; //avoid resultSet not open error
    ArrayList<Struct> bolist = 
    	(ArrayList<Struct>) ResultSetHelper.asList(bors, SQLHelper.isDerbyConn(conn));
    
    ResultSet sors = getFromSO(conn, cid[0]);
    if (!success[0]) return false; //avoid resultSet not open error    
    ArrayList<Struct> solist = 
    	(ArrayList<Struct>) ResultSetHelper.asList(sors, SQLHelper.isDerbyConn(conn));
    
    if (bolist != null && solist != null) {
      if (solist.size()!=0 && bolist.size()!=0) {
        if (rand.nextInt()%2 == 0)  getDataForInsertFromSO(solist, oid, cid, sid, qty, type, time, price, size);
        else getDataForInsertFromBO(bolist, oid, cid, sid, qty, type, time, price, size);
      } //randomly populate data from buyorders or sellorders
      else if (solist.size()!= 0) getDataForInsertFromSO(solist, oid, cid, sid, qty, type, time, price, size);
      else if (bolist.size()!= 0) getDataForInsertFromBO(bolist, oid, cid, sid, qty, type, time, price, size);
      else return false;
    }
    else if (bolist == null) {
      if (solist.size()!= 0) getDataForInsertFromSO(solist, oid, cid, sid, qty, type, time, price, size);
      else return false;
    }
    else if (solist ==null){
      if (bolist.size()!= 0) getDataForInsertFromBO(bolist, oid, cid, sid, qty, type, time, price, size);
      else return false;
    }
    else {
      return false;
    }
    
    return true;
  }
  
  protected ResultSet getFromBO(Connection conn, int cid) {
  	String sql = null;
  	if (byCidRange)
  		sql = "select * from trade.buyorders where cid=" + cid + " and status='filled'";
  	else if (byTidList)
  		sql = "select * from trade.buyorders where tid=" + getMyTid() + " and status='filled'";
  	else 
  		sql = "select * from trade.buyorders where status='filled'";
  	
  	try {
  		return conn.createStatement().executeQuery(sql);
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	} 	
  	return null;	//won't reach it here 	
  }
  
  protected ResultSet getFromSO(Connection conn, int cid) {
  	String sql = null;
  	if (byCidRange)
  		sql = "select * from trade.sellorders where cid=" + cid + " and status='filled'";
  	else if (byTidList)
  		sql = "select * from trade.sellorders where tid=" + getMyTid() + " and status='filled'";
  	else
  		sql = "select * from trade.sellorders where status='filled'";
  	
  	try {
  		return conn.createStatement().executeQuery(sql);
  	} catch (SQLException se) {
  		SQLHelper.handleSQLException(se);
  	} 	
  	return null;	//won't reach it here 	
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
		if (!usingTrigger) {
			int size = 30;
	    populateTx (dConn, gConn, size); 
		}
	}
	
  //populate the table
  public void populateTx (Connection dConn, Connection gConn, int initSize) {
  	insert(dConn, gConn, initSize);
  }
  
  public void insert(Connection dConn, Connection gConn, int size) {
    int[] cid = new int[size];
    int[] oid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    String[] type = new String[size];
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] price = new BigDecimal[size];

    boolean success = false;
    getDataForInsert(gConn, oid, cid, sid, qty, type, time, price, size); //get the data
    SQLException derbySe = null;
    SQLException gfxdSe = null;    
    
    if (dConn != null) {	    	    	
	    for (int i=0; i<size; i++) {
	    	//insert to each table
	    	try {
	    		insertToGFXDTable(gConn, oid[i], cid[i], sid[i], qty[i], 
	    				type[i], time[i], price[i]); 
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		gfxdSe = se; 
	    	}
	    	try {
	    		success = insertToDerbyTable(dConn, oid[i], cid[i], sid[i], 
	    				qty[i], type[i], time[i], price[i]); 
	        int count = 0;
	        while (!success) {
	          if (count>=maxNumOfTries) {
	            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
	            gConn.rollback();
	            dConn.rollback();
	            break;
	          }
	          success = insertToDerbyTable(dConn, oid[i], cid[i], sid[i],
	          		qty[i], type[i], time[i], price[i]);  //retry insert to derby table         
	          
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
	        insertToGFXDTable(gConn, oid[i], cid[i], sid[i], qty[i],
	        		type[i], time[i], price[i]); 
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
      String type, Timestamp time, BigDecimal price) throws SQLException{
	  PreparedStatement stmt = getStmt(conn, insert);
	  int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    count = insertToTable(stmt, oid, cid, sid, qty, type, time, price, tid);
    Log.getLogWriter().info("gfxd inserts " + count + " record");
  }
  
  //for derby insert
  protected boolean insertToDerbyTable(Connection conn, int oid, int cid, int sid, int qty,
  String type, Timestamp time, BigDecimal price) throws SQLException{
  	PreparedStatement stmt = getStmt(conn, insert);
  	int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    try {
    	count = insertToTable(stmt, oid, cid, sid, qty, type, time, price, tid);
      Log.getLogWriter().info("derby inserts " + count + " record");
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
    BigDecimal price = getPrice();
    int qty = getQty();
    int sid = getSid();
    int newSid = getSid();
    int whichUpdate = getWhichTxUpdate();
    if (whichUpdate <0) {
    	Log.getLogWriter().info("Does not have single row operation in no primary key table");
    	return;
    }
    int tid2 = 0;
    if (byTidList) tid2=sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    int oid=0;
    if (byTidList) {
    	oid = getOidByTid(gConn, tid);
    }
    else if (byCidRange) {
    	oid = getOidByCid(gConn, cid);
    }
    int newOid = oid + rand.nextInt(100);
       
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int smallCid = 0;
    int largeCid = 0;
    
    if (byCidRange) {
    	int low= GFXDTxHelper.getRangeEnds(getTableName(), cid)[0];
    	int high = GFXDTxHelper.getRangeEnds(getTableName(), cid)[1];
    	smallCid = rand.nextInt(high-low) + low;
    	largeCid = smallCid + 10;
    }

    boolean opFailed = false;   
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeUpdate(gConn, keys, whichUpdate, cid, sid, oid, price, 
        			smallCid, largeCid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		updateToGFXDTableCidRangeTx(gConn, cid, sid, newSid, oid, newOid, price, qty,
    				smallCid, largeCid, whichUpdate);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListUpdate(gConn, keys, whichUpdate, cid, sid, oid, price,
        		 tid, tid2, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableTidListTx(gConn, cid, sid, newSid, oid, newOid, price, qty,
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
	    	data = new Object[12];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_TXHISTORY;
	    	data[1] = updateToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = newSid;
	    	data[5] = oid;
	    	data[6] = newOid;
	    	data[7] = price;
	    	data[8] = qty;
	    	data[9] = smallCid;
	    	data[10] = largeCid;
	    	data[11] = whichUpdate;
    	} else if (byTidList) {
	    	data = new Object[12];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_TXHISTORY;
	    	data[1] = updateToDerbyTableTidListTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = newSid;
	    	data[5] = oid;
	    	data[6] = newOid;
	    	data[7] = price;
	    	data[8] = qty;
	    	data[9] = whichUpdate; 
	    	data[10] = tid;
	    	data[11] = tid2;
    	}
    			
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
      SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }	
	}
	
	protected void getKeysForCidRangeUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, int sid, int oid, BigDecimal price, int smallCid,
			int largeCid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
  
    switch (whichUpdate) {
    case 0: 
  		//"update trade.txhistory set oid = ? where cid = ? and oid= ? ",
    	sql = "select * from trade.txhistory where cid="+cid + " and oid=" +oid;
      rs = conn.createStatement().executeQuery(sql); 
      break;	    	    
    case 1: 
      //"update trade.txhistory set price = ? , qty=? where cid = ? and oid= ? and price <? ",
    	sql = "select * from trade.txhistory where cid="+cid + " and oid=" +oid 
  	  + " and price<" + price;
	    rs = conn.createStatement().executeQuery(sql); 	    
	    break;
    case 2:
      //"update trade.txhistory set sid = ?  where cid>? and cid<? and sid = ? and price <?",
      sql = "select * from trade.txhistory where cid>"+smallCid 
    		+" and cid<" +largeCid + " and sid = " +sid + " and price<" + price;
	    rs = conn.createStatement().executeQuery(sql); 
	    break;
	  case 3: 
    //"update trade.txhistory set price = ? where cid>? and cid<? and sid = ?  ",  
    	sql = "select * from trade.txhistory where cid>? and cid<? and sid=?";
    	PreparedStatement ps = conn.prepareStatement(sql);
    	ps.setInt(1, smallCid);
    	ps.setInt(2, largeCid);
    	ps.setInt(3, sid);
	    rs = ps.executeQuery(); 
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
    processKeys(rs, keys);
	}	
	
	protected void processKeys(ResultSet rs, HashMap<String, Integer> keys) 
		throws SQLException {
    int _oid =0;
    int _cid=0;
    int _sid=0;
    int _qty=0;
    BigDecimal _price = null;
    String _type = null;
    Timestamp _time = null;
    int _tid = 0;
    
    while (rs.next()) {
     	_oid = rs.getInt("OID");
     	_cid = rs.getInt("CID");
     	_sid = rs.getInt("SID");
     	_qty = rs.getInt("QTY");
     	_price = rs.getBigDecimal("PRICE");
     	_time = rs.getTimestamp("ORDERTIME");
     	_type = rs.getString("TYPE");
     	_tid = rs.getInt("TID");
       Log.getLogWriter().info("oid: " + _oid +
       		"cid: " + _cid +
       		"sid: " + _sid +
       		"qty: " + _qty +
       		"price: " + _price +
       		"ordertime: " + _time +
       		"type: " + _type +
       		"tid: " + _tid +
       		" exists for update");
     	String key = getTableName()+"_"+_oid+"_"+_cid+"_"+ _sid
     		+"_"+_qty+"_"+_price+"_"+_time+"_"+_type+"_"+_tid;
     	keys.put(key, TXHISTORYUPDATE);
     }    
     rs.close();
	}
	
	protected void updateToGFXDTableCidRangeTx(Connection conn,  int cid, int sid, 
			int newSid, int oid, int newOid, BigDecimal price, int qty, int smallCid, 
			int largeCid, int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update txhistory table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    count = updateToTableCidRangeTx(stmt, cid, sid, newSid, oid, newOid, price, qty, 
    		smallCid, largeCid, whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableCidRangeTx(Connection conn,  int cid, int sid, 
			int newSid, int oid, int newOid, BigDecimal price, int qty, int smallCid, 
			int largeCid, int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update txhistory table in derby, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    try {
    	count = updateToTableCidRangeTx(stmt, cid, sid, newSid, oid, newOid, price, qty, 
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
	
	protected int updateToTableCidRangeTx(PreparedStatement stmt, int cid, int sid, int newSid,
			int oid, int newOid, BigDecimal price, int qty, int smallCid, int largeCid,
			int whichUpdate) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.txhistory set oid = ? where cid = ? and oid= ? ",
      Log.getLogWriter().info("updating txhistory table oid to " + newOid +" for cid: "
      		+ cid + " and oid: " + oid );
      stmt.setInt(1, newOid);
      stmt.setInt(2, cid);
      stmt.setInt(3, oid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"update trade.txhistory set price = ? , qty=? where cid = ? and oid= ? and price <? ",
      Log.getLogWriter().info("updating txhistory table price to " + price +
      		" and  qty to " + qty + " for cid: "+ cid + " and oid: " + oid + 
      		" and price < " + price);
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, qty);
      stmt.setInt(3, cid);
      stmt.setInt(4, oid);
      stmt.setBigDecimal(5, price);
     rowCount =  stmt.executeUpdate();
     break;
    case 2: 
      // "update trade.txhistory set sid = ?  where cid>? and cid<? and sid = ? and price <?",
      Log.getLogWriter().info("updating txhistory table sid to " + newSid +
      		" for cid: " + smallCid + " to " + largeCid + " and sid: " + sid +
      		" and price < " + price);
      stmt.setInt(1,newSid);
      stmt.setInt(2, smallCid);
      stmt.setInt(3, largeCid);
      stmt.setInt(4, sid);
      stmt.setBigDecimal(5, price);
      rowCount = stmt.executeUpdate();
      break; 
    case 3: 
		  //  "update trade.txhistory set price = ? where cid>? and cid<? and sid = ?  ", 
      Log.getLogWriter().info("updating txhistory table price to " + price +
      		" for cid: " + smallCid + " to " + largeCid +
      		" and sid: " + sid);
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, smallCid);
      stmt.setInt(3, largeCid);
      stmt.setInt(4, sid);
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
			int whichUpdate, int cid, int sid, int oid, BigDecimal price, 
			int tid, int tid2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.txhistory set oid = ? where oid= ? and tid=? ",
    	sql = "select * from trade.txhistory where oid=" + oid 
  	  	+ " and tid=" + tid;
	    rs = conn.createStatement().executeQuery(sql); 
	    break;
    case 1: 
      // "update trade.txhistory set price = ? , qty=? where oid= ? and price <? and tid =? ",
    	sql = "select * from trade.txhistory where oid=" +oid 
  	  	+ " and price<" + price + " and tid=" + tid;
	    rs = conn.createStatement().executeQuery(sql); 
	    break;
    case 2: 
      //"update trade.txhistory set sid = ?  where sid = ? and price<? and (tid =? or tid=? )",
      sql = "select * from trade.txhistory where sid="+sid + " and price<" + price  
      	+ " and (tid=" + tid +" or tid=" + tid2 + ")";
	    rs = conn.createStatement().executeQuery(sql); 
	    break;
    case 3:
    	//"update trade.txhistory set price = ? where sid <? and tid =? ",
    	sql = "select * from trade.txhistory where sid< ? and tid =? ";
    	PreparedStatement ps = conn.prepareStatement(sql);
    	ps.setInt(1, sid);
    	ps.setInt(2, tid);
	    rs = ps.executeQuery(); 
	    break;
    default:
     throw new TestException ("Wrong update statement here");
    }	
    processKeys(rs, keys);
	}
	
	protected void updateToGFXDTableTidListTx(Connection conn, int cid, int sid,  
			int newSid, int oid, int newOid, BigDecimal price, int qty, int whichUpdate, 
			int tid, int tid2) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update txhistory table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    count = updateToTableTidListTx(stmt, cid, sid, newSid, oid, newOid, price, qty,
    		whichUpdate, tid, tid2);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableTidListTx(Connection conn, int cid, int sid,  
			int newSid, int oid, int newOid, BigDecimal price, int qty, int whichUpdate, 
			int tid, int tid2) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update txhistory table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    try {
    	count = updateToTableTidListTx(stmt, cid, sid, newSid, oid, newOid, price, qty, 
    			whichUpdate, tid, tid2);
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
			int sid, int newSid, int oid, int newOid, BigDecimal price, int qty, 
			int whichUpdate, int tid, int tid2) throws SQLException {
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.txhistory set oid = ? where oid= ? and tid=? ", 	    
      Log.getLogWriter().info("updating txhistory table oid to " + newOid 
      		+" for  oid: " + oid + " and tid=" + tid);
      stmt.setInt(1, newOid);
      stmt.setInt(2, oid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      //"update trade.txhistory set price = ? , qty=? where oid= ? and price <? and tid =? ",
      Log.getLogWriter().info("updating txhistory table price to " + price +
      		" and  qty to " + qty + " for oid: " + oid + 
      		" and price < " + price + " and tid=" + tid);
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, qty);
      stmt.setInt(3, oid);
      stmt.setBigDecimal(4, price);
      stmt.setInt(5, tid);
      rowCount =  stmt.executeUpdate();            
      break;
    case 2: 
	    // "update trade.txhistory set sid = ?  where sid = ? and price<? and (tid =? or tid=? )",  
      Log.getLogWriter().info("updating txhistory table sid to " + newSid +  
      		" for sid: " + sid +	" and price < " + price +
      		"and (tid=" + tid + " or tid=" + tid2 + ")" );
      stmt.setInt(1, newSid);
      stmt.setInt(2, sid);
      stmt.setBigDecimal(3, price);
      stmt.setInt(4, tid);
      stmt.setInt(5, tid2);
      rowCount = stmt.executeUpdate();
      break;
    case 3:
    	// "update trade.txhistory set price = ? where sid <? and tid =? ",         
      Log.getLogWriter().info("updating txhistory table price to " + price +
      		" for sid< " + sid + " and tid=" +tid);
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, sid);
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
	
	protected int getOidByCid(Connection gConn, int cid) {
  	ResultSet rs = null;
  	List<Struct> result = null;
		try {
      String sql="select oid from trade.txhistory where cid= " + cid;
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
      String sql="select oid from trade.txhistory where tid= " + tid;
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
		else if (byTidList && !singleRowTx)
			return rand.nextInt(updateByTidList.length);
		else 
			return -1;
	}
	
	//return correct delete stmt based on whether stmt only update on single row
	protected int getWhichTxDelete() {
		if (byCidRange && !singleRowTx) 
			return rand.nextInt(deleteByCidRange.length);
		else if (byTidList && !singleRowTx)
			return rand.nextInt(deleteByTidList.length);
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
	  	  	//String type, Timestamp time, BigDecimal price)
	  		break;
	  	case updateToDerbyTableCidRangeTx: 
	  		success = updateToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (Integer)data[5], (Integer) data[6], (BigDecimal) data[7], 
	  				(Integer) data[8], (Integer)data[9], (Integer)data[10], (Integer)data[11]);
	  		//Connection conn,  int cid, int sid, 
				//int newSid, int oid, int newOid, BigDecimal price, int qty, int smallCid, 
				//int largeCid, int whichUpdate)
	  		break;
	  	case updateToDerbyTableTidListTx: 
	  		success = updateToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (Integer)data[5], (Integer) data[6], (BigDecimal) data[7], 
	  				(Integer) data[8], (Integer)data[9], (Integer)data[10], (Integer)data[11]);
	  		//(Connection conn, int cid, int sid,  
				//int newSid, int oid, int newOid, BigDecimal price, int qty, int whichUpdate, 
				//int tid, int tid2)
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

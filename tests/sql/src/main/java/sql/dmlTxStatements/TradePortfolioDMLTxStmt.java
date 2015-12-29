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

import sql.SQLBB;
import sql.SQLHelper;
import sql.dmlStatements.TradePortfolioDMLStmt;
import sql.sqlTx.SQLDerbyTxBB;
import sql.sqlutil.DMLTxStmtsFactory;
import sql.sqlutil.GFXDTxHelper;
import util.TestException;
import util.TestHelper;

public class TradePortfolioDMLTxStmt extends TradePortfolioDMLStmt implements
		DMLTxStmtIF {
	
	String[] updateByCidRange = {
      "update trade.portfolio set availQty=availQty-100 where cid = ? and sid = ? ", 
      "update trade.portfolio set qty=qty-? where cid = ? and sid = ? ",
      "update trade.portfolio set tid = ?  where cid = ? and sid = ? ",
      //multiple rows
      "update trade.portfolio set subTotal = ? * qty  where cid>=? and cid <? and sid = ? ",
	};
	
	int updateByCidRangeSingleRowStmt = 3;
	
	String[] updateByTidList = {
      "update trade.portfolio set availQty=availQty-100 where cid = ? and sid = ?  and tid= ?", 
      "update trade.portfolio set qty=qty-? where cid = ? and sid = ?  and tid= ?",
      //multiple rows
      "update trade.portfolio set subTotal = ? * qty  where sid = ?  and tid= ?",
	};
	
	int updateByTidListSingleRowStmt = 2;
	
	String[] deleteByCidRange = {
			"delete from trade.portfolio where cid=? and sid=?",
      //multiple rows
      "delete from trade.portfolio where qty < 100",
      
	};
	
	int deleteByCidRangeSingleRowStmt = 1;
	
	String[] deleteByTidList = {
      "delete from trade.portfolio where cid=? and sid=? and tid=?",
      //multiple rows
      "delete from trade.portfolio where qty <100 and tid=?",
	};
	
	int deleteByTidListSingleRowStmt = 1;

	@SuppressWarnings("unchecked")
	public void deleteTx(Connection dConn, Connection gConn,
			HashMap<String, Integer> modifiedKeys, ArrayList<SQLException> dExList,
			ArrayList<SQLException> sExList) {
		int size =1;
		int[] cid = new int[size];
		int[] sid = new int[size];
		int[] tid = new int[size];
		HashMap<String, Integer> keys = new HashMap<String, Integer>();
		int txId = modifiedKeys.get(thisTxId);
    int whichDelete = getWhichTxDelete();
    
		getCidAndTid(gConn, txId, cid, tid);
		try {
			sid[0] = getSid(gConn, cid[0]);
		} catch (SQLException se) {
			SQLHelper.handleSQLException(se);
		}
    boolean opFailed = false;
    
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeDelete(gConn, keys, whichDelete, cid[0], sid[0], txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		deleteToGFXDTableCidRangeTx(gConn, cid[0], sid[0], whichDelete);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListDelete(gConn, keys, whichDelete, cid[0], sid[0], tid[0], txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		deleteToGFXDTableTidListTx(gConn, cid[0], sid[0], whichDelete, tid[0]);
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
	    	data = new Object[5];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_PORTFOLIO;
	    	data[1] = deleteToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid[0];
	    	data[3] = sid[0];
	    	data[4] = whichDelete;
    	} else if (byTidList) {
	    	data = new Object[6];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_PORTFOLIO;
	    	data[1] = deleteToDerbyTableTidListTx; //which operation
	    	data[2] = cid[0];
	    	data[3] = sid[0];
	    	data[4] = whichDelete;
	    	data[5] = tid[0];    		
    	}
    	    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
        SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }

	}
	
	protected void getKeysForCidRangeDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int sid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //			"delete from trade.portfolio where cid=? and sid=?",
    	sql = "select cid, sid from trade.portfolio where cid="+cid + " and sid=" +sid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " and sid: " + sid +" exists for update");
      	keys.put(getTableName()+"_"+rs.getInt("CID")+"_"+rs.getInt("SID"), txId);
      }    
      rs.close();
      break;
    case 1: 
	    //      "delete from trade.portfolio where qty < 100", //multiple records
      sql = "select cid, sid from trade.portfolio where qty<100";
	    rs = conn.createStatement().executeQuery(sql); 
	    while (rs.next()) {
	    	int availCid = rs.getInt("CID");
	    	int availSid = rs.getInt("SID");
	      Log.getLogWriter().info("cid: " + availCid + 
	      		" and sid: " + availSid +" exists for update");
	    	keys.put(getTableName()+"_"+availCid+"_"+availSid, txId);
	    }          
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void deleteToGFXDTableCidRangeTx(Connection conn, int cid, int sid, 
			 int whichDelete) throws SQLException {
	  //for gemfirexd checking
   PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
   int tid = getMyTid();
   int count = -1;
   Log.getLogWriter().info("delete portfolio table in gemfirexd, myTid is " + tid);
   Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
   count = deleteToTableCidRangeTx(stmt, cid, sid, whichDelete);
   Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
	}
	
	protected int deleteToTableCidRangeTx(PreparedStatement stmt, int cid, int sid, 
			int whichDelete) throws SQLException {
   int rowCount = 0;
   switch (whichDelete) {
   case 0: 
 		//"delete from trade.portfolio where cid=? and sid=?",
     Log.getLogWriter().info("delete from portfolio for cid: " + cid + " and sid: " + sid);
     stmt.setInt(1, cid);
     stmt.setInt(2, sid);
     rowCount = stmt.executeUpdate();    
     break;
   case 1: 
	    // "delete from trade.portfolio where qty < 100",  //multiple records
     Log.getLogWriter().info("delete from portfolio for qty <100 ");
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
	
	protected boolean deleteToDerbyTableCidRangeTx(Connection conn, int cid, int sid, 
			 int whichDelete) throws SQLException {
   PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
   int tid = getMyTid();
   int count = -1;
   Log.getLogWriter().info("delete porfolio table in derby, myTid is " + tid);
   Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
   try {
   	count = deleteToTableCidRangeTx(stmt, cid, sid, whichDelete);
   } catch (SQLException se) {
     if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
       Log.getLogWriter().info("detected the lock issue, will try it again");
       return false;
     } else throw se;
   }
   Log.getLogWriter().info("derby deleted " + count + " rows");
   return true;
	}
	
	protected void getKeysForTidListDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int cid, int sid, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //"delete from trade.portfolio where cid=? and sid=? and tid=?",      
    	sql = "select cid, sid from trade.portfolio where cid="+cid +
    		" and sid=" +sid + " and tid=" + tid;
    	rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " and sid: " + sid +" exists for update");
      	keys.put(getTableName()+"_"+rs.getInt("CID")+"_"+rs.getInt("SID"), txId);
      }    
      rs.close();
      break;
    case 1: 
			//"delete from trade.portfolio where qty <100 and tid=?",
      sql = "select cid, sid from trade.portfolio where qty<100 and tid=" + tid;
	    rs = conn.createStatement().executeQuery(sql); 
	    while (rs.next()) {
	    	int availCid = rs.getInt("CID");
	    	int availSid = rs.getInt("SID");
	      Log.getLogWriter().info("cid: " + availCid + 
	      		" and sid: " + availSid +" exists for update");
	    	keys.put(getTableName()+"_"+availCid+"_"+availSid, txId);
	    }          
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void deleteToGFXDTableTidListTx(Connection conn, int cid, int sid, 
			int whichDelete, int tid) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;    
    Log.getLogWriter().info("delete portfolio table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    count = deleteToTableTidListTx(stmt, cid, sid, whichDelete, tid);
    Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
		
	}
	
	protected boolean deleteToDerbyTableTidListTx(Connection conn, int cid,  
			int sid, int whichDelete, int tid) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
    int count = -1;
    Log.getLogWriter().info("delete portfolio table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
    try {
    	count = deleteToTableTidListTx(stmt, cid, sid, whichDelete, tid);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby deleted " + count + " rows");
    return true;
	}
	
	protected int deleteToTableTidListTx(PreparedStatement stmt, int cid, int sid,
			int whichDelete, int tid) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
			//"delete from trade.portfolio where cid=? and sid=? and tid=?", 
      Log.getLogWriter().info("delete from portfolio for cid: " + cid + 
      		" and sid: " + sid + " and tid: " + tid);
      stmt.setInt(1, cid);
      stmt.setInt(2, sid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
			//"delete from trade.portfolio where qty <100 and tid=?",
      Log.getLogWriter().info("delete from portfolio for qty<100 and tid: " + tid);
      stmt.setInt(1, tid);
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
  	return "portfolio";
  }

  @SuppressWarnings("unchecked")
	public void insertTx(Connection dConn, Connection gConn,
			HashMap<String, Integer> modifiedKeys, ArrayList<SQLException> dExList,
			ArrayList<SQLException> sExList) {

		int size = 1;
		int[] cid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    int[] tid = new int[size];
    BigDecimal[] sub = new BigDecimal[size];

    getDataForInsert(gConn, sid, qty, sub, size); //get the data
    int txId = modifiedKeys.get(thisTxId);
    getCidAndTid(gConn, txId, cid, tid);
    boolean opFailed = false;
    
    try {
    	insertToGFXDTable(gConn, cid[0], sid[0], qty[0], sub[0]);
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
    	modifiedKeys.put(getTableName()+"_"+cid[0]+"_"+sid[0], txId);
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = new Object[6];    	
    	data[0] = DMLTxStmtsFactory.TRADE_PORTFOLIO;
    	data[1] = insertToDerbyTable; //which operation
    	data[2] = cid[0];
    	data[3] = sid[0];
    	data[4] = qty[0];
    	data[5] = sub[0];
    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data);
    	SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    } 
  }
	
  /**
   * The method got specific key from BB and insert a new record
   */
  public void insert(Connection dConn, Connection gConn, int size) {
    for (int i =0; i<size; i++) {
    	insert (dConn, gConn, size, getCid());
    }   
  }
  
  public void insert(Connection dConn, Connection gConn, int size, int cid) {
    int[] sid = new int[size];
    int[] qty = new int[size];
    BigDecimal[] sub = new BigDecimal[size];

    boolean success = false;
    getDataForInsert(gConn, sid, qty, sub, size); //get the data
    SQLException derbySe = null;
    SQLException gfxdSe = null;
    if (dConn != null) {	    	    	
	    for (int i=0; i<size; i++) {
	    	//insert to each table
	    	try {
	    		insertToGFXDTable(gConn, cid, sid[i], qty[i], sub[i]); 
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		gfxdSe = se; 
	    	}
	    	try {
	    		success = insertToDerbyTable(dConn, cid, sid[i], qty[i], sub[i]); 
	        int count = 0;
	        while (!success) {
	          if (count>=maxNumOfTries) {
	            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
	            gConn.rollback();
	            dConn.rollback();
	            break;
	          }
	          success = insertToDerbyTable(dConn, cid, sid[i], qty[i], sub[i]);  //retry insert to derby table         
	          
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
			    	  //sid may not exist due to unique key constraint violation, as we get sid randomly instead of querying
			    		//currently product throw in doubt transaction, which should be changed after #41898 is fixed
					    if (derbySe !=null  && SQLHelper.isSameRootSQLException(derbySe.getSQLState(), se)) {
					    	if (derbySe.getSQLState().equals("23505"))
					    		Log.getLogWriter().warning("got duplicate primary key exception and masked as" +
					    				" in doubt transaction exception -- primary key should be checked at operation time ");
					    	else if (derbySe.getSQLState().equals("23503"))
					    		Log.getLogWriter().warning("got foreign key check violation and masked as" +
			    				" in doubt transaction exception -- which is bug# 41898 ");
					    	
						    derbySe = null;
						    gfxdSe = null;
						    continue;
					    } 
			    	} else {
			    		SQLHelper.handleSQLException(se);
			    	}
			    	//not expect any exception during commit time except commit conflict exception
			    	//as two threads might get same cid and sid
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
    	try {
        insertToGFXDTable(gConn, cid, sid, qty, sub, size); 
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
  
  //for gemfirexd checking
  protected void insertToGFXDTable(Connection conn, int cid, int sid, int qty,
  		BigDecimal sub) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    count = insertToTable(stmt, cid, sid, qty, sub, tid); 
    Log.getLogWriter().info("gfxd inserts " + count + " record");
  }
  
  //insert multiple
  protected void insertToGFXDTable(Connection gConn, int cid, int[] sid, int[] qty, 
  		BigDecimal[] sub, int size) throws SQLException {
    PreparedStatement stmt = gConn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    for (int i=0; i<size; i++) {
    	count = insertToTable(stmt, cid, sid[i], qty[i], sub[i], tid); 
    	Log.getLogWriter().info("gfxd inserts " + count + " record");
    }
  }
  
  //for derby insert
  protected boolean insertToDerbyTable(Connection conn, int cid, int sid, int qty,
  		BigDecimal sub) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    try {
      count = insertToTable(stmt, cid, sid, qty, sub, tid); 
      Log.getLogWriter().info("derby inserts " + count + " record");
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry   
      else throw se;
    }
    return true;
  }
  
  protected void getDataForInsert(Connection gConn, int[] sid, int[] qty, 
		  BigDecimal[] sub, int size) {
    try {
      for (int i = 0; i<size; i++) {
        sid[i] = getSid();
        qty[i] = getQty(); //from 100 to 2000
        sub[i] = getPrice(gConn, sid[i]).multiply(new BigDecimal(String.valueOf(qty[i])));
      }
    } catch (SQLException se) {
    	SQLHelper.handleSQLException(se); //throws TestException.
    } 
  }
  
  //TODO to check if any subTotal is negative
  //return negative price if the sec_id does not exists, which should not occur
  //still there should be no negative subtotal in the db
  protected BigDecimal getPrice(Connection gConn, int sid) throws SQLException{
  	ResultSet rs = null;
  	try {
  		BigDecimal price;
      String sql="select price from trade.securities where sec_id = " + sid;
      rs = gConn.createStatement().executeQuery(sql);        
      if (rs!=null && rs.next()) {
        price = rs.getBigDecimal("price");  //should always return 1 record unless the record is deleted
        return price;
      } 
  	}catch (SQLException se) {
    	SQLHelper.handleSQLException(se); //throws TestException.
    } finally {
    	if (rs != null) rs.close();
    }
    return new BigDecimal(-1); 
    //if sec_id is deleted, there should be no child table reference to it
    //The insert will fail due to primary key reference (used for calculating subtotal)
    //The update should never happen as delete restrict should prevent sec_id being deleted
  }
  
  
  //find cid, sid pair in the db to be updated
  protected int getSid(Connection gConn, int cid) throws SQLException{
  	ResultSet rs = null;
  	int sid = 0;
  	try {
      String sql="select sid from trade.portfolio where cid = " + cid;
      rs = gConn.createStatement().executeQuery(sql);   
      int count = 0;
      int whichOne = rand.nextInt(10);
      if (rs!=null && rs.next()) {
        sid = rs.getInt(1);  
        if (count == whichOne) return sid;
        ++count;
      } 
  	}catch (SQLException se) {
    	SQLHelper.handleSQLException(se); //throws TestException.
    } finally {
    	if (rs != null) rs.close();
    }
    return sid;     
  }

	public void populateTx(Connection dConn, Connection gConn) {
		int size = 10;
    populateTx (dConn, gConn, size); 
	}
	
  //populate the table
  public void populateTx (Connection dConn, Connection gConn, int initSize) {
  	insert(dConn, gConn, initSize);
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
    
    int sid =0;
    BigDecimal price = null;
    int whichUpdate = getWhichTxUpdate();
    try {
	    sid = getSid(gConn, cid);
	    price = getPrice(gConn, sid);
    } catch (SQLException se) {
    	SQLHelper.handleSQLException(se);
    }
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int qty = getQty();
    int smallCid = 0;
    int largeCid = 0;
    /*following only used in cidRange*/
    if (byCidRange) {
    	smallCid= GFXDTxHelper.getRangeEnds(getTableName(), cid)[0];
    	largeCid = GFXDTxHelper.getRangeEnds(getTableName(), cid)[1];
    }

    boolean opFailed = false;   
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeUpdate(gConn, keys, whichUpdate, cid, sid, price,
        			smallCid, largeCid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		updateToGFXDTableCidRangeTx(gConn, cid, sid, price, qty, smallCid, largeCid,
    	       whichUpdate);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListUpdate(gConn, keys, whichUpdate, cid, sid, price, tid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableTidListTx(gConn, cid, sid, price, qty, whichUpdate, tid);
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
	    	data = new Object[9];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_PORTFOLIO;
	    	data[1] = updateToDerbyTableCidRangeTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = price;
	    	data[5] = qty;
	    	data[6] = smallCid;
	    	data[7] = largeCid;
	    	data[8] = whichUpdate;
    	} else if (byTidList) {
	    	data = new Object[8];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_PORTFOLIO;
	    	data[1] = updateToDerbyTableTidListTx; //which operation
	    	data[2] = cid;
	    	data[3] = sid;
	    	data[4] = price;
	    	data[5] = qty;
	    	data[6] = whichUpdate; 
	    	data[7] = tid;
    	}
    			
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
        SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }	

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
	
  //find  a cid which was put by this tid
  protected int getCidByList(Connection conn, int tid) {
  	String sql = "select cid from trade.portfolio where tid = ?";
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
			int whichUpdate, int cid, int sid, BigDecimal price, int smallCid,
			int largeCid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.portfolio set availQty=availQty-100 where cid = ? and sid = ? ",   
    case 1: 
      //"update trade.portfolio set qty=qty-? where cid = ? and sid = ? ",
    case 2:
      //"update trade.portfolio set tid = ?  where cid = ? and sid = ? ",
    	sql = "select cid, sid from trade.portfolio where cid="+cid + " and sid=" +sid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " and sid: " + sid +" exists for update");
      	keys.put(getTableName()+"_"+rs.getInt("CID")+"_"+rs.getInt("SID"), txId);
      }    
      rs.close();
      break;
    case 3: 
      //"update trade.portfolio set subTotal = ? * qty  where cid>? and cid <? and sid = ? ",	
      sql = "select cid, sid from trade.portfolio where cid>"+smallCid 
      	+" and cid<" +largeCid + " and sid=" + sid;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availCid = rs.getInt("CID");
      	int availSid = rs.getInt("SID");
        Log.getLogWriter().info("cid: " + availCid + 
        		" and sid: " + availSid +" exists for update");
      	keys.put(getTableName()+"_"+availCid+"_"+availSid, txId);
      }          
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void getKeysForTidListUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int cid, int sid, BigDecimal price, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
  		//"update trade.portfolio set availQty=availQty-100 where cid = ? and sid = ?  and tid= ?",        
    case 1: 
      //"update trade.portfolio set qty=qty-? where cid = ? and sid = ?  and tid= ?",
    	sql = "select cid, sid from trade.portfolio where cid="+cid
    		+ " and sid=" +sid + " and tid = " + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("cid: " + cid + " and sid: " + sid +" exists for update");
      	keys.put(getTableName()+"_"+rs.getInt("CID")+"_"+rs.getInt("SID"), txId);
      }    
      rs.close();
      break;
    case 2: 
      //"update trade.portfolio set subTotal = ? * qty  where sid = ?  and tid= ?",		
      sql = "select cid, sid from trade.portfolio where sid=" + sid + " and tid = " + tid;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availCid = rs.getInt("CID");
      	int availSid = rs.getInt("SID");
        Log.getLogWriter().info("cid: " + availCid + 
        		" and sid: " + availSid +" exists for update");
      	keys.put(getTableName()+"_"+availCid+"_"+availSid, txId);
      }          
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void getCidAndTid(Connection gConn, int txId, int[] cid, int[] tid) {
    if (byTidList) {
    	tid[0] = sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    	cid[0] = getCidByList(gConn, tid[0]); //use this cid instead
    }
    if (byCidRange) {
    	Object txIdCid = SQLBB.getBB().getSharedMap().get(cid_txId + txId);
    	cid[0] = sql.sqlutil.GFXDTxHelper.getColocatedCid(getTableName(), (Integer)txIdCid);
    } 
	}
	
	protected void updateToGFXDTableCidRangeTx(Connection conn,  int cid, int sid, 
			BigDecimal price, int qty, int smallCid, int largeCid, int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update portfolio table in gemfirexd, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    count = updateToTableCidRangeTx(stmt, cid, sid, price, qty, smallCid, largeCid,
	       whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableCidRangeTx(Connection conn,  int cid, int sid, 
			BigDecimal price, int qty, int smallCid, int largeCid, int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("update portfolio table in derby, myTid is " + tid);
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    try {
    	count = updateToTableCidRangeTx(stmt, cid, sid, price, qty, smallCid, largeCid,
 	       whichUpdate);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	protected void updateToGFXDTableTidListTx(Connection conn, int cid, int sid,  
			BigDecimal price, int qty, int whichUpdate, int tid) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update portfolio table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    count = updateToTableTidListTx(stmt, cid, sid, price, qty, whichUpdate, tid);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableTidListTx(Connection conn, int cid, int sid,  
			BigDecimal price, int qty, int whichUpdate, int tid) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update portfolio table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    try {
    	count = updateToTableTidListTx(stmt, cid, sid, price, qty, whichUpdate, tid);
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
			BigDecimal price, int qty, int smallCid, int largeCid, int whichUpdate) throws SQLException { 
    int rowCount = 0;
    int tid = getMyTid();
    switch (whichUpdate) {
    case 0: 
  		//"update trade.portfolio set availQty=availQty-100 where cid = ? and sid = ? ",       
      Log.getLogWriter().info("updating portfolio table availQty to (availQty-100) for cid: "
      		+ cid + " and sid: " + sid );
      stmt.setInt(1, cid);
      stmt.setInt(2, sid);
      rowCount = stmt.executeUpdate();
      break;
    case 1: 
      // "update trade.portfolio set qty=qty-? where cid = ? and sid = ? ",
      Log.getLogWriter().info("updating portfolio to qty - " + qty 
      		+ " for cid: " + cid + " and sid: " + sid); 
      stmt.setInt(1, qty);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
     rowCount =  stmt.executeUpdate();
     break;
    case 2: 
      // "update trade.portfolio set tid = ?  where cid = ? and sid = ? ",
      Log.getLogWriter().info("updating portfolio table tid to " + tid
          + " for cid: " + cid + " and sid: " + sid);
      stmt.setInt(1, tid);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
      rowCount = stmt.executeUpdate();
      break; 
    case 3: 
		  //"update trade.portfolio set subTotal = ? * qty  where cid>=? and cid <? and sid = ? ",
      Log.getLogWriter().info("updating portfolio table subTotal to qty * " + price + 
      		", for cid: " + smallCid + " to " + largeCid + " and sid: " + sid);
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
	
	protected int updateToTableTidListTx(PreparedStatement stmt, int cid,  
			int sid, BigDecimal price, int qty, int whichUpdate, int tid) throws SQLException {
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		//       "update trade.portfolio set availQty=availQty-100 where cid = ? and sid = ?  and tid= ?",      
      Log.getLogWriter().info("updating portfolio table availQty to (availQty-100) for cid: "
      		+ cid + " and sid: " + sid  + " and tid: " + tid);
      stmt.setInt(1, cid);
      stmt.setInt(2, sid);  
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
      //       "update trade.portfolio set qty=qty-? where cid = ? and sid = ?  and tid= ?",
      Log.getLogWriter().info("updating portfolio to qty - " + qty 
      		+ " for cid: " + cid + " and sid: " + sid + " and tid: " + tid);
      stmt.setInt(1, qty);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);  
      stmt.setInt(4, tid);
      rowCount = stmt.executeUpdate();              
      break;
    case 2: 
	    // "update trade.portfolio set subTotal = ? * qty  where sid = ?  and tid= ?", //multiple records
      Log.getLogWriter().info("updating portfolio table subTotal to qty * " + price + 
      		", for sid: " + sid + " and tid: " + tid);
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
	
	
  public void performDerbyTxOps(Connection dConn, Object[]data, ArrayList<SQLException> dExList) {
  	boolean success = true;
  	try {
	  	switch ((Integer) data[1]) {
	  	case insertToDerbyTable: 
	  		success = insertToDerbyTable(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (BigDecimal) data[5]);
	  			//insertToDerbyTable(Connection conn, int cid, int sid, int qty,
	  			//BigDecimal sub)
	  		break;
	  	case updateToDerbyTableCidRangeTx: 
	  		success = updateToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(BigDecimal) data[4], (Integer) data[5], (Integer)data[6], 
	  				(Integer)data[7], (Integer)data[8]);
	  		//(Connection conn,  int cid, int sid, 
				//BigDecimal price, int qty, int smallCid, int largeCid, int whichUpdate)
	  		break;
	  	case updateToDerbyTableTidListTx: 
	  		success = updateToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(BigDecimal) data[4], (Integer) data[5], (Integer)data[6], (Integer)data[7]);
	  		//(Connection conn, int cid, int sid,  
				//BigDecimal price, int qty, int whichUpdate, int tid)
	  		break;
	  	case deleteToDerbyTableCidRangeTx: 
	  		success = deleteToDerbyTableCidRangeTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4]);
	  		//((Connection conn, int cid, int sid, int whichDelete)
	  		break;
	  	case deleteToDerbyTableTidListTx: 
	  		success = deleteToDerbyTableTidListTx(dConn, (Integer)data[2], (Integer)data[3], 
	  				(Integer) data[4], (Integer) data[5]);
	  		//(Connection conn, int cid, int sid, int whichDelete, int tid)
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
  		dExList.add(null); //to compare two exception list
  	}
  }
	
  public static final int insertToDerbyTable = 1;
  public static final int updateToDerbyTableCidRangeTx = 2;
  public static final int updateToDerbyTableTidListTx = 3;
  public static final int deleteToDerbyTableCidRangeTx = 4;
  public static final int deleteToDerbyTableTidListTx = 5;  

}

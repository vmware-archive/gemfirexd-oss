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
import hydra.TestConfig;

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
import sql.SQLPrms;
import sql.dmlStatements.TradeSecuritiesDMLStmt;
import sql.sqlTx.SQLDerbyTxBB;
import sql.sqlutil.DMLTxStmtsFactory;
import util.TestException;
import util.TestHelper;

public class TradeSecuritiesDMLTxStmt extends TradeSecuritiesDMLStmt 
		implements DMLTxStmtIF {

	//securities are replicate tables for tx cidRange,  
	String[] updateByCidRange = {
	    "update trade.securities set price = ? where sec_id = ?  ", 
	    "update trade.securities set symbol = ? where sec_id = ? ",
	    //"update trade.securities set price = ? where sec_id > ? and symbol >?  " //multiple records
	};
	int updateByCidRangeSingleRowStmt = 2;
	
	//no need for unique on tid, as long as there are colocated 
	String[] updateByTidList = {
	    "update trade.securities set price = ? where sec_id = ? and tid = ? ", 
	    "update trade.securities set symbol = ? where sec_id = ? and tid = ?",
	    //delete multi rows in one statement
	    "update trade.securities set price = ? where sec_id > ? and symbol >? and tid = ?  " //multiple records
	};
	int updateByTidListSingleRowStmt = 2;
	
  String[] deleteByCidRange = {
  		"delete from trade.securities where sec_id = ? ",
      "delete from trade.securities where symbol= ? and exchange = ? ",
      //delete multi rows in one statement
      "delete from trade.securities where price >? and price <? " //multiple rows
   };
  int deleteByCidRangeSingleRowStmt = 2;
  
  String[] deleteByTidList = {
  		"delete from trade.securities where sec_id = ? and tid = ?",
      "delete from trade.securities where symbol= ? and exchange = ? and tid = ?",
      //delete multi rows in one statement
      "delete from trade.securities where price >? and price <? and tid =?" //multiple rows
  };
	int deleteByTidListSingleRowStmt = 2;
  
	public void deleteTx(Connection dConn, Connection gConn, 
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList){
		int tid = -1;
		int sid = -1;
		BigDecimal price = getPrice();
		BigDecimal price2 = price.add(new BigDecimal(1));
		String symbol = getSymbol();
		String exchange = getExchange();		
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int whichDelete = getWhichTxDelete();
    int txId = modifiedKeys.get(thisTxId);
    
    if (byTidList) {
    	tid = sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    	sid = getSidByList(gConn, tid);   
    } else if (byCidRange){
    	//is replicated, could be any key
    	sid = rand.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary));
    }

    boolean opFailed = false;
    
    try {
    	if (byCidRange) {
        try {
        	getKeysForCidRangeDelete(gConn, keys, whichDelete, sid, symbol, exchange,
        			price, price2, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }  
    		deleteToGFXDTableCidRangeTx(gConn, sid, symbol, exchange, price, price2, whichDelete);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListDelete(gConn, keys, whichDelete, sid, symbol, exchange, 
        			price, price2, tid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		deleteToGFXDTableTidListTx(gConn, sid, symbol, exchange, price, price2, whichDelete, tid);
    	}
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
    	if (se.getSQLState().equalsIgnoreCase("X0Z04") 
    			&& byCidRange) { 
  			throw new TestException("got unexpected non colocated exception: " +
  					"trade.securities is replicate table, all keys are colocated\n"
  					+ TestHelper.getStackTrace(se));
	  	} else {
	    	opFailed = true; 
	    	//TODO possible 23503 foreign key violation constraint, this check at execution time
	    	//which actually should be checked at commit time, need to modify test once product
	    	//change occurs #41872
	    	sExList.add(se);	
	  	}
    }
    
    //no need to check if non colocated, as all keys should be colocated (replicate or query by tid)
    
    if (!opFailed) {
    	sExList.add(null); //to compare two exception list
    	modifiedKeys.putAll(keys);
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = null;
    	if (byCidRange) {
	    	data = new Object[8];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_SECURITIES;
	    	data[1] = deleteToDerbyTableCidRangeTx; //which operation
	    	data[2] = sid;
	    	data[3] = symbol;
	    	data[4] = exchange;
	    	data[5] = price;
	    	data[6] = price2;
	    	data[7] = whichDelete;
    	} else if (byTidList) {
	    	data = new Object[9];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_SECURITIES;
	    	data[1] = deleteToDerbyTableTidListTx; //which operation
	    	data[2] = sid;
	    	data[3] = symbol;
	    	data[4] = exchange;
	    	data[5] = price;
	    	data[6] = price2;
	    	data[7] = whichDelete;
	    	data[8] = tid;    		
    	}
    	    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
      SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }

	}

	public void populateTx(Connection dConn, Connection gConn) {
		//using overrided populate method so each insert being commited
		int initSize = TestConfig.tab().intAt(SQLPrms.initSecuritiesSizePerThread, 100);
		populate(dConn, gConn, initSize);
	}

	public void queryTx(Connection dConn, Connection gConn) {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	public void updateTx(Connection dConn, Connection gConn,
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
    BigDecimal price = getPrice();
    String symbol =  getSymbol();
    HashMap<String, Integer> keys = new HashMap<String, Integer>();
    int tid = -1;
    int txId = modifiedKeys.get(thisTxId);
  	int whichUpdate = getWhichTxUpdate();
  	int sid = -1;
  	boolean opFailed = false;
    
    if (byTidList) {
    	tid = sql.sqlutil.GFXDTxHelper.getColocatedTid(getTableName(), getMyTid());
    	sid = getSidByList(gConn, tid);    	
    } else if (byCidRange){
    	//is replicated, could be any key
    	sid = rand.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary));
    }
    
    try {
    	if (byCidRange) {
        //replicate table, could be any key
    		try {
    			getKeysForCidRangeUpdate(gConn, keys, whichUpdate, sid, symbol, txId);
    		} catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableCidRangeTx(gConn, sid, symbol, price, whichUpdate);
    	}
    	else if (byTidList) {
        try {
        	getKeysForTidListUpdate(gConn, keys, whichUpdate, sid, symbol, tid, txId);
        } catch (SQLException se) {
        	SQLHelper.handleSQLException(se);
        }   
    		updateToGFXDTableTidListTx(gConn, sid, symbol, price, whichUpdate, tid);
    	}
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
    	opFailed = true;
    	sExList.add(se);
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
	    	data[0] = DMLTxStmtsFactory.TRADE_SECURITIES;
	    	data[1] = updateToDerbyTableCidRangeTx; //which operation
	    	data[2] = sid;
	    	data[3] = symbol;
	    	data[4] = price;
	    	data[5] = whichUpdate;   	 		
    	} else if (byTidList){
	    	data = new Object[7];    	
	    	data[0] = DMLTxStmtsFactory.TRADE_SECURITIES;
	    	data[1] = updateToDerbyTableTidListTx; //which operation
	    	data[2] = sid;
	    	data[3] = symbol;
	    	data[4] = price;
	    	data[5] = whichUpdate;
	    	data[6] = tid; 
    	}
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data); //put into the derby tx bb    	
      SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }	
	}
	
	//gConn, sid, symbol, price, whichUpdate, tid
	protected void updateToGFXDTableCidRangeTx(Connection conn, int sid, String symbol, 
			BigDecimal price, int whichUpdate) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update securities table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    count = updateToTableCidRangeTx(stmt, sid, symbol, price, whichUpdate);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableCidRangeTx(Connection conn, int sid, String symbol, 
			BigDecimal price, int whichUpdate) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByCidRange[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update securities table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByCidRange[whichUpdate]);
    try {
    	count = updateToTableCidRangeTx(stmt, sid, symbol, price, whichUpdate);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	//gConn, sid, symbol, price, whichUpdate, tid
	protected void updateToGFXDTableTidListTx(Connection conn, int sid, String symbol, 
			BigDecimal price, int whichUpdate, int tid) throws SQLException {
	  //for gemfirexd checking
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update securities table in gemfirexd, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    count = updateToTableTidListTx(stmt, sid, symbol, price, whichUpdate, tid);
    Log.getLogWriter().info("gfxd updated " + count + " rows"); 
	}
	
	protected boolean updateToDerbyTableTidListTx(Connection conn, int sid, String symbol, 
			BigDecimal price, int whichUpdate, int tid) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(updateByTidList[whichUpdate]);
    int count = -1;
    Log.getLogWriter().info("update securities table in derby, myTid is " + getMyTid());
    Log.getLogWriter().info("update statement is " + updateByTidList[whichUpdate]);
    try {
    	count = updateToTableTidListTx(stmt, sid, symbol, price, whichUpdate, tid);
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
        Log.getLogWriter().info("detected the lock issue, will try it again");
        return false;
      } else throw se;
    }
    Log.getLogWriter().info("derby updated " + count + " rows");
    return true;
	}
	
	protected int updateToTableCidRangeTx(PreparedStatement stmt, int sid,  
			String symbol, BigDecimal price, int whichUpdate) throws SQLException { 
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		// "update trade.securities set price = ? where sec_id = ?  ", 
      Log.getLogWriter().info("updating price to " + price +
      		", for sec_id: " + sid );
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, sid);  
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
      //  "update trade.securities set symbol = ? where sec_id = ? ",
      Log.getLogWriter().info("updating symbol to " + symbol +
      		", for sec_id: " + sid );
      stmt.setString(1, symbol);
      stmt.setInt(2, sid);  
      rowCount = stmt.executeUpdate();              
      break;
    case 2: 
	    //"update trade.securities set price = ? where sec_id > ? and symbol >? " //multiple records
      Log.getLogWriter().info("updating price to " + price +
      		", for sec_id> " + sid + " and symbol> " + symbol );
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, sid); 
      stmt.setString(3, symbol); 
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
	
	//currently both are same, when update statement changed, this needs to be modified
	protected int updateToTableTidListTx(PreparedStatement stmt, int sid,  
			String symbol, BigDecimal price, int whichUpdate, int tid) throws SQLException {
    int rowCount = 0;
    switch (whichUpdate) {
    case 0: 
  		// "update trade.securities set price = ? where sec_id = ?  ", 
      Log.getLogWriter().info("updating price to " + price +
      		", for sec_id: " + sid + " and tid: " + tid);
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, sid);  
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
      //  "update trade.securities set symbol = ? where sec_id = ? and tid = ?",
      Log.getLogWriter().info("updating symbol to " + symbol +
      		", for sec_id: " + sid + " and tid: " + tid);
      stmt.setString(1, symbol);
      stmt.setInt(2, sid);  
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();              
      break;
    case 2: 
	    //"update trade.securities set price = ? where sec_id > ? and symbol >? and tid = ?  " //multiple records
      Log.getLogWriter().info("updating price to " + price +
      		", for sec_id> " + sid + " and symbol> " + symbol + " and tid: " + tid);
      stmt.setBigDecimal(1, price);
      stmt.setInt(2, sid); 
      stmt.setString(3, symbol); 
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
	
	//which keys could be modified by the statement
	protected void getKeysForTidListUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int sid, String symbol, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
	    //"update trade.securities set price = ? where sec_id = ? and tid = ? ",    
    case 1: 
      // "update trade.securities set symbol = ? where sec_id = ? and tid = ?",
    	sql = "select sec_id from trade.securities where sec_id="+sid +" and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("sec_id: " + sid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }  
      rs.close();
      break;
    case 2: 
    	 //"update trade.securities set price = ? where sec_id > ? and symbol >? and tid = ?  " //multiple records 
      sql = "select sec_id from trade.securities where sec_id>"+sid 
      	+" and symbol>'"+symbol + "' and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availSid = rs.getInt(1);
        Log.getLogWriter().info("sec_id: " + availSid + " exists for update");
      	keys.put(getTableName()+"_"+availSid, txId);
      }  
      rs.close();
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	//which keys could be modified by the statement
	protected void getKeysForCidRangeUpdate(Connection conn, HashMap<String, Integer > keys, 
			int whichUpdate, int sid, String symbol, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichUpdate) {
    case 0: 
	    //	    "update trade.securities set price = ? where sec_id = ?  ",   
    case 1: 
      //  "update trade.securities set symbol = ? where sec_id = ? ",
    	sql = "select sec_id from trade.securities where sec_id="+sid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("sec_id: " + sid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }  
      rs.close();
      break;
    case 2: 
	    //"update trade.securities set price = ? where sec_id > ? and symbol >?  " //multiple records 
      sql = "select sec_id from trade.securities where sec_id>"+sid 
      	+" and symbol>'"+symbol;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availSid = rs.getInt(1);
        Log.getLogWriter().info("sec_id: " + availSid + " exists for update");
      	keys.put(getTableName()+"_"+availSid, txId);
      }  
      rs.close();
      break; 
    default:
     throw new TestException ("Wrong update statement here");
    }	
	}
	
	protected void getKeysForCidRangeDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int sid, String symbol, String exchange,
			BigDecimal price, BigDecimal price2, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //"delete from trade.securities where sec_id = ? ",   
    	sql = "select sec_id from trade.securities where sec_id="+sid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("sec_id: " + sid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }  
      rs.close();
      break;
    case 1: 
      //   "delete from trade.securities where symbol= ? and exchange = ? ",
    	sql = "select sec_id from trade.securities where symbol='"+symbol +
    		"' and exchange='" + exchange + "'";
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
      	int availSid = rs.getInt(1);
        Log.getLogWriter().info("sec_id: " + availSid + " exists for update");
      	keys.put(getTableName()+"_"+availSid, txId);
      }  
      rs.close();
      break;
    case 2: 
	    //"delete from trade.securities where price >? and price <? " //multiple rows  
      sql = "select sec_id from trade.securities where price>"+price 
      	+" and price<"+price2;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availSid = rs.getInt(1);
        Log.getLogWriter().info("sec_id: " + availSid + " exists for update");
      	keys.put(getTableName()+"_"+availSid, txId);
      }  
      rs.close();
      break; 
    default:
     throw new TestException ("Wrong delete statement here");
    }	
	}
	
	protected void getKeysForTidListDelete(Connection conn, HashMap<String, Integer > keys, 
			int whichDelete, int sid, String symbol, String exchange,
			BigDecimal price, BigDecimal price2, int tid, int txId) throws SQLException {
    String sql = null;
    ResultSet rs = null;
    switch (whichDelete) {
    case 0: 
	    //  		"delete from trade.securities where sec_id = ? and tid = ?",          
    	sql = "select sec_id from trade.securities where sec_id="+sid +" and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
        Log.getLogWriter().info("sec_id: " + sid + " exists for update");
      	keys.put(getTableName()+"_"+rs.getInt(1), txId);
      }  
      rs.close();
      break;
    case 1: 
      // "delete from trade.securities where symbol= ? and exchange = ? and tid = ?",
    	sql = "select sec_id from trade.securities where symbol='"+symbol +
    		"' and exchange='" + exchange + "' and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      if (rs.next()) {
      	int availSid = rs.getInt(1);
        Log.getLogWriter().info("sec_id: " + availSid + " exists for update");
      	keys.put(getTableName()+"_"+availSid, txId);
      }  
      rs.close();
      break;
    case 2: 
	    // "delete from trade.securities where price >? and price <? and tid =?" //multiple rows 
      sql = "select sec_id from trade.securities where price>"+price 
      	+" and price<"+price2 +" and tid=" + tid;
      rs = conn.createStatement().executeQuery(sql); 
      while (rs.next()) {
      	int availSid = rs.getInt(1);
        Log.getLogWriter().info("sec_id: " + availSid + " exists for update");
      	keys.put(getTableName()+"_"+availSid, txId);
      }  
      rs.close();
      break; 
    default:
     throw new TestException ("Wrong delete statement here");
    }	
	}
	
	protected void deleteToGFXDTableCidRangeTx(Connection conn, int sid, 
			String symbol, String exchange, BigDecimal price, BigDecimal price2, 
			int whichDelete) throws SQLException {
	  //for gemfirexd checking
   PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
   int tid = getMyTid();
   int count = -1;
   Log.getLogWriter().info("delete securities table in gemfirexd, myTid is " + tid);
   Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
   count = deleteToTableCidRangeTx(stmt, sid, symbol, exchange, price, price2, whichDelete);
   Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
	}
	
	protected boolean deleteToDerbyTableCidRangeTx(Connection conn, int sid, 
			String symbol, String exchange, BigDecimal price, BigDecimal price2, 
			int whichDelete) throws SQLException {
	  //for gemfirexd checking
   PreparedStatement stmt = conn.prepareStatement(deleteByCidRange[whichDelete]);
   int tid = getMyTid();
   int count = -1;
   Log.getLogWriter().info("delete securities table in derby, myTid is " + tid);
   Log.getLogWriter().info("delete statement is " + deleteByCidRange[whichDelete]);
   try {
  	 count = deleteToTableCidRangeTx(stmt, sid, symbol, exchange, price, price2, whichDelete);
   } catch (SQLException se) {
     if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
       Log.getLogWriter().info("detected the lock issue, will try it again");
       return false;
     } else throw se;
   }
   Log.getLogWriter().info("derby deleted " + count + " rows");
   return true;
	}
	
	protected int deleteToTableCidRangeTx(PreparedStatement stmt,int sid, 
			String symbol, String exchange, BigDecimal price, BigDecimal price2, 
			int whichDelete) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
  		// "delete from trade.securities where sec_id = ? ",      
      Log.getLogWriter().info("delete from securities for sec_id: " + sid);
      stmt.setInt(1, sid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
	    //"delete from trade.securities where symbol= ? and exchange = ? ",
      Log.getLogWriter().info("delete from securites for symbol: " + symbol
      		+ " and exchange: " + exchange);
      stmt.setString(1, symbol);
      stmt.setString(2, exchange); 
      rowCount = stmt.executeUpdate();       
      break;
    case 2:
    	//"delete from trade.securities where price >? and price <? " //multiple rows
      Log.getLogWriter().info("delete from securities for price> " + price
      		+ " and price< " + price2);
      stmt.setBigDecimal(1, price);
      stmt.setBigDecimal(2, price2);
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
	
	protected void deleteToGFXDTableTidListTx(Connection conn, int sid, 
			String symbol, String exchange, BigDecimal price, BigDecimal price2, 
			int whichDelete, int tid) throws SQLException {
	  //for gemfirexd checking
   PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
   int count = -1;
   Log.getLogWriter().info("delete securities table in gemfirexd, myTid is " + getMyTid());
   Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
   count = deleteToTableTidListTx(stmt, sid, symbol, exchange, price, price2, whichDelete, tid);
   Log.getLogWriter().info("gfxd deleted " + count + " rows"); 
	}
	
	protected boolean deleteToDerbyTableTidListTx(Connection conn, int sid, 
			String symbol, String exchange, BigDecimal price, BigDecimal price2, 
			int whichDelete, int tid) throws SQLException {
	  //for gemfirexd checking
   PreparedStatement stmt = conn.prepareStatement(deleteByTidList[whichDelete]);
   int count = -1;
   Log.getLogWriter().info("delete securities table in derby, myTid is " + getMyTid());
   Log.getLogWriter().info("delete statement is " + deleteByTidList[whichDelete]);
   try {
  	 count = deleteToTableTidListTx(stmt, sid, symbol, exchange, price, price2, whichDelete, tid);
   } catch (SQLException se) {
     if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
       Log.getLogWriter().info("detected the lock issue, will try it again");
       return false;
     } else throw se;
   }
   Log.getLogWriter().info("derby deleted " + count + " rows");
   return true;
	}
	
	protected int deleteToTableTidListTx(PreparedStatement stmt,int sid, 
			String symbol, String exchange, BigDecimal price, BigDecimal price2, 
			int whichDelete, int tid) throws SQLException {
    int rowCount = 0;
    switch (whichDelete) {
    case 0: 
  		//   		"delete from trade.securities where sec_id = ? and tid = ?",       
      Log.getLogWriter().info("delete from securities for sec_id: " + sid +" and tid: " + tid);
      stmt.setInt(1, sid);
      stmt.setInt(2, tid);
      rowCount = stmt.executeUpdate();    
      break;
    case 1: 
	    // "delete from trade.securities where symbol= ? and exchange = ? and tid = ?",
      Log.getLogWriter().info("delete from securities for symbol: " + symbol
      		+ " and exchange: " + exchange +" and tid: " + tid );
      stmt.setString(1, symbol);
      stmt.setString(2, exchange); 
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();       
      break;
    case 2:
    	//"delete from trade.securities where price >? and price <? and tid =?" //multiple rows     
      Log.getLogWriter().info("delete from securities for price> " + price
      		+ " and price< " + price2 +" and tid: " + tid);
      stmt.setBigDecimal(1, price);
      stmt.setBigDecimal(2, price2);
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
  protected int getSidByList(Connection conn, int tid) {
  	String sql = "select sec_id from trade.securities where tid = ?";
  	try {
  		PreparedStatement ps = conn.prepareStatement(sql);
  		ps.setInt(1, tid);
  		ResultSet rs = ps.executeQuery();
  		if (rs.next()) {
  			return rs.getInt(1);
  		}
  		rs.close();
  	} catch (SQLException se) {
  		if (!SQLHelper.checkGFXDException(conn, se)) return getSid(); //return random sid
  		SQLHelper.handleSQLException(se);
  	}
  	return getSid(); //random
  }
	
	//insert in tx 
	//override so that each insert gets committed immediately
	public void populate (Connection dConn, Connection gConn, int initSize) {
		insert(dConn, gConn, initSize);
	}
	
  //override the method, so that each insert gets committed so no 
  public void insert(Connection dConn, Connection gConn, int size) {
    int[] sec_id = new int[size];
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    BigDecimal[] price = new BigDecimal[size];
    SQLException derbySe = null;
    SQLException gfxdSe = null;
    getDataForInsert(sec_id, symbol, exchange, price, size); //get the data
    for (int i=0; i<symbol.length; i++) {
    	//symbol[i] = getSymbol(4); //cause unique key conflict
    	symbol[i] = getSymbol(4, 9);
    }
        
    if (dConn != null) {
	    for (int i=0; i<sec_id.length; i++) {
	    	//insert to each table
	    	try {
	    		insertToGFXDTable(gConn, sec_id[i], symbol[i], exchange[i], price[i]); 
	    	} catch (SQLException se) {
	    		//for each insert there will be no non colocate exception thrown here
	    		SQLHelper.handleSQLException(se);
	    		//SQLHelper.printSQLException(se);
	    		//gfxdSe = se;
	    	}
	    	try {
	    		boolean success = insertToDerbyTable(dConn, sec_id[i], symbol[i], exchange[i], price[i]); 
	        int count = 0;
	        while (!success) {
	          if (count>=maxNumOfTries) {
	            Log.getLogWriter().info("Could not get the lock to finish insert into derby, abort this operation");
	            gConn.rollback();
	            dConn.rollback();
	            break;
	          }
	          success = insertToDerbyTable(dConn, sec_id[i], symbol[i], exchange[i], price[i]);  //retry insert to derby table         
	          
	          count++;
	        }
	    	} catch (SQLException se) {
	    		SQLHelper.printSQLException(se);
	    		derbySe = se;
	    	}
		    
	    	getLock();
	      //commit to each db
		    try {
		    	gConn.commit();
		    	Log.getLogWriter().info("gfxd committed the operation");
		    	dConn.commit();
		    	Log.getLogWriter().info("derby committed the operation");
		    } catch (SQLException se) {
		    	SQLHelper.printSQLException(se);
		    	//gfxd should not have non colocated exception as it commit per insert
		    	
		    	//TODO modify the test once #41898 is resolved
		    	if (derbySe.getSQLState().equals("23505") && se.getSQLState().equals("X0Z05")) {
		    	//it is possible for gfxd fail to detect unique key violation in the transaction
		    	//but it should fail at commit time -- so no record should be in either db
		    		derbySe = null;
		    		gfxdSe = null;
		    	} else 
		    		SQLHelper.handleSQLException(se);
		    }
		    
		    releaseLock();
	    } 
    }
    else {
	    for (int i=0; i<sec_id.length; i++) {
	    	//insert to each table
	    	try {
	    		insertToGFXDTable(gConn, sec_id[i], symbol[i], exchange[i], price[i]); 
	    	} catch (SQLException se) {
	    		//commit each time, should not got commit conflict exception here
	    		Log.getLogWriter().info("inserting to securities using tx failed");
	    		SQLHelper.handleSQLException(se);
	    	}
	    	try {
	    		gConn.commit();
	    	} catch (SQLException se) {
	    		//could get commit failed exception duet ot unique key constraint violation here
	    		//TODO should not throw In doubt transaction here, will change it when #41898 is fixed
		    	if (se.getSQLState().equals("X0Z05")) {
		    		Log.getLogWriter().info("got expected In doubt exception (unique key voilation exception)," +
		    				" continuing testing");
		    	} else {
		    		SQLHelper.handleSQLException(se);
		    	}
	    	}
	    }
    } 
  }

  //commit each time
  protected void insertToGFXDTable(Connection conn, int sec_id, 
      String symbol, String exchange, BigDecimal price) throws SQLException {
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into gemfirexd, myTid is " + tid);
    count = insertToTable(stmt, sec_id, symbol, exchange, price, tid);
    Log.getLogWriter().info("gfxd inserts " + count + " record");
  }
	
  //insert into Derby
  protected boolean insertToDerbyTable(Connection conn, int sec_id, 
      String symbol, String exchange, BigDecimal price) throws SQLException {
    PreparedStatement stmt = getStmt(conn, insert);
    int tid = getMyTid();
    int count = -1;
    Log.getLogWriter().info("Insert into derby, myTid is " + tid);
    try {
      count = insertToTable(stmt, sec_id, symbol, exchange, price, tid);
      Log.getLogWriter().info("derby inserts " + count + " record");
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) return false;
      else throw se;
    }    
    return true;
  }

  //used in one of the tx, insert one record only
  @SuppressWarnings("unchecked")
  public void insertTx(Connection dConn, Connection gConn, 
  		HashMap<String, Integer> modifiedKeys,
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList) {
  	int size = 1;
    int[] sec_id = new int[size];
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    BigDecimal[] price = new BigDecimal[size];
    getDataForInsert(sec_id, symbol, exchange, price, size); //get the data
    boolean opFailed = false;
    
    try {
    	insertToGFXDTable(gConn, sec_id[0], symbol[0], exchange[0], price[0]);
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
    	opFailed = true;
    	sExList.add(se);  //unique key violation only, should be processed at commit time?
    } 
    int txId = modifiedKeys.get(thisTxId);
    
    if (!opFailed) {
    	sExList.add(null); //to compare two exception list
      modifiedKeys.put(getTableName()+"_"+sec_id[0], txId);
    }
    
    //write to derby tx bb for this op
    if (dConn != null) {
    	Object[] data = new Object[6];    	
    	data[0] = DMLTxStmtsFactory.TRADE_SECURITIES;
    	data[1] = insertToDerbyTable; //which operation
    	data[2] = sec_id[0];
    	data[3] = symbol[0];
    	data[4] = exchange[0];
    	data[5] = price[0];
    	
    	ArrayList<Object[]> derbyOps = (ArrayList<Object[]>)SQLDerbyTxBB.getBB().getSharedMap().get(txId);
    	derbyOps.add(data);
    	SQLDerbyTxBB.getBB().getSharedMap().put(txId, derbyOps);
    }
  }
  
  public static String getTableName() {
  	return "securities";
  }
  
  public String getMyTableName() {
  	return getTableName();
  }
  
  public void performDerbyTxOps(Connection dConn, Object[]data, ArrayList<SQLException> dExList) {
  	boolean success = true;
  	try {
	  	switch ((Integer) data[1]) {
	  	case insertToDerbyTable: 
	  		success = insertToDerbyTable(dConn, (Integer)data[2], (String)data[3], 
	  				(String) data[4], (BigDecimal) data[5]);
	  			//insertToDerbyTable(Connection conn, int sec_id, 
	      	//String symbol, String exchange, BigDecimal price)
	  		break;
	  	case updateToDerbyTableCidRangeTx: 
	  		success = updateToDerbyTableCidRangeTx(dConn, (Integer)data[2], (String)data[3], 
	  				(BigDecimal) data[4], (Integer) data[5]);
	  		//updateToDerbyTableCidRangeTx(Connection conn, int sid, String symbol, 
				//BigDecimal price, int whichUpdate, int tid)
	  		break;
	  	case updateToDerbyTableTidListTx: 
	  		success = updateToDerbyTableTidListTx(dConn, (Integer)data[2], (String)data[3], 
	  				(BigDecimal) data[4], (Integer) data[5], (Integer)data[6]);
	  		//updateToDerbyTableTidListTx(Connection conn, int sid, String symbol, 
				//BigDecimal price, int whichUpdate, int tid)
	  		break;
	  	case deleteToDerbyTableCidRangeTx: 
	  		success = deleteToDerbyTableCidRangeTx(dConn, (Integer)data[2], (String)data[3], 
	  				(String) data[4], (BigDecimal) data[5], (BigDecimal) data[6], (Integer) data[7]);
	  		//(Connection conn, int sid, String symbol, String exchange, BigDecimal price, BigDecimal price2, 
				//int whichDelete)
	  		break;
	  	case deleteToDerbyTableTidListTx: 
	  		success = deleteToDerbyTableTidListTx(dConn, (Integer)data[2], (String)data[3], 
	  				(String) data[4], (BigDecimal) data[5], (BigDecimal) data[6], 
	  				(Integer) data[7], (Integer) data[8]);
	  		//(Connection conn, int sid, String symbol, String exchange, BigDecimal price, BigDecimal price2, 
				//int whichDelete, int tid)
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
  		throw new TestException ("Only one thread performing derby ops at given time, should not fail");
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

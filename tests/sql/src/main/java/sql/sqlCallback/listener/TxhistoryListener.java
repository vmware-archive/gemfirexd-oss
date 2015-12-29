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
package sql.sqlCallback.listener;

import hydra.Log;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import sql.ClientDiscDBManager;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import util.TestException;

import com.pivotal.gemfirexd.callbacks.Event;

public class TxhistoryListener extends AbstractListener {
	static String insert = "insert into trade.txhistory values (?,?,?,?,?,?,?,?)";
  //cid int, oid int, sid int, qty int, price decimal (30, 20), ordertime timestamp, 
  //type varchar(10), tid int,  constraint type_ch check (type in ('buy', 'sell'))
	static String delete = "delete from trade.txhistory where cid=? and oid=? and sid = ?" +
			" and qty =? and price=? and ordertime =? and type=? and tid =?";
        static String deleteWithNullOrderTime = "delete from trade.txhistory where cid=? and oid=? and sid = ?" +
                        " and qty =? and price=? and ordertime is null and type=? and tid =?";
	
	private static ThreadLocal<Connection> dConn = new ThreadLocal<Connection>()  {
		protected Connection initialValue()  {
			Connection dConn =null;
			try {
				dConn = ClientDiscDBManager.getConnection(); 
				//may need to use url to get the correct db connection
			} catch (SQLException se) {
				SQLHelper.handleSQLException(se);
			}
			return dConn;
		}
	};
	
	private static ThreadLocal<PreparedStatement> insertStmt = new ThreadLocal<PreparedStatement> () {		
		protected PreparedStatement initialValue()  {
			
			PreparedStatement stmt = null;
			try {
				stmt = dConn.get().prepareStatement(insert);
			}catch (SQLException se) {
				SQLHelper.handleSQLException(se);
			}
			return stmt;
		}
	};
	
	private static ThreadLocal<PreparedStatement> deleteStmt = new ThreadLocal<PreparedStatement> () {		
		protected PreparedStatement initialValue()  {
			PreparedStatement stmt = null;
			try {
				stmt = dConn.get().prepareStatement(delete);
			}catch (SQLException se) {
				SQLHelper.handleSQLException(se);
			}
			return stmt;
		}
	};
	
        private static ThreadLocal<PreparedStatement> deleteWithNullOrderTimeStmt = new ThreadLocal<PreparedStatement> () {    
          protected PreparedStatement initialValue()  {
            PreparedStatement stmt = null;
            try {
              stmt = dConn.get().prepareStatement(deleteWithNullOrderTime);
            }catch (SQLException se) {
              SQLHelper.handleSQLException(se);
            }
            return stmt;
          }
        };
      
	private static ThreadLocal<Statement> stmt = new ThreadLocal<Statement> () {		
		protected Statement initialValue() {
			Statement s = null;
			try {
				s = dConn.get().createStatement();
			}catch (SQLException se) {
				SQLHelper.handleSQLException(se);
			}
			return s;
		}
	};

	@Override
	public void onEvent(Event event) throws SQLException {
		Event.Type type = event.getType();
		String eventType = null;
		boolean isPossibleDuplicate = event.isPossibleDuplicate();
		if (isPossibleDuplicate && !SQLTest.isHATest) {
			throw new TestException("Got possible duplicate event but it is  " +
					"not a HA Test");
		}
		
		switch (type) {
		case AFTER_UPDATE:
			update(event);
			break;
		case AFTER_INSERT:
			insert(event);
			break;
		case AFTER_DELETE:
			delete(event);
			break;
		default:
			throw new TestException("Listener got a 'none_after' callback event: " + eventType);
		}		
		dConn.get().commit(); //do not hold the lock	
	}
	
	private void insert (Event event) throws SQLException {
		Log.getLogWriter().info("in txhistory listener insert event inovcation");
		List<Object> oldRow = event.getOldRow();
		List<Object> newRow = event.getNewRow();
		boolean isPossibleDuplicate = event.isPossibleDuplicate();		
		if (isPossibleDuplicate) {
			Log.getLogWriter().info("possible duplicate record being inserted");
			Object o = SQLBB.getBB().getSharedMap().put("TxHistoryPosDup" , true);
			if (o== null) Log.getLogWriter().info("no value input in the key");
			else Log.getLogWriter().info("put into sharedMap for key TxHistoryPosDup: " + o);
			//does not compare results if duplicate record is being writen to backend db
		}		
		doInsert(oldRow, newRow, isPossibleDuplicate);
	}	
	
	private void doInsert(List<Object> oldRow, List<Object> newRow, 
			boolean isPossibleDuplicate) throws SQLException {
	//cid int, oid int, sid int, qty int, price decimal (30, 20), ordertime timestamp, 
	  //type varchar(10), tid int,  constraint type_ch check (type in ('buy', 'sell'))
		if (oldRow != null) throw new TestException("BEFORE_INSERT in txhistoryListener " +
				"is invoked, but oldRow is not null: " + oldRow);
	
		insertStmt.get().setInt(1, (Integer)( newRow.get(0)));
		insertStmt.get().setInt(2, (Integer) newRow.get(1));
		insertStmt.get().setInt(3, (Integer) newRow.get(2));
		insertStmt.get().setInt(4, (Integer) newRow.get(3));
		insertStmt.get().setBigDecimal(5, (BigDecimal) newRow.get(4));
		insertStmt.get().setTimestamp(6, (Timestamp) newRow.get(5));
		insertStmt.get().setString(7, (String) newRow.get(6));
		insertStmt.get().setInt(8, (Integer) newRow.get(7));
		Log.getLogWriter().info("inserting into txhistory in derby " +
				"cid is " + (Integer)( newRow.get(0)) +
				" oid is " + (Integer) newRow.get(1) +
				" sid is " +  newRow.get(2) +
				" qty is " + newRow.get(3) + 
				" price is " + (BigDecimal) newRow.get(4) +
				" ordertime is " + (Timestamp) newRow.get(5) +
				" type is " +  newRow.get(6) +
				" tid is " + (Integer) newRow.get(7));
		try {
			insertStmt.get().executeUpdate();
		} catch (SQLException se) {
			if (se.getSQLState().equals("23513")) {
				Log.getLogWriter().info("get check constraint violation, " +
					"let gemfirexd perform the constraint check as well");
				return;
			}
      else
      	throw se;
		}
	}
	
	private void delete (Event event) throws SQLException {
		List<Object> oldRow = event.getOldRow();
		List<Object> newRow = event.getNewRow();
		int count =0;
		if (newRow != null) throw new TestException("BEFORE_DELETE is invoked, but " +
				"newRow is not null: " + newRow);
		
		if (event.getPrimaryKey()[0] != null)
		Log.getLogWriter().warning("the primary key in txHistory listener is "
				+ event.getPrimaryKey()[0]); 

                Log.getLogWriter().info("deleting from txhistory in derby for " +
                    "cid is " + (Integer)( oldRow.get(0)) +
                    " oid is " + (Integer) oldRow.get(1) +
                    " sid is " +  oldRow.get(2) +
                    " qty is " + oldRow.get(3) + 
                    " price is " + (BigDecimal) oldRow.get(4) +
                    " ordertime is " + (Timestamp) oldRow.get(5) +
                    " type is " +  oldRow.get(6) +
                    " tid is " + (Integer) oldRow.get(7));
                
            if (oldRow.get(5) == null) {
              // ordertime could be null in certain rows
              deleteWithNullOrderTimeStmt.get().setInt(1, (Integer)(oldRow.get(0)));
              deleteWithNullOrderTimeStmt.get().setInt(2, (Integer)oldRow.get(1));
              deleteWithNullOrderTimeStmt.get().setInt(3, (Integer)oldRow.get(2));
              deleteWithNullOrderTimeStmt.get().setInt(4, (Integer)oldRow.get(3));
              deleteWithNullOrderTimeStmt.get().setBigDecimal(5,(BigDecimal)oldRow.get(4));
              deleteWithNullOrderTimeStmt.get().setString(6, (String)oldRow.get(6));
              deleteWithNullOrderTimeStmt.get().setInt(7, (Integer)oldRow.get(7));
          
              count = deleteWithNullOrderTimeStmt.get().executeUpdate();
              Log.getLogWriter().info("deleted " + count + " row");
            }
            else {
          
              deleteStmt.get().setInt(1, (Integer)(oldRow.get(0)));
              deleteStmt.get().setInt(2, (Integer)oldRow.get(1));
              deleteStmt.get().setInt(3, (Integer)oldRow.get(2));
              deleteStmt.get().setInt(4, (Integer)oldRow.get(3));
              deleteStmt.get().setBigDecimal(5, (BigDecimal)oldRow.get(4));
              deleteStmt.get().setTimestamp(6, (Timestamp)oldRow.get(5));
              deleteStmt.get().setString(7, (String)oldRow.get(6));
              deleteStmt.get().setInt(8, (Integer)oldRow.get(7));
              Log.getLogWriter().info(
                  "deleting from txhistory in derby for " + "cid is "
                      + (Integer)(oldRow.get(0)) + " oid is " + (Integer)oldRow.get(1)
                      + " sid is " + oldRow.get(2) + " qty is " + oldRow.get(3)
                      + " price is " + (BigDecimal)oldRow.get(4) + " ordertime is "
                      + (Timestamp)oldRow.get(5) + " type is " + oldRow.get(6)
                      + " tid is " + (Integer)oldRow.get(7));
              count = deleteStmt.get().executeUpdate();
              Log.getLogWriter().info("deleted " + count + " row");
            }
	}
	
	private void update(Event event) throws SQLException{	
		List<Object> oldRow = event.getOldRow();
		if (event.getPrimaryKey()[0] != null)
			Log.getLogWriter().warning("the primary key in txHistory listener is "
					+ event.getPrimaryKey()[0]); 
		String sql = getSql(event); 
		//add where clause
                if (oldRow.get(5) == null) {
                  sql += " where cid = " + (Integer)(oldRow.get(0)) 
                      + " and oid = " + (Integer)oldRow.get(1) 
                      + " and sid = " + oldRow.get(2)
                      + " and qty = " + oldRow.get(3) 
                      + " and price = " + (BigDecimal)oldRow.get(4) 
                      + " and ordertime is null "
                      + " and type = '" + oldRow.get(6) 
                      + "' and tid = "  + (Integer)oldRow.get(7);
                }
                else {
                  sql += " where cid = " + (Integer)(oldRow.get(0)) 
                      + " and oid = " + (Integer)oldRow.get(1) 
                      + " and sid = " + oldRow.get(2)
                      + " and qty = " + oldRow.get(3) 
                      + " and price = " + (BigDecimal)oldRow.get(4) 
                      + " and ordertime = '" + (Timestamp)oldRow.get(5) 
                      + "' and type = '" + oldRow.get(6)
                      + "' and tid = " + (Integer)oldRow.get(7);
                }
                
		Log.getLogWriter().info("update to derby: " + sql);
		try {
			stmt.get().executeUpdate(sql);
		} catch (SQLException se) {
      throw se;
		}
	}	

}

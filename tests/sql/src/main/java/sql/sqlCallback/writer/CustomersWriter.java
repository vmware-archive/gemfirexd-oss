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
package sql.sqlCallback.writer;

import java.sql.*;
import java.util.List;

import hydra.Log;
import hydra.TestConfig;
import sql.ClientDiscDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import util.TestException;


import com.pivotal.gemfirexd.callbacks.Event;

public class CustomersWriter extends AbstractWriter {
	static String delete = "delete from trade.customers where cid=?";
	static String insert = "insert into trade.customers values (?,?,?,?,?)";
	private static String initStr = TestConfig.tab().stringAt(SQLPrms.backendDB_url, 
			"jdbc:derby:test");
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
	
	private static ThreadLocal<Statement> stmt = new ThreadLocal<Statement> () {		
		protected Statement initialValue()  {
			Statement s = null;
			try {
				s = dConn.get().createStatement();
			}catch (SQLException se) {
				SQLHelper.handleSQLException(se);
			}
			return s;
		}
	};

	public CustomersWriter() {
	}
	
	@Override
	public void close() throws SQLException {
		// TODO

	}

	@SuppressWarnings("static-access")
	@Override
	public void init(String initStr) throws SQLException {
		if (!this.initStr.equals(initStr)) 
			throw new TestException ("init in Writer() does not get the initStr.");
	}

	@Override
	public void onEvent(Event event) throws SQLException {
		
		try {
			writeThrough(event);
		} catch (SQLException se) {
			if (!SQLHelper.checkDerbyException(dConn.get(), se)) {
				//TODO may add retry here instead of throwing exception and let gemfirexd fail the op
				throw new SQLException("could not get lock to write to back_end db",
						SQLHelper.lockIssueSQLState);
			} else {
				throw se;
			}
		}
	}
	
	private void writeThrough(Event event) throws SQLException {
		Event.Type type = event.getType();
		String eventType = null;
		boolean isPossibleDuplicate = event.isPossibleDuplicate();
		if (isPossibleDuplicate && !SQLTest.isHATest) {
			throw new TestException("Got possible duplicate event but it is  " +
					"not a HA Test");
		}
		
		switch (type) {
		case BEFORE_DELETE:
			delete(event);
			break;
		case BEFORE_INSERT:
			insert(event);
			break;
		case BEFORE_UPDATE:
			update(event);			
			break;
		case AFTER_UPDATE:
			eventType = "AFTER_UPDATE"; //fall through
		case AFTER_INSERT:
			eventType = "AFTER_INSERT"; //fall through
		case AFTER_DELETE:
			eventType = "AFTER_DELETE"; //fall through
		default:
			throw new TestException("Writer got a 'none_before' callback event: " + eventType);
		}		
		dConn.get().commit(); //do not hold the lock	

	}

	private void insert (Event event) throws SQLException {
		List<Object> oldRow = event.getOldRow();
		List<Object> newRow = event.getNewRow();
		boolean isPossibleDuplicate = event.isPossibleDuplicate();		
		//for HA case, before insert check if record has been inserted already to avoid duplicate
		if (isPossibleDuplicate) {
			ResultSet rs = stmt.get().executeQuery("select * from trade.customers where cid = "
					+ (Integer)event.getPrimaryKey()[0]) ;
			if (rs.next()) {
				Log.getLogWriter().info("this row has been inserted already " + newRow);
				//if it is there, do not insert again
			} else {
				doInsert(oldRow, newRow, isPossibleDuplicate);
			}
		} else doInsert(oldRow, newRow, isPossibleDuplicate);
			
	}	
	
	private void doInsert(List<Object> oldRow, List<Object> newRow, 
			boolean isPossibleDuplicate) throws SQLException {
	  /*
	   * trade.customers table fields
	   *   private int cid;
	   *   String cust_name;
	   *   Date since;
	   *   String addr;
	   *   int tid; //for update or delete unique records
	   */	
		
		int count = 0;
		if (oldRow != null) throw new TestException("BEFORE_INSERT is invoked, but " +
				"oldRow is not null: " + oldRow);
	
		insertStmt.get().setInt(1, (Integer)( newRow.get(0)));
		insertStmt.get().setString(2, (String) newRow.get(1));
		insertStmt.get().setDate(3, (Date) newRow.get(2));
		insertStmt.get().setString(4, (String) newRow.get(3));
		insertStmt.get().setInt(5, (Integer) newRow.get(4));
		Log.getLogWriter().info("inserting into customers in derby " +
				"cid is " + (Integer)( newRow.get(0)) +
				" cust_name is " + (String) newRow.get(1) +
				" since is " + (Date) newRow.get(2) +
				" addr is " + (String) newRow.get(3) + 
				" tid is " + (Integer) newRow.get(4));
		try {
			count = insertStmt.get().executeUpdate();
		} catch (SQLException se) {
			SQLHelper.handleSQLException(se);
		}
		if (count !=1 && !isPossibleDuplicate) throw new TestException("insert from writer does not " +
				"insert the corresponding row in back_end, inserted row is " + count);
	}
	
	private void delete (Event event) throws SQLException {
		List<Object> newRow = event.getNewRow();
		boolean isPossibleDuplicate = event.isPossibleDuplicate();		
		int count =0;		
		if (newRow != null) throw new TestException("BEFORE_DELETE is invoked, but " +
				"newRow is not null: " + newRow);
		deleteStmt.get().setInt(1, (Integer)event.getPrimaryKey()[0]); //through primary key
		Log.getLogWriter().info("deleting from customers in derby where cid =" 
				+  (Integer)event.getPrimaryKey()[0]);
		try {
			count = deleteStmt.get().executeUpdate();
		} catch (SQLException se) {
			if (se.getSQLState().equals("23503")) {
				Log.getLogWriter().info("get foreign key constraint violation, " +
					"let gemfirexd perform the constraint check as well");
				return;
			}
      else
      	throw se;
		}
		Log.getLogWriter().info("deleted " + count + " row");
		if (count !=1 && !isPossibleDuplicate && !SQLTest.setTx) 
			throw new TestException("delete from writer does not " +
				"delete the corresponding row in back_end, inserted row is " + count);			
	}
	
	private void update(Event event) throws SQLException{
		boolean isPossibleDuplicate = event.isPossibleDuplicate();		
		int count =0;
		String sql = getSql(event);
		//add where clause
		sql +=" where cid = " + event.getPrimaryKey()[0];
		Log.getLogWriter().info("update to derby: " + sql);
		try {
			count = stmt.get().executeUpdate(sql);
		} catch (SQLException se) {
			SQLHelper.handleSQLException(se);
		}
		if (count !=1 && !isPossibleDuplicate && !SQLTest.setTx) 
		  //with default txn isolation, #43725 may be hit (some rows have been updated/deleted
		  //in the back end thru writer but still exist in gfxd due to txn roll back
		  //another update will cause the update to backend to fail to update any
			throw new TestException("update from writer does not " +
				"update the corresponding row in back_end, updated row is " + count);
	}	

}

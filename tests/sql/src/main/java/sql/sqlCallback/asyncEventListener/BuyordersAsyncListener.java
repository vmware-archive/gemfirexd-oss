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
package sql.sqlCallback.asyncEventListener;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import sql.ClientDiscDBManager;
import sql.SQLHelper;
import sql.sqlCallback.SQLAsyncEventListenerTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

import com.pivotal.gemfirexd.callbacks.Event;

public class BuyordersAsyncListener extends AbstractAsyncListener {
	static String insert = "insert into trade.buyorders values (?,?,?,?,?,?,?,?)";
	static String delete = "delete from trade.buyorders where oid=?";
	
	
	private Event failedEvent = null;
	
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
	
  public boolean processEvents(List<Event> events) {
    Log.getLogWriter().info("start processing the list of async events");
    for (Event event : events){
      if (failedEvent != null) {
        if (!isSame(failedEvent, event)) {
          if (!event.isPossibleDuplicate()) Log.getLogWriter().warning("an already processed " +
              "event does not have isPossibleDuplicate() set to true");
          else Log.getLogWriter().info("this event has been processed before, do not retry");
          continue;
        } else {
          failedEvent = null; //reset the flag.
        }
      } 
      
      boolean success = processEvent(event);
      if (!success) {
        if (testUniqueKeys) {
          failedEvent = event;
          //restart from the batching from previously failed one, should not have out of order delivery to queue
          Log.getLogWriter().info("test unique key, will retry the failed event");
          return false;
        } else {
          Log.getLogWriter().info("the event does not succeed in back end, but will not continue to retry" +
              "to block the queue");
          //in the multiple listener case, insert p, insert c, delete c, delete p
          //if delete c and insert c are executed out of order in back end db, 
          //the delete p will continue to be retried and will block all the following events for the table.
        }
      }
    }
    return true;
  }
  
  public boolean processEvent(Event event) {
    try {
      Event.Type type = event.getType();
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
        Log.getLogWriter().warning("TestException: AsyncEventListener got a 'none_after' callback event: " + type);
      }   
      dConn.get().commit(); //do not hold the lock  
    }catch(SQLException se){
      SQLHelper.printSQLException(se);
      if (se.getSQLState().equals("08006") || se.getSQLState().equals("08003") || 
          se.getSQLState().equals("08001")) {
        Log.getLogWriter().info("connection is lost");
        Connection conn =null;
        try {
          conn = ClientDiscDBManager.getConnection(); 
          //may need to use url to get the correct db connection
        } catch (SQLException ex) {
          SQLHelper.handleSQLException(ex);
        }
        dConn.set(conn); //provide new connection
        resetStatements();
      } else 
        Log.getLogWriter().warning("AsyncEventListener Test gets exception\n" + 
            TestHelper.getStackTrace(se)); 
      return false;
    } 
    return true;
  }
	
	protected void insert (Event event) throws SQLException {
		List<Object> oldRow = event.getOldRow();
		List<Object> newRow = event.getNewRow();
		
		ResultSet oldRs = event.getOldRowAsResultSet();
		ResultSet newRs = event.getNewRowsAsResultSet();
		
		boolean isPossibleDuplicate = event.isPossibleDuplicate();		
		//for HA case, before insert check if record has been inserted already to avoid duplicate
		if (isPossibleDuplicate) {
			ResultSet rs = stmt.get().executeQuery("select * from trade.buyorders where oid = "
					+ (Integer)event.getPrimaryKey()[0]);
			if (rs.next()) {
				Log.getLogWriter().info("this row has been inserted already " + newRow);
				//if it is there, do not insert again
			} else {
			  if (useResultSetImpl) doInsert(oldRs, newRs, isPossibleDuplicate);
			  else doInsert(oldRow, newRow, isPossibleDuplicate);
			}
		} else doInsert(oldRow, newRow, isPossibleDuplicate);
			
	}	
	
	private void doInsert(List<Object> oldRow, List<Object> newRow, 
			boolean isPossibleDuplicate) throws SQLException {
	  /*
	   * trade.buyorders table fields
	   *   int oid;
	   *   int cid; 
	   *   int sid; 
	   *   int qty;
	   *   BigDecimal bid;
	   *   long ordertime;
	   *   String stauts;
	   *   int tid; 
	   */
		int count = 0;
		if (oldRow != null) {
		  SQLAsyncEventListenerTest.testException = new TestException("BEFORE_INSERT in buyordersListener " +
				"is invoked, but oldRow is not null: " + oldRow);
		  throw SQLAsyncEventListenerTest.testException;
		}
	
		try {
		insertStmt.get().setInt(1, (Integer)( newRow.get(0)));
		insertStmt.get().setInt(2, (Integer) newRow.get(1));
		insertStmt.get().setInt(3, (Integer) newRow.get(2));
		insertStmt.get().setInt(4, (Integer) newRow.get(3));
		insertStmt.get().setBigDecimal(5, (BigDecimal) newRow.get(4));
		insertStmt.get().setTimestamp(6, (Timestamp) newRow.get(5));
		insertStmt.get().setString(7, (String) newRow.get(6));
		insertStmt.get().setInt(8, (Integer) newRow.get(7));
		} catch (ClassCastException ce) {
			Log.getLogWriter().info("new row in buyorders " +
					"oid is " +  newRow.get(0) );
			throw ce;
		}
		Log.getLogWriter().info("inserting into buyorders in derby " +
				"oid is " + (Integer)( newRow.get(0)) +
				" cid is " + (Integer) newRow.get(1) +
				" sid is " +  newRow.get(2) +
				" qty is " + newRow.get(3) + 
				" bid is " + (BigDecimal) newRow.get(4) +
				" ordertime is " + (Timestamp) newRow.get(5) +
				" status is " +  newRow.get(6) +
				" tid is " + (Integer) newRow.get(7));

		count = insertStmt.get().executeUpdate();
		Log.getLogWriter().info("inserted " + count + " row");

		if (count !=1 && !isPossibleDuplicate && testUniqueKeys) {
		  SQLAsyncEventListenerTest.testException = new TestException("insert from listener does not " +
				"insert the corresponding row in back_end, inserted row is " + count);
		  throw SQLAsyncEventListenerTest.testException;
		}
	}
	
	 private void doInsert(ResultSet oldRs, ResultSet newRs, 
	      boolean isPossibleDuplicate) throws SQLException {
	    /*
	     * trade.buyorders table fields
	     *   int oid;
	     *   int cid; 
	     *   int sid; 
	     *   int qty;
	     *   BigDecimal bid;
	     *   long ordertime;
	     *   String stauts;
	     *   int tid; 
	     */
	    int count = 0;
	    if (oldRs != null) {
	      SQLAsyncEventListenerTest.testException = new TestException("BEFORE_INSERT in buyordersListener " +
	        "is invoked, but oldRow is not null: " + oldRs);
	      throw SQLAsyncEventListenerTest.testException;
	    }
	    
	    if (newRs == null) {
	      throw new TestException("event.getNewRowsAsResultSet() returns " + newRs);
	    }
	    newRs.next();
	    try {
  	    insertStmt.get().setInt(1, newRs.getInt(1));
  	    insertStmt.get().setInt(2, newRs.getInt(2));
  	    insertStmt.get().setInt(3, newRs.getInt(3));
  	    insertStmt.get().setInt(4, newRs.getInt(4));
  	    insertStmt.get().setBigDecimal(5, newRs.getBigDecimal(5));
  	    insertStmt.get().setTimestamp(6, newRs.getTimestamp(6));
  	    insertStmt.get().setString(7, newRs.getString(7));
  	    insertStmt.get().setInt(8, newRs.getInt(8));
	    } catch (ClassCastException ce) {
	      Log.getLogWriter().info("new row in buyorders " +
	          "oid is " +  newRs.getInt(1));
	      throw ce;
	    }
	    Log.getLogWriter().info("inserting into buyorders in derby using getNewResultSet() method " +
	        "oid is " + newRs.getInt(1) +
	        " cid is " + newRs.getInt(2) +
	        " sid is " +  newRs.getInt(3) +
	        " qty is " + newRs.getInt(4) + 
	        " bid is " + newRs.getBigDecimal(5) +
	        " ordertime is " + newRs.getTimestamp(6) +
	        " status is " +  newRs.getString(7) +
	        " tid is " + newRs.getInt(8));

	    count = insertStmt.get().executeUpdate();
	    Log.getLogWriter().info("inserted " + count + " row");

	    if (count !=1 && !isPossibleDuplicate && testUniqueKeys) {
	      SQLAsyncEventListenerTest.testException = new TestException("insert from listener does not " +
	        "insert the corresponding row in back_end, inserted row is " + count);
	      throw SQLAsyncEventListenerTest.testException;
	    }
	  }
	
	protected void delete (Event event) throws SQLException {
		List<Object> newRow = event.getNewRow();
		boolean isPossibleDuplicate = event.isPossibleDuplicate();		
		int count =0;
		if (newRow != null) throw new TestException("BEFORE_DELETE is invoked, but " +
				"newRow is not null: " + newRow);
		
		ResultSet oldRowRs = event.getOldRowAsResultSet();
		Log.getLogWriter().info("deleted old rs is " + ResultSetHelper.listToString(
		    ResultSetHelper.asList(oldRowRs, false)));
		
		deleteStmt.get().setInt(1, (Integer)event.getPrimaryKey()[0]); //through primary key
		Log.getLogWriter().info("deleting from buyorders in derby where oid ="
				+ (Integer)event.getPrimaryKey()[0] );
		count = deleteStmt.get().executeUpdate();
		Log.getLogWriter().info("deleted " + count + " row");
		
		if (count !=1 && !isPossibleDuplicate && testUniqueKeys) {
		  SQLAsyncEventListenerTest.testException = new TestException("delete from listener does not " +
				"delete the corresponding row in back_end, deleted row is " + count);
		  throw SQLAsyncEventListenerTest.testException;
		}
	}
	
	protected void update(Event event) throws SQLException{
		boolean isPossibleDuplicate = event.isPossibleDuplicate();		
		int count =0;
		
		String sql = getSql(event); 
		//add where clause
		sql +=" where oid = " + event.getPrimaryKey()[0];
		Log.getLogWriter().info("update to derby: " + sql);

		count = stmt.get().executeUpdate(sql);
		Log.getLogWriter().info("updated " + count + " row");
			
		if (count !=1 && !isPossibleDuplicate && testUniqueKeys) {
		  SQLAsyncEventListenerTest.testException = new TestException( sql + " -- update from listener does not " +
				"update the corresponding row in back_end, updated row is " + count);
		  throw SQLAsyncEventListenerTest.testException;
		}
	}	
	
	protected void resetStatements() {
	  resetInsert();
	  resetDelete();
	  resetStmt();
	}
	
	protected void resetInsert() {
    PreparedStatement stmt = null;
    try {
      stmt = dConn.get().prepareStatement(insert);
    }catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    insertStmt.set(stmt);
	}
	
	protected void resetDelete() {
    PreparedStatement stmt = null;
    try {
      stmt = dConn.get().prepareStatement(delete);
    }catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    deleteStmt.set(stmt);
	}
	
	protected void resetStmt() {
    Statement s = null;
    try {
      s = dConn.get().createStatement();
    }catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    stmt.set(s);
	}
	
  protected static ThreadLocal<Connection> dConn = new ThreadLocal<Connection>()  {
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
  
  public void close(){
    Log.getLogWriter().info("AsyncEventListener.close() gets called");
  }
  
  public void start() {
    Log.getLogWriter().info("AsyncEventListener.start() gets called");
  }
    
}

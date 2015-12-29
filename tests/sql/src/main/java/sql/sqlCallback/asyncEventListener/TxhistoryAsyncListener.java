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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import sql.ClientDiscDBManager;
import sql.SQLBB;
import sql.SQLHelper;
import util.TestException;
import util.TestHelper;

import com.pivotal.gemfirexd.callbacks.Event;

public class TxhistoryAsyncListener extends AbstractAsyncListener {
	static String insert = "insert into trade.txhistory values (?,?,?,?,?,?,?,?)";
  //cid int, oid int, sid int, qty int, price decimal (30, 20), ordertime timestamp, 
  //type varchar(10), tid int,  constraint type_ch check (type in ('buy', 'sell'))
	static String delete = "delete from trade.txhistory where cid=? and oid=? and sid = ?" +
			" and qty =? and price=? and ordertime =? and type=? and tid =?";
	
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

  @Override
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
			int count = insertStmt.get().executeUpdate();
			Log.getLogWriter().info("inserted " + count + " row");
		} catch (SQLException se) {
		  /*
			if (se.getSQLState().equals("23513")) {
				Log.getLogWriter().info("get check constraint violation, " +
					"let gemfirexd perform the constraint check as well");
				return;
			}
      else
      */
		  Log.getLogWriter().info("Aynch Event Listener invoked means the op succeeded in gfxd");
      throw se;
		}
	}
	
	protected void delete (Event event) throws SQLException {
		Log.getLogWriter().info("SZHU in TX delete() ");
		List<Object> oldRow = event.getOldRow();
		Log.getLogWriter().info("SZHU TX old row " + oldRow);
		List<Object> newRow = event.getNewRow();
		Log.getLogWriter().info("SZHU TX new row " + newRow);
		int count =0;
		if (newRow != null) throw new TestException("AFTER_DELETE is invoked, but " +
				"newRow is not null: " + newRow);
		
		
		if (event.getPrimaryKey()[0] != null)
		Log.getLogWriter().warning("the primary key in txHistory listener is "
				+ event.getPrimaryKey()[0]); 


		deleteStmt.get().setInt(1, (Integer)( oldRow.get(0)));
		deleteStmt.get().setInt(2, (Integer) oldRow.get(1));
		deleteStmt.get().setInt(3, (Integer) oldRow.get(2));
		deleteStmt.get().setInt(4, (Integer) oldRow.get(3));
		deleteStmt.get().setBigDecimal(5, (BigDecimal) oldRow.get(4));
		deleteStmt.get().setTimestamp(6, (Timestamp) oldRow.get(5));
		deleteStmt.get().setString(7, (String) oldRow.get(6));
		deleteStmt.get().setInt(8, (Integer) oldRow.get(7));
		Log.getLogWriter().info("deleting from txhistory in derby for " +
				"cid is " + (Integer)( oldRow.get(0)) +
				" oid is " + (Integer) oldRow.get(1) +
				" sid is " +  oldRow.get(2) +
				" qty is " + oldRow.get(3) + 
				" price is " + (BigDecimal) oldRow.get(4) +
				" ordertime is " + (Timestamp) oldRow.get(5) +
				" type is " +  oldRow.get(6) +
				" tid is " + (Integer) oldRow.get(7));

		count = deleteStmt.get().executeUpdate();
		Log.getLogWriter().info("deleted " + count + " row");
	}
	
	protected void update(Event event) throws SQLException{	
		Log.getLogWriter().info("SZHU in TX update" );
		List<Object> oldRow = event.getOldRow();
		Log.getLogWriter().info("TX update old row " + oldRow);
		if (event.getPrimaryKey()[0] != null)
			Log.getLogWriter().warning("the primary key in txHistory listener is "
					+ event.getPrimaryKey()[0]); 
		String sql = getSql(event); 
		//add where clause
		sql +=" where cid = " + (Integer)( oldRow.get(0)) +
				" and oid = " + (Integer) oldRow.get(1) +
				" and sid = " +  oldRow.get(2) +
				" and qty = " + oldRow.get(3) + 
				" and price = " + (BigDecimal) oldRow.get(4) +
				" and ordertime = '" + (Timestamp) oldRow.get(5) +
				"' and type = '" +  oldRow.get(6) +
				"' and tid = " + (Integer) oldRow.get(7);
		Log.getLogWriter().info("update to derby: " + sql);
		
		int	count = stmt.get().executeUpdate(sql);
		Log.getLogWriter().info("updated " + count + " row");

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
   
}

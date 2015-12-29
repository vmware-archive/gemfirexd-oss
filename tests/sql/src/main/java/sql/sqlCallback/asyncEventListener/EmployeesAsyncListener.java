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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import sql.ClientDiscDBManager;
import sql.SQLHelper;
import util.TestException;
import util.TestHelper;

import com.pivotal.gemfirexd.callbacks.Event;

public class EmployeesAsyncListener extends AbstractAsyncListener {
  static String insert = "insert into default1.employees (eid) values (?)";
  
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
      
      //restart from the batching from previously failed one
      boolean success = processEvent(event);
      if (!success) {
        failedEvent = event;
        if (testUniqueKeys) return false; //no unique keys test has known issue of out of order inserting to queue
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
    boolean isPossibleDuplicate = event.isPossibleDuplicate();    
    //for HA case, before insert check if record has been inserted already to avoid duplicate
    int pk = (Integer)event.getPrimaryKey()[0];
    if (isPossibleDuplicate) {      
      ResultSet rs = stmt.get().executeQuery("select * from default1.employees where eid = "
          + pk);
      if (rs.next()) {
        Log.getLogWriter().info("this row has been inserted already " + newRow);
        //if it is there, do not insert again
      } else {
        doInsert(oldRow, newRow, isPossibleDuplicate, pk);
      }
    } else doInsert(oldRow, newRow, isPossibleDuplicate, pk);
      
  } 
  
  private void doInsert(List<Object> oldRow, List<Object> newRow, 
      boolean isPossibleDuplicate, int pk) throws SQLException {
    int count = 0;
    if (oldRow != null) throw new TestException("BEFORE_INSERT in employeesListener " +
        "is invoked, but oldRow is not null: " + oldRow);
  
    try {
      insertStmt.get().setInt(1, pk);
    } catch (ClassCastException ce) {
      Log.getLogWriter().info("new row in employees " +
          "eid is " +  pk );
      throw ce;
    }
    Log.getLogWriter().info("inserting into employees in derby " +
        "eid is " + pk);
         
    count = insertStmt.get().executeUpdate();
    Log.getLogWriter().info("inserted " + count + " row");


    if (count !=1 && !isPossibleDuplicate) 
      throw new TestException("insert from listener does not " +
        "insert the corresponding row in back_end, inserted row is " + count);
  }

  @Override
  protected void delete(Event event) throws SQLException {
    Log.getLogWriter().warning("There shoud be no delete event in employees table in the test");    
  }

  @Override
  protected void update(Event event) throws SQLException {
    Log.getLogWriter().warning("There shoud be no update event in employees table in the test");    
  }
  
  protected void resetStatements() {
    resetInsert();
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

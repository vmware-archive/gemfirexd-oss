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

import hydra.DerbyServerHelper;
import hydra.Log;
import hydra.TestConfig;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import sql.ClientDiscDBManager;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

import com.pivotal.gemfirexd.callbacks.AsyncEventListener;
import com.pivotal.gemfirexd.callbacks.Event;

public abstract class AbstractAsyncListener implements AsyncEventListener {
	public static String initStr = "org.apache.derby.jdbc.ClientDriver," +
    "jdbc:derby://" + DerbyServerHelper.getEndpoint().getHost() +
    ":" + DerbyServerHelper.getEndpoint().getPort() + "/" + ClientDiscDBManager.getDBName() +
    ";create=true";
	
	protected static boolean testUniqueKeys = TestConfig.tab().booleanAt(SQLPrms.testUniqueKeys, true);
	public static boolean useResultSetImpl = true; //SQLTest.random.nextBoolean();
	
	static {
	  Log.getLogWriter().info("useResultSetImpl is " + useResultSetImpl);
	}
	
	public void close(){
	  Log.getLogWriter().info("AsyncEventListener.close() gets called");
	}
	
	@SuppressWarnings("static-access")
  public void init(String initStr){
	  Log.getLogWriter().info("in init AsyncListener");
		if (!this.initStr.equals(initStr)) {		  
			throw new TestException ("init in Listener() does not get the initStr.");
		}
	}
	
	public abstract boolean processEvents(List<Event> events);
	public abstract boolean processEvent(Event event);
	
	//the sql string does not have the where condition yet
	protected String getSql(Event event) throws SQLException {
	  StringBuilder str = new StringBuilder();
	  if (useResultSetImpl) {
      /*
      ResultSet oldRowRs = event.getOldRowAsResultSet();
      if (oldRowRs == null) Log.getLogWriter().warning("old row from getOldRowAsResultSet() is " + oldRowRs);
      */
      ResultSet newRowRs = event.getNewRowsAsResultSet();
      if (newRowRs == null) throw new TestException ("new row from event.getNewRowAsResultSet() is "
          + newRowRs + " on AFTER_UPDATE");
      ResultSetMetaData updatedColMeta = newRowRs.getMetaData();
      
      Log.getLogWriter().info("newRowRs is "); 

      for (int i=0; i<updatedColMeta.getColumnCount(); i++) {
        Log.getLogWriter().info(newRowRs.getObject(i + 1) + ", " );
      }
       
      str.append("update " + updatedColMeta.getSchemaName(1) + "." 
          + updatedColMeta.getTableName(1) + " set ");    
      for (int i=0; i<updatedColMeta.getColumnCount(); i++) {
        str.append(" " + updatedColMeta.getColumnName(i+1) + "=");
        appendValue(str, newRowRs.getObject(i+1),
            updatedColMeta.getColumnType(i+1));
      }
	  } else {
  		ResultSetMetaData meta = event.getResultSetMetaData();
  		List<Object> newRow = event.getNewRow();
  
    		if (event.getModifiedColumns() == null) throw new TestException("event.getModifiedColumns " +
    				"return null on AFTER_UPDATE");
    		
    		str.append("update " + meta.getSchemaName(1) + "." 
    				+ meta.getTableName(1) + " set ");		
    		for (int i=0; i<event.getModifiedColumns().length; i++) {
                            str.append(" " + meta.getColumnName(event.getModifiedColumns()[i]) + "=");
                            appendValue(str, newRow.get(event.getModifiedColumns()[i]-1),
                                                    meta.getColumnType(event.getModifiedColumns()[i]));
    		}
    		
		}
	  
	  str.delete(str.length() -1 , str.length());
		return str.toString();
	}

	//how to append using Statement
	protected void appendValue(StringBuilder str, Object value, int type) {
		switch (type){
		case Types.BIGINT:
		case Types.DECIMAL:
		case Types.NUMERIC:
		case Types.SMALLINT:
		case Types.TINYINT:
		case Types.INTEGER:
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.REAL:
		case Types.TIMESTAMP:
			str.append(value + ",");
			break;
		default:
			str.append("'" + value +"',");
		}
	}
	
	public void start() {
	  Log.getLogWriter().info("AsyncEventListener.start() gets called");
	}
  
  protected abstract void insert (Event event) throws SQLException;  
  	
  protected abstract void delete (Event event) throws SQLException;
  
  protected abstract void update(Event event) throws SQLException;
  
  protected boolean isSame(Event e1, Event e2) {
    if (e1.getType() != e2.getType()) {
      Log.getLogWriter().info("not the same type for two events");
      return false;
    }
    else {
      boolean isSameOldRow = e1.getOldRow() == null? e2.getOldRow() == null :getRow(e1, false).equals(getRow(e2, false)) ;
      boolean isSameNewRow = e1.getNewRow() == null? e2.getNewRow() == null :getRow(e1, true).equals(getRow(e2, true));
      boolean hasSameModifiedColumns = Arrays.equals(e1.getModifiedColumns(), e2.getModifiedColumns());
      boolean hasSamePK = Arrays.equals(e1.getPrimaryKey(), e2.getPrimaryKey());
        
      if (e1.getPrimaryKey()[0] == e2.getPrimaryKey()[0]) {
        Log.getLogWriter().info("isSameOldRow is " + isSameOldRow);
        Log.getLogWriter().info("isSameNewRow is " + isSameNewRow);
        Log.getLogWriter().info("hasSameModifiedColumns is " + hasSameModifiedColumns );
        Log.getLogWriter().info("has same primary keys is " + hasSamePK);
      }
      
      return isSameOldRow && isSameNewRow 
          && Arrays.equals(e1.getModifiedColumns(), e2.getModifiedColumns())
          && Arrays.equals(e1.getPrimaryKey(), e2.getPrimaryKey());
    }

  }
  
  protected String getRow(Event event, boolean getNewRow) {
    ResultSetMetaData meta = event.getResultSetMetaData();
    List<Object> row = null;
    if (getNewRow) row = event.getNewRow();
    else row = event.getOldRow();
      
    StringBuilder str = new StringBuilder();
    for (int i=0; i<row.size(); i++) {
      try {
        str.append(meta.getColumnName(i+1) + ": " + row.get(i) + ",");
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().warning("gets unexpected exception " + TestHelper.getStackTrace(se));
      }
    }
    /*
    for (int i=0; i<event.getModifiedColumns().length; i++) {
      try {
        str.append(meta.getColumnName(event.getModifiedColumns()[i]) + "=");
        appendValue(str, row.get(event.getModifiedColumns()[i]-1),
                              meta.getColumnType(event.getModifiedColumns()[i]));
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().warning("gets unexpected exception " + TestHelper.getStackTrace(se));
      }
    }
    */
    Log.getLogWriter().info(str.toString());
    return str.toString();
  }
}

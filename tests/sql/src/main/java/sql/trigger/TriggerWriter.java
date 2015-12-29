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
package sql.trigger;

import java.sql.*;
import java.util.List;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventCallback;
import com.pivotal.gemfirexd.callbacks.Event.Type;

import hydra.*;
import sql.SQLHelper;
import sql.SQLTest;

public class TriggerWriter extends SQLTest implements EventCallback {
	  
	  public static TriggerWriter getTriggerCallback() {	    
		  return new TriggerWriter();
	  }
	  
	  public void close() throws SQLException {   
		  Log.getLogWriter().info("close writer");
	  }

	  public void init(String initStr) throws SQLException {
		  Log.getLogWriter().info("init writer : " + initStr);
	  }

	  /* Tests simulate before trigger behaviors , not after trigger behaviors */
	  public void onEvent(Event event) throws SQLException {		  
		  Log.getLogWriter().info("onEvent , Event Type : " + event.getType());
		  Connection conn = getGFEConnection();
		  String sql = null;
		  List<Object> row;
		  PreparedStatement stmt;		
		  try{
			  if ((event.getType()).equals(Type.BEFORE_INSERT)){			  
				  row = event.getNewRow();	    
				  sql = "insert into trade.customers_audit values(?,?,?,?,?)";  
				  stmt = conn.prepareStatement(sql);
				  stmt.setInt(1, ((Integer)row.get(0)).intValue());
				  stmt.setString(2, (String)row.get(1));
				  stmt.setDate(3, (Date)row.get(2));
				  stmt.setString(4, (String)row.get(3));		    
				  stmt.setInt(5, ((Integer)row.get(4)).intValue());		    
			  } else if ((event.getType()).equals(Type.BEFORE_DELETE)){
				  row = event.getOldRow();
				  sql = "delete from trade.customers_audit where cid = ?";
				  stmt = conn.prepareStatement(sql);	
				  stmt.setInt(1, ((Integer)row.get(0)).intValue());
			  } else if ((event.getType()).equals(Type.BEFORE_UPDATE)){
				  row = event.getNewRow();
				  sql = "update trade.customers_audit set addr = ? where cid = 1";  
				  stmt = conn.prepareStatement(sql);
				  stmt.setString(1,(String)row.get(3));
			  } else {
				  throw new IllegalArgumentException ("Unknown Event Type " + event.getType());
			  }
			  int numRow = stmt.executeUpdate();
			  Log.getLogWriter().info("SZHU modified " + numRow + " row to audit table");
			  stmt.close();
			  commit(conn);
			  conn.commit();
			  closeGFEConnection(conn);
		  }catch(SQLException se){
	    	SQLHelper.handleSQLException(se);	    
		  }
	  }
}
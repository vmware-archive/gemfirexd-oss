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
package sql.joinStatements;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLHelper;
import sql.sqlutil.ResultSetHelper;
import util.TestException;


public class MultiTablesJoinStmt extends AbstractJoinStmt {
	
  protected static String[] uniqSelect = {
  	"select * from trade.securities n, trade.myJoinView v " +
  		"where n.sec_id = v.port_sid and n.tid=?" 
  };

  protected static String[] nonUniqSelect = {
  	"select * from trade.securities n, trade.myJoinView v " +
			"where n.sec_id = v.port_sid ",
  };
  protected static boolean[] useDisk = {false};
  
  protected static int maxNumOfTries = 1;
  
	@Override
	public void query(Connection dConn, Connection gConn) {
    int whichQuery;
    ResultSet derbyRS;
    ResultSet gfxdRS;
    int tid = getMyTid();
    if (testUniqueKeys) { //test uniqKeys
      whichQuery = rand.nextInt(uniqSelect.length);      
    } else {
    	 whichQuery = rand.nextInt(nonUniqSelect.length); 
    }
          
    if (dConn!=null) {
    	derbyRS = getUniqQuery(dConn, whichQuery, tid);
      if (derbyRS == null) {
        Log.getLogWriter().info("could not get the derby result set after retry, abort this query");
        return;
      }
      List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
      if (derbyList == null) {
      	Log.getLogWriter().info("could not get derby results");
      	return;
      }
      gfxdRS = getUniqQuery (gConn, whichQuery, tid); 
      List<Struct> gfxdList = ResultSetHelper.asList(gfxdRS, false);
      if (gfxdList == null) {
      	Log.getLogWriter().info("could not get gfxd results");
      	return;
      }
      
      ResultSetHelper.compareResultSets(derbyList, gfxdList); 
    } else {
      synchronized (useDisk){
        gfxdRS = getNonUniqQuery (gConn, whichQuery);
        //could not verify results.
        if (gfxdRS != null)
        	ResultSetHelper.asList(gfxdRS, false);  
        else if (isHATest)
        	Log.getLogWriter().info("could not get gfxd query results after retry due to HA");
        else if (useDisk[0]==true) {
          Log.getLogWriter().info("not able to get the results set due to size is too large");
          useDisk[0] = false;
          return;
        } else
        	throw new TestException ("gfxd query returns null and not a HA test");  
      } 
    }	
	}
	
  public static ResultSet getUniqQuery(Connection conn, int whichQuery, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery(conn, whichQuery, tid, success);
    int count = 0;
    while (!success[0]) {
      if (count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finish update in derby, abort this operation");
        return rs;
      }
      count++;
      rs = getUniqQuery(conn, whichQuery, tid, success);
    } //retry
    return rs;
  }
  
  public static ResultSet getUniqQuery(Connection conn, int whichQuery, 
  		int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";                         
      Log.getLogWriter().info(database + "Querying SecuritiesMyJoinView with TID:" + tid  + uniqSelect[whichQuery]);
      stmt = conn.prepareStatement(uniqSelect[whichQuery]);            
      stmt.setInt(1, tid);
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (se.getSQLState().equals("X0Z01") && isHATest) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got the expected node went down exception, continuing test");
        success[0] = false; 
      } else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  //retries when lock could not obtained
  public static ResultSet getNonUniqQuery(Connection conn, int whichQuery) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery(conn, whichQuery, success);
    while (!success[0]) {
      rs = getNonUniqQuery(conn, whichQuery, success);
    } //retry
    return rs;
  }
  
  public static ResultSet getNonUniqQuery(Connection conn, int whichQuery, 
  		boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";                         
      Log.getLogWriter().info(database + "Querying SecuritiesMyJoinView with no data "  + nonUniqSelect[whichQuery]);      
      stmt = conn.prepareStatement(nonUniqSelect[whichQuery]);      
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      /*else if (se.getSQLState().equals("0A000") && se.getMessage().contains("disk")) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got the unsupported exception, need to remove this once bug#40348 is fixed, continuing test");
        useDisk[0] = true;
        return null;
      } */
      else if (se.getSQLState().equals("X0Z01") && isHATest) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got the expected node went down exception, continuing test");
        success[0] = false; 
      } else SQLHelper.handleSQLException(se);
    }
    return rs;
  }
	
	public void createViewsForJoin(Connection conn) {
		String sql = "create view trade.myJoinView " +
				"(cid, name, port_sid, qty, subTotal, ask) " +
				"as select f.cid, cust_name, f.sid, f.qty, subTotal, ask " +
				"from trade.customers c, trade.portfolio f, trade.sellorders so " +
				"where c.cid= f.cid and c.cid = so.cid and f.tid = so.tid " +
						"and f.sid = so.sid and subTotal >10000";
			
		try {
			Statement stmt = conn.createStatement();
			Log.getLogWriter().info(sql);
			stmt.execute(sql);
			conn.commit();
		} catch (SQLException se) {
			SQLHelper.handleSQLException(se);
		}
	}
	
}

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
package examples.joinquerytodap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import com.pivotal.gemfirexd.procedure.OutgoingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public class JoinQueryTuneProcedure {   
  
  public static void execute(ResultSet[] outResultSet,
      ProcedureExecutionContext context) {
    Logger logger = Logger.getLogger("com.pivotal.gemfirexd");    
    logger.info("JoinQueryTuneProcedure::execute called");
    
    HashSet<JoinQueryTuneProcedure.Key> cachedRows = new HashSet<JoinQueryTuneProcedure.Key>();
    ResultSet rs = null;    
    
    try {
      Connection conn = context.getConnection();
      
      //cache results of inner query
      cacheRightRS(conn, cachedRows);
      logger.info("JoinQueryTuneProcedure::execute cachedRows from inner query are " + cachedRows.size());
      
      // get dsid once 
      ResultSet r  = conn.createStatement().executeQuery("values dsid()");
      r.next();
      String datanodeId = r.getString(1);
      r.close();
            
      // outer query
      String lsql = "<local> select a.equip_id, a.context_id, a.stop_date " /* + ", dsid() as datanode_id " */
          + "from CONTEXT_HISTORY a "
          + "left join CHART_HISTORY b "
          + "on (a.equip_id =b.equip_id and a.context_id=b.context_id and a.stop_date=b.stop_date) ";
      
      logger.info("JoinQueryTuneProcedure::execute  processing outer query started");
      Statement st = conn.createStatement();
      rs = st.executeQuery(lsql);
      logger.info("JoinQueryTuneProcedure::execute  executed outer query, now reading ResultSet" );
      
      //set single outgoing resultSet
      OutgoingResultSet ors = context.getOutgoingResultSet(1);
      
      // add a column definition to outgoing resultset
      ors.addColumn("eqp_id");
      ors.addColumn("cntxt_id");
      ors.addColumn("stop_dt");
      ors.addColumn("datanode_id");
      final Key l = new Key();
      
      int outCnt = 0;   
      int matchedCnt = 0;
      
      while(rs.next()) {
        outCnt++;
        l.eqp_id = rs.getString(1);
        l.cntxt_id = rs.getInt(2);
        
        // Compare the row
        if (cachedRows.contains(l)) {
          List<Object> row = new ArrayList<Object>();
          row.add(l.eqp_id);
          row.add(l.cntxt_id);
          row.add(rs.getTimestamp(3));
          row.add(datanodeId);
          
          ors.addRow(row);
          matchedCnt++;
        }
      }        
      
      // add end of result marker
      ors.endResults();
      logger.info("TSMCJoinStoreProc::execute processing outer query finished,"
          + " total rows retuned=" + outCnt + ", matched rows=" + matchedCnt);

    } catch (SQLException sqle) {
      sqle.printStackTrace();
      throw new RuntimeException(sqle);
    }
    finally {
      if (rs != null) {
        try {
          rs.close();
        }
        catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }
  
  static void cacheRightRS(Connection conn, HashSet<Key> cachedRows) {
    ResultSet rrs = null;
    Statement st = null;
    
    try {
      // inner query
      String sql = "<global> select equip_id, context_id "
          + "from RECEIVED_HISTORY_LOG where 1=1 " 
          + "and exec_time > 40 "
          + "and stop_date > '2014-01-24 18:47:59' "
          + "and stop_date < '2014-01-24 18:49:59' ";

      st = conn.createStatement();
      rrs = st.executeQuery(sql);
      while (rrs.next()) {
        cachedRows.add(new Key(rrs.getString(1), rrs.getInt(2)));
      }
    }
    catch (SQLException sqle) {
      sqle.printStackTrace();
    }
    finally {
      try {
        if (rrs != null) {
          rrs.close();
        }
      }
      catch (SQLException sqle) {
        sqle.printStackTrace();
      }
      if (st != null) {
        try {
          st.close();
        }
        catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  protected static class Key implements Comparable<Key> {
    protected String eqp_id;
    protected int cntxt_id;
    
    public Key(String eqp_id, int cntxt_id) {
      this.eqp_id = eqp_id;
      this.cntxt_id = cntxt_id;
    }

    public Key() {
    }

    @Override
    public int hashCode() {
      int h = this.eqp_id.hashCode();
      h |= this.cntxt_id;
      return h;
    }

    @Override
    public boolean equals(Object o) {
      return (compareTo((Key)o) == 0);
    }

    public int compareTo(Key o) {
      int res = this.eqp_id.compareTo(o.eqp_id);
      if (res != 0) {
        return res;
      }
      res = this.cntxt_id < o.cntxt_id ? -1 : this.cntxt_id > o.cntxt_id ? 1 : 0;
      
      return res;
    }

    public String toString() {
      return "key (" + hashCode() + ") " + this.eqp_id + " " + this.cntxt_id;
    }
  }
}

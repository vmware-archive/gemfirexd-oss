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
package sql.dmlStatements.json;
import hydra.Log;
import hydra.MasterController;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import sql.dmlStatements.TradeBuyOrdersDMLStmt;
import sql.security.SQLSecurityTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.query.Struct;

public class TradeBuyOrderDMLStmtJson extends TradeBuyOrdersDMLStmt{

  protected static String columns = "oid,cid,sid,qty,bid,ordertime,json_evalPath(json_details,'$..status') as status,tid" ;
  
  protected static String[] selectJSON = {
    //uniqkey queries
    "select " + columns + " from trade.buyorders where   json_evalPath(json_details,'$..status') = 'open' and tid = ?",
    "select cid, bid, cid, sid from trade.buyorders where  cast(json_evalPath(json_details,'$..cid') as integer) >? and cast(json_evalPath(json_details,'$..sid') as integer) <? and qty >? and orderTime<? and tid = ?",
    "select sid, count(*) from trade.buyorders  where status =? and tid =? GROUP BY sid HAVING count(*) >=1",  
    "select cid , min( cast(json_evalPath(json_details,'$..qty') as integer)*bid) as smallest_order from trade.buyorders  where status =? and tid =? GROUP BY cid",  
    "select cid, cast (avg( cast(json_evalPath(json_details,'$..qty') as integer)*bid) as decimal (30, 20)) as amount from trade.buyorders  where  cast(json_evalPath(json_details,'$..status') as varchar(10)) =? and tid =? GROUP BY cid ORDER BY amount",
    "select cid, max( qty*bid) as largest_order from trade.buyorders  where status =? and tid =?  GROUP BY cid HAVING max(qty*bid) > 2000 ORDER BY largest_order DESC, cid DESC ",
    "select cast(json_evalPath(b1.json_details,'$..oid') as integer) as oid, b1.cid, b1.sid, cast(json_evalPath(b1.json_details,'$..qty') as integer) as qty, b1.tid, cast(json_evalPath(b2.json_details,'$..oid') as integer) as oid2, b2.cid, b2.sid, cast(json_evalPath(b2.json_details,'$..qty') as integer) as qty2, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
        "where b1.cid = b2.cid and b1.tid = ? and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid", //add self join coverage
    "select cid , tid, json_evalPath(json_evalPath (buyorder_json,'$..buyorder[?(@.status == open)]') , '$..[?(@.oid == ?)]' ), status from trade.customers where  json_evalattribute(json_evalPath (buyorder_json,'$..buyorder[?(@.status == open)]') , '$..[?(@.oid == ?)]' ) and tid = ?  order by cid",
           
    //no uniqkey queries
    "select cid, bid, cid, sid from trade.buyorders where cid >?  and sid <? and qty >? and orderTime<?  ",
    "select sid, count(*) from trade.buyorders  where status =? GROUP BY sid HAVING count(*) >=1", 
    "select cid,count(sid) from trade.buyorders  where status =? GROUP BY cid",    
    "select cid, cast (avg(qty*bid) as decimal (30, 20)) as amount from trade.buyorders  where status =? GROUP BY cid ORDER BY amount",
    "select cid, max(qty*bid) as largest_order from trade.buyorders  where status =? GROUP BY cid HAVING max(qty*bid) > 20000 ORDER BY max(qty*bid), cid DESC ",
    "select b1.oid, b1.cid, b1.sid, b1.qty, b1.tid, b2.oid, b2.cid, b2.sid, b2.qty, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
    "where b1.cid = b2.cid and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid",
  };
    
  protected static String[] selectwoaggreateJSON = {
    //uniqkey queries
    "select " +  columns + "from trade.buyorders where  json_evalPath(json_details,'$..status')  = 'open' and tid = ?",
    "select cid, bid, cid, sid from trade.buyorders where cast(json_evalPath(json_details,'$..cid') as integer) >? and cast(json_evalPath(json_details,'$..sid') as integer) <? and qty >? and orderTime<? and tid = ?",
    "select" +  columns +" from trade.buyorders  where json_evalPath(json_details,'$..status')  =? and tid =? order by sid",  
    "select " +  columns + " from trade.buyorders  where json_evalPath(json_details,'$..status') =? and tid =? order by cid",  
    "select cid , ( json_evalPath(json_details,'$..qty') *bid) as totalAmt from trade.buyorders  where status =? and tid =? order by cid, oid, sid",
    "select " + columns + "  from trade.buyorders  where status =? and tid =? order by cid",
    "select b1.oid as oid, b1.cid, b1.sid, b1.qty as qty, b1.tid, b2.oid as oid2, b2.cid, b2.sid, b2.qty as qty2, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
        "where b1.cid = b2.cid and b1.tid = ? and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid", //add self join coverage
    //no uniqkey queries
    "select  " +  columns + " from trade.buyorders",
    "select cid, bid, cid, sid from trade.buyorders where cid >?  and sid <? and qty >? and orderTime<?  ",
    "select sid, count(*) from trade.buyorders  where status =? GROUP BY sid HAVING count(*) >=1", 
    "select cid,count(sid) from trade.buyorders  where status =? GROUP BY cid",    
    "select cid, cast (avg(qty*bid) as decimal (30, 20)) as amount from trade.buyorders  where status =? GROUP BY cid ORDER BY amount",
    "select cid, max(qty*bid) as largest_order from trade.buyorders  where status =? GROUP BY cid HAVING max(qty*bid) > 20000 ORDER BY max(qty*bid), cid DESC ",
    "select b1.oid, b1.cid, b1.sid, b1.qty, b1.tid, b2.oid, b2.cid, b2.sid, b2.qty, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
    "where b1.cid = b2.cid and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid",
  };
  
  
  protected static String[] select = {
    //uniqkey queries
    "select * from trade.buyorders where status = 'open' and tid = ?",
    "select cid, bid, cid, sid from trade.buyorders where cid >? and sid <? and qty >? and orderTime<? and tid = ?",
    "select sid, count(*) from trade.buyorders  where status =? and tid =? GROUP BY sid HAVING count(*) >=1",  
    "select cid, min(qty*bid) as smallest_order from trade.buyorders  where status =? and tid =? GROUP BY cid",  
    "select cid, cast (avg(qty*bid) as decimal (30, 20)) as amount from trade.buyorders  where status =? and tid =? GROUP BY cid ORDER BY amount",
    "select cid, max(qty*bid) as largest_order from trade.buyorders  where status =? and tid =?  GROUP BY cid HAVING max(qty*bid) > 2000 ORDER BY largest_order DESC, cid DESC ",
    "select b1.oid as oid, b1.cid, b1.sid, b1.qty as qty, b1.tid, b2.oid as oid2, b2.cid, b2.sid, b2.qty as qty2, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
        "where b1.cid = b2.cid and b1.tid = ? and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid", //add self join coverage
    //no uniqkey queries
    "select * from trade.buyorders",
    "select cid, bid, cid, sid from trade.buyorders where cid >?  and sid <? and qty >? and orderTime<?  ",
    "select sid, count(*) from trade.buyorders  where status =? GROUP BY sid HAVING count(*) >=1", 
    "select cid,count(sid) from trade.buyorders  where status =? GROUP BY cid",    
    "select cid, cast (avg(qty*bid) as decimal (30, 20)) as amount from trade.buyorders  where status =? GROUP BY cid ORDER BY amount",
    "select cid, max(qty*bid) as largest_order from trade.buyorders  where status =? GROUP BY cid HAVING max(qty*bid) > 20000 ORDER BY max(qty*bid), cid DESC ",
    "select b1.oid, b1.cid, b1.sid, b1.qty, b1.tid, b2.oid, b2.cid, b2.sid, b2.qty, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
    "where b1.cid = b2.cid and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid",
  };
  protected static String[] selectwoaggreate = {
    //uniqkey queries
    "select * from trade.buyorders where status = 'open' and tid = ?",
    "select cid, bid, cid, sid from trade.buyorders where cid >? and sid <? and qty >? and orderTime<? and tid = ?",
    "select * from trade.buyorders  where status =? and tid =? order by sid",  
    "select * from trade.buyorders  where status =? and tid =? order by cid",  
    "select cid,  (qty*bid) as totalAmt  from trade.buyorders  where status =? and tid =? order by cid, oid, sid",
    "select * from trade.buyorders  where status =? and tid =? order by cid",
    "select b1.oid as oid, b1.cid, b1.sid, b1.qty as qty, b1.tid, b2.oid as oid2, b2.cid, b2.sid, b2.qty as qty2, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
        "where b1.cid = b2.cid and b1.tid = ? and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid", //add self join coverage
    //no uniqkey queries
    "select * from trade.buyorders",
    "select cid, bid, cid, sid from trade.buyorders where cid >?  and sid <? and qty >? and orderTime<?  ",
    "select sid, count(*) from trade.buyorders  where status =? GROUP BY sid HAVING count(*) >=1", 
    "select cid,count(sid) from trade.buyorders  where status =? GROUP BY cid",    
    "select cid, cast (avg(qty*bid) as decimal (30, 20)) as amount from trade.buyorders  where status =? GROUP BY cid ORDER BY amount",
    "select cid, max(qty*bid) as largest_order from trade.buyorders  where status =? GROUP BY cid HAVING max(qty*bid) > 20000 ORDER BY max(qty*bid), cid DESC ",
    "select b1.oid, b1.cid, b1.sid, b1.qty, b1.tid, b2.oid, b2.cid, b2.sid, b2.qty, b2.tid from trade.buyorders b1, trade.buyorders b2 " +
    "where b1.cid = b2.cid and b1.sid = b2.sid and b1.qty < b2.qty and b1.oid < b2.oid",
  };
  
  protected static String insertJSON = "insert into trade.buyorders values (?,?,?,?,?,?,?,?,?)";
  protected static String putJSON = "put into trade.buyorders values (?,?,?,?,?,?,?,?,?)";
  
  //Namrata - need to modify update json stmt. to use & and | operators in jsonpath
  protected static String[] updateJSON = { 
    //uniq
    "update trade.buyorders set status = 'filled'  where sid = ? and bid>? and status = 'open' and tid = ? ",  //for trigger test it could be a batch update
    "update trade.buyorders set status = 'cancelled' where (ordertime >? or sid=?) and status = 'open' and tid =? ",  //batch operation
    "update trade.buyorders set bid = ? where cid = ? and sid= ? and status = 'open' and qty >? and tid =? ",
    "update trade.buyorders set bid = ? , qty=? where cid = ? and sid= ? and bid <? and status = 'open' and tid =? ",
    "update trade.buyorders set sid = ? where cid = ? and sid= ? and status = 'open' and tid =? ",
    //no uniq
    "update trade.buyorders set status = 'filled'  where sid = ? and bid>? and status = 'open' ",  //for trigger test it could be a batch update
    "update trade.buyorders set status = 'cancelled' where (ordertime <? or sid=?) and status = 'open'  ",  //batch operation
    "update trade.buyorders set bid = ? where cid = ? and sid= ? and status = 'open' and qty >? ",
    "update trade.buyorders set bid = ? , qty=? where cid = ? and sid= ? and bid <? and status = 'open'  " ,  
    "update trade.buyorders set sid = ? where cid = ? and sid= ? and status = 'open'  "
  }; 
  
//Namrata - need to modify delete json stmt. to use & and | operators in jsonpath
  protected static String[] deleteJSON = {
    //uniqkey                            
    "delete from trade.buyorders where cid=? and sid=? and tid=?", //could be batch delete, but no foreign key reference, this should work
    "delete from trade.buyorders where oid=? and (bid <? or qty>?) and tid=?",
    "delete from trade.buyorders where (status ='cancelled' or status = 'filled' or qty<?) and bid>? and tid=?",
    //non uniqkey
    "delete from trade.buyorders where cid=? and sid=?",
    "delete from trade.buyorders where oid=? and (bid <? or qty>?) ",
    "delete from trade.buyorders where (status ='cancelled' or status = 'filled' or qty<?) and bid>?",
  };
  
  protected static String insert = "insert into trade.buyorders values (?,?,?,?,?,?,?,?)";
  protected static String put = "put into trade.buyorders values (?,?,?,?,?,?,?,?)";
  
  LinkedList<Integer>  listOfOids = new LinkedList<Integer>();
  

  
  public void insert(Connection dConn, Connection gConn, int size, int[] cids, boolean isPut) {
    
    boolean usebatch = false;    
    
    if (SQLTest.syncHAForOfflineTest) {
      usebatch = false;
      size = 1;
    } //avoid #39605 due to X0Z09
    
    //usebatch = false;
    //size = 1;
    
    int[] cid = new int[size];
    for (int i=0; i<size; i++){
      cid[i] = cids[0];
    }
    int[] oid = new int[size];
    int[] sid = new int[size];
    int[] qty = new int[size];
    String status = "open";
    Timestamp[] time = new Timestamp[size];
    BigDecimal[] bid = new BigDecimal[size];
    List<SQLException> exceptionList = new ArrayList<SQLException>();
    
    if (dConn != null) {
      //get the data
      if (gConn==null || rand.nextInt(numGettingDataFromDerby) == 1) 
        getDataForInsert(dConn, oid, cid, sid, qty, time, bid, size); //populate thru loader, gConn could be null during insert
      else  
        getDataForInsert(gConn, oid, cid, sid, qty, time, bid, size);
      
      //due to #49452, fk was taken out 
      if (hasHdfs) {
        for (int id: sid) {
          if (id == 0) {
            Log.getLogWriter().info("do not insert 0 for hdfs tests when fk was taken out due to #49452");
            return;
          }
        }
      }
      
      boolean success;
      if (usebatch) {
        for (int id: sid) {
          if (id == 0) {
            Log.getLogWriter().info("could not get enough valid sid for " +
                        "batching insert"); //avoid #39065 in the test
            return;
          }
        } 
        success = insertToDerbyTableUsingBatch(dConn, oid, cid, sid, qty, status, time, bid, size, exceptionList); 
      }
      else success = insertToDerbyTable(dConn, oid, cid, sid, qty, status, time, bid, size, exceptionList);  //insert to derby table  
      if (!success) {
        Log.getLogWriter().info("Could not finish the op in derby, will abort this operation in derby");
        rollback(dConn); 
        if (alterTableDropColumn && SQLTest.alterTableException.get() != null
            && (Boolean)SQLTest.alterTableException.get() == true) 
          ; //do nothing, expect gfxd fail with the same reason due to alter table
        else return; 
      }
      if (gConn == null) {
        Log.getLogWriter().info("connection is null, must be a load test");
        return;
      }
      try {
        if (usebatch) insertToGfxdTableUsingBatch(gConn, oid, cid, sid, qty, status, time, bid, size, exceptionList, isPut); 
        else insertToGFETable(gConn, oid, cid, sid, qty, status, time, bid, size, exceptionList, isPut); 
      } catch (TestException te) {
        if (te.getMessage().contains("Execute SQL statement failed with: 23505")
            && isHATest && SQLTest.isEdge) {
          //checkTicket49605(dConn, gConn, "buyorders");
          try {
            checkTicket49605(dConn, gConn, "buyorders");
          } catch (TestException e) {
            Log.getLogWriter().info("insert failed due to #49605", e);
            //deleteRow(dConn, gConn, "buyorders", oid[0], -1, null, null);
            //use put to work around #43751/#49605 issue of region map missing insert
            Log.getLogWriter().info("retry this using put to work around #49605");
            if (usebatch) insertToGfxdTableUsingBatch(gConn, oid, cid, sid, qty, status, time, bid, size, exceptionList, true); 
            else insertToGFETable(gConn, oid, cid, sid, qty, status, time, bid, size, exceptionList, true); 
            
            checkTicket49605(dConn, gConn, "buyorders");
          }
        } else throw te;
      }
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else {
      getDataForInsert(gConn, oid, cid, sid, qty, time, bid, size); //get the data
      //due to #49452, fk was taken out 
      if (hasHdfs) {
        for (int id: sid) {
          if (id == 0) {
            Log.getLogWriter().info("do not insert 0 for hdfs tests when fk was taken out due to #49452");
            return;
          }
        }
      }
      insertToGFETable(gConn, oid, cid, sid, qty, status, time, bid, size, isPut);
    } //no verification
  }
//insert intogemfirexd
  protected void insertToGFETable(Connection conn, int[] oid, int[] cid, int[] sid, int[] qty,
      String status, Timestamp[] time, BigDecimal[] bid, int size, List<SQLException> exceptions, boolean isPut) {
    
    PreparedStatement stmt = getStmt(conn, isPut ? putJSON : insertJSON);
   
    if (SQLTest.testSecurity && stmt == null) {
        SQLHelper.handleGFGFXDException((SQLException)
                        SQLSecurityTest.prepareStmtException.get(), exceptions);
        SQLSecurityTest.prepareStmtException.set(null);
        return;
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    int count = -1; 
    for (int i=0 ; i<size ; i++) {
      try {
        count = insertToTable(stmt, oid[i], cid[i], sid[i], qty[i], status, time[i], bid[i], tid, isPut);
        if (count != (verifyRowCount.get(tid+"_insert"+i)).intValue()) {
          Log.getLogWriter().info("Gfxd insert to buyorders has different row count from that of derby " +
            "derby inserted " + (verifyRowCount.get(tid+"_insert"+i)).intValue() +
            " but gfxd inserted " + count);
        }
      } catch (SQLException se) {
        if (isPut && se.getSQLState().equals("0A000")) {
          Log.getLogWriter().info("Got expected Feature not Supported Exception during put, continuing test");  
        } else   
        SQLHelper.handleGFGFXDException(se, exceptions);
      }
    } 
  }
  
  //insert intogemfirexd/gfe w/o verification
  protected void insertToGFETable(Connection conn, int[] oid, int[] cid, int[] sid, int[] qty,
      String status, Timestamp[] time, BigDecimal[] bid, int size, boolean isPut)  {
    PreparedStatement stmt;

    if (SQLTest.hasJSON)
      stmt = getStmt(conn, isPut ? TradeBuyOrderDMLStmtJson.putJSON :  TradeBuyOrderDMLStmtJson.insertJSON);
    else
       stmt = getStmt(conn, isPut ? put : insert);
    
    if (SQLTest.testSecurity && stmt == null) {
        if (SQLSecurityTest.prepareStmtException.get() != null) {
          SQLSecurityTest.prepareStmtException.set(null);
          return;
        } else ; //need to find out why stmt is not obtained
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    
    for (int i=0 ; i<size ; i++) {
      try {
        insertToTable(stmt, oid[i], cid[i], sid[i], qty[i], status, time[i], bid[i], tid, isPut);
      } catch (SQLException se) {
        if (se.getSQLState().equals("23503"))  
          Log.getLogWriter().info("detected the foreign key constraint violation, continuing test");
        else if (se.getSQLState().equals("42500") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
             " continuing tests");
        } else if (alterTableDropColumn && se.getSQLState().equals("42802")) {
          Log.getLogWriter().info("Got expected column not found exception in insert, continuing test");
        } else if (se.getSQLState().equals("23505")
            && isHATest && SQLTest.isEdge) {
          Log.getLogWriter().info("detected pk constraint violation during insert -- relaxing due to #43571, continuing test");
        } else if (alterTableDropColumn && se.getSQLState().equals("42X14")) {
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else if (isPut && se.getSQLState().equals("0A000")) {
          Log.getLogWriter().info("Got expected Feature not Supported Exception during put, continuing test"); 
        } else   
          SQLHelper.handleSQLException(se); //handle the exception
      }
    }    
  }
  
  protected void addBatchInsert(PreparedStatement stmt, int oid, int cid, int sid, int qty,
      String status, Timestamp time, BigDecimal bid, int tid, boolean isPut) throws SQLException {
    
    JSONObject json = new JSONObject();
    String jsonLog ="";
    
    if (SQLTest.hasJSON &&  ! SQLHelper.isDerbyConn(stmt.getConnection()) ) {
           json = getJSONObject(oid,cid,sid,qty,status,time,bid,tid);
           jsonLog = ",JSON_DETAILS: " +json.toJSONString();
    }
    
    Log.getLogWriter().info( (SQLHelper.isDerbyConn(stmt.getConnection())? "Derby - " :"gemfirexd - "  ) +  (isPut ? "putting " : "inserting ") + " into trade.buyorders with data OID:" + oid +
        ",CID:"+ cid + ",SID:" + sid + ",QTY:" + qty + ",STATUS:" + status +
        ",TIME:"+ time + ",BID:" + bid + ",TID:" + tid + jsonLog);
    
    stmt.setInt(1, oid);
    stmt.setInt(2, cid);
    stmt.setInt(3, sid);
    stmt.setInt(4, qty);
    stmt.setBigDecimal(5, bid);
    stmt.setTimestamp(6, time);
    stmt.setString(7, status);       
    stmt.setInt(8, tid);
    if (SQLTest.hasJSON &&  ! SQLHelper.isDerbyConn(stmt.getConnection()) ) {  Clob jsonClob = stmt.getConnection().createClob();
    jsonClob.setString(1, json.toJSONString());
    stmt.setClob(9, jsonClob); }
    stmt.addBatch();
  }
  
  protected void insertToGfxdTableUsingBatch(Connection conn, int[] oid, int[] cid, int[] sid, int[] qty,
      String status, Timestamp[] time, BigDecimal[] bid, int size, List<SQLException> exceptions, boolean isPut) {
    PreparedStatement stmt ;
    
    if (SQLTest.hasJSON ) 
      stmt = getStmt(conn, isPut ? putJSON : insertJSON);
    else 
      stmt = getStmt(conn, isPut ? put : insert);
    
    if (SQLTest.testSecurity && stmt == null) {
      SQLHelper.handleGFGFXDException((SQLException)
          SQLSecurityTest.prepareStmtException.get(), exceptions);
      SQLSecurityTest.prepareStmtException.set(null);
      return;
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    int tid = getMyTid();
    
    int[] counts = null;
    for (int i=0 ; i<size ; i++) {
      try {
        
        addBatchInsert(stmt, oid[i], cid[i], sid[i], qty[i], status, time[i], bid[i], tid, isPut);
      } catch (SQLException se) {
        SQLHelper.printSQLException(se);
        SQLHelper.handleGFGFXDException(se, exceptions); //should not see any exceptions here        
      }   
    }
    
    try {
      counts = stmt.executeBatch(); 
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se); //not expect any exception on update//not expect any exception on this update 
    }   
    
    for (int i =0; i<counts.length; i++) {  
      if (counts[i] != -3) {   
        JSONObject json = new JSONObject();
        String jsonLog ="";
        if (SQLTest.hasJSON ) {
          json = getJSONObject(oid[i],cid[i],sid[i],qty[i],status,time[i],bid[i],tid);
          jsonLog = ",JSON_DETAILS: " +json.toJSONString();
   }
        
        Log.getLogWriter().info("gemfirexd - inserted" + counts[i] + " rows in trade.buyorders OID:" + oid[i] +
                    ",CID:"+ cid[i] + ",SID:" + sid[i] + ",QTY:" + qty[i] + ",STATUS:" + status +
                    ",TIME:"+ time[i] + ",BID:" + bid[i] + ",TID:" + tid + jsonLog);
        if (counts[i] != verifyRowCount.get(tid+"_insert"+i))
          Log.getLogWriter().warning("gemfirexd insert to buyorders has different row count from that of derby " +
            "derby inserted " + verifyRowCount.get(tid+"_insert"+i) +
            " but gfxd inserted " + counts[i]);
      } else {
        Log.getLogWriter().warning("gfxd failed to update in batch update");
      }
    } 
    
  }
  
  
  protected int insertToTable(PreparedStatement stmt, int oid, int cid, int sid, int qty,
      String status, Timestamp time, BigDecimal bid, int tid, boolean isPut) throws SQLException {
        
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - " +  " ";
    
    
    JSONObject json = new JSONObject();
    String  jsonLog ="";
    if (!SQLHelper.isDerbyConn(stmt.getConnection())) { 
       json = getJSONObject(oid,cid,sid,qty,status,time,bid,tid);
       jsonLog = ",JSON_DETAILS: " +json.toJSONString();
    }
    
    Log.getLogWriter().info( database + (isPut ? "putting" : "inserting" ) + " into trade.buyorders with OID:" + oid +
        ",CID:"+ cid + ",SID:" + sid + ",QTY:" + qty + ",STATUS:" + status +
        ",TIME:"+ time + ",BID:" + bid + ",TID:" + tid + jsonLog);
    
    stmt.setInt(1, oid);
    stmt.setInt(2, cid);
    stmt.setInt(3, sid);
    stmt.setInt(4, qty);
    stmt.setBigDecimal(5, bid);
    stmt.setTimestamp(6, time);
    stmt.setString(7, status);       
    stmt.setInt(8, tid);
    if (!SQLHelper.isDerbyConn(stmt.getConnection())) { 
    Clob jsonClob = stmt.getConnection().createClob();
    jsonClob.setString(1, json.toJSONString());
    stmt.setClob(9, jsonClob);
    }
    
    int rowCount = stmt.executeUpdate();

    Log.getLogWriter().info( database + (isPut ? "put " : "inserted " ) + rowCount + " rows in trade.buyorders with OID:" + oid +
        ",CID:"+ cid + ",SID:" + sid + ",QTY:" + qty + ",STATUS:" + status +
        ",TIME:"+ time + ",BID:" + bid + ",TID:" + tid + jsonLog);
    
    if (SQLTest.hasJSON &&  !SQLHelper.isDerbyConn(stmt.getConnection()) && !SQLTest.hasTx && !setCriticalHeap)  insertUpdateCustomerJson(stmt.getConnection(), cid, json);
    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 

    if ( database.contains("gemfirexd") && isPut) {
      if (! SQLTest.ticket49794fixed) {
        insertToBuyordersFulldataset(stmt.getConnection() , oid, cid, sid, qty, bid, time, status, tid);
        }      
      Log.getLogWriter().info( database +  (isPut ? "putting" : "inserting" ) + " into trade.buyorders with OID:" + oid +
          ",CID:"+ cid + ",SID:" + sid + ",QTY:" + qty + ",STATUS:" + status +
          ",TIME:"+ time + ",BID:" + bid + ",TID:" + tid + jsonLog);    
      
     rowCount = stmt.executeUpdate();
     
     Log.getLogWriter().info( database + (isPut ? "put" : "inserted" ) + rowCount + " rows in trade.buyorders with OID:" + oid +
         ",CID:"+ cid + ",SID:" + sid + ",QTY:" + qty + ",STATUS:" + status +
         ",TIME:"+ time + ",BID:" + bid + ",TID:" + tid + jsonLog);   
     warning = stmt.getWarnings(); //test to see there is a warning   
     if (warning != null) {
       SQLHelper.printSQLWarning(warning);
     } 
 }    
    return rowCount;
  }
  
  
  
  
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys, boolean[] unsupported){
    PreparedStatement stmt = null;
    String[] update ;
    if ( ! SQLHelper.isDerbyConn(conn) ) 
      update=TradeBuyOrderDMLStmtJson.updateJSON;
    else
      update = TradeBuyOrderDMLStmtJson.update;
    switch (whichUpdate) {
    case 0: 
      //  "update trade.buyorders set status = 'filled'  where sid = ? and bid>? and status = 'open' and tid = ? ",  //for trigger test it could be a batch update
      if (partitionKeys.contains("status")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
      //"update trade.buyorders set status = 'cancelled' where (ordertime >? or sid=?) and status = 'open' and tid =? ",  //batch operation
      if (partitionKeys.contains("status")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: 
      //"update trade.buyorders set bid = ? where cid = ? and sid= ? and status = 'open' and qty >? and tid =? ",
      if (partitionKeys.contains("bid")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: 
      //"update trade.buyorders set bid = ? , qty=? where cid = ? and sid= ? and bid <? and status = 'open' and tid =? ",
      if (partitionKeys.contains("bid") || partitionKeys.contains("qty")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: 
      //"update trade.buyorders set sid = ? where cid = ? and sid= ? and status = 'open' and tid =? ",
      if (partitionKeys.contains("sid")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 5: 
      // "update trade.buyorders set status = 'filled'  where sid = ? and bid>? and status = 'open' ",  //for trigger test it could be a batch update
      if (partitionKeys.contains("status")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 6: 
      // "update trade.buyorders set status = 'cancelled' where (ordertime <? or sid=?) and status = 'open'  ",  //batch operation
      if (partitionKeys.contains("status")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 7: 
      //"update trade.buyorders set bid = ? where cid = ? and sid= ? and status = 'open' and qty >? ",
      if (partitionKeys.contains("bid")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 8: 
      //"update trade.buyorders set bid = ? , qty=? where cid = ? and sid= ? and bid <? and status = 'open'  " ,  
      if (partitionKeys.contains("bid") || partitionKeys.contains("qty")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 9: 
      //"update trade.buyorders set sid = ? where cid = ? and sid= ? and status = 'open'  "
      if (partitionKeys.contains("sid")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
   
    return stmt;
  }
  
//check expected exceptions
  protected void updateGFETable (Connection conn, int[] cid, int[] sid, int[] newSid, int[] qty, 
      Timestamp[] orderTime, BigDecimal[] bid, BigDecimal[] bid2, int[] whichUpdate, int size,
      List<SQLException> exList) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], null);
      else 
        if ( SQLTest.hasJSON ) 
          stmt = getStmt(conn, updateJSON[whichUpdate[i]]);
        else 
          stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed
      
      if (SQLTest.testSecurity && stmt == null) {
        if (SQLSecurityTest.prepareStmtException.get() != null) {
          SQLHelper.handleGFGFXDException((SQLException)
                        SQLSecurityTest.prepareStmtException.get(), exList);
          SQLSecurityTest.prepareStmtException.set(null);
          return;
        }
      } //work around #43244
      if (setCriticalHeap && stmt == null) {
        return; //prepare stmt may fail due to XCL54 now
      }
      
      try {
        if (stmt!=null) {
          count = updateTable(stmt, cid[i], sid[i], newSid[i], qty[i], orderTime[i], bid[i], bid2[i], tid, whichUpdate[i] );
          if (count != (verifyRowCount.get(tid+"_update"+i)).intValue()){
            Log.getLogWriter().info("Gfxd update has different row count from that of derby " +
                    "derby updated " + (verifyRowCount.get(tid+"_update"+i)).intValue() +
                    " but gfxd updated " + count);
          }
        }
      } catch (SQLException se) {
         SQLHelper.handleGFGFXDException(se, exList);
      }          
    }  
  }
  
  //w/o verification
  protected void updateGFETable (Connection conn, int[] cid, int[] sid, int[] newSid, int[] qty, 
      Timestamp[] orderTime, BigDecimal[] bid, BigDecimal[] bid2, int[] whichUpdate, int size) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    for (int i=0 ; i<size ; i++) {
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], null);
      else 
        stmt = getStmt(conn, updateJSON[whichUpdate[i]]);
      
      try {
        if (stmt!=null)
          updateTable(stmt, cid[i], sid[i], newSid[i], qty[i], orderTime[i], bid[i], bid2[i], tid, whichUpdate[i] );
      } catch (SQLException se) {
        if (se.getSQLState().equals("23503") && (whichUpdate[i] == 4 || whichUpdate[i] == 9 )) 
          //check fk violation happens when updating on foreign keys or not
          Log.getLogWriter().info("detected foreign key constraint violation during update, continuing test");
        else if (se.getSQLState().equals("42502") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
             " continuing tests");
        } else if (alterTableDropColumn && (se.getSQLState().equals("42X14") || se.getSQLState().equals("42X04"))) {
          //42X04 is possible when column in where clause is dropped
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else
          SQLHelper.handleSQLException(se);
      }
    }    
  }
  protected int updateTable (PreparedStatement stmt, int cid, int sid, int newSid, int qty, 
      Timestamp orderTime, BigDecimal bid, BigDecimal bid2, int tid, int whichUpdate) throws SQLException {
    int rowCount =0;
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";    
    String query = " QUERY: " + update[whichUpdate];
        
    if (!SQLHelper.isDerbyConn(stmt.getConnection()) ) {    
      query = " QUERY: " + updateJSON[whichUpdate];    
    }
        
    switch (whichUpdate) {
    case 0: 
      //"update trade.buyorders set status = 'filled'  where sid = ? and bid>? and status = 'open' and tid = ? ",  //for trigger test it could be a batch update          
      Log.getLogWriter().info(database + "updating trade.buyorders with STATUS:filled where SID:" + sid  + ",BID:" + bid + ",STATUS:open,TID:" + tid + query );
      stmt.setInt(1, sid);
      stmt.setBigDecimal(2, bid);      
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) && bid !=null ) {
           updateBuyordersJson(stmt.getConnection() , " where sid = " + sid + " and bid> " + bid + " and status = 'filled' and tid = " + tid );
        }
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders with STATUS:filled where SID:" + sid  + ",BID:" + bid + ",STATUS:open,TID:" + tid + query );
      break;
    case 1:     
      //"update trade.buyorders set status = 'cancelled' where ordertime >? or sid=? and status = 'open' and tid =? ",  //batch operation
      Log.getLogWriter().info(database + "updating trade.buyorders with STATUS:cancelled where ORDERTIME:" + orderTime + ",SID:" + sid + ",TID:" + tid + query);
      stmt.setTimestamp(1, orderTime);
      stmt.setInt(2, sid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();    
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection())) {
        updateBuyordersJson(stmt.getConnection() , " where ( ordertime > '" + orderTime + "' or sid= " + sid + " ) and status = 'cancelled' and tid =" + tid);
       }
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders STATUS:cancelled where ORDERTIME:" + orderTime + ",SID:" + sid + ",TID:" + tid + query);
      break;
    case 2: 
      //"update trade.buyorders set bid = ? where cid = ? and sid= ? and status = 'open' and qty >? and tid =? ",
      Log.getLogWriter().info(database + "updating trade.buyorders with BID:" + bid + " where CID:" + cid 
          + ",SID:" + sid + ",STATUS:open,QTY:" + qty + ",TID:" + tid + query);
      stmt.setBigDecimal(1, bid);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
      stmt.setInt(4, qty);
      stmt.setInt(5, tid);
      rowCount = stmt.executeUpdate();
      
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) && bid !=null ) {
        updateBuyordersJson(stmt.getConnection() ,  " where cid = " + cid +" and sid= " + sid + " and status = 'open' and qty > " + qty + " and tid = " + tid);
       }
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders with BID:" + bid + " where CID:" + cid 
          + ",SID:" + sid + ",STATUS:open,QTY:" + qty + ",TID:" + tid + query);
      
      break;
    case 3: 
      //"update trade.buyorders set bid = ? , qty=? where cid = ? and sid= ? and bid <? and status = 'open' and tid =? ",
      Log.getLogWriter().info(database + "updating trade.buyorders with BID:" + bid + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",BID:" + bid2 + ",STATUS:open,TID:" + tid + query);
      stmt.setBigDecimal(1, bid);
      stmt.setInt(2, qty);
      stmt.setInt(3, cid);
      stmt.setInt(4, sid);
      stmt.setBigDecimal(5, bid2);
      stmt.setInt(6, tid);
      rowCount = stmt.executeUpdate();      
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders with BID:" + bid + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",BID:" + bid2 + ",STATUS:open,TID:" + tid + query);
      
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) && bid !=null) {
        updateBuyordersJson(stmt.getConnection() ,  " where cid = " + cid +" and sid= " + sid + "  and bid = " + bid +"  and status = 'open' and tid = " + tid);
       }
      
      break;
    case 4:
      //"update trade.buyorders set sid = ? where cid = ? and sid= ? and status = 'open' and tid =? ",
      Log.getLogWriter().info(database + "updating trade.buyorders with SID:" + newSid  
          + " where CID:" + cid  + ",SID:" + sid + ",STATUS:open,TID: " + tid + query);
      stmt.setInt(1, newSid);
      stmt.setInt(2,cid);
      stmt.setInt(3,sid);
      stmt.setInt(4,tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders with SID:" + newSid  
          + " where CID:" + cid  + ",SID:" + sid + ",STATUS:open,TID: " + tid + query);
      
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) ) {
        updateBuyordersJson(stmt.getConnection() ,  " where cid = " + cid +" and sid= " + newSid +"  and status = 'open' and tid = " + tid);
       }
      
      break;
    case 5:
      //"update trade.buyorders set status = 'filled'  where sid = ? and bid>? and status = 'open' ",  //for trigger test it could be a batch update
      Log.getLogWriter().info(database + "updating trade.buyorders with STATUS:filled where SID:" + sid  + ",BID:" + bid + ",STATUS:open" + query);
      stmt.setInt(1, sid);
      stmt.setBigDecimal(2,bid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders with STATUS:filled where SID:" + sid  + ",BID:" + bid + ",STATUS:open" + query);
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) ) {
        updateBuyordersJson(stmt.getConnection() ,  " where cid = " + cid +" and sid= " + sid + "  and bid > " + bid +"  and status = 'filled' ");
       }
      break;
    case 6: 
      //"update trade.buyorders set status = 'cancelled' where ordertime <? or sid=? and status = 'open'  ",  //batch operation
      Log.getLogWriter().info(database + "updating trade.buyorders with STATUS:cancelled where ORDERTIME:" + orderTime+ ",SID:" + sid + ",STATUS:open" + query);
      stmt.setTimestamp(1, orderTime);
      stmt.setInt(2, sid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders  with STATUS:cancelled where ORDERTIME:" + orderTime+ ",SID:" + sid + ",STATUS:open" + query);
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) ) {
        updateBuyordersJson(stmt.getConnection() ,  " where ordertime< '" + orderTime +"' and sid= " + sid + "  and status = 'cancelled'" );
       }
      break;
    case 7: 
      //"update trade.buyorders set bid = ? where cid = ? and sid= ? and status = 'open' and qty >? ",
      Log.getLogWriter().info(database + "updating on trade.buyorders with BID:" + bid + " where CID:" + cid 
          + ",SID:" + sid + ",STATUS:open,QTY:" + qty + query);
      stmt.setBigDecimal(1, bid);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
      stmt.setInt(4, qty);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders with BID:" + bid + " where CID:" + cid 
          + ",SID:" + sid + ",STATUS:open,QTY:" + qty + query);
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) ) {
        updateBuyordersJson(stmt.getConnection() ,  " where cid = " + cid +" and sid= " + sid + "  and status = 'open' and qty> " + qty);
       }
      break;
    case 8: 
      //"update trade.buyorders set bid = ? , qty=? where cid = ? and sid= ? and bid <? and status = 'open'  ",  
      Log.getLogWriter().info(database + "updating trade.buyorders with BID:" + bid + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",BID:" + bid2 + ",STATUS:open" + query);
      stmt.setBigDecimal(1, bid);
      stmt.setInt(2, qty);
      stmt.setInt(3, cid);
      stmt.setInt(4, sid);
      stmt.setBigDecimal(5, bid2);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders with BID:" + bid + ",QTY:" + qty + 
          " where CID:" + cid  + ",SID:" + sid + ",BID:" + bid2 + ",STATUS:open" + query);
      
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) ) {
        updateBuyordersJson(stmt.getConnection() ,  " where cid = " + cid +" and sid= " + sid + "  and bid = " + bid +"  and status = 'open' ");
       }
      break;
    case 9:
      //"update trade.buyorders set sid = ? where cid = ? and sid= ? and status = 'open'",
      Log.getLogWriter().info(database + "updating trade.buyorders with SID:" + newSid + " where CID:" + cid  + ",SID:" + sid + ",STATUS:open"  + query);
      stmt.setInt(1, newSid);
      stmt.setInt(2,cid);
      stmt.setInt(3,sid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.buyorders  with SID:" + newSid + " where CID:" + cid  + ",SID:" + sid + ",STATUS:open"  + query);
      if ( SQLTest.hasJSON &  !SQLHelper.isDerbyConn(stmt.getConnection()) ) {
        updateBuyordersJson(stmt.getConnection() ,  " where cid = " + cid +" and sid= " + newSid + "  and status = 'open' ");
       }
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
  
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, int oid, String status, 
      BigDecimal bid, int qty, int cid, int sid, Timestamp orderTime, int tid, 
      boolean[] success) throws SQLException {
    
    Boolean dumpResult = (Boolean) dumpNoAggregateRs.get();
    final Boolean dumpQueryPlan = (Boolean) dumpQueryPlanRs.get();
    String sql = dumpResult != null && dumpResult.booleanValue() ? ( ! SQLHelper.isDerbyConn(conn)  ? selectwoaggreateJSON[whichQuery] : selectwoaggreate[whichQuery])
        : (dumpQueryPlan != null && dumpQueryPlan.booleanValue() ? "explain "
            +  ( ! SQLHelper.isDerbyConn(conn)  ? selectJSON[whichQuery] : select[whichQuery]): (! SQLHelper.isDerbyConn(conn)  ? selectJSON[whichQuery] : select[whichQuery]));
    Log.getLogWriter().info("query selected is " +  sql);
    return getQuery(conn, whichQuery, oid, status, bid, qty, cid, sid, orderTime, tid, success, sql);
  }
  
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, int oid , String status, 
      BigDecimal bid, int qty, int cid, int sid, Timestamp orderTime, int tid, 
      boolean[] success, String sql) throws SQLException {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
     
    String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";   

     String query = " QUERY: " + select[whichQuery];
    
    
    if ( ! SQLHelper.isDerbyConn(conn)  )
      query = " QUERY: " + selectJSON[whichQuery]; 
    
    /* if ( oid > cid ) {
       whichQuery = 14;
     } */
     
   
    
    int i=1;
    try {
      stmt = conn.prepareStatement(sql);
      /*
      stmt = getStmt(conn, select[whichQuery]);  
      if (setCriticalHeap && stmt == null) {
        return null; //prepare stmt may fail due to XCL54 now
      }
      */
      switch (whichQuery){
      case 0:
        //"select * from trade.buyorders where status = 'open' and tid = ?",
        Log.getLogWriter().info(database + "querying trade.buyorders with TID:" +tid  + query);
        stmt.setInt(i++, tid);
        break;
      case 1: 
        //"select cid, bid, sid from trade.buyorders where cid >? and sid <? and qty >? and orderTime<? and tid = ?",        
        Log.getLogWriter().info(database + "querying trade.buyorders with  CID:" + cid + ",SID:" + sid 
            +",QTY:" + qty + ",ORDERTIME:" + orderTime + ",TID:" + tid +  query);   
        stmt.setInt(i++, cid);
        stmt.setInt(i++, sid);
        stmt.setInt(i++, qty);
        stmt.setTimestamp(i++, orderTime);
        stmt.setInt(i++, tid); 
        break;
      case 2:
        //"select sid, count(*) from trade.buyorders  where status =? and tid =? GROUP BY sid HAVING count(*) >2",  
        Log.getLogWriter().info(database + "querying trade.buyorders with STATUS:" + status  + ",TID:" +tid + query);   
        stmt.setString(i++, status);
        stmt.setInt(i++, tid);
        break;
      case 3:
        //"select cid, count(sid) from trade.buyorders  where status =? and tid =? GROUP BY cid",  
        Log.getLogWriter().info(database + "querying trade.buyorders with STATUS:" + status  + ",TID:" +tid + query);   
        stmt.setString(i++, status);
        stmt.setInt(i++, tid);
        break;
      case 4:
        //"select cid, sum(qty*bid) as amount from trade.buyorders  where status =? and tid =? GROUP BY cid ORDER BY amount",
        Log.getLogWriter().info( database + "querying trade.buyorders with STATUS:" + status  + ",TID:" +tid + query);   
        stmt.setString(i++, status);
        stmt.setInt(i++, tid);
        break;
      case 5:
        //"select cid, max(qty*bid) as 'largest order' from trade.buyorders  where status =? and tid =? GROUP BY cid ORDER BY max(qty*bid) DESC HAVING max(qty*bid) > 20000",
        Log.getLogWriter().info(database + "querying trade.buyorders with STATUS:" + status  + ",TID:" +tid + query);   
        stmt.setString(i++, status);
        stmt.setInt(i++, tid);
        break;
      case 6:
        //self join
        Log.getLogWriter().info(database + "querying trade.buyorders with TID:" +tid + query);   
        stmt.setInt(i++, tid);
        break;
      case 7: 
        //"select * from trade.buyorders",
        Log.getLogWriter().info(database + "querying trade.buyorders with no data" + query);
        break;
      case 8: 
        //"select cid, bid, sid from trade.buyorders where cid >?  and sid <? and qty >? and orderTime<?  ",        
        Log.getLogWriter().info(database + "querying trade.buyorders with CID:" + cid + ",SID:" + sid 
            +",QTY:" + qty + ",ORDERTIME:" + orderTime + ",TID:" + tid + query);   
        stmt.setInt(i++, cid);
        stmt.setInt(i++, sid);
        stmt.setInt(i++, qty);
        stmt.setTimestamp(i++, orderTime);
        break;
      case 9:
        //"select sid, count(*) from trade.buyorders  where status =? GROUP BY sid HAVING count(*) >2", 
        Log.getLogWriter().info(database + "querying trade.buyorders with STATUS:" + status + query);   
        stmt.setString(i++, status);
        break;
      case 10:
        //"select cid, count(sid) from trade.buyorders  where status =? GROUP BY cid",  
        Log.getLogWriter().info( database + "querying trade.buyorders with STATUS:" + status + query);   
        stmt.setString(i++, status);
        break;
      case 11:
        //"select cid, sum(qty*bid) as amount from trade.buyorders  where status =? GROUP BY cid ORDER BY amount",        
        Log.getLogWriter().info( database + "querying trade.buyorders with STATUS:" + status + query);   
        stmt.setString(i++, status);
        break;
      case 12:
        //"select cid, max(qty*bid) as 'largest order' from trade.buyorders  where status =? GROUP BY cid ORDER BY max(qty*bid) DESC HAVING max(qty*bid) > 20000"
        Log.getLogWriter().info(database + "querying trade.buyorders with STATUS:" + status + query);   
        stmt.setString(i++, status);
        break;
      case 13:
        //self join
        Log.getLogWriter().info(database + "querying trade.buyorders with no data"  + query);   
        break;     
      case 14:
        if ( SQLHelper.isDerbyConn(conn) ){   
            Log.getLogWriter().info(database + "querying trade.buyorders with STATUS:open, OID:" + oid  + ",TID: " + tid  + "select cid,tid,oid,status from trade.buyorders where status = 'open' and oid = ? and tid = ? order by cid ");   
            stmt = conn.prepareStatement("select cid,tid,oid,status from trade.buyorders where status = 'open' and oid = ? and tid = ? order by cid ");            
            stmt.setInt(1, oid);
            stmt.setInt(2, tid);            
        } else {
          Log.getLogWriter().info(database + "querying trade.buyorders with STATUS:open, OID:" + oid  + ",TID: " + tid  +" select cid,tid,cast(json_evalPath(json_evalPath(buyorder_json, '$..buyorder[?(@.status = open)]'), '$..[?(@.oid = " + oid +")].oid' ) as integer) as oid , cast(json_evalPath(json_evalPath(buyorder_json, '$..buyorder[?(@.status = open)]'), '$..[?(@.oid = " + oid +")].status' ) as varchar(10)) from trade.customers where  tid = ? order by cid ");
          stmt = conn.prepareStatement("select cid,tid,cast(json_evalPath(json_evalPath(buyorder_json, '$..buyorder[?(@.status = open)]'), '$..[?(@.oid = " + oid +")].oid' ) as integer) as oid , cast(json_evalPath(json_evalPath(buyorder_json, '$..buyorder[?(@.status = open)]'), '$..[?(@.oid = " + oid +")].status' ) as varchar(10)) as status from trade.customers where  tid = ? order by cid ");          
          stmt.setInt(1, tid);     
        }
        
        break;   
       default:
        throw new TestException("incorrect select statement, should not happen" );
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else throw se;
    }
    return rs;
  } 
    
  protected boolean getDataFromResult(Connection conn, int[] cid, int[] sid, int tid) {
    int defaultCid =0; 
    int defaultSid =0; 
    int size = cid.length;
    boolean[] success = new boolean[1];
    int num = 20; //used to randomly return the cid, sid pair
    int n = rand.nextInt(num);
    ResultSet rs = null;

    try {  
      int whichQuery = 0;
      rs = getQuery(conn, whichQuery, 0, null, null, 0, 0, 0, null, tid, success); //first query select *
      if (!success[0] || rs == null) return false; //we won't retry if could not get data from resultSet
      
      int i=0;
      int temp = 0;
      while (rs.next() && i<size) {
        if (temp ==0 ) {
          cid[i] = rs.getInt("CID");
          sid[i] = rs.getInt("SID");
        } else if (n>=temp) {
          cid[i] = rs.getInt("CID");
          sid[i] = rs.getInt("SID");
          i++;
        }
        temp++;                         
      } //update cids with result set
      rs.close();
      while (i<size) {
        cid[i] = defaultCid;  //cid is 0
        sid[i] = defaultSid; //sid is 0
        i++;
      } //continues to update cids with cid=0
    } catch (SQLException se) {
      if (!SQLHelper.checkGFXDException(conn, se)) return false; //need retry or no op
      if (SQLHelper.isAlterTableException(conn, se)) return false; //need retry or no op
      else SQLHelper.handleSQLException(se); //throws TestException.
    }    
    return true;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void query(Connection dConn, Connection gConn) {
    Log.getLogWriter().info("in query 940");
    int numOfNonUniq = select.length/2; //how many query statement is for non unique keys, non uniq query must be at the end
    int whichQuery = getWhichOne(numOfNonUniq, select.length); //randomly select one query sql based on test uniq or not

    //self join should only work when it is partitioned on join key (colocated join) or is replicated
    if (whichQuery == 6 || whichQuery == 13) {
      if (!SQLTest.testPartitionBy) whichQuery--; 
      else {
        ArrayList<String> partitionKey = (ArrayList<String>) SQLBB.getBB().getSharedMap().get("buyordersPartition");
        if (partitionKey.size() == 1 && isTicket49116Fixed  &&
            (partitionKey.contains("cid") || partitionKey.contains("sid"))) {
          //allow self join when partitioned on cid or sid field for the join
        } else if (partitionKey.size() == 0) {
          //allow self join for replicated table
        } else whichQuery--; 
      }
    }
    
    String status = statuses[rand.nextInt(statuses.length)];
    BigDecimal bid = getPrice();
    int[] oidArray = new int[10];     
    getOids(oidArray , 1);
    int oid = oidArray[0];
    int qty = getQty();
    int cid = getCid();  //anuy cid could be used in query string as we query records insert by this tid only
    int sid = getSid();  //anuy cid could be used in query string
    int tid = getMyTid();
    Timestamp orderTime = getRandTime();
    ResultSet discRS = null;
    ResultSet gfeRS = null;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    
    if (dConn!=null) {
      try {
        Log.getLogWriter().info("calling json queries");
        discRS = query(dConn, whichQuery, oid, status, bid, qty, cid, sid, orderTime, tid);
        if (discRS == null) {
          Log.getLogWriter().info("could not get the derby result set after retry, abort this query");
          Log.getLogWriter().info("Could not finish the op in derby, will abort this operation in derby");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) 
            ; //do nothing and expect gfxd fail with the same reason due to alter table
          else return;
        }
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exceptionList);
      }
      try {
        gfeRS = query (gConn, whichQuery, oid, status, bid, qty, cid, sid, orderTime, tid); 
        if (gfeRS == null) {
          if (isHATest) {
            Log.getLogWriter().info("Testing HA and did not get GFXD result set after retry");
            return;
          } else if (setCriticalHeap) {
            Log.getLogWriter().info("got XCL54 and does not get query result");
            return; //prepare stmt may fail due to XCL54 now
          } 
          else     
            throw new TestException("Not able to get gfe result set after retry");
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptionList);
      }
      SQLHelper.handleMissedSQLException(exceptionList);
      if (discRS == null || gfeRS == null) return;
      
      boolean success = false;

      try {
        if (whichQuery == 5) {
          success =  ResultSetHelper.compareSortedResultSets(discRS, gfeRS);
          if (!reproduce47943) return;

          boolean logOrderByResult = true;
          if (logOrderByResult) {
            try {
              PreparedStatement sps = gConn.prepareStatement(select[5]);
              sps.setString(1, status);
              sps.setInt(2, tid);
              gfeRS = sps.executeQuery();             
            } catch (SQLException se) {
              if (!SQLHelper.checkGFXDException(gConn, se)) 
                Log.getLogWriter().info("Not able to dump result due to node failure");
            }
            List <Struct> gfxdList = ResultSetHelper.asList(gfeRS, false);
            if (gfxdList != null) Log.getLogWriter().info( "gemfirexd returns " + ResultSetHelper.listToString(gfxdList) + " QUERY : " + select[5] );
            else Log.getLogWriter().info("Not able to dump result due to node failure");
          }
        
          String offsetClause = " OFFSET 3 ROWS FETCH NEXT 2 ROWS ONLY";
        
          String sql = select[5] + offsetClause;
          
          try {
            Log.getLogWriter().info("Derby - querying trade.buyorders QUERY: " + sql );
            PreparedStatement dps = dConn.prepareStatement(sql);
            dps.setString(1, status);
            dps.setInt(2, tid);
            discRS = dps.executeQuery();
          } catch (SQLException se) {
            if (!SQLHelper.checkDerbyException(dConn, se)) {
              Log.getLogWriter().info("Not able to get results due to node failure");
              return;
            } else {
              SQLHelper.handleSQLException(se);
            }         
          } 
          
          try {
            Log.getLogWriter().info("gemfirexd - querying trade.buyorders QUERY: " + sql );
            PreparedStatement sps = gConn.prepareStatement(sql);
            sps.setString(1, status);
            sps.setInt(2, tid);
            gfeRS = sps.executeQuery();
          } catch (SQLException se) {
            if (!SQLHelper.checkGFXDException(gConn, se)) {
              Log.getLogWriter().info("Not able to get gfxd results due to node failure");
              return;
            } else SQLHelper.handleSQLException(se); //do not expect other exceptions.
          }
          
          List <Struct> derbyList = ResultSetHelper.asList(discRS, true);
          List <Struct> gfxdList = ResultSetHelper.asList(gfeRS, false);
          if (derbyList != null && gfxdList != null)
            ResultSetHelper.compareResultSets(derbyList, gfxdList);
        } else success = ResultSetHelper.compareResultSets(discRS, gfeRS); 
        if (!success) {
          Log.getLogWriter().info("Not able to compare results due to derby server error");
        } //not able to compare results due to derby server error   
      } catch (TestException te) {
        if ((te.getMessage().contains("elements were missing") 
            || te.getMessage().contains("unexpected elements")) 
            && whichQuery>=2 && whichQuery<=5) {
          
          dumpNoAggregateRs.set(true);
          //This is best effort to dump aggregate results
          //either db dumped results should help to analyze the data mismatch issue
          
          //dump derby result
          try {
            discRS = query(dConn, whichQuery, status, bid, qty, cid, sid, orderTime, tid);
            List derbyList = ResultSetHelper.asList(discRS, true);
            if (derbyList != null)
              Log.getLogWriter().info("dump non-aggregate result from derby due to data mismatch:" +
                ResultSetHelper.listToString(derbyList));
            else Log.getLogWriter().info("not able to get derby results for dump");
          } catch (SQLException se) {
            Log.getLogWriter().info("not able to get derby results for dump");
          }
          
          //dump gfxd result
          try {
            gfeRS = query(gConn, whichQuery, status, bid, qty, cid, sid, orderTime, tid);
            List gfxdList = ResultSetHelper.asList(gfeRS, false);
            if (gfxdList != null)
              Log.getLogWriter().info("dump non-aggregate result from gfxd due to data mismatch:" +
                ResultSetHelper.listToString(gfxdList));
            else Log.getLogWriter().info("not able to get gfxd results for dump");
          } catch (SQLException se) {
            Log.getLogWriter().info("not able to get gfxd results for dump");
          }

          //dump query plan
          dumpNoAggregateRs.set(false);
          dumpQueryPlanRs.set(true);
          try {
            gfeRS = query(gConn, whichQuery, status, bid, qty, cid, sid,
                orderTime, tid);
            StringBuilder plan = new StringBuilder();
            while (gfeRS.next()) {
              plan.append(gfeRS.getString(1)).append('\n');
            }
            if (plan.length() > 0) {
              Log.getLogWriter().info(
                  "dump query plan in gfxd due to data mismatch:"
                      + plan);
            }
            else {
              Log.getLogWriter().info(
                  "no query plan in gfxd found");
            }
          } catch (SQLException se) {
            Log.getLogWriter().info(
                "not able to get gfxd queryplan due to exception "
                    + se.getSQLState(), se);
          }
          
          dumpQueryPlanRs.set(false);          
        } 
        
        throw te; 
      }

    }// we can verify resultSet
    else {
      try {
        gfeRS = query (gConn,  whichQuery, status, bid, qty, cid, sid, orderTime, tid);  
      } catch (SQLException se) {
        if (se.getSQLState().equals("42502") && SQLTest.testSecurity) {
          Log.getLogWriter().info("Got expected no SELECT permission, continuing test");
          return;
        } else if (alterTableDropColumn && se.getSQLState().equals("42X04")) {
          Log.getLogWriter().info("Got expected column not found exception, continuing test");
          return;
        } else SQLHelper.handleSQLException(se);
      }
      if (gfeRS != null)
        ResultSetHelper.asList(gfeRS, false);  
      else if (isHATest)
        Log.getLogWriter().info("could not get gfxd query results after retry due to HA");
      else if (setCriticalHeap)
        Log.getLogWriter().info("could not get gfxd query results after retry due to XCL54");
      else
        throw new TestException ("gfxd query returns null and not a HA test");  
    }
  }
  

  
//will retry 
  public static ResultSet getQuery(Connection conn, int whichQuery, int oid, String status, 
      BigDecimal bid, int qty, int cid, int sid, Timestamp orderTime, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    try {      
      rs = getQuery(conn, whichQuery, oid,  status, bid, qty, cid, sid, orderTime, tid, success);
      int count = 0;
      while (!success[0]) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
          return null; 
        }
        count++;   
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));    
        rs = getQuery(conn, whichQuery, oid, status, bid, qty, cid, sid, orderTime, tid, success);
      } //retry 
    } catch (SQLException se) {
      if (!SQLHelper.isAlterTableException(conn, se)) SQLHelper.handleSQLException(se);
      //allow alter table related exceptions.
    }
    return rs;
  } // whichQuery, status, bid, qty, cid, sid, orderTime, tid
  
  protected static ResultSet query(Connection conn, int whichQuery, int oid, String status, 
      BigDecimal bid, int qty, int cid, int sid, Timestamp orderTime, int tid) 
      throws SQLException {
   
    boolean[] success = new boolean[1];
    ResultSet rs = getQuery(conn, whichQuery,  oid, status, bid, qty, cid, sid, orderTime, tid, success);
    int count = 0;
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
        return null; 
      }
      count++;   
      MasterController.sleepForMs(rand.nextInt(retrySleepMs));    
      rs = getQuery(conn, whichQuery, oid, status, bid, qty, cid, sid, orderTime, tid, success);
    } //retry 
    return rs;
  }
  
  protected int deleteFromTable(PreparedStatement stmt, int cid, int sid, 
      int oid, BigDecimal bid, int qty, int tid, int whichDelete) throws SQLException {
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";    
    String query = " QUERY: " + delete[whichDelete];
    if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection())  ) {
      query = " QUERY: " + deleteJSON[whichDelete];
    }
    
    int rowCount = 0;
    
    switch (whichDelete) {
    case 0:   
      //"delete from trade.buyorders where cid=? and sid=? and tid=?", //could be batch delete, but no foreign key reference, this should work
      Log.getLogWriter().info(database + "deleting trade.buyorders with CID:" + cid + ",SID:" + sid 
          + ",TID:" + tid + query);  
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) )
      populateOids(stmt.getConnection() , "select oid from trade.buyorders  where cid=" + cid + " and sid=" + sid + " and tid=" + tid);
      stmt.setInt(1, cid);
      stmt.setInt(2, sid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.buyorders CID:" + cid + ",SID:" + sid 
          + ",TID:" + tid + query);  
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) && !setCriticalHeap) {
       //need to form these queries
        deleteCustomerJson(stmt.getConnection() );
      }
      break;
    case 1:
      //"delete from trade.buyorders where oid=? and (bid <? or qty>?) and tid=?",
      Log.getLogWriter().info(database +  "deleting trade.buyorders with OID:" + oid + ",BID:" + bid 
          + ",QTY:" + qty + ",TID:" + tid + query );  
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) )
        populateOids(stmt.getConnection() , "select oid from trade.buyorders  " + "where oid= " + oid + " and (bid < " + bid + " or qty> " + qty + ") and tid=" + tid);
      stmt.setInt(1, oid);
      stmt.setBigDecimal(2, bid);
      stmt.setInt(3, qty);
      stmt.setInt(4, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.buyorders OID:" + oid + ",BID:" + bid 
          + ",QTY:" + qty + ",TID:" + tid + query );
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) && !setCriticalHeap) {
        //need to form these queries
         deleteCustomerJson(stmt.getConnection() );
       }
      break;
    case 2:   
      //    "delete from trade.buyorders where (status ='cancelled' or status = 'filled' or qty<?) and bid>? and tid=?",
      Log.getLogWriter().info(database +  "deleting trade.buyorders with QTY:" + qty + ",BID:" + bid 
          + ",TID:" + tid + query);  
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) )
        populateOids(stmt.getConnection() , "select oid from trade.buyorders  " + " where (status ='cancelled' or status = 'filled' or qty< " + qty + ") and bid>" + bid+ " and tid= " + tid);
      stmt.setInt(1, qty);
      stmt.setBigDecimal(2, bid);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.buyorders QTY:" + qty + ",BID:" + bid 
          + ",TID:" + tid + query);  
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) && !setCriticalHeap) {
        //need to form these queries
         deleteCustomerJson(stmt.getConnection() );
       }
      break;
    case 3:   
      //"delete from trade.buyorders where cid=? and sid=? "
      Log.getLogWriter().info(database + "deleting trade.buyorders with CID:" + cid + ",SID:" + sid + query);
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) )
        populateOids(stmt.getConnection() , "select oid from trade.buyorders  " + "where cid=" + cid + " and sid=" + sid );
      stmt.setInt(1, cid);
      stmt.setInt(2, sid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.buyorders CID:" + cid + ",SID:" + sid + query);
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) && !setCriticalHeap) {
        //need to form these queries
         deleteCustomerJson(stmt.getConnection() );
       }
      break;
      
    case 4:
      //"delete from trade.buyorders where oid=? and (bid <? or qty>?)  ",
      Log.getLogWriter().info(database + "deleting trade.buyorders with OID:" + oid + ",BID:" + bid 
          + ",QTY:" + qty + query ); 
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) )
        populateOids(stmt.getConnection() , "select oid from trade.buyorders  " + "where oid= " + oid + " and (bid < " + bid + " or qty> " + qty + ")");
      stmt.setInt(1, oid);
      stmt.setBigDecimal(2, bid);
      stmt.setInt(3, qty);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.buyorders OID:" + oid + ",BID:" + bid 
          + ",QTY:" + qty + query ); 
      
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) && !setCriticalHeap) {
        //need to form these queries        
         deleteCustomerJson(stmt.getConnection() );
       }
      break;
    case 5:   
      //    "delete from trade.buyorders where (status ='cancelled' or status = 'filled' or qty<?) and bid>?",
      Log.getLogWriter().info(database + "deleting trade.buyorders with QTY:" + qty + ",BID:" + bid + query);  
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) )
        populateOids(stmt.getConnection() , "select oid from trade.buyorders  " + " where (status ='cancelled' or status = 'filled' or qty< " + qty + ") and bid>" + bid);
      stmt.setInt(1, qty);
      stmt.setBigDecimal(2, bid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.buyorders QTY:" + qty + ",BID:" + bid + query); 
      if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(stmt.getConnection()) && !setCriticalHeap) {
         deleteCustomerJson(stmt.getConnection() );
       }
      break;
    default:
      throw new TestException(database + "incorrect delete statement, should not happen");
    }  
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
    


    @SuppressWarnings("unchecked")
    private JSONObject getJSONObject(int oid, int cid, int sid, int qty,
        String status, Timestamp time, BigDecimal bid, int tid){
       
      JSONObject json = new JSONObject();
      json.put("oid", oid);
      json.put("cid", cid);
      json.put("sid", sid );
      json.put("qty", qty);
      json.put("status", status);
      json.put("time", time.getTime());
      json.put("bid", bid);
      json.put("tid", tid);
      
      return json;
    }
    
    @SuppressWarnings("unchecked")
    private void updateBuyordersJson(Connection conn, String whereClause ) throws SQLException{
      JSONObject json ;
      String query = "select cid,sid,qty,status,ordertime,bid,tid , oid , json_details  from trade.buyorders " + whereClause;
      Log.getLogWriter().info("Executing ... " + query);      
      String oids ="";
      String jsonDetails="";
      String sep = System.getProperty("line.separator");
      
      try{
        
        List<List<Object>> list = executeQueryGetList(conn, query);
      
      for   ( List<Object>  obj  : list ) {
        json=new JSONObject();
        json.put("cid", obj.get(0));
        json.put("oid", obj.get(7));
        json.put("sid",obj.get(1));
        json.put("qty", obj.get(2));
        json.put("status", obj.get(3));
        json.put("time", obj.get(4));
        json.put("bid", obj.get(5));
        json.put("tid", obj.get(6));
        JSONObject existingJson = null;
        
        if ( (String)obj.get(8) != null ) {
             existingJson = (JSONObject) new JSONParser().parse((String)obj.get(8));        
           }
        
        if (existingJson== null || existingJson.get("sid") !=  json.get("sid") ||  existingJson.get("qty") !=  json.get("qty")  || ! existingJson.get("status").equals(json.get("status"))) 
           {
        oids= oids + obj.get(7)+ " ,";
        jsonDetails =jsonDetails + json.toJSONString() + sep + " ";          
           conn.createStatement().executeUpdate( " update trade.buyorders set json_details = '" + json.toJSONString() +  "' where oid = " + obj.get(7) ); 
           if (!SQLTest.hasTx && !setCriticalHeap)
            insertUpdateCustomerJson(conn, (Integer) obj.get(0), json );
        }
      }  
      
      if ( ! oids.equals("") ) {
      Log.getLogWriter().info("updated customers and buyorders with OIDList: " + oids.substring(0,oids.length() -2 )  + " respective updated JSON_DETAILS:" + jsonDetails.substring(0,jsonDetails.length() -2 ));
      }
      }catch (SQLException se){
        conn.rollback();
        throw se;
      }
      catch (Exception e){
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
    
     
    @SuppressWarnings("unchecked")
    protected void insertUpdateCustomerJson(Connection conn, int cid , JSONObject json ){
      //for each insert/update/delete in buyorder update trade.customers buyorder_json column
      //operation =0 => insert , operation = 1 => update  
      if ( !SQLTest.hasJSON  || SQLHelper.isDerbyConn(conn))
        return;
      
      String jsonBuyorder = null ;
      JSONParser parser = new JSONParser();
      JSONArray jsonBuyorderArray = new JSONArray();
      JSONObject jsonBuyorderObject = new JSONObject();
      List<String>  list = executeQuery(conn, "select buyorder_json from trade.customers where cid = " + cid);
     
      
      try{
        
     if (list.size() > 0 ) {
         jsonBuyorder=  list.get(0);
     }
        
     // the customer alreay contains more than one object and we need to insert/delete current one
     if (jsonBuyorder != null  ) {
       jsonBuyorderObject = (JSONObject) parser.parse(jsonBuyorder);         
       jsonBuyorderArray = (JSONArray) jsonBuyorderObject.get("buyorder");
     }
     
     
    //it is a delete operation. we should have data in the customer table and customer should update the json. Get this json row from customer and delete that object 
    if (jsonBuyorderArray != null) {
            deleteElementFromJsonArray(jsonBuyorderArray, (Integer)json.get("oid"));    
        }
    
      jsonBuyorderArray.add(json);
      if (!SQLTest.hasTx)
      updateCustomerWithNewArray(conn, cid, jsonBuyorderObject, jsonBuyorderArray);
     
      }
      catch (ParseException pe ) {
        throw new TestException ( "Adding " + jsonBuyorderObject.toJSONString() + " to trade.customers" + "current Object is " + json.toJSONString()  + "Exception: " + TestHelper.getStackTrace(pe));
      }
          
  }
    
    
    protected void deleteCustomerJson(Connection conn ){
      //for each insert/update/delete in buyorder update trade.customers buyorder_json column
      //operation =0 => insert , operation = 1 => update  
    
      if ( !SQLTest.hasJSON  || SQLHelper.isDerbyConn(conn))
        return;

      String jsonBuyorder = null ;
      JSONParser parser = new JSONParser();
      JSONArray jsonBuyorderArray = new JSONArray();
      JSONObject jsonBuyorderObject = new JSONObject();
      try{
      for ( int oid : listOfOids ) {    
               String query = "select cid , buyorder_json from trade.customers where json_evalPath(json_details, '$..buyorder[?(@.oid==" + oid + ")]' ) is not null " +
                              " and json_evalPath(json_details, '$..buyorder[?(@.oid==" + oid + ")]' ) != ''" ;
               Log.getLogWriter().info("executing ..." + query);
               List<String> list = executeQuery(conn, query);
     
     for  (String jsonString  : list) {
       jsonBuyorder = jsonString;
       jsonBuyorderObject = (JSONObject) parser.parse(jsonBuyorder);         
       jsonBuyorderArray = (JSONArray) jsonBuyorderObject.get("buyorder");
       deleteElementFromJsonArray(jsonBuyorderArray, oid);
       
     }
      }      
      } catch (ParseException pe ) {
        throw new TestException ("Exception: " + TestHelper.getStackTrace(pe));
      } 
          
  }
    
    private void deleteElementFromJsonArray(JSONArray jsonBuyorderArray , int oid){
      
      for ( int currElement =0 ; currElement < jsonBuyorderArray.size() ; currElement++  ) {
        JSONObject jsonBuyorderCurrentObject = (JSONObject)jsonBuyorderArray.get(currElement);        
        int currentOid =  ((Long)jsonBuyorderCurrentObject.get("oid")).intValue();
        if ( currentOid == oid)  {
            jsonBuyorderArray.remove(currElement);
            break;
        }
      }
    }
    
    
    private List<String> executeQuery(Connection conn , String query){
      List<String> list = null;
      try{
         ResultSet rs =  conn.createStatement().executeQuery(query );
         list=  ResultSetHelper.getJsonAsList(conn, rs);
      } catch (SQLException se ) {
          if (se.getSQLState().equals("X0Z01") && isHATest ) {
              Log.getLogWriter().info("Retrying the operation " ) ;
              list = executeQuery( conn ,  query);
          } else {
            throw new TestException (TestHelper.getStackTrace(se));
          }
      }
      
      return list;
    }
    
    
    private List<List<Object>> executeQueryGetList(Connection conn , String query) throws SQLException{
      List<List<Object>> finalList = new ArrayList<List<Object>>();
      List<Object> list = null;
      try{
         ResultSet rs =  conn.createStatement().executeQuery(query );
         while (rs.next()) {
           //cid,sid,qty,status,ordertime,bid,tid , oid , json_details           
           list = new ArrayList<Object>();
           list.add(rs.getInt(1));
           list.add(rs.getInt(2));
           list.add(rs.getInt(3));
           list.add(rs.getString(4));
           list.add(rs.getTimestamp(5).getTime());
           list.add(rs.getBigDecimal(6));
           list.add(rs.getInt(7));
           list.add(rs.getInt(8));
           list.add(rs.getString(9));   
           finalList.add(list);
         }
      } catch (SQLException se ) {
          if (se.getSQLState().equals("X0Z01") && isHATest ) {
              Log.getLogWriter().info("Retrying the operation " ) ;
              finalList = executeQueryGetList( conn ,  query);
          } else {
            throw se;
          }
      }
      
      return finalList;
    }
    @SuppressWarnings("unchecked")
    private  void updateCustomerWithNewArray(Connection conn , int cid , JSONObject jsonBuyorderObject , JSONArray jsonBuyorderArray  ) {
      
      jsonBuyorderObject.put("buyorder" , jsonBuyorderArray);
      Log.getLogWriter().info("updating trade.customers for CID:" + cid + "  with buyorder_json:" + jsonBuyorderObject.toJSONString() );
      String stmt = "update trade.customers set buyorder_json = ? where cid = " + cid;
      
      try{
      PreparedStatement ps = conn.prepareStatement(stmt);      
      ps.setObject(1, jsonBuyorderObject.toJSONString());
      ps.execute();
      } catch (SQLException se ) {
        if (se.getSQLState().equals("X0Z01") && isHATest ) {
          Log.getLogWriter().info("Retrying the operation " ) ;          
          updateCustomerWithNewArray( conn ,  cid, jsonBuyorderObject , jsonBuyorderArray);
      } else {
        throw new TestException (TestHelper.getStackTrace(se));
      }
  }
    }
    
    
    private void populateOids(Connection conn , String query) throws SQLException{      
      try{
      listOfOids.clear();
      ResultSet rs = conn.createStatement().executeQuery(query);
      while (rs.next() ){
        listOfOids.add(rs.getInt(1));
      }
      }catch  (SQLException se ){
        if (se.getSQLState().equals("X0Z01") && isHATest ) {
          Log.getLogWriter().info("Retrying the operation " ) ;
          populateOids( conn ,  query);
      } else 
        throw se;
      }
    }
    
}

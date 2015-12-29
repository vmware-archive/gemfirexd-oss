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
/**
 * 
 */
package sql.joinStatements;

import hydra.Log;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLHelper;
import sql.SQLPrms;
import util.TestException;
import sql.sqlutil.ResultSetHelper;
import sql.dmlStatements.*;

/**
 * @author eshu
 *
 */
public class SecuritiesPortfolioJoinStmt extends AbstractJoinStmt {
  /*
   * trade.securities table fields
   *   private int sec_id;
   *   String symbol;
   *   String exchange;
   *   BigDecimal price;
   *   int tid; //for update or delete unique records to the thread
   *
   * trade.Portfolio table fields
   *   int cid;
   *   int sid;
   *   int qty;
   *   int availQty;
   *   BigDecimal subTotal;
   *   int tid; //for update or delete unique records to the thread
   */
  

  protected static String[] uniqSelect = 
  {"select * from trade.securities s, trade.portfolio f where sec_id = f.sid and f.tid = ?",
   "select cid, sid, symbol, price, qty from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and f.tid = ? ",
   "select cid, sid, symbol, exchange, qty, price from trade.securities s, trade.portfolio f where sec_id = f.sid and cid<? and(qty=availQty or price > ?) and f.tid = ?",
   "select cid, sid, symbol, exchange, price, subtotal from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and (subtotal >10000 or (price >= ? and price <= ?)) and f.tid = ?",
   /*comment out due to #41567*/
   // "select cid, sid, symbol, exchange, price, subtotal from trade.securities s LEFT OUTER JOIN trade.portfolio f ON sec_id = f.sid where f.tid=? "
    };

  protected static String[] nonUniqSelect = {
    "select * from trade.securities s, trade.portfolio f where sec_id = f.sid ",
    "select cid, sid, symbol, price, qty from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? ",
    "select cid, sid, symbol, exchange, qty, price from trade.securities s, trade.portfolio f where sec_id = f.sid and cid<? and(qty=availQty or price > ?) ",
    "select cid, sid, symbol, exchange, price, subtotal from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and (subtotal >10000 or (price >= ? and price <= ?)) ",
    /*comment out due to #41567*/
 //   "select cid, sid, symbol, exchange, price, subtotal from trade.securities s LEFT OUTER JOIN trade.portfolio f ON sec_id = f.sid  "
    };
  
  protected static String[] uniqSelectHdfs = 
  {"select * from trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n where sec_id = f.sid and f.tid = ?",
   "select cid, sid, symbol, price, qty from trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n where sec_id = f.sid and cid >? and f.tid = ? ",
   "select cid, sid, symbol, exchange, qty, price from trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n where sec_id = f.sid and cid<? and(qty=availQty or price > ?) and f.tid = ?",
   "select cid, sid, symbol, exchange, price, subtotal from trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where sec_id = f.sid and cid >? and (subtotal >10000 or (price >= ? and price <= ?)) and f.tid = ?",
   /*comment out due to #41567*/
   // "select cid, sid, symbol, exchange, price, subtotal from trade.securities s LEFT OUTER JOIN trade.portfolio f ON sec_id = f.sid where f.tid=? "
    };

  protected static String[] nonUniqSelectHdfs = {
    "select * from trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n where sec_id = f.sid ",
    "select cid, sid, symbol, price, qty from trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where sec_id = f.sid and cid >? ",
    "select cid, sid, symbol, exchange, qty, price from trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n where sec_id = f.sid and cid<? and(qty=availQty or price > ?) ",
    "select cid, sid, symbol, exchange, price, subtotal from trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where sec_id = f.sid and cid >? and (subtotal >10000 or (price >= ? and price <= ?)) ",
    /*comment out due to #41567*/
 //   "select cid, sid, symbol, exchange, price, subtotal from trade.securities s LEFT OUTER JOIN trade.portfolio f ON sec_id = f.sid  "
    }; 
  
  protected static boolean isColocated = checkColocation();
  protected static boolean[] useDisk = {false};

  public SecuritiesPortfolioJoinStmt(){
    
  }
  
  //---- implementations of interface declared methods ----//
  
  public void query(Connection dConn, Connection gConn) {
    int whichQuery;
    ResultSet derbyRS;
    ResultSet gfeRS;
    int cid = (dConn !=null ) ? AbstractDMLStmt.getCid(dConn) : AbstractDMLStmt.getCid(gConn); //cid to this thread if uniq
    int maxPrice = 100; //max price in the test is 100
    BigDecimal price1 = new BigDecimal(Integer.toString(rand.nextInt(maxPrice/2))); 
    BigDecimal price2 = price1.add(new BigDecimal(Integer.toString(rand.nextInt(maxPrice/2))));
    int tid = AbstractDMLStmt.getMyTid();
    
    if (dConn!=null) {
      if (testUniqueKeys) { //test uniqKeys
        whichQuery = rand.nextInt(uniqSelect.length); 
        derbyRS = getUniqQuery(dConn, whichQuery, cid, price1, price2, tid);
        if (derbyRS == null) {
          Log.getLogWriter().info("Did not execute this query");
          return;
        }
        List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
        if (derbyList == null) {
          Log.getLogWriter().info("Not able to convert derby resultSet to a list");
          return;
        }
        gfeRS = getUniqQuery (gConn, whichQuery, cid, price1, price2, tid);
        if (gfeRS==null && !isColocated) {
          Log.getLogWriter().info("tables are not colocated so not able to verify results");
          return;
        }
        List<Struct> gfeList = ResultSetHelper.asList(gfeRS, false);
        if (gfeList==null && isHATest) {
          Log.getLogWriter().info("could not get gfxd results set due to HA");
          return;
        }
        ResultSetHelper.compareResultSets(derbyList, gfeList);   
      } else {
        whichQuery = rand.nextInt(nonUniqSelect.length); 
        derbyRS = getNonUniqQuery(dConn, whichQuery, cid, price1, price2, tid);
        if (derbyRS == null) {
          Log.getLogWriter().info("Did not execute this query");
          return;
        }
        List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
        if (derbyList == null) {
          Log.getLogWriter().info("Not able to convert derby resultSet to a list");
          return;
        }
        gfeRS = getNonUniqQuery (gConn, whichQuery, cid, price1, price2, tid);
        List<Struct> gfeList = ResultSetHelper.asList(gfeRS, false);
        if (gfeList==null && isHATest) {
          Log.getLogWriter().info("could not get gfxd results set due to HA");
          return;
        }
        ResultSetHelper.compareResultSets(derbyList, gfeList);   
      }
    }// we can verify resultSet
    else {
      whichQuery = rand.nextInt(nonUniqSelect.length);
      synchronized (useDisk){
        gfeRS = getNonUniqQuery (gConn, whichQuery, cid, price1, price2, tid);
        //could not verify results.
        if (gfeRS==null & useDisk[0]==true) {
          Log.getLogWriter().info("not able to get the results set due to size is too large");
          useDisk[0] = false;
          return;
        }
      }
      ResultSetHelper.asList(gfeRS, false);      
    }   

  } 
  
  
  //---- other methods ----//
  
  
  //retries when lock could not obtained
  public static ResultSet getUniqQuery(Connection conn, int whichQuery, int cid, 
      BigDecimal price1, BigDecimal price2, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery(conn, whichQuery, cid, price1, price2, tid, success);
    int count=0;
    while (!success[0]) {
      if (count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finish update in derby, abort this operation");
        return rs;
    }
      count++;
      rs = getUniqQuery(conn, whichQuery, cid, price1, price2, tid, success);
    } //retry
    return rs;
  }
  
  public static ResultSet getUniqQuery(Connection conn, int whichQuery, int cid, BigDecimal price1, BigDecimal price2, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try { 
      
      Boolean hasHdfs = TestConfig.tab().booleanAt(SQLPrms.hasHDFS, false);      
      String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";        
      String query = (! SQLHelper.isDerbyConn(conn) && hasHdfs ) ? " QUERY: " +  uniqSelectHdfs[whichQuery] : " QUERY: " +  uniqSelect[whichQuery];        
      stmt = (! SQLHelper.isDerbyConn(conn) && hasHdfs )  ? conn.prepareStatement(uniqSelectHdfs[whichQuery]) :  conn.prepareStatement(uniqSelect[whichQuery]) ; 
         
      
      switch (whichQuery){
      case 0:
        //"select * from trade.securities s, trade.portfolio f where sec_id = f.sid and f.tid = ?",
        Log.getLogWriter().info(database + "Querying SecuritiesPortfolio with TID:"+ tid + query);
        stmt.setInt(1, tid);
        break;
      
      case 1: 
        //"select cid, sid, symbol, price, qty from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and f.tid = ? ",
        Log.getLogWriter().info(database + "Querying SecuritiesPortfolio with CID:" + cid + ",TID:"+ tid +  query);
        stmt.setInt(1, cid);
        stmt.setInt(2, tid); 
        break;
      
      case 2: 
        //"select cid, sid, symbol, exchange, qty, price from trade.securities s, trade.portfolio f where sec_id = f.sid and cid<? and(qty=availQty or price > ?) and f.tid = ?",
        Log.getLogWriter().info(database + "Querying SecuritiesPortfolio with CID:" + cid + ",PRICE:"+ price1 + ",TID:"+ tid + query);
        stmt.setInt(1, cid); //set cid<=?
        stmt.setBigDecimal(2, price1);
        stmt.setInt(3, tid);
        break;
  
      case 3:
        //"select cid, sid, symbol, exchange, price, subtotal from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and (subtotal >10000 or (price >= ? and price <= ?) and f.tid = ?"
        Log.getLogWriter().info(database + "Querying SecuritiesPortfolio with CID:" + cid + ",1_PRICE:"+ price1 + ",2_PRICE:" + price2 + ",TID:"+ tid + query);
        stmt.setInt(1, cid); //set cid>?
        stmt.setBigDecimal(2, price1); 
	stmt.setBigDecimal(3, price2);
        stmt.setInt(4, tid);
        break;
        
      case 4:
        //left out join
        Log.getLogWriter().info(database + "Querying SecuritiesPortfolio with LEFT OUTER JOIN TID:"+ tid + query);
        stmt.setInt(1, tid);
        break;
       
      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (se.getSQLState().equals("0A000") && testServerGroup && !isColocated) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got the expected non colocated exception, continuing tests.");
      } //if it is not colocated, it may throw exception
      else      SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
  //retries when lock could not obtained
  public static ResultSet getNonUniqQuery(Connection conn, int whichQuery, int cid,
      BigDecimal price1, BigDecimal price2, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;    
    rs = getNonUniqQuery(conn, whichQuery, cid, price1, price2, tid, success);
    while (!success[0]) {
      rs = getNonUniqQuery(conn, whichQuery, cid, price1, price2, tid, success);
    } //retry
    return rs;
  }
  
  public static ResultSet getNonUniqQuery(Connection conn, int whichQuery, int cid,
      BigDecimal price1, BigDecimal price2, int tid, boolean[] success) {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    try {
      
      Boolean hasHdfs = TestConfig.tab().booleanAt(SQLPrms.hasHDFS, false);      
      String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";        
      String query = (! SQLHelper.isDerbyConn(conn) && hasHdfs ) ? " QUERY: " +  nonUniqSelectHdfs[whichQuery] : " QUERY: " +  nonUniqSelect[whichQuery];        
      stmt = (! SQLHelper.isDerbyConn(conn) && hasHdfs )  ? conn.prepareStatement(nonUniqSelectHdfs[whichQuery]) :  conn.prepareStatement(nonUniqSelect[whichQuery]) ; 
         
      
      switch (whichQuery){
      case 0:
        //"select * from trade.securities s, trade.portfolio f where sec_id = f.sid and f.tid = ?",
        Log.getLogWriter().info(database +"Querying SecuritiesPortfolio with no data" + query);
        break;

      case 1:
        //"select cid, sid, symbol, price, qty from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and f.tid = ? ",
        Log.getLogWriter().info(database +"Querying SecuritiesPortfolio with CID:" + cid + query);
        stmt.setInt(1, cid);
        break;

      case 2:
        //"select cid, sid, symbol, exchange, qty, price from trade.securities s, trade.portfolio f where sec_id = f.sid and cid<? and(qty=availQty or price > ?) and f.tid = ?",
        Log.getLogWriter().info(database +"Querying SecuritiesPortfolio with CID:" + cid + ",PRICE:"+ price1 + query );
        stmt.setInt(1, cid); //set cid<=?
        stmt.setBigDecimal(2, price1);
        break;

      case 3:
        //"select cid, sid, symbol, exchange, price, subtotal from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and (subtotal >10000 or (price >= ? and price <= ?) and f.tid = ?"
        Log.getLogWriter().info(database +"Querying SecuritiesPortfolio with CID:" + cid + ",1_PRICE:"+ price1 + ",2_PRICE:" + price2 + query);
        stmt.setInt(1, cid); //set cid>?
        stmt.setBigDecimal(2, price1);
        stmt.setBigDecimal(3, price2);
        break;
        
      case 4:
        //Left outer join
        Log.getLogWriter().info(database +"Querying SecuritiesPortfolio with LEFT OUTER JOIN " + query);
        break;

      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (se.getSQLState().equals("0A000") && se.getMessage().matches(".*disk.*")) {
        SQLHelper.printSQLException(se);
        Log.getLogWriter().info("got the unsupported exception, need to remove this once bug#40348 is fixed, continuing test");
        useDisk[0] = true;
        return null;
      }
      else      SQLHelper.handleSQLException(se);
    }
    return rs;
  }
  
//to check if the tables are colocated
  protected static boolean checkColocation() {
    if (testServerGroup) {
      boolean isSecPortfCol = false;
      
      String secSG = (String) map.get("securitiesSG");
      String portfSG = (String) map.get("portfolioSG");
      String schemaSG = (String) map.get(tradeSchemaSG);
      String[] secSGArr = secSG.split(",");
      String[] portfSGArr = portfSG.split(",");
      String[] schemaSGArr = schemaSG.split(",");
      
      ArrayList<String> secSGList = new ArrayList<String>();
      ArrayList<String> portfSGList = new ArrayList<String>();
      ArrayList<String> schemaSGList = new ArrayList<String>();
      
      for (int i=0; i<schemaSGArr.length; i++) 
        schemaSGList.add(schemaSGArr[i]); //get sever groups for schema
      
      if (portfSG.equals("default")) {
        portfSGList.remove("default");
        portfSGList.addAll(schemaSGList);  //portoSG has the same SG as schema, "default" could be added back indicating in all SG
      } else {
        for (int i=0; i<portfSGArr.length; i++) 
          portfSGList.add(portfSGArr[i]); //get sever groups for portfolio
      }
           
      if (secSG.equals("default")) {
        secSGList.remove("default");
        secSGList.addAll(schemaSGList);  //secSG has the same SG as schema
      } else { 
        for (int i=0; i<secSGArr.length; i++) 
          secSGList.add(secSGArr[i]); //get sever groups for securities
      }
      
      ArrayList secParti = (ArrayList) map.get("securitiesPartition");
      ArrayList portfParti = (ArrayList) map.get("portfolioPartition");
      
      if (secParti.size() > 0 && portfParti.size()>0) {
        if (!portfSG.equals(secSG)) isSecPortfCol = false; //please note xxxSG are set string
        else {
          if (secParti.size()== 1 && secParti.get(0).equals("sec_id")
              && portfParti.size()== 1 && portfParti.get(0).equals("sid")
              && ((String)map.get("portfPartitionOn")).equals("default")) 
            //when server groups are same to check if both tables are partitioned on sid
            //if so, check if portfolio use default partition/colocation 
            isSecPortfCol = true;  
        }        
      } //both tables are partitioned, no replicate
      
      else if (secParti.size() == 0 && portfParti.size()>0 ) {
        if (secSGList.contains("default")) {
          isSecPortfCol = true;
        } //sec is replicated to all servers
        else {                 
          if (secSGList.contains(portfSGList)) 
            isSecPortfCol = true; //portf in the server groups that are a subset of sec
        } 
      } /*only sec table is replicate */ 
      
      else if (portfParti.size()==0 && secParti.size() >0) {
        if (portfSGList.contains("default")) {
            isSecPortfCol = true;  
        } //portf is replicated to all servers
        else {                 
          if (portfSGList.contains(secSGList)) 
            isSecPortfCol = true; //sec in the server groups that are a subset of portf
        } 
      } //portf table is replicated
      else {   
        for (int i=0; i<portfSGArr.length; i++) {
          if (secSGList.contains(portfSGArr[i])) {
            isSecPortfCol = true;
            break;
          }
        } //none has "default" case
        if (secSGList.contains("default") || portfSGList.contains("default")) {
          isSecPortfCol = true;
        } //only cust has default server groups       
      } //both tables are replicated
           
      if (isSecPortfCol) {
        Log.getLogWriter().info("portfolio and securities tables are colocated");
        return true; //if both are colocated
      }
      else {
        Log.getLogWriter().info("portfolio and securities tables are not colocated");
        return false;
      }
    }    
    return true; //default to true, any test config does not involve server group need to make sure the tables are colocated.
  }

}

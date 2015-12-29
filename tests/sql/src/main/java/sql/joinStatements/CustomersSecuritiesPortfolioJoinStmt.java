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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

import com.gemstone.gemfire.cache.query.Struct;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.dmlStatements.AbstractDMLStmt;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

/**
 * @author eshu
 *
 */
public class CustomersSecuritiesPortfolioJoinStmt extends AbstractJoinStmt {
  /*
   *    * trade.customers table fields
   *   private int cid;
   *   String cust_name;
   *   Date since;
   *   String addr;
   *   int tid; //for update or delete unique records
   *   
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
  {"select * from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid and f.tid = ?",
   "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid and f.cid >? and f.tid = ? ",
   "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid and f.cid<? and(qty=availQty or price > ?) and f.tid = ?",
   "select f.cid, cust_name, sid, symbol, exchange, price, subtotal  from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid and c.cid >? and (subtotal >10000 or (price >= ? and price <= ?)) and f.tid = ?",
    };

  protected static String[] nonUniqSelect = {
    "select * from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid ",
    "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid and f.cid >? ",
    "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid and f.cid<? and(qty=availQty or price > ?) ",
    "select f.cid, cust_name, sid, symbol, exchange, price, subtotal from trade.customers c, trade.securities s, trade.portfolio f where c.cid= f.cid and sec_id = f.sid and c.cid >? and (subtotal >10000 or (price >= ? and price <= ?))",
    };
  
  protected static String[] uniqSelectHdfs = 
  {"select * from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid and f.tid = ?",
   "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n, trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid and f.cid >? and f.tid = ? ",
   "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n, trade.securities s, trade.portfolio f  -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid and f.cid<? and(qty=availQty or price > ?) and f.tid = ?",
   "select f.cid, cust_name, sid, symbol, exchange, price, subtotal  from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n, trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid and c.cid >? and (subtotal >10000 or (price >= ? and price <= ?)) and f.tid = ?",
    };

  protected static String[] nonUniqSelectHdfs = {
    "select * from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n, trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid ",
    "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n, trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid and f.cid >? ",
    "select f.cid, cust_name, sid, symbol, exchange, price, qty from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n, trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid and f.cid<? and(qty=availQty or price > ?) ",
    "select f.cid, cust_name, sid, symbol, exchange, price, subtotal from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n, trade.securities s, trade.portfolio f -- GEMFIREXD-PROPERTIES queryHDFS=true \n where c.cid= f.cid and sec_id = f.sid and c.cid >? and (subtotal >10000 or (price >= ? and price <= ?))",
    };
  protected static boolean isColocated = checkColocation();
  protected static boolean[] useDisk = {false};
  
  public CustomersSecuritiesPortfolioJoinStmt(){
    
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
      gfeRS = getNonUniqQuery (gConn, whichQuery, cid, price1, price2, tid);
      //could not verify results.
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
    int count = 0;
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
    try{
      Boolean hasHdfs = TestConfig.tab().booleanAt(SQLPrms.hasHDFS, false);      
      String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";        
      String query = (! SQLHelper.isDerbyConn(conn) && hasHdfs ) ? " QUERY: " +  uniqSelectHdfs[whichQuery] : " QUERY: " +  uniqSelect[whichQuery];        
      stmt = (! SQLHelper.isDerbyConn(conn) && hasHdfs )  ? conn.prepareStatement(uniqSelectHdfs[whichQuery]) :  conn.prepareStatement(uniqSelect[whichQuery]) ; 
      
      switch (whichQuery){
      case 0:
        Log.getLogWriter().info(database  + "Querying CustomerSecuritiesPortfolio with TID:"+ tid + query);
        stmt.setInt(1, tid);
        break;
      
      case 1:        
        Log.getLogWriter().info(database  + "Querying CustomerSecuritiesPortfolio with CID:" + cid + ",TID:"+ tid + query);
        stmt.setInt(1, cid);
        stmt.setInt(2, tid); 
        break;
      
      case 2: 
        Log.getLogWriter().info(database  + "Querying CustomerSecuritiesPortfolio with CID:" + cid + ",PRICE:"+ price1 + ",TID:"+ tid + query);
        stmt.setInt(1, cid); //set cid<=?
        stmt.setBigDecimal(2, price1);
        stmt.setInt(3, tid);
        break;
  
      case 3:
        Log.getLogWriter().info(database  + "Querying CustomerSecuritiesPortfolio with CID:" + cid + ",1_PRICE:"+ price1 + ",2_PRICE:" + price2 + ",TID:"+ tid + query);
        stmt.setInt(1, cid); //set cid>?
        stmt.setBigDecimal(2, price1); 
        stmt.setBigDecimal(3, price2);
        stmt.setInt(4, tid);
        break;
       
      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
    	SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (se.getSQLState().equals("0A000") && testServerGroup && !isColocated) {
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
        Log.getLogWriter().info(database + "Querying CustomerSecuritiesNetworth with no data " + query);
        break;
      case 1:
        Log.getLogWriter().info(database + "Querying CustomerSecuritiesNetworth with CID:" + cid + query);
        stmt.setInt(1, cid);
        break;

      case 2:
        Log.getLogWriter().info(database + "Querying CustomerSecuritiesNetworth with CID:" + cid + ",PRICE:"+ price1 + query);
        stmt.setInt(1, cid); //set cid<=?
        stmt.setBigDecimal(2, price1);
        break;

      case 3:
        //"select cid, sid, symbol, exchange, price, subtotal from trade.securities s, trade.portfolio f where sec_id = f.sid and cid >? and (subtotal >10000 or (price >= ? and price <= ?) and f.tid = ?"
        Log.getLogWriter().info(database + "Querying CustomerSecuritiesNetworth with CID:" + cid + ",1_PRICE:"+ price1 + ",2_PRICE:" + price2 + ",TID:"+ tid + query);
        stmt.setInt(1, cid); //set cid>?
        stmt.setBigDecimal(2, price1);
        stmt.setBigDecimal(3, price2);
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
  
  //to check if the three tables are colocated
  protected static boolean checkColocation() {
    if (testServerGroup) {
      boolean isCustPortoCol = false;
      boolean isSecPortoCol = false;
      boolean isCustSecCol = false;
      
      String custSG = (String) map.get("customersSG");
      String secSG = (String) map.get("securitiesSG");
      String portoSG = (String) map.get("portfolioSG");
      String schemaSG = (String) map.get(tradeSchemaSG);
      
      String[] custSGArr = custSG.split(",");
      String[] secSGArr = secSG.split(",");
      String[] portoSGArr = portoSG.split(",");
      String[] schemaSGArr = schemaSG.split(",");
      
      ArrayList<String> portoSGList = new ArrayList<String>();
      ArrayList<String> secSGList = new ArrayList<String>();
      ArrayList<String> custSGList = new ArrayList<String>();
      ArrayList<String> schemaSGList = new ArrayList<String>();
      
      for (int i=0; i<schemaSGArr.length; i++) 
        schemaSGList.add(schemaSGArr[i]); //get sever groups for schema
      
      if (portoSG.equals("default")) {
      	portoSGList.remove("default");
      	portoSGList.addAll(schemaSGList);  //portoSG has the same SG as schema, "default" could be added back indicating in all SG
      } else {
        for (int i=0; i<portoSGArr.length; i++) 
          portoSGList.add(portoSGArr[i]); //get sever groups for portfolio
      }
      
      if (secSG.equals("default")) {
        secSGList.remove("default");
        secSGList.addAll(schemaSGList);  //secSG has the same SG as schema
      } else { 
        for (int i=0; i<secSGArr.length; i++) 
          secSGList.add(secSGArr[i]); //get sever groups for securities
      }
      
      if (custSG.equals("default")) {
         custSGList.remove("default");
         custSGList.addAll(schemaSGList);  //portoSG has the same SG as schema
      } else {
        for (int i=0; i<custSGArr.length; i++) 
          custSGList.add(custSGArr[i]); //get sever groups for customers
      }
           
      ArrayList custParti = (ArrayList) map.get("customersPartition");
      ArrayList secParti = (ArrayList) map.get("securitiesPartition");
      ArrayList portoParti = (ArrayList) map.get("portfolioPartition");
      
      if (custParti.size() > 0 && secParti.size()>0 && portoParti.size()>0) {
        ; //default to false
      } //all tables are partitioned, no replicate and no common fields
      
      else if (custParti.size() == 0 && secParti.size()>0 && portoParti.size()>0) {
        if (custSGList.contains("default")) {
          isCustPortoCol = true;  
          isCustSecCol = true; 
        } //customers is replicated to the servers (the schema default is to all servers)
        else { 
          if (custSGList.contains(portoSGList))
        	isCustPortoCol = true; //portfolio in the server groups that are a subset of customers'
          if (custSGList.contains(secSGList))
            isCustSecCol = true; //securities in the server groups that are a subset of customers'
        } 
        //now check if securities and Portfolio are colocated
        if (!secSG.equals(portoSG)) isSecPortoCol = false; //please note secSG are set string, otherwise need to parse to array also
        else {
          if (secParti.size()== 1 && secParti.get(0).equals("sec_id")
              && portoParti.size()== 1 && portoParti.get(0).equals("sid")
              && ((String)map.get("portfPartitionOn")).equals("deafult")) 
            isSecPortoCol = true;  
        } //sec, portfolio in same server group
      } /*only customers table is replicate */ 
      
      else if (secParti.size()==0 && custParti.size() >0  && portoParti.size()>0) {
        if (secSGList.contains("default")) {
          isSecPortoCol = true;
          isCustSecCol = true;
        } //securities is replicated to all servers
        else {                 
          if (secSGList.contains(portoSGList)) 
            isSecPortoCol = true; //portfolio in the server groups that are a subset of securities
          if (secSGList.contains(custSGList)) 
            isCustSecCol = true;
        } 
        //check if customers and portfolio are colocated
        if (!custSG.equals(portoSG)) isCustPortoCol = false; //please note xxxSG are set string, otherwise need to parse the array
        else {
          if (custParti.size()== 1 && custParti.get(0).equals("cid")
              && portoParti.size()== 1 && portoParti.get(0).equals("cid")
              && (((String)map.get("portfPartitionOn")).equals("deafult")
                  ||(((String)map.get("custPartitionOn")).equals("range")
                    && ((String)map.get("portfPartitionOn")).equals("range")))) 
            isCustPortoCol = true;  //needs to be default colocation in the test 
        }        
      } /*only securities table is replicate */ 
      
      else if (portoParti.size()==0 && secParti.size()>0 && custParti.size() >0 ){
       //no way that three tables can be colocated
        isCustSecCol=false;
      }  /*only portfolio table is replicate, it will not be colocated */ 
      
      else if (portoParti.size()>0 && secParti.size()==0 && custParti.size() ==0 ){
        if ((secSGList.contains(portoSGList)|| secSGList.contains("default"))
            && (custSGList.contains(portoSGList) || custSGList.contains("default"))) {
          isSecPortoCol = true;
          isCustSecCol = true;
          isCustPortoCol = true;
        } //customers and securities are replicated to all server groups that portfolio is partitioned
      } //only portfolio is partitioned
      
      else if (portoParti.size()==0 && secParti.size()>0 && custParti.size() ==0 ){
        if ((portoSGList.contains(secSGList) || portoSGList.contains("default"))
            && (custSGList.contains(secSGList) || custSGList.contains("default"))) {
          isSecPortoCol = true;
          isCustSecCol = true;
          isCustPortoCol = true;
        } //customers and portfolio are replicated to all server groups that portfolio is partitioned
      } //only securities is partitioned
      
      else if (portoParti.size()==0 && secParti.size()==0 && custParti.size() >0 ){
        if ((portoSGList.contains(custSGList) || portoSGList.contains("default"))
            && (secSGList.contains(custSGList) || secSGList.contains("default"))) {
          isSecPortoCol = true;
          isCustSecCol = true;
          isCustPortoCol = true;
        } //customers and portfolio are replicated to all server groups that portfolio is partitioned
      } //only customers table is partitioned

      else { 
    	if  (!custSGList.contains("default") && !secSGList.contains("default") 
                &&  !portoSGList.contains("default")) {
          for (int i=0; i<portoSGArr.length; i++) {
            if (custSGList.contains(portoSGArr[i]) && secSGList.contains(portoSGArr[i])) {
              isSecPortoCol = true;
              isCustSecCol = true;
              isCustPortoCol = true;
              break;
            }
          } 
    	}//none has "default" case
        
    	else if (custSGList.contains("default") && !secSGList.contains("default") 
            &&  !portoSGList.contains("default")) {
          for (int i=0; i<portoSGArr.length; i++) {
            if (secSGList.contains(portoSGArr[i])) {
              isSecPortoCol = true;
              isCustSecCol = true;
              isCustPortoCol = true;
              break;
            }
          }          
        } //only cust has default server groups
        else if (!custSGList.contains("default") && secSGList.contains("default") 
            &&  !portoSGList.contains("default")) {
          for (int i=0; i<portoSGArr.length; i++) {
            if (custSGList.contains(portoSGArr[i])) {
              isSecPortoCol = true;
              isCustSecCol = true;
              isCustPortoCol = true;
              break;
            }
          }                  
        } //only securities has default server groups
        else if (!custSGList.contains("default") && !secSGList.contains("default") 
            &&  portoSGList.contains("default")) {
          for (int i=0; i<secSGArr.length; i++) {
            if (custSGList.contains(secSGArr[i])) {
              isSecPortoCol = true;
              isCustSecCol = true;
              isCustPortoCol = true;
              break;
            }
          }                  
        }
        else {
          isSecPortoCol = true;
          isCustSecCol = true;
          isCustPortoCol = true;
        } //two all more tables replicate to default server group
      } //all three tables are replicated
      
      Log.getLogWriter().info("custParti.size() is " + custParti.size());
      Log.getLogWriter().info("secParti.size() is " + secParti.size());
      Log.getLogWriter().info("portoParti.size() is " + portoParti.size());
      Log.getLogWriter().info("custSGList is " + custSGList.toString());
      Log.getLogWriter().info("secSGList is " + secSGList.toString());
      Log.getLogWriter().info("portoSGList is " + portoSGList.toString());      
            
      if (isCustPortoCol && isSecPortoCol && isCustSecCol) {
        Log.getLogWriter().info("Customers, Securities and portfolio tables are colocated");
        return true; //if both are colocated
      }
      else {
        Log.getLogWriter().info("Customers, Securities and portfolio tables are not colocated");
        return false;
      }
    }
    
    return true; //default to true, any test config does not involve server group need to make sure the tables are colocated.
  }
}

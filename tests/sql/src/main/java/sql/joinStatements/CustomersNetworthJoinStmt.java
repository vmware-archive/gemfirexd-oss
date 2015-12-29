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
import java.sql.Date;
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
public class CustomersNetworthJoinStmt extends AbstractJoinStmt {
  /*
   * trade.customers table fields
   *   private int cid;
   *   String cust_name;
   *   Date since;
   *   String addr;
   *   int tid; //for update or delete unique records
   */
  
  protected static boolean isColocated = checkColocation();
  protected static boolean[] useDisk = {false};
  public static final String SQL_DRIVER_NAME="gemfirexd";
  protected static String[] uniqSelect = 
  {"select * from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = ?",
   "select n.cid, cust_name, n.securities, n.cash from trade.customers c, trade.networth n where  n.cid = c.cid and n.tid = ? and c.cid >?",
   "select n.cid, cust_name, since, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (securities >=? or c.cid <=?) and since >? and c.tid = ?",
   "select c.cid, addr, since, cust_name, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (n.cid >? or since <?) and (cash>(loanLimit-availLoan) or loanLimit=? or loanLimit-availLoan<?) and c.tid = ?",
   //(to reproduce #46849) "select c.cid, n.cid from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = ? and n.cid is null",
   /*(to reproduce #46783, #46979) "select c.cid, n.cid from trade.customers c left join trade.networth n on c.cid = n.cid where c.tid = ? and n.cid is null",*/
   "select c.cid, n.cid from trade.customers c left join trade.networth n on c.cid = n.cid where c.tid = ?"
   };

  protected static String[] nonUniqSelect = 
  {"select * from trade.customers c, trade.networth n where n.cid = c.cid ",
   "select n.cid, cust_name, n.securities, n.cash from trade.customers c, trade.networth n where  n.cid = c.cid and c.cid >?",
   "select n.cid, cust_name, since, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (securities >=? or c.cid <=?) and since >? ",
   "select c.cid, addr, since, cust_name, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (n.cid >? or since <?) and (cash>(loanLimit-availLoan) or loanLimit=? or loanLimit-availLoan<?) "

    };
  
  protected static String[] uniqSelectHdfs = 
  {"select * from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where n.cid = c.cid and n.tid = ?",
   "select n.cid, cust_name, n.securities, n.cash from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where  n.cid = c.cid and n.tid = ? and c.cid >?",
   "select n.cid, cust_name, since, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where n.cid = c.cid and (securities >=? or c.cid <=?) and since >? and c.tid = ?",
   "select c.cid, addr, since, cust_name, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where n.cid = c.cid and (n.cid >? or since <?) and (cash>(loanLimit-availLoan) or loanLimit=? or loanLimit-availLoan<?) and c.tid = ?",
   //(to reproduce #46849) "select c.cid, n.cid from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = ? and n.cid is null",
   /*(to reproduce #46783, #46979) "select c.cid, n.cid from trade.customers c left join trade.networth n on c.cid = n.cid where c.tid = ? and n.cid is null",*/
   "select c.cid, n.cid from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n left join trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n on c.cid = n.cid where c.tid = ?"
   };

  protected static String[] nonUniqSelectHdfs = 
  {"select * from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where n.cid = c.cid ",
   "select n.cid, cust_name, n.securities, n.cash from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where  n.cid = c.cid and c.cid >?",
   "select n.cid, cust_name, since, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n  where n.cid = c.cid and (securities >=? or c.cid <=?) and since >? ",
   "select c.cid, addr, since, cust_name, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c -- GEMFIREXD-PROPERTIES queryHDFS=true \n , trade.networth n -- GEMFIREXD-PROPERTIES queryHDFS=true \n where n.cid = c.cid and (n.cid >? or since <?) and (cash>(loanLimit-availLoan) or loanLimit=? or loanLimit-availLoan<?) "
   
    };
  public CustomersNetworthJoinStmt(){
    
  }
  
  //---- implementations of interface declared methods ----//
  
  public void query(Connection dConn, Connection gConn) {
    int whichQuery;
    ResultSet derbyRS;
    ResultSet gfeRS;
    int cid = (dConn !=null ) ? AbstractDMLStmt.getCid(dConn) : AbstractDMLStmt.getCid(gConn); //cid to this thread if uniq
    Date since = getSince();
    BigDecimal cash = new BigDecimal(Integer.toString(rand.nextInt(10000)));
    BigDecimal sec = new BigDecimal(Integer.toString(rand.nextInt(10000)));
    int loanLimit = 10000;
    BigDecimal loanAmount = new BigDecimal(Integer.toString(rand.nextInt(10000)));
    int tid = AbstractDMLStmt.getMyTid();
    Boolean hasHdfs = TestConfig.tab().booleanAt(SQLPrms.hasHDFS, false);
    if (dConn!=null) {
      if (testUniqueKeys) { //test uniqKeys
        whichQuery = rand.nextInt(uniqSelect.length); 
        derbyRS = getUniqQuery(dConn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid);
        if (derbyRS == null) {
          Log.getLogWriter().info("Did not execute this query");
          return;
        }
        List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
        if (derbyList == null) {
          Log.getLogWriter().info("Not able to convert derby resultSet to a list");
          return;
        }
        gfeRS = getUniqQuery (gConn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid);           
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
        derbyRS = getNonUniqQuery(dConn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid);
        if (derbyRS == null) {
          Log.getLogWriter().info("Did not execute this query");
          return;
        }
        List<Struct> derbyList = ResultSetHelper.asList(derbyRS, true);
        if (derbyList == null) {
          Log.getLogWriter().info("Not able to convert derby resultSet to a list");
          return;
        }
        gfeRS = getNonUniqQuery (gConn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid);   
        List<Struct> gfeList = ResultSetHelper.asList(gfeRS, false);
        if (gfeList == null) return;  //may not get results due to overflow/server down 
        ResultSetHelper.compareResultSets(derbyList, gfeList);   
      }
    }// we can verify resultSet
    else {
      whichQuery = rand.nextInt(nonUniqSelect.length);
      gfeRS = getNonUniqQuery (gConn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid);   
      //could not verify results.
      if (gfeRS==null & useDisk[0]==true) {
        Log.getLogWriter().info("not able to get the results set due to size is too large");
        useDisk[0] = false;
        return;
      }
      ResultSetHelper.asList(gfeRS, false);      
    }   

  } 
  
  
  //---- other methods ----//
  
  
  //retries when lock could not obtained
  public static ResultSet getUniqQuery(Connection conn, int whichQuery, int cid, Date since, 
      BigDecimal cash, BigDecimal sec, int loanLimit, BigDecimal loanAmount, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getUniqQuery(conn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid, success);
    int count=0;
    while (!success[0]) {
      if (count>=maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finish update in derby, abort this operation");
        return rs;
      }
      count++;
      rs = getUniqQuery(conn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid, success);
    } //retry
    return rs;
  }
  
  public static ResultSet getUniqQuery(Connection conn, int whichQuery, int cid, Date since, BigDecimal cash, BigDecimal sec, 
      int loanLimit, BigDecimal loanAmount, int tid, boolean[] success) {
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
        //"select * from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = ?"
        Log.getLogWriter().info(database + "Querying CustomerNetworth with CID:" + cid + ",TID:"+ tid + query);
        stmt.setInt(1, tid);
        break;
      
      case 1: 
        //"select n.cid, cust_name, n.securities, n.cash from trade.customers c, trade.networth n where  n.cid = c.cid and n.tid = ? and c.cid >?",
        Log.getLogWriter().info(database + "Querying CustomerNetworth with CID:" + cid + ",TID:"+ tid + query);
        stmt.setInt(1, tid);
        stmt.setInt(2, cid); //set cid>?
        break;
      
      case 2: 
        //"select n.cid, cust_name, since, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (securities >=? or c.cid <=?) and since >? and c.tid = ?",
        Log.getLogWriter().info(database + "Querying CustomerNetworth with SECURITIES:" + sec + ",CID:" + cid + ",SINCE:"+ since + ",TID:"+ tid + query);
        stmt.setBigDecimal(1, sec);
        stmt.setInt(2, cid); //set cid<=?
        stmt.setDate(3, since); //since >?
        stmt.setInt(4, tid);
        break;
  
      case 3:
        //"select c.cid, addr, since, cust_name, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (n.cid >? or since <?) and (cash>(loanLimit-availLoan) or loanLimit=? or loanLimit-availLoan<?) and c.tid = ?"
        Log.getLogWriter().info(database + "Querying CustomerNetworth with CID:" + cid + ",SINCE:"+ since + 
        		",LOANLIMIT:" + loanLimit + ",LOANAMOUT:" + loanAmount + ",TID:"+ tid + query);
        stmt.setInt(1, cid); //set cid>?
        stmt.setDate(2, since); //since <?
        stmt.setInt(3, loanLimit);
        stmt.setBigDecimal(4, loanAmount);
        stmt.setInt(5, tid);
        break;
        
      case 4:

        Log.getLogWriter().info(database + "Querying CustomerNetworth with SECURITIES:" + sec + ",CID:" + cid + ",TID:"+ tid + query);
        stmt.setInt(1, tid);
        //stmt.setInt(2, cid);
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
  public static ResultSet getNonUniqQuery(Connection conn, int whichQuery, int cid, Date since, 
      BigDecimal cash, BigDecimal sec, int loanLimit, BigDecimal loanAmount, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    rs = getNonUniqQuery(conn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid, success);
    while (!success[0]) {
      rs = getNonUniqQuery(conn, whichQuery, cid, since, cash, sec, loanLimit, loanAmount, tid, success);
    } //retry
    return rs;
  }
  
  public static ResultSet getNonUniqQuery(Connection conn, int whichQuery, int cid, Date since, BigDecimal cash, BigDecimal sec, 
      int loanLimit, BigDecimal loanAmount, int tid, boolean[] success) {
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
        //"select * from trade.customers c, trade.networth n where n.cid = c.cid "
        Log.getLogWriter().info(database + " Querying CustomerNetworth with none" + query);
        break;
      case 1: 
        //"select n.cid, cust_name, n.securities, n.cash from trade.customers c, trade.networth n where  n.cid = c.cid  and c.cid >?",
        Log.getLogWriter().info(database + " Querying CustomerNetworth with CID:" + cid + ",TID:"+ tid + query);
        stmt.setInt(1, cid); //set cid>?
        break;
      case 2:
        //"select n.cid, cust_name, since, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (securities >=? or c.cid <=?) and since >? ",
        Log.getLogWriter().info(database + " Querying CustomerNetworth with SECURITIES:" + sec + ",CID:" + cid + ",SINCE:"+ since + ",TID:"+ tid + query);
        stmt.setBigDecimal(1, sec);
        stmt.setInt(2, cid); //set cid<=?
        stmt.setDate(3, since); //since >?
        break;
      case 3:
        //"select c.cid, addr, since, cust_name, securities, cash, (loanLimit-availLoan) as loanAmount from trade.customers c, trade.networth n where n.cid = c.cid and (n.cid >? or since <?) and (cash>(loanLimit-availLoan) or loanLimit=? or loanLimit-availLoan<?) "
        Log.getLogWriter().info(database + " Querying CustomerNetworth with CID:" + cid + ",SINCE:"+ since + 
        		",LOANLIMIT:" + loanLimit + ",LOANAMOUNT:" + loanAmount + ",TID:" +
        				""+ tid + query);
        stmt.setInt(1, cid); //set cid>?
        stmt.setDate(2, since); //since <?
        stmt.setInt(3, loanLimit);
        stmt.setBigDecimal(4, loanAmount);
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
  @SuppressWarnings("unchecked")
	protected static boolean checkColocation() {
    if (testServerGroup) {
      boolean isCustNetworthCol = false;
      
      String custSG = (String) map.get("customersSG");
      String netSG = (String) map.get("networthSG");
      String schemaSG = (String) map.get(tradeSchemaSG);
      String[] custSGArr = custSG.split(",");
      String[] netSGArr = netSG.split(",");
      String[] schemaSGArr = schemaSG.split(",");
      
      ArrayList<String> custSGList = new ArrayList<String>();
      ArrayList<String> netSGList = new ArrayList<String>();
      ArrayList<String> schemaSGList = new ArrayList<String>();
      
      for (int i=0; i<schemaSGArr.length; i++) 
        schemaSGList.add(schemaSGArr[i]); //get sever groups for schema
      
      if (netSG.equals("default")) {
        netSGList.remove("default");
        netSGList.addAll(schemaSGList);  //netSG has the same SG as schema, "default" could be added back indicating in all SG
      } else {
        for (int i=0; i<netSGArr.length; i++) 
          netSGList.add(netSGArr[i]); //get sever groups for portfolio
      }
      
      if (custSG.equals("default")) {
    	custSGList.remove("default");
    	custSGList.addAll(schemaSGList);  //portoSG has the same SG as schema, "default" could be added back indicating in all SG
      } else {
        for (int i=0; i<custSGArr.length; i++) 
          custSGList.add(custSGArr[i]); //get sever groups for customers
      }
      
      ArrayList<String> custParti = (ArrayList<String>) map.get("customersPartition");
      ArrayList<String> netParti = (ArrayList<String>) map.get("networthPartition");
      
      if (custParti.size() > 0 && netParti.size()>0) {
        if (!netSG.equals(custSG)) isCustNetworthCol = false; //please note xxxSG are set string
        else {
          if (custParti.size()== 1 && custParti.get(0).equals("cid")
              && netParti.size()== 1 && netParti.get(0).equals("cid")
              && (((String)map.get("netPartitionOn")).equals("default") ||
                  ((String)map.get("custPartitionOn")).equals(map.get("netPartitionOn")))) 
            //when server groups are same to check if both tables are partitioned on cid
            //if so, check if networth use default partition/colocation
            //or both table are partitioned on range and use colocate with key words
            isCustNetworthCol = true;  
        }        
      } //both tables are partitioned, no replicate
      
      else if (custParti.size() == 0 && netParti.size()>0 ) {
        if (custSGList.contains("default")) {
          isCustNetworthCol = true;
        } //customers is replicated to all servers
        else {                  
          if (custSGList.contains(netSGList))
            isCustNetworthCol = true; //networth in the server groups that are a subset of customers'
        } 
      } /*only customers table is replicate */ 
      
      else if (netParti.size()==0 && custParti.size() >0) {
        if (netSGList.contains("default")) {
            isCustNetworthCol = true;  
        } //networth is replicated to all servers
        else {                  
          if (netSGList.contains(custSGList)) 
            isCustNetworthCol = true; //customers in the server groups that are a subset of networth
        } 
      } //networth table is replicated
      else {   
        for (int i=0; i<netSGArr.length; i++) {
          if (custSGList.contains(netSGArr[i])) {
            isCustNetworthCol = true;
            break;
          }
        } //none has "default" case
        if (custSGList.contains("default") || netSGList.contains("default")) {
          isCustNetworthCol = true;
        } //only cust has default server groups       
      } //both tables are replicated
           
      if (isCustNetworthCol) {
        Log.getLogWriter().info("Customers and networth tables are colocated");
        return true; //if both are colocated
      }
      else {
        Log.getLogWriter().info("Customers and networth tables are not colocated");
        return false;
      }
    }    
    return true; //default to true, any test config does not involve server group need to make sure the tables are colocated.
  }
}

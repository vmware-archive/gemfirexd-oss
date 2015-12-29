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
package sql.dmlStatements.json;

import hydra.Log;
import hydra.MasterController;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.simple.JSONObject;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlStatements.AbstractDMLStmt;
import sql.dmlStatements.TradeCompaniesDMLStmt;
import sql.security.SQLSecurityTest;
import sql.sqlTx.SQLDistTxTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import util.TestHelper;

/**
 * @author eshu
 *
 */
public class TradeSecuritiesDMLStmtJson extends AbstractDMLStmt {
  /*
   * trade.securities table fields
   *   private int sec_id;
   *   String symbol;
   *   String exchange;
   *   BigDecimal price;
   *   int tid; //for update or delete unique records to the thread
   */
  
  private static boolean reproduceTicket48725 = true;
  protected static boolean addSecidInProjection = TestConfig.tab().booleanAt(sql.SQLPrms.addSecidInProjection, false);
  protected static String insert = "insert into trade.securities (sec_id, symbol, price, exchange, tid )values (?,?,?,?,?)";
  protected static String put = "put into trade.securities (sec_id, symbol, price, exchange, tid )values (?,?,?,?,?)";
  protected static String[] update = {"update trade.securities set price = ? where sec_id = ? and tid = ? ",  //for trigger test
                                      "update trade.securities set symbol = ? where sec_id = ? and tid = ?",
                                      "update trade.securities set symbol = ?, exchange =? where sec_id = ? and tid = ?",
                                      "update trade.securities set exchange =? where sec_id = ? and tid = ?", //expect unique key constraint check to work
                                      "update trade.securities set price = ? where sec_id = ?",  //non uniq for trigger
                                      "update trade.securities set symbol = ? where sec_id = ? ", //non uniq
              "update trade.securities set exchange =? where sec_id = ? " //non uniq
                                      }; 
  protected static String[] select = {"select sec_id, symbol, price, exchange, tid from trade.securities where tid = ?",
                                   (reproduceTicket48725 ? "select cast(avg( distinct price) as decimal (30, 20)) as avg_distinct_price from trade.securities where tid=? and symbol >?"
                                       : "select cast(avg( price) as decimal (30, 20)) as avg_price from trade.securities where tid=? and symbol >?"),
                                    "select " + (addSecidInProjection? "sec_id, " : "") + "price, symbol, exchange from trade.securities where (price<? or price >=?) and tid =?",
                                    "select sec_id, symbol, price, exchange from trade.securities  where (price >=? and price<?) and exchange =? and tid =?",
                                    "select sec_id, symbol, price, exchange, tid from trade.securities where sec_id = ?",
                                    "select sec_id, price, symbol from trade.securities where symbol >?",
                                     "select price, symbol, exchange from trade.securities where (price<? or price >=?) ",
                                     "select sec_id, symbol, price, exchange from trade.securities  where (price >=? and price<?) and exchange =?"
                                     };
  protected static String[] delete = {"delete from trade.securities where (sec_id = ? or price = ? ) and tid = ?",
                                     "delete from trade.securities where (symbol= ? and exchange = ? ) and tid = ?",
                                     "delete from trade.securities where sec_id=?" //for concTest w/ non unique keys
                                      };

  protected static String[] exchanges = {
                                        "nasdaq", "nye", "amex", "lse", "fse", "hkse", "tse" /*, "notAllowed"*/
                                        };
  
  protected static String insertJSON =  "insert into trade.securities (sec_id, symbol, price, exchange, tid, json_details )values (?,?,?,?,?,?)";
  protected static String putJSON = "put into trade.securities (sec_id, symbol, price, exchange, tid, json_details )values (?,?,?,?,?,?)";
  
  protected static String[] selectJSON = {"select  cast(json_evalPath(json_details, 'sec_id') as integer) as sec_id, cast(json_evalPath(json_details, 'symbol') as varchar(10) ) as symbol, price , cast(json_evalPath(json_details, 'exchange') as varchar(10) as exchange,  cast(json_evalPath(json_details, 'tid') as integer)  as tid from trade.securities where tid = ?",
    (reproduceTicket48725 ?               
        "select cast(avg( distinct (price)) as decimal (30, 20)) as avg_distinct_price from trade.securities where tid=? and cast(json_evalPath(json_details, 'symbol') as varchar(10)) > ?"
        : "select cast(avg( price as decimal (30, 20)) as avg_price from trade.securities where tid=? and and cast(json_evalPath(json_details, 'symbol') as varchar(10)) > ?"),
"select " + (addSecidInProjection? "cast(json_evalPath(json_details, 'sec_id')  as integer) as sec_id, " : "") + " price, cast(json_evalPath(json_details, 'symbol') as varchar(10)) as symbol, exchange  from trade.securities where (price<? or price >=?) and tid =?",
"select sec_id, symbol, price, cast(json_evalPath(json_details, 'exchange') as varchar(10)) as exchange from trade.securities  where (price >=? and price<?) and cast(json_evalPath(json_details, 'exchange') as varchar(10)) =? and cast(json_evalPath(json_details, 'tid') as integer) =?",
"select sec_id,  symbol, price, exchange ,   tid from trade.securities where sec_id = ?",
"select sec_id,  price , symbol  from trade.securities where symbol >?",
"select price ,  cast(json_evalPath(json_details, 'symbol') as varchar(10)) as symbol  ,  exchange   from trade.securities where (price<? or price >=?) ",
"select sec_id,  symbol,  price, cast(json_evalPath(json_details, 'exchange') as varchar(10)) as exchange from trade.securities  where (price >=? and price<?) and cast(json_evalPath(json_details, 'exchange') as varchar(10)) =?"
  }; 
  
  
  
  protected static String[] updateJSON = {"update trade.securities set price = ? , json_details  = ? where  sec_id = ?  and tid =?  ",  //for trigger test
    "update trade.securities set symbol = ? ,  json_details  = ? where (cast(json_evalPath(json_details, 'sec_id')  as integer)) = ? and cast(json_evalPath(json_details, 'tid')  as integer) = ?",
    "update trade.securities set symbol = ?, exchange =? , json_details = ? where cast(json_evalPath(json_details, 'sec_id')  as integer) = ? and tid = ?",
    "update trade.securities set exchange =? , json_details = ? where sec_id = ? and tid = ?", //expect unique key constraint check to work
    "update trade.securities set price = ? ,json_details = ? where cast(json_evalPath(json_details, 'sec_id')  as integer) = ?",  //non uniq for trigger
    "update trade.securities set symbol = ? ,json_details = ? where cast(json_evalPath(json_details, 'sec_id')  as integer) = ? ", //non uniq
    "update trade.securities set exchange =? ,json_details = ? where sec_id = ? " //non uniq
    }; 
  
  protected static String[] deleteJSON = {"delete from trade.securities where cast(json_evalPath(json_details, 'sec_id')  as integer)  = ? or price = ? ) and tid = ?",
    "delete from trade.securities where (symbol= ? and exchange = ? ) and cast(json_evalPath(json_details, 'tid')  as integer) = ?",
    "delete from trade.securities where sec_id=?" //for concTest w/ non unique keys
  };
  
  protected static int maxNumOfTries = 1; 
  //protected static HashMap verifyRowCount = new HashMap();
  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  protected static int maxLength = TestConfig.tab().intAt(sql.SQLPrms.maxSymbolLength,6); //longest symbol length 
  protected static int minLength = TestConfig.tab().intAt(sql.SQLPrms.minSymbolLength,1);
  protected static boolean isSingleSitePublisher = TestConfig.tab().
      booleanAt(sql.wan.SQLWanPrms.isSingleSitePublisher, true);
  protected static ArrayList<String> partitionKeys = null;
  boolean reproduce39418 = false;
  protected static String rangePrice = "20"; //difference range used in the price in the query 
  
  static {
    if (testUniqueKeys || testWanUniqueness) {
      if (maxLength > 7) maxLength = 7; //total length is 10
    }
  }
  
  //---- implementations of interface declared methods ----//
 
  /**
   * use connection to insert data and expect to get the same exceptions
   */
  public void insert(Connection dConn, Connection gConn, int size) {
    insert(dConn, gConn, size, false);
  }
  
  public void put(Connection dConn, Connection gConn, int size) {
    insert(dConn, gConn, size, true);
  }
  /**
   * use connection to insert data and expect to get the same exceptions
   */
  private void insert(Connection dConn, Connection gConn, int size, boolean isPut) {
    int[] sec_id = new int[size];
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    BigDecimal[] price = new BigDecimal[size];
    List<SQLException> exceptionList = new ArrayList<SQLException>();
    
    getDataForInsert(sec_id, symbol, exchange, price, size); //get the data
    int count = 0;
    if (dConn != null) {
      boolean success = insertToDerbyTable(dConn, sec_id, symbol, exchange, 
          price, size, exceptionList);  //insert to derby table  
      while (!success) {
        if (isWanTest && !isSingleSitePublisher) {
          return; //to avoid unique key constraint check failure in multi wan publisher case
        }
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the insert op in derby, will abort this operation in derby");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        exceptionList .clear(); //clear the exceptionList and retry
        success = insertToDerbyTable(dConn, sec_id, symbol, exchange, price, size, exceptionList); 
        count++;
      }
      try {
        insertToGFETable(gConn, sec_id, symbol, exchange, price, size, exceptionList, isPut); 
      } catch (TestException te) {
        if (te.getMessage().contains("Execute SQL statement failed with: 23505")
            && isHATest && SQLTest.isEdge) {
          //checkTicket49605(dConn, gConn, "securities");
          try {
            checkTicket49605(dConn, gConn, "securities", sec_id[0], -1, null, null);
          } catch (TestException e) {
            Log.getLogWriter().info("insert failed due to #49605 ", e);
            //deleteRow(dConn, gConn, "securities", sec_id[0], -1, null, null);
            Log.getLogWriter().info("retry this using put to work around #49605");
            insertToGFETable(gConn, sec_id, symbol, exchange, price, size, exceptionList, true); 
          }
        } else throw te;
      }
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else {
      insertToGFETable(gConn, sec_id, symbol, exchange, price, size, isPut);
    } //no verification
    
    if (hasCompanies) {      
      TradeCompaniesDMLStmt companies = new TradeCompaniesDMLStmt();
      
      companies.insert(dConn, gConn, size, symbol, exchange);
      
      if (dConn!=null) commit(dConn); //to commit and avoid rollback the successful operations
      commit(gConn);
    }
  }

  protected void getNonRepeatPK(int[] sec_id, Connection conn) {
    int maxSecId = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary);
    int size = sec_id.length;
    ArrayList<Integer> list = new ArrayList<Integer>();
    if (size > maxSecId)
      throw new TestException("test issue, not enough data in the test yet");
    while (list.size() < size) {
      int num = rand.nextBoolean()? rand.nextInt(maxSecId) + 1 : getSid(conn);
      if (!list.contains(num)) list.add(num);
    }
    
    for (int i=0; i<size; i++) sec_id[i] = list.get(i);
  }

  //for verification dConn will not be null
  public void update(Connection dConn, Connection gConn, int size) {
    int numOfNonUniqUpdate = 3;  //how many update statement is for non unique keys
    int whichUpdate= getWhichOne(numOfNonUniqUpdate, update.length);
    
    //adding batch update in the test
    boolean useBatch = false;
    int maxsize = 10; 
    if ((whichUpdate == 0 || whichUpdate == 4) && rand.nextBoolean() 
        && !alterTableDropColumn && !testSecurity && !setCriticalHeap) {
      //do not run batch update with security on (see #48514)
      //even though handles the exception here, it will hit another issue of #39605
      //so avoid batch update when authorization is on
      size = rand.nextInt(maxsize) + 1;
      useBatch = true;
    }
    
    int[] sec_id = new int[size];
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    BigDecimal[] price = new BigDecimal[size];
    List<SQLException> exceptionList = new ArrayList<SQLException>();
    
    getDataForUpdate(gConn, sec_id, symbol, exchange, price, size); //get the data
    if (useBatch) {
      getNonRepeatPK(sec_id, gConn);
    }
    
    
    //Log.getLogWriter().info("data used sec_id: "+sec_id+" symbol: "+symbol+" exchange: "+exchange+" price: "+price);
    if (dConn != null) {
      boolean success = false;
      if (useBatch) success = updateDerbyTableUsingBatch(dConn, sec_id, symbol, exchange, price, size, whichUpdate, exceptionList);
      else success = updateDerbyTable(dConn, sec_id, symbol, exchange, price, size, whichUpdate, exceptionList);   
      int count =0;
      while (!success) {
        if (isWanTest && !isSingleSitePublisher) {
          rollback(dConn); 
          return; //to avoid unique key constraint check failure in multi wan publisher case
        } else rollback(dConn); //clean up the batched stmt
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the update op in derby, will abort this operation in derby");
          rollback(dConn); 
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        exceptionList .clear(); //clear the exceptionList and retry
        if (useBatch) success = updateDerbyTableUsingBatch(dConn, sec_id, symbol, exchange, price, size, whichUpdate, exceptionList);
        else success = updateDerbyTable(dConn, sec_id, symbol, exchange, price, size, whichUpdate, exceptionList);   
        count++;
      }
      if (useBatch) updateGfxdTableUsingBatch(gConn, sec_id, symbol, exchange, price, size, whichUpdate, exceptionList); 
      else updateGFETable(gConn, sec_id, symbol, exchange, price, size, whichUpdate, exceptionList); 
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else {
      if (useBatch) updateGfxdTableUsingBatch(gConn, sec_id, symbol, exchange, price, size, whichUpdate);
      else updateGFETable(gConn, sec_id, symbol, exchange, price, size, whichUpdate);
    } //no verification
  }

  public void delete(Connection dConn, Connection gConn) {
    int numOfNonUniqDelete = 1;  //how many delete statement is for non unique keys
    int whichDelete = getWhichOne(numOfNonUniqDelete, delete.length);
    
    if (SQLTest.syncHAForOfflineTest && whichDelete == 0) whichDelete = 1; //avoid #39605 
    
    List<SQLException> exceptionList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    String symbol = getSymbol();
    BigDecimal price = getPrice();
    String exchange = getExchange();
    int sec_id = rand.nextBoolean()? getSid(gConn):rand.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary));   

    //for testUniqueKeys both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, sec_id, symbol, price, exchange, whichDelete, exceptionList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the delete op in derby, will abort this operation in derby");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        exceptionList .clear(); //clear the exceptionList and retry
        success = deleteFromDerbyTable(dConn, sec_id, symbol, price, exchange, whichDelete, exceptionList);
        count++;
      }
      deleteFromGFETable(gConn, sec_id, symbol, price, exchange, whichDelete, exceptionList);
      SQLHelper.handleMissedSQLException(exceptionList);
    } 
    else {
      deleteFromGFETable(gConn, sec_id, symbol, price, exchange, whichDelete); //w/o verification
    }
  }

  //query database using a randomly chosen select statement
  public void query(Connection dConn, Connection gConn) {
    int numOfNonUniq = 4; //how many select statement is for non unique keys, non uniq query must be at the end
    int whichQuery = getWhichOne(numOfNonUniq, select.length); //randomly select one query sql based on test uniq or not
    
    int sec_id = rand.nextInt((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary));
    String symbol = getSymbol();
    BigDecimal price = getPrice();
    String exchange = getExchange();
    int tid = getMyTid();
    ResultSet discRS = null;
    ResultSet gfeRS = null;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();
    
    if (dConn!=null) { 
      try {
        discRS = query(dConn, whichQuery, sec_id, symbol, price, exchange, tid);
        if (discRS == null) {
          Log.getLogWriter().info("could not get the derby result set after retry, abort this query");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) 
            ; //do nothing, expect gfxd fail with the same reason due to alter table
          else return;
        }
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exceptionList);
      }
      try {
        gfeRS = query (gConn, whichQuery, sec_id, symbol, price, exchange, tid); 
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
      
      boolean success = ResultSetHelper.compareResultSets(discRS, gfeRS); 
      if (!success) {
        Log.getLogWriter().info("Not able to compare results, continuing test");
      } //not able to compare results due to derby server error
         
    }// we can verify resultSet
    else {
      try {
        gfeRS =  query (gConn, whichQuery, sec_id, symbol, price, exchange, tid);  
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
  
  //populate the table
  public void populate (Connection dConn, Connection gConn) {
    int initSize = TestConfig.tab().intAt(SQLPrms.initSecuritiesSizePerThread, 20);
    populate(dConn, gConn, initSize);
  }
  
   
  //---- other methods ----//
  
  /**
   * get primary keys from the BB and populate the arrays for insert to table
   * randomly choose one of the exchagnes
   * price will be in the range of .01 to 100.00
   */
  protected void getDataForInsert(int[] sec_id, String[] symbol, String[] exchange, 
      BigDecimal[] price, int size) {
    int key = (int) SQLBB.getBB().getSharedCounters().add(SQLBB.tradeSecuritiesPrimary, size);
    int counter;
    
    for (int i = 0 ; i <size ; i++) {
      counter = key - i;
      sec_id[i]= counter;
      symbol[i] = getSymbol();
      exchange[i] = getExchange();
      price[i] = getPrice();
      if (i==1) {
      price[i] = price[0]; //test avg(distinct price)
      }
    }
  }
  
  /**
   * get primary keys from the BB and populate the arrays for insert to table
   * randomly choose one of the exchagnes
   * price will be in the range of .01 to 100.00
   */
  protected void getDataForUpdate(Connection conn, int[] sec_id, String[] symbol, String[] exchange, 
      BigDecimal[] price, int size) {
    int key = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary);
    
    for (int i = 0 ; i <size ; i++) {
      sec_id[i]= rand.nextBoolean()? rand.nextInt(key) + 1 : getSid(conn);
      symbol[i] = getSymbol();
      exchange[i] = getExchange();
      price[i] = getPrice();
    }
  }
  
  //get a randomly generated symbol
  protected String getSymbol() {
    return getSymbol(maxLength);
  }
  
  //get a randomly generated symbol
  protected String getSymbol(int maxLength) {
    return getSymbol(minLength, maxLength);
  }
  
  protected String getSymbol(int minLength, int maxLength) {
    int aVal = 'a';
    int symbolLength = rand.nextInt(maxLength-minLength+1) + minLength;
    char[] charArray = new char[symbolLength];
    for (int j = 0; j<symbolLength; j++) {
      charArray[j] = (char) (rand.nextInt(26) + aVal); //one of the char in a-z
    }
    if (testUniqueKeys || testWanUniqueness) return new String(charArray) + getMyTid(); //encoding uniqueness
    else return new String(charArray);   
  }
  

  //get a price between .01 to 100.00
  protected BigDecimal getPrice() {
    if (!reproduce39418)
      return new BigDecimal (Double.toString((rand.nextInt(10000)+1) * .01));
    else
      return new BigDecimal (((rand.nextInt(10000)+1) * .01));  //to reproduce bug 39418
  }
  
  //get an exchange
  protected String getExchange() {
    return exchanges[rand.nextInt(exchanges.length)]; // get a random exchange
  }
  
  //insert into Derby
  protected boolean insertToDerbyTable(Connection conn, int[] sec_id, 
      String[] symbol, String[] exchange, BigDecimal[] price, int size, 
      List<SQLException> exceptions)  {
    PreparedStatement stmt = getStmt(conn, insert);
    if (stmt == null) return false;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      try {
      verifyRowCount.put(tid+"_insert"+i, 0);
        count = insertToTable(stmt, sec_id[i], symbol[i], exchange[i], price[i], tid);
        verifyRowCount.put(tid+"_insert"+i, new Integer(count));
        Log.getLogWriter().info("Derby inserts " + verifyRowCount.get(tid+"_insert"+i) + " rows");
      }  catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se))
          return false;
        else if (se.getSQLState().equals("23505") && isWanTest && !isSingleSitePublisher) {
          Log.getLogWriter().info("get unique key constraint violation " +
              "with multiple wan publishers, rollback the operation and continuing");
          try {
            conn.rollback(); //rollback and retry
          } catch (SQLException e) {
            SQLHelper.handleSQLException(e);
          }
          return false;
        }
        else SQLHelper.handleDerbySQLException(se, exceptions);
      } catch (IllegalArgumentException ie) {
        if (reproduce39418) {
          Log.getLogWriter().info("derby gets the IllegalArgumentException: " +
              TestHelper.getStackTrace(ie));
        } else {
          throw new TestException (TestHelper.getStackTrace(ie));
        }
      }
    }
    return true;
  }
  
  //insert into gemfirexd/gfe
  protected void insertToGFETable(Connection conn, int[] sec_id, 
      String[] symbol, String[] exchange, BigDecimal[] price, int size, 
      List<SQLException> exceptions, boolean isPut)  {
    PreparedStatement stmt ; 
    if (SQLTest.hasJSON )
          stmt =  getStmt(conn, isPut ? putJSON : insertJSON);
    else 
          stmt =getStmt(conn, isPut ? put : insert);
    
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
        count = insertToTable(stmt, sec_id[i], symbol[i], exchange[i], price[i], tid, isPut);
        if (count != ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue()) {
          Log.getLogWriter().info("Gfxd insert has different row count from that of derby " +
            "derby inserted " + ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue() +
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
  
  //insert into gemfirexd/gfe w/o verification
  protected void insertToGFETable(Connection conn, int[] sec_id, 
      String[] symbol, String[] exchange, BigDecimal[] price, int size, boolean isPut)  {
    PreparedStatement stmt;
    
    if (SQLTest.hasJSON)
      stmt = getStmt(conn, isPut ? putJSON : insertJSON);
    else  stmt = getStmt(conn, isPut ? put : insert);
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
        insertToTable(stmt, sec_id[i], symbol[i], exchange[i], price[i], tid, isPut);
      } catch (SQLException se) {
        if (( /*temp comment out (se.getErrorCode() == -1) &&  */("23505".equals(se.getSQLState()) ))) {
          Log.getLogWriter().info("Got the expected exception due to unique " +
              "constraint check, continuing tests");
        } else if (se.getSQLState().equals("42500") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
             " continuing tests");
        } else if (alterTableDropColumn && (se.getSQLState().equals("42802")
            || se.getSQLState().equals("42X14"))) {
          Log.getLogWriter().info("Got expected column not found exception in insert, continuing test");
        }
        else if (isPut && se.getSQLState().equals("0A000")) {
          Log.getLogWriter().info("Got expected Feature not Supported Exception during put, continuing test"); 
        } else  
          SQLHelper.handleSQLException(se); //handle the exception
      }
    }    
  }

  //insert a record into the table
  protected int insertToTable(PreparedStatement stmt, int sec_id, 
      String symbol, String exchange, BigDecimal price, int tid) throws SQLException {
    return insertToTable(stmt, sec_id, symbol, exchange, price, tid, false);
  }
  //insert a record into the table
  @SuppressWarnings("unchecked")
  protected int insertToTable(PreparedStatement stmt, int sec_id, 
      String symbol, String exchange, BigDecimal price, int tid, boolean isPut) throws SQLException {
    boolean isDerbyConn = SQLHelper.isDerbyConn(stmt.getConnection());
    String txId =  SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " ";
    String driverName = stmt.getConnection().getMetaData().getDriverName();
    String database = isDerbyConn ?"Derby - " :"gemfirexd - " + txId;
    
    String json = getJSON(sec_id, symbol, exchange, price, tid);
    
  
    Log.getLogWriter().info( database +  ( isPut ? "putting" : "inserting") + " on trade.securities with SEC_ID:" + sec_id +
        ",SYMBOL:"+ symbol + ",PRICE:" + price + ",EXCHANGE:" + exchange + ",TID:" + tid + ( !isDerbyConn && SQLTest.hasJSON ? ", JSON: " + json : " " ) );
    
    
    stmt.setInt(1, sec_id);
    stmt.setString(2, symbol);
    stmt.setBigDecimal(3, price);
    stmt.setString(4, exchange);       
    stmt.setInt(5, tid);
   
    if ( !isDerbyConn && SQLTest.hasJSON ) {
      stmt.setString(6, json);
    }
    
    int rowCount = stmt.executeUpdate();
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    Log.getLogWriter().info( database + ( isPut ? "put " : "inserted ") + rowCount + " rows in trade.securities SEC_ID:" + sec_id +
        ",SYMBOL:"+ symbol + ",PRICE:" + price + ",EXCHANGE:" + exchange + ",TID:" + tid + ( !isDerbyConn && SQLTest.hasJSON ? ", JSON: " + json : " " ));
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
        
    if ( driverName.toLowerCase().contains("gemfirexd") && isPut) {
      if (! SQLTest.ticket49794fixed) {
        //manually update fulldataset table for above entry.
         insertToSecuritiesFulldataset(stmt.getConnection() , sec_id,symbol,price,exchange,tid);
        }      
     Log.getLogWriter().info( database + ( isPut ? "putting " : "inserting ") + " in trade.securities with SEC_ID:" + sec_id +
          ",SYMBOL:"+ symbol + ",PRICE:" + price + ",EXCHANGE:" + exchange + ",TID:" + tid + ( !isDerbyConn && SQLTest.hasJSON ? ", JSON: " + json : " " ));
     rowCount = stmt.executeUpdate();
     Log.getLogWriter().info( database + ( isPut ? "put " : "inserted ") + rowCount + " rows in trade.securities SEC_ID:" + sec_id +
         ",SYMBOL:"+ symbol + ",PRICE:" + price + ",EXCHANGE:" + exchange + ",TID:" + tid + ( !isDerbyConn && SQLTest.hasJSON ? ", JSON: " + json : " " ));
     warning = stmt.getWarnings(); //test to see there is a warning   
     if (warning != null) {
       SQLHelper.printSQLWarning(warning);
     } 
    }    
    return rowCount;
  }
  
  
  protected void insertToSecuritiesFulldataset (Connection conn, int sec_id, String symbol, BigDecimal price, String exchange, int tid){
    //manually update fulldataset table for above entry.
     try{
      
       Log.getLogWriter().info(" Trigger behaviour is not defined for putDML hence deleting  the  row  from TRADE.SECURITIES_FULLDATASET with data SEC_ID:" +  sec_id );
       conn.createStatement().execute("DELETE FROM TRADE.SECURITIES_FULLDATASET  WHERE  sec_id = "  + sec_id );      

      PreparedStatement preparedInsertStmt = conn.prepareStatement("insert into trade.SECURITIES_fulldataset values (?,?,?,?,?)");          
      
      preparedInsertStmt.setInt(1, sec_id);
      preparedInsertStmt.setString(2, symbol);
      preparedInsertStmt.setBigDecimal(3, price);
      preparedInsertStmt.setString(4, exchange);       
      preparedInsertStmt.setInt(5, tid);       
      Log.getLogWriter().info(" Trigger behaviour is not defined for putDML hence inserting  the  row  into  TRADE.SECURITIES_FULLDATASET with data SEC_ID:" +  sec_id +  ",SYMBOL" + symbol  + ",EXCHANGE:" +  exchange + ",PRICE:" + price + ".TID:" + tid );
      preparedInsertStmt.executeUpdate();
     } catch (SQLException se) {
       Log.getLogWriter().info("Error while updating TRADE.SECURITIES_FULLDATASET table. It may cause Data inconsistency " + se.getMessage() ); 
     }
  }
  
  //add the exception to expceptionList to be compared with gfe
  protected boolean deleteFromDerbyTable(Connection dConn, int sec_id, String symbol, 
      BigDecimal price, String exchange, int whichDelete, List<SQLException> exList){
    PreparedStatement stmt = getStmt(dConn, delete[whichDelete]); 
    if (stmt == null) return false;
    int tid = getMyTid();
    int count=-1;
    
    try {
      verifyRowCount.put(tid+"_delete", 0);
        count = deleteFromTable(stmt, sec_id, symbol, exchange, price, tid, whichDelete);
        verifyRowCount.put(tid+"_delete", new Integer(count));
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dConn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  //compare whether the exceptions got are same as those from derby
  protected void deleteFromGFETable(Connection dConn, int sec_id, String symbol, 
      BigDecimal price, String exchange, int whichDelete, List<SQLException> exList){
    PreparedStatement stmt = getStmt(dConn, delete[whichDelete]); 
    if (SQLTest.testSecurity && stmt == null) {
      SQLHelper.handleGFGFXDException((SQLException)
          SQLSecurityTest.prepareStmtException.get(), exList);
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
    try {
      count = deleteFromTable(stmt, sec_id, symbol, exchange, price, tid, whichDelete);
      if (count != ((Integer)verifyRowCount.get(tid+"_delete")).intValue()){
        Log.getLogWriter().info("Gfxd delete (securities) has different row count from that of derby " +
                "derby deleted " + ((Integer)verifyRowCount.get(tid+"_delete")).intValue() +
                " but gfxd deleted " + count);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  //for delete w/o verification
  protected void deleteFromGFETable(Connection dConn, int sec_id, String symbol, 
      BigDecimal price, String exchange, int whichDelete){
    PreparedStatement stmt = getStmt(dConn, delete[whichDelete]); 
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
    
    try {
      deleteFromTable(stmt, sec_id, symbol, exchange, price, tid, whichDelete);
    } catch (SQLException se) {
      if (se.getSQLState().equals("23503"))  
        Log.getLogWriter().info("detected delete caused the foreign key constraint violation, continuing test");
      else if ((se.getSQLState().equals("42500") || se.getSQLState().equals("42502"))
          && testSecurity) {
        Log.getLogWriter().info("Got the expected exception for authorization," +
           " continuing tests");
      } else if (alterTableDropColumn && (se.getSQLState().equals("42X14") || se.getSQLState().equals("42X04"))) {
        //42X04 is possible when column in where clause is dropped
        Log.getLogWriter().info("Got expected column not found exception in delete, continuing test");
      } else
        SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  //delete from table based on whichDelete
  protected int deleteFromTable(PreparedStatement stmt, int sec_id, String symbol, 
      String exchange, BigDecimal price, int tid, int whichDelete) throws SQLException {
    String txId =  SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " ";
    boolean isDerbyConn =  SQLHelper.isDerbyConn(stmt.getConnection());
    String database =isDerbyConn?"Derby - " :"gemfirexd - " + txId;
    String query = " QUERY : " + ( SQLTest.hasJSON && ! isDerbyConn ? deleteJSON[whichDelete] : delete[whichDelete]);
    int rowCount=0;
    switch (whichDelete) {
    case 0:   
      //"delete from trade.securities where (sec_id = ? or price = ? ) and tid = ?",
      Log.getLogWriter().info(database + "deleting trade.securities with SEC_ID:" +sec_id + ",PRICE:" + price + ",TID:" +tid + query);
      stmt.setInt(1, sec_id);
      stmt.setBigDecimal(2, price);
      stmt.setInt(3,tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.securities with SEC_ID:" +sec_id + ",PRICE:" + price + ",TID:" +tid + query);
      break;
    case 1:
      //"delete from trade.securities where (symbol= ? and exchange = ? ) and tid = ?",
      Log.getLogWriter().info(database + "deleting trade.securities with SYMBOL:"+ symbol +",EXCHANGE:" + exchange + ",TID:" + tid + query);
      stmt.setString(1, symbol);
      stmt.setString(2, exchange);
      stmt.setInt(3, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.securities with SYMBOL:"+ symbol +",EXCHANGE:" + exchange + ",TID:" + tid + query);
      break;
    case 2:
      //"delete from trade.securities where sec_id=?" //for concTest w/ non unique keys
      Log.getLogWriter().info(database + "deleting trade.securities with SEC_ID:" +sec_id + query);
      stmt.setInt(1, sec_id);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " + rowCount + " rows in trade.securities with SEC_ID:" +sec_id + query);
      break;
    default:
      throw new TestException("incorrect delete statement, should not happen");
    }  
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }   
  
  protected boolean updateDerbyTable(Connection conn, int[] sec_id, String[] symbol, 
      String[] exchange, BigDecimal[] price, int size, int whichUpdate, 
      List<SQLException> exceptions) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    boolean[] unsupported = new boolean[1];
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate, unsupported);
    else stmt = getStmt(conn, update[whichUpdate]); //use only this after bug#39913 is fixed
    
    if (stmt == null) {
      if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true)
        return true; //do the same in gfxd to get alter table exception
      else if (unsupported[0]) return true; //do the same in gfxd to get unsupported exception
      else return false;
      /*
      try {
        conn.prepareStatement(update[whichUpdate[i]]);
      } catch (SQLException se) {
        if (se.getSQLState().equals("08006") || se.getSQLState().equals("08003"))
          return false;
      } 
      */
      //this test of connection is lost is necessary as stmt is null
      //could be caused by not allowing update on partitioned column. 
      //the test of connection lost could be removed after #39913 is fixed
      //just return false if stmt is null
    }
    
    for (int i=0 ; i<size ; i++) {
      try {
        if (stmt != null) {
          verifyRowCount.put(tid+"_update"+i, 0);
          count = updateTable(stmt, sec_id[i], symbol[i], exchange[i], price[i], tid, whichUpdate );
          verifyRowCount.put(tid+"_update"+i, new Integer(count));          
        }
      }  catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se))
          return false;
        else if (se.getSQLState().equals("23505") && isWanTest && !isSingleSitePublisher) {
          Log.getLogWriter().info("get unique key constraint violation " +
              "with multiple wan publishers, rollback the operation and continuing");
          try {
            conn.rollback(); //rollback and retry
          } catch (SQLException e) {
            SQLHelper.handleSQLException(e);
          }
          return false;
        }
        else     
          SQLHelper.handleDerbySQLException(se, exceptions);          
      }
    }
    return true;
  }
  
  protected boolean updateDerbyTableUsingBatch(Connection conn, int[] sec_id, String[] symbol, 
      String[] exchange, BigDecimal[] price, int size, int whichUpdate, 
      List<SQLException> exceptions) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
       
    boolean[] unsupported = new boolean[1];
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate, unsupported);
    else stmt = getStmt(conn, update[whichUpdate]); //use only this after bug#39913 is fixed
    
    if (stmt == null) {
      if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true)
        return true; //do the same in gfxd to get alter table exception
      else if (unsupported[0]) return true; //do the same in gfxd to get unsupported exception
      else return false; 
    }
    
    int counts[] = null;
    for (int i=0 ; i<size ; i++) {
      if (stmt != null) {
        try {          
          
          Log.getLogWriter().info(" Derby - batch updating trade.securities with PRICE:" + price[i] +  " where SEC_ID:" + sec_id[i] +
              (whichUpdate == 0 ? ",TID: " + tid : "" ) + " QUERY: " + update[whichUpdate]);
          stmt.setBigDecimal(1, price[i]);
          stmt.setInt(2, sec_id[i]);
          if (whichUpdate == 0) stmt.setInt(3, tid);
          
          stmt.addBatch();
        } catch (SQLException se) {
          if (!SQLHelper.checkDerbyException(conn, se)) return false; //retry
          else     
            SQLHelper.handleDerbySQLException(se, exceptions);          
        }
      }      
    }
    
    try {
      counts = stmt.executeBatch(); 
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry
      else SQLHelper.handleDerbySQLException(se, exceptions); //not expect any exception on update of price field
    }   
    
    for (int i =0; i<counts.length; i++) {  
      if (counts[i] != -3) {
        verifyRowCount.put(tid+"_update"+i, 0);
        verifyRowCount.put(tid+"_update"+i, new Integer(counts[i]));
        Log.getLogWriter().info(" Derby - updated " + counts[i] + " rows in trade.securities with PRICE:" + price[i] +  " where SEC_ID:" + sec_id[i]  +
            (whichUpdate == 0 ? ",TID:" + tid : "" ));
      } else throw new TestException("derby failed to update a row in batch update");
    }
    return true;

  }
  
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate, boolean[] unsupported){
    if (partitionKeys == null) setPartitionKeys();

    return getCorrectStmt(conn, whichUpdate, partitionKeys, unsupported);
  }
  
  @SuppressWarnings("unchecked")
  protected void setPartitionKeys() {
    if (!isWanTest) {
      partitionKeys= (ArrayList<String>)partitionMap.get("securitiesPartition");
    }
    else {
      int myWanSite = getMyWanSite();
      partitionKeys = (ArrayList<String>)wanPartitionMap.
        get(myWanSite+"_securitiesPartition");
    }
    Log.getLogWriter().info("partition keys are " + partitionKeys);
  }
  
  //used to parse the partitionKey and test unsupported update on partitionKey, no need after bug #39913 is fixed
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys, boolean[] unsupported){
    PreparedStatement stmt = null;
    
    String[] update ;
    if (SQLTest.hasJSON && !SQLHelper.isDerbyConn(conn)) 
        update = TradeSecuritiesDMLStmtJson.updateJSON;
    else 
      update = TradeSecuritiesDMLStmtJson.update;
    
    
    switch (whichUpdate) {
    case 0: 
      //"update trade.securities set price = ? where sec_id = ? and tid = ? "
      if (partitionKeys.contains("price")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
      //"update trade.securities set symbol = ? where sec_id = ? and tid = ?",
      if (partitionKeys.contains("symbol")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: 
      //"update trade.securities set symbol = ?, exchange =? where sec_id = ? and tid = ?",
      if (partitionKeys.contains("symbol") || partitionKeys.contains("exchange")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: 
      //    "update trade.securities set exchange =? where sec_id = ? and tid = ?", 
      if (partitionKeys.contains("exchange")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: 
      //"update trade.securities set price = ? where sec_id = ?", 
      if (partitionKeys.contains("price")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 5: 
      // "update trade.securities set symbol = ? where sec_id = ? "
      if (partitionKeys.contains("symbol")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 6: 
      //   "update trade.securities set exchange =? where sec_id = ? "  
      if (partitionKeys.contains("exchange")) {
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
  
  protected void updateGFETable(Connection conn, int[] sec_id, String[] symbol, 
      String[] exchange, BigDecimal[] price, int size, int whichUpdate, 
      List<SQLException> exceptions) {
    PreparedStatement stmt;
    int tid = getMyTid();
    int count = 0;
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate, null);    
    else  if (SQLTest.hasJSON ) 
              stmt = getStmt(conn, updateJSON[whichUpdate]);
          else stmt = getStmt(conn, update[whichUpdate]); //use only this after bug#39913 is fixed

    if (SQLTest.testSecurity && stmt == null) {
      if (SQLSecurityTest.prepareStmtException.get() != null) {
        SQLHelper.handleGFGFXDException((SQLException)
          SQLSecurityTest.prepareStmtException.get(), exceptions);
        SQLSecurityTest.prepareStmtException.set(null);
        return;
      }
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    
    for (int i=0 ; i<size ; i++) {
      try {
        if (stmt!=null) {
          count = updateTable(stmt, sec_id[i], symbol[i], exchange[i], price[i], tid, whichUpdate );
          if (count != ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue()){
            Log.getLogWriter().info("Gfxd update has different row count from that of derby " +
                    "derby updated " + ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue() +
                    " but gfxd updated " + count);
          }
        }
      }  catch (SQLException se) {
        if (exceptions.size()>0) {
          int firstIndex = 0;
          SQLException derbySe = exceptions.get(firstIndex);
  
          //handle special case here.
          if (derbySe.getSQLState().equals("23505") && hasCompanies) {
            if (se.getSQLState().equals("23503")) {
              SQLHelper.printSQLException(se);
              Log.getLogWriter().info("allow update unique key to check foreign key reference first");
              exceptions.remove(firstIndex);
              return;
            }
          }
        }
        SQLHelper.handleGFGFXDException(se, exceptions);
      }    
    }
  }
  
  protected void updateGfxdTableUsingBatch(Connection conn, int[] sec_id, String[] symbol, 
      String[] exchange, BigDecimal[] price, int size, int whichUpdate, 
      List<SQLException> exceptions) {
    PreparedStatement stmt;
    int tid = getMyTid();
    
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate, null);
    else stmt = getStmt(conn, update[whichUpdate]); //use only this after bug#39913 is fixed

    if (SQLTest.testSecurity && stmt == null) {
      if (SQLSecurityTest.prepareStmtException.get() != null) {
        SQLHelper.handleGFGFXDException((SQLException)
          SQLSecurityTest.prepareStmtException.get(), exceptions);
        SQLSecurityTest.prepareStmtException.set(null);
        return;
      }
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    
    if (stmt == null) return;
    
    int counts[] = null;
    for (int i=0 ; i<size ; i++) {
      try {
        String json="";
        String jsonLog = ",JSON_DETAILS:";
        if (SQLTest.hasJSON){
            json =  getJSON(sec_id[i], symbol[i], exchange[i], price[i], tid);
            jsonLog = jsonLog + json + " ";
        }
        Log.getLogWriter().info(" gemfirexd - batch Updating trade.securities with PRICE:" + price[i] + jsonLog +  " where SEC_ID:" + sec_id[i]  +
            (whichUpdate == 0 ? ",TID:" + tid : "" ) +   update[whichUpdate]);
        int param=1;
        stmt.setBigDecimal(param++, price[i]);
        if (SQLTest.hasJSON ) {
          stmt.setString(param++, json);          
        }
        stmt.setInt(param++, sec_id[i]);        
        if (whichUpdate == 0) stmt.setInt(param++, tid);
        
        stmt.addBatch();
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptions); //should not see any exceptions
      }     
    }
    
    try {
      counts = stmt.executeBatch(); 
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exceptions); //not expect any exception on update of price field
    }   
    
    for (int i =0; i<counts.length; i++) {         
      if (counts[i] != -3) {
        Log.getLogWriter().info("gemfirexd - updated " + counts[i] + " rows in trade.securities table with PRICE:" + price[i] + " where SEC_ID:" + sec_id[i]  +
            (whichUpdate == 0 ? ",TID:" + tid : "" ) +  update[whichUpdate] );

        if (counts[i] != ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue()) 
          Log.getLogWriter().warning("Gfxd update has different row count from that of derby " +
              "derby updated " + ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue() +
              " but gfxd updated " + counts[i]);
      } else {
        //throw new TestException("gfxd failed to update a row in batch update");
        Log.getLogWriter().warning("gfxd failed to update in batch update");
      }
    }   
  }
  

  protected void updateGFETable(Connection conn, int[] sec_id, String[] symbol, 
      String[] exchange, BigDecimal[] price, int size, int whichUpdate) {
    PreparedStatement stmt;
    int tid = getMyTid();
    
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate, null);
    else stmt = getStmt(conn, update[whichUpdate]); //use only this after bug#39913 is fixed
    
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    for (int i=0 ; i<size ; i++) {
      try {
        if (stmt != null)
          updateTable(stmt, sec_id[i], symbol[i], exchange[i], price[i], tid, whichUpdate );
      }  catch (SQLException se) {
        if (( /* (se.getErrorCode() == -1)  && */ ("23505".equals(se.getSQLState()) ))) {
          Log.getLogWriter().info("Got the expected exception due to unique constraint check");
        }
        else if (( /* (se.getErrorCode() == -1)  && */ ("23503".equals(se.getSQLState()) ))) {
            Log.getLogWriter().info("Got the expected exception due to unique constraint check");
          }
        else if (se.getSQLState().equals("42502") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
          " continuing tests");
        } else if (alterTableDropColumn && se.getSQLState().equals("42X14")) {
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else if (hasCompanies && se.getSQLState().equals("23503")) {
          Log.getLogWriter().info("Got expected violation of foreign key constraint in update, continuing test");
        } else 
          SQLHelper.handleSQLException(se); //handle the exception
      }    
    }
  }
  
  protected void updateGfxdTableUsingBatch(Connection conn, int[] sec_id, String[] symbol, 
      String[] exchange, BigDecimal[] price, int size, int whichUpdate) {
    PreparedStatement stmt;
    int tid = getMyTid();
    String[] query = new String[size];
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate, null);
    else stmt = getStmt(conn, updateJSON[whichUpdate]); //use only this after bug#39913 is fixed
    
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    }
    
    if (stmt == null) return;
    
    int counts[] = null;
    for (int i=0 ; i<size ; i++) {
      try {
        Log.getLogWriter().info("gemfirexd - updating trade.securities with SEC_ID:" + sec_id[i]  + ",PRICE:" + price[i] +
            (whichUpdate == 0 ? ",TID:" + tid : "" ) + "QUERY: " +  updateJSON[whichUpdate] );
        HashMap<String,Object> map = (HashMap<String,Object>) getCurrentRowOfUpdate(stmt.getConnection() , sec_id[i]);
        String json = getJSON(sec_id[i], (String) map.get("symbol"), (String) map.get("exchange"), price[i], tid);
        stmt.setBigDecimal(1, price[i]);
        stmt.setString(2,json);
        stmt.setInt(3, sec_id[i]);
        query[i]=update[whichUpdate];
        if (whichUpdate == 0) stmt.setInt(4, tid);
        
        stmt.addBatch();
      } catch (SQLException se) {
        if (se.getSQLState().equals("42502") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
          " continuing tests");
        } else if (alterTableDropColumn && se.getSQLState().equals("42X14")) {
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else if (hasCompanies && se.getSQLState().equals("23503")) {
          Log.getLogWriter().info("Got expected violation of foreign key constraint in update, continuing test");
        } else 
          SQLHelper.handleSQLException(se); //handle the exception
      }      
    }
    
    try {
      counts = stmt.executeBatch(); 
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se); //not expect any exception on update of price field
    }   
    
    for (int i =0; i<counts.length; i++) {         
      if (counts[i] != -3) {
        Log.getLogWriter().info("gemfirexd -  updated " + counts[i] + " rows in trade.securities with SEC_ID:" + sec_id[i]  + "PRICE:" + price[i] +
            (whichUpdate == 0 ? "TID:" + tid : "" ) + " QUERY:" + query[i]);

      } else {
        //throw new TestException("gfxd failed to update a row in batch update");
        Log.getLogWriter().warning("gemfirexd -  failed to update in batch update in " + i + " update");
      }
    }   
  }
  
  //update table based on the update statement
  protected int updateTable(PreparedStatement stmt, int sec_id, String symbol,
      String exchange, BigDecimal price, int tid, int whichUpdate) throws SQLException {
    //Log.getLogWriter().info("update table with sec_id: " + sec_id + " symbol: " + symbol +
    //    " exchange: " + exchange + " price: " + price);
    
    boolean isDerbyConn= SQLHelper.isDerbyConn(stmt.getConnection());
    String database =isDerbyConn?"Derby - " :"gemfirexd - " ;    
    String jsonString = "";
    String json="";
    String query = " QUERY: " +  ( SQLTest.hasJSON && ! isDerbyConn ? updateJSON[whichUpdate] : update[whichUpdate] ); 
    String successStmt = "";
    int i =1;
     
     if ( ! isDerbyConn && SQLTest.hasJSON)        
     { 
        json = getJSON(sec_id, symbol, exchange, price, tid);
     }
     
     HashMap<String,Object> map = new HashMap<String, Object>();
     
     if ( SQLTest.hasJSON & ! isDerbyConn ) {
       map = (HashMap<String,Object>) getCurrentRowOfUpdate(stmt.getConnection() , sec_id);
       query = " QUERY: " + updateJSON[whichUpdate];      
     }
     
     
     
    switch (whichUpdate) {
    case 0: 
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSON(sec_id, (String) map.get("symbol"), (String) map.get("exchange"), price, tid);
        jsonString = ",JSON_DETAILS: " + json;
        }
      
      Log.getLogWriter().info(database + "updating trade.securities with PRICE:" + price + jsonString +
        " where SEC_ID:" + sec_id +   "TID:" + tid + query);
      stmt.setBigDecimal(i++, price);
      
      if ( ! isDerbyConn && SQLTest.hasJSON) {
        stmt.setObject(i++, json);        
      }
      stmt.setInt(i++, sec_id);
      stmt.setInt(i++, tid);

      successStmt=" rows in trade.securities with PRICE:" + price +  jsonString +
        " where SEC_ID:" + sec_id +   "TID:" + tid + query;
      //stmt.executeUpdate();
      break;
    case 1: 
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSON(sec_id, symbol, (String) map.get("exchange"), (BigDecimal) map.get("price"), tid);
        jsonString = ",JSON_DETAILS: " + json;
        }
      
      Log.getLogWriter().info(database + "updating trade.securities with SYMBOL:" + symbol + jsonString + " where SEC_ID:" + sec_id +
          ",TID:" + tid + query);
      stmt.setString(i++, symbol);
      
      if ( ! isDerbyConn && SQLTest.hasJSON) {
        stmt.setObject(i++, json);
      }
      stmt.setInt(i++, sec_id);
      stmt.setInt(i++, tid);
      successStmt=" rows in trade.securities  SYMBOL:" + symbol +  jsonString + " where SEC_ID:" + sec_id +
          ",TID:" + tid + query;
      //stmt.executeUpdate();
      break;
    case 2: 
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSON(sec_id, symbol, exchange, (BigDecimal) map.get("price"), tid);
        jsonString = ",JSON_DETAILS: " + json;
        }
      
      Log.getLogWriter().info(database + "updating trade.securities with SYMBOL:" + symbol + 
          ",EXCHANGE:" + exchange +  jsonString + " where SEC_ID:" + sec_id +  ",TID:" + tid + query);
      stmt.setString(i++, symbol);
      stmt.setString(i++, exchange);
      
        if ( ! isDerbyConn && SQLTest.hasJSON) {
          Clob jsonClob = stmt.getConnection().createClob();
          jsonClob.setString(1, json);          
          stmt.setClob(i++, jsonClob);
        }      
      stmt.setInt(i++, sec_id);
      stmt.setInt(i++, tid);

      successStmt=" rows in trade.securities with SYMBOL:" + symbol + 
          ",EXCHANGE:" + exchange +  jsonString + " where SEC_ID:" + sec_id +  ",TID:" + tid + query;
      //stmt.executeUpdate();
      break;
    case 3: //update name, since
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSON(sec_id, (String) map.get("symbol"), exchange, (BigDecimal) map.get("price"), tid);
        jsonString = ",JSON_DETAILS: " + json;
        }
      
      Log.getLogWriter().info(database + "updating trade.securities with EXCHANGE:" + exchange + 
          jsonString + " where SEC_ID:" + sec_id + ",TID:" + tid + query);
      stmt.setString(i++, exchange);
      if ( ! isDerbyConn && SQLTest.hasJSON) {
        Clob jsonClob = stmt.getConnection().createClob();
        jsonClob.setString(1, json);
        stmt.setClob(i++, jsonClob);
      }
      stmt.setInt(i++, sec_id);
      stmt.setInt(i++, tid);      
      successStmt=" rows in trade.securities with EXCHANGE:" + exchange + 
      jsonString + " where SEC_ID:" + sec_id + ",TID:" + tid + query;
      //stmt.executeUpdate();
      break;
    case 4:
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSON(sec_id, (String) map.get("symbol"), (String) map.get("exchange"), price, tid);
        jsonString = ",JSON_DETAILS: " + json;
        }
      
      Log.getLogWriter().info(database + "updating trade.securities with PRICE:" + price +  jsonString + " where SEC_ID:" + sec_id  +  query);     
      stmt.setBigDecimal(i++, price);
      if ( ! isDerbyConn && SQLTest.hasJSON) {
        Clob jsonClob = stmt.getConnection().createClob();
        jsonClob.setString(1, json);
        stmt.setClob(i++, jsonClob);
      }
      stmt.setInt(i++, sec_id);
      successStmt=" rows in trade.securities with PRICE:" + price +  jsonString + " where SEC_ID:" + sec_id  +  query;
      //stmt.executeUpdate();
      break; //non uniq keys
    case 5:
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSON(sec_id, symbol, (String) map.get("exchange"), (BigDecimal) map.get("price"), tid);
        jsonString = ",JSON_DETAILS: " + json;
        }
      
      Log.getLogWriter().info(database + "updating trade.securities with SYMBOL:" + symbol +  jsonString + " where SEC_ID:" + sec_id + query);
      stmt.setString(i++, symbol);
      
      if ( ! isDerbyConn && SQLTest.hasJSON) {
        Clob jsonClob = stmt.getConnection().createClob();
        jsonClob.setString(1, json);
        stmt.setClob(i++, jsonClob);
      }
      
      stmt.setInt(i++, sec_id);
    
      successStmt=" rows in trade.securities with SYMBOL:" + symbol +  jsonString + " where SEC_ID:" + sec_id + query;
      //stmt.executeUpdate();
      break;
    case 6:
      
      if ( SQLTest.hasJSON & !SQLHelper.isDerbyConn(stmt.getConnection())) {
        json = getJSON(sec_id, (String) map.get("symbol"), exchange, (BigDecimal) map.get("price"), tid);
        jsonString = ",JSON_DETAILS: " + json;
        }
      
      Log.getLogWriter().info(database + "updating trade.securities with EXCHANGE:" + exchange +  jsonString + " where SEC_ID:" + sec_id +  query);
      stmt.setString(i++, exchange);
      
      if ( ! isDerbyConn && SQLTest.hasJSON) {
        Clob jsonClob = stmt.getConnection().createClob();
        jsonClob.setString(1, json);
        stmt.setClob(i++, jsonClob);
      }
      
      stmt.setInt(i++, sec_id);
      successStmt=" rows in trade.securities with EXCHANGE:" + exchange +  jsonString + " where SEC_ID:" + sec_id +  query;
      //stmt.executeUpdate();
      break;
    default:
     throw new TestException ("Wrong update sql string here");
    }
    int rowCount = stmt.executeUpdate();
    Log.getLogWriter().info(database + "updated " + rowCount + successStmt);
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
  
  //will retry 
  public static ResultSet getQuery(Connection conn, int whichQuery, int sec_id, String symbol, 
      BigDecimal price, String exchange, int tid) {
    boolean[] success = new boolean[1];
    ResultSet rs = null;
    try {
      rs = getQuery(conn, whichQuery, sec_id, symbol, price, exchange, tid, success);
      int count = 0;
      while (!success[0]) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
          return null; 
        }
        count++;   
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        rs = getQuery(conn, whichQuery, sec_id, symbol, price, exchange, tid, success);
      } //retry 
    } catch (SQLException se) {
      if (!SQLHelper.isAlterTableException(conn, se)) SQLHelper.handleSQLException(se);
      //allow alter table related exceptions.
    }
    return rs;
  }
  
  protected static ResultSet query (Connection conn, int whichQuery, int sec_id, String symbol, 
      BigDecimal price, String exchange, int tid) throws SQLException {
    boolean[] success = new boolean[1];
    ResultSet rs = getQuery(conn, whichQuery, sec_id, symbol, price, exchange, tid, success);
    int count = 0;
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
        return null; 
      }
      count++;   
      MasterController.sleepForMs(rand.nextInt(retrySleepMs));
      rs = getQuery(conn, whichQuery, sec_id, symbol, price, exchange, tid, success);
    } //retry 
    return rs;
  }
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, int sec_id, String symbol, 
      BigDecimal price, String exchange, int tid, boolean[] success) throws SQLException {
    BigDecimal price1 = null;
    //used for query price range
    if (price !=null) price1=  price.add(new BigDecimal(rangePrice));
    return getQuery(conn, whichQuery, sec_id, symbol, price, price1, exchange, tid, success);
  }
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, int sec_id, String symbol, 
      BigDecimal price, BigDecimal price1, String exchange, int tid, boolean[] success) throws SQLException {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;
    String txId =  SQLDistTxTest.curTxId.get() == null ? "" : "TXID:" + (Integer)SQLDistTxTest.curTxId.get() + " ";
    String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - " + txId;
    String query = " QUERY: " +  ( SQLTest.hasJSON && ! SQLHelper.isDerbyConn(conn) ? selectJSON[whichQuery] : select[whichQuery] );
    //Log.getLogWriter().info("data will be used in the query sec_id:" +sec_id + " symbol: " + symbol 
    //    + " exchang: " + exchange + " first price: " + price + " second price: " + price1 );     
    try {
      stmt = conn.prepareStatement(select[whichQuery]);
      /*
      stmt = getStmt(conn, select[whichQuery]);  
      if (setCriticalHeap && stmt == null) {
        return null; //prepare stmt may fail due to XCL54 now
      }
      */
      
      switch (whichQuery){
      case 0:
        //"select * from trade.securities where tid = ?"
        Log.getLogWriter().info(database + "querying trade.securities with TID:" +tid + query);
        stmt.setInt(1, tid);
        break;
      case 1: 
        //"select sec_id, price from trade.securities where tid=? and symbol >?"
        Log.getLogWriter().info(database +  "querying trade.securities with SYMBOL:" + symbol + ",TID:" + tid + query );   
        stmt.setInt(1, tid);
        stmt.setString(2, symbol); 
        break;
      case 2:
        //"select price, symbol, exchange from trade.securities where (price<? or price >=?) and tid =?"
        Log.getLogWriter().info(database + "querying trade.securities with 1_PRICE:" + price 
            + ",2_PRICE:" + price1 + ",TID:" +tid + query);   
        stmt.setBigDecimal(1, price);
        stmt.setBigDecimal(2, price1);
        stmt.setInt(3, tid);
        break;
      case 3:
        //"select sec_id, symbol, price, exchange from trade.securities  where (price >=? and price<?) and exchagne =? and tid =?"
        Log.getLogWriter().info(database + "querying trade.securities with EXCHANGE:" + exchange + ",1_PRICE:" + price 
            + ",2_PRICE:" + price1 + ",TID:" +tid + query);   
        stmt.setBigDecimal(1, price);
        stmt.setBigDecimal(2, price1);
        stmt.setString(3, exchange);
        stmt.setInt(4, tid);
        break;
      case 4:
        //"select * from trade.securities where sec_id = ?"
        Log.getLogWriter().info(database + "querying trade.securities with SEC_ID:" +sec_id + query );     
        stmt.setInt(1, sec_id);
        break;
      case 5: 
        //"select sec_id, price from trade.securities where symbol >?"
        Log.getLogWriter().info(database + "querying trade.securities with SYMBOL:" + symbol + query);     
        stmt.setString(1, symbol); 
        break;
      case 6:
        //"select price, symbol, exchange from trade.securities where (price<? or price >=?)"
        Log.getLogWriter().info(database + "querying trade.securities with 1_PRICE:" + price 
            + ",2_PRICE:" + price1 + query );     
        stmt.setBigDecimal(1, price);
        stmt.setBigDecimal(2, price1);
        break;
      case 7:
        //"select sec_id, symbol, price, exchange from trade.securities  where (price >=? and price<?) and exchagne =? "
        Log.getLogWriter().info(database + "querying trade.securities with EXCHANGE:" + exchange 
            + ",1_PRICE:" + price + ",2_PRICE:" + price1 + query);     
        stmt.setBigDecimal(1, price);
        stmt.setBigDecimal(2, price1);
        stmt.setString(3, exchange);
        break;
      default:
        throw new TestException("incorrect select statement, should not happen");
      }
      rs = stmt.executeQuery();
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(conn, se)) success[0] = false; //handle lock could not acquire or deadlock
      else if (!SQLHelper.checkGFXDException(conn, se)) success[0] = false; //hand X0Z01 and #41471
      else throw se;
    }
    return rs;
  }

  @SuppressWarnings("unchecked")
  protected String getJSON(int sec_id , String symbol, String exchange , BigDecimal price , int tid){
    JSONObject json = new JSONObject();
    
    if ( SQLTest.hasJSON ) 
    {       
       json.put("sec_id", sec_id);
       json.put("symbol", symbol);
       json.put("exchange", exchange);
       json.put("price", price);
       json.put("tid" , tid);    
       return json.toJSONString();
    }
    
    else return null;
  }
  
  
  
protected  Map<String, Object> getCurrentRowOfUpdate(Connection conn , int sec_id){
    
    HashMap<String,Object>  map = new HashMap<String, Object>();
    
    String query = "select symbol, price, exchange, tid from trade.securities where sec_id = " + sec_id;
    try{
    ResultSet rs = conn.createStatement().executeQuery(query);
    
    if ( rs.next() ) {
      map.put("sec_id", sec_id);
      map.put("symbol", rs.getString(1));
      map.put("price", rs.getBigDecimal(2));
      map.put("exchange", rs.getString(3));
      map.put("tid", rs.getInt(4));
    }  else {
      map.put("sec_id", sec_id);
      map.put("symbol", " ");
      map.put("price", new BigDecimal(0));
      map.put("exchange", " ");
      map.put("tid", 0);
    }
    
    }catch (SQLException se){
      throw new TestException(TestHelper.getStackTrace(se));
    }
    
    return map;
  }
}

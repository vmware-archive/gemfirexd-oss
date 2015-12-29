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
package sql.dmlStatements;

import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLTest;
import sql.security.SQLSecurityTest;
import sql.sqlutil.ResultSetHelper;
import sql.sqlutil.UDTPrice;
import util.TestException;

public class TradeCompaniesDMLStmt extends AbstractDMLStmt {
  /*
   * trade.companies table fields
   *   String symbol;
   *   String exchange;
   *   short companytype
   *   byte uid CHAR FOR BIT DATA
   *   UUID uuid (UUID)
   *   String companyname char (100)
   *   Clob companyinfo
   *   String note (LONG VARCHAR)
   *   UDTPrice histPrice (UDT price)
   *   long asset (bigint)
   *   byte logo VARCHAR FOR BIT DATA
   *   int tid; //for update or delete unique records to the thread
   */
  protected static String insert = "insert into trade.companies (symbol, exchange, companytype," +
  " uid, uuid, companyname, companyinfo, note, histprice, asset, logo, tid) values (?,?,?,?,?,?,?,?,?,?,?,?)";
  protected static String put = "put into trade.companies (symbol, exchange, companytype," +
  " uid, uuid, companyname, companyinfo, note, histprice, asset, logo, tid) values (?,?,?,?,?,?,?,?,?,?,?,?)";

  protected static boolean isTicket46907Fixed = false;
  protected static boolean isTicket46933Fixed = true;
  protected static boolean isTicket46980Fixed = TestConfig.tab().booleanAt(sql.SQLPrms.ticket46980fixed, true);
  protected static boolean isTicket46981Fixed = false;
  protected static boolean isTicket47013Fixed = false;
  protected static boolean isTicket47018Fixed = true;
  public static final boolean reproduce46886 = ResultSetHelper.reproduce46886;
  
  protected static String[] select = {
    "select * from trade.companies where tid = ? and symbol like ?",
    "select histprice, uid, symbol, companytype, exchange, companyname from trade.companies where exchange = ? and symbol = ? ", //may need to be unique
    "select histprice, companyname, exchange, note, symbol, uid from trade.companies where uid = ? ", 
    "select histprice, uid, exchange, asset, companyinfo, symbol, companyname from trade.companies where companyName= ? and tid = ? ",     
    "select histprice, uuid, " + 
    (RemoteTestModule.getMyVmid()%2 ==0 ? "asset" : "char(asset)") + 
    ", symbol, exchange, companyname from trade.companies where (trade.getLowPrice(histPrice) <=? or trade.getHighPrice(histPrice)>=?) and tid = ? ",     
    "select " + (isTicket47018Fixed? "histprice, uuid," :"" ) + " asset, symbol, logo, exchange, companyname from trade.companies where " 
    + (isTicket47018Fixed ? "trade.getLowPrice(histPrice) <=? and " : " " ) + "companyname is null and tid = ? ",   
    "select histprice, uuid, companyinfo, companyname, asset, symbol, exchange from trade.companies where trade.getLowPrice(histPrice) <=? and tid = ? order by trade.getHighPrice(histPrice) ", // OFFSET 3 ROWS FETCH NEXT 2 ROWS ONLY ", 
    "select histprice, uid, asset, symbol, logo, exchange, companyname from trade.companies where trade.getLowPrice(histPrice) >=? and companytype = ? and tid = ? ",  
    "select symbol, uid, logo, exchange, companyname from trade.companies where companyinfo is null and tid = ? " +
    (RemoteTestModule.getMyVmid()%2 ==0 ? "union " : "intersect ") + 
    "select symbol, uid, logo, exchange, companyname from trade.companies where companyName is not null and companytype <=? and tid = ? ",
    
    "select symbol, uid, logo, exchange, companyname, uuid, histprice from trade.companies where companyinfo is null and tid = ? " +
    (RemoteTestModule.getMyVmid()%2 ==0 ? "intersect all " : "union all ") + 
    "select symbol, uid, logo, exchange, companyname, uuid, histprice from trade.companies where companyName like '%a%' and trade.getHighPrice(histPrice) >=? and tid = ? ",
    
    "select count(symbol) as symbolcount, exchange from trade.securities where tid = ? and symbol like '%b%' group by exchange " +
    "union all " +
    "select count(symbol) as symbolcount, exchange from trade.companies where tid = ? group by exchange order by symbolcount ",
    
    "select count(symbol) as symbolcount, exchange from trade.securities where tid = ? and symbol like '%b%' group by exchange " +
    "union all " +
    "select count(symbol) as symbolcount, exchange from trade.companies where tid = ? group by exchange ",
    
    (isTicket46907Fixed ? 
    "select symbol, exchange, trade.getHighPrice(histPrice) as highPrice, 'qualified' = " +
    "case when trade.getHighPrice(histPrice) > ? then 'qualified' " +
    "else 'does not qualify' " +
    "end as QualifiedRecordHigh " +
    "from trade.companies where tid = ? " 
    :
    "select symbol, exchange, trade.getHighPrice(histPrice) as highPrice, " +
    "case when trade.getHighPrice(histPrice) > ? then 'qualified' " +
    "else 'does not qualify' " +
    "end as QualifiedRecordHigh " +
    "from trade.companies where tid = ? " ),
    
    "select coalesce (trim('a' from trim(companyName)), char(asset), note) from trade.companies where tid = ? and companyType >= ? order by companyName, asset",
    "select * from trade.companies where tid = ?",
    
    "select symbol, " + (reproduce46886 ? "companytype, " : "" ) + "asset from trade.companies where trade.getLowPrice(histPrice) <=? and companyname is null and tid = ? ", 
  };
  
  protected static String[] update = { 
    "update trade.companies set companytype = ?, companyName = ? where symbol =? and exchange = ? ",
    "update trade.companies set companytype = ? where symbol =? and exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse') ",
    "update trade.companies set uuid = ?, uid = ? where symbol >=? and tid = ?",
    "update trade.companies set companyinfo = ?  where symbol =? and exchange = ? ",
    "update trade.companies set note = ?  where trade.getHighPrice(histPrice) <=? and companytype =? and tid=? and companyname like '%=_%' escape '='",
    "update trade.companies set companytype = ? where symbol =? and companytype in (?, ?) and tid = ?",
    "update trade.companies set histprice = ? where tid = ? and asset >= (select avg(asset) from trade.companies where tid = ?)",
    "update trade.companies set histprice = ?, logo = ? where tid = ? and asset >= ? and companytype <= ? and symbol like ?",
  }; 
  
  protected static String[] delete = {                           
    "delete from trade.companies where symbol = ? and exchange = ?", 
    "delete from trade.companies where companyName = ? and tid=?",
    "delete from trade.companies where companyType IN (?, ?) and trade.getHighPrice(histPrice) <=? and tid=?",
    "delete from trade.companies where tid=? and symbol < ? and  trade.getHighPrice(histPrice) >=? and asset <= ? and companyType = ?",
    "delete from trade.companies where tid=? and trade.getLowPrice(histPrice) <=? and note like ? and companyType = ?",
  };
  
  protected static boolean isSingleSitePublisher = TestConfig.tab().
  booleanAt(sql.wan.SQLWanPrms.isSingleSitePublisher, true);
  protected static int maxNumOfTries = 1;

  static int minNameLength =10;
  static int maxNameLength = 90;
  static int maxLongVarchar = 32700;
  static BigDecimal defaultPrice = new BigDecimal("20");
  static int uidLength = 16;
  protected static ArrayList<String> partitionKeys = null;
  protected static ConcurrentHashMap<String, Integer> verifyRowCount = new ConcurrentHashMap<String, Integer>();
  protected static boolean isTicket46898Fixed = false;
  protected static boolean isTicket46899Fixed = false;
  
  @Override
  public void delete(Connection dConn, Connection gConn) {
    int whichDelete = rand.nextInt(delete.length);
    if (SQLTest.syncHAForOfflineTest && whichDelete != 0) whichDelete = 0; 
    //avoid #39605 see #49611
    
    
    int size = 1; 
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    short[] companyType = new short[size];
    UUID[] uid = new UUID[size];
    String[] companyName = new String[size];
    String[] note = new String[size]; 
    UDTPrice[] price = new UDTPrice[size];
    long[] asset = new long[size];
    byte[][] logo = new byte[100][size];
    UUID uid2 = UUID.randomUUID();
    List<SQLException> exceptionList = new ArrayList<SQLException>(); //for compare exceptions got from two sources
    
    boolean getData = getDataFromQuery(gConn, symbol, exchange, companyType, uid, companyName, note, price, asset, logo, size);
    if (!getData) {
      Log.getLogWriter().info("no row available to be used in the query");
      return;
    }
    
    int tid = getMyTid();
    
    if (setCriticalHeap) resetCanceledFlag();
       
    //for verification both connections are needed
    if (dConn != null) {
      boolean success = deleteFromDerbyTable(dConn, whichDelete, symbol[0], exchange[0], companyType[0], uid[0], uid2, companyName[0], 
          note[0], price[0], asset[0], logo[0], tid, exceptionList);
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the delete op in derby, will abort this operation in derby");
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return;
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        exceptionList.clear();
        success = deleteFromDerbyTable(dConn, whichDelete, symbol[0], exchange[0], companyType[0], uid[0], uid2, companyName[0], 
            note[0], price[0], asset[0], logo[0], tid, exceptionList); //retry
      }
      deleteFromGfxdTable(gConn, whichDelete, symbol[0], exchange[0], companyType[0], uid[0], uid2, companyName[0], 
          note[0], price[0], asset[0], logo[0], tid, exceptionList);
      SQLHelper.handleMissedSQLException(exceptionList);
    } 
    else {
      deleteFromGfxdTable(gConn, whichDelete, symbol[0], exchange[0], companyType[0], uid[0], uid2, companyName[0], 
          note[0], price[0], asset[0], logo[0], tid); //w/o verification
    }

  }

  @Override
  public void insert(Connection dConn, Connection gConn, int size) {
    //not to be used
  }
  
  public void insert(Connection dConn, Connection gConn, int size, String[] symbol, String[] exchange) {
    short[] companyType = new short[size];
    UUID[] uid = new UUID[size];
    String[] companyName = new String[size];
    Clob[] companyInfo = getClob(size);
    String[] note = new String[size]; 
    UDTPrice[] price = new UDTPrice[size];
    long[] asset = new long[size];
    byte[][] logo = new byte[100][size];
    List<SQLException> exceptionList = new ArrayList<SQLException>();

    getDataForInsert(companyType, uid, companyName, note, price, asset, logo, size); //get the data

    if (setCriticalHeap) resetCanceledFlag();
    
    int count = 0;
    if (dConn != null) {
      boolean success = insertToDerbyTable(dConn, symbol, exchange, companyType, uid, companyName, companyInfo,
          note, price, asset, logo, size, exceptionList);  //insert to derby table  
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
        success = insertToDerbyTable(dConn, symbol, exchange, companyType, uid, companyName, companyInfo,
            note, price, asset, logo, size, exceptionList); 
        count++;
      }
      try {
        insertToGfxdTable(gConn, symbol, exchange, companyType, uid, companyName, companyInfo,
          note, price, asset, logo, size, exceptionList); 
      } catch (TestException te) {
        if (te.getMessage().contains("Execute SQL statement failed with: 23505")
            && isHATest && SQLTest.isEdge) {
          //checkTicket49605(dConn, gConn, "companies");
          try {
            checkTicket49605(dConn, gConn, "companies", -1, -1, symbol[0], exchange[0]);
          } catch (TestException e) {
            Log.getLogWriter().info("insert failed due to #49605 ", e);
            //deleteRow(dConn, gConn, "companies", -1, -1, symbol[0], exchange[0]);
            Log.getLogWriter().info("retry this using put to work around #49605");
            insertToGfxdTable(gConn, symbol, exchange, companyType, uid, companyName, companyInfo,
                note, price, asset, logo, size, exceptionList, true);
            
            checkTicket49605(dConn, gConn, "companies", -1, -1, symbol[0], exchange[0]);
          }
        } else throw te;
      }
      SQLHelper.handleMissedSQLException(exceptionList);
    } 
    else {
      insertToGfxdTable(gConn, symbol, exchange, companyType, uid, companyName, companyInfo,
          note, price, asset, logo, size);
    } //no verification
  
  }

  @Override
  public void query(Connection dConn, Connection gConn) { 
    int whichQuery = rand.nextInt(select.length); 
    int size = 1;

    String[] symbol = new String[size];
    String[] exchange = new String[size];
    short[] companyType = new short[size];
    UUID[] uid = new UUID[size];
    String[] companyName = new String[size];
    String[] note = new String[size]; 
    UDTPrice[] price = new UDTPrice[size];
    long[] asset = new long[size];
    byte[][] logo = new byte[100][size];
    UUID uid2 = UUID.randomUUID();
    
    boolean getData = getDataFromQuery(gConn, symbol, exchange, companyType, uid, companyName, note, price, asset, logo, size);
    if (!getData) {
      Log.getLogWriter().info("no row available to be used in the query");
      return;
    }
    
    int rate = 10;
    if (rand.nextInt(rate) == 0) {
      companyName[0] += "       " ;
      Log.getLogWriter().info("added white space pad for companyName" + ".");
    } else if (rand.nextInt(rate) == 1 && companyName[0] != null) {
      companyName[0] = companyName[0].trim();
      Log.getLogWriter().info("trim the white space for companyName " + companyName[0] + ".");
    }
    
    int tid = getMyTid();
    ResultSet discRS = null;
    ResultSet gfxdRS = null;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();

    if (dConn!=null) { 
      try {
        discRS = query(dConn, whichQuery, symbol[0], exchange[0], companyType[0], uid[0], uid2, companyName[0], 
            note[0], price[0], asset[0], logo[0], tid);
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
        gfxdRS = query (gConn, whichQuery, symbol[0], exchange[0], companyType[0], uid[0], uid2, companyName[0], 
            note[0], price[0], asset[0], logo[0], tid); 
        if (gfxdRS == null) {
          if (isHATest || !isTicket46898Fixed || !isTicket46899Fixed) {
            Log.getLogWriter().info("not able to get GFXD result set");
            return;
          } else if (setCriticalHeap) {
            Log.getLogWriter().info("got XCL54 and does not get query result");
            return; //prepare stmt may fail due to XCL54 now
          }
          else     
            throw new TestException("Not able to get gfxd result set after retry");
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptionList);
      }
      SQLHelper.handleMissedSQLException(exceptionList);
      if (discRS == null || gfxdRS == null) return;
      
      boolean success = ResultSetHelper.compareResultSets(discRS, gfxdRS); 
      if (!success) {
        Log.getLogWriter().info("Not able to compare results, continuing test");
      } //not able to compare results due to derby server error
         
      SQLHelper.closeResultSet(gfxdRS, gConn);
    }// we can verify resultSet
    else {
      try {
        gfxdRS =  query (gConn, whichQuery, symbol[0], exchange[0], companyType[0], uid[0], uid2, companyName[0], 
            note[0], price[0], asset[0], logo[0], tid);  
      } catch (SQLException se) {
        if (se.getSQLState().equals("0A000") && (whichQuery == 10 || whichQuery == 11)) {
          Log.getLogWriter().info("Got expected no support for group by and order by for union query");
          return;
        } else if (se.getSQLState().equals("X0X67") && select[whichQuery].contains("intersect all select")) {
          Log.getLogWriter().info("Got expected expection for union query needs comparison on UDT");
          return;
        }
        else if (se.getSQLState().equals("42502") && SQLTest.testSecurity) {
          Log.getLogWriter().info("Got expected no SELECT permission, continuing test");
          return;
        } else if (alterTableDropColumn && se.getSQLState().equals("42X04")) {
          Log.getLogWriter().info("Got expected column not found exception, continuing test");
          return;
        } else SQLHelper.handleSQLException(se);
      }
      
      if (gfxdRS != null)
        ResultSetHelper.asList(gfxdRS, false);  
      else if (isHATest)
        Log.getLogWriter().info("could not get gfxd query results after retry due to HA");
      else if (setCriticalHeap)
        Log.getLogWriter().info("could not get gfxd query results after retry due to XCL54");
      else if (!isTicket47013Fixed) 
        Log.getLogWriter().info("could not get gfxd query results after retry due to ticket#47013");
      else
        throw new TestException ("gfxd query returns null and not a HA test"); 
      
      SQLHelper.closeResultSet(gfxdRS, gConn);
    }
    SQLHelper.closeResultSet(gfxdRS, gConn);
  }

  @Override
  public void update(Connection dConn, Connection gConn, int size) {   
    int[] whichUpdate = new int[size]; 
    
    //adding batch update in the test
    for (int i =0; i<size; i++) {
      whichUpdate[i] = rand.nextInt(update.length);
    }
    
    boolean useBatch = false;
    int maxsize = 10; 
    if (size == 1 && whichUpdate[0] == 0 && rand.nextBoolean()
        && !alterTableDropColumn && !testSecurity && !setCriticalHeap) {
      size = rand.nextInt(maxsize) + 1;
      useBatch = true;
    }
  
    String[] symbol = new String[size];
    String[] exchange = new String[size];
    short[] companyType = new short[size];
    UUID[] uid = new UUID[size];
    String[] companyName = new String[size];
    String[] note = new String[size]; 
    UDTPrice[] price = new UDTPrice[size];
    long[] asset = new long[size];
    byte[][] logo = new byte[100][size];
    UUID[] uid2 = new UUID[size];
    
    for (int i =0; i<size; i++) {
      uid2[i] = UUID.randomUUID();
    }
    
    boolean getData = getDataFromQuery(gConn, symbol, exchange, companyType, uid, companyName, note, price, asset, logo, size);
    if (!getData) {
      Log.getLogWriter().info("no row available to be used in the query");
      return;
    }
    List<SQLException> exceptionList = new ArrayList<SQLException>();
    
    short[] newcompanyType = new short[size];
    UUID[] newuid = new UUID[size];
    String[] newcompanyName = new String[size];
    Clob[] newcompanyInfo = getClob(size);
    String[] newnote = new String[size]; 
    UDTPrice[] newprice = new UDTPrice[size];
    long[] newasset = new long[size];
    byte[][] newlogo = new byte[100][size];

    getDataForInsert(newcompanyType, newuid, newcompanyName, newnote, newprice, 
        newasset, newlogo, size); 

    if (setCriticalHeap) resetCanceledFlag();
    
    if (dConn != null) { 
      boolean success = false;
      if (useBatch) success = updateDerbyTableUsingBatch(dConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
          note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
          newnote, newprice, newasset, newlogo, size, exceptionList);  //update derby table using batch update  
      else success = updateDerbyTable(dConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
          note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
          newnote, newprice, newasset, newlogo, size, exceptionList);  //update derby table  
      int count = 0;
      while (!success) {
        if (count >= maxNumOfTries) {
          Log.getLogWriter().info("Could not finish the update op in derby, will abort this operation in derby");
          rollback(dConn); 
          if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true) break; 
          //expect gfxd fail with the same reason due to alter table
          else return; 
        }
        MasterController.sleepForMs(rand.nextInt(retrySleepMs));
        count++; 
        rollback(dConn);
        exceptionList.clear();
        if (useBatch) success = updateDerbyTableUsingBatch(dConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
            note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
            newnote, newprice, newasset, newlogo, size, exceptionList);  //update derby table using batch update  
        else success = updateDerbyTable(dConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
            note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
            newnote, newprice, newasset, newlogo, size, exceptionList);  //update derby table  
      } //retry only once.
      if (useBatch) updateGfxdTableUsingBatch(gConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
          note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
          newnote, newprice, newasset, newlogo, size, exceptionList); 
      else updateGfxdTable(gConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
          note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
          newnote, newprice, newasset, newlogo, size, exceptionList); 
      
      SQLHelper.handleMissedSQLException(exceptionList);
    }
    else { 
      if (useBatch) updateGfxdTableUsingBatch(gConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
          note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
          newnote, newprice, newasset, newlogo, size);
      else updateGfxdTable(gConn, whichUpdate, symbol, exchange, companyType, uid, uid2, companyName, 
            note, price, asset, logo, newcompanyType, newuid, newcompanyName, newcompanyInfo,
            newnote, newprice, newasset, newlogo, size);
    } //no verification

  }
  
  protected void getDataForInsert(short[] type, UUID[] uid, String[] companyName,
      String[] note, UDTPrice[] price, long[] asset, byte[][] logo, int size) {
    int maxLogoLength = 100;
    for (int i = 0 ; i <size ; i++) {
      type[i] = getCompanyType();
      uid[i] =  UUID.randomUUID();;
      companyName[i] = rand.nextInt(10) == 1 ?  null : getString(minNameLength, maxNameLength);
      note[i] = getString(0, maxLongVarchar);
      price[i] = getRandomPrice();
      asset[i] = rand.nextLong();
      logo[i] = getByteArray(maxLogoLength);
      
    }
  }
  
  protected void getNonRepeatRandomNums(ArrayList<Integer> list, int size, int numOfRows) {
    if (numOfRows<=size) {
      for (int i=1; i<=numOfRows; i++) {
        list.add(i);
      }
      return;
    }
    
    while (list.size() < size) {
      int num = rand.nextInt(numOfRows)+1;
      if (!list.contains(num)) list.add(num);
    }
  }
  
  protected boolean getDataFromQuery(Connection gConn, String[] symbol, String[] exchange,
      short[] type, UUID[] uid, String[] companyName, String[] note, UDTPrice[] price, 
      long[] asset, byte[][] logo, int size) {
    //size = 1;
    int tid = testUniqueKeys ? getMyTid() : getRandomTid();
    String sql = "select * from trade.companies where companyname is not null and tid = " + tid;
    Log.getLogWriter().info(sql);
    try {
      ResultSet rs = gConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
          ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
      int numOfRows = 0;
      while (rs.next()) {
        ++numOfRows;
      }
      
      if (numOfRows == 0) return false;
      
      ArrayList<Integer>list = new ArrayList<Integer>();      
      
      getNonRepeatRandomNums(list, size, numOfRows);
      if (numOfRows<size) size = numOfRows;
            
      for (int i = 0 ; i <size ; i++) {
        //int whichRow = rand.nextInt(numOfRows)+1;
        //avoiding #48245
        int whichRow = list.get(i);
        
        rs.beforeFirst();
        for (int k=0; k<whichRow; k++) {
          rs.next();        
        }
        
        ResultSetMetaData rsmd = rs.getMetaData();
         
        if (alterTableDropColumn) {
          for (int j=0; j < rsmd.getColumnCount(); j++) {
            int column = j+1 ;
            if (rsmd.getColumnName(column).equalsIgnoreCase("SYMBOL")) {
              symbol[i] = rs.getString("SYMBOL"); //
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("exchange")){
              exchange[i] = rs.getString("exchange");
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("companytype")){
              type[i] = rs.getShort("companytype");
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("UUID")){
              uid[i] =  (UUID) rs.getObject("UUID");
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("companyName")){
              companyName[i] = rs.getString("companyName");
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("note")){
              note[i] = rs.getString("note");
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("histprice")){
              price[i] = (UDTPrice) rs.getObject("histprice");
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("asset")){
              asset[i] = rs.getLong("asset");
            } else if (rsmd.getColumnName(column).equalsIgnoreCase("LOGO")){
              logo[i] = rs.getBytes("LOGO"); 
            }
            Log.getLogWriter().info("symbol: " + symbol[i] + 
                " exchange: " + exchange[i] + "companytype: " + type[i] + 
                " uid: " + (uid[i] == null? "null" : ResultSetHelper.convertByteArrayToString(getUidBytes(uid[i]))) +
                " uuid: " + (uid[i] == null? "null" :uid[i]) + 
                " companyName: " + (companyName[i] == null? "null" :companyName[i]) + 
                " histprice:" + (price[i] == null? "null" :price[i]));
          }          
        } else {
          symbol[i] = rs.getString("SYMBOL");
          exchange[i] = rs.getString("exchange");
          type[i] = rs.getShort("companytype");
          uid[i] =  (UUID) rs.getObject("UUID");
          companyName[i] = rs.getString("companyName");
          note[i] = rs.getString("note");
          price[i] = (UDTPrice) rs.getObject("histprice");
          asset[i] = rs.getLong("asset");
          logo[i] = rs.getBytes("LOGO");    
          Log.getLogWriter().info("symbol: " + symbol[i] + 
              " exchange: " + exchange[i] + "companytype: " + type[i] + 
              " uid: " + ResultSetHelper.convertByteArrayToString(getUidBytes(uid[i])) +
              " uuid: " + uid[i] + 
              " companyName: " + companyName[i] + " histprice:" + price[i]);    
        }        
      }
      rs.close();
    } catch (SQLException se) {
      if (!SQLHelper.checkGFXDException(gConn, se)) return false;
      else if (alterTableDropColumn && se.getSQLState().equals("42X04")) {
        //if companyname is dropped
        Log.getLogWriter().info("Got expected column not found exception, continuing test");
        return false;
      }
      else SQLHelper.handleSQLException(se);
    } 
    return true;
  }
  
  protected short getCompanyType() {
    return digits[rand.nextInt(digits.length)];
  }
  
  protected UDTPrice getRandomPrice() {
    BigDecimal low = new BigDecimal (Double.toString((rand.nextInt(3000)+1) * .01)).add(new BigDecimal("20"));
    BigDecimal high = new BigDecimal("10").add(low);
    return new UDTPrice(low, high);
  }
  
  protected String getString(int minLength, int maxLength) {
    int length = rand.nextInt(maxLength-minLength+1) + minLength;
    return getRandPrintableVarChar(length);
  }
  
  protected String getRandPrintableVarChar(int length) {
    if (length == 0) {
      return "";
    }
    
    int sp = ' ';
    int tilde = '~';
    char[] charArray = new char[length];
    for (int j = 0; j<length; j++) {
      charArray[j] = (char) (rand.nextInt(tilde-sp) + sp); 
    }
    return new String(charArray);
  }
  
  protected BigInteger getBigInt(int minLength, int maxLength) {
    int length = rand.nextInt(maxLength-minLength+1) + minLength;
    return getBigInt(length);
  }
  
  protected BigInteger getBigInt(int length) {    
    StringBuilder str = new StringBuilder();
    for (int j = 0; j<length; j++) {
      str.append((digits[rand.nextInt(digits.length)])); 
    }
    return new BigInteger(str.toString());
  }
  
  protected boolean insertToDerbyTable(Connection conn, String[] symbol, 
      String[] exchange, short[] companyType, UUID[] uid, 
      String[] companyName, Clob[] companyInfo, String[] note, UDTPrice[] price, 
      long[] asset, byte[][] logo,  int size, 
      List<SQLException> exceptions)  {
    PreparedStatement stmt = getStmt(conn, insert);
    if (stmt == null) return false;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      try {
        verifyRowCount.put(tid+"_insert"+i, 0);
        count = insertToTable(stmt, symbol[i], exchange[i], companyType[i], uid[i], 
            companyName[i], companyInfo[i], note[i], price[i], asset[i], logo[i], tid);
        verifyRowCount.put(tid+"_insert"+i, new Integer(count));
        
      }  catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se))
          return false;
        else SQLHelper.handleDerbySQLException(se, exceptions);
      } 
    }
    return true;
  }
  
  protected void insertToGfxdTable(Connection conn, String[] symbol, 
      String[] exchange, short[] companyType, UUID[] uid, 
      String[] companyName, Clob[] companyInfo, String[] note, UDTPrice[] price, 
      long[] asset, byte[][] logo,  int size, List<SQLException> exceptions)  {
    
    PreparedStatement stmt = getStmt(conn, insert);
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
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      try {
        count = insertToTable(stmt, symbol[i], exchange[i], companyType[i], uid[i], 
            companyName[i], companyInfo[i], note[i], price[i], asset[i], logo[i], tid);
        if (count != ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue()) {
          String str ="Gfxd insert has different row count from that of derby " +
            "derby inserted " + ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue() +
            " but gfxd inserted " + count;
          if (failAtUpdateCount && !isHATest) throw new TestException (str);
          else Log.getLogWriter().warning(str);
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptions);
      }
    }  
  }
  
  protected void insertToGfxdTable(Connection conn, String[] symbol, 
      String[] exchange, short[] companyType, UUID[] uid, 
      String[] companyName, Clob[] companyInfo, String[] note, UDTPrice[] price, 
      long[] asset, byte[][] logo,  int size, List<SQLException> exceptions, boolean isPut)  {
    
    PreparedStatement stmt = getStmt(conn, isPut ? put  : insert);
    
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
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      try {
        count = insertToTable(stmt, symbol[i], exchange[i], companyType[i], uid[i], 
            companyName[i], companyInfo[i], note[i], price[i], asset[i], logo[i], tid, isPut);
        if (count != ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue()) {
          String str = "Gfxd insert has different row count from that of derby " +
            "derby inserted " + ((Integer)verifyRowCount.get(tid+"_insert"+i)).intValue() +
            " but gfxd inserted " + count;
          if (failAtUpdateCount && !isHATest) throw new TestException (str);
          else Log.getLogWriter().warning(str);
        }
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exceptions);
      }
    }  
  }
  
  protected void insertToGfxdTable(Connection conn, String[] symbol, 
      String[] exchange, short[] companyType, UUID[] uid, 
      String[] companyName, Clob[] companyInfo, String[] note, UDTPrice[] price, 
      long[] asset, byte[][] logo,  int size)  {
    
    PreparedStatement stmt = getStmt(conn, insert);
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      try {
        count = insertToTable(stmt, symbol[i], exchange[i], companyType[i], uid[i], 
            companyName[i], companyInfo[i], note[i], price[i], asset[i], logo[i], tid);
      } catch (SQLException se) {
        if (se.getSQLState().equals("23505"))
          Log.getLogWriter().info("detected primary key constraint violation during insert, continuing test");
        else if (se.getSQLState().equals("23503")) {
          Log.getLogWriter().info("detected foreign key constraint violation during insert, continuing test");
        } //refers unique key in securities table
        else if (se.getSQLState().equals("42500") && testSecurity) 
          Log.getLogWriter().info("Got the expected exception for authorization," +
             " continuing tests");
        else if (alterTableDropColumn && (se.getSQLState().equals("42802")
            || se.getSQLState().equals("42X14"))) {
          Log.getLogWriter().info("Got expected column not found exception in insert, continuing test");
        }
        else
          SQLHelper.handleSQLException(se);
      }
    }  
  }

  //insert a record into the table
  protected int insertToTable(PreparedStatement stmt, String symbol, String exchange, 
      short companyType, UUID uid, String companyName, Clob companyInfo, String note, 
      UDTPrice price, long asset, byte[] logo, int tid) throws SQLException {
    return insertToTable(stmt, symbol, exchange, companyType, uid, companyName, companyInfo, note, price, asset, logo, tid, false);
  }
  //insert a record into the table
  protected int insertToTable(PreparedStatement stmt, String symbol, String exchange, 
      short companyType, UUID uid, String companyName, Clob companyInfo, String note, 
      UDTPrice price, long asset, byte[] logo, int tid, boolean isPut) throws SQLException {
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    
    
    Log.getLogWriter().info(database + (isPut ? "putting" : "inserting") + " into trade.companies with SYMBOL:"+ symbol + 
        ",EXCHANGE:" + exchange + ",COMPANYTYPE:" + companyType +
        ",UID:" + ResultSetHelper.convertByteArrayToString(getUidBytes(uid)) + 
        ",UUID:" + uid + ",COMPANYNAME:" + companyName +
        ",COMPANYINFO:" + (ResultSetHelper.useMD5Checksum && companyInfo != null ? 
        ResultSetHelper.convertClobToChecksum(companyInfo, companyInfo.length()) : 
          getStringFromClob(companyInfo)) +
        ",NOTE:" + (ResultSetHelper.useMD5Checksum && note !=null && note.length() > ResultSetHelper.longVarCharSize ? 
          ResultSetHelper.convertStringToChecksum(note, note.length()) : note) +
        ",HIGHPRICE:" + price + ",ASSET:" + ((getMyTid() % 11 == 0)? null : asset) +
        ",LOGO:" + ResultSetHelper.convertByteArrayToString(logo) + ",TID:" + tid);
    
    
    stmt.setString(1, symbol);
    stmt.setString(2, exchange); 
    stmt.setShort(3, companyType);
    stmt.setBytes(4, getUidBytes(uid));
    stmt.setObject(5, uid);

    stmt.setString(6, companyName);
    if (companyInfo == null)
      stmt.setNull(7, Types.CLOB);
    else
      stmt.setClob(7, companyInfo);
    stmt.setString(8, note);
    stmt.setObject(9, price);
    if (getMyTid() % 11 == 0) 
      stmt.setNull(10, Types.BIGINT);
    else
      stmt.setLong(10, asset);
    stmt.setBytes(11, logo);
    stmt.setInt(12, tid);

    int rowCount = stmt.executeUpdate();
    
    Log.getLogWriter().info(database + (isPut ? "put " : "inserted ") + rowCount + " rows into trade.companies SYMBOL:"+ symbol + 
        ",EXCHANGE:" + exchange + ",COMPANYTYPE:" + companyType +
        ",UID:" + ResultSetHelper.convertByteArrayToString(getUidBytes(uid)) + 
        ",UUID:" + uid + ",COMPANYNAME:" + companyName +
        ",COMPANYINFO:" + (ResultSetHelper.useMD5Checksum && companyInfo != null ? 
        ResultSetHelper.convertClobToChecksum(companyInfo, companyInfo.length()) : 
          getStringFromClob(companyInfo)) +
        ",NOTE:" + (ResultSetHelper.useMD5Checksum && note !=null && note.length() > ResultSetHelper.longVarCharSize ? 
          ResultSetHelper.convertStringToChecksum(note, note.length()) : note) +
        ",HIGHPRICE:" + price + ",ASSET:" + ((getMyTid() % 11 == 0)? null : asset) +
        ",LOGO:" + ResultSetHelper.convertByteArrayToString(logo) + ",TID:" + tid);
    
    SQLWarning warning = stmt.getWarnings(); //test to see there is a warning
    if (warning != null) {
      SQLHelper.printSQLWarning(warning);
    } 
    return rowCount;
  }
  
  public static byte[] getUidBytes(UUID uuid){
    /*
    byte[] bytes = new byte[uidLength];
    rand.nextBytes(bytes);
    return bytes;
    */
    if (uuid == null) return null;
    long[] longArray = new long[2];
    longArray[0] = uuid.getMostSignificantBits();
    longArray[1] = uuid.getLeastSignificantBits();

    byte[] bytes = new byte[uidLength];
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    bb.putLong(longArray[0]);
    bb.putLong(longArray[1]);
    return bytes;
  }

  protected static ResultSet query (Connection conn, int whichQuery, String symbol,
      String exchange, short type, UUID uid, UUID uid2, String companyName,
      String note, UDTPrice price, long asset, byte[] logo, int tid) throws SQLException {
    boolean[] success = new boolean[1];
    ResultSet rs = getQuery(conn, whichQuery, symbol, exchange, type, uid, uid2, companyName, 
        note, price, asset, logo, tid, success);
    int count = 0;
    while (!success[0]) {
      if (count >= maxNumOfTries) {
        Log.getLogWriter().info("Could not get the lock to finisht the op in derby, abort this operation");
        return null; 
      }
      count++;   
      MasterController.sleepForMs(rand.nextInt(retrySleepMs));
      rs = getQuery(conn, whichQuery, symbol, exchange, type, uid, uid2, companyName, 
          note, price, asset, logo, tid, success);
    } //retry 
    return rs;
  }
  
  protected static ResultSet getQuery(Connection conn, int whichQuery, String symbol,
      String exchange, short type, UUID uid, UUID uid2, String companyName,
      String note, UDTPrice price, long asset, byte[] logo, int tid, boolean[] success) throws SQLException {
    PreparedStatement stmt;
    ResultSet rs = null;
    success[0] = true;

    String database = SQLHelper.isDerbyConn(conn)?"Derby - " :"gemfirexd - ";      
    String query = " QUERY: " + select[whichQuery];
    
    try {
      stmt = conn.prepareStatement(select[whichQuery]);
      
      switch (whichQuery){
      case 0:
        //"select * from trade.customers where tid = ? and symbol like ?",
        boolean reproduce48223 = false;
        String str = symbol.length() > 2 ? symbol.substring(0, 2) : symbol;
        if (reproduce48223) {
          if (tid%7 != 0) str += "%";
        } else str += "%";
        Log.getLogWriter().info(database + "querying trade.companies with TID:" +tid + ",SYMBOL:" + str + query);
        stmt.setInt(1, tid);
        stmt.setString(2, str);
        break;
      case 1: 
        //"select histprice, uid, symbol, exchange, companyname from trade.companies where exchange = ? and symbol = ? ",       
        Log.getLogWriter().info(database + "querying trade.companies with SYMBOL:" + symbol + ",EXCHANGE:" + exchange + query);   
        stmt.setString(1, exchange);
        stmt.setString(2, symbol); 
        break;
      case 2:
        //"select histprice, companyname, exchange, note, symbol, uid from trade.companies where uid = ? ", 
        
        Log.getLogWriter().info(database + "querying trade.companies with UID:" + 
            ResultSetHelper.convertByteArrayToString(getUidBytes(uid)) + query);   
        stmt.setBytes(1, getUidBytes(uid));
        break;
      case 3:
        //"select histprice, uid, exchange, asset, symbol, companyname from trade.companies where "select histprice, uid, exchange, asset, symbol, companyname from trade.companies where companyName= ? and tid = ? ",  and tid = ? ",    
        Log.getLogWriter().info(database + "querying trade.companies with COMPANYNAME:" + companyName
            + ",TID:" +tid + query);   
        stmt.setString(1, companyName);
        stmt.setInt(2, tid);
        break;
       
      case 4:
        //"select histprice, uuid, asset, symbol, exchange, companyname from trade.companies where (getLowPrice(histPrice) <=? or getHighPrice(histPrice)>=?) and tid = ? ",     
        Log.getLogWriter().info(database + "querying trade.companies with TID:" + tid + 
            ",LOWPRICE:" + UDTPrice.getLowPrice(price) + ",HIGHPRICE:" + 
            UDTPrice.getHighPrice(price) + query);     
        stmt.setBigDecimal(1, UDTPrice.getLowPrice(price));
        stmt.setBigDecimal(2, UDTPrice.getHighPrice(price));
        stmt.setInt(3, tid);
        break;
        
      case 5: 
        //"select histprice, uuid, asset, symbol, logo, companytype, exchange, companyname from trade.companies where trade.getLowPrice(histPrice) <=? and companyname is null and tid = ? ",  
        Log.getLogWriter().info(database + "querying trade.companies with TID:" + tid + 
            (isTicket47018Fixed? ",LOWPRICE:" + UDTPrice.getLowPrice(price) : "") + query);   
        if (isTicket47018Fixed) {
          stmt.setBigDecimal(1, UDTPrice.getLowPrice(price));
          stmt.setInt(2, tid);
        } else {
          stmt.setInt(1, tid); //avoid #47018 noise in 1.1 release
        }
        break;
      case 6:
        //"select histprice, uuid, companyinfo, asset, symbol, exchange, companyname from trade.companies where trade.getLowPrice(histPrice) <=? and tid = ? order by trade.getHighPrice(histPrice) OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY ", 
        Log.getLogWriter().info(database + "querying trade.companies with TID:" + tid + 
            ",LOWPRICE:" + UDTPrice.getLowPrice(price) + query);    
        stmt.setBigDecimal(1, UDTPrice.getLowPrice(price));
        stmt.setInt(2, tid);
        break;
      case 7:
        //"select histprice, uid, asset, symbol, logo, exchange, companyname from trade.companies where trade.getLowPrice(histPrice) >=? and companytype = ? and tid = ? ",  
        Log.getLogWriter().info(database + "querying trade.companies with COMPANYTYPE:" + type + ",TID:" + tid + 
            ",LOWPRICE:" + UDTPrice.getLowPrice(price) + query);     
        stmt.setBigDecimal(1, UDTPrice.getLowPrice(price));
        stmt.setShort(2, type);
        stmt.setInt(3, tid);
        break;
      case 8:
        //"select symbol, uid, logo, exchange, companyname where companyinfo is null and tid = ? " +
        //(getMyTid()%2 ==0 ? "union " : "union all") + 
        //"select symbol, uid, logo, exchange, companyname where companyName is not null and companytype <=? and tid = ? ",
        Log.getLogWriter().info(database + "querying trade.companies with COMPANYTYPE:" + type + ",TID:" + tid + query);     
        stmt.setInt(1, tid);
        stmt.setShort(2, type);
        stmt.setInt(3, tid);
        break;
      case 9:
        //"select symbol, uid, logo, exchange, companyname, uuid, histprice from trade.companies where companyinfo is null and tid = ? " +
        //"union all " + 
        //"select symbol, uid, logo, exchange, companyname, uuid, histprice from trade.companies where companyName like '%a%' and trade.getHighPrice(histPrice) >=? and tid = ? ",
        Log.getLogWriter().info(database + "querying trade.companies with HIGHPRICE:" + UDTPrice.getHighPrice(price) + ",TID:" + tid + query);    
        stmt.setInt(1, tid);
        stmt.setBigDecimal(2, UDTPrice.getHighPrice(price));
        stmt.setInt(3, tid);
        break;
      case 10:
        Log.getLogWriter().info(database + "querying trade.companies with TID:" + tid + query);     
        stmt.setInt(1, tid);
        stmt.setInt(2, tid);
        if (!isTicket46898Fixed) return null;
        break;  
      case 11:
        Log.getLogWriter().info(database + "querying trade.companies with TID:" + tid + query);     
        stmt.setInt(1, tid);
        stmt.setInt(2, tid);
        if (!isTicket46899Fixed) return null;
        break; 
      case 12:
        Log.getLogWriter().info(database + "querying trade.companies with HIGHPRICE:" + UDTPrice.getHighPrice(price) + ",TID:" + tid + query);     
        stmt.setBigDecimal(1, UDTPrice.getHighPrice(price));
        stmt.setInt(2, tid); 
        //if (isTicket46907Fixed) {
        //  rs = stmt.executeQuery();
        //  Log.getLogWriter().info("result set is " + ResultSetHelper.asList(rs, true));
        //  return rs;
        //}
        if (!isTicket47013Fixed && SQLTest.isEdge) {
          Log.getLogWriter().info("will not execute this query due to #47013");
          return null;
        }
        break;
      case 13:
      //"select coalesce (trim('a' from trim(companyName)), note, char(companyType), char(asset)) from trade.companies where tid = ? and companyType >= ? order by companyName, asset"
        Log.getLogWriter().info(database + "querying trade.companies with COMPANYTYPE:" + type
            + ",TID:" +tid + query);   
        stmt.setShort(2, type);
        stmt.setInt(1, tid);      
        break;  
      case 14:
        //"select * from trade.companies where tid = ?",
          Log.getLogWriter().info(database + "querying trade.companies with TID:" +tid + query);   
          stmt.setInt(1, tid);      
          break;  
         
      case 15: 
        //"select symbol, companytype from trade.companies where trade.getLowPrice(histPrice) <=? and companyname is null and tid = ? ",  
        Log.getLogWriter().info(database + "querying trade.companies with TID:" + tid + 
            ",LOWPRICE:" + UDTPrice.getLowPrice(price) + query);   

        stmt.setBigDecimal(1, UDTPrice.getLowPrice(price));
        stmt.setInt(2, tid);
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

  protected boolean deleteFromDerbyTable(Connection dConn, int whichDelete, 
      String symbol,String exchange, short type, UUID uid, UUID uid2, String companyName,
      String note, UDTPrice price, long asset, byte[] logo, int tid, List<SQLException> exList){    
    int count = -1;
    
    try {
      String deleteSql = delete[whichDelete];
      
      /* query hint does not work in derby for delete
      if (deleteSql.contains("tid")) {
        deleteSql = addQueryHintToDerbySql(delete[whichDelete]);
      }      
      
      Log.getLogWriter().info(deleteSql);
      */
      
      PreparedStatement stmt = getStmt(dConn, deleteSql); 
      
      if (stmt == null) return false;
      else {
        verifyRowCount.put(tid+"_delete_", 0);
        count = deleteFromTable(stmt, symbol, exchange, type, uid, uid2, companyName, 
            note, price, asset, logo, tid, whichDelete);
        verifyRowCount.put(tid+"_delete_", new Integer(count));
        
      } 
    } catch (SQLException se) {
      if (!SQLHelper.checkDerbyException(dConn, se))
        return false;
      else SQLHelper.handleDerbySQLException(se, exList); //handle the exception
    }
    return true;
  }
  
  protected String addQueryHintToDerbySql(String sql) {
    String derbyQueryhint = " --DERBY-PROPERTIES index=trade.indexcompaniestid \n";
    int index = sql.indexOf("where");
    return sql.substring(0, index) + derbyQueryhint + sql.substring(index);
    
  }
  
  //compare whether the exceptions got are same as those from derby
  protected void deleteFromGfxdTable(Connection gConn, int whichDelete, String symbol,
      String exchange, short type, UUID uid, UUID uid2, String companyName,
      String note, UDTPrice price, long asset, byte[] logo, int tid, List<SQLException> exList){
    int count = -1;
    
    
    PreparedStatement stmt = getStmt(gConn, delete[whichDelete]); 
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
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    try {
      
      count = deleteFromTable(stmt, symbol, exchange, type, uid, uid2, companyName, 
          note, price, asset, logo, tid, whichDelete);
      if (count != (verifyRowCount.get(tid+"_delete_")).intValue()){
        String str = "Gfxd delete (companies) has different row count from that of derby " +
                "derby deleted " + (verifyRowCount.get(tid+"_delete_")).intValue() +
                " but gfxd deleted " + count;        
        if (failAtUpdateCount && !isHATest) throw new TestException (str);
        else Log.getLogWriter().warning(str);
      }
    } catch (SQLException se) {
      SQLHelper.handleGFGFXDException(se, exList); //handle the exception
    }
  }
  
  protected void deleteFromGfxdTable(Connection gConn, int whichDelete, String symbol,
      String exchange, short type, UUID uid, UUID uid2, String companyName,
      String note, UDTPrice price, long asset, byte[] logo, int tid){
    PreparedStatement stmt = getStmt(gConn, delete[whichDelete]); 
    if (SQLTest.testSecurity && stmt == null) {
      if (SQLSecurityTest.prepareStmtException.get() != null) {
        SQLSecurityTest.prepareStmtException.set(null);
        return;
      } else Log.getLogWriter().warning("does not get stmt"); 
    } //work around #43244
    if (setCriticalHeap && stmt == null) {
      return; //prepare stmt may fail due to XCL54 now
    }
    
    if (stmt == null && alterTableDropColumn) {
      Log.getLogWriter().info("prepare stmt failed due to missing column");
      return; //prepare stmt may fail due to alter table now
    } 
    
    if (stmt == null && SQLTest.setTx && isHATest) {
      Log.getLogWriter().info("prepare stmt failed due to node failure");
      return; //prepare stmt may fail due to tx no HA support yet
    } 
    if (stmt == null) {
      throw new TestException("Does not expect statement to be null, but it is.");
    }
    
    try {   
      deleteFromTable(stmt, symbol, exchange, type, uid, uid2, companyName, 
          note, price, asset, logo, tid, whichDelete);        
    } catch (SQLException se) {
      if ((se.getSQLState().equals("42500") || se.getSQLState().equals("42502"))
          && testSecurity) {
        Log.getLogWriter().info("Got the expected exception for authorization," +
           " continuing tests");
      } else if (alterTableDropColumn && (se.getSQLState().equals("42X14") || se.getSQLState().equals("42X04"))) {
        //42X04 is possible when column in where clause is droppedT
        Log.getLogWriter().info("Got expected column not found exception in delete, continuing test");
      } else SQLHelper.handleSQLException(se); //handle the exception
    }
  }
  
  protected int deleteFromTable(PreparedStatement stmt, String symbol,
      String exchange, short type, UUID uid, UUID uid2, String companyName,
      String note, UDTPrice price, long asset, byte[] logo, int tid, 
      int whichDelete) throws SQLException {
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    
    String query = " QUERY: " + delete[whichDelete];
    
    int rowCount = 0;
    switch (whichDelete) {
    case 0:   
      //"delete from trade.companies where symbol = ? and exchange = ?",
      Log.getLogWriter().info(database + "deleting  trade.companies with SYMBOL:" + symbol + 
          ",EXCHANGE:" + exchange + ",TID:" + tid + query);  
      stmt.setString(1, symbol);
      stmt.setString(2, exchange);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " +  rowCount + " rows in trade.companies with SYMBOL:" + symbol + 
          ",EXCHANGE:" + exchange + ",TID:" + tid + query);
      break;
    case 1:
      //"delete from trade.companies where companyName = ? and tid=?",
      Log.getLogWriter().info(database + "deleting  trade.companies with  COMPANYNAME:" + companyName 
                + ",TID:" + tid + query);  
      stmt.setString(1, companyName);
      stmt.setInt(2, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " +  rowCount + " rows in trade.companies with  COMPANYNAME:" + companyName 
                + ",TID:" + tid + query);  
      break;
    case 2:   
      //"delete from trade.companies where companyType IN (?, ?) and trade.getHighPrice(histPrice) <? and tid=?",     
      short type2 = (short) ((type + 1) % 10);
      Log.getLogWriter().info(database + "deleting  trade.companies with 1_TYPE:" + type + ",2_TYPE:" + type2 +
          ",HIGHPRICE:" + UDTPrice.getHighPrice(price) + ",TID:" + tid + query);  
      stmt.setShort(1, type);
      stmt.setShort(2, type2);
      stmt.setBigDecimal(3, UDTPrice.getHighPrice(price));
      stmt.setInt(4, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " +  rowCount + " rows in trade.companies with 1_TYPE:" + type + ",2_TYPE:" + type2 +
          ",HIGHPRICE:" + UDTPrice.getHighPrice(price) + ",TID:" + tid + query); 
      break;
    case 3:   
      //"delete from trade.companies where tid=? and symbol < ? and  trade.getHighPrice(histPrice) >=? and asset <= ? and companyType = ?",
      Log.getLogWriter().info(database + "deleting  trade.companies with  TID:" + tid +
          ",SYMBOL:" + symbol + ",HIGHPRICE:" + UDTPrice.getHighPrice(price) + ",ASSET:" + asset + 
          ",TYPE:" + type + query);  
      stmt.setInt(1, tid);
      stmt.setString(2, symbol);
      stmt.setBigDecimal(3, UDTPrice.getHighPrice(price));
      stmt.setLong(4, asset);
      stmt.setShort(5, type);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " +  rowCount + " rows in trade.companies with  TID:" + tid +
          ",SYMBOL:" + symbol + ",HIGHPRICE:" + UDTPrice.getHighPrice(price) + ",ASSET:" + asset + 
          ",TYPE:" + type + query);  
      break;
    case 4: 
      String pattern = (note != null && note.length() > 4 ) ? "%" + note.substring(1, 4) : "%abc";
      pattern += '%';
      //"delete from trade.companies where tid=? and trade.getLowPrice(histPrice) <=? and note like ? and companyType = ?",
      Log.getLogWriter().info(database + "deleting  trade.companies with TID:" + tid +
          ",LOWPRICE:" + UDTPrice.getLowPrice(price) + ",TYPE:" + type + ",PATTERN:" + pattern + query);  
      stmt.setInt(1, tid);
      stmt.setString(3, pattern);
      stmt.setBigDecimal(2, UDTPrice.getLowPrice(price));
      stmt.setShort(4, type);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "deleted " +  rowCount + " rows in trade.companies TID:" + tid +
          ",LOWPRICE:" + UDTPrice.getLowPrice(price) + ",TYPE:" + type + ",PATTERN:" + pattern + query); 
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
  
  protected boolean updateDerbyTable (Connection conn, int[] whichUpdate, String[] symbol,
      String[] exchange, short[] type, UUID[] uid, UUID[] uid2, String[] companyName,
      String[] note, UDTPrice[] price, long[] asset, byte[][] logo, short[] newcompanyType, UUID[] newuid, 
      String[] newcompanyName, Clob[] newcompanyInfo, String[] newnote, UDTPrice[] newprice, 
      long[] newasset, byte[][] newlogo, int size, List<SQLException> exList) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      boolean[] unsupported = new boolean[1];
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], unsupported);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed
            
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
      
      try {
        if (stmt !=null) {
          verifyRowCount.put(tid+"_update"+i, 0);
          count = updateTable(stmt, symbol[i], exchange[i], type[i], uid[i], uid2[i], companyName[i], 
              note[i], price[i], asset[i], logo[i], 
              newcompanyType[i], newuid[i], newcompanyName[i], newcompanyInfo[i], 
              newnote[i], newprice[i], newasset[i], newlogo[i], tid, whichUpdate[i] );
          verifyRowCount.put(tid+"_update"+i, new Integer(count));
          
        } 
      } catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se)) { //handles the deadlock of aborting
          return false;
        } else
            SQLHelper.handleDerbySQLException(se, exList);
      }    
    }  
    return true;
  }
  
  protected boolean updateDerbyTableUsingBatch (Connection conn, int[] whichUpdate, String[] symbol,
      String[] exchange, short[] type, UUID[] uid, UUID[] uid2, String[] companyName,
      String[] note, UDTPrice[] price, long[] asset, byte[][] logo, short[] newcompanyType, UUID[] newuid, 
      String[] newcompanyName, Clob[] newcompanyInfo, String[] newnote, UDTPrice[] newprice, 
      long[] newasset, byte[][] newlogo, int size, List<SQLException> exList) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    
    
    
    boolean[] unsupported = new boolean[1];
    
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[0], unsupported);
    else stmt = getStmt(conn, update[whichUpdate[0]]); //use only this after bug#39913 is fixed
          
    if (stmt == null) {
      if (alterTableDropColumn && SQLTest.alterTableException.get() != null && (Boolean)SQLTest.alterTableException.get() == true)
        return true; //do the same in gfxd to get alter table exception
      else if (unsupported[0]) return true; //do the same in gfxd to get unsupported exception
      else return false;
    }
    
    int counts[] = null;
    for (int i=0 ; i<size ; i++) {
      try {
        Log.getLogWriter().info("Derby - batch updating trade.companies with TYPE:" + newcompanyType[i]  + ",COMPANYNAME:" + newcompanyName[i] +
            ",SYMBOL:" + symbol[i] + ",EXCHANGE:" + exchange[i] + "QUERY : " + update[whichUpdate[0]]);
        stmt.setShort(1, newcompanyType[i]);
        stmt.setString(2, newcompanyName[i]);      
        stmt.setString(3, symbol[i]);
        stmt.setString(4, exchange[i]);
        
        stmt.addBatch();
      } catch (SQLException se) {
        if (!SQLHelper.checkDerbyException(conn, se)) return false; //retry
        else     
          SQLHelper.handleDerbySQLException(se, exList);          
      }    
    }
    
    try {
      counts = stmt.executeBatch(); 
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      if (!SQLHelper.checkDerbyException(conn, se)) return false;  //for retry
      else SQLHelper.handleDerbySQLException(se, exList); //not expect any exception on this update 
    }   
    
    for (int i =0; i<counts.length; i++) {  
      if (counts[i] != -3) {
        verifyRowCount.put(tid+"_update"+i, 0);
        verifyRowCount.put(tid+"_update"+i, new Integer(counts[i]));
        Log.getLogWriter().info("Derby batch updated " + counts[i] + " rows in trade.companies TYPE:" + newcompanyType[i]  + ",COMPANYNAME:" + newcompanyName[i] +
            ",SYMBOL:" + symbol[i] + ",EXCHANGE:" + exchange[i] + "QUERY : " + update[whichUpdate[0]]);
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
      partitionKeys= (ArrayList<String>)partitionMap.get("companiesPartition");
    }
    else {
      int myWanSite = getMyWanSite();
      partitionKeys = (ArrayList<String>)wanPartitionMap.
        get(myWanSite+"_companiesPartition");
    }
    Log.getLogWriter().info("partition keys are " + partitionKeys);
  }
  
  //used to parse the partitionKey and test unsupported update on partitionKey, no need after bug #39913 is fixed
  protected PreparedStatement getCorrectStmt(Connection conn, int whichUpdate,
      ArrayList<String> partitionKeys, boolean[] unsupported){
    PreparedStatement stmt = null;
    switch (whichUpdate) {
    case 0: 
    //"update trade.companies set companytype = ?, companyName = ? where symbol =? and exchange = ? ",  
      if (partitionKeys.contains("companytype") || partitionKeys.contains("companyname")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 1: 
    //"update trade.companies set companytype = ? where symbol =? and exchange in (\"nasdaq\", \"nye\", \"amex\", \"lse\", \"fse\", \"hkse\", \"tse\") ",
      if (partitionKeys.contains("companytype")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 2: 
      //"update trade.companies set uuid = ?, uid = ? where symbol >=? and tid = ?",
      if (partitionKeys.contains("uuid") || partitionKeys.contains("_uid")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 3: 
      //"update trade.companies set companyinfo = ?  where symbol =? and exchange = ? ",
      if (partitionKeys.contains("companyinfo")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 4: 
      //"update trade.companies set note = ?  where trade.getHighPrice(histPrice) <=? and companytype =? and tid=? ",
      if (partitionKeys.contains("note")) {
        Log.getLogWriter().info("Will update gemfirexd on partition key");
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 5: 
      // "update trade.companies set companytype = ? where symbol =? and companytype in (?, ?) and tid = ?",
      if (partitionKeys.contains("companytype")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true;//if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 6: 
      // "update trade.companies set histprice = ? where tid = ? and asset >= (select avg(asset) from trade.companies where tid = ?)",
      if (partitionKeys.contains("histprice")) {
        if (!SQLHelper.isDerbyConn(conn))
          stmt = getUnsupportedStmt(conn, update[whichUpdate]);
        else unsupported[0] = true; //if derbyConn, stmt is null so no update in derby as well
      } else stmt = getStmt(conn, update[whichUpdate]);
      break;
    case 7: 
      //"update trade.companies set histprice = ?, logo = ? where tid = ? and asset >= ? and companytype <= ? and symbol like ? ",
      if (partitionKeys.contains("histprice") || partitionKeys.contains("logo")) {
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
  
  protected int updateTable (PreparedStatement stmt, String symbol,
      String exchange, short type, UUID uid, UUID uid2, String companyName,
      String note, UDTPrice price, long asset, byte[] logo, 
      short newcompanyType, UUID newuid, String newcompanyName, Clob newcompanyInfo, String newnote, 
      UDTPrice newprice, long newasset, byte[] newlogo, int tid, int whichUpdate) throws SQLException {
    int rowCount = 0; 
    
    String database = SQLHelper.isDerbyConn(stmt.getConnection())?"Derby - " :"gemfirexd - ";  
    String query = " QUERY: " + update[whichUpdate];
    
    switch (whichUpdate) {
    case 0: 
      //"update trade.companies set companytype = ?, companyName = ? where symbol =? and exchange = ? ",  
      Log.getLogWriter().info(database + "updating trade.companies with COMPANYTYPE:" + newcompanyType  + ",COMPANYNAME:" + newcompanyName +
          " where SYMBOL:" + symbol + ",EXCHANGE:" + exchange + query);
      stmt.setShort(1, newcompanyType);
      stmt.setString(2, newcompanyName);      
      stmt.setString(3, symbol);
      stmt.setString(4, exchange);
      if (isTicket46933Fixed){
        rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies with COMPANYTYPE:" + newcompanyType  + ",COMPANYNAME:" + newcompanyName +
          " where SYMBOL:" + symbol + ",EXCHANGE:" + exchange + query);
    }
      else Log.getLogWriter().info("do not execute the update due to ticket #46933");
      break;
    case 1: 
      //"update trade.companies set companytype = ? where symbol =? and exchange in (\"nasdaq\", \"nye\", \"amex\", \"lse\", \"fse\", \"hkse\", \"tse\") ",
      Log.getLogWriter().info(database + "updating trade.companies with COMPANYTYPE:" + newcompanyType  + 
          " where SYMBOL:" + symbol + query);
      stmt.setShort(1, newcompanyType);    
      stmt.setString(2, symbol);
      if (isTicket46980Fixed){
        rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies with COMPANYTYPE:" + newcompanyType  + 
          " where SYMBOL:" + symbol + query); }
      else Log.getLogWriter().info("do not execute the update due to ticket #46980");
      break;
        
    case 2: 
      //"update trade.companies set uuid = ?, uid = ? where symbol >=? and tid = ?",
      Log.getLogWriter().info(database + "updating trade.companies with UID:" + 
          ResultSetHelper.convertByteArrayToString(getUidBytes(newuid)) + 
          ",UUID:" + newuid +
          " where TID:" + tid + ",SYMBOL:" + symbol + query);
      stmt.setBytes(2, getUidBytes(newuid));
      stmt.setObject(1, newuid);
      stmt.setInt(4, tid);
      stmt.setString(3, symbol);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies with UID:" + 
          ResultSetHelper.convertByteArrayToString(getUidBytes(newuid)) + 
          ",UUID:" + newuid +
          " where TID:" + tid + ",SYMBOL:" + symbol + query);
      break;
    case 3: 
      //"update trade.companies set companyinfo = ?  where symbol =? and exchange = ? ",
      Log.getLogWriter().info(database + "updating trade.companies with COMPANYINFO:" + 
          (ResultSetHelper.useMD5Checksum && newcompanyInfo != null ? 
          ResultSetHelper.convertClobToChecksum(newcompanyInfo, newcompanyInfo.length())
          : getStringFromClob(newcompanyInfo)) + 
          " where SYMBOL:" + symbol + ",EXCHANGE:" + exchange + query);
      stmt.setClob(1, newcompanyInfo);
      stmt.setString(2, symbol);
      stmt.setString(3, exchange);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies with COMPANYINFO:" + 
          (ResultSetHelper.useMD5Checksum && newcompanyInfo != null ? 
          ResultSetHelper.convertClobToChecksum(newcompanyInfo, newcompanyInfo.length())
          : getStringFromClob(newcompanyInfo)) + 
          " where SYMBOL:" + symbol + ",EXCHANGE:" + exchange + query);
      break;
    case 4: 
      //"update trade.companies set note = ?  where trade.getHighPrice(histPrice) <=? and companytype =? and tid=? ",
      Log.getLogWriter().info(database + "updating trade.companies with NOTE:" + 
          (ResultSetHelper.useMD5Checksum && newnote != null && newnote.length() > ResultSetHelper.longVarCharSize ? 
            ResultSetHelper.convertStringToChecksum(newnote, newnote.length()) : newnote) + 
          " where HIGHPRICE:" + UDTPrice.getHighPrice(price) + 
          ",COMPANYTYPE:" + type + ",TID:" + tid + query);
      stmt.setBigDecimal(2, UDTPrice.getHighPrice(price));
      stmt.setString(1, newnote);
      stmt.setInt(3, type);
      stmt.setInt(4, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies with NOTE:" + 
          (ResultSetHelper.useMD5Checksum && newnote != null && newnote.length() > ResultSetHelper.longVarCharSize ? 
            ResultSetHelper.convertStringToChecksum(newnote, newnote.length()) : newnote) + 
          " where HIGHPRICE:" + UDTPrice.getHighPrice(price) + 
          ",COMPANYTYPE:" + type + ",TID:" + tid + query);
      break; 
    case 5: 
      //"update trade.companies set companytype = ? where symbol =? and companytype in (?, ?) and tid = ?",
      Log.getLogWriter().info(database + "updating trade.companies with COMPANYTYPE:" + newcompanyType  + 
          " where SYMBOL:" + symbol + ",FIRSTTYPE:" + type + ",SECONDTYPE:" + (type+1) % 10 +
          ",TID:" + tid + query);
      stmt.setShort(1, newcompanyType);    
      stmt.setString(2, symbol);
      stmt.setInt(3, type);
      stmt.setInt(4, (type+1) % 10);
      stmt.setInt(5, tid);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies with COMPANYTYPE:" + newcompanyType  + 
          " where SYMBOL:" + symbol + ",FIRSTTYPE:" + type + ",SECONDTYPE:" + (type+1) % 10 +
          ",TID:" + tid + query);
      break;
    case 6: 
      //"update trade.companies set histprice = ? where tid = ? and asset >= (select avg(asset) from trade.companies where tid = ?)",
      Log.getLogWriter().info(database + "updating trade.companies with HISTPRICE:" + newprice  + 
          " where TID:" + tid  + query);
      stmt.setObject(1, newprice);    
      stmt.setInt(2, tid);
      stmt.setInt(3, tid);
      if (isTicket46981Fixed){
        rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies with HISTPRICE:" + newprice  + 
          " where TID:" + tid  + query); }
      else Log.getLogWriter().info("do not execute the update due to ticket #46981");
      break;
    case 7: 
      //"update trade.companies set histprice = ?, logo = ? where tid = ? and asset >= ? and companytype <= ? and symbol like ? ",
      String pattern = symbol.length()>1 ? '_' + symbol.substring(1, 2) + '%' : "_%";
      Log.getLogWriter().info(database + "updating trade.companies with HISTPRICE:" + newprice  + 
          ",NEWLOGO:" + newlogo + " where TID:" + tid + ",ASSET:" + asset +
          ",COMPANYTYPE:" + type + ",SYMBOL:" + pattern + query);
      stmt.setObject(1, newprice);    
      stmt.setBytes(2, newlogo);
      stmt.setInt(3, tid);
      stmt.setLong(4, asset);
      stmt.setShort(5, type);
      stmt.setString(6, pattern);
      rowCount = stmt.executeUpdate();
      Log.getLogWriter().info(database + "updated " + rowCount + " rows in trade.companies HISTPRICE:" + newprice  + 
          ",NEWLOGO:" + newlogo + " where TID:" + tid + ",ASSET:" + asset +
          ",COMPANYTYPE:" + type + ",SYMBOL:" + pattern + query);
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
  
  //check expected exceptions
  protected void updateGfxdTable (Connection conn, int[] whichUpdate, String[] symbol,
      String[] exchange, short[] type, UUID[] uid, UUID[] uid2, String[] companyName,
      String[] note, UDTPrice[] price, long[] asset, byte[][] logo, short[] newcompanyType, UUID[] newuid, 
      String[] newcompanyName, Clob[] newcompanyInfo, String[] newnote, UDTPrice[] newprice, 
      long[] newasset, byte[][] newlogo, int size, List<SQLException> exList) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    int count = -1;
    
    for (int i=0 ; i<size ; i++) {
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], null);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed
      
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
          count = updateTable(stmt, symbol[i], exchange[i], type[i], uid[i], uid2[i], companyName[i], 
              note[i], price[i], asset[i], logo[i], 
              newcompanyType[i], newuid[i], newcompanyName[i], newcompanyInfo[i], 
              newnote[i], newprice[i], newasset[i], newlogo[i], tid, whichUpdate[i]);
          if (count != (verifyRowCount.get(tid+"_update"+i)).intValue()){
            String str = "Gfxd update has different row count from that of derby " +
                    "derby updated " + (verifyRowCount.get(tid+"_update"+i)).intValue() +
                    " but gfxd updated " + count;
            if (failAtUpdateCount && !isHATest) throw new TestException (str);
            else Log.getLogWriter().warning(str);
          }
        }
      } catch (SQLException se) {
         SQLHelper.handleGFGFXDException(se, exList);
      }    
    }  
  }
  
  @SuppressWarnings("null")
  protected void updateGfxdTableUsingBatch (Connection conn, int[] whichUpdate, String[] symbol,
      String[] exchange, short[] type, UUID[] uid, UUID[] uid2, String[] companyName,
      String[] note, UDTPrice[] price, long[] asset, byte[][] logo, short[] newcompanyType, UUID[] newuid, 
      String[] newcompanyName, Clob[] newcompanyInfo, String[] newnote, UDTPrice[] newprice, 
      long[] newasset, byte[][] newlogo, int size, List<SQLException> exList) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    
    
    
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[0], null);
    else stmt = getStmt(conn, update[whichUpdate[0]]); //use only this after bug#39913 is fixed
    
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
    
    if (stmt == null) return;
    int counts[] = null;
    
    for (int i=0 ; i<size ; i++) {
      try {
        Log.getLogWriter().info("gemfire  - batch updating trade.companies with TYPE:" + 
            newcompanyType[i]  + ",COMPANYNAME:" + newcompanyName[i] +
            ",SYMBOL:" + symbol[i] + ",EXCHANGE:" + exchange[i] + " QUERY : " +  update[whichUpdate[0]]);
        stmt.setShort(1, newcompanyType[i]);
        stmt.setString(2, newcompanyName[i]);      
        stmt.setString(3, symbol[i]);
        stmt.setString(4, exchange[i]);
        
        stmt.addBatch();
      } catch (SQLException se) {   
        SQLHelper.printSQLException(se);
        SQLHelper.handleGFGFXDException(se, exList); //should not see any exceptions here        
      }    
    }
    
    try {
      counts = stmt.executeBatch(); 
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se); //not expect any exception on this update 
    }   
    
    if (counts == null) {
      Log.getLogWriter().warning("Batch update failed in gfxd, will check if derby got same issue, etc");
      return;
    }
    
    for (int i =0; i<counts.length; i++) {  
      if (counts[i] != -3) {
        Log.getLogWriter().info("gemfire  - batch updated  trade.companies  TYPE:" + 
            newcompanyType[i]  + ",COMPANYNAME:" + newcompanyName[i] +
            ",SYMBOL:" + symbol[i] + ",EXCHANGE:" + exchange[i] + " QUERY : " +  update[whichUpdate[0]]);
        if (counts[i] != ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue()) {
          String str = "gfxd updated has different row count from that of derby " +
              "derby updated " + ((Integer)verifyRowCount.get(tid+"_update"+i)).intValue() +
              " but gfxd updated " + counts[i];
          if (failAtUpdateCount && !isHATest) throw new TestException (str);
          else Log.getLogWriter().warning(str);
        }
      } else {
        Log.getLogWriter().warning("gfxd failed to update in batch update");
      }
    }   
  }
  
  protected void updateGfxdTable (Connection conn, int[] whichUpdate, String[] symbol,
      String[] exchange, short[] type, UUID[] uid, UUID[] uid2, String[] companyName,
      String[] note, UDTPrice[] price, long[] asset, byte[][] logo, short[] newcompanyType, UUID[] newuid, 
      String[] newcompanyName, Clob[] newcompanyInfo, String[] newnote, UDTPrice[] newprice, 
      long[] newasset, byte[][] newlogo, int size) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    
    for (int i=0 ; i<size ; i++) {
      
      if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[i], null);
      else stmt = getStmt(conn, update[whichUpdate[i]]); //use only this after bug#39913 is fixed

      try {
        if (stmt!=null)
          updateTable(stmt, symbol[i], exchange[i], type[i], uid[i], uid2[i], companyName[i], 
              note[i], price[i], asset[i], logo[i], 
              newcompanyType[i], newuid[i], newcompanyName[i], newcompanyInfo[i], 
              newnote[i], newprice[i], newasset[i], newlogo[i], tid, whichUpdate[i]);
      } catch (SQLException se) {
        if (se.getSQLState().equals("42502") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
          " continuing tests");
        } else if (alterTableDropColumn && (se.getSQLState().equals("42X14") || se.getSQLState().equals("42X04"))) {
          //42X04 is possible when column in where clause is dropped
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else SQLHelper.handleSQLException(se);
      }    
    }  
  }
  
  protected void updateGfxdTableUsingBatch (Connection conn, int[] whichUpdate, String[] symbol,
      String[] exchange, short[] type, UUID[] uid, UUID[] uid2, String[] companyName,
      String[] note, UDTPrice[] price, long[] asset, byte[][] logo, short[] newcompanyType, UUID[] newuid, 
      String[] newcompanyName, Clob[] newcompanyInfo, String[] newnote, UDTPrice[] newprice, 
      long[] newasset, byte[][] newlogo, int size) {
    PreparedStatement stmt = null;
    int tid = getMyTid();
    
    
    
    if (SQLTest.testPartitionBy)    stmt = getCorrectStmt(conn, whichUpdate[0], null);
    else stmt = getStmt(conn, update[whichUpdate[0]]); //use only this after bug#39913 is fixed
  
    if (stmt == null) {
      Log.getLogWriter().info("could not get stmt to proceed");
      return;
    }
    
    for (int i=0 ; i<size ; i++) {
      try {
        Log.getLogWriter().info("gemfire - batch updating trade.companies with TYPE:" +
            newcompanyType[i]  + ",COMPANYNAME:" + newcompanyName[i] +
            ",SYMBOL:" + symbol[i] + ",EXCHANGE:" + exchange[i] + whichUpdate[0] );
        stmt.setShort(1, newcompanyType[i]);
        stmt.setString(2, newcompanyName[i]);      
        stmt.setString(3, symbol[i]);
        stmt.setString(4, exchange[i]);
        
        stmt.addBatch();
      } catch (SQLException se) {   
        if (se.getSQLState().equals("42502") && testSecurity) {
          Log.getLogWriter().info("Got the expected exception for authorization," +
          " continuing tests");
        } else if (alterTableDropColumn && (se.getSQLState().equals("42X14") || se.getSQLState().equals("42X04"))) {
          //42X04 is possible when column in where clause is dropped
          Log.getLogWriter().info("Got expected column not found exception in update, continuing test");
        } else SQLHelper.handleSQLException(se);
      }    
    }

    int counts[] = null;
    try {
      counts = stmt.executeBatch(); 
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se); //not expect any exception for update
    }  
    
    if (counts == null) {
      if (SQLTest.setTx && !testUniqueKeys) {
        Log.getLogWriter().info("possibly got conflict exception");
        return;
      } else if (SQLTest.setTx && isHATest) {
        //TODO we will remove this check once txn HA is supported
        Log.getLogWriter().info(" got node failure exception");
        return;
      } else throw new TestException("Does not expect batch update to fail, but no updateCount[] returns");
    }
    
    for (int i =0; i<counts.length; i++) {         
      if (counts[i] != -3) {
        Log.getLogWriter().info("gemfire - batch updated trade.companies with TYPE:" +
            newcompanyType[i]  + ",COMPANYNAME:" + newcompanyName[i] +
            ",SYMBOL:" + symbol[i] + ",EXCHANGE:" + exchange[i] + whichUpdate[0] );
      } else {
        //throw new TestException("gfxd failed to update a row in batch update");
        Log.getLogWriter().warning("gfxd failed to update in batch update in " + i + " update");
      }
    }   
  }

  @Override
  public void put(Connection dConn, Connection gConn, int size) {
    // TODO Auto-generated method stub
    
  }

}
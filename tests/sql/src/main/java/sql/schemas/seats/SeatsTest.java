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
package sql.schemas.seats;

import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.HDFSStoreDescription;
import hydra.HDFSStoreHelper;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.query.Struct;
import com.pivotal.gemfirexd.Attribute;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.backupAndRestore.BackupAndRestoreBB;
import sql.backupAndRestore.BackupRestoreTest;
import sql.schemas.SchemaTest;
import sql.sqlTx.SQLTxBB;
import sql.sqlutil.ResultSetHelper;
import sql.GFEDBManager.Isolation;
import util.TestException;
import util.TestHelper;

public class SeatsTest extends SchemaTest {
  public static SeatsTest seats;
  protected static final String SCHEMA_NAME = "SEATS";
  protected static String dataGroup = "DataGroup";
  protected static final String DATAGROUPCLAUSE = random.nextBoolean() ? 
      " SERVER GROUPS (" + dataGroup + ") " : 
        " SERVER GROUPS (" + dataGroup.toUpperCase() + ") ";
  protected static final String VERIFIERGROUPCLAUSE = " SERVER GROUPS (VerifierGroup) ";
  protected static List<Struct> flights;
  protected static boolean[] flightSet = new boolean[1];
  protected static List<Struct> customers;
  protected static List<Struct> customersInitBalance;
  protected static boolean[] customerSet = new boolean[1];
  protected static Map<Long, List<Struct>> customerByAP = new HashMap<Long, List<Struct>>();
  
  private static Timestamp startDate;
  public static int[] DISTANCES = {20, 50, 100};
  public static final int PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 25;
  
  public static final int PROB_REPOPULATE_FLIGHTINFO = 5;
  public static final int PROB_ADD_RESERVATION = 3;
  public static final int FLIGHTS_NUM_SEATS = 150;
  public static final int FLIGHTS_FIRST_CLASS_OFFSET = 10;
  public static final int HUNDRED = 100;
  public static final int THOUSAND = 1000;
  public static final int MILLION = 1000000;
  
  public static final int CACHE_LIMIT_PENDING_INSERTS = 10000;
  public static final int CACHE_LIMIT_PENDING_UPDATES = 5000;
  public static final int CACHE_LIMIT_PENDING_DELETES = 5000;
  
  public static final int RESERVATION_PRICE_MIN = 100;
  public static final int RESERVATION_PRICE_MAX = 1000;
  
  public static final int PROB_DELETE_WITH_CUSTOMER_ID_STR = 20;
  public static final int PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20;
  public static final int PROB_DELETE_WITH_FREQUENTFLYER_ID_STR = 20;
  public static final int PROB_REQUEUE_DELETED_RESERVATION = 50;
  public static final int PROB_UPDATE_FREQUENT_FLYER = 25;
    
  private final HydraThreadLocal my_reservations = new HydraThreadLocal();
  private int[] setFlightThread = new int[1];
  public static HydraThreadLocal gfxdNonTxnConn = new HydraThreadLocal();
  public static HydraThreadLocal curTxId = new HydraThreadLocal();
  public static boolean useNonTxnConn = false;  
  public static boolean addTxId = true;
  public static final String INSERT = "insert";
  public static final String UPDATE = "update";
  public static final String DELETE = "delete";
  
  protected static boolean verifyBalance  = true;
  protected String initBalTablePrefix =  "init_cust_price_";
  protected String tempTablePrefix = "session.";
  protected String updateBalTablePrefix = "cust_price_";
  protected boolean useTempTable = false;
  
  protected long num_reservations = 0l;
  
  protected static boolean reproduce50962 = TestConfig.tab().booleanAt(SQLPrms.toReproduce50962, false);
  protected static boolean reproduce51113 = TestConfig.tab().booleanAt(SQLPrms.toReproduce51113, false);
  protected static boolean reproduce51090 = TestConfig.tab().booleanAt(SQLPrms.toReproduce51090, false);
  protected static boolean reproduce43511 = TestConfig.tab().booleanAt(SQLPrms.toReproduce43511, false);
  protected static boolean reproduce51255 = TestConfig.tab().booleanAt(SQLPrms.toReproduce51255, true);

  protected static HydraThreadLocal increaseMemOpExectued = new HydraThreadLocal();
  
  // ----------------------------------------------------------------
  // TIME CONSTANTS
  // ----------------------------------------------------------------
  
  /** Number of microseconds in a day */
  public static final long MILLISECONDS_PER_MINUTE = 60000l; // 60sec * 1,000
  
  /** Number of microseconds in a day */
  public static final long MILLISECONDS_PER_DAY = 86400000l; // 60sec * 60min * 24hr * 1,000 
  
  protected boolean verifyBalanceTablesCreated = false;
  
  public static void HydraTask_createSchema() throws SQLException{
    if (seats == null) seats = new SeatsTest();
    seats.createSchema();
  }
  
  protected void createSchema() throws SQLException{
    ddlThread = getMyTid();
    
    Connection c = getGFEConnection();  
    Statement st = c.createStatement(); 
    
    String sql = "create schema " + SCHEMA_NAME;
    Log.getLogWriter().info(sql);
    st.execute(sql);
    Log.getLogWriter().info("executed " + sql);
  }
  
  public static void HydraTask_createSeatsTables() throws SQLException{
    if (seats == null) seats = new SeatsTest();
    seats.createSeatsTables();
  }
  
  protected void createSeatsTables() throws SQLException { 
    if (isHDFSTest) {
      HDFSStoreDescription hdfsStoreDesc = HDFSStoreHelper.getHDFSStoreDescription(ConfigPrms.getHDFSStoreConfig());
      hdfsStoreName = hdfsStoreDesc.getName();
    }
    
    StringBuilder sb = getSqlScript();  

    // here is our splitter ! We use ";" as a delimiter for each request  
    // then we are sure to have well formed statements  
    String[] inst = sb.toString().split(";");  

    Connection c = getGFEConnection();  
    Statement st = getConnectionWithSchema(c, SCHEMA_NAME, Isolation.NONE).createStatement();  

    for(int i = 0; i<inst.length; i++) {  
      // we ensure that there is no spaces before or after the request string  
      // in order to not execute empty statements 
      try {
        if(!inst[i].trim().equals("") && !inst[i].contains("exit")) {  
          if (isOffheap) {
            if (inst[i].contains("CREATE TABLE") ) {
              if (!mixOffheapTables) inst[i]+=OFFHEAPCLAUSE;
              else if (random.nextBoolean()) inst[i]+=OFFHEAPCLAUSE;
            }
          }
          
          if (hasPersistentTables) {
            if (inst[i].contains("CREATE TABLE")) inst[i]+=PERSISTENTCLAUSE;
          }
          
          if (isHATest && !inst[i].toUpperCase().contains("REPLICATE")) {
            if (inst[i].contains("CREATE TABLE")) inst[i]+=REDUNDANCYCLAUSE;      
          }

          if (isHDFSTest) {
            if (inst[i].contains("CREATE TABLE RESERVATION ")) inst[i]+=getFlightHDFSClause();      
          }
  
          //adding server group clause
          if (inst[i].contains("CREATE TABLE")) inst[i]+=DATAGROUPCLAUSE;
          
          log().info(">>"+inst[i]); 
          st.executeUpdate(inst[i]);                 
        }  
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
    }  

  }  
  private String getFlightHDFSClause() {
    /*work around #49001 -- will turn on casade in certain tests as it 
     * will affect a few txns which query the child tables of the trade 
     * like trade history, holding history etc
    return " EVICTION BY CRITERIA ( t_st_id = '" + status_completed
      + "') CASCADE EVICT INCOMING HDFSSTORE (" + hdfsStoreName + ")";
      */
    return " EVICTION BY CRITERIA ( DATE(F_ARRIVE_TIME) < CURRENT_DATE - 7)" +
    		" EVICTION FREQUENCY 1 MINUTES HDFSSTORE (" + hdfsStoreName + ")";
  }
  
  public static void HydraTask_runImportTable() throws SQLException {
    seats.runImportTable();
  }
  
  protected void runImportTable() throws SQLException {
    Properties p = new Properties();
    p.setProperty(Attribute.SKIP_CONSTRAINT_CHECKS, "true");
    Log.getLogWriter().info("setting " + Attribute.SKIP_CONSTRAINT_CHECKS + " to true" );
    Connection conn = getGFEConnection(p);
    
    runImportTable(getConnectionWithSchema(conn, SCHEMA_NAME, Isolation.READ_COMMITTED));
  }
  
  public static synchronized void HydraTask_initialize() {
    if (seats == null) seats = new SeatsTest();
    seats.initialize();
  }
  
  public static synchronized void HydraTask_createCaseInsensitiveIndexOnSystable() {
    if (seats == null) seats = new SeatsTest();
    seats.createCaseInsensitiveIndexOnSystable();
  }
  
  protected void createCaseInsensitiveIndexOnSystable() {
    ddlThread = getMyTid(); 
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    createCaseInsensitiveIndexOnSystable(conn);
  }
  
  public static synchronized void HydraTask_initializeInfo() {
    if (seats == null) seats = new SeatsTest();
    seats.initializeInfo();
  }
  
  protected void initializeInfo()  {
    //set non txn connection
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    gfxdNonTxnConn.set(conn);
    
    try {
      synchronized(setFlightThread) {
        if (flights ==null) {
          setFlightThread[0] = getMyTid();
          flights = getFlights(true, conn);
           
          num_reservations = getInitNumReservations(conn);
          
          customers = getCustomer(true, conn);
          boolean set = false;
          while (!set) {
            try {
              customersInitBalance = getCustomerInitBalance(conn);
              if (customersInitBalance != null) set = true;
            } catch (SQLException se) {
              if (se.getSQLState().equals("XCL54") && setCriticalHeap) {
                log().info("got expected query cancellation exception, continue testing");
                increaseCriticalHeapPercent = true;
              } else SQLHelper.handleSQLException(se);
            }
          }
          
          setCustomersMapByAP(conn);
          
          commit(conn);
        }
        
        if (setCriticalHeap) {
          createNewReservationTables(conn);
        }
        
        if (!reproduce51255 && ddlThread == getMyTid()) { //temp work around the issue
          String createTable = "create table " + updateBalTablePrefix + getMyTid() +
          "(C_ID bigint, C_BALANCE float) " 
          + (usePartitionBy ? " partition by column(c_id) " : "")
          + DATAGROUPCLAUSE ;
          
          if (!verifyBalanceTablesCreated) {
            log().info(createTable);
            conn.createStatement().execute(createTable);
            
            createNewTableAvoidLeftJoin(conn);
            verifyBalanceTablesCreated = true;
          }
        }
      }  
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void initialize() {
    my_reservations.set(new ArrayList<Reservation>());
  }
  
  protected void setCustomersMapByAP(Connection conn) throws SQLException{
    String sql = "select AP_ID from AIRPORT";
    log().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    
    while(rs.next()) {
      long airport_depart_id = rs.getLong(1);
      List<Struct> customersByAP = getCustomersByAP(airport_depart_id, conn);
      customerByAP.put(airport_depart_id, customersByAP);
    }
  }

  public static void HydraTask_doTxns() throws SQLException{
    if (seats == null) seats = new SeatsTest();
    
    if (startDate == null) seats.setStartDate();
    
    if (setCriticalHeap) {
      boolean[] executed = (boolean[])increaseMemOpExectued.get();
      if (executed == null) increaseMemOpExectued.set(new boolean[1]);
      else if (executed[0]) throw new TestException("Test issue, increaseMemOpExectued " +
      		"is not being reset");
    }

    //for read_committed case
    int numOfTxnsInTask = 5;
    for (int i=0; i<numOfTxnsInTask; i++) {
      seats.doTxns(Isolation.READ_COMMITTED);
      
      if (setCriticalHeap) {
        boolean[] executed = (boolean[])increaseMemOpExectued.get();
        if (executed[0]) {
          executed[0] = false;
          increaseMemOpExectued.set(executed);
          break;
        }
      }
      
    }
  }
  
  protected void doTxns(Isolation isolation) {
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, isolation);
    LinkedList<Reservation> cache;
    
    long txId = SQLTxBB.getBB().getSharedCounters().incrementAndRead(
        SQLTxBB.txId);
    Log.getLogWriter().info("setting hydra thread local curTxId to " + txId);
    curTxId.set(txId);
    
    try {
      if (reproduce50962) executeFindFlights(conn);
      
      else {
        //randomly select a transaction
        int whichOne = 0;
        int chance = random.nextInt(HUNDRED); {
          if (chance <20) whichOne=0;
          else if (chance < 60) whichOne = 1;
          else if (chance < 80) whichOne = 2;
          else if (chance < 90) whichOne = 3;            
          else {
            whichOne = 4;
            if (setCriticalHeap) {
              if (reproduce43511) {
                if (random.nextInt(HUNDRED) <10) performOrderByWithoutIndex(conn); 
              } else {
                if (random.nextInt(THOUSAND) == 0 && ddlThread == getMyTid()) performOrderByWithoutIndex(conn); 
                else {
                  if (!increaseCriticalHeapPercent) {
                    if (random.nextInt(HUNDRED) < 10) {
                      doSelectReservation();
                      whichOne = 1;
                    } else if (random.nextInt(HUNDRED) < 20) {
                      increaseMemOp(conn);
                      boolean[] executed = (boolean[])increaseMemOpExectued.get();
                      executed[0] = true;
                      increaseMemOpExectued.set(executed);
                      return;
                    }
                  }
                }
              }
            }
          }
          
        }
        switch (whichOne) {
        case 0:
          executeFindFlights(conn);
          break;
        case 1:  
          cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
          int insertSize = cache.size();
          if (insertSize < 1000)
            executeFindOpenSeats(conn);        
          else if(insertSize > CACHE_LIMIT_PENDING_INSERTS - 1000)
            executeNewReservation(conn);
          
          else {
            if (random.nextBoolean()) executeFindOpenSeats(conn);
            else executeNewReservation(conn);
          }
          break;
        case 2:
          cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
          int updateSize = cache.size();
          if (updateSize > 500) executeUpdateReservation(conn);
          break;
        case 3:
          cache = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES);
          int deleteSize = cache.size();
          if (deleteSize > 500) executeDeleteReservation(conn);
          break;
        case 4:
          executeUpdateCustomer(conn);
          break;
        default:
          throw new TestException("Test issue, wrong txn chosen");
        }
      }
      commit(conn); //TODO, to file exception using thin client driver when this line is commented out.
      closeGFEConnection(conn);
    } catch (SQLException se) {
      if (isHATest) {
        if (se.getSQLState().equals(X0Z01)) {
          log().info("Got expected node failure exception during select query, continue testing");
        } else if (SQLHelper.gotTXNodeFailureException(se)) {
          log().info("Got expected node failure exception during dml op using txn, continue testing");
        } else if (isOfflineTest && 
            (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) { 
          log().info("Got expected Offline exception, continuing test");
        } else SQLHelper.handleSQLException(se); 
      } else SQLHelper.handleSQLException(se);
    }
    
    boolean[] getCanceled = (boolean[]) SQLTest.getCanceled.get();
    if (getCanceled != null && getCanceled[0]) {     
      increaseCriticalHeapPercent = true;
      log().info("increaseCriticalHeapPercent is set to " + increaseCriticalHeapPercent);
      getCanceled[0] = false;
      SQLTest.getCanceled.set(getCanceled);
      
      System.gc(); //for gc
      log().info("Garbage collector is called");
    }
    
  }
  
  protected void performOrderByWithoutIndex(Connection conn) throws SQLException{
    String sql = "select * from reservation r, customer c, flight f " +
      " where r.r_c_id = c.c_id and f.f_id = r.r_f_id and r.r_id < ? order by r.r_id";
    
    ResultSet rs = null;
    try {
      PreparedStatement ps = conn.prepareStatement(sql);
      int r_id = random.nextInt(MILLION);
      ps.setLong(1, r_id);
      log().info("executing " + sql + " with r.r_id < " + r_id);
      rs = ps.executeQuery();
      
      ResultSetMetaData rsmd = rs.getMetaData();
      int numColumns = rsmd.getColumnCount();
      
      while(rs.next()) {
        for (int i=0; i<numColumns; i++) rs.getObject(i+1); //discard the processed results
      }
    
    } catch (SQLException se) {
      if (setCriticalHeap && se.getSQLState().equals("XCL54")) {
        log().info("got expected low memory exception, continue testing");    
      } else if (isHATest && (SQLHelper.gotTXNodeFailureException(se) 
          || se.getSQLState().equals("X0Z01"))) {
        log().info("got expected node failure exception, continue testing");
      } else if (isOfflineTest && 
          (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) { 
        log().info("Got expected Offline exception, continuing test");
      } 
      else SQLHelper.handleSQLException(se);
    } finally {
      if (rs != null) rs.close();
    }
    
  }
  
  protected void createNewReservationTables(Connection conn) throws SQLException {
    String tableName = "reservation_" + getMyTid();
    String dropTable = "DROP TABLE IF EXISTS " + tableName;
    
    String createTable = "create table " + tableName + 
    " as select * FROM reservation WITH NO DATA" 
    + " partition by column(r_id) " 
    +  (isHATest ? " BUCKETS 17 maxpartsize 10 persistent " + REDUNDANCYCLAUSE + " " : "" ) 
    + (isOffheap ? (!mixOffheapTables ? OFFHEAPCLAUSE : 
            (random.nextBoolean() ? OFFHEAPCLAUSE : "")) : "")
    + (isOffheap ? PERSISTENTCLAUSE : (random.nextBoolean() ? PERSISTENTCLAUSE : ""))
    + DATAGROUPCLAUSE;
    
    String checkSystables = " select * from sys.systables where tablename = '" 
      + tableName.toUpperCase() + "'";
    
    boolean tableExist = false;
    
    if (!tableExist) {
      log().info(dropTable);
      conn.createStatement().execute(dropTable);
      log().info(createTable);
      conn.createStatement().execute(createTable);
      //ddl auto committed
    }
    
    log().info(checkSystables);
    ResultSet rs = conn.createStatement().executeQuery(checkSystables);
    if (rs.next()) {
      log().info(tableName + " has been created");
    } else {
      rs = conn.createStatement().executeQuery("select * from sys.systables");
      throw new TestException(tableName + " is created but not in the systables\n" 
          + ResultSetHelper.listToString(ResultSetHelper.asList(rs, false)) );
    }
   
  }
  
  protected String getReservationTableName(Connection conn, String tablePrefix) throws SQLException {
    String sql = "select tablename from sys.systables where tablename like '" 
      + tablePrefix.toUpperCase() + "%'";
    List<Struct> rsList = getResultSet(conn, sql);
    if (rsList != null && rsList.size()>0) return (String) rsList.get(random.nextInt(rsList.size())).get("tablename".toUpperCase());
    else return tablePrefix+getMyTid();  
  }
  
  
  protected void increaseMemOp(Connection conn) throws SQLException {
    
    String tableName = getReservationTableName(conn, "reservation_");
 
    String truncateTable = "truncate table " + tableName;
    String deleteTable = "delete from " + tableName;
    
    String insertSubselect = " put into " + tableName + " select * from reservation";
    
    if (increaseCriticalHeapPercent && random.nextInt(HUNDRED) <10) {
      if (random.nextBoolean()) {
        log().info(deleteTable);
        conn.createStatement().execute(deleteTable);
      } else {
        log().info(truncateTable);
        conn.createStatement().execute(truncateTable);
      }
    }
    
    try {
      log().info(insertSubselect); //use put
      conn.createStatement().execute(insertSubselect); //insert into table
      conn.commit();
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z03")) {
        log().info("got bug #51253, continue testing");
        return;
      } else throw se;
    }
    
    conn.commit();
  }
  
  protected void setStartDate() throws SQLException {
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    String sql = "select CFP_FLIGHT_START from CONFIG_PROFILE";
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs.next()) startDate = rs.getTimestamp(1);
    else throw new TestException ("Does not get result for " + sql);
    
    Log.getLogWriter().info("startDate is " + startDate);
  }
  
  public static void HydraTask_createSeatsIndex() throws SQLException{
    if (seats == null) seats = new SeatsTest();

    seats.createSeatsIndexes();
  }
  
  protected void createSeatsIndexes() { 
    try {
      String sql = "create index index_airport on AIRPORT_DISTANCE (D_AP_ID0, D_DISTANCE asc)";
      createSeatsIndex(sql);
      
      sql = "create index index_customer on CUSTOMER (C_BASE_AP_ID)";
      createSeatsIndex(sql);
      
      
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void createCaseInsensitiveIndexOnSystable(Connection conn) { 
    try {    
      String sql = "create index index_systables_insensitive on sys.systables (tablename) " +
        "-- GEMFIREXD-PROPERTIES caseSensitive = false";
      log().info("executing " + sql);
      createSeatsIndex(sql);
      log().info("executed " + sql);
    } catch (SQLException se) {
      if (se.getSQLState().equals("42X62")) log().info("got expected 'CREATE INDEX' is not allowed in the 'SYS' schema");
      else SQLHelper.handleSQLException(se);
    }
  }
  
  protected void createSeatsIndex(String sql) throws SQLException {
    long start = System.currentTimeMillis();
    Log.getLogWriter().info("create index starts from " + start);
    
    createIndex(sql);
    
    long end = System.currentTimeMillis();
    Log.getLogWriter().info("create index finishes at " + end);
    
    long time = end - start;
    
    Log.getLogWriter().info("create index takes " + time/1000 + " seconds");
  }
  
  protected void createIndex(String sql) throws SQLException {
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    conn.createStatement().execute(sql);
    Log.getLogWriter().info("executed " + sql);
  }
   
  protected void doSelectReservation() {
    try {
      Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.READ_COMMITTED);
      String sql = "select * from reservation ";
      
      log().info(sql);
      ResultSet rs = conn.createStatement().executeQuery(sql);
      List<Struct> list = ResultSetHelper.asList(rs, false);   
      //Log.getLogWriter().info(ResultSetHelper.listToString(list));
      if (list != null) list.clear();
    } catch (SQLException se) {
      if (isOfflineTest && 
          (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) { 
        log().info("Got expected Offline exception, continuing test");
      } else SQLHelper.handleSQLException(se);
    }
  }
  
  // -----------------------------------------------------------------
  // RESERVED SEAT BITMAPS
  // -----------------------------------------------------------------
  
  public enum CacheType {
    PENDING_INSERTS     (CACHE_LIMIT_PENDING_INSERTS),
    PENDING_UPDATES     (CACHE_LIMIT_PENDING_UPDATES),
    PENDING_DELETES     (CACHE_LIMIT_PENDING_DELETES),
    ;
    
    private CacheType(int limit) {
      this.limit = limit;
    }
    private final int limit;
    
    public int getLimit() {
      return this.limit;
    }
  }
  
  protected final Map<CacheType, LinkedList<Reservation>> CACHE_RESERVATIONS = 
    new HashMap<CacheType, LinkedList<Reservation>>();{
      for (CacheType ctype : CacheType.values()) {
          CACHE_RESERVATIONS.put(ctype, new LinkedList<Reservation>());
    } // FOR
  } 
  
  //may need to use bb to sync this
  protected final Map<CustomerId, Set<FlightInfo>> CACHE_CUSTOMER_BOOKED_FLIGHTS = 
    new HashMap<CustomerId, Set<FlightInfo>>();
  protected final Map<FlightInfo, BitSet> CACHE_BOOKED_SEATS = 
    new HashMap<FlightInfo, BitSet>();

  private static final BitSet FULL_FLIGHT_BITSET = new BitSet(FLIGHTS_NUM_SEATS);
  static {
      for (int i = 0; i < FLIGHTS_NUM_SEATS; i++)
          FULL_FLIGHT_BITSET.set(i);
  } // STATIC
  
  protected BitSet getSeatsBitSet(FlightInfo flight_id) {
      BitSet seats = CACHE_BOOKED_SEATS.get(flight_id);
      if (seats == null) {
        synchronized (CACHE_BOOKED_SEATS) {
          seats = CACHE_BOOKED_SEATS.get(flight_id);
          if (seats == null) {
            seats = new BitSet(FLIGHTS_NUM_SEATS);
            CACHE_BOOKED_SEATS.put(flight_id, seats);
          }
        } // SYNCH
      }
      return (seats);
  }
  
  /**
   * Returns true if the given BitSet for a Flight has all of its seats reserved 
   * @param seats
   * @return
   */
  protected boolean isFlightFull(BitSet seats) {
    assert(FULL_FLIGHT_BITSET.size() == seats.size());
    return FULL_FLIGHT_BITSET.equals(seats);
  }
  
  class FlightInfo{
    long flight_id;
    long airline_id;
    long depart_airport_id;
    long arrive_airport_id;
    Timestamp departDate;

    FlightInfo(long flight_id, long airline_id, long depart_airport_id, long arrive_airport_id, Timestamp departDate) {
      this.flight_id = flight_id;
      this.airline_id = airline_id;
      this.depart_airport_id = depart_airport_id;
      this.arrive_airport_id = arrive_airport_id;
      this.departDate = departDate;      
    }
    
    protected long getFlightId() {
      return this.flight_id;
    }
    
    protected long getAlId() {
      return this.airline_id;
    }
    
    protected long getDepartAirportId() {
      return this.depart_airport_id;
    }
    
    protected long getArriveAirportId() {
      return this.arrive_airport_id;
    }
    
    protected int calDepartTime(Timestamp startDate) {
      return (int)((departDate.getTime() - startDate.getTime()) / 3600000);
    }
    
    public Timestamp getDepartDate(Timestamp start) {
      return (new Timestamp(start.getTime() + 
          (calDepartTime(start) * MILLISECONDS_PER_MINUTE * 60)));
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof FlightInfo) {
        FlightInfo o = (FlightInfo)obj;
        return (this.flight_id == o.flight_id);
      }
      return (false);
    }
    
    public int hashCode() {
      int result = 17;
      result = 37 * result + (int) (flight_id^(flight_id>>>32));       
      return result;
    }
    
    @Override
    public String toString() {
      return String.format("FlightInfo{flight_id=%d,airline_id=%d," +
      		"depart_airport_id=%d,arrive_airport_id=%d,departDate%1$TD %1$TT}", 
          this.flight_id, this.airline_id, this.depart_airport_id, 
          this.arrive_airport_id, this.departDate);
    }
  }
  
  class CustomerId {   
    private long id;
    private long depart_airport_id;
    
    public CustomerId(long id, long depart_airport_id) {
      this.id = id;
      this.depart_airport_id = depart_airport_id;
    }
    
    public long[] toArray() {
        return (new long[]{ this.id, this.depart_airport_id });
    }
    
    /**
     * @return the id
     */
    public long getId() {
        return id;
    }

    /**
     * @return the depart_airport_id
     */
    public long getDepartAirportId() {
        return depart_airport_id;
    }
    
    @Override
    public String toString() {
      return String.format("CustomerId{airport=%d,id=%d}", 
          this.depart_airport_id, this.id);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CustomerId) {
          CustomerId c = (CustomerId)obj;
          // Ignore id!
          return (this.depart_airport_id == c.depart_airport_id &&
                  this.id == c.id);
                  
      }
      return (false);
    }
    
    public int hashCode() {
      int result = 17;
      result = 37 * result + (int) id;
      result = 37 * result + (int) depart_airport_id;
      return result;
    }
}
  
  /**
   * When a customer looks for an open seat, they will then attempt to book that seat in
   * a new reservation. Some of them will want to change their seats. This data structure
   * represents a customer that is queued to change their seat. 
   */
  protected static class Reservation {
    public final long id;
    public final FlightInfo flightInfo;
    public final CustomerId customer_id;
    public int seatnum;
    public final Double price;
    
    public Reservation(long id, FlightInfo flightInfo, 
        CustomerId customer_id, int seatnum, double price) {
      this.id = id;
      this.flightInfo = flightInfo;
      this.customer_id = customer_id;
      this.seatnum = seatnum;
      this.price = price;
      assert(this.seatnum >= 0) : "Invalid seat number\n" + this;
      assert(this.seatnum < FLIGHTS_NUM_SEATS) : "Invalid seat number\n" + this;
    }
    
    public void setSeatnum(int seat) {
      this.seatnum = seat;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Reservation) {
        Reservation r = (Reservation)obj;
        // Ignore id!
        return (this.seatnum == r.seatnum &&
                this.flightInfo.equals(r.flightInfo) &&
                this.customer_id.equals(r.customer_id)&&
                this.price == r.price);
                  
      }
      return (false);
    }
    
    @Override
    public String toString() {
      return String.format("{Id:%d / %s / %s / SeatNum:%d}",
                             this.id, this.flightInfo, this.customer_id, this.seatnum);
    }
    
    public int hashCode() {
      int result = 17;
      result = 37 * result + (int) id;
      result = 37 * result + (int) seatnum;
      result = 37 * result + (int) new Double(price).hashCode();
      result = 37 * result + (int) (flightInfo.flight_id^(flightInfo.flight_id>>>32));   
      result = 37 * result + (int) (customer_id.id^(customer_id.id>>>32));
      return result;
    }
  } 
  
  protected FlightInfo getRandomFlightInfo(Connection conn) {
    int sleepMs = 10000;
    try {
      if (reproduce50962) {
        List<Struct> myflight = getFlights(true, conn);
        Struct aStruct = myflight.get(random.nextInt(myflight.size()));;
        if (myflight != null) {
          aStruct = myflight.get(random.nextInt(myflight.size()));
          return new FlightInfo((Long) aStruct.get("F_ID"),
              (Long) aStruct.get("F_AL_ID"), 
              (Long)aStruct.get("F_DEPART_AP_ID"),
              (Long)aStruct.get("F_ARRIVE_AP_ID"), 
              (Timestamp)aStruct.get("F_DEPART_TIME"));
        } else return null;
      } else {
        if (flights ==null) {
          if (random.nextInt(10) == 0) {
            synchronized(setFlightThread) {
              if (flights ==null) {
                flights = getFlights(true, conn);
                setFlightThread[0] = getMyTid();
              }
            }  
          } else {
            Log.getLogWriter().info("sleep for " + sleepMs);
            MasterController.sleepForMs(sleepMs);
            return null;
          }
        }
        
        //re_populate
        if (random.nextInt(HUNDRED) < PROB_REPOPULATE_FLIGHTINFO && setFlightThread[0] == getMyTid()) {
          List<Struct> flightInfo = getFlights(true, conn);
        
          if (flightInfo != null) flights = flightInfo; //avoid query failed due to HA or low memory
        }
          
        if (flights != null) {
          int size = flights.size();
          if (size <1) return null;
          Struct aStruct = flights.get(random.nextInt(size));
          return new FlightInfo((Long) aStruct.get("F_ID"),
              (Long) aStruct.get("F_AL_ID"), 
              (Long)aStruct.get("F_DEPART_AP_ID"),
              (Long)aStruct.get("F_ARRIVE_AP_ID"), 
              (Timestamp)aStruct.get("F_DEPART_TIME"));
        }
        else return null;
      }
      
    } catch (SQLException se) {
      if (isOfflineTest && 
          (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) { 
        log().info("Got expected Offline exception, continuing test");
      } else 
        SQLHelper.handleSQLException(se);
    }
    return null;
  }
  
  protected List<Struct> getFlights(boolean doSelect, Connection conn) throws SQLException{
    if (!doSelect) return null;
    
    String sql = "select F_ID, F_AL_ID, F_DEPART_AP_ID, F_ARRIVE_AP_ID, F_DEPART_TIME" +
        " from flight";
    
    if (logDML) Log.getLogWriter().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    return ResultSetHelper.asList(rs, false);   
  }
  
  protected long getInitNumReservations(Connection conn) throws SQLException {
    String sql = "select CFP_NUM_RESERVATIONS from CONFIG_PROFILE";
    
    if (logDML) Log.getLogWriter().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs.next()) return rs.getLong(1);
    else throw new TestException("Could not get result for " + sql); 
  }
  
  // ----------------------------------------------------------------
  // FindFlights
  // ----------------------------------------------------------------
  private boolean executeFindFlights(Connection conn) throws SQLException {
    long depart_airport_id;
    long arrive_airport_id;
    Timestamp start_date;
    Timestamp stop_date;
    
    FlightInfo flight = getRandomFlightInfo(conn);
    while (flight == null) flight = getRandomFlightInfo(conn); //may need to check if null caused by critical heap
    
    depart_airport_id = flight.getDepartAirportId();
    arrive_airport_id = flight.getArriveAirportId();
    
    Timestamp flightDate = flight.getDepartDate(startDate);
    long range = Math.round(MILLISECONDS_PER_DAY * 0.5);
    start_date = new Timestamp(flightDate.getTime() - range);
    stop_date = new Timestamp(flightDate.getTime() + range);
        
    // If distance is greater than zero, then we will also get flights from nearby airports
    long distance = -1;
    if (random.nextInt(100) < PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
      distance = DISTANCES[random.nextInt(DISTANCES.length)];
    }
    
    new FindFlight().doTxn(conn, depart_airport_id, arrive_airport_id, start_date, stop_date, distance);
    conn.commit();    
    return true;
  }

  // ----------------------------------------------------------------
  // FindOpenSeats
  // ----------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private boolean executeFindOpenSeats(Connection conn) throws SQLException {
    FlightInfo flight = getRandomFlightInfo(conn);
    while (flight==null) flight = getRandomFlightInfo(conn);
    //log().info("flightInfo" + flight);
    Long airport_depart_id = flight.getDepartAirportId();
    
    Object[][] results = new FindOpenSeats().doTxn(conn, flight.getFlightId());
    conn.commit();
    
    int rowCount = results.length;
    assert (rowCount <= FLIGHTS_NUM_SEATS) :
        String.format("Unexpected %d open seats returned for %s", rowCount, flight);

    // there is some tiny probability of an empty flight .. maybe 1/(20**150)
    // if you hit this assert (with valid code), play the lottery!
    if (rowCount == 0) return (true);
    
    // Store pending reservations in our queue for a later transaction            
    BitSet seats = getSeatsBitSet(flight);
    List<Reservation> tmp_reservations = (ArrayList<Reservation>) my_reservations.get();
    tmp_reservations.clear();    
    ArrayList<CustomerId> customerList = new ArrayList<CustomerId>();
    
    for (Object row[] : results) {
      if (row == null || random.nextInt(HUNDRED) > PROB_ADD_RESERVATION ) continue; 
      Integer seatnum = (Integer)row[1];
      Double price = (Double) row[2];
    
      // We first try to get a CustomerId based at this departure airport
      if (logDML) log().info("Looking for a random customer to fly on " + flight);
      List<Struct> customersByAP = random.nextInt(100) == 1 ? 
          getCustomersByAP(airport_depart_id, conn)
          : customerByAP.get(airport_depart_id);
      while (customersByAP == null) {
        //TODO, check whether connection failed with node failure exception
        //could return false here to let caller handle this issue
        //or use nonTxnConnection to get customer info
        customersByAP = getCustomersByAP(airport_depart_id, conn);
      }
      CustomerId customer_id = getRandomCustomerId(customersByAP);

      // We will go for a random one if:
      //  (1) The Customer is already booked on this Flight
      //  (2) We already made a new Reservation just now for this Customer
      int tries = FLIGHTS_NUM_SEATS;
      while (tries-- > 0 && (customer_id == null || customerList.contains(customer_id))) { 
        customer_id = getRandomCustomerId(conn);
        if (logDML) log().info("RANDOM CUSTOMER: " + customer_id);
      } // WHILE
      assert(customer_id != null) :
          String.format("Failed to find a unique Customer to reserve for seat #%d on %s", seatnum, flight);
      customerList.add(customer_id);
      
      Reservation r = new Reservation(getNextReservationId(getMyTid()),
                                      flight,
                                      customer_id,
                                      seatnum.intValue(),
                                      price);
      seats.set(seatnum);
      tmp_reservations.add(r);
      //if (logDML) log().info("QUEUED INSERT: " + flight + " / " + flight.getFlightId()
      //    + " -> " + customer_id);
      if (logDML) log().info("QUEUED INSERT: " + r);
      //break; //break out here so that update on flight in new reservation could avoid
             //conflict exception, as the same flight is queued here. 
    } // WHILE
  
    if (tmp_reservations.isEmpty() == false) {
      Collections.shuffle(tmp_reservations);
      synchronized(INSERT) {
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        for (int i=0; i<tmp_reservations.size(); i++) {
          if (i==0)
            cache.addFirst(tmp_reservations.get(i));
          else if (i ==1)
            cache.addLast(tmp_reservations.get(i));
          else
            cache.add(cache.size()/i, tmp_reservations.get(i));
          //avoid too many conflict exception when update on the same flight seatLeft
        }
        while (cache.size() > CACHE_LIMIT_PENDING_INSERTS) {
          cache.remove();
        } // WHILE
      
        if (logDML) log().info(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]",
                    tmp_reservations.size(), flight, cache.size()));
      }
    }
    return (true);
  }
  
  public long getNextReservationId(int id) {
    // Offset it by the client id so that we can ensure it's unique
    return (id | this.num_reservations++<<48);
  }
  
  protected CustomerId getRandomCustomerId(Connection c) {
    Connection conn = useNonTxnConn ? (Connection) gfxdNonTxnConn.get() : c;
    
    int sleepMs = 10000;
    try {
      if (customers ==null) {
        if (random.nextInt(10) == 0) {
          synchronized(customerSet) {
            if (customers ==null) customers = getCustomer(true, conn);
          }  
        } else {
          Log.getLogWriter().info("sleep for " + sleepMs);
          MasterController.sleepForMs(sleepMs);
          return null;
        }
      }
      
      //re_populate, do not execute if we use the original balance as base to verify operations
      if (random.nextInt(1000) == 0 && setFlightThread[0] == getMyTid() && !verifyBalance) {
        List<Struct> custInfo = getCustomer(true, conn);
        if (custInfo != null) customers = custInfo; //query results could be null due to HA or low memory 
      }

      if (customers != null) {
        int size = customers.size();
        if (size < 1) return null;
        Struct aStruct = customers.get(random.nextInt(size));
        return new CustomerId((Long) aStruct.get("C_ID"),
            (Long) aStruct.get("C_BASE_AP_ID"));
      }
      else return null;

      
    } catch (SQLException se) {
      if (isOfflineTest && 
          (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) { 
        log().info("Got expected Offline exception, continuing test");
      } else SQLHelper.handleSQLException(se);
    }
    return null;
  }
  
  protected List<Struct> getCustomer(boolean doSelect, Connection conn) throws SQLException{
    if (!doSelect) return null;
    
    String sql = "select C_ID, C_BASE_AP_ID, C_BALANCE from customer";
    
    if (logDML) Log.getLogWriter().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    return ResultSetHelper.asList(rs, false);   
  }
  
  protected List<Struct> getCustomerInitBalance(Connection conn) throws SQLException{
    String sql = "select C_ID, C_BALANCE from customer order by c_id";
    
    if (logDML) Log.getLogWriter().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (rs == null && setCriticalHeap) {
      boolean[] getCanceled = (boolean[]) SQLTest.getCanceled.get();
      if (getCanceled[0]) {
        if (ddlThread != -1)
          increaseCriticalHeapPercentage();
        else MasterController.sleepForMs(10000); //10 seconds
        
        getCanceled[0] = false;
        SQLTest.getCanceled.set(getCanceled);
        return null;
      }
    }
    return ResultSetHelper.asList(rs, false);   
  }
  
  protected CustomerId getRandomCustomerId(List<Struct> list) throws SQLException{      
    if (list!=null && list.size() > 0) {
      Struct aStruct = list.get(random.nextInt(list.size()));
      return new CustomerId((Long) aStruct.get("C_ID"),
          (Long) aStruct.get("C_BASE_AP_ID"));
    } else return null;    
  }
  
  protected List<Struct> getCustomersByAP(long airport_depart_id, Connection c) throws SQLException{    
    Connection conn = useNonTxnConn ? (Connection) gfxdNonTxnConn.get() : c;

    String sql = "select C_ID, C_BASE_AP_ID from customer where C_BASE_AP_ID = " + airport_depart_id;
    
    if (logDML) Log.getLogWriter().info(sql);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    List<Struct> list = ResultSetHelper.asList(rs, false);   
    
    return list;
  }
  
  
  
  //check the customerId is not in the reservation table
  //not enforced in the create table ddl
  protected boolean checkCustomer(Isolation isolation, long flight_id, long customer_id) 
  throws SQLException{
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, isolation);
    String sql = "select R_C_ID from RESERVATION where R_F_ID = " + flight_id 
        + " and R_C_ID = " + customer_id;
    
    ResultSet rs = conn.createStatement().executeQuery(sql);
    if (!rs.next()) return false;
    else {
      if (rs.next()) throw new TestException("A customer( " + customer_id + 
          ") reservered more than once on the the same flight " + flight_id);
      else return true;
    }
  }
  
  // ----------------------------------------------------------------
  // NewReservation
  // ----------------------------------------------------------------
  
  private boolean executeNewReservation(Connection conn) throws SQLException{
    boolean success = false;
    try {
      success = makeNewReservation(conn);
    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        log().info("Get expected conflict exception, continue testing");
      } else if (se.getSQLState().equals("23505")) {
        log().info("Get expected duplicate key exception, continue testing");
        //due to update or insert from another client jvm
      } else throw se;
    }
    return success;
  }
  
  private boolean makeNewReservation(Connection conn) throws SQLException {
      Reservation reservation = null;
      BitSet seats = null;
      boolean success = false;
      
      while (reservation == null) {
        Reservation r = null;
        synchronized(INSERT) {
          LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
          if (cache == null) throw new TestException("Unexpected " + CacheType.PENDING_INSERTS);
          if (logDML)
            log().info(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]",
                                    cache.size()));
          r = cache.poll();
        } 
        if (r == null) {
          if (logDML) log().warning("Unable to execute NewReservation - No available reservations to insert");
          break;
        }
        
        seats = getSeatsBitSet(r.flightInfo);
        
        if (isFlightFull(seats)) {
          if (logDML) log().info(String.format("%s is full", r.flightInfo));
          continue;
        }
        else if (isCustomerBookedOnFlight(r.customer_id, r.flightInfo)) {
          if (logDML) log().info(String.format("%s is already booked on %s", r.customer_id, r.flightInfo));
          continue;
        }
        reservation = r; 
      } // WHILE
      if (reservation == null) {
        if (logDML) log().warning("Failed to find a valid pending insert Reservation\n" + this.toString());
        return false;
      }
      
      // Generate a random price for now
      //double price = 2.0 * random.number(RESERVATION_PRICE_MIN,
      //                                RESERVATION_PRICE_MAX);

      
      double price = reservation.price;
      
      // Generate random attributes
      long attributes[] = new long[9];
      for (int i = 0; i < attributes.length; i++) {
        attributes[i] = random.nextLong();
      } // FOR
      
      if (logDML) log().info("Calling NewResveraton");
      boolean retry = true;
      int retryNum = 0;
      int maxRetries = 3;
      while (retry && retryNum<maxRetries) {
        try {
          success = new NewReservation().doTxn(conn, reservation.id,
                         reservation.customer_id.id,
                         reservation.flightInfo.flight_id,
                         (long) reservation.seatnum,
                         price,
                         attributes);
          retry = false;
          retryNum++;
        } catch (SQLException se) {
          if (se.getSQLState().equals("X0Z02")) {
            log().info("update on flight could get conflict exception, will retry");
            retry = true;
          }
          else throw se;
        }
      }
      if (!success) {
        conn.rollback(); 
        return success;
      }
      conn.commit();
      
      // Mark this seat as successfully reserved
      seats.set(reservation.seatnum);
      
      // Set it up so we can play with it later
      this.requeueReservation(reservation);
      
      return success;
  }

  /**
   * Returns true if the given Customer already has a reservation booked on the target Flight
   * @param customer_id
   * @param flight_id
   * @return
   */
  protected boolean isCustomerBookedOnFlight(CustomerId customer_id, FlightInfo flight_id) {
      Set<FlightInfo> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
      return (flights != null && flights.contains(flight_id));
  }
  
  /**
   * Take an existing Reservation that we know is legit and randomly decide to 
   * either queue it for a later update or delete transaction 
   * @param r
   */
  protected void requeueReservation(Reservation r) {
    CacheType ctype = null;
    
    if (random.nextBoolean()) {
      ctype = CacheType.PENDING_DELETES;
    } else {
      ctype = CacheType.PENDING_UPDATES;
    }
    assert(ctype != null);
    
    

    if (ctype == CacheType.PENDING_DELETES) {
      synchronized(DELETE) {
        addToQueue(ctype, r);
      }
    } else {
      synchronized(UPDATE) {
        addToQueue(ctype, r);
      }
    }
  }
  
  private void addToQueue(CacheType ctype, Reservation r) {
    LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
    if (cache == null) throw new TestException("Unexpected " + ctype);
    cache.add(r);
    if (logDML) log().info(String.format("Queued %s for %s [cache=%d]", r, ctype, cache.size()));
    
    while (cache.size() > ctype.limit) {
      cache.remove();
    }
  }
  
  private boolean executeUpdateReservation(Connection conn) throws SQLException{
    boolean success = false;
    try {
      success = updateReservation(conn);

    } catch (SQLException se) {
      if (se.getSQLState().equals("23505")) {
        log().info("Get expected duplicate key exception, continue testing");

      } else if (se.getSQLState().equals("X0Z02")) {
        log().info("Get expected conflict exception, continue testing");
        //concurrent update on reservation could leads to this
        //seatNo and r_f_id has unique key constraint.
        //txn1 update will hold lock on the key to block another update instead of 23505
      } else throw se;

    }
    return success;
  }
  
  private boolean updateReservation(Connection conn) throws SQLException {

    
    if (logDML) log().info("Let's look for a Reservation that we can update");
    
    // Pull off the first pending seat change and throw that ma at the server
    Reservation r = null;
    synchronized(UPDATE) {
      LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
      if (cache == null) { 
        throw new TestException("Unexpected " + CacheType.PENDING_UPDATES);
      }
      r = cache.poll();
      
      if (r == null) {
        if (logDML) log().info(String.format("Failed to find Reservation to update [cache=%d]", cache.size()));
        return (false);
      }
    } 
       
    if (logDML) log().info("Ok let's try to update " + r);
    
    long value = random.nextInt(1<<20);
    long attribute_idx = random.nextInt(UpdateReservation.NUM_UPDATES);
    int newseatnum = random.nextInt(FLIGHTS_NUM_SEATS);

    if (logDML) log().info("Calling UpdateReservation");
    new UpdateReservation().doTxn(conn, r.id,
                   r.flightInfo.getFlightId(),
                   r.customer_id.getId(),
                   r.seatnum,
                   newseatnum,
                   attribute_idx,
                   value);
    conn.commit();
    
    //here to add method to verify update is successful 
    //as this thread is holding the reservation for the newseatnum, and no other txns could
    //change the seatnum in the reservation or update to the newseatnum due to unique constrant
    
    r.setSeatnum(newseatnum);
    this.requeueReservation(r);
    return (true);
  }

  private boolean executeDeleteReservation(Connection conn) throws SQLException{
    boolean success = false;
    try {
      success = deleteReservation(conn);

    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        log().info("Get expected conflict exception, continue testing");
      } else throw se;

    }
    return success;
  }
  
  private boolean deleteReservation(Connection conn) throws SQLException {
    if (logDML) log().info("Let's look for a Reservation that we can delete");
    
    Reservation r = null;
    synchronized(DELETE){
      LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES);
      if (cache == null) {
        throw new TestException("Unexpected " + CacheType.PENDING_DELETES);
      }
      r = cache.poll();
      
      if (r == null) {
        if (logDML) log().info(String.format("Failed to find Reservation to delete [cache=%d]", cache.size()));
        return (false);
      }
    } 

    if (logDML) log().info("Ok let's try to delete " + r);
    
    int rand = random.nextInt(HUNDRED);
    
    // Parameters
    long f_id = r.flightInfo.getFlightId();
    Long c_id = null;
    String c_id_str = null;
    String ff_c_id_str = null;
    Long ff_al_id = null;
    long r_seatnum = r.seatnum;
    
    // Delete with the Customer's id as a string 
    if (rand < PROB_DELETE_WITH_CUSTOMER_ID_STR) {
      c_id_str = Long.toString(r.customer_id.getId());
    }
    // Delete using their FrequentFlyer information
    else if (rand < PROB_DELETE_WITH_CUSTOMER_ID_STR + PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
      ff_c_id_str = Long.toString(r.customer_id.getId());
      ff_al_id = r.flightInfo.getAlId();
    }
    // Delete using their Customer id
    else {
      c_id = r.customer_id.getId();
    }
    
    if (logDML) log().info("Calling deleteReservation txn");
    boolean success = new DeleteReservation().doTxn(conn, f_id, c_id, c_id_str, ff_c_id_str, ff_al_id, r_seatnum);
    if (!success) {
      conn.rollback();
      log().info("rollback the connection");
      return false;
    }
        
    conn.commit();
    
    // We can remove this from our set of full flights because know that there is now a free seat
    BitSet seats = getSeatsBitSet(r.flightInfo);
    seats.set(r.seatnum, false);
  
    // And then put it up for a pending insert
    if (random.nextInt(HUNDRED) < PROB_REQUEUE_DELETED_RESERVATION) {
      synchronized(INSERT) {
        CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS).add(r);
      }
    }

    return true;
    
  }
  
  private boolean executeUpdateCustomer(Connection conn) throws SQLException{
    boolean success = false;
    try {
      success = updateCustomer(conn);

    } catch (SQLException se) {
      if (se.getSQLState().equals("X0Z02")) {
        log().info("Get expected conflict exception, continue testing");
      } else throw se;

    }
    return success;
  }
  
  private boolean updateCustomer(Connection conn) throws SQLException {
    // Pick a random customer and then have at it!
    CustomerId customer_id = getRandomCustomerId(conn);
    
    Long c_id = null;
    String c_id_str = null;
    long attr0 = random.nextLong();
    long attr1 = random.nextLong();
    long update_ff = (random.nextInt(HUNDRED) < PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
    
    // Update with the Customer's id as a string 
    if (random.nextInt(HUNDRED) < PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
        c_id_str = Long.toString(customer_id.getId());
    }
    // Update using their Customer id
    else {
        c_id = customer_id.getId();
    }

    if (logDML) log().info("Calling updateCustomer ");
    new UpdateCustomer().doTxn(conn, c_id, c_id_str, update_ff, attr0, attr1);
    
    conn.commit();
    
    return true;
  }
  
  public static void HydraTask_addTxIdCol() {
    if (seats == null) seats = new SeatsTest();
    seats.addTxIdCol();
  }
  
  protected void addTxIdCol() {
    if (!addTxId) return;
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    
    ArrayList<String[]> tables = getTableNames(conn);
    for (String[] table: tables) {
      if (!table[1].equals("AIRPORT") && !table[1].contains("PRICE")) //table created eralier due to 
        alterTableAddTxId(conn, table[0]+"." + table[1]);
    } 
  }
  
  public static void HydraTask_verifyBalance() {
    if (seats == null) seats = new SeatsTest();
    seats.verifyBalance();
  }
  
  protected void verifyBalance() {
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    List<Struct> calJoinBalance = null;
    List<Struct> calLeftJoinBalance = null;
    String tableName = null;
    
    //increase the heap, may need to start a new data node if the increase heap statement itself failed
    int newheap = 95;
    if (setCriticalHeap) {
      setCriticalHeapPercentage(conn, newheap, dataGroup);
      MasterController.sleepForMs(1 * MILLSECPERMIN);
    }
 
    List<Struct> priceList = null;
    String sql = null;
    try {
      /* failed due to #51075
      String sql = "select C_ID, C_BALANCE - price as C_BALANCE from customer as c, " +
      		" (select R_C_ID, sum(R_PRICE) as price from reservation group by R_C_ID) as r" +
      		" where c.c_ID = r.R_C_ID";
      if (logDML) Log.getLogWriter().info(sql);
      ResultSet rs = conn.createStatement().executeQuery(sql);
      calBalance = ResultSetHelper.asList(rs, false);	
      */
      //work around #51075 by populate the results to a global temp table
      sql = "select R_C_ID, sum(R_PRICE) as price from reservation " +
      (reproduce51113 ? "" : " where txid > 0 " ) +
      " group by R_C_ID";
      
      if (logDML) Log.getLogWriter().info(sql);
      ResultSet rs = conn.createStatement().executeQuery(sql);
      
      priceList = ResultSetHelper.asList(rs, false);
      rs.close();
      
      
      String createTempTable = "DECLARE GLOBAL TEMPORARY TABLE " + updateBalTablePrefix  + getMyTid() +
        "(R_C_ID bigint, PRICE float) NOT LOGGED";
      
      String createTable = "create table " + updateBalTablePrefix + getMyTid() +
      "(C_ID bigint, C_BALANCE float) " 
      + (usePartitionBy ? " partition by column(c_id) " : "")
      + DATAGROUPCLAUSE ;
      
      if (useTempTable) {
        log().info(createTempTable);
        conn.createStatement().execute(createTempTable);
        tableName = tempTablePrefix + updateBalTablePrefix + getMyTid();
      } else {
        if (!verifyBalanceTablesCreated) {
          if (reproduce51255) {
            log().info(createTable);
            conn.createStatement().execute(createTable);
          }          
        }
        tableName = updateBalTablePrefix + getMyTid(); 
        if (!verifyBalanceTablesCreated) {
          if (!reproduce51090) {
            String createIndex = "create index index_" + tableName + " on " + tableName + "(c_id)";
            log().info(createIndex);
            conn.createStatement().execute(createIndex);
          } 
        }
        
      }
      conn.commit();
        
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    
    try {
      if (priceList == null) throw new TestException("priceList should not be null by now");
      log().info("inserting into " + tableName + " with results of " + sql);
      insertBalIntoNewTable(conn, tableName, priceList, "R_C_ID", "PRICE");
    } catch (SQLException se) {
      if (se.getSQLState().equals(XCL54)) {
        if (reproduce51255) throw new TestException ("got ticket #51255 unexpected low memory " +
        		"exception: " + TestHelper.getStackTrace(se));
      } 
      
      SQLHelper.handleSQLException(se);
    }
    
    boolean useOrderBy = true;
    String orderByClause = " order by c.c_id";
    String join = "select c.C_ID, c.C_BALANCE - nvl(r.c_balance, 0) as C_BALANCE from customer as c join " +
    tableName + " as r" +
    " on c.c_ID = r.C_ID " +
    (useOrderBy? orderByClause: "");
    
    String leftjoin = "select c.C_ID, c.C_BALANCE - nvl(r.c_balance, 0) as C_BALANCE from customer as c left outer join " +
    tableName + " as r" +
    " on c.c_ID = r.C_ID " +
    (useOrderBy? orderByClause: "");
    
    try { 
      log().info("executing join query " + join);
      calJoinBalance = getQueryResultSet(conn, join);
      log().info("join results size is " + calJoinBalance.size());
      

      log().info("executing join query " + leftjoin);
      calLeftJoinBalance = getQueryResultSet(conn, leftjoin);
      log().info("join results size is " + calLeftJoinBalance.size());

       
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);    
    }
    
    if (useOrderBy) {
      //create a new temp table
      //insert the intial ordered balance for each customer
      //remove the customers not being updated in the test run use subquery
      
      //compare the two tables result
      try {
        if (reproduce51255 && !verifyBalanceTablesCreated) {
          createNewTableAvoidLeftJoin(conn);
        }
        verifyBalanceTablesCreated = true;
        
        //populate with init data
        String initBalTableName = initBalTablePrefix + getMyTid();  
        insertBalIntoNewTable(conn, initBalTableName, customersInitBalance, "C_ID", "C_BALANCE");
        
        removeNotUpdatedCustomerBal(conn);
         
        ResultSet newInitRs = getNewInitCustBal(conn);
        /*
        ResultSet newCustBal = conn.createStatement().executeQuery("select c_id, c_balance from customer"); 
        
        List<Struct> custBal = ResultSetHelper.asList(newCustBal, false);
        log().info("current customer bal is " + ResultSetHelper.listToString(custBal));
        */
        List<Struct> newInitCustBal = ResultSetHelper.asList(newInitRs, false);
        if (reproduce51113) return;
        if (calJoinBalance != null) ResultSetHelper.compareSortedResultSets(newInitCustBal, calJoinBalance, 
            "UpdatedInitialCustomersBalance", "CalculatedCustomerBalance");
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);    
      }
    }
    
    if (useOrderBy && calLeftJoinBalance != null) ResultSetHelper.compareSortedResultSets(customersInitBalance, calLeftJoinBalance, 
        "InitialCustomersBalance", "CalculatedLeftJoinCustomerBalance");
    
    commit(conn);
    closeGFEConnection(conn);
  }
  
  private void removeNotUpdatedCustomerBal(Connection conn) throws SQLException{
    String tableName = (useTempTable? tempTablePrefix : "") 
      + updateBalTablePrefix + getMyTid();
    String delete = "delete from " + initBalTablePrefix + getMyTid() + 
      " as i where not exists (select * from " + tableName +
      " as u where u.c_id = i.c_ID )";

    log().info(delete);
    conn.createStatement().execute(delete);
    conn.commit();
    
  }
  
  private ResultSet getNewInitCustBal(Connection conn) throws SQLException{
    String tableName =  initBalTablePrefix + getMyTid();
    String sql = "select * from " + tableName + " order by c_id";
    
    log().info(sql);
    return conn.createStatement().executeQuery(sql);
  }
  
  private void insertBalIntoNewTable(Connection conn, String tableName, List<Struct> priceList,
      String fieldName1, String fieldName2) throws SQLException{
    String insertUsingBatching = "insert into " + tableName + " values(?, ?)";
    log().info(insertUsingBatching);
    PreparedStatement stmt = conn.prepareStatement(insertUsingBatching);
    
    int[] counts = null;
    int size = priceList.size();
    log().info("inserting total size of " + size);
    
    for (int i=0 ; i<size ; i++) { 
      try {
        stmt.setObject(1, priceList.get(i).get(fieldName1));
        stmt.setObject(2, priceList.get(i).get(fieldName2));
        stmt.addBatch();
        Log.getLogWriter().info("batch insert into " + tableName +
            " with " + fieldName1 +":" + priceList.get(i).get(fieldName1) 
            + "," + fieldName2 + ":" + priceList.get(i).get(fieldName2));
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);    
      }
      if (isEdge && i% 65534 == 0) {
        log().info("executing batch statement");
        counts = stmt.executeBatch(); //work around #51362
        conn.commit();
      }
    }
    
    try {
      log().info("executing last batch statement to finish insert");
      counts = stmt.executeBatch(); 
      conn.commit();
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    } 
  }
  
  private void createNewTableAvoidLeftJoin(Connection conn) {
    try {
      String createTable = "create table "+ initBalTablePrefix + getMyTid() +
      "(C_ID bigint, C_BALANCE float) partition by column (c_id) "
      + (usePartitionBy ? " colocate with (" + updateBalTablePrefix + getMyTid() + ")" : "")
      + DATAGROUPCLAUSE ;
      
      if (!verifyBalanceTablesCreated) {
        log().info(createTable);
        conn.createStatement().execute(createTable);
      }
      String tableName = initBalTablePrefix + getMyTid();  
  
      conn.commit();
          
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);    
    }
  }
  
  
  
  private List<Struct> getQueryResultSet(Connection conn, String sql) throws SQLException{
    List<Struct> list = null;
    long start = System.currentTimeMillis();
    Log.getLogWriter().info("query starts from " + start);
    ResultSet rs = conn.createStatement().executeQuery(sql);
    long selectDone = System.currentTimeMillis();
    Log.getLogWriter().info("query result available at " + selectDone);
    
    long selectTime = selectDone - start;
    long seconds = selectTime/1000;
    long minutes = seconds/60;
    Log.getLogWriter().info("select takes " + 
        (minutes >0 ? minutes + " minutes" : seconds + " seconds" ));
    
    list = ResultSetHelper.asList(rs, false); 
    long end = System.currentTimeMillis();
    Log.getLogWriter().info("process query finishes at " + end);
 
    long queryProcessTime = end - selectDone;
    
    seconds = queryProcessTime/1000;
    minutes = seconds/60;
    Log.getLogWriter().info("process select query results takes " + 
        (minutes >0 ? minutes + " minutes" : seconds + " seconds" ));
    
    return list;
  }
  
  public static void HydraTask_setHeapPercentage() {
    if (useHeapPercentage) {
      //force eviction to kick in
      seats.setHeapPercentage();
    } else Log.getLogWriter().info("eviction heap percentage is not set");
  }
  
  protected void setHeapPercentage() {
    setHeapPercentage(initEvictionHeapPercentage);
  }
  
  protected double getEvictionHeapPercentage() {
    double heapPercent = 0;
    Connection gConn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    try {
      String sql = "select distinct sys.get_eviction_heap_percentage() from sys.members " +
      		"where servergroups = '" +
        dataGroup.toUpperCase() + "'" 
        ;
      ResultSet rs = gConn.createStatement().executeQuery(sql);
      if (rs.next()) {
        heapPercent = rs.getDouble(1);
        if (rs.next()) {
          sql = "select sys.get_eviction_heap_percentage(), id, servergroups from sys.members ";
          
          rs = gConn.createStatement().executeQuery(sql);
          
          throw new TestException("more than 1 heap percentage is set in the same server group: " + 
              sql + "\n" + ResultSetHelper.listToString(ResultSetHelper.asList(rs, false)));
        }
      } else {
        throw new TestException("test issue, heap eviction is not set in the test");
      }
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    return heapPercent;
  }
  
  protected double getCriticalHeapPercentage() {
    Connection gConn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    String sql = "select distinct sys.get_critical_heap_percentage() as criticalHeapPercentage from sys.members where servergroups = '" +
      dataGroup.toUpperCase() + "'";
    log().info(sql);
    return getCriticalHeapPercentage(gConn, sql, dataGroup.toUpperCase());
  }
  
  protected double getCriticalOffHeapPercentage() {
    Connection gConn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    String sql = "select distinct sys.get_critical_offheap_percentage() as criticalOffHeapPercentage from sys.members where servergroups = '" +
      dataGroup.toUpperCase() + "'";
    log().info(sql);
    return getCriticalHeapPercentage(gConn, sql, dataGroup.toUpperCase());
  }
  /*
  protected double getCriticalHeapPercentage(Connection gConn, String sql) {
    double heapPercent = 0;
    Connection gConn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    try {
      String sql = "select distinct sys.get_critical_heap_percentage() as criticalHeapPercentage from sys.members where servergroups = '" +
        dataGroup.toUpperCase() + "'";
      log().info(sql);
      ResultSet rs = gConn.createStatement().executeQuery(sql);
      
      List<Struct> rsList = ResultSetHelper.asList(rs, false);
      
      if (rsList != null) {
        if (rsList.size() > 1) {
          String newsql = "select sys.get_critical_heap_percentage() as criticalHeapPercentage, id, servergroups as criticalHeapPercentage from sys.members where servergroups = '" +
          dataGroup.toUpperCase() + "'";
          log().info(newsql);
          ResultSet newrs = gConn.createStatement().executeQuery(newsql);
        
          List<Struct> newrsList = ResultSetHelper.asList(newrs, false);
          
          
          if (isHATest) {
            log().warning("hit #51290, continue testing: " + 
                sql + "\n" + ResultSetHelper.listToString(ResultSetHelper.asList(rs, false)));
          }
          else throw new TestException("more than 1 critical heap percentage are set in the same server group: " + 
              sql + "\n" + ResultSetHelper.listToString(rsList) + "\n" +
              newsql + "\n" + ResultSetHelper.listToString(newrsList)); 
        } else if (rsList.size() == 1) {
          float heapPercentF = (Float) rsList.get(0).get("criticalHeapPercentage".toUpperCase());
          heapPercent = Double.parseDouble(Float.toString(heapPercentF));
        }
      }
    } catch (SQLException se) {
      if (isHATest && se.getSQLState().equals(X0Z01)) {
        log().info("got expected node failure exception during select query, continue testing");
      } else if (isOfflineTest && 
          (se.getSQLState().equals("X0Z09") || se.getSQLState().equals("X0Z08"))) { 
        log().info("Got expected Offline exception, continuing test");
      } else SQLHelper.handleSQLException(se);
    }
    return heapPercent;
  }
  */
  
  public static void HydraTask_setCriticalHeapPercentage() {
    seats.setCriticalHeapPercentage();
  }
  
  protected void setCriticalHeapPercentage() {
    if (!setCriticalHeap) {
      Log.getLogWriter().info("No critical heap is set");
      return;
    }
    ddlThread = getMyTid();
    Connection gConn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    double heapPercent = getEvictionHeapPercentage();
    
    criticalHeapPercentage = heapPercent + 10;
    setCriticalHeapPercentage(gConn, criticalHeapPercentage, dataGroup);
    
  }
  
  public static void HydraTask_increaseCriticalHeapPercentage() {
    seats.increaseCriticalHeapPercentage();
  }
  
  protected void increaseCriticalHeapPercentage() {
    if (!setCriticalHeap) {
      Log.getLogWriter().info("No critical heap is set");
      return;
    }
    
    Connection gConn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    double heapPercent = getCriticalHeapPercentage();
    
    increaseCriticalHeapPercentage(gConn, heapPercent, dataGroup);
  }
  
  /*
  protected void increaseCriticalHeapPercentage() {
    if (!setCriticalHeap) {
      Log.getLogWriter().info("No critical heap is set");
      return;
    }
    Connection gConn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.NONE);
    double heapPercent = getCriticalHeapPercentage();
    if (heapPercent > 0) log().info("critical heap percentage is " + heapPercent);
    else log().info("could not get ciritical heap percentage"); //HA, low memory etc
    
    long currentTime = System.currentTimeMillis();
    int waitMinute = 3;
    int waitTime = waitMinute * MILLSECPERMIN; //wait time to reset critical heap
    if (increaseCriticalHeapPercent && (lastCriticalHeapUpdated == 0 || 
        currentTime - lastCriticalHeapUpdated > waitTime)) {
      if (heapPercent > 0) {
        criticalHeapPercentage = heapPercent + 3;
      }
      else criticalHeapPercentage += 3;
      
      log().info("new critical percent to be set " + criticalHeapPercentage);
      if (criticalHeapPercentage <= 82) {
        setCriticalHeapPercentage(gConn, criticalHeapPercentage, dataGroup);
        if (criticalHeapPercentage<80) {
          increaseCriticalHeapPercent = false;        
        }     
        log().info("increaseCriticalHeapPercent is set to " + increaseCriticalHeapPercent);
        lastCriticalHeapUpdated = System.currentTimeMillis();
      }
      
    }
  }
  */
  //used for HA and critical heap set, so that PROBserver could count PR rebalance correctly
  public static void HydraTask_addInitialData() throws SQLException{
    if (isHATest && setCriticalHeap) {
      try {
        seats.addInitialData();
      } catch (SQLException se) {
        if (se.getSQLState().equals("XCL54") && setCriticalHeap) {
          Log.getLogWriter().info("got expected query cancellation exception, continue testing");
          increaseCriticalHeapPercent = true;
        } else SQLHelper.handleSQLException(se);
      }
    }
  }
  
  protected void addInitialData() throws SQLException {
    String tableName = "reservation_" + getMyTid();
    String sql = "put into " + tableName + 
    " select * from reservation where r_id < 200 ";
    
    log().info(sql); //use put
    Connection conn = getConnectionWithSchema(getGFEConnection(), SCHEMA_NAME, Isolation.READ_COMMITTED);
    conn.createStatement().execute(sql); //insert into table
    conn.commit();
  }
  public static synchronized void HydraTask_initEdges() {
    isEdge = true;
  }
  

}

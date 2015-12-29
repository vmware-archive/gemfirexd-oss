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
package cacheperf.comparisons.gemfirexd.useCase4;

import cacheperf.comparisons.gemfirexd.QueryPerfClient;
import cacheperf.comparisons.gemfirexd.QueryPerfException;
import cacheperf.comparisons.gemfirexd.QueryPerfPrms;
import cacheperf.comparisons.gemfirexd.useCase4.UseCase4Prms.QueryType;
import com.gemstone.gnu.trove.TIntIntHashMap;
import hydra.BasePrms;
import hydra.EnvHelper;
import hydra.FileUtil;
import hydra.MasterController;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.gemfirexd.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import objects.query.QueryPrms;

public class UseCase4Client extends QueryPerfClient {

  private static HydraThreadLocal localuseCase4stats = new HydraThreadLocal();

  protected UseCase4Stats useCase4stats;

  protected boolean logQueries;
  protected boolean logQueryResults;

  // table sizes
  protected int numAccountProfiles = UseCase4Prms.getRowsInAccountProfile();
  protected int numAccounts = UseCase4Prms.getRowsInAccount();
  protected int numHoldings = UseCase4Prms.getRowsInHolding();
  protected int numOrders = UseCase4Prms.getRowsInOrders();
  protected int numQuotes = UseCase4Prms.getRowsInQuote();

  private static int[] listOfAccounts = null;
  private static int[] listOfOrderIds = null;
  private static int[] listOfOrderAccAccId = null;

  protected PreparedStatement holdingAggPS = null;
  protected PreparedStatement portSummPS = null;
  protected PreparedStatement mktSummPS = null;
  protected PreparedStatement holdingCountPS = null;
  protected PreparedStatement uptClosedOrderSelfJoinPS = null;
  protected PreparedStatement uptClosedOrderPS = null;
  protected PreparedStatement findOrderByStatusPS = null;
  protected PreparedStatement findOrderByAccAccIdPS = null;
  protected PreparedStatement findOrderIdAndAccAccIdPS = null;
  protected PreparedStatement findOrderCntAccAccIdPS = null;
  protected PreparedStatement findOrderCntAccAccIdAndStatusPS = null;

//------------------------------------------------------------------------------
// load data
//------------------------------------------------------------------------------

  public static void loadDataTask() throws SQLException {
    UseCase4Client c = new UseCase4Client();
    c.initialize();
    if (c.ttgid == 0) {
      c.loadData();
    }
  }

  private void loadData() throws SQLException {
    long start;
    int rows;

    start = this.useCase4stats.startQuote();
    rows = loadRows("Quote", getDataFileNameForQuote(), 6);
    this.useCase4stats.endQuote(start, rows);

    start = this.useCase4stats.startHolding();
    rows = loadRows("Holding", getDataFileNameForHolding(), 6);
    this.useCase4stats.endHolding(start, rows);

    start = this.useCase4stats.startAccountProfile();
    rows = loadRows("AccountProfile", getDataFileNameForAccountProfile(), 6);
    this.useCase4stats.endAccountProfile(start, rows);

    start = this.useCase4stats.startAccount();
    rows = loadRows("Account", getDataFileNameForAccount(), 6);
    this.useCase4stats.endAccount(start, rows);

    start = this.useCase4stats.startOrder();
    rows = loadRows("Orders", getDataFileNameForOrders(), 6);
    this.useCase4stats.endOrder(start, rows);
  }

  private int loadRows(String table, String dataFileName, int threads) throws SQLException {
    int rows = 0;
    Log.getLogWriter().info("Loading data for " + table + "...");
    String stmt = "CALL SYSCS_UTIL.IMPORT_TABLE_EX('APP', '"
                + table + "', '"
                + dataFileName + "', ',', NULL, NULL, 0, 0, "
                + threads + ", 0, NULL, NULL)";
    this.connection.createStatement().execute(stmt);
    ResultSet rs = this.connection.createStatement().executeQuery("select count(*) from app." + table);
    if (rs.next()) {
      rows = rs.getInt(1);
    } else {
      String s = "Unable to count rows in " + table + ".";
      throw new QueryPerfException(s);
    }
    rs.close();
    Log.getLogWriter().info("Done with data load for " + table + " (" + rows + " rows).");
    return rows;
  }

  private String getDataFileNameForAccount() {
    String fn = UseCase4Prms.getDataFileNameForAccount();
    String newfn = EnvHelper.expandEnvVars(fn);
    Log.getLogWriter().info("Data file for Account: " + fn);
    return newfn;
  }

  private String getDataFileNameForAccountProfile() {
    String fn = UseCase4Prms.getDataFileNameForAccountProfile();
    String newfn = EnvHelper.expandEnvVars(fn);
    Log.getLogWriter().info("Data file for AccountProfile: " + fn);
    return newfn;
  }

  private String getDataFileNameForHolding() {
    String fn = UseCase4Prms.getDataFileNameForHolding();
    String newfn = EnvHelper.expandEnvVars(fn);
    Log.getLogWriter().info("Data file for Holding: " + fn);
    return newfn;
  }

  private String getDataFileNameForOrders() {
    String fn = UseCase4Prms.getDataFileNameForOrders();
    String newfn = EnvHelper.expandEnvVars(fn);
    Log.getLogWriter().info("Data file for Orders: " + fn);
    return newfn;
  }

  private String getDataFileNameForQuote() {
    String fn = UseCase4Prms.getDataFileNameForQuote();
    String newfn = EnvHelper.expandEnvVars(fn);
    Log.getLogWriter().info("Data file for Quote: " + fn);
    return newfn;
  }

//------------------------------------------------------------------------------
// init account data
//------------------------------------------------------------------------------

  /**
   * TASK to initialize the account information.
   */
  public static void initAccountsTask() throws SQLException {
    UseCase4Client c = new UseCase4Client();
    c.initialize();
    if (c.jid == 0) {
      c.initAccounts();
    }
  }

  private void initAccounts() throws SQLException {
    //ResultSet r = this.connection.createStatement()
    //                  .executeQuery("select count(*) from app.holding");
    ResultSet r = this.connection.createStatement()
                      .executeQuery("select count(*) from app.account");
    r.next();
    int totalAccounts = r.getInt(1);
    r.close();
    if (totalAccounts == 0) {
      String s = "No accounts found";
      throw new QueryPerfException(s);
    }

    listOfAccounts = new int[totalAccounts];

    Log.getLogWriter().info("Caching " + totalAccounts
                         + " APP.HOLDING.ACCOUNT_ACCOUNTID information.");
    //r = this.connection.createStatement()
    //        .executeQuery("select distinct account_accountid from app.holding");
    r = this.connection.createStatement()
            .executeQuery("select accountid from app.account");
    for (int i = 0; i < listOfAccounts.length; i++) {
      r.next();
      listOfAccounts[i] = r.getInt(1);
    }
    r.close();

    TIntIntHashMap orderIdAccId = new TIntIntHashMap();

    r = this.connection.createStatement().executeQuery("select orderid, account_accountid from app.orders");
    while(r.next()) {
      orderIdAccId.put(r.getInt(1), r.getInt(2));
    }
    r.close();

    listOfOrderIds = orderIdAccId.keys();
    listOfOrderAccAccId = orderIdAccId.getValues();
  }

//------------------------------------------------------------------------------
// main query workload
//------------------------------------------------------------------------------

  /**
   * TASK to execute the queries.
   */
  public static void runQueriesTask() throws SQLException {
    UseCase4Client c = new UseCase4Client();
    c.initialize(QUERIES);
    c.runQueries();
  }

  private void runQueries() throws SQLException {
    do {
      executeTaskTerminator(); // does not commit
      executeWarmupTerminator(); // does not commit
      enableQueryPlanGeneration();

      runQuery();

      disableQueryPlanGeneration();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount; // not really needed
    } while (!executeBatchTerminator()); // does not commit
  }

  private void runQuery() throws SQLException {
    QueryType queryType = UseCase4Prms.getQueryType();
    int results = 0;
    int acc = listOfAccounts[this.rng.nextInt(listOfAccounts.length-1)];
    long start = this.useCase4stats.startQuery();
    switch (queryType) {
      case holdingAgg:
        results = holdingAgg(acc);
        break;
      case portSumm:
        results = portSumm(acc);
        break;
      case mktSumm:
        results = mktSumm(acc);
        break;
      case holdingCount:
        results = holdingCount(acc);
        break;
      case uptClosedOrderSelfJoin:
        results = uptClosedOrderSelfJoin(acc);
        break;
      case uptClosedOrder:
        results = uptClosedOrder(acc);
        break;
      case findOrderByStatus:
        results = findOrderByStatus(acc);
        break;
      case findOrderByAccAccId:
        results = findOrderByAccAccId(acc);
        break;
      case findOrderIdAndAccAccId:
        results = findOrderIdAndAccAccId(acc);
        break;
      case findOrderCntAccAccId:
        results = findOrderCntAccAccId(acc);
        break;
      case findOrderCntAccAccIdAndStatus:
        results = findOrderCntAccAccIdAndStatus(acc);
        break;
      case mixed:
        results = mixed(acc);
        break;
      default:
        throw new QueryPerfException("Should not happen");
    }
    this.useCase4stats.endQuery(queryType, start, results, this.histogram);
  }

//------------------------------------------------------------------------------
// holdingAgg
//------------------------------------------------------------------------------

  private static final String holdingAggStr = "SELECT h.quote_symbol, sum(q.price * h.quantity) - SUM(h.purchaseprice * h.quantity) as gain FROM app.Holding h, app.Quote q where h.account_accountid = ? and h.quote_symbol=q.symbol GROUP BY h.quote_symbol HAVING SUM(q.price * h.quantity) - SUM(h.purchaseprice * h.quantity) > 0 ORDER BY gain desc";

  private int holdingAgg(int acc) throws SQLException {
    int results = 0;
    if (holdingAggPS == null) {
      holdingAggPS = this.connection.prepareStatement(holdingAggStr);
    }
    holdingAggPS.setInt(1, acc);
    ResultSet rs = holdingAggPS.executeQuery();
    while (rs.next()) {
      rs.getString(1);
      rs.getFloat(2);
      ++results;
    }
    return results;
  }

//------------------------------------------------------------------------------
// portSumm
//------------------------------------------------------------------------------

  private static final String portSummStr = "SELECT SUM(h.purchaseprice * h.quantity) as purchaseBasis, sum(q.price * h.quantity) as marketValue, count(*) FROM app.Holding h, app.Quote q where h.account_accountid =? and h.quote_symbol=q.symbol ORDER BY marketValue desc";

  private int portSumm(int acc) throws SQLException {
    int results = 0;
    if (portSummPS == null) {
      portSummPS = this.connection.prepareStatement(portSummStr);
    }
    portSummPS.setInt(1, acc);
    ResultSet rs = portSummPS.executeQuery();
    while (rs.next()) {
      rs.getFloat(1);
      rs.getFloat(2);
      rs.getInt(3);
      ++results;
    }
    return results;
  }

//------------------------------------------------------------------------------
// mktSumm
//------------------------------------------------------------------------------

  private static final String mktSummStr = "SELECT SUM(q.price)/count(*) as tradeStockIndexAverage, SUM(q.open1)/count(*) as tradeStockIndexOpenAverage, SUM(q.volume) as tradeStockIndexVolume, COUNT(*) as cnt , SUM(q.change1) FROM app.Quote q";

  private int mktSumm(int acc) throws SQLException {
    int results = 0;
    if (mktSummPS == null) {
      mktSummPS = this.connection.prepareStatement(mktSummStr);
    }
    ResultSet rs = mktSummPS.executeQuery();
    while (rs.next()) {
      rs.getFloat(1);
      rs.getFloat(2);
      rs.getFloat(3);
      rs.getInt(4);
      rs.getFloat(5);
      ++results;
    }
    return results;
  }

//------------------------------------------------------------------------------
// holdingCount
//------------------------------------------------------------------------------

  private static final String holdingCountStr = "SELECT count(*) FROM app.Holding h WHERE h.account_Accountid = ?";

  private int holdingCount(int acc) throws SQLException {
    int results = 0;
    if (holdingCountPS == null) {
      holdingCountPS = this.connection.prepareStatement(holdingCountStr);
    }
    holdingCountPS.setInt(1, acc);
    ResultSet rs = holdingCountPS.executeQuery();
    while (rs.next()) {
      rs.getInt(1);
      ++results;
    }
    return results;
  }

//------------------------------------------------------------------------------
// uptClosedOrderSelfJoin
//------------------------------------------------------------------------------

  private static final String uptClosedOrderSelfJoinStr = "UPDATE app.Orders o SET o.orderstatus = 'completed' WHERE o.account_accountid = ? AND o.orderid IN (SELECT o2.orderid FROM app.Orders o2 WHERE o2.orderstatus = 'closed' AND o2.account_accountid = ?)";

  private int uptClosedOrderSelfJoin(int acc) throws SQLException {
    if (uptClosedOrderSelfJoinPS == null) {
      uptClosedOrderSelfJoinPS = this.connection.prepareStatement(uptClosedOrderSelfJoinStr);
    }
    uptClosedOrderSelfJoinPS.setInt(1, acc);
    uptClosedOrderSelfJoinPS.setInt(2, acc);
    uptClosedOrderSelfJoinPS.execute();
    return 1;
  }

//------------------------------------------------------------------------------
// uptClosedOrder
//------------------------------------------------------------------------------

  private static final String uptClosedOrderStr = "UPDATE app.Orders o SET o.orderstatus = 'completed' WHERE o.account_accountid = ? AND o.orderstatus = 'closed'";

  private int uptClosedOrder(int acc) throws SQLException {
    if (uptClosedOrderPS == null) {
      uptClosedOrderPS = this.connection.prepareStatement(uptClosedOrderStr);
    }
    uptClosedOrderPS.setInt(1, acc);
    uptClosedOrderPS.execute();
    return 1;
  }

//------------------------------------------------------------------------------
// findOrderByStatus
//------------------------------------------------------------------------------

  private static final String findOrderByStatusStr = "SELECT o.* FROM app.Orders o WHERE o.orderstatus = ? AND o.account_accountid = ? order by orderid DESC";

  private int findOrderByStatus(int acc) throws SQLException {
    int results = 0;
    if (findOrderByStatusPS == null) {
      findOrderByStatusPS = this.connection.prepareStatement(findOrderByStatusStr);
    }
    findOrderByStatusPS.setString(1, "open");
    findOrderByStatusPS.setInt(2, acc);
    findOrderByStatusPS.executeQuery();
    return 1;
  }

//------------------------------------------------------------------------------
// findOrderByAccAccId
//------------------------------------------------------------------------------

  private static final String findOrderByAccAccIdStr = "SELECT o.* FROM app.Orders o WHERE o.account_accountid = ? order by orderid DESC";

  private int findOrderByAccAccId(int acc) throws SQLException {
    if (findOrderByAccAccIdPS == null) {
      findOrderByAccAccIdPS = this.connection.prepareStatement(findOrderByAccAccIdStr);
    }
    findOrderByAccAccIdPS.setInt(1, acc);
    findOrderByAccAccIdPS.executeQuery();
    return 1;
  }

//------------------------------------------------------------------------------
// findOrderIdAndAccAccId
//------------------------------------------------------------------------------

  private static final String findOrderIdAndAccAccIdStr = "SELECT o.* FROM app.Orders o WHERE o.orderid = ? AND o.account_accountid = ?";

  private int findOrderIdAndAccAccId(int acc) throws SQLException {
    if (findOrderIdAndAccAccIdPS == null) {
      findOrderIdAndAccAccIdPS = this.connection.prepareStatement(findOrderIdAndAccAccIdStr);
    }
    final int qOrderIdx = this.rng.nextInt(listOfOrderIds.length-1);
    findOrderIdAndAccAccIdPS.setInt(1, listOfOrderIds[qOrderIdx]);
    findOrderIdAndAccAccIdPS.setInt(2, listOfOrderAccAccId[qOrderIdx]);
    findOrderIdAndAccAccIdPS.executeQuery();
    return 1;
  }

//------------------------------------------------------------------------------
// findOrderCntAccAccId
//------------------------------------------------------------------------------

  private static final String findOrderCntAccAccIdStr = "SELECT count(*) FROM app.Orders o WHERE o.account_accountid = ?";

  private int findOrderCntAccAccId(int acc) throws SQLException {
    int results = 0;
    if (findOrderCntAccAccIdPS == null) {
      findOrderCntAccAccIdPS = this.connection.prepareStatement(findOrderCntAccAccIdStr);
    }
    findOrderCntAccAccIdPS.setInt(1, acc);
    ResultSet rs = findOrderCntAccAccIdPS.executeQuery();
    while (rs.next()) {
      rs.getInt(1);
      ++results;
    }
    return results;
  }

//------------------------------------------------------------------------------
// findOrderCntAccAccIdAndStatus
//------------------------------------------------------------------------------

  private static final String findOrderCntAccAccIdAndStatusStr = "SELECT count(*) FROM app.Orders o WHERE o.account_accountid  = ? and o.orderstatus = ?";

  private int findOrderCntAccAccIdAndStatus(int acc) throws SQLException {
    int results = 0;
    if (findOrderCntAccAccIdAndStatusPS == null) {
      findOrderCntAccAccIdAndStatusPS = this.connection.prepareStatement(findOrderCntAccAccIdAndStatusStr);
    }
    findOrderCntAccAccIdAndStatusPS.setInt(1, acc);
    findOrderCntAccAccIdAndStatusPS.setString(2, "open");
    ResultSet rs = findOrderCntAccAccIdAndStatusPS.executeQuery();
    while (rs.next()) {
      rs.getInt(1);
      ++results;
    }
    return results;
  }

//------------------------------------------------------------------------------
// mixed
//------------------------------------------------------------------------------

  private int mixed(int acc) throws SQLException {
    throw new UnsupportedOperationException("TBD");
  }

//------------------------------------------------------------------------------
// statistics task
//------------------------------------------------------------------------------

  /**
   * TASK to register the performance statistics object.
   */
  public static void openStatisticsTask() {
    UseCase4Client c = new UseCase4Client();
    c.openStatistics();
  }

  private void openStatistics() {
    this.useCase4stats = getUseCase4Stats();
    if (this.useCase4stats == null) {
      this.useCase4stats = UseCase4Stats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
    setUseCase4Stats(this.useCase4stats);
  }
  
  /** 
   * TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    UseCase4Client c = new UseCase4Client();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }
  
  protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.useCase4stats != null) {
      RemoteTestModule.closeClockSkewStatistics();
      this.useCase4stats.close();
    }
  }

//------------------------------------------------------------------------------
// ddl
//------------------------------------------------------------------------------

  public static void executeDDLTask()
  throws FileNotFoundException, IOException, SQLException {
    UseCase4Client c = new UseCase4Client();
    c.initialize();
    if (c.sttgid == 0) {
      c.executeDDL();
    }
  }

  private void executeDDL()
  throws FileNotFoundException, IOException, SQLException {
    String fn = getDDLFileName();
    List<String> stmts = getDDLStatements(fn);
    for (String stmt : stmts) {
      // @todo verify data balance without configuring buckets
      Log.getLogWriter().info("Creating table: " + stmt);
      execute(stmt, this.connection);
      commitDDL();
    }
  }

  private String getDDLFileName() {
    String fn = UseCase4Prms.getDDLFileName();
    String newfn = EnvHelper.expandEnvVars(fn);
    Log.getLogWriter().info("DDL file: " + fn);
    return newfn;
  }

  private List<String> getDDLStatements(String fn)
  throws FileNotFoundException, IOException {
    Log.getLogWriter().info("Reading statements from " + fn);
    String text = FileUtil.getText(fn).trim();
    StringTokenizer tokenizer = new StringTokenizer(text, ";", false);
    List<String> stmts = new ArrayList();
    while (tokenizer.hasMoreTokens()) {
      String stmt = tokenizer.nextToken().trim();
      stmts.add(stmt);
    }
    Log.getLogWriter().info("Read statements: " + stmts);
    return stmts;
  }

//------------------------------------------------------------------------------
// drop tables
//------------------------------------------------------------------------------

  public static void dropTablesTask()
  throws SQLException {
    UseCase4Client c = new UseCase4Client();
    c.initialize();
    if (c.queryAPI != QueryPrms.GFXD && c.sttgid == 0) {
      c.dropTables();
    }
  }

  private void dropTables()
  throws SQLException {
    dropTable("quotes");
    dropTable("holding");
    dropTable("orders");
    dropTable("account");
    dropTable("accountprofile");
  }

  private void dropTable(String table)
  throws SQLException {
    String stmt = "drop table if exists " + table;
    execute(stmt, this.connection);
    commitDDL();
  }

//------------------------------------------------------------------------------
// execution support
//------------------------------------------------------------------------------

  private ResultSet execute(String stmt, Connection conn)
  throws SQLException {
    if (this.logQueries) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    } 
    ResultSet rs = null;
    Statement s = conn.createStatement();
    boolean result = s.execute(stmt);
    if (result == true) {
      rs = s.getResultSet();
    }
    if (this.logQueries) {
      Log.getLogWriter().info("Executed: " + stmt + " on: " + conn);
    }
    s.close();
    return rs;
  }

  private void execute(PreparedStatement stmt)
  throws SQLException {
    if (this.logQueries) {
      Log.getLogWriter().info("Executing: " + stmt);
    }
    stmt.execute();
    if (this.logQueries) {
      Log.getLogWriter().info("Executed: " + stmt);
    }
  }

  private void commitDDL() {
    if (this.queryAPI != QueryPrms.GFXD) {
      try {
        this.connection.commit();
      } catch (SQLException e) {
        throw new QueryPerfException("Commit failed: " + e);
      }
    } // GFXD does not need to commit DDL
  }

//------------------------------------------------------------------------------
// hydra thread locals
//------------------------------------------------------------------------------

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.useCase4stats = getUseCase4Stats();
  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    setUseCase4Stats(this.useCase4stats);
  }

  /**
   * Gets the per-thread UseCase4Stats wrapper instance.
   */
  protected UseCase4Stats getUseCase4Stats() {
    UseCase4Stats useCase4stats = (UseCase4Stats)localuseCase4stats.get();
    return useCase4stats;
  }

  /**
   * Sets the per-thread UseCase4Stats wrapper instance.
   */
  protected void setUseCase4Stats(UseCase4Stats useCase4stats) {
    localuseCase4stats.set(useCase4stats);
  }

//------------------------------------------------------------------------------
// OVERRIDDEN METHODS
//------------------------------------------------------------------------------

  protected void initLocalParameters() {
    super.initLocalParameters();

    this.logQueries = QueryPrms.logQueries();
    this.logQueryResults = QueryPrms.logQueryResults();
  }

  protected boolean getLogQueries() {
    return this.logQueries;
  }

  protected void setLogQueries(boolean b) {
    this.logQueries = b;
  }
}

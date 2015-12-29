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
package cacheperf.comparisons.gemfirexd.useCase1;

import cacheperf.CachePerfPrms;
import cacheperf.comparisons.gemfirexd.*;
import cacheperf.comparisons.gemfirexd.useCase1.UseCase1Stats.Stmt;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchingInfo;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import hydra.*;
import hydra.blackboard.SharedCounters;
import hydra.gemfirexd.*;
import hydra.gemfirexd.NetworkServerHelper.Endpoint;
import java.io.*;
import java.rmi.RemoteException;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Vector;
import objects.query.QueryPrms;
import sql.SQLHelper;
import sql.sqlutil.ResultSetHelper;
import util.TestHelper;

public class UseCase1Client extends QueryPerfClient {

  private static final String MATCH_CALL = "{CALL SEC_OWNER.matchStoredProc(?, ?, ?)}";
  private static final String PURGE_CALL = "{CALL SEC_OWNER.boPurgeStoredProc()}";

  private static final String IPAY = "IPAY";
  private static final String CLIENT_ACCOUNT = "CLIENT_ACCOUNT";
  private static final String SECT_CHANNEL_DATA = "SECT_CHANNEL_DATA";
  private static final String SECL_BO_DATA_STATUS_HIST = "SECL_BO_DATA_STATUS_HIST";

  protected static final String CONFLICT_STATE = "X0Z02";
  protected static final String DEADLOCK_STATE = "ORA-00060"; // oracle table lock
  protected static final String DUPLICATE_STR = "The statement was aborted because it would have caused a duplicate key value";

  protected static final int numBuckets = UseCase1Prms.getNumBuckets();
  protected static final boolean timeStmts = UseCase1Prms.timeStmts();

  // trim intervals
  protected static final int LOAD = 16823492;
  protected static final int ETLGEN = 16823493;
  protected static final int DUMMY = 16823494;
  protected static final int MATCH = 16823495;
  protected static final int MATCHPURGE = 16823496;
  protected static final int INBOUND = 16823497;
  protected static final int OUTBOUND = 16823498;

  protected static final String LOAD_NAME = "load";
  protected static final String ETLGEN_NAME = "etlgen";
  protected static final String DUMMY_NAME = "dummy";
  protected static final String MATCH_NAME = "match";
  protected static final String MATCHPURGE_NAME = "matchpurge";
  protected static final String INBOUND_NAME = "inbound";
  protected static final String OUTBOUND_NAME = "outbound";

  // hydra thread locals
  public static HydraThreadLocal localuseCase1stats = new HydraThreadLocal();

  private static List<String> clientAccounts = null;
  private static final Object clientAccountsLock = new Object();

  private static List<MatchCriteria> matchCriterias = null;
  private static final Object matchCriteriasLock = new Object();

  public UseCase1Stats useCase1stats; // statistics

  private boolean frontloaded = false;

  protected boolean logQueries;
  protected boolean logQueryResults;

//------------------------------------------------------------------------------
// statistics task
//------------------------------------------------------------------------------

  /**
   *  TASK to register the useCase1 performance statistics object.
   */
  public static void openStatisticsTask() {
    UseCase1Client c = new UseCase1Client();
    c.openStatistics();
  }

  private void openStatistics() {
    this.useCase1stats = getUseCase1Stats();
    if (this.useCase1stats == null) {
      this.useCase1stats = UseCase1Stats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
    setUseCase1Stats(this.useCase1stats);
  }

  /**
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    UseCase1Client c = new UseCase1Client();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }

  protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.useCase1stats != null) {
      RemoteTestModule.closeClockSkewStatistics();
      this.useCase1stats.close();
    }
  }

//------------------------------------------------------------------------------
// connection
//------------------------------------------------------------------------------

  public static void connectUseCase1ThinWanClientTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    c.connectUseCase1ThinWanClient();
    c.updateHydraThreadLocals();
  }
  private void connectUseCase1ThinWanClient() throws SQLException {
    switch (this.queryAPI) {
      case QueryPrms.GFXD:
        LonerHelper.connect(); // for statistics
        String clientName = RemoteTestModule.getMyClientName();
        String dsName = clientName.substring(clientName.indexOf("ds"), clientName.length());
        List endpoints = NetworkServerHelper.getNetworkLocatorEndpoints(dsName);
        this.connection = QueryUtil.gfxdWanClientSetup(this, endpoints);
        break;
      default:
        unsupported();
    }
  }

//------------------------------------------------------------------------------
// ddl
//------------------------------------------------------------------------------

  public static void executeDDLTask()
  throws FileNotFoundException, IOException, SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    if (c.ttgid == 0) {
      c.executeDDL();
    }
  }
  private void executeDDL()
  throws FileNotFoundException, IOException, SQLException {
    String fn = getDDLFile(UseCase1Prms.getDDLFile());
    List<String> stmts = getDDLStatements(fn);
    for (String stmt : stmts) {
      if (this.queryAPI == QueryPrms.GFXD) {
        //if (stmt.contains("partition")) {
        //  stmt += " buckets " + this.numBuckets;
        //}
      }
      Log.getLogWriter().info("Executing DDL: " + stmt);
      try {
        execute(stmt, this.connection);
      } catch (SQLException e) {
        if (stmt.contains("DROP")) {
          if (!e.getMessage().contains("does not exist") && // GFXD/MYSQL
              !e.getMessage().contains("Unknown table ")) { // ORACLE
            throw e;
          }
        } else {
          throw e;
        }
      }
      commitDDL();
    }
  }

  public static void setHeapPercentageTask()
  throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    if (c.ttgid == 0) {
      c.setHeapPercentage(UseCase1Prms.getHeapPercentage());
    }
  }

  protected void setHeapPercentage(double percentage)
  throws SQLException {
    String sql = "call SYS.SET_EVICTION_HEAP_PERCENTAGE_SG(?,?)";
    CallableStatement cs = this.connection.prepareCall(sql);
    Log.getLogWriter().info("CALL SYS.SET_EVICTION_HEAP_PERCENTAGE_SG(" + percentage + ", null)");
    cs.setDouble(1, percentage);
    cs.setNull(2, Types.VARCHAR);
    cs.executeUpdate();
  }

//------------------------------------------------------------------------------
// data
//------------------------------------------------------------------------------

  // for the useCase1 performance test, this data can be anything, so just use as-is
  private static final String SECT_CHANNEL_RAW_DATA = "<MSG filename=\"SamplePAYEXT.txt\" sourceFormat=\"PAYEXT\"> <MLD>UNB+UNOA:3</MLD> <ADR.SenderId>5025007000009</ADR.SenderId> <ADR>14</ADR> <DTE>006981815:1+120430:1159+80314</DTE> <MLD>PAYEXT1</MLD> <MLD>UNH+1+PAYEXT:D:96A:UN</MLD> <CRN>BGM+451+GHFRT0088733652</CRN> <TYP>BUSIN</TYP> <TYP>PAI::9</TYP> <TYP>:ZZZ</TYP> <ACI>FCA+14</ACI> <DTE>DTM+137:20120430:102</DTE> <MLD>RFF+CR</MLD> <CRN.ClientRef>7XWzXJPAaUR2D</CRN.ClientRef> <CRN>RFF+ABO:5025006768762</CRN> <VAL>MOA+9</VAL> <VAL.Amount>125.37</VAL.Amount> <VAL.Currency>GBP</VAL.Currency> <MLD>DTM+209</MLD> <DTE.ValueDate>20120302</DTE.ValueDate> <MLD>102</MLD> <MLD>RFF+PB</MLD> <ACN.CompanyId>6777734</ACN.CompanyId> <MLD>FII+OR</MLD> <ACN.OrigAccountNo>19882892</ACN.OrigAccountNo> <FII.OrigBankId>ouuAeSpCLP</FII.OrigBankId> <MLD>FII+BF</MLD> <ACN.BeneAccountNo>P988WgvLVJ</ACN.BeneAccountNo> <FII.BeneBankId>WU0D8ZPtAT</FII.BeneBankId> <ADR>:::</ADR> <ADR.BeneBankAddr>auqvTxVzXtmUHujYOEto</ADR.BeneBankAddr> <ADR.BeneBankCtry>ES</ADR.BeneBankCtry> <MLD>NAD+OY+5025006768762</MLD> <ADR.OrigName>Spanish American Caribbean</ADR.OrigName> <MLD>NAD+BE+1000021496</MLD> <ADR.BeneName>ASAM CLARKE AND CO (MADRID USD)C/GOYA 11</ADR.BeneName> <ADR.BeneAddr>MADRID28001+ES</ADR.BeneAddr> <TYP>PRC+8</TYP> <TYP>DOC+380+37902012</TYP> <VAL>MOA+12:1607.9</VAL> <VAL>MOA+39:1607.9</VAL> <VAL>MOA+52</VAL> <DTE>DTM+171:20120229:102</DTE> <MLD>GIS+37</MLD> <MLD>UNT+23+1</MLD> </MSG>";

  // the values should come from SECM_CHANNEL_ONBOARDED.CHANNEL_NAME
  private static final String[] CHANNEL_NAMES = {"PYS", "CCP-ED", "ISO"};

  private static final String SECT_CHANNEL_DATA_SQL = "insert into SEC_OWNER.SECT_CHANNEL_DATA (channel_txn_id, value_date, company_id, client_account, amount, currency, client_ref_no, orig_bank_id, bene_accnt_no, bene_bank_id, bene_bank_addr, bene_name, bene_addr, backoffice_code, txn_type, data_life_status, match_status, match_categ_id, etl_time, channel_name, raw_data, file_type_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  PreparedStatement SECT_CHANNEL_DATA_PS = null;

  /**
   * Generate data and wait for queues to drain.
   */
  public static void generateDataTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize(LOAD);
    c.generateDataAndDrainQueues();
  }

  private void generateDataAndDrainQueues() throws SQLException {
    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    if (tgname.contains("client")) {
      generateData();
      sendDrainSignal();
      waitForDrainedSignal();
    } else if (tgname.contains("server")) {
      if (this.jid == 0) {
        waitForDrainSignal();
        waitForQueuesToDrain();
        sendDrainedSignal();
      }
    } else {
      String s = "Unexpected thread group: " + tgname;
      throw new QueryPerfException(s);
    }
    cacheEndTrim(this.trimIntervals, this.trimInterval);
  }

  // signal the servers to wait for gateway queues to drain
  private void sendDrainSignal() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    counters.increment(UseCase1Blackboard.drainSignal);
    Log.getLogWriter().info("Sent drain signal");
  }

  // wait for clients to signal the servers to wait for gateway queues to drain
  private void waitForDrainSignal() throws SQLException {
    Log.getLogWriter().info("Waiting for drain signal...");
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    int numETLLoadClients = UseCase1Prms.getNumETLLoadClients();
    while (counters.read(UseCase1Blackboard.drainSignal) < numETLLoadClients) {
      MasterController.sleepForMs(1000);
    }
    Log.getLogWriter().info("Got drain signal");
  }

  // signal the clients that gateway queues have drained
  private void sendDrainedSignal() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    counters.increment(UseCase1Blackboard.drainedSignal);
    Log.getLogWriter().info("Sent drained signal");
  }

  // wait for servers to signal the clients that the gateway queues have drained
  private void waitForDrainedSignal() throws SQLException {
    Log.getLogWriter().info("Waiting for drained signal...");
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    int numETLGatewayServers = UseCase1Prms.getNumETLGatewayServers();
    while (counters.read(UseCase1Blackboard.drainedSignal) < numETLGatewayServers) {
      MasterController.sleepForMs(1000);
    }
    Log.getLogWriter().info("Got drained signal");
  }

  private void generateData() throws SQLException {
    generate_SECT_CHANNEL_DATA(this.connection);
  }

  private void generate_SECT_CHANNEL_DATA(Connection conn) throws SQLException {
    if (SECT_CHANNEL_DATA_PS == null) {
      SECT_CHANNEL_DATA_PS = conn.prepareStatement(SECT_CHANNEL_DATA_SQL);
    }
    PreparedStatement stmt = SECT_CHANNEL_DATA_PS;

    int batchSize = UseCase1Prms.getBatchSize();
    int numSectChannelDataRows = UseCase1Prms.getNumSectChannelDataRows();

    String tgname = RemoteTestModule.getCurrentThread().getThreadGroupName();
    int tgthreads = TestConfig.getInstance().getThreadGroup(tgname).getTotalThreads();
    int totalBatches = numSectChannelDataRows/(batchSize * tgthreads);
    Log.getLogWriter().info("Generating " + (totalBatches * batchSize)
                      + " rows of data using " + tgthreads + " threads");
    if (totalBatches <= 0) {
      String s = "Cannot generate data with only " + (totalBatches * batchSize)
               + " rows. Fix" +
                 " UseCase1Prms.numSectChannelDataRows=" + numSectChannelDataRows +
                 " UseCase1Prms.batchSize=" + batchSize +
                 " this.numThreads=" + this.numThreads;
      throw new QueryPerfException(s);
    }
    for (int i = 0; i < totalBatches; i++) {
      long start = this.useCase1stats.startStmt(Stmt.insertSectChannelData);
      for (int j = 0; j < batchSize; j++) {
        fillSectChannelDataStmt(conn, stmt);
        stmt.addBatch();
      }
      stmt.executeBatch();
      conn.commit();
      stmt.clearBatch();
      this.useCase1stats.endStmt(Stmt.insertSectChannelData, start, batchSize, null);
    }
    stmt.close();
  }

  private void fillSectChannelDataStmt(Connection conn, PreparedStatement stmt)
  throws SQLException {
    UUID txid = UUID.randomUUID();
    stmt.setString(1, txid.toString()); // CHANNEL_TX_ID

    Timestamp valueDate = generateTimestamp();
    stmt.setTimestamp(2, valueDate); // VALUE_DATE

    String companyId = generateNumericString(7);
    stmt.setString(3, companyId); // COMPANY_ID

    String clientAccount = generateNumericString(8);
    stmt.setString(4, clientAccount); // CLIENT_ACCOUNT

    Double amount = generateDecimal(5,2);
    stmt.setDouble(5, amount); // AMOUNT

    String currency = "GBP";
    stmt.setString(6, currency); // CURRENCY

    String clientRefNo = generateString(13);
    stmt.setString(7, clientRefNo); // CLIENT_REF_NO

    String origBankId = generateString(10,15);
    stmt.setString(8, origBankId); // ORIG_BANK_ID

    String beneAccntNo = generateString(10);
    stmt.setString(9, beneAccntNo); // BENE_ACCNT_NO

    String beneBankId = generateString(10);
    stmt.setString(10, beneBankId); // BENE_BANK_ID

    String beneBankAddr = generateString(20);
    stmt.setString(11, beneBankAddr); // BENE_BANK_ADDR

    String beneName = generateString(10);
    stmt.setString(12, beneName); //  BENE_NAME

    String beneAddr = generateString(20);
    stmt.setString(13, beneAddr); // BENE_ADDR

    String backofficeCode = IPAY;
    stmt.setString(14, backofficeCode); // BACKOFFICE_CODE (should come from SECM_BACKOFFICE_ONBOARDED.BACKOFFICE_CODE)

    String txnType = "DDI";
    stmt.setString(15, txnType); // TXN_TYPE

    stmt.setInt(16, 0); // DATA_LIFE_STATUS

    stmt.setInt(17, 1); // MATCH_STATUS

    stmt.setInt(18, -1); // MATCH_CATEG_ID // TBD: this int should be null

    stmt.setLong(19, -1); // ETL_TIME

    String channelName = CHANNEL_NAMES[this.rng.nextInt(CHANNEL_NAMES.length - 1)]; // perhaps default to "UNKNOWN" sometimes
    stmt.setString(20, channelName); // CHANNEL_NAME

    final Clob clob = conn.createClob();
    clob.setString(1, SECT_CHANNEL_RAW_DATA);
    stmt.setClob(21, clob); // RAW_DATA

    String fileTypeId = UUID.randomUUID().toString();
    stmt.setString(22, fileTypeId); // FILE_TYPE_ID

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("INSERT into SECT_CHANNEL_DATA with" +
        " CURRENCY=" + currency +
        " AMOUNT=" + amount +
        " CLIENT_REF_NO=" + clientRefNo +
        " VALUE_DATE=" + valueDate +
        " CLIENT_ACCOUNT=" + clientAccount +
        " COMPANY_ID=" + companyId);
    }
  }

  private void waitForQueuesToDrain() throws SQLException {
    Log.getLogWriter().info("Waiting for queues to drain...");
    final StatisticsFactory f = DistributedSystemHelper.getDistributedSystem();
    Statistics[] stats =
      f.findStatisticsByType(f.findType(GatewaySenderStats.typeName));
    while (true) {
      long qsize = 0;
      for (int i=0; i < stats.length; i++) {
        qsize += stats[i].getInt(GatewaySenderStats.getEventQueueSizeId());
      }
      if (qsize == 0) {
        break;
      }
      MasterController.sleepForMs(250);
    }
    Log.getLogWriter().info("Waited for queues to drain");
  }

  public static void exportTablesTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    if (c.ttgid == 0 && UseCase1Prms.exportTables()) {
      c.exportTables();
    }
  }

  protected void exportTables() throws SQLException {
    final Connection conn = openTmpConnection();
    try {
      List<String> tables = getTableNames(conn);
      for (String table:tables)  {
        exportTable(conn, table);
      }
    } finally {
      closeTmpConnection(conn);
    }
  }

  protected void exportTable(Connection conn, String table) throws SQLException {
    String exportTable = "CALL SYSCS_UTIL.EXPORT_TABLE (?, ?, ?, null, null, null)";
    String dir = FabricServerHelper.getFabricServerDescription().getSysDir()
               + "/" + table.toUpperCase() + ".file";
    CallableStatement cs = conn.prepareCall(exportTable);
    cs.setString(1, "SEC_OWNER");
    cs.setString(2, table);
    cs.setString(3, dir);
    cs.execute();
    Log.getLogWriter().info("Exported table: SEC_OWNER." + table + " to " + dir);
  }

  private List<String> getTableNames(Connection conn) throws SQLException {
    List<String> tables = new ArrayList<String>();
    try {
      ResultSet rs = conn.createStatement().executeQuery(
        "SELECT tablename FROM sys.systables WHERE tableschemaname= 'SEC_OWNER'");
      while (rs.next()) {
        String table = rs.getString(1);
        tables.add(table);
        Log.getLogWriter().info("Found table to export: " + table);
      }
      rs.close();
      rs = null;
    } catch (SQLException e) {
      throw new QueryPerfException(TestHelper.getStackTrace(e));
    }
    return tables;
  }

  /**
   * Imports tables in each WAN site. Do not turn on WAN until after import.
   * Run this from a client.
   */
  public static void importTablesTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    if (c.sttgid == 0) {
      c.importTables();
    }
  }

  protected void importTables() throws SQLException {
    String dir = UseCase1Prms.getImportTableDir();
    final Connection conn = openTmpConnection();
    try {
      for (String table : UseCase1Prms.getImportTables()) {
        importTable(conn, dir, table);
      }
    } finally {
      closeTmpConnection(conn);
    }
  }

  // hard-wired to 32 import threads per wan site
  protected void importTable(Connection conn, String dir, String table) throws SQLException {
    String importTable = "CALL SYSCS_UTIL.IMPORT_TABLE_EX (?, ?, ?, null, null, null, 0, 0, 32, 0, null, null)";
    Log.getLogWriter().info("Importing table: SEC_OWNER." + table + " from " + dir);
    CallableStatement cs = conn.prepareCall(importTable);
    cs.setString(1, "SEC_OWNER");
    cs.setString(2, table);
    cs.setString(3, dir + "/" + table + ".file");
    cs.execute();
    Log.getLogWriter().info("Imported table: SEC_OWNER." + table);
  }

//------------------------------------------------------------------------------
// traffic cop tasks
//------------------------------------------------------------------------------

  /**
   * UseCase1 dummy workload for testing traffic cop. Clients just sleep as
   * specified in {@link UseCase1Prms#dummyWorkloadSleepSec}.
   */
  public static void trafficCopTask1() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    c.trafficCop1();
  }

  private void trafficCop1() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    pauseTrafficCop(30);
    pauseClients(counters);
    shutDownAll(counters, "ds_12");
    killServers("ds_12");
    restartServers("ds_12");
    resumeClients(counters);
    pauseTrafficCop(30);
    stopTest(counters);
  }

  /**
   * UseCase1 inbound/outbound workload for traffic cop. Clients work as specified
   * in {@link CachePerfPrms#batchSeconds}. In between batches, they check for
   * signals from the traffic cop. No bounce occurs.
   */
  public static void trafficCopNoBounceTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    c.trafficCopNoBounce();
  }

  private void trafficCopNoBounce() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    pauseTrafficCop(UseCase1Prms.getTrafficCopSleepSec());
    stopTest(counters);
  }

  /**
   * UseCase1 match workload for traffic cop. Clients work as specified in
   * {@link CachePerfPrms#batchSeconds}. In between batches, they check for
   * signals from the traffic cop.
   */
  public static void trafficCopMatchTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    c.trafficCopMatch();
  }

  private void trafficCopMatch() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    pauseTrafficCop(UseCase1Prms.getTrafficCopSleepSec());
    pauseClients(counters);
    shutDownAll(counters, "ds_12");
    killServers("ds_12");
    restartServers("ds_12");
    resumeClients(counters);
    pauseTrafficCop(UseCase1Prms.getTrafficCopSleepSec());
    stopTest(counters);
  }

  private void shutDownAll(SharedCounters counters, String ds) {
    Log.getLogWriter().info("Traffic cop issuing shut-down-all on " + ds);
    FabricServerHelper.shutDownAllFabricServers(ds, 300);
    Log.getLogWriter().info("Traffic cop issued shut-down-all on " + ds);
  }

  /**
   * Stops all server JVMs in the specified distributed system.
   */
  private void killServers(String ds) {
    Log.getLogWriter().info("Traffic cop killing servers in " + ds);
    Map infos = UseCase1Blackboard.getInstance().getSharedMap().getMap();
    List<ClientVmInfo> targets = new ArrayList();
    for (Iterator i = infos.values().iterator(); i.hasNext();) {
      ClientVmInfo info = (ClientVmInfo)i.next();
      if (info.getClientName().contains("data") && info.getClientName().contains(ds)) {
        targets.add(info);
      }
    }
    killServers(targets);
    Log.getLogWriter().info("Traffic cop killed servers in " + ds);
  }

  private void killServers(List<ClientVmInfo> targets) {
    Log.getLogWriter().info("Killing " + targets);
    List<Thread> threads = new ArrayList();
    for (final ClientVmInfo target : targets) {
      Runnable action = new Runnable() {
        public void run() {
          Log.getLogWriter().info("Traffic cop killing server " + target);
          try {
            ClientVmMgr.stop("Killing " + target, target);
            Log.getLogWriter().info("Traffic cop killed server " + target);
          } catch (ClientVmNotFoundException e) {
            throw new QueryPerfException(TestHelper.getStackTrace(e));
          }
        }
      };
      String name = "Traffic Cop Dynamic Client VM Stopper";
      Thread t = new HydraSubthread(action);
      t.start();
      threads.add(t);
    }
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new QueryPerfException(TestHelper.getStackTrace(e));
      }
    }
    Log.getLogWriter().info("Killed " + targets);
  }

  /**
   * Restarts all server JVMs in the specified distributed system.
   */
  private void restartServers(String ds) {
    Log.getLogWriter().info("Traffic cop restarting servers in " + ds);
    Map infos = UseCase1Blackboard.getInstance().getSharedMap().getMap();
    List<ClientVmInfo> targets = new ArrayList();
    for (Iterator i = infos.values().iterator(); i.hasNext();) {
      ClientVmInfo info = (ClientVmInfo)i.next();
      if (info.getClientName().contains("data") && info.getClientName().contains(ds)) {
        targets.add(info);
      }
    }
    restartServers(targets);
    Log.getLogWriter().info("Traffic cop restarted servers in " + ds);
  }

  private void restartServers(List<ClientVmInfo> targets) {
    Log.getLogWriter().info("Starting " + targets);
    List<Thread> threads = new ArrayList();
    for (final ClientVmInfo target : targets) {
      Runnable action = new Runnable() {
        public void run() {
          Log.getLogWriter().info("Traffic cop starting server " + target);
          try {
            ClientVmMgr.start("Starting " + target, target);
            Log.getLogWriter().info("Traffic cop started server " + target);
          } catch (ClientVmNotFoundException e) {
            throw new QueryPerfException(TestHelper.getStackTrace(e));
          }
        }
      };
      String name = "Traffic Cop Dynamic Client VM Starter";
      Thread t = new HydraSubthread(action);
      t.start();
      threads.add(t);
    }
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new QueryPerfException(TestHelper.getStackTrace(e));
      }
    }
    Log.getLogWriter().info("Started " + targets);
  }

  private void pauseClients(SharedCounters counters) {
    // pause the work by clients in all WAN sites
    Log.getLogWriter().info("Traffic cop setting pause signal");
    counters.setIfLarger(UseCase1Blackboard.pauseSignal, 1);
    // wait for them all to pause
    int numClients = UseCase1Prms.getNumClients();
    while (counters.read(UseCase1Blackboard.pauseCount) < numClients) {
      MasterController.sleepForMs(2000);
    }
    Log.getLogWriter().info("Traffic cop sees all clients have paused");
  }

  private void resumeClients(SharedCounters counters) {
    Log.getLogWriter().info("Traffic cop resuming clients");
    counters.zero(UseCase1Blackboard.pauseSignal);
    while (counters.read(UseCase1Blackboard.pauseCount) > 0) {
      MasterController.sleepForMs(2000);
    }
    Log.getLogWriter().info("Traffic cop sees all clients have resumed");
  }

  private void stopTest(SharedCounters counters) {
    // stop the clients in all WAN sites
    Log.getLogWriter().info("Sending stop signal from traffic cop");
    counters.setIfLarger(UseCase1Blackboard.stopSignal, 1);
    throw new StopSchedulingTaskOnClientOrder("Traffic cop terminating");
  }

  private void pauseTrafficCop(int seconds) {
    // sleep to let everyone do some work
    Log.getLogWriter().info("Traffic cop sleeping " + seconds + " seconds");
    MasterController.sleepForMs(seconds * 1000);
  }

//------------------------------------------------------------------------------
// client tasks
//------------------------------------------------------------------------------

  // run commands --------------------------------------------------------------

  /**
   * Runs SQL commands from a file on a specified distributed system.
   */
  public static void useCase1RunCommandFilesTask()
  throws FileNotFoundException, IOException, SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    if (c.ttgid == 0) {
      c.useCase1RunCommandFiles();
    }
  }

  private void useCase1RunCommandFiles()
  throws FileNotFoundException, IOException, SQLException {
    boolean useThinClient = UseCase1Prms.useThinClient();
    String dsName = UseCase1Prms.getDSName();
    String path = "$JTESTS/cacheperf.comparisons.gemfirexd.useCase1/sql/";
    List<String> fns = UseCase1Prms.getSQLCommandFiles();
    Log.getLogWriter().info("Running commands on " + dsName + " using " + fns);
    for (String fn : fns) {
      String newfn = EnvHelper.expandEnvVars(path + fn);
      Log.getLogWriter().info("Running commands on " + dsName + " using " + newfn);
      if (useThinClient) {
        NetworkServerHelper.executeSQLCommands(dsName, newfn, 120);
      } else {
        FabricServerHelper.executeSQLCommands(dsName, newfn, 120);
      }
      Log.getLogWriter().info("Ran commands on " + dsName + " using " + newfn);
    }
    Log.getLogWriter().info("Ran commands on " + dsName);
  }

  // dummy workload ------------------------------------------------------------

  /**
   * UseCase1 dummy workload for testing traffic cop. Just sleeps as
   * specified in {@link UseCase1Prms#dummyWorkloadSleepSec}.
   */
  public static void useCase1DummyWorkloadTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize(DUMMY);
    c.useCase1DummyWorkload();
  }

  private void useCase1DummyWorkload() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    while (true) {
      terminateIfNeeded(counters);
      pauseIfNeeded(counters);
      doDummyWorkload();
    }
  }

  private void doDummyWorkload()
  throws SQLException {
    int seconds = UseCase1Prms.getDummyWorkloadSleepSec();
    Log.getLogWriter().info("Working for " + seconds + " seconds");
    long start = this.useCase1stats.startDummy();
    MasterController.sleepForMs(seconds * 1000);
    this.useCase1stats.endDummy(start);
    Log.getLogWriter().info("Completed a " + seconds + "-second work cycle");
  }

  // etl workload --------------------------------------------------------------

  /**
   * Generate more data.
   */
  public static void generateMoreDataTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize(ETLGEN);
    c.generateMoreData();
  }
  private void generateMoreData() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    int throttleMs = UseCase1Prms.getWorkloadThrottleMs();
    while (true) {
      // check with traffic cop
      terminateIfNeeded(counters);
      pauseIfNeeded(counters);

      // run a workload batch
      long batchEnd = System.currentTimeMillis() + this.batchSeconds * 1000;
      while (System.currentTimeMillis() < batchEnd) {
        if (throttleMs != 0) { // throttle was set so choose each time at random
          MasterController.sleepForMs(UseCase1Prms.getWorkloadThrottleMs());
        }
        generateMore_SECT_CHANNEL_DATA(this.connection);
      }
      Log.getLogWriter().info("Completed a workload batch");
    }
  }
  private void generateMore_SECT_CHANNEL_DATA(Connection conn) throws SQLException {
    if (SECT_CHANNEL_DATA_PS == null) {
      SECT_CHANNEL_DATA_PS = conn.prepareStatement(SECT_CHANNEL_DATA_SQL);
    }
    PreparedStatement stmt = SECT_CHANNEL_DATA_PS;
    long start = this.useCase1stats.startStmt(Stmt.insertSectChannelData);
    fillSectChannelDataStmt(conn, stmt);
    stmt.executeUpdate();
    conn.commit();
    this.useCase1stats.endStmt(Stmt.insertSectChannelData, start);
  }

  // match workload ------------------------------------------------------------

  /**
   * UseCase1 match workload. Throttles as specified in {@link UseCase1Prms
   * #workloadThrottleMs}.
   */
  public static void useCase1MatchWorkloadTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize(MATCH);
    synchronized (clientAccountsLock) {
      c.getClientAccounts();
    }
    c.useCase1MatchWorkload();
  }

  /**
   * Look up the current client accounts for all threads in this JVM to use.
   */
  private void getClientAccounts() throws SQLException {
    if (clientAccounts == null) {
      Log.getLogWriter().info("Looking up current client accounts");
      clientAccounts = new ArrayList<String>();
      PreparedStatement clientAccountPS = this.connection.prepareStatement(
                "select client_account from SEC_OWNER.SECT_CHANNEL_DATA");
      ResultSet rs = clientAccountPS.executeQuery();
      while (rs.next()) {
        String clientAccount = rs.getString(1);
        clientAccounts.add(clientAccount);
      }
      rs.close();
      rs = null;
      Log.getLogWriter().info("Looked up " + clientAccounts.size()
                                           + " current client accounts");
    } else {
      Log.getLogWriter().info("Found " + clientAccounts.size()
                                       + " current client accounts");
    }
  }

  private void useCase1MatchWorkload() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    int throttleMs = UseCase1Prms.getWorkloadThrottleMs();
    while (true) {
      // check with traffic cop
      terminateIfNeeded(counters);
      pauseIfNeeded(counters);

      // initialize the match stored procedure call
      CallableStatement callableStmt = this.connection.prepareCall(MATCH_CALL);
      callableStmt.registerOutParameter(3, Types.INTEGER);

      // run a workload batch
      long batchEnd = System.currentTimeMillis() + this.batchSeconds * 1000;
      while (System.currentTimeMillis() < batchEnd) {
        if (throttleMs != 0) { // throttle was set so choose each time at random
          MasterController.sleepForMs(UseCase1Prms.getWorkloadThrottleMs());
        }
        int index = this.rng.nextInt(0, clientAccounts.size() - 1);
        String clientAccount = clientAccounts.get(index);
        doSingleMatchWithOnePrimaryKey(callableStmt, clientAccount);
      }
      Log.getLogWriter().info("Completed a workload batch");
    }
  }

  private void doSingleMatchWithOnePrimaryKey(CallableStatement callableStmt,
                                              String clientAccount)
  throws SQLException {
    long start = this.useCase1stats.startMatch();

    // set up inBackOfficeMsg with one client account

    Map<String, String> tableMap = new HashMap<String, String>();
    tableMap.put(CLIENT_ACCOUNT, clientAccount);

    Map<String,Map<String,String>> inBackOfficeMsg = new HashMap<String,Map<String,String>>();
    inBackOfficeMsg.put(SECL_BO_DATA_STATUS_HIST, tableMap);

    // set up singlePrimaryMatchingKeySet with one MatchingInfo

    Set<MatchingInfo> singlePrimaryMatchingKeySet = new HashSet<MatchingInfo>();
    MatchingInfo mi = new MatchingInfo();
    mi.setBackOfficeCode(IPAY);
    mi.setKeyName(CLIENT_ACCOUNT);
    mi.setMatchingPriority(1);
    mi.setChnDataTable(SECT_CHANNEL_DATA);
    mi.setBoDataTable(SECL_BO_DATA_STATUS_HIST);
    mi.setBoOnBoardTimestamp(new Timestamp(new Date().getTime()));
    mi.setKeyOnBoardTimestamp(new Timestamp(new Date().getTime()));
    singlePrimaryMatchingKeySet.add(mi);

    SortedMap<Integer,Set<MatchingInfo>> inMatchingKeyMap = new TreeMap<Integer,Set<MatchingInfo>>();
    inMatchingKeyMap.put(Integer.valueOf(1), singlePrimaryMatchingKeySet);

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Match Task matching CLIENT_ACCOUNT=" + clientAccount);
    }

    callableStmt.setObject(1, inBackOfficeMsg);
    callableStmt.setObject(2, inMatchingKeyMap);
    callableStmt.execute();

    int errorStateValue = callableStmt.getInt(3);
    ResultSet rs = callableStmt.getResultSet();
    int countOfRows = 0;
    while (rs.next()) {
      countOfRows++;
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("Match Task got" +
                                " rs.getString(1) " + rs.getString(1));
      }
    }
    rs.close();
    rs = null;
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Match Task got" +
                              " countOfRows=" + countOfRows +
                              " errorStateValue=" + errorStateValue);
    }
    this.useCase1stats.endMatch(start, countOfRows, errorStateValue);
  }

  // match and purge workload --------------------------------------------------

  /**
   * UseCase1 match and purge workload. Only one thread runs the purge, the rest run
   * match. Throttles as specified in {@link UseCase1Prms#workloadSleepMs}.
   */
  public static void useCase1MatchAndPurgeWorkloadTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize(MATCHPURGE);
    if (c.ttgid == 0) {
      c.useCase1PurgeWorkload();
    } else {
      synchronized (clientAccountsLock) {
        c.getClientAccounts();
      }
      c.useCase1MatchWorkload();
    }
  }

  private void useCase1PurgeWorkload() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    int throttleMs = UseCase1Prms.getWorkloadThrottleMs();
    int throttleChunkMs = UseCase1Prms.getWorkloadThrottleChunkMs();
    while (true) {
      // check with traffic cop
      terminateIfNeeded(counters);
      pauseIfNeeded(counters);

      // throttle if needed, in chunks
      if (throttleMs != 0) {
        long throttleEnd = System.currentTimeMillis()
                         + UseCase1Prms.getWorkloadThrottleMs();
        while (System.currentTimeMillis() < throttleEnd) {
          MasterController.sleepForMs(throttleChunkMs);

          // check with traffic cop
          terminateIfNeeded(counters);
          pauseIfNeeded(counters);
        }
      }

      // purge
      Log.getLogWriter().info("Starting BO purge");
      long start = this.useCase1stats.startPurge();
      CallableStatement callableStmt = this.connection.prepareCall(PURGE_CALL);
      callableStmt.execute();
      this.useCase1stats.endPurge(start);
      Log.getLogWriter().info("Completed BO purge");
    }
  }

  // inbound workload ----------------------------------------------------------

  /**
   * UseCase1 inbound workload. Throttles as specified in {@link UseCase1Prms
   * #workloadThrottleMs}. Runs the match stored procedure as part of the work.
   */
  public static void useCase1InboundWorkloadTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize(INBOUND);
    synchronized (matchCriteriasLock) {
      c.getMatchCriteria();
    }
    c.useCase1InboundWorkload();
  }

  private void useCase1InboundWorkload() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    int throttleMs = UseCase1Prms.getWorkloadThrottleMs();
    while (true) {
      // check with traffic cop
      terminateIfNeeded(counters);
      pauseIfNeeded(counters);

      // initialize the match stored procedure call
      CallableStatement callableStmt = this.connection.prepareCall(MATCH_CALL);
      callableStmt.registerOutParameter(3, Types.INTEGER);

      // run a workload batch
      long batchEnd = System.currentTimeMillis() + this.batchSeconds * 1000;
      while (System.currentTimeMillis() < batchEnd) {
        if (throttleMs != 0) { // throttle was set so choose each time at random
          MasterController.sleepForMs(UseCase1Prms.getWorkloadThrottleMs());
        }
        // TBD: for some small percentage of time, make the matchCriteria fail to match
        // TBD: for some small percentage of time, make the matchCriteria cause a multimatch
        int index = this.rng.nextInt(0, matchCriterias.size() - 1);
        doInbound(this.connection, callableStmt, matchCriterias.get(index), counters);
      }
      Log.getLogWriter().info("Completed a workload batch");
    }
  }

  private SortedMap<Integer,Set<MatchingInfo>> inMatchingKeyMap = null;

  private void doInbound(Connection conn, CallableStatement callableStmt,
                         MatchCriteria matchCriteria, SharedCounters counters)
  throws SQLException {
    long inbound_start = this.useCase1stats.startInbound();
    long stmt_start;

    // Steps 1-3 represent Matching Process Flow persistRawBOMessage
    // and are carried out using their own thread pool

    // 1
    // select MQ_NAME, BACKOFFICE_CODE from SECM_MQ_BO_MAP map, SECM_MQ mq where map.MQ_ID=mq.MQ_ID
    // this query can (and does) use the sample data in CORE_ENGINE_SECM_MQ_BO_MAP_Sample.sql and CORE_ENGINE_SECM_MQ_Sample.sql
    stmt_start = this.useCase1stats.startStmt(Stmt.getMQNameBOMap);
    Map<String,Set<String>> mqNameBOCodeSet = getMQNameBOMap(conn);
    this.useCase1stats.endStmt(Stmt.getMQNameBOMap, stmt_start);

    // 2
    // Alex and Elvis said to skip this one
    // select * from SECL_BO_DATA_STATUS_HIST where BO_TXN_ID = ? ORDER BY LAST_UPDATE_TIME DESC
    //stmt_start = this.useCase1stats.startStmt(Stmt.getByBackOfficeTxnId);
    //BOMessage boMessage = getByBackOfficeTxnId(conn, boTxnId);
    //this.useCase1stats.endStmt(Stmt.getByBackOfficeTxnId, stmt_start);

    // 3
    // INSERT INTO SECL_BO_RAW_DATA(BO_TXN_ID, BO_RAW_DATA, MESSAGE_ID, INCOMING_QUEUE) VALUES (?,?,?,?)
    // assign a unique transaction id here
    stmt_start = this.useCase1stats.startStmt(Stmt.insertBORawData);
    String boTxnId = UUID.randomUUID().toString();
    // generate raw data (formatted text) based on the match criteria
    // this simulates what comes from the back office via JMS for matching
    String boRawDataInit = createBORawData(matchCriteria);
    // the incoming queue name normally comes from the JMS message
    // Alex and Elvis suggested picking a random queue name, but picking
    // a random entry in a keySet is inefficient so rather than changing
    // the above data structure, we hardwire the value
    String incomingQueueName = "BO1.REQ.IN.QUEUE";
    // the message id probably comes from the JMS message
    // the DDL shows VARCHAR(64) but the above example is only 20 characters
    String msgId = generateString(20);

    insertBORawData(conn, boTxnId, boRawDataInit, msgId, incomingQueueName);
    this.useCase1stats.endStmt(Stmt.insertBORawData, stmt_start);

    // Steps 4-12?? represent Matching Process Flow processParsedMessage
    // and are carried out using their own thread pool with max 60 threads

    // 4
    // select * from SECL_BO_RAW_DATA where BO_TXN_ID = ?
    // this looks up what we just inserted, it might get optimized out in future
    stmt_start = this.useCase1stats.startStmt(Stmt.getBORawDataByPrimaryKey);
    BORawData boRawData = getBORawDataByPrimaryKey(conn, boTxnId);
    this.useCase1stats.endStmt(Stmt.getBORawDataByPrimaryKey, stmt_start);

    // 7
    // insert into SECL_BO_DATA_STATUS_HIST (BO_TXN_ID, BACKOFFICE_CODE, OFAC_MSG_ID, VALUE_DATE, CURRENCY, INSTR_CREATED_TIME, ACTUAL_VALUE_DATE, COMPANY_ID, BENE_BANK_NAME, BENE_ACCNT_NO, CLIENT_ACCOUNT, AMOUNT, CHANNEL_NAME, SCREENING_TIME, CLIENT_REF_NO) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    stmt_start = this.useCase1stats.startStmt(Stmt.insertStatusHist);
    String beneBankName = "WXXXX BANK";
    String beneAccntNo = "DE12330500000001296854 FOZZI BEAR CA";
    String channelName = "ISO";

    BOMessage boMessage = new BOMessage();
    boMessage.setBoTxnId(boTxnId);
    boMessage.setBoCode(IPAY); // ok to use all ipay for this test
    boMessage.setOfacMsgId(msgId);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "VALUE_DATE", matchCriteria.valueDate);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "CURRENCY", matchCriteria.currency);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "INSTR_CREATED_TIME", matchCriteria.valueDate); // this time should be later
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "ACTUAL_VALUE_DATE", matchCriteria.valueDate);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "COMPANY_ID", matchCriteria.companyId);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "BENE_BANK_NAME", beneBankName);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "BENE_ACCNT_NO", beneAccntNo);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "CLIENT_ACCOUNT", matchCriteria.clientAccount);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "AMOUNT", matchCriteria.amount);
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "SCREENING_TIME", Integer.valueOf(-1));
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "CLIENT_REF_NO", matchCriteria.clientRefNo);
    boMessage.setChannelName(channelName);

    int rowsInsertedStatusHist = insertStatusHist(conn, boMessage, stmt_start);

    // 9
    // CALL SEC_OWNER.matchStoredProc(?, ?, ?)
    // use small percentage of no match, same percentage for multimatch
    //
    // 10
    // CALL SEC_OWNER.queryExecutorStoredProc(?,?,?)
    // WITH RESULT PROCESSOR MatchResultProcessor ON TABLE SECT_CHANNEL_DATA

    long startm = this.useCase1stats.startMatch();

    // set up the match candidate with the current status history
    Map<String, String> tableMap = new HashMap<String, String>();
    tableMap.put("VALUE_DATE", matchCriteria.valueDate.toString());
    tableMap.put("CURRENCY", matchCriteria.currency);
    tableMap.put("INSTR_CREATED_TIME", matchCriteria.valueDate.toString());
    tableMap.put("ACTUAL_VALUE_DATE", matchCriteria.valueDate.toString());
    tableMap.put("COMPANY_ID", matchCriteria.companyId);
    tableMap.put("BENE_BANK_NAME", beneBankName);
    tableMap.put("BENE_ACCNT_NO", beneAccntNo);
    tableMap.put("CLIENT_ACCOUNT", matchCriteria.clientAccount);
    tableMap.put("AMOUNT", String.valueOf(matchCriteria.amount));
    tableMap.put("SCREENING_TIME", "-1");
    tableMap.put("CLIENT_REF_NO", matchCriteria.clientRefNo);
    tableMap.put("CHANNEL_NAME", channelName);

    Map<String,Map<String,String>> inBackOfficeMsg = new HashMap<String,Map<String,String>>();
    inBackOfficeMsg.put(SECL_BO_DATA_STATUS_HIST, tableMap);

    if (inMatchingKeyMap == null) {
      inMatchingKeyMap = new TreeMap<Integer,Set<MatchingInfo>>();

      // set up the matching keys arg with the two IPAY rules
      //
      // to match, all of the matching keys for a rule must match a SECT_CHANNEL_DATA row
      // if a matching key has a null value, it is skipped in the matching query
      // for this test, just let all values be NOT null
      //
      // note that the matching engine will re-query using COMPANY_ID (rule2)
      // instead of CLIENT_ACCOUNT (rule1) if needed to narrow the search to
      // one matched channel instruction/record

      Set<MatchingInfo> rule1 = new HashSet<MatchingInfo>();
      rule1.add(createMatchingInfo("CURRENCY", "VARCHAR"));
      rule1.add(createMatchingInfo("AMOUNT", "DECIMAL"));
      rule1.add(createMatchingInfo("CLIENT_REF_NO", "VARCHAR"));
      rule1.add(createMatchingInfo("VALUE_DATE", "TIMESTAMP"));
      rule1.add(createMatchingInfo("CLIENT_ACCOUNT", "VARCHAR"));
      inMatchingKeyMap.put(Integer.valueOf(1), rule1);

      Set<MatchingInfo> rule2 = new HashSet<MatchingInfo>();
      rule2.add(createMatchingInfo("CURRENCY", "VARCHAR"));
      rule2.add(createMatchingInfo("AMOUNT", "DECIMAL"));
      rule2.add(createMatchingInfo("CLIENT_REF_NO", "VARCHAR"));
      rule2.add(createMatchingInfo("VALUE_DATE", "TIMESTAMP"));
      rule2.add(createMatchingInfo("COMPANY_ID", "VARCHAR"));
      inMatchingKeyMap.put(Integer.valueOf(2), rule2);
    }

    // invoke the match stored procedure
    callableStmt.setObject(1, inBackOfficeMsg);
    callableStmt.setObject(2, inMatchingKeyMap);
    callableStmt.execute();

    int errorStateValue = callableStmt.getInt(3);
    ResultSet rs = callableStmt.getResultSet();
    int countOfRows = 0;
    List<String> channelTxnIds = new ArrayList();
    while (rs.next()) {
      countOfRows++;
      channelTxnIds.add(rs.getString("CHANNEL_TXN_ID"));
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("Match Task got" +
                                " rs.getString(1) " + rs.getString(1));
      }
    }
    rs.close();
    rs = null;
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Match Task got" +
                              " countOfRows=" + countOfRows +
                              " errorStateValue=" + errorStateValue);
    }
    this.useCase1stats.endMatch(startm, countOfRows, errorStateValue);

    // ?
    //
    // Alex and Elvis suggest inserting another row in SECL_BO_DATA_STATUS_HIST
    // to reflect the now-available MATCH_STATUS and DATA_LIFE_STATUS (there
    // might be as many as 5 records per BO_TXN_ID, but I'm not sure where the
    // others might come in))

    // Steps 13-?? represent Matching Process Flow formatMessage??
    // and are carried out using their own thread pool??

    // 13
    // SELECT BO_TXN_ID, CLIENT_ID , CLIENT_NAME , CLIENT_ACCOUNT  , COMPANY_ID  , CLIENT_REF_NO  , VALUE_DATE  , AMOUNT  , CURRENCY  , ORIG_BANK_ID  , BACKOFFICE_CODE  , BENE_ACCNT_NO  , BENE_NAME  , BENE_ADDR ,  BENE_BANK_ID  , BENE_BANK_NAME  ,  BENE_BANK_ADDR  , INSTR_CREATED_TIME  , INSTR_CREATED_BY  ,  DATA_LIFE_STATUS  ,  MATCH_STATUS  ,   MATCH_DATE  ,   MATCH_CATEG_ID  ,   MATCHING_TIME ,  MANUAL_MATCH ,  MATCHING_REASON ,  SCREENING_TIME  ,  IS_MANUAL , IS_RESENT ,  CHANNEL_NAME  , TXN_TYPE , OFAC_MSG_ID , HIT_STATUS  , ACTUAL_VALUE_DATE  , LAST_UPDATE_TIME FROM SECL_BO_DATA_STATUS_HIST WHERE BO_TXN_ID = ? ORDER BY LAST_UPDATE_TIME DESC
    Map<String, Object> logTableNewValues = new HashMap(); // where does this come from?
    // this would normally look up all current entries in SECL_BO_DATA_STATUS_HIST
    // and look at ONLY THE FIRST ONE
    // then do an insert into SECL_BO_DATA_STATUS_HIST that is a merge of the result
    // of the lookup and the provided new values as overrides
    int rowsAddedForStep13 = getByBackOfficeTxnId2(conn, boTxnId, logTableNewValues);

    // 14
    // INSERT INTO SECL_OFAC_MESSAGE (BO_TXN_ID, CHN_TXN_ID, HIT_STATUS, MESSAGE_GUID, LAST_UPDATE_TIME, NO_OF_CHUNKS, FS_MESSAGE_ID) VALUES(?,?,?,?,CURRENT_TIMESTAMP,?,?)
    //          BO_TXN_ID = a83e0e55-1947-4626-81cf-7a8868accb53
    //          CHN_TXN_ID = NULL
    //          HIT_STATUS = -1
    //          MESSAGE_GUID = b0bcbef6-6d42-41e7-ba9b-925c1cf9a5f8
    //          LAST_UPDATE_TIME = current timestamp
    //          NO_OF_CHUNKS = 1
    //          FS_MESSAGE_ID = 00000000011000009341
    //
    // The OFAC tables are what goes to Firco. The data comes from BO_DATA_STATUS_HIST and SECT_CHANNEL_DATA.
    // If no match, then one chunk, just from status hist.
    // If match, combine status hist and channel data. Number of chunks is dependent on the combined payload size.
    // Multiple chunks needed if > 126K. For test, use 1 chunk.
    // The HIT_STATUS is -1 here.

    stmt_start = this.useCase1stats.startStmt(Stmt.persistFsMessage);
    OFACMessage ofacMessage = new OFACMessage();
    ofacMessage.setMessageGuId(UUID.randomUUID().toString());
    ofacMessage.setBoTranId(boTxnId);
    String channelTxnId = null;
    if (channelTxnIds.size() == 1) {
      channelTxnId = channelTxnIds.get(0);
      ofacMessage.setChnTxnId(channelTxnId);
    }
    ofacMessage.setFsHitStatus(FsHitStatus.NOT_YET_KNOWN);
    // artifically make the FS_MESSAGE_ID into a key computable on the outbound side
    int fsMessageIdNum = getNextKey();
    counters.increment(UseCase1Blackboard.maxFsMessageId);
    String fsMessageId = String.valueOf(fsMessageIdNum);
    ofacMessage.setFsMessageId(fsMessageId);
    ofacMessage.setNoOfChunks(1);
    int numRowsAddedForStep14 = persistFsMessage(conn, ofacMessage);
    this.useCase1stats.endStmt(Stmt.persistFsMessage, stmt_start);

    // 15
    // INSERT INTO SECL_OFAC_CHUNKED_MESSAGE (CHUNK_ID, FS_MESSAGE_ID, BACKOFFICE_TXN_ID, CHANNEL_TXN_ID, CHUNK_SEQ, CHUNKED_MSG, SENT_DATE, RECEIVING_TIME, OFAC_COMMENTS) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    stmt_start = this.useCase1stats.startStmt(Stmt.persistChunkedMessages);
    FsChunkedMessage fsChunkedMessage = new FsChunkedMessage();
    // artifically make the CHUNK_ID into the same as the FS_MESSAGE_ID
    fsChunkedMessage.setChunkId(fsMessageId);
    fsChunkedMessage.setFsMessageId(fsMessageId);
    fsChunkedMessage.setBoTranId(boTxnId);
    fsChunkedMessage.setChnTxnId(channelTxnId);
    fsChunkedMessage.setChunkSequence(1);
    // just use SECL_BO_DATA_STATUS_HIST clob since we don't have the SECT_CHANNEL_DATA clob in hand
    // when there is exactly one match, the SECT_CHANNEL_DATA clob is normally sent, else just the SECL_BO_DATA_STATUS_HIST clob
    fsChunkedMessage.setChunkedMessage(boRawDataInit.getBytes());
    fsChunkedMessage.setSentDate(new Timestamp(new Date().getTime()));
    fsChunkedMessage.setReceivedDate(null);
    fsChunkedMessage.setOfacComment(null);
    fsChunkedMessage.setFsMessageSendStatus(FsMessageSendStatus.FS_MSG_SENT);
    persistChunkedMessages(conn, fsChunkedMessage);
    this.useCase1stats.endStmt(Stmt.persistChunkedMessages, stmt_start);

    this.useCase1stats.endInbound(inbound_start);
  }

  // 1 ------------------------------------------------------------------------

  protected final static String GET_MQ_BO_MAP_SQL = "select MQ_NAME, BACKOFFICE_CODE from SEC_OWNER.SECM_MQ_BO_MAP map, SEC_OWNER.SECM_MQ mq where map.MQ_ID=mq.MQ_ID";
  protected PreparedStatement GET_MQ_BO_MAP_PS = null;

  public Map<String,Set<String>> getMQNameBOMap(Connection conn)
  throws SQLException {
    if (GET_MQ_BO_MAP_PS == null) {
      GET_MQ_BO_MAP_PS = conn.prepareStatement(GET_MQ_BO_MAP_SQL);
    }
    List<MQNameBOCode> mqNameBOCodeList = new ArrayList();
    ResultSet rs = GET_MQ_BO_MAP_PS.executeQuery();
    while (rs.next()) {
      MQNameBOCode mqNameBoCode = new MQNameBOCode();
      mqNameBoCode.mqName = rs.getString("MQ_NAME");
      mqNameBoCode.boCode = rs.getString("BACKOFFICE_CODE");
      mqNameBOCodeList.add(mqNameBoCode);
    }
    rs.close();
    rs = null;

    Map<String,Set<String>> mqNameBOCodeSet = new HashMap<String,Set<String>>();
    for (MQNameBOCode mqNameBOCode : mqNameBOCodeList) {
      Set<String> boSet = mqNameBOCodeSet.get(mqNameBOCode.mqName);
      if (boSet == null) {
        boSet = new HashSet<String>();
        mqNameBOCodeSet.put(mqNameBOCode.mqName, boSet);
      }
      if (!boSet.contains(mqNameBOCode.boCode)) {
        boSet.add(mqNameBOCode.boCode);
      }
    }
    return mqNameBOCodeSet;
  }

  /**
   * Look up the current match criteria for all threads in this JVM to use.
   */
  private void getMatchCriteria() throws SQLException {
    if (matchCriterias == null) {
      Log.getLogWriter().info("Looking up current match criteria");
      matchCriterias = new ArrayList<MatchCriteria>();
      PreparedStatement matchCriteriaPS = this.connection.prepareStatement("select CURRENCY, AMOUNT, CLIENT_REF_NO, VALUE_DATE, CLIENT_ACCOUNT, COMPANY_ID from SEC_OWNER.SECT_CHANNEL_DATA");
      ResultSet rs = matchCriteriaPS.executeQuery();
      while (rs.next()) {
        matchCriterias.add(new MatchCriteria(rs));
      }
      rs.close();
      rs = null;
      Log.getLogWriter().info("Looked up " + matchCriterias.size()
                                           + " current match criteria");
    } else {
      Log.getLogWriter().info("Found " + matchCriterias.size()
                                       + " current match criteria");
    }
  }

  // 2 ------------------------------------------------------------------------

  // the useCase1 version of this method reads all result and builds a list of objects but only returns the first one
  // the query should do a FETCH FIRST ROW ONLY

  protected final static String GET_BY_BO_TXN_ID_SQL = "select * from SEC_OWNER.SECL_BO_DATA_STATUS_HIST where BO_TXN_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY";
  protected PreparedStatement GET_BY_BO_TXN_ID_PS = null;

  public BOMessage getByBackOfficeTxnId(Connection conn, String boTxnId)
  throws SQLException {
    if (GET_BY_BO_TXN_ID_PS == null) {
      GET_BY_BO_TXN_ID_PS = conn.prepareStatement(GET_BY_BO_TXN_ID_SQL);
    }
    GET_BY_BO_TXN_ID_PS.setString(1, boTxnId);
    ResultSet rs =  GET_BY_BO_TXN_ID_PS.executeQuery();
    BOMessage boMessage = null;
    if (rs.next()) {
      boMessage = new BOMessage();

      boMessage.setBoTxnId(rs.getString("BO_TXN_ID"));
      boMessage.setBoCode(rs.getString("BACKOFFICE_CODE"));
      boMessage.setOfacMsgId(rs.getString("OFAC_MSG_ID"));

      boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "MATCH_STATUS", rs.getString("MATCH_STATUS"));
      boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "DATA_LIFE_STATUS", rs.getString("DATA_LIFE_STATUS"));
      boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "MATCH_DATE", rs.getString("MATCH_DATE"));
      boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "MATCHING_TIME", rs.getString("MATCHING_TIME"));
      boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "MATCH_CATEG_ID", rs.getString("MATCH_CATEG_ID"));
      boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "IS_RESENT", rs.getString("IS_RESENT"));
      boMessage.setChannelName(rs.getString("CHANNEL_NAME"));
      boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "TXN_TYPE", rs.getString("TXN_TYPE"));
    }
    rs.close();
    rs = null;
    return boMessage;
  }

  // 3 ------------------------------------------------------------------------

  protected final static String INSERT_RAW_DATA_SQL = "INSERT INTO SEC_OWNER.SECL_BO_RAW_DATA (BO_TXN_ID, BO_RAW_DATA, MESSAGE_ID, INCOMING_QUEUE) VALUES(?,?,?,?)";
  protected PreparedStatement INSERT_RAW_DATA_PS = null;

  public int insertBORawData(Connection conn, String boTxnId, String rawData, String msgId, String incomingQueueName)
  throws SQLException {
    if (INSERT_RAW_DATA_PS == null) {
      INSERT_RAW_DATA_PS = conn.prepareStatement(INSERT_RAW_DATA_SQL);
    }
    INSERT_RAW_DATA_PS.setString(1, boTxnId);
    final Clob clob = conn.createClob();
    clob.setString(1, rawData);
    INSERT_RAW_DATA_PS.setClob(2, clob);
    INSERT_RAW_DATA_PS.setString(3, msgId);
    INSERT_RAW_DATA_PS.setString(4, incomingQueueName);

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("INSERT into SECL_BO_RAW_DATA BO_TXN_ID=" + boTxnId + " BO_RAW_DATA=" + rawData + " MESSAGE_ID=" + msgId + " INCOMING_QUEUE=" + incomingQueueName);
    }
    return INSERT_RAW_DATA_PS.executeUpdate();
  }

  // 4 ------------------------------------------------------------------------

  protected final static String GET_BY_PK_SQL = "select * from SEC_OWNER.SECL_BO_RAW_DATA where BO_TXN_ID = ?";
  protected PreparedStatement GET_BY_PK_PS = null;

  public BORawData getBORawDataByPrimaryKey(Connection conn, String boTxnId) throws SQLException {
    if (GET_BY_PK_PS == null) {
      GET_BY_PK_PS = conn.prepareStatement(GET_BY_PK_SQL);
    }
    GET_BY_PK_PS.setString(1, boTxnId);
    ResultSet rs = GET_BY_PK_PS.executeQuery();
    BORawData boRawData = null;
    while (rs.next()) {
      if (boRawData == null) {
        boRawData = new BORawData();
      } else {
        String s = "Got more than one result from a primary key lookup on SECL_BO_RAW_DATA using boTxnId=" + boTxnId;
        throw new SQLException(s);
      }
      boRawData.setBoTxnId(rs.getString("BO_TXN_ID"));
      boRawData.setMsgId(rs.getString("MESSAGE_ID"));
      boRawData.setIncomingQueueName(rs.getString("INCOMING_QUEUE"));
      try {
        boRawData.setRawData(rs.getString("BO_RAW_DATA"));
      } catch (UnsupportedEncodingException e) {
        throw new QueryPerfException("Bad raw data", e);
      }
    }
    rs.close();
    rs = null;
    return boRawData;
  }

  // 7 ------------------------------------------------------------------------

  // protected final static String INSERT_SECL_BO_DATA_STATUS_HIST_SQL = "insert into SEC_OWNER.SECL_BO_DATA_STATUS_HIST (BO_TXN_ID, BACKOFFICE_CODE, OFAC_MSG_ID, VALUE_DATE, CURRENCY, INSTR_CREATED_TIME, ACTUAL_VALUE_DATE, COMPANY_ID, BENE_BANK_NAME, BENE_ACCNT_NO, CLIENT_ACCOUNT, AMOUNT, CHANNEL_NAME, SCREENING_TIME, CLIENT_REF_NO) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  // protected PreparedStatement INSERT_SECL_BO_DATA_STATIS_HIST_PS = null;

  public int insertStatusHist(Connection conn, BOMessage boMessage, long stmt_start)
  throws SQLException {
    boMessage.putBoTableData("SECL_BO_DATA_STATUS_HIST", "SCREENING_TIME", Integer.valueOf(-1));
    InsertStatementBuilder isb = new InsertStatementBuilder();
    isb.insertInto(BOMessage.TABLENAME).set("BO_TXN_ID", boMessage.getBoTxnId())
       .set("BACKOFFICE_CODE", boMessage.getBoCode()).set("OFAC_MSG_ID", boMessage.getOfacMsgId());

    Set<String> boDataCols = boMessage.getBoTableColumnNames(boMessage.TABLENAME);
    if (null != boDataCols) {
      for (String columnName : boDataCols) {
        isb.set(columnName, boMessage.getBoTableData(BOMessage.TABLENAME, columnName));
      }
    }
    String sql = isb.getSql();
    PreparedStatement ps = conn.prepareStatement(sql);
    Object[] values = isb.getSqlValues();
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("insertStatusHist: sql=" + sql + " values=" + Arrays.asList(values));
    }
    for (int i = 1; i <= values.length; i++) {
      ps.setObject(i, values[i-1]);
    }
    int numRowsAffected = ps.executeUpdate();
    this.useCase1stats.endStmt(Stmt.insertStatusHist, stmt_start);

    // Insert extra BO tables
    Set<String> extraTableNames = boMessage.getBoTableNames();
    for (String tableName : extraTableNames) {
      if (!BOMessage.TABLENAME.equals(tableName)) {
        Set<String> columnNames = boMessage.getBoTableColumnNames(tableName);
        if (null != columnNames && columnNames.size() > 0) {
          long start = this.useCase1stats.startStmt(Stmt.insertExtraBOTables);
          InsertStatementBuilder isb2 = new InsertStatementBuilder();
          isb2.insertInto(tableName).set("BO_TXN_ID", boMessage.getBoTxnId());
          for (String columnName : columnNames) {
            Object val = boMessage.getBoTableData(tableName, columnName);
            if (null != val) {
              isb2.set(columnName, val);
            }
          }
          PreparedStatement ps2 = conn.prepareStatement(isb2.getSql());
          Object[] values2 = isb2.getSqlValues();
          for (int i = 1; i <= values2.length; i++) {
            ps2.setObject(i, values2[i]);
          }
          numRowsAffected += ps2.executeUpdate();
          this.useCase1stats.endStmt(Stmt.insertExtraBOTables, start);
        }
      }
    }
    return numRowsAffected;
  }

  // 13 -----------------------------------------------------------------------

  // the useCase1 version of this method reads all result and builds a list of objects but only returns the first one
  // the query should do a FETCH FIRST ROW ONLY (as we do here)

  protected final static String SELECT_BO_DATA_SQL = "SELECT BO_TXN_ID, CLIENT_ID, CLIENT_NAME, CLIENT_ACCOUNT, COMPANY_ID, CLIENT_REF_NO, VALUE_DATE, AMOUNT, CURRENCY, ORIG_BANK_ID, BACKOFFICE_CODE, BENE_ACCNT_NO, BENE_NAME, BENE_ADDR, BENE_BANK_ID, BENE_BANK_NAME, BENE_BANK_ADDR, INSTR_CREATED_TIME, INSTR_CREATED_BY, DATA_LIFE_STATUS, MATCH_STATUS, MATCH_DATE, MATCH_CATEG_ID, MATCHING_TIME, MANUAL_MATCH, MATCHING_REASON, SCREENING_TIME, IS_MANUAL, IS_RESENT, CHANNEL_NAME, TXN_TYPE, OFAC_MSG_ID, HIT_STATUS, ACTUAL_VALUE_DATE, LAST_UPDATE_TIME FROM SEC_OWNER.SECL_BO_DATA_STATUS_HIST WHERE BO_TXN_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY";
  protected PreparedStatement SELECT_BO_DATA_PS = null;

  protected final static String QUERY_TO_INSERT_BO_LOG_TABLE_SQL = "INSERT INTO SEC_OWNER.SECL_BO_DATA_STATUS_HIST (BO_TXN_ID, CLIENT_ID, CLIENT_NAME, CLIENT_ACCOUNT, COMPANY_ID, CLIENT_REF_NO, VALUE_DATE, AMOUNT, CURRENCY, ORIG_BANK_ID, BACKOFFICE_CODE, BENE_ACCNT_NO, BENE_NAME, BENE_ADDR, BENE_BANK_ID, BENE_BANK_NAME, BENE_BANK_ADDR, INSTR_CREATED_TIME, INSTR_CREATED_BY, DATA_LIFE_STATUS, MATCH_STATUS, MATCH_DATE, MATCH_CATEG_ID, MATCHING_TIME, MANUAL_MATCH, MATCHING_REASON, SCREENING_TIME, IS_MANUAL, IS_RESENT, CHANNEL_NAME, TXN_TYPE, OFAC_MSG_ID, HIT_STATUS, ACTUAL_VALUE_DATE, LAST_UPDATE_TIME) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)";
  protected PreparedStatement QUERY_TO_INSERT_BO_LOG_TABLE_PS = null;

  // returns the number of rows inserted into SECL_BO_DATA_STATUS_HIST
  public int getByBackOfficeTxnId2(Connection conn, String boTxnId,  Map<String, Object> logTableNewValues)
  throws SQLException {
    long stmt_start = this.useCase1stats.startStmt(Stmt.getByBackOfficeTxnId2);
    if (SELECT_BO_DATA_PS == null) {
      SELECT_BO_DATA_PS = conn.prepareStatement(SELECT_BO_DATA_SQL);
    }
    SELECT_BO_DATA_PS.setString(1, boTxnId);
    ResultSet rs =  SELECT_BO_DATA_PS.executeQuery();
    Map<String, Object> backOfficeMap = null;
    if (rs.next()) {
      backOfficeMap = new HashMap();
      backOfficeMap.put("BO_TXN_ID", rs.getObject("BO_TXN_ID"));
      backOfficeMap.put("CLIENT_ID", rs.getObject("CLIENT_ID"));
      backOfficeMap.put("CLIENT_NAME", rs.getObject("CLIENT_NAME"));
      backOfficeMap.put("CLIENT_ACCOUNT", rs.getObject("CLIENT_ACCOUNT"));
      backOfficeMap.put("COMPANY_ID", rs.getObject("COMPANY_ID"));
      backOfficeMap.put("CLIENT_REF_NO", rs.getObject("CLIENT_REF_NO"));
      backOfficeMap.put("VALUE_DATE", rs.getObject("VALUE_DATE"));
      backOfficeMap.put("AMOUNT", rs.getObject("AMOUNT"));
      backOfficeMap.put("CURRENCY", rs.getObject("CURRENCY"));
      backOfficeMap.put("ORIG_BANK_ID", rs.getObject("ORIG_BANK_ID"));
      backOfficeMap.put("BACKOFFICE_CODE", rs.getObject("BACKOFFICE_CODE"));
      backOfficeMap.put("BENE_ACCNT_NO", rs.getObject("BENE_ACCNT_NO"));
      backOfficeMap.put("BENE_NAME", rs.getObject("BENE_NAME"));
      backOfficeMap.put("BENE_ADDR", rs.getObject("BENE_ADDR"));
      backOfficeMap.put("BENE_BANK_ID", rs.getObject("BENE_BANK_ID"));
      backOfficeMap.put("BENE_BANK_NAME", rs.getObject("BENE_BANK_NAME"));
      backOfficeMap.put("BENE_BANK_ADDR", rs.getObject("BENE_BANK_ADDR"));
      backOfficeMap.put("INSTR_CREATED_TIME", rs.getObject("INSTR_CREATED_TIME"));
      backOfficeMap.put("INSTR_CREATED_BY", rs.getObject("INSTR_CREATED_BY"));
      backOfficeMap.put("DATA_LIFE_STATUS", rs.getObject("DATA_LIFE_STATUS"));
      backOfficeMap.put("MATCH_STATUS", rs.getObject("MATCH_STATUS"));
      backOfficeMap.put("MATCH_DATE", rs.getObject("MATCH_DATE"));
      backOfficeMap.put("MATCH_CATEG_ID", rs.getObject("MATCH_CATEG_ID"));
      backOfficeMap.put("MATCHING_TIME", rs.getObject("MATCHING_TIME"));
      backOfficeMap.put("MANUAL_MATCH", rs.getObject("MANUAL_MATCH"));
      backOfficeMap.put("MATCHING_REASON", rs.getObject("MATCHING_REASON"));
      backOfficeMap.put("SCREENING_TIME", rs.getObject("SCREENING_TIME"));
      backOfficeMap.put("IS_MANUAL", rs.getObject("IS_MANUAL"));
      backOfficeMap.put("IS_RESENT", rs.getObject("IS_RESENT"));
      backOfficeMap.put("CHANNEL_NAME", rs.getObject("CHANNEL_NAME"));
      backOfficeMap.put("TXN_TYPE", rs.getObject("TXN_TYPE"));
      backOfficeMap.put("OFAC_MSG_ID", rs.getObject("OFAC_MSG_ID"));
      backOfficeMap.put("HIT_STATUS", rs.getObject("HIT_STATUS"));
      backOfficeMap.put("ACTUAL_VALUE_DATE", rs.getObject("ACTUAL_VALUE_DATE"));
    }
    rs.close();
    rs = null;
    this.useCase1stats.endStmt(Stmt.getByBackOfficeTxnId2, stmt_start);

    if (backOfficeMap != null) {
      stmt_start = this.useCase1stats.startStmt(Stmt.insertToBOLogTable);
      if (QUERY_TO_INSERT_BO_LOG_TABLE_PS == null) {
        QUERY_TO_INSERT_BO_LOG_TABLE_PS = conn.prepareStatement(QUERY_TO_INSERT_BO_LOG_TABLE_SQL);
      }
      Map<String,Object> backOfficeDataMap = backOfficeMap;
      //Aashu - Check if map has new value for column otherwise pick the value from last updated row in log table
      // this branch will always be executed in this test
      // for this test, logTableNewvalues is an empty map
      PreparedStatement ps = QUERY_TO_INSERT_BO_LOG_TABLE_PS;
      Object obj;

      obj = logTableNewValues.get("BO_TXN_ID") == null ? backOfficeDataMap.get("BO_TXN_ID") : logTableNewValues.get("BO_TXN_ID");
      ps.setObject(1, obj);

      obj = logTableNewValues.get("CLIENT_ID") == null ? backOfficeDataMap.get("CLIENT_ID") : logTableNewValues.get("CLIENT_ID");
      if (obj == null) {
        ps.setObject(2, obj);
      }

      obj = logTableNewValues.get("CLIENT_NAME") == null ? backOfficeDataMap.get("CLIENT_NAME") : logTableNewValues.get("CLIENT_NAME");
      if (obj == null) {
        ps.setNull(3, Types.VARCHAR);
      } else {
        ps.setObject(3, obj);
      }

      obj = logTableNewValues.get("CLIENT_ACCOUNT") == null ? backOfficeDataMap.get("CLIENT_ACCOUNT") : logTableNewValues.get("CLIENT_ACCOUNT");
      if (obj == null) {
        ps.setNull(4, Types.VARCHAR);
      } else {
        ps.setObject(4, obj);
      }

      obj = logTableNewValues.get("COMPANY_ID") == null ? backOfficeDataMap.get("COMPANY_ID") : logTableNewValues.get("COMPANY_ID");
      if (obj == null) {
        ps.setNull(5, Types.VARCHAR);
      } else {
        ps.setObject(5, obj);
      }

      obj = logTableNewValues.get("CLIENT_REF_NO") == null ? backOfficeDataMap.get("CLIENT_REF_NO") : logTableNewValues.get("CLIENT_REF_NO");
      if (obj == null) {
        ps.setNull(6, Types.VARCHAR);
      } else {
        ps.setObject(6, obj);
      }

      obj = logTableNewValues.get("VALUE_DATE") == null ? backOfficeDataMap.get("VALUE_DATE") : logTableNewValues.get("VALUE_DATE");
      if (obj == null) {
        ps.setNull(7, Types.TIMESTAMP);
      } else {
        ps.setObject(7, obj);
      }

      obj = logTableNewValues.get("AMOUNT") == null ? backOfficeDataMap.get("AMOUNT") : logTableNewValues.get("AMOUNT");
      if (obj == null) {
        ps.setNull(8, Types.DECIMAL);
      } else {
        ps.setObject(8, obj);
      }

      obj = logTableNewValues.get("CURRENCY") == null ? backOfficeDataMap.get("CURRENCY") : logTableNewValues.get("CURRENCY");
      if (obj == null) {
        ps.setNull(9, Types.VARCHAR);
      } else {
        ps.setObject(9, obj);
      }

      obj = logTableNewValues.get("ORIG_BANK_ID") == null ? backOfficeDataMap.get("ORIG_BANK_ID") : logTableNewValues.get("ORIG_BANK_ID");
      if (obj == null) {
        ps.setNull(10, Types.VARCHAR);
      } else {
        ps.setObject(10, obj);
      }

      obj = logTableNewValues.get("BACKOFFICE_CODE") == null ? backOfficeDataMap.get("BACKOFFICE_CODE") : logTableNewValues.get("BACKOFFICE_CODE");
      if (obj == null) {
        ps.setNull(11, Types.VARCHAR);
      } else {
        ps.setObject(11, obj);
      }

      obj = logTableNewValues.get("BENE_ACCNT_NO") == null ? backOfficeDataMap.get("BENE_ACCNT_NO") : logTableNewValues.get("BENE_ACCNT_NO");
      if (obj == null) {
        ps.setNull(12, Types.VARCHAR);
      } else {
        ps.setObject(12, obj);
      }

      obj = logTableNewValues.get("BENE_NAME") == null ? backOfficeDataMap.get("BENE_NAME") : logTableNewValues.get("BENE_NAME");
      if (obj == null) {
        ps.setNull(13, Types.VARCHAR);
      } else {
        ps.setObject(13, obj);
      }

      obj = logTableNewValues.get("BENE_ADDR") == null ? backOfficeDataMap.get("BENE_ADDR") : logTableNewValues.get("BENE_ADDR");
      if (obj == null) {
        ps.setNull(14, Types.VARCHAR);
      } else {
        ps.setObject(14, obj);
      }

      obj = logTableNewValues.get("BENE_BANK_ID") == null ? backOfficeDataMap.get("BENE_BANK_ID") : logTableNewValues.get("BENE_BANK_ID");
      if (obj == null) {
        ps.setNull(15, Types.VARCHAR);
      } else {
        ps.setObject(15, obj);
      }

      obj = logTableNewValues.get("BENE_BANK_NAME") == null ? backOfficeDataMap.get("BENE_BANK_NAME") : logTableNewValues.get("BENE_BANK_NAME");
      if (obj == null) {
        ps.setNull(16, Types.VARCHAR);
      } else {
        ps.setObject(16, obj);
      }

      obj = logTableNewValues.get("BENE_BANK_ADDR") == null ? backOfficeDataMap.get("BENE_BANK_ADDR") : logTableNewValues.get("BENE_BANK_ADDR");
      if (obj == null) {
        ps.setNull(17, Types.VARCHAR);
      } else {
        ps.setObject(17, obj);
      }

      obj = logTableNewValues.get("INSTR_CREATED_TIME") == null ? backOfficeDataMap.get("INSTR_CREATED_TIME") : logTableNewValues.get("INSTR_CREATED_TIME");
      if (obj == null) {
        ps.setNull(18, Types.TIMESTAMP);
      } else {
        ps.setObject(18, obj);
      }

      obj = logTableNewValues.get("INSTR_CREATED_BY") == null ? backOfficeDataMap.get("INSTR_CREATED_BY") : logTableNewValues.get("INSTR_CREATED_BY");
      if (obj == null) {
        ps.setNull(19, Types.VARCHAR);
      } else {
        ps.setObject(19, obj);
      }

      obj = logTableNewValues.get("DATA_LIFE_STATUS") == null ? backOfficeDataMap.get("DATA_LIFE_STATUS") : logTableNewValues.get("DATA_LIFE_STATUS");
      if (obj == null) {
        ps.setNull(20, Types.SMALLINT);
      } else {
        ps.setObject(20, obj);
      }

      obj = logTableNewValues.get("MATCH_STATUS") == null ? backOfficeDataMap.get("MATCH_STATUS") : logTableNewValues.get("MATCH_STATUS");
      if (obj == null) {
        ps.setNull(21, Types.SMALLINT);
      } else {
        ps.setObject(21, obj);
      }

      obj = logTableNewValues.get("MATCH_DATE") == null ? backOfficeDataMap.get("MATCH_DATE") : logTableNewValues.get("MATCH_DATE");
      if (obj == null) {
        ps.setNull(22, Types.TIMESTAMP);
      } else {
        ps.setObject(22, obj);
      }

      obj = logTableNewValues.get("MATCH_CATEG_ID") == null ? backOfficeDataMap.get("MATCH_CATEG_ID") : logTableNewValues.get("MATCH_CATEG_ID");
      if (obj == null) {
        ps.setNull(23, Types.INTEGER);
      } else {
        ps.setObject(23, obj);
      }

      obj = logTableNewValues.get("MATCHING_TIME") == null ? backOfficeDataMap.get("MATCHING_TIME") : logTableNewValues.get("MATCHING_TIME");
      if (obj == null) {
        ps.setNull(24, Types.INTEGER);
      } else {
        ps.setObject(24, obj);
      }

      obj = logTableNewValues.get("MANUAL_MATCH") == null ? backOfficeDataMap.get("MANUAL_MATCH") : logTableNewValues.get("MANUAL_MATCH");
      if (obj == null) {
        ps.setNull(25, Types.CHAR);
      } else {
        ps.setObject(25, obj);
      }

      obj = logTableNewValues.get("MATCHING_REASON") == null ? backOfficeDataMap.get("MATCHING_REASON") : logTableNewValues.get("MATCHING_REASON");
      if (obj == null) {
        ps.setNull(26, Types.VARCHAR);
      } else {
        ps.setObject(26, obj);
      }

      obj = logTableNewValues.get("SCREENING_TIME") == null ? backOfficeDataMap.get("SCREENING_TIME") : logTableNewValues.get("SCREENING_TIME");

      if (obj == null) obj = Integer.valueOf(42);
      ps.setObject(27, obj);

      obj = logTableNewValues.get("IS_MANUAL") == null ? backOfficeDataMap.get("IS_MANUAL") : logTableNewValues.get("IS_MANUAL");
      if (obj == null) {
        ps.setNull(28, Types.CHAR);
      } else {
        ps.setObject(28, obj);
      }

      obj = logTableNewValues.get("IS_RESENT") == null ? backOfficeDataMap.get("IS_RESENT") : logTableNewValues.get("IS_RESENT");
      if (obj == null) {
        ps.setNull(29, Types.CHAR);
      } else {
        ps.setObject(29, obj);
      }

      obj = logTableNewValues.get("CHANNEL_NAME") == null ? backOfficeDataMap.get("CHANNEL_NAME") : logTableNewValues.get("CHANNEL_NAME");
      if (obj == null) {
        ps.setNull(30, Types.VARCHAR);
      } else {
        ps.setObject(30, obj);
      }

      obj = logTableNewValues.get("TXN_TYPE") == null ? backOfficeDataMap.get("TXN_TYPE") : logTableNewValues.get("TXN_TYPE");
      if (obj == null) {
        ps.setNull(31, Types.VARCHAR);
      } else {
        ps.setObject(31, obj);
      }

      obj = logTableNewValues.get("OFAC_MSG_ID") == null ? backOfficeDataMap.get("OFAC_MSG_ID") : logTableNewValues.get("OFAC_MSG_ID");
      if (obj == null) {
        ps.setNull(32, Types.VARCHAR);
      } else {
        ps.setObject(32, obj);
      }

      obj = logTableNewValues.get("HIT_STATUS") == null ? backOfficeDataMap.get("HIT_STATUS") : logTableNewValues.get("HIT_STATUS");
      if (obj == null) {
        ps.setNull(33, Types.SMALLINT);
      } else {
        ps.setObject(33, obj);
      }

      obj = logTableNewValues.get("ACTUAL_VALUE_DATE") == null ? backOfficeDataMap.get("ACTUAL_VALUE_DATE") : logTableNewValues.get("ACTUAL_VALUE_DATE");
      if (obj == null) {
        ps.setNull(34, Types.VARCHAR);
      } else {
        ps.setObject(34, obj);
      }
      int rows = QUERY_TO_INSERT_BO_LOG_TABLE_PS.executeUpdate();
      this.useCase1stats.endStmt(Stmt.insertToBOLogTable, stmt_start);
      return rows;
    } else {
      String s = "This branch should never be executed in this test";
      throw new QueryPerfException(s);
    }
  }

  // 14 ------------------------------------------------------------------------

  private static final String INSERT_OFAC_LOG_SQL = "INSERT INTO SEC_OWNER.SECL_OFAC_MESSAGE (BO_TXN_ID, CHN_TXN_ID, HIT_STATUS, MESSAGE_GUID, LAST_UPDATE_TIME, NO_OF_CHUNKS, FS_MESSAGE_ID) VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)";
  private PreparedStatement INSERT_OFAC_LOG_PS = null;

  private int persistFsMessage(Connection conn, OFACMessage ofacMessage)
  throws SQLException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering FsChunkedMessagePersisterDAOImpl.persistFsMessage");
    }
    if (INSERT_OFAC_LOG_PS == null) {
      INSERT_OFAC_LOG_PS = conn.prepareStatement(INSERT_OFAC_LOG_SQL);
    }
    INSERT_OFAC_LOG_PS.setString(1, ofacMessage.getBoTranId());
    INSERT_OFAC_LOG_PS.setString(2, ofacMessage.getChnTxnId());
    INSERT_OFAC_LOG_PS.setShort(3, (short)ofacMessage.getFsHitStatus().getValue());
    INSERT_OFAC_LOG_PS.setString(4, ofacMessage.getMessageGuId());
    INSERT_OFAC_LOG_PS.setInt(5, ofacMessage.getNoOfChunks());
    INSERT_OFAC_LOG_PS.setString(6, ofacMessage.getFsMessageId());
    int insertedRows = INSERT_OFAC_LOG_PS.executeUpdate();
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting FsChunkedMessagePersisterDAOImpl.persistFsMessage : Rows inserted : " + insertedRows);
    }
    return insertedRows;
  }

  // 15 ------------------------------------------------------------------------

  private static final String INSERT_LOG_OFAC_CHUNKED_SQL = "INSERT INTO SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE (CHUNK_ID, FS_MESSAGE_ID, BACKOFFICE_TXN_ID, CHANNEL_TXN_ID, CHUNK_SEQ, CHUNKED_MSG, SENT_DATE, RECEIVING_TIME, OFAC_COMMENTS) values (?,?,?,?,?,?,?,?,?)";
  private PreparedStatement INSERT_LOG_OFAC_CHUNKED_PS = null;

  public int persistChunkedMessages(Connection conn, FsChunkedMessage fsChunkedMessage)
  throws SQLException {
    if (INSERT_LOG_OFAC_CHUNKED_PS == null) {
      INSERT_LOG_OFAC_CHUNKED_PS = conn.prepareStatement(INSERT_LOG_OFAC_CHUNKED_SQL);
    }
    PreparedStatement ps = INSERT_LOG_OFAC_CHUNKED_PS;
    ps.setString(1, fsChunkedMessage.getChunkId());
    ps.setString(2, fsChunkedMessage.getFsMessageId());
    ps.setString(3, fsChunkedMessage.getBoTranId());
    ps.setString(4, fsChunkedMessage.getChnTxnId());
    ps.setInt(5, fsChunkedMessage.getChunkSequence());
    if (fsChunkedMessage.getChunkedMessage() != null){
      ps.setString(6, String.valueOf(fsChunkedMessage.getChunkedMessage()));
    } else{
      ps.setString(6, null);
    }
    ps.setTimestamp(7, fsChunkedMessage.getSentDate());
    ps.setTimestamp(8, fsChunkedMessage.getReceivedDate());
    ps.setString(9, fsChunkedMessage.getOfacComment());

    return INSERT_LOG_OFAC_CHUNKED_PS.executeUpdate();
  }

  //----------------------------------------------------------------------------

  private String createBORawData(MatchCriteria matchCriteria) {
    StringBuilder sb = new StringBuilder();
    sb.append("<MSG filename=\"SamplePAYEXT.txt\" sourceFormat=\"PAYEXT\">").append("\n")
      .append("<MLD>UNB+UNOA:3</MLD>").append("\n")
      .append("<ADR.SenderId>5025007000009</ADR.SenderId>").append("\n")
      .append("<ADR>14</ADR>").append("\n")
      .append("<DTE>006981815:1+120430:1159+80314</DTE>").append("\n")
      .append("<MLD>PAYEXT1</MLD>").append("\n")
      .append("<MLD>UNH+1+PAYEXT:D:96A:UN</MLD>").append("\n")
      .append("<CRN>BGM+451+GHFRT0088733652</CRN>").append("\n")
      .append("<TYP>BUSIN</TYP>").append("\n")
      .append("<TYP>PAI::9</TYP>").append("\n")
      .append("<TYP>:ZZZ</TYP>").append("\n")
      .append("<ACI>FCA+14</ACI>").append("\n")
      .append("<DTE>DTM+137:20120430:102</DTE>").append("\n")
      .append("<MLD>RFF+CR</MLD>").append("\n")
      .append("<CRN>RFF+ABO:5025006768762</CRN>").append("\n")
      .append("<CRN.ClientRef>").append(matchCriteria.clientRefNo).append("</CRN.ClientRef>").append("\n")
      .append("<CRN>RFF+ABO:5025006768762</CRN>").append("\n")
      .append("<VAL>MOA+9</VAL>").append("\n")
      .append("<VAL.Amount>").append(matchCriteria.amount).append("</VAL.Amount>").append("\n")
      .append("<VAL.Currency>").append(matchCriteria.currency).append("</VAL.Currency>").append("\n")
      .append("<MLD>DTM+209</MLD>").append("\n")
      .append("<DTE.ValueDate>").append(matchCriteria.valueDate).append("</DTE.ValueDate>").append("\n")
      .append("<MLD>102</MLD>").append("\n")
      .append("<MLD>RFF+PB</MLD>").append("\n")
      .append("<ACN.CompanyId>").append(matchCriteria.companyId).append("</ACN.CompanyId>").append("\n")
      .append("<MLD>FII+OR</MLD>").append("\n")
      .append("<ACN.OrigAccountNo>").append(matchCriteria.clientAccount).append("</ACN.OrigAccountNo>").append("\n")
      .append("<FII.OrigBankId>GOYFEbSMpo</FII.OrigBankId>").append("\n")
      .append("<MLD>FII+BF</MLD>").append("\n")
      .append("<ACN.BeneAccountNo>tGA4TRr6jn</ACN.BeneAccountNo>").append("\n")
      .append("<FII.BeneBankId>j7EYfLjkhh</FII.BeneBankId>").append("\n")
      .append("<ADR>:::</ADR>").append("\n")
      .append("<ADR.BeneBankAddr>MFdjiKakfqsHvfcgTsbq</ADR.BeneBankAddr>").append("\n")
      .append("<ADR.BeneBankCtry>ES</ADR.BeneBankCtry>").append("\n")
      .append("<MLD>NAD+OY+5025006768762</MLD>").append("\n")
      .append("<ADR.OrigName>Spanish American Caribbean</ADR.OrigName>").append("\n")
      .append("<MLD>NAD+BE+1000021496</MLD>").append("\n")
      .append("<ADR.BeneName>ASAM CLARKE AND CO (MADRID USD)C/GOYA 11</ADR.BeneName>").append("\n")
      .append("<ADR.BeneAddr>MADRID28001+ES</ADR.BeneAddr>").append("\n")
      .append("<TYP>PRC+8</TYP>").append("\n")
      .append("<TYP>DOC+380+37902012</TYP>").append("\n")
      .append("<VAL>MOA+12:1607.9</VAL>").append("\n")
      .append("<VAL>MOA+39:1607.9</VAL>").append("\n")
      .append("<VAL>MOA+52</VAL>").append("\n")
      .append("<DTE>DTM+171:20120229:102</DTE>").append("\n")
      .append("<MLD>GIS+37</MLD>").append("\n")
      .append("<MLD>UNT+23+1</MLD>").append("\n")
      .append("</MSG>")
      .append("\n");
    return sb.toString();
  }

  private MatchingInfo createMatchingInfo(String keyName, String dataType) {
    MatchingInfo mi = new MatchingInfo();
    mi.setBackOfficeCode(IPAY);
    mi.setKeyName(keyName);
    mi.setDataType(dataType);
    mi.setMatchingPriority(1);
    mi.setChnDataTable(SECT_CHANNEL_DATA);
    mi.setBoDataTable(SECL_BO_DATA_STATUS_HIST);
    mi.setBoOnBoardTimestamp(new Timestamp(new Date().getTime()));
    mi.setKeyOnBoardTimestamp(new Timestamp(new Date().getTime()));
    return mi;
  }

  static class MQNameBOCode {
    public String mqName;
    public String boCode;
  }

  static class MatchCriteria {
    public String currency;
    public Double amount;
    public String clientRefNo;
    public Timestamp valueDate;
    public String clientAccount;
    public String companyId;

    public MatchCriteria(ResultSet rs) throws SQLException {
      this.currency = rs.getString(1);
      this.amount = rs.getDouble(2);
      this.clientRefNo = rs.getString(3);
      this.valueDate = rs.getTimestamp(4);
      this.clientAccount = rs.getString(5);
      this.companyId = rs.getString(6);
    }
  }

  // outbound workload ---------------------------------------------------------

  /**
   * UseCase1 outbound workload. Throttles as specified in {@link UseCase1Prms
   * #workloadThrottleMs}.
   */
  public static void useCase1OutboundWorkloadTask() throws SQLException {
    UseCase1Client c = new UseCase1Client();
    c.initialize(OUTBOUND);
    c.useCase1OutboundWorkload();
  }

  private void useCase1OutboundWorkload() throws SQLException {
    SharedCounters counters = UseCase1Blackboard.getInstance().getSharedCounters();
    int throttleMs = UseCase1Prms.getWorkloadThrottleMs();
    while (true) {
      // check with traffic cop
      terminateIfNeeded(counters);
      pauseIfNeeded(counters);

      // run a workload batch
      long batchEnd = System.currentTimeMillis() + this.batchSeconds * 1000;
      while (System.currentTimeMillis() < batchEnd) {
        if (throttleMs != 0) { // throttle was set so choose each time at random
          MasterController.sleepForMs(UseCase1Prms.getWorkloadThrottleMs());
        }
        enableQueryPlanGeneration();
        doOutbound(this.connection, counters);
        disableQueryPlanGeneration();
      }
      Log.getLogWriter().info("Completed a workload batch");
    }
  }

  private void doOutbound(Connection conn, SharedCounters counters)
  throws SQLException {
    long stmt_start;

    // first get a valid FS_MESSAGE_ID
    stmt_start = this.useCase1stats.startStmt(Stmt.getFsMessageId);
    int fsMessageIdNum = getNextKey();
    while (!frontloaded) {
      // allow the inbound side to frontload 100 messages for each outbound thread
      long maxFsMessageId = counters.read(UseCase1Blackboard.maxFsMessageId);
      if (maxFsMessageId < 100 * this.numThreads) {
        Log.getLogWriter().info("Sleeping 10 seconds to allow inbound side to frontload messages");
        MasterController.sleepForMs(10000);
      } else {
        frontloaded = true;
      }
    }
    while (true) {
      // make sure outbound never gets ahead of inbound
      long maxFsMessageId = counters.read(UseCase1Blackboard.maxFsMessageId);
      if (fsMessageIdNum > maxFsMessageId + 75 * this.numThreads) {
        Log.getLogWriter().info("Sleeping 10 seconds to allow inbound side to get ahead");
        MasterController.sleepForMs(10000);
      } else {
        break;
      }
    }
    String fsMessageId = String.valueOf(fsMessageIdNum);
    this.useCase1stats.endStmt(Stmt.getFsMessageId, stmt_start);

    long outbound_start = this.useCase1stats.startOutbound();

    // 0 (not in UseCase1 list)
    // SELECT * FROM SEC_OWNER.SECL_OFAC_MESSAGE WHERE FS_MESSAGE_ID = ? FETCH FIRST ROW ONLY
    // look up enough information to do the next steps which involves an extra query over what UseCase1 is doing here
    stmt_start = this.useCase1stats.startStmt(Stmt.selectInitialOFACMsg);
    OFACMessage ofacMessage = selectInitialOFACMsg(conn, fsMessageId);
    this.useCase1stats.endStmt(Stmt.selectInitialOFACMsg, stmt_start);

    // 1
    // INSERT INTO SECL_OFAC_MESSAGE (BO_TXN_ID, CHN_TXN_ID, HIT_STATUS, MESSAGE_GUID, LAST_UPDATE_TIME, NO_OF_CHUNKS, FS_MESSAGE_ID) VALUES(?,?,?,?,CURRENT_TIMESTAMP,?,?)
    stmt_start = this.useCase1stats.startStmt(Stmt.persistFsMessage);
    int numRowsAddedForOutboundStep1 = persistFsMessage(conn, ofacMessage);
    this.useCase1stats.endStmt(Stmt.persistFsMessage, stmt_start);

    // 2
    // INSERT INTO SECL_OFAC_CHUNKED_MESSAGE (CHUNK_ID, FS_MESSAGE_ID, BACKOFFICE_TXN_ID, CHANNEL_TXN_ID, CHUNK_SEQ, CHUNKED_MSG, SENT_DATE, RECEIVING_TIME, OFAC_COMMENTS) values (?,?,?,?,?,?,?,?,?)
    stmt_start = this.useCase1stats.startStmt(Stmt.persistChunkedMessages);
    FsChunkedMessage fsChunkedMessage = new FsChunkedMessage();
    fsChunkedMessage.setChunkId(fsMessageId);
    fsChunkedMessage.setFsMessageId(fsMessageId);
    fsChunkedMessage.setBoTranId(ofacMessage.getBoTranId());
    fsChunkedMessage.setChnTxnId(ofacMessage.getChnTxnId());
    fsChunkedMessage.setChunkSequence(1);
    // for the test, just dummy up the clob with the sect channel raw data
    fsChunkedMessage.setChunkedMessage(SECT_CHANNEL_RAW_DATA.getBytes());
    fsChunkedMessage.setSentDate(new Timestamp(new Date().getTime()));
    fsChunkedMessage.setReceivedDate(new Timestamp(new Date().getTime()));
    fsChunkedMessage.setOfacComment(null);
    fsChunkedMessage.setFsMessageSendStatus(FsMessageSendStatus.FS_MSG_SENT);
    persistChunkedMessages(conn, fsChunkedMessage);
    this.useCase1stats.endStmt(Stmt.persistChunkedMessages, stmt_start);

    // 3 (same as 9, 12, 16)
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE CHUNK_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY
    stmt_start = this.useCase1stats.startStmt(Stmt.selectBasedOnChunkId);
    FsChunkedMessage fscm = selectBasedOnChunkId(conn, fsChunkedMessage);
    this.useCase1stats.endStmt(Stmt.selectBasedOnChunkId, stmt_start);

    // 4
    // INSERT INTO SECL_OFAC_CHUNKED_MESSAGE (CHUNK_ID, CHUNK_SEQ, SENT_LIFE_STATUS, BACKOFFICE_TXN_ID, OFAC_COMMENTS, CHECKSUM, CHANNEL_TXN_ID, CHUNKED_MSG, FIRC_OUT_RES_HEADER, SENT_DATE, RECEIVING_TIME, ACK_STATUS, OUT_STATUS, FS_MESSAGE_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    stmt_start = this.useCase1stats.startStmt(Stmt.persistChunkedMessages2);
    persistChunkedMessages2(conn, fscm);
    this.useCase1stats.endStmt(Stmt.persistChunkedMessages2, stmt_start);

    // 5 prequel (not in UseCase1 list)
    stmt_start = this.useCase1stats.startStmt(Stmt.getByBackOfficeTxnId);
    BOMessage boMessage = getByBackOfficeTxnId(conn, ofacMessage.getBoTranId());
    this.useCase1stats.endStmt(Stmt.getByBackOfficeTxnId, stmt_start);

    // 5
    // INSERT INTO SECL_BO_DATA_STATUS_HIST (BO_TXN_ID, BACKOFFICE_CODE, OFAC_MSG_ID, VALUE_DATE, CURRENCY, INSTR_CREATED_TIME, ACTUAL_VALUE_DATE, COMPANY_ID, BENE_BANK_NAME, BENE_ACCNT_NO, CLIENT_ACCOUNT, AMOUNT, CHANNEL_NAME, SCREENING_TIME, CLIENT_REF_NO) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    // what should change here from the last row inserted by the inbound processing?
    stmt_start = this.useCase1stats.startStmt(Stmt.insertStatusHist);
    int rowsInsertedStatusHist = insertStatusHist(conn, boMessage, stmt_start);

    // 6
    // INSERT INTO SECL_CHN_DATA_STATUS_HIST (CHANNEL_TXN_ID ,BACKOFFICE_CODE ,CHN_TXN_CREATED_TIME ,DATA_LIFE_STATUS ,MATCH_STATUS ,MATCH_CATEG_ID ,TXN_TYPE ,HIT_STATUS ,ACTUAL_VALUE_DATE ,LAST_UPDATE_TIME) VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
    insertChnHistFromMsg(conn, fscm);

    // 7
    // SELECT * FROM SECL_OFAC_MESSAGE WHERE FS_MESSAGE_ID = ? ORDER BY LAST_UPDATE_TIME DESC
    stmt_start = this.useCase1stats.startStmt(Stmt.selectInitialOFACMsgOrdered);
    OFACMessage ofac = selectInitialOFACMsgOrdered(conn, fsMessageId);
    this.useCase1stats.endStmt(Stmt.selectInitialOFACMsgOrdered, stmt_start);

    // 8
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE CHUNK_ID IN (SELECT DISTINCT CHUNK_ID FROM SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND CHUNK_SEQ = ?) ORDER BY LAST_UPDATE_TIME DESC
    stmt_start = this.useCase1stats.startStmt(Stmt.selectBasedOnFircMsgId);
    List<String> chunkIds = selectBasedOnFircMsgId(conn, fscm);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("selectBasedOnFircMsgId: chunkIds=" + chunkIds);
    }
    this.useCase1stats.endStmt(Stmt.selectBasedOnFircMsgId, stmt_start);

    // 9 (same as 3, 12, 16)
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE CHUNK_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY
    stmt_start = this.useCase1stats.startStmt(Stmt.selectBasedOnChunkId);
    fscm = selectBasedOnChunkId(conn, fscm);
    this.useCase1stats.endStmt(Stmt.selectBasedOnChunkId, stmt_start);

    // 10
    // SELECT COUNT(*) FROM SECL_OFAC_CHUNKED_MESSAGE
    stmt_start = this.useCase1stats.startStmt(Stmt.countChunkedMessages);
    int totalChunkRows = countChunkedMessages(conn);
    this.useCase1stats.endStmt(Stmt.countChunkedMessages, stmt_start);

    // 11
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND CHUNK_SEQ = ? ORDER BY LAST_UPDATE_TIME DESC
    stmt_start = this.useCase1stats.startStmt(Stmt.getByFircosoftMessageId);
    FsChunkedMessage fscm2 = getByFircosoftMessageId(conn, fscm);
    this.useCase1stats.endStmt(Stmt.getByFircosoftMessageId, stmt_start);

    // 12 (same as 3, 9, 16)
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE CHUNK_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY
    stmt_start = this.useCase1stats.startStmt(Stmt.selectBasedOnChunkId);
    fscm = selectBasedOnChunkId(conn, fscm);
    this.useCase1stats.endStmt(Stmt.selectBasedOnChunkId, stmt_start);

    // 13
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND ACK_STATUS IS NOT NULL
    stmt_start = this.useCase1stats.startStmt(Stmt.selectBasedOnAckStatus);
    fscm = selectBasedOnAckStatus(conn, fscm);
    this.useCase1stats.endStmt(Stmt.selectBasedOnAckStatus, stmt_start);

    // 14
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND OUT_STATUS IS NOT NULL
    stmt_start = this.useCase1stats.startStmt(Stmt.selectBasedOnOutStatus);
    fscm = selectBasedOnOutStatus(conn, fscm);
    this.useCase1stats.endStmt(Stmt.selectBasedOnOutStatus, stmt_start);

    // 15
    // SELECT COUNT(*) FROM SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND SENT_LIFE_STATUS = 4 AND (ACK_STATUS IS NULL OR OUT_STATUS = 5)
    stmt_start = this.useCase1stats.startStmt(Stmt.countUnprocessedChunksOnAckQueue);
    countUnprocessedChunksOnAckQueue(conn, fsMessageId);
    this.useCase1stats.endStmt(Stmt.countUnprocessedChunksOnAckQueue, stmt_start);

    // 16 (same as 3, 9, 12)
    // SELECT * FROM SECL_OFAC_CHUNKED_MESSAGE WHERE CHUNK_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY
    stmt_start = this.useCase1stats.startStmt(Stmt.selectBasedOnChunkId);
    fscm = selectBasedOnChunkId(conn, fscm);
    this.useCase1stats.endStmt(Stmt.selectBasedOnChunkId, stmt_start);

    this.useCase1stats.endOutbound(outbound_start);
  }

  // 0 -------------------------------------------------------------------------
  // look up enough information to do the next steps which involves an extra query over what UseCase1 is doing here
  private static final String SELECT_INITIAL_OFAC_MSG_SQL = "SELECT * FROM SEC_OWNER.SECL_OFAC_MESSAGE WHERE FS_MESSAGE_ID = ? FETCH FIRST ROW ONLY";
  private PreparedStatement SELECT_INITIAL_OFAC_MSG_PS = null;

  private OFACMessage selectInitialOFACMsg(Connection conn, String fsMessageId)
  throws SQLException {
    if (SELECT_INITIAL_OFAC_MSG_PS == null) {
      SELECT_INITIAL_OFAC_MSG_PS = conn.prepareStatement(SELECT_INITIAL_OFAC_MSG_SQL);
    }
    SELECT_INITIAL_OFAC_MSG_PS.setString(1, fsMessageId);
    ResultSet rs = SELECT_INITIAL_OFAC_MSG_PS.executeQuery();
    if (!rs.next()) {
      String s = "Did not find SECL_OFAC_MESSAGE with FS_MESSAGE_ID=" + fsMessageId;
      throw new QueryPerfException(s);
    }
    OFACMessage ofacMessage = new OFACMessage();
    ofacMessage.setMessageGuId(rs.getString("MESSAGE_GUID"));
    ofacMessage.setBoTranId(rs.getString("BO_TXN_ID"));
    ofacMessage.setChnTxnId(rs.getString("CHN_TXN_ID"));
    ofacMessage.setFsHitStatus(FsHitStatus.HIT);
    ofacMessage.setFsMessageId(fsMessageId);
    ofacMessage.setNoOfChunks(rs.getInt("NO_OF_CHUNKS"));
    rs.close();
    rs = null;
    return ofacMessage;
  }

  // 4 -------------------------------------------------------------------------
  private static final String INSERT_LOG_OFAC_CHUNKED_2_SQL = "INSERT INTO SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE (CHUNK_ID, CHUNK_SEQ, SENT_LIFE_STATUS, BACKOFFICE_TXN_ID, OFAC_COMMENTS, CHECKSUM, CHANNEL_TXN_ID, CHUNKED_MSG, FIRC_OUT_RES_HEADER, SENT_DATE, RECEIVING_TIME, ACK_STATUS, OUT_STATUS, FS_MESSAGE_ID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  private PreparedStatement INSERT_LOG_OFAC_CHUNKED_2_PS = null;

  public int persistChunkedMessages2(Connection conn, FsChunkedMessage fsChunkedMessage)
  throws SQLException {
    if (INSERT_LOG_OFAC_CHUNKED_2_PS == null) {
      INSERT_LOG_OFAC_CHUNKED_2_PS = conn.prepareStatement(INSERT_LOG_OFAC_CHUNKED_2_SQL);
    }
    PreparedStatement ps = INSERT_LOG_OFAC_CHUNKED_2_PS;
    ps.setString(1, fsChunkedMessage.getChunkId());
    ps.setInt(2, fsChunkedMessage.getChunkSequence());
    FsDataLifeStatus fsdatalife = FsDataLifeStatus.SENT_SS;
    ps.setObject(3, FsDataLifeStatus.getFsDataLifeStatus(fsdatalife));
    ps.setString(4, fsChunkedMessage.getBoTranId());
    ps.setString(5, fsChunkedMessage.getOfacComment());
    ps.setString(6, "checksum_here");
    ps.setString(7, fsChunkedMessage.getChnTxnId());
    ps.setString(8, String.valueOf(fsChunkedMessage.getChunkedMessage()));
    final Clob clob = conn.createClob();
    clob.setString(1, "THIS IS THE FIRCOSOFT HEADER DATA");
    ps.setClob(9, clob);
    ps.setTimestamp(10, fsChunkedMessage.getSentDate());
    ps.setTimestamp(11, fsChunkedMessage.getReceivedDate());
    FsAckStatus fsack = FsAckStatus.ACK_HIT;
    ps.setObject(12, FsAckStatus.getFsAckStatusCode(fsack));
    FsOutStatus fsout = FsOutStatus.OUT_PASSED;
    ps.setObject(13, FsOutStatus.getFsOutStatusCode(fsout));
    ps.setString(14, fsChunkedMessage.getFsMessageId());

    return INSERT_LOG_OFAC_CHUNKED_2_PS.executeUpdate();
  }

  // 6 -------------------------------------------------------------------------

  private static final String CHN_HIST_SUBQUERY_SQL = "SELECT CHANNEL_TXN_ID, BACKOFFICE_CODE, CHN_TXN_CREATED_TIME, DATA_LIFE_STATUS, MATCH_STATUS, MATCH_CATEG_ID, TXN_TYPE, HIT_STATUS, ACTUAL_VALUE_DATE, LAST_UPDATE_TIME FROM SEC_OWNER.SECL_CHN_DATA_STATUS_HIST WHERE CHANNEL_TXN_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY";
  private PreparedStatement CHN_HIST_SUBQUERY_PS = null;

  private static final String CHN_HIST_SUBQUERY_FROM_SECT_SQL = "SELECT CHANNEL_TXN_ID, BACKOFFICE_CODE, CHN_TXN_CREATED_TIME, DATA_LIFE_STATUS, MATCH_STATUS, MATCH_CATEG_ID, TXN_TYPE, HIT_STATUS, ACTUAL_VALUE_DATE, LAST_UPDATE_TIME FROM SEC_OWNER.SECT_CHANNEL_DATA WHERE CHANNEL_TXN_ID = ? FETCH FIRST ROW ONLY";
  private PreparedStatement CHN_HIST_SUBQUERY_FROM_SECT_PS = null;

  private static final String INSERT_CHN_HIST_SQL = "INSERT INTO SEC_OWNER.SECL_CHN_DATA_STATUS_HIST (CHANNEL_TXN_ID, BACKOFFICE_CODE, CHN_TXN_CREATED_TIME, DATA_LIFE_STATUS, MATCH_STATUS, MATCH_CATEG_ID, TXN_TYPE, HIT_STATUS, ACTUAL_VALUE_DATE, LAST_UPDATE_TIME) VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)";
  private PreparedStatement INSERT_CHN_HIST_PS = null;

  public int insertChnHistFromMsg(Connection conn, FsChunkedMessage fsChunkedMessage)
  throws SQLException {
    if (CHN_HIST_SUBQUERY_PS == null) {
      CHN_HIST_SUBQUERY_PS = conn.prepareStatement(CHN_HIST_SUBQUERY_SQL);
    }
    if (CHN_HIST_SUBQUERY_FROM_SECT_PS == null) {
      CHN_HIST_SUBQUERY_FROM_SECT_PS = conn.prepareStatement(CHN_HIST_SUBQUERY_FROM_SECT_SQL);
    }
    if (INSERT_CHN_HIST_PS == null) {
      INSERT_CHN_HIST_PS = conn.prepareStatement(INSERT_CHN_HIST_SQL);
    }

    int rows = 0;

    // first try the SECL_CHN_DATA_STATUS_HIST table
    long stmt_start_1 = this.useCase1stats.startStmt(Stmt.insertChnHistQuery1);
    CHN_HIST_SUBQUERY_PS.setString(1, fsChunkedMessage.getChnTxnId());
    ResultSet rs = CHN_HIST_SUBQUERY_PS.executeQuery();
    if (rs.next()) {
      // insert into SECL_CHN_DATA_STATUS_HIST
      fillInsertChnHistStmt(INSERT_CHN_HIST_PS, rs);
      this.useCase1stats.endStmt(Stmt.insertChnHistQuery1, stmt_start_1);
      rows = insertChnHistStmt(INSERT_CHN_HIST_PS);
    } else {
      this.useCase1stats.endStmt(Stmt.insertChnHistQuery1, stmt_start_1);
      // try the SECT_CHANNEL_DATA table
      long stmt_start_2 = this.useCase1stats.startStmt(Stmt.insertChnHistQuery2);
      CHN_HIST_SUBQUERY_FROM_SECT_PS.setString(1, fsChunkedMessage.getChnTxnId());
      rs = CHN_HIST_SUBQUERY_FROM_SECT_PS.executeQuery();
      if (rs.next()) {
        // insert into SECL_CHN_DATA_STATUS_HIST
        fillInsertChnHistStmt(INSERT_CHN_HIST_PS, rs);
        this.useCase1stats.endStmt(Stmt.insertChnHistQuery2, stmt_start_2);
        rows = insertChnHistStmt(INSERT_CHN_HIST_PS);
      } else {
        this.useCase1stats.endStmt(Stmt.insertChnHistQuery2, stmt_start_2);
      }
    }
    rs.close();
    rs = null;
    return rows;
  }

  private void fillInsertChnHistStmt(PreparedStatement ps, ResultSet rs)
  throws SQLException {
    ps.setObject(1, rs.getObject("CHANNEL_TXN_ID"));
    ps.setObject(2, rs.getObject("BACKOFFICE_CODE"));
    ps.setObject(3, rs.getObject("CHN_TXN_CREATED_TIME"));
    ps.setObject(4, rs.getObject("DATA_LIFE_STATUS"));
    ps.setObject(5, rs.getObject("MATCH_STATUS"));
    ps.setObject(6, rs.getObject("MATCH_CATEG_ID"));
    ps.setObject(7, rs.getObject("TXN_TYPE"));
    Object obj = rs.getObject("HIT_STATUS");
    if (obj == null) {
      ps.setNull(8, Types.SMALLINT);
    } else {
      ps.setObject(8, rs.getObject("HIT_STATUS"));
    }
    obj = rs.getObject("ACTUAL_VALUE_DATE");
    if (obj == null) {
      ps.setNull(9, Types.TIMESTAMP);
    } else {
      ps.setObject(9, rs.getObject("ACTUAL_VALUE_DATE"));
    }
  }

  private int insertChnHistStmt(PreparedStatement ps)
  throws SQLException {
    long stmt_start = this.useCase1stats.startStmt(Stmt.insertChnHist);
    int rows = 0;
    try {
      rows = INSERT_CHN_HIST_PS.executeUpdate();
      this.useCase1stats.endStmt(Stmt.insertChnHist, stmt_start);
    } catch (SQLIntegrityConstraintViolationException e) {
      this.useCase1stats.endStmt(Stmt.insertChnHistConstraintViolation, stmt_start);
    }
    return rows;
  }

  // 3, 9, 12, 16 --------------------------------------------------------------

  private static final String SELECT_BASED_ON_CHUNKID_SQL = "SELECT * FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE CHUNK_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY";
  private PreparedStatement SELECT_BASED_ON_CHUNKID_PS = null;

  // for the time being, the fsMessageId and chunkId are the same
  public FsChunkedMessage selectBasedOnChunkId(Connection conn, FsChunkedMessage fsChunkedMessage)
  throws SQLException {
    if (SELECT_BASED_ON_CHUNKID_PS == null) {
      SELECT_BASED_ON_CHUNKID_PS = conn.prepareStatement(SELECT_BASED_ON_CHUNKID_SQL);
    }
    SELECT_BASED_ON_CHUNKID_PS.setString(1, fsChunkedMessage.getFsMessageId());
    ResultSet rs = SELECT_BASED_ON_CHUNKID_PS.executeQuery();
    FsChunkedMessage fscm = new FsChunkedMessage();
    if (rs.next()) {
      readFsChunkedMessageResult(fscm, fsChunkedMessage, rs);
    }
    rs.close();
    rs = null;
    return fscm;
  }

  private void readFsChunkedMessageResult(FsChunkedMessage fscm, FsChunkedMessage fsChunkedMessage, ResultSet rs)
  throws SQLException {
    Object resultObj, obj;
    resultObj = rs.getObject("CHUNK_ID");
    obj = (resultObj == null) ? fsChunkedMessage.getChunkId() : resultObj;
    fscm.setChunkId((String)obj);
    resultObj = rs.getObject("FS_MESSAGE_ID");
    obj = (resultObj == null) ? fsChunkedMessage.getFsMessageId() : resultObj;
    fscm.setFsMessageId((String)obj);
    resultObj = rs.getObject("BACKOFFICE_TXN_ID");
    obj = (resultObj == null) ? fsChunkedMessage.getBoTranId() : resultObj;
    fscm.setBoTranId((String)obj);
    resultObj = rs.getObject("CHANNEL_TXN_ID");
    obj = (resultObj == null) ? fsChunkedMessage.getChnTxnId() : resultObj;
    fscm.setChnTxnId((String)obj);
    resultObj = rs.getObject("CHUNK_SEQ");
    // just use the one we already have
    fscm.setChunkSequence(fsChunkedMessage.getChunkSequence());
    resultObj = rs.getObject("CHUNKED_MSG");
    // just use the one we already have
    fscm.setChunkedMessage(fsChunkedMessage.getChunkedMessage());
    resultObj = rs.getObject("SENT_DATE");
    obj = (resultObj == null) ? fsChunkedMessage.getSentDate() : resultObj;
    fscm.setSentDate((Timestamp)obj);
    resultObj = rs.getObject("RECEIVING_TIME");
    obj = (resultObj == null) ? fsChunkedMessage.getReceivedDate() : resultObj;
    fscm.setReceivedDate((Timestamp)obj);
    resultObj = rs.getObject("OFAC_COMMENTS");
    obj = (resultObj == null) ? fsChunkedMessage.getOfacComment() : resultObj;
    fscm.setOfacComment((String)obj);
    resultObj = rs.getObject("SENT_LIFE_STATUS");
    // just use the one we already have
    fscm.setFsMessageSendStatus(fsChunkedMessage.getFsMessageSendStatus());
  }

  // 7 -------------------------------------------------------------------------

  private static final String SELECT_INITIAL_OFAC_MSG_ORDERED_SQL = "SELECT * FROM SEC_OWNER.SECL_OFAC_MESSAGE WHERE FS_MESSAGE_ID = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY";
  private PreparedStatement SELECT_INITIAL_OFAC_MSG_ORDERED_PS = null;

  private OFACMessage selectInitialOFACMsgOrdered(Connection conn, String fsMessageId)
  throws SQLException {
    if (SELECT_INITIAL_OFAC_MSG_ORDERED_PS == null) {
      SELECT_INITIAL_OFAC_MSG_ORDERED_PS = conn.prepareStatement(SELECT_INITIAL_OFAC_MSG_ORDERED_SQL);
    }
    SELECT_INITIAL_OFAC_MSG_ORDERED_PS.setString(1, fsMessageId);
    ResultSet rs = SELECT_INITIAL_OFAC_MSG_ORDERED_PS.executeQuery();
    if (!rs.next()) {
      String s = "Did not find SECL_OFAC_MESSAGE with FS_MESSAGE_ID=" + fsMessageId;
      throw new QueryPerfException(s);
    }
    OFACMessage ofacMessage = new OFACMessage();
    ofacMessage.setMessageGuId(rs.getString("MESSAGE_GUID"));
    ofacMessage.setBoTranId(rs.getString("BO_TXN_ID"));
    ofacMessage.setChnTxnId(rs.getString("CHN_TXN_ID"));
    ofacMessage.setFsHitStatus(FsHitStatus.HIT);
    ofacMessage.setFsMessageId(fsMessageId);
    ofacMessage.setNoOfChunks(rs.getInt("NO_OF_CHUNKS"));
    rs.close();
    rs = null;
    return ofacMessage;
  }

  // 8 -------------------------------------------------------------------------
  // This query was changed by UseCase1. It is no longer a subquery.

  //private static final String SELECT_BASED_FS_MSG_ID_SQL = "SELECT * FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE CHUNK_ID IN (SELECT DISTINCT CHUNK_ID FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND CHUNK_SEQ = ?) ORDER BY LAST_UPDATE_TIME DESC";
  private static final String SELECT_BASED_FS_MSG_ID_SQL = "SELECT CHUNK_ID FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND CHUNK_SEQ = ? ORDER BY LAST_UPDATE_TIME DESC";
  private PreparedStatement SELECT_BASED_FS_MSG_ID_PS = null;

  public List<String> selectBasedOnFircMsgId(Connection conn, FsChunkedMessage fscm)
  throws SQLException {
    if (SELECT_BASED_FS_MSG_ID_PS == null) {
      SELECT_BASED_FS_MSG_ID_PS = conn.prepareStatement(SELECT_BASED_FS_MSG_ID_SQL);
    }
    PreparedStatement ps = SELECT_BASED_FS_MSG_ID_PS;
    ps.setObject(1, fscm.getFsMessageId());
    ps.setObject(2, fscm.getChunkSequence());
    ResultSet rs = ps.executeQuery();
    List<String> chunkIds = new ArrayList();
    while (rs.next()) {
      chunkIds.add(rs.getString("CHUNK_ID"));
    }
    rs.close();
    rs = null;

    // in their code, this was followed by an insert

    return chunkIds;
  }

  // 10 ------------------------------------------------------------------------

  private static final String COUNT_CHUNK_SQL = "SELECT COUNT(*) FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE";
  private PreparedStatement COUNT_CHUNK_PS = null;

  public int countChunkedMessages(Connection conn)
  throws SQLException {
    if (COUNT_CHUNK_PS == null) {
      COUNT_CHUNK_PS = conn.prepareStatement(COUNT_CHUNK_SQL);
    }
    ResultSet rs = COUNT_CHUNK_PS.executeQuery();
    if (rs.next()) {
      return rs.getInt(1);
    } else {
      String s = "SECL_OFAC_CHUNKED_MESSAGE table has no rows";
      throw new QueryPerfException(s);
    }
  }

  // 11 ------------------------------------------------------------------------

  private static final String GET_BY_FSCM_SQL = "SELECT * FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND CHUNK_SEQ = ? ORDER BY LAST_UPDATE_TIME DESC FETCH FIRST ROW ONLY";
  private PreparedStatement GET_BY_FSCM_PS = null;

  public FsChunkedMessage getByFircosoftMessageId(Connection conn, FsChunkedMessage fsChunkedMessage)
  throws SQLException {
    if (GET_BY_FSCM_PS == null) {
      GET_BY_FSCM_PS = conn.prepareStatement(GET_BY_FSCM_SQL);
    }
    PreparedStatement ps = GET_BY_FSCM_PS;
    ps.setString(1, fsChunkedMessage.getFsMessageId());
    ps.setInt(2, fsChunkedMessage.getChunkSequence());
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      FsChunkedMessage fscm = new FsChunkedMessage();
      readFsChunkedMessageResult(fscm, fsChunkedMessage, rs);
      return fscm;
    } else {
      String s = "No SECL_OFAC_CHUNKED_MESSAGE with FS_MESSAGE_ID="
               + fsChunkedMessage.getFsMessageId() + " and CHUNK_SEQ="
               + fsChunkedMessage.getChunkSequence();
      throw new QueryPerfException(s);
    }
  }

  // 13 ------------------------------------------------------------------------

  private static final String GET_BY_ACK_SQL = "SELECT * FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND ACK_STATUS IS NOT NULL";
  private PreparedStatement GET_BY_ACK_PS = null;

  public FsChunkedMessage selectBasedOnAckStatus(Connection conn, FsChunkedMessage fsChunkedMessage)
  throws SQLException {
    if (GET_BY_ACK_PS == null) {
      GET_BY_ACK_PS = conn.prepareStatement(GET_BY_ACK_SQL);
    }
    PreparedStatement ps = GET_BY_ACK_PS;
    ps.setString(1, fsChunkedMessage.getFsMessageId());
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      FsChunkedMessage fscm = new FsChunkedMessage();
      readFsChunkedMessageResult(fscm, fsChunkedMessage, rs);
      return fscm;
    } else {
      String s = "No SECL_OFAC_CHUNKED_MESSAGE with FS_MESSAGE_ID="
               + fsChunkedMessage.getFsMessageId() + " and ACK_STATUS=NOT NULL";
      throw new QueryPerfException(s);
    }
  }

  // 14 ------------------------------------------------------------------------

  private static final String GET_BY_OUT_SQL = "SELECT * FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND OUT_STATUS IS NOT NULL";
  private PreparedStatement GET_BY_OUT_PS = null;

  public FsChunkedMessage selectBasedOnOutStatus(Connection conn, FsChunkedMessage fsChunkedMessage)
  throws SQLException {
    if (GET_BY_OUT_PS == null) {
      GET_BY_OUT_PS = conn.prepareStatement(GET_BY_OUT_SQL);
    }
    PreparedStatement ps = GET_BY_OUT_PS;
    ps.setString(1, fsChunkedMessage.getFsMessageId());
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      FsChunkedMessage fscm = new FsChunkedMessage();
      readFsChunkedMessageResult(fscm, fsChunkedMessage, rs);
      return fscm;
    } else {
      String s = "No SECL_OFAC_CHUNKED_MESSAGE with FS_MESSAGE_ID="
               + fsChunkedMessage.getFsMessageId() + " and OUT_STATUS=NOT NULL";
      throw new QueryPerfException(s);
    }
  }

  // 15 ------------------------------------------------------------------------

  private static final String COUNT_UNPROCESSED_SQL = "SELECT COUNT(*) FROM SEC_OWNER.SECL_OFAC_CHUNKED_MESSAGE WHERE FS_MESSAGE_ID = ? AND SENT_LIFE_STATUS = 4 AND (ACK_STATUS IS NULL OR OUT_STATUS = 5)";
  private PreparedStatement COUNT_UNPROCESSED_PS = null;

  public void countUnprocessedChunksOnAckQueue(Connection conn, String fsMessageId)
  throws SQLException {
    if (COUNT_UNPROCESSED_PS == null) {
      COUNT_UNPROCESSED_PS = conn.prepareStatement(COUNT_UNPROCESSED_SQL);
    }
    PreparedStatement ps = COUNT_UNPROCESSED_PS;
    ps.setString(1, fsMessageId);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      int numRows = rs.getInt(1);
      //Log.getLogWriter().info("Found " + numRows + " unprocessed chunks");
    //} else {
      //Log.getLogWriter().info("Found no unprocessed chunks");
    }
  }

//------------------------------------------------------------------------------
// workload support
//------------------------------------------------------------------------------

  private void terminateIfNeeded(SharedCounters counters) {
    if (counters.read(UseCase1Blackboard.stopSignal) > 0) {
      cacheEndTrim(this.trimIntervals, this.trimInterval);
      updateHydraThreadLocals();
      throw new StopSchedulingTaskOnClientOrder("Received stop signal from traffic cop");
    }
  }

  private void pauseIfNeeded(SharedCounters counters) {
    int cycles = 0;
    while (counters.read(UseCase1Blackboard.pauseSignal) > 0) {
      if (cycles == 0) {
        // first time through so notify traffic cop that this client is paused
        Log.getLogWriter().info("Received pause signal from traffic cop");
        counters.increment(UseCase1Blackboard.pauseCount);
      }
      // take a rest before polling again
      MasterController.sleepForMs(5000);
      ++cycles;
    }
    if (cycles > 0) {
      int seconds = 5 * cycles;
      Log.getLogWriter().info("Received resume signal from traffic cop after " + seconds + " seconds");
      // notify traffic cop that this client is no longer paused
      counters.decrement(UseCase1Blackboard.pauseCount);
    }
  }

//------------------------------------------------------------------------------
// data generation support
//------------------------------------------------------------------------------

  //private static final String BASE_TIME = "2012-01-01 01:01:00";
  //private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  //private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(TIMESTAMP_FORMAT);

  protected Timestamp generateTimestamp() {
    Date baseDate = null;
    SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      baseDate = DATE_FORMAT.parse("2012-01-01 01:01:00");
    } catch (ParseException e) {
      throw new QueryPerfException("2012-01-01 01:01:00 did not parse into a Date", e);
    }
    double addOn = Math.random() * 86400000 * 100;
    long newTime = baseDate.getTime() + (long)addOn;
    return new Timestamp(newTime);
  }

  private static final String READABLE_STRING = "854ku45Q985a.lsdk;,.ifpq4z58Ao45u.sdflkjsdgkjqwJKL:EIUR[p4pnm,.zxc239*h1@0*Fn/~5.+3&gwNa(.3K-c/2bd(kb1.(=wvz!/56NIwk-4/(#mDhn%kd#9jas9_n!KC0-c>3*(fbn3Fl)Fhaw.2?nz~l;1q3=Fbak1>ah1Bci23fripB319v*bnFl2Ba-cH$lfb?A)_2bgFo2_+Vv$al+b124kasbFV[2G}b@9ASFbCk2.KIhb4K";
  private static final int READABLE_STRING_LEN = READABLE_STRING.length();

  protected String generateString(int size) {
    if (size > READABLE_STRING_LEN) {
      String s = "Cannot create substring of size " + size
               + " from readable string of size " + READABLE_STRING_LEN;
      throw new QueryPerfException(s);
    }
    int offset = this.rng.nextInt(READABLE_STRING_LEN - size);
    return READABLE_STRING.substring(offset, offset+size);
  }

  protected String generateString(int minsize, int maxsize) {
    return generateString(this.rng.nextInt(maxsize - minsize) + minsize);
  }

  private static final char[] INTS = new char[] { '0', '1', '2', '3', '4',
                                                  '5', '6', '7', '8', '9' };

  protected String generateNumericString(int size) {
    char[] chars = new char[size];
    for (int i = 0; i < size; i++) {
      chars[i] = INTS[this.rng.nextInt(9)];
    }
    return new String(chars);
  }

  protected Double generateDecimal(int size, int decimals) {
    int i;
    char[] chars = new char[size + 1 + decimals];
    for (i = 0; i < size; i++) {
      chars[i] = INTS[this.rng.nextInt(9)];
    }
    while (chars[0] == '0') {
      chars[0] = INTS[this.rng.nextInt(9)];
    }
    chars[i++] = '.';
    for (int j = 0; j < decimals; j++) {
      chars[i+j] = INTS[this.rng.nextInt(9)];
    }
    return new Double(new String(chars));
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

  private String getDDLFile(String fname) {
    String fn = "$JTESTS/cacheperf.comparisons.gemfirexd.useCase1/ddl/" + fname;
    String newfn = EnvHelper.expandEnvVars(fn);
    Log.getLogWriter().info("DDL file: " + newfn);
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
// support
//------------------------------------------------------------------------------

  public static void dumpBucketsHook() {
    String clientName = RemoteTestModule.getMyClientName();
    // @todo rework this to determine if this is a datahost
    if (clientName.contains("server")
        || clientName.contains("sender") || clientName.contains("receiver")
        || clientName.contains("dbsync") || clientName.contains("prdata")) {
      Log.getLogWriter().info("Dumping local buckets");
      ResultSetHelper.dumpLocalBucket();
      Log.getLogWriter().info("Dumped local buckets");
    }
  }

  public static void dumpBucketsTask() {
    UseCase1Client c = new UseCase1Client();
    c.initialize();
    if (c.jid == 0) {
      Log.getLogWriter().info("Dumping local buckets");
      ResultSetHelper.dumpLocalBucket();
      Log.getLogWriter().info("Dumped local buckets");
    }
  }

  public static void postClientVmInfoTask() {
    int myVmid = RemoteTestModule.getMyVmid();
    ClientVmInfo info = new ClientVmInfo(myVmid,
                                         RemoteTestModule.getMyClientName(),
                                         null);
    UseCase1Blackboard.getInstance().getSharedMap().put(myVmid, info);
  }

//------------------------------------------------------------------------------
// hydra thread locals
//------------------------------------------------------------------------------

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.useCase1stats = getUseCase1Stats();
  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    setUseCase1Stats(this.useCase1stats);
  }

  /**
   * Gets the per-thread UseCase1Stats wrapper instance.
   */
  protected UseCase1Stats getUseCase1Stats() {
    useCase1stats = (UseCase1Stats)localuseCase1stats.get();
    return useCase1stats;
  }

  /**
   * Sets the per-thread UseCase1Stats wrapper instance.
   */
  protected void setUseCase1Stats(UseCase1Stats useCase1stats) {
    localuseCase1stats.set(useCase1stats);
  }

//------------------------------------------------------------------------------
// OVERRIDDEN METHODS
//------------------------------------------------------------------------------

  protected void initialize(int trimInterval) {
    super.initialize(trimInterval);
    cacheStartTrim(this.trimIntervals, this.trimInterval);
  }

  protected void initLocalParameters() {
    super.initLocalParameters();

    this.logQueries = QueryPrms.logQueries();
    this.logQueryResults = QueryPrms.logQueryResults();
  }

  protected String nameFor(int name) {
    switch (name) {
      case LOAD:
        return LOAD_NAME;
      case ETLGEN:
        return ETLGEN_NAME;
      case DUMMY:
        return DUMMY_NAME;
      case MATCH:
        return MATCH_NAME;
      case MATCHPURGE:
        return MATCHPURGE_NAME;
      case INBOUND:
        return INBOUND_NAME;
      case OUTBOUND:
        return OUTBOUND_NAME;
    }
    return super.nameFor(name);
  }

  /**
   * Gets the client's hydra site-specific task threadgroup id.  Exactly one
   * logical hydra thread in all of the threads in the threadgroups assigned to
   * the current task threads that are in the wan site of this thread has a
   * given sttgid.  The sttgids for each wan site are numbers consecutively
   * starting from 0.
   * <p>
   * Assumes client names are of the form *_ds_site_*, where site is the site
   * number.
   */
  protected int sttgid() {
    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    if (!clientName.contains("ds")) {
      return ttgid(); // this is not a site-specific hydra client
    }
    String arr[] = clientName.split("_");
    String localsite = null;
    for (int i = 0; i < arr.length; i++) {
      if (arr[i].equals("ds")) {
        localsite = arr[i+1];
        break;
      }
    }
    if (localsite == null) {
      String s = "Client name is not of the form *_ds_site_*, where site is the site number";
      throw new QueryPerfException(s);
    }
    String localthreadname = Thread.currentThread().getName();
    Vector threadgroups = RemoteTestModule.getCurrentThread().getCurrentTask()
                                          .getThreadGroupNames();
    int count = 0;
    for (List<String> mapping : RemoteTestModule.getClientMapping()) {
      String threadgroup = mapping.get(0);
      String threadname = mapping.get(1);
      if (threadgroups.contains(threadgroup)) {
        // candidate thread is in the task thread group
        String carr[] = threadname.split("_");
        for (int i = 0; i < carr.length; i++) {
          if (carr[i].equals("ds")) {
            if (localthreadname.startsWith(threadname)) {
              // this is the calling thread
              return count;
            }
            String site = carr[i+1];
            if (site.equals(localsite)) {
              // candidate thread is in this site
              ++count;
            }
          }
        }
      }
    }
    String s = "Should not happen";
    throw new HydraInternalException(s);
  }

  protected boolean getLogQueries() {
    return this.logQueries;
  }

  protected void setLogQueries(boolean b) {
    this.logQueries = b;
  }

  protected boolean generateQueryPlan() {
    return this.queryPlanFrequency != 0
        && this.ttgid < this.maxQueryPlanners //&& this.warmedUp
        && timeToGenerateQueryPlan();
  }
}

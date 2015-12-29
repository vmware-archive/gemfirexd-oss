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
package cacheperf.comparisons.gemfirexd.useCase5;

import cacheperf.CachePerfPrms;
import cacheperf.comparisons.gemfirexd.*;
import cacheperf.comparisons.gemfirexd.useCase5.UseCase5Stats.Stmt;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import hydra.*;
import hydra.gemfirexd.*;
import java.io.*;
import java.rmi.RemoteException;
import java.sql.*;
import java.util.*;
import objects.query.QueryPrms;
import sql.SQLHelper;
import sql.sqlutil.ResultSetHelper;

public class UseCase5Client extends QueryPerfClient {

  protected static final String CONFLICT_STATE = "X0Z02";
  protected static final String DEADLOCK_STATE = "ORA-00060"; // oracle table lock
  protected static final String DUPLICATE_STR = "The statement was aborted because it would have caused a duplicate key value";

  protected static final int numBuckets = UseCase5Prms.getNumBuckets();
  protected static final boolean timeStmts = UseCase5Prms.timeStmts();

  // trim intervals
  protected static final int TRANSACTIONS = 1580007;

  protected static final String TRANSACTIONS_NAME = "transactions";

  // hydra thread locals
  protected static HydraThreadLocal localuseCase5stats = new HydraThreadLocal();

  private static final String selstm = "select msn from terminals where terminal_id = ?";
  private static final String inslog = "insert into err_logs (source_id, log_time, err_msg) values (?, current_timestamp, ?)";
  private static final String updbal = "update terminal_accounts set outstanding_amount = outstanding_amount + ?  where terminal_id = ? and account_type = ? ";
  private static final String insbal = "insert into terminal_accounts (terminal_id, account_type, outstanding_amount) values (?, ?, ?)";
  private static final String updterm = "update terminals set msn = ?, logger_seq =?, status_update_time=current_timestamp where terminal_id = ?";
  private static final String insterm = "insert into terminals (terminal_id, terminal_ref_id, status_current, status_update_time, physical_location_group_id, msn, logger_seq) values (?, ?, 0, current_timestamp, 0, 0, 0)";
  private static final String instkt = "insert into betting_tickets (tsn, source_id, ticket_json, status) values (?, ?, ?, 1)";
  private static final String instsn = "insert into ticket_pools ( pool_id, source_id, tsn) values (?, ?, ?)";

  private PreparedStatement selstmPS = null;
  private PreparedStatement inslogPS = null;
  private PreparedStatement updbalPS = null;
  private PreparedStatement insbalPS = null;
  private PreparedStatement updtermPS = null;
  private PreparedStatement instermPS = null;
  private PreparedStatement instktPS = null;
  private PreparedStatement instsnPS = null;

  public UseCase5Stats useCase5stats; // statistics

  protected boolean logQueries;
  protected boolean logQueryResults;

  protected ResultSet rs = null;
  protected int result = 0;

//------------------------------------------------------------------------------
// statistics task
//------------------------------------------------------------------------------

  /**
   *  TASK to register the useCase5 performance statistics object.
   */
  public static void openStatisticsTask() {
    UseCase5Client c = new UseCase5Client();
    c.openStatistics();
  }

  private void openStatistics() {
    this.useCase5stats = getUseCase5Stats();
    if (this.useCase5stats == null) {
      this.useCase5stats = UseCase5Stats.getInstance();
      RemoteTestModule.openClockSkewStatistics();
    }
    setUseCase5Stats(this.useCase5stats);
  }
  
  /** 
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    UseCase5Client c = new UseCase5Client();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
  }
  
  protected void closeStatistics() {
    MasterController.sleepForMs(2000);
    if (this.useCase5stats != null) {
      RemoteTestModule.closeClockSkewStatistics();
      this.useCase5stats.close();
    }
  }

//------------------------------------------------------------------------------
// ddl
//------------------------------------------------------------------------------

  public static void executeDDLTask()
  throws FileNotFoundException, IOException, SQLException {
    UseCase5Client c = new UseCase5Client();
    c.initialize();
    if (c.sttgid == 0) {
      c.executeDDL();
    }
  }
  private void executeDDL()
  throws FileNotFoundException, IOException, SQLException {
    String fn = getDDLFile(UseCase5Prms.getDDLFile());
    List<String> stmts = getDDLStatements(fn);
    for (String stmt : stmts) {
      if (this.queryAPI == QueryPrms.GFXD) {
        if (stmt.contains("partition")) {
          stmt += " buckets " + this.numBuckets;
        }
      }
      Log.getLogWriter().info("Executing DDL: " + stmt);
      try {
        execute(stmt, this.connection);
      } catch (SQLException e) {
        if (stmt.contains("DROP") &&
            e.getMessage().indexOf("does not exist") == -1 && // GFXD/MYSQL
            e.getMessage().indexOf("Unknown table ") == -1) { // ORACLE
          throw e;
        }
      }
      commitDDL();
    }
  }

//------------------------------------------------------------------------------
// benchmark
//------------------------------------------------------------------------------

  /**
   * The UseCase5 benchmark task.
   */
  public static void useCase5InsertTask() throws SQLException {
    UseCase5Client c = new UseCase5Client();
    c.initialize(TRANSACTIONS);
    c.useCase5Insert();
  }
  private void useCase5Insert() throws SQLException {
    char [] chars = new char[1024];
    java.util.Arrays.fill(chars, 'A');
    String tkt = new String(chars);
    StringBuilder tsnbuf = new StringBuilder();
    tsnbuf.setLength(19);
    Random ran = new Random(this.tid);

    do {
      executeTaskTerminator(); // does not commit
      executeWarmupTerminator(); // does not commit
      insert(tkt, tsnbuf, ran);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount; // not really needed
    } while (!executeBatchTerminator()); // does not commit
  }

  private void insert(String tkt, StringBuilder tsnbuf, Random ran)
  throws SQLException {
    long start = this.useCase5stats.startTransaction();
    for (int j = 0; j < 19; j++) {
      tsnbuf.setCharAt(j, Character.toUpperCase(
                          Character.forDigit(ran.nextInt(16),16)));
    }
    _insert(this.connection, this.tid, tsnbuf.toString(), "1", 
            0, 100, 1, 100, 123, 456, tkt);
    this.useCase5stats.endTransaction(start, 0);
  }

  private void _insert(Connection conn, int source_id, String tsn,
                       String pool_ids_comma_sep_string,
                       int balance_type1, int balance_value1,
                       int balance_type2, int balance_value2,
                       int msn, int logger_seq, String ticket_json)
    throws SQLException {

    int balance_type = balance_type1;
    int balance_value = balance_value1;
    int rowcnt = 0;

    if (inslogPS == null) {
      inslogPS = conn.prepareStatement(inslog);
    }
    inslogPS.setInt(1, source_id);
    inslogPS.setString(2, ticket_json);
    inslogPS.executeUpdate();

    if (instsnPS == null) {
      instsnPS = conn.prepareStatement(instsn);
    }
    for (String _pool_id : pool_ids_comma_sep_string.split(",")) {
      instsnPS.setInt(1, Integer.parseInt(_pool_id));
      instsnPS.setInt(2, source_id);
      instsnPS.setString(3, tsn);
      instsnPS.executeUpdate();
    }

    if (instktPS == null) {
      instktPS = conn.prepareStatement(instkt);
    }
    instktPS.setString(1, tsn);
    instktPS.setInt(2, source_id);
    instktPS.setString(3, ticket_json);
    instktPS.executeUpdate();

    for (int i = 0; i < 2; i++) {
      if (balance_type >= 0) {
        if (updbalPS == null) {
          updbalPS = conn.prepareStatement(updbal);
        }
        updbalPS.setInt(1, balance_value);
        updbalPS.setInt(2, source_id);
        updbalPS.setInt(3, balance_type);
        rowcnt = updbalPS.executeUpdate();
        if (rowcnt == 0) {
          if (insbalPS == null) {
            insbalPS = conn.prepareStatement(insbal);
          }
          insbalPS.setInt(1, source_id);
          insbalPS.setInt(2, balance_type);
          insbalPS.setInt(3, balance_value);
          insbalPS.executeUpdate();
        }
        balance_type = balance_type2;
        balance_value = balance_value2;
      }
    }
    if (updtermPS == null) {
      updtermPS = conn.prepareStatement(updterm);
    }
    updtermPS.setInt(1, msn);
    updtermPS.setInt(2, logger_seq);
    updtermPS.setInt(3, source_id);
    rowcnt = updtermPS.executeUpdate();
    if (rowcnt == 0) {
      if (instermPS == null) {
        instermPS = conn.prepareStatement(insterm);
      }
      instermPS.setInt(1, source_id);
      instermPS.setString(2, Integer.toHexString(source_id).toUpperCase());
      instermPS.executeUpdate();
    }
    conn.commit();
  }

  public void query(Connection conn, int source_id) throws SQLException {
    ResultSet rs = null;
    char [] chars = new char[1024];
    java.util.Arrays.fill(chars, 'A');
    String tkt = new String(chars);
    StringBuilder tsnbuf=new StringBuilder();
    tsnbuf.setLength(19);

    Random ran = new Random();
    if (selstmPS == null) {
      selstmPS = conn.prepareStatement(selstm);
    }
    for (int donecount = 0; donecount < 1234567; donecount++) {
      for (int j = 0; j < 19; j++)
          tsnbuf.setCharAt(j, Character.toUpperCase(
                           Character.forDigit(ran.nextInt(16),16)));
      selstmPS.setInt(1, source_id);
      rs = selstmPS.executeQuery();
      rs.next();
      int msn = rs.getInt(1);
      assert msn == 123;
      rs.close();
      rs = null;
    }
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
    String fn = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase5/ddl/" + fname;
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
    UseCase5Client c = new UseCase5Client();
    c.initialize();
    if (c.jid == 0) {
      Log.getLogWriter().info("Dumping local buckets");
      ResultSetHelper.dumpLocalBucket();
      Log.getLogWriter().info("Dumped local buckets");
    }
  }

//------------------------------------------------------------------------------
// hydra thread locals
//------------------------------------------------------------------------------

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.useCase5stats = getUseCase5Stats();
  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    setUseCase5Stats(this.useCase5stats);
  }

  /**
   * Gets the per-thread UseCase5Stats wrapper instance.
   */
  protected UseCase5Stats getUseCase5Stats() {
    useCase5stats = (UseCase5Stats)localuseCase5stats.get();
    return useCase5stats;
  }

  /**
   * Sets the per-thread UseCase5Stats wrapper instance.
   */
  protected void setUseCase5Stats(UseCase5Stats useCase5stats) {
    localuseCase5stats.set(useCase5stats);
  }

//------------------------------------------------------------------------------
// OVERRIDDEN METHODS
//------------------------------------------------------------------------------

  protected void initLocalParameters() {
    super.initLocalParameters();

    this.logQueries = QueryPrms.logQueries();
    this.logQueryResults = QueryPrms.logQueryResults();
  }

  protected String nameFor(int name) {
    switch (name) {
      case TRANSACTIONS:
        return TRANSACTIONS_NAME;
    }
    return super.nameFor(name);
  }

  protected boolean getLogQueries() {
    return this.logQueries;
  }

  protected void setLogQueries(boolean b) {
    this.logQueries = b;
  }
}

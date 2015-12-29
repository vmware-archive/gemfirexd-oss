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
package cacheperf.comparisons.gemfirexd.useCase5.poc;

import cacheperf.CachePerfPrms;
import cacheperf.comparisons.gemfirexd.*;
import cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Stats.Stmt;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import hydra.*;
import hydra.gemfirexd.*;
import java.nio.charset.Charset;
import java.io.*;
import java.rmi.RemoteException;
import java.sql.Blob;
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
import sql.sqlutil.ResultSetHelper;

import javax.sql.rowset.serial.SerialBlob;

public class UseCase5Client extends QueryPerfClient {

  public static enum STATUS {
    ON, OFF;
  }

  protected static final int NUM_TERMINALS = UseCase5Prms.getNumTerminals();

  protected static final String CONFLICT_STATE = "X0Z02";
  protected static final String DEADLOCK_STATE = "ORA-00060"; // oracle table lock
  protected static final String DUPLICATE_STR = "The statement was aborted because it would have caused a duplicate key value";

  protected static final int numBuckets = UseCase5Prms.getNumBuckets();
  protected static final boolean timeStmts = UseCase5Prms.timeStmts();

  private static final String CB_SELL_PREFIX = "S_";
  private static final String CB_PAY_PREFIX = "P_";
  private static final int C_CB = 101; // Event ID for Cash Betting
  private static final short C_TicketType_CB = 1;
  private static final short C_TransactionType_Sell = 1;
  private static final short C_TransactionType_Pay = 2;
  private static final String C_CBEventInfo = "Cash betting";
  private static final String C_CBMessageResult = "TICKET_SOLD_MESSAGE";
  private static final String C_CashPaidMessageResult = "TICKET_PAID_MESSAGE";
  private static final String C_FTerminalID = "terminal_id";
  private static final String C_FTerminalStatus = "terminal_status";
  private static final int OLTP_SERVICE_ID = 99; // value is hidden in compiled code

  // trim intervals
  protected static final int TERMINALS = 1980507;
  protected static final int SELL_TRANSACTIONS = 1980508;
  protected static final int PAY_TRANSACTIONS = 1980509;

  protected static final String TERMINALS_NAME = "terminals";
  protected static final String SELL_TRANSACTIONS_NAME = "sells";
  protected static final String PAY_TRANSACTIONS_NAME = "pays";

  // hydra thread locals
  protected static HydraThreadLocal localuseCase5stats = new HydraThreadLocal();

  // prepared statements
  protected PreparedStatement insTerminalStmt = null;
  protected PreparedStatement insSourceStmt = null;
  private PreparedStatement insSourceMessageLogStmt;
  private PreparedStatement insSingleTicketStmt;
  private PreparedStatement insTicketHistoryStmt;
  private PreparedStatement insSourceMessageResultStmt;
  private PreparedStatement insTicketsToPoolStmt;
  private PreparedStatement selTerminalStatusStmt;
  private PreparedStatement insTerminalBalanceTransactionLogStmt;
  private PreparedStatement selectTicketByTSNStmt;
  private PreparedStatement updTicketForPayStmt;

  public UseCase5Stats useCase5stats; // statistics

  protected boolean logQueries;
  protected boolean logQueryResults;

  private byte[] ticketContent = "RACEBET () HV SAT WIN 1*1 $10".getBytes(Charset.forName("UTF-8"));
  private float ticketCost = 10.0F;

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
      Statement s = this.connection.createStatement();
      s.execute(stmt);
      this.connection.commit();
    }
  }

//------------------------------------------------------------------------------
// terminal setup
//------------------------------------------------------------------------------

  /**
   * Set up terminals.
   */
  public static void setupTerminalsTask() throws SQLException {
    UseCase5Client c = new UseCase5Client();
    c.initialize(TERMINALS);
    if (c.ttgid == 0) {
      c.setupTerminals();
    }
  }

  private void setupTerminals() throws SQLException {
    Log.getLogWriter().info("Creating " + NUM_TERMINALS + " terminals...");
    long stmt_start = 0;
    for (int i = 0; i < NUM_TERMINALS; i++) {

      /**
       * TABLE terminals
       *
       * terminal_id                 BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY
       * terminal_status_update_time TIMESTAMP NOT NULL
       * terminal_status             SMALLINT NOT NULL
       * PRIMARY KEY (terminal_id)
       * -- FOREIGN KEY (terminal_id) REFERENCES source_types (ref_id)
       *
       * CREATE UNIQUE INDEX terminal_udx01 ON terminals(terminal_id)
       * CREATE INDEX source_types_clx02 ON source_types (source_id, source_type_id)
       */
      if (this.insTerminalStmt == null) {
        String stmt = "INSERT INTO terminals (terminal_status_update_time, terminal_status) VALUES (?, ?)";
        this.insTerminalStmt = this.connection.prepareStatement(stmt, Statement.RETURN_GENERATED_KEYS);
      }

      Timestamp t = new Timestamp(System.currentTimeMillis());
      this.insTerminalStmt.setTimestamp(1, t);
      this.insTerminalStmt.setShort(2, (short)1);
      if (this.logQueries) {
        Log.getLogWriter().info("EXECUTING insTerminalStmt: " + t);
      }
      if (this.timeStmts) {
        stmt_start = this.useCase5stats.startStmt(Stmt.insTerminal);
      }
      if (this.insTerminalStmt.executeUpdate() != 1) {
        throw new SQLException("insTerminal failed for Terminal " + i);
      }
      if (this.timeStmts) {
        this.useCase5stats.endStmt(Stmt.insTerminal, stmt_start);
      }

      // get the generated terminal_id
      ResultSet rs = this.insTerminalStmt.getGeneratedKeys();
      if (rs == null) {
        throw new SQLException("insTerminal failed to get generated keys for Terminal " + i);
      }
      int terminalId = -1;
      if (rs.next()) {
        terminalId = rs.getInt(1);
      } else {
        throw new SQLException("insTerminal failed to get generated key for Terminal " + i);
      }
      if (this.logQueries) {
        Log.getLogWriter().info("GENERATED terminalId: " + terminalId);
      }
      rs.close();
      rs = null;

      /**
       * TABLE source_types
       *
       * source_id      CHAR(10) NOT NULL
       * ref_id         INT NOT NULL       -- terminal_id for terminal operations 
       * source_type_id SMALLINT NOT NULL
       * -- PRIMARY KEY (source_id)
       * -- FOREIGN KEY (ref_id) REFERENCES terminals (terminal_id)
       *
       * CREATE INDEX source_types_clx01 ON source_types (ref_id)
       */
      if (this.insSourceStmt == null) {
        String stmt = "INSERT INTO source_types (source_id, ref_id, source_type_id) VALUES (?, ?, ?)";
        this.insSourceStmt = this.connection.prepareStatement(stmt, Statement.RETURN_GENERATED_KEYS);
      }

      String hexTerminalId = Integer.toHexString(terminalId);
      this.insSourceStmt.setString(1, hexTerminalId);
      this.insSourceStmt.setInt(2, terminalId);
      this.insSourceStmt.setShort(3, (short)1);
      if (this.logQueries) {
        Log.getLogWriter().info("EXECUTING insSourceStmt: " + hexTerminalId + " " + terminalId);
      }
      if (this.timeStmts) {
        stmt_start = this.useCase5stats.startStmt(Stmt.insSource);
      }
      this.insSourceStmt.executeUpdate();
      if (this.timeStmts) {
        this.useCase5stats.endStmt(Stmt.insSource, stmt_start);
      }

      this.connection.commit();
      Log.getLogWriter().info("Created terminal " + terminalId);
    }
  }

//------------------------------------------------------------------------------
// sell benchmark
//------------------------------------------------------------------------------

  /**
   * Sell cash bets task.
   */
  public static void sellCashBetsTask()
  throws SQLException, NumberFormatException {
    UseCase5Client c = new UseCase5Client();
    c.initialize(SELL_TRANSACTIONS);
    c.sellCashBets();
  }

  private void sellCashBets()
  throws SQLException, NumberFormatException {
    List<String> poolIds = new ArrayList();
    poolIds.add("1001");
    poolIds.add("1002");
    poolIds.add("1003");
    do {
      int key = getNextKey(); // transaction id
      executeTaskTerminator();
      executeWarmupTerminator();
      enableQueryPlanGeneration();

      long start = this.useCase5stats.startCBSell();
      logToSourceMessageAndValidate(key, CB_SELL_PREFIX);
      sell(key, poolIds);
      this.useCase5stats.endCBSell(start);

      disableQueryPlanGeneration();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
    } while (!executeBatchTerminator());
  }

  private void sell(int key, List<String> poolIds)
  throws SQLException, NumberFormatException {
    int terminalIdInternal = this.rng.nextInt(1, NUM_TERMINALS);
    String terminalId = Integer.toHexString(terminalIdInternal);
    String ticketTSN = String.valueOf(key);
    String ticketTransactionId = CB_SELL_PREFIX + key;
    ResultSet rs = null;
    long stmt_start = 0;

    /**
     * TABLE tickets
     *
     * ticket_id                  BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY
     * ticket_type                SMALLINT NOT NULL
     * tsn                        CHAR(20) NOT NULL
     * base_investment            BIGINT NOT NULL
     * total_investment           BIGINT NOT NULL
     * parent_ticket_id           BIGINT NOT NULL DEFAULT -1 -- Default when it is the initial sell of a ticket
     * current_ticket_status      SMALLINT NOT NULL DEFAULT 1
     * current_responded_ap       SMALLINT NOT NULL
     * current_responded_terminal INT NOT NULL
     * paid_status                SMALLINT NOT NULL DEFAULT 6
     * paid_amount                BIGINT NOT NULL DEFAULT 0
     * dividend_status            SMALLINT NOT NULL DEFAULT 9
     * dividend_amount            BIGINT NOT NULL DEFAULT 0
     * locked_by                  BIGINT NOT NULL DEFAULT -1 -- this field is used when pay and cancel ticket
     * ticket_json                BLOB(1024) NOT NULL
     *
     * PRIMARY KEY (ticket_id)
     *
     * CREATE UNIQUE INDEX tickets_udx01 ON tickets (tsn);
     */
    if (this.insSingleTicketStmt == null) {
      String s = "INSERT INTO tickets (tsn, ticket_type, base_investment, total_investment, current_responded_ap, current_responded_terminal, ticket_json) VALUES (?, ?, ?, ?, ?, ?, ?)";
      this.insSingleTicketStmt = this.connection.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);
    }
    this.insSingleTicketStmt.setString(1, ticketTSN);
    this.insSingleTicketStmt.setShort(2, C_TicketType_CB);
    this.insSingleTicketStmt.setLong(3, 500L);
    this.insSingleTicketStmt.setLong(4, (long)ticketCost * 100);
    this.insSingleTicketStmt.setInt(5, OLTP_SERVICE_ID);
    this.insSingleTicketStmt.setInt(6, terminalIdInternal);
    this.insSingleTicketStmt.setBlob(7, new SerialBlob(ticketContent));
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING insSingleTicketStmt: " + ticketTSN + " " + terminalIdInternal);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insSingleTicket);
    }
    if (this.insSingleTicketStmt.executeUpdate() != 1) {
      throw new SQLException("insSingleTicket failed for Ticket TSN " + key);
    }

    // get the generated ticket_id
    rs = this.insSingleTicketStmt.getGeneratedKeys();
    if (rs == null) {
        throw new SQLException("insSingleTicket failed to get generated keys for Ticket TSN " + key);
    }
    long ticketId = -1;
    if (rs.next()) {
      ticketId = rs.getLong(1);
      rs.close();
      rs = null;
    } else {
      throw new SQLException("insSingleTicket failed to get generated key for Ticket TSN " + key);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insSingleTicket, stmt_start);
    }

    /**
     * TABLE ticket_transaction_log
     *
     * seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
     * transaction_id       CHAR(32) NOT NULL,
     * transaction_type     SMALLINT NOT NULL,
     * ticket_id            BIGINT NOT NULL default 0,
     * responded_ap         SMALLINT NOT NULL,
     * responded_terminal   INT NOT NULL,
     * event_info           VARCHAR(255) NOT NULL,
     * created_time         TIMESTAMP default current_TIMESTAMP NOT NULL,
     *
     * PRIMARY KEY (seq_no)
     *
     * CREATE UNIQUE INDEX ticket_transaction_log_udx01 ON ticket_transaction_log (transaction_id);
     */
    if (this.insTicketHistoryStmt == null) {
      String s = "INSERT INTO ticket_transaction_log (transaction_id, ticket_id, responded_ap, responded_terminal, event_info, transaction_type) VALUES (?, ?, ?, ?, ?, ?)";
      this.insTicketHistoryStmt = this.connection.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);
    }
    this.insTicketHistoryStmt.setString(1, ticketTransactionId);
    this.insTicketHistoryStmt.setLong(2, ticketId);
    this.insTicketHistoryStmt.setInt(3, OLTP_SERVICE_ID);
    this.insTicketHistoryStmt.setInt(4, terminalIdInternal);
    this.insTicketHistoryStmt.setString(5, C_CBEventInfo);
    this.insTicketHistoryStmt.setShort(6, C_TransactionType_Sell);
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insTicketHistory);
    }
    if (this.insTicketHistoryStmt.executeUpdate() != 1) {
      throw new SQLException("insTicketHistory failed for Ticket TSN " + key);
    }

    // get the generated transaction seq_no
    rs = this.insTicketHistoryStmt.getGeneratedKeys();
    if (rs == null) {
      throw new SQLException("insTicketHistory failed to get generated keys for Ticket TSN " + key);
    }
    long ticketTransactionSeqNo = -1L;
    if (rs.next()) {
      ticketTransactionSeqNo = rs.getLong(1);
      rs.close();
      rs = null;
    } else {
      throw new SQLException("insTicketHistory failed to get generated key for Ticket TSN " + key);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insTicketHistory, stmt_start);
    }

    /**
     * TABLE source_message_results
     *
     * seq_no         BIGINT GENERATED ALWAYS AS IDENTITY
     * transaction_id CHAR(32) NOT NULL
     * return_message VARCHAR(256) NOT NULL
     * responded_ap   INT NOT NULL
     * ce_seq_no      BIGINT default 0
     * created_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
     *
     * PRIMARY KEY (seq_no)
     *
     * CREATE INDEX source_message_results_clx01 ON source_message_results (transaction_id)
     */
    if (this.insSourceMessageResultStmt == null) {
      String s = "INSERT INTO source_message_results (transaction_id, return_message, responded_ap) VALUES (?, ?, ?)";
      this.insSourceMessageResultStmt = connection.prepareStatement(s);
    }
    this.insSourceMessageResultStmt.setString(1, ticketTransactionId);
    this.insSourceMessageResultStmt.setString(2, C_CBMessageResult);
    this.insSourceMessageResultStmt.setInt(3, OLTP_SERVICE_ID);
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insSourceMessageResult);
    }
    this.insSourceMessageResultStmt.executeUpdate();
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insSourceMessageResult, stmt_start);
    }

    /**
     * TABLE tickets_in_pool
     *
     * seq_no    BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY
     * pool_id   BIGINT NOT NULL
     * tsn       CHAR(20) NULL
     * ticket_id BIGINT NULL
     *
     * PRIMARY KEY (seq_no)
     *
     * CREATE INDEX tickets_in_pool_clx01 ON tickets_in_pool (pool_id)
     */
    if (this.insTicketsToPoolStmt == null) {
      String s = "INSERT INTO tickets_in_pool (pool_id, ticket_id) VALUES (?, ?)";
      this.insTicketsToPoolStmt = this.connection.prepareStatement(s);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insTicketsToPool);
    }
    for (String poolId : poolIds) {
      this.insTicketsToPoolStmt.setLong(2, ticketId);
      this.insTicketsToPoolStmt.setLong(1, Long.parseLong(poolId));
      this.insTicketsToPoolStmt.addBatch();
    }
    this.insTicketsToPoolStmt.executeBatch();
    this.insTicketsToPoolStmt.clearBatch();
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insTicketsToPool, stmt_start);
    }

    /**
     * TABLE terminal_balance_transaction_log
     *
     * seq_no                        BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY
     * terminal_id                   BIGINT NOT NULL
     * ticket_transaction_log_seq_no BIGINT NOT NULL
     * transaction_amount            BIGINT NOT NULL
     * transaction_type              SMALLINT NOT NULL
     * created_time                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL (not used here)
     *
     * PRIMARY KEY (seq_no)
     */
    if (this.insTerminalBalanceTransactionLogStmt == null) {
      String stmt = "INSERT INTO terminal_balance_transaction_log (terminal_id, ticket_transaction_log_seq_no, transaction_type, transaction_amount) VALUES (?, ?, ?, ?)";
      this.insTerminalBalanceTransactionLogStmt = this.connection.prepareStatement(stmt);
    }
    this.insTerminalBalanceTransactionLogStmt.setLong(1, terminalIdInternal);
    this.insTerminalBalanceTransactionLogStmt.setLong(2, ticketTransactionSeqNo);
    this.insTerminalBalanceTransactionLogStmt.setShort(3, C_TransactionType_Sell);
    this.insTerminalBalanceTransactionLogStmt.setLong(4, (long)ticketCost * 100);
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insTerminalBalanceTransactionLog);
    }
    this.insTerminalBalanceTransactionLogStmt.executeUpdate();
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insTerminalBalanceTransactionLog, stmt_start);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startCommit();
    }
    this.connection.commit();
    if (this.timeStmts) {
      this.useCase5stats.endCommit(stmt_start);
    }
  }

  private void logToSourceMessageAndValidate(int key, String prefix)
  throws SQLException {
    int terminalIdInternal = this.rng.nextInt(1, NUM_TERMINALS);
    String terminalId = Integer.toHexString(terminalIdInternal);
    String ticketTransactionId = prefix + key;
    ResultSet rs = null;
    long stmt_start = 0;

    /**
     * TABLE source_message_logs
     *
     * seq_no          BIGINT GENERATED ALWAYS AS IDENTITY
     * transaction_id  CHAR(32) NOT NULL
     * message_content BLOB(256) NOT NULL
     * arrival_time    TIMESTAMP NOT NULL
     * source_id       CHAR(10) NOT NULL
     * event_id        INT NOT NULL
     * msn             SMALLINT NOT NULL default 0
     * responded_ap    SMALLINT NOT NULL
     *
     * PRIMARY KEY (seq_no)
     *
     * CREATE UNIQUE INDEX source_message_logs_clx01 ON source_message_logs (transaction_id, source_id)
     */
    if (this.insSourceMessageLogStmt == null) {
      String s = "INSERT INTO source_message_logs (transaction_id, message_content, arrival_time, source_id, event_id, responded_ap) VALUES (?, ?, ?, ?, ?, ?)";
      this.insSourceMessageLogStmt = this.connection.prepareStatement(s);
    }
    this.insSourceMessageLogStmt.setString(1, ticketTransactionId);
    this.insSourceMessageLogStmt.setBytes(2, ticketContent);
    this.insSourceMessageLogStmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
    this.insSourceMessageLogStmt.setString(4, terminalId);
    this.insSourceMessageLogStmt.setInt(5, C_CB);
    this.insSourceMessageLogStmt.setInt(6, OLTP_SERVICE_ID);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING insSourceMessageLogStmt: " + ticketTransactionId + " " + terminalId);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insSourceMessageLog);
    }
    if (insSourceMessageLogStmt.executeUpdate() != 1) {
      throw new SQLException("insSourceMessageLog failed for Ticket TSN " + key);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insSourceMessageLog, stmt_start);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startCommit();
    }
    this.connection.commit();
    if (this.timeStmts) {
      this.useCase5stats.endCommit(stmt_start);
    }

    /**
     * CREATE VIEW mv_source_id_and_terminal AS
     * SELECT source_id, terminal_id, terminal_status_update_time, terminal_status, source_type_id
     * FROM source_types, terminals
     * WHERE source_types.ref_id = terminals.terminal_id;
     */
    if (this.selTerminalStatusStmt == null) {
      String s = "SELECT terminal_status, terminal_id FROM mv_source_id_and_terminal WHERE source_id = ? AND source_type_id = 1";
      this.selTerminalStatusStmt = this.connection.prepareStatement(s);
    }
    this.selTerminalStatusStmt.setString(1, terminalId);
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.selTerminalStatus);
    }
    rs = this.selTerminalStatusStmt.executeQuery();
    if (rs.next()) {
      int tmpTerminalId = rs.getInt(C_FTerminalID);
      int tmpTerminalStatus = rs.getShort(C_FTerminalStatus);
      rs.close();
      rs = null;
    } else {
      throw new SQLException("selTerminalStatus failed for Terminal " + terminalId);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.selTerminalStatus, stmt_start);
    }
  }

//------------------------------------------------------------------------------
// pay benchmark
//------------------------------------------------------------------------------

  /**
   * Pay cash bets task.
   */
  public static void payCashBetsTask()
  throws SQLException, NumberFormatException {
    UseCase5Client c = new UseCase5Client();
    c.initialize(PAY_TRANSACTIONS);
    c.payCashBets();
  }

  private void payCashBets()
  throws SQLException, NumberFormatException {
    do {
      int key = getNextKey(); // transaction id
      executeTaskTerminator();
      executeWarmupTerminator();
      enableQueryPlanGeneration();

      long start = this.useCase5stats.startCBPay();
      logToSourceMessageAndValidate(key, CB_PAY_PREFIX);
      pay(key);
      this.useCase5stats.endCBPay(start);

      disableQueryPlanGeneration();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
    } while (!executeBatchTerminator());
  }

  private void pay(int key)
  throws SQLException {
    String ticketTSN = String.valueOf(key);
    String ticketTransactionId = CB_PAY_PREFIX + key;
    int terminalIdInternal = this.rng.nextInt(1, NUM_TERMINALS);
    long ticketId = 0;
    int ticketStatus = 0;
    long paidAmount = 0;
    long dividend = 0;
    ResultSet rs = null;
    long stmt_start = 0;

    if (this.selectTicketByTSNStmt == null) {
      String s = "SELECT ticket_json, ticket_id, current_ticket_status, base_investment, total_investment, paid_amount, total_investment, dividend_amount FROM tickets WHERE tsn=?";
      this.selectTicketByTSNStmt = this.connection.prepareStatement(s);
    }
    this.selectTicketByTSNStmt.setString(1, ticketTSN);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING selectTicketByTSNStmt: " + ticketTSN);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.selectTicketByTSN);
    }
    rs = this.selectTicketByTSNStmt.executeQuery();
    if (rs.next()) {
      Blob content = rs.getBlob("ticket_json");
      byte[] contentBytes = content.getBytes(1, (int)content.length());
      ticketId = rs.getLong("ticket_id");
      ticketStatus = rs.getInt("current_ticket_status");
      paidAmount = rs.getLong("paid_amount");
      dividend = rs.getLong("total_investment");
      rs.getLong("base_investment");
      rs.getLong("total_investment");
      rs.close();
      rs = null;
    } else {
      throw new SQLException("selectTicketByTSN failed for Ticket TSN " + ticketTSN);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.selectTicketByTSN, stmt_start);
    }

    if (this.updTicketForPayStmt == null) {
      String s = "UPDATE tickets SET current_ticket_status =?, current_responded_ap = ?, current_responded_terminal =?, paid_amount=?, dividend_status=?, dividend_amount=?, locked_by=? where tsn=?";
      this.updTicketForPayStmt = this.connection.prepareStatement(s);
    }
    this.updTicketForPayStmt.setShort(1, (short)ticketStatus);
    this.updTicketForPayStmt.setInt(2, OLTP_SERVICE_ID);
    this.updTicketForPayStmt.setInt(3, terminalIdInternal);
    this.updTicketForPayStmt.setLong(4, paidAmount);
    this.updTicketForPayStmt.setShort(5, (short)1);
    this.updTicketForPayStmt.setLong(6, dividend);
    this.updTicketForPayStmt.setInt(7, -1);
    this.updTicketForPayStmt.setString(8, ticketTSN);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING updTicketForPayStmt: " + ticketTSN);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.updTicketForPay);
    }
    if (this.updTicketForPayStmt.executeUpdate() == 0) {
      throw new SQLException("updTicketForPay failed for Ticket TSN " + ticketTSN);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.updTicketForPay, stmt_start);
    }

    /**
     * TABLE ticket_transaction_log
     *
     * seq_no               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
     * transaction_id       CHAR(32) NOT NULL,
     * transaction_type     SMALLINT NOT NULL,
     * ticket_id            BIGINT NOT NULL default 0,
     * responded_ap         SMALLINT NOT NULL,
     * responded_terminal   INT NOT NULL,
     * event_info           VARCHAR(255) NOT NULL,
     * created_time         TIMESTAMP default current_TIMESTAMP NOT NULL,
     *
     * PRIMARY KEY (seq_no)
     *
     * CREATE UNIQUE INDEX ticket_transaction_log_udx01 ON ticket_transaction_log (transaction_id);
     */
    if (this.insTicketHistoryStmt == null) {
      String s = "INSERT INTO ticket_transaction_log (transaction_id, ticket_id, responded_ap, responded_terminal, event_info, transaction_type) VALUES (?, ?, ?, ?, ?, ?)";
      this.insTicketHistoryStmt = this.connection.prepareStatement(s, Statement.RETURN_GENERATED_KEYS);
    }
    this.insTicketHistoryStmt.setString(1, ticketTransactionId);
    this.insTicketHistoryStmt.setLong(2, ticketId);
    this.insTicketHistoryStmt.setInt(3, OLTP_SERVICE_ID);
    this.insTicketHistoryStmt.setInt(4, terminalIdInternal);
    this.insTicketHistoryStmt.setString(5, C_CashPaidMessageResult);
    this.insTicketHistoryStmt.setShort(6, C_TransactionType_Pay);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING insTicketHistoryStmt: " + ticketTransactionId + " " + ticketId);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insTicketHistory);
    }
    if (this.insTicketHistoryStmt.executeUpdate() != 1) {
      throw new SQLException("insTicketHistory failed for Ticket TSN " + key);
    }

    // get the generated transaction seq_no
    rs = this.insTicketHistoryStmt.getGeneratedKeys();
    if (rs == null) {
      throw new SQLException("insTicketHistory failed to get generated keys for Ticket TSN " + key);
    }
    long ticketTransactionSeqNo = -1L;
    if (rs.next()) {
      ticketTransactionSeqNo = rs.getLong(1);
      rs.close();
      rs = null;
    } else {
      throw new SQLException("insTicketHistory failed to get generated key for Ticket TSN " + key);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insTicketHistory, stmt_start);
    }

    /**
     * TABLE source_message_results
     *
     * seq_no         BIGINT GENERATED ALWAYS AS IDENTITY
     * transaction_id CHAR(32) NOT NULL
     * return_message VARCHAR(256) NOT NULL
     * responded_ap   INT NOT NULL
     * ce_seq_no      BIGINT default 0
     * created_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
     *
     * PRIMARY KEY (seq_no)
     *
     * CREATE INDEX source_message_results_clx01 ON source_message_results (transaction_id)
     */
    if (this.insSourceMessageResultStmt == null) {
      String s = "INSERT INTO source_message_results (transaction_id, return_message, responded_ap) VALUES (?, ?, ?)";
      this.insSourceMessageResultStmt = connection.prepareStatement(s);
    }
    this.insSourceMessageResultStmt.setString(1, ticketTransactionId);
    this.insSourceMessageResultStmt.setString(2, C_CashPaidMessageResult);
    this.insSourceMessageResultStmt.setInt(3, OLTP_SERVICE_ID);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING insSourceMessageResultStmt: " + ticketTSN);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insSourceMessageResult);
    }
    if (this.insSourceMessageResultStmt.executeUpdate() == 0) {
      throw new SQLException("insSourceMessageResult failed for Ticket TSN " + ticketTSN);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insSourceMessageResult, stmt_start);
    }

    /**
     * TABLE terminal_balance_transaction_log
     *
     * seq_no                        BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY
     * terminal_id                   BIGINT NOT NULL
     * ticket_transaction_log_seq_no BIGINT NOT NULL
     * transaction_amount            BIGINT NOT NULL
     * transaction_type              SMALLINT NOT NULL
     * created_time                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL (not used here)
     *
     * PRIMARY KEY (seq_no)
     */
    if (this.insTerminalBalanceTransactionLogStmt == null) {
      String stmt = "INSERT INTO terminal_balance_transaction_log (terminal_id, ticket_transaction_log_seq_no, transaction_type, transaction_amount) VALUES (?, ?, ?, ?)";
      this.insTerminalBalanceTransactionLogStmt = this.connection.prepareStatement(stmt);
    }
    this.insTerminalBalanceTransactionLogStmt.setLong(1, terminalIdInternal);
    this.insTerminalBalanceTransactionLogStmt.setLong(2, ticketTransactionSeqNo);
    this.insTerminalBalanceTransactionLogStmt.setShort(3, C_TransactionType_Pay);
    this.insTerminalBalanceTransactionLogStmt.setLong(4, -dividend);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING insTerminalBalanceTransactionLogStmt: " + ticketTSN);
    }
    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startStmt(Stmt.insTerminalBalanceTransactionLog);
    }
    if (this.insTerminalBalanceTransactionLogStmt.executeUpdate() != 1) {
      throw new SQLException("insTerminalBalanceTransactionLog failed for Ticket TSN " + ticketTSN);
    }
    if (this.timeStmts) {
      this.useCase5stats.endStmt(Stmt.insTerminalBalanceTransactionLog, stmt_start);
    }

    if (this.timeStmts) {
      stmt_start = this.useCase5stats.startCommit();
    }
    this.connection.commit();
    if (this.timeStmts) {
      this.useCase5stats.endCommit(stmt_start);
    }
  }

//------------------------------------------------------------------------------
// execution support
//------------------------------------------------------------------------------

  private String getDDLFile(String fname) {
    String fn = "$JTESTS/cacheperf/comparisons/gemfirexd/useCase5/poc/ddl/" + fname;
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
      case TERMINALS:
        return TERMINALS_NAME;
      case SELL_TRANSACTIONS:
        return SELL_TRANSACTIONS_NAME;
      case PAY_TRANSACTIONS:
        return PAY_TRANSACTIONS_NAME;
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

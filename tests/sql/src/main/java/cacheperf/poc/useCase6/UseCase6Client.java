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
package cacheperf.poc.useCase6;

import cacheperf.CachePerfPrms;
import cacheperf.comparisons.gemfirexd.QueryPerfClient;
import cacheperf.comparisons.gemfirexd.QueryPerfPrms;
import cacheperf.comparisons.gemfirexd.QueryUtil;
import hydra.HydraConfigException;
import hydra.HydraThreadLocal;
import hydra.Log;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import objects.query.QueryPrms;

public class UseCase6Client extends QueryPerfClient {

  public static final String param1 = "AAAAAAAAAAAAAAAA";
  public static final String param2 = "AAAAAAAAAA";
  public static final String param3 = "20090101000000";

  private static final String TransactTableStmt =
     "CREATE TABLE Transact (Field001 Decimal(2,0) NOT NULL, Field002 Varchar(10), TransactNo DECIMAL(7,0) NOT NULL, Field004 DECIMAL(7,0) NOT NULL, Field005 Varchar(16) NOT NULL, Field006 Varchar(9) NOT NULL, Field007 Varchar(16) NOT NULL, Field008 Varchar(24) NOT NULL, Field009 DECIMAL(5,0) NOT NULL, Field010 Varchar(10), Field011 Varchar(10), Field012 Varchar(4) NOT NULL, Field013 DECIMAL(10,0), Field014 Varchar(3) NOT NULL, Field015 DECIMAL(10,0), Field016 DECIMAL(5,0) NOT NULL, Field017 Varchar(10), Field018 DECIMAL(5,0) NOT NULL, Field019 Varchar(10), Field020 Varchar(3) NOT NULL, Field021 timestamp NOT NULL, Field022 timestamp, Field023 timestamp NOT NULL, Field024 timestamp NOT NULL, Field025 timestamp, Field026 timestamp, Field027 timestamp, Field028 DECIMAL(5,0) NOT NULL, Field029 Varchar(10), Field030 Varchar(10) NOT NULL, Field031 DECIMAL(10,0), Field032 DECIMAL(5,0), Field033 Decimal(15,4) NOT NULL, Field034 DECIMAL(5,0) NOT NULL, Field035 Varchar(10), Field036 Varchar(10), Field037 Varchar(10), Field038 DECIMAL(10,0), Field039 Varchar(10), Field040 timestamp, Field041 DECIMAL(5,0) NOT NULL, Field042 Varchar(10), Field043 Varchar(3), Field044 Varchar(10), Field045 Varchar(16), Field046 DECIMAL(5,0), Field047 Varchar(10), Field048 DECIMAL(5,0), Field049 Varchar(10), Field050 Varchar(10), Field051 DECIMAL(3,0) NOT NULL, Field052 Varchar(16), Field053 Varchar(138), Field054 Varchar(138), Field055 Varchar(138), Field056 Varchar(138), Field057 Varchar(48), Field058 Varchar(48), Field059 Decimal(19,4) NOT NULL, Field060 Decimal(15,4) NOT NULL, Field061 Decimal(15,4) NOT NULL, Field062 Decimal(19,4) NOT NULL, Field063 Decimal(15,4) NOT NULL, Field064 Decimal(15,4) NOT NULL, Field065 Decimal(15,4) NOT NULL, Field066 Decimal(15,4) NOT NULL, Field067 Decimal(15,4) NOT NULL, Field068 Decimal(15,4) NOT NULL, Field069 Decimal(15,4) NOT NULL, Field070 Decimal(25,15) NOT NULL, Field071 Decimal(15,4) NOT NULL, Field072 Decimal(20,9) NOT NULL, Field073 Decimal(15,9) NOT NULL, Field074 DECIMAL(10,0), Field075 Decimal(15,4) NOT NULL, Field076 Decimal(15,4) NOT NULL, Field077 Decimal(15,4) NOT NULL, Field078 Decimal(15,4) NOT NULL, Field079 Decimal(25,15) NOT NULL, Field080 DECIMAL(10,0), Field081 Decimal(15,9) NOT NULL, Field082 DECIMAL(10,0), Field083 timestamp NOT NULL, Field084 Varchar(10) NOT NULL, Field085 timestamp NOT NULL, Field086 Varchar(10) NOT NULL, Field087 DECIMAL(7,0) NOT NULL, Field088 Varchar(10), Field089 Varchar(10), Field090 timestamp, Field091 timestamp, Field092 CHAR(1) NOT NULL, Field093 timestamp, Field094 Varchar(2), Field095 Varchar(10), Field096 Varchar(16), Field097 timestamp, Field098 Decimal(15,4), Field099 Varchar(10), Field100 timestamp, Field101 Varchar(10), Field102 timestamp, Field103 DECIMAL(5,0) NOT NULL, Field104 Varchar(10), Field105 CHAR(3) NOT NULL, Field106 timestamp NOT NULL, Field107 Varchar(10), CONSTRAINT Transact_PK PRIMARY KEY(TransactNo))";

  private static final String CashflowTableStmt =
     "CREATE TABLE Cashflow (Field001 DECIMAL(2,0) NOT NULL, TransactNo DECIMAL(7,0) NOT NULL, LegNo DECIMAL(7,0) NOT NULL, CashflowNo DECIMAL(7,0) NOT NULL, Field005 Varchar(20) NOT NULL, Field006 Varchar(3) NOT NULL, Field007 Varchar(3) NOT NULL, Field008 timestamp NOT NULL, Field009 timestamp NOT NULL, Field010 timestamp NOT NULL, Field011 timestamp NOT NULL, Field012 timestamp, Field013 timestamp, Field014 DECIMAL(5,0) NOT NULL, Field015 DECIMAL(5,0), Field016 Decimal(15,4) NOT NULL, Field017 Decimal(15,4) NOT NULL, Field018 Decimal(15,4) NOT NULL, Field019 DECIMAL(10,0) NOT NULL, Field020 Decimal(15,4) NOT NULL, Field021 Decimal(19,4) NOT NULL, Field022 Decimal(15,4) NOT NULL, Field023 Decimal(15,4) NOT NULL, Field024 Decimal(5,0) NOT NULL, Field025 Varchar(10) NOT NULL, Field026 Decimal(10,0) NOT NULL, Field027 Decimal(15,4) NOT NULL, Field028 Decimal(20,9) NOT NULL, Field029 Decimal(15,9) NOT NULL, Field030 Decimal(15,4) NOT NULL, Field031 Decimal(15,4) NOT NULL, Field032 Decimal(15,4) NOT NULL, Field033 Decimal(15,4) NOT NULL, Field034 Decimal(10,0) NOT NULL, Field035 Decimal(15,9) NOT NULL, Field036 Decimal(19,4) NOT NULL, Field037 Decimal(10,0) NOT NULL, Field038 Decimal(10,0) NOT NULL, Field039 timestamp NOT NULL, Field040 Varchar(10) NOT NULL, Field041 timestamp NOT NULL, Field042 Varchar(10) NOT NULL, Field043 DECIMAL(7,0) NOT NULL, Field044 Decimal(1,0) NOT NULL, Field045 Decimal(10,0), Field046 Decimal(10,0), Field047 timestamp, Field048 timestamp, Field049 Varchar(10), Field050 DECIMAL(10,0), Field051 DECIMAL(10,0), Field052 DECIMAL(10,0), Field053 Varchar(10), Field054 Varchar(10), Field055 Varchar(10), Field056 DECIMAL(10,0), Field057 Varchar(10), Field058 Varchar(10), Field059 Varchar(10), Field060 Varchar(10), Field061 timestamp, Field062 timestamp, Field063 DECIMAL(10,0) NOT NULL, Field064 DECIMAL(10,0) NOT NULL, Field065 DECIMAL(10,0) NOT NULL, Field066 DECIMAL(10,0) NOT NULL, Field067 Varchar(10), Field068 DECIMAL(10,0), Field069 Varchar(10), Field070 timestamp, CONSTRAINT Cashflow_PK PRIMARY KEY(TransactNo, LegNo, CashflowNo))";

  private static final String TransactIndexStmt =
     "create index transact_index1 on Transact(Field001 asc, Field007 asc, Field008 asc, Field016 asc, Field018 asc, Field026 asc, Field027 asc, Field034 asc, Field041 asc, Field051 asc)";

  private static final String CashflowIndexStmt =
     "create index cashflow_index1 on Cashflow(TransactNo asc, Field005 asc, Field006 asc, Field007 asc, Field014 asc, Field045 asc, Field046 asc, Field047 asc, Field048 asc, Field051 asc, Field053 asc, Field054 asc)";

  private static final String TransactInsertStmt1 =
      "1,'AAAAAAAAAA',";

  private static final String TransactInsertStmt2 =
     ",111111,'AAAAAAAAAAAAAAAA','AAAAAAAAA','AAAAAAAAAAAAAAAA','AAAAAAAAAA',1111,'AAAAAAAAAA','AAAAAAAAAA','AAAA',111111111,'AAA',111111111,1111,'AAAAAAAAAA',1111,'AAAAAAAAAA','AAA','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-04-01 00:00:00.000','2009-01-01 00:00:00.000',1111,'AAAAAAAAAA','AAAAAAAAAA',111111111,1111,1111.0,1111,'AAAAAAAAAA','AAAAAAAAAA','AAAAAAAAAA',111111111,'AAAAAAAAAA','2009-01-01 00:00:00.000',1111,'AAAAAAAAAA','AAA','AAAAAAAAAA','AAAAAAAAAAAAAAAA',1111,'AAAAAAAAAA',1111,'AAAAAAAAAA','AAAAAAAAAA',11,'AAAAAAAAAAAAAAAA','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA','AAAAAAAAAAAAAAAAAAAAAAAA','AAAAAAAAAAAAAAAAAAAAAAAA',1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,1111.0,111111111,1111.0,1111.0,1111.0,1111.0,1111.0,111111111,1111.0,111111111,'2009-01-01 00:00:00.000','AAAAAAAAAA','2009-01-01 00:00:00.000','AAAAAAAAAA',111111,'AAAAAAAAAA','AAAAAAAAAA','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','A','2009-01-01 00:00:00.000','AA','AAAAAAAAAA','AAAAAAAAAAAAAAAA','2009-01-01 00:00:00.000',1111.0,'AAAAAAAAAA','2009-01-01 00:00:00.000','AAAAAAAAAA','2009-01-01 00:00:00.000',1111,'AAAAAAAAAA','AAA','2009-01-01 00:00:00.000','AAAAAAAAAA'";

  private static final String CashflowInsertStmt1 = "1,";
  private static final String CashflowInsertStmt2 = ",111111,";
  private static final String CashflowInsertStmt3 = ",'AAAAAAAAAA','AAA','AAA','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000',1111,1111,111.00,111.00,111.00,111111111,111.00,111.00,111.00,111.00,1111,'AAAAAAAAAA',111111111,111.00,111.00,111.00,111.00,111.00,111.00,111.00,111111111,111.00,111.00,111111111,111111111,'2009-01-01 00:00:00.000','AAAAAAAAAA','2009-01-01 00:00:00.000','AAAAAAAAAA',111111,1,111111111,111111111,'2009-04-01 00:00:00.000','2009-01-01 00:00:00.000','AAAAAAAAAA',111111111,111111111,111111111,'AAAAAAAAAA','AAAAAAAAAA','AAAAAAAAAA',111111111,'AAAAAAAAAA','AAAAAAAAAA','AAAAAAAAAA','AAAAAAAAAA','2009-01-01 00:00:00.000','2009-01-01 00:00:00.000',111111111,111111111,111111111,111111111,'AAAAAAAAAA',111111111,'AAAAAAAAAA','2009-01-01 00:00:00.000'";

  protected static final boolean logQueries = QueryPrms.logQueries();

  private boolean isValue = UseCase6Prms.isValue();

  public static void createSqlDataTask() throws SQLException {
    UseCase6Client c = new UseCase6Client();
    c.initialize(CREATES);
    c.createSqlData();
  }
  private void createSqlData() throws SQLException {
    PreparedStatement transactPS = null;
    PreparedStatement cashflowPS = null;
    if (UseCase6Prms.usePreparedStatements()) {
      transactPS = getTransactPreparedStatement();
      cashflowPS = getCashflowPreparedStatement();
    }
    int resultSetSize = UseCase6Prms.getResultSetSize(); // Cashflow count
    do {
      int key = getNextKey(); // Transact count
      executeTaskTerminator(); // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination
      createSqlData(key, resultSetSize, transactPS, cashflowPS);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;
    } while (!executeBatchTerminator()); // commits at batch termination
  }
  private void createSqlData(int transactNo, int cashflowCount,
          PreparedStatement transactPS, PreparedStatement cashflowPS)
  throws SQLException {

    long start = this.querystats.startCreate();

    String stmt;

    int sz = CachePerfPrms.getTxSize();

    // Transact
    if (transactPS == null) {
      stmt = "insert into Transact values ("
                        + TransactInsertStmt1 + transactNo
                        + TransactInsertStmt2 + ")";
      execute(stmt, this.connection);
      if (sz == 1) {
        this.connection.commit();
      }
    }
    else {
      transactPS.setInt(1, transactNo);
      execute(transactPS);
      if (sz == 1) {
        this.connection.commit();
      }
    }
    if (sz != 1) {
      this.connection.commit();
    }

    // Cashflow
    for (int cashflowNo = 1; cashflowNo <= cashflowCount; cashflowNo++) {
      if (cashflowPS == null) {
        stmt = "insert into Cashflow values ("
                          + CashflowInsertStmt1 + transactNo
                          + CashflowInsertStmt2 + cashflowNo
                          + CashflowInsertStmt3 + ")";
        execute(stmt, this.connection);
        if (sz == 1) {
          this.connection.commit();
        }
      } else {
        cashflowPS.setInt(1, transactNo);
        cashflowPS.setInt(2, cashflowNo);
        execute(cashflowPS);
        if (sz == 1) {
          this.connection.commit();
        }
      }
    }
    if (sz != 1) {
      this.connection.commit();
    }
    this.querystats.endCreate(start, 1 + cashflowCount, this.histogram);
  }

  public static void querySqlDataTask() throws SQLException {
    UseCase6Client c = new UseCase6Client();
    c.initialize(QUERIES);
    c.querySqlData();
  }

  public void querySqlData() throws SQLException {
    Log.getLogWriter().info("Pass by " + isValue);

    List<TransactEntity> trnList = null;
    TransactEntity trnEntity = null;

    TransactDao trnDao = new TransactDao(this.connection, this.querystats,
                                                          this.histogram);
    trnList = trnDao.select(param1,param2, param3, isValue);

    Log.getLogWriter().info(" Size of Transact list = " + trnList.size());

    CashflowDao cfDao = new CashflowDao(this.connection, this.querystats,
                                                         this.histogram);

    int num_tran = trnList.size();

    do {
      int key = getNextKey();
      executeTaskTerminator(); // commits at task termination
      executeWarmupTerminator(); // commits at warmup termination

      trnEntity = trnList.get(key);
      cfDao.select(trnEntity.getTransactNo(), param2, param3, isValue);

      ++this.count;
      ++this.keyCount;
      ++this.iterationsSinceTxEnd;

    } while (!executeBatchTerminator()); // commits at batch termination
  }

  public static void createTablesTask() throws SQLException {
    UseCase6Client c = new UseCase6Client();
    c.initialize();
    if (c.ttgid == 0) {
      c.createTables();
    }
  }
  private void createTables() throws SQLException {
    String stmt;

    String tableType = UseCase6Prms.getTableType();

    stmt = TransactTableStmt;
    switch (this.queryAPI) {
      case QueryPrms.GFXD:
        if (tableType.equalsIgnoreCase("replicated")) {
          stmt += " replicate";
        } else if (tableType.equalsIgnoreCase("partitioned")) {
          int redundantCopies = UseCase6Prms.getRedundantCopies();
          stmt += " partition by column(TransactNo) "
                + "redundancy " + redundantCopies;
        } else {
          String s = "Unsupported table type: " + tableType;
          throw new HydraConfigException(s);
        }
        break;
      case QueryPrms.MYSQL:
        break;
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(this.queryAPI);
        throw new HydraConfigException(s);
    }
    execute(stmt, this.connection);

    stmt = CashflowTableStmt;
    switch (this.queryAPI) {
      case QueryPrms.GFXD:
        if (tableType.equalsIgnoreCase("replicated")) {
          stmt += " replicate";
        } else if (tableType.equalsIgnoreCase("partitioned")) {
          int redundantCopies = UseCase6Prms.getRedundantCopies();
          stmt += " partition by column(TransactNo) colocate with (Transact) "
                + "redundancy " + redundantCopies;
        }
        break;
      case QueryPrms.MYSQL:
        break;
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(this.queryAPI);
        throw new HydraConfigException(s);
    }
    execute(stmt, this.connection);

    this.connection.commit();
  }

  public static void createIndexesTask() throws SQLException {
    if (!UseCase6Prms.createIndexes()) {
      return; // noop
    }
    UseCase6Client c = new UseCase6Client();
    c.initialize();
    if (c.ttgid == 0) {
      c.createIndexes();
    }
  }
  private void createIndexes() throws SQLException {
    execute(TransactIndexStmt, this.connection);
    execute(CashflowIndexStmt, this.connection);

    this.connection.commit();
  }

  public static void cleanupTask() throws SQLException {
    UseCase6Client c = new UseCase6Client();
    c.initialize();
    if (c.ttgid == 0) {
      c.cleanup();
    }
  }
  private void cleanup() throws SQLException {
    String stmt;

    stmt = "drop table Cashflow";
    try {
      execute(stmt, this.connection);
    } catch (SQLException e) {
      Log.getLogWriter().info(e);
    }

    stmt = "drop table Transact";
    try {
      execute(stmt, this.connection);
    } catch (SQLException e) {
      Log.getLogWriter().info(e);
    }

    stmt = "drop index transact_index1";
    try {
      execute(stmt, this.connection);
    } catch (SQLException e) {
      Log.getLogWriter().info(e);
    }

    stmt = "drop index cashflow_index1";
    try {
      execute(stmt, this.connection);
    } catch (SQLException e) {
      Log.getLogWriter().info(e);
    }

    this.connection.commit();
  }

  private ResultSet execute(String stmt, Connection conn) throws SQLException {
    if (logQueries) {
      Log.getLogWriter().info("Executing: " + stmt + " on: " + conn);
    } 
    ResultSet rs = null;
    Statement s = conn.createStatement();
    boolean result = s.execute(stmt);
    if (result == true) {
      rs = s.getResultSet();
    }
    if (logQueries) {
      Log.getLogWriter().info("Executed: " + stmt + " on: " + conn);
    }
    s.close();
    return rs;
  }

  private void execute(PreparedStatement stmt)
  throws SQLException {
    if (logQueries) {
      Log.getLogWriter().info("Executing: " + stmt);
    }
    stmt.execute();
    if (logQueries) {
      Log.getLogWriter().info("Executed: " + stmt);
    }
  }

  //----------------------------------------------------------------------------
  // Hydra thread locals and their instance field counterparts
  //----------------------------------------------------------------------------

  private static HydraThreadLocal localtransactps = new HydraThreadLocal();
  private static HydraThreadLocal localcashflowps = new HydraThreadLocal();

  /**
   * Gets the per-thread Transact PreparedStatement wrapper instance.
   */
  protected PreparedStatement getTransactPreparedStatement()
  throws SQLException {
    PreparedStatement ps = (PreparedStatement)localtransactps.get();
    if (ps == null) {
      ps = this.connection.prepareStatement("insert into Transact values ("
         + TransactInsertStmt1 + "?" + TransactInsertStmt2 + ")"
         );
      localtransactps.set(ps);
    }
    return ps;
  }

  /**
   * Gets the per-thread Cashflow PreparedStatement wrapper instance.
   */
  protected PreparedStatement getCashflowPreparedStatement()
  throws SQLException {
    PreparedStatement ps = (PreparedStatement)localcashflowps.get();
    if (ps == null) {
      ps = this.connection.prepareStatement("insert into Cashflow values ("
         + CashflowInsertStmt1 + "?" + CashflowInsertStmt2 + "?"
         + CashflowInsertStmt3 + ")"
         );
      localcashflowps.set(ps);
    }
    return ps;
  }
}

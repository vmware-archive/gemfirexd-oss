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
package cacheperf.comparisons.gemfirexd.poc;

import cacheperf.*;
import cacheperf.comparisons.gemfirexd.*;
import hydra.*;
import hydra.gemfirexd.*;
import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.StringTokenizer;
import javax.sql.rowset.serial.SerialBlob;
import objects.query.QueryPrms;
import sql.SQLHelper;
import sql.sqlutil.ResultSetHelper;

public class POCClient extends QueryPerfClient {

  public static final BigDecimal BIG_DECIMAL_1 = new BigDecimal(1); // used in useCase2 insert
  public static final BigDecimal BIG_DECIMAL_9 = new BigDecimal(9); // used in useCase2 insert and query
  public static final String USECASE2_APP = "APP943415";
  public static final String Y_FLAG = String.valueOf('Y');
  public static final String N_FLAG = String.valueOf('N');

//------------------------------------------------------------------------------
// ddl
//------------------------------------------------------------------------------

  public static void executeDDLTask()
  throws FileNotFoundException, IOException, SQLException {
    POCClient c = new POCClient();
    c.initialize();
    if (c.sttgid == 0) {
      c.executeDDL();
    }
  }
  private void executeDDL()
  throws FileNotFoundException, IOException, SQLException {
    String ddlFile = POCPrms.getDDLFileName();
    if (ddlFile != null) {
      String tmp = "$JTESTS/cacheperf/comparisons/gemfirexd/poc/ddl/" + ddlFile;
      String fn = EnvHelper.expandEnvVars(tmp);
      if (FileUtil.exists(fn)) {
        Log.getLogWriter().info("Executing DDL file: " + fn);
        String text = FileUtil.getText(fn).trim();
        StringTokenizer tokenizer = new StringTokenizer(text, ";", false);
        while (tokenizer.hasMoreTokens()) {
          String stmt = tokenizer.nextToken().trim();
          Log.getLogWriter().info("Executing DDL: " + stmt);
          try {
            execute(stmt, this.connection);
          } catch (SQLException e) {
            if (stmt.toLowerCase().trim().startsWith("drop")) {
              Log.getLogWriter().info("Failed to drop table");
            } else {
              throw e;
            }
          }
          if (this.queryAPI != QueryPrms.GFXD) {
            try {
              this.connection.commit();
            } catch (SQLException e) {
              throw new QueryPerfException("Commit failed: " + e);
            }
          }
        }
      } else {
        String s = fn + " not found";
        throw new HydraConfigException(s);
      }
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

//------------------------------------------------------------------------------
// DATA LOADING TASK
//------------------------------------------------------------------------------

  /**
   * The data loading task.
   */
  public static void loadDataTask()
  throws InterruptedException, SQLException {
    POCClient c = new POCClient();
    c.initialize();
    if (c.sttgid == 0) {
      c.loadData();
    }
  }
  private void loadData()
  throws InterruptedException, SQLException {
    POCPrms.POCType pocType = POCPrms.getPOCType();
    loadData(pocType);
  }
  private void loadData(POCPrms.POCType pocType)
  throws SQLException {
    switch (pocType) {
      case UseCase3:
        break;
      case TelcelLoyalty:
        break;
      case UseCase2:
        useCase2LoadData();
        break;
      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
  }

//------------------------------------------------------------------------------
// BENCHMARK TASK
//------------------------------------------------------------------------------

  /**
   * The benchmark task.
   */
  public static void executeBenchmarkTask()
  throws InterruptedException, SQLException {
    POCClient c = new POCClient();
    c.initialize(QUERIES);
    c.executeBenchmark();
  }
  private void executeBenchmark()
  throws InterruptedException, SQLException {
    POCPrms.POCType pocType = POCPrms.getPOCType();
    switch (pocType) {
      case UseCase3:
        useCase3Benchmark();
        break;
      case TelcelLoyalty:
        telcelLoyaltyBenchmark();
        break;
      case UseCase2:
        useCase2Benchmark();
        break;
      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
  }

//------------------------------------------------------------------------------
// UseCase3
//------------------------------------------------------------------------------

  private void useCase3Benchmark() throws SQLException {
  }

//------------------------------------------------------------------------------
// TELCEL LOYALTY
//------------------------------------------------------------------------------

  private void telcelLoyaltyBenchmark() throws SQLException {
    String stmt = "SELECT no_o_id FROM (SELECT no_o_id FROM new_order" +
                  " WHERE no_d_id = ? AND no_w_id = ?" +
                  " ORDER BY no_o_id ASC) WHERE ROWNUM = 1";
    PreparedStatement pstmt = this.connection.prepareStatement(stmt);
    do {
      executeTaskTerminator(); // does not commit
      executeWarmupTerminator(); // does not commit
      enableQueryPlanGeneration();

      telcelLoyalty(pstmt);

      disableQueryPlanGeneration();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount; // not really needed
    } while (!executeBatchTerminator()); // does not commit
    pstmt.close();
  }

  private void telcelLoyalty(PreparedStatement pstmt) throws SQLException {
    int d_id = 5;
    int w_id = 12;
    pstmt.setInt(1, d_id);
    pstmt.setInt(2, w_id);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING: " +
          "SELECT no_o_id FROM new_order" + " WHERE no_d_id = " + d_id +
          " AND no_w_id = " + w_id + " ORDER BY no_o_id ASC");
    }
    ResultSet rs = pstmt.executeQuery();
    int no_o_id = 0;
    if (rs.next()) no_o_id = rs.getInt("no_o_id");
    if (this.logQueryResults) Log.getLogWriter().info("no_o_id=" + no_o_id);
    rs.close();
    rs = null;
    this.connection.commit();
  }

//------------------------------------------------------------------------------
// USECASE2
//------------------------------------------------------------------------------

  /**
   * TABLE XML_DOC_1
   *   XML_DOC_ID_NBR DECIMAL(19) NOT NULL,
   *   STRUCTURE_ID_NBR DECIMAL(22) NOT NULL,
   *   CREATE_MINT_CD CHAR(1) NOT NULL,
   *   MSG_PAYLOAD_QTY DECIMAL(22) NOT NULL,
   *   MSG_PAYLOAD1_IMG BLOB(2000) NOT NULL,
   *   MSG_PAYLOAD2_IMG BLOB(2000),
   *   MSG_PAYLOAD_SIZE_NBR DECIMAL(22),
   *   MSG_PURGE_DT DATE,
   *   DELETED_FLG CHAR(1) NOT NULL,
   *   LAST_UPDATE_SYSTEM_NM VARCHAR(30),
   *   LAST_UPDATE_TMSTP TIMESTAMP NOT NULL,
   *   MSG_MAJOR_VERSION_NBR DECIMAL(22),
   *   MSG_MINOR_VERSION_NBR DECIMAL(22),
   *   OPT_LOCK_TOKEN_NBR DECIMAL(22) DEFAULT 1,
   *   PRESET_DICTIONARY_ID_NBR DECIMAL(22) DEFAULT 0 NOT NULL
   */
  private void useCase2LoadData() throws SQLException {
    long currentTime = System.currentTimeMillis();
    Date date = new Date(currentTime);
    Timestamp timestamp = new Timestamp(currentTime);
    SerialBlob blob = new SerialBlob("12345678452984560289456029847609487234785012934857109348156034650234560897628900985760289207856027895602785608560786085602857602985760206106110476191087345601456105610478568347562686289765927868972691785634975604562056104762978679451308956205620437861508561034756028475180756917856190348756012876510871789546913485620720476107856479238579385923847934".getBytes(Charset.forName("UTF-8")));

    String stmt = "INSERT INTO XML_DOC_1 ("
                + " LAST_UPDATE_SYSTEM_NM,"    //  1
                + " XML_DOC_ID_NBR,"           //  2
                + " MSG_PAYLOAD1_IMG,"         //  3
                + " MSG_PAYLOAD_QTY,"          //  4
                + " MSG_MINOR_VERSION_NBR,"    //  5
                + " PRESET_DICTIONARY_ID_NBR," //  6
                + " CREATE_MINT_CD,"           //  7
                + " MSG_PAYLOAD_SIZE_NBR,"     //  8
                + " DELETED_FLG,"              //  9
                + " STRUCTURE_ID_NBR,"         // 10
                + " MSG_PURGE_DT,"             // 11
                + " OPT_LOCK_TOKEN_NBR,"       // 12
                + " MSG_MAJOR_VERSION_NBR,"    // 13
                + " LAST_UPDATE_TMSTP"         // 14
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement pstmt = this.connection.prepareStatement(stmt);

    int numBatches = POCPrms.getNumBatches();
    int batchSize = POCPrms.getBatchSize();

    Log.getLogWriter().info("Generating " + numBatches + " batches of " + batchSize);
    for (int i = 0; i < numBatches; i++) {
      for (int j = 0; j < batchSize; j++) {
        pstmt.setString(1, USECASE2_APP);
        pstmt.setInt(2, i * batchSize + j);
        pstmt.setBlob(3, blob);
        pstmt.setBigDecimal(4, BIG_DECIMAL_1);
        pstmt.setBigDecimal(5, BIG_DECIMAL_1);
        pstmt.setBigDecimal(6, BIG_DECIMAL_1);
        pstmt.setString(7, N_FLAG);
        pstmt.setBigDecimal(8, BIG_DECIMAL_1);
        pstmt.setString(9, N_FLAG);
        pstmt.setBigDecimal(10, BIG_DECIMAL_9);
        pstmt.setDate(11, date);
        pstmt.setBigDecimal(12, BIG_DECIMAL_1);
        pstmt.setBigDecimal(13, BIG_DECIMAL_1);
        pstmt.setTimestamp(14, timestamp);
        pstmt.addBatch();
      }
      pstmt.executeBatch();
      pstmt.clearBatch();
    }
    pstmt.close();
  }

  private void useCase2Benchmark() throws SQLException {
    String stmt = "SELECT count(*) FROM XML_DOC_1 where XML_DOC_ID_NBR in (?, ?, ?, ?, ?) and STRUCTURE_ID_NBR in (8, 9)";
    PreparedStatement pstmt = this.connection.prepareStatement(stmt);
    String ustmt = "UPDATE XML_DOC_1 SET DELETED_FLG = ? where XML_DOC_ID_NBR = ?";
    PreparedStatement upstmt = this.connection.prepareStatement(ustmt);
    int numBatches = POCPrms.getNumBatches();
    int batchSize = POCPrms.getBatchSize();
    int maxVal = numBatches * batchSize - 1;
    int updatePercentage = POCPrms.getUpdatePercentage();
    long start = -1;
    do {
      executeTaskTerminator(); // does not commit
      executeWarmupTerminator(); // does not commit
      //enableQueryPlanGeneration();

      if (updatePercentage > 0 && this.rng.nextInt(0,100) < updatePercentage) {
        start = this.querystats.startUpdate();
        useCase2Update(upstmt, maxVal);
        this.querystats.endUpdate(start, this.histogram);
      } else {
        start = this.querystats.startQuery();
        useCase2Query(pstmt, maxVal);
        this.querystats.endQuery(start, this.histogram);
      }

      //disableQueryPlanGeneration();
      ++this.batchCount;
      ++this.count;
      ++this.keyCount; // not really needed
    } while (!executeBatchTerminator()); // does not commit
    pstmt.close();
    upstmt.close();
  }

  private void useCase2Query(PreparedStatement pstmt, int maxVal) throws SQLException {
    int n1 = this.rng.nextInt(maxVal);
    int n2 = this.rng.nextInt(maxVal);
    int n3 = this.rng.nextInt(maxVal);
    int n4 = this.rng.nextInt(maxVal);
    int n5 = this.rng.nextInt(maxVal);
    pstmt.setInt(1, n1);
    pstmt.setInt(2, n2);
    pstmt.setInt(3, n3);
    pstmt.setInt(4, n4);
    pstmt.setInt(5, n5);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING: SELECT count(*) FROM XML_DOC_1 where XML_DOC_ID_NBR in (" + n1 + ", " + n2 + ", " + n3 + ", " + n4 + ", " + n5 + ") and STRUCTURE_ID_NBR in (8, 9)");
    }
    ResultSet rs = pstmt.executeQuery();
    if (!rs.next()) {
      throw new SQLException("Failed to find a result for SELECT count(*) FROM XML_DOC_1 where XML_DOC_ID_NBR in (" + n1 + ", " + n2 + ", " + n3 + ", " + n4 + ", " + n5 + ") and STRUCTURE_ID_NBR in (8, 9)");
    }
    rs.close();
    rs = null;
  }

  private void useCase2Update(PreparedStatement pstmt, int maxVal) throws SQLException {
    int docNum = this.rng.nextInt(maxVal);
    pstmt.setString(1, Y_FLAG);
    pstmt.setInt(2, docNum);
    if (this.logQueries) {
      Log.getLogWriter().info("EXECUTING: UPDATE XML_DOC_1 SET DELETED_FLG = " + Y_FLAG + " where XML_DOC_ID_NBR = " + docNum);
    }
    pstmt.executeUpdate();
  }

//------------------------------------------------------------------------------
// OVERRIDDEN METHODS
//------------------------------------------------------------------------------

  protected boolean logQueries;
  protected boolean logQueryResults;

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

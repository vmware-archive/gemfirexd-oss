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
package gfxdperf.tpch.oracle;

import gfxdperf.PerfTestException;
import gfxdperf.terminators.TrimReporter;
import gfxdperf.tpch.DBException;
import gfxdperf.tpch.TPCHClient;
import gfxdperf.tpch.TPCHPrms;
import gfxdperf.tpch.TPCHPrms.TableName;
import hydra.FileUtil;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.ProcessMgr;
import hydra.gemfirexd.LonerHelper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import resultsUtil.FileLineReader;

/**
 * Client for measuring TPCH performance with Oracle.
 * <p> 
 * This class expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key.
 * Only one index on the primary key is needed.
 */
public class OracleClient extends TPCHClient {

  /** Prefix for each column in the table */
  public static String COLUMN_PREFIX = "field";

  /** File in the tests directory for initial DDL */
  public static final String IN_DDL_FILE = System.getProperty("JTESTS") + "/gfxdperf/tpch/oracle/ddl.sql";

  /** File in the test result directory for generated DDL */
  public static final String OUT_DDL_FILE = System.getProperty("user.dir") + "/ddl.sql";

  /** Primary key column in the table */
  public static final String PRIMARY_KEY = "TPCH_KEY";
  
  /** Query to list all indexes */
  public static final String LIST_INDEXES_QUERY = "SELECT index_name, column_name FROM all_ind_columns WHERE index_owner = 'SYSTEM' and table_name = ";

  protected static HydraThreadLocal localoraclestats = new HydraThreadLocal();

  protected OracleStats oraclestats;

//------------------------------------------------------------------------------
// HydraThreadLocals

  public void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.oraclestats = (OracleStats)localoraclestats.get();
  }

  public void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    localoraclestats.set(this.oraclestats);
  }

//------------------------------------------------------------------------------
// LONER

  /**
   * Connects to a loner distributed system. Used for monitoring Oracle servers.
   */
  public static void startLonerTask() {
    LonerHelper.connect(); // for system statistics
  }

//------------------------------------------------------------------------------
// DDL

  /**
   * Modifies the DDL in the tests source directory in {@link #IN_DDL_FILE} and
   * writes it out to the test results directory as {@link #OUT_DDL_FILE}. This
   * includes the basic schema.
   */
  public static void generateDDLTask() throws IOException {
    OracleClient client = new OracleClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.generateDDL();
    }
  }

  private void generateDDL() throws IOException {
    String ddl = generateTableDDL();
    FileUtil.writeToFile(OUT_DDL_FILE, ddl);
  }

  private String generateTableDDL() throws FileNotFoundException, IOException {
    StringBuilder sql = new StringBuilder();

    List<String> ddls = getDDLStatements(IN_DDL_FILE);
    for (String ddl : ddls) {
      sql.append(ddl);
      sql.append(";\n");
    }
    return sql.toString();
  }

  private List<String> getDDLStatements(String fn)
  throws FileNotFoundException, IOException {
    String text = FileUtil.getText(fn).trim();
    List<String> stmts = new ArrayList();
    String[] tokens = text.split(";");
    for (String token : tokens) {
      String stmt = token.trim();
      stmts.add(stmt);
    }
    return stmts;
  }

  /**
   * Executes the DDL generated in {@link #generateDDLTask} as found in {@link
   * #OUT_DDL_FILE}.
   */
  public static void executeDDLTask() throws IOException, SQLException {
    OracleClient client = new OracleClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.executeDDL();
    }
  }

  private void executeDDL() throws IOException, SQLException {
    Connection conn = ((OracleDB)this.db).getConnection();
    Statement stmt = conn.createStatement();
    List<String> ddls = getDDLStatements(OUT_DDL_FILE);
    for (String ddl : ddls) {
      if (ddl.length() > 0) {
        Log.getLogWriter().info("Executing DDL: " + ddl);
        boolean result = stmt.execute(ddl);
        Log.getLogWriter().info("Executed DDL: " + ddl + " with result "
                               + result);
      }
    }
    stmt.close();
    conn.commit();
  }

//------------------------------------------------------------------------------
// INDEXES

  public static void createIndexesTask() throws DBException {
    OracleClient client = new OracleClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.createIndexes();
    }
  }

  protected void createIndexes() throws DBException {
    List<String> indexes = TPCHPrms.getIndexes();
    Log.getLogWriter().info("Creating indexes: " + indexes);
    for (String indexStmt : indexes) {
      try {
        Log.getLogWriter().info("Executing " + indexStmt);
        long startTime = this.oraclestats.startCreateIndex();
        this.db.createIndex(indexStmt);
        this.oraclestats.endCreateIndex(startTime);
        Log.getLogWriter().info("Done executing " + indexStmt);
      } catch (DBException e) {
        Throwable cause = e.getCause();
        if (cause.getMessage().contains("ORA-01408: such column list already indexed")) { // already indexed perhaps by automatic index creation
          Log.getLogWriter().info("Already an index for " + indexStmt + "; " + cause.getMessage());
        } else {
          throw e;
        }
      }
    }
    Log.getLogWriter().info("Created indexes: " + indexes);
  }
  
  /** List all indexes on tables used in this test.
   * 
   * @throws DBException
   * @throws IOException 
   * @throws FileNotFoundException 
   */
  public static void listIndexes() throws DBException, FileNotFoundException, IOException {
    OracleClient client = new OracleClient();
    client.initialize();
    List<String> ddls = client.getDDLStatements(IN_DDL_FILE);
    for (String createStmt: ddls) {
      String[] tokens = createStmt.split("[\\s]+");
      for (int i = 0; i < tokens.length; i++) {
        // looking for a line of the form "CREATE TABLE <tableName> ..."
        if ((i+2 < tokens.length) && tokens[i].toLowerCase().endsWith("create") &&
            tokens[i+1].toLowerCase().equals("table")) {
          String tableName = tokens[i+2];
          String sql = LIST_INDEXES_QUERY + "'" + tableName + "'";
          ResultSet rs = (ResultSet) client.db.executeSql(sql);
          try {
            StringBuilder sb = new StringBuilder();
            sb.append("Indexes on table " + tableName + ":\n");
            while (rs.next()) {
              sb.append("   index " + rs.getString(1) + " on column " + rs.getString(2) + "\n");
            }
            rs.close();
            Log.getLogWriter().info(sb.toString());
            break;
          } catch (SQLException e) {
            String s = "Unable to process output of " + sql;
            throw new DBException(s);
          }
        }
      }
    }
  }

  /** Drop all indexes on the user tables
   * 
   * @throws DBException Thrown if anything goes wrong when dropping indexes.
   */
  public static void dropIndexes() throws DBException {
    Log.getLogWriter().info("Removing indexes....");
    OracleClient client = new OracleClient();
    client.initialize();
    Map<String, List<String>> indexMap = getIndexMap();
    for (String tableName: indexMap.keySet()) {
      List<String> indexList = indexMap.get(tableName);
      for (String indexName: indexList) {
        String sql = "DROP INDEX " + indexName;
        Log.getLogWriter().info("Executing " + sql);
        try {
          client.db.executeSql(sql);
        } catch (DBException e) {
          Throwable cause = e.getCause();
          if ((cause != null) && (cause.getMessage().contains("specified index does not exist"))) {
            Log.getLogWriter().info(cause.getMessage());
          } else {
            throw e;
          }
        }
      }
    }
  }

  /** Return a Map with information about tables and their indexes.
   * 
   * @return A Map containingMap where the key (String) is a the name of a table and the value
   *         is a List of index names (Strings) on that table.
   */
  protected static Map<String, List<String>> getIndexMap() {
    final String INDEX_FILE_SUFFIX = "_index.inc";
    Map<String, List<String>> indexMap = new HashMap<String, List<String>>(); // key is table name, value is List of index names
    String dirPath = System.getProperty("JTESTS") + File.separator + "gfxdperf" + File.separator + "tpch";
    File dir = new File(dirPath);
    String[] dirContents = dir.list();
    for (String fileName: dirContents) {
      if (fileName.endsWith(INDEX_FILE_SUFFIX)) {
        FileLineReader flr;
        try {
          flr = new FileLineReader(dir.getAbsoluteFile() + File.separator + fileName);
        } catch (FileNotFoundException e) {
          String s = "Not able to find " + fileName;
          throw new PerfTestException(s);
        }
        String line = flr.readNextLine();
        while (line != null) {
          String[] tokens = line.split("[\\s]+");
          for (int i = 0; i < tokens.length; i++) {
            // looking for a line of the form "CREATE INDEX <indexName> ON <tableName>"
            if ((i+4 < tokens.length) && tokens[i].toLowerCase().endsWith("create") &&
                tokens[i+1].toLowerCase().equals("index") &&
                tokens[i+3].toLowerCase().equals("on")) {
              String indexName = tokens[i+2];
              String tableName = tokens[i+4];
              List<String> indexList = indexMap.get(tableName);
              if (indexList == null) {
                indexList = new ArrayList<String>();
              }
              indexList.add(indexName);
              indexMap.put(tableName, indexList);
              break;
            }
          }
          line = flr.readNextLine();
        }
      }
    }
    return indexMap;
  }

//------------------------------------------------------------------------------
// DB initialization and cleanup

  public static void initDBTask() throws DBException {
    OracleClient client = new OracleClient();
    client.initialize();
    client.initDB();
    client.updateHydraThreadLocals();
  }

  protected void initDB() throws DBException {
    this.db = new OracleDB();
    this.db.init(this.rng);
    this.oraclestats = OracleStats.getInstance();
  }

  public static void cleanupDBTask() throws DBException, InterruptedException {
    OracleClient client = new OracleClient();
    client.initialize();
    client.cleanupDB();
    client.updateHydraThreadLocals();
  }

  protected void cleanupDB() throws DBException, InterruptedException {
    if (this.oraclestats != null) {
      Thread.sleep(2000);
      this.oraclestats.close();
      this.oraclestats = null;
    }
    this.db.cleanup();
  }

//------------------------------------------------------------------------------
// Table drop

  public static void dropTableTask() throws DBException {
    OracleClient client = new OracleClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.dropTable();
    }
  }

  protected void dropTable() throws DBException {
    TableName tableName = TPCHPrms.getTableName();
    this.db.dropTable(tableName);
  }

//------------------------------------------------------------------------------
// Table import

  public static void importTableTask() throws DBException {
    OracleClient client = new OracleClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.importTable();
    }
  }

  protected void importTable() throws DBException {
    String trimIntervalName = TPCHPrms.getTrimInterval();
    TableName tableName = TPCHPrms.getTableName();
    String fn = TPCHPrms.getDataPath() + "/" + tableName.toString().toLowerCase() + ".tbl";

    int rows = getRowCount(fn);

    long startTime = this.oraclestats.startImportTable();
    this.db.importTable(fn, tableName);
    long endTime = completeTask(trimIntervalName);
    this.oraclestats.endImportTable(endTime, rows);
    TrimReporter.reportTrimInterval(this, trimIntervalName, startTime, endTime, true);
  }

  protected int getRowCount(String fn) {
    String output = ProcessMgr.fgexec("wc " + fn, 300);
    String[] tokens = output.split(" ");
    int rows;
    try {
      rows = Integer.parseInt(tokens[0]);
    } catch (NumberFormatException e) {
      String s = "Unable to process output of wc";
      throw new PerfTestException(s);
    }
    Log.getLogWriter().info(fn + " contains " + rows + " rows");
    return rows;
  }

  @Override
  protected long completeTask(String trimIntervalName) {
    // nothing we need to do besides noting the time
    return System.currentTimeMillis();
  }

//------------------------------------------------------------------------------
// VALIDATION

  public static void validateQueriesTask() throws DBException, InterruptedException {
    OracleClient client = new OracleClient();
    client.initialize();
    client.validateQueries();
  }

  public static void validateQueryTask() throws DBException, InterruptedException {
    OracleClient client = new OracleClient();
    client.initialize();
    client.validateQuery();
  }

//------------------------------------------------------------------------------
// WORKLOAD

  public static void doWorkloadTask() throws DBException, InterruptedException {
    OracleClient client = new OracleClient();
    client.initialize();
    client.doWorkload();
  }
}

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

package com.pivotal.gemfirexd.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.tools.internal.GfxdDdlUtils;
import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.PlatformUtils;
import org.apache.ddlutils.task.DatabaseToDdlTask;
import org.apache.ddlutils.task.DdlToDatabaseTask;
import org.apache.ddlutils.task.WriteDataToDatabaseCommand;
import org.apache.ddlutils.task.WriteDataToFileCommand;
import org.apache.ddlutils.task.WriteSchemaToDatabaseCommand;
import org.apache.ddlutils.task.WriteSchemaToFileCommand;

/**
 * Unit tests for DdlUtils with GemFireXD.
 * 
 * @author swale
 */
public class DdlUtilsTest extends JdbcTestBase {

  public DdlUtilsTest(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  public void testBug51732() throws Throwable {
    final Properties props = new Properties();
    // generates too many logs at fine level
    props.setProperty("log-level", "config");
    setupConnection(props);
    final int netPort = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    final String clientUrl = startNetServer(netPort, null);
    final Connection conn = jdbcConn;
    final Connection netConn = getNetConnection(netPort, null, null);

    final String schemaGFXDile = "TestSchema.sql";
    final String schema = null;

    runImportExportTest_Create(conn, netConn, 100, 65000, schema);

    // also check for schema export as SQL

    final String[] commonArgs = new String[] { "write-schema-to-sql",
        "-driver-class=com.pivotal.gemfirexd.jdbc.ClientDriver",
        "-url=jdbc:gemfirexd://" + clientUrl, "-file=" + schemaGFXDile,
        "-to-database-type=gemfirexd" };

    ArrayList<String> args = new ArrayList<String>();
    args.addAll(Arrays.asList(commonArgs));

    if (currentUserName != null) {
      args.add("-user=" + currentUserName);
      args.add("-password=" + currentUserPassword);
    }

    GfxdDdlUtils.main(args.toArray(new String[args.size()]));

  }

  public void testSchemaCreation() throws Throwable {
    final Properties props = new Properties();
    // generates too many logs at fine level
    props.setProperty("log-level", "config");
    setupConnection(props);
    final int netPort = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    final String clientUrl = startNetServer(netPort, null);
    final Connection conn = jdbcConn;
    final Connection netConn = getNetConnection(netPort, null, null);

    runImportExportTest(conn, netConn, clientUrl, currentUserName,
        currentUserPassword, 100, 65000, null);
  }

  public void testFKAutoGenRewrite() throws Throwable {
    final Properties props = new Properties();
    // force partitioned tables by default
    props.setProperty(com.pivotal.gemfirexd.Attribute.TABLE_DEFAULT_PARTITIONED,
        "true");
    // generates too many logs at fine level
    props.setProperty("log-level", "config");
    setupConnection(props);
    final int netPort = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    final String clientUrl = startNetServer(netPort, null);
    final Connection netConn = getNetConnection(netPort, null, null);

    String[] clientHost = clientUrl.split(":");
    runImportExportFKTest(netConn, clientHost[0], netPort, currentUserName,
        currentUserPassword, 100, 65000, true, false, false);
  }

  public void testFKAlterIdentityColumns() throws Throwable {
    final Properties props = new Properties();
    // force partitioned tables by default
    props.setProperty(com.pivotal.gemfirexd.Attribute.TABLE_DEFAULT_PARTITIONED,
        "true");
    // generates too many logs at fine level
    props.setProperty("log-level", "config");
    setupConnection(props);
    final int netPort = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    final String clientUrl = startNetServer(netPort, null);
    final Connection netConn = getNetConnection(netPort, null, null);

    String[] clientHost = clientUrl.split(":");
    runImportExportFKTest(netConn, clientHost[0], netPort, currentUserName,
        currentUserPassword, 100, 65000, true, true, true);
  }

  public static void main(String[] args) throws Throwable {
    // get the server host:port, alterIdentity, numEntries, rowSize from args
    if (args.length != 4) {
      System.err.println("expected args: <host:port> <alterIdentity(BOOLEAN)> "
          + "<numEntries> <rowSize>");
      System.exit(1);
      // never reached
      return;
    }
    final Connection conn = DriverManager.getConnection("jdbc:gemfirexd://"
        + args[0]);
    String[] clientUrl = args[0].split(":");
    int port = Integer.parseInt(clientUrl[1]);
    runImportExportFKTest(conn, clientUrl[0], port, null, null,
        Integer.parseInt(args[2]), Integer.parseInt(args[3]), true,
        Boolean.parseBoolean(args[1]), false);
  }

  public static void runImportExportTest(final Connection conn,
      final Connection netConn, final String clientUrl, final String userName,
      final String password, final int numEntries, final int rowSize,
      final VM vm1) throws Throwable {

    final String schemaFileName = "TestSchema.xml";
    final String schemaGFXDile = "TestSchema.sql";
    final String schemaGFXDile2 = "TestSchema2.sql";
    final String dataFileName = "TestSchema-data.xml";
    final String schema1 = null;
    final String schema2 = "EMP";
    final String schema3 = "TEST";

    runImportExportTest_Create(conn, netConn, numEntries, rowSize, schema1);
    runImportExportTest_Create(conn, netConn, numEntries, rowSize, schema2);
    runImportExportTest_Create(conn, netConn, numEntries, rowSize, schema3);
    runImportExportTest_Export(clientUrl, userName, password, schemaFileName,
        dataFileName);
    runImportExportTest_Drop(conn, schema3, "");
    runImportExportTest_Drop(conn, schema2, "");
    runImportExportTest_Drop(conn, schema1, "");
    runImportExportTest_ImportFail(netConn, clientUrl, userName, password,
        rowSize, schema1, schemaFileName, dataFileName);
    runImportExportTest_Drop(conn, schema3, "film_");
    runImportExportTest_Drop(conn, schema2, "film_");
    runImportExportTest_Drop(conn, schema1, "film_");
    runImportExportTest_Import(netConn, clientUrl, userName, password,
        schemaFileName, dataFileName);
    runImportExportTest_Check(netConn, numEntries, rowSize, schema1,
        schemaFileName, dataFileName);
    runImportExportTest_Check(netConn, numEntries, rowSize, schema2,
        schemaFileName, dataFileName);
    runImportExportTest_Check(netConn, numEntries, rowSize, schema3,
        schemaFileName, dataFileName);

    // also check for schema export as SQL
    runImportExportTest_Schema_Export(clientUrl, userName, password,
        schemaGFXDile, false);
    runImportExportTest_Drop(netConn, schema3, "film_");
    runImportExportTest_Drop(netConn, schema2, "film_");
    runImportExportTest_Drop(netConn, schema1, "film_");
    runImportExportTest_Schema_Import(conn, schemaGFXDile);
    runImportExportTest_Schema_Check(netConn, schema1, false);
    runImportExportTest_Schema_Check(netConn, schema2, false);
    runImportExportTest_Schema_Check(netConn, schema3, false);
    // now check GemFireXD extensions using full SQL export
    runImportExportTest_Create_Extensions(conn);
    runImportExportTest_Schema_Export(clientUrl, userName, password,
        schemaGFXDile2, true);
    runImportExportTest_Drop(netConn, schema3, "film_");
    runImportExportTest_Drop(netConn, schema2, "film_");
    runImportExportTest_Drop(netConn, schema1, "film_");
    runImportExportTest_Drop_Extensions(conn, vm1 == null);
    runImportExportTest_Schema_Import(conn, schemaGFXDile2);
    if (vm1 == null) {
      runImportExportTest_Schema_Check(netConn, schema1, true);
    }
    else {
      vm1.invoke(DdlUtilsTest.class, "runImportExportTest_Schema_Check",
          new Object[] { null, schema1, true });
    }
    runImportExportTest_Schema_Check(netConn, schema2, false);
    runImportExportTest_Schema_Check(netConn, schema3, false);

    // delete the intermediate files
    runImportExportTest_Drop_Extensions(conn, vm1 == null);
    new File(dataFileName).delete();
    new File(schemaFileName).delete();
    new File(schemaGFXDile).delete();
    new File(schemaGFXDile2).delete();
  }

  static void runImportExportTest_Create(final Connection conn,
      final Connection netConn, final int numEntries, final int rowSize,
      final String schema) throws Throwable {

    final String schemaPrefix = schema != null ? (schema + '.') : "";
    // create a couple of tables with FK reference
    final Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE " + schemaPrefix + "language ("
        + "language_id smallint NOT NULL,"
        + "language_name varchar(20) NOT NULL, PRIMARY KEY (language_id))");
    stmt.execute("CREATE TABLE " + schemaPrefix + "film ("
        + "film_id smallint NOT NULL,"
        + "title varchar(255) NOT NULL, description clob,"
        + "release_year date DEFAULT NULL, language_id smallint NOT NULL,"
        + "original_language_id smallint DEFAULT NULL,"
        + "rental_duration smallint NOT NULL DEFAULT 3,"
        + "rental_rate decimal(4,2) NOT NULL DEFAULT 4.99,"
        + "length smallint DEFAULT NULL,"
        + "replacement_cost decimal(5,2) NOT NULL DEFAULT 19.99,"
        + "rating char(8) DEFAULT 'G',"
        + "special_features varchar(20) DEFAULT NULL,"
        + "last_update timestamp DEFAULT CURRENT_TIMESTAMP,"
        + "PRIMARY KEY (film_id), "
        + "CONSTRAINT ck_rating CHECK (rating in ('G', 'PG', 'PG-13', "
        + "  'R', 'NC-17')), "
        + "CONSTRAINT ck_special CHECK (special_features in ('Trailers',"
        + "  'Commentaries','Deleted Scenes','Behind the Scenes')), "
        + "CONSTRAINT fk_film_language FOREIGN KEY (language_id) "
        + "  REFERENCES " + schemaPrefix + "language (language_id), "
        + "CONSTRAINT fk_film_language_original FOREIGN KEY "
        + "  (original_language_id) REFERENCES language (language_id))");
    stmt.execute("CREATE INDEX " + schemaPrefix + "idx1 on " + schemaPrefix
        + "film(title)");

    // check the index creation
    ResultSet rs = netConn.getMetaData().getIndexInfo(null, schema, "FILM",
        false, false);
    boolean foundIdx1 = false;
    boolean foundIdx2 = false;
    while (rs.next()) {
      if ("TITLE".equals(rs.getString("COLUMN_NAME"))) {
        assertEquals("IDX1", rs.getString("INDEX_NAME"));
        foundIdx1 = true;
      }
    }
    assertFalse(rs.next());
    rs = netConn.getMetaData().getImportedKeys(null, schema, "FILM");
    while (rs.next()) {
      if ("fk_film_language_original".equalsIgnoreCase(rs.getString(
          "FK_NAME"))) {
        assertEquals(TestUtil.getCurrentDefaultSchemaName(),
            rs.getString("PKTABLE_SCHEM"));
        assertEquals("LANGUAGE", rs.getString("PKTABLE_NAME"));
        assertEquals("LANGUAGE_ID", rs.getString("PKCOLUMN_NAME"));
        assertEquals("ORIGINAL_LANGUAGE_ID", rs.getString("FKCOLUMN_NAME"));
        foundIdx2 = true;
      }
    }
    assertFalse(rs.next());
    assertTrue(foundIdx1);
    assertTrue(foundIdx2);

    // add some data to the tables
    final int numLanguages = 10;
    for (int langId = 1; langId <= numLanguages; langId++) {
      stmt.execute("insert into " + schemaPrefix + "language values (" + langId
          + ", 'lang" + langId + "')");
    }

    PreparedStatement pstmt = netConn.prepareStatement("insert into "
        + schemaPrefix + "film (film_id, title, description, language_id, "
        + "original_language_id) values (?, ?, ?, ?, ?)");
    final char[] desc = new char[rowSize];
    int id;
    for (id = 1; id <= numEntries; id++) {
      pstmt.setInt(1, id);
      pstmt.setString(2, "film" + id);
      for (int index = 0; index < desc.length; index++) {
        desc[index] = (char)('<' + (index % 32));
      }
      pstmt.setString(3, new String(desc));
      pstmt.setInt(4, (id % numLanguages) + 1);
      pstmt.setInt(5, ((id + 2) % numLanguages) + 1);
      pstmt.addBatch();
      // split into batches of size 1000
      if (id != numEntries && (id % 1000) == 0) {
        pstmt.executeBatch();
      }
    }
    pstmt.executeBatch();
  }

  static void runImportExportTest_Export(final String clientUrl,
      final String userName, final String password,
      final String schemaFileName, final String dataFileName) throws Throwable {

    // first dump this to XML

    String dbtype = (new PlatformUtils()).determineDatabaseType(
        "com.pivotal.gemfirexd.jdbc.ClientDriver", "jdbc:gemfirexd://" + clientUrl);
    getLogger().info("DB TYPE = " + dbtype);

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("com.pivotal.gemfirexd.jdbc.ClientDriver");
    dataSource.setUrl("jdbc:gemfirexd://" + clientUrl);
    if (userName != null) {
      dataSource.setUsername(userName);
      dataSource.setPassword(password);
    }

    DatabaseToDdlTask fromDBTask = new DatabaseToDdlTask();
    fromDBTask.addConfiguredDatabase(dataSource);
    fromDBTask.setDatabaseType(dbtype);

    WriteSchemaToFileCommand writeSchemaToFile = new WriteSchemaToFileCommand();
    File schemaFile = new File(schemaFileName);
    writeSchemaToFile.setFailOnError(true);
    writeSchemaToFile.setOutputFile(schemaFile);

    WriteDataToFileCommand writeDataToFile = new WriteDataToFileCommand();
    File dataFile = new File(dataFileName);
    writeDataToFile.setFailOnError(true);
    writeDataToFile.setOutputFile(dataFile);

    fromDBTask.addWriteSchemaToFile(writeSchemaToFile);
    fromDBTask.addWriteDataToFile(writeDataToFile);

    fromDBTask.execute();

    dataSource.close();

    // end dump to XML
  }

  static void runImportExportTest_Drop(final Connection conn,
      final String schema, final String indexPrefix) throws Throwable {

    final String schemaPrefix = schema != null ? (schema + '.') : "";
    final Statement stmt = conn.createStatement();
    // drop the tables in GemFireXD
    stmt.execute("drop index " + schemaPrefix + indexPrefix + "idx1");
    stmt.execute("drop table " + schemaPrefix + "film");
    stmt.execute("drop table " + schemaPrefix + "language");
  }

  static void runImportExportTest_ImportFail(final Connection netConn,
      final String clientUrl, final String userName, final String password,
      final int rowSize, final String schema, final String schemaFileName,
      final String dataFileName) throws Throwable {

    final String schemaPrefix = schema != null ? (schema + '.') : "";
    final Statement stmt = netConn.createStatement();
    final char[] desc = new char[rowSize];
    final int id;

    // check for failure in loading tables using the XML data with existing
    // duplicate data (atomically for batching)

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("com.pivotal.gemfirexd.jdbc.ClientDriver");
    dataSource.setUrl("jdbc:gemfirexd://" + clientUrl);
    if (userName != null) {
      dataSource.setUsername(userName);
      dataSource.setPassword(password);
    }

    DdlToDatabaseTask toDBTask = new DdlToDatabaseTask();
    toDBTask.addConfiguredDatabase(dataSource);
    File schemaFile = new File(schemaFileName);
    toDBTask.setSchemaFile(schemaFile);

    WriteSchemaToDatabaseCommand writeSchemaToDB =
        new WriteSchemaToDatabaseCommand();
    writeSchemaToDB.setAlterDatabase(true);
    writeSchemaToDB.setFailOnError(true);

    toDBTask.addWriteSchemaToDatabase(writeSchemaToDB);
    toDBTask.execute();

    WriteDataToDatabaseCommand writeDataToDB = new WriteDataToDatabaseCommand();
    File dataFile = new File(dataFileName);
    writeDataToDB.setIsolationLevel(Connection.TRANSACTION_READ_COMMITTED);
    writeDataToDB.setUseBatchMode(true);
    writeDataToDB.setBatchSize(1000);
    writeDataToDB.setDataFile(dataFile);
    writeDataToDB.setFailOnError(true);

    id = 10;
    stmt.execute("insert into " + schemaPrefix + "language values (" + id
        + ", 'lang" + id + "')");
    final PreparedStatement pstmt = netConn.prepareStatement("insert into "
        + schemaPrefix + "film (film_id, title, description, language_id, "
        + "original_language_id) values (?, ?, ?, ?, ?)");
    pstmt.setInt(1, id);
    pstmt.setString(2, "film" + id);
    for (int index = 0; index < desc.length; index++) {
      desc[index] = (char)('<' + (index % 32));
    }
    pstmt.setString(3, new String(desc));
    pstmt.setInt(4, id);
    pstmt.setInt(5, id);
    pstmt.execute();

    toDBTask = new DdlToDatabaseTask();
    toDBTask.addConfiguredDatabase(dataSource);
    toDBTask.setSchemaFile(schemaFile);

    toDBTask.addWriteDataToDatabase(writeDataToDB);
    try {
      toDBTask.execute();
      fail("expected constraint violation");
    } catch (Throwable t) {
      while (t.getCause() != null && !(t instanceof SQLException)) {
        t = t.getCause();
      }
      if (!(t instanceof SQLException)
          || !"23505".equals(((SQLException)t).getSQLState())) {
        throw t;
      }
    }

    assertEquals(1, stmt.executeUpdate("delete from " + schemaPrefix
        + "film where film_id=" + id));
    assertEquals(1, stmt.executeUpdate("delete from " + schemaPrefix
        + "language where language_id=" + id));

    dataSource.close();
  }

  static void runImportExportTest_Import(final Connection netConn,
      final String clientUrl, final String userName, final String password,
      final String schemaFileName, final String dataFileName) throws Throwable {

    // now load the tables using the XML data

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("com.pivotal.gemfirexd.jdbc.ClientDriver");
    dataSource.setUrl("jdbc:gemfirexd://" + clientUrl);
    if (userName != null) {
      dataSource.setUsername(userName);
      dataSource.setPassword(password);
    }

    DdlToDatabaseTask toDBTask = new DdlToDatabaseTask();
    toDBTask.addConfiguredDatabase(dataSource);
    File schemaFile = new File(schemaFileName);
    toDBTask.setSchemaFile(schemaFile);

    WriteSchemaToDatabaseCommand writeSchemaToDB =
        new WriteSchemaToDatabaseCommand();
    writeSchemaToDB.setAlterDatabase(true);
    writeSchemaToDB.setFailOnError(true);

    toDBTask.addWriteSchemaToDatabase(writeSchemaToDB);
    toDBTask.execute();

    WriteDataToDatabaseCommand writeDataToDB = new WriteDataToDatabaseCommand();
    File dataFile = new File(dataFileName);
    writeDataToDB.setIsolationLevel(Connection.TRANSACTION_READ_COMMITTED);
    writeDataToDB.setUseBatchMode(true);
    writeDataToDB.setBatchSize(1000);
    writeDataToDB.setDataFile(dataFile);
    writeDataToDB.setFailOnError(true);

    // next check for the success case
    toDBTask = new DdlToDatabaseTask();
    toDBTask.addConfiguredDatabase(dataSource);
    toDBTask.setSchemaFile(schemaFile);

    toDBTask.addWriteDataToDatabase(writeDataToDB);
    toDBTask.execute();

    dataSource.close();

    // end load the tables using the XML data
  }

  static void runImportExportTest_Check(final Connection netConn,
      final int numEntries, final int rowSize, final String schema,
      final String schemaFileName, final String dataFileName) throws Throwable {

    final String schemaPrefix = schema != null ? (schema + '.') : "";
    final int numLanguages = 10;
    ResultSet rs;
    final char[] desc = new char[rowSize];
    int id;

    // verify the results
    Statement netStmt = netConn.createStatement();
    rs = netStmt.executeQuery("select * from " + schemaPrefix
        + "language order by language_id");
    for (int langId = 1; langId <= numLanguages; langId++) {
      assertTrue(rs.next());
      assertEquals(langId, rs.getInt(1));
      assertEquals("lang" + langId, rs.getString(2));
    }
    assertFalse(rs.next());

    rs = netStmt.executeQuery("select film_id, title, description, "
        + "language_id, original_language_id from " + schemaPrefix
        + "film order by film_id");
    for (id = 1; id <= numEntries; id++) {
      assertTrue(rs.next());
      assertEquals(id, rs.getInt(1));
      assertEquals("film" + id, rs.getString(2));
      for (int index = 0; index < desc.length; index++) {
        desc[index] = (char)('<' + (index % 32));
      }
      assertEquals(new String(desc), rs.getString(3));
      assertEquals((id % numLanguages) + 1, rs.getInt(4));
      assertEquals(((id + 2) % numLanguages) + 1, rs.getInt(5));
    }
    assertFalse(rs.next());

    // check the index creation
    rs = netConn.getMetaData().getIndexInfo(null, schema, "FILM", false, false);
    boolean foundIdx1 = false;
    boolean foundIdx2 = false;
    while (rs.next()) {
      if ("TITLE".equals(rs.getString("COLUMN_NAME"))) {
        // GemFireXD prepends table name to index name (#43964)
        assertEquals("FILM_IDX1", rs.getString("INDEX_NAME"));
        foundIdx1 = true;
      }
    }
    assertFalse(rs.next());
    rs = netConn.getMetaData().getImportedKeys(null, schema, "FILM");
    while (rs.next()) {
      if ("fk_film_language_original".equalsIgnoreCase(rs.getString(
          "FK_NAME"))) {
        assertEquals(TestUtil.getCurrentDefaultSchemaName(),
            rs.getString("PKTABLE_SCHEM"));
        assertEquals("LANGUAGE", rs.getString("PKTABLE_NAME"));
        assertEquals("LANGUAGE_ID", rs.getString("PKCOLUMN_NAME"));
        assertEquals("ORIGINAL_LANGUAGE_ID", rs.getString("FKCOLUMN_NAME"));
        foundIdx2 = true;
      }
    }
    assertFalse(rs.next());
    assertTrue(foundIdx1);
    assertTrue(foundIdx2);
  }

  static void runImportExportTest_Schema_Export(final String clientUrl,
      final String userName, final String password, final String schemaSQL,
      final boolean gfxdExtensions) {
    final String[] commonArgs = new String[] { "write-schema-to-sql",
        "-driver-class=com.pivotal.gemfirexd.jdbc.ClientDriver",
        "-url=jdbc:gemfirexd://" + clientUrl, "-file=" + schemaSQL };

    ArrayList<String> args = new ArrayList<String>();
    args.addAll(Arrays.asList(commonArgs));
    if (gfxdExtensions) {
      args.add("-export-all");
    }
    else {
      args.add("-generic");
    }
    if (userName != null) {
      args.add("-user=" + userName);
      args.add("-password=" + password);
    }

    GfxdDdlUtils.main(args.toArray(new String[args.size()]));
  }

  static void runImportExportTest_Schema_Import(final Connection conn,
      final String schemaSQL) throws Exception {
    // ignore suspect strings for drop/alter on non-existing tables
    addExpectedException(new Object[] { "42Y55",
        "Exception in execution of command" });
    GemFireXDUtils.executeSQLScripts(conn,
        new String[] { new File(schemaSQL).getAbsolutePath() }, true,
        Misc.getCacheLogWriter(), null, null, false);
    removeExpectedException(new Object[] { "42Y55",
        "Exception in execution of command" });
  }

  static void runImportExportTest_Create_Extensions(final Connection conn)
      throws SQLException {
    final Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE language2 (language_id smallint NOT NULL,"
        + "language_name varchar(20) NOT NULL, PRIMARY KEY (language_name))"
        + " PARTITION BY COLUMN (language_id) BUCKETS 73");
    stmt.execute("CREATE TABLE film2 (film_id int NOT NULL "
        + "GENERATED ALWAYS AS IDENTITY,"
        + "title varchar(255) NOT NULL, description clob,"
        + "release_year date DEFAULT NULL, language_id smallint NOT NULL,"
        + "PRIMARY KEY (film_id)) PARTITION BY COLUMN (language_id) "
        + "COLOCATE WITH (language2) BUCKETS 73");
    stmt.execute("create diskstore ds1 ('test_ext1')");
  }

  @SuppressWarnings("serial")
  static void runImportExportTest_Drop_Extensions(final Connection conn,
      final boolean singleVM) throws SQLException {

    final Statement stmt = conn.createStatement();
    // drop the tables in GemFireXD
    stmt.execute("drop table film2");
    stmt.execute("drop table language2");
    if (singleVM) {
      ((DiskStoreImpl)Misc.getGemFireCache().findDiskStore("DS1")).destroy();
    }
    else {
      DistributedTestBase.invokeInEveryVM(new SerializableRunnable() {
        @Override
        public void run() {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          if (cache != null) {
            ((DiskStoreImpl)cache.findDiskStore("DS1")).destroy();
          }
        }
      });
    }
  }

  static void runImportExportTest_Schema_Check(Connection conn,
      final String schema, final boolean checkGFXDExtensions) throws Exception {

    if (conn == null) {
      conn = getConnection();
    }
    // check the tables
    ResultSet rs = conn.getMetaData().getTables(null, schema, null,
        new String[] { "TABLE" });
    boolean foundLang = false;
    boolean foundFilm = false;
    boolean foundLang2 = false;
    boolean foundFilm2 = false;
    while (rs.next()) {
      String tableName = rs.getString("TABLE_NAME");
      if ("LANGUAGE".equals(tableName)) {
        foundLang = true;
      }
      else if ("FILM".equals(tableName)) {
        foundFilm = true;
      }
      else if ("LANGUAGE2".equals(tableName)) {
        foundLang2 = true;
      }
      else if ("FILM2".equals(tableName)) {
        foundFilm2 = true;
      }
    }
    assertTrue(foundLang);
    assertTrue(foundFilm);
    if (checkGFXDExtensions) {
      assertTrue(foundLang2);
      assertTrue(foundFilm2);
      // checks for LANGUAGE2
      Region<?, ?> lang2 = Misc.getRegionForTable(TestUtil
          .getCurrentDefaultSchemaName() + ".LANGUAGE2", true);
      GemFireContainer container = (GemFireContainer)lang2.getUserAttribute();
      int[] pkColumns = container.getExtraTableInfo().getPrimaryKeyColumns();
      assertNotNull(pkColumns);
      assertEquals(1, pkColumns.length);
      assertEquals(2, pkColumns[0]);
      PartitionAttributes<?, ?> pattrs = lang2.getAttributes()
          .getPartitionAttributes();
      assertNotNull(pattrs);
      assertTrue(pattrs.getPartitionResolver() instanceof
          GfxdPartitionByExpressionResolver);
      GfxdPartitionByExpressionResolver resolver =
          (GfxdPartitionByExpressionResolver)pattrs.getPartitionResolver();
      assertEquals(73, pattrs.getTotalNumBuckets());
      String[] pcols = resolver.getColumnNames();
      assertNotNull(pcols);
      assertEquals(1, pcols.length);
      assertEquals("LANGUAGE_ID", pcols[0]);

      // checks for FILM2
      Region<?, ?> film2 = Misc.getRegionForTable(TestUtil
          .getCurrentDefaultSchemaName() + ".FILM2", true);
      container = (GemFireContainer)film2.getUserAttribute();
      pkColumns = container.getExtraTableInfo().getPrimaryKeyColumns();
      assertNotNull(pkColumns);
      assertEquals(1, pkColumns.length);
      assertEquals(1, pkColumns[0]);
      pattrs = film2.getAttributes()
          .getPartitionAttributes();
      assertNotNull(pattrs);
      assertTrue(pattrs.getPartitionResolver() instanceof
          GfxdPartitionByExpressionResolver);
      resolver = (GfxdPartitionByExpressionResolver)pattrs
          .getPartitionResolver();
      pcols = resolver.getColumnNames();
      assertNotNull(pcols);
      assertEquals(1, pcols.length);
      assertEquals("LANGUAGE_ID", pcols[0]);
      assertEquals("/" + TestUtil.getCurrentDefaultSchemaName() + "/LANGUAGE2",
          pattrs.getColocatedWith());
      assertEquals(73, pattrs.getTotalNumBuckets());

      DiskStore ds = Misc.getGemFireCache().findDiskStore("DS1");
      File[] diskDirs = ds.getDiskDirs();
      assertNotNull(ds);
      assertNotNull(diskDirs);
      assertEquals(1, diskDirs.length);
      assertEquals("test_ext1", diskDirs[0].getName());
    }
    else {
      assertFalse(foundLang2);
      assertFalse(foundFilm2);
    }

    // check the index creation
    rs = conn.getMetaData().getIndexInfo(null, schema, "FILM", false, false);
    boolean foundIdx1 = false;
    boolean foundIdx2 = false;
    while (rs.next()) {
      if ("TITLE".equals(rs.getString("COLUMN_NAME"))) {
        assertEquals("FILM_IDX1", rs.getString("INDEX_NAME"));
        foundIdx1 = true;
      }
    }
    assertFalse(rs.next());
    rs = conn.getMetaData().getImportedKeys(null, schema, "FILM");
    while (rs.next()) {
      if ("fk_film_language_original".equalsIgnoreCase(rs.getString(
          "FK_NAME"))) {
        assertEquals(TestUtil.getCurrentDefaultSchemaName(),
            rs.getString("PKTABLE_SCHEM"));
        assertEquals("LANGUAGE", rs.getString("PKTABLE_NAME"));
        assertEquals("LANGUAGE_ID", rs.getString("PKCOLUMN_NAME"));
        assertEquals("ORIGINAL_LANGUAGE_ID", rs.getString("FKCOLUMN_NAME"));
        foundIdx2 = true;
      }
    }
    assertFalse(rs.next());
    assertTrue(foundIdx1);
    assertTrue(foundIdx2);
  }

  public static void runImportExportFKTest(final Connection conn,
      final String host, final int port, final String userName,
      final String password, final int numEntries, final int rowSize,
      boolean singleServer, boolean alterIdentityColumns, boolean usePR)
      throws Throwable {
    final Statement stmt = conn.createStatement();
    // drop the tables first if possible
    stmt.execute("drop table if exists film_details");
    stmt.execute("drop table if exists film");
    stmt.execute("drop table if exists language");

    String partStr = usePR ? " PARTITION BY PRIMARY KEY" : " REPLICATE";
    String colocStr = usePR ? " COLOCATE WITH (FILM)" : "";
    // create some tables with FK reference on autogen columns
    stmt.execute("CREATE TABLE language (language_id smallint NOT NULL,"
        + "language_name varchar(20) NOT NULL, PRIMARY KEY (language_id))"
        + partStr);
    stmt.execute("CREATE TABLE film (film_id int NOT NULL "
        + "GENERATED ALWAYS AS IDENTITY,"
        + "title varchar(255) NOT NULL, description clob,"
        + "release_year date DEFAULT NULL, language_id smallint NOT NULL,"
        + "PRIMARY KEY (film_id))" + partStr);
    stmt.execute("CREATE TABLE film_details (film_id int NOT NULL,"
        + "row_number int NOT NULL,"
        + "original_language_id smallint DEFAULT NULL,"
        + "rental_duration smallint NOT NULL DEFAULT 3,"
        + "rental_rate decimal(4,2) NOT NULL DEFAULT 4.99,"
        + "length smallint DEFAULT NULL,"
        + "replacement_cost decimal(5,2) NOT NULL DEFAULT 19.99,"
        + "rating char(8) DEFAULT 'G',"
        + "special_features varchar(20) DEFAULT NULL,"
        + "last_update timestamp DEFAULT CURRENT_TIMESTAMP,"
        + "PRIMARY KEY (film_id), "
        + "CONSTRAINT ck_rating CHECK (rating in ('G', 'PG', 'PG-13', "
        + "  'R', 'NC-17')), "
        + "CONSTRAINT ck_special CHECK (special_features in ('Trailers',"
        + "  'Commentaries','Deleted Scenes','Behind the Scenes')), "
        + "CONSTRAINT fk_film_id FOREIGN KEY (film_id) "
        + "  REFERENCES film (film_id), "
        + "CONSTRAINT fk_film_language_original FOREIGN KEY "
        + "  (original_language_id) REFERENCES language (language_id))"
        + partStr + colocStr);

    // add some data to the tables
    final int numLanguages = 10;
    for (int langId = 1; langId <= numLanguages; langId++) {
      stmt.execute("insert into language values (" + langId + ", 'lang"
          + langId + "')");
    }

    PreparedStatement pstmt = conn.prepareStatement("insert into film "
        + "(title, description, language_id) values (?, ?, ?)",
        Statement.RETURN_GENERATED_KEYS);
    ResultSet rs;
    ArrayList<Integer> filmIds = new ArrayList<Integer>(numEntries);
    char[] desc = new char[rowSize];
    int id;
    for (id = 1; id <= numEntries; id++) {
      pstmt.setString(1, "film" + id);
      for (int index = 0; index < desc.length; index++) {
        desc[index] = (char)('<' + ((index + id) % 32));
      }
      pstmt.setString(2, new String(desc));
      pstmt.setInt(3, (id % numLanguages) + 1);
      pstmt.execute();
      rs = pstmt.getGeneratedKeys();
      assertTrue(rs.next());
      filmIds.add(rs.getInt(1));
      assertFalse(rs.next());
    }

    pstmt = conn.prepareStatement("insert into film_details ("
        + "film_id, row_number, original_language_id) values (?, ?, ?)");
    for (id = 1; id <= numEntries; id++) {
      pstmt.setInt(1, filmIds.get(id - 1));
      pstmt.setInt(2, id);
      pstmt.setInt(3, ((id + 2) % numLanguages) + 1);
      pstmt.addBatch();
      // split into batches of size 1000
      if (id != numEntries && (id % 1000) == 0) {
        pstmt.executeBatch();
      }
    }
    pstmt.executeBatch();

    // first dump the schema and data to XML

    final String schemaFile = "TestSchema.xml";
    final String dataFile = "TestSchema-data.xml";

    final List<String> commonArgs;
    if (userName != null) {
      commonArgs = Arrays.asList(new String[] { "-client-bind-address=" + host,
          "-client-port=" + port, "-user=" + userName,
          "-password=" + password });
    }
    else {
      commonArgs = Arrays.asList(new String[] { "-client-bind-address=" + host,
          "-client-port=" + port });
    }

    ArrayList<String> args = new ArrayList<String>();
    args.addAll(Arrays.asList(new String[] { "write-schema-to-xml",
        "-file=" + schemaFile }));
    args.addAll(commonArgs);
    GfxdDdlUtils.main(args.toArray(new String[args.size()]));

    args.clear();
    args.addAll(Arrays.asList(new String[] { "write-data-to-xml",
        "-file=" + dataFile }));
    args.addAll(commonArgs);
    GfxdDdlUtils.main(args.toArray(new String[args.size()]));

    // end dump to XML

    // delete the tables in GemFireXD
    stmt.execute("drop table film_details");
    stmt.execute("drop table film");
    stmt.execute("drop table language");

    // create the schema

    args.clear();
    args.addAll(Arrays.asList(new String[] { "write-schema-to-db",
        "-files=" + schemaFile }));
    args.addAll(commonArgs);
    if (alterIdentityColumns) {
      args.add("-alter-identity-columns");
    }
    GfxdDdlUtils.main(args.toArray(new String[args.size()]));

    // now load the tables using the XML data
    args.clear();
    args.addAll(Arrays.asList(new String[] { "write-data-to-db",
        "-files=" + dataFile }));
    args.addAll(commonArgs);
    if (alterIdentityColumns) {
      // test loading schema from DB itself
      args.add("-alter-identity-columns");
      args.add("-ensure-fk-order=false");
      args.add("-batch-size=20");
    }
    else {
      // test loading schema from given schema XML file
      args.add("-schema-files=" + schemaFile);
      args.add("-batch-size=1");
    }
    GfxdDdlUtils.main(args.toArray(new String[args.size()]));

    // end load the tables using the XML data

    // verify the results
    Statement netStmt = conn.createStatement();
    rs = netStmt.executeQuery("select * from language order by language_id");
    for (int langId = 1; langId <= numLanguages; langId++) {
      assertTrue(rs.next());
      assertEquals(langId, rs.getInt(1));
      assertEquals("lang" + langId, rs.getString(2));
    }
    assertFalse(rs.next());

    // check that FK reference is properly established using
    // the row_number column of film_details
    TreeSet<Integer> allFilmIds = new TreeSet<Integer>();
    rs = netStmt.executeQuery("select f.film_id, title, description, "
        + "language_id, row_number from film f, "
        + "film_details d where f.film_id = d.film_id order by row_number");
    for (id = 1; id <= numEntries; id++) {
      assertTrue(rs.next());
      final int fId;
      assertTrue(allFilmIds.add(rs.getInt(1)));
      if (alterIdentityColumns) {
        assertEquals("film" + id, rs.getString(2));
        fId = id;
      }
      else {
        String filmId = rs.getString(2);
        fId = Integer.parseInt(filmId.substring(4));
      }
      for (int index = 0; index < desc.length; index++) {
        desc[index] = (char)('<' + ((index + fId) % 32));
      }
      assertEquals(new String(desc), rs.getString(3));
      assertEquals((fId % numLanguages) + 1, rs.getInt(4));
      assertEquals(id, rs.getInt(5));
    }
    assertFalse(rs.next());
    if (singleServer) {
      // expect film_id to be incremental from a single JVM
      assertEquals(numEntries - 1, allFilmIds.last() - allFilmIds.first());
    }
    // when using ALTER TABLE to set the identity columns, expect the IDs
    // to match with those inserted earlier
    if (alterIdentityColumns) {
      assertEquals(filmIds.size(), allFilmIds.size());
      assertTrue(allFilmIds.containsAll(filmIds));
    }

    // delete the intermediate files
    new File(dataFile).delete();
    new File(schemaFile).delete();
  }
}

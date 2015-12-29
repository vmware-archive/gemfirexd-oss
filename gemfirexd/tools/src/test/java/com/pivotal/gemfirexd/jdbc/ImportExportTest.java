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

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.sql.*;
import java.util.Properties;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.pivotal.gemfirexd.ToursDBUtil;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

import org.apache.derbyTesting.junit.JDBC;

public class ImportExportTest extends JdbcTestBase {
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(ImportExportTest.class));
    }

  
  public ImportExportTest(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  public void testCaseInsensitivity() throws SQLException {
    // set current user to null for this test to enable usage of default 'APP'
    // schema
    currentUserName = null;
    currentUserPassword = null;
    Connection conn = getConnection();
    ToursDBUtil.createAndLoadToursDB(conn);

    Statement s = conn.createStatement();
    ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM flights");
    assertTrue(rs.next());
    int numRows = rs.getInt(1);
    rs.close();
    
    s.executeUpdate("CALL SYSCS_UTIL.EXPORT_TABLE(" +
                    "'aPp','fLightS','flites.csv',null,null,null)");

    s.executeUpdate("CREATE TABLE FLIGHTS2(FLIGHT_ID CHAR(6) NOT NULL ,"
                    + " SEGMENT_NUMBER INTEGER NOT NULL ,"
                    + " ORIG_AIRPORT CHAR(3),"
                    + " DEPART_TIME TIME,"
                    + " DEST_AIRPORT CHAR(3),"
                    + " ARRIVE_TIME TIME,"
                    + " MEAL CHAR(1),"
                    + " FLYING_TIME DOUBLE PRECISION,"
                    + " MILES INTEGER,"
                    + " AIRCRAFT VARCHAR(6),"
                    + " CONSTRAINT FLIGHTS_PK2 Primary Key ("
                    + " FLIGHT_ID,"
                    + " SEGMENT_NUMBER),"
                    + " CONSTRAINT MEAL_CONSTRAINT2"
                    + " CHECK (meal IN ('B', 'L', 'D', 'S')))");
         
    
    s.executeUpdate("CALL SYSCS_UTIL.IMPORT_TABLE(" +
                    "'ApP','Flights2','flites.csv',null,null,null,0)");
    
    rs = s.executeQuery("SELECT * FROM flights2");
                        
    assertEquals(numRows, JDBC.assertDrainResults(rs));

    conn.close();
  }

  private void testImportHelper(Connection conn, String dataScript,
      String testDir, int expectedRows) throws SQLException, IOException,
      PrivilegedActionException, StandardException {
    GemFireXDUtils.executeSQLScripts(conn, new String[] { dataScript }, false,
        getLogger(), "<path_prefix>", testDir, false);

    String viewQuery = "SELECT * from TD_INSTRUMENT_SCD";
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(viewQuery);
    int rows = 0;
    while (rs.next()) {
      rows++;
    }
    assertEquals(expectedRows, rows);
    assertFalse(rs.next());
  }

  public void testImport() throws Exception {
    
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    // System.setProperty("gemfirexd.optimizer.trace", "true");
    System
        .setProperty(
            "gemfirexd.debug.true",
            "QueryDistribution"
            // ",TracePlanGeneration"
            // + ",DumpParsedTree" + ",DumpBindTree" + ",DumpOptimizedTree"
            //    + ",TraceRSIteration"
                // + ",PrintRCList"
                + ",TraceActivation,TraceClientHA,TraceSingleHop,TraceExecute,TraceNCJ,TraceImport"
        // + ",TraceGroupByQI"
        // + ",TraceGroupByRSIteration"
        // + ",TraceAggregation"
        // + ",TraceLock_*"
        );
    //System.setProperty("DistributionManager.VERBOSE", "true");
    SanityManager.DEBUG_SET("PrintRCList");
    Connection conn = getConnection();
    // Remove setting of this isolation level after fixing #49166 which is a dup of Bug #51834
    // Till that time let it run as non tx at least
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
    try {
      com.pivotal.gemfirexd.internal.impl.load.ImportBase.TEST_IMPORT = true;
      // create the schema
      String testDir = getResourcesDir();
      String createScript = testDir + "/lib/ImportData/schema.sql";
      GemFireXDUtils.executeSQLScripts(conn, new String[] { createScript },
          false, getLogger(), null, null, false);
      testImportHelper(conn, testDir + "/lib/ImportData/import1.sql", testDir,
          103);
      testImportHelper(conn, testDir + "/lib/ImportData/import2.sql", testDir,
          103);
      testImportHelper(conn, testDir + "/lib/ImportData/import3.sql", testDir,
          103);
      testImportHelper(conn, testDir + "/lib/ImportData/import4.sql", testDir,
          103);
      testImportHelper(conn, testDir + "/lib/ImportData/import5.sql", testDir,
          103);
      String scriptDrop = testDir + "/lib/ImportData/schemaDrop.sql";
      GemFireXDUtils.executeSQLScripts(conn, new String[] { scriptDrop }, false,
          getLogger(), null, null, false);
    } finally {
      com.pivotal.gemfirexd.internal.impl.load.ImportBase.TEST_IMPORT = false;
    }
  }
}

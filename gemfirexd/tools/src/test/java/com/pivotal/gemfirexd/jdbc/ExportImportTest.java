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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.pivotal.gemfirexd.TestUtil;

public class ExportImportTest extends JdbcTestBase {

  public ExportImportTest(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  public void testExportTable_defaultSchema() throws SQLException {

    File outfile = new File("myfile.flt");
    if (outfile.exists()) {
      outfile.delete();
    }

    Connection conn = getConnection();
    Statement stmt = conn.createStatement();

    stmt.execute("create table t1(flight_id int not null, "
        + "segment_number int not null, aircraft varchar(20) not null, "
        + "CONSTRAINT FLIGHTS_PK PRIMARY KEY (FLIGHT_ID, SEGMENT_NUMBER))");

    stmt
        .execute("insert into t1 values (1, 1, 'kingfisher'), (1, 2, 'jet'), (2, 1, 'ai'), (3, 1, 'ial')");
    stmt
        .execute("CALL SYSCS_UTIL.EXPORT_TABLE(null, 't1', 'myfile.flt', null, null, null)");

    stmt.execute("create table imported_t1(flight_id int not null, "
        + "segment_number int not null, aircraft varchar(20) not null, "
        + "CONSTRAINT IMPORTED_FLIGHTS_PK PRIMARY KEY (FLIGHT_ID, SEGMENT_NUMBER))");
    stmt
        .execute("CALL SYSCS_UTIL.IMPORT_TABLE(null, 'imported_t1', 'myfile.flt', null, null, null, 0)");

    stmt.execute("select * from imported_t1");
    ResultSet rs = stmt.getResultSet();
    while(rs.next()) {
      System.out.println(rs.getInt(1)+", "+rs.getInt(2)+", "+rs.getString(3));
    }
    stmt.close();
    conn.close();
  }

  public void testExportTable_joinQuery__defaultSchema() throws SQLException {

    File outfile = new File("myfile.flt");
    if (outfile.exists()) {
      outfile.delete();
    }

    Connection conn = getConnection();
    Statement stmt = conn.createStatement();

    stmt.execute("create table t1(flight_id int not null, "
        + "segment_number int not null, aircraft varchar(20) not null, "
        + "CONSTRAINT FLIGHTS_PK PRIMARY KEY (FLIGHT_ID)) replicate");

    stmt
        .execute("create table t2(flight_id int not null, "
            + "segment_number int not null, aircraft varchar(20) not null, "
            + "CONSTRAINT FLIGHTS_FK foreign KEY (FLIGHT_ID) references t1(flight_id)) replicate");

    stmt
        .execute("insert into t1 values (1, 1, 'kingfisher'), (2, 2, 'jet'), (3, 1, 'ai'), (4, 1, 'ial')");
    stmt
        .execute("insert into t2 values (1, 1, 'kingfisher_dep'), (2, 2, 'jet_dep'), (3, 1, 'ai_dep'), (4, 1, 'ial_dep')");

    stmt
        .execute("CALL SYSCS_UTIL.EXPORT_QUERY("
            + "'select * from t1, t2 where t1.flight_id = t2.flight_id', 'myfile.flt', null, null, null)");
    stmt.close();
    conn.close();
  }
  
  public void testImport_bug50128() throws SQLException {
    
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute("create table trade.securities (sec_id int not null, symbol varchar(10) not null, "
        + "price decimal (30, 20), exchange varchar(10) not null, tid int, "
        + "constraint sec_pk primary key (sec_id), "
        + "constraint sec_uq unique (symbol, exchange), "
        + "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse'))) "
        + "partition by primary key  REDUNDANCY 1");

    String importFile = TestUtil.getResourcesDir() + File.separator
        + "lib" + File.separator + "ImportData" + File.separator
        + "SECURITIES.dat";
    stmt.execute("CALL SYSCS_UTIL.IMPORT_TABLE_EX ('TRADE', 'SECURITIES', '"
        + importFile + "', null, null, null, 0, 0, 6, 0, null, null)");
    
    ResultSet r = stmt.executeQuery("select count(1) from trade.securities");
    assertTrue(r.next());
    assertEquals(1179 , r.getInt(1));
    assertFalse(r.next());
    r.close();
    
  }

 public void testBug51374_2() throws Exception {
    //Test with varchar column
    Properties props = new Properties();
//    props.setProperty("log-level", "fine");
//    props.setProperty("gemfirexd.debug.true", "TraceImport");
    Connection cxn = TestUtil.getConnection(props);

    Statement stmt = cxn.createStatement();

    String s[] = { "a\nbc", "p\"q\"\nr", "", "   \"\n\"\"", "", null }; 

    stmt.execute("CREATE table app.t1(col1 int, col2 varchar(10)) persistent "
        + "partition by (col1) ");
    PreparedStatement ps = cxn.prepareStatement("insert into app.t1 "
        + "values (?, ?)");
    for (int j = 0; j < s.length; j++) {
      ps.setInt(1, j);
      ps.setString(2, s[j]);
      ps.execute();
    }
    
    try {
      // export data
      stmt.execute("CALL SYSCS_UTIL.EXPORT_TABLE('APP','T1', 'T1.dat',NULL, NULL, NULL)");

      // import back into a table and verify
      stmt.execute("CREATE table app.t2(col1 int, col2 varchar(10)) persistent "
          + "partition by (col1)");
      stmt.execute("CALL SYSCS_UTIL.IMPORT_TABLE_EX('APP', "
          + "'T2', 'T1.dat', NULL, NULL, NULL, 0, 0, 2, 0, null, null)");

      ResultSet rs = stmt.executeQuery("select col1, col2 from app.t2 order"
          + " by col1");
      int i = 0;
      while (rs.next()) {
        String output = rs.getString(2);
        assertEquals(s[i], output);
        System.out.println("output=" + output);
        i++;
      }
    } finally {
      new File("T1.dat").delete();
    }
  }
}

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
package com.pivotal.gemfirexd.ddl;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

@SuppressWarnings("serial")
public class TriggerDUnit extends DistributedSQLTestBase {

  public TriggerDUnit(String name) {
    super(name);
  }

  private final String goldenTextFile = TestUtil.getResourcesDir()
      + "/lib/checkQuery.xml";

  
  /////////////// ROW TRIGGERS TEST ///////////////////
  public void testTriggers_actionInsert() throws Exception {
    // Start one client a four servers
    startVMs(1, 1);
    startServerVMs(2, 0, "sg1");
    startServerVMs(1, 0, "SG2");

    clientSQLExecute(1, "create schema dom");
    // Create a schema with default server groups GemFire extension
    clientSQLExecute(1, "create table dom.flights(flight_id int not null,"
        + " segment_number int not null, aircraft varchar(20) not null)");

    clientSQLExecute(1,
        "create table dom.flights_history(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");

    clientSQLExecute(1,
        "create table dom.flights_history_two(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");

    clientSQLExecute(1,
        "create table dom.flights_history_del(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");

    String triggerStmnt = "CREATE TRIGGER trig1 "
        + "AFTER UPDATE ON dom.flights " + "REFERENCING OLD AS UPDATEDROW "
        + "FOR EACH ROW MODE DB2SQL "
        + "INSERT INTO dom.flights_history VALUES "
        + "(UPDATEDROW.FLIGHT_ID, UPDATEDROW.AIRCRAFT, "
        + "'INSERTED FROM trig1')";
    clientSQLExecute(1, triggerStmnt);

    triggerStmnt = "CREATE TRIGGER trig2 " + "AFTER INSERT ON dom.flights "
        + "REFERENCING NEW AS NEWROW " + "FOR EACH ROW MODE DB2SQL "
        + "INSERT INTO dom.flights_history_two VALUES "
        + "(NEWROW.FLIGHT_ID, NEWROW.AIRCRAFT, " + "'INSERTED FROM trig2')";

    clientSQLExecute(1, triggerStmnt);
    
    triggerStmnt = "CREATE TRIGGER trig3 " + "AFTER DELETE ON dom.flights "
    + "REFERENCING OLD AS OLDROW " + "FOR EACH ROW MODE DB2SQL "
    + "INSERT INTO dom.flights_history_del VALUES "
    + "(OLDROW.FLIGHT_ID, OLDROW.AIRCRAFT, " + "'INSERTED FROM trig3')";
    
    clientSQLExecute(1, triggerStmnt);
    
    clientSQLExecute(1, "insert into dom.flights values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    clientSQLExecute(1,
        "update dom.flights set segment_number = 10 where segment_number = 20");

    clientSQLExecute(1,
    "delete from dom.flights");
    
    String jdbcSQL = "select * from dom.flights_history";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigone");

    jdbcSQL = "select * from dom.flights_history_two";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
        "trigone_2");
    
    jdbcSQL = "select * from dom.flights_history_del";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
        "trigone_3");
    
    clientSQLExecute(1, "drop trigger trig1");
    clientSQLExecute(1, "drop trigger trig2");
    clientSQLExecute(1, "drop trigger trig3");
    
    clientSQLExecute(1, "truncate table dom.flights");
    clientSQLExecute(1, "truncate table dom.flights_history");
    clientSQLExecute(1, "truncate table dom.flights_history_two");
    clientSQLExecute(1, "truncate table dom.flights_history_del");
    
    clientSQLExecute(1, "insert into dom.flights values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    clientSQLExecute(1,
        "update dom.flights set segment_number = 10 where segment_number = 20");

    clientSQLExecute(1,
    "delete from dom.flights");
    
    jdbcSQL = "select * from dom.flights_history";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "empty");

    jdbcSQL = "select * from dom.flights_history_two";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
        "empty");
    
    jdbcSQL = "select * from dom.flights_history_del";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
        "empty"); 
  }

  public void testTriggers_Replicate_actionInsert() throws Exception {
    
    // Bug #51761
    if (isTransactional) {
      return;
    }
    
    // Start one client a four servers
    startVMs(1, 1);
    startServerVMs(2, 0, "sg1");
    startServerVMs(1, 0, "SG2");

    clientSQLExecute(1, "create schema dom");
    // Create a schema with default server groups GemFire extension
    clientSQLExecute(1, "create table dom.flights(flight_id int not null,"
        + " segment_number int not null, aircraft varchar(20) not null) replicate");

    clientSQLExecute(1,
        "create table dom.flights_history(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");

    clientSQLExecute(1,
        "create table dom.flights_history_two(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");

    clientSQLExecute(1,
        "create table dom.flights_history_del(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");
    
    String triggerStmnt = "CREATE TRIGGER trig1 "
        + "AFTER UPDATE ON dom.flights " + "REFERENCING OLD AS UPDATEDROW "
        + "FOR EACH ROW MODE DB2SQL "
        + "INSERT INTO dom.flights_history VALUES "
        + "(UPDATEDROW.FLIGHT_ID, UPDATEDROW.AIRCRAFT, "
        + "'INSERTED FROM trig1')";
    clientSQLExecute(1, triggerStmnt);

    triggerStmnt = "CREATE TRIGGER trig2 " + "AFTER INSERT ON dom.flights "
        + "REFERENCING NEW AS NEWROW " + "FOR EACH ROW MODE DB2SQL "
        + "INSERT INTO dom.flights_history_two VALUES "
        + "(NEWROW.FLIGHT_ID, NEWROW.AIRCRAFT, " + "'INSERTED FROM trig2')";

    clientSQLExecute(1, triggerStmnt);
    
    triggerStmnt = "CREATE TRIGGER trig3 " + "AFTER DELETE ON dom.flights "
    + "REFERENCING OLD AS OLDROW " + "FOR EACH ROW MODE DB2SQL "
    + "INSERT INTO dom.flights_history_del VALUES "
    + "(OLDROW.FLIGHT_ID, OLDROW.AIRCRAFT, " + "'INSERTED FROM trig3')";
    
    clientSQLExecute(1, triggerStmnt);
    
    clientSQLExecute(1, "insert into dom.flights values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    clientSQLExecute(1,
        "update dom.flights set segment_number = 10 where segment_number = 20");

    clientSQLExecute(1,
    "delete from dom.flights");
    
    String jdbcSQL = "select * from dom.flights_history";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigone");

    jdbcSQL = "select * from dom.flights_history_two";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
        "trigone_2");
    
    jdbcSQL = "select * from dom.flights_history_del";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
        "trigone_3");
  }

  public void testTriggers_actionUpdate() throws Exception {
    // Start one client a four servers
    startVMs(1, 3);

    clientSQLExecute(1, "create schema dom");
    // Create a schema with default server groups GemFire extension
    clientSQLExecute(1, "create table dom.flights(flight_id int not null,"
        + " segment_number int not null, aircraft varchar(20) not null)");

    clientSQLExecute(1, "create table dom.flights_rep(flight_id int not null,"
        + " segment_number int not null, aircraft varchar(20) not null) replicate");
    
    clientSQLExecute(1, "insert into dom.flights values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    clientSQLExecute(1, "insert into dom.flights_rep values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");
    
    clientSQLExecute(1,
        "create table dom.flights_history(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");

    clientSQLExecute(1,
        "create table dom.flights_history_rep(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");
    
    clientSQLExecute(1, "insert into dom.flights_history values "
        + "(4, 'lalta', 'status1'), (3, 'delta', 'status2')");
    
    clientSQLExecute(1, "insert into dom.flights_history_rep values "
        + "(4, 'lalta', 'status1'), (3, 'delta', 'status2')");

    String triggerStmnt = "CREATE TRIGGER trig1 "
        + "AFTER UPDATE ON dom.flights "
        + "REFERENCING OLD AS UPDATEDROW "
        + "FOR EACH ROW MODE DB2SQL "
        + "update dom.flights_history set status='INSERTED FROM trig1' where 1=1";
    
    clientSQLExecute(1, triggerStmnt);
    
    triggerStmnt = "CREATE TRIGGER trig2 "
      + "AFTER UPDATE ON dom.flights_rep "
      + "REFERENCING OLD AS UPDATEDROW "
      + "FOR EACH ROW MODE DB2SQL "
      // trig1 deliberately so that same xml resultset can be used for verification
      + "update dom.flights_history_rep set status='INSERTED FROM trig1' where 1=1";
    
    clientSQLExecute(1, triggerStmnt);
    
    clientSQLExecute(1,
        "update dom.flights set segment_number = 10 where segment_number = 20");

    clientSQLExecute(1,
    "update dom.flights_rep set segment_number = 10 where segment_number = 20");
    
    String jdbcSQL = "select * from dom.flights_history";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigone");
    
    jdbcSQL = "select * from dom.flights_history_rep";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigone");
  }

  public void testTriggers_actionDelete() throws Exception {
    // Start one client a four servers
    startVMs(1, 3);

    clientSQLExecute(1, "create schema dom");
    // Create a schema with default server groups GemFire extension
    clientSQLExecute(1, "create table dom.flights(flight_id int not null,"
        + " segment_number int not null, aircraft varchar(20) not null)");

    clientSQLExecute(1,
        "create table dom.flights_rep(flight_id int not null, segment_number "
            + "int not null, aircraft varchar(20) not null) replicate");

    clientSQLExecute(1, "insert into dom.flights values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    clientSQLExecute(1, "insert into dom.flights_rep values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    clientSQLExecute(1,
        "create table dom.flights_history(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null) "
            + "partition by column(status)");

    clientSQLExecute(1,
        "create table dom.flights_history_rep(flight_id int not null, aircraft "
            + "varchar(20) not null, status varchar(50) not null) replicate");

    clientSQLExecute(1, "insert into dom.flights_history values "
        + "(4, 'lalta', 'status1'), (3, 'delta', 'status2')");

    clientSQLExecute(1, "insert into dom.flights_history_rep values "
        + "(4, 'lalta', 'status1'), (3, 'delta', 'status2')");

    String triggerStmnt = "CREATE TRIGGER trig1 "
        + "AFTER UPDATE ON dom.flights " + "REFERENCING OLD AS UPDATEDROW "
        + "FOR EACH ROW MODE DB2SQL "
        + "delete from dom.flights_history where flight_id = 4";
    clientSQLExecute(1, triggerStmnt);

    triggerStmnt = "CREATE TRIGGER trig2 " + "AFTER UPDATE ON dom.flights_rep "
        + "REFERENCING OLD AS UPDATEDROW " + "FOR EACH ROW MODE DB2SQL "
        + "delete from dom.flights_history_rep where flight_id = 4";
    clientSQLExecute(1, triggerStmnt);

    clientSQLExecute(1,
        "update dom.flights set segment_number = 10 where segment_number = 20");

    clientSQLExecute(1,
        "update dom.flights_rep set segment_number = 10 where segment_number = 20");

    String jdbcSQL = "select * from dom.flights_history";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigtwo");

    jdbcSQL = "select * from dom.flights_history_rep";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigtwo");
  }

  public void testTriggers_Replicate_actionInsert_defaultSchema()
      throws Exception {
    // Start one client a four servers
    startVMs(1, 1);
    startServerVMs(2, 0, "sg1");
    startServerVMs(1, 0, "SG2");

    clientSQLExecute(1, "create schema dom");

    clientSQLExecute(1,
        "create table dom.flights_history(flight_id int not null,"
            + " aircraft varchar(20) not null, status varchar(50) not null)");

    // Create a schema with default server groups GemFire extension
    clientSQLExecute(
        1,
        "create table dom.flights(flight_id int not null,"
            + " segment_number int not null, aircraft varchar(20) not null) replicate");

    clientSQLExecute(1, "insert into dom.flights values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    String triggerStmnt = "CREATE TRIGGER trig1 "
        + "AFTER UPDATE ON dom.flights " + "REFERENCING OLD AS UPDATEDROW "
        + "FOR EACH ROW MODE DB2SQL "
        + "INSERT INTO dom.flights_history VALUES "
        + "(UPDATEDROW.FLIGHT_ID, UPDATEDROW.AIRCRAFT, "
        + "'INSERTED FROM trig1')";
    clientSQLExecute(1, triggerStmnt);

    clientSQLExecute(1,
        "update dom.flights set segment_number = 10 where segment_number = 20");

    String jdbcSQL = "select * from dom.flights_history";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigone");
  }

  public void testTriggersTX_actionUpdate() throws Exception {
    // Start one client a four servers
    startVMs(1, 3);

    final Connection conn = TestUtil.jdbcConn;
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn.setAutoCommit(false);
    Statement stmt = conn.createStatement();

    stmt.execute("create schema dom");
    stmt.execute("create table dom.flights(flight_id int not null,"
        + " segment_number int not null, aircraft varchar(20) not null)");

    stmt.execute("create table dom.flights_rep(flight_id int not null,"
        + " segment_number int not null, aircraft varchar(20) "
        + "not null) replicate");

    stmt.execute("insert into dom.flights values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

    stmt.execute("insert into dom.flights_rep values (1, 10, 'delta'), "
        + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");
    conn.commit();

    stmt.execute("create table dom.flights_history(flight_id int not null,"
        + " aircraft varchar(20) not null, status varchar(50) not null)");

    stmt.execute("create table dom.flights_history_rep(flight_id int not null,"
        + " aircraft varchar(20) not null, status varchar(50) not null)");

    stmt.execute("insert into dom.flights_history values "
        + "(4, 'lalta', 'status1'), (3, 'delta', 'status2')");

    stmt.execute("insert into dom.flights_history_rep values "
        + "(4, 'lalta', 'status1'), (3, 'delta', 'status2')");
    conn.commit();

    String triggerStmnt = "CREATE TRIGGER trig1 AFTER UPDATE ON dom.flights "
        + "REFERENCING OLD AS UPDATEDROW FOR EACH ROW MODE DB2SQL "
        + "update dom.flights_history set status='INSERTED FROM trig1' "
        + "where 1=1";

    stmt.execute(triggerStmnt);

    triggerStmnt = "CREATE TRIGGER trig2 AFTER UPDATE ON dom.flights_rep "
        + "REFERENCING OLD AS UPDATEDROW FOR EACH ROW MODE DB2SQL "
        // trig1 deliberately so that same xml resultset can be used for
        // verification
        + "update dom.flights_history_rep set status='INSERTED FROM trig1' "
        + "where 1=1";

    stmt.execute(triggerStmnt);

    stmt.execute("update dom.flights set segment_number = 10 "
        + "where segment_number = 20");

    stmt.execute("update dom.flights_rep set segment_number = 10 "
        + "where segment_number = 20");

    String jdbcSQL = "select * from dom.flights_history";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigone");

    jdbcSQL = "select * from dom.flights_history_rep";
    sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile, "trigone");

    // check old rows on a different connection
    final Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn2.setAutoCommit(false);
    Statement stmt2 = conn2.createStatement();

    jdbcSQL = "select * from dom.flights_history";
    stmt2.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt2, false, goldenTextFile, "trigone_1");

    jdbcSQL = "select * from dom.flights_history_rep";
    stmt2 = conn2.createStatement();
    stmt2.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt2, false, goldenTextFile, "trigone_1");

    // also check after commit
    conn.commit();

    jdbcSQL = "select * from dom.flights_history";
    stmt2 = conn2.createStatement();
    stmt2.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt2, false, goldenTextFile, "trigone");
    stmt.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt, false, goldenTextFile, "trigone");

    jdbcSQL = "select * from dom.flights_history_rep";
    stmt2 = conn2.createStatement();
    stmt2.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt2, false, goldenTextFile, "trigone");
    stmt = conn.createStatement();
    stmt.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt, false, goldenTextFile, "trigone");

    conn2.commit();
    jdbcSQL = "select * from dom.flights_history";
    stmt2 = conn2.createStatement();
    stmt2.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt2, false, goldenTextFile, "trigone");

    jdbcSQL = "select * from dom.flights_history_rep";
    stmt2 = conn2.createStatement();
    stmt2.execute(jdbcSQL);
    TestUtil.verifyResults(true, stmt2, false, goldenTextFile, "trigone");
  }

  /////////////// STATEMENT TRIGGERS TEST ///////////////////

  private static List<String> historyEnties;

  public static void fhmbip() {
    if (historyEnties == null) {
      historyEnties = new ArrayList<String>();
    }
    historyEnties.add("Before insert entry");
  }

  public static void fhmaip() {
    if (historyEnties == null) {
      historyEnties = new ArrayList<String>();
    }
    historyEnties.add("After insert entry");
  }

  public static void fhmbup() {
    if (historyEnties == null) {
      historyEnties = new ArrayList<String>();
    }
    historyEnties.add("Before update entry");
  }

  public static void fhmaup() {
    if (historyEnties == null) {
      historyEnties = new ArrayList<String>();
    }
    historyEnties.add("After update entry");
  }

  public static void fhmbdp() {
    if (historyEnties == null) {
      historyEnties = new ArrayList<String>();
    }
    historyEnties.add("Before delete entry");
  }

  public static void fhmadp() {
    if (historyEnties == null) {
      historyEnties = new ArrayList<String>();
    }
    historyEnties.add("After delete entry");
  }

  //Uncomment after fixing #42304
  public void _testStmntTriggers_WithProcedures() throws Exception {
    startVMs(1, 1);

    clientSQLExecute(
        1,"create procedure flightHistoryMaintainerBeforeInsertProc() " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA  " +
        "EXTERNAL NAME 'com.pivotal.gemfirexd.ddl.TriggerDUnit.fhmbip'");
    clientSQLExecute(
        1,"create procedure flightHistoryMaintainerAfterInsertProc() " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA  " +
        "EXTERNAL NAME 'com.pivotal.gemfirexd.ddl.TriggerDUnit.fhmaip'");
    clientSQLExecute(
        1,"create procedure flightHistoryMaintainerBeforeUpdateProc() " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA  " +
        "EXTERNAL NAME 'com.pivotal.gemfirexd.ddl.TriggerDUnit.fhmbup'");
    clientSQLExecute(
        1,"create procedure flightHistoryMaintainerAfterUpdateProc() " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA  " +
        "EXTERNAL NAME 'com.pivotal.gemfirexd.ddl.TriggerDUnit.fhmaup'");
    clientSQLExecute(
        1,"create procedure flightHistoryMaintainerBeforeDeleteProc() " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA  " +
        "EXTERNAL NAME 'com.pivotal.gemfirexd.ddl.TriggerDUnit.fhmbdp'");
    clientSQLExecute(
        1,"create procedure flightHistoryMaintainerAfterDeleteProc() " +
        "LANGUAGE JAVA PARAMETER STYLE JAVA  " +
        "EXTERNAL NAME 'com.pivotal.gemfirexd.ddl.TriggerDUnit.fhmadp'");
    
    clientSQLExecute(
        1,"create table dom.flights(flight_id int not null," +
                " segment_number int not null, aircraft varchar(20) not null)");
    
//    String triggerStmnt = "CREATE TRIGGER BEF_INS NO CASCADE BEFORE INSERT ON dom.flights "
//      + "FOR EACH STATEMENT MODE DB2SQL"
//      + " call flightHistoryMaintainerBeforeInsertProc()";
//    
//    clientSQLExecute(1,triggerStmnt);
    
    String triggerStmnt = "CREATE TRIGGER AFT_INS AFTER INSERT ON dom.flights "
      + "FOR EACH STATEMENT MODE DB2SQL"
      + " call flightHistoryMaintainerAfterInsertProc()";
    
    clientSQLExecute(1,triggerStmnt);
    
//    triggerStmnt = "CREATE TRIGGER BEF_UPD NO CASCADE BEFORE UPDATE ON dom.flights "
//      + "FOR EACH STATEMENT MODE DB2SQL"
//      + " call flightHistoryMaintainerBeforeUpdateProc()";
//    
//    clientSQLExecute(1,triggerStmnt);
    
    triggerStmnt = "CREATE TRIGGER AFT_UPD AFTER UPDATE ON dom.flights "
      + "FOR EACH STATEMENT MODE DB2SQL"
      + " call flightHistoryMaintainerAfterUpdateProc()";
    
    clientSQLExecute(1,triggerStmnt);
    
//    triggerStmnt = "CREATE TRIGGER BEF_DEL NO CASCADE BEFORE DEL ON dom.flights "
//      + "FOR EACH STATEMENT MODE DB2SQL"
//      + " call flightHistoryMaintainerBeforeDeleteProc()";
//    
//    clientSQLExecute(1,triggerStmnt);
    
    triggerStmnt = "CREATE TRIGGER AFT_DEL AFTER DELETE ON dom.flights "
      + "FOR EACH STATEMENT MODE DB2SQL"
      + " call flightHistoryMaintainerAfterDeleteProc()";

    clientSQLExecute(1, triggerStmnt);

    for (int i = 0; i < 2; i++) {
      clientSQLExecute(1, "insert into dom.flights values (1, 10, 'delta'), "
          + "(2, 10, 'palta'), (3, 20, 'delta'), (4, 20, 'lalta')");

      String jdbcSQL = "select * from dom.flights";
      sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
          "trigStmntBeforeUpdate");

      clientSQLExecute(1, "update dom.flights set segment_number = 1000");

      jdbcSQL = "select * from dom.flights";
      sqlExecuteVerify(new int[] { 1 }, null, jdbcSQL, goldenTextFile,
          "trigStmntAfterUpdate");

      clientSQLExecute(1, "delete from dom.flights");

//      this.clientVMs.get(0).invoke(TriggerDUnit.class, "verifyHistoryEntries");
      verifyHistoryEntries();
    }
  }

  public static void verifyHistoryEntries() {
    //assertEquals(6, historyEnties.size());
//    for(int i=0; i<historyEnties.size(); i++) {
//      System.out.println("KN: historyEntries["+i+"] = "+historyEnties.get(i));
//    }
    assertEquals(3, historyEnties.size());

    // assertEquals("Before insert entry", historyEnties.get(0));

    assertEquals("After insert entry", historyEnties.get(0));

    // assertEquals("Before update entry", historyEnties.get(2));

    assertEquals("After update entry", historyEnties.get(1));

    // assertEquals("Before delete entry", historyEnties.get(4));

    assertEquals("After delete entry", historyEnties.get(2));

    historyEnties.clear();
  }
}

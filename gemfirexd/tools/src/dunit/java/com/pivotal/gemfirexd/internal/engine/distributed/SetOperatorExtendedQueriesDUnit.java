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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.HashSet;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.distributed.SetQueriesDUnitHelper.CreateTableDUnitF;

@SuppressWarnings("serial")
public class SetOperatorExtendedQueriesDUnit extends DistributedSQLTestBase {
  private static SetQueriesDUnitHelper setQueriesDUnitHelper = new SetQueriesDUnitHelper();
  
  static boolean[] doRunThisCaseForSetOpDistinct_withPK_withColoc =
  {true, true, true, true}; //  {0, 1, 2, 3}
  static boolean[] doRunThisCaseForSetOpAll_withPK_withColoc =
  {true, true, true, true}; //  {0, 1, 2, 3}

  public SetOperatorExtendedQueriesDUnit(String name) {
    super(name); 
  }

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    return "config";
  }

  /*
   * Handle scenarios where table have been created with primary keys
   */
  private final CreateTableDUnitF[] createTableFunctions_withColoc_withPK =
    new SetQueriesDUnitHelper.CreateTableDUnitF[] {
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onPK_1(), //0
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onCol_1(), //1
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onRange_ofPK_1(), //2
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onRange_ofCol_1(), //3
  };

  /*
   * Handle scenarios where table have NOT been created with primary keys
   * This means, there would be DUPLICATE values in tables
   */
  private final CreateTableDUnitF[] createTableFunctions_noPK =
    new SetQueriesDUnitHelper.CreateTableDUnitF[]
                                                {
      setQueriesDUnitHelper.new CreateTable_noPK_PR_onCol_1(), //0
      setQueriesDUnitHelper.new CreateTable_noPK_PR_onRange_ofCol_1(), //1
                                                };
 
  /*
   * Colocated, no Index, with PK
   */
  public void testSetOperators_withColoc_withPK_noIndex_red0() throws Exception {
    startVMs(1, 3);
    int redundancy = 0;
    for(int i=createTableFunctions_withColoc_withPK.length-1 ; i >= 0; i--) {
        caseSetOperators_withColoc_withPK_scenario1(i, false, redundancy);
    }
  } /**/
  
  /*
   * Colocated, no Index, with PK
   */
  public void testSetOperators_withColoc_withPK_noIndex_red1() throws Exception {
    startVMs(1, 3);
    int redundancy = 1;
    for(int i=createTableFunctions_withColoc_withPK.length-1 ; i >= 0; i--) {
        caseSetOperators_withColoc_withPK_scenario1(i, false, redundancy);
    }
  } /**/
  
  /*
   * Colocated, no Index, with PK
   */
  public void testSetOperators_withColoc_withPK_noIndex_red2() throws Exception {
    startVMs(1, 3);
    int redundancy = 2;
    for(int i=createTableFunctions_withColoc_withPK.length-1 ; i >= 0; i--) {
        caseSetOperators_withColoc_withPK_scenario1(i, false, redundancy);
    }
  } /**/

  /*
   * Colocated, with Index, with pK
   */
  public void testSetOperators_withColoc_withPK_withIndex_red0() throws Exception {
    startVMs(1, 3);
    int redundancy = 0;
    for(int i=createTableFunctions_withColoc_withPK.length-1 ; i >= 0; i--) {
      caseSetOperators_withColoc_withPK_scenario1(i, true, redundancy);
    }
  } /**/
  
  /*
   * Colocated, with Index, with pK
   */
  public void testSetOperators_withColoc_withPK_withIndex_red1() throws Exception {
    startVMs(1, 3);
    int redundancy = 1;
    for(int i=createTableFunctions_withColoc_withPK.length-1 ; i >= 0; i--) {
      caseSetOperators_withColoc_withPK_scenario1(i, true, redundancy);
    }
  } /**/
  
  /*
   * Colocated, with Index, with pK
   */
  public void testSetOperators_withColoc_withPK_withIndex_red2() throws Exception {
    startVMs(1, 3);
    int redundancy = 2;
    for(int i=createTableFunctions_withColoc_withPK.length-1 ; i >= 0; i--) {
      caseSetOperators_withColoc_withPK_scenario1(i, true, redundancy);
    }
  } /**/

  /*
   * Non-colocated, with and without Index, NO PK
   */
  public void testSetOperators_noColoc_noPK_noIndex_red0() throws Exception {
    startVMs(1, 3);
    int redundancy = 0;
    caseSetOperators_noColoc_noPK_scenario1(false, redundancy);
  } /**/
  
  /*
   * Non-colocated, with and without Index, NO PK
   */
  public void testSetOperators_noColoc_noPK_withIndex_red0() throws Exception {
    startVMs(1, 3);
    int redundancy = 0;
    caseSetOperators_noColoc_noPK_scenario1(true, redundancy);
  } /**/
  
  /*
   * Non-colocated, with and without Index, NO PK
   */
  public void testSetOperators_noColoc_noPK_noIndex_red1() throws Exception {
    startVMs(1, 3);
    int redundancy = 1;
    caseSetOperators_noColoc_noPK_scenario1(false, redundancy);
  } /**/
  
  /*
   * Non-colocated, with and without Index, NO PK
   */
  public void testSetOperators_noColoc_noPK_withIndex_red1() throws Exception {
    startVMs(1, 3);
    int redundancy = 1;
    caseSetOperators_noColoc_noPK_scenario1(true, redundancy);
  } /**/

  /*
   * Non-colocated, with and without Index, NO PK
   */
  public void testSetOperators_noColoc_noPK_noIndex_red2() throws Exception {
    startVMs(1, 3);
    int redundancy = 2;
    caseSetOperators_noColoc_noPK_scenario1(false, redundancy);
  } /**/

  /*
   * Non-colocated, with and without Index, NO PK
   */
  public void testSetOperators_noColoc_noPK_withIndex_red2() throws Exception {
    startVMs(1, 3);
    int redundancy = 2;
    caseSetOperators_noColoc_noPK_scenario1(true, redundancy);
  } /**/
  
  /*
   * Test single node scenario
   * Especially to handle a bug where first child of union returns compact exec row,
   * and second returns value row, it gives class cast execption
   * where clause: equality on PK
   * Non-colocated
   */
  public void testSetOperators_specialTest1_SingleNodeCase_1() throws Exception {
    startVMs(1, 1);

    { // Both values are present - 2      
      int loc1 = 9; // only in driver table
      int loc2 = 28; // both in driver table and other table
      String query1 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders1" + " where id = " + loc1;
      String query2 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders2" + " where id = " + loc2;

      Connection conn = case1SetOperators_PR_PR_SingleNodeScenario1_half1(false); 
      startVMs(0, 2);
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("union", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("union all", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("intersect", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("intersect all", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("except", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("except all", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half3(conn); 
    }
  }

  /*
   * Test single node scenario
   * Especially to handle a bug where first child of union returns compact exec row,
   * and second returns value row, it gives class cast execption
   * where clause: equality on PK
   * Non-colocated
   */
  public void testSetOperators_specialTest1_SingleNodeCase_2() throws Exception {
    startVMs(1, 1);

    { // Both values are present - 2
      int loc1 = 9; // only in driver table
      int loc2 = 18; // both in driver table and other table
      String query1 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders1" + " where id = " + loc1;
      String query2 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders2" + " where id = " + loc2;

      Connection conn = case1SetOperators_PR_PR_SingleNodeScenario1_half1(false); 
      startVMs(0, 2);
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("union", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("union all", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("intersect", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("intersect all", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("except", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half2("except all", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario1_half3(conn); 
    }
  }

  /*
   * Test single node scenario
   * Especially to handle a bug where first child of union returns compact exec row,
   * and second returns value row, it gives class cast execption
   * where clause: equality on PK
   * Non-colocated
   */
  public void testSetOperators_specialTest1_SingleNodeCase_3() throws Exception {
    startVMs(1, 1);

    { // Both values are present - 2      
      int loc1 = 9; // only in driver table
      int loc2 = 28; // both in driver table and other table
      String query1 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders1" + " where id = " + loc1;
      String query2 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders2" + " where id = " + loc2;

      Connection conn = case1SetOperators_PR_PR_SingleNodeScenario2_half1(false); 
      startVMs(0, 2);
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("union", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("union all", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("intersect", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("intersect all", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("except", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("except all", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half3(conn); 
    }
  }

  /*
   * Test single node scenario
   * Especially to handle a bug where first child of union returns compact exec row,
   * and second returns value row, it gives class cast execption
   * where clause: equality on PK
   * Non-colocated
   */
  public void testSetOperators_specialTest1_SingleNodeCase_4() throws Exception {
    startVMs(1, 1);

    { // Both values are present - 2
      int loc1 = 9; // only in driver table
      int loc2 = 18; // both in driver table and other table
      String query1 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders1" + " where id = " + loc1;
      String query2 = "Select * from " + SetQueriesDUnitHelper.schemaName + ".orders2" + " where id = " + loc2;

      Connection conn = case1SetOperators_PR_PR_SingleNodeScenario2_half1(false); 
      startVMs(0, 2);
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("union", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("union all", 2, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("intersect", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("intersect all", 0, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("except", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half2("except all", 1, query1, query2,conn); 
      case1SetOperators_PR_PR_SingleNodeScenario2_half3(conn); 
    }
  }

  /*
   * Colocated
   * Test various combinations of PR PR and RR 
   */
  private void caseSetOperators_withColoc_withPK_scenario1(
      int tableIndex,
      boolean createIndex,
      int redundancy) throws Exception {
    boolean setOPDistinct =  doRunThisCaseForSetOpDistinct_withPK_withColoc[tableIndex];
    boolean setOpAll =  doRunThisCaseForSetOpAll_withPK_withColoc[tableIndex];

    if (setOPDistinct || setOpAll) {
      Properties props = new Properties();
      Connection conn = TestUtil.getConnection(props);    

      SetQueriesDUnitHelper.createSchema(conn);

      getLogWriter().info("SetOperatorQueriesDUnit.caseSetOperators_withColoc_withPK_scenario1: "
          + "With Index " + createIndex
          + " with redundancy " + redundancy); 

      getLogWriter().info("Execute create table (main): " 
          + "["
          + tableIndex
          + "] "
          + createTableFunctions_withColoc_withPK[tableIndex].getClass().getName());
      createTableFunctions_withColoc_withPK[tableIndex].createTableFunction(conn, "orders1", redundancy);

      getLogWriter().info("Execute create table (colocated): " 
          + "["
          + tableIndex
          + "] "
          + createTableFunctions_withColoc_withPK[tableIndex].getClass().getName());
      createTableFunctions_withColoc_withPK[tableIndex].createColocatedTableFunction(conn, "orders2", redundancy);

      if (createIndex) {
        SetQueriesDUnitHelper.createLocalIndex(conn, "orders1", "vol");
        SetQueriesDUnitHelper.createLocalIndex(conn, "orders2", "vol");
      } 

      SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders1", 0);
      SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders2", 10);

      // execute
      getLogWriter().info("call execute with: "
          + "doUnion/Intersect/Except " + setOPDistinct
          + " doUnion/Intersect/ExceptDistinct " + setOPDistinct
          + " doUnion/Intersect/ExceptAll " + setOpAll);
      String onFailMessage = "create table: " 
        + "[" + tableIndex + "]"
        + "[" + tableIndex+ "] "
        + "with index" + "(" + createIndex + ") "
        + "redundancy" + "(" + redundancy + ") ";;
        SetQueriesDUnitHelper.callExecuteSetOp_withPK(getLogWriter(), 
            setOPDistinct, setOPDistinct, setOpAll, 
            conn, onFailMessage);

        // drop tables
        SetQueriesDUnitHelper.dropTable(conn, "orders2");
        SetQueriesDUnitHelper.dropTable(conn, "orders1");
        SetQueriesDUnitHelper.dropSchema(conn);
    } 
    else {
      getLogWriter().info("SetOperatorQueriesDUnit.caseSetOperators_withColoc_withPK_scenario1: "
          + "With Index " + createIndex
          + " with redundancy " + redundancy
          + "Did not execute for being disabled"); 
    }
  } /**/
  
  /*
   * Non-colocated
   * Test various combinations of PR PR and RR 
   */
  private void caseSetOperators_noColoc_noPK_scenario1(
      boolean createIndex,
      int redundancy) throws Exception {
    Properties props = new Properties();
    Connection conn = TestUtil.getConnection(props);    

    SetQueriesDUnitHelper.createSchema(conn);

    getLogWriter().info("SetOperatorQueriesDUnit.caseSetOperators_noColoc_withPK_scenario1: "
        + "With Index " + createIndex
        + " with redundancy " + redundancy); 

    getLogWriter().info("Execute left create table: " 
        + createTableFunctions_noPK[0].getClass().getName());
    createTableFunctions_noPK[0].createTableFunction(conn, "orders1", redundancy);

    getLogWriter().info("Execute right create table: " 
        + createTableFunctions_noPK[1].getClass().getName());
    createTableFunctions_noPK[1].createTableFunction(conn, "orders2", redundancy);

    if (createIndex) {
      getLogWriter().info("Execute create index on left table: "); 
      SetQueriesDUnitHelper.createLocalIndex(conn, "orders1", "vol");
      getLogWriter().info("Execute create index on right table: "); 
      SetQueriesDUnitHelper.createLocalIndex(conn, "orders2", "vol");
    } 

    // first table X 3
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders1", 0);
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders1", 0);
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders1", 0);
    // second table X 2
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders2", 10);
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders2", 10);

    // execute
    getLogWriter().info("call execute with: "
        + "doUnion/Intersect/Except " + true
        + " doUnion/Intersect/ExceptDistinct " + true
        + " doUnion/Intersect/ExceptAll " + true);
    String onFailMessage = "create table-noPK: " 
      + "with index" + "(" + createIndex + ") "
      + "redundancy" + "(" + redundancy + ") ";

    SetQueriesDUnitHelper.callExecuteSetOp_noPK(getLogWriter(),
        true, true, true, 
        conn, onFailMessage);

    // drop tables
    SetQueriesDUnitHelper.dropTable(conn, "orders2");
    SetQueriesDUnitHelper.dropTable(conn, "orders1");
    SetQueriesDUnitHelper.dropSchema(conn);
  } /**/

  /*
   * Non-colocated
   * Partition on range-col + partition on pk
   * Redundancy 1 - all tables present on all nodes 
   */
  private Connection case1SetOperators_PR_PR_SingleNodeScenario1_half1(boolean createIndex) throws Exception {
    Properties props = new Properties();
    Connection conn = TestUtil.getConnection(props);    

    SetQueriesDUnitHelper.createSchema(conn);
    setQueriesDUnitHelper.new CreateTable_withPK_PR_onRange_ofCol_1().createTableFunction(conn, "orders1", 1);
    setQueriesDUnitHelper.new CreateTable_withPK_PR_onPK_1().createTableFunction(conn, "orders2", 1);
    if (createIndex) {
      SetQueriesDUnitHelper.createLocalIndex(conn, "orders1", "vol");
      SetQueriesDUnitHelper.createLocalIndex(conn, "orders2", "vol");
    }
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders1", 0);
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders2", 10);

    return conn;
  } /**/

  /*
   * Non-colocated
   * Partition on range-col + partition on pk
   * Redundancy 1 - all tables present on all nodes 
   */
  private void case1SetOperators_PR_PR_SingleNodeScenario1_half2(String queryType, 
      int expectedRows,
      String query1,
      String query2, 
      Connection conn) throws Exception {
    String execQuery = query1 + " " + queryType + " " + query2;
    getLogWriter().info("Execute Query: " + execQuery);
    ResultSet rs = conn.createStatement().executeQuery(execQuery);
    int rowCount = 0;
    Map<Integer, Integer> resultMap = new TreeMap<Integer, Integer>(); 
    while(rs.next()) {
      rowCount++;
      resultMap.put(rs.getInt(1), (resultMap.get(rs.getInt(1)) == null? 1 : resultMap.get(rs.getInt(1)) + 1));
    }
    rs.close();

    getLogWriter().info("expectedRows : " + expectedRows + " Got : " + rowCount );
    getLogWriter().info(resultMap.toString());
    assertEquals(expectedRows, rowCount);
  } /**/
  
  /*
   * Non-colocated
   * Partition on range-col + partition on pk
   * Redundancy 1 - all tables present on all nodes 
   */
  private void case1SetOperators_PR_PR_SingleNodeScenario1_half3(Connection conn) throws Exception {
    SetQueriesDUnitHelper.dropTable(conn, "orders2");
    SetQueriesDUnitHelper.dropTable(conn, "orders1");
    SetQueriesDUnitHelper.dropSchema(conn);
  } /**/

  /*
   * Non-colocated
   * Partition on range-col + partition on pk
   * Redundancy 1 - all tables present on all nodes 
   */
  private Connection case1SetOperators_PR_PR_SingleNodeScenario2_half1(boolean createIndex) throws Exception {
    Properties props = new Properties();
    Connection conn = TestUtil.getConnection(props);    

    SetQueriesDUnitHelper.createSchema(conn);
    setQueriesDUnitHelper.new CreateTable_withPK_PR_onCol_1().createTableFunction(conn, "orders1", 1);
    setQueriesDUnitHelper.new CreateTable_withPK_PR_onRange_ofPK_1().createTableFunction(conn, "orders2", 1);
    if (createIndex) {
      SetQueriesDUnitHelper.createLocalIndex(conn, "orders1", "vol");
      SetQueriesDUnitHelper.createLocalIndex(conn, "orders2", "vol");
    }
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders1", 0);
    SetQueriesDUnitHelper.loadSampleData(getLogWriter(), conn, 20, "orders2", 10);

    return conn;
  } /**/

  /*
   * Non-colocated
   * Partition on range-col + partition on pk
   * Redundancy 1 - all tables present on all nodes 
   */
  private void case1SetOperators_PR_PR_SingleNodeScenario2_half2(String queryType, 
      int expectedRows,
      String query1,
      String query2, 
      Connection conn) throws Exception {
    String execQuery = query1 + " " + queryType + " " + query2;
    getLogWriter().info("Execute Query: " + execQuery);
    ResultSet rs = conn.createStatement().executeQuery(execQuery);
    int rowCount = 0;
    Map<Integer, Integer> resultMap = new TreeMap<Integer, Integer>(); 
    while(rs.next()) {
      rowCount++;
      resultMap.put(rs.getInt(1), (resultMap.get(rs.getInt(1)) == null? 1 : resultMap.get(rs.getInt(1)) + 1));
    }
    rs.close();

    getLogWriter().info("expectedRows : " + expectedRows + " Got : " + rowCount );
    getLogWriter().info(resultMap.toString());
    assertEquals(expectedRows, rowCount);
  } /**/
  
  /*
   * Non-colocated
   * Partition on range-col + partition on pk
   * Redundancy 1 - all tables present on all nodes 
   */
  private void case1SetOperators_PR_PR_SingleNodeScenario2_half3(Connection conn) throws Exception {
    SetQueriesDUnitHelper.dropTable(conn, "orders2");
    SetQueriesDUnitHelper.dropTable(conn, "orders1");
    SetQueriesDUnitHelper.dropSchema(conn);
  } /**/
  
  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug42918() throws Exception {
    try {
      String query = "SELECT COUNT(ID1) FROM TESTTABLE1 UNION SELECT ID1 FROM TESTTABLE1 where ID1 = 0";

      startVMs(1, 3);
      String table1 = "create table TESTTABLE1 (ID1 int not null, "
          + " DESCRIPTION1 varchar(1024) not null, ADDRESS1 varchar(1024) not null ) ";

      clientSQLExecute(1, table1 + "  partition by column(ID1)");

      for (int i = 1; i < 4; ++i) {
        String insert = "Insert into  TESTTABLE1 values(" + i + ",'desc1_" + i
            + "','add1_" + i + "')";
        clientSQLExecute(1, insert);
      }

      try {
        TestUtil.getConnection().createStatement()
            .executeQuery(query);
        fail("Test should fail with feature not supported exception");
      } catch (SQLException sqle) {
        assertEquals("0A000", sqle.getSQLState());
      }
    } finally {
      clientSQLExecute(1, "drop table TESTTABLE1");
    }
  } /**/
  
  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug46711() throws Exception {
    try {
      String query1 = "select id, name from trade.t1 where type =21 and name like 'a%' " +
      		"union select id, name from trade.t1 where type =22 and name like 'a%'";
      String query2 = "select id, name from trade.t1 where type =21 and name = 'aa' " +
                "union select id, name from trade.t1 where type =22 and name = 'ab'";

      startVMs(1, 3);
      clientSQLExecute(1, "create table trade.t1 ( id int primary key, "
          + "name varchar(10), type int) partition by primary key");
      clientSQLExecute(1, "Insert into  trade.t1 values(1,'aa',21)");
      clientSQLExecute(1, "Insert into  trade.t1 values(2,'ab',22)");
      clientSQLExecute(1, "Insert into  trade.t1 values(3,'ac',23)");
      clientSQLExecute(1, "Insert into  trade.t1 values(4,'ad',24)");
      clientSQLExecute(1, "Insert into  trade.t1 values(5,'ae',25)");

      {//query1
        ResultSet rs46711 = TestUtil.getConnection().createStatement()
            .executeQuery(query1);
        int count = 1;
        while (rs46711.next()) {
          assertEquals(count, rs46711.getInt(1));
          count++;
        }
        assertEquals(3, count);
      } 
      
      {//query2
        ResultSet rs46711 = TestUtil.getConnection().createStatement()
            .executeQuery(query2);
        int count = 1;
        while (rs46711.next()) {
          assertEquals(count, rs46711.getInt(1));
          count++;
        }
        assertEquals(3, count);
      } 
      
    } finally {
      clientSQLExecute(1, "drop table trade.t1");
    }
  } /**/
  
  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug46899_replicated() throws Exception {
    try {
      String query1 = "select count(symbol) as symbolcount, exchange "
          + "from t1 where tid = 1 and symbol like '%b%' "
          + "group by exchange " + "union all "
          + "select count(symbol) as symbolcount, exchange "
          + "from t2 where tid = 11 group by exchange";
      String query2 = "select count(symbol) as symbolcount, exchange "
          + "from t1 where tid = 1 and symbol like '%b%' "
          + "group by exchange " + "union all "
          + "select count(symbol) as symbolcount, exchange "
          + "from t2 where tid = 11 group by exchange order by symbolcount";

      startVMs(1, 3);

      clientSQLExecute(1,
          "create table t1 (tid int, sid int, exchange int, symbol varchar(10)) "
              +
              // "partition by column(tid)" +
              "replicate" + "");
      clientSQLExecute(1,
          "create table t2 (tid int, sid int, exchange int, symbol varchar(10)) "
              +
              // "partition by column(tid)" +
              "replicate" + "");
      clientSQLExecute(1, "insert into t1 values (1,1,1,'abc')");
      clientSQLExecute(1, "insert into t1 values (1,1,1,'abcd')");
      clientSQLExecute(1, "insert into t2 values (11,11,11,'c')");
      clientSQLExecute(1, "insert into t2 values (11,11,11,'cd')");

      {// query1
        ResultSet rs46899 = TestUtil.getConnection().createStatement()
            .executeQuery(query1);
        int count = 0;
        while (rs46899.next()) {
          assertEquals(2, rs46899.getInt(1));
          count++;
        }
        assertEquals(2, count);
      }

      {// query2
        ResultSet rs46899 = TestUtil.getConnection().createStatement()
            .executeQuery(query2);
        int count = 0;
        while (rs46899.next()) {
          assertEquals(2, rs46899.getInt(1));
          count++;
        }
        assertEquals(2, count);
      }

    } finally {
      clientSQLExecute(1, "drop table t2");
      clientSQLExecute(1, "drop table t1");
    }
  } /**/
  
  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug46899_partitioned() throws Exception {
    try {
      String query1 = "select count(symbol) as symbolcount, exchange "
          + "from t1 where tid = 1 and symbol like '%b%' "
          + "group by exchange " + "union all "
          + "select count(symbol) as symbolcount, exchange "
          + "from t2 where tid = 11 group by exchange";
      String query2 = "select count(symbol) as symbolcount, exchange "
          + "from t1 where tid = 1 and symbol like '%b%' "
          + "group by exchange " + "union all "
          + "select count(symbol) as symbolcount, exchange "
          + "from t2 where tid = 11 group by exchange order by symbolcount";

      startVMs(1, 3);

      clientSQLExecute(1,
          "create table t1 (tid int, sid int, exchange int, symbol varchar(10)) "
              + "partition by column(tid)");
      clientSQLExecute(1,
          "create table t2 (tid int, sid int, exchange int, symbol varchar(10)) "
              + "partition by column(tid)");
      clientSQLExecute(1, "insert into t1 values (1,1,1,'abc')");
      clientSQLExecute(1, "insert into t1 values (1,1,1,'abcd')");
      clientSQLExecute(1, "insert into t2 values (11,11,11,'c')");
      clientSQLExecute(1, "insert into t2 values (11,11,11,'cd')");

      try {// query1
        TestUtil.getConnection().createStatement()
            .executeQuery(query1);
        fail("Test should fail with feature not supported exception");
      } catch (SQLException sqle) {
        assertEquals("0A000", sqle.getSQLState());
      }

      try {// query2
        TestUtil.getConnection().createStatement()
            .executeQuery(query2);
        fail("Test should fail with feature not supported exception");
      } catch (SQLException sqle) {
        assertEquals("0A000", sqle.getSQLState());
      }

    } finally {
      clientSQLExecute(1, "drop table t2");
      clientSQLExecute(1, "drop table t1");
    }
  } /**/
  
  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug46805_replicated() throws Exception {
    try {
      String query1 = "select * from t3";
      String query2 = "select * from t4";

      startVMs(1, 3);

      clientSQLExecute(1, "create table t1 (c1 int, c2 int) replicate");
      clientSQLExecute(1, "create table t2 (c1 int, c2 int) replicate");
      clientSQLExecute(1, "create table t3 (c1 int, c2 int) replicate");
      clientSQLExecute(1, "create table t4 (c1 int, c2 int) replicate");
      clientSQLExecute(1, "insert into t1 values (1,1)");
      clientSQLExecute(1, "insert into t1 values (2,2)");
      clientSQLExecute(1, "insert into t2 values (3,3)");
      clientSQLExecute(1, "insert into t2 values (4,4)");
      clientSQLExecute(1, "insert into t3 select * from t1 union select * from t2");
      clientSQLExecute(1, "insert into t4 select * from t1 union all select * from t2");
      


      {// query1
        HashSet<Integer> expected = new HashSet<Integer>();
        for (int i = 0; i < 4; ++i) {
          expected.add(i + 1);
        }

        ResultSet rs46805 = TestUtil.getConnection().createStatement()
            .executeQuery(query1);
        int count = 0;
        while (rs46805.next()) {
          assertTrue(expected.remove(rs46805.getInt(1)));
          count++;
        }
        assertEquals(4, count);
      }

      {// query2
        HashSet<Integer> expected = new HashSet<Integer>();
        for (int i = 0; i < 4; ++i) {
          expected.add(i + 1);
        }

        ResultSet rs46805 = TestUtil.getConnection().createStatement()
            .executeQuery(query2);
        int count = 0;
        while (rs46805.next()) {
          assertTrue(expected.remove(rs46805.getInt(1)));
          count++;
        }
        assertEquals(4, count);
      }

    } finally {
      clientSQLExecute(1, "drop table t2");
      clientSQLExecute(1, "drop table t1");
    }
  } /**/
  
  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug46805_partitioned() throws Exception {
    try {
      startVMs(1, 3);

      clientSQLExecute(1, "create table t1 (c1 int, c2 int) "
          + "partition by column(c1)" + "");
      clientSQLExecute(1, "create table t2 (c1 int, c2 int) "
          + "partition by column(c1)" + "");
      clientSQLExecute(1, "create table t3 (c1 int, c2 int) "
          + "partition by column(c1)" + "");
      clientSQLExecute(1, "create table t4 (c1 int, c2 int) "
          + "partition by column(c1)" + "");
      clientSQLExecute(1, "insert into t1 values (1,1)");
      clientSQLExecute(1, "insert into t1 values (2,2)");
      clientSQLExecute(1, "insert into t2 values (3,3)");
      clientSQLExecute(1, "insert into t2 values (4,4)");

      {
      	clientSQLExecute(1, "insert into t4 select * from t1 union all select * from t2");
        String query = "select * from t4";
        HashSet<Integer> expected = new HashSet<Integer>();
        for (int i = 0; i < 4; ++i) {
          expected.add(i + 1);
        }

        ResultSet rs46805 = TestUtil.getConnection().createStatement()
            .executeQuery("select * from t4");
        int count = 0;
        while (rs46805.next()) {
          assertTrue(expected.remove(rs46805.getInt(1)));
          count++;
        }
        assertEquals(4, count);
      }
      
      try {
      	clientSQLExecute(1, "insert into t4 select * from t1 union select * from t2");
        fail("Test should fail with feature not supported exception");
      } catch (SQLException sqle) {
        assertEquals("0A000", sqle.getSQLState());
      }

    } finally {
      clientSQLExecute(1, "drop table t2");
      clientSQLExecute(1, "drop table t1");
    }
  } /**/
  
  /*
   * Entity Framework integration issue
   */
  public void test45342() throws Exception {
    BufferedReader scriptReader = new BufferedReader(new FileReader(
        TestUtil.getResourcesDir()
            + "/lib/GemFireXD_EF_IntegrationQuery.sql"));
    StringBuilder qry = new StringBuilder();
    String qryLine;
    while ((qryLine = scriptReader.readLine()) != null) {
      qry.append(qryLine).append('\n');
    }

    startVMs(1, 3);
    try {
      clientSQLExecute(1, "create schema TESTAPP");
      clientSQLExecute(1,
          "CREATE TABLE TESTAPP.TESTTABLE(ID INT, NAME VARCHAR(50))");
      clientSQLExecute(1, "create schema TEMP");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMATABLES(TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), TABLE_NAME VARCHAR(50), " +
          "TABLE_TYPE VARCHAR(50)) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMACOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), ORDINAL_POSITION INTEGER, COLUMN_DEFAULT VARCHAR(50), " +
          "IS_NULLABLE VARCHAR(50), DATA_TYPE INTEGER, TYPE_NAME VARCHAR(50), CHARACTER_OCTET_LENGTH INTEGER, " +
          "COLUMN_SIZE INTEGER, SCALE INTEGER, PRECISION_RADIX INTEGER, IS_AUTOINCREMENT VARCHAR(50), " +
          "PRIMARY_KEY VARCHAR(50), EDM_TYPE VARCHAR(50))  replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMAVIEWS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50)) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMAINDEXES (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50), INDEX_NAME VARCHAR(50), INDEX_QUALIFIER VARCHAR(50), INDEX_TYPE VARCHAR(50), " +
          "COLUMN_NAME VARCHAR(50), SUNIQUE SMALLINT, SORT_TYPE VARCHAR(50), FILTER_CONDITION VARCHAR(50), " +
          "PRIMARY_KEY VARCHAR(50)) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMAINDEXCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50), INDEX_NAME VARCHAR(50), INDEX_QUALIFIER VARCHAR(50), INDEX_TYPE VARCHAR(50), " +
          "COLUMN_NAME VARCHAR(50), ORDINAL_POSITION SMALLINT, IS_NOT_UNIQUE SMALLINT, SORT_TYPE VARCHAR(50), " +
          "FILTER_CONDITION VARCHAR(50)) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMAFOREIGNKEYS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50), CONSTRAINT_NAME VARCHAR(50), PRIMARYKEY_TABLE_CATALOG VARCHAR(50), " +
          "PRIMARYKEY_TABLE_SCHEMA VARCHAR(50), PRIMARYKEY_TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), " +
          "PRIMARYKEY_COLUMN_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), DEFERRABILITY VARCHAR(50)) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMAFOREIGNKEYCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50), CONSTRAINT_NAME VARCHAR(50), PRIMARYKEY_TABLE_CATALOG VARCHAR(50), " +
          "PRIMARYKEY_TABLE_SCHEMA VARCHAR(50), PRIMARYKEY_TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), " +
          "PRIMARYKEY_COLUMN_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), ORDINAL_POSITION SMALLINT, " +
          "DEFERRABILITY VARCHAR(50)) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMAPRIMARYKEYCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), " +
          "ORDINAL_POSITION SMALLINT) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMACONSTRAINTCOLUMNS (TABLE_CATALOG VARCHAR(50), TABLE_SCHEMA VARCHAR(50), " +
          "TABLE_NAME VARCHAR(50), CONSTRAINT_NAME VARCHAR(50), PRIMARYKEY_TABLE_CATALOG VARCHAR(50), " +
          "PRIMARYKEY_TABLE_SCHEMA VARCHAR(50), PRIMARYKEY_TABLE_NAME VARCHAR(50), PRIMARYKEY_NAME VARCHAR(50), " +
          "PRIMARYKEY_COLUMN_NAME VARCHAR(50), COLUMN_NAME VARCHAR(50), ORDINAL_POSITION SMALLINT, " +
          "DEFERRABILITY VARCHAR(50)) replicate");
      clientSQLExecute(
          1,
          "CREATE TABLE TEMP.SCHEMASCHEMAS (SCHEMA_CATALOG VARCHAR(50), SCHEMA_NAME VARCHAR(50)) replicate");

      for (int i = 1; i < 4; ++i) {
        String insert = "Insert into TESTAPP.TESTTABLE values(" + i + ",'desc1_"  + i + "')";
        clientSQLExecute(1, insert);
      }

      ResultSet rs45342 = TestUtil.getConnection().createStatement()
          .executeQuery(qry.toString());
      assertFalse(rs45342.next());
    } finally {
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMASCHEMAS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMACONSTRAINTCOLUMNS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMAPRIMARYKEYCOLUMNS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMAFOREIGNKEYCOLUMNS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMAFOREIGNKEYS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMAINDEXCOLUMNS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMAINDEXES");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMAVIEWS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMACOLUMNS");
      clientSQLExecute(1, "drop table if exists TEMP.SCHEMATABLES");
      clientSQLExecute(1, "drop schema TEMP RESTRICT");
      clientSQLExecute(1, "drop table if exists TESTAPP.TESTTABLE");
      clientSQLExecute(1, "drop schema TESTAPP RESTRICT");
    }
  }/**/

  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug45876_replicated() throws Exception {
    try {
      startVMs(1, 3);

      clientSQLExecute(1, "create table t1 (c1 int, c2 int) replicate");
      clientSQLExecute(1, "create table t2 (c1 int, c2 int) replicate");
      clientSQLExecute(1, "insert into t1 values (1,1)");
      clientSQLExecute(1, "insert into t1 values (2,2)");
      clientSQLExecute(1, "insert into t2 values (3,3)");
      clientSQLExecute(1, "insert into t2 values (2,3)");
      clientSQLExecute(1, "insert into t2 values (4,4)");
      clientSQLExecute(1, "insert into t2 values (3,4)");

      {
        HashSet<Integer> expected = new HashSet<Integer>();
        expected.add(0);
        for (int i = 3; i < 5; ++i) {
          expected.add(i);
        }
        ResultSet rs = TestUtil
            .getConnection()
            .createStatement()
            .executeQuery(
                "SELECT 0, 2 from T1 UNION SELECT MAX(C1), C2 FROM T2 GROUP BY C2");
        int count = 0;
        while (rs.next()) {
          assertTrue(expected.remove(rs.getInt(1)));
          count++;
        }
        assertEquals(3, count);
      }

      {
        HashSet<Integer> expected = new HashSet<Integer>();
        for (int i = 1; i < 5; ++i) {
          expected.add(i);
        }
        ResultSet rs = TestUtil
            .getConnection()
            .createStatement()
            .executeQuery(
                "SELECT c1, c1 from T1 UNION SELECT MAX(C1), C1 FROM T2 GROUP BY C1");
        int count = 0;
        while (rs.next()) {
          assertTrue(expected.remove(rs.getInt(1)));
          count++;
        }
        assertEquals(4, count);
      }
    } finally {
      clientSQLExecute(1, "drop table t2");
      clientSQLExecute(1, "drop table t1");
    }
  } /**/

  /*
   * Defects
   * Related to Set Operators
   */
  public void testBug45876_partitioned() throws Exception {
    try {
      startVMs(1, 3);
  
      clientSQLExecute(1, "create table t1 (c1 int, c2 int) "
          + "partition by column(c1)" + "");
      clientSQLExecute(1, "create table t2 (c1 int, c2 int) "
          + "partition by column(c1)" + "");
      clientSQLExecute(1, "insert into t1 values (1,1)");
      clientSQLExecute(1, "insert into t1 values (2,2)");
      clientSQLExecute(1, "insert into t2 values (3,3)");
      clientSQLExecute(1, "insert into t2 values (2,3)");
      clientSQLExecute(1, "insert into t2 values (4,4)");
      clientSQLExecute(1, "insert into t2 values (3,4)");
  
      try {
        HashSet<Integer> expected = new HashSet<Integer>();
        expected.add(0);
        for (int i = 3; i < 5; ++i) {
          expected.add(i);
        }
        ResultSet rs = TestUtil
            .getConnection()
            .createStatement()
            .executeQuery(
                "SELECT 0, 2 from T1 UNION SELECT MAX(C1), C2 FROM T2 GROUP BY C2");
        int count = 0;
        while (rs.next()) {
          assertTrue(expected.remove(rs.getInt(1)));
          count++;
        }
        assertEquals(3, count);
        fail("Test should fail with feature not supported exception");
      } catch (SQLException sqle) {
        assertEquals("0A000", sqle.getSQLState());
      }
  
      try {
        HashSet<Integer> expected = new HashSet<Integer>();
        for (int i = 1; i < 5; ++i) {
          expected.add(i);
        }
        ResultSet rs = TestUtil
            .getConnection()
            .createStatement()
            .executeQuery(
                "SELECT c1, c1 from T1 UNION SELECT MAX(C1), C1 FROM T2 GROUP BY C1");
        int count = 0;
        while (rs.next()) {
          assertTrue(expected.remove(rs.getInt(1)));
          count++;
        }
        assertEquals(4, count);
        fail("Test should fail with feature not supported exception");
      } catch (SQLException sqle) {
        assertEquals("0A000", sqle.getSQLState());
      }
    } finally {
      clientSQLExecute(1, "drop table t2");
      clientSQLExecute(1, "drop table t1");
    }
  } /**/
}

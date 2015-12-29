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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

@SuppressWarnings("serial")
public class OuterJoinQueriesDUnit extends DistributedSQLTestBase {

  private final String table1ddl_one = "create table emp.EMPLOYEE(lastname varchar(30), depId int) " +
  		"partition by (depId)";
  private final String table2ddl_one = "create table emp.DEPT(deptname varchar(30), depId int) " +
  		"partition by (depId) colocate with (emp.EMPLOYEE)";
  
  private final String table1ddl_one_REP = "create table emp.EMPLOYEE(lastname varchar(30), depId int) " +
  "replicate";
  private final String table2ddl_one_PR = "create table emp.DEPT(deptname varchar(30), depId int) " +
  "partition by (depId)";
  private final String table2ddl_one_REP = "create table emp.DEPT(deptname varchar(30), depId int) " +
  "replicate";
  
  private final String empDepIdIndex = "create index edx on emp.EMPLOYEE(depId)";
  private final String deptDepIdIndex = "create index ddx on emp.DEPT(depId)";
  
  private final String bdgBidIndex = "create index bdx on bdg(bId)";
  private final String resBidIndex = "create index rdx on res(bid)";
  private final String domBidIndex = "create index dodx on dom(bid)";
  
  private void createFirstSetOfIndex() throws Exception {
    clientSQLExecute(1, empDepIdIndex);
    clientSQLExecute(1, deptDepIdIndex);
  }

  private void createSecondSetOfIndex() throws Exception {
    clientSQLExecute(1, bdgBidIndex);
    clientSQLExecute(1, resBidIndex);
    clientSQLExecute(1, domBidIndex);
  }
  
  private void dropFirstSetOfTables() throws Exception {
    clientSQLExecute(1, "drop table Emp.depT");
    clientSQLExecute(1, "drop table emp.emploYee");
  }
  
  private void dropSecondSetOfTables() throws Exception {
    clientSQLExecute(1, "drop table dom");
    clientSQLExecute(1, "drop table res");
    clientSQLExecute(1, "drop table bdg");
  }
  
  private final String[] ojQueries_one = new String[] { 
      "SELECT emp.Employee.LastName as lname, " +
      "emp.Employee.DepID as did1, " +
      "emp.Dept.DeptName as depname, " +
      "emp.Dept.DepID as did2" +
      "  FROM emp.employee  "
      + "LEFT OUTER JOIN emp.dept ON emp.employee.depID = emp.dept.DepID",
      //+ "where emp.employee.depID = emp.dept.DepID",
      
      "SELECT emp.Employee.LastName as lname, " +
      "emp.Employee.DepID as did1, " +
      "emp.Dept.DeptName as depname, " +
      "emp.Dept.DepID as did2" +
      "  FROM emp.employee  "
      + "RIGHT OUTER JOIN emp.dept ON emp.employee.depID = emp.dept.DepID"
      //+ "where emp.employee.depID = emp.dept.DepID"
      };
  
  private final String[] xmlOutPutTags_one = new String[] { "leftouterJoin_one", "rightouterJoin_one"};

  private final String goldenTextFile = TestUtil.getResourcesDir()
      + "/lib/checkQuery.xml";

  public OuterJoinQueriesDUnit(String name) {
    super(name);
  }

  public void insertRows() throws Exception {
    clientSQLExecute(1, "insert into emp.employee values " +
                "('Jones', 33), ('Rafferty', 31), " +
                "('Robinson', 34), ('Steinberg', 33), " +
                "('Smith', 34), ('John', null)");
    clientSQLExecute(1, "insert into emp.dept values ('sales', 31), " +
                "('engineering', 33), ('clerical', 34), ('marketing', 35)");
  }
  
  public void testOuterJoin_NonColocated() throws Exception {
    startVMs(1, 1);
    clientSQLExecute(1, "create schema emp");
    clientSQLExecute(1, "create table emp.EMPLOYEE(lastname varchar(30), depId int)");
    clientSQLExecute(1, "create table emp.DEPT(deptname varchar(30), depId int)");
    insertRows();
    for (int i = 0; i < ojQueries_one.length; i++) {
      String ojQuery1 = ojQueries_one[i];
      String opTag = xmlOutPutTags_one[i];
      try {
        sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      }
      catch(Exception ex) {
        getLogWriter().info("trace", ex);
      }
    }
  }
  
  public void testOuterJoin_1_PR_PR() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1, "create schema emp");
    for (int j = 0; j < 2; j++) {
      clientSQLExecute(1, table1ddl_one);
      clientSQLExecute(1, table2ddl_one);
      insertRows();
      if (j==1) {
        createFirstSetOfIndex();
      }
      for (int i = 0; i < ojQueries_one.length; i++) {
        String ojQuery1 = ojQueries_one[i];
        String opTag = xmlOutPutTags_one[i];
        sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      }
      dropFirstSetOfTables();
    }
  }

  public void testOuterJoin_1_RR_RR() throws Exception {
    startVMs(1, 3);
    for (int j = 0; j < 2; j++) {
      clientSQLExecute(1, table1ddl_one_REP);
      clientSQLExecute(1, table2ddl_one_REP);
      insertRows();
      if (j == 1) {
        createFirstSetOfIndex();
      }
      for (int i = 0; i < ojQueries_one.length; i++) {
        String ojQuery1 = ojQueries_one[i];
        String opTag = xmlOutPutTags_one[i];
        sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      }
      dropFirstSetOfTables();
    }
  }
  
  public void testOuterJoin_1_PR_RR() throws Exception {
    startVMs(1, 3);
    for (int j = 0; j < 2; j++) {
      clientSQLExecute(1, table1ddl_one);
      clientSQLExecute(1, table2ddl_one_REP);
      insertRows();
      if (j == 1) {
        createFirstSetOfIndex();
      }
      for (int i = 0; i < ojQueries_one.length; i++) {
        String ojQuery1 = ojQueries_one[i];
        String opTag = xmlOutPutTags_one[i];
        sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      }
      dropFirstSetOfTables();
    }
  }
  
  public void testOuterJoin_1_RR_PR() throws Exception {
    startVMs(1, 3);
    for (int j = 0; j < 2; j++) {
      clientSQLExecute(1, table1ddl_one_REP);
      clientSQLExecute(1, table2ddl_one_PR);
      insertRows();
      if (j == 1) {
        createFirstSetOfIndex();
      }
      for (int i = 0; i < ojQueries_one.length; i++) {
        String ojQuery1 = ojQueries_one[i];
        String opTag = xmlOutPutTags_one[i];
        sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      }
      dropFirstSetOfTables();
    }
  }
  
  // NPC is non partitioning column
  public void testOuterJoin_exceptionAsNPCJoinCriteria() throws Exception {
    startVMs(1, 1);
    clientSQLExecute(1, "create schema emp");
    clientSQLExecute(1, "create table emp.EMPLOYEE(lastname varchar(30), depId int) " +
        "partition by (lastname)");
    clientSQLExecute(1, "create table emp.DEPT(deptname varchar(30), depId int, lastname varchar(30)) " +
                "partition by (lastname) colocate with (emp.EMPLOYEE)");
    clientSQLExecute(1, "insert into emp.employee values " +
        "('Jones', 33), ('Rafferty', 31), " +
        "('Robinson', 34), ('Steinberg', 33), " +
        "('Smith', 34), ('John', null)");
    clientSQLExecute(1, "insert into emp.dept values ('sales', 31, 'ln1'), " +
        "('engineering', 33, 'ln2'), ('clerical', 34, 'ln3'), ('marketing', 35, 'ln4')");
    boolean gotex = false;
    try {
      String ojQuery1 = ojQueries_one[0];
      String opTag = xmlOutPutTags_one[0];
      sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      fail("expected an exception");
    } catch (Exception ex) {
      gotex = true;
      getLogWriter().info("trace", ex);
    }
    assertTrue(gotex);
    gotex = false;
    try {
      String ojQuery1 = ojQueries_one[1];
      String opTag = xmlOutPutTags_one[1];
      sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      fail("expected an exception");
    } catch (Exception ex) {
      gotex = true;
      getLogWriter().info("trace2", ex);
    }
    assertTrue(gotex);
  }
  
  //////////////////////////// 3 tables test ////////////////////////
  
  // IVC is Invalid Join condition - invalid because rid is not the
  // partitioning column.
  public void testOuterJoin_PR_PR_PR_But_IJC() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1, "create table bdg(name varchar(30), bid int not null) partition by (bid)");
    clientSQLExecute(1, "create table res(person varchar(30), bid int not null, rid int not null) " +
                "partition by (bid) colocate with (bdg)");
    clientSQLExecute(1, "create table dom(domain varchar(30), bid int not null, rid int not null) " +
                "partition by (bid) colocate with (bdg)");
    clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
    clientSQLExecute(1, "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
    clientSQLExecute(1, "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
    String ojQuery1 = "select * from " +
                "bdg left outer join " +
                "     res left outer join dom on res.rid = dom.rid " +
                "on bdg.bid = res.bid";
    String opTag = "threeTableOJ_PRPRPR";
    try {
      sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      fail("expected an exception as join condition is invalid");
    }
    catch(Exception ex) {
      // Ignore the exception as expected
    }
  }
  
  //ICC is Invalid colocate criteria
  public void testOuterJoin_PR_PR_PR_But_ICC() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1, "create table bdg(name varchar(30), bid int not null) partition by (bid)");
    clientSQLExecute(1, "create table res(person varchar(30), bid int not null, rid int not null) " +
                "partition by (bid) colocate with (bdg)");
    clientSQLExecute(1, "create table dom(domain varchar(30), bid int not null, rid int not null) " +
                "partition by (bid)");
    clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
    clientSQLExecute(1, "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
    clientSQLExecute(1, "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
    String ojQuery1 = "select * from " +
                "bdg left outer join " +
                "     res left outer join dom on res.rid = dom.rid " +
                "on bdg.bid = res.bid";
    String opTag = "threeTableOJ_PRPRPR";
    try {
      sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      fail("expected an exception as colocation is not satisfied");
    }
    catch(Exception ex) {
      // Ignore the exception as expected
    }
  }
  
  public void testOuterJoin_PR_PR_PR() throws Exception {
    startVMs(1, 3);
    for (int i = 0; i < 2; i++) {
      clientSQLExecute(1,
          "create table bdg(name varchar(30), bid int not null) partition by (bid)");
      clientSQLExecute(1,
          "create table res(person varchar(30), bid int not null, rid int not null) "
              + "partition by (bid) colocate with (bdg)");
      clientSQLExecute(1,
          "create table dom(domain varchar(30), bid int not null, rid int not null) "
              + "partition by (bid) colocate with (bdg)");
      clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
      clientSQLExecute(1,
          "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
      clientSQLExecute(
          1,
          "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
      if (i == 1) {
        createSecondSetOfIndex();
      }
      String ojQuery1 = "select * from " + "bdg left outer join "
          + "     res left outer join dom on res.bid = dom.bid "
          + "on bdg.bid = res.bid";
      String opTag = "threeTableOJ_PRPRPR";
      sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      dropSecondSetOfTables();
    }
  }
  
  public void testOuterJoin_RR_RR_RR() throws Exception {
    startVMs(1, 3);
    for (int i = 0; i < 2; i++) {
      clientSQLExecute(1,
          "create table bdg(name varchar(30), bid int not null) replicate");
      clientSQLExecute(1,
          "create table res(person varchar(30), bid int not null, rid int not null) "
              + "replicate");
      clientSQLExecute(1,
          "create table dom(domain varchar(30), bid int not null, rid int not null) "
              + "replicate");
      clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
      clientSQLExecute(1,
          "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
      clientSQLExecute(
          1,
          "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
      if (i == 1) {
        createSecondSetOfIndex();
      }
      String ojQuery1 = "select * from " + "bdg left outer join "
          + "     res left outer join dom on res.bid = dom.bid "
          + "on bdg.bid = res.bid";
      String opTag = "threeTableOJ_PRPRPR";
      sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      dropSecondSetOfTables();
    }
  }
  
  public void testOuterJoin_PR_RR_RR() throws Exception {
    startVMs(1, 3);
    for (int i = 0; i < 2; i++) {
      clientSQLExecute(1,
          "create table bdg(name varchar(30), bid int not null) partition by (bid)");
      clientSQLExecute(1,
          "create table res(person varchar(30), bid int not null, rid int not null) "
              + "replicate");
      clientSQLExecute(1,
          "create table dom(domain varchar(30), bid int not null, rid int not null) "
              + "replicate");
      clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
      clientSQLExecute(1,
          "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
      clientSQLExecute(
          1,
          "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
      if (i == 1) {
        createSecondSetOfIndex();
      }
      String ojQuery1 = "select * from " + "bdg left outer join "
          + "     res left outer join dom on res.bid = dom.bid "
          + "on bdg.bid = res.bid";
      String opTag = "threeTableOJ_PRPRPR";
      sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
      dropSecondSetOfTables();
    }
  }
  
  public void testOuterJoin_PR_PR_RR() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1, "create table bdg(name varchar(30), bid int not null) partition by (bid)");
    clientSQLExecute(1, "create table res(person varchar(30), bid int not null, rid int not null) " +
                "partition by (bid) colocate with (bdg)");
    clientSQLExecute(1, "create table dom(domain varchar(30), bid int not null, rid int not null) " +
                "replicate");
    clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
    clientSQLExecute(1, "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
    clientSQLExecute(1, "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
    String ojQuery1 = "select * from " +
                "bdg left outer join " +
                "     res left outer join dom on res.bid = dom.bid " +
                "on bdg.bid = res.bid";
    String opTag = "threeTableOJ_PRPRPR";
    sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
  }
  
  public void testOuterJoin_RR_PR() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1, "create schema emp");
    clientSQLExecute(1,
        "create table emp.EMPLOYEE(lastname varchar(30), depId int) "
            + "replicate");
    clientSQLExecute(1,
        "create table emp.DEPT(deptname varchar(30), depId int, lastname varchar(30)) "
            + "partition by (lastname)");
    createFirstSetOfIndex();
    clientSQLExecute(1, "insert into emp.employee values "
        + "('Jones', 33), ('Rafferty', 31), "
        + "('Robinson', 34), ('Steinberg', 33), "
        + "('Smith', 34), ('John', null)");
    clientSQLExecute(
        1,
        "insert into emp.dept values ('sales', 31, 'ln1'), "
            + "('engineering', 33, 'ln2'), ('clerical', 34, 'ln3'), ('marketing', 35, 'ln4')");

    String ojQuery1 = ojQueries_one[0];
    String opTag = xmlOutPutTags_one[0];
    sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
  }
  
  public void testOuterJoin_RR_PR_RR() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table bdg(name varchar(30), bid int not null) replicate");
    clientSQLExecute(1,
        "create table res(person varchar(30), bid int not null, rid int not null) "
            + "partition by (bid)");
    clientSQLExecute(1,
        "create table dom(domain varchar(30), bid int not null, rid int not null) "
            + "replicate");
    clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
    clientSQLExecute(1,
        "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
    createSecondSetOfIndex();
    clientSQLExecute(
        1,
        "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
    String ojQuery1 = "select * from " + "bdg left outer join "
        + "     res left outer join dom on res.bid = dom.bid "
        + "on bdg.bid = res.bid";
    String opTag = "threeTableOJ_PRPRPR";
    sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
  }

  public void testOuterJoin_RR_PR_RR_1() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table bdg(name varchar(30), bid int not null) replicate");
    clientSQLExecute(1,
        "create table res(person varchar(30), bid int not null, rid int not null) "
            + "partition by list (bid) ( values(0,2) )");
    clientSQLExecute(1,
        "create table dom(domain varchar(30), bid int not null, rid int not null) "
            + "replicate");
    clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
    clientSQLExecute(1,
        "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
    createSecondSetOfIndex();
    clientSQLExecute(
        1, "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
    
    String ojQuery1 = "select * from " + "bdg left outer join "
        + "     res left outer join dom on res.bid = dom.bid "
        + "on bdg.bid = res.bid";
    String opTag = "threeTableOJ_PRPRPR";
    sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
  }

  public void testOuterJoin_RR_PR_Bug() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table bdg(name varchar(30), bid int not null) replicate");
    clientSQLExecute(1,
        "create table res(person varchar(30), bid int not null, rid int not null) "
            + "partition by (bid)");
    clientSQLExecute(1,
        "create table dom(domain varchar(30), bid int not null, rid int not null) "
            + "replicate");
    clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
    clientSQLExecute(1,
        "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
    createSecondSetOfIndex();
    clientSQLExecute(
        1,
        "insert into dom values('www.grahamellis.co.uk', 1, 101), ('www.sheepbingo.co.uk', 2, 102 )");
    String ojQuery1 = "select * from " + "bdg left outer join "
        + "     res on bdg.bid = res.bid";
    String opTag = "twoTableOJ_RRPR";
    sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
  }
  
  public void testOuterJoin_RR_RR_PR() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table bdg(name varchar(30), bid int not null) replicate");
    clientSQLExecute(1,
        "create table res(person varchar(30), bid int not null, rid int not null) "
            + " replicate");
    clientSQLExecute(1,
        "create table dom(domain varchar(30), bid int not null, rid int not null) "
            +"partition by column(bid)");
    clientSQLExecute(1, "insert into bdg values('404', 1), ('405', 2)");
    clientSQLExecute(1,
        "insert into res values('graham', 1, 101), ('lisa', 1, 102)");
    String ojQuery1 = "select * from " + "bdg left outer join "
    + "     res left outer join dom on res.bid = dom.bid "
    + "on bdg.bid = res.bid";
    String opTag = "ThreeTableOJ_RR_RR_PR";
    sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
  }
  
  //Bug#50917
  public void testOuterJoin_RR_RR_PR2() throws Exception {
    Properties props = new Properties();
    //props.setProperty("gemfirexd.debug.true", 
    //    "QueryDistribution,TraceExecution,TraceOuterJoinMerge");
    
    startVMs(1, 1,0, null, props);
    startVMs(0, 2, 0, "SG2, SG3", props);
    
    clientSQLExecute(1,
        "create table trade.customers (cust_cid int not null, cust_name " +
        "varchar(100), cust_tid int, " +
        "primary key (cust_cid))  replicate");
    
    clientSQLExecute(1,
        "create table trade.portfolio (port_cid int not null, port_sid int " +
        "not null, port_qty int not null, port_tid int, constraint portf_pk " +
        "primary key (port_cid, port_sid), constraint cust_fk foreign key (port_cid) " +
        "references trade.customers (cust_cid) " +
        "on delete restrict " +
        ")  replicate");
    
    clientSQLExecute(1,
        "create table trade.sellorders (sell_oid int not null constraint " +
        "orders_pk primary key, sell_cid int, sell_sid int, sell_qty int" + 
        ") partition by column (sell_qty)  " +
        "SERVER GROUPS (SG2,SG3)");
    
    Connection conn = TestUtil.getConnection(props);
    PreparedStatement ps1 = conn.prepareStatement("insert into trade.customers" +
    		" values (?, ?, ?)");
    for (int i = 1; i <= 2; i++) {
      ps1.setInt(1, i); //cid
      ps1.setString(2, "name" + i); //cust_name
      ps1.setInt(3, i); //tid
      ps1.execute();
    }
    
    Statement st1 = conn.createStatement();
    st1.execute("insert into trade.portfolio " +
    "values(1, 1, 100, 1), (1, 2, 150, 1), (2, 2, 200, 1) ");
    
    Statement st2 = conn.createStatement();
    st2.execute("insert into trade.sellorders " +
        "values(1, 2, 2, 100)");
    
    PreparedStatement ps3 = conn
        .prepareStatement("insert into trade.sellorders" + " values (?, ?, ?, ?)");
    for (int i = 1000; i <= 2000; i++) {
      ps3.setInt(1, i); 
      ps3.setInt(2, i); 
      ps3.setInt(3, i); 
      ps3.setInt(4, i);
      ps3.execute();
    }
    
    String ojQuery1 = "select * from trade.customers c LEFT OUTER JOIN " +
    		"trade.portfolio f LEFT OUTER JOIN trade.sellorders so on " +
    		"f.port_cid = so.sell_cid on c.cust_cid= f.port_cid where " +
    		"f.port_tid = 1";
    Statement st3 = conn.createStatement();
    st3.execute(ojQuery1);
    
    String opTag = "ThreeTableOJ_RR_RR_PR2";
    TestUtil.verifyResults(true, st3, false, goldenTextFile, opTag);

    String ojQuery2 = "select * from (trade.customers c LEFT OUTER JOIN " +
      "trade.portfolio f on c.cust_cid= f.port_cid) LEFT OUTER JOIN trade.sellorders so on " +
      "f.port_cid = so.sell_cid where " +
      "f.port_tid = 1";

    Statement st4 = conn.createStatement();
    st4.execute(ojQuery2);
    TestUtil.verifyResults(true, st4, false, goldenTextFile, opTag);
  }
  
  public void testOuterJoin_RR_PR_PR() throws Exception {
    startVMs(1, 3);
    clientSQLExecute(1,
        "create table bdg(name varchar(30), bid int not null) replicate");
    clientSQLExecute(1,
        "create table res( person varchar(30), bid int not null, rid int not null) "
            + "partition by list (rid) (values(0,1), values(2,3) , values(4,7))");
    clientSQLExecute(1,
        "create table dom(domain varchar(30), bid int not null, rid int not null) "
            + "partition by list(rid) (values(0,1), values(2,3) , values(4,7) ) colocate with (res)" );
    //populate  bdg
    int k = 1;
    for(int i = 1; i <7;++i) {
      clientSQLExecute(1, "insert into bdg values('bdg"+i+"'," + (k++)+")");
      if(k ==4) {
        k =1;
      }
    }
    //populate res
    k =1;
    for(int i = 1; i <7;++i) {
      clientSQLExecute(1, "insert into res values('res"+i+"',"+(k++) +","+(i+100)+")");
      if(k == 4) {
        k = 1;
      }
    }
    //populate dom
    for(int i = 1; i <4;++i) {
      clientSQLExecute(1,"insert into dom values('dom"+i+"',"+(i+6) +","+(i+100)+")");
    }
    
   // createSecondSetOfIndex();
    
    String ojQuery1 = "select * from " + "bdg left outer join  res on bdg.bid = res.bid " +
    		" left outer join dom on res.rid = dom.rid";
    String opTag = "Three_tableOJ_RR_PR_PR";
    sqlExecuteVerify(new int[] { 1 }, null, ojQuery1, goldenTextFile, opTag);
  }
  
  public void testForumBugFromPgibb() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement stmnt = conn.createStatement();
    stmnt
        .execute("create table Customers(CustomerId integer not null generated always as identity, "
            + "CustomerCode varchar(8) not null, constraint PKCustomers primary key (CustomerId), "
            + "constraint UQCustomers unique (CustomerCode)) "
            + "partition by column (CustomerId) redundancy 1 persistent");

    stmnt.execute("insert into Customers (CustomerCode) values ('CC1')");
    stmnt.execute("insert into Customers (CustomerCode) values ('CC2')");
    stmnt.execute("insert into Customers (CustomerCode) values ('CC3')");
    stmnt.execute("insert into Customers (CustomerCode) values ('CC4')");

    stmnt
        .execute("create table Devices(DeviceId integer not null generated always as identity, "
            + "CustomerId integer not null, MACAddress varchar(12) not null, "
            + "constraint PKDevices primary key (DeviceId), "
            + "constraint UQDevices unique (CustomerId, MACAddress),"
            + " constraint FKDevicesCustomers foreign key (CustomerId) references Customers (CustomerId))"
            + " partition by column (CustomerId) colocate with (Customers) redundancy 1 persistent");

    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (1, '000000000001')");
    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (1, '000000000002')");
    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (2, '000000000001')");
    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (2, '000000000002')");

    String[] queries = new String[] {
        "select c.CustomerCode, count(d.DeviceId) from Customers c left outer join Devices d on c.CustomerId = d.CustomerId group by c.CustomerCode",
        "select c.*, d.DeviceId from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by c.CustomerId",
        "select c.*, d.DeviceId from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by d.DeviceId",
        "select c.*, d.MACAddress from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by c.CustomerId",
        "select c.*, d.MACAddress from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by d.DeviceId" };

    int[] numRows = new int[] {4, 6, 6, 6, 6 };
    
    for (int i = 0; i < queries.length; i++) {
      System.out.println("executing query(" + i + "): " + queries[i]);
      stmnt.execute(queries[i]);

      java.sql.ResultSet rs = stmnt.getResultSet();
      int numcols = rs.getMetaData().getColumnCount();
      int cnt = 0;
      System.out.println("--------------------------------------");
      while (rs.next()) {
        cnt++;
        String result = "";
        for (int j = 0; j < numcols; j++) {
          if (j == numcols - 1) {
            result += rs.getObject(j + 1);
          }
          else {
            result += rs.getObject(j + 1) + ", ";
          }
        }
        System.out.println(result);
      }
      assertEquals(numRows[i], cnt);
      System.out.println("--------------------------------------\n\n");
    }

    stmnt.execute("drop table Devices");
    stmnt.execute("drop table Customers");
  }
  
  public void testForumBugFromPgibb_RR_PR() throws Exception {
    startVMs(1, 0);
    startVMs(0, 3);

    Connection conn = TestUtil.getConnection();
    Statement stmnt = conn.createStatement();
    stmnt
        .execute("create table Customers(CustomerId integer not null generated always as identity, "
            + "CustomerCode varchar(8) not null, constraint PKCustomers primary key (CustomerId), "
            + "constraint UQCustomers unique (CustomerCode)) "
            + " replicate");

    stmnt.execute("insert into Customers (CustomerCode) values ('CC1')");
    stmnt.execute("insert into Customers (CustomerCode) values ('CC2')");
    stmnt.execute("insert into Customers (CustomerCode) values ('CC3')");
    stmnt.execute("insert into Customers (CustomerCode) values ('CC4')");

    stmnt
        .execute("create table Devices(DeviceId integer not null generated always as identity, "
            + "CustomerId integer not null, MACAddress varchar(12) not null, "
            + "constraint PKDevices primary key (DeviceId), "
            + "constraint UQDevices unique (CustomerId, MACAddress),"
            + " constraint FKDevicesCustomers foreign key (CustomerId) references Customers (CustomerId))"
            + " partition by column (CustomerId) redundancy 1 persistent");

    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (1, '000000000001')");
    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (1, '000000000002')");
    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (2, '000000000001')");
    stmnt
        .execute("insert into Devices (CustomerId, MACAddress) values (2, '000000000002')");

    String[] queries = new String[] {
        "select c.CustomerCode, count(d.DeviceId) from Customers c left outer join Devices d on c.CustomerId = d.CustomerId group by c.CustomerCode",
        "select c.*, d.DeviceId from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by c.CustomerId",
        "select c.*, d.DeviceId from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by d.DeviceId",
        "select c.*, d.MACAddress from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by c.CustomerId", //};//,
        "select c.*, d.MACAddress from Customers c left outer join Devices d on c.CustomerId = d.CustomerId order by d.DeviceId" };

    int[] numRows = new int[] {4, 6, 6, 6, 6 };
    
    for (int i = 0; i < queries.length; i++) {
      System.out.println("executing query(" + i + "): " + queries[i]);
      stmnt.execute(queries[i]);

      java.sql.ResultSet rs = stmnt.getResultSet();
      int numcols = rs.getMetaData().getColumnCount();
      int cnt = 0;
      System.out.println("--------------------------------------");
      while (rs.next()) {
        cnt++;
        String result = "";
        for (int j = 0; j < numcols; j++) {
          if (j == numcols - 1) {
            result += rs.getObject(j + 1);
          }
          else {
            result += rs.getObject(j + 1) + ", ";
          }
        }
        System.out.println(result);
      }
      assertEquals(numRows[i], cnt);
      System.out.println("--------------------------------------\n\n");
    }

    stmnt.execute("drop table Devices");
    stmnt.execute("drop table Customers");
  }
  
  public void testBug45380() throws Exception {
    startVMs(1, 0);
    startVMs(0, 3);

    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    {
      s.execute("create table trade.t1Rep ( id int primary key, "
          + "name varchar(10), type int) replicate");
      s.execute("Insert into  trade.t1Rep values(1,'a',21)");
      s.execute("Insert into  trade.t1Rep values(2,'b',22)");
      s.execute("Insert into  trade.t1Rep values(3,'c',23)");
      s.execute("Insert into  trade.t1Rep values(4,'d',24)");
      s.execute("Insert into  trade.t1Rep values(5,'e',25)");

      s.execute("create table trade.t2Rep ( id int primary key, "
          + "name varchar(10), type int) replicate");
      s.execute("Insert into  trade.t2Rep values(1,'a',21)");
      s.execute("Insert into  trade.t2Rep values(2,'b',22)");
      s.execute("Insert into  trade.t2Rep values(3,'c',23)");
      s.execute("Insert into  trade.t2Rep values(4,'d',24)");
      s.execute("Insert into  trade.t2Rep values(5,'e',25)");

      s.execute("create table trade.t1Par ( id int primary key, "
          + "name varchar(10), type int) partition by primary key");
      s.execute("Insert into  trade.t1Par values(1,'a',21)");
      s.execute("Insert into  trade.t1Par values(2,'b',22)");
      s.execute("Insert into  trade.t1Par values(3,'c',23)");
      s.execute("Insert into  trade.t1Par values(4,'d',24)");
      s.execute("Insert into  trade.t1Par values(5,'e',25)");

      s.execute("create table trade.t2Par ( id int primary key, "
          + "name varchar(10), type int) partition by primary key colocate with (trade.t1Par)");
      s.execute("Insert into  trade.t2Par values(1,'a',21)");
      s.execute("Insert into  trade.t2Par values(2,'b',22)");
      s.execute("Insert into  trade.t2Par values(3,'c',23)");
      s.execute("Insert into  trade.t2Par values(4,'d',24)");
      s.execute("Insert into  trade.t2Par values(5,'e',25)");
    }
    
    {
      // give all rows
      String query = "select t1.type, t1.id, t1.name from trade.t1Rep as t1 "
          + " left outer join trade.t2Rep as t2 on t1.id = t2.id "
          + "where case when t1.id is not null then 1 else -1 end "
          + "IN (?,?,?)";
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ps1.setInt(3, 3);
      ResultSet rs = ps1.executeQuery();
      int count = 0;
      while(rs.next()) {
        count++;
      }
      assertEquals(5, count);
    }
    
    {
      // give all rows
      String query = "select t1.type, t1.id, t1.name from trade.t1Par as t1 "
          + " left outer join trade.t2Par as t2 on t1.id = t2.id "
          + "where case when t1.id is not null then 1 else -1 end "
          + "IN (?,?,?)";
      PreparedStatement ps1 = conn.prepareStatement(query);
      ps1.setInt(1, 1);
      ps1.setInt(2, 2);
      ps1.setInt(3, 3);
      ResultSet rs = ps1.executeQuery();
      int count = 0;
      while(rs.next()) {
        count++;
      }
      assertEquals(5, count);
    }
  }
}

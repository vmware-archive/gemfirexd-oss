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
//
//  ViewDUnit.java
//  gemfire
//
//  Created by Eric Zoerner on 2009-10-12.

package com.pivotal.gemfirexd.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.snappydata.test.dunit.SerializableRunnable;
import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.ToursDBUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;

@SuppressWarnings("serial")
public class ViewDUnit extends DistributedSQLTestBase {
  
  public ViewDUnit(String name) {
    super(name);
  }
  
  public void testCreateView() throws Exception {
    startVMs(1, 4);

    Connection conn = TestUtil.getConnection();
    ToursDBUtil.createAndLoadToursDB(conn);
    
    Statement s = conn.createStatement();
    s.executeUpdate("CREATE VIEW ese "
                    + "AS SELECT city_name, country, language "
                    + "FROM cities WHERE language LIKE '%ese'");
    
    ResultSet rs = s.executeQuery("SELECT * FROM ese");
    assertEquals(7, JDBC.assertDrainResults(rs));
    
    conn.close();
  }

  public void testCreateViewEmp() throws Exception {
    startVMs(1, 4);

    Connection conn = TestUtil.getConnection();    
    Statement s = conn.createStatement();
    
    s.executeUpdate("create table emp (empno INTEGER, name  VARCHAR(128))");
    
    s.executeUpdate("CREATE VIEW empview AS SELECT * FROM emp");
    
    ResultSet rs = s.executeQuery("SELECT * FROM empview");
    assertEquals(0, JDBC.assertDrainResults(rs));
    
    conn.close();
  }
  
  public void testCreateViewJoinEmp() throws Exception {
    startVMs(1, 4);

    Connection conn = TestUtil.getConnection();    
    Statement s = conn.createStatement();
    
    s.executeUpdate("create table emp (empno INTEGER, name  VARCHAR(128))");
    s.execute("insert into emp values(1, 'name-1'), (2, 'name-2'), (3, 'name-3')");
    s.executeUpdate("CREATE VIEW empview AS SELECT * FROM emp");
    
    ResultSet rs = s.executeQuery("SELECT empno, name FROM empview where empno = 2");
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      assertEquals(2, rs.getInt(1));
      assertEquals("name-2", rs.getString(2));
    }
    assertEquals(1, cnt);
    conn.close();
  }

  public void testCreateViewJoinTable() throws Exception {
    startVMs(1, 4);

    Connection conn = TestUtil.getConnection();    
    Statement s = conn.createStatement();
    
    s.executeUpdate("create table emp (empno INTEGER, name  VARCHAR(128)) partition by column(empno)");
    s.execute("insert into emp values(1, 'name-1'), (2, 'name-2'), (3, 'name-3')");
    s.executeUpdate("CREATE VIEW empview AS SELECT * FROM emp");

    s.executeUpdate("create table emp2 "
        + "(empno1 INTEGER, name1  VARCHAR(128)) partition by column(empno1) "
        + "colocate with (emp)");
    s.execute("create synonym synForEmp2 for emp2");
    s.execute("insert into synForEmp2 values(10, 'name-1'), (2, 'name-2'), (30, 'name-3')");
    
    ResultSet rs = s.executeQuery("SELECT empno, empno1, name1 FROM empview, synForEmp2 where empno = empno1");
    int cnt = 0;
    while(rs.next()) {
      cnt++;
      assertEquals(2, rs.getInt(1));
      assertEquals(2, rs.getInt(2));
      assertEquals("name-2", rs.getString(3));
    }
    assertEquals(1, cnt);
    s.executeUpdate("create table emp4 (empno1 INTEGER, name  VARCHAR(128))");
    s.execute("create synonym synForEmpView for empview");
    try {
      rs = s
          .executeQuery("SELECT * FROM synForEmpView, emp4 where empno = empno1");
    } catch (SQLException ex) {
      assertEquals("0A000", ex.getSQLState());
    }
    s.execute("CReate table EMPLOYEE (COMM int not null, bonus int not null, name varchar(128))");
    s.execute("insert into EMPLOYEE values(10, 5, 'name-1'), (20, 5, 'name-2'), (30, 5, 'name-3')");
    s.executeUpdate("CREATE VIEW V1 (COL_SUM, COL_DIFF) AS SELECT COMM + BONUS, COMM - BONUS FROM EMPLOYEE");
    s.execute("select * from V1");
    rs = s.getResultSet();
    cnt = 0;
    while(rs.next()) {
      cnt++;
    }
    assertEquals(3, cnt);
    
    s.execute("CREATE VIEW EMP_RES (RESUME) AS VALUES "
        + "'Delores M. Quintana', 'Heather A. Nicholls', 'Bruce Adamson'");
    cnt = 0;
    s.execute("select * from EMP_RES");
    rs = s.getResultSet();
    while(rs.next()) {
      cnt++;
    }
    assertEquals(3, cnt);
    conn.close();
  }

  public void testColocatedJoinsWithGrouping_44616_44619_44621()
      throws Exception {

    startVMs(1, 3);

    final Connection conn = TestUtil.getConnection();
    final Statement stmt = conn.createStatement();

    // create the tables and view
    stmt.execute("create table p_test1 (idval int, dataval1 varchar(20)) "
        + "partition by column (idval)");
    stmt.execute("create table p_test2 (idval int, dataval2 varchar(20)) "
        + "partition by column (idval) colocate with (p_test1)");
    stmt.execute("create table p_test3 (idval int, dataval3 varchar(20)) "
        + "partition by column (idval) colocate with (p_test1)");
    stmt.execute("create view p_view1_2 (idval, dataval1, dataval2) as "
        + "select t1.idval, t1.dataval1, t2.dataval2 from "
        + "p_test1 t1, p_test2 t2 where t1.idval = t2.idval");

    // insert some data
    for (int id = 1; id <= 20; id++) {
      stmt.execute("insert into p_test1 values (" + id + ", 'd1_" + id + "')");
      stmt.execute("insert into p_test2 values (" + id + ", 'd2_" + id + "')");
      stmt.execute("insert into p_test3 values (" + id + ", 'd3_" + id + "')");
    }

    ResultSet rs = stmt.executeQuery("select v1.idval, v1.dataval1, "
        + "v1.dataval2, t3.dataval3 from p_view1_2 v1, p_test3 t3 "
        + "where v1.idval = t3.idval order by v1.idval");
    for (int id = 1; id <= 20; id++) {
      assertTrue(rs.next());
      assertEquals(id, rs.getInt(1));
    }
    assertFalse(rs.next());

    // next the more complex case in #44621
    stmt.execute("create table trade.customers (cid int not null, "
        + "cust_name varchar(100), since date, addr varchar(100), tid int, "
        + "primary key (cid))  partition by range (cid) "
        + "( VALUES BETWEEN 0 AND 999, VALUES BETWEEN 1000 AND 1999, "
        + "VALUES BETWEEN 2000 AND 2999, VALUES BETWEEN 3000 AND 3999, "
        + "VALUES BETWEEN 4000 AND 100000)");
    stmt.execute("create table trade.networth (cid int not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), "
        + "loanlimit int, availloan decimal (30, 20),  tid int, "
        + "constraint netw_pk primary key (cid), constraint cust_newt_fk "
        + "foreign key (cid) references trade.customers (cid) on delete "
        + "restrict, constraint cash_ch check (cash>=0), constraint sec_ch check "
        + "(securities >=0), constraint availloan_ck check (loanlimit>=availloan "
        + "and availloan >=0))  partition by range (cid) "
        + "( VALUES BETWEEN 0 AND 999, VALUES BETWEEN 1000 AND 1999, "
        + "VALUES BETWEEN 2000 AND 2999, VALUES BETWEEN 3000 AND 3999, "
        + "VALUES BETWEEN 4000 AND 100000) colocate with (trade.customers)");
    stmt.execute("create table emp.employees (eid int not null constraint "
        + "employees_pk primary key, emp_name varchar(100), since date, "
        + "addr varchar(100), ssn varchar(9))  replicate");
    stmt.execute("create table trade.trades (tid int, cid int, eid int, "
        + "tradedate date, primary Key (tid), foreign key (cid) "
        + "references trade.customers (cid), constraint emp_fk "
        + "foreign key (eid) references emp.employees (eid))  "
        + "partition by range (cid) ( VALUES BETWEEN 0 AND 999, "
        + "VALUES BETWEEN 1000 AND 1999, VALUES BETWEEN 2000 AND 2999, "
        + "VALUES BETWEEN 3000 AND 3999, VALUES BETWEEN 4000 AND 100000) "
        + "colocate with (trade.customers)");

    // view in #44619
    stmt.execute("create view trade.cust_count_since2010_vw (since_date, "
        + "cust_count) as select since, count(*) from trade.customers "
        + "group by since having since > '2010-01-01'");
    stmt.execute("create view trade.cust_tradeCount_with5KNetworth_vw "
        + "(cid, name, cash, trade_count) as select t1.cid, t1.cust_name, "
        + "t2.cash, count(t3.tradedate) from trade.customers t1, "
        + "trade.networth t2, trade.trades t3 where t1.cid = t2.cid and "
        + "t2.cid = t3.cid group by t1.cid, t1.cust_name, t2.cash "
        + "having t2.cash > 5000");

    // insert some data
    for (int id = 1; id <= 20; id++) {
      int year = (id / 2) + 2;
      stmt.execute("insert into trade.customers (cid, cust_name, since) "
          + "values (" + id + ", 'n_" + id + "', '20"
          + (year < 10 ? ("0" + year) : year) + "-01-01')");
      stmt.execute("insert into trade.networth (cid, cash, loanlimit) values ("
          + id + ", " + (((id % 4) + 1) * 2000) + ", " + (id * 1000) + ")");
      stmt.execute("insert into emp.employees (eid, emp_name) values (" + id
          + ", 'n_" + id + "')");
      stmt.execute("insert into trade.trades (tid, cid, eid, tradedate) "
          + "values (" + (id * 2) + ", " + id + ", " + id + ", '2010-01-01')");
    }

    // for #44619
    try {
      rs = stmt.executeQuery("select * from trade.cust_count_since2010_vw "
          + "where cust_count = 2");
      fail("Test should fail with feature not supported exception");
    } catch (SQLException sqle) {
      assertEquals("0A000", sqle.getSQLState());
    }

    // for #44621
    try {
      rs = stmt.executeQuery("select t1.eid, t1.emp_name, v1.trade_count from "
          + "emp.employees t1, trade.cust_tradeCount_with5KNetworth_vw v1 "
          + "where t1.emp_name = v1.name");
      fail("Test should fail with feature not supported exception");
    } catch (SQLException sqle) {
      assertEquals("0A000", sqle.getSQLState());
    }
    
    // for #45083
    stmt.execute("create view trade.cust_trade_vw (cid, name, tradedate) as " +
                "select t1.cid, t1.cust_name, t2.tradedate from " +
                "trade.customers t1, trade.trades t2 where t1.cid = t2.cid");
    stmt.execute("create view trade.cust_networth_trade_vw (cid, name, tradedate, cash) " +
                "as select v1.cid, v1.name, v1.tradedate, t1.cash from trade.cust_trade_vw v1, " +
                "trade.networth t1 where v1.cid = t1.cid");

    rs = stmt.executeQuery("select count(cid) total_customers, sum(cash) total_cash from " +
                "trade.cust_networth_trade_vw where tradedate > '2007-01-01' group by name " +
                "having sum(cash) > 5000");
    int count = 0;
    while (rs.next()) {
      assertEquals(1, rs.getInt(1));
      count++;
    }
    assertEquals(10, count);
  }

  public void testColocatedJoinsWithGrouping_45416_44619_44621_replicated()
      throws Exception {

    startVMs(1, 3);

    final Connection conn = TestUtil.getConnection();
    final Statement stmt = conn.createStatement();

    stmt.execute("create table trade.customers (cid int not null, "
        + "cust_name varchar(100), since date, addr varchar(100), tid int, "
        + "primary key (cid))  replicate");
    stmt.execute("create table trade.networth (cid int not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), "
        + "loanlimit int, availloan decimal (30, 20),  tid int, "
        + "constraint netw_pk primary key (cid), constraint cust_newt_fk "
        + "foreign key (cid) references trade.customers (cid) on delete "
        + "restrict, constraint cash_ch check (cash>=0), constraint sec_ch check "
        + "(securities >=0), constraint availloan_ck check (loanlimit>=availloan "
        + "and availloan >=0))  replicate");
    stmt.execute("create table emp.employees (eid int not null constraint "
        + "employees_pk primary key, emp_name varchar(100), since date, "
        + "addr varchar(100), ssn varchar(9))  replicate");
    stmt.execute("create table trade.trades (tid int, cid int, eid int, "
        + "tradedate date, primary Key (tid), foreign key (cid) "
        + "references trade.customers (cid), constraint emp_fk "
        + "foreign key (eid) references emp.employees (eid))  "
        + "replicate");

    // view in #44619
    stmt.execute("create view trade.cust_count_since2010_vw (since_date, "
        + "cust_count) as select since, count(*) from trade.customers "
        + "group by since having since > '2010-01-01'");
    stmt.execute("create view trade.cust_tradeCount_with5KNetworth_vw "
        + "(cid, name, cash, trade_count) as select t1.cid, t1.cust_name, "
        + "t2.cash, count(t3.tradedate) from trade.customers t1, "
        + "trade.networth t2, trade.trades t3 where t1.cid = t2.cid and "
        + "t2.cid = t3.cid group by t1.cid, t1.cust_name, t2.cash "
        + "having t2.cash > 5000");

    // insert some data
    for (int id = 1; id <= 20; id++) {
      int year = (id / 2) + 2;
      stmt.execute("insert into trade.customers (cid, cust_name, since) "
          + "values (" + id + ", 'n_" + id + "', '20"
          + (year < 10 ? ("0" + year) : year) + "-01-01')");
      stmt.execute("insert into trade.networth (cid, cash, loanlimit) values ("
          + id + ", " + (((id % 4) + 1) * 2000) + ", " + (id * 1000) + ")");
      stmt.execute("insert into emp.employees (eid, emp_name) values (" + id
          + ", 'n_" + id + "')");
      stmt.execute("insert into trade.trades (tid, cid, eid, tradedate) "
          + "values (" + (id * 2) + ", " + id + ", " + id + ", '2010-01-01')");
    }
    
    // for #44619
    {
     ResultSet rs = stmt.executeQuery("select * from trade.cust_count_since2010_vw "
          + "where cust_count = 2");
      assertTrue(rs.next());
      assertEquals("2011-01-01", rs.getString(1));
      assertFalse(rs.next());
      
      rs = stmt.executeQuery("select * from trade.cust_count_since2010_vw "
          + "where cust_count = 1");
      assertTrue(rs.next());
      assertEquals("2012-01-01", rs.getString(1));
      assertFalse(rs.next());
    }

    // Existing test for #44621
    /* Assertion Failure issue, tracked by 45416*/
    {
      ResultSet rs = stmt
          .executeQuery("select t1.eid, t1.emp_name, v1.trade_count from "
              + "emp.employees t1, trade.cust_tradeCount_with5KNetworth_vw v1 "
              + "where t1.emp_name = v1.name");
      for (int id = 2; id <= 20; id += 4) {
        assertTrue(rs.next());
        assertEquals(id, rs.getInt(1));
        assertEquals("n_" + id, rs.getString(2));
        assertTrue(rs.next());
        assertEquals(id + 1, rs.getInt(1));
        assertEquals("n_" + (id + 1), rs.getString(2));
      }
      assertFalse(rs.next());
    }/**/
    
    // create more views
    stmt.execute("create view trade.cust_trade_vw (cid, name, tradedate) as " +
                "select t1.cid, t1.cust_name, t2.tradedate from " +
                "trade.customers t1, trade.trades t2 where t1.cid = t2.cid");
    stmt.execute("create view trade.cust_networth_trade_vw (cid, name, tradedate, cash) " +
                "as select v1.cid, v1.name, v1.tradedate, t1.cash from trade.cust_trade_vw v1, " +
                "trade.networth t1 where v1.cid = t1.cid");

    { // for #45083
      ResultSet rs = stmt
          .executeQuery("select count(cid) total_customers, sum(cash) total_cash from "
              + "trade.cust_networth_trade_vw where tradedate > '2007-01-01' group by name "
              + "having sum(cash) > 5000");
      int count = 0;
      while (rs.next()) {
        assertEquals(1, rs.getInt(1));
        count++;
      }
      assertEquals(10, count);
    }
    
    { // for #45416
      PreparedStatement c_ps = conn
          .prepareStatement("select count(cid) total_customers, sum(cash) total_cash from "
              + "trade.cust_networth_trade_vw where tradedate > '2007-01-01' group by name "
              + "having sum(cash) > ?");
      c_ps.setInt(1, 5000);
      ResultSet rs = c_ps.executeQuery();
      int count = 0;
      while (rs.next()) {
        assertEquals(1, rs.getInt(1));
        count++;
      }
      assertEquals(10, count);
    }
    
    { // for #45416, verify avg
      PreparedStatement c_ps = conn
          .prepareStatement("select count(cid) total_customers, avg(cash) total_cash from "
              + "trade.cust_networth_trade_vw where tradedate > '2007-01-01' group by name "
              + "having sum(cash) > ?");
      c_ps.setInt(1, 5000);
      ResultSet rs = c_ps.executeQuery();
      int count = 0;
      while (rs.next()) {
        assertEquals(1, rs.getInt(1));
        count++;
      }
      assertEquals(10, count);
    }
  }
  
  // make sure that region size optimization is applied for query on views
  public void test51466_1() throws Exception {
    startVMs(1, 2);
    Connection conn = TestUtil.getConnection();
    
    Statement st = conn.createStatement();

    st.execute("create table test (col1 int, col2 int, col3 int) partition " +
    		"by range (col1)(VALUES BETWEEN 0 AND 3, VALUES BETWEEN 3 AND 5)");
    st.execute("create view test_v1 as select * from test");
    st.execute("create view test_v2 as select * from test_v1");
    
    st.execute("insert into test values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)");
    
    //observer1: make sure that the region size optimization is triggered
    SerializableRunnable csr1 = new SerializableRunnable(
        "set observer") {
      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private static final long serialVersionUID = 1L;

              @Override
              public void regionSizeOptimizationTriggered2(
                  com.pivotal.gemfirexd.internal.impl.sql.compile.SelectNode selectNode) {
                if (selectNode.hasAggregatesInSelectList()) {
                  getLogWriter().info(
                      "csr1 regionSizeOptimizationTriggered2 called");
                  assertTrue(selectNode.convertCountToRegionSize());
                } else {
                  getLogWriter().info(
                      "csr1 regionSizeOptimizationTriggered2 called...no aggregates " +
                      "in the select");
                }
              }
            });
      }
    };
    serverExecute(1, csr1);
    serverExecute(2, csr1);
//    serverExecute(3, csr1);
    
    ResultSet rs = st.executeQuery("select count(*) from test_v1");
    rs.next();
    assertEquals(5, rs.getInt(1));
    
    rs = st.executeQuery("select count(*) from test_v2");
    rs.next();
    assertEquals(5, rs.getInt(1));
    
    rs = st.executeQuery("select count(*) from (select * from test) as t");
    rs.next();
    assertEquals(5, rs.getInt(1));
  }
 
  // make sure that the region size optimization is *not* triggered
  // for the queries in the test
  public void test51466_2() throws Exception {
    startVMs(1, 2);
    Connection conn = TestUtil.getConnection();

    Statement st = conn.createStatement();

    st.execute("create table test1 (c1_1 int, c1_2 int, c1_3 int) " +
    		"partition by (c1_1)");
    st.execute("insert into test1 values(1,1,1), (2,2,2), (3,3,3), " +
    		"(4,4,4), (5,5,5)");

    //with where clause
    st.execute("create view test_v3 as select * from test1 where c1_1 > 0");
    
    st.execute("create table test2 (c2_1 int, c2_2 int, c2_3 int) partition " +
    		"by (c2_1) colocate with (test1)");
    st.execute("insert into test2 values(1,1,1), (2,2,2), (3,3,3), " +
    		"(4,4,4), (5,5,5)");
    //join
    st.execute("create view test_v4 as select test1.c1_1,test2.c2_1 from " +
    		"test1 join test2 on test1.c1_1=test2.c2_1");
    st.execute("create view test_v5 as select test1.c1_1, test2.c2_1 from " +
        "test1, test2 where test1.c1_1=test2.c2_1");
    //union
    st.execute("create table test3 (col1 int, col2 int, col3 int) replicate");
    st.execute("create table test4 (col1 int, col2 int, col3 int) replicate");
    st.execute("insert into test3 values(1,1,1), (2,2,2), (3,3,3), " +
        "(4,4,4), (5,5,5)");
    st.execute("insert into test4 values(1,1,1), (2,2,2), (3,3,3), " +
        "(4,4,4), (5,5,5)");
    st.execute("create view test_v6 as select test3.col1 from " +
        "test3 union all select test4.col1 from test4");
    

    // observer2: make sure that the region size optimization is *not* triggered
    SerializableRunnable csr2 = new SerializableRunnable(
        "set observer") {
      @Override
      public void run() {
        GemFireXDQueryObserver old = GemFireXDQueryObserverHolder
            .setInstance(new GemFireXDQueryObserverAdapter() {
              private static final long serialVersionUID = 1L;

              @Override
              public void regionSizeOptimizationTriggered2(
                  com.pivotal.gemfirexd.internal.impl.sql.compile.SelectNode selectNode) {
                getLogWriter().info(
                    "csr2 regionSizeOptimizationTriggered2 called");
                assertFalse(selectNode.convertCountToRegionSize());
              }
            });
      }
    };
    serverExecute(1, csr2);
    serverExecute(2, csr2);
//    serverExecute(3, csr2);

    ResultSet rs2 = st.executeQuery("select count(*) from test_v3");
    rs2.next();
    assertEquals(5, rs2.getInt(1));
    rs2.close();
    
    ResultSet rs3 = st.executeQuery("select count(*) from test_v4");
    rs3.next();
    assertEquals(5, rs3.getInt(1));
    rs3.close();
    
    ResultSet rs4 = st.executeQuery("select count(*) from test_v5");
    rs4.next();
    assertEquals(5, rs4.getInt(1));
    rs4.close();
    
    ResultSet rs5 = st.executeQuery("select count(*) from test_v6");
    rs5.next();
    assertEquals(10, rs5.getInt(1));
    rs5.close();
    
  }
}

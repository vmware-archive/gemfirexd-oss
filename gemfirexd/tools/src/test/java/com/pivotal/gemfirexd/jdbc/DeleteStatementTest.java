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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import static junit.framework.Assert.*;

import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;

/**
 * The junit test for the delete statement.
 * @author yjing
 *
 */

public class DeleteStatementTest extends JdbcTestBase {

  public DeleteStatementTest(String name) {
    super(name);

  }

  public void testDeleteRule() throws SQLException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid))");
    try {
      Monitor.getStream().println(
          "<ExpectedException action=add>"
              + "java.sql.SQLSyntaxErrorException"
              + "</ExpectedException>");
       Monitor.getStream().flush();
       
       Monitor.getStream().println("<ExpectedException action=add>"
           + "java.sql.SQLException"
           + "</ExpectedException>");
       Monitor.getStream().flush();
      s
          .execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
              + "securities decimal (30, 20), loanlimit int, "
              + "availloan decimal (30, 20),  tid int,"
              + "constraint netw_pk primary key (cid), "
              + "constraint cash_ch check (cash>=0), "
              + "constraint sec_ch check (securities >=0),"
              + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete cascade, "
              + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");
      assertTrue("Exception is expected!", false);

    }
    catch (Exception e) {
      Monitor.getStream().println("<ExpectedException action=remove>"
          + "java.sql.SQLException"
          + "</ExpectedException>");
      Monitor.getStream().flush();
      
      Monitor.getStream().println(
          "<ExpectedException action=remove>"
              + "java.sql.SQLSyntaxErrorException"
              + "</ExpectedException>");
       Monitor.getStream().flush();
       
     

    }
    try {
      Monitor.getStream().println(
          "<ExpectedException action=add>"
              + "java.sql.SQLSyntaxErrorException"
              + "</ExpectedException>");
       Monitor.getStream().flush();
       
       Monitor.getStream().println("<ExpectedException action=add>"
           + "java.sql.SQLException"
           + "</ExpectedException>");
       Monitor.getStream().flush();
      s
          .execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
              + "securities decimal (30, 20), loanlimit int, "
              + "availloan decimal (30, 20),  tid int,"
              + "constraint netw_pk primary key (cid), "
              + "constraint cash_ch check (cash>=0), "
              + "constraint sec_ch check (securities >=0),"
              + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete set null, "
              + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");
      assertTrue("Exception is expected!", false);

    }
    catch (Exception e) {
      Monitor.getStream().println("<ExpectedException action=remove>"
          + "java.sql.SQLException"
          + "</ExpectedException>");
      Monitor.getStream().flush();
      
      Monitor.getStream().println(
          "<ExpectedException action=remove>"
              + "java.sql.SQLSyntaxErrorException"
              + "</ExpectedException>");
      Monitor.getStream().flush();

    }
    
  
    s.execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
              + "securities decimal (30, 20), loanlimit int, "
              + "availloan decimal (30, 20),  tid int,"
              + "constraint netw_pk primary key (cid), "
              + "constraint cash_ch check (cash>=0), "
              + "constraint sec_ch check (securities >=0),"
              + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, "
              + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");    
   s.execute("DROP TABLE trade.networth");
   
   s.execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
       + "securities decimal (30, 20), loanlimit int, "
       + "availloan decimal (30, 20),  tid int,"
       + "constraint netw_pk primary key (cid), "
       + "constraint cash_ch check (cash>=0), "
       + "constraint sec_ch check (securities >=0),"
       + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete no action, "
       + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");    
  s.execute("DROP TABLE trade.networth");

  s.execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
      + "securities decimal (30, 20), loanlimit int, "
      + "availloan decimal (30, 20),  tid int,"
      + "constraint netw_pk primary key (cid), "
      + "constraint cash_ch check (cash>=0), "
      + "constraint sec_ch check (securities >=0),"
      + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid), "
      + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");    
 s.execute("DROP TABLE trade.networth");
   
  }

  public void testDelete() throws SQLException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid))");
    s
        .execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0)," 
        /*    +"constraint cust_newt_fk foreign key (cid) references trade.customers (cid), "*/ 
            +"constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");
    s
        .execute("INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    s
        .execute("INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    s
        .execute("INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    s
        .execute("INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    s
        .execute("INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    s
        .execute("INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    s
        .execute("INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    s.executeUpdate("DELETE FROM trade.customers WHERE cid>10");

    ResultSet rs1 = s.executeQuery("SELECT * FROM trade.customers");
    JDBC.assertDrainResults(rs1, 1);

    // ResultSet rs = s.executeQuery("SELECT * FROM trade.networth where cid=10");
    // JDBC.assertDrainResults(rs, 0);

  }

  public void _testDeleteWithNoAction() throws SQLException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid))");
    s
        .execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete NO ACTION, "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");
    s
        .execute("INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    s
        .execute("INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    s
        .execute("INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    s
        .execute("INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    s
        .execute("INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");

    s
        .execute("INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    boolean gotExpectedException = false;
    try {

      Monitor.getStream().println(
          "<ExpectedException action=add>"
              + "java.sql.SQLIntegrityConstraintViolationException"
              + "</ExpectedException>");
      Monitor.getStream().flush();

      s.execute("DELETE FROM trade.customers WHERE cid=10");
    }
    catch (Exception e) {
      gotExpectedException = true;
    }
    finally {
      Monitor.getStream().println(
          "<ExpectedException action=remove>"
              + "java.sql.SQLIntegrityConstraintViolationException"
              + "</ExpectedException>");
      Monitor.getStream().flush();
    }

    if (!gotExpectedException) {
      throw new SQLException("Test failed");
    }
    ResultSet rs = s.executeQuery("SELECT * FROM trade.networth where cid=10");
    JDBC.assertDrainResults(rs, 1);

    ResultSet rs1 = s
        .executeQuery("SELECT * FROM trade.customers where cid=10");
    JDBC.assertDrainResults(rs1, 0);
  }

  public void _testDeleteWithRestrict() throws SQLException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s
        .execute("create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid))");
    s
        .execute("create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete RESTRICT, "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");
    s
        .execute("INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    s
        .execute("INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    s
        .execute("INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    s
        .execute("INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    s
        .execute("INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");

    s
        .execute("INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    s
        .execute("INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    boolean gotExpectedException = false;
    try {

      Monitor.getStream().println(
          "<ExpectedException action=add>"
              + "java.sql.SQLIntegrityConstraintViolationException"
              + "</ExpectedException>");
      Monitor.getStream().flush();

      /*    Monitor.getStream().println("<ExpectedException action=add>"
              + "com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager.StandardGfxdIndexException"
              + "</ExpectedException>");
          Monitor.getStream().flush(); */

      s.execute("DELETE FROM trade.customers WHERE cid=10");
    }
    catch (Exception e) {
      gotExpectedException = true;
    }
    finally {
      Monitor.getStream().println(
          "<ExpectedException action=remove>"
              + "java.sql.SQLIntegrityConstraintViolationException"
              + "</ExpectedException>");
      Monitor.getStream().flush();
    }

    if (!gotExpectedException) {
      throw new SQLException("Test failed");
    }
    ResultSet rs = s.executeQuery("SELECT * FROM trade.networth where cid=10");
    JDBC.assertDrainResults(rs, 1);

    ResultSet rs1 = s
        .executeQuery("SELECT * FROM trade.customers where cid=10");
    JDBC.assertDrainResults(rs1, 1);
  }

}

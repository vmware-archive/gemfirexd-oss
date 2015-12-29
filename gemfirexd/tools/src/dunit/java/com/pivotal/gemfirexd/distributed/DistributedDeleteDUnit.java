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
package com.pivotal.gemfirexd.distributed;

import java.sql.SQLException;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

@SuppressWarnings("serial")
public class DistributedDeleteDUnit extends DistributedSQLTestBase {

  public DistributedDeleteDUnit(String name) {
    super(name);
  }

  public void testDeleteOnTableWithoutReferencedKey() throws Exception {

    startVMs(1, 2);

    //Create a table
    clientSQLExecute(1,
        "create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid))");
    clientSQLExecute(
        1,
        "create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid>10");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_1",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    // "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete cascade, " +
    //   s.executeUpdate("DELETE FROM trade.customers WHERE cid=60");

    // ResultSet rs1 = s.executeQuery("SELECT * FROM trade.customers");
    // JDBC.assertDrainResults(rs1, 5);
  }

  public void testDeleteOnTableWithoutReferencedKeyAndDifferentPartitionStrategy()
      throws Exception {

    startVMs(1, 2);

    //Create a table
    clientSQLExecute(1,
        "create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid)) REPLICATE");
    clientSQLExecute(
        1,
        "create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0)) PARTITION BY PRIMARY KEY");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid>10");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_1",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    // "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete cascade, " +
    //   s.executeUpdate("DELETE FROM trade.customers WHERE cid=60");

    // ResultSet rs1 = s.executeQuery("SELECT * FROM trade.customers");
    // JDBC.assertDrainResults(rs1, 5);
  }

  public void testDeleteOnTableWithReferencedPrimaryKey() throws Exception {
    startVMs(1, 2);

    //Create a table
    clientSQLExecute(1,
        "create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid))");
    clientSQLExecute(
        1,
        "create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete RESTRICT, "
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    addExpectedException(new int[] { 1 }, null, StandardException.class);
    try {
      clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid>10");
      fail("Exception is expected!");
    } catch (SQLException sqle) {
      if (!"23503".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    try {
      clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid = 20");
      fail("Exception is expected!");
    } catch (SQLException sqle) {
      if (!"23503".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    removeExpectedException(new int[] { 1 }, null, StandardException.class);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_2",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid=60");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);
    //   s.executeUpdate("DELETE FROM trade.customers WHERE cid=60");

    // ResultSet rs1 = s.executeQuery("SELECT * FROM trade.customers");
    // JDBC.assertDrainResults(rs1, 5);
  }
  
  public void testDeleteOnTableWithReferencedPrimaryKeyAndDifferentStrategy() throws Exception {

    startVMs(1, 2);

    //Create a table
    clientSQLExecute(1,
        "create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100)," + "tid int, primary key (cid)) REPLICATE ");
    clientSQLExecute(
        1,
        "create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete RESTRICT, "
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0)) PARTITION BY PRIMARY KEY");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    addExpectedException(new int[] { 1 }, null, StandardException.class);
    try {
      clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid>10");
      fail("Exception is expected!");
    } catch (SQLException sqle) {
      if (!"23503".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    try {
      clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid = 20");
      fail("Exception is expected!");
    } catch (SQLException sqle) {
      if (!"23503".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    removeExpectedException(new int[] { 1 }, null, StandardException.class);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_2",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid=60");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);
    //   s.executeUpdate("DELETE FROM trade.customers WHERE cid=60");

    // ResultSet rs1 = s.executeQuery("SELECT * FROM trade.customers");
    // JDBC.assertDrainResults(rs1, 5);
  }

  //Neeraj: uncomment after bug #42503 is fixed
  public void _testDeleteOnTableWithReferencedUniqueKey() throws Exception {

    startVMs(1, 2);

    //Create a table
    clientSQLExecute(1,
        "create table trade.customers (cid int not null, cust_name varchar(100), "
            + "since date, addr varchar(100),"
            + "tid int not null, primary key (cid), unique (tid))");
    clientSQLExecute(
        1,
        "create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint cust_newt_fk foreign key (cid) references trade.customers (tid) on delete RESTRICT, "
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    addExpectedException(new int[] { 1 }, null, StandardException.class);
    try {
      clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid>10");
      fail("Exception is expected!");
    } catch (SQLException sqle) {
      if (!"23503".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    try {
      clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid = 30");
      fail("Exception is expected!");
    } catch (SQLException sqle) {
      if (!"23503".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    removeExpectedException(new int[] { 1 }, null, StandardException.class);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_2",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid=60");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);
    //   s.executeUpdate("DELETE FROM trade.customers WHERE cid=60");

    // ResultSet rs1 = s.executeQuery("SELECT * FROM trade.customers");
    // JDBC.assertDrainResults(rs1, 5);
  }

  public void testDeleteOnTableWithReferencedPrimaryKeyAndRedundancy()
      throws Exception {

    startVMs(1, 2);

    // Create a table
    clientSQLExecute(1, "create table trade.customers (cid int not null, "
        + "cust_name varchar(100), since date, addr varchar(100),"
        + "tid int, primary key (cid)) "
        + "PARTITION BY PRIMARY KEY REDUNDANCY 2");
    clientSQLExecute(1, "create table trade.networth (cid int not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), loanlimit int, "
        + "availloan decimal (30, 20),  tid int,"
        + "constraint netw_pk primary key (cid), "
        + "constraint cash_ch check (cash>=0), "
        + "constraint sec_ch check (securities >=0),"
        + "constraint cust_newt_fk foreign key (cid) references "
        + "trade.customers (cid) on delete RESTRICT, "
        + "constraint availloan_ck check (loanlimit>=availloan "
        + "and availloan >=0)) PARTITION BY PRIMARY KEY REDUNDANCY 2");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid=60");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    //   s.executeUpdate("DELETE FROM trade.customers WHERE cid=60");

    // ResultSet rs1 = s.executeQuery("SELECT * FROM trade.customers");
    // JDBC.assertDrainResults(rs1, 5);

    // also check with smallint and bigint
    clientSQLExecute(1, "drop table trade.networth");
    clientSQLExecute(1, "drop table trade.customers");

    // Create a table
    clientSQLExecute(1, "create table trade.customers (cid smallint not null, "
        + "cust_name varchar(100), since date, addr varchar(100),"
        + "tid int, primary key (cid)) "
        + "PARTITION BY PRIMARY KEY REDUNDANCY 2");
    clientSQLExecute(1, "create table trade.networth (cid smallint not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), loanlimit int, "
        + "availloan decimal (30, 20),  tid int,"
        + "constraint netw_pk primary key (cid), "
        + "constraint cash_ch check (cash>=0), "
        + "constraint sec_ch check (securities >=0),"
        + "constraint cust_newt_fk foreign key (cid) references "
        + "trade.customers (cid) on delete RESTRICT, "
        + "constraint availloan_ck check (loanlimit>=availloan "
        + "and availloan >=0)) PARTITION BY PRIMARY KEY REDUNDANCY 2");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid=60");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    clientSQLExecute(1, "drop table trade.networth");
    clientSQLExecute(1, "drop table trade.customers");

    // Create a table
    clientSQLExecute(1, "create table trade.customers (cid bigint not null, "
        + "cust_name varchar(100), since date, addr varchar(100),"
        + "tid int, primary key (cid)) "
        + "PARTITION BY PRIMARY KEY REDUNDANCY 2");
    clientSQLExecute(1, "create table trade.networth (cid bigint not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), loanlimit int, "
        + "availloan decimal (30, 20),  tid int,"
        + "constraint netw_pk primary key (cid), "
        + "constraint cash_ch check (cash>=0), "
        + "constraint sec_ch check (securities >=0),"
        + "constraint cust_newt_fk foreign key (cid) references "
        + "trade.customers (cid) on delete RESTRICT, "
        + "constraint availloan_ck check (loanlimit>=availloan "
        + "and availloan >=0)) PARTITION BY PRIMARY KEY REDUNDANCY 2");

    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
    clientSQLExecute(
        1,
        "INSERT INTO trade.customers VALUES (60, 'name60', CURRENT_DATE, 'address60',60)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");

    clientSQLExecute(1, "DELETE FROM trade.customers WHERE cid=60");

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);

    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.customers",
        TestUtil.getResourcesDir() + "/lib/deleteOperation.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);
  }

  /** see bugs #40144 and #40158 */
  public void testDeleteOnTableWithReferencedFKAndNonPKPartitioning()
      throws Exception {

    // Start one client a three servers
    startVMs(1, 3);

    clientSQLExecute(1, "create table trade.customers (cid int not null, "
        + "cust_name varchar(100), addr varchar(100), tid int, "
        + "primary key (cid)) partition by range (cid) "
        + "( VALUES BETWEEN 0 AND 999, VALUES BETWEEN 1009 AND 1102, "
        + "VALUES BETWEEN 1193 AND 1251, VALUES BETWEEN 1291 AND 1677, "
        + "VALUES BETWEEN 1678 AND 10000)");

    clientSQLExecute(1, "create table trade.networth (cid int not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), "
        + "loanlimit int, availloan decimal (30, 20), tid int, "
        + "constraint netw_pk primary key (cid), constraint cust_newt_fk "
        + "foreign key (cid) references trade.customers (cid) on delete "
        + "restrict, constraint cash_ch check (cash>=0), constraint sec_ch "
        + "check (securities >=0), constraint availloan_ck check "
        + "(loanlimit >= availloan and availloan >=0)) "
        + "partition by column(loanLimit, availloan)");

    clientSQLExecute(1, "insert into trade.customers values "
        + "(1000, 'CUST_ONE', 'ADDR_ONE', 1), "
        + "(1100, 'CUST_TWO', 'ADDR_TWO', 2), "
        + "(1200, 'CUST_THREE', 'ADDR_THREE', 3), "
        + "(1300, 'CUST_FOUR', 'ADDR_FOUR', 4), "
        + "(1400, 'CUST_FIVE', 'ADDR_FIVE', 5)");

    serverSQLExecute(2, "insert into trade.networth values "
        + "(1000, 10.2, 100.3, 10000, 5000.5, 1), "
        + "(1100, 11.2, 111.3, 11111, 5111.5, 2), "
        + "(1200, 12.2, 112.3, 12222, 5222.5, 3), "
        + "(1300, 13.2, 113.3, 13333, 5333.5, 4)");

    // verify inserts
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select cid, addr, tid from trade.customers", TestUtil.getResourcesDir()
            + "/lib/checkQuery.xml", "dd_cust_insert");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select cid, securities, loanlimit, tid from trade.networth", TestUtil.getResourcesDir()
            + "/lib/checkQuery.xml", "dd_netw_insert");

    // check FK violation
    checkFKViolation(1, "delete from trade.customers where cid = 1000");
    checkFKViolation(-2, "delete from trade.customers where cid = 1100");
    checkFKViolation(-3, "delete from trade.customers where cid = 1300");

    // check no FK violation
    clientSQLExecute(1, "delete from trade.networth where cid = 1300");
    serverSQLExecute(2, "delete from trade.networth where cid = 1000");
    serverSQLExecute(3, "delete from trade.customers where cid = 1000");
    serverSQLExecute(1, "delete from trade.customers where cid = 1300");

    // verify after deletes
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select cid, addr, tid from trade.customers", TestUtil.getResourcesDir()
            + "/lib/checkQuery.xml", "dd_cust_delete");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "select cid, securities, loanlimit, tid from trade.networth", TestUtil.getResourcesDir()
            + "/lib/checkQuery.xml", "dd_netw_delete");
  }
  
  /**
   * Test for 40050.
   * @throws Exception
   */
  public void test40050() throws Exception {

    startVMs(1, 2);

    //Create a table
    clientSQLExecute(
        1,
        "create table trade.networth (cid int not null, cash decimal (30, 20), "
            + "securities decimal (30, 20), loanlimit int, "
            + "availloan decimal (30, 20),  tid int,"
            + "constraint netw_pk primary key (cid), "
            + "constraint cash_ch check (cash>=0), "
            + "constraint sec_ch check (securities >=0),"
            + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0)) PARTITION BY PRIMARY KEY REDUNDANCY 2");

    
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
        "INSERT INTO trade.networth  VALUES (60, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (70, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (80, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (90, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (100, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (110, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (120, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (130, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (140, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (150, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (160, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (170, 2310.00, 1234.00, 3456, 1245.00, 10)");
    clientSQLExecute(1,
    "INSERT INTO trade.networth  VALUES (180, 2310.00, 1234.00, 3456, 1245.00, 20)");
    
    
    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/selectWithRedundancy2.xml", "q_1",
        true /* do not use prep stmnt */, false /* do not check for type information */);
    
    clientSQLExecute(1, "INSERT INTO trade.networth  VALUES (190, 2310.00, 1234.00, 3456, 1245.00, 20)");
    
    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/selectWithRedundancy2.xml", "q_2",
        true /* do not use prep stmnt */, false /* do not check for type information */);
    
    clientSQLExecute(1, "DELETE FROM trade.networth where cid>180");
    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/selectWithRedundancy2.xml", "q_3",
        true /* do not use prep stmnt */, false /* do not check for type information */);
    clientSQLExecute(1, "DELETE FROM trade.networth where cid>10");
    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/selectWithRedundancy2.xml", "q_4",
        true /* do not use prep stmnt */, false /* do not check for type information */);
    
    clientSQLExecute(1, "INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 3456, 1245.00, 20)");
    sqlExecuteVerify(new int[] { 1 }, null, "select cid From trade.networth",
        TestUtil.getResourcesDir() + "/lib/selectWithRedundancy2.xml", "q_5",
        true /* do not use prep stmnt */, false /* do not check for type information */);
  }
}

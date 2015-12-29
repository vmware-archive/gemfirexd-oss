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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.EntryOperationImpl;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

public class DefaultPartitionTest extends JdbcTestBase {

  public DefaultPartitionTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(DefaultPartitionTest.class));
  }

  public void testDefaultPartitionWhenPKFKExists() throws SQLException,
      StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");
    Cache cache = CacheFactory.getAnyInstance();
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, primary key (ID))"
        + " PARTITION BY PRIMARY KEY");
    s
        .execute("create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID int not null, THIRDID int not null, foreign key (ID_FK) references EMP.PARTITIONTESTTABLE(ID))");
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE_FK");
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    assertNotNull(pr);
    GfxdPartitionByExpressionResolver scpr = (GfxdPartitionByExpressionResolver)pr;
    assertEquals("/EMP/PARTITIONTESTTABLE", scpr.getMasterTable(false /* immediate master*/));
    assertEquals(1, scpr.getColumnNames().length);
    assertEquals("ID_FK", scpr.getColumnNames()[0]);

    String colocateTable = rattr.getPartitionAttributes().getColocatedWith();
    assertEquals("/EMP/PARTITIONTESTTABLE", colocateTable);
  }

  public void testDefaultPartitionWhenPKFKExists_composite()
      throws SQLException, StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");
    Cache cache = CacheFactory.getAnyInstance();
    s
        .execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID, SECONDID))"
            + " PARTITION BY PRIMARY KEY");
    s
        .execute("create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID_FK int not null, THIRDID int not null, "
            + "foreign key (ID_FK, SECONDID_FK) references EMP.PARTITIONTESTTABLE(ID, SECONDID))");
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE_FK");
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    assertNotNull(pr);
    GfxdPartitionByExpressionResolver scpr = (GfxdPartitionByExpressionResolver)pr;
    assertEquals("/EMP/PARTITIONTESTTABLE", scpr.getMasterTable(false /* immediate master*/));
    assertEquals(2, scpr.getColumnNames().length);
    assertEquals("ID_FK", scpr.getColumnNames()[0]);
    assertEquals("SECONDID_FK", scpr.getColumnNames()[1]);

    String colocateTable = rattr.getPartitionAttributes().getColocatedWith();
    assertEquals("/EMP/PARTITIONTESTTABLE", colocateTable);
  }

  public void testDefaultPartitionWhenPKFKExists_composite_pkIn2ndtoo()
      throws SQLException, StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");
    Cache cache = CacheFactory.getAnyInstance();
    s
        .execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID, SECONDID))"
            + " PARTITION BY PRIMARY KEY");
    s
        .execute("create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID_FK int not null, THIRDID int not null, primary key (THIRDID), "
            + "foreign key (ID_FK, SECONDID_FK) references EMP.PARTITIONTESTTABLE(ID, SECONDID))");
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE_FK");
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    assertNotNull(pr);
    GfxdPartitionByExpressionResolver scpr = (GfxdPartitionByExpressionResolver)pr;
    assertEquals("/EMP/PARTITIONTESTTABLE", scpr.getMasterTable(false /* immediate master*/));
    assertEquals(2, scpr.getColumnNames().length);
    assertEquals("ID_FK", scpr.getColumnNames()[0]);
    assertEquals("SECONDID_FK", scpr.getColumnNames()[1]);

    String colocateTable = rattr.getPartitionAttributes().getColocatedWith();
    assertEquals("/EMP/PARTITIONTESTTABLE", colocateTable);
  }

  public void testDefaultPartitionWhenNoPKFKExists() throws SQLException,
      StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");
    Cache cache = CacheFactory.getAnyInstance();
    s
        .execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID, SECONDID))");
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    assertNotNull(pr);
    GfxdPartitionByExpressionResolver scpr = (GfxdPartitionByExpressionResolver)pr;
    assert(scpr.isDefaultPartitioning());
    assertEquals("/EMP/PARTITIONTESTTABLE", scpr.getMasterTable(false /* immediate master*/));
    assertEquals(2, scpr.getColumnNames().length);
    assertEquals("ID", scpr.getColumnNames()[0]);
    assertEquals("SECONDID", scpr.getColumnNames()[1]);
  }

  public void testDefaultPartition1stUniqKey() throws SQLException,
      StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");
    Cache cache = CacheFactory.getAnyInstance();
    s
        .execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, unique (ID), unique (SECONDID))");
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    assertNotNull(pr);
    GfxdPartitionByExpressionResolver scpr = (GfxdPartitionByExpressionResolver)pr;
    assert(scpr.isDefaultPartitioning());
    assertEquals("/EMP/PARTITIONTESTTABLE", scpr.getMasterTable(false /* immediate master*/));
    assertEquals(1, scpr.getColumnNames().length);
    assertEquals("ID", scpr.getColumnNames()[0]);
  }

  public void testDefaultPartitionWhenMultFKExists() throws SQLException,
      StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");
    Cache cache = CacheFactory.getAnyInstance();
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, primary key (ID))");
    s
        .execute("create table EMP.PARTITIONTESTTABLE_2nd (ID_2 int not null, "
            + " SECONDID_2 int not null, THIRDID_2 int not null, primary key (SECONDID_2))");
    s
        .execute("create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID_FK int not null, THIRDID_FK int not null, "
            + "foreign key (SECONDID_FK) references EMP.PARTITIONTESTTABLE_2nd(SECONDID_2),"
            + "foreign key (ID_FK) references EMP.PARTITIONTESTTABLE(ID))");
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE_FK");
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    assertNotNull(pr);
    GfxdPartitionByExpressionResolver scpr = (GfxdPartitionByExpressionResolver)pr;
    assert(scpr.isDefaultPartitioning());
    assertEquals(1, scpr.getColumnNames().length);
    assertEquals("SECONDID_FK", scpr.getColumnNames()[0]);
  }

  public void testDefaultPartitionWhenNoConditionForDefaultExists()
      throws SQLException, StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");
    Cache cache = CacheFactory.getAnyInstance();
    s
        .execute("create table EMP.PARTITIONTESTTABLE (ID int, SECONDID int, THIRDID int)");
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    assertNotNull(pr);
    GfxdPartitionByExpressionResolver scpr = (GfxdPartitionByExpressionResolver)pr;
    assert(scpr.isDefaultPartitioning());
    assertEquals(0, scpr.getColumnNames().length);
    EntryOperationImpl dEntryOp = new EntryOperationImpl(null, null,
        new Integer(10), null, null);
    Serializable srobj = scpr.getRoutingObject(dEntryOp);
    Integer robj = (Integer)srobj;
    assertEquals(10, robj.intValue());
  }

  public void testMultiTableMultiFK() throws Exception {

    setupConnection();

    // create schema and verify that the schema region exists
    sqlExecute("create schema EMP", true);
    assertNotNull(Misc.getRegionForTable("EMP", false));

    // create table with pk constraint
    sqlExecute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, constraint part_pk "
        + "primary key (ID))", false);

    // create another table with pk and fk constraints
    // should throw exception due to duplicate constraint
    try {
      sqlExecute("create table EMP.PARTITIONTESTTABLE2 (ID int not null, "
          + " SECONDID int not null, THIRDID int not null, constraint part_pk "
          + "primary key (SECONDID, THIRDID), constraint part_fk foreign key "
          + "(ID) references EMP.PARTITIONTESTTABLE(ID))", false);
    } catch (SQLException ex) {
      // check for the expected exception
      if (!"X0Y32".equals(ex.getSQLState())) {
        throw ex;
      }
    }

    // create another table with pk and fk constraints
    sqlExecute("create table EMP.PARTITIONTESTTABLE2 (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, constraint part2_pk "
        + "primary key (SECONDID, THIRDID), constraint part_fk foreign key "
        + "(ID) references EMP.PARTITIONTESTTABLE(ID))", false);

    // create a third table with pk and fk constraints
    sqlExecute("create table EMP.PARTITIONTESTTABLE3 (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, constraint part3_pk "
        + "primary key (ID), constraint part3_fk foreign key "
        + "(SECONDID, THIRDID) references EMP.PARTITIONTESTTABLE2(SECONDID, "
        + "THIRDID))", false);

    // check that the constraints belong to the proper tables

    sqlExecuteVerifyText("select TABLENAME, CONSTRAINTNAME from "
        + "SYS.SYSCONSTRAINTS con, SYS.SYSTABLES tab where con.TABLEID = "
        + "tab.TABLEID and CONSTRAINTNAME='PART_PK'", getResourcesDir()
        + "/lib/checkCreateTable.xml", "emp_fkconstr1", false, false);

    sqlExecuteVerifyText("select TABLENAME, CONSTRAINTNAME from "
        + "SYS.SYSCONSTRAINTS con, SYS.SYSTABLES tab where con.TABLEID = "
        + "tab.TABLEID and CONSTRAINTNAME='PART2_PK'", getResourcesDir()
        + "/lib/checkCreateTable.xml", "emp_fkconstr2", true, false);

    sqlExecuteVerifyText("select TABLENAME, CONSTRAINTNAME from "
        + "SYS.SYSCONSTRAINTS con, SYS.SYSTABLES tab where con.TABLEID = "
        + "tab.TABLEID and CONSTRAINTNAME='PART3_PK'", getResourcesDir()
        + "/lib/checkCreateTable.xml", "emp_fkconstr3", true, false);

    sqlExecuteVerifyText("select TABLENAME, CONSTRAINTNAME from "
        + "SYS.SYSCONSTRAINTS con, SYS.SYSTABLES tab where con.TABLEID = "
        + "tab.TABLEID and CONSTRAINTNAME='PART3_FK'", getResourcesDir()
        + "/lib/checkCreateTable.xml", "emp_fkconstr4", true, false);

    // drop the tables and schema
    sqlExecute("drop table EMP.PARTITIONTESTTABLE3", false);
    sqlExecute("drop table EMP.PARTITIONTESTTABLE2", false);
    sqlExecute("drop table EMP.PARTITIONTESTTABLE", false);
    sqlExecute("drop schema EMP restrict", false);
  }

  public void testMultiTableMultiFKSubsetPK() throws Exception {
    setupConnection();

    // create table with pk constraint
    sqlExecute(
        "create table trade.customers (cid int not null, cust_name varchar(100),"
            + " since date, addr varchar(100), tid int, primary key (cid))",
        false);

    // create another table with pk and fk constraints with fk a subset of pk
    // (bug #40093)
    sqlExecute(
        "create table trade.portfolio (cid int not null, sid int not null,"
            + " qty int not null, availQty int not null, subTotal decimal(30,20),"
            + " tid int, constraint portf_pk primary key (cid, sid), constraint"
            + " cust_port_fk foreign key (cid) references trade.customers (cid),"
            + " constraint qty_ck check (qty>=0), constraint avail_ch"
            + " check (availQty>=0 and availQty<=qty))", false);

    // create a third table with pk and fk constraints
    sqlExecute(
        "create table trade.sellorders (oid int not null constraint orders_pk"
            + " primary key, cid int, sid int, qty int, price decimal (10, 2),"
            + " order_time date, status varchar(10), tid int, constraint portf_fk"
            + " foreign key (cid, sid) references trade.portfolio (cid, sid), "
            + "constraint status_ch check (status in ('cancelled', 'pending',"
            + " 'filled')))", false);

    // check that the constraints belong to the proper tables

    sqlExecuteVerifyText("select TABLENAME, CONSTRAINTNAME from "
        + "SYS.SYSCONSTRAINTS con, SYS.SYSTABLES tab where con.TABLEID = "
        + "tab.TABLEID and CONSTRAINTNAME='PORTF_PK'", getResourcesDir()
        + "/lib/checkCreateTable.xml", "emp_fkconstr5", false, false);

    sqlExecuteVerifyText("select TABLENAME, CONSTRAINTNAME from "
        + "SYS.SYSCONSTRAINTS con, SYS.SYSTABLES tab where con.TABLEID = "
        + "tab.TABLEID and CONSTRAINTNAME='CUST_PORT_FK'", getResourcesDir()
        + "/lib/checkCreateTable.xml", "emp_fkconstr6", true, false);

    sqlExecuteVerifyText("select TABLENAME, CONSTRAINTNAME from "
        + "SYS.SYSCONSTRAINTS con, SYS.SYSTABLES tab where con.TABLEID = "
        + "tab.TABLEID and CONSTRAINTNAME='PORTF_FK'", getResourcesDir()
        + "/lib/checkCreateTable.xml", "emp_fkconstr7", true, false);

    // dropping a parent table first should throw an exception
    try {
      sqlExecute("drop table trade.portfolio", false);
    } catch (SQLException ex) {
      // check for the expected exception
      if (!"X0Y25".equals(ex.getSQLState())) {
        throw ex;
      }
    }

    // drop the tables and schema
    sqlExecute("drop table trade.sellorders", false);
    sqlExecute("drop table trade.portfolio", false);
    sqlExecute("drop table trade.customers", false);
    sqlExecute("drop schema trade restrict", false);
  }

  /**
   * Check that the default policy for tables is now REPLICATE.
   */
  public void testDefaultReplicate() throws SQLException {
    // override the flag set in TestUtil
    skipDefaultPartitioned = true;

    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute("create table EMP.TESTTABLE (ID int, "
        + "DESCRIPTION varchar(1024) not null, "
        + "ADDRESS varchar(1024) not null,"
        + "TID int, primary key (ID), unique (tid))");
    // also create an explicit indexes
    stmt.execute("create index test_idx1 on EMP.testtable(description)");
    // explicit global index creation should fail
    try {
      stmt.execute("create global hash index test_idx3 on "
          + "EMP.testtable(address)");
      fail("expected global hash index creation to fail for replicated table");
    } catch (SQLException sqle) {
      if (!"X0Z15".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    // unique index should succeed
    stmt.execute("create unique index test_idx2 on EMP.testtable(address)");

    // check the table and constraint/index types
    Region<?, ?> region = Misc.getRegionForTable("EMP.TESTTABLE", true);
    assertEquals(DataPolicy.REPLICATE, region.getAttributes().getDataPolicy());
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALHASH1", "ID");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALSORTEDMAP", "TID");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALSORTEDMAP", "DESCRIPTION");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALSORTEDMAP", "ADDRESS");
  }

  /**
   * Check that the default policy for tables is switched to REPLICATE correctly
   * when using system property.
   */
  public void testDefaultReplicateSystemProp() throws SQLException {
    // set system property
    System.setProperty(GfxdConstants.TABLE_DEFAULT_PARTITIONED_SYSPROP, "false");
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute("create table EMP.TESTTABLE (ID int, "
        + "DESCRIPTION varchar(1024) not null, "
        + "ADDRESS varchar(1024) not null,"
        + "TID int, primary key (ID), unique (tid))");
    // also create an explicit indexes
    stmt.execute("create index test_idx1 on EMP.testtable(description)");
    // explicit global index creation should fail
    try {
      stmt.execute("create global hash index test_idx3 on "
          + "EMP.testtable(address)");
      fail("expected global hash index creation to fail for replicated table");
    } catch (SQLException sqle) {
      if (!"X0Z15".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }
    // unique index should succeed
    stmt.execute("create unique index test_idx2 on EMP.testtable(address)");

    // check the table and constraint/index types
    Region<?, ?> region = Misc.getRegionForTable("EMP.TESTTABLE", true);
    assertEquals(DataPolicy.REPLICATE, region.getAttributes().getDataPolicy());
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALHASH1", "ID");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALSORTEDMAP", "TID");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALSORTEDMAP",
        "DESCRIPTION");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALSORTEDMAP",
        "ADDRESS");
  }

  /**
   * Check that the default policy for tables is switched to PARTITION correctly
   * when using system property.
   */
  public void testDefaultPartitionedSystemProp() throws SQLException {
    // override the flag set in TestUtil
    skipDefaultPartitioned = true;

    // set system property
    System.setProperty(GfxdConstants.TABLE_DEFAULT_PARTITIONED_SYSPROP, "true");
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    stmt.execute("create table EMP.TESTTABLE (ID int, "
        + "DESCRIPTION varchar(1024) not null, "
        + "ADDRESS varchar(1024) not null,"
        + "ADDRESS2 varchar(1024) not null,"
        + "TID int, primary key (ID), unique (tid))");
    // also create an explicit indexes
    stmt.execute("create index test_idx1 on EMP.testtable(description)");
    // explicit global index creation should be fine now
    stmt.execute("create global hash index test_idx3 on "
        + "EMP.testtable(address)");
    // unique index should also succeed
    stmt.execute("create unique index test_idx2 on EMP.testtable(address2)");

    // check the table and constraint/index types
    Region<?, ?> region = Misc.getRegionForTable("EMP.TESTTABLE", true);
    assertEquals(DataPolicy.PARTITION, region.getAttributes().getDataPolicy());
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALHASH1", "ID");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "GLOBALHASH", "TID");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "LOCALSORTEDMAP",
        "DESCRIPTION");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "GLOBALHASH", "ADDRESS");
    AlterTableTest.checkIndexType("EMP", "TESTTABLE", "GLOBALHASH",
        "ADDRESS2");
  }
}

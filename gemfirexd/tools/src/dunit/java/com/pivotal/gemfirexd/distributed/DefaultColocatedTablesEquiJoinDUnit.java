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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

import io.snappydata.test.dunit.VM;

@SuppressWarnings("serial")
public class DefaultColocatedTablesEquiJoinDUnit extends DistributedSQLTestBase {

  public DefaultColocatedTablesEquiJoinDUnit(String name) {
    super(name);
  }

  void checkColocation(int[] clientNums, int[] serverNums, String table1,
      String table2, String dumpEntries) {
    ArrayList<VM> vmlist = getVMs(clientNums, serverNums);
    assertNotNull(vmlist);
    assertTrue(vmlist.size() > 0);
    for (int i = 0; i < vmlist.size(); i++) {
      VM executeVM = vmlist.get(i);
      if (executeVM != null) {
        executeVM.invoke(DefaultColocatedTablesEquiJoinDUnit.class,
            "verifyTablesColocated", new Object[] { table1 , table2 ,
                new Boolean(dumpEntries) });
      }
    }
  }

  public static void verifyTablesColocated(String table1AsRegionPath,
      String table2AsRegionPath, Boolean dumpEntries) {
    Region colocatedTableForSecondTable = Misc.getGemFireCache().getRegion(
        table2AsRegionPath);
    assertNotNull(colocatedTableForSecondTable);
    PartitionAttributes pattrs = colocatedTableForSecondTable.getAttributes()
        .getPartitionAttributes();
    assertNotNull(pattrs);
    assertEquals(table1AsRegionPath, pattrs.getColocatedWith());
    getGlobalLogger().info(
        "table 2: " + table2AsRegionPath + " getColocatedwith returned: "
            + pattrs.getColocatedWith());
    if (dumpEntries.booleanValue()) {
      getGlobalLogger().info(
          "Dumping all entries in table1: " + table1AsRegionPath);
      dumpRegionEntries(table1AsRegionPath);
      dumpRegionEntries(table2AsRegionPath);
    }
  }

  public static void dumpRegionEntries(String regionPath) {
    PartitionedRegion reg = (PartitionedRegion)Misc.getGemFireCache()
        .getRegion(regionPath);
    assertNotNull(reg);
    Iterator<?> irl = reg.getSharedDataView().getLocalEntriesIterator(
        (InternalRegionFunctionContext)null, true, false, true, reg);
    getGlobalLogger().info("Dumping all local entries in table1: " + regionPath);
    int i = 0;
    while (irl.hasNext()) {
      RowLocation rl = (RowLocation)irl.next();
      getGlobalLogger().info("entry" + (i++) + " = " + rl);
    }
  }

  // Disabled due to NPE in MemHeapScanController.next
  // filed as #40095
  // Reenabling the test on 23-01-2009. The test was due to the merge issue.
  // Fixed in revision r23493
  public void testDistributedQueryPKFKExists() throws Exception {
    // Start one client a one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID))"
    // + " PARTITION BY PRIMARY KEY");
    );
    clientSQLExecute(
        1,
        "create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID_2 int not null, THIRDID_2 int not null, foreign key (ID_FK) references EMP.PARTITIONTESTTABLE(ID))");

    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE values (1, 1, 1), (2, 2, 2), (3, 3, 3) ");
    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE_FK values (1, 1, 1), (2, 2, 2), (3, 3, 3) ");

    checkColocation(new int[] { 1 }, new int[] { 1 , 2 },
        "/EMP/PARTITIONTESTTABLE", "/EMP/PARTITIONTESTTABLE_FK", "true");

    sqlExecuteVerify(
        new int[] { 1 },
        null,
        "select * from EMP.PARTITIONTESTTABLE, EMP.PARTITIONTESTTABLE_FK where ID = ID_FK",
        TestUtil.getResourcesDir() + "/lib/checkEquiJoinQuery.xml",
        "defaultquery_test1", false, false);
  }

  // Disabled due to NPE in getRoutingObjectFromGlobalIndex.getRoutingObjectFromGlobalIndex (lcc is null)
  // filed as #40096
  // Reenabling the test on 23-01-2009. The test was due to the merge issue.
  // Fixed in revision r23493
  public void testDistributedQueryPKFKExists_pkOn2ndtablealso()
      throws Exception {
    // Start one client a one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID))"
    // + " PARTITION BY PRIMARY KEY");
    );
    clientSQLExecute(
        1,
        "create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID_2 int not null, THIRDID_2 int not null, primary key (SECONDID_2), foreign key (ID_FK) references EMP.PARTITIONTESTTABLE(ID))");

    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE values (1, 1, 1), (2, 2, 2), (3, 3, 3) ");
    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE_FK values (1, 5, 1), (2, 7, 2), (3, 9, 3) ");

    checkColocation(new int[] { 1 }, new int[] { 1 , 2 },
        "/EMP/PARTITIONTESTTABLE", "/EMP/PARTITIONTESTTABLE_FK", "true");

    sqlExecuteVerify(
        new int[] { 1 },
        null,
        "select * from EMP.PARTITIONTESTTABLE, EMP.PARTITIONTESTTABLE_FK where ID = ID_FK",
        TestUtil.getResourcesDir() + "/lib/checkEquiJoinQuery.xml",
        "defaultquery_test2", false, false);
  }

  public void testDistributedQueryNoPKFKExists_explicitPartitionByPrimary()
      throws Exception {
    // Start one client a one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID))"
            + " PARTITION BY PRIMARY KEY");

    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID int not null, THIRDID int not null)");

    boolean gotException = false;
    try {
      clientSQLExecute(
          1,
          "select * from EMP.PARTITIONTESTTABLE, EMP.PARTITIONTESTTABLE_FK where ID = ID_FK");
    }
    catch (SQLException ex) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testDistributedQueryNoPKFKExists_implicitPartitionByPrimary_failAsNoPKFK()
      throws Exception {
    // Start one client a one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID))");

    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null, "
            + " SECONDID int not null, THIRDID int not null)");

    boolean gotException = false;
    try {
      clientSQLExecute(
          1,
          "select * from EMP.PARTITIONTESTTABLE, EMP.PARTITIONTESTTABLE_FK where ID = ID_FK");
    }
    catch (SQLException ex) {
      gotException = true;
    }
    assertTrue(gotException);
  }

  public void testDistributedQueryNoPKFKExists_singleSystemNoException()
      throws Exception {
    // Start one client a one server
    startServerVMs(1, 0, null);

    serverSQLExecute(1, "create schema EMP");
    serverSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " SECONDID int not null, THIRDID int not null, primary key (ID))"
            + " PARTITION BY PRIMARY KEY");

    serverSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE_FK (ID_FK int not null constraint fk references EMP.PARTITIONTESTTABLE (ID), "
            + " SECONDID int not null, THIRDID int not null)");

    boolean gotException = false;
    try {
      serverSQLExecute(
          1,
          "select * from EMP.PARTITIONTESTTABLE, EMP.PARTITIONTESTTABLE_FK where ID = ID_FK");
    }
    catch (SQLException ex) {
      gotException = true;
    }
    assertFalse(gotException);
  }
  
  public void testBug46953()
      throws Exception {
    // Start one client a one server
    startVMs(1, 3);

    clientSQLExecute(1, "create schema TRADE");
    clientSQLExecute(1, "create table trade.custrep "
        + "(c_balance int not null, c_first int not null, c_middle varchar(10), "
        + "c_id int primary key ) "
        + "replicate" 
        );
    clientSQLExecute(1, "create table trade.custpart "
        + "(c_balance int not null, c_first int not null, c_middle varchar(10), "
        + "c_id int primary key ) "
        + "partition by column (c_first)"
        );

    { // insert values
      Connection conn = TestUtil.getConnection();
      String[] securities = { "IBM", "MOT", "INTC", "TEK", "AMD", "CSCO",
          "DELL", "HP", "SMALL1", "SMALL2" };
      PreparedStatement psInsert = conn
          .prepareStatement("insert into trade.custrep values (?, ?, ?, ?)");
      for (int i = 0; i < 3; i++) {
        psInsert.setInt(1, 1);
        psInsert.setInt(2, i * 2);
        psInsert.setString(3, securities[i % 9]);
        psInsert.setInt(4, i * 4);
        psInsert.executeUpdate();
      }
    }
    
    { // insert values
      Connection conn = TestUtil.getConnection();
      String[] securities = { "IBM", "MOT", "INTC", "TEK", "AMD", "CSCO",
          "DELL", "HP", "SMALL1", "SMALL2" };
      PreparedStatement psInsert = conn
          .prepareStatement("insert into trade.custpart values (?, ?, ?, ?)");
      for (int i = 0; i < 3; i++) {
        psInsert.setInt(1, 1);
        psInsert.setInt(2, i * 2);
        psInsert.setString(3, securities[i % 9]);
        psInsert.setInt(4, i * 4);
        psInsert.executeUpdate();
      }
    }

    boolean gotException = false;
    try {
      clientSQLExecute(1, "SELECT * FROM trade.custpart, "
          + "(select max(c_balance) from trade.custpart) as mytab(c1)");
    } catch (SQLException ex) {
      assertEquals("0A000", ex.getSQLState());
      gotException = true;
    }
    assertTrue(gotException);
    
    { // insert values
      Connection conn = TestUtil.getConnection();
      PreparedStatement ps = conn
          .prepareStatement("SELECT * FROM trade.custrep, "
              + "(select max(c_balance) from trade.custrep) as mytab(c1)");
      ResultSet r = ps.executeQuery();
      int count = 0;
      while (r.next()) {
        assertEquals(1, r.getInt(1));
        assertEquals(1, r.getInt(5));
        count++;
      }
      assertEquals(3, count);
      r.close();
    }
  }
}

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
import java.sql.SQLException;
import java.sql.Statement;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import io.snappydata.test.dunit.SerializableRunnable;

@SuppressWarnings("serial")
public class PersistentReplicateTableDUnit extends DistributedSQLTestBase {

  private final static String DISKSTORE = "TestPersistenceDiskStore";

  public PersistentReplicateTableDUnit(String name) {
    super(name);
  }

  public String getSuffix() throws Exception {
    String suffix = " PERSISTENT " + "'" + DISKSTORE + "'";
    return suffix;
  }

  public void createDiskStore(boolean useClient, int vmNum) throws Exception {
    SerializableRunnable csr = getDiskStoreCreator(DISKSTORE);
    if (useClient) {
      if (vmNum == 1) {
        csr.run();
      }
      else {
        clientExecute(vmNum, csr);
      }
    }
    else {
      serverExecute(vmNum, csr);
    }
  }

  @Override
  protected String[] testSpecificDirectoriesForDeletion() {
    return new String[] { "test_dir" };
  }

  /**
   * Test insufficient data store behaviour for distributed/update/delete/select
   * and for primary key based select/update/delete
   * 
   * @throws Exception
   */
  public void testInsufficientDatastoreBehaviourBug42447() throws Exception {
    startVMs(1, 3);
    createDiskStore(true, 1);
    // Create a schema
    clientSQLExecute(1, "create schema trade");

    clientSQLExecute(1, "create table trade.customers (cid int not null, "
        + "cust_name varchar(100), tid int, primary key (cid)) replicate "
        + getSuffix());
    Connection conn = TestUtil.getConnection();
    PreparedStatement psInsert = conn
        .prepareStatement("insert into trade.customers values (?,?,?)");
    for (int i = 1; i < 31; ++i) {
      psInsert.setInt(1, i);
      psInsert.setString(2, "name" + i);
      psInsert.setInt(3, i);
      psInsert.executeUpdate();
    }
    stopVMNums(-3, -2, -1);
    stopVMNum(1);
    restartVMNums(1);
    try {
      conn = TestUtil.getConnection();
      Statement stmt = conn.createStatement();

      // Test bulk operations
      try {
        stmt.executeQuery("select * from trade.customers");
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        PreparedStatement ps = conn
            .prepareStatement("select * from trade.customers "
                + "where tid > ?");
        ps.setInt(1, 0);
        ps.executeQuery();
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        stmt.executeUpdate("update trade.customers set tid = 5 where tid > 3");
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        PreparedStatement ps = conn
            .prepareStatement("update trade.customers set tid = ?"
                + " where tid > ?");
        ps.setInt(1, 5);
        ps.setInt(2, 3);
        ps.executeUpdate();
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        stmt.executeUpdate(" delete from trade.customers  where tid > 3");
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        PreparedStatement ps = conn
            .prepareStatement(" delete from trade.customers  where tid > ?");
        ps.setInt(1, 3);
        ps.executeUpdate();
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      // Test PK based operations
      try {
        ResultSet rs = stmt
            .executeQuery("select * from trade.customers where cid  = 1");
        rs.next();
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals(sqle.getSQLState(), "X0Z08");
      }

      try {
        PreparedStatement ps = conn
            .prepareStatement("select * from trade.customers "
                + "where cid = ?");
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        rs.next();
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        stmt.executeUpdate("update trade.customers set tid = 5 where cid = 3");
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }
      
      try {
        psInsert = conn
            .prepareStatement("insert into trade.customers values (?,?,?)");
        psInsert.setInt(1, 40);
        psInsert.setString(2, "name40");
        psInsert.setInt(3, 40);
        psInsert.executeUpdate();
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        PreparedStatement ps = conn
            .prepareStatement("update trade.customers set tid = ?"
                + " where cid = ?");
        ps.setInt(1, 5);
        ps.setInt(2, 3);
        ps.executeUpdate();
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        stmt.executeUpdate(" delete from trade.customers  where  cid = 3");
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

      try {
        PreparedStatement ps = conn
            .prepareStatement(" delete from trade.customers  where cid = ?");
        ps.setInt(1, 3);
        ps.executeUpdate();
        fail("Test should fail due to insufficient data stores");
      } catch (SQLException sqle) {
        assertEquals("X0Z08", sqle.getSQLState());
      }

    } finally {
      // restartVMNums(-1, -2, -3);
      // restarting a VM so that DB cleanup can go on without problems else
      // subsequent tests start seeing stale datadictionary
      restartVMNums(-1);
    }
  }
}

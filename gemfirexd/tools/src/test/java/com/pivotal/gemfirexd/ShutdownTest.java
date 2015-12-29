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
package com.pivotal.gemfirexd;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.gemstone.junit.UnitTest;

/**
 * Tests shutdown
 * @author sjigyasu
 *
 */

public class ShutdownTest extends TestUtil implements UnitTest {

  public ShutdownTest(String name) {
    super(name);
  }
  
  /**
   * Verifies that there is no SQL error 25001 "Cannot close a connection while a transaction is still active."
   * when FabricService.stop() is called.  The error was noticed in teardown() of TransactionTest.Bug42915
   * 
   * The error would occur if in BaseMonitor.shutdown() GemFireStore.stop() was called before the topService.stop() (which is 
   * FabricDatabase.stop() ) was called, causing active status of database to be false.  To avoid this, only the GemfireCache.close()
   * and InternalDistributedSystem.disconnect() are called and not GemfireStore.stop() when it is a case of shutdown and not shutdownAll.
   * See BaseMonitor.shutdown().
   * 
   * This test was added in context of Bug# 45163
   *  
   */
  public void testSQLError20001OnFabricServiceStop() throws SQLException {

    try{
      Connection conn = getConnection();
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      conn.setAutoCommit(false);
      Statement stmt = conn.createStatement();
      stmt.execute("Create table t1 (c1 int not null primary key, "
          + "c2 int not null, c3 int not null)");
      stmt.execute("Create table t2 (c1 int not null primary key, "
          + "c2 int not null, c3 int not null, "
          + "foreign key (c1) references t1(c1))");
      conn.commit();

      Connection childConn = getConnection();
      childConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      childConn.setAutoCommit(false);
      Statement childStmnt = childConn.createStatement();
      childStmnt.execute("insert into t1 values(1, 1, 1)");

      stmt = conn.createStatement();
      boolean gotException = false;
      try {
        stmt.execute("insert into t2 values(1, 1, 1)");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
        gotException = true;
      }
      assertTrue(gotException);

      // now commit in second TX, then insert in second should succeed
      childConn.commit();
      stmt.execute("insert into t2 values(1, 1, 1)");

      // if now parent update is tried then it should conflict
      try {
        childStmnt.execute("update t1 set c2 = 2 where c1 = 1");
        fail("expected conflict exception");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
      // and so too should delete
      try {
        childStmnt.execute("delete from t1 where c1 = 1");
        fail("expected conflict exception");
      } catch (SQLException sqle) {
        if (!"X0Z02".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
      // after rollback in first TX the update and delete should then pass
      conn.rollback();
      childStmnt.execute("update t1 set c2 = 2 where c1 = 1");
      childStmnt.execute("delete from t1 where c1 = 1");
      childConn.commit();

      // now try from the same transaction and it should not fail
      stmt.execute("delete from t2 where c1 = 1");
      stmt.execute("insert into t1 values(1, 1, 1)");
      conn.commit();
      stmt.execute("insert into t2 values(1, 1, 1)");
      // delete should still fail
      try {
        stmt.execute("delete from t1 where c1 = 1");
        fail("expected a constraint violation");
      } catch (SQLException sqle) {
        if (!"23503".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }
      // update of row should be fine
      stmt.execute("update t1 set c2 = 3 where c1 = 1");
      conn.commit();

      // verify the final values
      ResultSet rs = stmt.executeQuery("select c2, c3 from t1 where c1 = 1");
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertEquals(1, rs.getInt(2));
      assertFalse(rs.next());
      rs.close();
      conn.commit();
      
      
      Properties shutdownProperties = new Properties();
      
      if (bootUserName != null) {
        shutdownProperties.setProperty("user", bootUserName);
        shutdownProperties.setProperty("password", bootUserPassword);
        getLogger().info(
            "shutting down with " + bootUserName + " and boot password "
                + bootUserPassword);
      }
      else if (currentUserName != null) {
        shutdownProperties.setProperty("user", currentUserName);
        shutdownProperties.setProperty("password", currentUserPassword);
        getLogger().info(
            "shutting down with " + currentUserName + " and password "
                + currentUserPassword);
      }      
      stopNetServer();
      final FabricService service = FabricServiceManager
          .currentFabricServiceInstance();
      if (service != null) {
        service.stop(shutdownProperties);
        getLogger().info("Fabric Server shutdown status " + service.status());
      }
    }
    catch(SQLException e)
    {
      assertFalse(e.getSQLState().equals("25001"));
      //e.printStackTrace();
      throw e;
    }
  }
}

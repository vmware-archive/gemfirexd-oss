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
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import io.snappydata.test.dunit.SerializableRunnable;

@SuppressWarnings("serial")
public class NCJoinDefaultColocatedTablesEquiJoinDUnit extends DefaultColocatedTablesEquiJoinDUnit {

  public NCJoinDefaultColocatedTablesEquiJoinDUnit(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
      }
    });
    super.setUp();
  }

  @Override
  public void tearDown2() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
      }
    });
    super.tearDown2();
  }
  
  
  /*
   * TODO: This test case should through "Not supported exception" as in
   * @see com.pivotal.gemfirexd.distributed.DefaultColocatedTablesEquiJoinDUnit#testBug46953()
   * 
   * To do so, this test should be removed (and thus same test in super class will be executed.)
   */
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
      assertEquals("XJ001", ex.getSQLState());
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

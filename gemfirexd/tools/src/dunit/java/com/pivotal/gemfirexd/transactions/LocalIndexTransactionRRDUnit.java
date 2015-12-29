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

package com.pivotal.gemfirexd.transactions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.pivotal.gemfirexd.TestUtil;

@SuppressWarnings("serial")
public class LocalIndexTransactionRRDUnit extends LocalIndexTransactionDUnit {

  public LocalIndexTransactionRRDUnit(String name) {
    super(name);
  }

  /** check that conflicts during commit lead to proper rollback in indexes */
  public void testIndexesInCommitConflict_44435() throws Exception {
    startVMs(1, 1);

    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(getIsolationLevel());
    conn.setAutoCommit(false);
    Statement st = conn.createStatement();
    st.execute("Create table t1 (c1 int not null primary key, c2 int not null, "
        + "c3 int not null, c4 int not null, c5 int not null)");
    st.execute("create index i1 on t1 (c5)");
    conn.commit();

    final int numRows = 20;
    PreparedStatement pstmt = conn
        .prepareStatement("insert into t1 values(?,?,?,?,?)");
    for (int c1 = 1; c1 <= numRows; c1++) {
      pstmt.setInt(1, c1);
      pstmt.setInt(2, c1);
      pstmt.setInt(3, c1);
      pstmt.setInt(4, c1);
      pstmt.setInt(5, c1);
      pstmt.executeUpdate();

    }
    conn.commit();

    // create a read-write conflict that will throw on commit

    ResultSet rs = st.executeQuery("select * from t1");
    while (rs.next()) {
      assertEquals(rs.getInt(1), rs.getInt(2));
    }
    rs.close();

    Connection conn2 = TestUtil.getConnection();
    conn2.setTransactionIsolation(getIsolationLevel());
    conn2.setAutoCommit(false);
    Statement st2 = conn2.createStatement();
    assertEquals(1, st2.executeUpdate("update t1 set c5 = 5 where c5 = 1"));
    try {
      conn2.commit();
      fail("expected conflict exception");
    } catch (SQLException sqle) {
      if (!"X0Z02".equals(sqle.getSQLState())) {
        throw sqle;
      }
    }

    assertEquals(1, st.executeUpdate("update t1 set c5 = 5 where c5 = 1"));
    conn.commit();

    boolean foundOne = false;
    rs = st.executeQuery("select * from t1 where c5 = 5");
    for (int i = 1; i <= 2; i++) {
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(5));
      if (foundOne) {
        assertEquals(5, rs.getInt(1));
      }
      else if (rs.getInt(1) == 1) {
        foundOne = true;
      }
      else {
        assertEquals(5, rs.getInt(1));
      }
    }
    assertFalse(rs.next());

    assertEquals(1, st.executeUpdate("update t1 set c5 = 10 where c1 = 1"));
    conn.commit();

    rs = st.executeQuery("select * from t1 where c5 = 10");
    foundOne = false;
    for (int i = 1; i <= 2; i++) {
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(5));
      if (foundOne) {
        assertEquals(10, rs.getInt(1));
      }
      else if (rs.getInt(1) == 1) {
        foundOne = true;
      }
      else {
        assertEquals(10, rs.getInt(1));
      }
    }
    conn.commit();
  }

  @Override
  protected int getIsolationLevel() {
    return Connection.TRANSACTION_REPEATABLE_READ;
  }
}

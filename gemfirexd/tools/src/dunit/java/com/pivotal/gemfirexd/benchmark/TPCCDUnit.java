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
package com.pivotal.gemfirexd.benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

/**
 * 
 * @author Soubhik Chakraborty
 */
@SuppressWarnings("serial")
public class TPCCDUnit extends DistributedSQLTestBase {

  public TPCCDUnit(String name) {
    super(name);
  }

  public void testTicket40864_1() throws Exception {
    startVMs(1, 2);

    clientSQLExecute(
        1,
        "create table customer ("
            + "c_w_id integer not null, c_d_id integer not null, c_id integer not null,"
            + " constraint pk_customer primary key (c_w_id, c_d_id, c_id) ) "
            + "partition by column(c_w_id)");

    clientSQLExecute(
        1,
        "create table new_order ("
            + "no_w_id integer not null, no_d_id integer not null, no_o_id integer not null,"
            + " constraint pk_new_order primary key (no_w_id, no_d_id, no_o_id)) "
            + "partition by column (no_w_id) colocate with (customer)");

    clientSQLExecute(1,
        "create index ndx_neworder_w_id_d_id on new_order (no_w_id, no_d_id)");

    populateTablesAndDelete(false);
  }

  private void populateTablesAndDelete(boolean largeData) throws SQLException,
      InterruptedException {

    PreparedStatement customers = TestUtil.jdbcConn
        .prepareStatement("insert into customer values(?,?,?)");
    PreparedStatement neworders = TestUtil.jdbcConn
        .prepareStatement("insert into new_order values(?,?,?)");

    for (int w_id = 1; w_id <= 2; w_id++) {
      for (int d_id = 1; d_id <= 10; d_id++) {
        for (int c_id = 1; c_id <= (largeData ? 3000 : 100); c_id++) {
          customers.setInt(1, w_id);
          customers.setInt(2, d_id);
          customers.setInt(3, c_id);
          customers.addBatch();

          if (c_id > (largeData ? 2100 : 10)) {
            neworders.setInt(1, w_id);
            neworders.setInt(2, d_id);
            neworders.setInt(3, c_id);
            neworders.addBatch();
          }
        }
        getLogWriter().info("committing for d_id=" + d_id + " w_id=" + w_id);
        customers.executeBatch();
        neworders.executeBatch();
        customers.clearBatch();
        neworders.clearBatch();
      }
    }

    Thread terminals[] = new Thread[20];
    for (int i = terminals.length; i > 0; i--)
      terminals[i - 1] = new Thread(new ticket40864Query(i),
          "40864Query thread " + i);

    getLogWriter().info("spwaned all the threads");

    for (int i = terminals.length; i > 0; i--)
      terminals[i - 1].start();

    getLogWriter().info("started all the threads");

    for (int i = terminals.length; i > 0; i--)
      terminals[i - 1].join();
  }

  private class ticket40864Query implements Runnable {
    private final int threadNo;

    public ticket40864Query(int threadNo) {
      this.threadNo = threadNo;
    }

    public void run() {
      Connection dbConnect = null;
      try {
        dbConnect = TestUtil.getConnection();
      } catch (SQLException e1) {
        e1.printStackTrace();
      }

      int w_id = (threadNo % 2) + 1;
      boolean rowsEffected = false;
      int o_id = -1;
      PreparedStatement qryRow = null, delRow = null, chkRow = null;

      for (int d_id = 1; d_id <= 10; d_id++) {
        do {
          o_id = -1;

          try {
            if (qryRow == null) {
              qryRow = dbConnect
                  .prepareStatement("SELECT no_o_id FROM new_order "
                      + " WHERE no_d_id = ? AND no_w_id = ? ORDER BY no_o_id ASC");
            }

            qryRow.setInt(1, d_id);
            qryRow.setInt(2, w_id);
            ResultSet rs = qryRow.executeQuery();
            if (rs.next())
              o_id = rs.getInt("no_o_id");

            if (o_id == -1) {
              continue;
            }

            if (delRow == null) {
              delRow = dbConnect.prepareStatement("DELETE FROM new_order "
                  + " WHERE no_d_id = ? AND no_w_id = ? AND no_o_id = ?");
            }
            delRow.setInt(1, d_id);
            delRow.setInt(2, w_id);
            delRow.setInt(3, o_id);

            int result = delRow.executeUpdate();
            getLogWriter().info(
                "check: Delete d_id=" + d_id + " w_id=" + w_id + " o_id="
                    + o_id + " effected " + result + " rows");
            if (result != 0)
              rowsEffected = true;

            if (chkRow == null) {
              chkRow = dbConnect
                  .prepareStatement("SELECT no_d_id FROM new_order "
                      + " WHERE no_d_id = ? AND no_w_id = ? AND no_o_id = ?");
            }
            chkRow.setInt(1, d_id);
            chkRow.setInt(2, w_id);
            chkRow.setInt(3, o_id);
            ResultSet chkrs = chkRow.executeQuery();
            assertFalse("DELETE should have deleted row o_id=" + o_id + " d_id"
                + d_id + " w_id=" + w_id, chkrs.next());
          } catch (SQLException e) {
            getLogWriter().error(e.toString());
          }

        } while (o_id != -1 && !rowsEffected);
        if (rowsEffected == false) {
          getLogWriter().error("DELETE never returned non-zero rows effected");
        }
      }

    }
  }

}

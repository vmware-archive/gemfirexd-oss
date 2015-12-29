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
package sql.schemas.seats;

import hydra.Log;

import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.SQLException;

import sql.schemas.Transactions;
import sql.sqlutil.SQLStmt;
import util.TestException;

public class UpdateCustomer extends Transactions {
  public final SQLStmt GetCustomerIdStr = new SQLStmt(
      "SELECT C_ID " +
      "  FROM CUSTOMER " +
      " WHERE C_ID_STR = ? "
  );
  
  public final SQLStmt GetCustomer = new SQLStmt(
      "SELECT * " +
      "  FROM CUSTOMER " +
      " WHERE C_ID = ? "
  );
  
  public final SQLStmt GetBaseAirport = new SQLStmt(
      "SELECT * " +
      "  FROM AIRPORT, COUNTRY " +
      " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
  );
  
  public final SQLStmt UpdateCustomer = new SQLStmt(
      "UPDATE CUSTOMER " +
      "   SET C_IATTR00 = ?, " +
      "       C_IATTR01 = ? " +
      (SeatsTest.addTxId? ", TXID = " + SeatsTest.curTxId.get() : "") +
      " WHERE C_ID = ?"
  );
  
  public final SQLStmt GetFrequentFlyers = new SQLStmt(
      "SELECT * FROM FREQUENT_FLYER " +
      " WHERE FF_C_ID = ?"
  );
          
  public final SQLStmt UpdatFrequentFlyers = new SQLStmt(
      "UPDATE FREQUENT_FLYER " +
      "   SET FF_IATTR00 = ?, " +
      "       FF_IATTR01 = ? " +
      (SeatsTest.addTxId? ", TXID = " + SeatsTest.curTxId.get() : "") +
      " WHERE FF_C_ID = ? " + 
      "   AND FF_AL_ID = ? "
  );
  
  public void doTxn(Connection conn, Long c_id, String c_id_str, Long update_ff, 
      long attr0, long attr1) throws SQLException {
      // Use C_ID_STR to get C_ID
      if (c_id == null) {
        assert(c_id_str != null);
        assert(c_id_str.isEmpty() == false);
        ResultSet rs = this.getPreparedStatement(conn, GetCustomerIdStr, c_id_str).executeQuery();
        if (rs.next()) {
          c_id = rs.getLong(1);
        } else {
          rs.close();
          throw new TestException(String.format("No Customer information record found for string '%s'", c_id_str));
        }
        rs.close();
      }
      assert(c_id != null);
      
      ResultSet rs = this.getPreparedStatement(conn, GetCustomer, c_id).executeQuery();
      if (rs.next() == false) {
        rs.close();
        throw new TestException(String.format("No Customer information record found for id '%d'", c_id));
      }
      assert(c_id == rs.getLong(1));
      long base_airport = rs.getLong(3);
      rs.close();
      
      // Get their airport information
      // TODO: Do something interesting with this data
      ResultSet airport_results = this.getPreparedStatement(conn, GetBaseAirport, base_airport).executeQuery();
      boolean adv = airport_results.next();
      airport_results.close();
      assert(adv);
      
      if (update_ff != null) {
        ResultSet ff_results = this.getPreparedStatement(conn, GetFrequentFlyers, c_id).executeQuery(); 
        while (ff_results.next()) {
          long ff_al_id = ff_results.getLong(2); 
            this.getPreparedStatement(conn, UpdatFrequentFlyers, attr0, attr1, c_id, ff_al_id).executeUpdate();
        } // WHILE
        ff_results.close();
      }
      
      int updated = this.getPreparedStatement(conn, UpdateCustomer, attr0, attr1, c_id).executeUpdate();
      if (updated != 1) {
        String msg = String.format("Failed to update customer #%d - Updated %d records", c_id, updated);
        throw new TestException(msg);
      }
      
      return;
  }
}

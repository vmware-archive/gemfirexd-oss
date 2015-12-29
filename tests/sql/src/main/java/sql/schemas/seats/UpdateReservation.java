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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import sql.schemas.Transactions;
import sql.sqlutil.SQLStmt;
import util.TestException;

public class UpdateReservation extends Transactions {
  public final SQLStmt CheckSeat = new SQLStmt(
    "SELECT R_ID " +
    "  FROM RESERVATION " +
    " WHERE R_F_ID = ? and R_SEAT = ?");

  public final SQLStmt CheckCustomer = new SQLStmt(
    "SELECT R_ID " + 
    "  FROM RESERVATION " +
    " WHERE R_F_ID = ? AND R_C_ID = ?");

  private static final String BASE_SQL = "UPDATE RESERVATION " +
                                         "   SET R_SEAT = ?, %s = ? " +
                                         (SeatsTest.addTxId? ", TXID = " + SeatsTest.curTxId.get() : "") +
                                         " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?";
  
  public final SQLStmt ReserveSeat0 = new SQLStmt(String.format(BASE_SQL, "R_IATTR00"));
  public final SQLStmt ReserveSeat1 = new SQLStmt(String.format(BASE_SQL, "R_IATTR01"));
  public final SQLStmt ReserveSeat2 = new SQLStmt(String.format(BASE_SQL, "R_IATTR02"));
  public final SQLStmt ReserveSeat3 = new SQLStmt(String.format(BASE_SQL, "R_IATTR03"));

  public static final int NUM_UPDATES = 4;
  public final SQLStmt ReserveSeats[] = {
    ReserveSeat0,
    ReserveSeat1,
    ReserveSeat2,
    ReserveSeat3,
  };
  
  public void doTxn(Connection conn, long r_id, long f_id, long c_id, long seatnum, 
      long newseatnum, long attr_idx, long attr_val) throws SQLException {
    assert(attr_idx >= 0);
    assert(attr_idx < ReserveSeats.length);
    boolean found;
    
    PreparedStatement stmt = null;
    ResultSet results = null;
    
    // Check if Seat is Available
    stmt = this.getPreparedStatement(conn, CheckSeat, f_id, newseatnum);
    results = stmt.executeQuery();
    found = results.next();
    results.close();
    if (found) {
      //this could occur as multiple nodes checking on flights
      Log.getLogWriter().info(String.format(" Seat %d is already reserved on flight #%d", seatnum, f_id));
    }
    // Check if the Customer already has a seat on this flight
    stmt = this.getPreparedStatement(conn, CheckCustomer, f_id, c_id);
    results = stmt.executeQuery();
    found = results.next();
    results.close();
    if (found == false) {
      throw new TestException(String.format(" Customer %d does not have an existing reservation on flight #%d", c_id, f_id));
    }
    
    // Update the seat reservation for the customer
    stmt = this.getPreparedStatement(conn, ReserveSeats[(int)attr_idx], newseatnum, attr_val, r_id, c_id, f_id);
    int updated = stmt.executeUpdate();
    if (updated != 1) {
      String msg = String.format("Failed to update reservation on flight %d for customer #%d - Updated %d records", f_id, c_id, updated);
      if (logDML) Log.getLogWriter().warning(msg);
      throw new TestException(msg);
    }
    
    if (logDML) Log.getLogWriter().info(String.format("Updated reservation on flight %d for customer %d", f_id, c_id));
    return;
  } 
}

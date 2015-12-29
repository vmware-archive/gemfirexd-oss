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

public class NewReservation extends Transactions {
  public final SQLStmt GetFlight = new SQLStmt(
      "SELECT F_AL_ID, F_SEATS_LEFT, AIRLINE.* " +
      "  FROM FLIGHT, AIRLINE" +
      " WHERE F_ID = ? AND F_AL_ID = AL_ID");

  public final SQLStmt GetCustomer = new SQLStmt(
        "SELECT C_BASE_AP_ID, C_BALANCE, C_SATTR00 " +
        "  FROM CUSTOMER" +
        " WHERE C_ID = ? ");
  
  public final SQLStmt CheckSeat = new SQLStmt(
        "SELECT R_ID " +
        "  FROM RESERVATION " +
        " WHERE R_F_ID = ? and R_SEAT = ?");
  
  public final SQLStmt CheckCustomer = new SQLStmt(
        "SELECT R_ID " + 
        "  FROM RESERVATION " +
        " WHERE R_F_ID = ? AND R_C_ID = ?");
  
  public final SQLStmt UpdateFlight = new SQLStmt(
        "UPDATE FLIGHT " +
        "   SET F_SEATS_LEFT = F_SEATS_LEFT - 1 " + 
        (SeatsTest.addTxId? ", TXID = " + SeatsTest.curTxId.get() : "") +
        " WHERE F_ID = ? ");
  
  public final SQLStmt UpdateCustomer = new SQLStmt(
        "UPDATE CUSTOMER " +
        "   SET C_BALANCE = C_BALANCE + ?, " +
        "       C_IATTR10 = C_IATTR10 + 1, " + 
        "       C_IATTR11 = C_IATTR11 + 1, " +
        "       C_IATTR12 = ?, " +
        "       C_IATTR13 = ?, " +
        "       C_IATTR14 = ?, " +
        "       C_IATTR15 = ? " +
        (SeatsTest.addTxId? ", TXID = " + SeatsTest.curTxId.get() : "") +
        " WHERE C_ID = ? ");
  
  public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
        "UPDATE FREQUENT_FLYER " +
        "   SET FF_IATTR10 = FF_IATTR10 + 1, " + 
        "       FF_IATTR11 = ?, " +
        "       FF_IATTR12 = ?, " +
        "       FF_IATTR13 = ?, " +
        "       FF_IATTR14 = ? " +
        (SeatsTest.addTxId? ", TXID = " + SeatsTest.curTxId.get() : "") +
        " WHERE FF_C_ID = ? " +
        "   AND FF_AL_ID = ?");
  
  public final SQLStmt InsertReservation = new SQLStmt(
        "INSERT INTO RESERVATION (" +
        "   R_ID, " +
        "   R_C_ID, " +
        "   R_F_ID, " +
        "   R_SEAT, " +
        "   R_PRICE, " +
        "   R_IATTR00, " +
        "   R_IATTR01, " +
        "   R_IATTR02, " +
        "   R_IATTR03, " +
        "   R_IATTR04, " +
        "   R_IATTR05, " +
        "   R_IATTR06, " +
        "   R_IATTR07, " +
        "   R_IATTR08 " +
        (SeatsTest.addTxId? ", TXID " : "") +
        ") VALUES (" +
        "   ?, " +  // R_ID
        "   ?, " +  // R_C_ID
        "   ?, " +  // R_F_ID
        "   ?, " +  // R_SEAT
        "   ?, " +  // R_PRICE
        "   ?, " +  // R_ATTR00
        "   ?, " +  // R_ATTR01
        "   ?, " +  // R_ATTR02
        "   ?, " +  // R_ATTR03
        "   ?, " +  // R_ATTR04
        "   ?, " +  // R_ATTR05
        "   ?, " +  // R_ATTR06
        "   ?, " +  // R_ATTR07
        "   ? " +   // R_ATTR08
        (SeatsTest.addTxId? ", " + SeatsTest.curTxId.get(): "") +  
        ")");
  
  public boolean doTxn(Connection conn, long r_id, long c_id, long f_id, long seatnum, 
      double price, long attrs[]) throws SQLException {    
    // Flight Information
    PreparedStatement stmt = this.getPreparedStatement(conn, GetFlight, f_id);
    ResultSet results = stmt.executeQuery();
    if (!results.next()) {
      throw new TestException(String.format(" Invalid flight #%d", f_id));
    } 

    long airline_id = results.getLong(1);
    long seats_left = results.getLong(2);
    results.close();
    if (seats_left <= 0) {
      //this is possible when multiple inserts of reservations from different clients
      Log.getLogWriter().warning(String.format(" No more seats available for flight #%d", f_id));
      return false;
    }
    // Check if Seat is Available
    stmt = this.getPreparedStatement(conn, CheckSeat, f_id, seatnum);
    results = stmt.executeQuery();
    if (results.next()) {
      //TODO, check with inserted seats from BB to verify this
      Log.getLogWriter().warning(String.format(" Seat %d is already reserved on flight #%d", seatnum, f_id));
      return false;
    }
    results.close();
    // Check if the Customer already has a seat on this flight
    stmt = this.getPreparedStatement(conn, CheckCustomer, f_id, c_id);
    results = stmt.executeQuery();
    if (results.next()) {
      //this is possible when multiple inserts of reservations from different clients
      //could use bb to track this if needed.
      Log.getLogWriter().warning(String.format(" Customer %d already owns on a reservations on flight #%d", c_id, f_id));
    }
    results.close();

    // Get Customer Information
    stmt = this.getPreparedStatement(conn, GetCustomer, c_id);
    results = stmt.executeQuery();
    if (!results.next()) {
      throw new TestException(String.format(" Invalid customer id: %d", c_id));
    }
    results.close();
 
    StringBuilder sb = new StringBuilder();
    stmt = this.getPreparedStatement(conn, InsertReservation);
    stmt.setLong(1, r_id);
    stmt.setLong(2, c_id);
    stmt.setLong(3, f_id);
    stmt.setLong(4, seatnum);
    stmt.setDouble(5, price);
    sb.append("setting R_ID:" + r_id + "  R_C_ID:" + c_id + "  R_F_ID:" + f_id  + 
        " R_SEAT:" + seatnum + " R_PRICE:" + price );
    
    for (int i = 0; i < attrs.length; i++) {
      stmt.setLong(6 + i, attrs[i]);
      sb.append(" R_IATTR0" + i + ":" + attrs[i]);
    } // FOR
    if (logDML) Log.getLogWriter().info(sb.toString());
    int updated = stmt.executeUpdate();
    if (updated != 1) {
      String msg = String.format("Failed to add reservation for flight #%d - Inserted %d records for InsertReservation", f_id, updated);
      if (logDML) Log.getLogWriter().warning(msg);
      throw new TestException(msg); 
      //only works for transaction, which is default isolation for seats schema
      //otherwise HA could cause update count to be wrong
    }
    
    updated = this.getPreparedStatement(conn, UpdateFlight, f_id).executeUpdate();
    if (updated != 1) {
        String msg = String.format("Failed to add reservation for flight #%d - Updated %d records for UpdateFlight", f_id, updated);
        if (logDML) Log.getLogWriter().warning(msg);
        throw new TestException(msg); 
        //only works for transaction, which is default isolation for seats schema
        //otherwise HA could cause update count to be wrong
    }
    
    updated = this.getPreparedStatement(conn, UpdateCustomer, price, attrs[0], attrs[1], attrs[2], attrs[3], c_id).executeUpdate();
    if (updated != 1) {
        String msg = String.format("Failed to add reservation for flight #%d - Updated %d records for UpdateCustomer", f_id, updated);
        if (logDML) Log.getLogWriter().warning(msg);
        throw new TestException(msg); 
        //only works for transaction, which is default isolation for seats schema
        //otherwise HA could cause update count to be wrong
    }
    
    // We don't care if we updated FrequentFlyer 
    updated = this.getPreparedStatement(conn, UpdateFrequentFlyer, attrs[4], attrs[5], attrs[6], attrs[7], c_id, airline_id).executeUpdate();
  //  if (updated != 1) {
  //      String msg = String.format("Failed to add reservation for flight #%d - Updated %d records for UpdateFre", f_id, updated);
  //      if (debug) LOG.warn(msg);
  //      throw new UserAbortException(ErrorType.VALIDITY_ERROR + " " + msg);
  //  }
  
    if (logDML) 
      Log.getLogWriter().info(String.format("Reserved new seat on flight %d for customer %d [seatsLeft=%d]",
                                f_id, c_id, seats_left-1));
    
    return true;
  }
}

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

import sql.schemas.Transactions;
import sql.sqlutil.SQLStmt;
import util.TestException;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FindOpenSeats extends Transactions {
  public final SQLStmt GetFlight = new SQLStmt(
      "SELECT F_STATUS, F_BASE_PRICE, F_SEATS_TOTAL, F_SEATS_LEFT, " +
      "       (F_BASE_PRICE + (F_BASE_PRICE * " +
      "(1 - (F_SEATS_LEFT / cast(F_SEATS_TOTAL as double))))) AS F_PRICE " +
      "  FROM FLIGHT WHERE F_ID = ?"
  );
  
  public final SQLStmt GetSeats = new SQLStmt(
      "SELECT R_ID, R_F_ID, R_SEAT " + 
      "  FROM RESERVATION WHERE R_F_ID = ?"
  );
  
  protected boolean reproduce51122 = false;
  
  public Object[][] doTxn(Connection conn, long f_id) throws SQLException {
    // 150 seats
    final long seatmap[] = new long[]
      {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,     
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
       -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    assert(seatmap.length == SeatsTest.FLIGHTS_NUM_SEATS);
      
    // First calculate the seat price using the flight's base price
    // and the number of seats that remaining
    PreparedStatement f_stmt = this.getPreparedStatement(conn, GetFlight);
    f_stmt.setLong(1, f_id);
    if (logDML) Log.getLogWriter().info("F_ID:" + f_id);
    ResultSet f_results = f_stmt.executeQuery();
    double base_price;
    long seats_total;
    long seats_left;
    double seat_price;
    if (f_results.next()) {
      // long status = results[0].getLong(0);
      base_price = f_results.getDouble(2);
      seats_total = f_results.getLong(3);
      seats_left = f_results.getLong(4);
      seat_price = f_results.getDouble(5);
      f_results.close();
    } else {
      throw new TestException("does not get result for " + GetFlight.getSQL() + 
          " for F_ID:" + f_id);
    }
    
    double _seat_price = base_price + (base_price * (1.0 - (seats_left/(double)seats_total)));
    if (logDML) Log.getLogWriter().info(String.format("Flight %d - SQL[%.2f] <-> JAVA[%.2f] [basePrice=%f, total=%d, left=%d]",
                                f_id, seat_price, _seat_price, base_price, seats_total, seats_left));
    
    // Then build the seat map of the remaining seats
    PreparedStatement s_stmt = this.getPreparedStatement(conn, GetSeats);
    s_stmt.setLong(1, f_id);
    ResultSet s_results = s_stmt.executeQuery();
    while (s_results.next()) {
      long r_id = s_results.getLong(1);
      int seatnum = s_results.getInt(3);
      if (logDML) Log.getLogWriter().info(String.format("Reserved Seat: fid %d / rid %d / seat %d", f_id, r_id, seatnum));
      if (seatmap[seatnum] != -1) throw new TestException("Duplicate seat reservation: R_ID=" + r_id);
      seatmap[seatnum] = 1;
    } // WHILE
    s_results.close();
    //could use select distinct (R_SEAT) FROM RESERVATION WHERE R_F_ID = ?"
    //and select r_seat from reservation where r_f_id = ? and compare results

    int ctr = 0;
    Object[][] returnResults = new Object[SeatsTest.FLIGHTS_NUM_SEATS][];
    for (int i = 0; i < seatmap.length; ++i) {
      if (seatmap[i] == -1) {
        // Charge more for the first seats
        double price = seat_price * (i < SeatsTest.FLIGHTS_FIRST_CLASS_OFFSET ? 2.0 : 1.0);
        if (!reproduce51122) price = Math.round(price*100)/100f;
        Object[] row = new Object[]{ f_id, i, price };
        returnResults[ctr++] = row;
        if (ctr == returnResults.length) break;
      }
    } // FOR
//      assert(seats_left == returnResults.getRowCount()) :
//          String.format("Flight %d - Expected[%d] != Actual[%d]", f_id, seats_left, returnResults.getRowCount());
   
    return returnResults;
  }
}

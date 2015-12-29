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

public class FindFlight extends Transactions {
  public final SQLStmt GetNearbyAirports = new SQLStmt(
      "SELECT * " +
      "  FROM AIRPORT_DISTANCE WHERE D_AP_ID0 = ? " +
      "  AND D_DISTANCE <= ? " +
      " ORDER BY D_DISTANCE ASC "
  );
  
  public final SQLStmt GetAirportInfo = new SQLStmt(
      "SELECT AP_CODE, AP_NAME, AP_CITY, AP_LONGITUDE, AP_LATITUDE, " +
            " CO_ID, CO_NAME, CO_CODE_2, CO_CODE_3 " +
       " FROM AIRPORT, COUNTRY " +
      " WHERE AP_ID = ? AND AP_CO_ID = CO_ID "
  );

  private final static String BaseGetFlights =
    "SELECT F_ID, F_AL_ID, F_SEATS_LEFT, " +
          " F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, " +
          " AL_NAME, AL_IATTR00, AL_IATTR01 " +
     " FROM FLIGHT, AIRLINE " +
    " WHERE F_DEPART_AP_ID = ? " +
    "   AND F_DEPART_TIME >= ? AND F_DEPART_TIME <= ? " +
    "   AND F_AL_ID = AL_ID " +
    "   AND F_ARRIVE_AP_ID IN (??)";

  public final SQLStmt GetFlights1 = new SQLStmt(BaseGetFlights, 1);
  public final SQLStmt GetFlights2 = new SQLStmt(BaseGetFlights, 2);
  public final SQLStmt GetFlights3 = new SQLStmt(BaseGetFlights, 3);
  
  public List<Object[]> doTxn(Connection conn, long depart_aid, long arrive_aid, Timestamp start_date, Timestamp end_date, long distance) throws SQLException {
    assert(start_date.equals(end_date) == false);
    
    final List<Long> arrive_aids = new ArrayList<Long>();
    arrive_aids.add(arrive_aid);
    
    final List<Object[]> finalResults = new ArrayList<Object[]>();
    
    if (distance > 0) {
      // First get the nearby airports for the departure and arrival cities
      PreparedStatement nearby_stmt = this.getPreparedStatement(conn, GetNearbyAirports, depart_aid, distance);
      ResultSet nearby_results = nearby_stmt.executeQuery();
      while (nearby_results.next()) {
        long aid = nearby_results.getLong(1);
        double aid_distance = nearby_results.getDouble(2); 
        if (logDML) Log.getLogWriter().info("DEPART NEARBY: " + aid + " distance=" + aid_distance + " miles");
        arrive_aids.add(aid);
      } // WHILE
      nearby_results.close();
    }
    
    // H-Store doesn't support IN clauses, so we'll only get nearby flights to nearby arrival cities
    int num_nearby = arrive_aids.size(); 
    if (num_nearby > 0) {
      PreparedStatement f_stmt = null;
      if (num_nearby == 1) {
        f_stmt = this.getPreparedStatement(conn, GetFlights1);
        Log.getLogWriter().info("executing " + GetFlights1);
      } else if (num_nearby == 2) {
        f_stmt = this.getPreparedStatement(conn, GetFlights2);
        Log.getLogWriter().info("executing " + GetFlights2);
      } else {
        f_stmt = this.getPreparedStatement(conn, GetFlights3);
        Log.getLogWriter().info("executing " + GetFlights3);
      }
      assert(f_stmt != null);
      
      StringBuilder sb = new StringBuilder();
      // Set Parameters
      sb.append("setting F_DEPART_AP_ID:" + depart_aid);
      f_stmt.setLong(1, depart_aid);
      sb.append(", F_DEPART_TIME:" + start_date);
      f_stmt.setTimestamp(2, start_date);
      sb.append(", F_DEPART_TIME:" + end_date);
      f_stmt.setTimestamp(3, end_date);
      sb.append(", F_ARRIVE_AP_ID IN(");
      for (int i = 0, cnt = Math.min(3, num_nearby); i < cnt; i++) {
        long aid = arrive_aids.get(i);
        f_stmt.setLong(4 + i, aid);
        if (i < cnt -1) sb.append(aid + ", ");
        else sb.append(aid + ")");
      } 
      if (logDML) Log.getLogWriter().info(sb.toString()); 
        
      // Process Result
      ResultSet flightResults = f_stmt.executeQuery();
      
      Log.getLogWriter().info("executing " + GetAirportInfo);
      PreparedStatement ai_stmt = this.getPreparedStatement(conn, GetAirportInfo); 
      ResultSet ai_results = null;
      while (flightResults.next()) {
        long f_depart_airport = flightResults.getLong(4);
        long f_arrive_airport = flightResults.getLong(6);
        
        Object row[] = new Object[13];
        int r = 0;
        
        row[r++] = flightResults.getLong(1);    // [00] F_ID
        row[r++] = flightResults.getLong(3);    // [01] SEATS_LEFT
        row[r++] = flightResults.getString(8);  // [02] AL_NAME
        
        // DEPARTURE AIRPORT
        ai_stmt.setLong(1, f_depart_airport);
        if (logDML) Log.getLogWriter().info("setting AP_ID = " + f_depart_airport);
        ai_results = ai_stmt.executeQuery();
        boolean adv = ai_results.next();
        assert(adv);
        row[r++] = flightResults.getDate(5);    // [03] DEPART_TIME
        row[r++] = ai_results.getString(1);     // [04] DEPART_AP_CODE
        row[r++] = ai_results.getString(2);     // [05] DEPART_AP_NAME
        row[r++] = ai_results.getString(3);     // [06] DEPART_AP_CITY
        row[r++] = ai_results.getString(7);     // [07] DEPART_AP_COUNTRY
        ai_results.close();
        
        // ARRIVAL AIRPORT
        ai_stmt.setLong(1, f_arrive_airport);
        ai_results = ai_stmt.executeQuery();
        adv = ai_results.next();
        assert(adv);
        row[r++] = flightResults.getDate(7);    // [08] ARRIVE_TIME
        row[r++] = ai_results.getString(1);     // [09] ARRIVE_AP_CODE
        row[r++] = ai_results.getString(2);     // [10] ARRIVE_AP_NAME
        row[r++] = ai_results.getString(3);     // [11] ARRIVE_AP_CITY
        row[r++] = ai_results.getString(7);     // [12] ARRIVE_AP_COUNTRY
        ai_results.close();
        
        finalResults.add(row);
        if (logDML)
          Log.getLogWriter().info(String.format("Flight %d / %s /  %s -> %s / %s",
                                    row[0], row[2], row[4], row[9], row[03]));
      } // WHILE
      flightResults.close();
    }
    if (logDML) {
      for (Object[] row: finalResults) {
        StringBuilder sb= new StringBuilder();
        for (Object info: row) {
          sb.append(info + ",");
        }          
        Log.getLogWriter().info("Flight Information:" + sb.toString());
      }
    }
    return (finalResults);
  }
  
}

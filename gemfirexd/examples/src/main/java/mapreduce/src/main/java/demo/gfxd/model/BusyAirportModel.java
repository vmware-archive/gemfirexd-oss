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
package demo.gfxd.model;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Model of the BUSY_AIRPORT table:
 *
 * CREATE TABLE BUSY_AIRPORT
 *    (
 *        AIRPORT CHAR(3),
 *        FLIGHTS INTEGER,
 *        STAMP TIMESTAMP
 *    )
 *
 * This class is the glue that bridges the output of a reducer to an actual
 * SqlFire table.
 */
public class BusyAirportModel {

  private String airport;
  private int flights;
  private long timestamp;

  public BusyAirportModel(String airport, int flights) {
    this.airport = airport;
    this.flights = flights;
    this.timestamp = System.currentTimeMillis();
  }

  public void setFlights(int idx, PreparedStatement ps) throws SQLException {
    ps.setInt(idx, flights);
  }

  public void setAirport(int idx, PreparedStatement ps) throws SQLException {
    ps.setString(idx, airport);
  }

  public void setStamp(int idx, PreparedStatement ps) throws SQLException {
    ps.setTimestamp(idx, new Timestamp(timestamp));
  }
}

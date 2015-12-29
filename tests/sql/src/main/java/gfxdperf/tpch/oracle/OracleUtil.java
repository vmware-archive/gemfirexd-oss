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
package gfxdperf.tpch.oracle;

import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.jdbc.ClientAttribute;

import gfxdperf.PerfTestException;

import hydra.HostHelper;
import hydra.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleUtil {

  public static final String THIN_DRIVER = "oracle.jdbc.OracleDriver";
  public static final String PROTOCOL = "jdbc:oracle:thin:";

  /**
   * Opens a connection, making this thread a thin client. Uses the configured
   * transaction isolation and autocommit settings.
   */
  public static Connection openConnection() throws SQLException {
    String databaseServerHost = OraclePrms.getDatabaseServerHost();
    if (HostHelper.isLocalHost(databaseServerHost)) {
      databaseServerHost = "localhost";
    }
    String url = PROTOCOL
               + OraclePrms.getUser() + "/"
               + OraclePrms.getPassword() + "@"
               + databaseServerHost + ":"
               + OraclePrms.getDatabaseServerPort() + ":"
               + OraclePrms.getDatabaseName();
    Log.getLogWriter().info("Creating connection using " + THIN_DRIVER 
       + " with " + url); 
    System.setProperty("java.security.egd", "file:///dev/urandom");
    loadDriver(THIN_DRIVER);
    Connection conn = DriverManager.getConnection(url);
    conn.setAutoCommit(OraclePrms.getAutoCommit());
    conn.setTransactionIsolation(OraclePrms.getTxIsolation());
    Log.getLogWriter().info("Created connection for " + url
       + " using transaction isolation: "
       + OraclePrms.getTxIsolation(conn.getTransactionIsolation())
       + " autocommit: "
       + OraclePrms.getAutoCommit());
    return conn;
  }

  /**
   * Loads the given JDBC driver.
   */
  private static void loadDriver(String driver) {
    try {
      Class.forName(driver).newInstance();
    } catch (Exception e) {
      String s = "Problem loading JDBC driver: " + driver;
      throw new PerfTestException(s, e);
    }
  }
}

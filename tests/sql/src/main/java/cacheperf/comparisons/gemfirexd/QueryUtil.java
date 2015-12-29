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
package cacheperf.comparisons.gemfirexd;

import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.jdbc.ClientAttribute;

import hydra.HydraConfigException;
import hydra.Log;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.ThinClientHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import objects.query.QueryPrms;

public class QueryUtil
{  
  //--------------------------------------------------------------------------
  // GFXD
  //--------------------------------------------------------------------------
  public static Connection gfxdEmbeddedSetup(QueryPerfClient client)
  throws SQLException {
    Properties p = new Properties();
    return gfxdEmbeddedSetup(client, p);
  }
  
  public static Connection gfxdEmbeddedSetup(QueryPerfClient client, Properties p)
  throws SQLException {
    p.setProperty(Attribute.ENABLE_STATS, String.valueOf(QueryPerfPrms.enableStats()));
    p.setProperty(Attribute.ENABLE_TIMESTATS, String.valueOf(QueryPerfPrms.enableTimeStats()));
    return getConnection(client, "jdbc:gemfirexd:",
                         "com.pivotal.gemfirexd.jdbc.EmbeddedDriver", p);
  }

  public static Connection gfxdClientSetup(QueryPerfClient client)
  throws SQLException {
    if (QueryPerfPrms.useNetworkLocator()) {
      return gfxdClientLocatorSetup(client, NetworkServerHelper.getNetworkLocatorEndpoints());
    } else {
      return gfxdClientServerSetup(client, NetworkServerHelper.getNetworkServerEndpoints());
    }
  }
  
  public static Connection gfxdClientSetup(QueryPerfClient client, Properties p)
  throws SQLException {
    if (QueryPerfPrms.useNetworkLocator()) {
      return gfxdClientLocatorSetup(client, NetworkServerHelper.getNetworkLocatorEndpoints(), p);
    } else {
      return gfxdClientServerSetup(client, NetworkServerHelper.getNetworkServerEndpoints(), p);
    }
  }

  public static Connection gfxdWanClientSetup(QueryPerfClient client)
  throws SQLException {
    if (QueryPerfPrms.useNetworkLocator()) {
      return gfxdClientLocatorSetup(client, NetworkServerHelper.getNetworkLocatorEndpointsInWanSite());
    } else {
      return gfxdClientServerSetup(client, NetworkServerHelper.getNetworkServerEndpointsInWanSite());
    }
  }

  public static Connection gfxdWanClientSetup(QueryPerfClient client, List endpoints)
  throws SQLException {
    if (QueryPerfPrms.useNetworkLocator()) {
      return gfxdClientLocatorSetup(client, endpoints);
    } else {
      return gfxdClientServerSetup(client, endpoints);
    }
  }
  
  public static Connection gfxdClientLocatorSetup(QueryPerfClient client, List endpoints)
  throws SQLException {   
    return gfxdClientLocatorSetup(client, endpoints, null);
  }
  

  public static Connection gfxdClientLocatorSetup(QueryPerfClient client, List endpoints, Properties overrideProp)
  throws SQLException {
    if (endpoints.size() == 0) {
      String s = "No network locator endpoints found";
      throw new QueryPerfException(s);
    }
    Properties p = ThinClientHelper.getConnectionProperties();
    if (overrideProp != null) p.putAll(overrideProp);
    String url = "jdbc:gemfirexd://" + endpoints.get(0);
    List secondaryLocators = endpoints.subList(1, endpoints.size());
    if (secondaryLocators.size() > 0) {
      String secondaries = "";
      for (int i = 0; i < secondaryLocators.size(); i++) {
        if (i > 0) secondaries += ",";
        secondaries += secondaryLocators.get(i).toString();
      }
      p.setProperty(ClientAttribute.SECONDARY_LOCATORS, secondaries);
    }
    return getConnection(client, url, "com.pivotal.gemfirexd.jdbc.ClientDriver", p);
  }

  public static Connection gfxdClientServerSetup(QueryPerfClient client, List endpoints)
  throws SQLException {
    return gfxdClientServerSetup(client, endpoints, null);
  }
  
  public static Connection gfxdClientServerSetup(QueryPerfClient client, List endpoints, Properties overrideProp)
  throws SQLException {
    if (endpoints.size() == 0) {
      String s = "No network server endpoints found";
      throw new QueryPerfException(s);
    }
    Properties p = ThinClientHelper.getConnectionProperties();
    if (overrideProp != null) p.putAll(overrideProp);
    String url = "jdbc:gemfirexd://" + endpoints.get(0);
    return getConnection(client, url, "com.pivotal.gemfirexd.jdbc.ClientDriver", p);
  }

  private static Connection getConnection(QueryPerfClient client,
                            String protocol, String driver,
                            Properties p) throws SQLException {
    Log.getLogWriter().info("Creating connection using " + driver + " with " + protocol + " and " + p);
    loadDriver(driver);
    Connection conn = DriverManager.getConnection(protocol, p);
    configure(client, conn);
    return conn;
  }

  public static void closeGFXDConnection(Connection conn) {
    try {
      // the shutdown=true attribute shuts down Derby
      String protocol = "jdbc:gemfirexd:" + ";shutdown=true";
      DriverManager.getConnection(protocol);
    } catch (SQLException se) {
      if (((se.getErrorCode() == 50000)
          && ("XJ015".equals(se.getSQLState())))) {
        // we got the expected exception
      } else {
        // if the error code or SQLState is different, we have
        // an unexpected exception (shutdown failed)
        Log.getLogWriter().severe("Shutdown failed:" + se.getMessage());
      }
    }
  }
  
//--------------------------------------------------------------------------
  // GPDB
  //--------------------------------------------------------------------------
  public static Connection gpdbSetup(QueryPerfClient client)
  throws SQLException {
    loadDriver(gpdbDriver());
    String url = gpdbURL();
    Log.getLogWriter().info("Connecting to " + url);
    Connection conn = DriverManager.getConnection(url);
    configure(client, conn);
    return conn;
  }

  public static String gpdbDriver() {
    return "org.postgresql.Driver";
  }

  public static String gpdbURL() {
    String url = "jdbc:postgresql://" + QueryPerfPrms.getDatabaseServerHost()
               + ":" + QueryPerfPrms.getDatabaseServerPort()
               + "/" + QueryPerfPrms.getDatabaseName()
               + "?user=" + QueryPerfPrms.getUser();
    String password = QueryPerfPrms.getPassword();
    if (password != null) {
      url += "&password=" + password;
    }
    return url;
  }

  

  //--------------------------------------------------------------------------
  // MySQL
  //--------------------------------------------------------------------------
  public static Connection mySQLSetup(QueryPerfClient client)
  throws SQLException {
    loadDriver(mySQLDriver());
    String url = mySQLURL();
    Log.getLogWriter().info("Connecting to " + url);
    Connection conn = DriverManager.getConnection(url);
    configure(client, conn);
    return conn;
  }

  public static String mySQLDriver() {
    return "com.mysql.jdbc.Driver";
  }

  public static String mySQLURL() {
    String url = "jdbc:mysql://" + QueryPerfPrms.getDatabaseServerHost()
               + ":" + QueryPerfPrms.getDatabaseServerPort()
               + "/" + QueryPerfPrms.getDatabaseName()
               + "?user=" + QueryPerfPrms.getUser();
    String password = QueryPerfPrms.getPassword();
    if (password != null) {
      url += "&password=" + password;
    }
    return url;
  }

  //--------------------------------------------------------------------------
  // Oracle
  //--------------------------------------------------------------------------
  public static Connection oracleSetup(QueryPerfClient client)
  throws SQLException {
    System.setProperty("java.security.egd", "file:///dev/urandom");
    loadDriver(oracleDriver());
    String url = oracleURL();
    Log.getLogWriter().info("Connecting to " + url);
    Connection conn = DriverManager.getConnection(url);
    configure(client, conn);
    return conn;
  }

  public static String oracleDriver() {
    return "oracle.jdbc.OracleDriver";
  }

  public static String oracleURL() {
    String url = "jdbc:oracle:thin:"
               + QueryPerfPrms.getUser() + "/" + QueryPerfPrms.getPassword()
               + "@" + QueryPerfPrms.getDatabaseServerHost()
               + ":1521:" + QueryPerfPrms.getDatabaseName();
    return url;
  }

  //--------------------------------------------------------------------------
  // RTE
  //--------------------------------------------------------------------------
  public static Connection rteSetup(QueryPerfClient client)
  throws SQLException {
    loadDriver("com.gemstone.gemfire.sql.jdbcDriver");
    Properties endpoints = new Properties();
    String host = System.getProperty( "host", "hs20h.gemstone.com" );
    String port = System.getProperty( "port", "50005" );
    endpoints.setProperty( "endpoints", "endone=" + host + ":" + port );
    Connection conn = DriverManager.getConnection("jdbc:gfsql:", endpoints);
    return conn;
  }

  //--------------------------------------------------------------------------
  // Connection configuration
  //--------------------------------------------------------------------------

  private static void configure(QueryPerfClient client, Connection conn)
  throws SQLException {
    conn.setAutoCommit(false);
    if (client.txIsolation == QueryPerfPrms.TRANSACTION_NONE) {
      if (client.queryAPI == QueryPrms.MYSQL ||
          client.queryAPI == QueryPrms.MYSQLC ||
          client.queryAPI == QueryPrms.ORACLE ||
          client.queryAPI == QueryPrms.GPDB) {
        String s = QueryPrms.getAPIString(client.queryAPI)
                 + " does not support transaction isolation level \"none\"";
        throw new HydraConfigException(s);
      }
    }
    conn.setTransactionIsolation(client.txIsolation);
    Log.getLogWriter().info("Using transaction isolation level: " +
        QueryPerfPrms.getTxIsolation(conn.getTransactionIsolation()));
  }

  //--------------------------------------------------------------------------
  // Drivers
  //--------------------------------------------------------------------------

  /**
   * The JDBC driver is loaded by loading its class.  If you are using JDBC 4.0
   * (Java SE 6) or newer, JDBC drivers may be automatically loaded, making
   * this code optional.
   * <p>
   * In an embedded environment, any static Derby system properties
   * must be set before loading the driver to take effect.
   */
  public static void loadDriver(String driver) {
    try {
      Class.forName(driver).newInstance();
    } catch (ClassNotFoundException e) {
      String s = "Problem loading JDBC driver: " + driver;
      throw new QueryPerfException(s, e);
    } catch (InstantiationException e) {
      String s = "Problem loading JDBC driver: " + driver;
      throw new QueryPerfException(s, e);
    } catch (IllegalAccessException e) {
      String s = "Problem loading JDBC driver: " + driver;
      throw new QueryPerfException(s, e);
    }
  }
}

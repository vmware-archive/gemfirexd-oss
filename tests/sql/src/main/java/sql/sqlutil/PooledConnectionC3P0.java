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
package sql.sqlutil;

import hydra.Log;
import hydra.TestConfig;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.NetworkServerHelper.Endpoint;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import sql.GFEDBClientManager;
import sql.SQLPrms;
import sql.GFEDBManager.Isolation;
import util.TestException;
import util.TestHelper;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class PooledConnectionC3P0 extends GFEDBClientManager {
  private static ComboPooledDataSource ds;
  private static Properties connProp = new Properties();
  private static int numOfWorkers = TestConfig.tab().intAt(SQLPrms.numOfWorkers, 0);
  
  static {
    if (numOfWorkers == 0) throw new TestException("numOfWorkers needs to be set to run this test");
  }
  
  public static Connection getConnection() throws SQLException {
    if (ds == null) {
      getDataSource(Isolation.NONE);
    }
    return ds.getConnection();
  }
  
  public static Connection getRCConnection() throws SQLException {
    if (ds == null) {
      getDataSource(Isolation.READ_COMMITTED);
    }
    return ds.getConnection();
  }
  
  public static Connection getRRConnection() throws SQLException {
    if (ds == null) {
      getDataSource(Isolation.REPEATABLE_READ);
    }
    return ds.getConnection();
  }
  
  public static Connection getRCConnection(Properties p) throws SQLException {
    if (ds == null) {
      connProp.putAll(p);
      getDataSource(Isolation.READ_COMMITTED);
    }
    return ds.getConnection();
  }
  
  public static Connection getRRConnection(Properties p) throws SQLException {
    if (ds == null) {
      connProp.putAll(p);
      getDataSource(Isolation.REPEATABLE_READ);
    }
    return ds.getConnection();
  }
  
  public synchronized static ComboPooledDataSource getDataSource(Isolation isolation) {
    if (ds == null)
      setupDataSource(isolation);
  
    return ds;
  }
  
  @SuppressWarnings("unchecked")
  private static void setupDataSource(Isolation isolation) {
    ds = new ComboPooledDataSource();
    Endpoint locatorEndPoint = (Endpoint) (NetworkServerHelper.getNetworkLocatorEndpoints()).get(0);
    String hostname = getHostNameFromEndpoint(locatorEndPoint);
    int port = getPortFromEndpoint(locatorEndPoint); 
    
    connProp.putAll(getExtraConnProp()); //singlehop conn properties and any additional prop
    
    try {
      ds.setProperties(connProp); 
      ds.setDriverClass(driver);
      ds.setJdbcUrl(protocol + hostname+ ":" + port);
      
      ds.setMinPoolSize(5);
      ds.setAcquireIncrement(5);
      ds.setMaxPoolSize(numOfWorkers + 100);
      ds.setMaxStatementsPerConnection(10);
      
      if (isolation == Isolation.NONE) {
        ds.setConnectionCustomizerClassName( "sql.sqlutil.MyConnectionCustomizer");
      } else if (isolation == Isolation.READ_COMMITTED) {
        ds.setConnectionCustomizerClassName("sql.sqlutil.IsolationRCConnectionCustomizer");
      } else {
        ds.setConnectionCustomizerClassName("sql.sqlutil.IsolationRRConnectionCustomizer");
      }
      
      Log.getLogWriter().info("Pooled data source url is set as " + ds.getJdbcUrl());
      
      StringBuilder sb = new StringBuilder();
      
      for (Iterator iter = connProp.entrySet().iterator(); iter.hasNext(); ) {
        Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
        
        sb.append(entry.getKey() + " is set to " + entry.getValue() +"\n");
      }
      
      Log.getLogWriter().info("Pooled data source setting the following connection prop: " + sb.toString());
      
    } catch (Exception e) {
      throw new TestException("could not set data source" + TestHelper.getStackTrace(e));
    }
  }
  
  

}

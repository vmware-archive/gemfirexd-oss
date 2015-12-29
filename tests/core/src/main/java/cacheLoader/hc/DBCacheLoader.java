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

package cacheLoader.hc;

import com.gemstone.gemfire.cache.*;

import java.sql.*;
import java.util.Properties;

import oracle.jdbc.pool.*;

import hydra.*;
import util.*;

public class DBCacheLoader implements CacheLoader {

  /**
   * the connection pool
   */
  private OracleDataSource cachedDS;

  /**
   * Initializes loader; specifically, creates ConnectionPool.
   */
  public void init() {
    this.cachedDS = createConnectionPool();
  }

  /**
   * Creates a database connection pool object using JDBC 2.0.
   */
  private OracleDataSource createConnectionPool() {
    try {
      // Create a OracleConnectionPoolDataSource instance.
      OracleDataSource ds = new OracleDataSource();
      ds.setConnectionCacheName("oraCache");
      ds.setConnectionCachingEnabled(true);
      ds.setURL(TestConfig.tab().stringAt(DBParms.jdbcUrl));
      ds.setUser(TestConfig.tab().stringAt(DBParms.rdbUser));
      ds.setPassword(TestConfig.tab().stringAt(DBParms.rdbPassword));
      Properties cacheProps = new Properties();
      cacheProps.setProperty("MinLimit",
          String.valueOf(TestConfig.tab().intAt(DBParms.poolMinLimit)));
      ds.setConnectionCacheProperties(cacheProps);
      return ds;
    } catch (Exception ex) {
      Log.getLogWriter().info("Unable to create connection pool: " + ex, ex);
      throw new HydraRuntimeException("Problem creating connection pool", ex);
    }
  }

  /**
   * Returns an array of bytes containing the RDB blob data
   * corresponding to the entry name, which must be a String
   * that can be parsed as an int.
   */
  public Object load(LoaderHelper helper) {
    int sleepMs = DBParms.getSleepMs();
    int key = Integer.parseInt((String)helper.getKey());
    boolean logDetails = BridgeParms.getLogDetails();
    byte[] value;
    if (sleepMs > 0) {
      if (logDetails) {
        Log.getLogWriter().info("Sleeping for " + sleepMs
            + " ms instead of looking up key " + key);
      }
      MasterController.sleepForMs(sleepMs);
      value = Util.intTObytes(key);
    } else {
      try {
        if (logDetails) {
          Log.getLogWriter()
              .info("DBCacheLoader trying to getBlob for " + key);
        }
        Connection conn = this.cachedDS.getConnection();
        value = Util.getBlob(conn, key);
        conn.close();
      } catch (SQLException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
    return value;
  }

  public void close() {
  }
}

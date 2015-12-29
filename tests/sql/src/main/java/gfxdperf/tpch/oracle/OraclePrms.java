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

import hydra.BasePrms;
import hydra.EnvHelper;
import hydra.FileUtil;
import hydra.HydraConfigException;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * A class used to store keys for test configuration settings.
 */
public class OraclePrms extends BasePrms {
  
  static {
    setValues(OraclePrms.class);
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Autocommit setting. Defaults to false.
   */
  public static Long autocommit;

  public static boolean getAutoCommit() {
    Long key = autocommit;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
//------------------------------------------------------------------------------

  /**
   * (String)
   * Database name used in connection URL.
   */
  public static Long databaseName;

  public static String getDatabaseName() {
    Long key = databaseName;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s);
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Database server host used in connection URL.
   */
  public static Long databaseServerHost;

  public static String getDatabaseServerHost() {
    Long key = databaseServerHost;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s);
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Database server port used in connection URL.
   */
  public static Long databaseServerPort;

  public static String getDatabaseServerPort() {
    Long key = databaseServerPort;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s);
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Oracle home.
   */
  public static Long home;

  public static String getHome() {
    Long key = home;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s);
    }
    return val;
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Password used in connection URL. Can be specified as {@link #NONE}, in
   * which case no password is used.
   */
  public static Long password;

  public static String getPassword() {
    Long key = password;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s);
    }
    return val.equalsIgnoreCase(BasePrms.NONE) ? null : val;
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Transaction isolation. Defaults to "none".
   */
  public static Long txIsolation;

  public static int getTxIsolation() {
    Long key = txIsolation;
    String val = tasktab().stringAt(key, tab().stringAt(key, "none"));
    return getTxIsolation(key, val);
  }

  private static int getTxIsolation(Long key, String val) {
    if (val.equalsIgnoreCase("read_committed") ||
             val.equalsIgnoreCase("readCommitted")) {
      return Connection.TRANSACTION_READ_COMMITTED;
    }
    else if (val.equalsIgnoreCase("serializable")) {
      return Connection.TRANSACTION_SERIALIZABLE;
    }
    else {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  public static String getTxIsolation(int n) {
    switch (n) {
      case Connection.TRANSACTION_READ_COMMITTED:
           return "read_committed";
      case Connection.TRANSACTION_SERIALIZABLE:
           return "serializable";
      default:
        String s = "Unknown transaction isolation level: " + n;
        throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * User used in connection URL.
   */
  public static Long user;

  public static String getUser() {
    Long key = user;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    if (val == null) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s);
    }
    return val;
  }
}

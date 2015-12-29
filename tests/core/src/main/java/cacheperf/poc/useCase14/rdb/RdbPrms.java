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

package cacheperf.poc.useCase14.rdb;

import hydra.*;

/**
 * A class used to store keys for test configuration settings.
 */

public class RdbPrms extends BasePrms {

  /**
   * (String) Table name. Required.
   */
  public static Long tableName;
  public static String getTableName() {
    Long key = tableName;
    return tab().stringAt(key);
  }

  /**
   * (String) Driver name.
   */
  public static Long driverName;
  public static String getDriverName() {
    Long key = driverName;
    return tab().stringAt(key, "oracle.jdbc.driver.OracleDriver");
  }

  /**
   * (String) DB URL.
   */
  public static Long dbUrl;
  public static String getDbUrl() {
    Long key = dbUrl;
    return tab().stringAt(key, "jdbc:oracle:thin:@orahost:1521:or81");
  }

  /**
   * (String) User name.
   */
  public static Long userName;
  public static String getUserName() {
    Long key = userName;
    return tab().stringAt(key, "marilynd");
  }

  /**
   * (String) Password.
   */
  public static Long password;
  public static String getPassword() {
    Long key = password;
    return tab().stringAt(key, "marilynd");
  }

  //----------------------------------------------------------------------------
  //  Required stuff
  //----------------------------------------------------------------------------

  static {
    setValues(RdbPrms.class);
  }
  public static void main(String args[]) {
      dumpKeys();
  }
}

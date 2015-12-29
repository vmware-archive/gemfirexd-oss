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

import hydra.*;

public class DBParms extends BasePrms {

    /** (int) The number of values to preload in this slave */
    public static Long numPreload;

    /** (int) The number of bytes per blob value.
     *  NOTE: With the current driver-independent implmentation, 
     *  values > 4000 are not handled correctly by Oracle's thin driver.
     *  Specifically, Oracle loses the Blob value, and a resulting
     *  getBlob yields a Blob of length 0.  This is a known Oracle
     *  bug, and the only way around it is to use Oracle-specific
     *  classes (e.g. see Util.generateAndInsertBlob()).
     */
    public static Long numBytes;

    /** (String) The JDBC driver class name. */
    public static Long jdbcDriver;

    /** (String) The JDBC URL. */
    public static Long jdbcUrl;

    /** (String) The RDB user. */
    public static Long rdbUser;

    /** (String) The RDB password. */
    public static Long rdbPassword;

    /** (int) The pool minimum limit. */
    public static Long poolMinLimit;

    /** (boolean) Optional detailed JDBC Driver logging. */
    public static Long driverLogging;

    /** Returns the value of the driverLogging config parameter, 
     *  or false if the value is not specified.
     */
    public static boolean getDriverLogging() {
      try {
	  return TestConfig.tab().booleanAt(driverLogging);
      } catch (HydraConfigException ex) {
	  return false;
      }
    }

    /** (String) The name of the RDB table that holds the data.
     */
    public static Long tableName;

    /** Returns the tableName config parameter value, or "GemFireData"
     *  if the value is not specified.
     */
    public static String getTableName() {
      try {
	  return TestConfig.tab().stringAt(tableName);
      } catch (HydraConfigException ex) {
	  return "GemFireData";
      }
    }

    /** (String) The type of the key column in tableName.
     */
    public static Long keyColumnType;

    /** Returns the keyColumnType config parameter value, or 
     *  "number (10)" if the value is not specified.
     */
    public static String getKeyColumnType() {
      try {
	  return TestConfig.tab().stringAt(keyColumnType);
      } catch (HydraConfigException ex) {
	  return "number (10)";
      }
    }

    /** (String) The type of the value column in tableName.
     */
    public static Long valueColumnType;

    /** Returns the valueColumnType config parameter value, or 
     *  "blob" if the value is not specified.
     */
    public static String getValueColumnType() {
      try {
	  return TestConfig.tab().stringAt(valueColumnType);
      } catch (HydraConfigException ex) {
	  return "blob";
      }
    }

    /** (int) The sleep time, in milliseconds:
     *  if 0, DBCacheLoader.load calls Util.getBlob to get data;
     *  if not 0, DBCacheLoader.load sleeps instead.
     */
    public static Long sleepMs; 

    /** Returns sleepMs value, or 0 if not specified.
     */
    public static int getSleepMs() {
      try {
	  return TestConfig.tab().intAt(sleepMs);
      } catch (HydraConfigException ex) {
	  return 0;
      }
    }

    static {
	BasePrms.setValues(DBParms.class);
    }

}

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

package objects.query;

import hydra.BasePrms;
import hydra.HydraConfigException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;


/**
 * A class used to store keys for test configuration settings.
 */
public class QueryPrms extends BasePrms {

  public static final String OQL_API = "OQL";
  public static final String GFXD_API = "GFXD";
  public static final String GFE_API = "GFE";
  public static final String MYSQL_API = "MYSQL";
  public static final String MYSQLC_API = "MYSQLC";
  public static final String ORACLE_API = "ORACLE";
  public static final String GPDB_API = "GPDB";
  public static final String RTE_API = "RTE";

  public static final int OQL = 0;
  public static final int GFXD = 1;
  public static final int GFE = 2;
  public static final int MYSQL = 3;
  public static final int MYSQLC = 4;
  public static final int ORACLE = 5;
  public static final int RTE = 6;
  public static final int GPDB = 7;
  //these two api types are supposed to be temporary to help debug performance issues with gemfirexd
  public static final String GFE_GFK_DVD_API = "GFE_GFK_DVD";
  public static final String GFE_GFK_API = "GFE_GFK";
  public static final int GFE_GFK_DVD = 20;
  public static final int GFE_GFK = 21;

  public static final String BROKER_OBJECT_TYPE = "objects.query.broker.Broker";
  public static final String SECTOR_OBJECT_TYPE = "objects.query.sector.Sector";
  public static final String LARGE_OBJECT_OBJECT_TYPE = "objects.query.largeobject.LargeObject";
  public static final String TINY_OBJECT_OBJECT_TYPE = "objects.query.tinyobject.TinyObject";
  public static final String SECURITIES_TYPE = "objects.query.securities.Securities";
  
  public static final String REPLICATE_DATA_POLICY = "REPLICATE";
  public static final String PARTITION_DATA_POLICY = "PARTITION";
  public static final String NO_DATA_POLICY = "NONE";
  
  public static final int REPLICATE = 0;
  public static final int PARTITION = 1;
  public static final int NONE = 2;
  
  public static final String DEFAULT_PARTITION_TYPE = "PartitionByDefault";
  public static final String PARTITION_BY_PK_TYPE = "PartitionByPK";
  public static final String PARTITION_BY_COLUMN_TYPE = "PartitionByColumn";
  public static final String PARTITION_BY_RANGE_TYPE = "PartitionByRange";
  public static final String PARTITION_BY_LIST_TYPE = "PartitionByList";
  public static final String PARTITION_BY_LIST_SPECIAL_CASE_TYPE = "PartitionByListSpecialCase";

 
  public static final int PARTITION_BY_PK = 0;
  public static final int PARTITION_BY_COLUMN = 1;
  public static final int PARTITION_BY_RANGE = 2;
  public static final int DEFAULT_PARTITION = 3;
  public static final int PARTITION_BY_LIST = 4;
  public static final int PARTITION_BY_LIST_SPECIAL_CASE = 5;
  
  public static int getDataPolicy(Long key, String val) {
    if (val.equalsIgnoreCase(REPLICATE_DATA_POLICY)) {
      return REPLICATE;
    }
    else if (val.equalsIgnoreCase(PARTITION_DATA_POLICY)) {
      return PARTITION;
    }
    else if (val.equalsIgnoreCase(NO_DATA_POLICY)) {
      return NONE;
    }
    else {
      String s = "Illegal value for " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }
  
  public static int getPartitionType(Long key, String val) {
    if (val.equalsIgnoreCase(PARTITION_BY_PK_TYPE)) {
      return PARTITION_BY_PK;
    }
    else if (val.equalsIgnoreCase(PARTITION_BY_COLUMN_TYPE)) {
      return PARTITION_BY_COLUMN;
    }
    else if (val.equalsIgnoreCase(PARTITION_BY_RANGE_TYPE)) {
      return PARTITION_BY_RANGE;
    }
    else if (val.equalsIgnoreCase(PARTITION_BY_LIST_TYPE)) {
      return PARTITION_BY_LIST;
    }
    else if (val.equalsIgnoreCase(PARTITION_BY_LIST_SPECIAL_CASE_TYPE)) {
      return PARTITION_BY_LIST_SPECIAL_CASE;
    }
    else if (val.equalsIgnoreCase(DEFAULT_PARTITION_TYPE)) {
      return DEFAULT_PARTITION;
    }
    else {
      String s = "Illegal value for " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * (String)
   * The query API (e.g., SQL, OQL).
   */
  public static Long api;
  public static int getAPI() {
    Long key = api;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    if (val.equalsIgnoreCase(GFE_API)) {
      return GFE;
    }
    else if (val.equalsIgnoreCase(OQL_API)) {
      return OQL;
    }
    else if (val.equalsIgnoreCase(GFXD_API)) {
      return GFXD;
    }
    else if (val.equalsIgnoreCase(MYSQL_API)) {
      return MYSQL;
    }
    else if (val.equalsIgnoreCase(MYSQLC_API)) {
      return MYSQLC;
    }
    else if (val.equalsIgnoreCase(ORACLE_API)) {
      return ORACLE;
    }
    else if (val.equalsIgnoreCase(GPDB_API)) {
      return GPDB;
    }
    else if (val.equalsIgnoreCase(RTE_API)) {
      return RTE;
    }
    //temporary for gemfirexd performance debugging/analysis
    else if (val.equalsIgnoreCase(GFE_GFK_API)) {
      return GFE_GFK;
    }
    else if (val.equalsIgnoreCase(GFE_GFK_DVD_API)) {
      return GFE_GFK_DVD;
    }
    else {
      String s = "Illegal value for " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  public static String getAPIString(int apiCode) {
    switch (apiCode) {
      case OQL: return OQL_API;
      case GFXD: return GFXD_API;
      case GFE: return GFE_API;
      case GFE_GFK: return GFE_GFK_API;
      case GFE_GFK_DVD: return GFE_GFK_DVD_API;
      case MYSQL: return MYSQL_API;
      case MYSQLC: return MYSQLC_API;
      case ORACLE: return ORACLE_API;
      case RTE: return RTE_API;
      case GPDB: return GPDB_API;
      default:
        String s = "Unknown API: " + apiCode;
        throw new QueryFactoryException(s);
    }
  }

  /**
   * (String)
   * Fully qualified classname of query object.
   */
  public static Long objectType;
  public static String getObjectType() {
    Long key = objectType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    return getObjectType(key, val);
  }
  
  private static String getObjectType(Long key, String val) {
    if (val.equals(BROKER_OBJECT_TYPE)) {
      return BROKER_OBJECT_TYPE;
    }
    else if (val.equals(SECTOR_OBJECT_TYPE)) {
      return SECTOR_OBJECT_TYPE;
    }
    else if (val.equals(LARGE_OBJECT_OBJECT_TYPE)) {
      return LARGE_OBJECT_OBJECT_TYPE;
    }
    else if (val.equals(TINY_OBJECT_OBJECT_TYPE)) {
      return TINY_OBJECT_OBJECT_TYPE;
    }
    else if (val.equals(SECURITIES_TYPE)) {
      return SECURITIES_TYPE;
    }
    else {
      String s = "Illegal value for " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * (String(s))
   * Fully qualified classnames of query objects.
   */
  public static Long objectTypes;
  public static List getObjectTypes() {
    Long key = objectTypes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key));
    List vals = new ArrayList();
    for (Iterator i = val.iterator(); i.hasNext();) {
      vals.add(getObjectType(key, (String)i.next()));
    }
    return vals;
  }

  /**
   * (boolean)
   * Whether to log indexes before creating them.  Defaults to false.
   */
  public static Long logIndexes;
  public static boolean logIndexes() {
    Long key = logIndexes;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to log queries before performing them.  Defaults to false.
   */
  public static Long logQueries;
  public static boolean logQueries() {
    Long key = logQueries;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to log query results after performing them.  Defaults to false.
   */
  public static Long logQueryResults;
  public static boolean logQueryResults() {
    Long key = logQueryResults;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
  
  /**
   * (boolean)
   * Whether to log query results after performing them.  Defaults to false.
   */
  public static Long logQueryResultSize;
  public static boolean logQueryResultSize() {
    Long key = logQueryResultSize;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to log updates before performing them.  Defaults to false.
   */
  public static Long logUpdates;
  public static boolean logUpdates() {
    Long key = logUpdates;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
  
  /**
   * (boolean)
   * Whether to log warnings and errors after executing prepared statements.
   * Defaults to false.
   */
  public static Long logWarnings;
  public static boolean logWarnings() {
    Long key = logWarnings;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to do simple validation after performing queries.  Defaults to false.
   */
  public static Long validateResults;
  public static boolean validateResults() {
    Long key = validateResults;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  static {
    setValues(QueryPrms.class);
  }
  public static void main( String args[] ) {
    dumpKeys();
  }
}

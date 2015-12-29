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
package objects.query.largeobject;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.HydraConfigException;
import hydra.RegionPrms;

import java.util.Vector;

import objects.query.QueryPrms;

/**
 * A class used to store keys for test configuration settings.
 */
public class LargeObjectPrms extends BasePrms {

  public static final int NO_QUERY = -1;

  public static final String RANDOM_EQUALITY_ON_LARGE_OBJECT_ID =
      "randomEqualityOnLargeObjectId";
  public static final String PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID = 
    "putNewLargeObjectByLargeObjectId";
  public static final String GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID = 
    "getAndPutLargeObjectByLargeObjectId";
  public static final String INSERT_INTO_LARGE_OBJECT = 
    "insertIntoLargeObject";
  public static final String DELETE_LARGE_OBJECT_BY_LARGE_OBJECT_ID = 
    "deleteLargeObjectByLargeObjectId";
  //public static final String DELETE_FROM_LARGE_OBJECT = "deleteFromLargeObject";

  public static final int RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY = 0;
  public static final int PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY= 1;
  public static final int GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY= 2;
  public static final int INSERT_INTO_LARGE_OBJECT_QUERY = 3;
  //public static final int DELETE_FROM_LARGE_OBJECT_QUERY = 4;
  public static final int DELETE_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY = 5;
  //------------------------------------------------------------------
  //Index and Primary Keys
  //------------------------------------------------------------------
  public static final String PRIMARY_KEY_INDEX_ON_LARGE_OBJECT_ID =
      "primaryKeyIndexOnLargeObjectId";
  public static final String UNIQUE_INDEX_ON_LARGE_OBJECT_ID=
    "uniqueKeyIndexOnLargeObjectId";
  
  public static final int PRIMARY_KEY_INDEX_ON_LARGE_OBJECT_ID_QUERY = 0;
  public static final int UNIQUE_INDEX_ON_LARGE_OBJECT_ID_QUERY = 1;

  /**
   * (String(s))
   * A Vector of index types to create
   */
  public static Long indexTypes;

  public static Vector getIndexTypes() {
    Long key = indexTypes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));
    return val;
  }

  public static int getIndexType(String val) {
    Long key = indexTypes;
    if (val.equals(PRIMARY_KEY_INDEX_ON_LARGE_OBJECT_ID)) {
      return PRIMARY_KEY_INDEX_ON_LARGE_OBJECT_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_LARGE_OBJECT_ID)) {
      return UNIQUE_INDEX_ON_LARGE_OBJECT_ID_QUERY;
    }
    else if (val.equals(NONE)) {
      return NO_QUERY;
    }
    else {
      String s = "Unsupported value for "
      //+ QueryPrms.getAPIString(QueryPrms.SQL) + " index "
          + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * (String(s))
   * Type of query.
   */
  public static Long queryType;

  public static int getQueryType(int api) {
    Long key = queryType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    switch (api) {
      case QueryPrms.GFXD:
        return getSQLQueryType(key, val);
      case QueryPrms.MYSQL:
        return getSQLQueryType(key, val);
      case QueryPrms.MYSQLC:
        return getSQLQueryType(key, val);
      case QueryPrms.OQL:
        return getSQLQueryType(key, val);
      case QueryPrms.GFE:
        return getSQLQueryType(key, val);
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(RANDOM_EQUALITY_ON_LARGE_OBJECT_ID)) {
      return RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY;
    }
    else if (val.equalsIgnoreCase(INSERT_INTO_LARGE_OBJECT)) {
      return INSERT_INTO_LARGE_OBJECT_QUERY;
    }
   // else if (val.equalsIgnoreCase(DELETE_FROM_LARGE_OBJECT)) {
     // return DELETE_FROM_LARGE_OBJECT_QUERY;
    //}
    else {
      String s =
          "Unsupported value for " + QueryPrms.getAPIString(QueryPrms.GFXD)
              + " query " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }
  
  /**
   * (String(s))
   * Type of query.
   */
  public static Long updateQueryType;

  public static int getUpdateQueryType(int api) {
    Long key = updateQueryType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    switch (api) {
      case QueryPrms.GFXD:
        return getSQLUpdateQueryType(key, val);
      case QueryPrms.MYSQL:
        return getSQLUpdateQueryType(key, val);
      case QueryPrms.MYSQLC:
        return getSQLUpdateQueryType(key, val);
      case QueryPrms.OQL:
        return getSQLUpdateQueryType(key, val);
      case QueryPrms.GFE:
        return getSQLUpdateQueryType(key, val);
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLUpdateQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID)) {
      return PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY;
    }
    else if (val.equalsIgnoreCase(GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID)) {
      return GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY;
    }
    else {
      String s =
          "Unsupported value for " + QueryPrms.getAPIString(QueryPrms.GFXD)
              + " query " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  
  /**
   * (String(s))
   * Type of query.
   */
  public static Long deleteQueryType;

  public static int getDeleteQueryType(int api) {
    Long key = deleteQueryType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    switch (api) {
      case QueryPrms.GFXD:
        return getSQLDeleteQueryType(key, val);
      case QueryPrms.MYSQL:
        return getSQLDeleteQueryType(key, val);
      case QueryPrms.MYSQLC:
        return getSQLDeleteQueryType(key, val);
      case QueryPrms.OQL:
        return getSQLDeleteQueryType(key, val);
      case QueryPrms.GFE:
        return getSQLDeleteQueryType(key, val);
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLDeleteQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(DELETE_LARGE_OBJECT_BY_LARGE_OBJECT_ID)) {
      return DELETE_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY;
    }
    else {
      String s =
          "Unsupported value for " + QueryPrms.getAPIString(QueryPrms.GFXD)
              + " query " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }

  /**
   * (String(s))
   * Fields to return in result set.  Defaults to "*".
   */
  public static Long largeObjectFields;

  public static String getLargeObjectFields() {
    Long key =largeObjectFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return LargeObject.commaSeparatedStringFor(val);
  }
  
  public static Vector getLargeObjectFieldsAsVector() {
    Long key =largeObjectFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return val;
  }

  /**
   * (int)
   * Number of unique large objects to create.  This is a required field.
   * Large Object IDs must be numbered from 0 to numLargeObjects - 1.
   */
  public static Long numLargeObjects;

  public static int getNumLargeObjects() {
    Long key = numLargeObjects;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s = BasePrms.nameForKey(numLargeObjects) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----
  //Query Sizing Params
  //----

  /**
   * Data policy for SQL LargeObject Table.
   */
  public static Long largeObjectDataPolicy;

  public static int getLargeObjectDataPolicy() {
    Long key = largeObjectDataPolicy;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }
  
  /**
   * Data policy partition type for SQL LargeObject table.
   */
  public static Long largeObjectPartitionType;

  public static int getLargeObjectPartitionType() {
    Long key = largeObjectPartitionType;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.PARTITION_BY_PK_TYPE ));
    return QueryPrms.getPartitionType(key, val);
  }

  /**
   * Partition redundancy for SQL LargeObject Table.  Defaults to 0.
   */
  public static Long largeObjectPartitionRedundancy;

  public static int getLargeObjectPartitionRedundancy() {
    Long key = largeObjectPartitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
 
  /**
   * Whether to use offheap. Defaults to false.
   */
  public static Long largeObjectOffHeap;

  public static boolean getLargeObjectOffHeap() {
    Long key = largeObjectOffHeap;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Total number of buckets in the SQL LargeObject table.  Defaults to 0,
   * which uses the product default.
   */
  public static Long largeObjectPartitionTotalNumBuckets;

  public static int getLargeObjectPartitionTotalNumBuckets() {
    Long key = largeObjectPartitionTotalNumBuckets;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
 
  /**
   * Column to partition on.
   * For SQL LargeObject Table;
   */
  public static Long largeObjectPartitionColumn;

  public static String getLargeObjectPartitionColumn() {
    Long key = largeObjectPartitionColumn;
    String val =
        tasktab().stringAt(key, tab().stringAt(key));
    return val;
  }
  
  /**
   * indexes for create table statement.
   * For SQL LargeObject Table;
   */
  public static Long largeObjectCreateTableIndexes;

  public static Vector getLargeObjectCreateTableIndexes() {
    Long key = largeObjectCreateTableIndexes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));
    return val;
  }
  
  /**
   * (String)
   * Name of LargeObject region from {@link hydra.RegionPrms#names}.
   * The region name is {@link LargeObject#REGION_TABLE_NAME}.
   */
  public static Long largeObjectRegionConfig;
  public static String getLargeObjectRegionConfig() {
    Long key = largeObjectRegionConfig;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }
  
  static {
    setValues(LargeObjectPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}

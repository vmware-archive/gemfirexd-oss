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
package objects.query.tinyobject;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.HydraConfigException;
import hydra.RegionPrms;

import java.util.Vector;

import objects.query.QueryPrms;

/**
 * A class used to store keys for test configuration settings.
 */
public class TinyObjectPrms extends BasePrms {

  public static final int NO_QUERY = -1;

  public static final String RANDOM_EQUALITY_ON_TINY_OBJECT_ID =
      "randomEqualityOnTinyObjectId";
  public static final String PUT_NEW_TINY_OBJECT_BY_TINY_OBJECT_ID = 
    "putNewTinyObjectByTinyObjectId";
  public static final String GET_AND_PUT_TINY_OBJECT_BY_TINY_OBJECT_ID = 
    "getAndPutTinyObjectByTinyObjectId";
  public static final String INSERT_INTO_TINY_OBJECT = 
    "insertIntoTinyObject";
  public static final String DELETE_TINY_OBJECT_BY_TINY_OBJECT_ID = 
    "deleteTinyObjectByTinyObjectId";
  //public static final String DELETE_FROM_TINY_OBJECT = "deleteFromTinyObject";

  public static final int RANDOM_EQUALITY_ON_TINY_OBJECT_ID_QUERY = 0;
  public static final int PUT_NEW_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY= 1;
  public static final int GET_AND_PUT_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY= 2;
  public static final int INSERT_INTO_TINY_OBJECT_QUERY = 3;
  //public static final int DELETE_FROM_TINY_OBJECT_QUERY = 4;
  public static final int DELETE_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY = 5;
  //------------------------------------------------------------------
  //Index and Primary Keys
  //------------------------------------------------------------------
  public static final String PRIMARY_KEY_INDEX_ON_TINY_OBJECT_ID =
      "primaryKeyIndexOnTinyObjectId";
  public static final String UNIQUE_INDEX_ON_TINY_OBJECT_ID=
    "uniqueKeyIndexOnTinyObjectId";
  
  public static final int PRIMARY_KEY_INDEX_ON_TINY_OBJECT_ID_QUERY = 0;
  public static final int UNIQUE_INDEX_ON_TINY_OBJECT_ID_QUERY = 1;

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
    if (val.equals(PRIMARY_KEY_INDEX_ON_TINY_OBJECT_ID)) {
      return PRIMARY_KEY_INDEX_ON_TINY_OBJECT_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_TINY_OBJECT_ID)) {
      return UNIQUE_INDEX_ON_TINY_OBJECT_ID_QUERY;
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
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(RANDOM_EQUALITY_ON_TINY_OBJECT_ID)) {
      return RANDOM_EQUALITY_ON_TINY_OBJECT_ID_QUERY;
    }
    else if (val.equalsIgnoreCase(INSERT_INTO_TINY_OBJECT)) {
      return INSERT_INTO_TINY_OBJECT_QUERY;
    }
   // else if (val.equalsIgnoreCase(DELETE_FROM_TINY_OBJECT)) {
     // return DELETE_FROM_TINY_OBJECT_QUERY;
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
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLUpdateQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(PUT_NEW_TINY_OBJECT_BY_TINY_OBJECT_ID)) {
      return PUT_NEW_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY;
    }
    else if (val.equalsIgnoreCase(GET_AND_PUT_TINY_OBJECT_BY_TINY_OBJECT_ID)) {
      return GET_AND_PUT_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY;
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
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLDeleteQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(DELETE_TINY_OBJECT_BY_TINY_OBJECT_ID)) {
      return DELETE_TINY_OBJECT_BY_TINY_OBJECT_ID_QUERY;
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
  public static Long tinyObjectFields;

  public static String getTinyObjectFields() {
    Long key =tinyObjectFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return TinyObject.commaSeparatedStringFor(val);
  }
  
  public static Vector getTinyObjectFieldsAsVector() {
    Long key =tinyObjectFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return val;
  }

  /**
   * (int)
   * Number of unique tiny objects to create.  This is a required field.
   * Tiny Object IDs must be numbered from 0 to numTinyObjects - 1.
   */
  public static Long numTinyObjects;

  public static int getNumTinyObjects() {
    Long key = numTinyObjects;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s = BasePrms.nameForKey(numTinyObjects) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----
  //Query Result Set Size and Sizing Params
  //----

  /**
   * (int)
   * Desired size of each query result set.  Defaults to 1.
   */
  public static Long resultSetSize;

  public static int getResultSetSize() {
    Long key = resultSetSize;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(resultSetSize) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (boolean)
   * Whether to use best fit in computing the result set.  Defaults to false.
   */
  public static Long useBestFit;

  public static boolean useBestFit() {
    Long key = useBestFit;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * Data policy for SQL TinyObject Table.
   */
  public static Long tinyObjectDataPolicy;

  public static int getTinyObjectDataPolicy() {
    Long key = tinyObjectDataPolicy;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }
  
  /**
   * Data policy partition type for SQL TinyObject table.
   */
  public static Long tinyObjectPartitionType;

  public static int getTinyObjectPartitionType() {
    Long key = tinyObjectPartitionType;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.PARTITION_BY_PK_TYPE ));
    return QueryPrms.getPartitionType(key, val);
  }

  /**
   * Partition redundancy for SQL TinyObject Table.  Defaults to 0.
   */
  public static Long tinyObjectPartitionRedundancy;

  public static int getTinyObjectPartitionRedundancy() {
    Long key = tinyObjectPartitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
 
  /**
   * Total number of buckets in the SQL TinyObject table.  Defaults to 0,
   * which uses the product default.
   */
  public static Long tinyObjectPartitionTotalNumBuckets;

  public static int getTinyObjectPartitionTotalNumBuckets() {
    Long key = tinyObjectPartitionTotalNumBuckets;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
 
  /**
   * Column to partition on.
   * For SQL TinyObject Table;
   */
  public static Long tinyObjectPartitionColumn;

  public static String getTinyObjectPartitionColumn() {
    Long key = tinyObjectPartitionColumn;
    String val =
        tasktab().stringAt(key, tab().stringAt(key));
    return val;
  }
  
  /**
   * indexes for create table statement.
   * For SQL TinyObject Table;
   */
  public static Long tinyObjectCreateTableIndexes;

  public static Vector getTinyObjectCreateTableIndexes() {
    Long key = tinyObjectCreateTableIndexes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));
    return val;
  }
  
  /**
   * (String)
   * Name of TinyObject region from {@link hydra.RegionPrms#names}.
   * The region name is {@link TinyObject#REGION_TABLE_NAME}.
   */
  public static Long tinyObjectRegionConfig;
  public static String getTinyObjectRegionConfig() {
    Long key = tinyObjectRegionConfig;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }
  
  static {
    setValues(TinyObjectPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}

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
import hydra.HydraConfigException;
import hydra.Log;
import hydra.gemfirexd.GfxdHelperPrms;

import java.util.ArrayList;
import java.util.List;

import objects.query.QueryPrms;

public class GFXDTinyObjectQueryFactory extends SQLTinyObjectQueryFactory {

  public GFXDTinyObjectQueryFactory() {
   
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    String s;
    List stmts = new ArrayList();
    
    String stmt = "create table " + TinyObject.getTableName()
                + " (" + getIdCreateStatement() + "," + FIELD_SPEC + ")";

    int dataPolicy = TinyObjectPrms.getTinyObjectDataPolicy();
    if (dataPolicy == QueryPrms.REPLICATE) {
      stmt += " REPLICATE";
    }
    else if (dataPolicy == QueryPrms.PARTITION) {
      int buckets = TinyObjectPrms.getTinyObjectPartitionTotalNumBuckets();
      int partitionType = TinyObjectPrms.getTinyObjectPartitionType();
      switch (partitionType) {
        case QueryPrms.DEFAULT_PARTITION:
          break;
        case QueryPrms.PARTITION_BY_COLUMN:
          stmt += " PARTITION BY COLUMN ("
               + TinyObjectPrms.getTinyObjectPartitionColumn() + ")";
          break;
        case QueryPrms.PARTITION_BY_PK:
          stmt += " PARTITION BY PRIMARY KEY";
          break;
        case QueryPrms.PARTITION_BY_RANGE:
          int numTinyObjects = TinyObjectPrms.getNumTinyObjects();
          buckets = TinyObjectPrms.getTinyObjectPartitionTotalNumBuckets();
          if (numTinyObjects % buckets != 0) {
            s = BasePrms.nameForKey(
                TinyObjectPrms.tinyObjectPartitionTotalNumBuckets)
              + ": " + buckets + " does not evenly divide "
              + BasePrms.nameForKey(TinyObjectPrms.numTinyObjects)
              + ": " + numTinyObjects;
            throw new HydraConfigException(s);
          }
          int tinyObjectsPerBucket = numTinyObjects / buckets;
          Log.getLogWriter().info("Configuring partition by range for "
             + numTinyObjects + " objects using " + buckets + " ranges and "
             + tinyObjectsPerBucket + " objects per range");
          stmt += " PARTITION BY RANGE ("
               + TinyObjectPrms.getTinyObjectPartitionColumn() + ")";
          stmt += " (";
          for (int i = 0; i < buckets; i++) {
            if (i != 0) stmt += ", ";
            int x = i * tinyObjectsPerBucket;
            int y = (i + 1) * tinyObjectsPerBucket;
            stmt += "VALUES BETWEEN " + x + " AND " + y;
          }
          stmt += ")";
          break;
       
        default:
          s = "Invalid partitioning scheme";
          throw new HydraConfigException(s);
      }
      int redundancy = TinyObjectPrms.getTinyObjectPartitionRedundancy();
      if (redundancy != 0) {
        stmt += " REDUNDANCY " + redundancy;
      }
      if (buckets != 0) {
        stmt += " BUCKETS " + buckets;
      }
    }
    if (GfxdHelperPrms.persistTables()) {
      stmt += " PERSISTENT SYNCHRONOUS DISKDIR ('tinyobject')";
    }
    stmts.add(stmt);
    return stmts;
  }
  
  public String getCreateSchemaStatement() {
    String stmt ="create schema perfTest";
    return stmt;
  }
  
  public String getDropSchemaStatement() {
    String stmt ="drop schema perfTest restrict";
    return stmt;
  }
}

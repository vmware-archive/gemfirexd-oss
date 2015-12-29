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
import hydra.HydraConfigException;
import hydra.Log;

import java.util.ArrayList;
import java.util.List;

import objects.query.QueryPrms;

public class MySQLCLargeObjectQueryFactory extends MySQLLargeObjectQueryFactory {
  public MySQLCLargeObjectQueryFactory() {
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    String s;
    List stmts = new ArrayList();
    
    String stmt = "create table " + LargeObject.getTableName()
                + " (" + getIdCreateStatement() + "," + FIELD_SPEC 
                + ") ENGINE=NDBCLUSTER";

    int dataPolicy = LargeObjectPrms.getLargeObjectDataPolicy();
    if (dataPolicy == QueryPrms.REPLICATE) {
        s = "Replicated data policy is not supported in MySQL Cluster";
        throw new UnsupportedOperationException(s);
    }
    else if (dataPolicy == QueryPrms.PARTITION) {
      int buckets = LargeObjectPrms.getLargeObjectPartitionTotalNumBuckets();
      int partitionType = LargeObjectPrms.getLargeObjectPartitionType();
      switch (partitionType) {
        case QueryPrms.DEFAULT_PARTITION:
          s = "Default partition type is currently not supported "
            + "for table LargeObject";
          throw new UnsupportedOperationException(s);

        case QueryPrms.PARTITION_BY_COLUMN:
          s = "Partition by column is currently not supported "
            + "for table LargeObject";
          throw new UnsupportedOperationException(s);
     
        case QueryPrms.PARTITION_BY_PK:
          stmt += " PARTITION BY KEY(id)";
          //stmt += " PARTITION BY HASH(id)";
          if (buckets != 0) {
            stmt += " PARTITIONS " + buckets;
          }
          break;
        case QueryPrms.PARTITION_BY_RANGE:
          int numLargeObjects = LargeObjectPrms.getNumLargeObjects();
          buckets = LargeObjectPrms.getLargeObjectPartitionTotalNumBuckets();
          if (numLargeObjects % buckets != 0) {
            s = BasePrms.nameForKey(
                LargeObjectPrms.largeObjectPartitionTotalNumBuckets)
              + ": " + buckets + " does not evenly divide "
              + BasePrms.nameForKey(LargeObjectPrms.numLargeObjects)
              + ": " + numLargeObjects;
            throw new HydraConfigException(s);
          }
          int largeObjectsPerBucket = numLargeObjects / buckets;
          Log.getLogWriter().info("Configuring partition by range for "
             + numLargeObjects + " objects using " + buckets + " ranges and "
             + largeObjectsPerBucket + " objects per range");
          stmt += " PARTITION BY RANGE ("
               + LargeObjectPrms.getLargeObjectPartitionColumn() + ")";
          stmt += " (";
          for (int i = 0; i < buckets; i++) {
            if (i != 0) stmt += ", ";
            int y = (i + 1) * largeObjectsPerBucket;
            stmt += "PARTITION p" + i + " VALUES LESS THAN (" + y + ")";
          }
          stmt += ")";
          break;
        default:
          s = "Invalid partitioning scheme";
          throw new HydraConfigException(s);
      }
    }
    stmts.add(stmt);
    return stmts;
  }
}

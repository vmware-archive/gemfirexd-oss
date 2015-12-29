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
package objects.query.sector;

import hydra.HydraConfigException;

import java.util.ArrayList;
import java.util.List;

import objects.query.QueryPrms;

public class GFXDRiskQueryFactory extends SQLRiskQueryFactory {

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    List stmts = new ArrayList();
    int dataPolicy = SectorPrms.getPositionDataPolicy();
    if (dataPolicy == QueryPrms.REPLICATE) {
      stmts.add("create table " + Risk.getTableName()
          + " (" + getIdCreateStatement() + " , " + getPositionIdCreateStatement() + ", " + getRiskValueCreateStatement() + ")"
          + " REPLICATE SERVER GROUPS (sg1)");
    }
    else if (dataPolicy == QueryPrms.PARTITION) {
      String stmt = "create table " + Risk.getTableName()
      + " (" + getIdCreateStatement() + ", " + getPositionIdCreateStatement() + ", " + getRiskValueCreateStatement() + ") ";

      int partitionType = SectorPrms.getRiskPartitionType();
      switch (partitionType) {
        case QueryPrms.DEFAULT_PARTITION:
          break;
        case QueryPrms.PARTITION_BY_COLUMN:
          stmt += " PARTITION BY COLUMN (" + SectorPrms.getRiskPartitionColumn() + ")";
          break;
        case QueryPrms.PARTITION_BY_PK:
          stmt += " PARTITION BY PRIMARY KEY";
          break;
        case QueryPrms.PARTITION_BY_RANGE:
          //@todo dynamically calculate the range values
          stmt += " PARTITION BY RANGE (" +  SectorPrms.getRiskPartitionColumn() + ") (VALUES BETWEEN 0 AND 9, VALUES BETWEEN 10 AND 19) ";
          break;
        default:
          String s = "Invalid partitioning scheme";
          throw new HydraConfigException(s);
      }
      int buckets = SectorPrms.getRiskPartitionTotalNumBuckets();
      if (buckets != 0) {
        stmt += " BUCKETS " + buckets;
      }
      boolean offheap = SectorPrms.getRiskOffHeap();
      if (offheap) {
        stmt += " OFFHEAP";
      }
      int redundancy = SectorPrms.getRiskPartitionRedundancy();
      if (redundancy != 0) {
        stmt += " REDUNDANCY " + redundancy;
      }
      stmts.add(stmt);
    }
    else {
      stmts.add("create table " + Risk.getTableName()
          + " (id int not null, position_id int, risk_value int)");
    }
    return stmts;
  }
}

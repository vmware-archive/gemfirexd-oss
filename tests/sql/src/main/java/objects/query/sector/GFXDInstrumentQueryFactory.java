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
import hydra.Log;

import java.util.ArrayList;
import java.util.List;

import objects.query.QueryPrms;

public class GFXDInstrumentQueryFactory extends SQLInstrumentQueryFactory {

  public GFXDInstrumentQueryFactory() {
    positionQueryFactory = new GFXDPositionQueryFactory();
  }

  public void init() {
    super.init();
    positionQueryFactory.init();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------

  public List getTableStatements() {
    List stmts = new ArrayList();
  
    String error = "";
    int dataPolicy = SectorPrms.getInstrumentDataPolicy();
    if (dataPolicy == QueryPrms.REPLICATE) {
      stmts.add("create table " + Instrument.getTableName()
          + " (" + getIdCreateStatement() + ", " + getSectorIdCreateStatement() + ")" +
          		" REPLICATE SERVER GROUPS (sg1)");
    }
    else if (dataPolicy == QueryPrms.PARTITION) {
    
      String stmt = "create table " + Instrument.getTableName()
          + " (" + getIdCreateStatement() + ", " + getSectorIdCreateStatement() + ")"; 
      int partitionType = SectorPrms.getInstrumentPartitionType();
      switch (partitionType) {
        case QueryPrms.DEFAULT_PARTITION:
          break;
        case QueryPrms.PARTITION_BY_COLUMN:
          stmt += " PARTITION BY COLUMN (" + SectorPrms.getInstrumentPartitionColumn() + ")";
          break;
        case QueryPrms.PARTITION_BY_PK:
          stmt += " PARTITION BY PRIMARY KEY";
          break;
        case QueryPrms.PARTITION_BY_RANGE:
          error = "Partition by range is currently unavailable for Instruments table";
          throw new HydraConfigException(error);
        default:
          error= "Invalid partitioning scheme";
          throw new HydraConfigException(error);
      }
      int redundancy = SectorPrms.getInstrumentPartitionRedundancy();
      if (redundancy != 0) {
        stmt += " REDUNDANCY " + redundancy;
      }
      int buckets = SectorPrms.getInstrumentPartitionTotalNumBuckets();
      if (buckets != 0) {
        stmt += " BUCKETS " + buckets;
      }
      boolean offheap = SectorPrms.getInstrumentOffHeap();
      if (offheap) {
        stmt += " OFFHEAP";
      }
      stmts.add(stmt);
    }
    else {
      stmts.add("create table " + Instrument.getTableName()
          + " (id varchar(20) not null, sector_id int)");
    }

    //do book table here before instrument table
    stmts.addAll(positionQueryFactory.getTableStatements());
    
    return stmts;
  }
}


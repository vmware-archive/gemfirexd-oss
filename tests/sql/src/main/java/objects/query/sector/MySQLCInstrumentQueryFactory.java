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

public class MySQLCInstrumentQueryFactory extends MySQLInstrumentQueryFactory {

  public MySQLCInstrumentQueryFactory() {
    positionQueryFactory = new MySQLCPositionQueryFactory();
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
        case QueryPrms.PARTITION_BY_PK:
          stmt += " ENGINE=NDBCLUSTER";
          break;
        default:
          error= "Invalid partitioning scheme";
          throw new HydraConfigException(error);
      }
      int buckets = SectorPrms.getInstrumentPartitionTotalNumBuckets();
      if (buckets != 0) {
        stmt += " PARTITIONS " + buckets;
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


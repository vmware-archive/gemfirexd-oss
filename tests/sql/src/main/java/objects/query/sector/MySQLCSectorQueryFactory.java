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

public class MySQLCSectorQueryFactory extends MySQLSectorQueryFactory {

  public MySQLCSectorQueryFactory() {
    instrumentQueryFactory = new MySQLCInstrumentQueryFactory();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    List stmts = new ArrayList();
    //do book table here before instrument table
    String error = "";
    int dataPolicy = SectorPrms.getSectorDataPolicy();
    if (dataPolicy == QueryPrms.REPLICATE) {
      stmts.add("create table " + Sector.getTableName()
          + " (" + getIdCreateStatement() + ", " + getNameCreateStatement() +", " + getMarketCapCreateStatement() + ") REPLICATE"
          );
    }
    else if (dataPolicy == QueryPrms.PARTITION) {
      int partitionType = SectorPrms.getSectorPartitionType();
      String stmt = "create table " + Sector.getTableName()
      + " (" + getIdCreateStatement() + ", " + getNameCreateStatement() +", " + getMarketCapCreateStatement() + ")";
      
      switch (partitionType) {
        case QueryPrms.PARTITION_BY_PK:
          stmt += " ENGINE=NDBCLUSTER";
          break;
        default:
          error= "Invalid partitioning scheme";
          throw new HydraConfigException(error);
      }
      int buckets = SectorPrms.getSectorPartitionTotalNumBuckets();
      if (buckets != 0) {
        stmt += " PARTITIONS " + buckets;
      }
      stmts.add(stmt); 
    }
    else {
      stmts.add("create table " + Sector.getTableName()
          + " (id int not null, name varchar(20), market_cap double)");
    }
    stmts.addAll(instrumentQueryFactory.getTableStatements());
    
    return stmts;
  }
  
  public String getCreateSchemaStatement() {
    //String stmt ="create schema perfTest default server groups (SG1)";
    String stmt ="create schema perfTest ";
    return stmt;
  }
  
  public String getDropSchemaStatement() {
    String stmt ="drop schema perfTest";
    return stmt;
  }
}

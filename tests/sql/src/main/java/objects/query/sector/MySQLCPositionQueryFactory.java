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

public class MySQLCPositionQueryFactory extends MySQLPositionQueryFactory {

  public MySQLCPositionQueryFactory() {
    riskQueryFactory = new MySQLCRiskQueryFactory();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  /*
   * This table can be partitioned by default, column, or range.  
   * -Range will be numPositions/numNodes
   */
  public List getTableStatements() {
    List stmts = new ArrayList();
    int dataPolicy = SectorPrms.getPositionDataPolicy();
    if (dataPolicy == QueryPrms.REPLICATE) {
      stmts.add("create table " + Position.getTableName()
          + " ("  + getIdCreateStatement() + ", " + getBookIdCreateStatement()+ ", " + getInstrumentIdCreateStatement() + ", "
          + getAmountCreateStatement() + ", " + getSyntheticCreateStatement() + ", " + getOwnerCreateStatement() + ", "
          + getSymbolCreateStatement() 
          + " REPLICATE SERVER GROUPS (sg1)");
    }
    else if (dataPolicy == QueryPrms.PARTITION) {
      String stmt =
          "create table " + Position.getTableName()
              + " (" + getIdCreateStatement() + ", " + getBookIdCreateStatement()+ ", " + getInstrumentIdCreateStatement() + ", "
              + getAmountCreateStatement() + ", " + getSyntheticCreateStatement() + ", " + getOwnerCreateStatement() + ", "
              + getSymbolCreateStatement() + ")";

      int partitionType = SectorPrms.getPositionPartitionType();
      switch (partitionType) {
        case QueryPrms.DEFAULT_PARTITION:
          break;
        case QueryPrms.PARTITION_BY_COLUMN:
          stmt += " ENGINE=NDBCLUSTER";
          break;
        case QueryPrms.PARTITION_BY_PK:
          stmt += " ENGINE=NDBCLUSTER";
          break;
        case QueryPrms.PARTITION_BY_RANGE:
          stmt +=
              " PARTITION BY RANGE (" + SectorPrms.getPositionPartitionColumn()
                  + ") (";
          stmt += getRangeValuesStmt(SectorPrms.getPositionPartitionColumn());
          stmt += ")";
          break;
        default:
          String s = "Invalid partitioning scheme";
          throw new HydraConfigException(s);
      }
      int buckets = SectorPrms.getPositionPartitionTotalNumBuckets();
      if (buckets != 0) {
        stmt += " PARTITIONS " + buckets;
      }
      stmts.add(stmt);
    }
    else {
      stmts.add("create table " + Position.getTableName()
          + " (id int not null, book_id int, instrument varchar(20), "
          + "amount int, synthetic int, owner varchar(20), "
          + "symbol varchar(5))");
    }
    //do book table here before instrument table
    stmts.addAll(riskQueryFactory.getTableStatements());
    return stmts;
  }

}

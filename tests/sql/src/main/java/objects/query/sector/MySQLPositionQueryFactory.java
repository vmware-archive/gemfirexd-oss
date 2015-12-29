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

public class MySQLPositionQueryFactory extends SQLPositionQueryFactory {

  public MySQLPositionQueryFactory() {
    riskQueryFactory = new MySQLRiskQueryFactory();
  }

  public void init() {
    super.init();
    riskQueryFactory.init();
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
    String stmt;
    int dataPolicy = SectorPrms.getPositionDataPolicy();
    if (dataPolicy == QueryPrms.NONE) {
      stmt = "create table " + Position.getTableName()
           + " ("
           + "id int not null, "
           + "book_id int, "
           + "instrument varchar(20), "
           + "amount int, "
           + "synthetic int, "
           + "owner varchar(20), "
           + "symbol varchar(5)"
           + ")";
    } else {
      stmt = "create table " + Position.getTableName()
           + " ("
           + getIdCreateStatement() + ", "
           + getBookIdCreateStatement()+ ", "
           + getInstrumentIdCreateStatement() + ", "
           + getAmountCreateStatement() + ", "
           + getSyntheticCreateStatement() + ", "
           + getOwnerCreateStatement() + ", "
           + getSymbolCreateStatement()
           + ")";

    }
    stmts.add(stmt);
    stmts.addAll(riskQueryFactory.getTableStatements());
    return stmts;
  }

}

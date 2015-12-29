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

public class MySQLInstrumentQueryFactory extends SQLInstrumentQueryFactory {

  public MySQLInstrumentQueryFactory() {
    positionQueryFactory = new MySQLPositionQueryFactory();
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
    String stmt;
    int dataPolicy = SectorPrms.getInstrumentDataPolicy();
    if (dataPolicy == QueryPrms.NONE) {
      stmt = "create table " + Instrument.getTableName()
           + " ("
           + "id varchar(20) not null, "
           + "sector_id int"
           + ")";
    } else {
      stmt = "create table " + Instrument.getTableName()
           + " ("
           + getIdCreateStatement() + ", "
           + getSectorIdCreateStatement()
           + ")";
    }
    stmts.add(stmt);
    stmts.addAll(positionQueryFactory.getTableStatements());
    return stmts;
  }
}

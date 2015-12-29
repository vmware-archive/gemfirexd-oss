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

import hydra.HydraConfigException;

import java.util.ArrayList;
import java.util.List;

import objects.query.QueryPrms;

public class MySQLLargeObjectQueryFactory extends SQLLargeObjectQueryFactory {

  public MySQLLargeObjectQueryFactory() {
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public List getTableStatements() {
    String s;
    List stmts = new ArrayList();
    
    String stmt = "create table " + LargeObject.getTableName()
                + " (" + getIdCreateStatement() + "," + FIELD_SPEC 
                + ") ENGINE=INNODB";

    int dataPolicy = LargeObjectPrms.getLargeObjectDataPolicy();
    if (dataPolicy == QueryPrms.PARTITION) {
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
          break;
        case QueryPrms.PARTITION_BY_RANGE:
          s = "Partition by range is currently not supported "
            + "for table LargeObject";
          throw new UnsupportedOperationException(s);
        default:
          s = "Invalid partitioning scheme";
          throw new HydraConfigException(s);
      }
    }
    stmts.add(stmt); 
    return stmts;
  }
  
  public String getCreateSchemaStatement() {
    String stmt ="create schema perfTest";
    return stmt;
  }
  
  public String getDropSchemaStatement() {
    String stmt ="drop schema perfTest";
    return stmt;
  }
}

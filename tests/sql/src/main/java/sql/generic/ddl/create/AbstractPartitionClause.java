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
package sql.generic.ddl.create;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;

public abstract class AbstractPartitionClause {
  public static String[] availablePartitionClause = { 
      " ",                                      // default
      " PARTITION BY PRIMARY KEY ",             // by PK 
      " partition by column ",                  // by column
      " partition by list ",                    // by list
      " partition by range ",                   // by range
      " partition by $FUNCTION ",               // by function
      " partition by $A + $B",                  // by expression
      " replicate " };                          // replicated

  protected List<ColumnInfo> partColumns = new ArrayList<ColumnInfo>();
  protected String partitionClause;
  protected MatchedColumnsHelper matchedColumnHelper = new MatchedColumnsHelper();
  protected Random random = SQLOldTest.random;

  public abstract boolean generatePartitionClause(TableInfo table);

  public String getParitionClause() {
      return partitionClause;
  }

  public List<ColumnInfo> getPartitionColumns() {
      return partColumns;
  }
  
  
  protected boolean isOfNumberType(ColumnInfo column) {
    int type = column.getColumnType();
    if (type == Types.BIGINT || type == Types.DECIMAL || type == Types.FLOAT
        || type == Types.INTEGER || type == Types.NUMERIC
        || type == Types.SMALLINT) {
      return true;
    } else {
      return false;
    }
  }
}

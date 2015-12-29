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

import hydra.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;
import util.TestException;

import com.gemstone.gemfire.LogWriter;

public class PartitionClauseGenerator {
  Random random;
  LogWriter log;

  public PartitionClauseGenerator(Random rand, LogWriter log) {
    this.random = rand;
    this.log = log;
  }

  public String getPartitionedClause(TableInfo table, String partitionMapping) {
    String[] strArray = partitionMapping.split(":");
    if (strArray.length <= 1) {
      processHardCodedPartitionClause(table, partitionMapping);
      return partitionMapping;
    }

    // parse in the partition key
    String tableName = strArray[0];
    String partition = strArray[1];

    if (partition.equalsIgnoreCase("random")) {
      return generateRandomPartitionClause(table);
    } else if (partition.equalsIgnoreCase("replicate")) {
      return generateReplicatedPartitionClause(table);
    } else {
      try {
        int whichClause = Integer.parseInt(partition);
        return generatePartitionedClause(table, whichClause);
      } catch (NumberFormatException n) {
        throw new TestException("while parsing partion clause", n);
      }
    }
  }

  private void processHardCodedPartitionClause(TableInfo table,
      String partitionMapping) {    
    String part = partitionMapping.toUpperCase().trim();
    if (part.equalsIgnoreCase("")) {
      generatePartitionedClause(table, 0);
    } else if (part.contains("PRIMARY")) {
      List<ColumnInfo> pkCols = table.getPrimaryKey().getColumns();
      table.setPartitioningColumns(pkCols);
    } else if (part.contains("REPLICATE")){
      //do nothing
    }else if (part.length() > 0) {
      log.info("rdrd" + part);
      int start = part.indexOf('(') + 1;
      int end = part.indexOf(")", start);
      String[] colStrs = part.substring(start , end ).split(",");
      List<ColumnInfo> cols = new ArrayList<ColumnInfo>();
      for (String c : colStrs) {
        ColumnInfo col = table.getColumn(c.trim());
        cols.add(col);
      }
      table.setPartitioningColumns(cols);
    } else {
      throw new TestException("Unknown DDL Extension");
    }

    table.setPartitioningClause(partitionMapping);

    // check for colocation
    if (part.contains("COLOCATE")) {
      int idx = part.indexOf("COLOCATE");
      int start = part.indexOf("(", idx);
      int end = part.indexOf(")", idx);
      String parentTableName = part.substring(start, end);
      TableInfo parent = GenericBBHelper.getTableInfo(parentTableName);
      table.setColocatedParent(parent);
    }
  }

  public String generateRandomPartitionClause(TableInfo table) {
    return generatePartitionedClause(table, -1);
  }

  public String generateReplicatedPartitionClause(TableInfo table) {
    // last clause is "replicate"
    return generatePartitionedClause(table,
        AbstractPartitionClause.availablePartitionClause.length - 1);
  }

  protected String generatePartitionedClause(TableInfo table, int whichClause) {
    int whichClauseIndex;

    AbstractPartitionClause partitionClause = null;
    boolean clauseFound = false;
    for (int k1 = 0; k1 < 10 && !clauseFound; k1++) {
      if (whichClause == -1) {
        int r1 = SQLOldTest.withReplicatedTables ? 0 : 1;
        whichClauseIndex = random.nextInt(AbstractPartitionClause.availablePartitionClause.length-r1);
      } else {
        whichClauseIndex = whichClause;
      }

      log.info("Generating partition clause for "
          + table.getFullyQualifiedTableName() + ", whichClauseIndex="
          + whichClauseIndex + ", retryCount=" + (k1 + 1));

      switch (whichClauseIndex) {
      case 0: {
        // default partitioned
        partitionClause = new DefaultPartitionClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      case 1: {
        // " PARTITION BY PRIMARY KEY "
        partitionClause = new PartitionByPrimaryKeyPartitionClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      case 2: {
        // partition by column
        partitionClause = new PartitionByColumnsClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      case 3: {
        // partition by list
        partitionClause = new PartitionByListClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      case 4: {
        // " partition by range"
        partitionClause = new PartitionByRangeClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      case 5: {
        // " partition by  $FUNCTION "
        partitionClause = new PartitionByFunctionClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      case 6: {
        // " partition by ( $A + $B )"
        partitionClause = new PartitionByExpressionClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      case 7: {
        // " replicate "
        partitionClause = new ReplicatedPartitionClause();
        clauseFound = partitionClause.generatePartitionClause(table);
        break;
      }
      default: {
        throw new TestException("Unknown partition Clause Index "
            + whichClauseIndex);
      }
      }
    }
    table.setPartitioningColumns(partitionClause.getPartitionColumns());
    table.setPartitioningClause(partitionClause.getParitionClause());
    return partitionClause.getParitionClause();
  }
}

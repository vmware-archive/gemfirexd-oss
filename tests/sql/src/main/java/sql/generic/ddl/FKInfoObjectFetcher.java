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
package sql.generic.ddl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sql.generic.GenericBBHelper;
import util.TestException;
import util.TestHelper;

/**
 * FKInfoObjectFetcher
 * 
 * @author Namrata Thanvi
 */
public class FKInfoObjectFetcher extends GenericTableInfoObjectFetcher {

  List<FKConstraint> fkconstraint = new ArrayList<FKConstraint>();

  Connection conn;

  public FKInfoObjectFetcher(TableInfo tableInfo, Executor executor,ConstraintInfoHolder constraintInfo) {
    super(tableInfo, executor, constraintInfo);
    conn = executor.getConnection();
  }

  // need to handle multiple foreign keys on a table. currently all are added to
  // same constraint
  public void fetch() {
    try {
      String prevConstraintName = " ";
      List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
      List<ColumnInfo> refColumnList = new ArrayList<ColumnInfo>();
      boolean fkConstraint = false;
      boolean constraintAdded = false;
      String pkTableName ="",fullParentName ="";
      String prevParent = "";
      String constraintName = "";
      ResultSet foreignKeys = conn.getMetaData().getImportedKeys(null,
          tableInfo.getSchemaName(), tableInfo.getTableName());
      while (foreignKeys.next()) {
        fkConstraint=true;        
         pkTableName = foreignKeys.getString("PKTABLE_NAME");
        String pkTachleSchema = foreignKeys.getString("PKTABLE_SCHEM");
         fullParentName = pkTachleSchema + "." + pkTableName;
        String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
        String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
        constraintName = foreignKeys.getString("FK_NAME");
        ColumnInfo parentCol = GenericBBHelper.getTableInfo(fullParentName)
            .getColumn(pkColumnName);
        if (!prevConstraintName.equals(" ")
            && prevConstraintName != constraintName) {
          fkconstraint.add(new FKConstraint(tableInfo
              .getFullyQualifiedTableName(), prevConstraintName, columnList,
              prevParent, refColumnList));
          columnList = new ArrayList<ColumnInfo>();
          refColumnList = new ArrayList<ColumnInfo>();
          constraintAdded=true;
        }
        columnList.add(tableInfo.getColumn(fkColumnName));
        refColumnList.add(parentCol);
        prevConstraintName = constraintName;
        constraintAdded = false;
        prevParent = fullParentName;
      }
      
      if ( fkConstraint && ! constraintAdded) {
        fkconstraint.add(new FKConstraint(tableInfo
            .getFullyQualifiedTableName(), constraintName, columnList,
            fullParentName, refColumnList));
      }
      
      tableInfo.setForeignKeys(fkconstraint,constraintInfo);      
      List<String> list =  new ArrayList<String> ( constraintInfo.getTablesWithFKConstraint()) ;
      if (fkconstraint.size() > 0 && !list.contains(tableInfo.getFullyQualifiedTableName())) {         
        list.add(tableInfo.getFullyQualifiedTableName());
      }
      else if(fkconstraint.size() == 0 && list.contains(tableInfo.getFullyQualifiedTableName())){
        list.remove(tableInfo.getFullyQualifiedTableName());
      }
      constraintInfo.setTablesWithFKConstraint(list);
    } catch (SQLException se) {
      throw new TestException(
          "Error while retrieving the Foreign key information "
              + TestHelper.getStackTrace(se));
    }
  }
}

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

import hydra.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.text.TabExpander;


import util.TestException;
import util.TestHelper;

/**
 * PKInfoObjectfetcher
 * 
 * @author Namrata Thanvi
 */

public class PKInfoObjectFetcher  extends GenericTableInfoObjectFetcher {
  List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
  String constraintName;
  Connection conn;
  
  PKInfoObjectFetcher (TableInfo tableInfo , Executor executor, ConstraintInfoHolder constraintInfo ){
    super (tableInfo, executor, constraintInfo);
    conn=executor.getConnection();
  }
  //fetch primary key constraints of the table
  public void fetch(){
    try{
      boolean pkConstraint = false;
      ResultSet primaryKeys = conn.getMetaData().getPrimaryKeys(null,tableInfo.getSchemaName(), tableInfo.getTableName());
      while ( primaryKeys.next()) {
        pkConstraint=true;
        String columnName = primaryKeys.getString("COLUMN_NAME");  
        constraintName = primaryKeys.getString("PK_NAME");        
        columnList.add(tableInfo.getColumn(columnName));        
      }     
      List<String> list =  new ArrayList<String> ( constraintInfo.getTablesWithPKConstraint()) ;
      if (pkConstraint) {
        tableInfo.setPrimaryKey(new PKConstraint(tableInfo.getFullyQualifiedTableName(), constraintName, columnList));
        if (!list.contains(tableInfo.getFullyQualifiedTableName())) {
          list.add(tableInfo.getFullyQualifiedTableName());
        }
      }
      else if (list.contains(tableInfo.getFullyQualifiedTableName())) {
        list.remove(tableInfo.getFullyQualifiedTableName());
      }
      constraintInfo.setTablesWithPKConstraint(list);
      
    } catch (SQLException se ) {
      throw new TestException ("Error while retrieving the primary key information " + TestHelper.getStackTrace(se));
    }
  }
}

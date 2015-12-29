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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import util.TestException;
import util.TestHelper;

/**
 * ColumnInfoObjectFetcher
 * 
 * @author Namrata Thanvi
 */

public class ColumnInfoObjectFetcher extends GenericTableInfoObjectFetcher {
  List<ColumnInfo> columnList = new  ArrayList<ColumnInfo>();
  
  ColumnValuesGenerator valueGenerator = new ColumnValuesGenerator(executor);
  String FullyQualifiedTableName;
  String fullyQualifiedColumnName;
  
  public ColumnInfoObjectFetcher (TableInfo tableInfo , Executor executor, ConstraintInfoHolder constraintInfo){
    super(tableInfo , executor, constraintInfo);
  }
  //pending - column level constraint population
  public void fetch(){
    try{
     ResultSet rs = executor.getConnection().getMetaData().getColumns(null, tableInfo.getSchemaName(), tableInfo.getTableName(), null);
     FullyQualifiedTableName = tableInfo.getFullyQualifiedTableName().toUpperCase();
    
    while ( rs.next() ){
      boolean isNull;
      String columnName = rs.getString("COLUMN_NAME");
      int columnType = rs.getInt("DATA_TYPE");   
      int columnSize = rs.getInt("COLUMN_SIZE");
      if ( rs.getInt("NULLABLE") == 0 ) 
         isNull = false;
      else 
        isNull = true;
      fullyQualifiedColumnName = FullyQualifiedTableName + "." + columnName;
      columnList.add(new ColumnInfo(columnName , columnType , columnSize , isNull , valueGenerator.getValueList( fullyQualifiedColumnName,  columnType ,  columnSize )));  
    }
    
    tableInfo.setColumnList(columnList);
    rs.close();
    } catch (SQLException se ) {
      throw new TestException ("Error while retrieving the column information " + TestHelper.getStackTrace(se));
    }
  }
  
}

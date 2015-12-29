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

import sql.generic.SqlUtilityHelper;
import util.TestException;
import util.TestHelper;


/**
 * CheckInfoObjectFetcher
 * 
 * @author Namrata Thanvi
 */

public class CheckInfoObjectFetcher extends GenericTableInfoObjectFetcher {

  List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
  String constraintName;
  Connection conn;
  Executor executor;
  String tableName;
  String schemaName;
  
  CheckInfoObjectFetcher (TableInfo tableInfo , Executor executor,ConstraintInfoHolder constraintInfo ){
    super (tableInfo, executor, constraintInfo );
    conn=executor.getConnection();
    this.executor=executor;
    this.tableName = SqlUtilityHelper.getTableNameWithoutSchema(tableInfo.getFullyQualifiedTableName()).toUpperCase();
    this.schemaName = SqlUtilityHelper.getSchemaFromTableName(tableInfo.getFullyQualifiedTableName()).toUpperCase();    
  }
  //fetch primary key constraints of the table
  public void fetch(){
    try{
      
      String query = "select  CONSTRAINTNAME, REFERENCEDCOLUMNS , CHECKDEFINITION , TABLESCHEMANAME , TABLENAME   from sys.sysconstraints , sys.syschecks , sys.systables  "; 
      String where = " where type='C' and sys.syschecks.CONSTRAINTId= sys.sysconstraints.CONSTRAINTID and sys.sysconstraints.TABLEID = sys.systables.TABLEID ";
      where += " and TABLENAME = ? and TABLESCHEMANAME =? order by CONSTRAINTNAME";
      query = query + where;
      String prevConstraintName = " ";
      List<Object> params = new ArrayList<Object>();
      List<ColumnInfo> checkColumns = new ArrayList<ColumnInfo>();
      params.add(tableName);
      params.add(schemaName);
      boolean checkConstraint = false;
      boolean constraintAdded = false;
      String constraintName ="" ,definition = "" , prevDefinition = "";
      
      ResultSet rs = executor.executeQuery(query, params);
      List<CheckConstraint>  checkConstraintList= new ArrayList<CheckConstraint>();
      
      while (rs.next() ){
          checkConstraint=true;
          definition = rs.getString("CHECKDEFINITION");
          String refColumns =  rs.getString("REFERENCEDCOLUMNS");
          constraintName =  rs.getString("CONSTRAINTNAME");    
          String[] columnList = refColumns.replace("(", "").replace(")" , "" ).split(",");                                                 
          if (!prevConstraintName.equals(" ")  && prevConstraintName != constraintName) {                 
            checkConstraintList.add(new CheckConstraint(tableInfo.tableName , prevConstraintName , checkColumns , prevDefinition));
            checkColumns = new ArrayList<ColumnInfo>();
            constraintAdded = true;
          }
          for ( String columnPlace : columnList) {
            checkColumns.add(tableInfo.getColumnList().get(Integer.parseInt(columnPlace) - 1));
           }          
          prevConstraintName = constraintName;
          constraintAdded = false;
          prevDefinition=definition;
      }
      
      if ( checkConstraint && ! constraintAdded) {
        checkConstraintList.add(new CheckConstraint(tableInfo.tableName , constraintName , checkColumns , definition));
      }
      tableInfo.setCheckconstraints(checkConstraintList);
      List<String> list =  new ArrayList<String> (constraintInfo.getTablesWithCheckConstraint());
      if (checkConstraintList.size() > 0 && !list.contains(tableInfo.getFullyQualifiedTableName())) {         
        list.add(tableInfo.getFullyQualifiedTableName());
      }
      else if(checkConstraintList.size() == 0 && list.contains(tableInfo.getFullyQualifiedTableName())){
        list.remove(tableInfo.getFullyQualifiedTableName());
      }
      constraintInfo.setTablesWithCheckConstraint(list);
    
    } catch (SQLException se ) {
      throw new TestException ("Error while retrieving the check key information " + TestHelper.getStackTrace(se));
    }
  }
  
}

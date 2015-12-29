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

import util.TestException;
import util.TestHelper;

/**
 * UniqueKeyInfoObjectFetcher
 * 
 * @author Namrata Thanvi
 */

public class UniqueKeyInfoObjectFetcher   extends GenericTableInfoObjectFetcher {
  Connection conn;
  List<UniqueConstraint> ukConstraint = new ArrayList<UniqueConstraint>(); 
  
  public UniqueKeyInfoObjectFetcher (TableInfo tableInfo , Executor executor, ConstraintInfoHolder constraintInfo ){
    super (tableInfo, executor, constraintInfo );
    conn=executor.getConnection();
  }
 
  
  public void fetch(){
    try{
      boolean uniqConstraint = false;
      boolean constraintAdded = false;
      String columnName = "";
      String constraintName = ""; 
      String indexName =  "";
      String prevConstraintName=" ";
      List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();      
      ResultSet uniqueKeys = conn.getMetaData().getIndexInfo(null,tableInfo.getSchemaName(), tableInfo.getTableName() , true, false);
      UniqueConstraint uniq;
      boolean partOfPk =false;
      
      while ( uniqueKeys.next()) {
        partOfPk =false;       
        uniqConstraint=true;
        columnName = uniqueKeys.getString("COLUMN_NAME");        
        indexName = uniqueKeys.getString("INDEX_NAME");   
        constraintName = getConstraintNameFromIndex(indexName);        
        if ( ! prevConstraintName.equals(" ") && prevConstraintName != constraintName) {          
          // got new unique key constraint, add previous constraint to the list
          uniq = new UniqueConstraint(tableInfo.getFullyQualifiedTableName() , prevConstraintName , columnList);
          if ( !tableInfo.checkUniqIsPartOfPk(uniq))
          ukConstraint.add(uniq);          
          columnList = new ArrayList<ColumnInfo>();
          constraintAdded = true;
        }
        //continue with old constraint
        columnList.add(tableInfo.getColumn(columnName));  
        prevConstraintName=constraintName;
        constraintAdded = false;
      }           
      
      if ( uniqConstraint && ! constraintAdded && !partOfPk) {        
        uniq = new UniqueConstraint(tableInfo.getFullyQualifiedTableName() , constraintName , columnList);
        if ( !tableInfo.checkUniqIsPartOfPk(uniq))
        ukConstraint.add(uniq);
      }
      
      tableInfo.setUniqueKeys(ukConstraint);
      List<String> list =  new ArrayList<String> (constraintInfo.getTablesWithUniqConstraint());
      if (ukConstraint.size() > 0 && !list.contains(tableInfo.getFullyQualifiedTableName())) {         
        list.add(tableInfo.getFullyQualifiedTableName());
      }
      else if(ukConstraint.size() == 0 && list.contains(tableInfo.getFullyQualifiedTableName())){
        list.remove(tableInfo.getFullyQualifiedTableName());
      }
      constraintInfo.setTablesWithUniqConstraint(list);
    } catch (SQLException se ) {
      throw new TestException ("Error while retrieving the unique key information " + TestHelper.getStackTrace(se));
    }
  }
  
  
  public String getConstraintNameFromIndex(String indexName) {
    String query = "SELECT CONSTRAINTNAME FROM SYS.SYSCONSTRAINTS , SYS.SYSKEYS , SYS.SYSCONGLOMERATES ";
    String where = "WHERE SYS.SYSCONSTRAINTS.CONSTRAINTID = SYS.SYSKEYS.CONSTRAINTID AND SYS.SYSKEYS.CONGLOMERATEID = sys.sysCONGLOMERATEs.CONGLOMERATEID " +
                   "AND CONGLOMERATENAME = '"+ indexName + "'";
    query = query + where;
    String constraintName = "";
    try {
      ResultSet rs = executor.executeQuery(query);
      while (rs.next())
        constraintName = rs.getString(1);
    }
    catch (SQLException se) {
      throw new TestException(
          "Error while retrieving the Unique key information " + query + " "
              + TestHelper.getStackTrace(se));
    }
    return constraintName;
  }
  
}

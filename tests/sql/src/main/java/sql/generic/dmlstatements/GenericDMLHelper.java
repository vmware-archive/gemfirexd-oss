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
package sql.generic.dmlstatements;

import hydra.Log;
import hydra.RemoteTestModule;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import util.TestException;
import sql.generic.SqlUtilityHelper;
import sql.generic.ddl.ColumnInfo;
import sql.generic.ddl.TableInfo;


/**
 * GenericDMLHelper
 * 
 * @author Namrata Thanvi
 */

public class GenericDMLHelper {    

  

  public static  String  getQuery(String columnName, String symbol) {
    String tableName = SqlUtilityHelper.getTableName(columnName);
    String columnNameinTable = SqlUtilityHelper.getColumnName(columnName, true);
    
    String query = "select " + columnNameinTable + " from " + tableName
                    + " where " + columnNameinTable + symbol + " ?" 
                    + " and tid = " + getMyTid() + " order by " + columnNameinTable
                    + " desc";
    return query;
}
  
  
  protected static int getMyTid() {
    if (SQLOldTest.testWanUniqueness) {
      return sql.wan.WanTest.myWanSite; //return wan site id as tid, use tid to confine each site op on its set of keys
    }
    
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    return myTid;
  }
  
  public static boolean isDerby(Connection conn)  {
    try {
    if (conn.getMetaData().getDriverName().toLowerCase().contains("derby")) 
      return true;
      else  
        return false;
  } catch ( SQLException se ) {
    throw new TestException(" can not get driver information from connection object" + se.getMessage());
  }         
}   
  
  public static boolean updateOnPk(String stmt) {
    Log.getLogWriter().info(" in  updateOnPk " + stmt );
    if (! stmt.contains("UPDATE " ) ){
      return  false;
    }
    int start = stmt.toUpperCase().indexOf(" SET ");
    int end = stmt.toUpperCase().indexOf(" WHERE ");
    String tableName = stmt.substring((stmt.toUpperCase().indexOf("UPDATE ") + 7 ), start );
    if ( end < start ) end = stmt.length();
    
    Log.getLogWriter().info("table Name is " + tableName + "fetched from index " + ( stmt.toUpperCase().indexOf("UPDATE ") + 7 ) + " start index " + start);
    Log.getLogWriter().info(" subString is " +  stmt.substring(start,end) + " with start " + start + " end " + end  );
    String stmtToCheck = stmt.substring(start,end);
    TableInfo  info = GenericBBHelper.getTableInfo(tableName);
    List<ColumnInfo>  partitioningColumns = info.getPartitioningColumns();
    
    for ( ColumnInfo column : partitioningColumns){
         Log.getLogWriter().info("checking column " + column.getColumnName() );
         if ( stmtToCheck.contains(column.getColumnName().toUpperCase())) {
           return true;
         }
    }
    return true;
  }
  
}

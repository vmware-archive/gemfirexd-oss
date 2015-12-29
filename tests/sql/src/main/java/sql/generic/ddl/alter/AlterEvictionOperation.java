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
package sql.generic.ddl.alter;

import hydra.Log;

import java.sql.SQLException;
import java.util.List;

import sql.generic.GenericBBHelper;
import sql.generic.SQLOldTest;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.TableInfo;

public class AlterEvictionOperation extends Operation {

  String evictionMaxsize = "";
  int minValue = 5000;
  int maxValue = 10000;

  final String keyToReplace = "[EVICTION-MAXSIZE]";
  
  AlterEvictionOperation(String ddl , Executor executor , DDLAction action){
    super(ddl,executor,action);
  }
  
  public AlterEvictionOperation parse(){
    switch (action) {
      case SET :
        setEvictionMaxSize();
        break;
     }
    return this;
  }

  protected void setEvictionMaxSize(){
    tableInfo = getTableInfo();
    evictionMaxsize = getEvicionMaxSize();
    parsedDDL= parsedDDL.replace(tableNameToReplace, tableInfo.getFullyQualifiedTableName());
    parsedDDL= parsedDDL.replace(keyToReplace, evictionMaxsize);
  }
  
  // get random table
  protected  TableInfo getTableInfo(){
    List<String> tables = constraintHolder.getTablesList();
    tableName=  tables.get(SQLOldTest.random.nextInt(tables.size()));
    return GenericBBHelper.getTableInfo(tableName);
  }
  
  protected String getEvicionMaxSize()
  {
    int evictSize = (int)(SQLOldTest.random.nextDouble() * (maxValue-minValue)) + minValue;
    return " "+evictSize;
  }
  
  public void handleAlterDDLException (SQLException  derby , SQLException gemxd){
    Log.getLogWriter().info("Derby Exception is : " + derby);  
    Log.getLogWriter().info("GFXD Exception is : " + gemxd);   
    compareExceptions(derby,gemxd);
  }
  
  public void handleAlterDDLException (SQLException gemxd){ 
    Log.getLogWriter().info("GFXD Exception is : " + gemxd);      
  }
  
}
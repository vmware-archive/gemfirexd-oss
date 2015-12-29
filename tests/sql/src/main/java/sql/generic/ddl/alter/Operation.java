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
import java.util.HashMap;

import sql.SQLHelper;
import sql.generic.GenericBBHelper;
import sql.generic.ddl.ConstraintInfoHolder;
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import sql.generic.ddl.TableInfoGenerator;
import sql.generic.ddl.Executor.ExceptionMapKeys;
import sql.generic.ddl.TableInfo;
import util.TestException;
import util.TestHelper;

public abstract  class Operation {
  DDLAction action;
  Executor executor;
  String ddl;
  String parsedDDL;
  TableInfo tableInfo;
  ConstraintInfoHolder constraintHolder;
  String columnName = "";
  String constraintName = "";
  boolean executeDDL = true;
  TableInfoGenerator tableInfoGen;
  String tableName;
  boolean genericDDL = true;
  boolean isFixed51273 = false;
  boolean isFixed51200 = false;  
  
  final String consNameToReplace = "[CONSTRAINT-NAME]";
  final String  tableNameToReplace = "[TABLE-NAME]";
  final String columnNameToReplace =  "[COLUMN-NAME]"; 
  
  Operation(String ddl , Executor executor , DDLAction action){
    this.action = action;
    this.ddl = ddl;
    this.executor = executor;
    constraintHolder =  GenericBBHelper.getConstraintInfoHolder();
    parsedDDL = ddl;
    if(!ddl.contains("[")) 
      genericDDL = false;
  }
  
  abstract public Operation parse();
  
  public void execute() {
    if(!genericDDL){
      String[] ddlArr = ddl.split(" ");
      tableName = ddlArr[2];
      tableInfo = GenericBBHelper.getTableInfo(tableName);
    }
    if (executeDDL) {
      Log.getLogWriter().info("DDL after parsing :: " + getParsedDDL());
      if (this.action == DDLAction.SET) {
        Log.getLogWriter().info("Executing alter statement on GFXD ...");
        HashMap<ExceptionMapKeys, SQLException> exceptionMap = executor
            .executeOnlyOnGFXD(parsedDDL);
        if (exceptionMap.get(ExceptionMapKeys.GEMXD) == null) {
          Log.getLogWriter().info(
              "Successfully executed alter statement GFXD ...");
          executor.commit();
          updateBB();
        }
        else
          handleAlterDDLException(exceptionMap.get(ExceptionMapKeys.GEMXD));
      }
      else {
        HashMap<ExceptionMapKeys, SQLException> exceptionMap = executor
            .executeOnBothDb(parsedDDL);
        if (exceptionMap.get(ExceptionMapKeys.DERBY) == null
            && exceptionMap.get(ExceptionMapKeys.GEMXD) == null) {
          Log.getLogWriter().info(
              "Successfully executed alter statement on DERBY and GFXD ...");
          executor.commit();
          updateBB();
        }
        else
          handleAlterDDLException(exceptionMap.get(ExceptionMapKeys.DERBY),
              exceptionMap.get(ExceptionMapKeys.GEMXD));
      }
    } else {
      Log.getLogWriter().info("Could not execute alter statement, continuing test ...");
    }
  }

  public void compareExceptions(SQLException dse, SQLException sse) {
    Log.getLogWriter().info("In compare exceptions...");
    if (dse != null && sse != null) {
      if (!dse.getSQLState().equalsIgnoreCase(sse.getSQLState())) {    
        Log.getLogWriter().warning("Compare exceptions failed...");
        Log.getLogWriter().info("Derby exception : ");
        SQLHelper.printSQLException(dse);
        Log.getLogWriter().info("GFXD Exception : ");
        SQLHelper.printSQLException(sse);
        throw new TestException("Exceptions are not same -- for derby: " + TestHelper.getStackTrace(dse)
            + "\n for gfxd: " + TestHelper.getStackTrace(sse));
      } else {
        Log.getLogWriter().info("Got the same exceptions from derby and gfxd.");
      }
    } else if (dse != null) {      
      SQLHelper.printSQLException(dse);
      Log.getLogWriter().warning("Compare exceptions failed");
      throw new TestException("Derby got the exception, but gfxd did not\n" + TestHelper.getStackTrace(dse));
      
    } else {
      Log.getLogWriter().warning("Compare exceptions failed");
      SQLHelper.printSQLException(sse);
      throw new TestException("GFXD got the exception, but derby did not\n" + TestHelper.getStackTrace(sse));
    }
  }
  abstract protected void handleAlterDDLException(SQLException sqlException);

  abstract protected void handleAlterDDLException (SQLException  derby , SQLException gemxd);

  protected void updateBB(){
    tableInfoGen = new TableInfoGenerator(tableInfo, executor, constraintHolder);
    tableInfo = tableInfoGen.updateTableInfo();
    tableInfoGen.saveTableInfoToBB();
  }
  
  protected void replacePlaceHolders(String tableName, String constraintName, String columnName){
    parsedDDL= parsedDDL.replace(tableNameToReplace, tableInfo.getFullyQualifiedTableName());
    parsedDDL= parsedDDL.replace(consNameToReplace, constraintName);
    parsedDDL= parsedDDL.replace(columnNameToReplace, columnName);
  }
  
  public String getParsedDDL() {
    return parsedDDL;
  }
   
}

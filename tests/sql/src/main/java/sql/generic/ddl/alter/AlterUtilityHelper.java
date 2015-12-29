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

import sql.generic.SQLOldTest;
import sql.generic.ddl.Constraint.ConstraintType;

public class AlterUtilityHelper {

  
  public static String  getGenericColumnName(String tableName){
    // 1 out of 10 times it test the boundaries and remaining time it can give  you the some logical name
    String columnName ="";
    //to hit the scenario of problem with very long column Names
    if ( SQLOldTest.random.nextInt() %10 == 0) {       
      int start = 97;
      int end = 122;
      int charLength = SQLOldTest.random.nextInt(130) + 1; 
      while (columnName.length() < charLength ){
        columnName+= (char)((int)(SQLOldTest.random.nextDouble() * (end - start + 1) ) + start);
      } //to hit the scenario that column already exist
    } else if  (SQLOldTest.random.nextInt() %10 == 1)  {
        columnName= tableName + "_column_1" ;
    } //successfully return a new column name
      else {
      columnName= tableName + "_column_" +  SQLOldTest.random.nextInt(30) ;
    }    
    return columnName;
  }
  

  // get constraint name in case of add new constraint
  public static String  getRandomConstraintName(String tableName, ConstraintType constraintType){
    return tableName + "_" + constraintType + SQLOldTest.random.nextInt(30);
  }
  
}
  


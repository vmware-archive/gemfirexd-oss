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

import sql.generic.dmlstatements.DBRow.Column;

/**
 * GenericLikeExpression
 * 
 * @author Namrata Thanvi
 */



public class GenerateLikeExpression {
  
  private String columnName;
  private DBRow row;
  final int combination = 4; 
  
  public GenerateLikeExpression (String columnName , DBRow  row) {
    this.columnName=columnName;
    this.row = row;     
  }
  
  
  
  //to support more expression add more here
  
  public String getLikeExpression(){
    
   String partcolumnName=  getColumnNameSubstring();
   return getLikeExpression(partcolumnName);
  }
  
  private String getLikeExpression(String partColumnname){
    
    int myExpresion = GenericDML.rand.nextInt(combination);    
    String finalLikeExpression ="";
    switch  (myExpresion){
      case 0:
              finalLikeExpression= "%" + partColumnname + "%";
               break;
      case 1: 
               finalLikeExpression= "_" + partColumnname + "_";
               break;
      case 2:
               finalLikeExpression= "%" + partColumnname + "_";
               break;
      case 3:
              finalLikeExpression= "_" + partColumnname + "%";
              break;
    }
    
    return  finalLikeExpression;
  }
  
  public String getLikeColumnName(){
    return row.find( columnName.substring(columnName.indexOf("(") + 1, columnName.indexOf(")")).trim() ).getName();    
  }
  
  public static String getLikeColumnName(String columnNameWithRelationShip){
     return columnNameWithRelationShip.substring(columnNameWithRelationShip.indexOf("(") + 1, columnNameWithRelationShip.indexOf(")")).trim() ;
  }
  
  
  
  private String getColumnNameSubstring(){
    Column columnNameinDb = row.find(getLikeColumnName());
    String value = (String) columnNameinDb.getValue();
    int length=value.length();
    
    int startLength , endLength;
    int length1 = GenericDML.rand.nextInt(length);
    int length2 = GenericDML.rand.nextInt(length);
    if ( length1 >= length2 ){
        endLength = length1 ;
        startLength = length2 ;        
     } else {
       endLength = length2 ;
       startLength = length1 ; 
     }
    
    if (startLength == endLength && endLength < length  ) {
      endLength++;
    }
    
    if (startLength == endLength && startLength == length &&  startLength > 0 ) {
      startLength--;
    }
        
    return  ((String)columnNameinDb.getValue()).substring(startLength , endLength);    
  }
}

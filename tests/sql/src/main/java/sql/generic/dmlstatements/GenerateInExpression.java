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

import java.util.List;
import static sql.generic.SqlUtilityHelper.getTableName;
import static sql.generic.SqlUtilityHelper.getColumnName;


/**
 * GenericInExpression
 * 
 * @author Namrata Thanvi
 */

public class GenerateInExpression {
  private String columnName;
  private DBRow row;
  private DMLExecutor executor;
  String prepareStatement;
  
  public GenerateInExpression (String columnName , DBRow  row , DMLExecutor executor ,String stmt) {
    this.columnName=columnName;
    this.row = row;     
    this.executor = executor;
    this.prepareStatement = stmt;
  }
  
  
  public List<DBRow> getInExpressionValues() {
    int totalColumnsInExpression = getNumberOfValuesToGenerate();
    String query = GenericDMLHelper.getQuery(getInColumnName(), ">");
    List<DBRow> rowUsedForInPredicate =  executor.getRandomRowsForDml(query,row.find(getInColumnName()).getValue(),row.find(getInColumnName()).getType(),totalColumnsInExpression, getTableName(columnName));
    createPrepareStatementForIn(rowUsedForInPredicate.size() , totalColumnsInExpression);
    return rowUsedForInPredicate;
  }
  

  public boolean  isInColumnName(){
   if  ( columnName.contains("[") )
   return true;
   else 
      return false;
  }
  
  
  private void createPrepareStatementForIn(int rows , int actualColumns ) {
    //do not find matching rows
    
    String column=columnName.toUpperCase().trim().replace("[", "\\[");
    column=column.replace("]", "\\]");
    String pattternToReplace = "::[ ]*("+ column + ")[ ]*::";
        
    if ( rows != 0  ) {    
    prepareStatement=prepareStatement.replaceAll(pattternToReplace , getReplacementString(rows));
    }
        
  }
  
  public String getModifiedPrepareStatement(){
    return prepareStatement;
  }
  
  private String  getReplacementString(int rows ) {
    
    int count =0;
    final String PARAMETER = " ? ,";
      
    String replacementString = new String();
     while ( count++ < rows ){
       replacementString += PARAMETER;      
     }
     
     
       return replacementString.substring(0,replacementString.length() - 1 );
   }
  
  private String getInColumnName(){
    return getColumnName(columnName, true);
  }
  
  private int getNumberOfValuesToGenerate(){
    String[]  values = columnName.split("[\\[\\]]");
     return Integer.parseInt(values[1].trim());
  }
  
}

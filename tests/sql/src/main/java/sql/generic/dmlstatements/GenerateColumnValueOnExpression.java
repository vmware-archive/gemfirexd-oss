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

import static sql.generic.SqlUtilityHelper.getColumnName;
import java.util.List;




public class GenerateColumnValueOnExpression {
  
  private DBRow row ;
  private DBRow.Column  dbColumn;
  protected DMLExecutor executor;
  String prepareStatement;
  
  public GenerateColumnValueOnExpression(DBRow row , DMLExecutor executor , String stmt){
     this.row = row;    
     this.executor = executor;
     this.prepareStatement =stmt; 
  }
  
  public  Object getColumnValue(String column )  {
  dbColumn = row.find(getColumnName(column,true));
  Object columnValue = dbColumn.getValue();
  
  if (column.contains("<") || column.contains("!=")) {  
  //relation with some other column
    columnValue = getLesserColumnValue(column, dbColumn.getValue(), dbColumn.getType()).getValue();
  } else if (column.contains(">")) {
    //relation with some other column
    columnValue =   getGreaterColumnValue(column, dbColumn.getValue(),dbColumn.getType()).getValue();
  } else if (column.toLowerCase().contains ("substr")) {
    //it is an like expression
    columnValue =  new GenerateLikeExpression(column , row).getLikeExpression();
  } else if ( column.contains("[")) {
    //it is an in expression    
    GenerateInExpression inExpr = new GenerateInExpression(column, row , executor , prepareStatement);
    List<DBRow> values = inExpr.getInExpressionValues();    
    if (values !=null && values.size() >  0) {
      prepareStatement=inExpr.getModifiedPrepareStatement();
      columnValue=values;
    }
  }
 
 return columnValue;
}
   

public String getModifiedPrepareStatement(){
    return prepareStatement;
  }
  
protected DBRow.Column getGreaterColumnValue( String columnName, Object value ,int type)  {
    return getLesserOrGreaterColumnValue(columnName, value, " > ",type);
}

protected DBRow.Column getLesserColumnValue( String columnName, Object value, int type)  {
    return getLesserOrGreaterColumnValue(columnName, value, " < ",type);
}


protected DBRow.Column getLesserOrGreaterColumnValue(String columnName, Object value, String symbol , int type){
  
  return executor.getRandomColumnForDML(GenericDMLHelper.getQuery(columnName, symbol) , value , type);
}


}

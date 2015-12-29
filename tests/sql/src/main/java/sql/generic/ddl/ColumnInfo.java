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

import java.util.List;


/**
 * ColumnInfo
 * 
 * @author Namrata Thanvi
 */

public class ColumnInfo implements java.io.Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  String columnName;
  int columnType;
  boolean isNull;
  List<Object> valueList;   
  int columnSize ;
  
  public ColumnInfo ( String columnName , int columnType , int columnSize , boolean isNull , List<Object> valueList){
    this.columnName = columnName;
    this.columnType = columnType;
    this.valueList = valueList;   
    this.isNull=isNull;
    this.columnSize=columnSize;
  }
    
  
  public ColumnInfo ( String columnName , int columnType  , boolean isNull ){
    this.columnName = columnName;
    this.columnType = columnType;
    this.valueList = null;   
    this.columnSize=0;
    this.isNull=isNull;
  }
  
  public String getColumnName() {
    return columnName;
  }
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
  public int getColumnType() {
    return columnType;
  }
    
  public void setColumnType(int columnType) {
    this.columnType = columnType;
  }
  
  
  public boolean isNull() {
    return isNull;
  }


  public void setNull(boolean isNull) {
    this.isNull = isNull;
  }

  public List<Object> getValueList(){
    return valueList;    
  }
}

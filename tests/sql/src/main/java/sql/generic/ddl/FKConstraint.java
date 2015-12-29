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

import java.util.List;

/**
 * FKConstraint
 * 
 * @author Namrata Thanvi
 */
public class FKConstraint extends Constraint implements java.io.Serializable {
 
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String parentTable;
  private List<ColumnInfo> parentColumns;

  public FKConstraint (String tableName , String constraintName , List<ColumnInfo> columns , String parentTable , List<ColumnInfo> parentColumns){
    super(tableName , constraintName,  columns,ConstraintType.FK_CONSTRAINT);
    this.parentTable = parentTable;
    this.parentColumns = parentColumns;
  }
  
  public String getParentTable() {
    return parentTable;
  }

  public void setParentTable(String parentTable) {
    this.parentTable = parentTable;
  }
 
  public List<ColumnInfo> getParentColumns() {
    return parentColumns;
  }

  public void setParentColumns(List<ColumnInfo> parentColumns) {
    this.parentColumns = parentColumns;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FKConstraint && obj != null) {
      if (((FKConstraint)obj).getConstraintName().equals(this.getConstraintName()))
        return true;
      else
        return false;
    }
    else
      return false;
  }

  @Override
  public int hashCode() {
    return this.getConstraintName().hashCode();
  }
  
  
}
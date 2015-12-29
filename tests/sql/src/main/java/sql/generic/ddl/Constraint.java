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

import java.util.Collections;
import java.util.List;

/**
 * Constraint
 * 
 * @author Namrata Thanvi 
 * All the objects of this class are modified by the tableInfo only. please do not modify directly until and unless required.
 */

public abstract class Constraint implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  public enum ConstraintType {
    FK_CONSTRAINT, PK_CONSTRAINT, UNIQUE, CHECK
  }
  
  String fullyQualifiedTableName;
  String constraintName;
  List<ColumnInfo> columns;
  ConstraintType constraintType;

  public Constraint(String fullyQualifiedTableName, String constraintName,
      List<ColumnInfo> columns) {
    this.columns = columns;
    this.fullyQualifiedTableName = fullyQualifiedTableName;
    this.constraintName = constraintName;
  }

  public Constraint(String fullyQualifiedTableName, String constraintName,
      List<ColumnInfo> columns, ConstraintType constraintType) {
    this.columns = columns;
    this.fullyQualifiedTableName = fullyQualifiedTableName;
    this.constraintName = constraintName;
    this.constraintType = constraintType;
  }

  public String getConstraintName() {
    return constraintName;
  }

  public void setConstraintName(String constraintName) {
    this.constraintName = constraintName;
  }

  public List<ColumnInfo> getColumns() {
    return Collections.unmodifiableList(columns);
  }

  public void setColumns(List<ColumnInfo> columns) {
    this.columns = columns;
  }

  public String getFullyQualifiedTableName() {
    return fullyQualifiedTableName;
  }

  public void setFullyQualifiedTableName(String fullyQualifiedTableName) {
    this.fullyQualifiedTableName = fullyQualifiedTableName;
  }

  public ConstraintType getConstraintType() {
    return constraintType;
  }

  public void setConstraintType(ConstraintType constraintType) {
    this.constraintType = constraintType;
  }
}

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
 * CheckConstraint
 * 
 * @author Namrata Thanvi
 */
public class CheckConstraint extends Constraint {
private static final long serialVersionUID = 1L;
String definition;
  
  public CheckConstraint (String tableName , String constraintName , List<ColumnInfo> columns , String definition ){
    super(tableName , constraintName,  columns , ConstraintType.CHECK);
    this.definition = definition;
  }

  public String getDefinition() {
    return definition;
  }

  public void setDefinition(String definition) {
    this.definition = definition;
  }
}
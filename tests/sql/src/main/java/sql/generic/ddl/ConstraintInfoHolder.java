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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public  class  ConstraintInfoHolder implements java.io.Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  List<String> tablesWithPKConstraint;
  List<String> tablesWithFKConstraint;
  List<String> tablesWithUniqConstraint;
  List<String> tablesWithCheckConstraint;
  List<String> tablesList;
  HashMap<String , List<FKConstraint>> droppedFkList;
  
  //private static ConstraintInfoHolder INSTANCE = new ConstraintInfoHolder();
  
  public ConstraintInfoHolder() {
    tablesWithPKConstraint=new ArrayList<String>();
    tablesWithFKConstraint = new ArrayList<String>();
    tablesWithCheckConstraint = new ArrayList<String>();
    tablesWithUniqConstraint = new ArrayList<String>(); 
    tablesList  = new ArrayList<String>(); 
    droppedFkList =  new HashMap<String , List<FKConstraint>>(); 
  }

  public List<String> getTablesWithPKConstraint() {
    return Collections.unmodifiableList(tablesWithPKConstraint);
  }
  
  public void setTablesWithPKConstraint(List<String> tablesWithPKConstraint) {
    this.tablesWithPKConstraint = tablesWithPKConstraint;
  }
  
  public List<String> getTablesWithFKConstraint() {
    return Collections.unmodifiableList(tablesWithFKConstraint);
  }
  
  public void setTablesWithFKConstraint(List<String> tablesWithFKConstraint) {
    this.tablesWithFKConstraint = tablesWithFKConstraint;
  }

  public List<String> getTablesWithUniqConstraint() {
    return Collections.unmodifiableList(tablesWithUniqConstraint);
  }
  
  public void setTablesWithUniqConstraint(List<String> tablesWithUniqConstraint) {
    this.tablesWithUniqConstraint = tablesWithUniqConstraint;
  }
  
  public List<String> getTablesWithCheckConstraint() {
    return Collections.unmodifiableList(tablesWithCheckConstraint);
  }

  public void setTablesWithCheckConstraint(List<String> tablesWithCheckConstraint) {
    this.tablesWithCheckConstraint = tablesWithCheckConstraint;
  }
  public List<String> getTablesList() {
    return Collections.unmodifiableList(tablesList);
  }
  
  public void setTablesList(List<String> tablesList) {
    this.tablesList = tablesList;
  }

  public Map<String , List<FKConstraint>>  getDroppedFkList() {
   return Collections.unmodifiableMap(droppedFkList);
  }

  public void setDroppedFkList(HashMap<String , List<FKConstraint>> droppedFkList) {
    this.droppedFkList = droppedFkList;
  }
}

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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;


import com.pivotal.gemfirexd.internal.impl.sql.compile.ProjectRestrictNode;

/**
 * 
 * @author Asif
 *
 */
//Make sure that the Scope object is never cached , except in the stack of QueryInfoContext.
// and once popped out it should not be referred to anywhere
public class Scope
{
  private int numPRTables = 0;
  private int numTables = 0;
  //Each table appearing in a query gets a unique number across the subqueries present.
  //Since the tableCount & prCount only represent the tables in a current scope, while 
  // the TableQueryInfoList is prepared based on actual table number ( which implies that in
  //subqueries there may be empty slots) we need to find out , given a table number , the scope to
  //which it belongs. 
  
  final private DMLQueryInfo dmlInfo ;
  //Zero based
  private int scopeLevel;
  
  /**
   * This is used intermediately and should be released at the outer most 
   * calling point otherwise it will leak memory.
   * @see QueryInfoContext#cleanUp 
   */
  private ProjectRestrictNode parentPRN;
  
  Scope (DMLQueryInfo top, int scopeLevel) {
    this.dmlInfo = top;   
    this.scopeLevel = scopeLevel;
  }
  
  
  
  void addToPRTableCount(int cnt) {
    checkValidity();
    this.numPRTables += cnt;
  }

  void addToTableCount(int cnt) {
    checkValidity();
    this.numTables += cnt;
  }

  int getPRTableCount() {
    checkValidity();
    return this.numPRTables;
  }

  int getTableCount() {
    checkValidity();
    return this.numTables;
  }
  void setParentPRN(ProjectRestrictNode prn) {
    checkValidity();
    parentPRN = prn;
  }
  
  ProjectRestrictNode getParentPRN() {
    checkValidity();
    return parentPRN;
  }

  /**
   * The caller should call this method to release any 
   * unwarranted handles to the query tree. 
   * 
   * If not done so, parentPRN like references will leak
   * memory.
   */
  void cleanUp() {
    setParentPRN(null);
  }
  
  DMLQueryInfo getDMLQueryInfo() {
    checkValidity();
    return this.dmlInfo;
  }
  
  public int getScopeLevel() {
    checkValidity();
    return this.scopeLevel;
  }
  public void  unsetLevel() {
    checkValidity();
    this.scopeLevel = -1;
  }
  public void checkValidity() {
   if(this.scopeLevel == -1) {
     throw new IllegalStateException("A Scope object should not be referred to once popped out");
   }
  }

}

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
package com.pivotal.gemfirexd.tools.gfxdtop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.swing.table.AbstractTableModel;

public class StatementTableModel extends AbstractTableModel {
  
  
  /**
   * 
   */
  private static final long serialVersionUID = 5313465349050294789L;  

  private String[] columnNames = { "Statement", "numExecution", "numExecutionsInProgress", 
        "executionTime", "totalExecutionTime", "numTimesGlobalIndexLookup" };
  
  private static final int STATEMENT_INDEX=0;
  private static final int NnumExecution_INDEX=1;
  private static final int NumExecutionsInProgress_INDEX=2;
  private static final int ExecutionTime_INDEX=3;
  private static final int TotalExecutionTimeT_INDEX=4;
  private static final int NumTimesGlobalIndexLookupT_INDEX=5;
  
  private List<Statement> statementList = Collections.EMPTY_LIST;

  public StatementTableModel() {
  }

  public int getColumnCount() {
    return columnNames.length;
  }

  public int getRowCount() {
    return statementList.size();
  }

  public String getColumnName(int col) {
    return columnNames[col];
  }
  
  
  public Object getValueAt(int row, int col) {
    Statement me = statementList.get(row);
    switch (col) {
    case STATEMENT_INDEX:
      return me.getQueryDefinition();
    case NnumExecution_INDEX:
      return me.getNumExecution();
    case NumExecutionsInProgress_INDEX:
      return me.getNumExecutionsInProgress();
    case ExecutionTime_INDEX:
      return me.getExecutionTime();
    case TotalExecutionTimeT_INDEX:
      return me.getTotalExecutionTime();    
    case NumTimesGlobalIndexLookupT_INDEX:
      return me.getNumTimesGlobalIndexLookup();          
    default:
      return null;
    }
  }

  public Class getColumnClass(int c) {
    return getValueAt(0, c).getClass();
  }

  public void setStatementList(List<Statement> statementList) {
    this.statementList = statementList;
  }
  
  public  void setStatementList(Collection<Statement> statementList) {
    List<Statement> list = new ArrayList<Statement>();
    for(Statement s : statementList)
      list.add(s);
    Collections.sort(list);    
    this.statementList = list;
  }

  
}
